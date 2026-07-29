from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator

import grpc.aio

from subscriber import logger
from subscriber.config import SubscriberConfig
from subscriber.engine.dashllm_grpc import DashllmGrpcClient
from subscriber.engine.metadata import (
    KvCacheDescriptor,
    MetadataProtocolError,
    MetadataTemporarilyUnavailable,
)
from subscriber.health.events import LivenessEvent
from subscriber.metrics import report_engine_probe
from subscriber.proto import engine_service_rpc_pb2
from subscriber.types import KvCacheGroupSpec

_METADATA_RETRY_BASE_S = 0.5
_METADATA_RETRY_MAX_S = 30.0

_HEALTH_LOG_SUPPRESS_INTERVAL = 12


def _metadata_response_error(payload: object) -> tuple[int, str] | None:
    """Return an explicit metadata response error, if present."""

    err_code = getattr(payload, "err_code", 0)
    if not isinstance(err_code, int) or err_code == (
        engine_service_rpc_pb2.KV_CACHE_DESCRIPTOR_OK
    ):
        return None
    err_msg = getattr(payload, "err_msg", "")
    return err_code, err_msg if isinstance(err_msg, str) else ""


def _parse_kv_cache_descriptor(payload: object) -> KvCacheDescriptor:
    """Parse a gRPC metadata response into an authoritative result.

    An empty ``items`` list is a valid empty topology. A missing ``items``
    field, a wrong field shape, duplicate ``group_idx`` values, or invalid
    sizes are malformed authoritative responses and raise
    :class:`MetadataProtocolError`.
    """

    raw = getattr(payload, "items", None)
    if raw is None:
        raise MetadataProtocolError(
            "kv cache descriptor response is missing the items field"
        )
    specs: list[KvCacheGroupSpec] = []
    seen_group_idx: set[int] = set()
    for entry in raw:
        group_idx = getattr(entry, "group_idx", None)
        kind = getattr(entry, "kind", None)
        block_size = getattr(entry, "block_size", None)
        if (
            not isinstance(group_idx, int)
            or not isinstance(kind, str)
            or not isinstance(block_size, int)
        ):
            raise MetadataProtocolError(
                "kv cache descriptor entry has an invalid field shape"
            )
        if group_idx in seen_group_idx:
            raise MetadataProtocolError(
                f"kv cache descriptor has duplicate group_idx {group_idx}"
            )
        seen_group_idx.add(group_idx)
        if block_size <= 0:
            raise MetadataProtocolError(
                f"kv cache descriptor group_idx {group_idx} has invalid "
                f"block_size {block_size}"
            )
        sliding_window = getattr(entry, "sliding_window", None)
        if isinstance(sliding_window, int) and sliding_window != -1:
            if sliding_window < -1:
                raise MetadataProtocolError(
                    f"kv cache descriptor group_idx {group_idx} has invalid "
                    f"sliding_window {sliding_window}"
                )
            window: int | None = sliding_window
        else:
            window = None
        specs.append(
            KvCacheGroupSpec(
                group_idx=group_idx,
                kind=kind,
                block_size=block_size,
                sliding_window=window,
            )
        )
    settings = getattr(payload, "settings", None)
    use_eagle_pop = (
        bool(getattr(settings, "use_eagle_pop", False)) if settings else False
    )
    mamba_cache_mode = (
        str(getattr(settings, "mamba_cache_mode", "") or "none") if settings else "none"
    )
    return KvCacheDescriptor(
        groups=tuple(specs),
        use_eagle_pop=use_eagle_pop,
        mamba_cache_mode=mamba_cache_mode,
    )


class VllmControl:
    """Own vLLM health polling and DashLLM metadata control RPCs."""

    def __init__(
        self,
        config: SubscriberConfig,
        grpc_client: DashllmGrpcClient,
    ) -> None:
        self._config = config
        self._dashllm_grpc_client = grpc_client
        self._closed = False

    async def watch_liveness(self) -> AsyncGenerator[LivenessEvent, None]:
        """Poll engine liveness via gRPC GetWorkerStatus."""

        was_healthy: bool | None = None
        consecutive_failures = 0

        while True:
            probe_started_at = time.monotonic()
            probe_result = "alive"
            try:
                status = await self._dashllm_grpc_client.get_worker_status(
                    self._config.engine_kvcache_worker_status_timeout_ms / 1000
                )
                event = (
                    LivenessEvent.HEALTHY if status.alive else LivenessEvent.UNHEALTHY
                )
                probe_result = "alive" if status.alive else "dead"
                if not status.alive and was_healthy is not False:
                    logger.warning(
                        "engine health probe reported not alive",
                        step="engine_health",
                        tags={
                            "target": self._config.engine_grpc_endpoint,
                        },
                    )
            except asyncio.CancelledError:
                raise
            except grpc.aio.AioRpcError as exc:
                event = LivenessEvent.UNHEALTHY
                code = exc.code()
                probe_result = (
                    "timeout"
                    if code is not None and code.name == "DEADLINE_EXCEEDED"
                    else "rpc_error"
                )
                if was_healthy is not False:
                    logger.warning(
                        "engine health probe failed",
                        step="engine_health",
                        tags={
                            "error": type(exc).__name__,
                            "code": code.name if code is not None else "UNKNOWN",
                            "details": exc.details(),
                            "target": self._config.engine_grpc_endpoint,
                        },
                    )
            except Exception as exc:
                event = LivenessEvent.UNHEALTHY
                probe_result = "rpc_error"
                if was_healthy is not False:
                    logger.warning(
                        "engine health probe failed unexpectedly",
                        step="engine_health",
                        tags={
                            "error": exc.__class__.__name__,
                            "message": str(exc),
                            "target": self._config.engine_grpc_endpoint,
                        },
                    )

            report_engine_probe(
                result=probe_result,
                latency_ms=(time.monotonic() - probe_started_at) * 1000,
            )

            is_healthy = event is LivenessEvent.HEALTHY

            if not is_healthy:
                consecutive_failures += 1
                if was_healthy is False and (
                    consecutive_failures % _HEALTH_LOG_SUPPRESS_INTERVAL == 0
                ):
                    logger.warning(
                        "engine still unhealthy",
                        step="engine_health",
                        tags={
                            "consecutive_failures": consecutive_failures,
                            "target": self._config.engine_grpc_endpoint,
                        },
                    )
            else:
                if was_healthy is False:
                    logger.info(
                        "engine health recovered",
                        step="engine_health",
                        tags={
                            "consecutive_failures_before_recovery": (
                                consecutive_failures
                            ),
                            "target": self._config.engine_grpc_endpoint,
                        },
                    )
                consecutive_failures = 0

            was_healthy = is_healthy
            yield event
            await asyncio.sleep(self._config.engine_health_interval_s)

    async def fetch_kv_cache_descriptor(self) -> KvCacheDescriptor:
        """Fetch per-group metadata with bounded exponential-backoff retries.

        Retryable failures (transport errors and explicit UNAVAILABLE responses)
        exhaust the bounded per-attempt retry policy, then raise
        :class:`MetadataTemporarilyUnavailable`. Non-retryable failures (any
        other explicit error code, or a malformed response) raise
        :class:`MetadataProtocolError` immediately. A successful response returns
        :class:`KvCacheDescriptor`; an empty group tuple is valid.
        """

        max_retries = self._config.engine_kvcache_descriptor_max_retries
        delay = _METADATA_RETRY_BASE_S
        for attempt in range(1, max_retries + 1):
            try:
                payload = await self._dashllm_grpc_client.get_kv_cache_descriptor(
                    self._config.engine_kvcache_descriptor_timeout_ms / 1000
                )
                response_error = _metadata_response_error(payload)
                if response_error is not None:
                    err_code, err_msg = response_error
                    if err_code == (
                        engine_service_rpc_pb2.KV_CACHE_DESCRIPTOR_UNAVAILABLE
                    ):
                        raise MetadataTemporarilyUnavailable(
                            f"DashLLM metadata unavailable ({err_code}): {err_msg}"
                        )
                    raise MetadataProtocolError(
                        f"DashLLM metadata non-retryable error ({err_code}): {err_msg}"
                    )
                metadata = _parse_kv_cache_descriptor(payload)
            except asyncio.CancelledError:
                raise
            except MetadataProtocolError:
                logger.warning(
                    "received non-retryable kv cache descriptor response",
                    step="kv_metadata",
                    tags={
                        "target": self._config.engine_grpc_endpoint,
                        "attempt": attempt,
                    },
                    exc_info=True,
                )
                raise
            except Exception as exc:
                if attempt >= max_retries:
                    logger.warning(
                        "failed to fetch kv cache descriptor; max retries exhausted",
                        step="kv_metadata",
                        tags={
                            "error": type(exc).__name__,
                            "message": str(exc),
                            "target": self._config.engine_grpc_endpoint,
                            "attempts": attempt,
                            "max_retries": max_retries,
                        },
                    )
                    raise MetadataTemporarilyUnavailable(
                        f"kv cache descriptor unavailable after "
                        f"{max_retries} attempts: {exc}"
                    ) from exc
                logger.warning(
                    "failed to fetch kv cache descriptor; retrying",
                    step="kv_metadata",
                    tags={
                        "error": type(exc).__name__,
                        "message": str(exc),
                        "target": self._config.engine_grpc_endpoint,
                        "retry_in_s": delay,
                        "attempt": attempt,
                        "max_retries": max_retries,
                    },
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, _METADATA_RETRY_MAX_S)
                continue
            if logger.is_debug_enabled():
                logger.debug(
                    "fetched kv cache descriptor",
                    step="kv_metadata",
                    tags={
                        "group_count": len(metadata.groups),
                        "groups": [
                            {
                                "group_idx": spec.group_idx,
                                "kind": spec.kind,
                                "block_size": spec.block_size,
                                "sliding_window": spec.sliding_window,
                            }
                            for spec in metadata.groups
                        ],
                    },
                )
            return metadata
        raise AssertionError("unreachable")

    async def close(self) -> None:
        """Mark control as closed. The gRPC client is owned by the adapter."""

        if self._closed:
            return
        self._closed = True
