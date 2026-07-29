"""Periodic gRPC full KV cache block snapshot source."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from typing import cast

import grpc
import msgspec.msgpack

from subscriber import logger
from subscriber.config import SubscriberConfig
from subscriber.engine.base import EngineEventBatch
from subscriber.engine.dashllm_grpc import DashllmGrpcClient
from subscriber.metrics import BatchTelemetry
from subscriber.proto import engine_service_rpc_pb2
from subscriber.trace import generate_trace_id
from subscriber.types import (
    BlockSnapshot,
    BlockSnapshotItem,
    KVEventBatch,
)


class VllmSnapshotSource:
    """Periodic full KV cache block snapshot poller over DashLLM gRPC.

    Polls ``get_all_kv_cache_blocks`` on a fixed interval and yields one
    :class:`EngineEventBatch` per poll. The gRPC client lifecycle is owned by
    the composing adapter; :meth:`close` is a no-op. Generation tracking
    discards poll results that complete after :meth:`reset_generation_state`.

    Each poll is reported through the snapshot pipeline's
    :class:`~subscriber.metrics.MetricsReporter` — success and failure share
    the same output surface. Callers use the yielded
    :class:`EngineEventBatch`'s ``telemetry.drop_reason`` to distinguish
    reportable-but-drop poll outcomes (empty ``batches``) from successful
    snapshots (populated ``batches``).
    """

    def __init__(
        self, config: SubscriberConfig, grpc_client: DashllmGrpcClient
    ) -> None:
        self._config = config
        self._grpc_client = grpc_client
        self._generation = 0
        self._snapshot_requested = asyncio.Event()
        self._signal_warned = False

    def request_immediate_snapshot(self) -> None:
        """Signal the snapshot source to poll immediately.

        Synchronous, event-loop-only: sets an asyncio.Event that wakes the
        subscribe loop from its interval wait. Does not perform any I/O.
        Multiple calls before consumption coalesce into one extra poll.
        """

        if self._snapshot_requested.is_set():
            if not self._signal_warned:
                logger.warning(
                    "snapshot signal coalesced: previous signal not yet consumed",
                    step="grpc_snapshot",
                )
                self._signal_warned = True
        else:
            self._signal_warned = False
        if logger.is_debug_enabled():
            logger.debug(
                "snapshot signal received",
                step="grpc_snapshot",
            )
        self._snapshot_requested.set()

    async def _wait_interval(self, interval_s: float) -> None:
        """Wait for the configured interval or until signaled to poll early."""

        try:
            await asyncio.wait_for(self._snapshot_requested.wait(), timeout=interval_s)
        except TimeoutError:
            pass
        self._snapshot_requested.clear()
        self._signal_warned = False

    async def subscribe(self) -> AsyncGenerator[EngineEventBatch, None]:
        """Yield periodic full snapshots for kvcm reconciliation.

        Every poll — success, transport failure, or discarded-after-reset —
        yields one :class:`EngineEventBatch` carrying a per-poll
        :class:`BatchTelemetry`. The consumer submits the telemetry to the
        snapshot :class:`MetricsReporter`; empty ``batches`` combined with a
        set ``drop_reason`` signal that this poll should be reported but not
        enqueued for downstream forwarding.
        """

        interval_s = self._config.engine_snapshot_full_sync_interval_s
        while True:
            telemetry = BatchTelemetry(pipeline="snapshot")
            trace_id = generate_trace_id()
            generation = self._generation

            try:
                response = cast(
                    engine_service_rpc_pb2.KvCacheBlockListPB,
                    await self._grpc_client.get_all_kv_cache_blocks(
                        timeout_s=(
                            self._config.engine_kvcache_snapshot_timeout_ms / 1000
                        ),
                    ),
                )
            except grpc.aio.AioRpcError as exc:
                telemetry.mark("snapshot_fetch")
                telemetry.mark_dropped("fetch_failed")
                logger.warning(
                    "gRPC snapshot poll failed",
                    step="grpc_snapshot",
                    tags={
                        "code": exc.code().name if exc.code() else "UNKNOWN",
                        "details": exc.details() or "",
                        "trace_id": trace_id,
                    },
                )
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                await self._wait_interval(interval_s)
                continue
            except Exception as exc:
                telemetry.mark("snapshot_fetch")
                telemetry.mark_dropped("fetch_failed")
                logger.warning(
                    "gRPC snapshot poll unexpected error, will retry",
                    step="grpc_snapshot",
                    tags={
                        "error": exc.__class__.__name__,
                        "message": str(exc),
                        "trace_id": trace_id,
                    },
                )
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                await self._wait_interval(interval_s)
                continue

            fetch_ms = telemetry.mark("snapshot_fetch") * 1000
            payload_bytes = response.ByteSize()
            telemetry.gauge("full_snapshot_payload_bytes", payload_bytes)

            if generation != self._generation:
                telemetry.mark_dropped("generation_reset")
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                continue

            raw_snapshot = response.raw_snapshot
            snapshot_version = response.snapshot_version
            block_size = response.block_size

            if not raw_snapshot:
                telemetry.mark_dropped("empty_snapshot")
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                await self._wait_interval(interval_s)
                continue

            try:
                items: list[tuple[bytes | int, int, int, int]] = msgspec.msgpack.decode(
                    raw_snapshot,
                    type=list[tuple[bytes | int, int, int, int]],
                )
            except msgspec.ValidationError as exc:
                telemetry.mark_dropped("schema_mismatch")
                logger.warning(
                    "snapshot schema mismatch",
                    step="grpc_snapshot",
                    tags={
                        "error": exc.__class__.__name__,
                        "message": str(exc),
                        "trace_id": trace_id,
                    },
                )
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                await self._wait_interval(interval_s)
                continue
            except msgspec.DecodeError as exc:
                telemetry.mark_dropped("decode_failed")
                logger.warning(
                    "snapshot msgpack decode failed",
                    step="grpc_snapshot",
                    tags={
                        "error": exc.__class__.__name__,
                        "message": str(exc),
                        "trace_id": trace_id,
                    },
                )
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                await self._wait_interval(interval_s)
                continue

            telemetry.mark("decode")

            batch = KVEventBatch(
                ts=time.time(),
                events=[
                    BlockSnapshot(
                        medium="GPU",
                        block_size=block_size,
                        items=[
                            BlockSnapshotItem(
                                block_hash=item[0],
                                group_idx=item[1],
                            )
                            for item in items
                        ],
                        snapshot_version=snapshot_version,
                    )
                ],
            )
            telemetry.mark("snapshot_build")

            if generation != self._generation:
                telemetry.mark_dropped("generation_reset")
                yield EngineEventBatch(
                    batches=[], telemetry=telemetry, trace_id=trace_id
                )
                continue

            if logger.is_debug_enabled():
                logger.debug(
                    "snapshot poll completed",
                    step="grpc_snapshot",
                    tags={
                        "block_count": len(items),
                        "snapshot_version": snapshot_version,
                        "payload_bytes": payload_bytes,
                        "fetch_ms": round(fetch_ms, 2),
                        "trace_id": trace_id,
                    },
                )

            yield EngineEventBatch(
                batches=[batch], telemetry=telemetry, trace_id=trace_id
            )

            await self._wait_interval(interval_s)

    async def reset_generation_state(self) -> None:
        """Advance the generation so in-flight poll results are discarded."""

        self._generation += 1

    async def close(self) -> None:
        """No-op; the gRPC client lifecycle is owned by the adapter."""

        return None
