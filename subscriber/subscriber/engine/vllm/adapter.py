"""vLLM engine adapter — composes incremental ZMQ, gRPC control, and snapshot."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from subscriber.config import SubscriberConfig
from subscriber.engine.base import AbstractEngineAdapter, EngineEventBatch
from subscriber.engine.dashllm_grpc import DashllmGrpcClient
from subscriber.engine.metadata import KvCacheDescriptor
from subscriber.engine.vllm.control import VllmControl
from subscriber.engine.vllm.incremental import VllmIncrementalSource
from subscriber.engine.vllm.snapshot import VllmSnapshotSource
from subscriber.health.events import LivenessEvent

MEDIUM_VLLM_GPU = "GPU"
MEDIUM_VLLM_CPU = "CPU"

_KVCM_MEDIUM_MAP = {
    MEDIUM_VLLM_GPU: "hbm",
    MEDIUM_VLLM_CPU: "mem",
}


@AbstractEngineAdapter.register("vllm")
class VllmAdapter(AbstractEngineAdapter):
    """Public vLLM adapter composed of transport, control, and snapshot."""

    def __init__(self, config: SubscriberConfig) -> None:
        self._config = config
        self._grpc_client = DashllmGrpcClient(config.engine_grpc_endpoint)
        self._control = VllmControl(config, self._grpc_client)

        self._incremental = VllmIncrementalSource(config)
        self._snapshot = VllmSnapshotSource(config, self._grpc_client)

    async def subscribe_kv_events(self) -> AsyncGenerator[EngineEventBatch, None]:
        """Yield vLLM ZMQ batches, keeping replay-before-live ordering."""

        events = self._incremental.subscribe()
        try:
            async for event_batch in events:
                yield event_batch
        finally:
            await events.aclose()

    async def subscribe_snapshot_events(self) -> AsyncGenerator[EngineEventBatch, None]:
        """Yield periodic full snapshots for kvcm reconciliation."""

        events = self._snapshot.subscribe()
        try:
            async for event_batch in events:
                yield event_batch
        finally:
            await events.aclose()

    async def watch_liveness(self) -> AsyncGenerator[LivenessEvent, None]:
        """Yield gRPC-based health events from the control helper."""

        async for event in self._control.watch_liveness():
            yield event

    async def fetch_kv_cache_descriptor(self) -> KvCacheDescriptor:
        """Delegate engine metadata retrieval to the control helper."""

        return await self._control.fetch_kv_cache_descriptor()

    async def close(self) -> None:
        """Release control, snapshot, transport, and the gRPC client."""

        try:
            await self._control.close()
        finally:
            try:
                await self._snapshot.close()
            finally:
                try:
                    await self._incremental.close()
                finally:
                    await self._grpc_client.close()

    async def reset_generation_state(self) -> None:
        """Clear sequence state and snapshot generation after recovery."""

        await self._incremental.reset_generation_state()
        await self._snapshot.reset_generation_state()

    def request_immediate_snapshot(self) -> None:
        self._snapshot.request_immediate_snapshot()

    def map_medium(self, medium: str | None) -> str:
        if medium is None:
            return ""
        return _KVCM_MEDIUM_MAP.get(medium, "")

    def supported_mediums(self) -> list[str]:
        return list(_KVCM_MEDIUM_MAP.values())

    def storage_type(self) -> str:
        return self._config.kvcm_storage_type
