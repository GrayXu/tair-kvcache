from __future__ import annotations

from collections.abc import AsyncGenerator

from subscriber.config import SubscriberConfig
from subscriber.engine.base import AbstractEngineAdapter, EngineEventBatch
from subscriber.health.events import LivenessEvent
from subscriber.metrics import BatchTelemetry
from subscriber.trace import generate_trace_id


@AbstractEngineAdapter.register("sglang")
class SGLangAdapter(AbstractEngineAdapter):
    """Placeholder adapter for SGLang engine integration.

    Until SGLang exposes event and liveness sources, every subscription is an
    empty async generator and reset has no local state to clear.
    """

    def __init__(self, config: SubscriberConfig) -> None:
        self._config = config

    async def subscribe_kv_events(self) -> AsyncGenerator[EngineEventBatch, None]:
        """Yield no incremental batches until an SGLang event source exists."""

        if False:  # pragma: no cover - placeholder to satisfy async generator protocol
            yield EngineEventBatch(
                [], BatchTelemetry(pipeline="incremental"), generate_trace_id()
            )

    async def subscribe_snapshot_events(self) -> AsyncGenerator[EngineEventBatch, None]:
        """Yield no snapshots until SGLang exposes a snapshot source."""

        if False:
            yield EngineEventBatch(
                [], BatchTelemetry(pipeline="snapshot"), generate_trace_id()
            )

    async def watch_liveness(self) -> AsyncGenerator[LivenessEvent, None]:
        """Yield no liveness updates until SGLang exposes a liveness source."""

        if False:  # pragma: no cover - placeholder to satisfy async generator protocol
            yield

    def map_medium(self, medium: str | None) -> str:
        return ""

    def supported_mediums(self) -> list[str]:
        return []

    def storage_type(self) -> str:
        return self._config.kvcm_storage_type

    def request_immediate_snapshot(self) -> None:
        """Ignore snapshot signals until SGLang exposes a snapshot source."""

        return None

    async def reset_generation_state(self) -> None:
        """Perform no reset because this placeholder owns no transport state."""

    async def close(self) -> None:
        return await super().close()
