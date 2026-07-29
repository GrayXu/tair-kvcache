from __future__ import annotations

import pytest

from subscriber.config import SubscriberConfig
from subscriber.engine.base import AbstractEngineAdapter
from subscriber.engine.metadata import MetadataUnsupportedError
from subscriber.engine.sglang import SGLangAdapter
from subscriber.kvcm.enum import KvcmStorageType


def test_sglang_adapter_is_registered() -> None:
    assert "sglang" in AbstractEngineAdapter._registry
    assert AbstractEngineAdapter._registry["sglang"] is SGLangAdapter


def test_snapshot_subscription_is_a_required_adapter_contract() -> None:
    assert "subscribe_snapshot_events" in AbstractEngineAdapter.__abstractmethods__


def test_snapshot_signal_is_a_required_adapter_contract() -> None:
    assert "request_immediate_snapshot" in AbstractEngineAdapter.__abstractmethods__


def test_sglang_adapter_can_be_created_via_factory() -> None:
    config = SubscriberConfig()
    adapter = AbstractEngineAdapter.create("sglang", config)
    assert isinstance(adapter, SGLangAdapter)


def test_sglang_adapter_stub_methods_return_empty() -> None:
    config = SubscriberConfig()
    adapter = SGLangAdapter(config)

    assert adapter.map_medium("hbm") == ""
    assert adapter.supported_mediums() == []
    assert adapter.storage_type() == KvcmStorageType.ST_EVENT_REPORT_L1P5


def test_sglang_adapter_explicitly_implements_snapshot_signal_as_noop() -> None:
    adapter = SGLangAdapter(SubscriberConfig())

    assert "request_immediate_snapshot" in SGLangAdapter.__dict__
    assert adapter.request_immediate_snapshot() is None


async def test_sglang_adapter_subscribe_kv_events_is_empty_generator() -> None:
    config = SubscriberConfig()
    adapter = SGLangAdapter(config)

    events = []
    async for batch in adapter.subscribe_kv_events():
        events.append(batch)

    assert events == []


async def test_sglang_adapter_watch_liveness_is_empty_generator() -> None:
    config = SubscriberConfig()
    adapter = SGLangAdapter(config)

    events = []
    async for event in adapter.watch_liveness():
        events.append(event)

    assert events == []


async def test_sglang_adapter_reset_generation_state_is_noop() -> None:
    config = SubscriberConfig()
    adapter = SGLangAdapter(config)

    await adapter.reset_generation_state()


async def test_sglang_adapter_fetch_metadata_raises_unsupported() -> None:
    config = SubscriberConfig()
    adapter = SGLangAdapter(config)

    with pytest.raises(MetadataUnsupportedError):
        await adapter.fetch_kv_cache_descriptor()


async def test_sglang_adapter_close_is_noop() -> None:
    config = SubscriberConfig()
    adapter = SGLangAdapter(config)

    await adapter.close()
