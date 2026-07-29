from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from enum import Enum
from unittest.mock import AsyncMock, call

import pytest
from pytest_mock import MockerFixture

from subscriber.config import SubscriberConfig
from subscriber.engine.metadata import KvCacheDescriptor
from subscriber.kvcm.base import AbstractKvCacheManagerClient
from subscriber.kvcm.client import KvcmClient
from subscriber.kvcm.errors import KvcmReportRejectedError, KvcmUnavailableError
from subscriber.types import (
    AllBlocksCleared,
    BlockRemoved,
    BlockSnapshot,
    BlockSnapshotItem,
    BlockStored,
    KvCacheGroupSpec,
    KVEventBatch,
)


class FakeSdkClient(AbstractKvCacheManagerClient):
    def __init__(self, base_url: str = "") -> None:
        self.base_url = base_url
        self.started = False
        self.start = AsyncMock(side_effect=self._start)
        self.register_instance = AsyncMock(
            return_value={"header": {"status": {"code": "OK"}}}
        )
        self.report_event = AsyncMock(
            return_value={"header": {"status": {"code": "OK"}}}
        )
        self.close = AsyncMock()
        self.ready = True
        self.is_ready = AsyncMock(side_effect=self._is_ready)

    async def _start(self) -> None:
        self.started = True

    async def _is_ready(self) -> bool:
        return self.ready

    async def start(self) -> None:
        self.started = True

    async def is_ready(self) -> bool:
        return self.ready

    async def register_instance(
        self, data: dict[str, object], check_response: bool = True
    ) -> dict[str, object]:
        return {"header": {"status": {"code": "OK"}}}

    async def report_event(
        self, data: dict[str, object], check_response: bool = True
    ) -> dict[str, object]:
        return {"header": {"status": {"code": "OK"}}}

    async def close(self) -> None:
        pass


_VLLM_MEDIUM_MAP = {"GPU": "hbm", "CPU": "mem"}


@pytest.fixture(autouse=True)
def _mock_host_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    async def resolve_host_ip_port(port: int) -> str:
        return "10.0.0.8:9000"

    monkeypatch.setattr(
        "subscriber.kvcm.client.resolve_host_ip_port", resolve_host_ip_port
    )


@pytest.fixture(autouse=True)
def _default_deployment_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-a")


def _vllm_medium_mapper(medium: str | None) -> str:
    if medium is None:
        return ""
    return _VLLM_MEDIUM_MAP.get(medium, "")


def _client(config: SubscriberConfig | None = None) -> KvcmClient:
    client = KvcmClient(
        config or SubscriberConfig(kvcm_base_url="spectrum://vs-test:6382"),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        descriptor=KvCacheDescriptor(groups=()),
        manager_client=FakeSdkClient(),
    )
    client._host_ip_port_value = "10.0.0.8:9000"
    return client


def _make_kvcm(
    config: SubscriberConfig,
    fake_sdk: FakeSdkClient,
    *,
    on_snapshot_required: Callable[[], None] | None = None,
) -> KvcmClient:
    return KvcmClient(
        config,
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        descriptor=KvCacheDescriptor(groups=()),
        manager_client=fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )


def test_storage_type_maps_engine_type() -> None:
    assert (
        _client(SubscriberConfig(engine_type="vllm"))._storage_type == "ST_EVENT_REPORT"
    )
    assert (
        KvcmClient(
            SubscriberConfig(engine_type="unknown"),
            medium_mapper=_vllm_medium_mapper,
            storage_type="ST_UNSPECIFIED",
            supported_mediums=["hbm", "mem"],
            descriptor=KvCacheDescriptor(groups=()),
            manager_client=FakeSdkClient(),
        )._storage_type
        == "ST_UNSPECIFIED"
    )


def test_register_instance_request_uses_env_instance_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-a")
    request = _client()._register_instance_request()
    assert request["instance_id"] == "deploy-a_1"
    assert request["instance_group"] == ""
    assert request["block_size"] == 1
    assert request["location_spec_infos"] == [{"name": "vllm_1", "size": 1}]
    assert request["location_spec_groups"] == [
        {"name": "default", "spec_names": ["vllm_1"]}
    ]
    assert request["model_deployment"] == {
        "model_name": "default",
        "dtype": "",
        "use_mla": False,
        "tp_size": 1,
        "dp_size": 1,
        "lora_name": "",
        "pp_size": 1,
        "extra": "",
        "user_data": "",
        "use_eagle_pop": False,
    }


def test_register_instance_request_forwards_use_eagle_pop() -> None:
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        descriptor=KvCacheDescriptor(groups=(), use_eagle_pop=True),
        manager_client=FakeSdkClient(),
    )
    client._host_ip_port_value = "10.0.0.8:9000"

    request = client._register_instance_request()

    assert request["model_deployment"]["use_eagle_pop"] is True


def test_register_instance_request_appends_block_size_to_instance_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-a")
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 16}')
    request = _client()._register_instance_request()
    assert request["instance_id"] == "deploy-a_16"


def test_effective_block_size_uses_min_of_heterogeneous_group_metadata(mocker) -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=2192),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=16),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    assert client._effective_block_size() == 16
    warning.assert_called_once_with(
        "group metadata contains heterogeneous block_sizes; using min",
        step="kvcm_register",
        tags={"block_sizes": {0: 2192, 1: 16}, "effective_block_size": 16},
    )


def test_effective_block_size_uses_uniform_group_metadata() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=2192),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=2192),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    assert client._effective_block_size() == 2192


def test_effective_block_size_falls_back_to_env_without_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 64}')
    assert _client()._effective_block_size() == 64


def test_register_instance_request_prefers_group_metadata_block_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-a")
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 16}')
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=2192),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=2192),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"
    request = client._register_instance_request()
    assert request["block_size"] == 2192
    assert request["instance_id"] == "deploy-a_2192"


def test_fallback_spec_name_uses_effective_block_size_from_metadata() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=2192),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"
    specs = client._block_specs("hbm", group_idx=None)
    assert specs == [{"name": "vllm_2192", "uri": "vllm://10.0.0.8:9000/hbm"}]


def test_block_specs_use_registered_location_spec_name() -> None:
    client = _client()
    register_request = client._register_instance_request()
    block_events = client._report_events_for_batches(
        [
            KVEventBatch(
                ts=1.0,
                events=[
                    BlockStored(
                        block_hashes=[11],
                        parent_block_hash=None,
                        token_ids=[1, 2],
                        block_size=2,
                        lora_id=None,
                        medium="GPU",
                        lora_name=None,
                    )
                ],
            )
        ]
    )

    registered_spec_names = {
        spec["name"] for spec in register_request["location_spec_infos"]
    }
    block_specs = block_events[0]["block_add"]["specs"]

    assert block_specs == [{"name": "vllm_1", "uri": "vllm://10.0.0.8:9000/hbm"}]
    assert {spec["name"] for spec in block_specs} <= registered_spec_names


def test_register_and_block_specs_use_configured_block_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 16}')
    client = _client()

    register_request = client._register_instance_request()
    block_events = client._report_events_for_batches(
        [
            KVEventBatch(
                ts=1.0,
                events=[
                    BlockStored(
                        block_hashes=[11],
                        parent_block_hash=None,
                        token_ids=[1, 2],
                        block_size=16,
                        lora_id=None,
                        medium="GPU",
                        lora_name=None,
                    )
                ],
            )
        ]
    )

    assert register_request["block_size"] == 16
    assert register_request["location_spec_infos"] == [{"name": "vllm_16", "size": 16}]
    assert register_request["location_spec_groups"] == [
        {"name": "default", "spec_names": ["vllm_16"]}
    ]
    assert block_events[0]["block_add"]["specs"] == [
        {"name": "vllm_16", "uri": "vllm://10.0.0.8:9000/hbm"}
    ]


@pytest.mark.parametrize(
    ("raw_config", "expected_block_size"),
    [
        ('{"block_size": 16}', 16),
        ("{invalid-json", 1),
        ("[]", 1),
        ("1", 1),
        ('{"block_size": true}', 1),
        ('{"block_size": 0}', 1),
    ],
)
def test_block_size_falls_back_for_invalid_engine_config_shapes(
    monkeypatch: pytest.MonkeyPatch, raw_config: str, expected_block_size: int
) -> None:
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", raw_config)
    assert _client()._register_instance_request()["block_size"] == expected_block_size


@pytest.mark.parametrize("env_value", [None, "", "   "])
async def test_start_rejects_missing_or_blank_deployment_name(
    monkeypatch: pytest.MonkeyPatch, env_value: str | None
) -> None:
    """A blank SPECTRUM_DEPLOYMENT_NAME would produce a degenerate
    instance_id like "_16" shared by unrelated engine instances, breaking
    cross-instance KVCache isolation. Startup must fail instead."""

    if env_value is None:
        monkeypatch.delenv("SPECTRUM_DEPLOYMENT_NAME", raising=False)
    else:
        monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", env_value)
    client = _client()
    with pytest.raises(ValueError, match="SPECTRUM_DEPLOYMENT_NAME"):
        await client.start()


def test_empty_group_metadata_registers_with_default_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty metadata tuple is a valid topology and must register the
    same default location spec as the no-metadata path; KVCM rejects empty
    location_spec_infos."""

    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-a")
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 16}')
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=()),
    )
    request = client._register_instance_request()
    assert request["location_spec_infos"] == [{"name": "vllm_16", "size": 16}]
    assert request["location_spec_groups"] == [
        {"name": "default", "spec_names": ["vllm_16"]}
    ]


def test_register_instance_request_reads_parallelism_from_engine_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DS_LLM_ENGINE_CONFIG",
        '{"block_size": 64, "dtype": "bfloat16", '
        '"tensor_parallel_size": 2, "data_parallel_size": 4, '
        '"pipeline_parallel_size": 3}',
    )
    deployment = _client()._register_instance_request()["model_deployment"]
    assert deployment["tp_size"] == 2
    assert deployment["dp_size"] == 4
    assert deployment["pp_size"] == 3


def test_register_instance_request_falls_back_parallelism_for_invalid_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DS_LLM_ENGINE_CONFIG",
        '{"tensor_parallel_size": true, "data_parallel_size": -1}',
    )
    deployment = _client()._register_instance_request()["model_deployment"]
    assert deployment["tp_size"] == 1
    assert deployment["dp_size"] == 1
    assert deployment["pp_size"] == 1


def test_report_event_request_contains_common_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-a")
    client = _client(SubscriberConfig(engine_type="vllm"))
    events = [{"event_type": "EVENT_HOST_DOWN", "host_down": {}}]

    request = client._report_event_request(events)

    assert request["instance_id"] == "deploy-a_1"
    assert request["host_ip_port"] == "10.0.0.8:9000"
    assert request["storage_type"] == "ST_EVENT_REPORT"
    assert request["events"] == events


def test_report_events_for_batches_maps_all_supported_event_types() -> None:
    batch = KVEventBatch(
        ts=1.0,
        events=[
            BlockStored(
                block_hashes=[11, 12],
                parent_block_hash=None,
                token_ids=[1, 2],
                block_size=2,
                lora_id=None,
                medium="GPU",
                lora_name=None,
            ),
            BlockRemoved(block_hashes=[13], medium="CPU"),
            AllBlocksCleared(),
        ],
    )

    events = _client()._report_events_for_batches([batch])

    assert events == [
        {
            "event_type": "EVENT_BLOCK_ADD",
            "block_add": {
                "block_key": "11",
                "medium": "hbm",
                "specs": [{"name": "vllm_1", "uri": "vllm://10.0.0.8:9000/hbm"}],
            },
        },
        {
            "event_type": "EVENT_BLOCK_ADD",
            "block_add": {
                "block_key": "12",
                "medium": "hbm",
                "specs": [{"name": "vllm_1", "uri": "vllm://10.0.0.8:9000/hbm"}],
            },
        },
        {
            "event_type": "EVENT_BLOCK_DELETE",
            "block_delete": {
                "block_key": "13",
                "medium": "mem",
                "spec_names": ["vllm_1"],
            },
        },
        {"event_type": "EVENT_HOST_DOWN", "host_down": {}},
    ]


def test_report_events_for_batches_maps_unknown_medium_to_empty_string() -> None:
    batch = KVEventBatch(
        ts=1.0,
        events=[BlockRemoved(block_hashes=[13], medium="DISK")],
    )
    assert _client()._report_events_for_batches([batch]) == [
        {
            "event_type": "EVENT_BLOCK_DELETE",
            "block_delete": {
                "block_key": "13",
                "medium": "",
                "spec_names": ["vllm_1"],
            },
        }
    ]


def test_register_and_report_use_per_group_specs() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=16),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=16, sliding_window=4096),
        KvCacheGroupSpec(group_idx=2, kind="sliding_window", block_size=16),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"

    request = client._register_instance_request()
    assert request["location_spec_infos"] == [
        {"name": "full_0", "size": 16},
        {"name": "linear_1", "size": 16},
        {"name": "sliding_window_2", "size": 16},
    ]
    assert request["location_spec_groups"] == [
        {"name": "full_0", "spec_names": ["full_0"]},
        {"name": "linear_1", "spec_names": ["linear_1"]},
        {"name": "sliding_window_2", "spec_names": ["sliding_window_2"]},
    ]

    batch = KVEventBatch(
        ts=1.0,
        events=[
            BlockStored(
                block_hashes=[1],
                parent_block_hash=None,
                token_ids=[0],
                block_size=1,
                lora_id=None,
                medium="GPU",
                lora_name=None,
                group_idx=0,
                kv_cache_spec_kind="full_attention",
            ),
            BlockRemoved(block_hashes=[2], medium="GPU", group_idx=1),
        ],
    )
    events = client._report_events_for_batches([batch])
    assert events[0]["block_add"]["specs"] == [
        {"name": "full_0", "uri": "vllm://10.0.0.8:9000/hbm"}
    ]
    assert events[1]["block_delete"]["spec_names"] == ["linear_1"]


def test_kvcm_event_types_are_string_enum_values() -> None:
    event_type = _client()._node_register_event()["event_type"]

    assert isinstance(event_type, Enum)
    assert event_type == "EVENT_NODE_REGISTER"


async def test_start_creates_sdk_registers_instance_reports_node_and_starts_heartbeat():
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    await client.close()

    fake_sdk.start.assert_awaited_once()
    fake_sdk.register_instance.assert_called_once()
    register_request = fake_sdk.register_instance.call_args.args[0]
    assert register_request["block_size"] == 1

    fake_sdk.report_event.assert_called_once()
    node_request = fake_sdk.report_event.call_args.args[0]
    assert node_request["events"] == [
        {
            "event_type": "EVENT_NODE_REGISTER",
            "node_register": {"mediums": ["hbm", "mem"]},
        }
    ]
    fake_sdk.close.assert_awaited_once()


async def test_start_uses_injected_manager_client() -> None:
    fake_sdk = FakeSdkClient()
    client = KvcmClient(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        descriptor=KvCacheDescriptor(groups=()),
        manager_client=fake_sdk,
    )

    await client.start()
    await client.close()

    fake_sdk.start.assert_awaited_once()
    fake_sdk.is_ready.assert_awaited_once()
    fake_sdk.register_instance.assert_awaited_once()
    fake_sdk.close.assert_awaited_once()


@pytest.mark.parametrize(
    ("base_url_env", "expected_url"),
    [
        ("spectrum://vs-a:6382", "spectrum://vs-a:6382"),
        ("http://10.0.0.1:8080", "http://10.0.0.1:8080"),
        (
            "static://10.0.0.1:8080,10.0.0.2:8080",
            "static://10.0.0.1:8080,10.0.0.2:8080",
        ),
    ],
    ids=["spectrum", "http", "static"],
)
async def test_start_passes_kvcm_base_url_to_manager_client(
    monkeypatch: pytest.MonkeyPatch, base_url_env: str, expected_url: str
) -> None:
    created_clients: list[FakeSdkClient] = []

    def create_http_client(
        base_url: str, *, request_timeout_seconds: float
    ) -> FakeSdkClient:
        client = FakeSdkClient(base_url)
        created_clients.append(client)
        return client

    monkeypatch.setattr(
        "subscriber.kvcm.client.HttpKvCacheManagerClient", create_http_client
    )

    client = KvcmClient(
        SubscriberConfig(kvcm_base_url=base_url_env, kvcm_heartbeat_interval_s=60.0),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        descriptor=KvCacheDescriptor(groups=()),
    )

    await client.start()
    await client.close()

    assert [created.base_url for created in created_clients] == [expected_url]


def test_base_url_raises_when_kvcm_base_url_missing() -> None:
    with pytest.raises(ValueError, match="kvcm_base_url"):
        KvcmClient(
            SubscriberConfig(),
            medium_mapper=_vllm_medium_mapper,
            storage_type="ST_EVENT_REPORT",
            supported_mediums=["hbm", "mem"],
            descriptor=KvCacheDescriptor(groups=()),
        )


async def test_start_resolves_host_ip_port_once_and_reuses_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sdk = FakeSdkClient()
    config = SubscriberConfig(kvcm_heartbeat_interval_s=60.0)
    resolver_calls: list[None] = []

    async def resolve_host_ip_port(port: int) -> str:
        resolver_calls.append(None)
        return "10.0.0.7:8123"

    monkeypatch.setattr(
        "subscriber.kvcm.client.resolve_host_ip_port",
        resolve_host_ip_port,
    )
    client = _make_kvcm(config, fake_sdk)

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_kv_events(
        [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=7
    )
    await client._report_events([client._heartbeat_event()])
    await client.close()

    assert resolver_calls == [None]
    assert [
        call.args[0]["host_ip_port"] for call in fake_sdk.report_event.call_args_list
    ] == ["10.0.0.7:8123", "10.0.0.7:8123"]


async def test_start_survives_register_failure_and_starts_heartbeat(mocker) -> None:
    fake_sdk = FakeSdkClient()
    register_error = RuntimeError("register failed")
    fake_sdk.register_instance.side_effect = register_error
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

    await client.start()

    assert not client.is_registered
    warning.assert_any_call(
        "kvcm register_instance failed (%s: %s)",
        "RuntimeError",
        register_error,
        step="kvcm_register",
        tags={"phase": "register_instance"},
        exc_info=register_error,
    )
    assert client._heartbeat_task is not None

    await client.close()


async def test_start_resets_registered_when_node_register_report_fails(
    mocker,
) -> None:
    """Regression: if register_instance succeeds but the NODE_REGISTER report
    fails, _registered must stay False so the heartbeat loop retries."""

    fake_sdk = FakeSdkClient()
    report_error = RuntimeError("node register report failed")

    async def report_event(
        request: dict[str, object], **_: object
    ) -> dict[str, object]:
        events = request.get("events")
        if (
            isinstance(events, list)
            and events
            and events[0].get("event_type") == "EVENT_NODE_REGISTER"
        ):
            raise report_error
        return {"header": {"status": {"code": "OK"}}}

    fake_sdk.report_event.side_effect = report_event
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

    await client.start()

    assert not client.is_registered
    warning.assert_any_call(
        "kvcm node_register report failed (%s: %s)",
        "RuntimeError",
        report_error,
        step="kvcm_register",
        tags={"phase": "node_register"},
        exc_info=report_error,
    )
    assert client._heartbeat_task is not None

    await client.close()


async def test_start_without_endpoint_drops_batches_then_registers_after_recovery(
    mocker,
) -> None:
    fake_sdk = FakeSdkClient()
    fake_sdk.ready = False
    registered = asyncio.Event()

    async def register_instance(_request: dict[str, object]) -> dict[str, object]:
        registered.set()
        return {"header": {"status": {"code": "OK"}}}

    fake_sdk.register_instance.side_effect = register_instance
    warning = mocker.patch("subscriber.main.logger.warning")
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

    await client.start()

    fake_sdk.register_instance.assert_not_awaited()
    fake_sdk.report_event.assert_not_awaited()

    from subscriber.main import send_incremental_events

    queue = asyncio.Queue()
    batches = [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])]
    await queue.put(mocker.Mock(batches=batches, epoch_snapshot=1))
    coordinator = mocker.Mock()
    coordinator.wait_ready_epoch = AsyncMock(return_value=1)
    coordinator.is_epoch_current.return_value = True
    sender = asyncio.create_task(
        send_incremental_events(
            client,
            coordinator,
            queue,
            max_merged_queue_items=1,
            max_merged_report_events=1,
        )
    )
    await queue.join()

    warning.assert_any_call(
        "kvcm has no available endpoint; starting in not-ready state",
        step="kvcm_register",
    )
    send_warning = next(
        call
        for call in warning.call_args_list
        if call.args == ("failed to send kv event batch to kvcm; dropping batch",)
    )
    assert "kvcm client is not ready" in send_warning.kwargs["tags"]["message"]

    fake_sdk.ready = True
    await asyncio.wait_for(registered.wait(), timeout=0.2)

    fake_sdk.report_event.reset_mock()
    await client.report_kv_events(batches, epoch=1)

    fake_sdk.report_event.assert_awaited_once()
    sender.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sender
    await client.close()


async def test_report_events_before_start_raises_clear_error() -> None:
    client = _client()

    with pytest.raises(RuntimeError, match="kvcm client has not been started"):
        await client._report_events([client._heartbeat_event()])


async def test_report_kv_events_before_start_raises_clear_error() -> None:
    client = _client()

    with pytest.raises(RuntimeError, match="kvcm client has not been started"):
        await client.report_kv_events(
            [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
        )


async def test_report_kv_events_raises_when_started_but_not_registered() -> None:
    fake_sdk = FakeSdkClient()
    fake_sdk.ready = False
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

    await client.start()
    assert not client.is_registered

    with pytest.raises(RuntimeError, match="kvcm client is not ready"):
        await client.report_kv_events(
            [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
        )

    fake_sdk.register_instance.assert_not_awaited()
    await client.close()


async def test_heartbeat_reports_periodically() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
    )

    await client.start()
    await asyncio.sleep(0.03)
    await client.close()

    heartbeat_calls = [
        call.args[0]
        for call in fake_sdk.report_event.call_args_list
        if call.args[0]["events"]
        == [{"event_type": "EVENT_HEARTBEAT", "heartbeat": {"system_status": {}}}]
    ]
    assert heartbeat_calls
    fake_sdk.close.assert_awaited_once()


@pytest.mark.parametrize(
    "reset_response",
    [{"snapshot_required": False}, {}],
)
async def test_heartbeat_snapshot_required_deduplicates_until_reset(
    reset_response: dict[str, object],
) -> None:
    fake_sdk = FakeSdkClient()
    heartbeat_responses = [
        {"snapshot_required": True},
        {"snapshot_required": True},
        reset_response,
        {"snapshot_required": True},
    ]
    heartbeat_count = 0

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal heartbeat_count
        if request["events"][0]["event_type"] == "EVENT_HEARTBEAT":
            # Repeat the last response for stray heartbeats before close().
            response = heartbeat_responses[min(heartbeat_count, 3)]
            heartbeat_count += 1
            return response
        return {"header": {"status": {"code": "OK"}}}

    callback_heartbeats: list[int] = []
    second_snapshot_requested = asyncio.Event()

    def on_snapshot_required() -> None:
        callback_heartbeats.append(heartbeat_count)
        if len(callback_heartbeats) == 2:
            second_snapshot_requested.set()

    fake_sdk.report_event.side_effect = report_event
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )

    await client.start()
    await asyncio.wait_for(second_snapshot_requested.wait(), timeout=0.2)
    await client.close()

    # Fired on the first heartbeat of each snapshot_required=True streak:
    # heartbeat 2 is deduplicated, heartbeat 3 resets, heartbeat 4 re-fires.
    assert callback_heartbeats == [1, 4]


async def test_heartbeat_snapshot_callback_failure_retries(mocker) -> None:
    fake_sdk = FakeSdkClient()
    callback_error = RuntimeError("snapshot callback failed")
    callback_count = 0
    callback_retried = asyncio.Event()

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        if request["events"][0]["event_type"] == "EVENT_HEARTBEAT":
            return {"snapshot_required": True}
        return {"header": {"status": {"code": "OK"}}}

    def on_snapshot_required() -> None:
        nonlocal callback_count
        callback_count += 1
        if callback_count == 1:
            raise callback_error
        callback_retried.set()

    fake_sdk.report_event.side_effect = report_event
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )

    await client.start()
    await asyncio.wait_for(callback_retried.wait(), timeout=0.2)
    assert client.is_registered
    await client.close()

    assert callback_count == 2
    warning.assert_any_call(
        "failed to request immediate snapshot from kvcm heartbeat; "
        "continuing heartbeat",
        step="snapshot_signal",
        tags={
            "error": "RuntimeError",
            "message": str(callback_error),
            "failure_count": 1,
        },
        exc_info=callback_error,
    )


async def test_heartbeat_snapshot_persistent_failure_rate_limits_warnings(
    mocker,
) -> None:
    fake_sdk = FakeSdkClient()
    failure_count = 0
    three_failures = asyncio.Event()

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        if request["events"][0]["event_type"] == "EVENT_HEARTBEAT":
            return {"snapshot_required": True}
        return {"header": {"status": {"code": "OK"}}}

    def on_snapshot_required() -> None:
        nonlocal failure_count
        failure_count += 1
        if failure_count == 3:
            three_failures.set()
        raise RuntimeError("snapshot callback failed")

    fake_sdk.report_event.side_effect = report_event
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )

    await client.start()
    await asyncio.wait_for(three_failures.wait(), timeout=0.3)
    await client.close()

    assert failure_count >= 3
    snapshot_warns = [
        call
        for call in warning.call_args_list
        if call.kwargs.get("step") == "snapshot_signal"
    ]
    assert len(snapshot_warns) == 1
    assert snapshot_warns[0].kwargs["tags"]["failure_count"] == 1


async def test_heartbeat_snapshot_failure_count_resets_per_streak(mocker) -> None:
    fake_sdk = FakeSdkClient()
    heartbeat_count = 0
    failure_count = 0
    second_streak_failed = asyncio.Event()
    # True -> failure streak 1; False -> streak reset; True -> streak 2.
    heartbeat_responses = [
        {"snapshot_required": True},
        {"snapshot_required": False},
        {"snapshot_required": True},
    ]

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal heartbeat_count
        if request["events"][0]["event_type"] != "EVENT_HEARTBEAT":
            return {"header": {"status": {"code": "OK"}}}
        # Repeat the last response for stray heartbeats before close().
        response = heartbeat_responses[min(heartbeat_count, 2)]
        heartbeat_count += 1
        return response

    def on_snapshot_required() -> None:
        nonlocal failure_count
        failure_count += 1
        if failure_count == 2:
            second_streak_failed.set()
        raise RuntimeError("snapshot callback failed")

    fake_sdk.report_event.side_effect = report_event
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )

    await client.start()
    await asyncio.wait_for(second_streak_failed.wait(), timeout=0.3)
    await client.close()

    snapshot_warns = [
        call
        for call in warning.call_args_list
        if call.kwargs.get("step") == "snapshot_signal"
    ]
    assert len(snapshot_warns) == 2
    assert [call.kwargs["tags"]["failure_count"] for call in snapshot_warns] == [1, 1]


async def test_heartbeat_snapshot_dedup_resets_after_reregistration() -> None:
    fake_sdk = FakeSdkClient()
    heartbeat_count = 0
    callback_count = 0
    snapshot_requested_after_recovery = asyncio.Event()

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal heartbeat_count
        if request["events"][0]["event_type"] != "EVENT_HEARTBEAT":
            return {"header": {"status": {"code": "OK"}}}
        heartbeat_count += 1
        if heartbeat_count == 2:
            raise RuntimeError("kvcm unavailable")
        return {"snapshot_required": True}

    def on_snapshot_required() -> None:
        nonlocal callback_count
        callback_count += 1
        if callback_count == 2:
            snapshot_requested_after_recovery.set()

    fake_sdk.report_event.side_effect = report_event
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )

    await client.start()
    await asyncio.wait_for(snapshot_requested_after_recovery.wait(), timeout=0.3)
    assert client.is_registered
    await client.close()

    assert callback_count == 2
    assert fake_sdk.register_instance.await_count >= 2


@pytest.mark.parametrize("value", [1, "true"])
async def test_heartbeat_snapshot_required_requires_strict_bool(
    value: object,
) -> None:
    fake_sdk = FakeSdkClient()
    heartbeat_count = 0
    two_heartbeats_done = asyncio.Event()
    callback_count = 0

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal heartbeat_count
        if request["events"][0]["event_type"] == "EVENT_HEARTBEAT":
            heartbeat_count += 1
            if heartbeat_count >= 2:
                two_heartbeats_done.set()
            return {"snapshot_required": value}
        return {"header": {"status": {"code": "OK"}}}

    def on_snapshot_required() -> None:
        nonlocal callback_count
        callback_count += 1

    fake_sdk.report_event.side_effect = report_event
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
        on_snapshot_required=on_snapshot_required,
    )

    await client.start()
    await asyncio.wait_for(two_heartbeats_done.wait(), timeout=0.2)
    assert client.is_registered
    await client.close()

    assert callback_count == 0


async def test_heartbeat_snapshot_required_without_callback_is_ignored() -> None:
    fake_sdk = FakeSdkClient()
    heartbeat_count = 0
    two_heartbeats_done = asyncio.Event()

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal heartbeat_count
        if request["events"][0]["event_type"] == "EVENT_HEARTBEAT":
            heartbeat_count += 1
            if heartbeat_count >= 2:
                two_heartbeats_done.set()
            return {"snapshot_required": True}
        return {"header": {"status": {"code": "OK"}}}

    fake_sdk.report_event.side_effect = report_event
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

    await client.start()
    await asyncio.wait_for(two_heartbeats_done.wait(), timeout=0.2)
    assert client.is_registered
    await client.close()


async def test_heartbeat_failure_logs_warning_and_continues(mocker) -> None:
    fake_sdk = FakeSdkClient()
    heartbeat_error = RuntimeError("heartbeat failed")

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        events = request["events"]
        if (
            isinstance(events, list)
            and events
            and events[0].get("event_type") == "EVENT_HEARTBEAT"
        ):
            raise heartbeat_error
        return {"header": {"status": {"code": "OK"}}}

    fake_sdk.report_event.side_effect = report_event
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=0.01),
        fake_sdk,
    )

    await client.start()
    await asyncio.sleep(0.03)
    await client.close()

    warning.assert_any_call(
        "kvcm heartbeat report failed (%s: %s)",
        "RuntimeError",
        heartbeat_error,
        step="kvcm_heartbeat",
        exc_info=heartbeat_error,
    )


async def test_runtime_kvcm_failure_retries_registration_until_recovered() -> None:
    fake_sdk = FakeSdkClient()
    recovered = asyncio.Event()
    heartbeat_failed = False

    async def register_instance(_request: dict[str, object]) -> dict[str, object]:
        if fake_sdk.register_instance.await_count < 4:
            if fake_sdk.register_instance.await_count > 1:
                raise RuntimeError("kvcm registration unavailable")
        else:
            recovered.set()
        return {"header": {"status": {"code": "OK"}}}

    async def report_event(
        request: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal heartbeat_failed
        event_type = request["events"][0]["event_type"]
        if event_type == "EVENT_HEARTBEAT" and not heartbeat_failed:
            heartbeat_failed = True
            raise RuntimeError("kvcm unavailable")
        return {"header": {"status": {"code": "OK"}}}

    fake_sdk.register_instance.side_effect = register_instance
    fake_sdk.report_event.side_effect = report_event
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

    await client.start()
    await asyncio.wait_for(recovered.wait(), timeout=0.3)
    assert fake_sdk.register_instance.await_count == 4
    assert client.is_registered
    await client.close()


async def test_report_kv_events_reports_events_without_blocking_sdk_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    main_thread = threading.current_thread()
    report_threads: list[threading.Thread] = []

    async def track_report(
        request: dict[str, object], *, check_response: bool = True
    ) -> dict[str, object]:
        report_threads.append(threading.current_thread())
        return {"header": {"status": {"code": "OK"}}}

    monkeypatch.setattr(fake_sdk, "report_event", track_report)

    await client.start()
    await client.report_kv_events(
        [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=7
    )
    await client.close()

    assert report_threads
    assert report_threads == [main_thread, main_thread]


async def test_report_kv_events_reports_converted_events() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    batch = KVEventBatch(
        ts=1.0,
        events=[BlockRemoved(block_hashes=[13], medium="CPU")],
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_kv_events([batch], epoch=7, trace_id="incremental-trace")
    await client.close()

    fake_sdk.report_event.assert_called_once()
    request = fake_sdk.report_event.call_args.args[0]
    assert request["trace_id"] == "incremental-trace"
    assert request["events"] == [
        {
            "event_type": "EVENT_BLOCK_DELETE",
            "block_delete": {
                "block_key": "13",
                "medium": "mem",
                "spec_names": ["vllm_1"],
            },
        }
    ]


async def test_report_kv_events_logs_expansion_and_http_diagnostics(
    mocker,
) -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    batch = KVEventBatch(
        ts=1.0,
        events=[BlockRemoved(block_hashes=[13], medium="CPU")],
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    debug = mocker.patch("subscriber.kvcm.client.logger.debug")
    mocker.patch("subscriber.kvcm.client.logger.is_debug_enabled", return_value=True)

    await client.report_kv_events([batch], epoch=7, trace_id="incremental-trace")
    await client.close()

    debug.assert_called_once()
    args = debug.call_args
    assert args.args == ("kvcm report_event timing",)
    assert args.kwargs["step"] == "kvcm_send"
    tags = args.kwargs["tags"]
    assert tags["source_batch_count"] == 1
    assert tags["trace_id"] == "incremental-trace"
    assert tags["source_event_count"] == 1
    assert tags["report_event_count"] == 1
    assert tags["event_expand_ms"] >= 0
    assert tags["http_round_trip_ms"] >= 0
    assert tags["kvcm_send_total_ms"] >= 0


async def test_send_empty_batch_does_not_report_event() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_kv_events([], epoch=7)
    await client.close()

    fake_sdk.report_event.assert_not_called()


async def test_report_kv_events_no_longer_rejects_snapshot_batch() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    batch = KVEventBatch(
        ts=1.0,
        events=[
            BlockSnapshot(
                medium="GPU",
                block_size=16,
                items=[BlockSnapshotItem(block_hash=101, group_idx=0)],
                snapshot_version=1,
            )
        ],
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_kv_events([batch], epoch=7)
    await client.close()

    fake_sdk.report_event.assert_not_called()


def _snapshot_batch() -> KVEventBatch:
    return KVEventBatch(
        ts=1.0,
        events=[
            BlockSnapshot(
                medium="GPU",
                block_size=16,
                items=[
                    BlockSnapshotItem(block_hash=101, group_idx=0),
                    BlockSnapshotItem(block_hash=102, group_idx=0),
                ],
                snapshot_version=3,
            )
        ],
    )


def _empty_snapshot_batch() -> KVEventBatch:
    return KVEventBatch(
        ts=1.0,
        events=[
            BlockSnapshot(
                medium="GPU",
                block_size=16,
                items=[],
                snapshot_version=4,
            )
        ],
    )


async def test_report_snapshot_before_start_raises_clear_error() -> None:
    client = _client()

    with pytest.raises(RuntimeError, match="kvcm client has not been started"):
        await client.report_snapshot([_snapshot_batch()], epoch=1)


async def test_report_snapshot_raises_when_started_but_not_registered() -> None:
    fake_sdk = FakeSdkClient()
    fake_sdk.ready = False
    client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

    await client.start()
    assert not client.is_registered

    with pytest.raises(KvcmUnavailableError, match="kvcm client is not ready"):
        await client.report_snapshot([_snapshot_batch()], epoch=1)

    fake_sdk.report_event.assert_not_awaited()
    await client.close()


async def test_report_snapshot_reports_snapshot_event() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_snapshot(
        [_snapshot_batch()], epoch=7, trace_id="snapshot-trace"
    )
    await client.close()

    fake_sdk.report_event.assert_awaited_once()
    request = fake_sdk.report_event.call_args.args[0]
    assert request["trace_id"] == "snapshot-trace"
    assert request["instance_id"] == client._instance_id()
    assert request["host_ip_port"] == "10.0.0.8:9000"
    assert request["storage_type"] == "ST_EVENT_REPORT"
    assert request["events"] == [
        {
            "event_type": "EVENT_BLOCK_SNAPSHOT",
            "block_snapshot": {
                "blocks": [
                    {
                        "block_key": "101",
                        "medium": "hbm",
                        "specs": [
                            {
                                "name": "vllm_1",
                                "uri": "vllm://10.0.0.8:9000/hbm",
                            }
                        ],
                    },
                    {
                        "block_key": "102",
                        "medium": "hbm",
                        "specs": [
                            {
                                "name": "vllm_1",
                                "uri": "vllm://10.0.0.8:9000/hbm",
                            }
                        ],
                    },
                ]
            },
        }
    ]
    assert fake_sdk.report_event.call_args.kwargs == {"check_response": True}


async def test_report_snapshot_forwards_empty_snapshot_to_clear_stale_blocks() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_snapshot([_empty_snapshot_batch()], epoch=7)
    await client.close()

    fake_sdk.report_event.assert_awaited_once()
    request = fake_sdk.report_event.call_args.args[0]
    assert request["events"] == [
        {
            "event_type": "EVENT_BLOCK_SNAPSHOT",
            "block_snapshot": {"blocks": []},
        }
    ]
    assert fake_sdk.report_event.call_args.kwargs == {"check_response": True}


async def test_report_snapshot_without_snapshots_does_not_report_event() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    batch = KVEventBatch(
        ts=1.0,
        events=[BlockRemoved(block_hashes=[13], medium="CPU")],
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    await client.report_snapshot([batch], epoch=7)
    await client.close()

    fake_sdk.report_event.assert_not_called()


async def test_report_snapshot_classifies_rejected_report_error() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    fake_sdk.report_event.side_effect = RuntimeError(
        "KVCM /api/reportEvent failed: INTERNAL_ERROR"
    )

    with pytest.raises(KvcmReportRejectedError, match="INTERNAL_ERROR"):
        await client.report_snapshot(
            [_snapshot_batch()], epoch=7, trace_id="snapshot-trace"
        )
    await client.close()


async def test_report_snapshot_classifies_unavailable_error() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    fake_sdk.report_event.side_effect = RuntimeError("connection refused")

    with pytest.raises(KvcmUnavailableError, match="connection refused"):
        await client.report_snapshot([_snapshot_batch()], epoch=7)
    await client.close()


async def test_report_snapshot_logs_warning_for_item_results(mocker) -> None:
    fake_sdk = FakeSdkClient()
    fake_sdk.report_event.return_value = {
        "header": {"status": {"code": "OK"}},
        "item_results": ["OK", "INTERNAL_ERROR"],
    }
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()

    with pytest.raises(KvcmReportRejectedError, match="item_results"):
        await client.report_snapshot(
            [_snapshot_batch()], epoch=7, trace_id="snapshot-trace"
        )
    await client.close()

    warning.assert_called_once_with(
        "kvcm report_event returned partial item results",
        step="kvcm_send",
        tags={
            "epoch": 7,
            "trace_id": "snapshot-trace",
            "item_results": ["OK", "INTERNAL_ERROR"],
        },
    )


async def test_report_snapshot_logs_expansion_and_http_diagnostics(mocker) -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    debug = mocker.patch("subscriber.kvcm.client.logger.debug")
    mocker.patch("subscriber.kvcm.client.logger.is_debug_enabled", return_value=True)

    await client.report_snapshot(
        [_snapshot_batch()], epoch=7, trace_id="snapshot-trace"
    )
    await client.close()

    debug.assert_called_once()
    args = debug.call_args
    assert args.args == ("kvcm snapshot report_event timing",)
    assert args.kwargs["step"] == "kvcm_send"
    tags = args.kwargs["tags"]
    assert tags["epoch"] == 7
    assert tags["trace_id"] == "snapshot-trace"
    assert tags["source_batch_count"] == 1
    assert tags["source_event_count"] == 1
    assert tags["snapshot_block_count"] == 2
    assert tags["event_expand_ms"] >= 0
    assert tags["http_round_trip_ms"] >= 0
    assert tags["kvcm_send_total_ms"] >= 0


async def test_report_kv_events_logs_warning_for_item_results(mocker) -> None:
    fake_sdk = FakeSdkClient()
    fake_sdk.report_event.return_value = {
        "header": {"status": {"code": "OK"}},
        "item_results": ["OK", "INTERNAL_ERROR"],
    }
    warning = mocker.patch("subscriber.kvcm.client.logger.warning")
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    batch = KVEventBatch(
        ts=1.0,
        events=[BlockRemoved(block_hashes=[13], medium="CPU")],
    )

    await client.start()
    fake_sdk.report_event.reset_mock()

    from subscriber.kvcm.errors import KvcmReportRejectedError

    with pytest.raises(KvcmReportRejectedError, match="item_results"):
        await client.report_kv_events([batch], epoch=7, trace_id="incremental-trace")
    await client.close()

    warning.assert_called_once_with(
        "kvcm report_event returned partial item results",
        step="kvcm_send",
        tags={
            "epoch": 7,
            "trace_id": "incremental-trace",
            "item_results": ["OK", "INTERNAL_ERROR"],
        },
    )


async def test_report_kv_events_requests_manager_response_validation() -> None:
    fake_sdk = FakeSdkClient()
    client = _make_kvcm(
        SubscriberConfig(kvcm_heartbeat_interval_s=60.0),
        fake_sdk,
    )
    batch = KVEventBatch(
        ts=1.0,
        events=[BlockRemoved(block_hashes=[13], medium="CPU")],
    )

    await client.start()
    fake_sdk.report_event.reset_mock()
    fake_sdk.report_event.side_effect = RuntimeError(
        "KVCM /api/reportEvent failed: INTERNAL_ERROR "
        "ReportEvent partially failed; see item_results"
    )

    with pytest.raises(RuntimeError, match="INTERNAL_ERROR"):
        await client.report_kv_events([batch], epoch=7)

    await client.close()

    fake_sdk.report_event.assert_awaited_once()
    assert fake_sdk.report_event.call_args.kwargs == {"check_response": True}


@pytest.mark.parametrize(
    "engine_config",
    [None, '{"block_size": 16}'],
    ids=["default_block_size", "custom_block_size"],
)
def test_register_and_report_event_use_same_instance_id(
    monkeypatch: pytest.MonkeyPatch, engine_config: str | None
) -> None:
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-x")
    if engine_config is not None:
        monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", engine_config)
    else:
        monkeypatch.delenv("DS_LLM_ENGINE_CONFIG", raising=False)

    client = _client()
    register_request = client._register_instance_request()
    report_request = client._report_event_request(
        [{"event_type": "EVENT_HOST_DOWN", "host_down": {}}]
    )

    assert register_request["instance_id"] == report_request["instance_id"]


def test_location_specs_uses_effective_block_size_without_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 32}')
    client = _client()

    infos, groups = client._location_specs()

    assert infos == [{"name": "vllm_32", "size": 32}]
    assert groups == [{"name": "default", "spec_names": ["vllm_32"]}]
    assert client._effective_block_size() == 32


def test_location_specs_uses_per_group_block_size_with_metadata() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=128),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=64),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"

    infos, groups = client._location_specs()

    assert infos == [
        {"name": "full_0", "size": 128},
        {"name": "linear_1", "size": 64},
    ]
    assert groups == [
        {"name": "full_0", "spec_names": ["full_0"]},
        {"name": "linear_1", "spec_names": ["linear_1"]},
    ]


def test_register_request_block_size_coherent_with_location_specs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_LLM_ENGINE_CONFIG", '{"block_size": 64}')
    client = _client()

    request = client._register_instance_request()

    assert request["block_size"] == 64
    for spec_info in request["location_spec_infos"]:
        assert spec_info["size"] == request["block_size"]


def test_register_request_block_size_coherent_with_heterogeneous_metadata() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=2192),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=16),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"

    request = client._register_instance_request()

    assert request["block_size"] == 16
    spec_sizes = {s["name"]: s["size"] for s in request["location_spec_infos"]}
    assert spec_sizes == {"full_0": 2192, "linear_1": 16}


def test_block_spec_names_consistent_with_registered_specs() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=16),
        KvCacheGroupSpec(group_idx=1, kind="mamba", block_size=16),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"

    registered_names = {
        s["name"] for s in client._register_instance_request()["location_spec_infos"]
    }
    for group_idx in (0, 1):
        assert set(client._block_spec_names(group_idx)) <= registered_names


def test_location_spec_name_uses_effective_block_size_for_fallback() -> None:
    metadata = [
        KvCacheGroupSpec(group_idx=0, kind="full_attention", block_size=128),
    ]
    client = KvcmClient(
        SubscriberConfig(),
        medium_mapper=_vllm_medium_mapper,
        storage_type="ST_EVENT_REPORT",
        supported_mediums=["hbm", "mem"],
        manager_client=FakeSdkClient(),
        descriptor=KvCacheDescriptor(groups=tuple(metadata)),
    )
    client._host_ip_port_value = "10.0.0.8:9000"

    assert client._location_spec_name(group_idx=None) == "vllm_128"
    assert client._location_spec_name(group_idx=99) == "vllm_128"


# ---------------------------------------------------------------------------
# Registration state tests (simple boolean, no generation/snapshot)
# ---------------------------------------------------------------------------


class TestRegistrationState:
    """Tests for the simple boolean registration state."""

    def test_initial_state_is_unregistered(self) -> None:
        client = _client()
        assert client.is_registered is False

    def test_set_registration_state_true(self) -> None:
        client = _client()
        client._set_registration_state(True)
        assert client.is_registered is True

    def test_set_registration_state_false(self) -> None:
        client = _client()
        client._set_registration_state(True)
        client._set_registration_state(False)
        assert client.is_registered is False

    def test_deduplicated_true_does_not_re_log(self) -> None:
        client = _client()
        client._set_registration_state(True)
        # Calling again with same value should be a no-op (deduplication)
        client._set_registration_state(True)
        assert client.is_registered is True

    def test_deduplicated_false_does_not_re_log(self) -> None:
        client = _client()
        # Initial state is False; setting False again should be a no-op
        client._set_registration_state(False)
        assert client.is_registered is False

    def test_transitions_flip_correctly(self) -> None:
        client = _client()
        client._set_registration_state(True)
        assert client.is_registered is True
        client._set_registration_state(False)
        assert client.is_registered is False
        client._set_registration_state(True)
        assert client.is_registered is True

    def test_transitions_report_metrics_once(self, mocker: MockerFixture) -> None:
        report = mocker.patch("subscriber.kvcm.client.report_registration_transition")
        client = _client()

        client._set_registration_state(True)
        client._set_registration_state(True)
        client._set_registration_state(False)

        assert report.call_args_list == [
            call("unregistered", "registered"),
            call("registered", "unregistered"),
        ]


class TestTwoStepRegistrationPrerequisite:
    """Registration requires BOTH registerInstance and NODE_REGISTER to succeed."""

    async def test_register_instance_failure_leaves_unregistered(self) -> None:
        fake_sdk = FakeSdkClient()
        fake_sdk.register_instance.side_effect = RuntimeError("rpc fail")
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

        await client.start()

        assert client.is_registered is False
        await client.close()

    async def test_node_register_failure_leaves_unregistered(self) -> None:
        fake_sdk = FakeSdkClient()

        async def report_event(
            request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            events = request.get("events")
            if (
                isinstance(events, list)
                and events
                and events[0].get("event_type") == "EVENT_NODE_REGISTER"
            ):
                raise RuntimeError("node register failed")
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.report_event.side_effect = report_event
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

        await client.start()

        assert client.is_registered is False
        await client.close()

    async def test_both_steps_succeed_sets_registered(self) -> None:
        fake_sdk = FakeSdkClient()
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)

        await client.start()

        assert client.is_registered is True
        await client.close()


class TestIndefiniteUnavailableRetries:
    """Heartbeat loop retries registration indefinitely while KVCM is down."""

    async def test_retries_registration_indefinitely_until_available(self) -> None:
        fake_sdk = FakeSdkClient()
        fake_sdk.ready = False
        attempt_count = 0
        registered_event = asyncio.Event()

        async def is_ready() -> bool:
            nonlocal attempt_count
            attempt_count += 1
            # Become ready after 5 attempts
            return attempt_count >= 5

        async def register_instance(
            _request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            registered_event.set()
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.is_ready = AsyncMock(side_effect=is_ready)
        fake_sdk.register_instance = AsyncMock(side_effect=register_instance)
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

        await client.start()
        assert client.is_registered is False

        await asyncio.wait_for(registered_event.wait(), timeout=1.0)
        assert client.is_registered is True
        assert attempt_count >= 5
        await client.close()

    async def test_register_instance_failure_retries_next_cycle(self) -> None:
        fake_sdk = FakeSdkClient()
        call_count = 0
        registered_event = asyncio.Event()

        async def register_instance(
            _request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("temporary failure")
            registered_event.set()
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.register_instance = AsyncMock(side_effect=register_instance)
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

        await client.start()
        # First attempt fails, heartbeat loop retries
        await asyncio.wait_for(registered_event.wait(), timeout=1.0)
        assert client.is_registered is True
        assert call_count == 3
        await client.close()


class TestFalseToTrueRecovery:
    """false->true transition only restores future event reporting, no callback."""

    async def test_heartbeat_failure_transitions_to_unregistered(self) -> None:
        fake_sdk = FakeSdkClient()
        heartbeat_count = 0
        unregistered_event = asyncio.Event()

        async def report_event(
            request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            nonlocal heartbeat_count
            events = request.get("events")
            if (
                isinstance(events, list)
                and events
                and events[0].get("event_type") == "EVENT_HEARTBEAT"
            ):
                heartbeat_count += 1
                if heartbeat_count == 1:
                    raise RuntimeError("kvcm down")
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.report_event = AsyncMock(side_effect=report_event)
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

        await client.start()
        assert client.is_registered is True

        # Poll until heartbeat failure causes unregistered state
        async def wait_unregistered() -> None:
            while client.is_registered:
                await asyncio.sleep(0.005)
            unregistered_event.set()

        task = asyncio.create_task(wait_unregistered())
        await asyncio.wait_for(unregistered_event.wait(), timeout=1.0)
        assert client.is_registered is False
        task.cancel()
        await client.close()

    async def test_recovery_after_failure_restores_registered(self) -> None:
        fake_sdk = FakeSdkClient()
        heartbeat_count = 0
        recovered_event = asyncio.Event()

        async def report_event(
            request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            nonlocal heartbeat_count
            events = request.get("events")
            if (
                isinstance(events, list)
                and events
                and events[0].get("event_type") == "EVENT_HEARTBEAT"
            ):
                heartbeat_count += 1
                if heartbeat_count == 1:
                    raise RuntimeError("kvcm down")
            return {"header": {"status": {"code": "OK"}}}

        async def register_instance(
            _request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            if fake_sdk.register_instance.await_count > 1:
                recovered_event.set()
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.report_event = AsyncMock(side_effect=report_event)
        fake_sdk.register_instance = AsyncMock(side_effect=register_instance)
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

        await client.start()
        assert client.is_registered is True

        # Wait for failure then recovery
        await asyncio.wait_for(recovered_event.wait(), timeout=1.0)
        await asyncio.sleep(0.02)

        assert client.is_registered is True
        await client.close()

    async def test_recovery_does_not_invoke_any_subscriber_health_callback(
        self,
    ) -> None:
        """false->true recovery only restores future event reporting;
        no external callback or subscriber-health notification is made."""
        fake_sdk = FakeSdkClient()
        heartbeat_count = 0
        recovered_event = asyncio.Event()

        async def report_event(
            request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            nonlocal heartbeat_count
            events = request.get("events")
            if (
                isinstance(events, list)
                and events
                and events[0].get("event_type") == "EVENT_HEARTBEAT"
            ):
                heartbeat_count += 1
                if heartbeat_count == 1:
                    raise RuntimeError("kvcm down")
            return {"header": {"status": {"code": "OK"}}}

        async def register_instance(
            _request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            if fake_sdk.register_instance.await_count > 1:
                recovered_event.set()
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.report_event = AsyncMock(side_effect=report_event)
        fake_sdk.register_instance = AsyncMock(side_effect=register_instance)
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)

        await client.start()
        await asyncio.wait_for(recovered_event.wait(), timeout=1.0)
        await asyncio.sleep(0.02)

        # After recovery, report_kv_events should work (future event reporting restored)
        assert client.is_registered is True
        fake_sdk.report_event.reset_mock()
        await client.report_kv_events(
            [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
        )
        fake_sdk.report_event.assert_awaited_once()
        await client.close()


# ---------------------------------------------------------------------------
# KvcmReportError hierarchy and report_kv_events typed-error semantics
# ---------------------------------------------------------------------------


class TestKvcmReportErrorHierarchy:
    """Tests for the typed KvcmReportError exception hierarchy."""

    def test_kvcm_unavailable_error_is_kvcm_report_error(self) -> None:
        from subscriber.kvcm.errors import KvcmReportError, KvcmUnavailableError

        exc = KvcmUnavailableError("transport failure")
        assert isinstance(exc, KvcmReportError)

    def test_kvcm_report_rejected_error_is_kvcm_report_error(self) -> None:
        from subscriber.kvcm.errors import KvcmReportError, KvcmReportRejectedError

        exc = KvcmReportRejectedError("rejected")
        assert isinstance(exc, KvcmReportError)

    def test_kvcm_report_error_is_runtime_error(self) -> None:
        from subscriber.kvcm.errors import KvcmReportError

        exc = KvcmReportError("base")
        assert isinstance(exc, RuntimeError)


class TestSendBatchTypedErrors:
    """report_kv_events raises typed errors without changing registration state.

    Registration is owned by the heartbeat loop; report_kv_events transport failures
    must not flip ``is_registered``.
    """

    async def test_transport_failure_does_not_change_registration(self) -> None:
        """Retryable transport failure raises but leaves is_registered=True."""
        fake_sdk = FakeSdkClient()
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)
        await client.start()
        assert client.is_registered is True

        # Simulate a retryable transport failure
        fake_sdk.report_event.side_effect = RuntimeError("connection reset")

        from subscriber.kvcm.errors import KvcmUnavailableError

        with pytest.raises(KvcmUnavailableError, match="connection reset"):
            await client.report_kv_events(
                [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
            )

        # Registration must NOT change on a report_kv_events transport failure.
        assert client.is_registered is True
        await client.close()

    async def test_not_registered_raises_kvcm_unavailable(self) -> None:
        """report_kv_events when not registered raises KvcmUnavailableError."""
        fake_sdk = FakeSdkClient()
        fake_sdk.ready = False
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)
        await client.start()
        assert client.is_registered is False

        from subscriber.kvcm.errors import KvcmUnavailableError

        with pytest.raises(KvcmUnavailableError, match="not ready"):
            await client.report_kv_events(
                [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
            )
        await client.close()

    async def test_rejected_report_does_not_flip_registration(self) -> None:
        """A rejected/partial report is dropped without changing registration."""
        fake_sdk = FakeSdkClient()
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)
        await client.start()
        assert client.is_registered is True

        # Simulate a non-OK response (rejected report)
        fake_sdk.report_event.side_effect = RuntimeError(
            "KVCM /api/reportEvent failed: INTERNAL_ERROR "
            "ReportEvent partially failed; item_results=['OK', 'INTERNAL_ERROR']"
        )

        from subscriber.kvcm.errors import KvcmReportRejectedError

        with pytest.raises(KvcmReportRejectedError, match="INTERNAL_ERROR"):
            await client.report_kv_events(
                [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
            )

        # Registration must NOT change on rejected report
        assert client.is_registered is True
        await client.close()

    async def test_programming_error_not_classified_as_kvcm_down(self) -> None:
        """Programming errors remain distinct and are not KvcmReportError."""
        client = _client()

        from subscriber.kvcm.errors import KvcmReportError

        with pytest.raises(RuntimeError, match="has not been started"):
            await client.report_kv_events(
                [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
            )

        # Verify it's NOT a KvcmReportError
        try:
            await client.report_kv_events(
                [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
            )
        except RuntimeError as exc:
            assert not isinstance(exc, KvcmReportError)

    async def test_next_batch_sends_after_transport_failure(self) -> None:
        """After a transport failure the next new batch sends normally.

        Registration never flipped, so no re-registration is required.
        """
        fake_sdk = FakeSdkClient()
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=60.0), fake_sdk)
        await client.start()
        assert client.is_registered is True

        from subscriber.kvcm.errors import KvcmUnavailableError

        fake_sdk.report_event.side_effect = RuntimeError("transport failure")
        with pytest.raises(KvcmUnavailableError):
            await client.report_kv_events(
                [KVEventBatch(ts=1.0, events=[AllBlocksCleared()])], epoch=1
            )
        assert client.is_registered is True

        # Next new batch sends normally (no blind replay).
        fake_sdk.report_event.side_effect = None
        fake_sdk.report_event.return_value = {"header": {"status": {"code": "OK"}}}
        await client.report_kv_events(
            [KVEventBatch(ts=2.0, events=[AllBlocksCleared()])], epoch=1
        )
        await client.close()

    async def test_heartbeat_failure_flips_registration(self) -> None:
        """Heartbeat report failure flips registration to false.

        The heartbeat loop (not report_kv_events) owns reconnection state.
        """
        fake_sdk = FakeSdkClient()
        heartbeat_count = 0

        async def report_event(
            request: dict[str, object], **_kwargs: object
        ) -> dict[str, object]:
            nonlocal heartbeat_count
            events = request.get("events")
            if (
                isinstance(events, list)
                and events
                and events[0].get("event_type") == "EVENT_HEARTBEAT"
            ):
                heartbeat_count += 1
                if heartbeat_count == 1:
                    raise RuntimeError("heartbeat transport failure")
            return {"header": {"status": {"code": "OK"}}}

        fake_sdk.report_event = AsyncMock(side_effect=report_event)
        client = _make_kvcm(SubscriberConfig(kvcm_heartbeat_interval_s=0.01), fake_sdk)
        await client.start()
        assert client.is_registered is True

        # Wait for heartbeat failure to flip registration
        async def wait_unregistered() -> None:
            while client.is_registered:
                await asyncio.sleep(0.005)

        await asyncio.wait_for(wait_unregistered(), timeout=1.0)
        assert client.is_registered is False
        await client.close()
