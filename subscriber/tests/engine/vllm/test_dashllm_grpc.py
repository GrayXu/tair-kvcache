from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from subscriber.engine.dashllm_grpc import DashllmGrpcClient
from subscriber.proto import (
    engine_service_rpc_pb2,
    engine_service_rpc_pb2_grpc,
)


class _FakeGrpcChannel:
    def __init__(self, response: object | list[object]) -> None:
        self._responses = response if isinstance(response, list) else [response]
        self.paths: list[str] = []
        self.requests: list[object] = []
        self.timeouts: list[float] = []
        self.close = AsyncMock()

    def unary_unary(self, path: str, **_: Any) -> Any:
        self.paths.append(path)

        async def invoke(request: object, *, timeout: float) -> object:
            self.requests.append(request)
            self.timeouts.append(timeout)
            response_index = min(
                len(self.requests) - 1,
                len(self._responses) - 1,
            )
            return self._responses[response_index]

        return invoke


async def test_metadata_requests_reuse_one_channel(mocker: Any) -> None:
    response = engine_service_rpc_pb2.KvCacheDescriptorPB()
    channel = _FakeGrpcChannel(response)
    insecure_channel = mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    assert await client.get_kv_cache_descriptor(1.0) is response
    assert await client.get_kv_cache_descriptor(2.0) is response

    insecure_channel.assert_called_once()
    assert insecure_channel.call_args.args == ("127.0.0.1:18002",)
    assert insecure_channel.call_args.kwargs["options"]
    assert "/RpcService/GetKvCacheDescriptor" in channel.paths
    assert len(channel.requests) == 2
    assert channel.timeouts == [1.0, 2.0]


async def test_close_releases_created_channel_once(mocker: Any) -> None:
    response = engine_service_rpc_pb2.KvCacheDescriptorPB()
    channel = _FakeGrpcChannel(response)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    await client.get_kv_cache_descriptor(1.0)
    await client.close()
    await client.close()

    channel.close.assert_awaited_once()


def test_kv_cache_descriptor_pb_uses_authoritative_field_numbers_and_raw_wire():
    descriptor = engine_service_rpc_pb2.KvCacheDescriptorPB.DESCRIPTOR

    assert hasattr(engine_service_rpc_pb2, "KvCacheDescriptorErrorCode")
    assert not hasattr(engine_service_rpc_pb2, "KvCacheGroupMetadataErrorCode")
    assert descriptor.fields_by_name["items"].number == 1
    assert descriptor.fields_by_name["settings"].number == 2
    assert descriptor.fields_by_name["err_code"].number == 3
    assert descriptor.fields_by_name["err_msg"].number == 4

    # settings={use_eagle_pop: true, mamba_cache_mode: "light"},
    # err_code=UNAVAILABLE, err_msg="unavailable".
    msg = engine_service_rpc_pb2.KvCacheDescriptorPB.FromString(
        b"\x12\x09\x08\x01\x12\x05light\x18\x01\x22\x0bunavailable"
    )

    assert msg.settings.use_eagle_pop is True
    assert msg.settings.mamba_cache_mode == "light"
    assert msg.err_code == engine_service_rpc_pb2.KV_CACHE_DESCRIPTOR_UNAVAILABLE
    assert msg.err_msg == "unavailable"


# --- GetWorkerStatus proto and client tests ---


def test_status_version_pb_is_request_type() -> None:
    """StatusVersionPB exists and is the request message for GetWorkerStatus."""
    msg = engine_service_rpc_pb2.StatusVersionPB()
    assert msg.latest_cache_version == 0
    assert msg.latest_finished_version == 0


def test_worker_status_pb_alive_field_number_is_13() -> None:
    """WorkerStatusPB.alive must be field number 13."""
    descriptor = engine_service_rpc_pb2.WorkerStatusPB.DESCRIPTOR
    alive_field = descriptor.fields_by_name["alive"]
    assert alive_field.number == 13


def test_worker_status_pb_version_field_numbers_match_dashllm() -> None:
    descriptor = engine_service_rpc_pb2.WorkerStatusPB.DESCRIPTOR

    assert descriptor.fields_by_name["status_version"].number == 12
    assert descriptor.fields_by_name["latest_finished_version"].number == 15


def test_worker_status_pb_from_string_tolerates_unknown_fields() -> None:
    """WorkerStatusPB.FromString with unknown fields still parses alive."""
    # \x08\x01 = field 1 varint 1 (unknown), \x68\x01 = field 13 varint 1 (alive)
    msg = engine_service_rpc_pb2.WorkerStatusPB.FromString(b"\x08\x01\x68\x01")
    assert msg.alive is True


def test_worker_status_pb_parses_version_fields_by_wire_number() -> None:
    # field 12 = 100, field 13 = true, field 15 = 7
    msg = engine_service_rpc_pb2.WorkerStatusPB.FromString(b"\x60\x64\x68\x01\x78\x07")

    assert msg.status_version == 100
    assert msg.alive is True
    assert msg.latest_finished_version == 7


def test_worker_status_pb_alive_false_by_default() -> None:
    """Default WorkerStatusPB has alive=False."""
    msg = engine_service_rpc_pb2.WorkerStatusPB()
    assert msg.alive is False


def test_rpc_path_is_rpc_service_get_worker_status() -> None:
    """The full RPC path must be /RpcService/GetWorkerStatus (no package)."""
    stub_cls = engine_service_rpc_pb2_grpc.RpcServiceStub
    # Inspect the stub constructor source to verify path registration.
    # We verify by creating a stub with a fake channel that captures the path.
    paths: list[str] = []

    class _PathCapture:
        def unary_unary(self, path: str, **_: Any) -> Any:
            paths.append(path)
            return AsyncMock()

    stub_cls(_PathCapture())
    assert "/RpcService/GetWorkerStatus" in paths


async def test_get_worker_status_sends_initial_flexlb_cursors(
    mocker: Any,
) -> None:
    response = engine_service_rpc_pb2.WorkerStatusPB(
        alive=True,
        status_version=100,
        latest_finished_version=7,
    )
    channel = _FakeGrpcChannel(response)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    result = await client.get_worker_status(timeout_s=2.0)

    assert result is response
    assert result.alive is True
    assert "/RpcService/GetWorkerStatus" in channel.paths
    assert len(channel.requests) == 1
    request = channel.requests[0]
    assert isinstance(request, engine_service_rpc_pb2.StatusVersionPB)
    assert request.latest_cache_version == 0
    assert request.latest_finished_version == -1
    assert channel.timeouts == [2.0]


async def test_get_worker_status_sends_response_finished_cursor_on_next_request(
    mocker: Any,
) -> None:
    responses = [
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=100,
            latest_finished_version=7,
        ),
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=101,
            latest_finished_version=8,
        ),
    ]
    channel = _FakeGrpcChannel(responses)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)

    assert [request.latest_finished_version for request in channel.requests] == [-1, 7]


async def test_get_worker_status_ignores_stale_response_cursor(mocker: Any) -> None:
    responses = [
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=100,
            latest_finished_version=7,
        ),
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=99,
            latest_finished_version=3,
        ),
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=101,
            latest_finished_version=8,
        ),
    ]
    channel = _FakeGrpcChannel(responses)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)

    assert [request.latest_finished_version for request in channel.requests] == [
        -1,
        7,
        7,
    ]


async def test_get_worker_status_accepts_finished_cursor_reset_from_new_snapshot(
    mocker: Any,
) -> None:
    responses = [
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=100,
            latest_finished_version=7,
        ),
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=False,
            status_version=101,
            latest_finished_version=0,
        ),
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=102,
            latest_finished_version=1,
        ),
    ]
    channel = _FakeGrpcChannel(responses)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)

    assert [request.latest_finished_version for request in channel.requests] == [
        -1,
        7,
        0,
    ]


async def test_get_worker_status_ignores_zero_status_version(mocker: Any) -> None:
    responses = [
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=0,
            latest_finished_version=9,
        ),
        engine_service_rpc_pb2.WorkerStatusPB(
            alive=True,
            status_version=100,
            latest_finished_version=10,
        ),
    ]
    channel = _FakeGrpcChannel(responses)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)

    assert [request.latest_finished_version for request in channel.requests] == [-1, -1]


async def test_get_worker_status_rpc_failure_does_not_advance_cursor(
    mocker: Any,
) -> None:
    response = engine_service_rpc_pb2.WorkerStatusPB(
        alive=True,
        status_version=100,
        latest_finished_version=7,
    )
    client = DashllmGrpcClient("127.0.0.1:18002")
    mock_stub = AsyncMock()
    mock_stub.GetWorkerStatus = AsyncMock(
        side_effect=[RuntimeError("rpc failed"), response]
    )
    mocker.patch.object(client, "_get_rpc_service", return_value=mock_stub)

    with pytest.raises(RuntimeError, match="rpc failed"):
        await client.get_worker_status(timeout_s=1.0)
    await client.get_worker_status(timeout_s=1.0)

    requests = [call.args[0] for call in mock_stub.GetWorkerStatus.await_args_list]
    assert [request.latest_finished_version for request in requests] == [-1, -1]
    assert [request.latest_cache_version for request in requests] == [0, 0]


async def test_get_worker_status_uses_rpc_service_facade(mocker: Any) -> None:
    """get_worker_status must go through _get_rpc_service(), not a raw stub."""
    response = engine_service_rpc_pb2.WorkerStatusPB(alive=True)
    client = DashllmGrpcClient("127.0.0.1:18002")
    mock_stub = AsyncMock()
    mock_stub.GetWorkerStatus = AsyncMock(return_value=response)
    mocker.patch.object(client, "_get_rpc_service", return_value=mock_stub)

    result = await client.get_worker_status(timeout_s=1.5)

    assert result is response
    mock_stub.GetWorkerStatus.assert_awaited_once_with(
        engine_service_rpc_pb2.StatusVersionPB(
            latest_cache_version=0,
            latest_finished_version=-1,
        ),
        timeout=1.5,
    )


# --- GetAllKvCacheBlocks proto and client tests ---


def test_kv_cache_block_proto_field_numbers_match_dashllm() -> None:
    list_descriptor = engine_service_rpc_pb2.KvCacheBlockListPB.DESCRIPTOR

    assert list_descriptor.fields_by_name["raw_snapshot"].number == 1
    assert list_descriptor.fields_by_name["snapshot_version"].number == 2
    assert list_descriptor.fields_by_name["block_size"].number == 3


def test_kv_cache_block_list_parses_raw_snapshot_by_wire_number() -> None:
    msg = engine_service_rpc_pb2.KvCacheBlockListPB(
        raw_snapshot=b"\x93\xa1a\xa1b\xa1c",
        snapshot_version=42,
        block_size=1072,
    )
    parsed = engine_service_rpc_pb2.KvCacheBlockListPB.FromString(
        msg.SerializeToString()
    )

    assert parsed.raw_snapshot == b"\x93\xa1a\xa1b\xa1c"
    assert parsed.snapshot_version == 42
    assert parsed.block_size == 1072


def test_rpc_path_is_rpc_service_get_all_kv_cache_blocks() -> None:
    """The full RPC path must be package-free for DashLLM compatibility."""

    paths: list[str] = []

    class _PathCapture:
        def unary_unary(self, path: str, **_: Any) -> Any:
            paths.append(path)
            return AsyncMock()

    engine_service_rpc_pb2_grpc.RpcServiceStub(_PathCapture())

    assert "/RpcService/GetAllKvCacheBlocks" in paths


async def test_get_all_kv_cache_blocks_sends_empty_request_and_returns_response(
    mocker: Any,
) -> None:
    response = engine_service_rpc_pb2.KvCacheBlockListPB(
        block_size=16,
        snapshot_version=42,
    )
    channel = _FakeGrpcChannel(response)
    mocker.patch("grpc.aio.insecure_channel", return_value=channel)
    client = DashllmGrpcClient("127.0.0.1:18002")

    result = await client.get_all_kv_cache_blocks(timeout_s=2.0)

    assert result is response
    assert "/RpcService/GetAllKvCacheBlocks" in channel.paths
    assert len(channel.requests) == 1
    assert isinstance(
        channel.requests[0],
        engine_service_rpc_pb2.KvCacheBlocksRequestPB,
    )
    assert channel.timeouts == [2.0]
