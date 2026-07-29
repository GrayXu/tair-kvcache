"""Reusable gRPC client for local DashLLM control-plane RPCs."""

from __future__ import annotations

from typing import cast

import grpc

from subscriber.proto import (
    engine_service_rpc_pb2,
    engine_service_rpc_pb2_grpc,
)

# Arg names per grpc/include/grpc/impl/channel_arg_names.h.
_CHANNEL_OPTIONS: list[tuple[str, int]] = [
    ("grpc.max_receive_message_length", 8 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 2_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.enable_retries", 0),
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 1_000),
    ("grpc.tcp_receive_buffer_size", 512 * 1024),
]


class DashllmGrpcClient:
    """Own one asyncio-loop-local DashLLM gRPC channel and its stubs."""

    def __init__(self, target: str) -> None:
        self._target = target
        self._channel: grpc.aio.Channel | None = None
        self._rpc_service: engine_service_rpc_pb2_grpc.RpcServiceStub | None = None
        self._status_version = -1
        self._latest_cache_version = 0
        self._latest_finished_version = -1

    def _get_rpc_service(self) -> engine_service_rpc_pb2_grpc.RpcServiceStub:
        if self._rpc_service is None:
            self._channel = grpc.aio.insecure_channel(
                self._target, options=_CHANNEL_OPTIONS
            )
            self._rpc_service = engine_service_rpc_pb2_grpc.RpcServiceStub(
                self._channel
            )
        return self._rpc_service

    async def get_kv_cache_descriptor(self, timeout_s: float) -> object:
        """Fetch the vLLM KV cache descriptor response."""

        response = await self._get_rpc_service().GetKvCacheDescriptor(
            engine_service_rpc_pb2.KvCacheDescriptorRequestPB(), timeout=timeout_s
        )
        return cast(object, response)

    async def get_worker_status(
        self, timeout_s: float
    ) -> engine_service_rpc_pb2.WorkerStatusPB:
        """Probe liveness and advance the FlexLB-compatible finished cursor."""

        request = engine_service_rpc_pb2.StatusVersionPB(
            latest_cache_version=self._latest_cache_version,
            latest_finished_version=self._latest_finished_version,
        )
        response = await self._get_rpc_service().GetWorkerStatus(
            request,
            timeout=timeout_s,
        )
        typed_response = cast(engine_service_rpc_pb2.WorkerStatusPB, response)
        if (
            typed_response.status_version > 0
            and typed_response.status_version > self._status_version
        ):
            self._status_version = typed_response.status_version
            self._latest_finished_version = typed_response.latest_finished_version
        return typed_response

    async def get_all_kv_cache_blocks(self, timeout_s: float) -> object:
        """Fetch the full KV cache block snapshot from DashLLM."""

        response = await self._get_rpc_service().GetAllKvCacheBlocks(
            engine_service_rpc_pb2.KvCacheBlocksRequestPB(), timeout=timeout_s
        )
        return cast(object, response)

    async def close(self) -> None:
        """Release the channel when the owning adapter stops."""

        channel = self._channel
        self._channel = None
        self._rpc_service = None
        if channel is not None:
            await channel.close()
