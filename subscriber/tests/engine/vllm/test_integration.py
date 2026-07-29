"""Integration tests: real ZMQ and gRPC sockets, no mocks.

Exercises VllmAdapter against a FakeVllmPublisher that reproduces the wire
protocol of vllm.distributed.kv_events.ZmqEventPublisher (PUB 3-frame
multipart with msgspec.msgpack payloads, plus ROUTER/DEALER replay replies
with delimiter, topic, 8-byte big-endian sequence number, and payload after
vLLM PR #45177) without depending on the vllm package itself. The snapshot test
additionally hosts an in-process
DashLLM ``RpcService`` over a real gRPC channel.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import grpc
import msgspec
import pytest
import zmq
import zmq.asyncio

from subscriber.config import SubscriberConfig
from subscriber.engine.metadata import KvCacheDescriptor
from subscriber.engine.vllm import VllmAdapter
from subscriber.forwarding import (
    consume_incremental_events,
    consume_snapshot_events,
    send_incremental_events,
    send_snapshot_events,
)
from subscriber.kvcm.base import AbstractKvCacheManagerClient
from subscriber.kvcm.client import KvcmClient
from subscriber.kvcm.enum import KvcmReportEventType
from subscriber.pipeline.context import PipelineContext
from subscriber.proto import (
    engine_service_rpc_pb2,
    engine_service_rpc_pb2_grpc,
)
from subscriber.types import (
    AllBlocksCleared,
    BlockSnapshot,
    BlockSnapshotItem,
    BlockStored,
    KVEventBatch,
)

pytestmark = pytest.mark.integration


class FakeVllmPublisher:
    """Minimal stand-in for ZmqEventPublisher that speaks its exact wire
    protocol over real ZMQ sockets."""

    END_SEQ = (-1).to_bytes(8, "big", signed=True)

    def __init__(self, topic: str = "") -> None:
        self._ctx = zmq.asyncio.Context.instance()
        self._pub: zmq.asyncio.Socket = self._ctx.socket(zmq.PUB)
        self._pub.bind("tcp://127.0.0.1:0")
        self._router: zmq.asyncio.Socket = self._ctx.socket(zmq.ROUTER)
        self._router.bind("tcp://127.0.0.1:0")
        self._topic_bytes = topic.encode("utf-8")
        self._buffer: list[tuple[int, bytes]] = []
        self._replay_task: asyncio.Task[None] | None = None

    @property
    def pub_endpoint(self) -> str:
        return self._pub.getsockopt_string(zmq.LAST_ENDPOINT)

    @property
    def replay_endpoint(self) -> str:
        return self._router.getsockopt_string(zmq.LAST_ENDPOINT)

    def start_replay_server(self) -> None:
        self._replay_task = asyncio.create_task(self._serve_replay_loop())

    async def _serve_replay_loop(self) -> None:
        while True:
            client_id, _, start_seq_bytes = await self._router.recv_multipart()
            start_seq = int.from_bytes(start_seq_bytes, "big")
            for seq, payload in self._buffer:
                if seq >= start_seq:
                    await self._router.send_multipart(
                        [
                            client_id,
                            b"",
                            self._topic_bytes,
                            seq.to_bytes(8, "big"),
                            payload,
                        ]
                    )
            await self._router.send_multipart([client_id, b"", b"", self.END_SEQ, b""])

    async def publish(self, seq: int, batch: KVEventBatch) -> None:
        """Send live on the wire and record in the replay buffer."""
        payload = msgspec.msgpack.encode(batch)
        self._buffer.append((seq, payload))
        await self._pub.send_multipart(
            [self._topic_bytes, seq.to_bytes(8, "big"), payload]
        )

    def record_dropped(self, seq: int, batch: KVEventBatch) -> None:
        """Simulate a message lost in transit: kept in the replay buffer
        only, never sent live, so a later gap must be filled via replay."""
        payload = msgspec.msgpack.encode(batch)
        self._buffer.append((seq, payload))

    async def publish_until_delivered(
        self,
        seq: int,
        batch: KVEventBatch,
        delivered: asyncio.Event,
        interval: float = 0.02,
    ) -> None:
        """Work around the PUB/SUB slow-joiner race by resending until the
        subscriber confirms receipt. Safe because ZMQ PUB never queues a
        message for a socket that connects after it was sent, so at most
        one resend actually reaches the subscriber."""
        payload = msgspec.msgpack.encode(batch)
        if not any(existing_seq == seq for existing_seq, _ in self._buffer):
            self._buffer.append((seq, payload))
        while not delivered.is_set():
            await self._pub.send_multipart(
                [self._topic_bytes, seq.to_bytes(8, "big"), payload]
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(delivered.wait(), timeout=interval)

    async def close(self) -> None:
        if self._replay_task is not None:
            self._replay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._replay_task
        self._pub.close(linger=0)
        self._router.close(linger=0)


class FakeDashllmSnapshotService(engine_service_rpc_pb2_grpc.RpcServiceServicer):
    """In-process DashLLM RPC peer for the full-snapshot wire boundary."""

    def __init__(
        self,
        response: engine_service_rpc_pb2.KvCacheBlockListPB,
    ) -> None:
        self._response = response
        self.requests: list[engine_service_rpc_pb2.KvCacheBlocksRequestPB] = []

    async def GetAllKvCacheBlocks(
        self,
        request: engine_service_rpc_pb2.KvCacheBlocksRequestPB,
        context: grpc.aio.ServicerContext,
    ) -> engine_service_rpc_pb2.KvCacheBlockListPB:
        self.requests.append(request)
        return self._response


class FakeKvcmSignalManager(AbstractKvCacheManagerClient):
    """In-process KVCM transport that asks for a snapshot after a live add."""

    def __init__(self) -> None:
        self.incremental_requests: list[dict[str, Any]] = []
        self.snapshot_requests: list[dict[str, Any]] = []
        self.incremental_reported = asyncio.Event()

    async def start(self) -> None:
        return None

    async def is_ready(self) -> bool:
        return True

    async def register_instance(
        self, data: dict[str, Any], check_response: bool = True
    ) -> dict[str, Any]:
        return {"header": {"status": {"code": "OK"}}}

    async def report_event(
        self, data: dict[str, Any], check_response: bool = True
    ) -> dict[str, Any]:
        event_type = data["events"][0]["event_type"]
        if event_type == KvcmReportEventType.BLOCK_ADD:
            self.incremental_requests.append(data)
            self.incremental_reported.set()
            return {
                "header": {"status": {"code": "OK"}},
                "snapshot_required": True,
            }
        if event_type == KvcmReportEventType.BLOCK_SNAPSHOT:
            self.snapshot_requests.append(data)
        return {"header": {"status": {"code": "OK"}}}

    async def close(self) -> None:
        return None


class AlwaysReadyCoordinator:
    """Minimal ready epoch contract used to exercise both forwarding tasks."""

    def capture_epoch(self) -> int:
        return 1

    async def wait_ready_epoch(self) -> int:
        return 1

    def is_epoch_current(self, epoch: int) -> bool:
        return epoch == 1


async def _stop_consumer(consumer: asyncio.Task[None]) -> None:
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer


async def _wait_until(predicate: Callable[[], bool]) -> None:
    while not predicate():
        await asyncio.sleep(0.01)


async def test_real_zmq_publisher_delivers_single_event() -> None:
    publisher = FakeVllmPublisher()
    config = SubscriberConfig(
        zmq_pub_endpoint=publisher.pub_endpoint,
        zmq_replay_endpoint=publisher.replay_endpoint,
    )
    adapter = VllmAdapter(config)

    batch = KVEventBatch(
        ts=1.0,
        events=[
            BlockStored(
                block_hashes=[1, 2],
                parent_block_hash=None,
                token_ids=[10, 20, 30],
                block_size=16,
                lora_id=None,
                medium="GPU",
                lora_name=None,
            )
        ],
    )

    delivered = asyncio.Event()
    results: list[list[KVEventBatch]] = []

    async def _consume() -> None:
        async for received in adapter.subscribe_kv_events():
            results.append(received.batches)
            delivered.set()
            break

    consumer = asyncio.create_task(_consume())
    try:
        await asyncio.wait_for(
            publisher.publish_until_delivered(0, batch, delivered), timeout=5.0
        )
        await asyncio.wait_for(consumer, timeout=5.0)
    finally:
        await _stop_consumer(consumer)
        await publisher.close()

    assert results == [[batch]]


async def test_sequence_gap_triggers_pr_45177_four_frame_real_replay() -> None:
    """A real ROUTER/DEALER round trip accepts the topic-bearing replay reply
    introduced in vLLM PR #45177 and yields each sequence exactly once."""

    publisher = FakeVllmPublisher()
    publisher.start_replay_server()
    config = SubscriberConfig(
        zmq_pub_endpoint=publisher.pub_endpoint,
        zmq_replay_endpoint=publisher.replay_endpoint,
    )
    adapter = VllmAdapter(config)

    batch0 = KVEventBatch(ts=1.0, events=[AllBlocksCleared()])
    dropped_batch1 = KVEventBatch(ts=2.0, events=[AllBlocksCleared()])
    batch2 = KVEventBatch(ts=3.0, events=[AllBlocksCleared()])

    delivered = asyncio.Event()
    results: list[list[KVEventBatch]] = []

    async def _consume() -> None:
        async for received in adapter.subscribe_kv_events():
            results.append(received.batches)
            delivered.set()
            if len(results) >= 3:
                break

    consumer = asyncio.create_task(_consume())
    try:
        await asyncio.wait_for(
            publisher.publish_until_delivered(0, batch0, delivered), timeout=5.0
        )
        publisher.record_dropped(1, dropped_batch1)
        await publisher.publish(2, batch2)
        await asyncio.wait_for(consumer, timeout=5.0)
    finally:
        await _stop_consumer(consumer)
        await publisher.close()

    # The publisher's replay buffer returns every batch with seq >= start_seq,
    # including the live batch that exposed the gap. The adapter filters the
    # replay stream to the missing range only, so the triggering live batch
    # is forwarded exactly once.
    assert results == [[batch0], [dropped_batch1], [batch2]]


async def test_real_zmq_backlog_is_recorded_after_one_message_is_received() -> None:
    publisher = FakeVllmPublisher()
    config = SubscriberConfig(
        zmq_pub_endpoint=publisher.pub_endpoint,
        zmq_replay_endpoint=publisher.replay_endpoint,
    )
    adapter = VllmAdapter(config)
    batch = KVEventBatch(ts=1.0, events=[AllBlocksCleared()])
    warmup_received = asyncio.Event()

    async def _receive_warmup() -> tuple[int, bytes] | None:
        received = await adapter._incremental._recv_live_message()
        warmup_received.set()
        return received

    warmup = asyncio.create_task(_receive_warmup())
    try:
        await asyncio.wait_for(
            publisher.publish_until_delivered(0, batch, warmup_received), timeout=5.0
        )
        assert (await asyncio.wait_for(warmup, timeout=5.0))[0] == 0

        queue_metrics = MagicMock()
        adapter._incremental._zmq_queue_metrics = queue_metrics
        await publisher.publish(1, batch)
        await publisher.publish(2, batch)
        await asyncio.sleep(0.05)

        assert (await adapter._incremental._recv_live_message())[0] == 1
        assert (await adapter._incremental._recv_live_message())[0] == 2
    finally:
        await _stop_consumer(warmup)
        adapter._incremental._sub.close(linger=0)
        adapter._incremental._dealer.close(linger=0)
        await publisher.close()


async def test_real_dashllm_grpc_snapshot_yields_full_kv_cache_blocks() -> None:
    import msgspec.msgpack

    items = [(b"\xaa", 0, 3, 1), (b"\xbb", 2, 7, 1)]
    response = engine_service_rpc_pb2.KvCacheBlockListPB(
        raw_snapshot=msgspec.msgpack.encode(items),
        block_size=16,
        snapshot_version=9,
    )
    service = FakeDashllmSnapshotService(response)
    server = grpc.aio.server()
    engine_service_rpc_pb2_grpc.add_RpcServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    assert port != 0
    await server.start()

    adapter = VllmAdapter(
        SubscriberConfig(
            engine_grpc_endpoint=f"127.0.0.1:{port}",
            engine_snapshot_full_sync_interval_s=60.0,
        )
    )
    events = adapter.subscribe_snapshot_events()
    try:
        event = await asyncio.wait_for(events.__anext__(), timeout=5.0)
    finally:
        await events.aclose()
        await adapter.close()
        await server.stop(None)

    assert service.requests == [engine_service_rpc_pb2.KvCacheBlocksRequestPB()]
    assert len(event.batches) == 1
    assert event.batches[0].events == [
        BlockSnapshot(
            medium="GPU",
            block_size=16,
            items=[
                BlockSnapshotItem(block_hash=b"\xaa", group_idx=0),
                BlockSnapshotItem(block_hash=b"\xbb", group_idx=2),
            ],
            snapshot_version=9,
        )
    ]
    assert [span.name for span in event.telemetry.spans] == [
        "snapshot_fetch",
        "decode",
        "snapshot_build",
    ]


async def test_snapshot_required_signal_runs_snapshot_pipeline_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KVCM's signal wakes snapshot polling without stopping incremental sends."""

    import msgspec.msgpack

    response = engine_service_rpc_pb2.KvCacheBlockListPB(
        raw_snapshot=msgspec.msgpack.encode([(b"\xaa", 0, 3, 1)]),
        block_size=16,
        snapshot_version=9,
    )
    snapshot_service = FakeDashllmSnapshotService(response)
    grpc_server = grpc.aio.server()
    engine_service_rpc_pb2_grpc.add_RpcServiceServicer_to_server(
        snapshot_service, grpc_server
    )
    grpc_port = grpc_server.add_insecure_port("127.0.0.1:0")
    assert grpc_port != 0
    await grpc_server.start()

    publisher = FakeVllmPublisher()
    manager = FakeKvcmSignalManager()
    config = SubscriberConfig(
        zmq_pub_endpoint=publisher.pub_endpoint,
        zmq_replay_endpoint=publisher.replay_endpoint,
        engine_grpc_endpoint=f"127.0.0.1:{grpc_port}",
        engine_snapshot_full_sync_interval_s=60.0,
        kvcm_base_url="spectrum://vs-test:6382",
        kvcm_heartbeat_interval_s=60.0,
    )
    adapter = VllmAdapter(config)
    kvcm = KvcmClient(
        config,
        medium_mapper=adapter.map_medium,
        storage_type=adapter.storage_type(),
        supported_mediums=adapter.supported_mediums(),
        descriptor=KvCacheDescriptor(groups=()),
        manager_client=manager,
    )

    async def resolve_host_ip_port(port: int) -> str:
        return "127.0.0.1:9000"

    monkeypatch.setattr(
        "subscriber.kvcm.client.resolve_host_ip_port", resolve_host_ip_port
    )
    monkeypatch.setenv("SPECTRUM_DEPLOYMENT_NAME", "deploy-integration")
    await kvcm.start()

    coordinator = AlwaysReadyCoordinator()
    incremental_queue: asyncio.Queue[PipelineContext] = asyncio.Queue()
    snapshot_queue: asyncio.Queue[PipelineContext] = asyncio.Queue()
    incremental_producer = asyncio.create_task(
        consume_incremental_events(adapter, coordinator, incremental_queue, MagicMock())
    )
    incremental_sender = asyncio.create_task(
        send_incremental_events(
            kvcm,
            coordinator,
            incremental_queue,
            on_snapshot_required=adapter.request_immediate_snapshot,
            max_merged_queue_items=1,
            max_merged_report_events=1,
        )
    )
    snapshot_producer = asyncio.create_task(
        consume_snapshot_events(adapter, coordinator, snapshot_queue, MagicMock())
    )
    snapshot_sender = asyncio.create_task(
        send_snapshot_events(kvcm, coordinator, snapshot_queue)
    )

    try:
        await asyncio.wait_for(
            _wait_until(lambda: len(manager.snapshot_requests) == 1), timeout=5.0
        )
        await asyncio.wait_for(
            publisher.publish_until_delivered(
                0,
                KVEventBatch(
                    ts=1.0,
                    events=[
                        BlockStored(
                            block_hashes=[1],
                            parent_block_hash=None,
                            token_ids=[10],
                            block_size=16,
                            lora_id=None,
                            medium="GPU",
                            lora_name=None,
                        )
                    ],
                ),
                manager.incremental_reported,
            ),
            timeout=5.0,
        )
        await asyncio.wait_for(
            _wait_until(lambda: len(manager.snapshot_requests) >= 2), timeout=5.0
        )

        assert manager.incremental_requests
        assert len(snapshot_service.requests) >= 2
        assert not incremental_sender.done()
    finally:
        await _stop_consumer(incremental_producer)
        await _stop_consumer(incremental_sender)
        await _stop_consumer(snapshot_producer)
        await _stop_consumer(snapshot_sender)
        await kvcm.close()
        await adapter.close()
        await publisher.close()
        await grpc_server.stop(None)
