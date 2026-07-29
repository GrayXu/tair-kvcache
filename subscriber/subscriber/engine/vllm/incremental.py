from __future__ import annotations

import asyncio
import time
from collections import Counter
from collections.abc import AsyncGenerator, Callable, Mapping

import msgspec
import zmq
import zmq.asyncio

from subscriber import logger
from subscriber.config import SubscriberConfig
from subscriber.engine.base import EngineEventBatch
from subscriber.metrics import BatchTelemetry, report_zmq_message
from subscriber.trace import generate_trace_id
from subscriber.types import (
    BlockRemoved,
    BlockStored,
    KVEventBatch,
    format_block_hash,
)
from subscriber.utils.msgpack_helper import KVEventBatchMsgpackHelper

_END_SEQ = (-1).to_bytes(8, "big", signed=True)
_MAX_DEBUG_BLOCK_HASHES = 32
_RECV_FAILURE_WARN_INTERVAL_S = 5.0
_RECV_FAILURE_BACKOFF_S = 0.1


class _ZmqSequenceGapDiagnostics:
    """Per-window ZMQ socket-state accumulator used only for warning-log tags.

    Records the byte count of received messages and how often the SUB socket
    is observed with more data pending after ``recv_multipart``. When a
    sequence gap is detected, :meth:`snapshot_and_reset` returns a tag dict
    combining the caller-provided static socket state (via ``state_reader``)
    with the accumulated window counters, and clears the window.

    Stable libzmq does not expose an exact inbound queue depth for a SUB
    socket, so these observations are the best proxy for "was there more
    data waiting when we processed this message" at the time of a gap.

    This is deliberately *not* a metrics emitter: the two per-message
    counters that used to double as metrics (``zmq_received_message_count``
    and ``zmq_sequence_gap_count`` / ``zmq_missed_message_count``) are now
    reported through the lifecycle helper and the per-batch
    :class:`BatchTelemetry` respectively. The values captured here only
    surface as diagnostic tags on the ``zmq_replay`` warning log.
    """

    def __init__(
        self,
        *,
        state_reader: Callable[[], Mapping[str, object]],
    ) -> None:
        self._state_reader = state_reader
        self._received_message_bytes = 0
        self._queue_nonempty_observation_count = 0

    def record_message(
        self,
        *,
        message_bytes: int,
        queue_nonempty_after_receive: bool,
    ) -> None:
        self._received_message_bytes += message_bytes
        if queue_nonempty_after_receive:
            self._queue_nonempty_observation_count += 1

    def snapshot_and_reset(self) -> dict[str, object]:
        tags: dict[str, object] = {}
        try:
            tags.update(self._state_reader())
        except Exception as exc:
            tags["zmq_queue_state_error"] = type(exc).__name__
        tags.update(
            {
                "zmq_received_message_bytes": self._received_message_bytes,
                "zmq_queue_nonempty_observation_count": (
                    self._queue_nonempty_observation_count
                ),
            }
        )
        self._received_message_bytes = 0
        self._queue_nonempty_observation_count = 0
        return tags


def _event_for_debug(event: BlockStored | BlockRemoved) -> dict[str, object]:
    """Copy an event for logging without token IDs or mutating forwarding data."""

    event_data = msgspec.structs.asdict(event)
    event_data.pop("token_ids", None)
    event_data["block_hashes"] = [
        format_block_hash(block_hash) for block_hash in event.block_hashes
    ]
    if isinstance(event, BlockStored) and event.parent_block_hash is not None:
        event_data["parent_block_hash"] = format_block_hash(event.parent_block_hash)
    return event_data


def _summarize_batch(batch: KVEventBatch) -> dict[str, object]:
    event_type_counts = Counter(type(event).__name__ for event in batch.events)
    stored_blocks: list[dict[str, object]] = []
    removed_blocks: list[dict[str, object]] = []
    for event in batch.events:
        if isinstance(event, BlockStored):
            if len(stored_blocks) < _MAX_DEBUG_BLOCK_HASHES:
                stored_blocks.append(_event_for_debug(event))
        elif isinstance(event, BlockRemoved):
            if len(removed_blocks) < _MAX_DEBUG_BLOCK_HASHES:
                removed_blocks.append(_event_for_debug(event))
    return {
        "event_count": len(batch.events),
        "event_types": ",".join(
            f"{name}:{count}" for name, count in sorted(event_type_counts.items())
        ),
        "data_parallel_rank": batch.data_parallel_rank,
        "stored_block_count": len(stored_blocks),
        "stored_blocks": stored_blocks,
        "stored_blocks_truncated": sum(
            1 for event in batch.events if isinstance(event, BlockStored)
        )
        > len(stored_blocks),
        "removed_block_count": len(removed_blocks),
        "removed_blocks": removed_blocks,
        "removed_blocks_truncated": sum(
            1 for event in batch.events if isinstance(event, BlockRemoved)
        )
        > len(removed_blocks),
    }


class VllmIncrementalSource:
    """Own vLLM's ZMQ stream, replay protocol, and generation-local state."""

    def __init__(self, config: SubscriberConfig) -> None:
        self._config = config
        self._endpoint = config.dp_endpoints[0]
        self._ctx = zmq.asyncio.Context.instance()
        self._msgpack_helper = KVEventBatchMsgpackHelper()
        self._last_seq = -1
        self._generation = 0
        self._closed = False
        self._recv_failure_count = 0
        self._last_recv_warn_s: float | None = None
        logger.info(
            "connecting vLLM ZMQ sockets",
            step="zmq_connect",
            tags={
                "pub_endpoint": self._endpoint.zmq_pub_endpoint,
                "replay_endpoint": self._endpoint.zmq_replay_endpoint,
                "topic": self._endpoint.zmq_topic,
                "reconnect_ivl_ms": self._config.zmq_reconnect_ivl_ms,
                "reconnect_ivl_max_ms": self._config.zmq_reconnect_ivl_max_ms,
            },
        )
        self._sub = self._open_sub_socket()
        self._dealer = self._open_dealer_socket()
        self._zmq_gap_diagnostics = _ZmqSequenceGapDiagnostics(
            state_reader=self._zmq_queue_state,
        )

    def _open_sub_socket(self) -> zmq.asyncio.Socket:
        sub = self._ctx.socket(zmq.SUB)
        self._configure_sub_socket(sub)
        sub.connect(self._endpoint.zmq_pub_endpoint)
        sub.setsockopt_string(zmq.SUBSCRIBE, self._endpoint.zmq_topic)
        return sub

    def _open_dealer_socket(self) -> zmq.asyncio.Socket:
        dealer = self._ctx.socket(zmq.DEALER)
        dealer.connect(self._endpoint.zmq_replay_endpoint)
        return dealer

    def _replace_replay_socket(
        self, failed_dealer: zmq.asyncio.Socket, generation: int
    ) -> None:
        """Discard a failed replay socket so a stale reply cannot be reused."""

        if generation != self._generation or failed_dealer is not self._dealer:
            return
        failed_dealer.close(linger=0)
        self._dealer = self._open_dealer_socket()

    def _configure_sub_socket(self, sub: zmq.asyncio.Socket) -> None:
        """Apply reconnect and TCP keepalive options to the SUB socket."""

        sub.setsockopt(zmq.RECONNECT_IVL, self._config.zmq_reconnect_ivl_ms)
        sub.setsockopt(
            zmq.RECONNECT_IVL_MAX,
            self._config.zmq_reconnect_ivl_max_ms,
        )
        if self._config.zmq_tcp_keepalive:
            sub.setsockopt(zmq.TCP_KEEPALIVE, 1)
            sub.setsockopt(
                zmq.TCP_KEEPALIVE_IDLE,
                self._config.zmq_tcp_keepalive_idle_s,
            )
            sub.setsockopt(
                zmq.TCP_KEEPALIVE_INTVL,
                self._config.zmq_tcp_keepalive_intvl_s,
            )
            sub.setsockopt(
                zmq.TCP_KEEPALIVE_CNT,
                self._config.zmq_tcp_keepalive_cnt,
            )
        else:
            sub.setsockopt(zmq.TCP_KEEPALIVE, 0)

    async def subscribe(self) -> AsyncGenerator[EngineEventBatch, None]:
        """Yield replayed batches before the live batch that exposed a gap."""

        try:
            while True:
                received = await self._recv_live_message()
                if received is None:
                    continue
                seq, payload, trace_id = received
                generation = self._generation

                if seq > self._last_seq + 1:
                    missed = seq - self._last_seq - 1
                    replay_trace_id = generate_trace_id()
                    diagnostic_tags = self._zmq_gap_diagnostics.snapshot_and_reset()
                    logger.warning(
                        "kv event sequence gap detected, triggering replay",
                        step="zmq_replay",
                        tags={
                            "last_seq": self._last_seq,
                            "current_seq": seq,
                            "missed": missed,
                            "trace_id": replay_trace_id,
                            **diagnostic_tags,
                        },
                    )
                    replay_telemetry = BatchTelemetry(pipeline="incremental")
                    # Gap counters live on the telemetry: they belong to this
                    # main-path event so a downstream dashboard can attribute
                    # loss to the batch that exposed it.
                    replay_telemetry.count("zmq_sequence_gap_count", 1)
                    replay_telemetry.count("zmq_missed_message_count", missed)
                    replay_batches = await self._replay_missing_batches(
                        seq, generation, replay_trace_id
                    )
                    if generation != self._generation:
                        continue
                    if replay_batches is not None:
                        # The gap is resolved up to the live message: every
                        # missing batch still buffered was forwarded and the
                        # rest is permanently lost. Advance now so a live
                        # decode failure below cannot replay the same range.
                        self._last_seq = seq - 1
                    if replay_batches:
                        replay_telemetry.mark("replay_fetch")
                        yield EngineEventBatch(
                            replay_batches,
                            replay_telemetry,
                            trace_id=replay_trace_id,
                        )
                    elif replay_batches == []:
                        logger.warning(
                            "replay returned no batches, "
                            "publisher buffer may have been pruned",
                            step="zmq_replay",
                            tags={
                                "gap_start_seq": self._last_seq + 1,
                                "trace_id": replay_trace_id,
                            },
                        )

                telemetry = BatchTelemetry(pipeline="incremental")
                batch = self._msgpack_helper.decode(
                    payload,
                    step="zmq_subscribe",
                    tags={"seq": seq, "trace_id": trace_id},
                )
                if batch is None:
                    continue
                telemetry.mark("decode")
                if logger.is_debug_enabled():
                    logger.debug(
                        "decoded vLLM KV event batch",
                        step="zmq_subscribe",
                        tags={
                            "seq": seq,
                            "trace_id": trace_id,
                            **_summarize_batch(batch),
                        },
                    )
                if generation != self._generation:
                    continue
                self._last_seq = seq
                yield EngineEventBatch([batch], telemetry, trace_id=trace_id)
        finally:
            self._sub.close(linger=0)
            self._dealer.close(linger=0)

    async def _recv_live_message(self) -> tuple[int, bytes, str] | None:
        generation = self._generation
        sub = self._sub
        try:
            frames = await sub.recv_multipart()
        except Exception as exc:
            self._recv_failure_count += 1
            now_s = time.monotonic()
            if (
                self._last_recv_warn_s is None
                or now_s - self._last_recv_warn_s >= _RECV_FAILURE_WARN_INTERVAL_S
            ):
                self._last_recv_warn_s = now_s
                logger.warning(
                    "failed to receive kv event message",
                    step="zmq_subscribe",
                    tags={
                        "error": exc.__class__.__name__,
                        "message": str(exc),
                        "consecutive_failures": self._recv_failure_count,
                    },
                )
            # Back off briefly so a persistently failing socket cannot spin the
            # subscribe loop into a hot loop.
            await asyncio.sleep(_RECV_FAILURE_BACKOFF_S)
            return None
        if self._recv_failure_count > 0:
            logger.info(
                "kv event message receive recovered",
                step="zmq_subscribe",
                tags={"previous_consecutive_failures": self._recv_failure_count},
            )
            self._recv_failure_count = 0
            self._last_recv_warn_s = None
        if generation != self._generation:
            return None
        # Background throughput counter: emitted synchronously since it is a
        # per-message lifecycle event unrelated to any specific batch.
        report_zmq_message()
        self._zmq_gap_diagnostics.record_message(
            message_bytes=sum(len(frame) for frame in frames),
            queue_nonempty_after_receive=self._sub_queue_is_readable(sub),
        )
        if len(frames) != 3:
            logger.warning(
                "dropping malformed kv event message frames",
                step="zmq_subscribe",
                tags={"frame_count": len(frames)},
            )
            return None
        topic, seq_bytes, payload = frames
        if len(seq_bytes) != 8:
            logger.warning(
                "dropping kv event message with invalid sequence frame",
                step="zmq_subscribe",
                tags={"seq_frame_len": len(seq_bytes)},
            )
            return None
        seq = int.from_bytes(seq_bytes, "big")
        trace_id = generate_trace_id()
        if logger.is_debug_enabled():
            logger.debug(
                "received vLLM ZMQ live message",
                step="zmq_subscribe",
                tags={
                    "topic": topic.decode(errors="replace"),
                    "seq": seq,
                    "payload_bytes": len(payload),
                    "trace_id": trace_id,
                },
            )
        return seq, payload, trace_id

    def _zmq_queue_state(self) -> dict[str, bool | int]:
        """Return the stable libzmq queue signals available for the SUB socket."""

        receive_high_water_mark = self._sub.getsockopt(zmq.RCVHWM)
        if not isinstance(receive_high_water_mark, int):
            raise TypeError("ZMQ RCVHWM must be an integer")
        return {
            "zmq_sub_readable": self._sub_queue_is_readable(self._sub),
            "zmq_sub_rcvhwm": receive_high_water_mark,
            "zmq_exact_queue_depth_available": False,
        }

    @staticmethod
    def _sub_queue_is_readable(sub: zmq.asyncio.Socket) -> bool:
        """Whether libzmq currently has at least one message ready to receive."""

        try:
            events = sub.getsockopt(zmq.EVENTS)
            return isinstance(events, int) and bool(events & zmq.POLLIN)
        except Exception:
            return False

    async def _replay_missing_batches(
        self, current_seq: int, generation: int, trace_id: str
    ) -> list[KVEventBatch] | None:
        gap_start_seq = self._last_seq + 1
        if logger.is_debug_enabled():
            logger.debug(
                "requesting vLLM ZMQ replay",
                step="zmq_replay",
                tags={
                    "gap_start_seq": gap_start_seq,
                    "current_seq": current_seq,
                    "trace_id": trace_id,
                },
            )
        dealer = self._dealer
        try:
            async with asyncio.timeout(self._config.zmq_replay_timeout_s):
                await dealer.send_multipart([b"", gap_start_seq.to_bytes(8, "big")])
        except TimeoutError:
            self._replace_replay_socket(dealer, generation)
            self._log_replay_unavailable(
                gap_start_seq,
                current_seq,
                error="TimeoutError",
                message="replay timed out",
                trace_id=trace_id,
            )
            return None
        except Exception as exc:
            self._replace_replay_socket(dealer, generation)
            self._log_replay_unavailable(
                gap_start_seq,
                current_seq,
                error=exc.__class__.__name__,
                message=str(exc),
                trace_id=trace_id,
            )
            return None
        if generation != self._generation:
            return None

        replay_batches: list[KVEventBatch] = []
        while True:
            try:
                async with asyncio.timeout(self._config.zmq_replay_timeout_s):
                    frames = await dealer.recv_multipart()
            except TimeoutError:
                self._replace_replay_socket(dealer, generation)
                self._log_replay_unavailable(
                    gap_start_seq,
                    current_seq,
                    error="TimeoutError",
                    message="replay timed out",
                    trace_id=trace_id,
                )
                return None
            except Exception as exc:
                self._replace_replay_socket(dealer, generation)
                self._log_replay_unavailable(
                    gap_start_seq,
                    current_seq,
                    error=exc.__class__.__name__,
                    message=str(exc),
                    trace_id=trace_id,
                )
                return None
            if generation != self._generation:
                return None
            if len(frames) == 4:
                # vLLM PR #45177 added topic to replay replies. ROUTER
                # consumes client_id, so DEALER receives
                # (delimiter, topic, seq_bytes, payload).
                _, _, replay_seq_bytes, replay_payload = frames
            elif len(frames) == 3:
                # Before vLLM PR #45177, replay replies had no topic and
                # DEALER received (delimiter, seq_bytes, payload).
                _, replay_seq_bytes, replay_payload = frames
            else:
                self._replace_replay_socket(dealer, generation)
                logger.warning(
                    "dropping malformed kv event replay frames",
                    step="zmq_replay",
                    tags={
                        "gap_start_seq": gap_start_seq,
                        "current_seq": current_seq,
                        "frame_count": len(frames),
                        "trace_id": trace_id,
                    },
                )
                return None
            if replay_seq_bytes == _END_SEQ:
                if logger.is_debug_enabled():
                    logger.debug(
                        "completed vLLM ZMQ replay",
                        step="zmq_replay",
                        tags={
                            "gap_start_seq": gap_start_seq,
                            "current_seq": current_seq,
                            "batch_count": len(replay_batches),
                            "trace_id": trace_id,
                        },
                    )
                return replay_batches
            replay_seq = int.from_bytes(replay_seq_bytes, "big")
            # The replay endpoint returns every buffered batch with
            # seq >= gap_start_seq, including the live batch that exposed the
            # gap and anything newer; those arrive (or arrived) live, so only
            # the missing range is forwarded here. Keep draining to END.
            if replay_seq < gap_start_seq or replay_seq >= current_seq:
                continue
            batch = self._msgpack_helper.decode(
                replay_payload,
                step="zmq_replay",
                tags={
                    "gap_start_seq": gap_start_seq,
                    "current_seq": current_seq,
                    "trace_id": trace_id,
                },
            )
            if batch is None:
                self._replace_replay_socket(dealer, generation)
                return None
            if logger.is_debug_enabled():
                logger.debug(
                    "decoded vLLM KV event batch",
                    step="zmq_replay",
                    tags={
                        "gap_start_seq": gap_start_seq,
                        "current_seq": current_seq,
                        "replay_seq": replay_seq,
                        "trace_id": trace_id,
                        **_summarize_batch(batch),
                    },
                )
            replay_batches.append(batch)

    def _log_replay_unavailable(
        self,
        gap_start_seq: int,
        current_seq: int,
        *,
        error: str,
        message: str,
        trace_id: str,
    ) -> None:
        logger.warning(
            "kv event replay unavailable; sequence gap remains; forwarding live batch",
            step="zmq_replay",
            tags={
                "gap_start_seq": gap_start_seq,
                "current_seq": current_seq,
                "error": error,
                "message": message,
                "trace_id": trace_id,
            },
        )

    async def close(self) -> None:
        """Close the current ZMQ sockets once."""

        if self._closed:
            return
        self._closed = True
        self._sub.close(linger=0)
        self._dealer.close(linger=0)

    async def reset_generation_state(self) -> None:
        """Invalidate in-flight waits and recreate the ZMQ sockets."""

        self._generation += 1
        self._last_seq = -1
        self._sub.close(linger=0)
        self._dealer.close(linger=0)
        self._sub = self._open_sub_socket()
        self._dealer = self._open_dealer_socket()
        logger.info(
            "reset vLLM ZMQ generation state; sockets recreated",
            step="zmq_connect",
            tags={
                "generation": self._generation,
                "pub_endpoint": self._endpoint.zmq_pub_endpoint,
                "replay_endpoint": self._endpoint.zmq_replay_endpoint,
            },
        )
