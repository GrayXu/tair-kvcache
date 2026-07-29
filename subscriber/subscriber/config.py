from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, fields
from dataclasses import field as dataclass_field
from urllib.parse import SplitResult, urlsplit, urlunsplit

import yaml

from subscriber.kvcm.enum import KvcmQueryType, KvcmStorageType

_LOG_LEVELS = ("debug", "info", "warning", "error", "critical")


@dataclass(frozen=True)
class DpEndpoint:
    rank: int
    zmq_pub_endpoint: str
    zmq_replay_endpoint: str
    zmq_topic: str = ""


@dataclass
class SubscriberConfig:
    """Runtime configuration for the subscriber process."""

    # ZMQ
    zmq_pub_endpoint: str = "tcp://localhost:5557"
    zmq_replay_endpoint: str = "tcp://localhost:5558"
    zmq_topic: str = ""
    zmq_replay_timeout_s: float = 1.0
    zmq_reconnect_ivl_ms: int = 100
    zmq_reconnect_ivl_max_ms: int = 5000
    zmq_tcp_keepalive: bool = True
    zmq_tcp_keepalive_idle_s: int = 30
    zmq_tcp_keepalive_intvl_s: int = 5
    zmq_tcp_keepalive_cnt: int = 3

    # Data parallelism
    data_parallel_size: int = 1

    kv_event_queue_maxsize: int = 4096
    kv_event_merge_max_report_events: int = 16
    kv_event_merge_max_queue_items: int = 4
    snapshot_queue_maxsize: int = 16

    # Engine
    engine_type: str = "vllm"

    # Engine liveness (gRPC GetWorkerStatus polling).
    engine_health_interval_s: float = 5.0
    engine_health_failure_threshold: int = 3

    # Hybrid adapter: full snapshot reconciliation interval
    engine_snapshot_full_sync_interval_s: float = 30.0
    engine_kvcache_snapshot_timeout_ms: float = 5000.0
    engine_kvcache_worker_status_timeout_ms: float = 1000.0
    engine_kvcache_descriptor_timeout_ms: float = 1000.0
    engine_kvcache_descriptor_max_retries: int = 5

    # KV cache descriptor gRPC fetch (after engine is confirmed healthy).
    engine_grpc_endpoint: str = "127.0.0.1:18002"

    # Incremental pipeline enable/disable.
    incremental_kv_event_pipeline_enabled: bool = True

    # Worker identity port advertised to KVCM as host_ip_port. Must match the
    # engine endpoint port that FlexLB discovers via Spectrum.
    host_port: int = 8080

    # kvcm SDK
    kvcm_heartbeat_interval_s: float = 5.0
    kvcm_request_timeout_s: float = 5.0
    kvcm_query_type: str = KvcmQueryType.QT_PREFIX_MATCH_WITH_MAMBA
    kvcm_storage_type: str = KvcmStorageType.ST_EVENT_REPORT_L1P5
    kvcm_base_url: str = ""
    kvcm_instance_group: str = ""

    # Dynamic polling interval (reserved for future adaptive polling).
    poll_min_interval_s: float = 0.05
    poll_max_interval_s: float = 0.2
    poll_initial_interval_s: float = 0.1
    poll_target_snapshot_size: int = 100

    # Logging
    log_level: str = "info"

    # Subscriber health integration (DashServing same-host design).
    subscriber_health_enabled: bool = True
    subscriber_heartbeat_interval_s: float = 3.0
    subscriber_state_request_timeout_s: float = 2.0
    subscriber_shutdown_report_timeout_s: float = 2.0

    # Config-file keys that matched no known field, recorded by ``from_args``
    # so the CLI entry point can warn about them after logging is initialized.
    unknown_config_keys: list[str] = dataclass_field(default_factory=list)

    @property
    def subscriber_state_url(self) -> str:
        """Derive the DashServing state URL from DSV_CONTROL_PORT."""
        port = os.environ.get("DSV_CONTROL_PORT", "8601")
        return f"http://127.0.0.1:{port}/subscriber_state"

    @property
    def dp_endpoints(self) -> list[DpEndpoint]:
        return [
            DpEndpoint(
                rank=rank,
                zmq_pub_endpoint=_offset_tcp_endpoint(
                    self.zmq_pub_endpoint,
                    rank,
                    field_name="zmq_pub_endpoint",
                ),
                zmq_replay_endpoint=_offset_tcp_endpoint(
                    self.zmq_replay_endpoint,
                    rank,
                    field_name="zmq_replay_endpoint",
                ),
                zmq_topic=self.zmq_topic,
            )
            for rank in range(self.data_parallel_size)
        ]

    @classmethod
    def add_cli_args(
        cls,
        parser: argparse.ArgumentParser,
        default: object | str | None = None,
    ) -> None:
        """Register CLI overrides without masking YAML or dataclass defaults."""

        parser.add_argument("--zmq-pub-endpoint", default=default)
        parser.add_argument("--zmq-replay-endpoint", default=default)
        parser.add_argument("--zmq-topic", default=default)
        parser.add_argument("--zmq-replay-timeout-s", type=float, default=default)
        parser.add_argument("--data-parallel-size", type=int, default=default)
        parser.add_argument("--kvcm-heartbeat-interval-s", type=float, default=default)
        parser.add_argument("--kvcm-request-timeout-s", type=float, default=default)
        parser.add_argument("--kv-event-queue-maxsize", type=int, default=default)
        parser.add_argument("--snapshot-queue-maxsize", type=int, default=default)
        parser.add_argument(
            "--kv-event-merge-max-report-events", type=int, default=default
        )
        parser.add_argument(
            "--kv-event-merge-max-queue-items", type=int, default=default
        )
        parser.add_argument("--engine-type", default=default)
        parser.add_argument(
            "--log-level",
            choices=_LOG_LEVELS,
            default=default,
        )
        parser.add_argument("--engine-health-interval-s", type=float, default=default)
        parser.add_argument(
            "--engine-health-failure-threshold", type=int, default=default
        )
        parser.add_argument(
            "--engine-kvcache-worker-status-timeout-ms", type=float, default=default
        )
        parser.add_argument("--engine-grpc-endpoint", default=default)
        parser.add_argument(
            "--engine-kvcache-descriptor-max-retries", type=int, default=default
        )
        parser.add_argument(
            "--engine-kvcache-descriptor-timeout-ms", type=float, default=default
        )
        parser.add_argument("--host-port", type=int, default=default)
        parser.add_argument("--kvcm-query-type", default=default)
        parser.add_argument("--kvcm-storage-type", default=default)
        parser.add_argument("--kvcm-base-url", default=default)
        parser.add_argument("--kvcm-instance-group", default=default)
        parser.add_argument("--poll-min-interval-s", type=float, default=default)
        parser.add_argument("--poll-max-interval-s", type=float, default=default)
        parser.add_argument("--poll-initial-interval-s", type=float, default=default)
        parser.add_argument("--poll-target-snapshot-size", type=int, default=default)
        parser.add_argument(
            "--engine-snapshot-full-sync-interval-s", type=float, default=default
        )
        parser.add_argument(
            "--engine-kvcache-snapshot-timeout-ms", type=float, default=default
        )
        parser.add_argument(
            "--incremental-kv-event-pipeline-enabled",
            action=argparse.BooleanOptionalAction,
            default=default,
        )
        parser.add_argument("--zmq-reconnect-ivl-ms", type=int, default=default)
        parser.add_argument("--zmq-reconnect-ivl-max-ms", type=int, default=default)
        parser.add_argument(
            "--zmq-tcp-keepalive",
            action=argparse.BooleanOptionalAction,
            default=default,
        )
        parser.add_argument("--zmq-tcp-keepalive-idle-s", type=int, default=default)
        parser.add_argument("--zmq-tcp-keepalive-intvl-s", type=int, default=default)
        parser.add_argument("--zmq-tcp-keepalive-cnt", type=int, default=default)
        parser.add_argument(
            "--subscriber-health-enabled",
            action=argparse.BooleanOptionalAction,
            default=default,
        )
        parser.add_argument(
            "--subscriber-heartbeat-interval-s", type=float, default=default
        )
        parser.add_argument(
            "--subscriber-state-request-timeout-s", type=float, default=default
        )
        parser.add_argument(
            "--subscriber-shutdown-report-timeout-s", type=float, default=default
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> SubscriberConfig:
        """Build config with priority: CLI > yaml file > dataclass defaults."""

        config = cls()
        field_names = {f.name for f in fields(cls)}

        if getattr(args, "config", None):
            with open(args.config, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            for key, value in data.items():
                normalized = key.replace("-", "_")
                if normalized in field_names:
                    setattr(config, normalized, value)
                else:
                    config.unknown_config_keys.append(key)

        for field in fields(cls):
            dest = field.name
            cli_val = getattr(args, dest, None)
            if cli_val is not None:
                setattr(config, field.name, cli_val)

        config.validate()
        return config

    def validate(self) -> None:
        if not isinstance(self.log_level, str):
            raise ValueError(
                f"log_level must be one of: {', '.join(sorted(_LOG_LEVELS))}"
            )
        self.log_level = self.log_level.lower()
        if self.log_level not in _LOG_LEVELS:
            raise ValueError(
                f"log_level must be one of: {', '.join(sorted(_LOG_LEVELS))}"
            )
        if self.engine_health_failure_threshold < 1:
            raise ValueError("engine_health_failure_threshold must be >= 1")
        if self.engine_kvcache_descriptor_max_retries < 1:
            raise ValueError("engine_kvcache_descriptor_max_retries must be >= 1")
        if self.data_parallel_size < 1:
            raise ValueError("data_parallel_size must be >= 1")
        if self.data_parallel_size > 1:
            max_port_offset = self.data_parallel_size - 1
            for endpoint, field_name in (
                (self.zmq_pub_endpoint, "zmq_pub_endpoint"),
                (self.zmq_replay_endpoint, "zmq_replay_endpoint"),
            ):
                _offset_tcp_endpoint(
                    endpoint,
                    max_port_offset,
                    field_name=field_name,
                )
        if self.kv_event_queue_maxsize < 1:
            raise ValueError("kv_event_queue_maxsize must be >= 1")
        if self.snapshot_queue_maxsize < 1:
            raise ValueError("snapshot_queue_maxsize must be >= 1")
        if self.kv_event_merge_max_report_events < 1:
            raise ValueError("kv_event_merge_max_report_events must be >= 1")
        if self.kv_event_merge_max_queue_items < 1:
            raise ValueError("kv_event_merge_max_queue_items must be >= 1")
        if not 1 <= self.host_port <= 65535:
            raise ValueError("host_port must be in [1, 65535]")
        if not self.kvcm_base_url.strip():
            raise ValueError("kvcm_base_url is required")
        if self.kvcm_heartbeat_interval_s <= 0:
            raise ValueError("kvcm_heartbeat_interval_s must be > 0")
        if self.kvcm_request_timeout_s <= 0:
            raise ValueError("kvcm_request_timeout_s must be > 0")
        if self.zmq_replay_timeout_s <= 0:
            raise ValueError("zmq_replay_timeout_s must be > 0")
        if self.poll_min_interval_s <= 0:
            raise ValueError("poll_min_interval_s must be > 0")
        if self.poll_max_interval_s <= self.poll_min_interval_s:
            raise ValueError("poll_max_interval_s must be > poll_min_interval_s")
        if self.poll_target_snapshot_size < 1:
            raise ValueError("poll_target_snapshot_size must be >= 1")
        if self.engine_snapshot_full_sync_interval_s <= 0:
            raise ValueError("engine_snapshot_full_sync_interval_s must be > 0")
        self.poll_initial_interval_s = max(
            self.poll_min_interval_s,
            min(self.poll_max_interval_s, self.poll_initial_interval_s),
        )

        if self.subscriber_health_enabled:
            self._validate_subscriber_health()

    def _validate_subscriber_health(self) -> None:
        """Validate subscriber health integration fields when enabled."""
        for field_name in (
            "subscriber_heartbeat_interval_s",
            "subscriber_state_request_timeout_s",
            "subscriber_shutdown_report_timeout_s",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} must be > 0")

        if self.data_parallel_size != 1:
            raise ValueError(
                "data_parallel_size must be 1 when subscriber_health_enabled is true; "
                "multi-worker/rank support is not yet implemented"
            )


def _parse_tcp_endpoint_for_multi_dp(
    endpoint: str, *, field_name: str
) -> tuple[SplitResult, int]:
    parsed = urlsplit(endpoint)
    if parsed.scheme != "tcp":
        raise ValueError(
            f"{field_name} must be a tcp://host:port endpoint for multi-DP"
        )
    try:
        port = parsed.port
    except ValueError:
        raise ValueError(f"{field_name} must include a valid TCP port") from None
    if port is None:
        raise ValueError(f"{field_name} must include a TCP port for multi-DP")
    return parsed, port


def _offset_tcp_endpoint(endpoint: str, offset: int, *, field_name: str) -> str:
    if offset == 0:
        return endpoint
    parsed, port = _parse_tcp_endpoint_for_multi_dp(endpoint, field_name=field_name)
    offset_port = port + offset
    if offset_port > 65535:
        raise ValueError(f"{field_name} port offset exceeds 65535")
    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth = f"{auth}:{parsed.password}"
        host = f"{auth}@{host}"
    netloc = f"{host}:{offset_port}"
    return urlunsplit(
        (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the subscriber command-line parser."""

    parser = argparse.ArgumentParser(
        description="tair-kvcache subscriber — vLLM KV event forwarder"
    )
    parser.add_argument(
        "--config", default=None, metavar="FILE", help="Path to YAML config file"
    )
    SubscriberConfig.add_cli_args(parser)
    return parser
