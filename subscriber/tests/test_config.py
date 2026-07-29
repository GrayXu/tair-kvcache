from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from subscriber.config import SubscriberConfig, build_parser
from subscriber.kvcm.enum import KvcmStorageType

_KVCM_ARGS = ["--kvcm-base-url", "http://kvcm-test:8080"]


def test_defaults() -> None:
    config = SubscriberConfig()
    assert config.zmq_pub_endpoint == "tcp://localhost:5557"
    assert config.zmq_replay_endpoint == "tcp://localhost:5558"
    assert config.zmq_topic == ""
    assert config.engine_type == "vllm"


def test_incremental_kv_event_pipeline_is_enabled_by_default() -> None:
    config = SubscriberConfig()

    assert config.incremental_kv_event_pipeline_enabled is True


def test_incremental_kv_event_pipeline_cli_disable() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [*_KVCM_ARGS, "--no-incremental-kv-event-pipeline-enabled"]
    )

    config = SubscriberConfig.from_args(args)

    assert config.incremental_kv_event_pipeline_enabled is False


def test_incremental_kv_event_pipeline_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("incremental_kv_event_pipeline_enabled: false\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])

    config = SubscriberConfig.from_args(args)

    assert config.incremental_kv_event_pipeline_enabled is False


def test_engine_type_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--engine-type", "sglang"])
    config = SubscriberConfig.from_args(args)
    assert config.engine_type == "sglang"


def test_storage_type_defaults_to_event_report() -> None:
    config = SubscriberConfig()

    assert config.kvcm_storage_type == KvcmStorageType.ST_EVENT_REPORT_L1P5


def test_storage_type_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--kvcm-storage-type", "ST_CUSTOM"])
    config = SubscriberConfig.from_args(args)
    assert config.kvcm_storage_type == "ST_CUSTOM"


def test_blank_kvcm_base_url_is_rejected() -> None:
    parser = build_parser()
    args = parser.parse_args([])
    with pytest.raises(ValueError, match="kvcm_base_url is required"):
        SubscriberConfig.from_args(args)


def test_kv_event_queue_maxsize_defaults_to_4096() -> None:
    config = SubscriberConfig()

    assert config.kv_event_queue_maxsize == 4096


def test_kv_event_queue_maxsize_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--kv-event-queue-maxsize", "7"])

    config = SubscriberConfig.from_args(args)

    assert config.kv_event_queue_maxsize == 7


def test_kv_event_queue_maxsize_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("kv_event_queue_maxsize: 9\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])

    config = SubscriberConfig.from_args(args)

    assert config.kv_event_queue_maxsize == 9


def test_kv_event_queue_maxsize_must_be_positive() -> None:
    config = SubscriberConfig(kv_event_queue_maxsize=0)

    with pytest.raises(ValueError, match="kv_event_queue_maxsize must be >= 1"):
        config.validate()


def test_kv_event_merge_max_report_events_defaults_to_16() -> None:
    config = SubscriberConfig()

    assert config.kv_event_merge_max_report_events == 16


def test_kv_event_merge_max_report_events_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--kv-event-merge-max-report-events", "7"])

    config = SubscriberConfig.from_args(args)

    assert config.kv_event_merge_max_report_events == 7


def test_kv_event_merge_max_report_events_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("kv_event_merge_max_report_events: 9\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])

    config = SubscriberConfig.from_args(args)

    assert config.kv_event_merge_max_report_events == 9


def test_kv_event_merge_max_report_events_must_be_positive() -> None:
    config = SubscriberConfig(kv_event_merge_max_report_events=0)

    with pytest.raises(
        ValueError,
        match="kv_event_merge_max_report_events must be >= 1",
    ):
        config.validate()


def test_kv_event_merge_max_queue_items_defaults_to_4() -> None:
    config = SubscriberConfig()

    assert config.kv_event_merge_max_queue_items == 4


def test_kv_event_merge_max_queue_items_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--kv-event-merge-max-queue-items", "2"])

    config = SubscriberConfig.from_args(args)

    assert config.kv_event_merge_max_queue_items == 2


def test_kv_event_merge_max_queue_items_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("kv_event_merge_max_queue_items: 3\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])

    config = SubscriberConfig.from_args(args)

    assert config.kv_event_merge_max_queue_items == 3


def test_kv_event_merge_max_queue_items_must_be_positive() -> None:
    config = SubscriberConfig(kv_event_merge_max_queue_items=0)

    with pytest.raises(
        ValueError,
        match="kv_event_merge_max_queue_items must be >= 1",
    ):
        config.validate()


def test_kvcm_runtime_config_defaults() -> None:
    config = SubscriberConfig()
    assert config.kvcm_heartbeat_interval_s == 5.0


def test_kvcm_runtime_config_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            *_KVCM_ARGS,
            "--kvcm-heartbeat-interval-s",
            "2.5",
        ]
    )
    config = SubscriberConfig.from_args(args)
    assert config.kvcm_heartbeat_interval_s == 2.5


def test_kvcm_runtime_config_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        textwrap.dedent(
            """\
            kvcm_heartbeat_interval_s: 3.0
            """
        )
    )
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])
    config = SubscriberConfig.from_args(args)
    assert config.kvcm_heartbeat_interval_s == 3.0


def test_non_positive_kvcm_heartbeat_interval_raises() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--kvcm-heartbeat-interval-s", "0"])

    with pytest.raises(ValueError, match="^kvcm_heartbeat_interval_s must be > 0$"):
        SubscriberConfig.from_args(args)


def test_host_port_defaults_to_8080() -> None:
    assert SubscriberConfig().host_port == 8080


def test_host_port_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--host-port", "9001"])
    config = SubscriberConfig.from_args(args)
    assert config.host_port == 9001


def test_host_port_out_of_range_raises() -> None:
    parser = build_parser()
    for value in ("0", "65536"):
        args = parser.parse_args([*_KVCM_ARGS, "--host-port", value])
        with pytest.raises(ValueError, match=r"^host_port must be in \[1, 65535\]$"):
            SubscriberConfig.from_args(args)


def test_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
            zmq_pub_endpoint: "tcp://remote:5557"
        """)
    )
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])
    config = SubscriberConfig.from_args(args)
    assert config.zmq_pub_endpoint == "tcp://remote:5557"
    assert config.zmq_replay_endpoint == "tcp://localhost:5558"


def test_cli_overrides_yaml(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("engine_type: vllm\n")
    parser = build_parser()
    args = parser.parse_args(
        [*_KVCM_ARGS, "--config", str(yaml_file), "--engine-type", "sglang"]
    )
    config = SubscriberConfig.from_args(args)
    assert config.engine_type == "sglang"


def test_health_config_defaults() -> None:
    config = SubscriberConfig()
    assert config.engine_health_interval_s == 5.0
    assert config.engine_kvcache_worker_status_timeout_ms == 1000.0
    assert config.engine_health_failure_threshold == 3


def test_health_config_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            *_KVCM_ARGS,
            "--engine-health-interval-s",
            "0.25",
            "--engine-kvcache-worker-status-timeout-ms",
            "100",
            "--engine-health-failure-threshold",
            "5",
        ]
    )
    config = SubscriberConfig.from_args(args)
    assert config.engine_health_interval_s == 0.25
    assert config.engine_kvcache_worker_status_timeout_ms == 100.0
    assert config.engine_health_failure_threshold == 5


def test_health_config_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        textwrap.dedent(
            """\
            engine_health_interval_s: 2.5
            engine_kvcache_worker_status_timeout_ms: 750
            engine_health_failure_threshold: 7
            """
        )
    )
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])
    config = SubscriberConfig.from_args(args)
    assert config.engine_health_interval_s == 2.5
    assert config.engine_kvcache_worker_status_timeout_ms == 750
    assert config.engine_health_failure_threshold == 7


def test_legacy_engine_health_timeout_cli_argument_is_rejected() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([*_KVCM_ARGS, "--engine-health-timeout-s", "0.1"])


def test_zmq_transport_config_defaults() -> None:
    config = SubscriberConfig()
    assert config.zmq_reconnect_ivl_ms == 100
    assert config.zmq_reconnect_ivl_max_ms == 5000
    assert config.zmq_replay_timeout_s == 1.0
    assert config.zmq_tcp_keepalive is True
    assert config.zmq_tcp_keepalive_idle_s == 30
    assert config.zmq_tcp_keepalive_intvl_s == 5
    assert config.zmq_tcp_keepalive_cnt == 3


def test_zmq_replay_timeout_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--zmq-replay-timeout-s", "0.25"])

    config = SubscriberConfig.from_args(args)

    assert config.zmq_replay_timeout_s == 0.25


def test_zmq_replay_timeout_cli_overrides_yaml(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("zmq_replay_timeout_s: 0.75\n")
    parser = build_parser()
    args = parser.parse_args(
        [
            *_KVCM_ARGS,
            "--config",
            str(yaml_file),
            "--zmq-replay-timeout-s",
            "0.25",
        ]
    )

    config = SubscriberConfig.from_args(args)

    assert config.zmq_replay_timeout_s == 0.25


def test_zmq_replay_timeout_must_be_positive() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080", zmq_replay_timeout_s=0
    )

    with pytest.raises(ValueError, match="zmq_replay_timeout_s must be > 0"):
        config.validate()


def test_data_parallel_config_defaults() -> None:
    config = SubscriberConfig()
    assert config.data_parallel_size == 1
    assert config.dp_endpoints[0].rank == 0
    assert config.dp_endpoints[0].zmq_pub_endpoint == "tcp://localhost:5557"
    assert config.dp_endpoints[0].zmq_replay_endpoint == "tcp://localhost:5558"
    assert config.dp_endpoints[0].zmq_topic == ""


def test_data_parallel_size_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [*_KVCM_ARGS, "--data-parallel-size", "2", "--no-subscriber-health-enabled"]
    )
    config = SubscriberConfig.from_args(args)
    assert config.data_parallel_size == 2


def test_data_parallel_size_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("data_parallel_size: 3\nsubscriber_health_enabled: false\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])
    config = SubscriberConfig.from_args(args)
    assert config.data_parallel_size == 3


def test_dp_endpoints_expand_tcp_ports_by_rank() -> None:
    config = SubscriberConfig(data_parallel_size=3)
    assert [endpoint.rank for endpoint in config.dp_endpoints] == [0, 1, 2]
    assert [endpoint.zmq_pub_endpoint for endpoint in config.dp_endpoints] == [
        "tcp://localhost:5557",
        "tcp://localhost:5558",
        "tcp://localhost:5559",
    ]
    assert [endpoint.zmq_replay_endpoint for endpoint in config.dp_endpoints] == [
        "tcp://localhost:5558",
        "tcp://localhost:5559",
        "tcp://localhost:5560",
    ]


def test_non_positive_data_parallel_size_raises() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--data-parallel-size", "0"])

    with pytest.raises(ValueError, match="^data_parallel_size must be >= 1$"):
        SubscriberConfig.from_args(args)


def test_multi_dp_requires_tcp_endpoint() -> None:
    config = SubscriberConfig(
        data_parallel_size=2,
        zmq_pub_endpoint="ipc://kv-events",
        zmq_replay_endpoint="tcp://localhost:5558",
    )

    with pytest.raises(
        ValueError,
        match="^zmq_pub_endpoint must be a tcp://host:port endpoint for multi-DP$",
    ):
        config.validate()


def test_multi_dp_requires_endpoint_port() -> None:
    config = SubscriberConfig(
        data_parallel_size=2,
        zmq_pub_endpoint="tcp://localhost",
        zmq_replay_endpoint="tcp://localhost:5558",
    )

    with pytest.raises(
        ValueError,
        match="^zmq_pub_endpoint must include a TCP port for multi-DP$",
    ):
        config.validate()


def test_zmq_transport_config_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            *_KVCM_ARGS,
            "--zmq-reconnect-ivl-ms",
            "200",
            "--zmq-reconnect-ivl-max-ms",
            "3000",
            "--no-zmq-tcp-keepalive",
            "--zmq-tcp-keepalive-idle-s",
            "10",
            "--zmq-tcp-keepalive-intvl-s",
            "2",
            "--zmq-tcp-keepalive-cnt",
            "4",
        ]
    )
    config = SubscriberConfig.from_args(args)
    assert config.zmq_reconnect_ivl_ms == 200
    assert config.zmq_reconnect_ivl_max_ms == 3000
    assert config.zmq_tcp_keepalive is False
    assert config.zmq_tcp_keepalive_idle_s == 10
    assert config.zmq_tcp_keepalive_intvl_s == 2
    assert config.zmq_tcp_keepalive_cnt == 4


def test_log_level_cli_override() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--log-level", "debug"])
    config = SubscriberConfig.from_args(args)
    assert config.log_level == "debug"


def test_log_level_yaml_loading(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("log_level: 'warning'\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])
    config = SubscriberConfig.from_args(args)
    assert config.log_level == "warning"


def test_invalid_log_level_yaml_raises(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("log_level: 'verbose'\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])

    with pytest.raises(
        ValueError,
        match="^log_level must be one of: critical, debug, error, info, warning$",
    ):
        SubscriberConfig.from_args(args)


def test_non_string_log_level_yaml_raises(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("log_level: 123\n")
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])

    with pytest.raises(
        ValueError,
        match="^log_level must be one of: critical, debug, error, info, warning$",
    ):
        SubscriberConfig.from_args(args)


def test_non_positive_health_failure_threshold_raises() -> None:
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--engine-health-failure-threshold", "0"])

    with pytest.raises(
        ValueError,
        match="^engine_health_failure_threshold must be >= 1$",
    ):
        SubscriberConfig.from_args(args)


# --- Subscriber health integration config ---


def test_subscriber_health_config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DSV_CONTROL_PORT", raising=False)
    config = SubscriberConfig()
    assert config.subscriber_health_enabled is True
    assert config.subscriber_state_url == "http://127.0.0.1:8601/subscriber_state"
    assert config.subscriber_heartbeat_interval_s == 3.0
    assert config.subscriber_state_request_timeout_s == 2.0
    assert config.subscriber_shutdown_report_timeout_s == 2.0


def test_removed_dashserving_readiness_and_ttl_fields_are_not_in_config() -> None:
    config = SubscriberConfig()

    assert not hasattr(config, "subscriber_state_ttl_s")
    assert not hasattr(config, "dashserving_readiness_url")
    assert not hasattr(config, "dashserving_readiness_interval_s")


def test_subscriber_health_config_cli_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DSV_CONTROL_PORT", "9999")
    parser = build_parser()
    args = parser.parse_args(
        [
            *_KVCM_ARGS,
            "--subscriber-health-enabled",
            "--subscriber-heartbeat-interval-s",
            "5.0",
            "--subscriber-state-request-timeout-s",
            "3.0",
            "--subscriber-shutdown-report-timeout-s",
            "4.0",
        ]
    )
    config = SubscriberConfig.from_args(args)
    assert config.subscriber_health_enabled is True
    assert config.subscriber_state_url == "http://127.0.0.1:9999/subscriber_state"
    assert config.subscriber_heartbeat_interval_s == 5.0
    assert config.subscriber_state_request_timeout_s == 3.0
    assert config.subscriber_shutdown_report_timeout_s == 4.0


def test_subscriber_health_config_yaml_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DSV_CONTROL_PORT", "7777")
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        textwrap.dedent(
            """\
            subscriber_health_enabled: true
            subscriber_heartbeat_interval_s: 4.0
            subscriber_state_request_timeout_s: 1.5
            subscriber_shutdown_report_timeout_s: 3.5
            """
        )
    )
    parser = build_parser()
    args = parser.parse_args([*_KVCM_ARGS, "--config", str(yaml_file)])
    config = SubscriberConfig.from_args(args)
    assert config.subscriber_health_enabled is True
    assert config.subscriber_state_url == "http://127.0.0.1:7777/subscriber_state"
    assert config.subscriber_heartbeat_interval_s == 4.0
    assert config.subscriber_state_request_timeout_s == 1.5
    assert config.subscriber_shutdown_report_timeout_s == 3.5


def test_subscriber_health_cli_overrides_yaml(tmp_path: Path) -> None:
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        textwrap.dedent(
            """\
            subscriber_health_enabled: true
            subscriber_heartbeat_interval_s: 4.0
            """
        )
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            *_KVCM_ARGS,
            "--config",
            str(yaml_file),
            "--subscriber-heartbeat-interval-s",
            "2.0",
        ]
    )
    config = SubscriberConfig.from_args(args)
    assert config.subscriber_health_enabled is True
    assert config.subscriber_heartbeat_interval_s == 2.0


def test_subscriber_health_cli_disable_overrides_yaml_true(tmp_path: Path) -> None:
    """--no-subscriber-health-enabled must override YAML `true`.

    Guards against regressing back to action="store_true", which has no
    --no- form and would make a YAML-enabled flag impossible to disable
    from the CLI.
    """
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("subscriber_health_enabled: true\n")
    parser = build_parser()
    args = parser.parse_args(
        [*_KVCM_ARGS, "--config", str(yaml_file), "--no-subscriber-health-enabled"]
    )
    config = SubscriberConfig.from_args(args)
    assert config.subscriber_health_enabled is False


def test_subscriber_health_disabled_skips_validation() -> None:
    """When subscriber_health_enabled=False, no health validation runs."""
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080",
        subscriber_health_enabled=False,
        subscriber_heartbeat_interval_s=-1.0,
        data_parallel_size=4,
    )
    # Should not raise — validation only applies when enabled
    config.validate()


def test_subscriber_health_enabled_positive_intervals() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080",
        subscriber_health_enabled=True,
        subscriber_heartbeat_interval_s=0,
    )
    with pytest.raises(ValueError, match="subscriber_heartbeat_interval_s must be > 0"):
        config.validate()


def test_subscriber_health_enabled_positive_request_timeout() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080",
        subscriber_health_enabled=True,
        subscriber_state_request_timeout_s=0,
    )
    with pytest.raises(
        ValueError, match="subscriber_state_request_timeout_s must be > 0"
    ):
        config.validate()


def test_subscriber_health_enabled_positive_shutdown_timeout() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080",
        subscriber_health_enabled=True,
        subscriber_shutdown_report_timeout_s=-0.5,
    )
    with pytest.raises(
        ValueError, match="subscriber_shutdown_report_timeout_s must be > 0"
    ):
        config.validate()


def test_subscriber_health_data_parallel_size_must_be_1() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080",
        subscriber_health_enabled=True,
        data_parallel_size=2,
    )
    with pytest.raises(
        ValueError,
        match="data_parallel_size must be 1 when subscriber_health_enabled is true",
    ):
        config.validate()


def test_subscriber_health_valid_config_passes() -> None:
    """A fully valid enabled config passes validation."""
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080",
        subscriber_health_enabled=True,
        subscriber_heartbeat_interval_s=3.0,
        subscriber_state_request_timeout_s=2.0,
        subscriber_shutdown_report_timeout_s=2.0,
        data_parallel_size=1,
    )
    config.validate()  # Should not raise


def test_engine_snapshot_full_sync_interval_must_be_positive() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080", engine_snapshot_full_sync_interval_s=0
    )

    with pytest.raises(
        ValueError, match="engine_snapshot_full_sync_interval_s must be > 0"
    ):
        config.validate()


def test_engine_snapshot_full_sync_interval_negative_raises() -> None:
    config = SubscriberConfig(
        kvcm_base_url="http://kvcm-test:8080", engine_snapshot_full_sync_interval_s=-5.0
    )

    with pytest.raises(
        ValueError, match="engine_snapshot_full_sync_interval_s must be > 0"
    ):
        config.validate()
