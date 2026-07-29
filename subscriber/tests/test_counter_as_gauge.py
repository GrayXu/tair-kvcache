"""Tests for _base._dashlog_counter gauge-accumulation behavior.

_dashlog_counter accumulates deltas per (name, tags) key and reports the
running per-interval total via _dashlog_gauge (which reaches EAS
realtime_metrics). Accumulators reset after _RESET_INTERVAL_S seconds so
the reported gauge value represents a per-interval delta.
"""

from __future__ import annotations

import time

import pytest

from subscriber.metrics import _base


@pytest.fixture(autouse=True)
def _reset_accumulators():
    """Reset module-level accumulator state between tests."""
    _base._counter_accumulators.clear()
    _base._counter_last_reset_s = 0.0
    yield
    _base._counter_accumulators.clear()
    _base._counter_last_reset_s = 0.0


class TestCounterAsGauge:
    def test_single_call_reports_delta_as_gauge(self, mocker) -> None:
        gauge = mocker.patch("subscriber.metrics._base._dashlog_gauge")

        _base._dashlog_counter("zmq_received_message_count", 1)

        gauge.assert_called_once_with("zmq_received_message_count", 1.0, tags=None)

    def test_accumulates_within_interval(self, mocker) -> None:
        gauge = mocker.patch("subscriber.metrics._base._dashlog_gauge")
        now = time.monotonic()
        mocker.patch("time.monotonic", return_value=now)

        _base._dashlog_counter("zmq_received_message_count", 1)
        _base._dashlog_counter("zmq_received_message_count", 1)
        _base._dashlog_counter("zmq_received_message_count", 3)

        assert gauge.call_count == 3
        assert gauge.call_args_list[-1] == mocker.call(
            "zmq_received_message_count", 5.0, tags=None
        )

    def test_separate_keys_per_tags(self, mocker) -> None:
        gauge = mocker.patch("subscriber.metrics._base._dashlog_gauge")
        now = time.monotonic()
        mocker.patch("time.monotonic", return_value=now)

        _base._dashlog_counter("heartbeat_count", 1, tags={"status": "success"})
        _base._dashlog_counter("heartbeat_count", 1, tags={"status": "failure"})
        _base._dashlog_counter("heartbeat_count", 1, tags={"status": "success"})

        assert gauge.call_args_list[-1] == mocker.call(
            "heartbeat_count", 2.0, tags={"status": "success"}
        )

    def test_resets_after_interval(self, mocker) -> None:
        gauge = mocker.patch("subscriber.metrics._base._dashlog_gauge")
        now = time.monotonic()
        mocker.patch("time.monotonic", return_value=now)

        _base._dashlog_counter("msg_count", 5)

        mocker.patch("time.monotonic", return_value=now + _base._RESET_INTERVAL_S + 1)
        _base._dashlog_counter("msg_count", 2)

        assert gauge.call_args_list[-1] == mocker.call("msg_count", 2.0, tags=None)

    def test_reset_emits_zero_for_idle_keys(self, mocker) -> None:
        """A counter that stops incrementing must be zeroed at the next
        window reset, otherwise dashboards report its last accumulated
        value as a live rate forever."""

        gauge = mocker.patch("subscriber.metrics._base._dashlog_gauge")
        now = time.monotonic()
        mocker.patch("time.monotonic", return_value=now)

        _base._dashlog_counter("active_count", 5)
        _base._dashlog_counter("idle_count", 7, tags={"endpoint": "a"})

        mocker.patch("time.monotonic", return_value=now + _base._RESET_INTERVAL_S + 1)
        _base._dashlog_counter("active_count", 1)

        assert (
            mocker.call("idle_count", 0.0, tags={"endpoint": "a"})
            in gauge.call_args_list
        )
        assert gauge.call_args_list[-1] == mocker.call("active_count", 1.0, tags=None)
        # The key that fired again must not be zeroed in the same reset.
        assert mocker.call("active_count", 0.0, tags=None) not in gauge.call_args_list

    def test_does_not_call_dashlog_counter(self, mocker) -> None:
        mock_dashlog = mocker.patch("subscriber.metrics._base._dashlog")

        _base._dashlog_counter("test_metric", 1)

        mock_dashlog.Counter.assert_not_called()

    def test_swallows_gauge_errors(self, mocker) -> None:
        mocker.patch(
            "subscriber.metrics._base._dashlog_gauge",
            side_effect=RuntimeError("gauge failed"),
        )

        _base._dashlog_counter("test_metric", 1)
