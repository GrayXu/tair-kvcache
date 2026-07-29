"""Typed errors raised when reporting KV events to KVCM.

The forwarding loop catches :class:`KvcmReportError` to drop the affected
batch and continue consuming. This is intentionally lossy so a KVCM
control-plane failure cannot backpressure or stop engine serving. KVCM
availability is not a forwarding gate input; the KVCM heartbeat loop owns
re-registration independently.
"""

from __future__ import annotations


class KvcmReportError(RuntimeError):
    """Base error for KVCM event reporting failures."""


class KvcmUnavailableError(KvcmReportError):
    """KVCM is not registered or a retryable transport failure occurred.

    Registration state is deliberately NOT changed here: the KVCM heartbeat
    loop owns reconnection and re-registration.
    """


class KvcmReportRejectedError(KvcmReportError):
    """KVCM rejected the report (non-retryable for this batch).

    A rejected/partial report is dropped and observed without changing
    registration.
    """
