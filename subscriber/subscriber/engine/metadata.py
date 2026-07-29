"""Authoritative KV cache group metadata success/error model.

The subscriber fetches KV cache group topology from the engine before it may
register with kvcm. The fetch is authoritative: it either returns a definitive
:class:`KvCacheDescriptor` (an empty group tuple is a valid topology) or it
raises a typed :class:`MetadataFetchError`. Returning ``None`` is deliberately
not part of the contract because a single nullable return conflated valid empty
topology, retry exhaustion, protocol failure, and unsupported adapters.
"""

from __future__ import annotations

from dataclasses import dataclass

from subscriber.types import KvCacheGroupSpec


@dataclass(frozen=True)
class KvCacheDescriptor:
    """Immutable result of an authoritative metadata fetch.

    ``groups`` is ordered as returned by the engine; each spec carries its own
    ``group_idx`` (do not infer the index from tuple position). An empty tuple
    is a valid success (e.g. a single-group / attention-free model) and must
    never be represented as ``None``.

    ``use_eagle_pop`` indicates whether the engine pops the last matched
    FullAttention block during prefix cache lookup (MTP/Eagle correctness).
    ``mamba_cache_mode`` is the engine's mamba state caching strategy
    ("none" | "all" | "light" | "light_flex").
    """

    groups: tuple[KvCacheGroupSpec, ...]
    use_eagle_pop: bool = False
    mamba_cache_mode: str = "none"


class MetadataFetchError(RuntimeError):
    """Base error for an authoritative metadata fetch."""


class MetadataTemporarilyUnavailable(MetadataFetchError):
    """Retryable transport failure or explicit UNAVAILABLE response.

    Raised only after the bounded per-attempt retry policy is exhausted. The
    startup supervisor treats this as a transient condition: the process stays
    in ``starting`` until the deployment startup/liveness policy replaces it.
    """


class MetadataProtocolError(MetadataFetchError):
    """Non-retryable response or malformed metadata.

    Covers invalid status codes, wrong field shape, duplicate group ids, and
    invalid sizes. This is a fatal configuration/protocol error that causes
    startup ``failed``.
    """


class MetadataUnsupportedError(MetadataFetchError):
    """Adapter has no authoritative metadata implementation."""
