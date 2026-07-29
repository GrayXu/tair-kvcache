from __future__ import annotations

import dataclasses

import pytest

from subscriber.engine.metadata import (
    KvCacheDescriptor,
    MetadataFetchError,
    MetadataProtocolError,
    MetadataTemporarilyUnavailable,
    MetadataUnsupportedError,
)
from subscriber.types import KvCacheGroupSpec


def test_kv_cache_descriptor_holds_groups_tuple() -> None:
    spec = KvCacheGroupSpec(
        group_idx=0, kind="full_attention", block_size=16, sliding_window=None
    )
    metadata = KvCacheDescriptor(groups=(spec,))
    assert metadata.groups == (spec,)


def test_kv_cache_descriptor_empty_groups_is_valid_success() -> None:
    metadata = KvCacheDescriptor(groups=())
    assert metadata.groups == ()


def test_kv_cache_descriptor_is_frozen() -> None:
    metadata = KvCacheDescriptor(groups=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        metadata.groups = ()  # type: ignore[misc]


def test_metadata_fetch_error_is_runtime_error() -> None:
    assert issubclass(MetadataFetchError, RuntimeError)


def test_metadata_error_subclasses_derive_from_base() -> None:
    assert issubclass(MetadataTemporarilyUnavailable, MetadataFetchError)
    assert issubclass(MetadataProtocolError, MetadataFetchError)
    assert issubclass(MetadataUnsupportedError, MetadataFetchError)


def test_metadata_error_subclasses_are_catchable_as_base() -> None:
    for exc_type in (
        MetadataTemporarilyUnavailable,
        MetadataProtocolError,
        MetadataUnsupportedError,
    ):
        with pytest.raises(MetadataFetchError):
            raise exc_type("boom")
