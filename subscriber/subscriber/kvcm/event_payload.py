from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from subscriber.kvcm.enum import KvcmReportEventType
from subscriber.types import (
    AllBlocksCleared,
    BlockRemoved,
    BlockSnapshot,
    BlockStored,
    KVEventBatch,
)


@dataclass(frozen=True)
class ReportEventSourceCounts:
    """Number of subscriber batches and events that produced a request."""

    batch_count: int
    event_count: int


@dataclass(frozen=True)
class SnapshotBuildResult:
    """Result of building a snapshot event, including merge timing."""

    event: dict[str, object]
    merge_ms: float


def expand_report_events(
    batches: list[KVEventBatch],
    *,
    medium_mapper: Callable[[str | None], str],
    block_specs: Callable[[str, int | None], list[dict[str, str]]],
    block_spec_names: Callable[[int | None], list[str]],
) -> list[dict[str, object]]:
    """Expand subscriber batches into ordered KVCM report events."""

    events: list[dict[str, object]] = []
    for batch in batches:
        for event in batch.events:
            if isinstance(event, BlockStored):
                medium = medium_mapper(event.medium)
                specs = block_specs(medium, event.group_idx)
                for block_hash in event.block_hashes:
                    events.append(
                        {
                            "event_type": KvcmReportEventType.BLOCK_ADD,
                            "block_add": {
                                "block_key": str(block_hash),
                                "medium": medium,
                                "specs": specs,
                            },
                        }
                    )
            elif isinstance(event, BlockRemoved):
                medium = medium_mapper(event.medium)
                spec_names = block_spec_names(event.group_idx)
                for block_hash in event.block_hashes:
                    events.append(
                        {
                            "event_type": KvcmReportEventType.BLOCK_DELETE,
                            "block_delete": {
                                "block_key": str(block_hash),
                                "medium": medium,
                                "spec_names": spec_names,
                            },
                        }
                    )
            elif isinstance(event, AllBlocksCleared):
                events.append(
                    {"event_type": KvcmReportEventType.HOST_DOWN, "host_down": {}}
                )
    return events


def merge_snapshot_blocks(
    blocks: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Merge snapshot blocks sharing the same ``(block_key, medium)``.

    Hybrid engines (e.g. mamba + attention) emit the same block hash across
    multiple groups, each carrying a different spec name. KVCM rejects
    duplicate ``(medium, block_key)`` pairs, so we merge their specs into a
    single block entry. Order of first occurrence is preserved.

    TODO: This is a temporary single-threaded implementation. Based on the
    ``snapshot_merge_ms`` metric, evaluate whether to optimize with
    multi-threaded map-reduce merging or move the merge into KVCM (C++).
    """

    if not blocks:
        return []

    index: dict[tuple[str, str], int] = {}
    merged: list[dict[str, object]] = []
    for block in blocks:
        key = (str(block["block_key"]), str(block["medium"]))
        specs = cast(list[dict[str, str]], block["specs"])
        pos = index.get(key)
        if pos is not None:
            existing_specs = cast(list[dict[str, str]], merged[pos]["specs"])
            existing_specs.extend(specs)
        else:
            index[key] = len(merged)
            merged.append(
                {
                    "block_key": block["block_key"],
                    "medium": block["medium"],
                    "specs": list(specs),
                }
            )
    return merged


def collect_snapshot_blocks(
    batches: list[KVEventBatch],
    *,
    medium_mapper: Callable[[str | None], str],
    block_specs: Callable[[str, int | None], list[dict[str, str]]],
) -> list[dict[str, object]]:
    """Flatten all :class:`BlockSnapshot` items into raw block dicts.

    Each block carries the mapped medium and the location specs for its
    ``group_idx``. No merging is performed — blocks sharing the same
    ``(block_key, medium)`` appear as separate entries.
    """

    blocks: list[dict[str, object]] = []
    for batch in batches:
        for event in batch.events:
            if not isinstance(event, BlockSnapshot):
                continue
            medium = medium_mapper(event.medium)
            for item in event.items:
                blocks.append(
                    {
                        "block_key": str(item.block_hash),
                        "medium": medium,
                        "specs": block_specs(medium, item.group_idx),
                    }
                )
    return blocks


def build_merged_snapshot_blocks(
    batches: list[KVEventBatch],
    *,
    medium_mapper: Callable[[str | None], str],
    block_specs: Callable[[str, int | None], list[dict[str, str]]],
) -> list[dict[str, object]]:
    """Single-pass fused collect + merge for snapshot blocks.

    Replaces the two-pass ``collect_snapshot_blocks`` + ``merge_snapshot_blocks``
    pipeline. Merges blocks sharing ``(block_key, medium)`` inline and
    deduplicates specs by ``group_idx`` — the same ``(block_hash, group_idx)``
    pair appearing multiple times (high-concurrency duplicate) produces only one
    spec entry. Order of first occurrence is preserved.
    """

    if not batches:
        return []

    index: dict[tuple[str, str], int] = {}
    seen_groups: dict[int, set[int]] = {}
    merged: list[dict[str, object]] = []

    for batch in batches:
        for event in batch.events:
            if not isinstance(event, BlockSnapshot):
                continue
            medium = medium_mapper(event.medium)
            for item in event.items:
                block_key = str(item.block_hash)
                key = (block_key, medium)
                pos = index.get(key)
                if pos is None:
                    idx = len(merged)
                    index[key] = idx
                    seen_groups[idx] = {item.group_idx}
                    merged.append(
                        {
                            "block_key": block_key,
                            "medium": medium,
                            "specs": list(block_specs(medium, item.group_idx)),
                        }
                    )
                elif item.group_idx not in seen_groups[pos]:
                    seen_groups[pos].add(item.group_idx)
                    specs = cast(list[dict[str, str]], merged[pos]["specs"])
                    specs.extend(block_specs(medium, item.group_idx))

    return merged


def build_block_snapshot_event(
    batches: list[KVEventBatch],
    *,
    medium_mapper: Callable[[str | None], str],
    block_specs: Callable[[str, int | None], list[dict[str, str]]],
) -> SnapshotBuildResult | None:
    """Build a single ``EVENT_BLOCK_SNAPSHOT`` report event from snapshot batches.

    Convenience wrapper: collects raw blocks, merges duplicates, and wraps
    the result. Returns ``None`` when no snapshot is present.
    """

    blocks = collect_snapshot_blocks(
        batches, medium_mapper=medium_mapper, block_specs=block_specs
    )
    if not blocks:
        return None
    merge_started_at = time.monotonic()
    merged = merge_snapshot_blocks(blocks)
    merge_ms = (time.monotonic() - merge_started_at) * 1000
    return SnapshotBuildResult(
        event={
            "event_type": KvcmReportEventType.BLOCK_SNAPSHOT,
            "block_snapshot": {"blocks": merged},
        },
        merge_ms=merge_ms,
    )


def report_event_count(batches: list[KVEventBatch]) -> int:
    """Count KVCM report events without expanding payloads or serializing JSON."""

    count = 0
    for batch in batches:
        for event in batch.events:
            if isinstance(event, (BlockStored, BlockRemoved)):
                count += len(event.block_hashes)
            elif isinstance(event, AllBlocksCleared):
                count += 1
    return count


def source_counts(batches: list[KVEventBatch]) -> ReportEventSourceCounts:
    """Count source batches and unexpanded subscriber events."""

    return ReportEventSourceCounts(
        batch_count=len(batches),
        event_count=sum(len(batch.events) for batch in batches),
    )
