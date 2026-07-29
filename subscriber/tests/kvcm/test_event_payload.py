from __future__ import annotations

from subscriber.kvcm.event_payload import (
    ReportEventSourceCounts,
    build_block_snapshot_event,
    expand_report_events,
    merge_snapshot_blocks,
    report_event_count,
    source_counts,
)
from subscriber.types import (
    AllBlocksCleared,
    BlockRemoved,
    BlockSnapshot,
    BlockSnapshotItem,
    BlockStored,
    KVEventBatch,
)


def _medium_mapper(medium: str | None) -> str:
    return {"GPU": "hbm", "CPU": "mem"}.get(medium, "")


def _block_specs(medium: str, group_idx: int | None) -> list[dict[str, str]]:
    return [
        {
            "name": f"spec-{group_idx}",
            "uri": f"vllm://127.0.0.1:9000/{medium}",
        }
    ]


def _block_spec_names(group_idx: int | None) -> list[str]:
    return [f"spec-{group_idx}"]


def test_event_payload_expands_ordered_events_and_counts_report_events() -> None:
    batches = [
        KVEventBatch(
            ts=1.0,
            events=[
                BlockStored(
                    block_hashes=[11, 12],
                    parent_block_hash=None,
                    token_ids=[1, 2],
                    block_size=2,
                    lora_id=None,
                    medium="GPU",
                    lora_name=None,
                    group_idx=0,
                ),
                BlockRemoved(block_hashes=[13], medium="CPU", group_idx=1),
            ],
        ),
        KVEventBatch(ts=2.0, events=[AllBlocksCleared()]),
    ]

    events = expand_report_events(
        batches,
        medium_mapper=_medium_mapper,
        block_specs=_block_specs,
        block_spec_names=_block_spec_names,
    )
    assert events == [
        {
            "event_type": "EVENT_BLOCK_ADD",
            "block_add": {
                "block_key": "11",
                "medium": "hbm",
                "specs": [
                    {
                        "name": "spec-0",
                        "uri": "vllm://127.0.0.1:9000/hbm",
                    }
                ],
            },
        },
        {
            "event_type": "EVENT_BLOCK_ADD",
            "block_add": {
                "block_key": "12",
                "medium": "hbm",
                "specs": [
                    {
                        "name": "spec-0",
                        "uri": "vllm://127.0.0.1:9000/hbm",
                    }
                ],
            },
        },
        {
            "event_type": "EVENT_BLOCK_DELETE",
            "block_delete": {
                "block_key": "13",
                "medium": "mem",
                "spec_names": ["spec-1"],
            },
        },
        {"event_type": "EVENT_HOST_DOWN", "host_down": {}},
    ]
    assert source_counts(batches) == ReportEventSourceCounts(
        batch_count=2, event_count=3
    )
    assert report_event_count(batches) == 4


def test_build_block_snapshot_event_builds_single_event_with_all_blocks() -> None:
    batches = [
        KVEventBatch(
            ts=1.0,
            events=[
                BlockSnapshot(
                    medium="GPU",
                    block_size=16,
                    items=[
                        BlockSnapshotItem(block_hash=101, group_idx=0),
                        BlockSnapshotItem(block_hash=102, group_idx=1),
                    ],
                    snapshot_version=7,
                )
            ],
        )
    ]

    result = build_block_snapshot_event(
        batches,
        medium_mapper=_medium_mapper,
        block_specs=_block_specs,
    )
    assert result is not None
    assert result.event == {
        "event_type": "EVENT_BLOCK_SNAPSHOT",
        "block_snapshot": {
            "blocks": [
                {
                    "block_key": "101",
                    "medium": "hbm",
                    "specs": [{"name": "spec-0", "uri": "vllm://127.0.0.1:9000/hbm"}],
                },
                {
                    "block_key": "102",
                    "medium": "hbm",
                    "specs": [{"name": "spec-1", "uri": "vllm://127.0.0.1:9000/hbm"}],
                },
            ]
        },
    }
    assert result.merge_ms >= 0


def test_build_block_snapshot_event_returns_none_without_snapshots() -> None:
    batches = [
        KVEventBatch(
            ts=1.0,
            events=[
                BlockStored(
                    block_hashes=[11],
                    parent_block_hash=None,
                    token_ids=[1],
                    block_size=2,
                    lora_id=None,
                    medium="GPU",
                    lora_name=None,
                    group_idx=0,
                )
            ],
        )
    ]

    result = build_block_snapshot_event(
        batches,
        medium_mapper=_medium_mapper,
        block_specs=_block_specs,
    )
    assert result is None


def test_build_block_snapshot_event_concatenates_multiple_snapshots_in_order() -> None:
    batches = [
        KVEventBatch(
            ts=1.0,
            events=[
                BlockSnapshot(
                    medium="GPU",
                    block_size=16,
                    items=[BlockSnapshotItem(block_hash=1, group_idx=0)],
                ),
                BlockSnapshot(
                    medium="CPU",
                    block_size=16,
                    items=[BlockSnapshotItem(block_hash=2, group_idx=0)],
                ),
            ],
        )
    ]

    result = build_block_snapshot_event(
        batches,
        medium_mapper=_medium_mapper,
        block_specs=_block_specs,
    )
    assert result is not None
    assert result.event == {
        "event_type": "EVENT_BLOCK_SNAPSHOT",
        "block_snapshot": {
            "blocks": [
                {
                    "block_key": "1",
                    "medium": "hbm",
                    "specs": [{"name": "spec-0", "uri": "vllm://127.0.0.1:9000/hbm"}],
                },
                {
                    "block_key": "2",
                    "medium": "mem",
                    "specs": [{"name": "spec-0", "uri": "vllm://127.0.0.1:9000/mem"}],
                },
            ]
        },
    }


def test_merge_snapshot_blocks_merges_same_key_medium_different_specs() -> None:
    """Blocks sharing (block_key, medium) but with different spec names merge."""
    blocks = [
        {
            "block_key": "100",
            "medium": "hbm",
            "specs": [{"name": "spec-0", "uri": "vllm://host/hbm"}],
        },
        {
            "block_key": "100",
            "medium": "hbm",
            "specs": [{"name": "spec-1", "uri": "vllm://host/hbm"}],
        },
        {
            "block_key": "100",
            "medium": "hbm",
            "specs": [{"name": "spec-2", "uri": "vllm://host/hbm"}],
        },
    ]
    merged = merge_snapshot_blocks(blocks)
    assert merged == [
        {
            "block_key": "100",
            "medium": "hbm",
            "specs": [
                {"name": "spec-0", "uri": "vllm://host/hbm"},
                {"name": "spec-1", "uri": "vllm://host/hbm"},
                {"name": "spec-2", "uri": "vllm://host/hbm"},
            ],
        }
    ]


def test_merge_snapshot_blocks_no_merge_for_different_keys() -> None:
    """Blocks with different block_key are not merged."""
    blocks = [
        {
            "block_key": "1",
            "medium": "hbm",
            "specs": [{"name": "spec-0", "uri": "vllm://host/hbm"}],
        },
        {
            "block_key": "2",
            "medium": "hbm",
            "specs": [{"name": "spec-0", "uri": "vllm://host/hbm"}],
        },
    ]
    merged = merge_snapshot_blocks(blocks)
    assert len(merged) == 2
    assert merged[0]["block_key"] == "1"
    assert merged[1]["block_key"] == "2"


def test_merge_snapshot_blocks_no_merge_for_different_mediums() -> None:
    """Blocks with same key but different medium are not merged."""
    blocks = [
        {
            "block_key": "1",
            "medium": "hbm",
            "specs": [{"name": "spec-0", "uri": "vllm://host/hbm"}],
        },
        {
            "block_key": "1",
            "medium": "mem",
            "specs": [{"name": "spec-0", "uri": "vllm://host/mem"}],
        },
    ]
    merged = merge_snapshot_blocks(blocks)
    assert len(merged) == 2
    assert merged[0]["medium"] == "hbm"
    assert merged[1]["medium"] == "mem"


def test_merge_snapshot_blocks_preserves_order_of_first_occurrence() -> None:
    """Merged blocks appear in order of their first occurrence."""
    blocks = [
        {
            "block_key": "10",
            "medium": "hbm",
            "specs": [{"name": "spec-0", "uri": "u1"}],
        },
        {
            "block_key": "20",
            "medium": "hbm",
            "specs": [{"name": "spec-0", "uri": "u2"}],
        },
        {
            "block_key": "10",
            "medium": "hbm",
            "specs": [{"name": "spec-1", "uri": "u3"}],
        },
    ]
    merged = merge_snapshot_blocks(blocks)
    assert len(merged) == 2
    assert merged[0]["block_key"] == "10"
    assert merged[0]["specs"] == [
        {"name": "spec-0", "uri": "u1"},
        {"name": "spec-1", "uri": "u3"},
    ]
    assert merged[1]["block_key"] == "20"


def test_merge_snapshot_blocks_empty_input() -> None:
    """Empty input returns empty output."""
    assert merge_snapshot_blocks([]) == []
