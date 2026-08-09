from __future__ import annotations

from confluent_kafka import Consumer, TopicPartition


def build_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


class OffsetTracker:
    """Tracks the highest offset seen per partition since the last commit.

    Committing only at flush time -- not per message -- is what makes the
    at-least-once guarantee line up with "the Parquet file is durably
    written": if the process dies between a message being consumed and the
    next flush, the offset was never committed, so that message is
    re-delivered and re-written rather than silently lost. Downstream
    dedup on event_id is what makes the resulting duplicates harmless.
    """

    def __init__(self):
        self._max_offset: dict[tuple[str, int], int] = {}

    def record(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        if offset > self._max_offset.get(key, -1):
            self._max_offset[key] = offset

    def pending_offsets(self) -> list[TopicPartition]:
        return [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in self._max_offset.items()
        ]

    def clear(self) -> None:
        self._max_offset.clear()
