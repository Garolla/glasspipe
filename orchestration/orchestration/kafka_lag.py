"""Consumer lag between Redpanda and the landing service's committed offsets.

Complements parquet_backlog_reasonable (landing -> raw_nrt): that check
covers files landed but not yet loaded into ClickHouse. This one covers
the earlier hop -- bridge -> Redpanda -> landing -- which is where a real,
measurable queue can build up if landing falls behind. The SSE -> bridge
hop has no equivalent backlog number (it's a push stream); its health is
bridge_heartbeat_sensor's job instead.

Reads landing's own consumer group (KAFKA_GROUP_ID, default
"glasspipe-landing") rather than creating a new one: querying committed
offsets and watermark offsets doesn't require joining a group, so this
never affects landing's own consumption.
"""

from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient


@dataclass(frozen=True)
class ConsumerLag:
    total_lag: int
    partition_lags: dict[int, int]


def get_consumer_lag(*, bootstrap_servers: str, group_id: str, topic: str, timeout: float = 10.0) -> ConsumerLag:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    group_request = ConsumerGroupTopicPartitions(group_id)
    future = admin.list_consumer_group_offsets([group_request])[group_id]
    committed = {tp.partition: tp.offset for tp in future.result(timeout=timeout).topic_partitions if tp.topic == topic}

    # Own group id, never joined (no subscribe/poll) -- just used to open a
    # client that can ask the broker for watermark offsets.
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"{group_id}-lag-check"})
    try:
        partition_lags: dict[int, int] = {}
        for partition, committed_offset in committed.items():
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
            # No committed offset yet (-1) means landing hasn't consumed
            # anything on this partition -- the whole log is backlog.
            offset = committed_offset if committed_offset >= 0 else low
            partition_lags[partition] = high - offset
    finally:
        consumer.close()

    return ConsumerLag(total_lag=sum(partition_lags.values()), partition_lags=partition_lags)
