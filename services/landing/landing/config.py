from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class LandingConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    parquet_dir: str
    flush_max_records: int
    flush_max_seconds: float
    poll_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "LandingConfig":
        return cls(
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "wikipedia.recentchange"),
            kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "glasspipe-landing"),
            parquet_dir=os.environ.get("GLASSPIPE_PARQUET_DIR", "/data/parquet"),
            flush_max_records=int(os.environ.get("FLUSH_MAX_RECORDS", "500")),
            flush_max_seconds=float(os.environ.get("FLUSH_MAX_SECONDS", "30")),
            poll_timeout_seconds=float(os.environ.get("POLL_TIMEOUT_SECONDS", "1.0")),
        )
