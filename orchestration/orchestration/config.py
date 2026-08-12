from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class OrchestrationConfig:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    parquet_dir: str
    bridge_heartbeat_path: str
    sqlmesh_project_dir: str
    parquet_retention_days: int
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str

    @classmethod
    def from_env(cls) -> "OrchestrationConfig":
        return cls(
            clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            clickhouse_user=os.environ.get("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            parquet_dir=os.environ.get("GLASSPIPE_PARQUET_DIR", "/data/parquet"),
            bridge_heartbeat_path=os.environ.get("HEARTBEAT_PATH", "/data/bridge/heartbeat"),
            sqlmesh_project_dir=os.environ.get("SQLMESH_PROJECT_DIR", "/opt/transform"),
            parquet_retention_days=int(os.environ.get("PARQUET_RETENTION_DAYS", "1")),
            # Same env var names/defaults as services/bridge and services/landing --
            # this reads landing's consumer group, doesn't create its own.
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "wikipedia.recentchange"),
            kafka_group_id=os.environ.get("KAFKA_GROUP_ID", "glasspipe-landing"),
        )
