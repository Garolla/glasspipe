from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BridgeConfig:
    stream_url: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    checkpoint_path: str
    heartbeat_path: str
    user_agent: str
    reconnect_base_seconds: float
    reconnect_max_seconds: float
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_checkpoint_age_seconds: float

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        return cls(
            stream_url=os.environ.get(
                "STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
            ),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "wikipedia.recentchange"),
            checkpoint_path=os.environ.get("CHECKPOINT_PATH", "/data/bridge/checkpoint.txt"),
            heartbeat_path=os.environ.get("HEARTBEAT_PATH", "/data/bridge/heartbeat"),
            user_agent=os.environ.get("BRIDGE_USER_AGENT", "glasspipe-bridge/0.1 (+https://github.com/)"),
            reconnect_base_seconds=float(os.environ.get("RECONNECT_BASE_SECONDS", "2")),
            reconnect_max_seconds=float(os.environ.get("RECONNECT_MAX_SECONDS", "60")),
            connect_timeout_seconds=float(os.environ.get("CONNECT_TIMEOUT_SECONDS", "10")),
            read_timeout_seconds=float(os.environ.get("READ_TIMEOUT_SECONDS", "90")),
            max_checkpoint_age_seconds=float(os.environ.get("MAX_CHECKPOINT_AGE_SECONDS", "900")),
        )
