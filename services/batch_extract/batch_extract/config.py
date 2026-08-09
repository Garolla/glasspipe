from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BatchExtractConfig:
    project: str
    access: str
    api_base_url: str
    user_agent: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str
    poll_interval_hours: float
    lookback_days: int

    @classmethod
    def from_env(cls) -> "BatchExtractConfig":
        return cls(
            project=os.environ.get("PAGEVIEWS_PROJECT", "en.wikipedia"),
            access=os.environ.get("PAGEVIEWS_ACCESS", "all-access"),
            api_base_url=os.environ.get(
                "PAGEVIEWS_API_BASE_URL", "https://wikimedia.org/api/rest_v1"
            ),
            user_agent=os.environ.get(
                "BATCH_EXTRACT_USER_AGENT", "glasspipe-batch-extract/0.1 (+https://github.com/)"
            ),
            clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            clickhouse_user=os.environ.get("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "raw"),
            poll_interval_hours=float(os.environ.get("POLL_INTERVAL_HOURS", "24")),
            lookback_days=int(os.environ.get("LOOKBACK_DAYS", "1")),
        )
