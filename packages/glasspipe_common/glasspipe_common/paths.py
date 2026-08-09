"""Filesystem layout conventions for the Parquet durability buffer.

Kept in one place because three components need to agree on it: the
landing service (writes), ClickHouse/SQLMesh (reads, via `file()` glob),
and the Dagster cleanup job (deletes past TTL). Changing the layout means
changing it here, not independently in each component.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


def _default_base_dir() -> Path:
    return Path(os.environ.get("GLASSPIPE_PARQUET_DIR", "/data/parquet"))


@dataclass(frozen=True)
class ParquetPaths:
    base_dir: Path

    @classmethod
    def from_env(cls) -> "ParquetPaths":
        return cls(base_dir=_default_base_dir())

    @property
    def nrt_root(self) -> Path:
        return self.base_dir / "nrt"

    @property
    def dead_letter_root(self) -> Path:
        return self.base_dir / "dead_letter" / "nrt"

    def nrt_partition_dir(self, wiki: str, event_date: date) -> Path:
        safe_wiki = wiki.replace("/", "_")
        return self.nrt_root / f"wiki={safe_wiki}" / f"dt={event_date.isoformat()}"

    def dead_letter_partition_dir(self, event_date: date) -> Path:
        return self.dead_letter_root / f"dt={event_date.isoformat()}"

    def nrt_glob_pattern(self) -> str:
        """Pattern relative to the ClickHouse user_files mount, for `file()`."""
        return "nrt/**/*.parquet"

    def iter_nrt_files(self):
        if not self.nrt_root.exists():
            return
        yield from self.nrt_root.rglob("*.parquet")

    def file_age_seconds(self, path: Path, *, now: datetime | None = None) -> float:
        now = now or datetime.now()
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        return (now - mtime).total_seconds()


__all__ = ["ParquetPaths"]
