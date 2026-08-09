"""Durable Last-Event-ID checkpoint.

Written atomically (write to a temp file, then rename) so a crash mid-write
never leaves a half-written checkpoint that would corrupt the resume point.
"""

from __future__ import annotations

import os
from pathlib import Path


class Checkpoint:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> str | None:
        if not self.path.exists():
            return None
        value = self.path.read_text().strip()
        return value or None

    def write(self, last_event_id: str) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(last_event_id)
        os.replace(tmp_path, self.path)
