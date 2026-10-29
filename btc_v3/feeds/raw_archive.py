from __future__ import annotations

import gzip
import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO


class RawJsonlArchive:
    """Append-only UTC-daily gzip JSONL archive for raw public feed payloads.

    Phase 5.9.1 adds an explicit archive-session/record identifier to every
    record. Normalized SQLite rows can therefore keep a tiny deterministic
    reference to the full-fidelity payload instead of duplicating that payload
    inside every row.
    """

    def __init__(
        self,
        root: Path,
        component: str,
        *,
        flush_interval_seconds: float = 1.0,
        flush_record_limit: int = 256,
    ) -> None:
        self.root = root
        self.component = component
        self.flush_interval_seconds = max(0.05, float(flush_interval_seconds))
        self.flush_record_limit = max(1, int(flush_record_limit))
        self._day: str | None = None
        self._path: Path | None = None
        self._fh: TextIO | None = None
        self._last_flush_mono = time.monotonic()
        self._pending_records = 0
        self.archive_session_id = uuid.uuid4().hex
        self._record_seq = 0
        self._segment_seq = 0

    def _ensure_open(self, received_at_us: int) -> TextIO:
        dt = datetime.fromtimestamp(received_at_us / 1_000_000, tz=UTC)
        day = dt.strftime("%Y-%m-%d")
        if self._fh is not None and self._day == day:
            return self._fh
        self.close()
        day_dir = self.root / day
        day_dir.mkdir(parents=True, exist_ok=True)
        # Rollover collectors overlap. Each writer owns exclusive segments;
        # independent gzip streams must never interleave in a shared daily file.
        self._segment_seq += 1
        path = day_dir / f"{self.component}_{self.archive_session_id}_{self._segment_seq}.jsonl.gz"
        self._fh = gzip.open(path, mode="xt", encoding="utf-8")
        self._day = day
        self._path = path
        return self._fh

    def write(self, received_at_us: int, payload: object, *, timing: dict | None = None) -> dict[str, object]:
        """Write one full-fidelity record and return a compact stable reference."""
        fh = self._ensure_open(received_at_us)
        self._record_seq += 1
        record_seq = self._record_seq
        record = {
            "received_at_us": received_at_us,
            "component": self.component,
            "archive_session_id": self.archive_session_id,
            "record_seq": record_seq,
            "payload": payload,
        }
        if timing is not None:record['timing']=timing
        fh.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
        self._pending_records += 1
        now = time.monotonic()
        if (
            self._pending_records >= self.flush_record_limit
            or now - self._last_flush_mono >= self.flush_interval_seconds
        ):
            fh.flush()
            self._pending_records = 0
            self._last_flush_mono = now

        assert self._day is not None
        assert self._path is not None
        try:
            rel_path = self._path.relative_to(self.root.parent).as_posix()
        except ValueError:
            rel_path = self._path.as_posix()
        return {
            "storage": "raw_jsonl_gzip_ref_v1",
            "component": self.component,
            "utc_day": self._day,
            "path": rel_path,
            "archive_session_id": self.archive_session_id,
            "record_seq": record_seq,
            "received_at_us": int(received_at_us),
        }

    def close(self) -> None:
        if self._fh is not None:
            try:
                self._fh.flush()
            finally:
                self._fh.close()
            self._fh = None
            self._day = None
            self._path = None
            self._pending_records = 0
            self._last_flush_mono = time.monotonic()

    def __enter__(self) -> "RawJsonlArchive":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
