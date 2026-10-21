"""Persisted FIX sequence numbers, so a restart resumes the session instead of resetting it.

ElectronX keeps Order Entry and Drop Copy sequence numbers for a whole week and resets them
every Sunday at 06:15 UTC (spec: "FIX Sequence Scheduling").
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("fix.store")


def fix_week_start(now: datetime | None = None) -> datetime:
    """Most recent Sunday 06:15 UTC at or before `now`."""
    now = now or datetime.now(timezone.utc)
    days_since_sunday = (now.weekday() - 6) % 7
    boundary = (now - timedelta(days=days_since_sunday)).replace(hour=6, minute=15, second=0, microsecond=0)
    if boundary > now:
        boundary -= timedelta(days=7)
    return boundary


class SeqStore:
    """Sequence numbers in memory, written behind by a background thread.

    A synchronous file write per message costs milliseconds on Windows, which would dominate
    tick-to-send latency. The writer thread always persists the latest values; a hard crash
    can lose at most the last few increments, which the session recovers from via resend.
    """

    def __init__(self, path: Path | None = None):
        self._path = path
        self._sender = 1
        self._target = 1
        self._week = fix_week_start().isoformat()
        self._dirty = threading.Event()
        self._lock = threading.Lock()
        self._closed = False
        if path is not None:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                if data.get("week") == self._week:
                    self._sender, self._target = int(data["sender"]), int(data["target"])
            threading.Thread(target=self._writer, name=f"seq-{path.stem}", daemon=True).start()

    @property
    def next_sender(self) -> int:
        return self._sender

    @property
    def next_target(self) -> int:
        return self._target

    def consume_sender(self) -> int:
        seq = self._sender
        self._sender += 1
        self._dirty.set()
        return seq

    def set_sender(self, seq: int) -> None:
        self._sender = seq
        self._dirty.set()

    def incr_target(self) -> None:
        self._target += 1
        self._dirty.set()

    def set_target(self, seq: int) -> None:
        self._target = seq
        self._dirty.set()

    def reset(self) -> None:
        self._sender = self._target = 1
        self._week = fix_week_start().isoformat()
        self._dirty.set()

    def roll_week(self) -> bool:
        """Reset if the weekly boundary has passed since the numbers were last used."""
        if self._week != fix_week_start().isoformat():
            self.reset()
            return True
        return False

    def flush(self) -> None:
        if self._path is not None:
            self._write()

    def close(self) -> None:
        """Stop the writer thread after a final write."""
        self._closed = True
        self._dirty.set()
        self.flush()

    def _writer(self) -> None:
        while not self._closed:
            self._dirty.wait()
            self._dirty.clear()
            if not self._closed:
                self._write()

    def _write(self) -> None:
        with self._lock:  # flush() and the writer thread must not replace the file concurrently (Windows)
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self._path.with_suffix(".tmp")
                tmp.write_text(json.dumps({"sender": self._sender, "target": self._target, "week": self._week}),
                               encoding="utf-8")
                for attempt in range(20):
                    try:
                        os.replace(tmp, self._path)
                        return
                    except PermissionError:  # Windows: target briefly open elsewhere (reader, antivirus)
                        time.sleep(0.005 * (attempt + 1))
            except OSError as exc:
                log.error("could not persist sequence numbers to %s: %s", self._path, exc)
                return
            log.error("could not persist sequence numbers to %s", self._path)
