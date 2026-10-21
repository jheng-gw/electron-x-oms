"""Append-only JSONL event journal, written on a background thread to keep disk I/O off the event loop."""
from __future__ import annotations

import json
import queue
import threading
import time
from pathlib import Path
from typing import Any, Iterator


def journal_path(var_dir: Path, name: str = "journal") -> Path:
    """One journal per FIX week, matching the life of the Order Entry/Drop Copy sequence numbers."""
    from .fix.store import fix_week_start
    return var_dir / f"{name}-{fix_week_start():%Y%m%d}.jsonl"


class Journal:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._queue: queue.SimpleQueue = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name="journal", daemon=True)
        self._thread.start()

    def write(self, kind: str, **fields: Any) -> None:
        self._queue.put(("line", {"ts": time.time(), "kind": kind, **fields}))

    def snapshot(self, path: Path, data: dict) -> None:
        """Atomically replace a small JSON file (e.g. the positions file the reconciler reads)."""
        self._queue.put(("file", path, data))

    def close(self) -> None:
        self._queue.put(None)
        self._thread.join(timeout=2.0)

    def _run(self) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            while (item := self._queue.get()) is not None:
                if item[0] == "line":
                    f.write(json.dumps(item[1]) + "\n")
                else:
                    _, path, data = item
                    tmp = path.with_suffix(".tmp")
                    tmp.write_text(json.dumps(data), encoding="utf-8")
                    tmp.replace(path)
                if self._queue.empty():
                    f.flush()

    @staticmethod
    def read(path: Path) -> Iterator[dict]:
        if not path.exists():
            return
        with path.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
