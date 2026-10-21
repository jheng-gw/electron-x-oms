"""Sliding-window message throttle with headroom reserved for cancels."""
from __future__ import annotations

import time
from collections import deque
from typing import Callable


class Throttle:
    """Allows at most `limit` messages in any rolling 1-second window.

    New orders and replaces may only use `limit - reserve` slots, so cancels always have room
    to pull quotes even when the strategy is saturating the session.
    """

    def __init__(self, limit: int, reserve_fraction: float, clock: Callable[[], float] = time.monotonic):
        self.limit = max(1, limit)
        self.reserve = int(self.limit * reserve_fraction)
        self._clock = clock
        self._sent: deque[float] = deque()

    @classmethod
    def from_exchange_limit(cls, rate_limit: int, safety: float, reserve_fraction: float) -> "Throttle":
        return cls(int(rate_limit * safety), reserve_fraction)

    def _prune(self, now: float) -> None:
        while self._sent and now - self._sent[0] >= 1.0:
            self._sent.popleft()

    def try_acquire(self, priority: bool = False) -> bool:
        now = self._clock()
        self._prune(now)
        cap = self.limit if priority else self.limit - self.reserve
        if len(self._sent) >= cap:
            return False
        self._sent.append(now)
        return True

    @property
    def in_window(self) -> int:
        self._prune(self._clock())
        return len(self._sent)
