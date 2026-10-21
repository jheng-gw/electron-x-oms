"""Tick-to-send latency: time from receiving a market data message to writing the order to the socket."""
from __future__ import annotations


class LatencyStats:
    def __init__(self) -> None:
        self._samples_ns: list[int] = []
        self.total = 0

    def record(self, elapsed_ns: int) -> None:
        self._samples_ns.append(elapsed_ns)
        self.total += 1

    def drain(self) -> dict[str, float] | None:
        """Return p50/p99/max in microseconds for samples since the last drain."""
        samples, self._samples_ns = sorted(self._samples_ns), []
        if not samples:
            return None
        pick = lambda q: samples[min(len(samples) - 1, int(q * len(samples)))] / 1000
        return {"count": len(samples), "p50_us": pick(0.50), "p99_us": pick(0.99), "max_us": samples[-1] / 1000}
