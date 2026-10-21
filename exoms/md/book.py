"""Market-by-order book built from ElectronX market data.

ElectronX publishes every resting order individually, keyed by MDEntryID(278), which equals
the exchange OrderID(37). That lets the strategy leave its own orders out of the mid.
"""
from __future__ import annotations

from typing import Collection

BID, OFFER = "0", "1"                  # MDEntryType(269)
NEW, CHANGE, DELETE = "0", "1", "2"    # MDUpdateAction(279)


class OrderBook:
    __slots__ = ("symbol", "orders", "stale", "last_update_ns")

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.orders: dict[str, tuple[str, float, float]] = {}  # MDEntryID -> (side, price, size)
        self.stale = True  # True until a snapshot arrives, and again after a disconnect
        self.last_update_ns = 0

    def clear(self) -> None:
        self.orders.clear()

    def apply(self, action: str, entry_id: str, side: str, price: float | None, size: float) -> None:
        if action == DELETE:
            if entry_id in self.orders:
                del self.orders[entry_id]
            elif price is not None:  # no MDEntryID: fall back to removing one order at that price
                for oid, (s, p, _) in self.orders.items():
                    if p == price and (not side or s == side):
                        del self.orders[oid]
                        break
            return
        old = self.orders.get(entry_id)
        if old is not None:
            side = side or old[0]
            price = old[1] if price is None else price
        if side not in (BID, OFFER) or price is None:
            return  # trades, volume, settlement prices etc. are not resting orders
        self.orders[entry_id] = (side, price, size)

    def _prices(self, side: str, exclude: Collection[str]) -> list[float]:
        return [p for oid, (s, p, q) in self.orders.items() if s == side and q > 0 and oid not in exclude]

    def best_bid(self, exclude: Collection[str] = ()) -> float | None:
        prices = self._prices(BID, exclude)
        return max(prices) if prices else None

    def best_ask(self, exclude: Collection[str] = ()) -> float | None:
        prices = self._prices(OFFER, exclude)
        return min(prices) if prices else None

    def mid(self, exclude: Collection[str] = ()) -> float | None:
        bid, ask = self.best_bid(exclude), self.best_ask(exclude)
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2

    def depth(self, side: str) -> list[tuple[float, float]]:
        """Aggregated (price, size) levels, best first."""
        levels: dict[float, float] = {}
        for s, p, q in self.orders.values():
            if s == side:
                levels[p] = levels.get(p, 0) + q
        return sorted(levels.items(), reverse=side == BID)

    def __repr__(self) -> str:
        state = "STALE " if self.stale else ""
        return f"<{state}{self.symbol} {self.best_bid()} / {self.best_ask()}>"
