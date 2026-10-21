"""Instrument reference data from SecurityList [y]: tick size, lot size and price limits."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Instrument:
    symbol: str
    tick_size: float | None = None   # MinPriceIncrement(969)
    lot_size: int = 1                # MinTradeVol(562)
    low_limit: float | None = None   # LowLimitPrice(1148)
    high_limit: float | None = None  # HighLimitPrice(1149)
    state: str = ""                  # TradingSessionID(336) seen on market data, e.g. OPEN
    listed_open: bool | None = None  # returned by a SecurityList filtered on TradingSessionID(336)=OPEN

    @property
    def is_open(self) -> bool | None:
        """True/False once known; False if either market data or the filtered SecurityList says not OPEN.

        An empty book carries no market data entries, so its state only comes from the SecurityList."""
        if self.listed_open is False or (self.state and self.state != "OPEN"):
            return False
        return True if self.state == "OPEN" or self.listed_open else None

    @classmethod
    def from_security_list(cls, entry: dict[int, str]) -> "Instrument":
        def num(tag: int) -> float | None:
            return float(entry[tag]) if entry.get(tag) else None
        return cls(symbol=entry[55], tick_size=num(969), lot_size=int(num(562) or 1),
                   low_limit=num(1148), high_limit=num(1149))

    def on_tick(self, price: float) -> bool:
        if not self.tick_size:
            return True
        ticks = price / self.tick_size
        return abs(ticks - round(ticks)) < 1e-6


def price_decimals(tick: float) -> int:
    return max(0, -Decimal(str(tick)).normalize().as_tuple().exponent)
