"""Pre-trade risk checks, position tracking and the kill switch."""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Iterable

from ..config import RiskConfig
from ..md.instruments import Instrument
from ..oe.orders import BUY, Order

log = logging.getLogger("risk")


class RiskManager:
    def __init__(self, limits: RiskConfig):
        self.limits = limits
        self.positions: defaultdict[str, int] = defaultdict(int)
        self.killed = False
        self.kill_reason = ""

    def kill(self, reason: str) -> None:
        if not self.killed:
            log.critical("KILL SWITCH: %s", reason)
        self.killed = True
        self.kill_reason = reason

    def on_fill(self, symbol: str, side: str, qty: int) -> None:
        self.positions[symbol] += qty if side == BUY else -qty

    def check(self, symbol: str, side: str, qty: int, price: float, open_orders: Iterable[Order],
              ref_price: float | None, instrument: Instrument | None = None,
              replacing: Order | None = None) -> str | None:
        """Return a rejection reason, or None if the order may be sent."""
        lim = self.limits
        if self.killed:
            return f"kill switch active: {self.kill_reason}"
        if qty <= 0 or qty > lim.max_order_qty:
            return f"qty {qty} outside 1..{lim.max_order_qty}"
        if not lim.min_price <= price <= lim.max_price:
            return f"price {price} outside {lim.min_price}..{lim.max_price}"
        if instrument is not None:
            # Catch what the exchange would reject: repeated rejects count against production approval.
            if instrument.state and instrument.state != "OPEN":
                return f"{symbol} is {instrument.state}"
            if not instrument.on_tick(price):
                return f"price {price} not on tick {instrument.tick_size}"
            if qty % instrument.lot_size:
                return f"qty {qty} not a multiple of lot size {instrument.lot_size}"
            low, high = instrument.low_limit, instrument.high_limit
            if (low is not None and price < low) or (high is not None and price > high):
                return f"price {price} outside instrument limits {low}..{high}"
        if ref_price and abs(price - ref_price) / ref_price > lim.price_band:
            return f"price {price} more than {lim.price_band:.0%} from mid {ref_price:.4f}"

        others = [o for o in open_orders if o.is_live and o is not replacing]
        if replacing is None and len(others) >= lim.max_open_orders:
            return f"open order limit {lim.max_open_orders} reached"

        # Worst case: every open order on this side fills, plus this one.
        same_side = sum(o.leaves_qty for o in others if o.symbol == symbol and o.side == side)
        filled = replacing.cum_qty if replacing is not None else 0
        exposure = same_side + max(qty - filled, 0)
        position = self.positions[symbol]
        worst = position + exposure if side == BUY else position - exposure
        if abs(worst) > lim.max_position:
            return f"worst-case position {worst} exceeds limit {lim.max_position}"
        return None
