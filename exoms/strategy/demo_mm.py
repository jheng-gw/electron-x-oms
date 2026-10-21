"""Demo quoting strategy: one bid and one ask around mid, skewed by inventory.

It exists to exercise the plumbing (book -> strategy -> risk -> gateway -> exec reports), not to make money.
"""
from __future__ import annotations

import logging
import math

from ..config import StrategyConfig
from ..md.book import OrderBook
from ..md.instruments import Instrument
from ..oe.gateway import OrderGateway, OrderRejected
from ..oe.orders import BUY, SELL, Order

log = logging.getLogger("strategy")


class DemoQuoter:
    def __init__(self, symbol: str, gateway: OrderGateway, cfg: StrategyConfig, instrument: Instrument):
        self.symbol = symbol
        self.gateway = gateway
        self.cfg = cfg
        self.instrument = instrument
        self.quotes: dict[str, Order | None] = {BUY: None, SELL: None}
        self._last_block: dict[str, str] = {}

    @property
    def tick(self) -> float:
        return self.instrument.tick_size or self.cfg.tick_size

    def on_book(self, book: OrderBook, recv_ns: int) -> None:
        if book.symbol != self.symbol:
            return
        gw = self.gateway
        if book.stale:
            self.pull("market data stale")
            return
        # Our own resting orders are in the market-by-order feed; quoting off them would chase ourselves.
        mid = book.mid(exclude=gw.own_order_ids(self.symbol))
        if mid is None:
            return
        gw.reference_prices[self.symbol] = mid
        if not gw.ready or gw.risk.killed:
            return

        tick = self.tick
        skew = -gw.risk.positions[self.symbol] * self.cfg.skew_ticks_per_lot * tick
        half = self.cfg.half_spread_ticks * tick
        bid = math.floor(round((mid - half + skew) / tick, 6)) * tick
        ask = math.ceil(round((mid + half + skew) / tick, 6)) * tick
        self._work(BUY, round(bid, 10), recv_ns)
        self._work(SELL, round(ask, 10), recv_ns)

    def _work(self, side: str, price: float, recv_ns: int) -> None:
        order = self.quotes[side]
        try:
            if order is None or not order.is_live:
                self.quotes[side] = self.gateway.new_order(self.symbol, side, self.cfg.quote_qty, price, t0_ns=recv_ns)
            elif not order.is_pending and abs(order.price - price) >= self.cfg.requote_ticks * self.tick - 1e-9:
                self.gateway.replace(order, price, t0_ns=recv_ns)
            self._last_block.pop(side, None)
        except OrderRejected as exc:
            # Log each distinct reason once, e.g. when the position limit stops one side quoting.
            if self._last_block.get(side) != str(exc):
                self._last_block[side] = str(exc)
                log.info("%s %s quote blocked: %s", self.symbol, "bid" if side == BUY else "ask", exc)

    def on_order_event(self, order: Order, event: str) -> None:
        for side, quote in self.quotes.items():
            if quote is order and not order.is_live:
                self.quotes[side] = None

    def pull(self, reason: str) -> None:
        for order in self.quotes.values():
            if order is not None and order.is_live and self.gateway.cancel(order):
                log.info("pulled %s: %s", order.cl_ord_id, reason)
