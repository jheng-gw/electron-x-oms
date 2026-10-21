"""Test helpers for the ElectronX FIX API: one awaitable method per thing the API can do.

Unlike the engine there is no risk check, throttle or strategy in front of these calls, so use them
for poking at the API by hand, not for trading.

    python sample.py --with-sim          # in-process simulator, about 5 seconds
    python sample.py --target demo       # ElectronX demo (stop the engine first: one connection per SenderCompID)

Or from your own script:

    async with SampleClient(load_settings("sim")) as client:
        print(await client.account_summary())

There is no account balance on the ElectronX API: the spec defines no CollateralInquiry or
RequestForPositions message. account_summary() is the closest thing, built from Trade Capture Reports.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Callable

from exoms.config import Settings, load_settings
from exoms.fix.codec import FixMessage
from exoms.fix.session import FixSession, build_ssl_context
from exoms.fix.store import SeqStore
from exoms.md.instruments import Instrument, price_decimals

log = logging.getLogger("sample")

BUY, SELL = "1", "2"
DAY, GTC, IOC, FOK, GTD = "0", "1", "3", "4", "6"           # TimeInForce(59)
POST_ONLY, BEST_LIMIT, CROSS_LIMIT = "6", "R", "T"          # ExecInst(18), spec Table 14


class RequestFailed(Exception):
    """The exchange answered with a Reject [3], BusinessMessageReject [j] or a rejected report."""

    def __init__(self, msg: FixMessage):
        self.msg = msg
        super().__init__(f"{msg.msg_type}: {msg.get(58, '')}")


@dataclass
class Position:
    symbol: str
    bought: int = 0
    sold: int = 0
    cash: float = 0.0  # sum of sell notional minus buy notional

    @property
    def net(self) -> int:
        return self.bought - self.sold


@dataclass
class AccountSummary:
    """Positions and cash flow from fills in a time window. Not a cash balance: the API has none."""
    start: datetime
    end: datetime
    trades: int = 0
    positions: dict[str, Position] = field(default_factory=dict)

    @property
    def net_cash(self) -> float:
        return sum(p.cash for p in self.positions.values())

    def __str__(self) -> str:
        lines = [f"{self.trades} trades {self.start:%Y-%m-%d %H:%M} .. {self.end:%Y-%m-%d %H:%M} UTC",
                 f"{'symbol':40} {'bought':>7} {'sold':>7} {'net':>6} {'cash':>12}"]
        for p in sorted(self.positions.values(), key=lambda p: p.symbol):
            lines.append(f"{p.symbol:40} {p.bought:>7} {p.sold:>7} {p.net:>+6} {p.cash:>12.2f}")
        lines.append(f"{'net cash flow':40} {'':>7} {'':>7} {'':>6} {self.net_cash:>12.2f}")
        return "\n".join(lines)


class _Inbox:
    """SessionApp that keeps every application message so a request can wait for its answer."""

    def __init__(self) -> None:
        self.messages: list[FixMessage] = []
        self.logged_on = asyncio.Event()
        self._arrived = asyncio.Event()

    def on_logon(self, session: FixSession) -> None:
        self.logged_on.set()

    def on_disconnect(self, session: FixSession) -> None:
        self.logged_on.clear()

    def on_app_message(self, session: FixSession, msg: FixMessage) -> None:
        self.messages.append(msg)
        self._arrived.set()

    async def wait(self, since: int, match: Callable[[FixMessage], bool], timeout: float) -> FixMessage:
        """First message at index >= `since` that matches."""
        deadline = time.monotonic() + timeout
        while True:
            for msg in self.messages[since:]:
                if match(msg):
                    return msg
            since = len(self.messages)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("no answer from the exchange")
            self._arrived.clear()
            try:
                await asyncio.wait_for(self._arrived.wait(), remaining)
            except TimeoutError:
                pass


class SampleClient:
    TIMEOUT_S = 10.0

    def __init__(self, settings: Settings, market_data: bool = True):
        self.settings = settings
        ssl_ctx, server_name = build_ssl_context(settings.tls), settings.tls.server_name
        self.oe, self.md = _Inbox(), _Inbox()
        # Order Entry keeps sequence numbers all week, so share the engine's store.
        self.oe_session = FixSession(settings.oe, self.oe, SeqStore(settings.var_dir / "seq-oe.json"),
                                     ssl_ctx, server_name)
        self.md_session = FixSession(settings.md, self.md, SeqStore(), ssl_ctx, server_name) if market_data else None
        self.instruments: dict[str, Instrument] = {}
        self._tasks: list[asyncio.Task] = []
        self._id_prefix = f"S{int(time.time()):x}"
        self._id_seq = 0

    async def __aenter__(self) -> "SampleClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ---- connection ------------------------------------------------------------------------

    async def connect(self) -> None:
        """Log on to Order Entry (and Market Data), then load reference data."""
        sessions = [(self.oe_session, self.oe)]
        if self.md_session is not None:
            sessions.append((self.md_session, self.md))
        for session, inbox in sessions:
            self._tasks.append(asyncio.create_task(session.run()))
            await asyncio.wait_for(inbox.logged_on.wait(), self.TIMEOUT_S)
        if self.md_session is not None:
            for inst in await self.list_instruments():
                self.instruments[inst.symbol] = inst

    async def close(self) -> None:
        """Graceful Logout [5]. Resting DAY orders survive unless Cancel on Logout is enabled."""
        for session in (self.md_session, self.oe_session):
            if session is not None:
                await session.stop()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---- reference and market data (Market Data session) -----------------------------------

    async def list_instruments(self, symbol: str | None = None) -> list[Instrument]:
        """SecurityListRequest [x]: every instrument (559=4) or one symbol (559=0)."""
        req_id = self._next_id()
        body = [(320, req_id), (559, "0"), (55, symbol)] if symbol else [(320, req_id), (559, "4")]
        mark = len(self._require_md().messages)
        self.md_session.send("x", body)
        out: list[Instrument] = []
        while True:
            msg = await self.md.wait(mark, lambda m: m.get(320) == req_id or m.get(379) == req_id, self.TIMEOUT_S)
            mark = self.md.messages.index(msg) + 1
            if msg.msg_type == "j":
                raise RequestFailed(msg)
            out += [Instrument.from_security_list(e) for e in msg.groups(146, 55)]
            if msg.get(893, "Y") == "Y":  # LastFragment
                return out

    async def order_book(self, symbol: str, depth: int = 0) -> dict[str, list[tuple[float, float]]]:
        """MarketDataRequest [V] snapshot (263=0). depth 0 = full book (25 levels), 1 = top of book.
        Returns {"bids": [(price, qty), ...] best first, "asks": [...]}; one row per order."""
        req_id = self._next_id()
        mark = len(self._require_md().messages)
        self.md_session.send("V", [(262, req_id), (263, "0"), (264, str(depth)),
                                   (267, "2"), (269, "0"), (269, "1"), (146, "1"), (55, symbol)])
        msg = await self.md.wait(mark, lambda m: m.get(262) == req_id or m.get(379) == req_id, self.TIMEOUT_S)
        if msg.msg_type != "W":
            raise RequestFailed(msg)
        bids, asks = [], []
        for e in msg.groups(268, 269):
            if e.get(270):
                (bids if e[269] == "0" else asks).append((float(e[270]), float(e.get(271) or 0)))
        return {"bids": sorted(bids, reverse=True), "asks": sorted(asks)}

    # ---- orders (Order Entry session) ------------------------------------------------------

    async def new_order(self, symbol: str, side: str, qty: int, price: float, *, tif: str = DAY,
                        exec_inst: str | None = None, min_qty: int | None = None,
                        manual: bool = False) -> FixMessage:
        """NewOrderSingle [D]. Returns the first ExecutionReport, or for IOC/FOK the final one
        (filled or expired; CumQty(14) and AvgPx(6) give the outcome)."""
        cl = self._next_id()
        body = [(11, cl), (1, self.settings.account), (55, symbol), (54, side), (38, str(qty)), (40, "2"),
                (44, self._px(symbol, price)), (59, tif)]
        if exec_inst:
            body.append((18, exec_inst))
        if min_qty:
            body.append((110, str(min_qty)))
        body += [(582, self.settings.capacity), (1028, "Y" if manual else "N")]
        mark = len(self.oe.messages)
        report = await self._order_request("D", cl, body)
        if tif in (IOC, FOK):
            report = await self.oe.wait(mark, lambda m: m.msg_type == "8" and m.get(11) == cl
                                        and m.get(39) in ("2", "3", "4", "C"), self.TIMEOUT_S)
        return report

    async def replace_order(self, orig_cl_ord_id: str, symbol: str, side: str, qty: int, price: float,
                            *, tif: str = DAY) -> FixMessage:
        """OrderCancelReplaceRequest [G]. Use the returned report's ClOrdID(11) for the next change."""
        cl = self._next_id()
        return await self._order_request("G", cl, [
            (11, cl), (41, orig_cl_ord_id), (1, self.settings.account), (55, symbol), (54, side),
            (40, "2"), (38, str(qty)), (44, self._px(symbol, price)), (59, tif)])

    async def cancel_order(self, orig_cl_ord_id: str, symbol: str) -> FixMessage:
        """OrderCancelRequest [F]. Raises RequestFailed on an OrderCancelReject [9]."""
        cl = self._next_id()
        return await self._order_request("F", cl, [(11, cl), (41, orig_cl_ord_id), (55, symbol)])

    async def mass_cancel(self, symbol: str | None = None, account: str | None = None) -> FixMessage:
        """OrderMassCancelRequest [q]: one symbol (530=1), one account (530=7 + 1), or the whole session."""
        cl = self._next_id()
        body = [(11, cl), (530, "1" if symbol else "7")]
        if symbol:
            body.append((55, symbol))
        if account:
            body.append((1, account))
        report = await self._order_request("q", cl, body, answer="r")
        if report.get(531) == "0":
            raise RequestFailed(report)
        return report

    # ---- trades and positions --------------------------------------------------------------

    async def trade_history(self, start: datetime | None = None, end: datetime | None = None) -> list[dict]:
        """TradeCaptureReportRequest [AD] for all trades (569=0); ElectronX keeps one month."""
        end = end or datetime.now(timezone.utc) + timedelta(seconds=1)  # times go out to the second
        start = start or end - timedelta(days=30)
        req_id = self._next_id()
        mark = len(self.oe.messages)
        self.oe_session.send("AD", [(568, req_id), (569, "0"), (263, "0"),
                                    (9593, f"{start:%Y%m%d-%H:%M:%S}"), (9594, f"{end:%Y%m%d-%H:%M:%S}")])
        trades = []
        while True:
            msg = await self.oe.wait(mark, lambda m: m.get(568) == req_id or m.get(379) == req_id
                                     or m.msg_type == "AE", self.TIMEOUT_S)
            mark = self.oe.messages.index(msg) + 1
            if msg.msg_type == "j":
                raise RequestFailed(msg)
            if msg.msg_type == "AE":
                trades.append({"trade_id": msg.get(880), "time": msg.get(60), "symbol": msg.get(55),
                               "side": msg.get(54), "qty": int(msg.get_float(32)), "price": msg.get_float(31),
                               "cl_ord_id": msg.get(11), "order_id": msg.get(37)})
            elif msg.get(750) == "1":  # TradeRequestStatus completed
                return trades
            elif msg.get(749, "0") != "0":  # TradeRequestResult
                raise RequestFailed(msg)

    async def account_summary(self, start: datetime | None = None, end: datetime | None = None) -> AccountSummary:
        """Net position and cash flow per symbol from trade_history(). Not an exchange balance."""
        end = end or datetime.now(timezone.utc) + timedelta(seconds=1)
        start = start or end - timedelta(days=30)
        summary = AccountSummary(start, end)
        for t in await self.trade_history(start, end):
            p = summary.positions.setdefault(t["symbol"], Position(t["symbol"]))
            notional = t["qty"] * t["price"]
            if t["side"] == BUY:
                p.bought += t["qty"]
                p.cash -= notional
            else:
                p.sold += t["qty"]
                p.cash += notional
            summary.trades += 1
        return summary

    async def get_account_balance(self) -> float:
        raise NotImplementedError("ElectronX has no balance/collateral message; use account_summary() for "
                                  "positions and cash flow, or ask ElectronX / your clearing member for balances")

    # ---- internals -------------------------------------------------------------------------

    async def _order_request(self, msg_type: str, cl: str, body: list[tuple[int, str]],
                             answer: str = "8") -> FixMessage:
        mark = len(self.oe.messages)
        seq = self.oe_session.send(msg_type, body, header=[(50, self.settings.trader)])

        def match(m: FixMessage) -> bool:
            return (m.msg_type in (answer, "9") and m.get(11) == cl) or (m.msg_type == "j" and m.get(379) == cl) \
                or (m.msg_type == "3" and m.get_int(45) == seq)

        msg = await self.oe.wait(mark, match, self.TIMEOUT_S)
        if msg.msg_type in ("3", "j", "9") or msg.get(150) == "8":
            raise RequestFailed(msg)
        return msg

    def _require_md(self) -> _Inbox:
        if self.md_session is None:
            raise RuntimeError("created with market_data=False")
        return self.md

    def _next_id(self) -> str:
        self._id_seq += 1
        return f"{self._id_prefix}-{self._id_seq}"

    def _px(self, symbol: str, price: float) -> str:
        inst = self.instruments.get(symbol)
        tick = inst.tick_size if inst and inst.tick_size else self.settings.strategy.tick_size
        return f"{price:.{price_decimals(tick)}f}"


async def demo(settings: Settings, symbol: str) -> None:
    """Walk through every method once with 1-lot orders far from the market."""
    async with SampleClient(settings) as c:
        inst = c.instruments.get(symbol) or Instrument(symbol)
        tick = inst.tick_size or settings.strategy.tick_size
        print(f"{len(c.instruments)} instruments; {symbol} tick {tick} lot {inst.lot_size} "
              f"limits {inst.low_limit}..{inst.high_limit}")

        book = await c.order_book(symbol)
        print("book:", "bids", book["bids"][:3], "asks", book["asks"][:3])
        if not book["bids"]:
            print("empty book; skipping orders")
        else:
            far = max(book["bids"][0][0] - 20 * tick, inst.low_limit or tick)
            er = await c.new_order(symbol, BUY, inst.lot_size, far, exec_inst=POST_ONLY)
            print(f"placed  {er.get(11)} buy {er.get(38)} @ {er.get(44)} status {er.get(39)}")
            er = await c.replace_order(er.get(11), symbol, BUY, inst.lot_size, far - tick)
            print(f"replaced -> {er.get(11)} @ {er.get(44)}")
            er = await c.cancel_order(er.get(11), symbol)
            print(f"canceled {er.get(41)} status {er.get(39)}")

            if settings.target == "sim" and book["asks"]:  # a real fill only against the simulator
                er = await c.new_order(symbol, BUY, inst.lot_size, book["asks"][0][0], tif=IOC)
                print(f"IOC buy at the ask: {er.get(14)} filled @ avg {er.get(6)} (OrdStatus {er.get(39)})")

        r = await c.mass_cancel()
        print(f"mass cancel accepted (MassCancelResponse {r.get(531)})")
        print()
        print(await c.account_summary())
        try:
            await c.get_account_balance()
        except NotImplementedError as exc:
            print(f"\nget_account_balance(): {exc}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Exercise the ElectronX FIX API once, method by method")
    parser.add_argument("--target", choices=["sim", "demo"], default="sim")
    parser.add_argument("--with-sim", action="store_true", help="start an in-process simulator (--target sim)")
    parser.add_argument("--symbol", help="instrument (default: first of EXI_SYMBOLS)")
    parser.add_argument("-v", "--verbose", action="store_true", help="log every FIX message")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)-7s %(name)-9s %(message)s")

    settings = load_settings(args.target)
    symbol = args.symbol or settings.symbols[0]
    if args.target == "demo":
        if input(f"Place and cancel a 1-lot order in {symbol} on ElectronX demo? [y/N] ").strip().lower() != "y":
            return
    sim = sim_task = None
    if args.with_sim:
        from exoms.sim.exchange import SimExchange
        sim = SimExchange.from_settings(settings)
        await sim.start()
        sim_task = asyncio.create_task(sim.run_market())
        settings = replace(settings, md=replace(settings.md, port=sim.md_port), oe=replace(settings.oe, port=sim.oe_port))
        await asyncio.sleep(0.5)  # let the synthetic book fill
    try:
        await demo(settings, symbol)
    finally:
        if sim is not None:
            sim_task.cancel()
            await sim.close()


if __name__ == "__main__":
    asyncio.run(main())
