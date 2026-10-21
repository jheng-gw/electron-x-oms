"""Scripted Order Entry conformance run for ElectronX production approval.

ElectronX issues production order-entry credentials only after it has seen a minimum amount of
activity on the Beta (demo) Order Entry session (Production_API_Trading_Approval_Framework.pdf).
This module produces that activity in a controlled way and counts every message both ways:

  1. clean slate: mass cancel anything a previous run left resting
  2. Reject probes (non-integer OrderQty, missing required tags)
  3. BusinessMessageReject probes (MarketDataRequest, which order entry does not support)
  4-5. order rounds: resting orders far from the market, replaces, cancels
  6. a last order round, then the final mass cancel
  7. idle until enough Heartbeats have been sent (HeartBtInt x ~32)

All order activity comes first, within about a minute, so a book that closes during the long idle
(demo closed every instrument at about 06:15 UTC during a run) cannot interrupt it. Before each
cycle that sends orders, the book is confirmed OPEN again.

Each step runs in its own logon/logout cycle. From the second cycle on, the inbound sequence
number we expect is wound back by one (over the exchange's Logout, an admin message), so the
exchange's Logon reply looks like a gap: we send a ResendRequest and the exchange answers with a
SequenceReset-GapFill. Nothing is lost and no application message is replayed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
from typing import Awaitable, Callable

from .config import RiskConfig, Settings
from .fix.codec import FixMessage
from .fix.session import FixSession, build_ssl_context
from .fix.store import SeqStore
from .journal import Journal, journal_path
from .md.handler import MarketDataClient
from .md.instruments import price_decimals
from .oe.gateway import OrderGateway, OrderRejected
from .oe.orders import BUY, SELL, Order, OrdStatus
from .oe.throttle import Throttle
from .risk.checks import RiskManager

log = logging.getLogger("conformance")

MSG_NAMES = {
    "0": "Heartbeat", "1": "TestRequest", "2": "ResendRequest", "3": "Reject", "4": "SequenceReset",
    "5": "Logout", "8": "ExecutionReport", "9": "OrderCancelReject", "A": "Logon", "D": "NewOrderSingle",
    "F": "OrderCancelRequest", "G": "OrderCancelReplaceRequest", "H": "OrderStatusRequest",
    "j": "BusinessMessageReject", "q": "OrderMassCancelRequest", "r": "OrderMassCancelReport",
    "V": "MarketDataRequest", "x": "SecurityListRequest", "y": "SecurityList",
}

# Approval framework minimums for the Order Entry session.
SENT_MIN = {"A": 5, "5": 5, "0": 30, "2": 5, "D": 20, "F": 10, "G": 10}
RECEIVED_MIN = {"4": 5, "3": 5, "j": 5, "8": 40}

CYCLES = 7               # logon/logout cycles; every one after the first provokes a ResendRequest
HEARTBEAT_TARGET = 32    # a little over the minimum, in case one is not counted
REJECT_TARGET = 6
BUSINESS_REJECT_TARGET = 6
ROUND_ORDERS = 8         # per order round, alternating buy/sell
ROUND_REPLACES = 4
AWAY_FRACTION = 0.2      # resting orders at least this far from the market...
AWAY_TICKS = 20          # ...and at least this many ticks
RESPONSE_TIMEOUT_S = 5.0


class ConformanceError(Exception):
    pass


async def wait_for(predicate: Callable[[], bool], timeout: float, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


class CountingSession(FixSession):
    """FixSession that counts every message it sends and receives, by MsgType."""

    def __init__(self, *args, sent: Counter, received: Counter, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent = sent
        self.received = received
        self.last_received = ""
        self.last_by_type: dict[str, FixMessage] = {}

    def _send(self, msg_type, body, header=(), seq=None, poss_dup=False) -> int:
        seq = super()._send(msg_type, body, header, seq=seq, poss_dup=poss_dup)
        self.sent[msg_type] += 1
        return seq

    def _on_message(self, msg: FixMessage) -> None:
        self.received[msg.msg_type] += 1
        self.last_received = msg.msg_type
        self.last_by_type[msg.msg_type] = msg
        super()._on_message(msg)


class ConformanceRun:
    def __init__(self, settings: Settings, *, heartbeat_s: float = 30, symbol: str | None = None,
                 ref_price: float | None = None, pace_s: float = 0.1, pause_s: float = 1.0):
        self.settings = settings
        self.symbol = symbol or settings.symbols[0]
        self.heartbeat_s = heartbeat_s
        self.ref_price = ref_price
        self.pace_s = pace_s      # 10 msg/s at most from our side, far under the 100 msg/s throttle
        self.pause_s = pause_s    # between logout and the next logon
        self.oe_cfg = replace(settings.oe, heartbeat_s=heartbeat_s)
        self.sent: Counter = Counter()
        self.received: Counter = Counter()
        self.steps: list[dict] = []
        self.prices: list[dict] = []
        self.probes: list[dict] = []  # each probe and the message that answered it
        self.session: CountingSession | None = None
        self.md_session: FixSession | None = None
        self._ssl = build_ssl_context(settings.tls)
        self._server_name = settings.tls.server_name or None
        self._probe_id = 0

        var = settings.var_dir
        var.mkdir(parents=True, exist_ok=True)
        self.seq_path = var / "seq-oe.json"  # the engine's file: ElectronX keeps OE numbers all week
        # The engine's journal, so a fill here (there should be none) still counts toward positions.
        self.journal = Journal(journal_path(var))
        self.md = MarketDataClient((self.symbol,))
        self.gateway = OrderGateway(
            settings, RiskManager(self._risk_limits()),
            Throttle.from_exchange_limit(settings.rate_limit, settings.rate_safety, settings.cancel_reserve),
            self.journal, instruments=self.md.instruments)

    @staticmethod
    def estimated_duration_s(heartbeat_s: float) -> float:
        return HEARTBEAT_TARGET * heartbeat_s + 60

    def _risk_limits(self) -> RiskConfig:
        # One lot per order, far from the market: the price band is checked in _prices() instead.
        return RiskConfig(max_order_qty=10**6, max_position=10**6, max_open_orders=ROUND_ORDERS + 2,
                          min_price=-1e12, max_price=1e12, price_band=float("inf"))

    @property
    def inst(self):
        return self.md.instruments[self.symbol]

    # ---- driver ----------------------------------------------------------------------------

    async def run(self) -> dict:
        started = datetime.now(timezone.utc)
        t0 = time.monotonic()
        md_session = self.md_session = FixSession(self.settings.md, self.md, SeqStore(), self._ssl,
                                                  self._server_name)
        md_task = asyncio.create_task(md_session.run(), name="conformance-md")
        error = ""
        try:
            await self._load_reference_data()
            # (name, work, sends orders that need an OPEN book)
            steps: list[tuple[str, Callable[[], Awaitable[None]], bool]] = [
                ("clean slate", lambda: self._mass_cancel("clean slate: orders from previous runs"), False),
                ("Reject probes", self._reject_probes, True),
                ("BusinessMessageReject probes", self._business_reject_probes, False),
                ("order round 1", lambda: self._order_round(1, cancel_all=True), True),
                ("order round 2", lambda: self._order_round(2, cancel_all=True), True),
                ("order round 3 + final mass cancel", self._final_round, True),
                ("heartbeats", self._heartbeats, False),
            ]
            assert len(steps) == CYCLES
            for i, (name, work, needs_open) in enumerate(steps):
                if needs_open:
                    await self._confirm_open()
                await self._cycle(i + 1, name, work, rewind=i > 0)
        except (ConformanceError, OrderRejected) as exc:
            error = str(exc)
            log.error("conformance run stopped: %s", exc)
        finally:
            await md_session.stop()
            await md_task
            self.journal.close()
        return self._report(started, time.monotonic() - t0, error)

    async def _load_reference_data(self) -> None:
        log.info("loading reference data and the book for %s", self.symbol)
        book = self.md.books[self.symbol]
        if not await wait_for(lambda: self.inst.tick_size and not book.stale, 20.0):
            raise ConformanceError(f"no SecurityList/snapshot for {self.symbol} from market data")
        await wait_for(lambda: self.inst.is_open is not None, RESPONSE_TIMEOUT_S)
        self._require_open()
        inst = self.inst
        log.info("%s tick %s lot %s limits %s..%s, book %r", self.symbol, inst.tick_size, inst.lot_size,
                 inst.low_limit, inst.high_limit, book)

    async def _confirm_open(self) -> None:
        """Ask again whether the book is OPEN: an empty book sends no market data when its state changes."""
        if self.md_session is None or not self.md_session.logged_on:
            raise ConformanceError("market data session is down; cannot confirm the book is OPEN")
        self.md.request_open_check(self.md_session, self.symbol)
        await wait_for(lambda: self.inst.listed_open is not None, RESPONSE_TIMEOUT_S)
        self._require_open()

    async def _cycle(self, n: int, name: str, work: Callable[[], Awaitable[None]], rewind: bool) -> None:
        store = SeqStore(self.seq_path)
        if rewind and store.next_target > 1:
            if self.session is not None and self.session.last_received not in ("5", "0", "4", "A"):
                log.warning("last message before logout was %s, not an admin message; the resend will replay it",
                            self.session.last_received)
            store.set_target(store.next_target - 1)
        session = self.session = CountingSession(self.oe_cfg, self.gateway, store, self._ssl, self._server_name,
                                                 sent=self.sent, received=self.received)
        task = asyncio.create_task(session.run(), name=f"conformance-oe-{n}")
        log.info("cycle %d/%d: %s", n, CYCLES, name)
        before_sent, before_received = Counter(self.sent), Counter(self.received)
        t0 = time.monotonic()
        try:
            if not await wait_for(lambda: self.gateway.session is session and self.gateway.ready, 20.0):
                raise ConformanceError(f"cycle {n}: order entry session did not log on and recover")
            await work()
        finally:
            if self.gateway.live_orders() and self.gateway.ready and self.gateway.mass_cancel(f"end of cycle {n}"):
                await wait_for(lambda: not self.gateway.live_orders(), RESPONSE_TIMEOUT_S)
            await session.stop(f"conformance cycle {n} done")
            await task
            self.steps.append({
                "cycle": n, "step": name, "seconds": round(time.monotonic() - t0, 1),
                "sent": dict(self.sent - before_sent), "received": dict(self.received - before_received)})
        await asyncio.sleep(self.pause_s)

    # ---- steps -----------------------------------------------------------------------------

    async def _reject_probes(self) -> None:
        """NewOrderSingles the exchange answers with a session Reject [3]."""
        inst, s = self.inst, self.settings
        buy, _ = self._prices()
        px = self._fmt(buy)

        def nos(**overrides: str | None) -> list[tuple[int, str]]:
            fields = {11: "", 1: s.account, 55: self.symbol, 54: BUY, 38: str(inst.lot_size), 40: "2",
                      44: px, 59: "0", 582: s.capacity, 1028: "N"}
            for tag, value in overrides.items():
                fields[int(tag[1:])] = value
            fields[11] = self._next_probe_id()
            return [(tag, value) for tag, value in fields.items() if value is not None]

        # On ElectronX demo, four non-integer OrderQty probes and three missing-tag probes gave only four
        # Rejects [3] (the rest came back as [j] or [8]), so non-integer quantities go first and alone
        # can reach REJECT_TARGET. The missing-tag probes are a fallback. Each answer is in the report.
        probes = [(f"non-integer OrderQty {q}", nos(t38=q)) for q in ("1.5", "0.5", "2.5", "1.25", "3.5", "0.25", "1.75")]
        if inst.lot_size > 1:
            probes.insert(1, (f"OrderQty {inst.lot_size + 1} off lot size {inst.lot_size}",
                              nos(t38=str(inst.lot_size + 1))))
        probes += [
            ("missing OrderQty(38)", nos(t38=None)),
            ("missing Side(54)", nos(t54=None)),
            ("missing ManualOrderIndicator(1028)", nos(t1028=None)),
        ]
        await self._probe("3", REJECT_TARGET, probes)

    async def _business_reject_probes(self) -> None:
        """A message type the order entry gateway does not support, answered with BusinessMessageReject [j].

        The spec says market data is "not accessible via the order-entry session", and rejects an
        unsupported type with 380=3, so MarketDataRequest [V] is the unambiguous choice. (Where
        SecurityListRequest [x] is served is not stated, so it is not used.)"""
        md_req = [(263, "0"), (264, "1"), (267, "2"), (269, "0"), (269, "1"), (146, "1"), (55, self.symbol)]
        probes = [("MarketDataRequest [V] on order entry", "V", [(262, self._next_probe_id())] + md_req)
                  for _ in range(BUSINESS_REJECT_TARGET + 2)]
        await self._probe("j", BUSINESS_REJECT_TARGET, probes)

    async def _order_round(self, n: int, cancel_all: bool) -> list[Order]:
        """Resting orders far from the market, then replace some (further away) and cancel."""
        gw, inst = self.gateway, self.inst
        tick = inst.tick_size
        buy, sell = self._prices()
        self.prices.append({"round": n, "buy": buy, "sell": sell})
        log.info("round %d: buys from %s down, sells from %s up", n, self._fmt(buy), self._fmt(sell))
        orders = []
        for i in range(ROUND_ORDERS):
            side, k = (BUY, SELL)[i % 2], i // 2
            px = buy - k * tick if side == BUY else sell + k * tick
            orders.append(gw.new_order(self.symbol, side, inst.lot_size, self._round(px)))
            await asyncio.sleep(self.pace_s)
        await self._settle(orders, lambda o: o.status != OrdStatus.PENDING_NEW, "acknowledged")
        self._check_resting(orders)

        for order in [o for o in orders if o.is_live][:ROUND_REPLACES]:
            # Further from the market, one tick at a time: never toward it.
            gw.replace(order, self._round(order.price - tick if order.side == BUY else order.price + tick))
            await asyncio.sleep(self.pace_s)
        await self._settle(orders, lambda o: not o.is_pending, "replaced")

        to_cancel = [o for o in orders if o.is_live]
        if not cancel_all:
            to_cancel = to_cancel[:len(to_cancel) // 2]
        for order in to_cancel:
            gw.cancel(order)
            await asyncio.sleep(self.pace_s)
        await self._settle(to_cancel, lambda o: not o.is_live, "canceled")
        return orders

    async def _heartbeats(self) -> None:
        """Stay logged on and idle: the session sends a Heartbeat every HeartBtInt seconds."""
        remaining = HEARTBEAT_TARGET - self.sent["0"]
        if remaining <= 0:
            return
        log.info("idle for ~%.0fs to send %d more Heartbeats (HeartBtInt %ss)",
                 remaining * self.heartbeat_s, remaining, self.heartbeat_s)
        deadline = time.monotonic() + (remaining + 3) * (self.heartbeat_s + 1) * 1.5
        last_note = time.monotonic()
        while self.sent["0"] < HEARTBEAT_TARGET:
            if time.monotonic() > deadline:
                raise ConformanceError(f"only {self.sent['0']} Heartbeats sent before the deadline")
            if not (self.session and self.session.logged_on):
                raise ConformanceError("order entry session dropped while idling for Heartbeats")
            if time.monotonic() - last_note >= 60:
                last_note = time.monotonic()
                log.info("heartbeats sent: %d/%d", self.sent["0"], HEARTBEAT_TARGET)
            await asyncio.sleep(0.2)

    async def _final_round(self) -> None:
        await self._order_round(3, cancel_all=False)
        await self._mass_cancel("end of conformance run")
        if self.gateway.live_orders():
            raise ConformanceError(f"{len(self.gateway.live_orders())} orders still live after the mass cancel")

    # ---- helpers ---------------------------------------------------------------------------

    async def _probe(self, expect: str, target: int, probes: list[tuple]) -> None:
        """Send probes one at a time until `target` messages of type `expect` came back this cycle."""
        start, reports_before = self.received[expect], self.received["8"]
        answers = ("3", "j", "8", "9")
        for probe in probes:
            if self.received[expect] - start >= target:
                break
            label, msg_type, body = probe if len(probe) == 3 else (probe[0], "D", probe[1])
            before = {t: self.received[t] for t in answers}
            session = self._require_session()
            session.send(msg_type, body, header=[(50, self.settings.trader)])
            await wait_for(lambda: any(self.received[t] > before[t] for t in answers), RESPONSE_TIMEOUT_S)
            got_types = [t for t in answers if self.received[t] > before[t]]
            answer = got_types[0] if got_types else ""
            msg = session.last_by_type.get(answer) if answer else None
            text = msg.get(58, "") if msg is not None else ""
            ref_tag = msg.get(371, "") if msg is not None else ""
            self.probes.append({"probe": label, "msg_type": msg_type, "answer": answer, "text": text,
                                "ref_tag": ref_tag, "expected": expect})
            if answer:
                log.info("probe %s -> %s [%s]%s%s", label, MSG_NAMES.get(answer, answer), answer,
                         f" tag {ref_tag}" if ref_tag else "", f": {text}" if text else "")
            else:
                log.warning("probe %s -> no answer within %.0fs", label, RESPONSE_TIMEOUT_S)
            await asyncio.sleep(self.pace_s)
        got = self.received[expect] - start
        if got < target:
            log.warning("only %d %s received from %d probes", got, MSG_NAMES[expect], len(probes))
        if self.received["8"] > reports_before and self.gateway.ready:
            # A probe the exchange accepted after all is resting at a price far from the market.
            await self._mass_cancel("clear accepted probes")

    async def _mass_cancel(self, reason: str) -> None:
        before = self.received["r"]
        if not self.gateway.mass_cancel(reason):
            raise ConformanceError("mass cancel could not be sent")
        if not await wait_for(lambda: self.received["r"] > before, RESPONSE_TIMEOUT_S):
            raise ConformanceError("no OrderMassCancelReport")
        await wait_for(lambda: not self.gateway.live_orders(), RESPONSE_TIMEOUT_S)

    async def _settle(self, orders: list[Order], done: Callable[[Order], bool], what: str) -> None:
        if not await wait_for(lambda: all(done(o) for o in orders), RESPONSE_TIMEOUT_S):
            raise ConformanceError(f"orders not {what} within {RESPONSE_TIMEOUT_S:.0f}s: "
                                   f"{[o for o in orders if not done(o)]}")

    def _check_resting(self, orders: list[Order]) -> None:
        bad = [o for o in orders if o.status != OrdStatus.NEW]
        if bad:
            raise ConformanceError(f"orders did not rest as expected: {bad}")

    def _require_open(self) -> None:
        """Refuse to send orders unless the book is known to be OPEN: a closed book rejects every one."""
        inst = self.inst
        hint = "pick one with: exoms instruments --target demo --state OPEN"
        if inst.is_open is None:
            raise ConformanceError(f"could not confirm {self.symbol} is OPEN (no state on market data and no "
                                   f"answer to the OPEN SecurityListRequest); {hint}")
        if not inst.is_open:
            state = inst.state or "not in the OPEN instrument list"
            raise ConformanceError(f"{self.symbol} is not OPEN for trading ({state}); {hint}")

    def _prices(self) -> tuple[float, float]:
        """Top buy and sell price for a round: far from the market and inside the instrument limits.

        Buys go AWAY_FRACTION below the best bid (or reference), sells the same above the best ask.
        Each round uses up to ROUND_ORDERS/2 + 1 ticks beyond that, still inside the limits."""
        self._require_open()  # market data may have reported a state change since the run started
        inst, book = self.inst, self.md.books[self.symbol]
        tick = inst.tick_size
        own = self.gateway.own_order_ids(self.symbol)
        bid, ask = book.best_bid(own), book.best_ask(own)
        ref = book.mid(own) or bid or ask or self.ref_price
        if not ref:
            raise ConformanceError(f"the {self.symbol} book is empty: pass --ref-price")
        away = max(AWAY_TICKS * tick, AWAY_FRACTION * ref)
        buy = math.floor((min(bid or ref, ref) - away) / tick + 1e-9) * tick
        sell = math.ceil((max(ask or ref, ref) + away) / tick - 1e-9) * tick
        ladder = (ROUND_ORDERS // 2 + 1) * tick
        if inst.low_limit is not None:
            buy = max(buy, inst.low_limit + ladder)
        if inst.high_limit is not None:
            sell = min(sell, inst.high_limit - ladder)
        # After clamping to the limits, still never marketable and still well away from the touch.
        min_gap = AWAY_TICKS * tick
        if buy > min(bid if bid is not None else ref, ref) - min_gap:
            raise ConformanceError(f"buy {buy} too close to the market (bid {bid}, ask {ask})")
        if sell < max(ask if ask is not None else ref, ref) + min_gap:
            raise ConformanceError(f"sell {sell} too close to the market (bid {bid}, ask {ask})")
        if buy <= 0:
            raise ConformanceError(f"no positive buy price far enough below {ref}")
        return self._round(buy), self._round(sell)

    def _round(self, price: float) -> float:
        return round(price, price_decimals(self.inst.tick_size))

    def _fmt(self, price: float) -> str:
        return f"{price:.{price_decimals(self.inst.tick_size)}f}"

    def _next_probe_id(self) -> str:
        self._probe_id += 1
        return f"CONF{int(time.time())}-{self._probe_id}"

    def _require_session(self) -> CountingSession:
        if self.session is None or not self.session.logged_on:
            raise ConformanceError("order entry session is not logged on")
        return self.session

    # ---- report ----------------------------------------------------------------------------

    def _report(self, started: datetime, seconds: float, error: str) -> dict:
        rows = [{"direction": "client->exchange", "msg_type": t, "name": MSG_NAMES[t],
                 "count": self.sent[t], "minimum": m, "ok": self.sent[t] >= m} for t, m in SENT_MIN.items()]
        rows += [{"direction": "exchange->client", "msg_type": t, "name": MSG_NAMES[t],
                  "count": self.received[t], "minimum": m, "ok": self.received[t] >= m}
                 for t, m in RECEIVED_MIN.items()]
        report = {
            "target": self.settings.target, "session": self.settings.oe.sender_comp_id, "symbol": self.symbol,
            "started": started.isoformat(timespec="seconds"), "seconds": round(seconds, 1),
            "heartbeat_s": self.heartbeat_s, "passed": not error and all(r["ok"] for r in rows), "error": error,
            "requirements": rows, "sent": dict(sorted(self.sent.items())),
            "received": dict(sorted(self.received.items())), "prices": self.prices, "probes": self.probes,
            "cycles": self.steps,
        }
        path = self.settings.var_dir / f"conformance-{started:%Y%m%d-%H%M%S}.json"
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        report["path"] = str(path)
        return report


def format_report(report: dict) -> str:
    lines = [f"Order Entry conformance: {report['session']} on {report['target']}, {report['symbol']}, "
             f"{report['seconds']:.0f}s, HeartBtInt {report['heartbeat_s']}s", "",
             f"{'direction':17} {'message':32} {'count':>6} {'min':>5}  result"]
    for r in report["requirements"]:
        lines.append(f"{r['direction']:17} {r['name'] + ' (' + r['msg_type'] + ')':32} {r['count']:>6} "
                     f"{r['minimum']:>5}  {'ok' if r['ok'] else 'SHORT'}")
    required = {("client->exchange", t) for t in SENT_MIN} | {("exchange->client", t) for t in RECEIVED_MIN}
    other = [(d, t, n) for d, counts in (("client->exchange", report["sent"]), ("exchange->client", report["received"]))
             for t, n in counts.items() if (d, t) not in required]
    if other:
        lines += ["", "other messages:"]
        lines += [f"{d:17} {MSG_NAMES.get(t, '?') + ' (' + t + ')':32} {n:>6}" for d, t, n in other]
    lines.append("")
    if report["error"]:
        lines.append(f"STOPPED: {report['error']}")
    lines.append(("PASSED: every minimum met" if report["passed"] else "NOT PASSED") + f"; report saved to {report['path']}")
    return "\n".join(lines)
