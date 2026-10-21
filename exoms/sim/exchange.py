"""Local ElectronX simulator, modelled on the ElectronX FIX Specification (v.20261015.1).

What it reproduces, so the OMS can be exercised without touching the real venue:
  * FIXT.1.1 / FIX50SP2 sessions: MD gateway (resets each logon) and OE gateway shared by
    Order Entry and Drop Copy (sequence numbers persist across reconnects, ResendRequest replay),
    with a Heartbeat from EXI whenever it has sent nothing for HeartBtInt
  * Logon rules: DefaultApplVerID(1137)=9, no SenderSubID(50), one connection per SenderCompID
  * Order validation with the spec's reject paths (ExecutionReport 150=8, Reject [3], BusinessMessageReject [j]),
    including "Book is CLOSED" for an instrument whose state is not OPEN
  * Price-time matching against synthetic liquidity and random takers
  * Market-by-order data: W snapshot + X increments keyed by MDEntryID(278) = OrderID(37)
  * Cancel On Disconnect (378=8), mass cancel [q]/[r], SecurityList [y], rate limiting, drop copy
  * Trade Capture Reports: TradeCaptureReportRequest [AD] -> AQ, AE per fill, AQ
It is not a performance or exact-behaviour replica of the exchange.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from ..config import Settings
from ..fix.codec import FixMessage, FixParseError, FixParser, encode
from ..fix.session import utc_timestamp

log = logging.getLogger("sim")

BUY, SELL = "1", "2"
ADMIN = frozenset({"0", "1", "2", "3", "4", "5", "A"})  # gap-filled, never replayed
BEGIN = "FIXT.1.1"
SYNTHETIC = "SIM-MM"  # account used by the simulator's own liquidity


def _ts_parts() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S.%f")


@dataclass
class SimInstrument:
    symbol: str
    tick: float = 0.25
    lot: int = 1
    low: float = 0.0
    high: float = 500.0
    state: str = "OPEN"  # spec Table 1; anything but OPEN rejects new orders, as demo does with "Book is CLOSED"

    def ticks(self, price: float) -> int | None:
        t = price / self.tick
        return round(t) if abs(t - round(t)) < 1e-6 else None

    def fmt(self, ticks: int) -> str:
        return f"{ticks * self.tick:.2f}"


@dataclass(eq=False)
class SimOrder:
    order_id: str
    cl_ord_id: str
    owner: "SessionState | None"
    account: str
    trader: str
    symbol: str
    side: str
    qty: int
    px: int  # in ticks
    tif: str = "0"
    capacity: str = "2"
    manual: str = "N"
    cum_qty: int = 0
    notional: float = 0.0
    status: str = "0"
    prev_cl_ord_id: str = ""

    @property
    def leaves(self) -> int:
        return self.qty - self.cum_qty if self.status in ("0", "1") else 0

    @property
    def live(self) -> bool:
        return self.status in ("0", "1")


@dataclass
class Market:
    inst: SimInstrument
    fair: int
    bids: dict[int, list[SimOrder]] = field(default_factory=dict)
    asks: dict[int, list[SimOrder]] = field(default_factory=dict)
    bot_orders: dict[tuple[str, int], SimOrder] = field(default_factory=dict)

    def side_book(self, side: str) -> dict[int, list[SimOrder]]:
        return self.bids if side == BUY else self.asks

    def best(self, side: str) -> int | None:
        book = self.side_book(side)
        return (max(book) if side == BUY else min(book)) if book else None

    def resting(self) -> Iterable[SimOrder]:
        for book in (self.bids, self.asks):
            for queue in book.values():
                yield from queue


class SessionState:
    """Per-SenderCompID state that outlives TCP connections (as on the real OE gateway)."""

    def __init__(self, sender: str, role: str):
        self.sender = sender
        self.role = role
        self.out_seq = 1
        self.in_expected = 1
        self.history: dict[int, tuple[str, list[tuple[int, str]]]] = {}
        self.conn: SimConnection | None = None
        self.awaiting_gap_fill = False
        self.recent: deque[float] = deque()
        self.md_reqs: dict[str, str] = {}  # symbol -> MDReqID

    def reset(self) -> None:
        self.out_seq = self.in_expected = 1
        self.history.clear()

    def send(self, msg_type: str, body: list[tuple[int, str]], seq: int | None = None,
             poss_dup: bool = False) -> None:
        if seq is None:
            seq = self.out_seq
            self.out_seq += 1
            if msg_type not in ADMIN:
                self.history[seq] = (msg_type, body)
        if self.conn is None:
            return  # delivered later through ResendRequest
        now = utc_timestamp()
        fields = [(35, msg_type), (34, str(seq)), (49, "EXI"), (52, now), (56, self.sender)]
        if poss_dup:
            fields += [(43, "Y"), (122, now)]
        self.conn.write(encode(BEGIN, fields + body))


class SimConnection:
    def __init__(self, exchange: "SimExchange", reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter, gateway: str):
        self.ex = exchange
        self.reader = reader
        self.writer = writer
        self.gateway = gateway  # "md" or "oe"
        self.state: SessionState | None = None
        self.graceful = False
        self.last_write = 0.0
        self._heartbeats: asyncio.Task | None = None

    def write(self, data: bytes) -> None:
        if not self.writer.is_closing():
            self.writer.write(data)
            self.last_write = time.monotonic()

    def start_heartbeats(self, interval: float) -> None:
        """Send a Heartbeat whenever nothing else went out for HeartBtInt, as the spec says EXI does."""
        if interval > 0 and self._heartbeats is None:
            self._heartbeats = asyncio.create_task(self._heartbeat_loop(interval))

    async def _heartbeat_loop(self, interval: float) -> None:
        while not self.writer.is_closing():
            delay = self.last_write + interval - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            elif self.state is not None:
                self.state.send("0", [])

    async def serve(self) -> None:
        parser = FixParser()
        try:
            while data := await self.reader.read(65536):
                for msg in parser.feed(data):
                    self.ex.on_message(self, msg)
                if self.writer.is_closing():
                    break
        except (OSError, FixParseError) as exc:
            log.info("connection error: %s", exc)
        finally:
            if self._heartbeats is not None:
                self._heartbeats.cancel()
            self.ex.on_connection_closed(self)
            self.writer.close()

    def close(self) -> None:
        self.writer.close()


class SimExchange:
    def __init__(self, md_port: int, oe_port: int, instruments: list[SimInstrument],
                 roles: dict[str, str], accounts: dict[str, dict[str, str]], rate_limit: int = 100,
                 seed: int | None = None, tick_interval: float = 0.2, taker_prob: float = 0.3,
                 host: str = "127.0.0.1"):
        self.host = host
        self.md_port, self.oe_port = md_port, oe_port
        self.roles = roles          # SenderCompID -> md / oe / dc
        self.accounts = accounts    # account -> {authorized user: capacity}
        self.rate_limit = rate_limit
        self.tick_interval = tick_interval
        self.taker_prob = taker_prob
        self.rng = random.Random(seed)
        self.markets = {i.symbol: Market(i, fair=round(40.0 / i.tick)) for i in instruments}
        self.sessions: dict[str, SessionState] = {}
        self.orders: dict[tuple[str, str], SimOrder] = {}  # (sender, ClOrdID) -> live order
        self.trades: list[tuple[str, SimOrder, int, float, str, str]] = []  # (sender, order, qty, px, TradeID, time)
        self._servers: list[asyncio.base_events.Server] = []
        self._conns: set[SimConnection] = set()
        self._ids = 0
        self._md_batch: dict[str, list[list[tuple[int, str]]]] = {}

    @classmethod
    def from_settings(cls, settings: Settings, **kwargs) -> "SimExchange":
        return cls(
            md_port=settings.md.port, oe_port=settings.oe.port,
            instruments=[SimInstrument(s) for s in settings.symbols],
            roles={settings.md.sender_comp_id: "md", settings.oe.sender_comp_id: "oe",
                   settings.dc.sender_comp_id: "dc"},
            accounts={settings.account: {settings.trader: settings.capacity}},
            rate_limit=settings.rate_limit, **kwargs)

    # ---- lifecycle -------------------------------------------------------------------------

    async def start(self) -> None:
        for gateway, port in (("md", self.md_port), ("oe", self.oe_port)):
            server = await asyncio.start_server(
                lambda r, w, g=gateway: self._accept(r, w, g), self.host, port)
            actual = server.sockets[0].getsockname()[1]
            if gateway == "md":
                self.md_port = actual
            else:
                self.oe_port = actual
            self._servers.append(server)
        for mkt in self.markets.values():
            self._refresh_bot_quotes(mkt)
        self._flush_md()
        log.info("ElectronX simulator: market data on %s:%d, order entry + drop copy on %s:%d, symbols %s",
                 self.host, self.md_port, self.host, self.oe_port, ", ".join(self.markets))

    async def _accept(self, reader, writer, gateway: str) -> None:
        conn = SimConnection(self, reader, writer, gateway)
        self._conns.add(conn)
        try:
            await conn.serve()
        finally:
            self._conns.discard(conn)

    async def run_market(self) -> None:
        while True:
            await asyncio.sleep(self.tick_interval)
            self.step()

    def step(self) -> None:
        for mkt in self.markets.values():
            r = self.rng.random()
            if r < 0.3:
                mkt.fair += 1
            elif r < 0.6:
                mkt.fair -= 1
            mkt.fair = max(10, min(mkt.fair, round(mkt.inst.high / mkt.inst.tick) - 10))
            self._refresh_bot_quotes(mkt)
            if mkt.inst.state == "OPEN" and self.rng.random() < self.taker_prob:
                side = self.rng.choice((BUY, SELL))
                limit = mkt.fair + (8 if side == BUY else -8)
                taker = self._new_order_obj(None, f"TAKER{self._next_id('')}", SYNTHETIC, "", mkt.inst.symbol,
                                            side, self.rng.randint(1, 2), limit, tif="3")
                self._match(mkt, taker)
        self._flush_md()

    async def close(self) -> None:
        for server in self._servers:
            server.close()
        for conn in list(self._conns):
            conn.writer.transport.abort()
        for server in self._servers:
            await server.wait_closed()

    def drop_connection(self, sender: str) -> None:
        """Test hook: kill a session's TCP connection without a Logout."""
        state = self.sessions.get(sender)
        if state is not None and state.conn is not None:
            state.conn.writer.transport.abort()

    # ---- session layer ---------------------------------------------------------------------

    def on_message(self, conn: SimConnection, msg: FixMessage) -> None:
        if conn.state is None:
            self._on_logon(conn, msg)
            return
        state = conn.state
        mt, seq = msg.msg_type, msg.get_int(34)
        if mt == "4":
            state.in_expected = msg.get_int(36)
            state.awaiting_gap_fill = False
            return
        if seq > state.in_expected:
            if mt == "2":
                self._replay(state, msg.get_int(7), msg.get_int(16))
            if not state.awaiting_gap_fill:
                state.awaiting_gap_fill = True
                state.send("2", [(7, str(state.in_expected)), (16, "0")])
            return
        if seq < state.in_expected:
            if msg.get(43) != "Y":
                self._logout(conn, f"MsgSeqNum too low, expecting {state.in_expected} but received {seq}")
            return
        state.in_expected += 1

        if mt == "0":
            return
        if mt == "1":
            state.send("0", [(112, msg.get(112, ""))])
        elif mt == "2":
            self._replay(state, msg.get_int(7), msg.get_int(16))
        elif mt == "5":
            conn.graceful = True
            state.send("5", [])
            conn.close()
        elif self._rate_limited(state, msg):
            pass
        elif state.role == "md":
            self._on_md_message(state, msg)
        elif state.role == "oe":
            self._on_oe_message(state, msg)
        else:
            self._business_reject(state, msg, 3, "Drop copy sessions are read only")

    def _on_logon(self, conn: SimConnection, msg: FixMessage) -> None:
        sender = msg.get(49, "")
        role = self.roles.get(sender)
        temp = SessionState(sender, "?")
        temp.conn = conn

        def refuse(text: str) -> None:
            log.warning("logon from %s refused: %s", sender, text)
            temp.send("5", [(58, text)])
            conn.close()

        if msg.msg_type != "A":
            return refuse("first message must be Logon")
        if role is None or (role == "md") != (conn.gateway == "md"):
            return refuse(f"unknown SenderCompID {sender} on this gateway")
        if 50 in msg:
            return refuse("SenderSubID(50) is not allowed on Logon")
        if msg.get(1137) != "9":
            return refuse("DefaultApplVerID(1137) must be 9 (FIX50SP2)")
        if not msg.get(554):
            return refuse("Password(554) required")

        fresh = sender not in self.sessions
        state = self.sessions.setdefault(sender, SessionState(sender, role))
        if state.conn is not None:
            return refuse("SenderCompID already has an active connection")

        seq = msg.get_int(34)
        reply_reset = msg.get(141) == "Y" or role == "md"
        gap = False
        if reply_reset:
            state.reset()
            state.in_expected = seq + 1
        elif fresh and seq != 1:
            # Simulator restarted while the client kept its numbers: tell it we reset.
            state.reset()
            state.in_expected = 2
            reply_reset = True
        elif seq < state.in_expected:
            return refuse(f"MsgSeqNum too low, expecting {state.in_expected} but received {seq}")
        elif seq > state.in_expected:
            gap = True
        else:
            state.in_expected = seq + 1

        state.conn = conn
        conn.state = state
        body = [(98, "0"), (108, msg.get(108, "30"))] + ([(141, "Y")] if reply_reset else []) + [(1137, "9")]
        state.send("A", body)
        conn.start_heartbeats(float(msg.get(108, "30")))
        log.info("%s logged on (%s)", sender, role)
        if gap:
            state.awaiting_gap_fill = True
            state.send("2", [(7, str(state.in_expected)), (16, "0")])

    def _replay(self, state: SessionState, begin: int, end: int) -> None:
        last = state.out_seq - 1 if end == 0 else min(end, state.out_seq - 1)
        gap_start = None
        for s in range(begin, last + 1):
            if s in state.history:
                if gap_start is not None:
                    state.send("4", [(36, str(s)), (123, "Y")], seq=gap_start, poss_dup=True)
                    gap_start = None
                mt, body = state.history[s]
                state.send(mt, body, seq=s, poss_dup=True)
            elif gap_start is None:
                gap_start = s
        if gap_start is not None:
            state.send("4", [(36, str(last + 1)), (123, "Y")], seq=gap_start, poss_dup=True)

    def _logout(self, conn: SimConnection, text: str) -> None:
        if conn.state is not None:
            conn.state.send("5", [(58, text)])
        conn.graceful = True
        conn.close()

    def on_connection_closed(self, conn: SimConnection) -> None:
        state = conn.state
        if state is None or state.conn is not conn:
            return
        state.conn = None
        log.info("%s disconnected (%s)", state.sender, "logout" if conn.graceful else "TCP loss")
        if state.role == "oe" and not conn.graceful:
            self._cancel_on_disconnect(state)
        if state.role == "md":
            state.md_reqs.clear()

    def _rate_limited(self, state: SessionState, msg: FixMessage) -> bool:
        now = time.monotonic()
        while state.recent and now - state.recent[0] >= 1.0:
            state.recent.popleft()
        state.recent.append(now)
        if len(state.recent) <= self.rate_limit:
            return False
        self._business_reject(state, msg, 0, f"Message rate limit throttled for session {BEGIN}:{state.sender}->EXI")
        return True

    def _business_reject(self, state: SessionState, msg: FixMessage, reason: int, text: str) -> None:
        ref_id = msg.get(11) or msg.get(262) or msg.get(320) or ""
        state.send("j", [(45, msg.get(34, "0")), (58, text), (372, msg.msg_type), (379, ref_id), (380, str(reason))])

    def _session_reject(self, state: SessionState, msg: FixMessage, tag: int, reason: int, text: str) -> None:
        state.send("3", [(45, msg.get(34, "0")), (58, text), (371, str(tag)), (372, msg.msg_type), (373, str(reason))])

    # ---- market data gateway ---------------------------------------------------------------

    def _on_md_message(self, state: SessionState, msg: FixMessage) -> None:
        if msg.msg_type == "V":
            symbols = [e[55] for e in msg.groups(146, 55)] or [msg.get(55, "")]
            req_id, sub_type = msg.get(262, ""), msg.get(263, "0")
            for symbol in symbols:
                mkt = self.markets.get(symbol)
                if mkt is None:
                    self._business_reject(state, msg, 2, f"Unknown security {symbol}")
                elif sub_type == "2":
                    state.md_reqs.pop(symbol, None)
                else:
                    self._send_snapshot(state, mkt, req_id)
                    if sub_type == "1":
                        state.md_reqs[symbol] = req_id
        elif msg.msg_type == "x":
            if msg.get(559) == "4":
                insts = [m.inst for m in self.markets.values()]
            else:
                insts = [m.inst for s, m in self.markets.items() if s == msg.get(55)]
            if msg.get(336):  # TradingSessionID filters on instrument state
                insts = [i for i in insts if i.state == msg[336]]
            body: list[tuple[int, str]] = [(146, str(len(insts)))]
            for i in insts:
                body += [(55, i.symbol), (48, i.symbol), (22, "8"), (460, "14"), (167, "EVENT"), (231, "1"),
                         (864, "1"), (865, "5"), (866, datetime.now(timezone.utc).strftime("%Y%m%d")),
                         (868, "StartDate"), (969, str(i.tick)), (1151, i.symbol.rsplit("-", 2)[0]),
                         (562, str(i.lot)), (1306, "0"), (1148, f"{i.low:.2f}"), (1149, f"{i.high:.2f}"), (15, "USD")]
            body += [(320, msg.get(320, "")), (322, self._next_id("S")), (560, "0" if insts else "2"), (893, "Y")]
            state.send("y", body)
        else:
            self._business_reject(state, msg, 3, "Unsupported Message Type")

    def _md_entry(self, o: SimOrder, action: str) -> list[tuple[int, str]]:
        d, t = _ts_parts()
        size = 0 if action == "2" else o.leaves
        return [(279, action), (269, "0" if o.side == BUY else "1"), (278, o.order_id), (55, o.symbol),
                (48, o.symbol), (22, "8"), (167, "EVENT"), (270, self.markets[o.symbol].inst.fmt(o.px)),
                (271, str(size)), (272, d), (273, t), (336, self.markets[o.symbol].inst.state)]

    def _md(self, symbol: str, entry: list[tuple[int, str]]) -> None:
        self._md_batch.setdefault(symbol, []).append(entry)

    def _flush_md(self) -> None:
        batch, self._md_batch = self._md_batch, {}
        for symbol, entries in batch.items():
            for state in self.sessions.values():
                req_id = state.md_reqs.get(symbol)
                if req_id is None or state.conn is None:
                    continue
                body: list[tuple[int, str]] = [(262, req_id), (268, str(len(entries)))]
                for entry in entries:
                    body += entry
                state.send("X", body)

    def _send_snapshot(self, state: SessionState, mkt: Market, req_id: str) -> None:
        sym = mkt.inst.symbol
        orders = sorted(mkt.resting(), key=lambda o: (o.side, -o.px if o.side == BUY else o.px))
        body: list[tuple[int, str]] = [(22, "8"), (48, sym), (55, sym), (167, "EVENT"), (262, req_id),
                                       (268, str(len(orders)))]
        d, t = _ts_parts()
        for o in orders:
            body += [(269, "0" if o.side == BUY else "1"), (270, mkt.inst.fmt(o.px)), (271, str(o.leaves)),
                     (272, d), (273, t), (278, o.order_id), (336, mkt.inst.state)]
        body.append((1151, sym.rsplit("-", 2)[0]))
        state.send("W", body)

    # ---- order entry gateway ---------------------------------------------------------------

    def _on_oe_message(self, state: SessionState, msg: FixMessage) -> None:
        mt = msg.msg_type
        if mt in ("D", "F", "G", "q") and not msg.get(50):
            self._business_reject(state, msg, 5, "SenderSubID(50) required on order messages")
        elif mt == "D":
            self._on_new_order(state, msg)
        elif mt == "F":
            self._on_cancel(state, msg)
        elif mt == "G":
            self._on_replace(state, msg)
        elif mt == "q":
            self._on_mass_cancel(state, msg)
        elif mt == "AD":
            self._on_trade_capture_request(state, msg)
        else:
            self._business_reject(state, msg, 3, "Unsupported Message Type")
        self._flush_md()

    def _on_new_order(self, state: SessionState, msg: FixMessage) -> None:
        for tag in (1, 11, 38, 40, 54, 55, 582, 1028):
            if tag not in msg:
                return self._session_reject(state, msg, tag, 1, "Required tag missing")
        if not msg[38].isdigit() or int(msg[38]) <= 0:
            return self._session_reject(state, msg, 38, 6, "Incorrect data format for value")
        if msg[54] not in (BUY, SELL):
            return self._session_reject(state, msg, 54, 5, "Value is incorrect (out of range) for this tag")
        if msg[1028] not in ("Y", "N"):
            return self._session_reject(state, msg, 1028, 5, "Value is incorrect (out of range) for this tag")
        if 44 not in msg:
            return self._session_reject(state, msg, 44, 1, "Required tag missing")

        mkt = self.markets.get(msg[55])
        qty, cl = int(msg[38]), msg[11]
        price_ticks = mkt.inst.ticks(float(msg[44])) if mkt else None
        order = self._new_order_obj(state, cl, msg[1], msg.get(50, ""), msg[55], msg[54], qty,
                                    price_ticks or 0, tif=msg.get(59, "0"), capacity=msg[582], manual=msg[1028])

        users = self.accounts.get(msg[1])
        if mkt is None:
            return self._reject_order(order, 1, f"Unknown symbol {msg[55]}")
        if qty % mkt.inst.lot:
            return self._session_reject(state, msg, 38, 5, f"OrderQty must be a multiple of {mkt.inst.lot}")
        if users is None:
            return self._reject_order(order, 15, f"Unknown account {msg[1]}")
        if msg[50] not in users:
            return self._reject_order(order, 99, f"SenderSubID {msg[50]} is not an authorized trading user for {msg[1]}")
        if msg[582] != users[msg[50]]:
            return self._reject_order(order, 99, f"CustOrderCapacity must be {users[msg[50]]} for {msg[50]}")
        if msg[40] != "2" or order.tif not in ("0", "1", "3", "4"):
            return self._reject_order(order, 11, "Unsupported order characteristic")
        if price_ticks is None:
            return self._reject_order(order, 18, f"Invalid price increment (tick size {mkt.inst.tick})")
        if not mkt.inst.low <= float(msg[44]) <= mkt.inst.high:
            return self._reject_order(order, 3, f"Price outside {mkt.inst.low}..{mkt.inst.high}")
        if mkt.inst.state != "OPEN":
            return self._reject_order(order, 2, f"Book is {mkt.inst.state}")
        if (state.sender, cl) in self.orders:
            return self._reject_order(order, 6, "Duplicate order (ClOrdID)")

        self._report(order, "0")
        if order.tif == "4" and self._available(mkt, order) < qty:
            order.status = "C"
            return self._report(order, "C")
        self._match(mkt, order)

    def _on_cancel(self, state: SessionState, msg: FixMessage) -> None:
        order = self._find(state, msg.get(41, ""))
        if order is None or not order.live or order.symbol != msg.get(55):
            return self._cancel_reject(state, msg, "1", 1, "Unknown order")
        self._remove(order)
        order.status = "4"
        self.orders.pop((state.sender, order.cl_ord_id), None)
        order.prev_cl_ord_id, order.cl_ord_id = order.cl_ord_id, msg.get(11, "")
        self._report(order, "4", orig=msg.get(41, ""))

    def _on_replace(self, state: SessionState, msg: FixMessage) -> None:
        order = self._find(state, msg.get(41, ""))
        if order is None or not order.live or order.symbol != msg.get(55):
            return self._cancel_reject(state, msg, "2", 1, "Unknown order")
        mkt = self.markets[order.symbol]
        new_qty = int(msg.get_float(38, order.qty))
        new_px = mkt.inst.ticks(msg.get_float(44)) if 44 in msg else order.px
        if new_px is None:
            return self._cancel_reject(state, msg, "2", 18, "Invalid price increment (tick size)")
        if new_qty < order.cum_qty or new_qty <= 0 or new_qty % mkt.inst.lot:
            return self._cancel_reject(state, msg, "2", 99, "Invalid order quantity")

        # Price changes and quantity increases lose time priority; decreases keep it.
        loses_priority = new_px != order.px or new_qty > order.qty
        if loses_priority:
            self._remove(order)
        self.orders.pop((state.sender, order.cl_ord_id), None)
        order.prev_cl_ord_id, order.cl_ord_id = order.cl_ord_id, msg.get(11, "")
        order.px, order.qty = new_px, new_qty
        order.status = "2" if order.cum_qty >= order.qty else ("1" if order.cum_qty else "0")
        self._report(order, "5", orig=msg.get(41, ""))
        if not order.live:
            return
        self.orders[(state.sender, order.cl_ord_id)] = order
        if loses_priority:
            self._match(mkt, order)
        else:
            self._md(order.symbol, self._md_entry(order, "1"))

    def _on_mass_cancel(self, state: SessionState, msg: FixMessage) -> None:
        req_type = msg.get(530)
        body = [(11, msg.get(11, "")), (530, req_type or "")]
        if req_type not in ("1", "7") or (req_type == "1" and not msg.get(55)):
            state.send("r", body + [(531, "0"), (532, "99"), (58, "Unsupported mass cancel request")])
            return
        count = 0
        for (sender, _), order in list(self.orders.items()):
            if sender != state.sender or (req_type == "1" and order.symbol != msg.get(55)):
                continue
            if msg.get(1) and order.account != msg.get(1):
                continue
            self._remove(order)
            order.status = "4"
            # As ElectronX demo sends them: the mass cancel's ClOrdID in 11, the order's in 41, and 378=8.
            self._report(order, "4", text="Mass cancel", cl_ord_id=msg.get(11, ""), orig=order.cl_ord_id,
                         restatement="8")
            self.orders.pop((sender, order.cl_ord_id), None)
            count += 1
        state.send("r", body + [(531, req_type), (60, utc_timestamp())])
        log.info("mass cancel for %s: %d orders", state.sender, count)

    def _cancel_on_disconnect(self, state: SessionState) -> None:
        count = 0
        for (sender, cl), order in list(self.orders.items()):
            if sender == state.sender and order.tif == "0":
                self._remove(order)
                order.status = "4"
                self._report(order, "4", text="Cancel on disconnect", restatement="8")
                del self.orders[(sender, cl)]
                count += 1
        self._flush_md()
        if count:
            log.info("Cancel On Disconnect: %d orders canceled for %s", count, state.sender)

    def _cancel_reject(self, state: SessionState, msg: FixMessage, response_to: str, reason: int, text: str) -> None:
        order = self._find(state, msg.get(41, ""))
        state.send("9", [(11, msg.get(11, "")), (37, order.order_id if order else "NONE"), (39, "8"),
                         (41, msg.get(41, "")), (58, text), (102, str(reason)), (434, response_to)])

    def _find(self, state: SessionState, cl_ord_id: str) -> SimOrder | None:
        order = self.orders.get((state.sender, cl_ord_id))
        if order is None:  # one generation stale is still resolvable (spec: Order Modification)
            order = next((o for (s, _), o in self.orders.items()
                          if s == state.sender and o.prev_cl_ord_id == cl_ord_id), None)
        return order

    # ---- matching --------------------------------------------------------------------------

    def _new_order_obj(self, owner: SessionState | None, cl: str, account: str, trader: str, symbol: str,
                       side: str, qty: int, px: int, tif: str = "0", capacity: str = "2",
                       manual: str = "N") -> SimOrder:
        return SimOrder(order_id=self._next_id("O"), cl_ord_id=cl, owner=owner, account=account, trader=trader,
                        symbol=symbol, side=side, qty=qty, px=px, tif=tif, capacity=capacity, manual=manual)

    def _available(self, mkt: Market, order: SimOrder) -> int:
        opp = mkt.asks if order.side == BUY else mkt.bids
        return sum(o.leaves for p, q in opp.items() for o in q
                   if (p <= order.px if order.side == BUY else p >= order.px))

    def _match(self, mkt: Market, incoming: SimOrder) -> None:
        opp = mkt.asks if incoming.side == BUY else mkt.bids
        while incoming.leaves > 0 and opp:
            best = min(opp) if incoming.side == BUY else max(opp)
            if (incoming.side == BUY and best > incoming.px) or (incoming.side == SELL and best < incoming.px):
                break
            queue = opp[best]
            resting = queue[0]
            qty = min(incoming.leaves, resting.leaves)
            trade_id = self._next_id("T")
            self._fill(resting, qty, best, trade_id, aggressor=False)
            self._fill(incoming, qty, best, trade_id, aggressor=True)
            d, t = _ts_parts()
            self._md(mkt.inst.symbol, self._md_entry(resting, "2" if not resting.leaves else "1"))
            self._md(mkt.inst.symbol, [(279, "0"), (269, "2"), (278, trade_id + "P"), (55, mkt.inst.symbol),
                                       (48, mkt.inst.symbol), (22, "8"), (167, "EVENT"), (270, mkt.inst.fmt(best)),
                                       (271, str(qty)), (272, d), (273, t), (1003, trade_id),
                                       (2446, incoming.side)])
            if not resting.leaves:
                queue.pop(0)
                if not queue:
                    del opp[best]
                self._forget(resting)
        if incoming.leaves > 0 and incoming.tif in ("3", "4"):
            incoming.status = "C"
            self._report(incoming, "C")
            self._forget(incoming)
        elif incoming.leaves > 0:
            mkt.side_book(incoming.side).setdefault(incoming.px, []).append(incoming)
            self._md(mkt.inst.symbol, self._md_entry(incoming, "0"))
            if incoming.owner is not None:
                self.orders[(incoming.owner.sender, incoming.cl_ord_id)] = incoming
        else:
            self._forget(incoming)

    def _fill(self, order: SimOrder, qty: int, px: int, trade_id: str, aggressor: bool) -> None:
        price = px * self.markets[order.symbol].inst.tick
        order.cum_qty += qty
        order.notional += qty * price
        order.status = "2" if order.cum_qty >= order.qty else "1"
        if order.owner is not None:
            self.trades.append((order.owner.sender, order, qty, price, trade_id, utc_timestamp()))
        self._report(order, "F", last_qty=qty, last_px=price, trade_id=trade_id, aggressor=aggressor)

    def _on_trade_capture_request(self, state: SessionState, msg: FixMessage) -> None:
        """TradeCaptureReportRequest [AD]: AQ ack, one AE per fill in the window, then a final AQ (spec p.40)."""
        start, end = msg.get(9593, ""), msg.get(9594, "")
        ack = [(263, "0"), (568, msg.get(568, "")), (569, msg.get(569, "0")), (749, "0")]
        state.send("AQ", ack + [(750, "0")])
        for sender, o, qty, price, trade_id, ts in self.trades:
            if sender != state.sender or (start and ts < start) or (end and ts >= end):
                continue
            state.send("AE", [(22, "8"), (31, f"{price:.2f}"), (32, str(qty)), (48, o.symbol), (55, o.symbol),
                              (60, ts), (75, ts[:8]), (487, "0"), (552, "1"), (54, o.side), (11, o.cl_ord_id),
                              (37, o.order_id), (453, "1"), (448, f"firms/{o.account}/accounts/{o.account}"),
                              (447, "D"), (452, "12"), (571, trade_id), (828, "0"), (880, trade_id)])
        state.send("AQ", ack + [(750, "1")])

    def _remove(self, order: SimOrder) -> None:
        book = self.markets[order.symbol].side_book(order.side)
        queue = book.get(order.px, [])
        if order in queue:
            queue.remove(order)
            if not queue:
                del book[order.px]
            self._md(order.symbol, self._md_entry(order, "2"))

    def _forget(self, order: SimOrder) -> None:
        if order.owner is not None:
            self.orders.pop((order.owner.sender, order.cl_ord_id), None)

    def _refresh_bot_quotes(self, mkt: Market) -> None:
        """Keep five synthetic orders a side around fair value (they can trade with client orders).

        A book that is not OPEN stays empty, like a CLOSED book on demo."""
        wanted = {(BUY, mkt.fair - 2 - i) for i in range(5)} | {(SELL, mkt.fair + 2 + i) for i in range(5)}
        if mkt.inst.state != "OPEN":
            wanted = set()
        for key, order in list(mkt.bot_orders.items()):
            if key not in wanted or not order.live:
                if order.live:
                    self._remove(order)
                    order.status = "4"
                del mkt.bot_orders[key]
        for side, px in sorted(wanted - mkt.bot_orders.keys()):
            order = self._new_order_obj(None, f"BOT{self._next_id('')}", SYNTHETIC, "", mkt.inst.symbol,
                                        side, self.rng.randint(1, 10), px)
            mkt.bot_orders[(side, px)] = order
            self._match(mkt, order)

    # ---- execution reports -----------------------------------------------------------------

    def _reject_order(self, order: SimOrder, reason: int, text: str) -> None:
        order.status = "8"
        self._report(order, "8", text=text, rej_reason=reason)

    def _report(self, o: SimOrder, exec_type: str, *, orig: str = "", last_qty: int = 0, last_px: float = 0.0,
                text: str = "", trade_id: str = "", aggressor: bool | None = None, restatement: str = "",
                rej_reason: int | None = None, cl_ord_id: str = "") -> None:
        if o.owner is None:
            return  # synthetic liquidity has nobody to report to
        inst = self.markets[o.symbol].inst if o.symbol in self.markets else SimInstrument(o.symbol)
        avg = o.notional / o.cum_qty if o.cum_qty else 0.0
        body: list[tuple[int, str]] = [
            (1, o.account), (6, f"{avg:.2f}"), (11, cl_ord_id or o.cl_ord_id), (14, str(o.cum_qty)),
            (17, self._next_id("E")),
            (22, "8"), (31, f"{last_px:.2f}"), (32, str(last_qty)), (37, o.order_id), (38, str(o.qty)),
            (39, o.status), (40, "2")]
        if orig:
            body.append((41, orig))
        body += [(44, inst.fmt(o.px)), (48, o.symbol), (54, o.side), (55, o.symbol)]
        if text:
            body.append((58, text))
        body += [(59, o.tif), (60, utc_timestamp())]
        if rej_reason is not None:
            body.append((103, str(rej_reason)))
        if trade_id:
            body.append((119, f"{last_px * last_qty:.2f}"))
        body += [(150, exec_type), (151, str(o.leaves))]
        if restatement:
            body.append((378, restatement))
        if o.cum_qty:
            body.append((381, f"{o.notional:.2f}"))
        body += [(581, "1"), (582, o.capacity)]
        if trade_id:
            body += [(828, "0"), (880, trade_id)]
        body.append((1028, o.manual))
        if aggressor is not None:
            body.append((1057, "Y" if aggressor else "N"))
        o.owner.send("8", body)
        for state in self.sessions.values():
            if state.role == "dc":
                state.send("8", list(body))

    def _next_id(self, prefix: str) -> str:
        self._ids += 1
        n, digits = self._ids, ""
        while n:
            n, r = divmod(n, 36)
            digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[r] + digits
        return f"{prefix}{digits.rjust(12 - len(prefix), '0')}"
