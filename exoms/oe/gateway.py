"""Order entry gateway: risk + throttle in front of the FIX session, ElectronX tag rules,
and the order state machine driven by ExecutionReports."""
from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import Callable

from ..config import Settings
from ..fix.codec import FixMessage
from ..fix.session import FixSession
from ..journal import Journal
from ..latency import LatencyStats
from ..md.instruments import Instrument, price_decimals
from ..risk.checks import RiskManager
from .orders import BUY, FIX_ORD_STATUS, SELL, Order, OrdStatus
from .throttle import Throttle

log = logging.getLogger("oe")

OrderListener = Callable[[Order, str], None]  # (order, ExecType(150) or local event name)


class OrderRejected(Exception):
    """Blocked locally (risk, throttle or session state) before reaching the exchange."""


def _base36(n: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while n:
        n, r = divmod(n, 36)
        out = digits[r] + out
    return out or "0"


class OrderGateway:
    SENT_HISTORY = 2000  # outbound seq -> order, to resolve Reject [3] by RefSeqNum(45)

    def __init__(self, settings: Settings, risk: RiskManager, throttle: Throttle, journal: Journal,
                 latency: LatencyStats | None = None, instruments: dict[str, Instrument] | None = None):
        self.settings = settings
        self.risk = risk
        self.throttle = throttle
        self.journal = journal
        self.latency = latency or LatencyStats()
        self.instruments = instruments if instruments is not None else {}
        self.session: FixSession | None = None
        self.hold_new_orders = False  # set by the engine until its startup mass cancel has gone out
        self.reference_prices: dict[str, float] = {}  # latest mid per symbol, for the price band check
        self._by_id: dict[str, Order] = {}
        self._live: set[Order] = set()
        self._fills: dict[str, tuple[str, str, int]] = {}  # ExecID -> (symbol, side, qty), for busts
        self._seen_exec_ids: set[str] = set()
        self._sent: OrderedDict[int, tuple[Order | None, str, str]] = OrderedDict()
        self._mass_cancel_ids: set[str] = set()  # ClOrdIDs of our OrderMassCancelRequests
        self._listeners: list[OrderListener] = []
        # ClOrdIDs must be unique for the whole (week-long) session: prefix with the start time.
        self._id_prefix = _base36(int(time.time()))
        self._id_seq = 0

    @property
    def ready(self) -> bool:
        s = self.session
        return s is not None and s.logged_on and not s.recovering

    def subscribe(self, listener: OrderListener) -> None:
        self._listeners.append(listener)

    def live_orders(self) -> list[Order]:
        return list(self._live)

    def own_order_ids(self, symbol: str) -> set[str]:
        """Exchange OrderIDs of our resting orders, which appear as MDEntryIDs in market data."""
        return {o.order_id for o in self._live if o.symbol == symbol and o.order_id}

    def restore(self, event: dict) -> None:
        """Replay a fill or bust from this week's journal at startup."""
        if event["kind"] == "fill":
            self._apply_fill(event["exec_id"], event["symbol"], event["side"], event["qty"])
        elif event["kind"] == "bust":
            self._apply_bust(event["exec_id"], event["exec_ref_id"])

    # ---- order actions ---------------------------------------------------------------------

    def new_order(self, symbol: str, side: str, qty: int, price: float, *,
                  manual: bool = False, t0_ns: int | None = None) -> Order:
        self._require_ready()
        reason = self.risk.check(symbol, side, qty, price, self._live, self.reference_prices.get(symbol),
                                 self.instruments.get(symbol))
        if reason:
            raise OrderRejected(reason)
        if not self.throttle.try_acquire(priority=False):
            raise OrderRejected("throttled")

        order = Order(cl_ord_id=self._next_id(), symbol=symbol, side=side, qty=qty, price=price,
                      manual=manual, created_ns=time.perf_counter_ns())
        order.history.append(order.cl_ord_id)
        # Spec Table 15. 582 and 1028 are required on NewOrderSingle only.
        self._send("D", order, order.cl_ord_id, [
            (11, order.cl_ord_id), (1, self.settings.account), (55, symbol), (54, side),
            (38, str(qty)), (40, "2"), (44, self._px(symbol, price)), (59, "0"),
            (582, self.settings.capacity), (1028, "Y" if manual else "N"),
        ], t0_ns)
        self._by_id[order.cl_ord_id] = order
        self._live.add(order)
        self.journal.write("new", cl_ord_id=order.cl_ord_id, symbol=symbol, side=side, qty=qty,
                           price=price, manual=manual)
        return order

    def replace(self, order: Order, price: float, qty: int | None = None, *, t0_ns: int | None = None) -> None:
        """OrderCancelReplaceRequest [G]: one message to move a quote instead of cancel + new."""
        self._require_ready()
        if not order.is_live or order.is_pending:
            raise OrderRejected(f"{order.cl_ord_id} cannot be replaced while {order.status.value}")
        qty = order.qty if qty is None else qty
        reason = self.risk.check(order.symbol, order.side, qty, price, self._live,
                                 self.reference_prices.get(order.symbol), self.instruments.get(order.symbol),
                                 replacing=order)
        if reason:
            raise OrderRejected(reason)
        if not self.throttle.try_acquire(priority=False):
            raise OrderRejected("throttled")
        new_id = self._next_id()
        self._send("G", order, new_id, [
            (11, new_id), (41, order.cl_ord_id), (1, self.settings.account), (55, order.symbol),
            (54, order.side), (40, "2"), (38, str(qty)), (44, self._px(order.symbol, price)), (59, "0"),
        ], t0_ns)
        order.pending_cl_ord_id = new_id
        self._by_id[new_id] = order

    def cancel(self, order: Order) -> bool:
        """Cancels use the reserved throttle headroom and are allowed even when the kill switch is on."""
        if not self.ready or not order.is_live or order.pending_cl_ord_id:
            return False
        if not self.throttle.try_acquire(priority=True):
            log.error("throttle full, cannot cancel %s", order.cl_ord_id)
            return False
        new_id = self._next_id()
        self._send("F", order, new_id, [(11, new_id), (41, order.cl_ord_id), (55, order.symbol)])
        order.pending_cl_ord_id = new_id
        self._by_id[new_id] = order
        return True

    def mass_cancel(self, reason: str) -> bool:
        """OrderMassCancelRequest [q] with no filters: every order on this session, including ones
        this process doesn't know about (e.g. left over from a previous run)."""
        s = self.session
        if s is None or not s.logged_on or not self.throttle.try_acquire(priority=True):
            return False
        req_id = self._next_id()
        self._mass_cancel_ids.add(req_id)
        self._send("q", None, req_id, [(11, req_id), (530, "7")])
        log.warning("mass cancel sent (%s)", reason)
        return True

    def cancel_all(self, reason: str) -> int:
        if self.mass_cancel(reason):
            return len(self._live)
        sent = sum(self.cancel(order) for order in list(self._live))
        if sent:
            log.warning("cancel all (%s): %d cancels sent", reason, sent)
        return sent

    def _send(self, msg_type: str, order: Order | None, cl_ord_id: str, body: list[tuple[int, str]],
              t0_ns: int | None = None) -> None:
        # SenderSubID(50) = authorized trading user, required on every order-related message.
        seq = self.session.send(msg_type, body, header=[(50, self.settings.trader)])
        if t0_ns is not None:
            self.latency.record(time.perf_counter_ns() - t0_ns)
        self._sent[seq] = (order, msg_type, cl_ord_id)
        while len(self._sent) > self.SENT_HISTORY:
            self._sent.popitem(last=False)

    def _require_ready(self) -> None:
        if not self.ready:
            raise OrderRejected("order session not logged on")
        if self.hold_new_orders:
            raise OrderRejected("waiting for startup mass cancel")

    def _next_id(self) -> str:
        self._id_seq += 1
        return f"{self._id_prefix}-{self._id_seq}"

    def _px(self, symbol: str, price: float) -> str:
        inst = self.instruments.get(symbol)
        tick = inst.tick_size if inst and inst.tick_size else self.settings.strategy.tick_size
        return f"{price:.{price_decimals(tick)}f}"

    # ---- SessionApp ------------------------------------------------------------------------

    def on_logon(self, session: FixSession) -> None:
        self.session = session

    def on_disconnect(self, session: FixSession) -> None:
        # Cancel On Disconnect pulls our DAY orders on a TCP drop, but not on a graceful logout.
        # Either way the outcome arrives as ExecutionReports replayed after we log back on, so keep
        # the orders and let the replay settle them. Anything in flight is no longer in flight.
        for order in self._live:
            order.pending_cl_ord_id = ""
        if self._live:
            log.warning("order session down with %d live orders; waiting for replay on reconnect", len(self._live))

    def on_app_message(self, session: FixSession, msg: FixMessage) -> None:
        mt = msg.msg_type
        if mt == "8":
            self._on_execution_report(msg)
        elif mt == "9":
            self._on_cancel_reject(msg)
        elif mt == "j":
            self._on_rejected_message(msg.get(372, ""), msg.get(379, ""), msg.get(58, ""), "business reject")
        elif mt == "3":
            order, ref_type, cl_ord_id = self._sent.get(msg.get_int(45), (None, msg.get(372, ""), ""))
            self._on_rejected_message(ref_type, cl_ord_id, msg.get(58, ""), f"session reject (tag {msg.get(371)})")
        elif mt == "r":
            if msg.get(531) == "0":
                log.error("mass cancel rejected: %s; cancelling individually", msg.get(58))
                for order in list(self._live):
                    self.cancel(order)
            else:
                log.info("mass cancel accepted")

    def _on_execution_report(self, msg: FixMessage) -> None:
        cl_ord_id = msg.get(11, "")
        exec_type = msg.get(150, "")
        order = self._by_id.get(cl_ord_id) or self._by_id.get(msg.get(41, ""))
        # ElectronX demo reports each order a mass cancel pulled with ClOrdID(11) = the mass cancel's
        # ID, OrigClOrdID(41) = the order's, and ExecRestatementReason(378) = 8, as on Cancel On Disconnect.
        mass_cancel_id = cl_ord_id if exec_type == "4" and cl_ord_id in self._mass_cancel_ids else ""
        if mass_cancel_id:
            cl_ord_id = msg.get(41, "") or cl_ord_id

        if exec_type == "H":
            self._apply_bust(msg.get(17, ""), msg.get(19, ""), msg.get(58, ""))
            return
        if order is None:
            # e.g. replayed after a restart, or left over from a previous run: still count its fills.
            if exec_type == "F" and msg.get(1) == self.settings.account:
                self._record_fill(msg, msg.get(55, ""), msg.get(54, ""), cl_ord_id)
            else:
                log.info("execution report for untracked order %s (ExecType %s)", cl_ord_id, exec_type)
            return

        order.order_id = msg.get(37, order.order_id)
        if 14 in msg:
            order.cum_qty = int(msg.get_float(14))
        if 6 in msg:
            order.avg_px = msg.get_float(6)
        if 58 in msg:
            order.text = msg[58]
        if exec_type == "5":  # replaced: adopt the new ClOrdID, price and qty
            order.cl_ord_id = cl_ord_id
            order.history.append(cl_ord_id)
            order.price = msg.get_float(44, order.price)
            order.qty = int(msg.get_float(38, order.qty))
        if exec_type in ("4", "5") or cl_ord_id == order.pending_cl_ord_id:
            order.pending_cl_ord_id = ""
        status = FIX_ORD_STATUS.get(msg.get(39, ""))
        if status is not None:
            order.status = status

        if exec_type == "F":
            self._record_fill(msg, order.symbol, order.side, order.cl_ord_id)
        elif exec_type == "8":
            log.error("order %s rejected (OrdRejReason %s): %s", cl_ord_id, msg.get(103), order.text)
        elif mass_cancel_id:
            log.info("order %s canceled by mass cancel %s", order.cl_ord_id, mass_cancel_id)
        elif exec_type == "4" and msg.get(378) == "8":
            log.warning("order %s canceled by Cancel On Disconnect", order.cl_ord_id)

        if order.is_live:
            self._notify(order, exec_type)
        else:
            self._finish(order, exec_type)

    def _record_fill(self, msg: FixMessage, symbol: str, side: str, cl_ord_id: str) -> None:
        exec_id, last_qty, last_px = msg.get(17, ""), int(msg.get_float(32)), msg.get_float(31)
        if self._apply_fill(exec_id, symbol, side, last_qty):
            self.journal.write("fill", exec_id=exec_id, cl_ord_id=cl_ord_id, symbol=symbol, side=side,
                               qty=last_qty, price=last_px, trade_id=msg.get(880), aggressor=msg.get(1057))
            log.info("FILL %s %d %s @ %s (position %+d)", "BUY" if side == BUY else "SELL", last_qty, symbol,
                     last_px, self.risk.positions[symbol])

    def _apply_fill(self, exec_id: str, symbol: str, side: str, qty: int) -> bool:
        if exec_id in self._seen_exec_ids:
            return False  # duplicate, e.g. PossDup replay
        self._seen_exec_ids.add(exec_id)
        self._fills[exec_id] = (symbol, side, qty)
        self.risk.on_fill(symbol, side, qty)
        return True

    def _apply_bust(self, exec_id: str, exec_ref_id: str, text: str = "") -> None:
        if exec_id in self._seen_exec_ids:
            return
        self._seen_exec_ids.add(exec_id)
        fill = self._fills.pop(exec_ref_id, None)
        if fill is None:
            log.error("trade bust %s for unknown fill %s", exec_id, exec_ref_id)
            return
        symbol, side, qty = fill
        self.risk.on_fill(symbol, SELL if side == BUY else BUY, qty)
        self.journal.write("bust", exec_id=exec_id, exec_ref_id=exec_ref_id, text=text)
        log.warning("TRADE BUST of %s (%s): position %s now %+d", exec_ref_id, text, symbol, self.risk.positions[symbol])

    def _on_cancel_reject(self, msg: FixMessage) -> None:
        order = self._by_id.get(msg.get(41, "")) or self._by_id.get(msg.get(11, ""))
        if order is None:
            return
        order.pending_cl_ord_id = ""
        log.warning("%s for %s rejected (CxlRejReason %s): %s", "cancel" if msg.get(434) == "1" else "replace",
                    order.cl_ord_id, msg.get(102), msg.get(58))
        if msg.get(102) == "1" and order.is_live:  # unknown order: the exchange no longer has it
            order.status = OrdStatus.CANCELED
            order.text = "unknown to exchange"
            self._finish(order, "unknown")
        else:
            self._notify(order, "cancel_reject")

    def _on_rejected_message(self, ref_msg_type: str, cl_ord_id: str, text: str, kind: str) -> None:
        """Reject [3] or BusinessMessageReject [j] (e.g. rate limit) for one of our order messages."""
        log.error("%s of %s %s: %s", kind, ref_msg_type, cl_ord_id or "?", text)
        order = self._by_id.get(cl_ord_id)
        if order is None:
            return
        if ref_msg_type == "D" and order.status == OrdStatus.PENDING_NEW:
            order.status = OrdStatus.REJECTED
            order.text = text
            self._finish(order, "rejected")
        elif order.pending_cl_ord_id == cl_ord_id:
            order.pending_cl_ord_id = ""
            self._by_id.pop(cl_ord_id, None)
            self._notify(order, "cancel_reject")

    def _finish(self, order: Order, event: str) -> None:
        self._live.discard(order)
        for cl_ord_id in (*order.history, order.pending_cl_ord_id):
            self._by_id.pop(cl_ord_id, None)
        self.journal.write("done", cl_ord_id=order.cl_ord_id, status=order.status.value, text=order.text)
        self._notify(order, event)

    def _notify(self, order: Order, event: str) -> None:
        for listener in self._listeners:
            listener(order, event)
