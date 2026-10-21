"""Market data session app: loads reference data, subscribes, and maintains one OrderBook per symbol."""
from __future__ import annotations

import logging
import time
from typing import Callable

from ..fix.codec import FixMessage
from ..fix.session import FixSession
from .book import BID, NEW, OFFER, OrderBook
from .instruments import Instrument

BookListener = Callable[[OrderBook, int], None]  # (book, receive time in perf_counter_ns)

log = logging.getLogger("md")


def _px(entry: dict[int, str]) -> float | None:
    return float(entry[270]) if entry.get(270) else None


class MarketDataClient:
    def __init__(self, symbols: tuple[str, ...]):
        self.books = {symbol: OrderBook(symbol) for symbol in symbols}
        self.instruments: dict[str, Instrument] = {symbol: Instrument(symbol) for symbol in symbols}
        self._listeners: list[BookListener] = []
        self._req_id = 0
        self._open_checks: dict[str, str] = {}  # SecurityReqID -> symbol, for the 336=OPEN requests

    def subscribe(self, listener: BookListener) -> None:
        self._listeners.append(listener)

    def _notify(self, book: OrderBook, recv_ns: int) -> None:
        for listener in self._listeners:
            listener(book, recv_ns)

    def _next_req_id(self, prefix: str) -> str:
        self._req_id += 1
        return f"{prefix}{int(time.time())}-{self._req_id}"

    # ---- SessionApp ------------------------------------------------------------------------

    def on_logon(self, session: FixSession) -> None:
        for book in self.books.values():
            book.clear()
            book.stale = True
            session.send("x", [(320, self._next_req_id("SEC")), (559, "0"), (55, book.symbol)])
            self.request_open_check(session, book.symbol)
            session.send("V", [
                (262, self._next_req_id("MD")),
                (263, "1"),   # snapshot + updates
                (264, "0"),   # full depth (25 levels)
                (267, "2"), (269, BID), (269, OFFER),
                (146, "1"), (55, book.symbol),
            ])
            log.info("subscribed to %s", book.symbol)

    def request_open_check(self, session: FixSession, symbol: str) -> None:
        """Ask for `symbol` filtered on the OPEN state; the answer sets Instrument.listed_open.

        An empty book never shows its state on market data, so this is the only way to see it."""
        self.instruments[symbol].listed_open = None
        req_id = self._next_req_id("OPEN")
        self._open_checks[req_id] = symbol
        session.send("x", [(320, req_id), (559, "0"), (55, symbol), (336, "OPEN")])

    def on_disconnect(self, session: FixSession) -> None:
        now = time.perf_counter_ns()
        for book in self.books.values():
            book.stale = True
            self._notify(book, now)

    def on_app_message(self, session: FixSession, msg: FixMessage) -> None:
        recv_ns = time.perf_counter_ns()
        mt = msg.msg_type
        if mt == "X":
            touched: dict[str, OrderBook] = {}
            for entry in msg.groups(268, 279):
                book = self.books.get(entry.get(55, ""))
                if book is None or book.stale:
                    continue  # ignore increments until the snapshot arrives
                book.apply(entry[279], entry.get(278, ""), entry.get(269, ""), _px(entry),
                           float(entry.get(271) or 0))
                self._track_state(book.symbol, entry)
                touched[book.symbol] = book
            for book in touched.values():
                book.last_update_ns = recv_ns
                self._notify(book, recv_ns)
        elif mt == "W":
            book = self.books.get(msg.get(55, ""))
            if book is None:
                return
            book.clear()
            for i, entry in enumerate(msg.groups(268, 269)):
                book.apply(NEW, entry.get(278) or f"W{i}", entry.get(269, ""), _px(entry),
                           float(entry.get(271) or 0))
                self._track_state(book.symbol, entry)
            book.stale = False
            book.last_update_ns = recv_ns
            log.info("snapshot %r", book)
            self._notify(book, recv_ns)
        elif mt == "y" and msg.get(320) in self._open_checks:
            symbol = self._open_checks.pop(msg[320])
            listed = any(e.get(55) == symbol for e in msg.groups(146, 55))
            self.instruments[symbol].listed_open = listed
            log.info("%s %s in the OPEN instrument list", symbol, "is" if listed else "is not")
        elif mt == "y":
            if msg.get(560, "0") != "0":
                log.error("security list request %s failed: SecurityRequestResult=%s", msg.get(320), msg.get(560))
            for entry in msg.groups(146, 55):
                if entry[55] in self.instruments:
                    inst = Instrument.from_security_list(entry)
                    old = self.instruments[entry[55]]
                    inst.state, inst.listed_open = old.state, old.listed_open
                    self.instruments[entry[55]] = inst
                    log.info("instrument %s tick %s lot %s limits %s..%s", inst.symbol, inst.tick_size,
                             inst.lot_size, inst.low_limit, inst.high_limit)
        elif mt == "Y":
            log.error("market data request %s rejected: %s", msg.get(262), msg.get(58))
        elif mt == "j":
            log.error("request %s rejected: %s", msg.get(379), msg.get(58))

    def _track_state(self, symbol: str, entry: dict[int, str]) -> None:
        state = entry.get(336)
        if state and self.instruments[symbol].state != state:
            log.info("%s is %s", symbol, state)
            self.instruments[symbol].state = state


class InstrumentLister:
    """One-shot SessionApp: request every listed instrument, collect SecurityList fragments, then stop.

    `state` (e.g. OPEN) asks the exchange for instruments in that state only, via TradingSessionID(336)."""

    def __init__(self, state: str = "") -> None:
        self.state = state
        self.instruments: list[Instrument] = []
        self.done = False
        self.result = ""  # SecurityRequestResult(560) of the last fragment: 0 = valid, 2 = no instruments found

    def on_logon(self, session: FixSession) -> None:
        session.send("x", [(320, f"LIST{int(time.time())}"), (559, "4")] + ([(336, self.state)] if self.state else []))

    def on_disconnect(self, session: FixSession) -> None:
        pass

    def on_app_message(self, session: FixSession, msg: FixMessage) -> None:
        if msg.msg_type == "y":
            self.result = msg.get(560, "")
            self.instruments += [Instrument.from_security_list(e) for e in msg.groups(146, 55)]
            if msg.get(893, "Y") == "Y":
                self.done = True
