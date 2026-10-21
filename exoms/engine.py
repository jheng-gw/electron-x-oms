"""Wires the trading process together: MD session -> books -> strategy -> risk -> OE gateway."""
from __future__ import annotations

import asyncio
import gc
import logging
import time

from .config import Settings
from .fix.session import FixSession, build_ssl_context
from .fix.store import SeqStore
from .journal import Journal, journal_path
from .latency import LatencyStats
from .md.handler import MarketDataClient
from .oe.gateway import OrderGateway
from .oe.throttle import Throttle
from .risk.checks import RiskManager
from .strategy.demo_mm import DemoQuoter

log = logging.getLogger("engine")


def tune_gc() -> None:
    """Move startup objects out of GC tracking and make collections rarer, to cut pause spikes."""
    gc.collect()
    gc.freeze()
    gc.set_threshold(50_000, 50, 100)


class Engine:
    def __init__(self, settings: Settings, trade: bool = True):
        self.settings = settings
        self.trade = trade
        var = settings.var_dir
        var.mkdir(parents=True, exist_ok=True)
        self.kill_file = var / "KILL"
        self.positions_file = var / "positions.json"

        self.journal = Journal(journal_path(var))
        self.risk = RiskManager(settings.risk)
        self.latency = LatencyStats()
        ssl_ctx = build_ssl_context(settings.tls)
        server_name = settings.tls.server_name or None

        self.md = MarketDataClient(settings.symbols)
        # Market data resets sequence numbers on every logon, so there is nothing to persist.
        self.md_session = FixSession(settings.md, self.md, SeqStore(), ssl_ctx, server_name)

        self.gateway = OrderGateway(
            settings, self.risk,
            Throttle.from_exchange_limit(settings.rate_limit, settings.rate_safety, settings.cancel_reserve),
            self.journal, self.latency, self.md.instruments)
        self.oe_session = FixSession(settings.oe, self.gateway, SeqStore(var / "seq-oe.json"), ssl_ctx, server_name)
        self.quoters: list[DemoQuoter] = []
        self._session_tasks: list[asyncio.Task] = []
        self._aux_tasks: list[asyncio.Task] = []
        self._restore_positions()

    def _restore_positions(self) -> None:
        events = [e for e in Journal.read(self.journal.path) if e.get("kind") in ("fill", "bust")]
        for event in events:
            self.gateway.restore(event)
        if events:
            log.info("restored %d fills/busts from %s: %s", len(events), self.journal.path.name,
                     dict(self.risk.positions))

    async def start(self) -> None:
        if self.kill_file.exists():
            self.risk.kill(f"{self.kill_file} present: {self.kill_file.read_text(encoding='utf-8').strip()}")
        sessions = [self.md_session]
        if self.trade:
            for symbol in self.settings.symbols:
                quoter = DemoQuoter(symbol, self.gateway, self.settings.strategy, self.md.instruments[symbol])
                self.md.subscribe(quoter.on_book)
                self.gateway.subscribe(quoter.on_order_event)
                self.quoters.append(quoter)
            sessions.append(self.oe_session)
            self.gateway.hold_new_orders = True
            self._aux_tasks += [asyncio.create_task(self._startup_clean_slate(), name="clean-slate"),
                                asyncio.create_task(self._kill_switch_watch(), name="kill-watch"),
                                asyncio.create_task(self._positions_snapshot(), name="positions")]
        self._aux_tasks.append(asyncio.create_task(self._report(), name="report"))
        self._session_tasks = [asyncio.create_task(s.run(), name=f"session-{s.name}") for s in sessions]
        tune_gc()

    async def run(self) -> None:
        await self.start()
        try:
            # asyncio.wait (unlike gather) does not cancel the sessions when Ctrl-C cancels us,
            # so shutdown() can still cancel orders and log out cleanly.
            await asyncio.wait(self._session_tasks, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        log.info("shutting down")
        self.risk.kill("shutdown")
        # Cancel On Disconnect does not fire on a graceful logout, so cancel explicitly first.
        if self.gateway.live_orders() and self.gateway.cancel_all("shutdown"):
            deadline = time.monotonic() + 2.0
            while self.gateway.live_orders() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
        if self.gateway.live_orders():
            log.warning("%d orders not confirmed canceled at shutdown", len(self.gateway.live_orders()))
        self._write_positions()
        for task in self._aux_tasks:
            task.cancel()
        await asyncio.gather(self.oe_session.stop(), self.md_session.stop(), return_exceptions=True)
        await asyncio.gather(*self._session_tasks, *self._aux_tasks, return_exceptions=True)
        self.journal.close()

    def _write_positions(self) -> None:
        self.journal.snapshot(self.positions_file, {
            "ts": time.time(), "account": self.settings.account, "positions": dict(self.risk.positions)})

    async def _startup_clean_slate(self) -> None:
        """Once the order session is up and caught up, cancel anything a previous run left resting."""
        while not (self.gateway.ready and self.gateway.mass_cancel("startup: clear orders from previous runs")):
            await asyncio.sleep(0.05)
        self.gateway.hold_new_orders = False

    async def _positions_snapshot(self) -> None:
        while True:
            self._write_positions()
            await asyncio.sleep(1.0)

    async def _kill_switch_watch(self) -> None:
        last_cancel = 0.0
        while True:
            await asyncio.sleep(0.5)
            if self.kill_file.exists() and not (self.risk.killed and self.risk.kill_reason != "shutdown"):
                self.risk.kill(self.kill_file.read_text(encoding="utf-8").strip() or "KILL file")
            if self.risk.killed and self.gateway.live_orders() and time.monotonic() - last_cancel > 2.0:
                last_cancel = time.monotonic()
                self.gateway.cancel_all(self.risk.kill_reason)

    async def _report(self) -> None:
        while True:
            await asyncio.sleep(10.0)
            books = " ".join(repr(b) for b in self.md.books.values())
            stats = self.latency.drain()
            lat = (f"tick-to-send p50 {stats['p50_us']:.0f}us p99 {stats['p99_us']:.0f}us "
                   f"max {stats['max_us']:.0f}us (n={stats['count']})") if stats else "no orders sent"
            log.info("%s | live orders %d | positions %s | %s", books, len(self.gateway.live_orders()),
                     dict(self.risk.positions) or "{}", lat)
