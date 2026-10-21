"""End-to-end: the real engine, gateway and drop copy client against the local simulator."""
import asyncio
import logging
import tempfile
import time
import unittest
from collections import Counter
from dataclasses import replace
from pathlib import Path

from exoms.config import load_settings
from exoms.conformance import CountingSession
from exoms.dropcopy.reconciler import DropCopyReconciler
from exoms.engine import Engine
from exoms.fix.codec import FixMessage
from exoms.fix.session import FixSession
from exoms.fix.store import SeqStore
from exoms.journal import Journal
from exoms.oe.gateway import OrderGateway
from exoms.oe.orders import BUY, OrdStatus
from exoms.oe.throttle import Throttle
from exoms.risk.checks import RiskManager
from exoms.md.handler import InstrumentLister
from exoms.sim.exchange import Market, SimExchange, SimInstrument

logging.basicConfig(level=logging.WARNING)


async def wait_for(predicate, timeout=6.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


class SimTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = load_settings("sim", env_file=None)
        base = replace(base, md=replace(base.md, port=0), oe=replace(base.oe, port=0), dc=replace(base.dc, port=0))
        self.sim = SimExchange.from_settings(base, seed=7, tick_interval=0.05, taker_prob=0.6)
        await self.sim.start()
        self.sim_task = asyncio.create_task(self.sim.run_market())
        self.settings = replace(
            base, var_dir=Path(self.tmp.name),
            md=replace(base.md, port=self.sim.md_port), oe=replace(base.oe, port=self.sim.oe_port),
            dc=replace(base.dc, port=self.sim.oe_port))
        self.cleanups: list = []

    async def asyncTearDown(self):
        for fn in reversed(self.cleanups):
            await fn()
        self.sim_task.cancel()
        await self.sim.close()
        self.tmp.cleanup()

    def make_gateway(self, settings=None):
        """A bare gateway + OE session, without the strategy."""
        settings = settings or self.settings
        journal = Journal(Path(self.tmp.name) / "gw.jsonl")
        gw = OrderGateway(settings, RiskManager(settings.risk), Throttle(90, 0.25), journal)
        session = FixSession(settings.oe, gw, SeqStore(Path(self.tmp.name) / "seq-gw.json"))
        task = asyncio.create_task(session.run())

        async def stop():
            await session.stop()
            await task
            journal.close()
        self.cleanups.append(stop)
        return gw, session


class EndToEndTest(SimTestCase):
    async def test_quoting_fills_and_drop_copy_agree(self):
        recon = DropCopyReconciler(self.settings)
        dc = FixSession(self.settings.dc, recon, SeqStore(Path(self.tmp.name) / "seq-dc.json"))
        dc_task = asyncio.create_task(dc.run())

        engine = Engine(self.settings)
        events = []
        engine.gateway.subscribe(lambda order, event: events.append((order.cl_ord_id, event, order.text)))
        await engine.start()

        self.assertTrue(await wait_for(lambda: engine.md.instruments[self.settings.symbols[0]].tick_size == 0.25),
                        "SecurityList not received")
        self.assertTrue(await wait_for(lambda: any(engine.risk.positions.values()), timeout=10), "no fills")
        rejects = [e for e in events if e[1] in ("8", "rejected")]
        self.assertEqual(rejects, [])
        self.assertGreater(engine.latency.total, 0)
        self.assertGreater(recon.exec_reports, 0)

        # Positions agree between the engine and the independent drop copy view.
        await asyncio.sleep(0.3)
        engine.risk.kill("test: freeze trading")
        engine.gateway.cancel_all("test")
        self.assertTrue(await wait_for(lambda: not engine.gateway.live_orders()))
        await asyncio.sleep(0.3)
        engine._write_positions()
        await asyncio.sleep(0.3)
        self.assertEqual(recon.check(), {})
        self.assertEqual(recon.my_positions(), {k: v for k, v in engine.risk.positions.items() if v})

        await engine.shutdown()
        await dc.stop()
        await dc_task

    async def test_bad_sender_sub_id_is_rejected_by_exchange(self):
        gw, _ = self.make_gateway(replace(self.settings, trader="nobody@example.com"))
        self.assertTrue(await wait_for(lambda: gw.ready))
        order = gw.new_order(self.settings.symbols[0], BUY, 1, 30.0)
        self.assertTrue(await wait_for(lambda: order.status == OrdStatus.REJECTED))
        self.assertIn("not an authorized trading user", order.text)

    async def test_cancel_on_disconnect_is_replayed_after_reconnect(self):
        gw, session = self.make_gateway()
        self.assertTrue(await wait_for(lambda: gw.ready))
        order = gw.new_order(self.settings.symbols[0], BUY, 1, 20.0)
        self.assertTrue(await wait_for(lambda: order.status == OrdStatus.NEW))

        self.sim.drop_connection(self.settings.oe.sender_comp_id)  # TCP loss, no Logout
        self.assertTrue(await wait_for(lambda: not session.logged_on, timeout=2))
        # Reconnect -> sequence gap -> ResendRequest -> the exchange replays the CoD cancel.
        with self.assertLogs("oe", level="WARNING") as logs:
            self.assertTrue(await wait_for(lambda: order.status == OrdStatus.CANCELED, timeout=8))
        self.assertIn("Cancel on disconnect", order.text)
        self.assertIn(f"order {order.cl_ord_id} canceled by Cancel On Disconnect", "\n".join(logs.output))
        self.assertTrue(await wait_for(lambda: gw.ready))

    async def test_recovers_when_local_sequence_numbers_are_lost(self):
        gw, session = self.make_gateway()
        self.assertTrue(await wait_for(lambda: gw.ready))
        gw.new_order(self.settings.symbols[0], BUY, 1, 20.0)
        await asyncio.sleep(0.2)
        await session.stop()
        # A new process with a fresh store: the exchange still expects the old numbering.
        fresh = FixSession(self.settings.oe, gw, SeqStore())
        task = asyncio.create_task(fresh.run())
        self.assertTrue(await wait_for(lambda: fresh.logged_on and not fresh.recovering, timeout=8))
        await fresh.stop()
        await task

    async def test_replace_then_mass_cancel(self):
        gw, _ = self.make_gateway()
        self.assertTrue(await wait_for(lambda: gw.ready))
        sym = self.settings.symbols[0]
        a, b = gw.new_order(sym, BUY, 1, 20.0), gw.new_order(sym, BUY, 2, 21.0)
        self.assertTrue(await wait_for(lambda: a.status == b.status == OrdStatus.NEW))
        first_id = a.cl_ord_id
        gw.replace(a, 22.0)
        self.assertTrue(await wait_for(lambda: a.price == 22.0 and not a.is_pending))
        self.assertNotEqual(a.cl_ord_id, first_id)
        self.assertTrue(gw.mass_cancel("test"))
        self.assertTrue(await wait_for(lambda: not gw.live_orders()))
        self.assertEqual({a.status, b.status}, {OrdStatus.CANCELED})


class MassCancelReportTest(SimTestCase):
    async def test_mass_cancel_reports_are_labelled_per_order(self):
        # Demo sends 11 = the mass cancel's ClOrdID, 41 = the order's, and 378 = 8 on each report.
        gw, session = self.make_gateway()
        self.assertTrue(await wait_for(lambda: gw.ready))
        sym = self.settings.symbols[0]
        a, b = gw.new_order(sym, BUY, 1, 20.0), gw.new_order(sym, BUY, 1, 21.0)
        self.assertTrue(await wait_for(lambda: a.status == b.status == OrdStatus.NEW))
        with self.assertLogs("oe", level="INFO") as logs:
            self.assertTrue(gw.mass_cancel("test"))
            self.assertTrue(await wait_for(lambda: not gw.live_orders()))
        self.assertEqual({a.status, b.status}, {OrdStatus.CANCELED})
        lines = "\n".join(logs.output)
        self.assertNotIn("Cancel On Disconnect", lines)
        for order in (a, b):
            self.assertIn(f"order {order.cl_ord_id} canceled by mass cancel", lines)


class HeartbeatTest(SimTestCase):
    async def test_exchange_heartbeats_so_client_never_probes(self):
        sent, received = Counter(), Counter()
        cfg = replace(self.settings.oe, heartbeat_s=1)
        journal = Journal(Path(self.tmp.name) / "hb.jsonl")
        gw = OrderGateway(self.settings, RiskManager(self.settings.risk), Throttle(90, 0.25), journal)
        session = CountingSession(cfg, gw, SeqStore(), sent=sent, received=received)
        task = asyncio.create_task(session.run())
        self.assertTrue(await wait_for(lambda: session.logged_on))
        await asyncio.sleep(4.5)  # idle: only Heartbeats in both directions
        self.assertGreaterEqual(received["0"], 3)
        self.assertEqual(sent["1"], 0)
        self.assertEqual(received["1"], 0)
        await session.stop()
        await task
        journal.close()


class InstrumentStateTest(SimTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.closed = "BFUT500-ERCOT-HB_SIM-261231-16"
        self.sim.markets[self.closed] = Market(SimInstrument(self.closed, state="CLOSED"), fair=160)

    async def list_instruments(self, state=""):
        lister = InstrumentLister(state)
        session = FixSession(self.settings.md, lister, SeqStore())
        task = asyncio.create_task(session.run())
        self.assertTrue(await wait_for(lambda: lister.done))
        await session.stop()
        await task
        return sorted(i.symbol for i in lister.instruments)

    async def test_state_filter_lists_only_open_instruments(self):
        self.assertEqual(await self.list_instruments(), sorted([self.settings.symbols[0], self.closed]))
        self.assertEqual(await self.list_instruments("OPEN"), [self.settings.symbols[0]])

    async def test_order_in_closed_book_is_rejected(self):
        gw, session = self.make_gateway(replace(self.settings, symbols=(self.closed,)))
        self.assertTrue(await wait_for(lambda: gw.ready))
        order = gw.new_order(self.closed, BUY, 1, 100.0)
        self.assertTrue(await wait_for(lambda: order.status == OrdStatus.REJECTED))
        self.assertEqual(order.text, "Book is CLOSED")


class RateLimitTest(SimTestCase):
    async def test_exchange_rejects_over_limit(self):
        received: list[FixMessage] = []

        class Capture:
            def on_logon(self, s): pass
            def on_disconnect(self, s): pass
            def on_app_message(self, s, msg): received.append(msg)

        session = FixSession(self.settings.oe, Capture(), SeqStore())
        task = asyncio.create_task(session.run())
        self.assertTrue(await wait_for(lambda: session.logged_on))
        s = self.settings
        for i in range(120):  # bypasses our own throttle on purpose
            session.send("D", [(11, f"R{i}"), (1, s.account), (55, s.symbols[0]), (54, "1"), (38, "1"),
                               (40, "2"), (44, "10.00"), (582, s.capacity), (1028, "N")], header=[(50, s.trader)])
        self.assertTrue(await wait_for(lambda: sum(m.msg_type == "j" for m in received) >= 20))
        rejects = [m for m in received if m.msg_type == "j"]
        self.assertEqual(rejects[0].get(380), "0")
        self.assertIn("rate limit", rejects[0].get(58))
        await session.stop()
        await task


if __name__ == "__main__":
    unittest.main()
