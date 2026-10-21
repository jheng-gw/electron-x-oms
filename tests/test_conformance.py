"""The scripted conformance run against the in-process simulator meets every approval minimum."""
import json
import unittest
from pathlib import Path

from exoms.conformance import RECEIVED_MIN, SENT_MIN, ConformanceRun, format_report
from tests.test_integration import SimTestCase


class ConformanceTest(SimTestCase):
    async def test_every_minimum_is_met(self):
        # A fractional HeartBtInt keeps the ~32 heartbeats to ~16s. The simulator accepts it; the CLI
        # only allows whole seconds. Not lower: the session drops after 2 x HeartBtInt of silence, and
        # at 0.25s an occasional event-loop stall in the full suite on Windows was enough to trip it.
        run = ConformanceRun(self.settings, heartbeat_s=0.5, pace_s=0.01, pause_s=0.1)
        report = await run.run()

        self.assertEqual(report["error"], "")
        self.assertTrue(report["passed"], format_report(report))
        for msg_type, minimum in SENT_MIN.items():
            self.assertGreaterEqual(report["sent"].get(msg_type, 0), minimum, msg_type)
        for msg_type, minimum in RECEIVED_MIN.items():
            self.assertGreaterEqual(report["received"].get(msg_type, 0), minimum, msg_type)

        # Controlled: one resend and one gap fill per reconnect, no fills, nothing left resting.
        self.assertEqual(report["sent"]["2"], report["received"]["4"])
        self.assertEqual(run.gateway.risk.positions.get(self.settings.symbols[0], 0), 0)
        self.assertEqual(run.gateway.live_orders(), [])
        self.assertEqual(self.sim.orders, {})
        self.assertEqual(len(report["prices"]), 3)
        for p in report["prices"]:
            self.assertLess(p["buy"], p["sell"])

        # Every probe is recorded with the message that answered it, and the non-integer
        # OrderQty probes alone reach the Reject target.
        probes = report["probes"]
        self.assertTrue(probes)
        self.assertTrue(all(p["answer"] for p in probes), probes)
        rejects = [p for p in probes if p["expected"] == "3"]
        self.assertTrue(all(p["probe"].startswith("non-integer OrderQty") for p in rejects), rejects)
        self.assertEqual({p["answer"] for p in rejects}, {"3"})
        self.assertEqual({p["ref_tag"] for p in rejects}, {"38"})

        saved = json.loads(Path(report["path"]).read_text(encoding="utf-8"))
        self.assertEqual(saved["passed"], True)
        self.assertEqual(Path(report["path"]).parent, self.settings.var_dir)

    async def test_refuses_to_send_orders_when_book_is_closed(self):
        # As on demo: the instrument is listed but CLOSED, and its book is empty, so market data
        # carries no state and only the OPEN-filtered SecurityList can tell.
        self.sim.markets[self.settings.symbols[0]].inst.state = "CLOSED"
        self.sim.step()
        run = ConformanceRun(self.settings, heartbeat_s=0.25, pace_s=0.01, pause_s=0.1, ref_price=50)
        report = await run.run()

        self.assertFalse(report["passed"])
        self.assertIn("is not OPEN", report["error"])
        self.assertIn("--state OPEN", report["error"])
        self.assertEqual(report["sent"], {})  # never logged on to order entry
        self.assertEqual(report["cycles"], [])

    async def test_stops_before_next_order_round_when_book_closes(self):
        # As on demo at 06:15 UTC: the book closes mid-run. The next order cycle must not start.
        run = ConformanceRun(self.settings, heartbeat_s=0.25, pace_s=0.01, pause_s=0.1)
        round_1 = run._order_round

        async def close_after_round(n, cancel_all):
            orders = await round_1(n, cancel_all)
            self.sim.markets[self.settings.symbols[0]].inst.state = "CLOSED"
            self.sim.step()
            return orders
        run._order_round = close_after_round
        report = await run.run()

        self.assertIn("is not OPEN", report["error"])
        self.assertEqual([c["step"] for c in report["cycles"]][-1], "order round 1")
        self.assertEqual(len(report["prices"]), 1)
        self.assertEqual(self.sim.orders, {})  # round 1 cancelled everything; nothing sent after


if __name__ == "__main__":
    unittest.main()
