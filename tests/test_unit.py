import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from exoms.config import RiskConfig
from exoms.fix.codec import FixParseError, FixParser, encode
from exoms.fix.store import SeqStore, fix_week_start
from exoms.md.book import BID, CHANGE, DELETE, NEW, OFFER, OrderBook
from exoms.md.instruments import Instrument, price_decimals
from exoms.oe.orders import BUY, SELL, Order, OrdStatus
from exoms.oe.throttle import Throttle
from exoms.risk.checks import RiskManager


class CodecTest(unittest.TestCase):
    def test_roundtrip_and_checksum(self):
        raw = encode("FIXT.1.1", [(35, "0"), (49, "A"), (56, "EXI"), (34, "1")])
        body = raw.split(b"\x019=", 1)[1].split(b"\x01", 1)[1].rsplit(b"10=", 1)[0]
        self.assertIn(f"9={len(body)}".encode(), raw)
        self.assertEqual(int(raw[-4:-1]), sum(raw[:-7]) % 256)
        (msg,) = FixParser().feed(raw)
        self.assertEqual(msg.msg_type, "0")
        self.assertEqual(msg[56], "EXI")

    def test_partial_feeds_and_multiple_messages(self):
        raw = encode("FIXT.1.1", [(35, "1"), (112, "X")]) + encode("FIXT.1.1", [(35, "0")])
        parser = FixParser()
        out = []
        for i in range(len(raw)):
            out += parser.feed(raw[i:i + 1])
        self.assertEqual([m.msg_type for m in out], ["1", "0"])

    def test_bad_checksum(self):
        raw = bytearray(encode("FIXT.1.1", [(35, "0")]))
        raw[-4:-1] = b"000" if raw[-4:-1] != b"000" else b"001"
        with self.assertRaises(FixParseError):
            FixParser().feed(bytes(raw))

    def test_repeating_groups(self):
        raw = encode("FIXT.1.1", [(35, "X"), (262, "R"), (268, "2"),
                                  (279, "0"), (269, "0"), (278, "A"), (270, "1.5"),
                                  (279, "2"), (278, "B"), (270, "2")])
        (msg,) = FixParser().feed(raw)
        self.assertEqual(msg.groups(268, 279), [{279: "0", 269: "0", 278: "A", 270: "1.5"},
                                                {279: "2", 278: "B", 270: "2"}])


class BookTest(unittest.TestCase):
    def test_market_by_order(self):
        book = OrderBook("S")
        book.apply(NEW, "a", BID, 10.0, 5)
        book.apply(NEW, "b", BID, 10.25, 1)
        book.apply(NEW, "c", OFFER, 11.0, 2)
        self.assertEqual(book.best_bid(), 10.25)
        self.assertEqual(book.mid(), (10.25 + 11.0) / 2)
        self.assertEqual(book.best_bid(exclude={"b"}), 10.0)  # leave our own order out
        book.apply(CHANGE, "a", "", None, 3)
        self.assertEqual(book.depth(BID), [(10.25, 1), (10.0, 3)])
        book.apply(DELETE, "b", "", 10.25, 0)
        self.assertEqual(book.best_bid(), 10.0)


class ThrottleTest(unittest.TestCase):
    def test_cancel_reserve(self):
        now = [0.0]
        t = Throttle(10, 0.3, clock=lambda: now[0])
        self.assertEqual(sum(t.try_acquire() for _ in range(20)), 7)       # new orders stop at 7
        self.assertEqual(sum(t.try_acquire(True) for _ in range(20)), 3)   # cancels get the last 3
        now[0] = 1.0
        self.assertTrue(t.try_acquire())


class RiskTest(unittest.TestCase):
    def setUp(self):
        self.risk = RiskManager(RiskConfig(max_order_qty=5, max_position=10, max_open_orders=4,
                                           min_price=0.01, max_price=1000, price_band=0.05))
        self.inst = Instrument("S", tick_size=0.25, lot_size=1, low_limit=0, high_limit=500, state="OPEN")

    def test_limits(self):
        r = self.risk
        self.assertIsNone(r.check("S", BUY, 1, 40.0, [], 40.0, self.inst))
        self.assertIn("qty", r.check("S", BUY, 6, 40.0, [], 40.0, self.inst))
        self.assertIn("tick", r.check("S", BUY, 1, 40.1, [], 40.0, self.inst))
        self.assertIn("from mid", r.check("S", BUY, 1, 30.0, [], 40.0, self.inst))
        r.positions["S"] = 9
        resting = [Order("x", "S", BUY, 1, 39.0, status=OrdStatus.NEW)]
        self.assertIn("worst-case", r.check("S", BUY, 1, 40.0, resting, 40.0, self.inst))
        self.assertIsNone(r.check("S", SELL, 1, 40.0, resting, 40.0, self.inst))
        r.kill("test")
        self.assertIn("kill switch", r.check("S", SELL, 1, 40.0, [], 40.0, self.inst))

    def test_closed_instrument(self):
        self.inst.state = "HALTED"
        self.assertIn("HALTED", self.risk.check("S", BUY, 1, 40.0, [], 40.0, self.inst))


class SequenceStoreTest(unittest.TestCase):
    def test_week_boundary_is_sunday_0615_utc(self):
        wed = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(fix_week_start(wed), datetime(2026, 10, 18, 6, 15, tzinfo=timezone.utc))
        sun_early = datetime(2026, 10, 18, 6, 14, tzinfo=timezone.utc)
        self.assertEqual(fix_week_start(sun_early), datetime(2026, 10, 11, 6, 15, tzinfo=timezone.utc))

    def test_persist_and_weekly_reset(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seq.json"
            s = SeqStore(path)
            s.consume_sender()
            s.incr_target()
            s.close()
            reopened = SeqStore(path)
            self.assertEqual((reopened.next_sender, reopened.next_target), (2, 2))
            reopened.close()
            data = json.loads(path.read_text())
            data["week"] = "2020-01-05T06:15:00+00:00"
            path.write_text(json.dumps(data))
            stale = SeqStore(path)
            self.assertEqual(stale.next_sender, 1)  # last week's numbers are discarded
            stale.close()


class InstrumentTest(unittest.TestCase):
    def test_price_decimals(self):
        self.assertEqual(price_decimals(0.25), 2)
        self.assertEqual(price_decimals(1.0), 0)
        self.assertEqual(price_decimals(0.005), 3)


if __name__ == "__main__":
    unittest.main()
