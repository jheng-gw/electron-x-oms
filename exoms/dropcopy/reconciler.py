"""Drop copy reconciler: an independent view of fills, compared against the trading engine.

Runs as its own process. It rebuilds positions purely from the drop copy session (which covers
every account under the clearing member, including orders entered in the ElectronX UI) and
compares them with the engine's positions file. A break that persists across two snapshots trips
the engine's kill switch by writing var/<target>/KILL.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict

from ..config import Settings
from ..fix.codec import FixMessage
from ..fix.session import FixSession
from ..journal import journal_path
from ..oe.orders import BUY

log = logging.getLogger("dropcopy")

# The same fill reaches the engine (order entry session) and us (drop copy session) a few ms apart.
# A fill landing inside that window can still cause one mismatch, which is why a break must
# persist across two separate snapshots before it counts.
ARRIVAL_SKEW_S = 0.05


class DropCopyReconciler:
    def __init__(self, settings: Settings, kill_on_break: bool = True):
        self.settings = settings
        self.kill_on_break = kill_on_break
        self.positions: defaultdict[tuple[str, str], int] = defaultdict(int)  # (account, symbol) -> qty
        self.exec_reports = 0
        self.breaks = 0
        self._events: list[tuple[float, str, str, int]] = []  # (received, account, symbol, signed qty)
        self._fills: dict[str, tuple[str, str, int]] = {}      # ExecID -> (account, symbol, signed qty)
        self._seen: set[str] = set()
        self._streak = 0
        self._last_snapshot_ts = 0.0
        var = settings.var_dir
        self.positions_file = var / "positions.json"
        self.kill_file = var / "KILL"
        # Our own state survives restarts, like the session's sequence numbers, for the FIX week.
        self.state_file = journal_path(var, "dropcopy").with_suffix(".json")
        self._load()

    def _load(self) -> None:
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        self._fills = {e: tuple(v) for e, v in data["fills"].items()}
        self._seen = set(data["seen"])
        for t, acct, sym, qty in data["events"]:
            self._add_event(t, acct, sym, qty)
        log.info("restored %d fills from %s", len(self._fills), self.state_file.name)

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {"fills": self._fills, "seen": sorted(self._seen), "events": self._events}
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        tmp.replace(self.state_file)

    def _add_event(self, t: float, acct: str, sym: str, qty: int) -> None:
        self._events.append((t, acct, sym, qty))
        self.positions[(acct, sym)] += qty

    # ---- SessionApp ------------------------------------------------------------------------

    def on_logon(self, session: FixSession) -> None:
        log.info("drop copy connected")

    def on_disconnect(self, session: FixSession) -> None:
        log.warning("drop copy disconnected: reconciliation paused")

    def on_app_message(self, session: FixSession, msg: FixMessage) -> None:
        if msg.msg_type != "8":
            return
        self.exec_reports += 1
        exec_type, exec_id = msg.get(150), msg.get(17, "")
        if exec_type not in ("F", "H") or exec_id in self._seen:
            return
        self._seen.add(exec_id)
        acct, sym = msg.get(1, ""), msg.get(55, "")
        if exec_type == "F":
            qty = int(msg.get_float(32)) * (1 if msg.get(54) == BUY else -1)
            self._fills[exec_id] = (acct, sym, qty)
            self._add_event(time.time(), acct, sym, qty)
            log.info("DC fill %s %s %+d @ %s -> %+d", acct, sym, qty, msg.get(31), self.positions[(acct, sym)])
        else:
            busted = self._fills.pop(msg.get(19, ""), None)
            if busted is not None:
                b_acct, b_sym, b_qty = busted
                self._add_event(time.time(), b_acct, b_sym, -b_qty)
                log.warning("DC trade bust of %s: %s %s -> %+d", msg.get(19), b_acct, b_sym,
                            self.positions[(b_acct, b_sym)])
        self._save()

    # ---- reconciliation --------------------------------------------------------------------

    def my_positions(self, as_of: float | None = None) -> dict[str, int]:
        account = self.settings.account
        if as_of is None:
            return {sym: q for (acct, sym), q in self.positions.items() if acct == account and q}
        out: defaultdict[str, int] = defaultdict(int)
        for t, acct, sym, qty in self._events:
            if acct == account and t <= as_of:
                out[sym] += qty
        return {sym: q for sym, q in out.items() if q}

    def check(self) -> dict[str, tuple[int, int]] | None:
        """Compare the engine's latest snapshot with our positions as of the snapshot's time.

        Returns {symbol: (engine, drop copy)} breaks, or None if there is no fresh snapshot.
        """
        try:
            snap = json.loads(self.positions_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        ts = snap.get("ts", 0)
        if time.time() - ts > 10:
            log.debug("engine positions file is stale; engine not running")
            return None
        if time.time() - ts < ARRIVAL_SKEW_S:
            return None  # too fresh: a fill may have reached the engine but not us yet
        engine = {sym: qty for sym, qty in snap.get("positions", {}).items() if qty}
        mine = self.my_positions(as_of=ts + ARRIVAL_SKEW_S)
        breaks = {sym: (engine.get(sym, 0), mine.get(sym, 0))
                  for sym in engine.keys() | mine.keys() if engine.get(sym, 0) != mine.get(sym, 0)}
        if not breaks:
            self._streak = 0
            return breaks
        if ts == self._last_snapshot_ts:
            return breaks  # same snapshot as last check; only a new one counts toward the streak
        self._last_snapshot_ts = ts
        self._streak += 1
        if self._streak >= 2:
            self.breaks += 1
            text = ", ".join(f"{s}: engine {e:+d} vs drop copy {d:+d}" for s, (e, d) in breaks.items())
            log.critical("POSITION BREAK %s", text)
            if self.kill_on_break and not self.kill_file.exists():
                self.kill_file.parent.mkdir(parents=True, exist_ok=True)
                self.kill_file.write_text(f"drop copy position break: {text}", encoding="utf-8")
                log.critical("kill switch tripped: wrote %s", self.kill_file)
        return breaks

    async def reconcile_loop(self, interval_s: float = 2.0) -> None:
        while True:
            await asyncio.sleep(interval_s)
            self.check()
