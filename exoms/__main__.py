"""Command line: `python -m exoms {sim,run,instruments,dropcopy,conformance}`."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from datetime import datetime, timezone

from .config import ConfigError, load_settings


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)-9s %(message)s",
                        datefmt="%H:%M:%S")


async def _run(args: argparse.Namespace) -> None:
    settings = load_settings(args.target if args.command != "sim" else "sim",
                             require_symbols=args.command != "instruments")

    if args.command == "sim":
        from .sim.exchange import SimExchange
        exchange = SimExchange.from_settings(settings, seed=args.seed)
        await exchange.start()
        try:
            await exchange.run_market()
        finally:
            await exchange.close()

    elif args.command == "run":
        if settings.target == "demo" and not args.no_trade and not args.yes:
            answer = input(f"Send orders to ElectronX demo as {settings.oe.sender_comp_id} "
                           f"for {', '.join(settings.symbols)}? [y/N] ")
            if answer.strip().lower() != "y":
                print("aborted; use --no-trade to stream market data only")
                return
        from .engine import Engine
        await Engine(settings, trade=not args.no_trade).run()

    elif args.command == "instruments":
        from .fix.session import FixSession, build_ssl_context
        from .fix.store import SeqStore
        from .md.handler import InstrumentLister
        lister = InstrumentLister(args.state.upper() if args.state else "")
        session = FixSession(settings.md, lister, SeqStore(), build_ssl_context(settings.tls),
                             settings.tls.server_name)
        task = asyncio.create_task(session.run())
        try:
            for _ in range(300):
                if lister.done:
                    break
                await asyncio.sleep(0.1)
        finally:
            await session.stop()
            await task
        print(f"{'symbol':40} {'tick':>6} {'lot':>4} {'low':>8} {'high':>8}")
        for inst in sorted(lister.instruments, key=lambda i: i.symbol):
            print(f"{inst.symbol:40} {inst.tick_size or '':>6} {inst.lot_size:>4} "
                  f"{inst.low_limit if inst.low_limit is not None else '':>8} "
                  f"{inst.high_limit if inst.high_limit is not None else '':>8}")
        if not lister.done:
            print("(timed out before the last SecurityList fragment)")
        asof = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"{len(lister.instruments)} instruments" + (f" in state {lister.state}" if lister.state else "")
              + f" at {asof} (SecurityRequestResult {lister.result or 'none received'})")

    elif args.command == "dropcopy":
        from .dropcopy.reconciler import DropCopyReconciler
        from .fix.session import FixSession, build_ssl_context
        from .fix.store import SeqStore
        recon = DropCopyReconciler(settings, kill_on_break=not args.alert_only)
        session = FixSession(settings.dc, recon, SeqStore(settings.var_dir / "seq-dc.json"),
                             build_ssl_context(settings.tls), settings.tls.server_name)
        loop_task = asyncio.create_task(recon.reconcile_loop())
        try:
            await session.run()
        finally:
            loop_task.cancel()
            await session.stop()

    elif args.command == "conformance":
        await _conformance(args, settings)


async def _conformance(args: argparse.Namespace, settings) -> None:
    from dataclasses import replace
    from .conformance import ConformanceRun, format_report
    symbol = args.symbol or settings.symbols[0]
    minutes = ConformanceRun.estimated_duration_s(args.heartbeat) / 60
    print(f"Order Entry conformance run as {settings.oe.sender_comp_id} on {settings.target}, {symbol}, "
          f"HeartBtInt {args.heartbeat}s: about {minutes:.0f} min (mostly idling for Heartbeats).", flush=True)
    if settings.target == "demo" and not args.yes:
        print("It logs on and off 7 times, provokes Rejects and BusinessMessageRejects, and places, replaces and\n"
              "cancels 1-lot orders far from the market, then mass cancels. Stop the engine first: ElectronX\n"
              "allows one connection per SenderCompID.")
        if input("Run it against ElectronX demo? [y/N] ").strip().lower() != "y":
            print("aborted")
            return
    sim = sim_task = None
    if args.with_sim:
        if settings.target != "sim":
            sys.exit("--with-sim needs --target sim")
        from .sim.exchange import SimExchange
        sim = SimExchange.from_settings(settings)
        await sim.start()
        sim_task = asyncio.create_task(sim.run_market())
        settings = replace(settings, md=replace(settings.md, port=sim.md_port),
                           oe=replace(settings.oe, port=sim.oe_port), dc=replace(settings.dc, port=sim.oe_port))
    try:
        report = await ConformanceRun(settings, heartbeat_s=args.heartbeat, symbol=symbol,
                                      ref_price=args.ref_price).run()
    finally:
        if sim is not None:
            sim_task.cancel()
            await sim.close()
    print(format_report(report))
    if not report["passed"]:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(prog="exoms", description="ElectronX demo OMS")
    parser.add_argument("-v", "--verbose", action="store_true", help="log every FIX message")
    sub = parser.add_subparsers(dest="command", required=True)

    p_sim = sub.add_parser("sim", help="run the local ElectronX simulator")
    p_sim.add_argument("--seed", type=int, default=None)

    p_run = sub.add_parser("run", help="run the trading engine (market data + order entry + strategy)")
    p_run.add_argument("--target", choices=["sim", "demo"], default="sim")
    p_run.add_argument("--no-trade", action="store_true", help="market data only; no order session")
    p_run.add_argument("--yes", action="store_true", help="skip the confirmation prompt for --target demo")

    p_ins = sub.add_parser("instruments", help="list tradable instruments (SecurityListRequest) and exit")
    p_ins.add_argument("--target", choices=["sim", "demo"], default="sim")
    p_ins.add_argument("--state", metavar="STATE",
                       help="only instruments in this state, e.g. OPEN (TradingSessionID(336) filter)")

    p_dc = sub.add_parser("dropcopy", help="run the drop copy reconciler")
    p_dc.add_argument("--target", choices=["sim", "demo"], default="sim")
    p_dc.add_argument("--alert-only", action="store_true", help="log breaks without tripping the kill switch")

    p_conf = sub.add_parser("conformance", help="scripted Order Entry activity for ElectronX production approval")
    p_conf.add_argument("--target", choices=["sim", "demo"], default="sim")
    p_conf.add_argument("--heartbeat", type=int, default=30, metavar="SECONDS",
                        help="HeartBtInt(108) to log on with; 30 (the spec's recommendation) takes ~17 min")
    p_conf.add_argument("--symbol", help="instrument to place orders in (default: first of EXI_SYMBOLS)")
    p_conf.add_argument("--ref-price", type=float, help="reference price if the book is empty")
    p_conf.add_argument("--with-sim", action="store_true", help="start an in-process simulator (--target sim)")
    p_conf.add_argument("--yes", action="store_true", help="skip the confirmation prompt for --target demo")

    args = parser.parse_args()
    if args.command == "conformance" and args.heartbeat < 1:
        parser.error("--heartbeat must be at least 1")
    _setup_logging(args.verbose)
    try:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(_run(args))
    except ConfigError as exc:
        sys.exit(f"config error: {exc}")


if __name__ == "__main__":
    main()
