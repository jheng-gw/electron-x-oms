# exoms: demo ElectronX OMS in Python

A demo order management system for the ElectronX FIX gateways (FIXT.1.1 / FIX 5.0 SP2), built from
`instructions.pdf` and the *ElectronX FIX Specification v.20261015.1*. It connects the three sessions
Huron River Energy was provisioned with, runs a toy quoting strategy behind pre-trade risk, and checks
every fill against the drop copy. A local simulator of the exchange lets you run all of it offline.

Pure standard library: no third-party dependencies.

```
 ElectronX :13001  MD ─► MarketDataClient ─► OrderBook (by order) ─► DemoQuoter ─► RiskManager ─► OrderGateway ─► OE :13101
                          SecurityList → tick/lot/limits                 ▲              Throttle       │
                                                                         └──── ExecutionReports ◄──────┘
 ElectronX :13101  DC ─► DropCopyReconciler (separate process) ── positions break ──► var/<target>/KILL ─► engine kill switch
```

## Quick start (simulator)

Run everything through [uv](https://docs.astral.sh/uv/) (Python 3.14 is pinned in `.python-version`):

```bash
uv run exoms sim
```

```bash
uv run exoms dropcopy
```

```bash
uv run exoms run
```

Run each in its own terminal. The engine logs fills plus a status line every 10s with tick-to-send latency.
Ctrl-C on the engine mass-cancels, waits for the cancels to be acknowledged, then logs out.

Tests (unit tests, plus end-to-end runs against an in-process simulator):

```bash
uv run python -m unittest discover -s tests -t .
```

## Connecting to ElectronX demo

1. `cp .env.example .env`, then fill in the three passwords from `instructions.pdf`.
2. Find a symbol: `uv run exoms instruments --target demo`, then set `EXI_SYMBOLS`.
3. Market data only first: `uv run exoms run --target demo --no-trade`.
4. Then `uv run exoms dropcopy --target demo` and `uv run exoms run --target demo` (it asks for confirmation before sending orders).

## Layout

| Path | What it does |
|---|---|
| `exoms/fix/codec.py` | FIX framing, BodyLength, CheckSum, repeating groups |
| `exoms/fix/session.py` | Initiator: logon, heartbeats/TestRequest, gap detection, ResendRequest, gap fill, reconnect, mutual TLS |
| `exoms/fix/store.py` | Sequence numbers, written behind by a thread; weekly reset at Sunday 06:15 UTC |
| `exoms/md/` | SecurityList reference data, market-by-order book, subscription handling |
| `exoms/oe/gateway.py` | Order messages per spec, order state machine, busts, rejects, mass cancel |
| `exoms/oe/throttle.py` | Rolling 1s window under the 100 msg/sec limit, with headroom kept for cancels |
| `exoms/risk/checks.py` | Size, price band, tick/lot/instrument limits, worst-case position, kill switch |
| `exoms/strategy/demo_mm.py` | Inventory-skewed bid/ask around the mid, leaving its own orders out of the mid |
| `exoms/dropcopy/reconciler.py` | Independent positions from drop copy; trips the kill switch on a persistent break |
| `exoms/sim/exchange.py` | ElectronX simulator: matching engine, validation, CoD, replay, rate limit |
| `var/<target>/` | Journal, positions snapshot, sequence numbers, `KILL` (gitignored) |

## ElectronX behaviour the code relies on (from the spec)

- **Logon:** `1137=9`, `554=<password>`, and never `50` (a Logon with tag 50 is rejected). Only one connection per SenderCompID.
- **Sequence numbers:**
  - Market data resets on every logon (`141=Y`).
  - Order entry and drop copy keep their numbers all week and reset Sunday 06:15 UTC.
  - Missed messages are recovered by ResendRequest replay (PossDup), and fills are de-duplicated by ExecID.
  - If local sequence state is lost, the session takes the number from the exchange's "MsgSeqNum too low, expecting N" logout.
- **Order fields:**
  - NewOrderSingle carries 1, 11, 38 (an integer), 40=2, 44, 54, 55, 59, 582, 1028.
  - Cancel carries 11, 41, 55.
  - Replace carries 11, 41, 1, 55, 54, 40, 38, 44, 59.
  - Every order message carries `50=<trader email>` in the header.
- **Cancel On Disconnect** fires on TCP loss only, not on a graceful logout. That's why shutdown and the kill switch cancel explicitly, using one OrderMassCancelRequest (`530=7`). The engine also mass-cancels at startup to clear anything left over from an earlier run.
- **Market data is by order:** MDEntryID(278) equals our OrderID(37), so the strategy leaves its own quotes out of the mid.
- **Rejects:**
  - Price off the tick: ExecutionReport `150=8` (103=18).
  - Bad lot size: Reject `3`.
  - Rate limit: BusinessMessageReject `j` (380=0, 379=ClOrdID).
  - The gateway resolves each of these back to the order and checks tick, lot and limits locally first.
- **Trade busts:** `150=H`, referencing the busted fill by ExecRefID(19), reverse the position in both the engine and the reconciler.

## Kill switch

Either the reconciler or you can write `var/<target>/KILL`. The engine sees it within 0.5s, stops quoting and
mass-cancels. It stays killed until restart, and it won't start while the file exists, so delete the file deliberately.

## Before production: ElectronX approval framework

ElectronX issues production order-entry credentials only after reviewing your Beta activity:

| Direction | Minimum activity |
|---|---|
| Client → exchange | Logon ×5, Logout ×5, Heartbeat ×30, ResendRequest ×5, NewOrderSingle ×20, OrderCancelRequest ×10, OrderCancelReplaceRequest ×10 |
| Exchange → client | SequenceReset ×5, Reject ×5, BusinessMessageReject ×5, ExecutionReport ×40 |

They also look for well-controlled activity: no runaway loops, sensible error handling and few malformed messages.
The conformance run below produces all of it in one controlled session.

## Conformance run

`uv run exoms conformance` drives the Order Entry session (`D-HURON-01-OE01`) through a fixed script and counts
every message in each direction. It runs 7 logon/logout cycles:

1. **Clean slate:** a mass cancel clears anything left over from a previous run.
2. **Reject probes:** NewOrderSingles with a non-integer `OrderQty`, a lot-size violation (if the lot is > 1) or a missing required tag. It sends one at a time and stops after 6 Rejects `3`.
3. **BusinessMessageReject probes:** MarketDataRequest `V`, which the order entry gateway doesn't support. It stops after 6 rejects `j`.
4. **Order round 1:** 8 one-lot orders, alternating buy and sell. Each is at least 20% (and at least 20 ticks) away from the market and inside the instrument's limits. 4 of them are replaced one tick further away, then all 8 are cancelled.
5. **Order round 2:** the same as round 1.
6. **Order round 3:** the same, but half are cancelled individually and the rest by the final mass cancel.
7. **Heartbeats:** idles until 32 Heartbeats have been sent.

All order activity happens in the first minute. On demo every instrument closed at about 06:15 UTC partway
through a run, so the long heartbeat idle comes last. Before each cycle that sends orders, the run checks
again that the book is OPEN, using a SecurityListRequest filtered on `336=OPEN`, because an empty book sends
no market data when it closes. If the book isn't OPEN, the run stops before sending anything. Pick an
OPEN instrument with `uv run exoms instruments --target demo --state OPEN`. On demo only the `HB_TEST` and
`HB_PILOT` hubs were open.

How the session-level counts are produced:
- **ResendRequest:** before each reconnect after the first, the run winds its expected inbound sequence number back by one, over the exchange's last Logout. The exchange's Logon reply then shows a gap, so we send a ResendRequest and the exchange answers with a SequenceReset-GapFill. That gives 6 of each, and no application message is replayed.
- **Pacing and safety:** it sends at most ~10 msg/s, never sends marketable orders, and refuses to run if the limits leave no price far enough from the touch.

Against the simulator (in-process, with a 1s heartbeat, about 1 minute):

```bash
uv run exoms conformance --target sim --with-sim --heartbeat 1
```

Against ElectronX demo:

```bash
uv run exoms conformance --target demo
```

- **Stop the engine first.** ElectronX allows one connection per SenderCompID, and the run shares the engine's `var/demo/seq-oe.json` and journal.
- **Confirmation:** the run asks before it starts.
- **Duration:** the default `--heartbeat 30` (the spec's recommended interval) takes about 17 minutes, almost all of it idling for Heartbeats.
- **Shorter heartbeat:** `--heartbeat 5` cuts that to about 4 minutes. The spec only *recommends* 30s and doesn't state a range, so the first demo run is the test of whether ElectronX accepts it.
- **Other options:** `--symbol` picks the instrument (default: the first of `EXI_SYMBOLS`), and `--ref-price` supplies a reference if the book is empty.

**Output:** the run prints a table of counts against the minimums, plus every other message type seen. It saves the full report (counts, prices used, per-cycle breakdown) to `var/<target>/conformance-<UTC time>.json`. It exits with status 1 if any minimum is short.

## Known gaps and assumptions

- **Tag 50 format:** `50` is the authorized user's email, as `instructions.pdf` and the reference guide say. The spec's example shows `firms/abc/users/def`; confirm with ElectronX if orders are rejected.
- **TLS hostname:** the TLS hostname isn't checked (we connect by IP) unless `EXI_TLS_SERVER_NAME` is set. The chain is still verified against `ca.crt`.
- **Positions:** positions cover this FIX week's journal only. There is no start-of-week position load from clearing yet. The reconciler also counts UI-entered trades on the account, so manual UI trading will (correctly) show up as a break.
- **Latency:** measured against the simulator on this machine: p50 ~350µs, p99 under 1ms tick-to-send. Over the internet to ElectronX, network time will dominate.
- **Simulator:** it follows the spec's documented behaviour. It's not a byte-exact copy of the real gateway, so test against demo before trusting it.
