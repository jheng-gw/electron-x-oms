"""Asyncio FIX initiator session: logon, heartbeats, sequence numbers, gap fill, reconnect."""
from __future__ import annotations

import asyncio
import logging
import re
import ssl
import time
from datetime import datetime, timezone
from typing import Protocol, Sequence

from ..config import SessionConfig, TlsConfig
from .codec import FixMessage, FixParseError, FixParser, encode
from .store import SeqStore

Fields = Sequence[tuple[int, str]]


class SessionApp(Protocol):
    def on_logon(self, session: "FixSession") -> None: ...
    def on_disconnect(self, session: "FixSession") -> None: ...
    def on_app_message(self, session: "FixSession", msg: FixMessage) -> None: ...


class SessionError(Exception):
    """Unrecoverable protocol problem; the connection is dropped and retried."""


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def build_ssl_context(tls: TlsConfig) -> ssl.SSLContext | None:
    """Mutual TLS: verify the exchange against ca.crt and present client.crt/client.key."""
    if not tls.enabled:
        return None
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(tls.ca_cert))
    ctx.load_cert_chain(str(tls.client_cert), str(tls.client_key))
    # Python 3.13+ turns on strict X.509 checks by default; ElectronX's certificates lack the Authority
    # Key Identifier those require. The chain is still verified against ca.crt.
    ctx.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)
    # We connect by IP, so hostname matching only works if ElectronX tells us the certificate name.
    ctx.check_hostname = bool(tls.server_name)
    return ctx


class FixSession:
    LOGON_TIMEOUT_S = 10.0
    MAX_BACKOFF_S = 30.0

    def __init__(self, cfg: SessionConfig, app: SessionApp, store: SeqStore,
                 ssl_context: ssl.SSLContext | None = None, server_hostname: str | None = None):
        self.cfg = cfg
        self.app = app
        self.store = store
        self._ssl = ssl_context
        self._server_hostname = server_hostname or None
        self.log = logging.getLogger(f"fix.{cfg.name}")
        self.logged_on = False
        self._writer: asyncio.StreamWriter | None = None
        self._parser = FixParser()
        self._stopping = False
        self._logout_sent = False
        self._logout_done = asyncio.Event()
        self._abort_reason = ""
        self._connected_at = self._last_sent = self._last_recv = 0.0
        self._test_req_at: float | None = None
        self._test_req_id = 0
        self._resend_until = 0

    @property
    def name(self) -> str:
        return self.cfg.name

    # ---- lifecycle -------------------------------------------------------------------------

    async def run(self) -> None:
        """Connect and serve until stop(), reconnecting with exponential backoff."""
        backoff = 1.0
        while not self._stopping:
            had_logon = False
            try:
                had_logon = await self._connect_and_serve()
            except (OSError, FixParseError, SessionError) as exc:
                if not self._stopping:
                    self.log.warning("connection lost: %s", exc or type(exc).__name__)
            finally:
                had_logon = had_logon or self.logged_on
                self._teardown()
            if self._stopping:
                break
            backoff = 1.0 if had_logon else min(backoff * 2, self.MAX_BACKOFF_S)
            self.log.info("reconnecting in %.0fs", backoff)
            await asyncio.sleep(backoff)

    async def stop(self, reason: str = "shutdown") -> None:
        """Send Logout, wait briefly for the exchange's reply, then close."""
        self._stopping = True
        if self.logged_on and self._writer is not None:
            self._logout_sent = True
            self._send("5", [(58, reason)])
            try:
                await asyncio.wait_for(self._logout_done.wait(), 2.0)
            except TimeoutError:
                self.log.warning("no Logout reply; closing anyway")
        if self._writer is not None:
            self._writer.close()
        self.store.close()

    def abort(self, reason: str) -> None:
        """Drop the TCP connection immediately (no Logout)."""
        self._abort_reason = reason
        if self._writer is not None:
            self._writer.transport.abort()

    async def _connect_and_serve(self) -> bool:
        cfg = self.cfg
        self.log.info("connecting to %s:%d as %s", cfg.host, cfg.port, cfg.sender_comp_id)
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(cfg.host, cfg.port, ssl=self._ssl,
                                    server_hostname=self._server_hostname if self._ssl else None),
            timeout=10.0)
        self._writer = writer
        self._parser = FixParser()
        self._logout_sent = False
        self._logout_done.clear()
        self._abort_reason = ""
        self._connected_at = self._last_recv = time.monotonic()
        self._test_req_at = None
        self._resend_until = 0
        if cfg.reset_on_logon:
            self.store.reset()
        elif self.store.roll_week():
            self.log.info("weekly sequence reset (Sunday 06:15 UTC) since last session")
        self._send_logon()

        timer = asyncio.create_task(self._timer_loop(), name=f"{cfg.name}-timer")
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    if self._logout_sent or self._stopping:
                        return True
                    raise ConnectionError(self._abort_reason or "connection closed by peer")
                self._last_recv = time.monotonic()
                for msg in self._parser.feed(data):
                    self._on_message(msg)
        finally:
            timer.cancel()

    def _teardown(self) -> None:
        was_logged_on = self.logged_on
        self.logged_on = False
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        if was_logged_on:
            self.log.info("logged off")
            self._call_app(self.app.on_disconnect, self)

    async def _timer_loop(self) -> None:
        hb = self.cfg.heartbeat_s
        while True:
            await asyncio.sleep(min(1.0, hb / 2))
            now = time.monotonic()
            if not self.logged_on:
                if now - self._connected_at > self.LOGON_TIMEOUT_S:
                    self.log.error("no Logon reply within %.0fs", self.LOGON_TIMEOUT_S)
                    self.abort("logon timeout")
                    return
                continue
            if now - self._last_sent >= hb:
                self._send("0", [])
            silent = now - self._last_recv
            if silent > 2 * hb:
                self.abort(f"no data from exchange for {silent:.0f}s")
                return
            if silent > 1.2 * hb and (self._test_req_at is None or self._test_req_at < self._last_recv):
                self._test_req_id += 1
                self._send("1", [(112, f"TEST{self._test_req_id}")])
                self._test_req_at = now

    # ---- outbound --------------------------------------------------------------------------

    def send(self, msg_type: str, body: Fields, header: Fields = ()) -> int:
        """Send an application message. `header` is for header fields such as SenderSubID(50)."""
        if not self.logged_on:
            raise SessionError(f"{self.name} session is not logged on")
        return self._send(msg_type, body, header)

    def _send(self, msg_type: str, body: Fields, header: Fields = (),
              seq: int | None = None, poss_dup: bool = False) -> int:
        if self._writer is None:
            raise SessionError(f"{self.name} session is not connected")
        if seq is None:
            seq = self.store.consume_sender()
        now = utc_timestamp()
        fields = [(35, msg_type), (49, self.cfg.sender_comp_id), (56, self.cfg.target_comp_id),
                  (34, str(seq))]
        if poss_dup:
            fields.append((43, "Y"))
        fields.append((52, now))
        if poss_dup:
            fields.append((122, now))
        fields.extend(header)
        fields.extend(body)
        self._writer.write(encode(self.cfg.begin_string, fields))
        self._last_sent = time.monotonic()
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("> %s", FixMessage(fields))
        return seq

    def _send_logon(self) -> None:
        # ElectronX rejects a Logon that carries SenderSubID(50), so no header extras here.
        cfg = self.cfg
        body = [(98, "0"), (108, str(cfg.heartbeat_s))]
        if cfg.reset_on_logon:
            body.append((141, "Y"))
        body += [(554, cfg.password), (1137, cfg.default_appl_ver_id)]
        self._send("A", body)

    @property
    def recovering(self) -> bool:
        """True while a ResendRequest we sent is still being answered."""
        return bool(self._resend_until)

    # ---- inbound ---------------------------------------------------------------------------

    def _on_message(self, msg: FixMessage) -> None:
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("< %s", msg)
        mt = msg.msg_type
        seq = msg.get_int(34)

        if mt == "A" and msg.get(141) == "Y":
            self.store.set_target(1)
            if not self.cfg.reset_on_logon:  # the counterparty reset on its own; our Logon counts as 1
                self.log.warning("counterparty reset sequence numbers on logon")
                self.store.set_sender(2)
        if mt == "4" and msg.get(123) != "Y":  # SequenceReset-Reset: jump without checks
            self.log.warning("sequence reset to %s", msg.get(36))
            self.store.set_target(msg.get_int(36))
            return

        expected = self.store.next_target
        if seq > expected:
            self.log.warning("sequence gap: expected %d, got %d; requesting resend", expected, seq)
            if mt == "A":
                self._on_logon(msg)
            elif mt == "5":
                self._on_logout(msg)
                return
            elif mt == "2":
                # Answer the counterparty's own ResendRequest even mid-gap, or both sides wait forever.
                self._on_resend_request(msg)
            if not self._resend_until:
                self._send("2", [(7, str(expected)), (16, "0")])
            self._resend_until = max(self._resend_until, seq)
            return  # dropped here; it comes back in the resend
        if seq < expected:
            if msg.get(43) == "Y":
                return  # duplicate we already processed
            text = f"MsgSeqNum too low, expected {expected} but received {seq}"
            self._send("5", [(58, text)])
            raise SessionError(text)

        if mt == "4":  # SequenceReset-GapFill
            self.store.set_target(msg.get_int(36))
        else:
            self.store.incr_target()
        if self._resend_until and self.store.next_target > self._resend_until:
            self._resend_until = 0

        if mt == "4":
            return
        if mt == "A":
            self._on_logon(msg)
        elif mt == "0":
            pass
        elif mt == "1":
            self._send("0", [(112, msg.get(112, ""))])
        elif mt == "2":
            self._on_resend_request(msg)
        elif mt == "5":
            self._on_logout(msg)
        else:
            if mt == "3":
                self.log.warning("session reject of seq %s (tag %s): %s", msg.get(45), msg.get(371), msg.get(58))
            self._call_app(self.app.on_app_message, self, msg)

    def _on_logon(self, msg: FixMessage) -> None:
        if self.logged_on:
            return
        self.logged_on = True
        self.log.info("logged on (heartbeat %ss)", msg.get(108, self.cfg.heartbeat_s))
        self._call_app(self.app.on_logon, self)

    def _on_logout(self, msg: FixMessage) -> None:
        text = msg.get(58, "")
        self.log.info("logout from exchange: %s", text)
        if not self._logout_sent:
            self._logout_sent = True
            self._send("5", [])
        expecting = re.search(r"MsgSeqNum too low, expecting (\d+)", text)
        if expecting and not self.logged_on:
            # Our stored numbers were lost or are stale; resume where the exchange says we are.
            # Anything we missed in between is then recovered with a ResendRequest.
            self.log.warning("adopting exchange's expected MsgSeqNum %s for our next logon", expecting[1])
            self.store.set_sender(int(expecting[1]))
        self._logout_done.set()
        if self._writer is not None:
            self._writer.close()

    def _on_resend_request(self, msg: FixMessage) -> None:
        # Never replay old orders: a stale order is worse than a missing one. Gap fill instead.
        begin, end = msg.get_int(7), self.store.next_sender
        self.log.info("resend request from %d: gap filling to %d", begin, end)
        self._send("4", [(123, "Y"), (36, str(end))], seq=begin, poss_dup=True)

    def _call_app(self, fn, *args) -> None:
        try:
            fn(*args)
        except Exception:
            self.log.exception("unhandled error in %s", getattr(fn, "__qualname__", fn))
