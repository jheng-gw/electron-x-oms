"""Minimal FIX tag=value codec: framing, BodyLength(9) and CheckSum(10)."""
from __future__ import annotations

from typing import Iterable

SOH = b"\x01"
_MASKED_TAGS = {554, 925}  # Password, NewPassword


class FixParseError(Exception):
    pass


class FixMessage:
    """Decoded message. Keeps every field in order (for repeating groups) plus a first-occurrence index."""

    __slots__ = ("fields", "_first")

    def __init__(self, fields: list[tuple[int, str]]):
        self.fields = fields
        self._first: dict[int, str] = {}
        for tag, value in fields:
            self._first.setdefault(tag, value)

    @property
    def msg_type(self) -> str:
        return self._first.get(35, "")

    def get(self, tag: int, default: str | None = None) -> str | None:
        return self._first.get(tag, default)

    def get_int(self, tag: int, default: int = 0) -> int:
        value = self._first.get(tag)
        return int(value) if value else default

    def get_float(self, tag: int, default: float = 0.0) -> float:
        value = self._first.get(tag)
        return float(value) if value else default

    def __contains__(self, tag: int) -> bool:
        return tag in self._first

    def __getitem__(self, tag: int) -> str:
        return self._first[tag]

    def groups(self, count_tag: int, first_tag: int) -> list[dict[int, str]]:
        """Split the repeating group introduced by `count_tag` into one dict per entry.

        Each entry starts at `first_tag`. Assumes the group runs to the end of the body,
        which holds for every message this OMS reads.
        """
        entries: list[dict[int, str]] = []
        in_group = False
        for tag, value in self.fields:
            if tag == count_tag:
                in_group = True
            elif tag == 10:
                break
            elif in_group:
                if tag == first_tag:
                    entries.append({})
                if entries:
                    entries[-1][tag] = value
        return entries

    def __repr__(self) -> str:
        parts = (f"{t}={'***' if t in _MASKED_TAGS else v}" for t, v in self.fields)
        return "|".join(parts)


def encode(begin_string: str, fields: Iterable[tuple[int, str]]) -> bytes:
    """Encode body fields (starting with 35=MsgType) into a complete framed message."""
    body = b"".join(f"{tag}={value}".encode() + SOH for tag, value in fields)
    head = f"8={begin_string}\x019={len(body)}\x01".encode()
    checksum = (sum(head) + sum(body)) % 256
    return head + body + f"10={checksum:03d}\x01".encode()


def decode(raw: bytes) -> FixMessage:
    fields = []
    for part in raw.split(SOH):
        if not part:
            continue
        tag, sep, value = part.partition(b"=")
        if not sep or not tag.isdigit():
            raise FixParseError(f"malformed field {part!r}")
        fields.append((int(tag), value.decode("latin-1")))
    return FixMessage(fields)


class FixParser:
    """Incremental framer: feed raw socket bytes, get back complete, checksum-verified messages."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[FixMessage]:
        self._buf += data
        messages = []
        while (msg := self._next()) is not None:
            messages.append(msg)
        return messages

    def _next(self) -> FixMessage | None:
        buf = self._buf
        start = buf.find(b"8=FIX")
        if start < 0:
            del buf[:-4]  # keep a possible partial "8=FI" prefix
            return None
        if start:
            del buf[:start]
        len_at = buf.find(b"\x019=")
        if len_at < 0:
            return None
        len_end = buf.find(SOH, len_at + 3)
        if len_end < 0:
            return None
        try:
            body_len = int(buf[len_at + 3:len_end])
        except ValueError as exc:
            raise FixParseError("bad BodyLength") from exc
        trailer = len_end + 1 + body_len
        end = trailer + 7  # "10=NNN\x01"
        if len(buf) < end:
            return None
        raw = bytes(buf[:end])
        del buf[:end]
        if raw[trailer:trailer + 3] != b"10=" or raw[end - 1:end] != SOH:
            raise FixParseError("BodyLength does not match message")
        if sum(raw[:trailer]) % 256 != int(raw[trailer + 3:trailer + 6]):
            raise FixParseError("bad CheckSum")
        return decode(raw)
