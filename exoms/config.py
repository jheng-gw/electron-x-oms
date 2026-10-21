"""Settings loaded from environment variables and an optional .env file."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SIM_SYMBOL = "BFUT500-ERCOT-HB_SIM-261231-15"


class ConfigError(Exception):
    pass


def load_dotenv(path: Path) -> None:
    """Populate os.environ from KEY=VALUE lines without overriding values already set."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, "").strip() or default


def _env_int(key: str, default: int) -> int:
    return int(_env(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(_env(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    return _env(key, "Y" if default else "N").upper() in ("Y", "YES", "1", "TRUE")


@dataclass(frozen=True)
class SessionConfig:
    name: str
    host: str
    port: int
    sender_comp_id: str
    target_comp_id: str
    password: str
    begin_string: str = "FIXT.1.1"
    default_appl_ver_id: str = "9"  # FIX50SP2
    heartbeat_s: int = 30
    reset_on_logon: bool = False


@dataclass(frozen=True)
class TlsConfig:
    enabled: bool
    ca_cert: Path
    client_cert: Path
    client_key: Path
    server_name: str


@dataclass(frozen=True)
class RiskConfig:
    max_order_qty: int
    max_position: int
    max_open_orders: int
    min_price: float
    max_price: float
    price_band: float  # max fractional distance from mid for a new order price


@dataclass(frozen=True)
class StrategyConfig:
    quote_qty: int
    half_spread_ticks: int
    requote_ticks: int
    skew_ticks_per_lot: float
    tick_size: float


@dataclass(frozen=True)
class Settings:
    target: str
    md: SessionConfig
    oe: SessionConfig
    dc: SessionConfig
    tls: TlsConfig
    account: str
    trader: str
    capacity: str
    symbols: tuple[str, ...]
    rate_limit: int
    rate_safety: float
    cancel_reserve: float
    risk: RiskConfig
    strategy: StrategyConfig
    var_dir: Path


def load_settings(target: str, env_file: Path | None = ROOT / ".env", require_symbols: bool = True) -> Settings:
    """Build settings for `sim` (local simulator) or `demo` (ElectronX demo environment)."""
    if target not in ("sim", "demo"):
        raise ConfigError(f"unknown target {target!r}; expected 'sim' or 'demo'")
    if env_file is not None:
        load_dotenv(env_file)

    sim = target == "sim"
    # EXI_HOST overrides the address, e.g. 127.0.0.1 when tunnelling through stunnel with EXI_USE_TLS=N.
    host = "127.0.0.1" if sim else _env("EXI_HOST", _env("EXI_PRIMARY_HOST", "206.109.100.83"))
    md_port = _env_int("EXI_SIM_MD_PORT" if sim else "EXI_MD_PORT", 13001)
    oe_port = _env_int("EXI_SIM_OE_PORT" if sim else "EXI_OE_PORT", 13101)  # Drop Copy shares this port

    common = dict(
        target_comp_id=_env("EXI_TARGET_COMP_ID", "EXI"),
        heartbeat_s=_env_int("EXI_HEARTBEAT_S", 30),
    )

    def session(name: str, port: int, sender_default: str, reset_on_logon: bool) -> SessionConfig:
        key = name.upper()
        password = "sim" if sim else _env(f"EXI_{key}_PASSWORD")
        return SessionConfig(name=name, host=host, port=port,
                             sender_comp_id=_env(f"EXI_{key}_SENDER", sender_default),
                             password=password, reset_on_logon=reset_on_logon, **common)

    # Market Data resets sequence numbers on every logon; Order Entry and Drop Copy keep them all week.
    md = session("md", md_port, "HURON-MD01", reset_on_logon=True)
    oe = session("oe", oe_port, "D-HURON-01-OE01", reset_on_logon=False)
    dc = session("dc", oe_port, "CM-HURON-DC01", reset_on_logon=False)

    raw_symbols = _env("EXI_SIM_SYMBOLS", SIM_SYMBOL) if sim else _env("EXI_SYMBOLS")
    symbols = tuple(s.strip() for s in raw_symbols.split(",") if s.strip())

    if not sim:
        missing = [f"EXI_{s.name.upper()}_PASSWORD" for s in (md, oe, dc) if not s.password]
        if not symbols and require_symbols:
            missing.append("EXI_SYMBOLS (list them with: python -m exoms instruments --target demo)")
        if missing:
            raise ConfigError(f"missing settings for demo target: {', '.join(missing)} (see .env.example)")

    def path(key: str, default: str) -> Path:
        p = Path(_env(key, default))
        return p if p.is_absolute() else ROOT / p

    tls = TlsConfig(
        enabled=not sim and _env_bool("EXI_USE_TLS", True),
        ca_cert=path("EXI_CA_CERT", "ca.crt"),
        client_cert=path("EXI_CLIENT_CERT", "client.crt"),
        client_key=path("EXI_CLIENT_KEY", "client.key"),
        server_name=_env("EXI_TLS_SERVER_NAME"),
    )

    return Settings(
        target=target, md=md, oe=oe, dc=dc, tls=tls,
        account=_env("EXI_ACCOUNT", "TA-HURON-00001"),
        trader=_env("EXI_TRADER", "jheng@greenwichcm.com"),
        capacity=_env("EXI_CAPACITY", "2"),
        symbols=symbols,
        rate_limit=_env_int("EXI_RATE_LIMIT", 100),
        rate_safety=_env_float("EXI_RATE_SAFETY", 0.9),
        cancel_reserve=_env_float("EXI_CANCEL_RESERVE", 0.25),
        risk=RiskConfig(
            max_order_qty=_env_int("EXI_MAX_ORDER_QTY", 5),
            max_position=_env_int("EXI_MAX_POSITION", 10),
            max_open_orders=_env_int("EXI_MAX_OPEN_ORDERS", 4),
            min_price=_env_float("EXI_MIN_PRICE", 0.01),
            max_price=_env_float("EXI_MAX_PRICE", 1000.0),
            price_band=_env_float("EXI_PRICE_BAND", 0.05),
        ),
        strategy=StrategyConfig(
            quote_qty=_env_int("EXI_QUOTE_QTY", 1),
            half_spread_ticks=_env_int("EXI_HALF_SPREAD_TICKS", 2),
            requote_ticks=_env_int("EXI_REQUOTE_TICKS", 1),
            skew_ticks_per_lot=_env_float("EXI_SKEW_TICKS_PER_LOT", 0.5),
            tick_size=_env_float("EXI_TICK_SIZE", 0.25),  # fallback; SecurityList's MinPriceIncrement wins
        ),
        var_dir=ROOT / "var" / target,
    )
