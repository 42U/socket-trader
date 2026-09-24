from __future__ import annotations

import asyncio
import websockets
import json
import logging
import logging.handlers
import math
import shutil
import socket
import subprocess
import sys
import time
import tempfile
import random
import os
import re
import platform
import threading
import concurrent.futures
import http.server
import secrets
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict, deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pathlib import Path
from colorama import init, Fore, Style

# pip install pyfiglet colorama websockets
try:
    import pyfiglet
except ImportError:
    pyfiglet = None

# Official Anthropic SDK — optional; only needed when a per-account AI gate
# uses provider "anthropic" (pip install anthropic).
try:
    import anthropic
except ImportError:
    anthropic = None

__version__ = "0.18.0"

IS_WINDOWS = platform.system() == "Windows"


def _detect_wsl() -> bool:
    """Return True if running under Windows Subsystem for Linux."""
    if IS_WINDOWS:
        return False
    if os.environ.get("WSL_DISTRO_NAME") or os.environ.get("WSL_INTEROP"):
        return True
    try:
        with open("/proc/version", "r") as f:
            return "microsoft" in f.read().lower()
    except OSError:
        return False


IS_WSL = _detect_wsl()

if IS_WINDOWS:
    # Windows 10+ handles ANSI natively — skip colorama conversion
    # so cursor-positioning sequences (\033[H, \033[s/u, \033[r) pass through
    init(convert=False)
    # Enable virtual terminal processing on Windows 10+
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass
else:
    init()

# ---------- Config persistence ----------
CONFIG_FILE = Path.home() / ".voidorigin_config.json"

paused = False
shutdown = asyncio.Event()
signal_count = 0
output_directory = None
active_account = None          # LEADER account: drives status bar / display and is always traded.
follower_accounts: list[str] = []  # FOLLOWERS that mimic the leader. Every signal fires on the
                                   # leader plus each follower. Empty = single-account mode.
roundrobin_accounts: list[str] = []  # ROUND-ROBIN pool: each entry signal goes to exactly ONE of
                                     # these, rotating randomly with no repeats until every pool
                                     # account has traded once. An account is a follower OR in the
                                     # pool, never both; the leader is always copy-traded.
_rr_remaining: list[str] = []   # accounts still owed a trade this round (shuffled)
_rr_last: str | None = None     # last account drawn — next round never starts with it
account_stops: dict[str, str] = {}  # account -> "hard" | "soft" once its session limit trips.
                                    # Absent = tradeable. NOT persisted (session-local lockout).
atm_strategy = "NQ_Med"        # ATM strategy template name (fallback)
follow_publisher_strategy = False  # If True, use the publisher's strategy per-signal when locally installed
atm_aliases: dict[str, str] = {}   # publisher strategy id -> local ATM template (or base name)
micro_mode = False             # If True, incoming instruments are translated to their CME micro (NQ→MNQ)
nt_port = 36973                # NinjaTrader AT Interface port (default 36973)
nt_host_override: str = ""     # Explicit NT host (empty = auto-detect local/WSL)
live_bridge_enabled = False    # If True, prefer the optional SocketTraderBridge AddOn
live_bridge_port = 36984       # Port the NinjaScript AddOn listens on (see addon/)
awaiting_directory_input = False
awaiting_user_input = False  # Block key handler during any input prompt

# ---------- Session state machine ----------
# States drive the status bar indicator in the pinned header.
# Add new states here as the system evolves.
SESSION_STATES = {
    "ready":        ("SESSION ACTIVE  ·  AWAITING SIGNALS",   None),
    "paused":       ("SESSION ACTIVE  ·  SIGNALS PAUSED",     "YELLOW"),
    "soft_stop":    ("SESSION ACTIVE  ·  STOP LIMIT HIT",     "RED"),
    "hard_stop":    ("SESSION ACTIVE  ·  HARD STOP — LOCKED", "RED"),
    "soft_target":  ("SESSION ACTIVE  ·  TARGET REACHED",     "GREEN"),
    "hard_target":  ("SESSION ACTIVE  ·  TARGET — LOCKED",    "GREEN"),
    "connecting":   ("CONNECTING TO SERVER",                   None),
    "reconnecting": ("CONNECTION LOST  ·  RECONNECTING",      "YELLOW"),
}
_session_state = "ready"

# Map state color names to colorama codes
_STATE_COLORS = {
    "YELLOW": Fore.LIGHTYELLOW_EX,
    "RED": Fore.RED,
    "GREEN": Fore.GREEN,
}


def set_session_state(state: str):
    """Update session state and refresh the header status bar."""
    global _session_state
    if state not in SESSION_STATES:
        return
    _session_state = state
    refresh_header_status()


def get_session_status_text() -> str:
    """Return the status text for the current session state, with color applied."""
    text, color_name = SESSION_STATES.get(_session_state, SESSION_STATES["ready"])
    if color_name and color_name in _STATE_COLORS:
        # Color the dynamic part (after the ·)
        parts = text.rsplit("·", 1)
        if len(parts) == 2:
            return parts[0] + "·" + _STATE_COLORS[color_name] + parts[1] + Fore.CYAN
    return text


_state_before_conn = "ready"  # header state to restore once the connection is back


def note_connection_down(reconnecting: bool = True):
    """Flip the header to a connection state, remembering what to restore.

    Repeated calls during one outage keep the original pre-outage state.
    """
    global _state_before_conn
    if _session_state not in ("connecting", "reconnecting"):
        _state_before_conn = _session_state
    set_session_state("reconnecting" if reconnecting else "connecting")


def note_connection_up():
    """Restore the pre-outage header state after a successful (re)connect.

    If something else moved the state mid-outage (a stop tripped, user
    paused), that newer state wins and is left untouched.
    """
    if _session_state in ("connecting", "reconnecting"):
        set_session_state(_state_before_conn)


# ---------- Risk management ----------
session_start_balances: dict[str, float] = {}   # account -> starting balance
session_current_balances: dict[str, float] = {}  # account -> latest polled CashValue (realized cash)
# Money that crossed the account boundary since the baseline (deposits
# positive, withdrawals negative). Subtracted out of every session P&L so a
# transfer is never read as a trading result — see session_pnl().
session_adjustments: dict[str, float] = {}      # account -> transferred since baseline
session_contracts: set[str] = set()              # instruments traded this session
soft_stopped = False                              # True if soft stop triggered
hard_stopped = False                              # True if hard stop triggered
BALANCE_POLL_INTERVAL = 3                         # seconds between balance checks

# A NinjaTrader broker-connection outage zeroes every AccountItem while the
# ATI port and the bridge AddOn keep answering, so a $52k account suddenly
# reads $0.00 and session P&L swings to -$52k of phantom loss — enough to
# trip a session stop, and enough to poison the 4:20 PM baseline re-snapshot
# into +$52k of phantom profit after reconnect. A real balance cannot step to
# exactly $0.00 between two polls, so zero readings are quarantined: the last
# known balance is held (and shown as stale) until real data returns.
# Known limits, by design: (1) a session-long genuine zeroing (the firm
# liquidates the account intraday) is indistinguishable from an outage, so
# enforcement freezes on the held balance with sticky STALE alerts as the
# operator's signal — the 4:20 PM reset then drops the quarantined baseline
# entirely so the next real reading re-seeds it clean. (2) An account whose
# cash legitimately reads $0.00 gets no baseline and therefore no stop/
# target enforcement until it reads nonzero; balance_monitor raises a
# sticky warning when that account has limits configured.
BALANCE_ZERO_EPS = 0.005    # cents precision: |reading| below this is "zero"
_balance_suspect_since: dict[str, float] = {}   # account -> monotonic ts of first quarantined read
_no_baseline_warned: set[str] = set()           # limits-without-baseline already warned


def _suspect_zero_balance(account: str, value: float) -> bool:
    """True when a ~$0.00 reading contradicts a materially nonzero last-known
    balance — the signature of NT answering while its broker feed is down."""
    if abs(value) > BALANCE_ZERO_EPS:
        return False
    last = session_current_balances.get(account)
    if last is None:
        last = session_start_balances.get(account)
    return last is not None and abs(last) > BALANCE_ZERO_EPS


def _ingest_balance(account: str, value, source: str) -> bool:
    """Gate every polled/streamed balance before it enters
    session_current_balances; True when the reading was accepted.

    A rejected reading freezes the account at its last known balance so the
    P&L display, the stop/target enforcement and the 4:20 PM baseline
    re-snapshot all keep operating on real numbers through an outage.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(v):
        return False
    if _suspect_zero_balance(account, v):
        if account not in _balance_suspect_since:
            _balance_suspect_since[account] = time.monotonic()
            last = session_current_balances.get(
                account, session_start_balances.get(account))
            logger.warning(
                f"BALANCE SUSPECT  {source} reported ${v:,.2f} for {account} "
                f"(last known ${last:,.2f}) — NinjaTrader connection likely "
                "down; holding last known balance")
            _dash_set_alert(
                Fore.YELLOW + f"  ⚠  NinjaTrader reports $0.00 for {account}"
                " — connection lost? Holding last known balance." +
                Style.RESET_ALL, sticky=True)
        return False
    started = _balance_suspect_since.pop(account, None)
    if started is not None:
        logger.info(f"BALANCE RESTORED  {source} {account} ${v:,.2f} "
                    f"after {time.monotonic() - started:.0f}s")
        _dash_set_alert(
            Fore.GREEN + f"  ✔  NinjaTrader balance feed restored for "
            f"{account} (${v:,.2f})." + Style.RESET_ALL)
    session_current_balances[account] = v
    return True


def _seed_start_balance(account: str, cash):
    """First real reading becomes the session baseline — never a ~$0.00 one.

    Seeding $0 while NT is disconnected would reconnect as pure phantom
    "profit"; a truly empty account loses nothing by having no baseline."""
    try:
        cash = float(cash)
    except (TypeError, ValueError):
        return
    if not math.isfinite(cash):
        return
    if account not in session_start_balances and abs(cash) > BALANCE_ZERO_EPS:
        session_start_balances[account] = cash
        # Anything transferred before this baseline is already inside it.
        session_adjustments.pop(account, None)


def _held_balance(account: str, polled) -> float | None:
    """A fresh ATI reading for display, with outage artifacts substituted by
    the last known good balance (None only when nothing is known at all)."""
    try:
        v = float(polled)
    except (TypeError, ValueError):
        v = None
    if v is not None and math.isfinite(v) and not _suspect_zero_balance(account, v):
        return v
    held = session_current_balances.get(account)
    if held is None:
        held = session_start_balances.get(account)
    if held is None and v is not None and math.isfinite(v):
        held = v
    return held


def session_pnl(account: str, current: float | None = None,
                risk: bool = False) -> float | None:
    """One account's session P&L: the balance move since its baseline, less
    any money that crossed the account boundary. None without a baseline.

    A deposit is not profit and a withdrawal is not a loss. Netting them out
    matters most to balance_monitor's stop/target check: counted as P&L, a
    deposit pushes a real loss out of the stop's reach and can trip a profit
    target nobody earned, and a withdrawal trips a stop against a position
    that never lost anything.

    Transfers are identified in _pnl_observe against NinjaTrader's
    RealizedPnL, so with no such feed this is exactly the old subtraction —
    it can only ever remove money it has positive evidence about."""
    start = session_start_balances.get(account)
    if current is None:
        current = session_current_balances.get(account)
    if start is None or current is None:
        return None
    adj = session_adjustments.get(account, 0.0)
    if risk and adj < 0:
        # Asked on behalf of the stop check, where the two directions do
        # not cost the same. A deposit (adj > 0) held out of P&L only ever
        # makes the number worse, so it is safe to apply. A withdrawal
        # (adj < 0) makes it BETTER, and cash owed by a fill that has not
        # settled yet is indistinguishable from one — so a mis-read there
        # would hold a stop off a losing account. Refusing the flattering
        # direction costs at worst an early stop on a real withdrawal,
        # which is the failure worth having.
        adj = 0.0
    return current - start - adj

# Auto-reset: futures session ends ~4:15 PM ET, reset P&L at 4:20 PM ET
try:
    ET = ZoneInfo("America/New_York")  # Eastern Time (handles EST/EDT automatically)
except ZoneInfoNotFoundError as exc:  # pragma: no cover — host tzdata dependent
    # Windows ships no IANA time zone database, so zoneinfo needs the tzdata
    # package there. A fixed UTC offset would silently mis-time the 4:20 PM ET
    # session reset across DST changes, so fail loudly rather than trade to a
    # clock that is quietly an hour off half the year.
    raise SystemExit(
        "\n  SocketTrader needs the IANA time zone database to track the futures\n"
        "  session (America/New_York) — Windows does not ship one.\n\n"
        "  Fix:  pip install tzdata\n\n"
        f"  (zoneinfo reported: {exc})\n"
    )
SESSION_RESET_HOUR = 16
SESSION_RESET_MINUTE = 20
# If launching after the reset time, mark today as already reset so we don't
# fire retroactively — only trigger when the app is running across the boundary.
_now_et = datetime.now(ET)
_past_reset_on_start = ((_now_et.hour == 16 and _now_et.minute >= 20) or _now_et.hour > 16)
_last_auto_reset_date: str | None = _now_et.strftime("%Y-%m-%d") if _past_reset_on_start else None
SESSION_SAVE_INTERVAL = 10  # save session state every N balance polls (~30s)
_balance_poll_count = 0


def get_session_id(now_et: datetime | None = None) -> str | None:
    """Return the session close-date for the current active CME futures session.

    Schedule (all times ET):
      Sun 6 PM → Mon 4:20 PM   (session ID = Monday's date)
      Mon 6 PM → Tue 4:20 PM   (session ID = Tuesday's date)
      ...
      Thu 6 PM → Fri 4:20 PM   (session ID = Friday's date)
      Fri 4:20 PM → Sun 6 PM   = weekend, no active session
      Daily 4:20 PM – 6:00 PM  = maintenance gap (Mon–Thu)

    Returns None if outside active session hours.
    """
    if now_et is None:
        now_et = datetime.now(ET)
    wd = now_et.weekday()  # Mon=0 … Sun=6
    h, m = now_et.hour, now_et.minute

    past_close = (h > SESSION_RESET_HOUR or
                  (h == SESSION_RESET_HOUR and m >= SESSION_RESET_MINUTE))
    before_open = h < 18

    # Weekend: Fri 4:20 PM → Sun 6 PM
    if wd == 4 and past_close:          # Friday after close
        return None
    if wd == 5:                          # Saturday
        return None
    if wd == 6 and before_open:          # Sunday before 6 PM
        return None

    # Daily maintenance gap 4:20 PM – 6 PM (Mon–Thu)
    if past_close and before_open:
        return None

    # Active session — ID is the date of the session close
    if h >= 18:
        close_date = (now_et + timedelta(days=1)).date()
    else:
        close_date = now_et.date()
    return close_date.strftime("%Y-%m-%d")


def save_session_state():
    """Persist session P&L data to config for crash recovery.

    Note: lockout flags (hard_stopped/soft_stopped) are intentionally NOT
    persisted — exit-and-restart is one of the ways to clear a hard stop.
    """
    session_id = get_session_id()
    if not session_id or not session_start_balances:
        return
    cfg = load_config()
    cfg["session"] = {
        "id": session_id,
        "start_balances": dict(session_start_balances),
        # Without this a crash mid-session would resurrect the baseline but
        # not the transfer, and the deposit would read as profit again.
        "adjustments": dict(session_adjustments),
        "contracts": list(session_contracts),
        "signal_count": signal_count,
        "rr": {"pool": sorted(roundrobin_accounts),
               "remaining": list(_rr_remaining),
               "last": _rr_last},
    }
    save_config(cfg)


def clear_saved_session():
    """Remove persisted session data from config."""
    cfg = load_config()
    if "session" in cfg:
        del cfg["session"]
        save_config(cfg)


def restore_session_state() -> bool:
    """Restore session state from config if still in the same active session.

    Returns True if session was restored.
    """
    cfg = load_config()
    saved = cfg.get("session")
    if not saved:
        _clear_positive_stops()
        return False

    current_session = get_session_id()
    if current_session and saved.get("id") == current_session:
        global signal_count, _rr_remaining, _rr_last
        for name, bal in saved.get("start_balances", {}).items():
            # Same guard as every other baseline write: a persisted ~$0.00
            # (saved through an outage, possibly by an older build) must
            # not come back as the baseline — recovery would read as pure
            # phantom profit and trip targets. The account re-seeds from
            # its first real reading instead.
            try:
                bal = float(bal)
            except (TypeError, ValueError):
                continue
            if math.isfinite(bal) and abs(bal) > BALANCE_ZERO_EPS:
                session_start_balances[name] = bal
        for name, adj in (saved.get("adjustments") or {}).items():
            try:
                adj = float(adj)
            except (TypeError, ValueError):
                continue
            # Only where the baseline itself came back: an adjustment whose
            # baseline was refused would net against the wrong number.
            if math.isfinite(adj) and name in session_start_balances:
                session_adjustments[name] = adj
        session_contracts.update(saved.get("contracts", []))
        signal_count = saved.get("signal_count", 0)
        # Resume the round-robin rotation only if the pool is unchanged —
        # a different pool starts a fresh round instead.
        rr_saved = saved.get("rr") or {}
        if rr_saved.get("pool") == sorted(roundrobin_accounts):
            _rr_remaining = [a for a in rr_saved.get("remaining", [])
                             if a in roundrobin_accounts]
            last = rr_saved.get("last")
            _rr_last = last if isinstance(last, str) else None
        return True

    # Different session or outside hours — clear stale data
    clear_saved_session()
    _clear_positive_stops()
    return False


def _clear_positive_stops():
    """Reset any positive stop limits to 0 — they are profit-protection
    limits that only make sense within the session that set them.
    Starting a new session with PnL at $0 would immediately trip them."""
    cfg = load_config()
    cleared = []
    for account, limits in cfg.get("account_limits", {}).items():
        if limits.get("stop", 0) > 0:
            old_stop = limits["stop"]
            limits["stop"] = 0
            cleared.append((account, old_stop))
            logger.info(f"RESET positive stop to 0 for {account} (new session)")
    if cleared:
        save_config(cfg)
        for account, old_stop in cleared:
            _dash_set_alert(
                Fore.YELLOW +
                f"  ⚠  Positive stop (${old_stop:+,.2f}) cleared for {account} — "
                f"new session. Press T to set a new limit." + Style.RESET_ALL,
                sticky=True)


def reset_session_pnl():
    """Re-snapshot all account balances and clear session state."""
    global soft_stopped, hard_stopped, signal_count, _last_auto_reset_date
    # Re-snapshot current balances as new starting point. A ~$0.00 current
    # must never become a baseline: it is either an outage artifact that
    # slipped in with no history to quarantine against (app booted while NT
    # was down) or an account that cannot trade anyway — and snapshotting
    # it makes the feed's recovery read as pure phantom profit, tripping
    # targets and pushing real losses out of the stop's reach. Dropping the
    # start instead leaves the account baseline-less until the next real
    # reading seeds one (balance_monitor's late-baseline path).
    #
    # An account still under zero-quarantine gets full amnesia instead of a
    # re-baseline: its held `current` is a stale pre-outage value, and
    # snapshotting it would hide whatever the zeros really meant (a firm
    # zeroing the account included) behind a frozen baseline for the whole
    # next session. Dropping both sides lets the first real post-outage
    # reading re-seed cleanly.
    for name in list(session_current_balances):
        if name in _balance_suspect_since:
            session_current_balances.pop(name)
            session_start_balances.pop(name, None)
            _balance_suspect_since.pop(name, None)
            logger.warning(
                f"RESET  {name} balance still quarantined — baseline dropped; "
                "it will re-seed from the next real reading")
            continue
        bal = session_current_balances[name]
        if abs(bal) > BALANCE_ZERO_EPS:
            session_start_balances[name] = bal
        else:
            session_start_balances.pop(name, None)
    # Every baseline just moved to the current balance, so whatever was
    # transferred before now sits inside it and must not be netted out twice.
    session_adjustments.clear()
    _no_baseline_warned.clear()
    session_contracts.clear()
    account_stops.clear()
    soft_stopped = False
    hard_stopped = False
    signal_count = 0
    _clear_positive_stops()
    set_session_state("ready")
    clear_saved_session()


# ---------- Input sanitization / validation ----------
MAX_SESSION_CONTRACTS = 50  # cap unique instruments per session
MAX_FIELD_LENGTH = 256      # max bytes per ATI field — prevent oversized payloads
MAX_SIGNAL_FIELDS = 20      # hard cap on semicolon-delimited fields

# All valid NinjaTrader 8 ATI OIF commands
VALID_ATI_COMMANDS = {
    "PLACE", "CANCEL", "CHANGE", "CLOSEPOSITION",
    "CLOSESTRATEGY", "REVERSEPOSITION",
    "CANCELALLORDERS", "FLATTENEVERYTHING",
}
VALID_ACTIONS = {"BUY", "SELL"}
# Per NT8 docs (Commands and Valid Parameters) the stop type is
# STOPMARKET — NT rejects "STOP" ("holds invalid order type parameter").
VALID_ORDER_TYPES = {"MARKET", "LIMIT", "STOPMARKET", "STOPLIMIT"}
VALID_TIF = {"DAY", "GTC"}


def sanitize_ati(value: str) -> str:
    # NOTE: also strips C0/C1 control characters, not just the ATI field
    # separators. Account names arrive from the web API and are printed into
    # the pinned terminal header, where a bare ESC would let a stored name
    # repaint the live session status of a running trading app.
    """Strip characters that could break ATI line-based parsing or inject fields."""
    return "".join(
        ch for ch in value.replace(';', '')
        if ch.isprintable() or ch == ' ')


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def strip_terminal_input(value: str) -> str:
    """Remove ANSI escape sequences and control chars from typed input.

    Arrow keys pressed at a cooked-mode prompt leak CSI sequences into the
    line buffer — one produced a garbage account_limits config key like
    '\\x1b[B\\x1b[B\\x1b[B6'. Applied to every read_line_raw result.
    """
    value = _ANSI_ESCAPE_RE.sub("", value)
    return "".join(c for c in value if c == "\t" or ord(c) >= 32)



def validate_signal(parts: list[str]) -> str | None:
    """Validate signal fields against NinjaTrader ATI OIF spec.

    Returns None if valid, or an error string describing the problem.
    """
    if not parts:
        return "empty signal"

    # Guard against oversized or malformed payloads
    if len(parts) > MAX_SIGNAL_FIELDS:
        return f"too many fields: {len(parts)} (max {MAX_SIGNAL_FIELDS})"
    for i, field in enumerate(parts):
        if len(field) > MAX_FIELD_LENGTH:
            return f"field {i} too long: {len(field)} bytes (max {MAX_FIELD_LENGTH})"

    cmd = parts[0].upper()
    if cmd not in VALID_ATI_COMMANDS:
        return f"unknown command: {cmd}"

    # Commands that require order fields: PLACE, REVERSEPOSITION
    if cmd in ("PLACE", "REVERSEPOSITION"):
        if len(parts) < 13:
            return f"{cmd} requires 13 fields, got {len(parts)}"
        action = parts[3].upper()
        if action not in VALID_ACTIONS:
            return f"invalid action: {action}"
        try:
            qty = int(parts[4])
            if qty <= 0:
                return f"invalid qty: {qty}"
        except ValueError:
            return f"non-numeric qty: {parts[4]}"
        order_type = parts[5].upper()
        if order_type not in VALID_ORDER_TYPES:
            return f"invalid order type: {order_type}"
        tif = parts[8].upper()
        if tif and tif not in VALID_TIF:
            return f"invalid TIF: {tif}"

    elif cmd == "CLOSEPOSITION":
        if len(parts) < 3:
            return "CLOSEPOSITION requires account and instrument"

    elif cmd == "CHANGE":
        if len(parts) < 11 or not parts[10]:
            return "CHANGE requires order ID (field 10)"

    elif cmd == "CANCEL":
        if len(parts) < 11 or not parts[10]:
            return "CANCEL requires order ID (field 10)"

    elif cmd == "CLOSESTRATEGY":
        if len(parts) < 13 or not parts[12]:
            return "CLOSESTRATEGY requires strategy ID (field 12)"

    return None


# ---------- Signal confirmation ----------
# After a signal fires, we snapshot positions for that instrument and verify
# NinjaTrader processed it by checking if the position changed.
CONFIRM_TIMEOUT = 9  # seconds to wait for position change after signal
MAX_PENDING_CONFIRMS = 20  # cap to prevent unbounded growth
_pending_confirms: list[dict] = []  # [{signal, ts, pre_pos, instrument, id, action}]
_confirms_lock = __import__("threading").Lock()

# ---------- Logging ----------
LOG_FILE = Path.home() / ".voidorigin_signals.log"
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per file
LOG_BACKUP_COUNT = 3             # keep 3 rotated copies (.log.1, .log.2, .log.3)
logger = logging.getLogger("sockettrader")
logger.setLevel(logging.INFO)


class _OwnerOnlyRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """RotatingFileHandler that keeps the log owner-only across rollovers.

    Chmodding once at startup is not enough: every rollover creates a
    brand new file at the process umask, so the live log drifts back to
    world-readable while only the rotated copies stay 0600. This file
    carries account numbers, balances and per-trade P&L, so the mode is
    re-applied to each new file as it is opened.
    """

    def _secure(self):
        if IS_WINDOWS:
            return
        try:
            os.chmod(self.baseFilename, 0o600)
        except OSError:
            pass          # logging must never take the app down

    def _open(self):
        stream = super()._open()
        self._secure()
        return stream

    def doRollover(self):
        super().doRollover()
        self._secure()


_log_handler = _OwnerOnlyRotatingFileHandler(
    str(LOG_FILE), maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_log_handler)
_log_handler._secure()

# ---------- Duplicate detection ----------
# Track recent signal IDs (the unique number at the end of each signal)
_recent_signal_ids: deque[str] = deque(maxlen=100)

# ---------- Post-reconnect replay guard ----------
# Observed 2026-08-07 04:21:58: after an unclean disconnect ("no close frame
# received") the server re-delivered the last signals on reconnect. The PLACE
# was caught by id dedup, but CLOSEPOSITION signals from the publisher carry
# NO signal id, so the replayed close fired again and flattened a live
# position opened 100s earlier. Defense: remember the exact text of recently
# fired signals; an ID-LESS signal that byte-matches one fired within
# REPLAY_LOOKBACK_S and arrives within REPLAY_GRACE_S of a (re)connect is a
# replay and is dropped. Outside the post-connect window identical closes
# always fire — a genuine re-close mid-session is never suppressed.
REPLAY_GRACE_S = 45        # how long after a (re)connect replays can arrive
REPLAY_LOOKBACK_S = 900    # how far back a fired signal can match
_MAX_FIRED_KEYS = 64
_recent_fired: dict[str, float] = {}   # canonical signal text -> monotonic ts
_last_connect_mono: float | None = None


def _note_fired_signal(signal_text: str):
    """Remember a signal we actually dispatched (for the replay guard)."""
    _recent_fired[signal_text] = time.monotonic()
    while len(_recent_fired) > _MAX_FIRED_KEYS:
        _recent_fired.pop(next(iter(_recent_fired)))


def note_connected():
    """Mark a successful (re)connect — starts the replay-guard window."""
    global _last_connect_mono
    _last_connect_mono = time.monotonic()


def _is_idless_replay(signal_text: str) -> bool:
    """True when an id-less signal is a server replay of one already fired."""
    if _last_connect_mono is None:
        return False
    now = time.monotonic()
    if now - _last_connect_mono > REPLAY_GRACE_S:
        return False
    fired_at = _recent_fired.get(signal_text)
    return fired_at is not None and now - fired_at <= REPLAY_LOOKBACK_S


def load_config() -> dict:
    """Load saved config from disk, or return empty dict."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
        # Self-heal: drop account_limits keys polluted with terminal control
        # characters (arrow-key escapes captured before input stripping).
        limits = cfg.get("account_limits")
        if isinstance(limits, dict):
            for key in [k for k in limits if any(ord(c) < 32 for c in k)]:
                del limits[key]
        return cfg
    return {}


def save_config(cfg: dict):
    """Persist config to disk atomically with restricted permissions."""
    try:
        fd, tmp = tempfile.mkstemp(
            dir=str(CONFIG_FILE.parent), suffix=".tmp", prefix=".voidorigin_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            if not IS_WINDOWS:
                os.chmod(tmp, 0o600)
            os.replace(tmp, CONFIG_FILE)
        except BaseException:
            os.unlink(tmp)
            raise
    except OSError as exc:
        print(Fore.RED + f"  ✖  Could not save config: {exc}" + Style.RESET_ALL)


# ---------- Micro contract conversion ----------
# CME micro equivalents keyed by full-size root symbol. Most micros take
# an "M" prefix (ES→MES, NQ→MNQ) but not all — Russell is M2K, silver is
# SIL — so translation is a lookup, never string surgery. The "micro_map"
# dict in the config file extends or overrides this table; mapping a root
# to itself (e.g. "GC": "GC") opts that symbol out of conversion.
MICRO_MAP = {
    "ES":  "MES",   # Micro E-mini S&P 500
    "NQ":  "MNQ",   # Micro E-mini Nasdaq-100
    "YM":  "MYM",   # Micro E-mini Dow
    "RTY": "M2K",   # Micro E-mini Russell 2000
    "GC":  "MGC",   # Micro Gold
    "SI":  "SIL",   # Micro Silver (1,000 oz)
    "HG":  "MHG",   # Micro Copper
    "CL":  "MCL",   # Micro WTI Crude Oil
    "NG":  "MNG",   # Micro Henry Hub Natural Gas
    "BTC": "MBT",   # Micro Bitcoin
    "ETH": "MET",   # Micro Ether
    "6E":  "M6E",   # E-micro EUR/USD
    "6A":  "M6A",   # E-micro AUD/USD
    "6B":  "M6B",   # E-micro GBP/USD
}
micro_map: dict[str, str] = dict(MICRO_MAP)  # active table: defaults + config overrides
_micro_unmapped_warned: set[str] = set()     # roots already warned about this run


def load_micro_map(cfg: dict) -> dict[str, str]:
    """Return the built-in micro table merged with config "micro_map" overrides."""
    merged = dict(MICRO_MAP)
    overrides = cfg.get("micro_map", {})
    if isinstance(overrides, dict):
        for root, micro in overrides.items():
            if isinstance(root, str) and isinstance(micro, str) and root.strip() and micro.strip():
                merged[root.strip().upper()] = micro.strip().upper()
    return merged


def to_micro_instrument(instrument: str) -> str:
    """Translate a full-size instrument to its micro: "NQ 06-26" → "MNQ 06-26".

    The expiry suffix is kept as-is — micros list the same contract months
    as their parent. Instruments already in micro form pass through
    unchanged, and a root with no known micro passes through with a
    one-time warning so the user notices they're still trading full size.
    """
    root, sep, rest = instrument.partition(" ")
    key = root.strip().upper()
    micro = micro_map.get(key)
    if micro:
        return micro + sep + rest
    if key and key not in micro_map.values() and key not in _micro_unmapped_warned:
        _micro_unmapped_warned.add(key)
        logger.warning(f"MICRO MODE  no micro equivalent for '{root}' — instrument sent unchanged")
        _dash_set_alert(
            Fore.YELLOW + f"  ⚠  Micro mode: no micro equivalent for {root} — sent full-size" + Style.RESET_ALL)
    return instrument


def toggle_micro_mode() -> bool:
    """Flip micro conversion on/off, persist it, and reload map overrides."""
    global micro_mode, micro_map
    micro_mode = not micro_mode
    cfg = load_config()
    cfg["micro_mode"] = micro_mode
    save_config(cfg)
    micro_map = load_micro_map(cfg)
    logger.info(f"MICRO MODE  {'ON' if micro_mode else 'OFF'}")
    return micro_mode


# ---------- Global strategy → symbol filter ----------
# One map — "GoldStrat only ever trades GC, NasdaqStrat only NQ" — applied
# to the WHOLE fan-out before any per-account leg exists, so it does not
# have to be repeated in every account's profile (the per-account
# scoped-rule pairs still work and compose on top). A listed strategy may
# only OPEN positions on its listed markets; strategies not listed are
# unrestricted. Exit priority holds: closes are never filtered, and a
# reversal for a filtered-out market is downgraded to a close for every
# account so the old position still exits.

strategy_symbols: dict[str, list[str]] = {}   # publisher strategy (lowercase) -> roots

# Publisher strategy names seen on the wire (field 11), most recent first.
# Feeds the clickable strategy pickers in the terminal and web filter
# editors so the user never has to transcribe a name from the log.
MAX_SEEN_STRATEGIES = 20
pub_strategies_seen: list[str] = []
_seen_dirty = False
_seen_save_last = 0.0
_SEEN_SAVE_INTERVAL = 5.0   # seconds between config writes from the signal path


def _flush_seen(force: bool = False):
    """Persist pub_strategies_seen to config ("strategies_seen").

    Throttled: at most one write per _SEEN_SAVE_INTERVAL, so a server
    spamming ever-new field-11 names cannot turn the signal path into a
    config-write loop. Later signals (exits included) flush what a burst
    left pending; a name lost to an exit mid-window costs only picker
    convenience. Best-effort — never breaks signal handling.
    """
    global _seen_dirty, _seen_save_last
    if not _seen_dirty:
        return
    now = time.monotonic()
    if not force and now - _seen_save_last < _SEEN_SAVE_INTERVAL:
        return
    _seen_save_last = now
    _seen_dirty = False
    try:
        cfg = load_config()
        cfg["strategies_seen"] = list(pub_strategies_seen)
        save_config(cfg)
    except OSError:
        pass


def _record_pub_strategy(name: str):
    """Remember a publisher strategy name for the filter pickers.

    Persisted only when a NEW name appears — strategies are few, so this
    almost never writes on the signal path — via the throttled _flush_seen.
    """
    name = sanitize_ati(name.strip())
    if not name:
        return
    if pub_strategies_seen and pub_strategies_seen[0] == name:
        _flush_seen()
        return
    is_new = name not in pub_strategies_seen
    if not is_new:
        pub_strategies_seen.remove(name)
    pub_strategies_seen.insert(0, name)
    del pub_strategies_seen[MAX_SEEN_STRATEGIES:]
    global _seen_dirty
    _seen_dirty = _seen_dirty or is_new
    _flush_seen()


def _known_roots() -> set[str]:
    """Every instrument root the app knows: catalog fulls + micros, plus
    any user-configured micro-map pairs."""
    roots: set[str] = set()
    for full, _desc, micro, _months, _group in FUTURES_CATALOG:
        roots.add(full)
        if micro:
            roots.add(micro)
    for k, v in micro_map.items():
        roots.add(k)
        if v:
            roots.add(v)
    return roots


def atm_base_key(name: str) -> str:
    """Identity key linking a publisher strategy name to its ATM template.

    The bundled templates carry instrument-prefixed PascalCase filenames
    (GC-MacroZoneB.xml) while publishers send snake_case ids
    (macro_zone_b); this collapses both — and MacroZoneB — to
    'macrozoneb' by stripping a leading '<known root>-' prefix and then
    normalizing away case and separators. Used so a filter keyed by the
    template name still catches the strategy on OTHER instruments, which
    is the whole point of the filter.
    """
    head, sep, tail = name.strip().partition("-")
    if sep and tail.strip() and head.strip().upper() in _known_roots():
        name = tail
    return _norm_atm_name(name)


def load_strategy_symbols(cfg: dict) -> dict[str, list[str]]:
    """Sanitize the config "strategy_symbols" map.

    Keys are publisher strategy names (field 11 of the raw signal, matched
    case-insensitively); values are symbol roots — a list or a "GC, NQ"
    style string. Micro/full twins fold together at match time, so "GC"
    covers MGC. Entries that sanitize to nothing are dropped, which is
    also how the web editor removes one.
    """
    out: dict[str, list[str]] = {}
    raw = cfg.get("strategy_symbols")
    if not isinstance(raw, dict):
        return out
    for strat, syms in raw.items():
        name = sanitize_ati(str(strat).strip()).lower()
        if not name:
            continue
        if isinstance(syms, str):
            syms = syms.replace(",", " ").split()
        if not isinstance(syms, list):
            continue
        roots: list[str] = []
        for sym in syms:
            root = sanitize_ati(str(sym).strip().upper())
            if root and root not in roots:
                roots.append(root)
        if roots:
            out[name] = roots
    return out


def save_strategy_symbols():
    """Persist the active strategy → symbol map to config."""
    cfg = load_config()
    if strategy_symbols:
        cfg["strategy_symbols"] = {k: list(v) for k, v in strategy_symbols.items()}
    else:
        cfg.pop("strategy_symbols", None)
    save_config(cfg)
    logger.info(f"STRATEGY FILTERS SAVED  {strategy_symbols or 'none'}")


def strategy_filter_symbols(pub_strategy: str) -> list[str] | None:
    """Allowed roots for a publisher strategy, or None when unfiltered.

    A filter key matches the raw signal name exactly (lowercased — the
    legacy behavior), or by ATM-template identity: the key and the name
    collapse to the same `atm_base_key`, so a filter saved against the
    template 'GC-MacroZoneB' catches signals carrying 'macro_zone_b', and
    a config `atm_aliases` redirect (publisher id → template) is followed
    too. If several keys collapse onto one strategy (hand-edited config —
    both editors consolidate duplicates), their allowed lists union with
    the exact key's roots first, so the answer does not depend on which
    spelling the wire happens to use.
    """
    strat = (pub_strategy or "").strip()
    if not strat:
        return None
    exact = strategy_symbols.get(strat.lower())
    merged: list[str] = list(exact) if exact else []
    bases = {atm_base_key(strat)}
    alias = atm_aliases.get(strat) or atm_aliases.get(strat.lower())
    if alias:
        bases.add(atm_base_key(alias))
    bases.discard("")
    for key, roots in sorted(strategy_symbols.items()):
        if key != strat.lower() and atm_base_key(key) in bases:
            for root in roots:
                if root not in merged:
                    merged.append(root)
    return merged or None


def strategy_symbol_block(pub_strategy: str, instrument: str) -> str | None:
    """Why the global filter refuses this ENTRY, or None to allow it.

    Never blocks: strategies not in the map, signals with no publisher
    strategy name, and signals with no instrument. Exits are not routed
    through this at all — the caller only consults it for commands that
    open a position.
    """
    allowed = strategy_filter_symbols(pub_strategy)
    if not allowed:
        return None
    root = _instrument_root(instrument)
    if not root:
        return None
    if _symbol_matches(allowed, root):
        return None
    return f"strategy '{pub_strategy}' only trades {', '.join(allowed)}"


def to_full_instrument(instrument: str) -> str:
    """Translate a micro instrument back to full size: "MNQ 06-26" → "NQ 06-26".

    Reverse lookup of the active micro table; symbols a user opted out with a
    self-mapping ("GC": "GC") are excluded so they never flip. Instruments
    already full-size (or unknown) pass through unchanged.
    """
    root, sep, rest = instrument.partition(" ")
    key = root.strip().upper()
    reverse = {v: k for k, v in micro_map.items() if v != k}
    full = reverse.get(key)
    if full:
        return full + sep + rest
    return instrument


# ---------- Per-account trade profiles ----------
# Every copy-trade account (the leader included) can carry a *profile*: a
# default rule plus optional rules scoped by symbol and/or the publisher's
# strategy name (fields 2 and 11 of the incoming signal). A rule reshapes how
# THAT account trades a signal — contract size (micros/full), quantity,
# direction inversion, entry delay, staggered entry, ATM template override,
# and an optional AI gate — independently of the leader and of the global
# micro toggle. A profile can also carry `symbols_allowed`, an account-wide
# market filter: the account only ENTERS trades on those roots (micro/full
# twins included) and simply sits out signals for anything else, while still
# participating fully — copy or round-robin — in the markets it does trade.
#
# Hard safety principle: EXITS ARE NEVER BLOCKED, DELAYED, OR AI-GATED.
# CLOSEPOSITION / CLOSESTRATEGY / CANCEL always flow immediately; a
# REVERSEPOSITION that a rule won't take as a new entry is downgraded to a
# CLOSEPOSITION so the old position still exits. The only exception is
# CHANGE on an inverted account, which is dropped because the publisher's
# price levels are for the opposite side (the account's own ATM template
# manages its stops).
#
# A profile can also carry `prop: true` (plus optional `prop_flat_et` /
# `prop_cutoff_et` ET times), marking the account as a prop-firm funded or
# evaluation account. Prop accounts run under the close-before-open engine:
# one position at a time (a new entry first closes — and CONFIRMS closed —
# whatever the account holds in other markets), no opposite sides across
# accounts, no new entries near the close, and an automatic flatten before
# the firm's own 4:59 PM ET liquidation. See the prop section below.

DEFAULT_RULE = {
    "enabled": True,            # False blocks NEW entries only — exits still flow
    "size": "inherit",          # inherit (global micro toggle) | micros | full
    "qty_mode": "copy",         # copy | fixed | multiple
    "qty_value": 1.0,           # contracts (fixed) or multiplier (multiple)
    "max_contracts": 0,         # hard cap on any single entry; 0 = no cap
    "direction": "normal",      # normal | invert (fade the publisher)
    "delay_ms": 0,              # wait before entering (entries only)
    "delay_jitter_ms": 0,       # + random 0..N ms on top of delay_ms
    "stagger_entries": 1,       # split an entry into N tranches (1 = off)
    "stagger_interval_ms": 1000,  # pause between tranches
    "atm": "",                  # per-account ATM template ("" = session default)
    "ai": None,                 # AI gate config dict, or None (see AI section)
}

RULE_CLAMPS = {
    "qty_value": (0.0, 1000.0),
    "max_contracts": (0, 1000),
    "delay_ms": (0, 600_000),
    "delay_jitter_ms": (0, 600_000),
    "stagger_entries": (1, 10),
    "stagger_interval_ms": (0, 600_000),
}

account_profiles: dict[str, dict] = {}   # account -> {"default": rule, "rules": [rule...]}
_atm_override_warned: set[str] = set()   # missing ATM templates already warned about
_pub_atm_fallback_warned: set[str] = set()  # publisher ATM names already alerted about
_stagger_placed: dict[tuple[str, str], int] = {}  # (account, ati id) -> tranches placed
_MAX_STAGGER_KEYS = 512
_leg_tasks: set[asyncio.Task] = set()    # in-flight deferred legs (delay/AI/stagger)


def _coerce_ai(raw: dict) -> dict | None:
    """Sanitize an AI-gate config dict from disk/editor; None if unusable.

    The endpoint must be http/https and the key env var must look like an
    env var name: an AI gate makes an outbound request carrying an API key,
    so a malformed URL or a stray env name here would leak a secret to the
    wrong place. AI gates are configured from the terminal only — the web
    API refuses to set them (see _web_set_profiles).
    """
    provider = str(raw.get("provider", "")).strip().lower()
    if provider not in AI_PROVIDERS:
        return None
    defaults = AI_PROVIDERS[provider]
    try:
        timeout_ms = int(raw.get("timeout_ms", 8000))
    except (TypeError, ValueError):
        timeout_ms = 8000
    endpoint = str(raw.get("endpoint") or defaults["endpoint"]).strip()
    if endpoint:
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            logger.warning(f"AI GATE  rejected endpoint '{endpoint[:80]}' — need http(s) URL")
            return None
    key_env = str(raw.get("api_key_env") or defaults["key_env"]).strip()
    if key_env and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", key_env):
        logger.warning(f"AI GATE  rejected api_key_env '{key_env[:40]}'")
        return None
    return {
        "provider": provider,
        "model": str(raw.get("model") or defaults["model"]).strip(),
        "endpoint": endpoint,
        "api_key_env": key_env,
        "timeout_ms": max(1000, min(timeout_ms, 60_000)),
        "on_error": "allow" if str(raw.get("on_error", "skip")).lower() == "allow" else "skip",
        "instructions": str(raw.get("instructions", ""))[:2000],
    }


def _coerce_rule(raw: dict, scoped: bool = False) -> dict:
    """Sanitize one rule dict from disk/editor. Unknown keys are dropped,
    known keys are type-coerced and clamped; only keys present in `raw`
    appear in the result (absent = inherit)."""
    out: dict = {}
    if not isinstance(raw, dict):
        return out
    if scoped:
        for key in ("symbols", "strategies"):
            vals = raw.get(key)
            if isinstance(vals, str):
                vals = vals.replace(",", " ").split()
            if isinstance(vals, list):
                cleaned = [str(v).strip() for v in vals if str(v).strip()]
                if cleaned:
                    out[key] = cleaned
    if "enabled" in raw:
        out["enabled"] = bool(raw["enabled"])
    size = str(raw.get("size", "")).strip().lower()
    if size in ("inherit", "micros", "full"):
        out["size"] = size
    qty_mode = str(raw.get("qty_mode", "")).strip().lower()
    if qty_mode in ("copy", "fixed", "multiple"):
        out["qty_mode"] = qty_mode
    if "qty_value" in raw:
        try:
            lo, hi = RULE_CLAMPS["qty_value"]
            out["qty_value"] = max(lo, min(float(raw["qty_value"]), hi))
        except (TypeError, ValueError):
            pass
    for key in ("max_contracts", "delay_ms", "delay_jitter_ms",
                "stagger_entries", "stagger_interval_ms"):
        if key in raw:
            try:
                lo, hi = RULE_CLAMPS[key]
                out[key] = max(lo, min(int(raw[key]), hi))
            except (TypeError, ValueError):
                pass
    direction = str(raw.get("direction", "")).strip().lower()
    if direction in ("normal", "invert"):
        out["direction"] = direction
    if "atm" in raw and isinstance(raw["atm"], str):
        out["atm"] = sanitize_ati(raw["atm"].strip())
    if "ai" in raw:
        out["ai"] = _coerce_ai(raw["ai"]) if isinstance(raw["ai"], dict) else None
    return out


def load_account_profiles(cfg: dict) -> dict[str, dict]:
    """Load and sanitize the "account_profiles" config section."""
    out: dict[str, dict] = {}
    raw = cfg.get("account_profiles")
    if not isinstance(raw, dict):
        return out
    for acct, prof in raw.items():
        if not isinstance(acct, str) or not acct.strip() or not isinstance(prof, dict):
            continue
        entry: dict = {}
        default = _coerce_rule(prof.get("default", {}))
        if default:
            entry["default"] = default
        rules = []
        raw_rules = prof.get("rules", [])
        if isinstance(raw_rules, list):
            for raw_rule in raw_rules:
                rule = _coerce_rule(raw_rule, scoped=True)
                if rule:
                    rules.append(rule)
        if rules:
            entry["rules"] = rules
        allowed = prof.get("symbols_allowed")
        if isinstance(allowed, str):
            allowed = allowed.replace(",", " ").split()
        if isinstance(allowed, list):
            cleaned = []
            for sym in allowed:
                name = str(sym).strip().upper()
                if name and name not in cleaned:
                    cleaned.append(name)
            if cleaned:
                entry["symbols_allowed"] = cleaned
        if prof.get("close_before_open"):
            entry["close_before_open"] = True
        if prof.get("prop"):
            entry["prop"] = True
            firm = str(prof.get("prop_firm", "")).strip().lower()
            if firm:
                entry["prop_firm"] = firm
                if firm not in PROP_FIRM_PRESETS:
                    logger.warning(
                        f"PROP  unknown firm '{firm}' for {acct} — using default "
                        "flat/cutoff times; set prop_flat_et explicitly")
            for key in ("prop_flat_et", "prop_cutoff_et"):
                raw_t = prof.get(key)
                if raw_t in (None, ""):
                    continue
                hm = _parse_prop_hhmm(raw_t)
                if hm is None:
                    logger.warning(
                        f"PROP  {acct} {key}='{raw_t}' rejected — expected 24h "
                        "ET 'HH:MM' between 12:00 and 17:59; using the firm preset")
                    continue
                entry[key] = f"{hm[0]:02d}:{hm[1]:02d}"
            preset = PROP_FIRM_PRESETS.get(
                firm, (PROP_FLAT_ET_DEFAULT, PROP_CUTOFF_ET_DEFAULT))
            eff_flat = _parse_hhmm(entry.get("prop_flat_et")) or preset[0]
            eff_cut = _parse_hhmm(entry.get("prop_cutoff_et")) or preset[1]
            if eff_cut >= eff_flat:
                logger.warning(
                    f"PROP  {acct} entry cutoff {eff_cut[0]:02d}:{eff_cut[1]:02d} is not "
                    f"before its flat time {eff_flat[0]:02d}:{eff_flat[1]:02d} — entries "
                    "could fire after the daily flatten already ran")
        if entry:
            out[acct.strip()] = entry
    return out


def save_account_profiles():
    """Prune empty profiles and persist account_profiles to config."""
    global account_profiles
    pruned: dict[str, dict] = {}
    for acct, prof in account_profiles.items():
        entry: dict = {}
        if prof.get("default"):
            entry["default"] = prof["default"]
        rules = [r for r in prof.get("rules", []) if r]
        if rules:
            entry["rules"] = rules
        if prof.get("symbols_allowed"):
            entry["symbols_allowed"] = prof["symbols_allowed"]
        if prof.get("close_before_open"):
            entry["close_before_open"] = True
        if prof.get("prop"):
            entry["prop"] = True
            for key in ("prop_firm", "prop_flat_et", "prop_cutoff_et"):
                if prof.get(key):
                    entry[key] = prof[key]
        if entry:
            pruned[acct] = entry
    account_profiles = pruned
    cfg = load_config()
    if pruned:
        cfg["account_profiles"] = pruned
    else:
        cfg.pop("account_profiles", None)
    save_config(cfg)
    logger.info(f"PROFILES SAVED  accounts={sorted(pruned)}")


def _instrument_root(instrument: str) -> str:
    return instrument.partition(" ")[0].strip().upper()


def _symbol_matches(rule_symbols: list[str], root: str) -> bool:
    """True when `root` matches any rule symbol or its micro/full twin.

    A rule written as ["NQ"] is meant to cover that market — it matches both
    NQ and MNQ regardless of the global micro toggle or per-account sizing.
    """
    if not root:
        return False
    reverse = {v: k for k, v in micro_map.items()}
    targets: set[str] = set()
    for sym in rule_symbols:
        key = str(sym).strip().upper()
        if not key:
            continue
        targets.add(key)
        if key in micro_map:
            targets.add(micro_map[key])
        if key in reverse:
            targets.add(reverse[key])
    return root in targets


def account_trades_symbol(account: str, instrument: str) -> bool:
    """Per-account symbol filter: does `account` trade this instrument at all?

    An account whose profile carries `symbols_allowed` (e.g. ["GC"]) only
    ENTERS trades on those markets — micro/full twins included, so "GC"
    covers MGC and "NQ" covers MNQ. An empty or absent filter trades
    everything. Signals without an instrument (CLOSESTRATEGY/CANCEL by id)
    pass — the filter gates entries, and exits are never blocked anyway.
    """
    allowed = account_profiles.get(account, {}).get("symbols_allowed") or []
    if not allowed:
        return True
    root = _instrument_root(instrument)
    if not root:
        return True
    return _symbol_matches(allowed, root)


# ---------- Prop-firm account mode ----------
# A profile with "prop": true marks a funded / evaluation account at a
# futures prop firm. Those firms ban configurations an ordinary broker
# account is free to run, and the violations that matter here are judged on
# the resulting positions — not intent — with account closure and profit
# forfeiture on the line. What the app enforces for prop accounts:
#
#   1. ONE POSITION AT A TIME (close-before-open). Before a new entry
#      fires, every position the account holds in a DIFFERENT market is
#      closed and the close is CONFIRMED against NinjaTrader. Two
#      strategies signalling two markets (a GC position, then an NQ entry)
#      would otherwise stack concurrent positions — within the letter of
#      most firms' rules when same-direction, but the user's chosen
#      safe-common-denominator policy, and the only shape that can never
#      drift into a correlated-hedge violation (Tradeify, for one, bans
#      opposing positions across an entire product GROUP — long ES /
#      short NQ counts).
#   2. NO OPPOSITE SIDES ACROSS ACCOUNTS. The hedge guard escalates to
#      `block` whenever a prop account is part of an opposite-entry
#      fan-out, and before a prop entry fires, any OTHER managed prop
#      account still holding the opposite side of that product group is
#      flattened first. Apex bans opposing positions "across multiple
#      accounts" on pain of closure; Topstep calls a hedge unappealable
#      "even if the overlap is brief or unintentional".
#   3. FLAT BY CLOSE. Firms auto-liquidate near the CME close and some
#      treat a held position as an outright breach (MyFundedFutures:
#      holding past 4:10 PM ET breaches the account). Prop accounts are
#      flattened at their flat-by-close time and new entries are refused
#      from the cutoff until the 18:00 ET Globex reopen (Friday's cutoff
#      holds through the weekend).
#
# Firm deadlines differ, so `prop_firm` picks safe defaults and
# `prop_flat_et` / `prop_cutoff_et` ("HH:MM" ET) override them. Times sit
# a couple of minutes AHEAD of each firm's own deadline so market closes
# fill before the firm's risk engine acts. See PROP_RULES.md for sources.

PROP_FLAT_ET_DEFAULT = (16, 57)     # ET flatten time when no firm preset applies
PROP_CUTOFF_ET_DEFAULT = (16, 55)   # ET entry cutoff when no firm preset applies
PROP_REOPEN_HOUR_ET = 18            # Globex reopen — prop entries allowed again
PROP_VERIFY_TRIES = 4               # close-before-open confirm polls (× FLATTEN_VERIFY_DELAY)

# firm -> (flat_et, cutoff_et). Aliases share one entry. Each flat time sits
# ahead of the firm's own deadline (breach or auto-liquidation) so our closes
# fill first; sources for every deadline are in PROP_RULES.md.
PROP_FIRM_PRESETS: dict[str, tuple[tuple[int, int], tuple[int, int]]] = {
    "apex":             ((16, 57), (16, 55)),  # Apex auto-liquidates 4:59 PM ET
    "topstep":          ((16, 5), (16, 2)),    # Topstep: flat 4:10, staff flatten 4:08 PM ET
    "mffu":             ((16, 7), (16, 5)),    # MFFU: held past 4:10 PM ET = breach
    "myfundedfutures":  ((16, 7), (16, 5)),
    "tpt":              ((16, 52), (16, 50)),  # TPT auto-closes 4:55 PM ET
    "takeprofittrader": ((16, 52), (16, 50)),
    "tradeify":         ((16, 57), (16, 55)),  # Tradeify auto-closes 4:59 PM ET
    "bulenox":          ((16, 57), (16, 55)),  # flat by 3:59 PM CT (= 4:59 PM ET)
    "elite":            ((16, 57), (16, 55)),  # ETF: 1 min before instrument close
    "etf":              ((16, 57), (16, 55)),
    "fundednext":       ((16, 57), (16, 55)),  # flat by end of trading day (5 PM ET)
    "alpha":            ((16, 17), (16, 15)),  # Alpha: closed before 4:20 PM ET
    "lucid":            ((16, 42), (16, 40)),  # Lucid auto-liquidates 4:45 PM ET
}


def is_prop_account(account: str) -> bool:
    """True when the account's profile carries "prop": true."""
    return bool(account_profiles.get(account, {}).get("prop"))


def closes_before_open(account: str) -> bool:
    """True when this account's ENTRIES run the close-before-open engine.

    Every prop account does. `close_before_open: true` opts an ORDINARY
    account into the same one-position-at-a-time entry behavior — the
    close-everything-then-place the publisher's server used to provide —
    without prop's time rules (no entry cutoff, no flat-by-close) and
    without being swept by other accounts' cross-account hedge preemption
    (that ban is a prop-firm rule, not theirs). Accounts with neither flag
    are never preemptively closed: multi-market / multi-direction trading.
    """
    prof = account_profiles.get(account, {})
    return bool(prof.get("prop") or prof.get("close_before_open"))


def multi_strategy_enabled() -> bool:
    """`multi_strategy: true` — coexist with a publisher running SEVERAL
    strategies whose entries arrive as stateless close-then-place pairs.

    Off (the default) every publisher CLOSEPOSITION flattens the whole
    instrument — correct for one strategy, but with two strategies sharing
    a market each entry's paired close tramples the other's just-opened
    position (open → closed 2s later → reopened; see 2026-08-31 01:10).
    On, closes are scoped to the strategy that sent them via the strategy
    ledger, same-direction entries stack (within `max_contracts`), and an
    opposite entry still resets the market first. Every unprovable or
    unattributable case falls back to the verbatim close — the flag never
    makes a close less likely to fire than the publisher intended unless
    the position provably belongs to a different live strategy.
    """
    return bool(load_config().get("multi_strategy", False))


def _parse_hhmm(raw) -> tuple[int, int] | None:
    """Parse an 'HH:MM' Eastern-time string; None when unusable."""
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", str(raw or "").strip())
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    return (h, mi) if h < 24 and mi < 60 else None


def _parse_prop_hhmm(raw) -> tuple[int, int] | None:
    """Parse a prop flat/cutoff time: 24h ET 'HH:MM' between 12:00 and 17:59.

    Every firm's deadline sits between 16:00 and 17:00 ET, and an
    unbounded parse would read a 12h-style "4:55" as 4:55 AM — flattening
    overnight positions at dawn and leaving the real close uncovered.
    EVERY ingest point (config loader, terminal editor, the runtime
    getters below) must use this, never bare _parse_hhmm.
    """
    hm = _parse_hhmm(raw)
    return hm if hm is not None and 12 <= hm[0] < 18 else None


def _prop_preset(account: str) -> tuple[tuple[int, int], tuple[int, int]]:
    firm = str(account_profiles.get(account, {}).get("prop_firm", "")).strip().lower()
    return PROP_FIRM_PRESETS.get(firm, (PROP_FLAT_ET_DEFAULT, PROP_CUTOFF_ET_DEFAULT))


def prop_flat_time(account: str) -> tuple[int, int]:
    """ET (hour, minute) this prop account is force-flattened at."""
    return (_parse_prop_hhmm(account_profiles.get(account, {}).get("prop_flat_et"))
            or _prop_preset(account)[0])


def prop_cutoff_time(account: str) -> tuple[int, int]:
    """ET (hour, minute) after which this prop account opens no new entry.

    Clamped to at least 2 minutes BEFORE the account's flat time: a custom
    flat time with a preset cutoff can otherwise invert the ordering, and
    an entry landing between the once-per-day flatten and a later cutoff
    would ride past the firm's deadline with nothing left to flatten it.
    """
    cut = (_parse_prop_hhmm(account_profiles.get(account, {}).get("prop_cutoff_et"))
           or _prop_preset(account)[1])
    fh, fm = prop_flat_time(account)
    latest = divmod(fh * 60 + fm - 2, 60)
    return min(cut, latest)


def _prop_entry_blocked_now(account: str, now_et: datetime | None = None) -> bool:
    """True inside the account's no-new-entries window around the close.

    Runs from the account's entry cutoff until the 18:00 ET Globex reopen.
    Friday's cutoff holds through the weekend until Sunday 18:00 ET. Exits
    are never blocked by this — it is consulted only for entry legs.
    """
    now = now_et or datetime.now(ET)
    wd = now.weekday()
    if wd == 5:                                   # Saturday
        return True
    if wd == 6:                                   # Sunday, pre-reopen
        return now.hour < PROP_REOPEN_HOUR_ET
    past_cutoff = (now.hour, now.minute) >= prop_cutoff_time(account)
    if wd == 4:                                   # Friday: closed into the weekend
        return past_cutoff
    return past_cutoff and now.hour < PROP_REOPEN_HOUR_ET


def _product_group(alias: str) -> str:
    """Correlation group for an instrument alias, per the futures catalog.

    Tradeify bans opposing positions across a whole product group (long ES
    against short NQ is a violation — both "Equity index"), so cross-account
    conflict checks match on this rather than on the bare underlying. An
    instrument the catalog doesn't know falls back to its own underlying
    root, which degrades to the same-market-only check.
    """
    root = _underlying_root(alias)
    for full, _desc, _micro, _months, group in FUTURES_CATALOG:
        if root == full:
            return group
    return root


def resolve_rule(account: str, instrument: str = "", pub_strategy: str = "") -> dict:
    """Effective rule for (account, signal): defaults ← account default ←
    first matching scoped rule. Scoped rules are evaluated in config order;
    a rule matches when its symbols AND strategies filters both pass (an
    empty filter passes everything)."""
    rule = dict(DEFAULT_RULE)
    prof = account_profiles.get(account)
    if not prof:
        return rule
    for key, val in prof.get("default", {}).items():
        if key in DEFAULT_RULE:
            rule[key] = val
    root = _instrument_root(instrument)
    strat = (pub_strategy or "").strip().lower()
    for scoped in prof.get("rules", []):
        symbols = scoped.get("symbols") or []
        strategies = scoped.get("strategies") or []
        if symbols and not _symbol_matches(symbols, root):
            continue
        if strategies and strat not in {str(s).strip().lower() for s in strategies}:
            continue
        for key, val in scoped.items():
            if key in DEFAULT_RULE:
                rule[key] = val
        break
    return rule


def explicit_rule_keys(account: str, instrument: str = "", pub_strategy: str = ""
                       ) -> set[str]:
    """Rule keys this account sets ITSELF for this signal, as opposed to
    inheriting. Mirrors resolve_rule's precedence: account default, then
    the first matching scoped rule."""
    prof = account_profiles.get(account)
    if not prof:
        return set()
    keys = {k for k in prof.get("default", {}) if k in DEFAULT_RULE}
    root = _instrument_root(instrument)
    strat = (pub_strategy or "").strip().lower()
    for scoped in prof.get("rules", []):
        symbols = scoped.get("symbols") or []
        strategies = scoped.get("strategies") or []
        if symbols and not _symbol_matches(symbols, root):
            continue
        if strategies and strat not in {str(s).strip().lower() for s in strategies}:
            continue
        keys |= {k for k in scoped if k in DEFAULT_RULE}
        break
    return keys


def profiles_active() -> bool:
    return bool(account_profiles)


def _qty_label(rule: dict) -> str:
    if rule["qty_mode"] == "fixed":
        return f"fixed {int(rule['qty_value'])}"
    if rule["qty_mode"] == "multiple":
        return f"x{rule['qty_value']:g}"
    return "copy"


def profile_summary(account: str) -> str:
    """One-line profile description for menus/status."""
    prof = account_profiles.get(account)
    if not prof:
        return "default"
    base = {**DEFAULT_RULE, **prof.get("default", {})}
    bits: list[str] = []
    if prof.get("prop"):
        fh, fm = prop_flat_time(account)
        firm = prof.get("prop_firm", "")
        bits.append(f"PROP{f' {firm}' if firm else ''} flat {fh:02d}:{fm:02d}")
    elif prof.get("close_before_open"):
        bits.append("CLOSE-B4-OPEN")
    if prof.get("symbols_allowed"):
        bits.append(f"only {','.join(prof['symbols_allowed'])}")
    if not base["enabled"]:
        bits.append("ENTRIES OFF")
    if base["size"] != "inherit":
        bits.append(base["size"])
    if base["qty_mode"] != "copy":
        bits.append(_qty_label(base))
    if base["max_contracts"]:
        bits.append(f"cap {base['max_contracts']}")
    if base["direction"] == "invert":
        bits.append("INVERT")
    if base["delay_ms"] or base["delay_jitter_ms"]:
        jit = f"+~{base['delay_jitter_ms']}" if base["delay_jitter_ms"] else ""
        bits.append(f"delay {base['delay_ms']}{jit}ms")
    if base["stagger_entries"] > 1:
        bits.append(f"{base['stagger_entries']}×{base['stagger_interval_ms']}ms")
    if base["atm"]:
        bits.append(f"ATM:{base['atm']}")
    if base["ai"]:
        bits.append(f"AI:{base['ai']['provider']}")
    n_rules = len(prof.get("rules", []))
    if n_rules:
        bits.append(f"{n_rules} scoped rule{'s' if n_rules > 1 else ''}")
    return " · ".join(bits) or "default"


def _flip_action(action: str) -> str:
    up = action.strip().upper()
    if up == "BUY":
        return "SELL"
    if up == "SELL":
        return "BUY"
    return action


def _apply_size(instrument: str, mode: str) -> str:
    if mode == "micros":
        return to_micro_instrument(instrument)
    if mode == "full":
        return to_full_instrument(instrument)
    return instrument


def _rule_qty(orig_qty: int, rule: dict) -> int:
    """Contracts this account should trade; 0 means skip the entry."""
    mode = rule["qty_mode"]
    if mode == "fixed":
        qty = int(rule["qty_value"])
    elif mode == "multiple":
        # round half up — round() banker's-rounds 0.5 down to 0
        qty = math.floor(orig_qty * float(rule["qty_value"]) + 0.5)
    else:
        qty = orig_qty
    cap = int(rule.get("max_contracts") or 0)
    if cap > 0:
        qty = min(qty, cap)
    return max(qty, 0)


def _apply_atm_override(parts: list[str], rule: dict):
    """Swap field 11 to the rule's ATM template when it exists locally."""
    name = rule.get("atm") or ""
    if not name or len(parts) < 12:
        return
    if validate_strategy(name):
        parts[11] = name
    elif name not in _atm_override_warned:
        _atm_override_warned.add(name)
        logger.warning(f"ATM OVERRIDE  template '{name}' not installed — using '{parts[11]}'")
        _dash_set_alert(
            Fore.YELLOW + f"  ⚠  Profile ATM '{name}' not found — using {parts[11]}" + Style.RESET_ALL)


def transform_signal_for_account(signal_text: str, account: str, rule: dict
                                 ) -> tuple[str | None, str | None, dict]:
    """Reshape one canonical signal for one account per its resolved rule.

    Returns (final_signal, skip_reason, meta). final_signal is None when the
    leg is skipped (reason set). meta carries the final instrument / action /
    qty and a human note when a command was downgraded. Exits always come
    back as a signal — never as a skip — except CHANGE on an inverted
    account (see module docstring).
    """
    parts = signal_text.split(";")
    cmd = parts[0].strip().upper() if parts else ""
    meta = {"instrument": "", "action": "", "qty": 0, "note": ""}

    if cmd == "CHANGE" and rule["direction"] == "invert":
        return None, "CHANGE dropped (inverted account)", meta

    if len(parts) >= 3 and parts[2]:
        parts[2] = _apply_size(parts[2], rule["size"])
        meta["instrument"] = parts[2]

    def _close_instead(why: str) -> tuple[str, None, dict]:
        instrument = parts[2] if len(parts) >= 3 else ""
        meta["note"] = f"downgraded to CLOSEPOSITION ({why})"
        close = f"CLOSEPOSITION;{parts[1] if len(parts) >= 2 else ''};{instrument};;;;;;;;;;"
        return _with_account(close, account), None, meta

    if cmd in ("PLACE", "REVERSEPOSITION"):
        order_type = parts[5].strip().upper() if len(parts) > 5 else ""
        inverted_nonmarket = (rule["direction"] == "invert"
                              and order_type not in ("", "MARKET"))
        try:
            orig_qty = int(parts[4])
        except (IndexError, ValueError):
            orig_qty = None
        qty = _rule_qty(orig_qty, rule) if orig_qty is not None else None

        symbol_ok = account_trades_symbol(
            account, parts[2] if len(parts) >= 3 else "")
        if cmd == "PLACE":
            if not symbol_ok:
                return None, f"symbol filtered ({_instrument_root(parts[2])})", meta
            if not rule["enabled"]:
                return None, "entries disabled", meta
            if inverted_nonmarket:
                return None, f"inverted {order_type} entry skipped", meta
            if qty is not None and qty < 1:
                return None, "sized to 0 contracts", meta
        else:  # REVERSEPOSITION — exit priority: never skip, downgrade instead
            if not symbol_ok:
                return _close_instead("symbol filtered")
            if not rule["enabled"]:
                return _close_instead("entries disabled")
            if inverted_nonmarket:
                return _close_instead("inverted non-market reversal")
            if qty is not None and qty < 1:
                return _close_instead("sized to 0 contracts")
            if is_prop_account(account) and _prop_entry_blocked_now(account):
                # A reversal OPENS the other side, and past the cutoff the
                # once-per-day flatten has fired or is about to — nothing
                # would re-flatten the fresh position before the firm's
                # deadline. The old position still exits.
                return _close_instead("prop flat-by-close window")

        if rule["direction"] == "invert" and len(parts) > 3:
            parts[3] = _flip_action(parts[3])
        if qty is not None:
            parts[4] = str(qty)
            meta["qty"] = qty
        _apply_atm_override(parts, rule)
        meta["action"] = parts[3] if len(parts) > 3 else ""

    return _with_account(";".join(parts), account), None, meta


def split_qty(total: int, tranches: int) -> list[int]:
    """Split contracts across tranches, front-loaded: 5 into 3 → [2, 2, 1]."""
    n = max(1, min(int(tranches), total))
    base, rem = divmod(total, n)
    return [base + 1 if i < rem else base for i in range(n)]


def _tranche_signal(signal_text: str, qty: int, tranche_idx: int) -> str:
    """Per-tranche copy of an entry: qty swapped; tranche 2+ gets ~T<k> id
    suffixes so NT never sees two orders sharing one instance-global id."""
    parts = signal_text.split(";")
    if len(parts) > 4 and parts[4]:
        parts[4] = str(qty)
    if tranche_idx > 0:
        for field in _ATI_GLOBAL_ID_FIELDS:
            if len(parts) > field and parts[field]:
                parts[field] = f"{parts[field]}~T{tranche_idx + 1}"
    return ";".join(parts)


def _record_stagger(signal_text: str, account: str, count: int):
    """Remember how many tranches an entry's ids were fanned into so a later
    CLOSESTRATEGY / CANCEL / CHANGE can target every tranche."""
    parts = signal_text.split(";")
    for field in _ATI_GLOBAL_ID_FIELDS:
        if len(parts) > field and parts[field]:
            _stagger_placed[(account, parts[field])] = count
    while len(_stagger_placed) > _MAX_STAGGER_KEYS:
        _stagger_placed.pop(next(iter(_stagger_placed)))


def _max_profile_stagger(account: str) -> int:
    """Largest stagger an account's profile can produce (restart fallback)."""
    prof = account_profiles.get(account)
    if not prof:
        return 1
    n = int(prof.get("default", {}).get("stagger_entries", 1) or 1)
    for rule in prof.get("rules", []):
        n = max(n, int(rule.get("stagger_entries", 1) or 1))
    return max(1, min(n, RULE_CLAMPS["stagger_entries"][1]))


def _expand_exit_ids(signal_text: str, account: str) -> list[str]:
    """Fan an id-targeted command (CLOSESTRATEGY/CANCEL/CHANGE) to every
    tranche this account placed under that id. Uses the recorded tranche
    count; falls back to the profile's max stagger after a restart. Extra
    variants targeting ids that never existed are rejected by NT harmlessly."""
    parts = signal_text.split(";")
    cmd = parts[0].strip().upper() if parts else ""
    id_field = 12 if cmd == "CLOSESTRATEGY" else 10
    base_id = parts[id_field] if len(parts) > id_field else ""
    if not base_id:
        return [signal_text]
    count = _stagger_placed.get((account, base_id))
    if count is None:
        count = _max_profile_stagger(account)
    if count <= 1:
        return [signal_text]
    out = [signal_text]
    for k in range(2, count + 1):
        variant = list(parts)
        for field in _ATI_GLOBAL_ID_FIELDS:
            if len(variant) > field and variant[field]:
                variant[field] = f"{variant[field]}~T{k}"
        out.append(";".join(variant))
    return out


def publisher_strategy_of(msg: str) -> str:
    """Publisher's ATM strategy name (field 11) from the raw ws message —
    extract_signal_string overwrites it, and profile rules scope on it."""
    try:
        data = json.loads(msg)
        parts = str(data.get("signal", "")).split(";")
        return sanitize_ati(parts[11].strip()) if len(parts) >= 12 else ""
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
        return ""


def publisher_envelope_of(msg: str) -> dict:
    """Strategy attribution from the ws envelope, for multi-strategy mode.

    The publisher tags every event with its originating strategy OUTSIDE
    the ATI string — observed wire shapes (RAW ENVELOPE, 2026-08-31):
      entry: {"event": "entry",          "strategy": "bread_n_butter",
              "event_id": "ent:nq_bread_n_butter:...", "signal": "PLACE;..."}
      close: {"event": "close_position", "strategy": "bread_n_butter",
              "event_id": "ent:...:close", "signal": "CLOSEPOSITION;..."}
    The ATI string itself can't carry this: a CLOSEPOSITION has no
    strategy field, and field 11 of entries is overwritten with the local
    ATM template before anything downstream sees it. Returns {} when the
    envelope has no attribution — every consumer treats that as "smart
    close handling unavailable" and keeps verbatim behavior.
    """
    try:
        data = json.loads(msg)
        if not isinstance(data, dict):
            return {}
        strategy = sanitize_ati(str(data.get("strategy") or "").strip())
        if not strategy:
            return {}
        return {"strategy": strategy,
                "event": str(data.get("event") or "").strip(),
                "event_id": sanitize_ati(str(data.get("event_id") or "").strip())}
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
        return {}


def plan_signal_legs(signal_text: str, pub_strategy: str = "",
                     manual: bool = False, envelope: dict | None = None,
                     source: str = ""
                     ) -> tuple[list[dict], list[tuple[str, str]]]:
    """Resolve one canonical signal into per-account leg plans.

    Returns (plans, skipped). Each plan: account, rule, final signal, files
    to write now (exit-id fan-out included), deferred flag (delay/AI/stagger
    entries run as a background task), and display metadata. With no
    profiles configured every leg is an instant identical copy — the classic
    fan-out.

    `envelope` is publisher_envelope_of(raw ws message) — strategy
    attribution that only exists outside the ATI string. With
    multi_strategy mode on it lets a CLOSEPOSITION be scoped or skipped
    per account and same-direction entries stack; absent (manual/web
    paths, publishers without it) every leg keeps verbatim behavior.
    """
    plans: list[dict] = []
    skipped: list[tuple[str, str]] = []
    ms_on = multi_strategy_enabled() and not manual
    parts = signal_text.split(";")
    cmd = parts[0].strip().upper() if parts else ""
    # The command AS THE PUBLISHER SENT IT. Downgrades below (global
    # filter, hedge guard, per-account transform) rewrite `cmd` to
    # CLOSEPOSITION, but those closes carry REVERSAL exit intent — they
    # must flatten whatever the account holds, whoever opened it — so
    # smart-close scoping keys on this, never on the rewritten command.
    publisher_cmd = cmd
    instrument = parts[2] if len(parts) >= 3 else ""

    # Contract-month roll: a publisher still signalling an expiring or
    # expired month gets every entry rejected broker-side ("Liquidation
    # only, contract is about to be expired" — 2026-08-27 11:45:26), so
    # the month is corrected to the known front BEFORE the fan-out.
    # Exits are corrected too — a rolled entry lives in the NEW month, so
    # its closes must follow — while the ORIGINAL month is kept as extra
    # close legs below, so a position opened pre-roll in the old contract
    # can never be stranded. Rolls only move FORWARD on cataloged roots;
    # deliberate back-month signals and exotic contracts pass untouched.
    stale_month_instr = None
    if cmd in ("PLACE", "REVERSEPOSITION", "CLOSEPOSITION") and instrument:
        fixed, was = correct_contract_month(instrument)
        if was:
            parts[2] = fixed
            signal_text = ";".join(parts)
            instrument = fixed
            if cmd != "PLACE":
                stale_month_instr = was
            logger.warning(f"MONTH ROLL  {was} → {fixed}  cmd={cmd}")
            _dash_set_alert(Fore.YELLOW + f"  ↷  ROLLED {was} → {fixed} "
                            "(front month)" + Style.RESET_ALL)

    # Global strategy → symbol filter: gates the whole fan-out before any
    # leg — including the round-robin draw — exists. Entries for a
    # non-listed market are refused outright; a reversal is downgraded to
    # a close for every account so the old position still exits (NT
    # no-ops the close on accounts holding nothing). Pure exits never
    # reach this check.
    if cmd in ("PLACE", "REVERSEPOSITION"):
        why = strategy_symbol_block(pub_strategy, instrument)
        if why:
            if cmd == "PLACE":
                logger.info(f"GLOBAL FILTER  entry dropped — {why}  signal={signal_text}")
                return [], [("all accounts", why)]
            logger.info(f"GLOBAL FILTER  reversal downgraded to close — {why}")
            signal_text = (f"CLOSEPOSITION;{parts[1] if len(parts) >= 2 else ''};"
                           f"{instrument};;;;;;;;;;")
            parts = signal_text.split(";")
            cmd = "CLOSEPOSITION"

    # Which account gets which canonical signal. Copy-trade accounts always
    # get the signal as-is. Round-robin accounts rotate: entries go to ONE
    # pool member; exits fan to the whole pool (only the holder's position/
    # ids match — the rest are rejected by NT harmlessly). REVERSEPOSITION
    # sends the reversal to the rotation pick and a CLOSEPOSITION to every
    # other pool member, so whoever holds the old position still exits.
    # Per-account symbol filters compose with both: a filtered copy account
    # skips the entry in its transform below; a filtered pool account is
    # never drawn for that instrument (_rr_next passes over it, slot kept).
    legs: list[tuple[str, str, bool]] = [  # (account, canonical signal, is_rr_pick)
        (a, signal_text, False)
        for a in copy_trade_accounts() if a not in account_stops]
    rr_pick: str | None = None
    if roundrobin_accounts:
        if cmd == "PLACE":
            rr_pick = _rr_next(instrument, pub_strategy)
            if rr_pick:
                legs.append((rr_pick, signal_text, True))
        elif cmd == "REVERSEPOSITION":
            rr_pick = _rr_next(instrument, pub_strategy)
            synth_close = (f"CLOSEPOSITION;{parts[1] if len(parts) >= 2 else ''};"
                           f"{instrument};;;;;;;;;;")
            for a in _rr_pool():
                if a == rr_pick:
                    legs.append((a, signal_text, True))
                else:
                    logger.info(f"RR REVERSE  account={a}  gets close-only leg")
                    legs.append((a, synth_close, False))
        else:
            legs += [(a, signal_text, False) for a in _rr_pool()]
        if rr_pick:
            logger.info(f"ROUND-ROBIN  pick={rr_pick}  remaining={_rr_remaining}")

    # The LEADER'S direction is dominant: it is the reference account, so
    # when it fades the publisher the whole group fades with it. Accounts
    # that do not set `direction` themselves inherit the leader's — without
    # this, turning invert on for the leader alone would put every follower
    # on the opposite side of it. An account that DOES set its own
    # direction keeps it, and the hedge guard warns about the divergence.
    leader_direction = "normal"
    if active_account:
        leader_direction = resolve_rule(
            active_account, instrument, pub_strategy)["direction"]

    for account, canonical, is_rr_pick in legs:
        rule = resolve_rule(account, instrument, pub_strategy)
        if (account != active_account
                and "direction" not in explicit_rule_keys(account, instrument, pub_strategy)):
            rule["direction"] = leader_direction
        final, skip_reason, meta = transform_signal_for_account(canonical, account, rule)
        if final is None:
            skipped.append((account, skip_reason or "skipped"))
            logger.info(f"LEG SKIPPED  account={account}  reason={skip_reason}  signal={canonical}")
            if is_rr_pick:
                _rr_return(account)   # nothing was placed — don't burn the turn
            continue
        final_parts = final.split(";")
        final_cmd = final_parts[0].strip().upper() if final_parts else ""
        base_deferred = final_cmd == "PLACE" and (
            rule["delay_ms"] > 0 or rule["delay_jitter_ms"] > 0
            or rule["stagger_entries"] > 1 or bool(rule["ai"]))
        # A close-before-open account's entry (prop, or the opt-in flag)
        # ALWAYS runs as a background leg: it must first close-and-CONFIRM
        # whatever the account holds (see the prop section), and that
        # confirmation cannot sit on the signal intake path.
        prop = is_prop_account(account)
        cbo = closes_before_open(account)
        deferred = base_deferred or (cbo and final_cmd == "PLACE")
        files = [final]
        if final_cmd in ("CLOSESTRATEGY", "CANCEL", "CHANGE"):
            files = _expand_exit_ids(final, account)
        elif final_cmd == "CLOSEPOSITION" and len(final_parts) >= 3 and final_parts[2]:
            # Send the close for BOTH contract sizes. The exit's instrument
            # is recomputed from current state, but the position was opened
            # under whatever the rule said THEN — a strategy-scoped `size`
            # rule does not match an exit (exits carry no strategy name),
            # and the global micro toggle can flip mid-position. Either way
            # the close would address a contract the account is not holding.
            # NT no-ops the variant that is not held, exactly as the
            # existing alias fan-out relies on.
            twin = _apply_size(final_parts[2],
                               "full" if final_parts[2] != to_full_instrument(final_parts[2])
                               else "micros")
            if twin and twin != final_parts[2]:
                alt = list(final_parts)
                alt[2] = twin
                files.append(";".join(alt))
        if (stale_month_instr and " " in stale_month_instr
                and final_cmd in ("CLOSEPOSITION", "REVERSEPOSITION")
                and len(final_parts) >= 3 and final_parts[2]):
            # The exit's month was rolled forward, but a position opened
            # BEFORE the roll still sits in the old contract — send plain
            # closes for the old month too (both contract sizes, matching
            # the twin fan-out above). NT no-ops every variant the account
            # does not hold. Always CLOSEPOSITION, never a reversal: an
            # old-month reversal would open a fresh position in a dying
            # contract.
            old_tail = stale_month_instr.split(" ", 1)[1]
            leg_acct = final_parts[1] if len(final_parts) >= 2 else ""
            old_inst = f"{final_parts[2].split(' ', 1)[0]} {old_tail}"
            old_twin = _apply_size(old_inst,
                                   "full" if old_inst != to_full_instrument(old_inst)
                                   else "micros")
            for variant in (old_inst, old_twin):
                if not variant:
                    continue
                sig2 = f"CLOSEPOSITION;{leg_acct};{variant};;;;;;;;;;"
                if sig2 not in files:
                    files.append(sig2)

        # ---- Multi-strategy mode: per-account close scoping & stacking.
        # Everything below is provable-state-only: no fresh bridge book,
        # no envelope attribution, or any inconsistency → verbatim legs.
        reset_first = False
        if (ms_on and publisher_cmd == "CLOSEPOSITION"
                and final_cmd == "CLOSEPOSITION"
                and envelope and envelope.get("strategy")):
            verdict, scoped, why = _smart_close_decision(
                account, final_parts[2] if len(final_parts) >= 3 else "",
                (envelope.get("strategy", ""), pub_strategy))
            if verdict == "drop":
                skipped.append((account, f"smart close: {why}"))
                logger.info(f"SMART CLOSE  account={account}  suppressed — "
                            f"{why}  signal={final}")
                continue
            if verdict == "scope":
                files = scoped
                note = f"smart close: {why}"
                meta["note"] = f"{meta['note']}; {note}" if meta["note"] else note
                logger.info(
                    f"SMART CLOSE  account={account}  {why}  "
                    f"ids={[f.split(';')[12] for f in scoped]}")
            elif why not in ("no live book to prove state",
                             "sole holder — full close"):
                logger.info(f"SMART CLOSE  account={account}  pass-through — {why}")
        elif ms_on and final_cmd == "PLACE":
            inst = meta["instrument"] or (final_parts[2] if len(final_parts) >= 3 else "")
            exp = _account_root_exposure(account, inst) if inst else None
            if exp is not None and not exp["twin"]:
                side = 1 if (meta["action"] or "").strip().upper() == "BUY" else -1
                held = exp["shown"] if exp["shown"] else exp["inflight"]
                if held and (held > 0) == (side > 0):
                    # Same-direction stack. max_contracts (when set) is the
                    # AGGREGATE cap on the root: skip a full stack — a
                    # missed trade is recoverable, closing teammates'
                    # positions to make room is not what anyone asked —
                    # and resize an entry that only partly fits.
                    cap = int(rule["max_contracts"] or 0)
                    if cap > 0:
                        room = cap - max(abs(exp["shown"]), abs(exp["inflight"]))
                        if room <= 0:
                            skipped.append(
                                (account, f"stacked to max_contracts ({cap})"))
                            logger.info(f"STACK FULL  account={account}  "
                                        f"cap={cap}  entry skipped")
                            if is_rr_pick:
                                _rr_return(account)
                            continue
                        if meta["qty"] and meta["qty"] > room:
                            final_parts[4] = str(room)
                            final = ";".join(final_parts)
                            files = [final]
                            note = f"stack resized {meta['qty']}→{room} (max_contracts {cap})"
                            logger.info(f"STACK RESIZE  account={account}  {note}")
                            meta["qty"] = room
                            meta["note"] = (f"{meta['note']}; {note}"
                                            if meta["note"] else note)
                elif held and not cbo:
                    # Opposite side of a standing (teammate-strategy)
                    # position: its paired close was scoped away, so this
                    # entry restores the publisher's reset — full close
                    # first, settle, then place. Close-before-open
                    # accounts already do exactly this in their preempt.
                    reset_first = True
                    deferred = True

        if meta["note"]:
            logger.info(f"LEG NOTE  account={account}  {meta['note']}  signal={canonical}")
        plans.append({
            "account": account,
            "rule": rule,
            "signal": final,
            "files": files,
            "deferred": deferred,
            "command": final_cmd,
            "instrument": meta["instrument"] or (final_parts[2] if len(final_parts) >= 3 else ""),
            "action": meta["action"],
            "qty": meta["qty"],
            "note": meta["note"],
            "rr_pick": is_rr_pick,
            "manual": manual,
            # Which surface fired it, so the P&L calendar can tell a click
            # in the web UI from one in the terminal from a chart trade.
            "manual_source": source,
            "prop": prop,
            "cbo": cbo,
            "reset_first": reset_first,
            "strat_ident": ((envelope or {}).get("strategy", ""), pub_strategy),
            # Simple close-before-open entries (no delay/AI/stagger of
            # their own) are batched into ONE close-confirm-enter wave so
            # N accounts cost one snapshot and one confirmation instead
            # of N.
            "prop_group": cbo and final_cmd == "PLACE" and not base_deferred,
        })
    plans = apply_hedge_guard(plans, skipped)
    return plans, skipped


def _entry_direction_conflict(plans: list[dict]) -> dict[str, dict[str, list[str]]]:
    """Entry legs that would open OPPOSITE sides of one underlying.

    A per-account `direction: invert` that is not applied to every account
    makes this happen on every signal, deterministically: the publisher
    says BUY, the inverted account sells, the rest buy, and the group is
    hedged the moment both fill. Prop firms judge the resulting positions,
    not the intent — Apex bans opposing positions "across multiple
    accounts" on pain of account closure, and Topstep treats a hedge as
    unappealable "even if the overlap is brief or unintentional".

    Keyed by underlying root (micro and full-size fold together) so a long
    MNQ against a short NQ counts, which is how those firms read it.
    """
    by_root: dict[str, dict[str, list[str]]] = {}
    for p in plans:
        # REVERSEPOSITION is included: it OPENS a position, and its action
        # is flipped for inverted accounts, so it can hedge the group the
        # same way a PLACE can. apply_hedge_guard only ever drops PLACE
        # legs, so a reversal conflict warns without stranding a position.
        if p["command"] not in ("PLACE", "REVERSEPOSITION"):
            continue
        action = (p["action"] or "").strip().upper()
        if action not in ("BUY", "SELL"):
            continue
        root = _underlying_root(p["instrument"])
        if root:
            by_root.setdefault(root, {}).setdefault(action, []).append(p["account"])
    return {root: sides for root, sides in by_root.items() if len(sides) > 1}


def hedge_guard_mode() -> str:
    """warn (default) | block | off — how to treat a manufactured hedge.

    Defaults to `warn`, not `block`, because inverting one account against
    the others is a supported strategy (fading the leader) on ordinary
    broker accounts, and silently refusing to send orders someone has
    configured is the wrong default. When a PROP account is part of the
    conflict, apply_hedge_guard escalates to `block` regardless of this
    setting — for those accounts an opposite position across accounts is
    an account-closure event, not a strategy.
    """
    mode = str(load_config().get("hedge_guard", "warn")).strip().lower()
    return mode if mode in ("block", "warn", "off") else "warn"


def apply_hedge_guard(plans: list[dict], skipped: list[tuple[str, str]]
                      ) -> list[dict]:
    """Refuse an entry fan-out that would hedge the group against itself.

    Entries only. Exits and reversals are never blocked — stranding a live
    position is its own hazard, and the exit-priority rule that governs the
    rest of the app applies here too.
    """
    conflicts = _entry_direction_conflict(plans)
    if not conflicts:
        return plans
    detail = "; ".join(
        f"{root}: " + " vs ".join(f"{side} {','.join(accts)}"
                                  for side, accts in sorted(sides.items()))
        for root, sides in sorted(conflicts.items()))
    mode = hedge_guard_mode()
    # A prop account on either side of the conflict overrides warn/off:
    # the firms judge the resulting positions, and both sides of the
    # hedge count against whoever holds the prop account.
    prop_involved = sorted({
        a for sides in conflicts.values() for accts in sides.values()
        for a in accts if is_prop_account(a)})
    if prop_involved and mode != "block":
        logger.warning(
            f"HEDGE GUARD  '{mode}' escalated to block — prop account(s) "
            f"in conflict: {', '.join(prop_involved)}")
        mode = "block"
    logger.warning(f"HEDGE GUARD ({mode})  entry legs would open opposite sides — {detail}")
    if mode == "off":
        return plans
    if mode == "warn":
        _dash_set_alert(
            Fore.YELLOW + f"  ⚠  HEDGE WARNING — opposite entries: {detail}"
            + Style.RESET_ALL, sticky=True)
        return plans
    _dash_set_alert(
        Fore.RED + f"  ⛔  HEDGE BLOCKED — {detail}. "
        + (f"Prop account(s) {', '.join(prop_involved)} may not hedge."
           if prop_involved else "Check per-account 'direction: invert'.")
        + Style.RESET_ALL, sticky=True)
    for p in plans:
        if p["command"] == "PLACE":
            skipped.append((p["account"], "hedge guard: opposite entries across accounts"))
            if p.get("rr_pick"):
                _rr_return(p["account"])
        elif p["command"] == "REVERSEPOSITION":
            # Exit priority forbids dropping a reversal — the old position
            # must still exit — so only its OPENING half is stripped: the
            # leg is rewritten as a CLOSEPOSITION. Without this, blocked
            # reversal legs fired in full while the alert claimed the
            # hedge was prevented.
            base = _with_account(
                f"CLOSEPOSITION;;{p['instrument']};;;;;;;;;;", p["account"])
            files = [base]
            twin = _apply_size(
                p["instrument"],
                "full" if p["instrument"] != to_full_instrument(p["instrument"])
                else "micros")
            if twin and twin != p["instrument"]:
                alt = base.split(";")
                alt[2] = twin
                files.append(";".join(alt))
            note = "downgraded to CLOSEPOSITION (hedge blocked)"
            p.update(signal=base, files=files, command="CLOSEPOSITION",
                     action="", qty=0, deferred=False, prop_group=False,
                     note=f"{p['note']}; {note}" if p["note"] else note)
            logger.info(f"HEDGE GUARD  reversal downgraded to close  account={p['account']}")
    return [p for p in plans if p["command"] != "PLACE"]


def _note_contract(signal_text: str):
    """Track a leg's (possibly account-specific) instrument for the hard-stop
    close safety net."""
    parts = signal_text.split(";")
    if len(parts) >= 3 and parts[2] and len(session_contracts) < MAX_SESSION_CONTRACTS:
        session_contracts.add(parts[2])


def _leg_blocked(account: str, manual: bool = False) -> str | None:
    """Why a pending leg must abort right now, or None to proceed.

    `manual` marks a leg the trader typed themselves: pause only mutes the
    publisher, never the trader (see the manual-trading section), so a
    manual leg ignores `paused` — every other gate still applies. Without
    this, routing prop entries through the deferred rail would silently
    swallow a deliberate manual order during pause while the ticket
    reported success.
    """
    if shutdown.is_set():
        return "shutdown"
    if hard_stopped:
        return "session hard stop"
    if paused and not manual:
        return "signals paused"
    if account in account_stops:
        return "account stop/target hit"
    if is_prop_account(account) and _prop_entry_blocked_now(account):
        return "prop entry cutoff (flat-by-close window)"
    return None


async def _interruptible_sleep(seconds: float, account: str,
                               manual: bool = False) -> bool:
    """Sleep in small steps, bailing early (False) if the leg gets blocked."""
    end = time.monotonic() + max(0.0, seconds)
    while True:
        if _leg_blocked(account, manual):
            return False
        remaining = end - time.monotonic()
        if remaining <= 0:
            return True
        await asyncio.sleep(min(0.25, remaining))


# ---------- Prop close-before-open engine ----------
# Every prop entry runs: snapshot NT → close what conflicts → CONFIRM the
# closes against NT → only then write the entry. All of it happens under
# ONE lock so two signals seconds apart cannot interleave (signal A's
# confirmed-flat check racing signal B's entry write is how a "one position
# at a time" account ends up holding two).

_prop_entry_lock: asyncio.Lock | None = None
_prop_lock_loop: asyncio.AbstractEventLoop | None = None


def _get_prop_lock() -> asyncio.Lock:
    """The one lock all prop entry work serializes under.

    Created lazily per running loop: asyncio.Lock binds to the loop that
    first awaits it, and the test suite runs each test in its own
    asyncio.run() loop.
    """
    global _prop_entry_lock, _prop_lock_loop
    loop = asyncio.get_running_loop()
    if _prop_entry_lock is None or _prop_lock_loop is not loop:
        _prop_entry_lock = asyncio.Lock()
        _prop_lock_loop = loop
    return _prop_entry_lock


# ---------- In-flight opens ----------
# An entry or reversal we WROTE but NinjaTrader has not yet shown as a
# position is invisible to a positions-only dump — a limit entry can work
# for minutes, and even a market fill lands a beat after the file write.
# A later signal's close-before-open would read that account as clear and
# stack a second market onto it. Every close-before-open account's opening
# write is therefore registered here and merged into the preemption dump
# as a pseudo-position until it ages out or a flatten kills it (flattening
# cancels the account's working orders, so the unfilled order dies too).

INFLIGHT_OPEN_TTL_S = 120.0   # covers write→fill→dump visibility for market
                              # orders with margin; a LIMIT entry older than
                              # this goes back to being invisible — documented
_inflight_opens: dict[tuple[str, str], dict] = {}   # (account, ROOT) -> row
_inflight_lock = threading.Lock()   # flatten paths run in worker threads


def _note_inflight_open(account: str, instrument: str, action: str,
                        qty: int = 1):
    """Record one opening write (PLACE / REVERSEPOSITION) as pending.

    A fresh same-direction row ACCUMULATES — multi-strategy stacking can
    write two entries on one root seconds apart, and the second signal's
    cap math must see both. An opposite-direction or stale row is
    replaced: the position it described is being flipped or is long past
    the write→fill window."""
    root = _alias_root(instrument).upper()
    if not account or not root:
        return
    try:
        signed = max(1, int(qty)) * (1 if (action or "").strip().upper() == "BUY" else -1)
    except (TypeError, ValueError):
        signed = 1 if (action or "").strip().upper() == "BUY" else -1
    with _inflight_lock:
        row = _inflight_opens.get((account, root))
        if (row is not None
                and time.monotonic() - row["ts"] <= INFLIGHT_OPEN_TTL_S
                and (row["qty"] > 0) == (signed > 0)):
            signed += row["qty"]
        _inflight_opens[(account, root)] = {
            "instrument": instrument, "qty": signed, "ts": time.monotonic()}


def _clear_inflight_opens(account: str, instrument: str | None = None):
    """Drop pending opens after a flatten — whole account, or one market."""
    with _inflight_lock:
        for key in list(_inflight_opens):
            if key[0] != account:
                continue
            if instrument is None or key[1] == _alias_root(instrument).upper():
                del _inflight_opens[key]


def _inflight_open_rows() -> list[tuple[str, str, int]]:
    """Fresh pending opens as (account, instrument, signed qty) rows,
    pruning entries past their TTL."""
    now = time.monotonic()
    rows: list[tuple[str, str, int]] = []
    with _inflight_lock:
        for key in list(_inflight_opens):
            row = _inflight_opens[key]
            if now - row["ts"] > INFLIGHT_OPEN_TTL_S:
                del _inflight_opens[key]
                continue
            rows.append((key[0], row["instrument"], row["qty"]))
    return rows


# ---------- Multi-strategy ledger ----------
# Which strategy opened what. In multi_strategy mode every entry WRITE is
# recorded here from the final per-account signal (real sized root, the
# per-account/per-tranche strategy id actually sent), so a publisher's
# stateless "CLOSEPOSITION NQ" from strategy X can be answered per
# account: scoped to X's own strategy ids, suppressed when X holds
# nothing here, or passed through verbatim the moment anything is
# unattributed. In-memory only — a restart empties it, which degrades to
# verbatim closes (fail-safe), never to a stranded position.

LEDGER_TTL_S = 18 * 3600      # one session; older rows are stale bookkeeping
_MAX_LEDGER_KEYS = 256
_MAX_LEDGER_ENTRIES = 12      # per (account, root) — publisher strategies are few
RESET_SETTLE_S = 1.5          # close→entry gap when an entry resets a market
                              # itself (the publisher's own pairs run 1–3s)
_strategy_ledger: dict[tuple[str, str], list[dict]] = {}  # (account, ROOT) -> rows
_ledger_lock = threading.Lock()   # entry writes run on the loop, flattens in threads


def _ledger_key(account: str, instrument: str) -> tuple[str, str]:
    return (account, _alias_root(instrument).upper())


def _ledger_fresh_rows(key: tuple[str, str]) -> list[dict]:
    """Rows under key still inside the TTL. Caller holds _ledger_lock."""
    now = time.monotonic()
    return [r for r in _strategy_ledger.get(key, [])
            if now - r["ts"] <= LEDGER_TTL_S]


def _ledger_note_entry(account: str, instrument: str, action: str, qty,
                       ident: tuple[str, str], sid: str, reset: bool = False):
    """Record one opening write. `ident` is (envelope slug, field-11 name)
    — both are kept because publishers have attributed by either. `sid` is
    the strategy id as written (per-account ~suffix, per-tranche ~T
    suffix included), the handle a scoped CLOSESTRATEGY targets later.
    `reset=True` (REVERSEPOSITION / a full reset) replaces the market's
    book — the flip just closed everything else this root held."""
    key = _ledger_key(account, instrument)
    if not key[0] or not key[1]:
        return
    try:
        q = max(1, int(qty))
    except (TypeError, ValueError):
        q = 1
    row = {"sid": sid or "",
           "slug": (ident[0] or "").strip().casefold(),
           "name": (ident[1] or "").strip().casefold(),
           "qty": q,
           "side": 1 if (action or "").strip().upper() == "BUY" else -1,
           "ts": time.monotonic()}
    with _ledger_lock:
        rows = [] if reset else _ledger_fresh_rows(key)
        rows.append(row)
        _strategy_ledger[key] = rows[-_MAX_LEDGER_ENTRIES:]
        while len(_strategy_ledger) > _MAX_LEDGER_KEYS:
            _strategy_ledger.pop(next(iter(_strategy_ledger)))


def _ledger_entries(account: str, instrument: str) -> list[dict]:
    with _ledger_lock:
        return list(_ledger_fresh_rows(_ledger_key(account, instrument)))


def _ledger_clear(account: str, instrument: str | None = None):
    """Forget attribution after a full flatten — whole account or one root."""
    with _ledger_lock:
        for key in list(_strategy_ledger):
            if key[0] != account:
                continue
            if instrument is None or key[1] == _alias_root(instrument).upper():
                del _strategy_ledger[key]


def _ledger_prune_sids(account: str, sids: set[str],
                       instrument: str | None = None):
    """Drop specific entries once a scoped/targeted close covers them."""
    with _ledger_lock:
        for key in list(_strategy_ledger):
            if key[0] != account:
                continue
            if instrument is not None and key[1] != _alias_root(instrument).upper():
                continue
            rows = [r for r in _ledger_fresh_rows(key) if r["sid"] not in sids]
            if rows:
                _strategy_ledger[key] = rows
            else:
                del _strategy_ledger[key]


def _ident_matches(row: dict, ident: tuple[str, str]) -> bool:
    """True when a ledger row belongs to this strategy identity. Publishers
    attribute by envelope slug ("bread_n_butter") or by ATI field-11 name
    ("NQ-MacroZoneB", 2026-08-18 era) — either non-empty match counts."""
    given = {(s or "").strip().casefold() for s in ident} - {""}
    mine = {row.get("slug", ""), row.get("name", "")} - {""}
    return bool(given & mine)


def _ledger_note_file(plan: dict, sig: str):
    """Keep the ledger in step with one signal file the plan just wrote."""
    parts = sig.split(";")
    cmd = parts[0].strip().upper() if parts else ""
    account = plan.get("account", "")
    ident = plan.get("strat_ident") or ("", "")
    if cmd in ("PLACE", "REVERSEPOSITION") and len(parts) >= 13:
        _ledger_note_entry(account, parts[2], parts[3], parts[4], ident,
                           parts[12], reset=(cmd == "REVERSEPOSITION"))
        _pnl_note_open(account, parts[2], ident,
                       manual=bool(plan.get("manual")),
                       source=plan.get("manual_source") or "")
    elif cmd == "CLOSEPOSITION" and len(parts) >= 3 and parts[2]:
        _ledger_clear(account, parts[2])
        _pnl_note_close(account, parts[2])
    elif cmd == "CLOSESTRATEGY" and len(parts) >= 13 and parts[12]:
        _ledger_prune_sids(account, {parts[12]})


# ---------- Live-bridge book ----------
# The AddOn broadcasts a FULL state line — every account, every open
# position — on every account/position/price event plus a ~5s idle
# heartbeat (see addon/SocketTraderBridge.cs). live_bridge_task caches
# each line here, so when a signal lands the client already KNOWS whether
# any prop account holds anything: a fresh book that shows nothing to
# close lets the entry skip the pre-entry ATI snapshot and fire at once.
# The book is only ever trusted to prove that NEGATIVE. The moment it
# shows work to do — or cannot carry the weight (stale, disconnected, an
# account missing, an outage-shaped balance) — the authoritative ATI dump
# decides, and closes fire off THAT, never off the book.

BRIDGE_BOOK_FRESH_S = 10.0   # 2× the AddOn heartbeat: one late line survives
_bridge_book: dict | None = None   # {"ts": ..., "accounts": {name: {"cash", "positions"}}}


def _bridge_ingest_book(accts: list):
    """Cache one bridge line as the live book (fed by live_bridge_task).

    Every line is a complete dump, so the previous book is replaced
    wholesale — merging would resurrect closed positions. An account whose
    position rows don't parse is left OUT of the book entirely: the book's
    only job is proving "nothing to close", and a position we failed to
    read is exactly the one that must not vanish into an all-flat read.
    """
    global _bridge_book
    book: dict[str, dict] = {}
    for a in accts if isinstance(accts, list) else []:
        if not isinstance(a, dict):
            continue
        name = str(a.get("name") or "")
        if not name:
            continue
        positions: list[tuple[str, int]] = []
        # A missing/non-list positions field is UNKNOWN, not flat — the
        # account is dropped so the fast path cannot prove it clear.
        raw_pos = a.get("positions")
        rows_ok = isinstance(raw_pos, list)
        for p in raw_pos if rows_ok else []:
            inst = str(p.get("inst") or "").strip() if isinstance(p, dict) else ""
            try:
                qty = int(p.get("qty"))
            except (AttributeError, TypeError, ValueError):
                qty = 0
            if not inst or not qty:
                rows_ok = False
                break
            positions.append((inst, qty))
        if not rows_ok:
            continue
        try:
            cash = float(a["cash"])
        except (KeyError, TypeError, ValueError):
            cash = None
        book[name] = {"cash": cash, "positions": positions}
    _bridge_book = {"ts": time.time(), "accounts": book}


def _bridge_book_snapshot(plans: list[dict] | None = None) -> dict | None:
    """The live-bridge book as a preemption snapshot, or None when it
    cannot be trusted to prove an account flat.

    Trust requires ALL of: the bridge enabled, authenticated and
    streaming; the book younger than BRIDGE_BOOK_FRESH_S; and every
    account the preemption would consult — each managed prop account,
    plus every ENTERING account in `plans` (a `close_before_open` opt-in
    need not be prop) — present in the dump with a believable balance.
    A ~$0.00 cash against a known nonzero balance is NT answering while
    its broker feed is down (_suspect_zero_balance) — position rows from
    that state are exactly the ones that must not read as flat.
    """
    book = _bridge_book
    if not (live_bridge_enabled and _live_bridge_connected and book):
        return None
    age = time.time() - book["ts"]
    if age > BRIDGE_BOOK_FRESH_S:
        return None
    consulted: list[str] = [a for a in target_accounts() if is_prop_account(a)]
    consulted += [p["account"] for p in plans or []
                  if p["account"] not in consulted]
    rows: list[dict] = []
    for acct in consulted:
        entry = book["accounts"].get(acct)
        if entry is None:
            return None
        cash = entry["cash"]
        if cash is None or _suspect_zero_balance(acct, cash):
            return None
        rows.extend({"account": acct, "instrument": inst, "qty": qty,
                     "avg_price": None} for inst, qty in entry["positions"])
    return {"ok": True, "accounts": {}, "working": {}, "ts": book["ts"],
            "positions": rows, "age": age}


def _bridge_book_after(since: float) -> dict | None:
    """The live book as a POST-CLOSE witness for a flatten: the bridge
    enabled and streaming, the book fresh, and RECEIVED at least
    BRIDGE_VERIFY_MARGIN_S after `since` (the moment the closes went out),
    so the line was built by the AddOn after them. Both stamps are this
    machine's clock. None when it cannot be trusted for that."""
    book = _bridge_book
    if not (live_bridge_enabled and _live_bridge_connected and book):
        return None
    if book["ts"] < since + BRIDGE_VERIFY_MARGIN_S:
        return None
    if time.time() - book["ts"] > BRIDGE_BOOK_FRESH_S:
        return None
    return book


def _book_entry_trusted(book: dict, account: str) -> dict | None:
    """One account's book entry, or None when the book cannot vouch for it
    — absent from the dump, or a ~$0.00 balance against a known nonzero
    one (NT answering while its broker feed is down)."""
    entry = book["accounts"].get(account)
    if entry is None:
        return None
    cash = entry["cash"]
    if cash is None or _suspect_zero_balance(account, cash):
        return None
    return entry


def _bridge_book_shows_flat(accounts: list[str], since: float) -> bool:
    """True only when a trusted post-close book line shows every one of
    `accounts` holding nothing."""
    book = _bridge_book_after(since)
    if book is None:
        return False
    for acct in accounts:
        entry = _book_entry_trusted(book, acct)
        if entry is None or entry["positions"]:
            return False
    return True


def _bridge_book_shows_cleared(targets: set[tuple[str, str]], since: float) -> bool:
    """True only when a trusted post-close book line shows none of the
    (account, ROOT) targets still held — a KEPT same-market position on
    another root is not in the way."""
    book = _bridge_book_after(since)
    if book is None:
        return False
    for acct, root in targets:
        entry = _book_entry_trusted(book, acct)
        if entry is None:
            return False
        if any(_alias_root(inst).upper() == root for inst, _qty in entry["positions"]):
            return False
    return True


def _account_root_exposure(account: str, instrument: str) -> dict | None:
    """What one account holds on an instrument's root, per the live book.

    Returns {"shown": signed qty the bridge book shows for this exact
    root, "inflight": signed qty of fresh opening writes on it, "twin":
    signed micro/full-twin qty shown} — or None when the book cannot
    vouch for the account (bridge off/stale, account missing, or an
    outage-shaped ~$0 balance). Smart-close and stack decisions REQUIRE
    the book: without it an account's real state is unknowable in time,
    and every caller must fall back to verbatim close-then-place.

    For "is anything held" use shown, falling back to inflight only when
    the book shows nothing — a shown position IS the write, matured.
    For sizing a stack against a cap take max(|shown|, |inflight|):
    over-counting just after a fill only delays a stack; under-counting
    could breach a firm's contract cap.
    """
    book = _bridge_book
    if not (live_bridge_enabled and _live_bridge_connected and book):
        return None
    if time.time() - book["ts"] > BRIDGE_BOOK_FRESH_S:
        return None
    entry = book["accounts"].get(account)
    if entry is None:
        return None
    cash = entry["cash"]
    if cash is None or _suspect_zero_balance(account, cash):
        return None
    root = _alias_root(instrument).upper()
    under = _underlying_root(instrument)
    shown = twin = 0
    for inst, qty in entry["positions"]:
        if _alias_root(inst).upper() == root:
            shown += qty
        elif _underlying_root(inst) == under:
            twin += qty
    inflight = 0
    for acct, inst, qty in _inflight_open_rows():
        if acct == account and _alias_root(inst).upper() == root:
            inflight += qty
    return {"shown": shown, "inflight": inflight, "twin": twin}


def _smart_close_decision(account: str, instrument: str,
                          ident: tuple[str, str]) -> tuple[str, list[str], str]:
    """How one account honors a publisher CLOSEPOSITION in multi-strategy
    mode. Returns (verdict, files, why):
      "pass"  → send the close verbatim (files unused)
      "drop"  → nothing to do for this account
      "scope" → send `files` (CLOSESTRATEGY on the sender's OWN entry ids)
                instead — its contracts close and its ATM bracket dies,
                the other strategies' positions stand.

    Doctrine: the close fires verbatim unless the account's state is
    PROVEN — a fresh bridge book agreeing with the write ledger — to
    belong to identified strategies, and even then the sender still
    closes everything it owns. Restarts, bracket exits beyond the ledger,
    manual positions, twins and direction drift all land on "pass":
    trampling a teammate strategy costs churn, but suppressing a close
    that should have fired strands a live position.
    """
    exp = _account_root_exposure(account, instrument)
    if exp is None:
        return "pass", [], "no live book to prove state"
    if exp["twin"]:
        return "pass", [], "micro/full twin position held"
    held = exp["shown"] if exp["shown"] else exp["inflight"]
    rows = _ledger_entries(account, instrument)
    if not rows:
        if held:
            return "pass", [], "position not attributed to any strategy"
        return "drop", [], "flat — nothing to close"
    if not held:
        # Ledger rows with nothing held or in flight: ATM brackets took
        # everything out since. The close is a no-op; the rows are stale.
        _ledger_clear(account, instrument)
        return "drop", [], "flat — nothing to close"
    total = sum(r["qty"] * r["side"] for r in rows)
    if total == 0 or (held > 0) != (total > 0):
        return "pass", [], "held direction contradicts the ledger"
    if abs(held) > abs(total):
        return "pass", [], "more contracts held than attributed"
    own = [r for r in rows if _ident_matches(r, ident)]
    others = [r for r in rows if not _ident_matches(r, ident)]
    if not own:
        names = sorted({r["slug"] or r["name"] or "?" for r in others})
        return "drop", [], f"position belongs to {', '.join(names)}"
    if not others:
        return "pass", [], "sole holder — full close"
    if any(not r["sid"] for r in own):
        return "pass", [], "own entry carries no strategy id to scope by"
    files = [f"CLOSESTRATEGY;{sanitize_ati(account)};;;;;;;;;;;{r['sid']}"
             for r in own]
    _ledger_prune_sids(account, {r["sid"] for r in own}, instrument)
    n = len(own)
    return "scope", files, f"scoped to {n} own entr{'y' if n == 1 else 'ies'}"


def _reset_close_files(account: str, instrument: str) -> list[str]:
    """Account-addressed CLOSEPOSITION files for a root — both contract
    sizes, mirroring the twin fan-out every planned close performs."""
    base = (f"CLOSEPOSITION;{sanitize_ati(account)};"
            f"{sanitize_ati(instrument)};;;;;;;;;;")
    files = [base]
    twin = _apply_size(instrument,
                       "full" if instrument != to_full_instrument(instrument)
                       else "micros")
    if twin and twin != instrument:
        alt = base.split(";")
        alt[2] = sanitize_ati(twin)
        files.append(";".join(alt))
    return files


def _prop_preempt_closures(plans: list[dict], snap: dict,
                           cross_account: bool = True,
                           exclude: set[str] = frozenset(),
                           entry_same_root: str = "close"
                           ) -> tuple[dict[str, list[str]], dict[str, bool]]:
    """What must CLOSE before these close-before-open entry legs may fire.

    Returns (to_close, keeps): to_close maps account -> position aliases to
    flatten; keeps flags accounts that RETAIN a position, so the flatten
    for them must stay scoped to the listed contracts (a whole-account
    flatten would also cancel the kept position's ATM bracket).

    Per held position of a close-before-open account (prop, or the
    `close_before_open` opt-in) that is ENTERING:
      - entry_same_root="close" (the ENTRY path): CLOSE EVERYTHING the
        account holds, the entry's own market included — an entry RESETS
        the account to the new signal, the close-then-place the publisher's
        server used to provide. Keeping the same market instead would let
        an opposite entry NET the position while both ATM brackets stay
        working (CLOSEPOSITION cancels an instrument's orders; a netting
        fill cancels nothing), and let a same-direction re-entry stack
        contracts past a firm's cap.
        ONE exception, multi_strategy mode: a held SAME-root SAME-direction
        position is KEPT when the account's `max_contracts` cap is set and
        the stack fits under it — several publisher strategies sharing a
        market each keep their own position instead of trampling each
        other's. The netting hazard cannot arise (same direction), the cap
        hazard is checked against the cap, and the micro/full twin still
        closes (different alias root — MFFU's named cap-evasion pattern).
        No cap configured, over cap, or opposite side → reset as ever.
      - entry_same_root="keep" (the REVERSAL-cleanup path): the entry's own
        root is KEPT — the reversal is flipping that position mid-flight,
        and closing it would flatten what should flip. Micro/full twins and
        every other market still close (a twin is the classic intra-account
        hedge one way and MFFU's named cap-evasion pattern the other).
    Per held position of a managed PROP account that is NOT entering
    (cross_account=True): CLOSE only when it sits on the OPPOSITE side of
    the entry's product group — opposing correlated positions across
    accounts is the ban every firm agrees on (Tradeify scopes it to whole
    product groups, so this matches groups, not bare symbols). Non-prop
    accounts are never swept cross-account: that ban is a prop-firm rule.
    """
    entering = {p["account"]: p for p in plans}
    pos_rows: dict[str, list[tuple[str, int]]] = {}
    for row in snap.get("positions", []):
        pos_rows.setdefault(row["account"], []).append((row["instrument"], row["qty"]))
    # Merge opening writes NT has not shown as positions yet: a just-written
    # entry or reversal must count as held, or a burst of signals seconds
    # apart stacks a second market onto the account (see In-flight opens).
    for acct, inst, qty in _inflight_open_rows():
        root = _alias_root(inst).upper()
        if any(_alias_root(i).upper() == root for i, _ in pos_rows.get(acct, [])):
            continue    # the real position already showed up — it wins
        pos_rows.setdefault(acct, []).append((inst, qty))
    group_sides = {(_product_group(p["instrument"]), (p["action"] or "").strip().upper())
                   for p in plans
                   if (p["action"] or "").strip().upper() in ("BUY", "SELL")}

    def opposite(qty: int, action: str) -> bool:
        return (qty > 0 and action == "SELL") or (qty < 0 and action == "BUY")

    # Multi-strategy stacking: which ENTERING accounts may keep a held
    # same-root same-direction position instead of resetting it. Requires
    # the mode on, a configured max_contracts cap, and the stack fitting
    # under it — sized against max(merged rows, raw in-flight writes), so
    # a burst where one entry is already shown and the next is still
    # in flight can't slip past the cap.
    stack_ok: dict[str, bool] = {}
    if entry_same_root == "close" and multi_strategy_enabled():
        inflight_rows = _inflight_open_rows()
        for acct, plan in entering.items():
            action = (plan.get("action") or "").strip().upper()
            try:
                cap = int((plan.get("rule") or {}).get("max_contracts") or 0)
                want_qty = max(0, int(plan.get("qty") or 0))
            except (TypeError, ValueError):
                continue
            if cap <= 0 or want_qty <= 0 or action not in ("BUY", "SELL"):
                continue
            root = _alias_root(plan.get("instrument") or "").upper()
            held = sum(q for i, q in pos_rows.get(acct, [])
                       if _alias_root(i).upper() == root)
            if held == 0 or opposite(held, action):
                continue
            inflight = sum(q for a2, i2, q in inflight_rows
                           if a2 == acct and _alias_root(i2).upper() == root)
            if max(abs(held), abs(inflight)) + want_qty <= cap:
                stack_ok[acct] = True

    to_close: dict[str, list[str]] = {}
    keeps: dict[str, bool] = {}
    for acct in target_accounts():
        plan = entering.get(acct)
        if plan is not None:
            if not closes_before_open(acct):
                continue    # plain accounts are never preemptively closed
        elif not is_prop_account(acct) or not cross_account or acct in exclude:
            # `exclude` names accounts with their OWN in-flight leg of the
            # same action — closing their position out from under a
            # reversal that is mid-fill would flatten what should flip.
            continue
        for alias, qty in pos_rows.get(acct, []):
            if plan is not None:
                # _alias_root on BOTH sides: the plan instrument is usually
                # the publisher's "ROOT MM-YY", but the web Rev path passes
                # whatever alias NT broadcast ("NQU26", "@NQ"), and a
                # mismatch here would flatten the very position the user
                # just reversed into.
                same_root = (_alias_root(alias).upper()
                             == _alias_root(plan["instrument"]).upper())
                if entry_same_root == "keep" and same_root:
                    keeps[acct] = True
                elif (same_root and stack_ok.get(acct)
                        and not opposite(qty, (plan.get("action") or "").strip().upper())):
                    # Multi-strategy stack: same market, same side, cap
                    # verified above — the entry ADDS to this position.
                    # The per-row side re-check keeps a stray opposite row
                    # (an old-month remnant sharing the root) closable.
                    keeps[acct] = True
                    logger.info(
                        f"STACK  account={acct}  keeping {alias} ({qty:+d}) — "
                        f"same-direction {plan.get('action')} entry adds to it")
                else:
                    to_close.setdefault(acct, []).append(alias)
            else:
                if any(_product_group(alias) == group and opposite(qty, action)
                       for group, action in group_sides):
                    to_close.setdefault(acct, []).append(alias)
                else:
                    keeps[acct] = True
    return to_close, keeps


def _prop_flatten_wave(to_close: dict[str, list[str]], keeps: dict[str, bool]):
    """Fire the closes for a prop preemption (blocking — call via to_thread).

    Leader-first, sequential — same load-bearing order as
    close_all_open_positions. An account keeping nothing is flattened
    whole (close_account_positions also cancels its working orders, so a
    resting entry can't refill after we close); an account that keeps a
    position gets targeted CLOSEPOSITIONs so the kept position's ATM
    bracket survives.
    """
    for acct in target_accounts():
        contracts = to_close.get(acct)
        if not contracts:
            continue
        if keeps.get(acct):
            for c in contracts:
                fire_close_position(acct, c)
                _clear_inflight_opens(acct, c)
            logger.info(f"PROP PREEMPT  account={acct}  targeted close={contracts}")
        else:
            closed = close_account_positions(acct)
            logger.info(f"PROP PREEMPT  account={acct}  flattened={closed}")


async def _prop_snapshot() -> dict | None:
    """One complete NT state dump requested now, or None when NT can't
    prove its state. "Now" is load-bearing: the guard must see a fill that
    landed a second ago, so a cached read is never enough here."""
    for _ in range(2):
        try:
            snap = await _snapshot_requested_after(time.time(), VERIFY_SNAPSHOT_WAIT)
        except Exception as exc:
            logger.error(f"PROP GUARD  snapshot failed: {exc}")
            continue
        if snap is not None and snap.get("ok"):
            return snap
        logger.warning("PROP GUARD  incomplete NT dump — retrying")
    return None


async def _prop_verify_cleared(to_close: dict[str, list[str]]) -> bool:
    """Confirm every preempted position is actually gone. Proof, not hope —
    same doctrine as verify_flat: an unverifiable state is NOT clear.

    Deliberately separate from verify_flat: this confirms specific
    (account, root) targets so a KEPT same-market position doesn't read as
    "still open", where verify_flat proves whole accounts flat. A change
    to either loop's dump-failure or retry semantics must be mirrored in
    the other."""
    targets = {(acct, _alias_root(c).upper())
               for acct, contracts in to_close.items() for c in contracts}
    gone, _remaining, _confirmed = await _watch_closed(
        lambda positions: [p for p in positions
                           if (p["account"], _alias_root(p["instrument"]).upper()) in targets],
        lambda since: _bridge_book_shows_cleared(targets, since),
        PROP_VERIFY_TRIES, (PROP_VERIFY_TRIES + 1) * FLATTEN_VERIFY_DELAY,
        "PROP VERIFY")
    return gone


def _prop_withhold(plans: list[dict], why: str):
    """Refuse prop entries loudly: log, sticky alert, round-robin refund."""
    accounts = ", ".join(p["account"] for p in plans)
    logger.error(f"PROP GUARD  entry withheld  accounts={accounts}  reason={why}")
    _dash_set_alert(
        Fore.RED + Style.BRIGHT +
        f"  ⛔  PROP GUARD — entry withheld for {accounts}: {why}. "
        "Check NinjaTrader before trading on." + Style.RESET_ALL, sticky=True)
    for p in plans:
        if p.get("rr_pick"):
            _rr_return(p["account"])


async def _prop_clear_for_entry(plans: list[dict]) -> tuple[bool, bool]:
    """Close-before-open for prop entry legs.

    Returns (clear_to_enter, closed_anything). Callers hold
    _get_prop_lock(), so nothing else can interleave between this
    confirmation and the entry writes that follow it — and they use
    closed_anything to alarm loudly if the entry then never fires, because
    a position destroyed for an entry that was withheld must not vanish
    into an info-level log line.
    """
    # Fast path: with the live bridge streaming, the book is already in
    # hand when the signal lands. A fresh book that shows nothing to close
    # clears the entry without the pre-entry ATI round-trip. It only ever
    # proves that negative — anything to close, or an untrustworthy book,
    # falls through to the authoritative dump, and closes fire off that.
    book = _bridge_book_snapshot(plans)
    if book is not None:
        book_close, _ = _prop_preempt_closures(plans, book)
        if not book_close:
            logger.info(
                f"PROP FAST PATH  bridge book ({book['age']:.1f}s old) shows "
                f"nothing to close — entering  "
                f"accounts={[p['account'] for p in plans]}")
            return True, False
        logger.info("PROP FAST PATH  bridge book shows positions to clear — "
                    "confirming against a fresh ATI dump")
    snap = await _prop_snapshot()
    if snap is None:
        _prop_withhold(plans, "NinjaTrader would not prove its position state")
        return False, False
    to_close, keeps = _prop_preempt_closures(plans, snap)
    if not to_close:
        return True, False
    detail = "; ".join(f"{a}: {', '.join(c)}" for a, c in sorted(to_close.items()))
    logger.info(f"PROP PREEMPT  closing before entry — {detail}")
    _dash_set_alert(
        Fore.CYAN + f"  ⏳  PROP one-trade rule — closing {detail} before entry"
        + Style.RESET_ALL)
    await asyncio.to_thread(_prop_flatten_wave, to_close, keeps)
    if await _prop_verify_cleared(to_close):
        return True, True
    logger.warning(f"PROP PREEMPT  close unconfirmed — retrying  {detail}")
    await asyncio.to_thread(_prop_flatten_wave, to_close, keeps)
    if await _prop_verify_cleared(to_close):
        return True, True
    _prop_withhold(plans, f"could not confirm close of {detail}")
    return False, True


def _prop_alert_closed_without_entry(accounts: str, why: str):
    """Positions were preempted but no entry followed — say so, loudly."""
    logger.error(f"PROP GUARD  closed without entry  accounts={accounts}  reason={why}")
    _dash_set_alert(
        Fore.RED + Style.BRIGHT +
        f"  ⛔  PROP — positions were closed for an entry that was then "
        f"withheld ({why}) on {accounts}. The account(s) are FLAT with no "
        "entry placed." + Style.RESET_ALL, sticky=True)


async def _run_prop_group(plans: list[dict], sig_id: str | None = None):
    """One signal's simple prop entry legs as a single close-confirm-enter
    wave: one snapshot, one flatten pass, one confirmation, then
    leader-first writes — so five prop accounts land together instead of
    serializing five confirmations."""
    try:
        async with _get_prop_lock():
            live = []
            for p in plans:
                reason = _leg_blocked(p["account"], p.get("manual", False))
                if reason:
                    logger.info(f"LEG ABORTED  account={p['account']}  reason={reason}")
                    if p.get("rr_pick"):
                        _rr_return(p["account"])
                else:
                    live.append(p)
            if not live:
                return
            ok, closed_any = await _prop_clear_for_entry(live)
            if not ok:
                return
            order = {a: i for i, a in enumerate(target_accounts())}
            placed: list[str] = []
            last_reason = ""
            for plan in sorted(live, key=lambda p: order.get(p["account"], len(order))):
                reason = _leg_blocked(plan["account"], plan.get("manual", False))
                if reason:
                    last_reason = reason
                    logger.info(f"LEG ABORTED  account={plan['account']}  reason={reason}")
                    if plan.get("rr_pick"):
                        _rr_return(plan["account"])
                    continue
                if await _write_entry_tranches(plan, [plan["qty"]], sig_id, quiet=True):
                    placed.append(plan["account"])
            not_placed = [p["account"] for p in live
                          if p["account"] not in placed]
            if closed_any and not_placed:
                # The confirmation window can cross the entry cutoff, or a
                # write can fail — either way positions died for an entry
                # that never happened, which must not be silent. This wins
                # the alert slot even when OTHER accounts placed: a green
                # banner over a destroyed position is the worst outcome.
                _prop_alert_closed_without_entry(
                    ", ".join(not_placed), last_reason or "entry write failed")
                if placed:
                    logger.info(f"PROP GROUP  placed={placed} while withheld={not_placed}")
            elif placed:
                _dash_set_alert(
                    Fore.GREEN + f"  ✔  PROP entry placed → {', '.join(placed)}"
                    + Style.RESET_ALL)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error(f"prop group error  accounts={[p['account'] for p in plans]}  error={exc}")


async def _prop_reversal_cleanup(plans: list[dict], cross_account: bool = True,
                                 exclude: set[str] = frozenset()):
    """After REVERSEPOSITION legs fire on prop accounts, close every OTHER
    market they still hold, plus any other managed prop account left on
    the opposite side of the product group (the cross-account hedge the
    PLACE path sweeps — a reversal must not be the one opening path that
    leaves it standing). The reversals themselves are never delayed —
    exit priority — so this trails them instead of gating them.

    `exclude` carries the accounts whose own reversal leg of this same
    signal is in flight: their position is about to flip on its own, and
    sweeping it mid-fill would flatten what should reverse. Accounts whose
    leg was downgraded to a close, or who got no leg at all, are NOT
    excluded — they are exactly the ones a standing opposite position can
    survive on. One task covers the whole fan-out: one snapshot, one
    flatten wave, one confirmation."""
    try:
        async with _get_prop_lock():
            snap = await _prop_snapshot()
            if snap is None:
                accounts = ", ".join(p["account"] for p in plans)
                logger.warning(f"PROP REVERSAL CLEANUP  no NT state for {accounts}")
                return
            to_close, keeps = _prop_preempt_closures(plans, snap,
                                                     cross_account=cross_account,
                                                     exclude=exclude,
                                                     entry_same_root="keep")
            if not to_close:
                return
            detail = "; ".join(f"{a}: {', '.join(c)}" for a, c in sorted(to_close.items()))
            logger.info(f"PROP REVERSAL CLEANUP  {detail}")
            await asyncio.to_thread(_prop_flatten_wave, to_close, keeps)
            if not await _prop_verify_cleared(to_close):
                _dash_set_alert(
                    Fore.RED + Style.BRIGHT +
                    f"  ⛔  PROP — could not confirm close of {detail} after a "
                    "reversal. Close it in NinjaTrader now." + Style.RESET_ALL,
                    sticky=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error(f"prop reversal cleanup error  "
                     f"accounts={[p['account'] for p in plans]}  error={exc}")


_prop_autoflat_done: dict[str, str] = {}   # account -> ET date already flattened
_prop_flat_task: asyncio.Task | None = None  # in-flight flat-by-close sweep
PROP_FLAT_WINDOW_MIN = 30                  # fire within this window past flat time


async def _check_prop_flat_by_close(now_et: datetime):
    """Flatten prop accounts at their flat-by-close time.

    balance_monitor calls this every poll. Firms auto-liquidate at their
    own deadline and some (MFFU) treat a position held past it as an
    account breach — closing on our side first keeps the record clean.
    Runs once per account per trading day, inside a bounded window so a
    late app start doesn't flatten the NEXT session's positions.
    """
    if now_et.weekday() >= 5:
        return
    today = now_et.strftime("%Y-%m-%d")
    minutes_now = now_et.hour * 60 + now_et.minute
    for acct in target_accounts():
        if not is_prop_account(acct) or _prop_autoflat_done.get(acct) == today:
            continue
        fh, fm = prop_flat_time(acct)
        start = fh * 60 + fm
        if not (start <= minutes_now < start + PROP_FLAT_WINDOW_MIN):
            continue
        _prop_autoflat_done[acct] = today
        # No exemption for accounts in account_stops: their stop/target
        # flatten SHOULD have left them flat, but _trip_account's close can
        # fail unconfirmed and never be looked at again — the close window
        # is the last chance to catch that before the firm's deadline. A
        # genuinely flat stopped account costs one quiet verification.
        # A flat account gets a quiet log line, not a "flattened" alert —
        # close_account_positions returns session-contract safety-net names
        # even when nothing was open, so its return value can't be the
        # evidence. A quick position query decides; when it reads empty,
        # verify_flat still has to PROVE it (an empty read can also mean
        # the query failed, and NT being unreachable at the close is
        # exactly when a stuck position must raise an alarm, not silence).
        # Per-account try: this runs as an unawaited background task, and
        # one account's error must neither kill the other accounts' sweeps
        # nor vanish unraised.
        try:
            try:
                pre = await asyncio.to_thread(_positions_now, acct, BALANCE_POLL_INTERVAL)
            except Exception:
                pre = {}
            if not pre and await verify_flat([acct]) == []:
                logger.info(f"PROP FLAT-BY-CLOSE  account={acct}  already flat")
                continue
            closed = await asyncio.to_thread(close_account_positions, acct)
            still = await verify_flat([acct])
            if still:
                await asyncio.to_thread(close_account_positions, acct)   # one retry
                still = await verify_flat([acct])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"PROP FLAT-BY-CLOSE  account={acct}  error={exc}")
            still = [{"account": acct, "instrument": "UNVERIFIED",
                      "qty": 0, "avg_price": None}]
            pre, closed = {}, []
        if still:
            detail = ", ".join(
                p["instrument"] if p["instrument"] == "UNVERIFIED"
                else f"{p['qty']:+d} {p['instrument']}" for p in still)
            logger.error(f"PROP FLAT-BY-CLOSE FAILED  account={acct}  still open: {detail}")
            _dash_set_alert(
                Fore.RED + Style.BRIGHT +
                f"  ⛔  {acct} NOT FLAT AT THE CLOSE ({detail}) — close it in "
                "NinjaTrader NOW before the firm's deadline." + Style.RESET_ALL,
                sticky=True)
        else:
            had = sorted(pre) or closed
            logger.info(f"PROP FLAT-BY-CLOSE  account={acct}  closed={had}")
            _dash_set_alert(
                Fore.CYAN + f"  🕔  PROP flat-by-close — {acct} flattened "
                f"({', '.join(had) or 'confirmed'})" + Style.RESET_ALL)


async def execute_plans(plans: list[dict], sig_id: str | None = None) -> list[str]:
    """Write instant legs now; launch deferred legs (delay/AI/stagger) and
    prop close-before-open waves as background tasks. Returns the accounts
    whose files were written NOW — deferred accounts report via alerts/log
    when they land."""
    written: list[str] = []
    prop_reversals: list[dict] = []
    group = [p for p in plans if p.get("prop_group")]
    if group:
        task = asyncio.create_task(_run_prop_group(group, sig_id))
        _leg_tasks.add(task)
        task.add_done_callback(_leg_tasks.discard)
    for plan in plans:
        if plan.get("prop_group"):
            continue     # riding the close-confirm-enter wave above
        if plan["deferred"]:
            task = asyncio.create_task(_run_deferred_leg(plan, sig_id))
            _leg_tasks.add(task)
            task.add_done_callback(_leg_tasks.discard)
            continue
        account = plan["account"]
        wrote_any = False
        for sig in plan["files"]:
            try:
                res = write_signal_to_file(sig)
            except Exception as exc:
                res = None
                logger.error(f"DISPATCH FAIL  account={account}  error={exc!r}")
            if res:
                wrote_any = True
                _note_contract(sig)
                _ledger_note_file(plan, sig)
            else:
                logger.error(f"DISPATCH FAIL  account={account}  result=None")
                _dash_set_alert(
                    Fore.RED + f"  ✖  Copy-trade write failed for {account}" + Style.RESET_ALL)
        if wrote_any:
            written.append(account)
            if plan["command"] == "PLACE":
                _record_stagger(plan["signal"], account, 1)
                # Until NT shows the fill, the write itself is the only
                # record — a same-root signal seconds later needs it for
                # exposure/stack math on EVERY account type.
                _note_inflight_open(account, plan["instrument"], plan["action"],
                                    plan.get("qty") or 1)
            elif plan["command"] == "REVERSEPOSITION":
                # A reversal OPENS the other side; until NT shows the
                # flipped position it must still count as held.
                _note_inflight_open(account, plan["instrument"], plan["action"],
                                    plan.get("qty") or 1)
                if plan.get("prop"):
                    prop_reversals.append(plan)
    if written:
        # The dashboard's next read should show the fill as soon as
        # NinjaTrader does — start the stream's next dump now.
        _snapshot_nudge()
    if prop_reversals:
        # The reversals were never delayed (exit priority); now one task
        # sweeps every other market the reversing prop accounts hold AND
        # any other prop account left opposite the new side — excluding
        # only accounts whose own reversal leg is mid-flight.
        reversing = {p["account"] for p in plans
                     if p["command"] == "REVERSEPOSITION"}
        task = asyncio.create_task(_prop_reversal_cleanup(
            prop_reversals, cross_account=True, exclude=reversing))
        _leg_tasks.add(task)
        task.add_done_callback(_leg_tasks.discard)
    return written


async def _run_deferred_leg(plan: dict, sig_id: str | None = None):
    """Background execution of one account's entry: delay → AI gate →
    staggered tranche writes, re-checking stops/pause before every write."""
    account, rule = plan["account"], plan["rule"]
    label = f"[{account}] " if len(target_accounts()) > 1 else ""
    try:
        manual = plan.get("manual", False)
        if plan.get("reset_first"):
            # Opposite-direction entry over a standing teammate-strategy
            # position (its own paired close was scoped away): restore the
            # publisher's reset semantics — full close now, a settle beat
            # for NT to process it (the publisher's own close→place gap is
            # 1–3s), then the normal entry flow.
            for sig in _reset_close_files(account, plan["instrument"]):
                write_signal_to_file(sig)
            _ledger_clear(account, plan["instrument"])
            _clear_inflight_opens(account, plan["instrument"])
            logger.info(f"RESET  account={account}  closed {plan['instrument']} "
                        "before opposite-direction entry")
            if not await _interruptible_sleep(RESET_SETTLE_S, account, manual):
                logger.info(f"LEG ABORTED  account={account}  during=reset settle")
                return
        delay_s = (rule["delay_ms"]
                   + (random.uniform(0, rule["delay_jitter_ms"]) if rule["delay_jitter_ms"] else 0)
                   ) / 1000.0
        if delay_s > 0 and not await _interruptible_sleep(delay_s, account, manual):
            logger.info(f"LEG ABORTED  account={account}  during=delay  reason={_leg_blocked(account, manual)}")
            return
        reason = _leg_blocked(account, manual)
        if reason:
            logger.info(f"LEG ABORTED  account={account}  reason={reason}")
            return

        qty = plan["qty"]
        if rule["ai"]:
            verdict = await asyncio.to_thread(ai_consult, rule["ai"], _ai_context(plan))
            if verdict.get("error"):
                policy = rule["ai"].get("on_error", "skip")
                logger.warning(f"AI GATE ERROR  account={account}  policy={policy}  error={verdict['error']}")
                if policy != "allow":
                    _dash_set_alert(
                        Fore.YELLOW + f"  ⚠  {label}AI gate error — entry skipped "
                        f"({verdict['error'][:50]})" + Style.RESET_ALL)
                    return
            elif verdict["decision"] == "skip":
                _dash_set_alert(
                    Fore.YELLOW + f"  ⚠  {label}AI vetoed entry: "
                    f"{(verdict.get('reason') or 'no reason')[:60]}" + Style.RESET_ALL)
                logger.info(f"AI VETO  account={account}  reason={verdict.get('reason')}  signal={plan['signal']}")
                return
            else:
                ai_qty = verdict.get("qty")
                if isinstance(ai_qty, int) and 0 < ai_qty < qty:
                    logger.info(f"AI RESIZE  account={account}  {qty} → {ai_qty} contracts")
                    qty = ai_qty

        tranches = split_qty(qty, rule["stagger_entries"]) if qty >= 1 else [qty]
        if plan.get("cbo"):
            # Close-before-open, then write — all under the prop lock so no
            # other prop entry can slip between the confirmation and the
            # writes (the whole tranche run stays inside for the same
            # reason: a later signal must not flatten a half-placed entry
            # and then race its own entry against our remaining tranches).
            async with _get_prop_lock():
                ok, closed_any = await _prop_clear_for_entry([plan])
                if not ok:
                    if plan.get("rr_pick"):
                        _rr_return(account)
                    return
                reason = _leg_blocked(account, manual)
                if reason:
                    logger.info(f"LEG ABORTED  account={account}  reason={reason}")
                    if plan.get("rr_pick"):
                        _rr_return(account)
                    if closed_any:
                        _prop_alert_closed_without_entry(account, reason)
                    return
                placed = await _write_entry_tranches(plan, tranches, sig_id)
                if not placed and closed_any:
                    _prop_alert_closed_without_entry(account, "entry write failed")
        else:
            await _write_entry_tranches(plan, tranches, sig_id)
    except asyncio.CancelledError:
        logger.info(f"LEG CANCELLED  account={account}")
        raise
    except Exception as exc:
        logger.error(f"deferred leg error  account={account}  error={exc}")


async def _write_entry_tranches(plan: dict, tranches: list[int],
                                sig_id: str | None, quiet: bool = False) -> int:
    """Write one entry's tranche files — the shared tail of every deferred
    leg, re-checking stops/pause before every write. Returns the number of
    tranches written. `quiet` suppresses the per-account success alert
    (the prop group prints one summary line instead of N)."""
    account, rule = plan["account"], plan["rule"]
    manual = plan.get("manual", False)
    label = f"[{account}] " if len(target_accounts()) > 1 else ""
    interval_s = rule["stagger_interval_ms"] / 1000.0
    placed = 0
    for i, tranche_qty in enumerate(tranches):
        if i > 0 and interval_s > 0 and not await _interruptible_sleep(interval_s, account, manual):
            logger.info(f"LEG ABORTED  account={account}  during=stagger {i + 1}/{len(tranches)}")
            break
        reason = _leg_blocked(account, manual)
        if reason:
            logger.info(f"LEG ABORTED  account={account}  at tranche {i + 1}/{len(tranches)}  reason={reason}")
            break
        sig = _tranche_signal(plan["signal"], tranche_qty, i)
        leader_first = (i == 0 and account == active_account)
        pre_pos = 0
        if leader_first:
            pre_pos = await asyncio.to_thread(
                _pre_position, account, plan["instrument"])
        path = write_signal_to_file(sig)
        if not path:
            logger.error(f"LEG WRITE FAIL  account={account}  tranche={i + 1}  signal={sig}")
            _dash_set_alert(
                Fore.RED + f"  ✖  {label}entry write failed" + Style.RESET_ALL)
            break
        placed += 1
        _note_contract(sig)
        _ledger_note_file(plan, sig)
        # Count this write as held until NT shows it — a positions-only
        # dump would otherwise let the next signal stack a second market
        # onto a close-before-open account before the fill lands, and
        # exposure/stack math needs it on every account type.
        _note_inflight_open(account, plan["instrument"], plan["action"],
                            tranche_qty)
        if leader_first:
            add_pending_confirm(sig, sig_id, plan["instrument"], plan["action"], pre_pos)
    if placed:
        _record_stagger(plan["signal"], account, placed)
        if not quiet:
            detail = f" {placed}/{len(tranches)} tranches" if len(tranches) > 1 else ""
            timing = f" after {rule['delay_ms']}ms" if rule["delay_ms"] else ""
            _dash_set_alert(
                Fore.GREEN + f"  ✔  {label}entry placed{detail}{timing}" + Style.RESET_ALL)
        logger.info(
            f"LEG DONE  account={account}  tranches={placed}/{len(tranches)}  "
            f"qty={sum(tranches)}  signal={plan['signal']}")
    return placed


# ---------- AI signal gate ----------
# A rule may route new entries through an AI before they fire. The model
# receives the proposed order plus session context and must answer with a
# strict JSON verdict: allow / skip, an optional DOWNWARD contract resize,
# and a short reason. Exits never pass through the gate. On any error or
# timeout the rule's on_error policy decides (default: skip — fail closed).

AI_PROVIDERS = {
    "anthropic": {"model": "claude-opus-5", "key_env": "ANTHROPIC_API_KEY",
                  "endpoint": ""},
    "openai":    {"model": "gpt-4o-mini", "key_env": "OPENAI_API_KEY",
                  "endpoint": "https://api.openai.com/v1/chat/completions"},
    "ollama":    {"model": "llama3.2", "key_env": "",
                  "endpoint": "http://localhost:11434/api/chat"},
    "custom":    {"model": "", "key_env": "",
                  "endpoint": ""},  # any OpenAI-compatible /chat/completions
}

AI_GATE_SYSTEM = (
    "You are a pre-trade risk gate for automated futures copy-trading. You "
    "receive one proposed order and the account's session context. Decide "
    "whether THIS account should take the trade right now.\n\n"
    "Respond with ONLY a JSON object, no prose:\n"
    '{"decision": "allow" or "skip", "qty": <integer or null>, '
    '"reason": "<one short sentence>"}\n\n'
    "- \"allow\" places the order; \"skip\" drops it for this account only.\n"
    "- \"qty\" may REDUCE the contract count; it can never increase it. "
    "Use null to keep the proposed size.\n"
    "- Exits are never routed through you; you only gate new entries.\n"
    "- If the context is ambiguous or concerning, prefer \"skip\"."
)

AI_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["allow", "skip"]},
        "qty": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
        "reason": {"type": "string"},
    },
    "required": ["decision", "qty", "reason"],
    "additionalProperties": False,
}


def _ai_context(plan: dict) -> dict:
    """Compact JSON context handed to the gate model (runs in a thread)."""
    now = datetime.now(ET)
    account = plan["account"]
    sig_parts = plan["signal"].split(";")
    ctx = {
        "time_et": now.strftime("%Y-%m-%d %H:%M:%S"),
        "weekday": now.strftime("%A"),
        "account": account,
        "command": plan["command"],
        "instrument": plan["instrument"],
        "action": plan["action"],
        "contracts": plan["qty"],
        "atm_strategy": sig_parts[11] if len(sig_parts) >= 12 else "",
        "signals_this_session": signal_count,
    }
    start = session_start_balances.get(account)
    current = session_current_balances.get(account)
    if start is not None and current is not None:
        ctx["session_pnl_usd"] = round(session_pnl(account, current), 2)
    try:
        positions = query_nt_positions(account, nt_port)
        ctx["open_position_this_instrument"] = positions.get(plan["instrument"], 0)
        if positions:
            ctx["open_positions"] = positions
    except Exception:
        pass
    return ctx


def _ai_parse_decision(text: str) -> dict:
    """Parse the model's JSON verdict; tolerate prose around the object."""
    obj = None
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            try:
                obj = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass
    if not isinstance(obj, dict):
        raise ValueError(f"unparseable AI response: {text[:120]!r}")
    decision = str(obj.get("decision", "")).strip().lower()
    if decision not in ("allow", "skip"):
        raise ValueError(f"bad decision {obj.get('decision')!r}")
    qty = obj.get("qty")
    if isinstance(qty, bool) or not isinstance(qty, (int, float)) or int(qty) < 1:
        qty = None
    else:
        qty = int(qty)
    return {"decision": decision, "qty": qty,
            "reason": str(obj.get("reason", ""))[:300]}


def _ai_http_json(url: str, payload: dict, headers: dict, timeout_s: float) -> dict:
    """POST JSON, return parsed JSON. Raises on HTTP/network errors."""
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        raise RuntimeError(f"HTTP {exc.code}: {body or exc.reason}") from exc


def _ai_call_anthropic(ai_cfg: dict, system_prompt: str, user_payload: str,
                       timeout_s: float) -> str:
    """Consult Claude via the official SDK. Structured output keeps the
    verdict machine-parseable; falls back to plain prompting on SDKs or
    models that don't support it."""
    if anthropic is None:
        raise RuntimeError("anthropic SDK not installed — pip install anthropic")
    key = os.environ.get(ai_cfg.get("api_key_env") or "ANTHROPIC_API_KEY")
    kwargs: dict = {"timeout": timeout_s, "max_retries": 0}
    if key:
        kwargs["api_key"] = key
    client = anthropic.Anthropic(**kwargs)
    request = dict(
        model=ai_cfg.get("model") or AI_PROVIDERS["anthropic"]["model"],
        max_tokens=2048,  # hard cap covers thinking + the small JSON verdict
        system=system_prompt,
        messages=[{"role": "user", "content": user_payload}],
    )
    try:
        resp = client.messages.create(
            **request,
            output_config={"format": {"type": "json_schema",
                                      "schema": AI_DECISION_SCHEMA}})
    except TypeError:
        # SDK predates output_config — the prompt already demands JSON
        resp = client.messages.create(**request)
    except anthropic.BadRequestError:
        # model doesn't support structured outputs — plain prompting
        resp = client.messages.create(**request)
    if getattr(resp, "stop_reason", None) == "refusal":
        raise RuntimeError("model refused the request")
    for block in resp.content:
        if getattr(block, "type", "") == "text":
            return block.text
    raise RuntimeError("no text block in model response")


def _ai_call_openai_compat(ai_cfg: dict, system_prompt: str, user_payload: str,
                           timeout_s: float) -> str:
    """OpenAI or any OpenAI-compatible /v1/chat/completions endpoint
    (LM Studio, vLLM, llama.cpp server, LocalAI, ...)."""
    url = ai_cfg.get("endpoint") or AI_PROVIDERS["openai"]["endpoint"]
    if not url:
        raise RuntimeError("no endpoint configured")
    headers = {}
    key_env = ai_cfg.get("api_key_env")
    key = os.environ.get(key_env) if key_env else None
    if key:
        headers["Authorization"] = f"Bearer {key}"
    body = {
        "model": ai_cfg.get("model") or AI_PROVIDERS["openai"]["model"],
        "messages": [{"role": "system", "content": system_prompt},
                     {"role": "user", "content": user_payload}],
        "max_tokens": 512,
    }
    data = _ai_http_json(url, body, headers, timeout_s)
    return data["choices"][0]["message"]["content"]


def _ai_call_ollama(ai_cfg: dict, system_prompt: str, user_payload: str,
                    timeout_s: float) -> str:
    """Local Ollama chat endpoint with JSON-constrained output."""
    url = ai_cfg.get("endpoint") or AI_PROVIDERS["ollama"]["endpoint"]
    body = {
        "model": ai_cfg.get("model") or AI_PROVIDERS["ollama"]["model"],
        "messages": [{"role": "system", "content": system_prompt},
                     {"role": "user", "content": user_payload}],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0},
    }
    data = _ai_http_json(url, body, {}, timeout_s)
    return data["message"]["content"]


def ai_consult(ai_cfg: dict, ctx: dict) -> dict:
    """Ask the configured AI whether to take an entry. Never raises.

    Returns {"decision", "qty", "reason"} on success or {"decision": "skip",
    "error": ...} on any failure — the caller applies the rule's on_error
    policy. Runs synchronously; call via asyncio.to_thread.
    """
    system_prompt = AI_GATE_SYSTEM
    instructions = ai_cfg.get("instructions") or ""
    if instructions:
        system_prompt += "\n\nAccount owner's guidance:\n" + instructions
    user_payload = json.dumps(ctx, indent=2)
    timeout_s = ai_cfg.get("timeout_ms", 8000) / 1000.0
    provider = ai_cfg.get("provider", "")
    started = time.time()
    try:
        if provider == "anthropic":
            text = _ai_call_anthropic(ai_cfg, system_prompt, user_payload, timeout_s)
        elif provider == "ollama":
            text = _ai_call_ollama(ai_cfg, system_prompt, user_payload, timeout_s)
        elif provider in ("openai", "custom"):
            text = _ai_call_openai_compat(ai_cfg, system_prompt, user_payload, timeout_s)
        else:
            return {"decision": "skip", "error": f"unknown provider {provider!r}"}
        verdict = _ai_parse_decision(text)
        latency_ms = int((time.time() - started) * 1000)
        logger.info(
            f"AI GATE  provider={provider}  model={ai_cfg.get('model')}  "
            f"decision={verdict['decision']}  qty={verdict['qty']}  "
            f"latency={latency_ms}ms  reason={verdict['reason'][:120]}")
        return verdict
    except Exception as exc:
        return {"decision": "skip",
                "error": f"{type(exc).__name__}: {str(exc)[:160]}"}


# ---------- Strategy template helpers ----------
def _nt_base() -> Path | None:
    """Return the NinjaTrader 8 root directory (parent of incoming/)."""
    if output_directory:
        return Path(output_directory).parent
    return None


def list_atm_strategies() -> list[str]:
    """List available ATM strategy template names from the NinjaTrader directory."""
    base = _nt_base()
    if not base:
        return []
    atm_dir = base / "templates" / "AtmStrategy"
    if not atm_dir.is_dir():
        return []
    return sorted(p.stem for p in atm_dir.glob("*.xml"))


def validate_strategy(name: str) -> bool:
    """Check if a strategy template exists in AtmStrategy."""
    base = _nt_base()
    if not base:
        return False
    return (base / "templates" / "AtmStrategy" / f"{name}.xml").exists()


def _norm_atm_name(name: str) -> str:
    """Case/separator-insensitive key: 'macro_zone_b' → 'macrozoneb'."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def resolve_publisher_atm(pub_strategy: str, instrument: str = "") -> str | None:
    """Best installed ATM template for a publisher strategy name, or None.

    Publishers put their internal strategy id in field 11 (e.g.
    'macro_zone_b') while the bundled templates carry instrument-prefixed
    PascalCase filenames (GC-MacroZoneB.xml), so an exact filename check
    alone misses every one and the signal silently falls back to the
    session default — sizing stops/targets for the wrong market. Matching
    stays deterministic: exact name, then config `atm_aliases`, then a
    normalized comparison ignoring case and separators, preferring the
    signal instrument's '<root>-<name>' template over a bare match. No
    fuzzy matching — a wrong ATM on a live order is worse than the
    fallback.
    """
    pub = pub_strategy.strip()
    if not pub:
        return None
    if validate_strategy(pub):
        return pub
    root_key = ""
    if instrument:
        root_key = _norm_atm_name(_instrument_root(to_full_instrument(instrument)))
    by_norm: dict[str, str] = {}
    for tmpl in list_atm_strategies():
        by_norm.setdefault(_norm_atm_name(tmpl), tmpl)
    for name in (atm_aliases.get(pub) or atm_aliases.get(pub.lower()), pub):
        if not name:
            continue
        if name != pub and validate_strategy(name):
            return name
        key = _norm_atm_name(name)
        for candidate in ((root_key + key if root_key else ""), key):
            if candidate and candidate in by_norm:
                return by_norm[candidate]
    return None


def strategy_filter_choices(templates: list[str] | None = None) -> list[dict]:
    """Clickable strategy names for the filter editors.

    Installed ATM templates lead — the naming convention filters key on —
    then publisher names seen on the wire that don't collapse onto an
    installed template (directly or through an `atm_aliases` redirect),
    then existing filter keys not otherwise covered, so a saved filter is
    always editable even if its template was deleted. Each entry:
    {"name", "kind": "atm"|"seen"|"filter", "base": atm_base_key(name)}.
    Pass `templates` when the caller already globbed the ATM directory
    (web_state does, twice a second) to avoid a second disk scan.
    """
    out: list[dict] = []
    covered: set[str] = set()
    for tmpl in (templates if templates is not None else list_atm_strategies()):
        base = atm_base_key(tmpl)
        if base in covered:
            continue
        out.append({"name": tmpl, "kind": "atm", "base": base})
        covered.add(base)
    for pub in pub_strategies_seen:
        base = atm_base_key(pub)
        alias = atm_aliases.get(pub) or atm_aliases.get(pub.lower())
        if base in covered or (alias and atm_base_key(alias) in covered):
            continue
        out.append({"name": pub, "kind": "seen", "base": base})
        covered.add(base)
    for key in sorted(strategy_symbols):
        base = atm_base_key(key)
        if base in covered:
            continue
        out.append({"name": key, "kind": "filter", "base": base})
        covered.add(base)
    return out


def is_trade_ready() -> bool:
    """Check all requirements for signals to fire."""
    if not output_directory or not Path(output_directory).is_dir():
        return False
    if not active_account:
        return False
    if not validate_strategy(atm_strategy):
        return False
    return True


# ---------- Copy-trade account fan-out ----------
def _dedup_accounts(*groups: list) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for group in groups:
        for a in group:
            if a and a not in seen:
                seen.add(a)
                out.append(a)
    return out


def copy_trade_accounts() -> list[str]:
    """The always-trade set: the leader first, then each follower.

    The leader (active_account) is always traded. Followers mimic it. The
    list is de-duplicated with the leader kept first, so a follower that
    also names the leader can't get two order files for one signal. With no
    followers this is just [leader] — classic single-account mode.
    """
    return _dedup_accounts([active_account], follower_accounts)


def target_accounts() -> list[str]:
    """Every account this session manages: leader, followers, and the
    round-robin pool. Risk limits, session locks, and flatten-all act on
    all of them; per-signal dispatch is decided in plan_signal_legs."""
    return _dedup_accounts([active_account], follower_accounts, roundrobin_accounts)


def sanitize_roundrobin(raw, leader: str | None, followers: list[str]) -> list[str]:
    """Clean a round-robin account list: strings only, no leader, no overlap
    with followers (copy-trade wins a conflict), de-duplicated in order."""
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for a in raw:
        if not isinstance(a, str):
            continue
        name = a.strip()
        if name and name != leader and name not in followers and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _rr_pool() -> list[str]:
    """Round-robin members currently allowed to trade (not stopped out)."""
    return [a for a in roundrobin_accounts if a not in account_stops]


def _rr_eligible(account: str, instrument: str, pub_strategy: str = "") -> bool:
    """Could this pool account actually take an entry on this instrument?

    Checks the STRUCTURAL reasons a leg would be dropped — the symbol
    filter and `entries: off` — so a permanently dead account is passed
    over at draw time instead of being handed the turn and skipped, which
    would make the pool miss the trade on every rotation. Per-signal
    reasons (an AI veto, sizing to zero on a small order) can't be known
    here; those return the slot afterwards via _rr_return().
    """
    if not account_trades_symbol(account, instrument):
        return False
    return bool(resolve_rule(account, instrument, pub_strategy)["enabled"])


def _rr_next(instrument: str = "", pub_strategy: str = "") -> str | None:
    """Draw the next round-robin account for an entry on `instrument`.

    Random without repeats inside a round: a shuffled round of the pool is
    consumed one account per entry signal; a fresh round never starts with
    the account that just traded, so two consecutive signals never hit the
    same account (pool size > 1). Locked accounts (session stop/target)
    forfeit their slot when reached. An account whose symbol filter
    excludes this instrument is passed over but KEEPS its slot — it is
    still owed a trade on a market it does accept, and the entry goes to
    the next eligible account instead. When no remaining slot can take
    this instrument, the pool members not already owed a slot are shuffled
    in as a fresh round. Returns None when no pool member trades this
    instrument (copy-trade legs are unaffected).
    """
    global _rr_last
    pool = _rr_pool()
    eligible = {a for a in pool if _rr_eligible(a, instrument, pub_strategy)}
    if not eligible:
        return None
    for _ in range(2):  # pass 2 runs after the top-up below, which
        i = 0           # guarantees an eligible slot exists
        while i < len(_rr_remaining):
            account = _rr_remaining[i]
            if account not in pool:          # locked/removed — forfeits slot
                _rr_remaining.pop(i)
                continue
            if account not in eligible:      # symbol-filtered — keeps slot
                i += 1
                continue
            _rr_remaining.pop(i)
            _rr_last = account
            return account
        fresh = [a for a in pool if a not in _rr_remaining]
        if not fresh:
            return None
        random.shuffle(fresh)
        if not _rr_remaining and len(fresh) > 1 and _rr_last and fresh[0] == _rr_last:
            swap = random.randrange(1, len(fresh))
            fresh[0], fresh[swap] = fresh[swap], fresh[0]
        _rr_remaining.extend(fresh)
    return None


def _rr_return(account: str):
    """Put a drawn round-robin slot back at the head of the round.

    _rr_next() pops the account and stamps _rr_last BEFORE the leg is
    transformed, so anything that drops it afterwards — entries disabled,
    sized to zero, an AI veto, the hedge guard — consumed the turn with no
    order placed. Round-robin sends an entry to exactly ONE pool member, so
    a burnt slot means the pool misses that trade entirely, not merely that
    the rotation is unfair.
    """
    global _rr_last
    if not account:
        return
    if account not in _rr_remaining:
        _rr_remaining.insert(0, account)
    _rr_last = None          # it never traded, so it must not be avoided next
    logger.info(f"ROUND-ROBIN  slot returned to {account} (leg not placed)")


def _rr_reset_rotation():
    """Restart the rotation (pool membership changed)."""
    global _rr_remaining, _rr_last
    _rr_remaining = []
    _rr_last = None


def tradeable_accounts() -> list[str]:
    """Target accounts not currently locked out by a hit stop/target."""
    return [a for a in target_accounts() if a not in account_stops]


def session_hard_locked() -> bool:
    """True when every target account is hard-stopped (session fully locked)."""
    tgt = target_accounts()
    return bool(tgt) and all(account_stops.get(a) == "hard" for a in tgt)


# ---------- NinjaTrader incoming folder detection ----------
_NT_USER_SKIP = {"Public", "Default", "Default User", "All Users", "desktop.ini"}
_NT_SCAN_SKIP = {
    "Windows", "WinSxS", "Program Files", "Program Files (x86)",
    "$RECYCLE.BIN", "System Volume Information", "ProgramData",
    "AppData", "Recovery", "PerfLogs", "MSOCache",
    "node_modules", ".git", ".cache",
}


def _run_cmd(args: list[str], timeout: float = 6.0) -> str | None:
    """Run a subprocess and return trimmed stdout, or None on any failure."""
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    out = (result.stdout or "").strip()
    return out or None


def _windows_to_wsl_path(win_path: str) -> str | None:
    """Convert a Windows path to a WSL (/mnt/...) path via wslpath."""
    return _run_cmd(["wslpath", "-u", win_path])


def _query_windows_docs_folder() -> str | None:
    """Ask Windows for the actual MyDocuments folder via PowerShell.

    Returns a filesystem path usable from the current OS (native path on
    Windows, /mnt/... path under WSL), or None if PowerShell is unavailable.
    Handles Windows folder redirection, OneDrive, and OneDrive-Corporate.
    """
    if not (IS_WINDOWS or IS_WSL):
        return None
    win_path = _run_cmd([
        "powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
        "[Environment]::GetFolderPath('MyDocuments')",
    ])
    if not win_path:
        return None
    if IS_WINDOWS:
        return win_path
    return _windows_to_wsl_path(win_path)


def _find_nt_incoming_candidates_windows() -> list[Path]:
    """Candidate NinjaTrader 8 incoming paths on native Windows."""
    home = Path.home()
    cands: list[Path] = []

    # Ask Windows for the real Documents folder (handles redirection)
    docs = _query_windows_docs_folder()
    if docs:
        cands.append(Path(docs) / "NinjaTrader 8" / "incoming")

    cands.extend([
        home / "Documents" / "NinjaTrader 8" / "incoming",
        home / "OneDrive" / "Documents" / "NinjaTrader 8" / "incoming",
        home / "NinjaTrader 8" / "incoming",
    ])

    # OneDrive-Corporate: "OneDrive - <tenant>"
    try:
        for onedrive_dir in home.glob("OneDrive - */Documents"):
            cands.append(onedrive_dir / "NinjaTrader 8" / "incoming")
    except OSError:
        pass

    for drive_letter in "CDEFGH":
        cands.append(Path(f"{drive_letter}:/NinjaTrader 8/incoming"))
        cands.append(Path(f"{drive_letter}:/Documents/NinjaTrader 8/incoming"))
    return cands


def _find_nt_incoming_candidates_wsl() -> list[Path]:
    """Candidate NinjaTrader 8 incoming paths when running under WSL.

    Scans /mnt/<letter>/Users/<user>/.../NinjaTrader 8/incoming across all
    mounted Windows drives, plus any path Windows reports for MyDocuments.
    """
    cands: list[Path] = []

    # Ask Windows directly (handles redirection + OneDrive-Corporate cleanly)
    docs = _query_windows_docs_folder()
    if docs:
        cands.append(Path(docs) / "NinjaTrader 8" / "incoming")

    mnt = Path("/mnt")
    if not mnt.is_dir():
        return cands

    try:
        drives = [p for p in mnt.iterdir() if p.is_dir() and len(p.name) == 1]
    except OSError:
        return cands

    for drive in drives:
        users = drive / "Users"
        try:
            if users.is_dir():
                for user_dir in users.iterdir():
                    if not user_dir.is_dir() or user_dir.name in _NT_USER_SKIP:
                        continue
                    cands.append(user_dir / "Documents" / "NinjaTrader 8" / "incoming")
                    cands.append(user_dir / "OneDrive" / "Documents" / "NinjaTrader 8" / "incoming")
                    cands.append(user_dir / "NinjaTrader 8" / "incoming")
                    try:
                        for onedrive_dir in user_dir.glob("OneDrive - */Documents"):
                            cands.append(onedrive_dir / "NinjaTrader 8" / "incoming")
                    except OSError:
                        pass
        except OSError:
            pass
        # Drive-root variants
        cands.append(drive / "NinjaTrader 8" / "incoming")
        cands.append(drive / "Documents" / "NinjaTrader 8" / "incoming")
    return cands


def _shallow_scan_for_nt(roots: list[Path], max_depth: int = 4, time_budget: float = 3.0) -> str | None:
    """Time-bounded BFS for `NinjaTrader 8/incoming` under the given roots.

    Skips system and cache directories. Intended as a last-resort fallback
    when the candidate list misses a custom install location.
    """
    deadline = time.monotonic() + time_budget
    for root in roots:
        try:
            if not root.is_dir():
                continue
        except OSError:
            continue
        queue: list[tuple[Path, int]] = [(root, 0)]
        while queue:
            if time.monotonic() > deadline:
                return None
            current, depth = queue.pop(0)
            try:
                children = list(current.iterdir())
            except (OSError, PermissionError):
                continue
            for child in children:
                if time.monotonic() > deadline:
                    return None
                try:
                    if not child.is_dir():
                        continue
                except OSError:
                    continue
                if child.name in _NT_SCAN_SKIP:
                    continue
                if child.name == "NinjaTrader 8":
                    incoming = child / "incoming"
                    try:
                        if incoming.is_dir():
                            return str(incoming.resolve())
                    except OSError:
                        pass
                    continue
                if depth + 1 <= max_depth:
                    queue.append((child, depth + 1))
    return None


def _scan_roots() -> list[Path]:
    """Roots to feed into the shallow scan fallback."""
    if IS_WINDOWS:
        roots = [Path.home()]
        for d in "CDEFGH":
            roots.append(Path(f"{d}:/"))
        return roots
    if IS_WSL:
        mnt = Path("/mnt")
        roots: list[Path] = []
        if mnt.is_dir():
            try:
                for p in mnt.iterdir():
                    if p.is_dir() and len(p.name) == 1:
                        users = p / "Users"
                        if users.is_dir():
                            roots.append(users)
                        else:
                            roots.append(p)
            except OSError:
                pass
        return roots
    return []


def find_ninjatrader_incoming(deep_scan: bool = True) -> str | None:
    """Search known locations for NinjaTrader 8\\incoming on Windows or WSL.

    If the fast candidate list misses and `deep_scan` is True, falls back to
    a time-bounded shallow scan under likely roots.
    """
    if IS_WINDOWS:
        candidates = _find_nt_incoming_candidates_windows()
    elif IS_WSL:
        candidates = _find_nt_incoming_candidates_wsl()
    else:
        return None

    # Dedupe while preserving order
    seen: set[str] = set()
    ordered: list[Path] = []
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(path)

    for path in ordered:
        try:
            if path.is_dir():
                return str(path.resolve())
        except OSError:
            continue

    if deep_scan:
        found = _shallow_scan_for_nt(_scan_roots())
        if found:
            return found

    return None


def detect_or_ask_directory(cfg: dict) -> str | None:
    """Determine the output directory from config, auto-detect, or user input."""
    # If already saved in config, verify it still exists
    saved = cfg.get("output_directory")
    if saved and Path(saved).is_dir():
        return saved

    if IS_WINDOWS or IS_WSL:
        env_label = "WSL" if IS_WSL else "Windows"
        print(Fore.CYAN + f"\n  🔍  Searching for NinjaTrader 8 incoming folder ({env_label})..." + Style.RESET_ALL)
        found = find_ninjatrader_incoming()
        if found:
            print(Fore.GREEN + f"  ✔  Found: {found}" + Style.RESET_ALL)
            confirm = input(Fore.WHITE + "  Use this path? [Y/n] " + Style.RESET_ALL).strip()
            if confirm.lower() != "n":
                return found
        else:
            print(Fore.YELLOW + "  ⚠  Could not auto-detect NinjaTrader 8 incoming folder." + Style.RESET_ALL)

    # Native Linux or no auto-detect match: ask the user
    if IS_WINDOWS or IS_WSL:
        print(Fore.CYAN + "\n  Enter the NinjaTrader 8 incoming folder path manually:" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + "\n┌─ LINUX DETECTED ─────────────────────────────────────┐" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter the path to your signal output folder.        │" + Style.RESET_ALL)
        print(Fore.CYAN + "│  (NinjaTrader incoming folder or any target dir)      │" + Style.RESET_ALL)
        print(Fore.CYAN + "└──────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    while True:
        raw = input(Fore.WHITE + "  PATH ▸ " + Style.RESET_ALL).strip().strip('"').strip("'")
        if not raw:
            print(Fore.YELLOW + "  ↩  No directory set. Press S → 5 after connecting to set one." + Style.RESET_ALL)
            return None
        path = Path(raw)
        if path.is_dir():
            return str(path.resolve())
        answer = input(Fore.YELLOW + f"  ⚠  '{raw}' does not exist. Create it? [y/N] " + Style.RESET_ALL).strip()
        if answer.lower() == "y":
            try:
                path.mkdir(parents=True, exist_ok=True)
                return str(path.resolve())
            except OSError as exc:
                print(Fore.RED + f"  ✖  Could not create: {exc}" + Style.RESET_ALL)
        else:
            print(Fore.YELLOW + "  Try again or press ENTER to skip." + Style.RESET_ALL)


_nt_host_cache: dict[int, str] = {}


def _wsl_windows_host_ip() -> str | None:
    """Return the Windows host IP as seen from WSL (the default gateway).

    On WSL2 classic NAT networking this is the vEthernet (WSL) adapter IP
    Windows services should be reachable on. Returns None if unavailable.
    """
    out = _run_cmd(["ip", "route", "show", "default"], timeout=1.0)
    if out:
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[0] == "default" and parts[1] == "via":
                ip = parts[2]
                if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
                    return ip
    # Fallback: parse /proc/net/route (hex, little-endian, col 2 = gateway)
    try:
        with open("/proc/net/route") as f:
            next(f)  # skip header
            for line in f:
                cols = line.split()
                if len(cols) >= 3 and cols[1] == "00000000":  # destination 0.0.0.0
                    gw_hex = cols[2]
                    if len(gw_hex) == 8 and gw_hex != "00000000":
                        ip = ".".join(str(int(gw_hex[i:i+2], 16)) for i in (6, 4, 2, 0))
                        if not ip.startswith("127.") and not ip.startswith("169.254."):
                            return ip
    except (OSError, ValueError, StopIteration):
        pass
    return None


def _nt_host_candidates() -> list[str]:
    """Return ATI host candidates in preference order."""
    # Explicit user override always wins — used when NinjaTrader runs on a
    # different machine on the LAN and auto-detection can't reach it.
    if nt_host_override:
        return [nt_host_override]
    if IS_WINDOWS:
        return ["127.0.0.1"]
    if IS_WSL:
        # 127.0.0.1 reaches Windows under WSL2 mirrored networking.
        # The default gateway is the Windows host IP under classic NAT.
        # resolv.conf nameserver is a legacy fallback (pre-systemd-resolved).
        cands = ["127.0.0.1"]
        gw = _wsl_windows_host_ip()
        if gw and gw not in cands:
            cands.append(gw)
        try:
            with open("/etc/resolv.conf") as f:
                for line in f:
                    if line.strip().startswith("nameserver"):
                        ip = line.split()[1]
                        # Skip stub resolver (127.0.0.53) and link-local
                        if (ip
                                and not ip.startswith("127.")
                                and not ip.startswith("169.254.")
                                and ip not in cands):
                            cands.append(ip)
                        break
        except OSError:
            pass
        return cands
    return ["127.0.0.1"]


def _probe_host(host: str, port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
        return True
    except OSError:
        return False


def invalidate_nt_host_cache() -> None:
    """Clear the cached ATI host (call after port/network changes)."""
    _nt_host_cache.clear()


BRIDGE_TOKEN_FILE = "SocketTraderBridge.token"


def bridge_token() -> str:
    """The shared secret guarding the AddOn socket, creating it if absent.

    Kept in config AND written next to NinjaTrader's user data, because the
    two processes have no other channel: SocketTrader owns the value, the
    AddOn reads the file at startup. Without this the socket would stream
    account balances and accept a FLATTEN from anyone who could reach the
    port — and it cannot be loopback-only, since SocketTrader usually runs
    under WSL and connects across the host's NAT subnet.
    """
    cfg = load_config()
    token = str(cfg.get("live_bridge_token") or "").strip()
    if not token:
        token = secrets.token_urlsafe(32)
        cfg["live_bridge_token"] = token
        save_config(cfg)
        logger.info("live bridge token generated")
    return token


def write_bridge_token(token: str | None = None) -> Path | None:
    """Publish the token where the AddOn reads it. Returns the path written."""
    token = token or bridge_token()
    base = _nt_base()
    if not base:
        return None
    path = Path(base) / BRIDGE_TOKEN_FILE
    try:
        existing = path.read_text(encoding="utf-8").strip() if path.exists() else None
        if existing != token:
            # Atomic + private, mirroring save_config: a truncating in-place
            # write can be read as empty by the AddOn mid-update, which
            # fails it closed until NinjaTrader restarts.
            fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".sttoken")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(token)
                try:
                    os.chmod(tmp, 0o600)
                except OSError:
                    pass          # DrvFs/NTFS ignores POSIX modes
                os.replace(tmp, path)
            except OSError:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            logger.info(f"live bridge token written → {path} "
                        "(restart or recompile the AddOn to pick it up)")
        return path
    except OSError as exc:
        logger.error(f"could not write bridge token to {path}: {exc}")
        return None


def bridge_auth_line() -> bytes:
    """The first line every bridge client must send."""
    return (json.dumps({"auth": bridge_token()}) + "\n").encode("utf-8")


def probe_live_bridge(host: str, port: int, timeout: float = 2.5) -> bool:
    """Probe the optional SocketTraderBridge AddOn. See _probe_live_bridge_detail."""
    ok, _reason = _probe_live_bridge_detail(host, port, timeout)
    return ok


def _probe_live_bridge_detail(host: str, port: int, timeout: float = 2.5) -> tuple[bool, str]:
    """Probe the AddOn and return (ok, reason).

    Success = TCP connect + at least one newline-delimited JSON line that
    parses and carries an 'accounts' field. On failure, `reason` is a
    short human-readable string (ConnectionRefused / Timeout / BadJSON /
    MissingField / etc.) so the caller can surface it to the user.
    """
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
    except ConnectionRefusedError:
        return False, "connection refused (nothing listening)"
    except socket.timeout:
        return False, "TCP connect timed out"
    except OSError as e:
        return False, f"connect error: {e}"
    try:
        # The AddOn now speaks only to authenticated peers and sends
        # nothing until it has seen the token.
        s.sendall(bridge_auth_line())
    except OSError as e:
        return False, f"auth send failed: {e}"
    try:
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                return False, "socket closed before any data"
            buf += chunk
    except socket.timeout:
        return False, "socket accepted but no data arrived (timeout)"
    except OSError as e:
        return False, f"read error: {e}"
    finally:
        try:
            if s is not None:
                s.close()
        except OSError:
            pass
    line = buf.split(b"\n", 1)[0].decode("utf-8", errors="ignore")
    try:
        obj = json.loads(line)
    except (ValueError, json.JSONDecodeError):
        preview = line[:60].replace("\n", " ")
        return False, f"response is not JSON: {preview!r}"
    if not isinstance(obj, dict) or "accounts" not in obj:
        return False, "JSON missing 'accounts' field (wrong port?)"
    return True, "ok"


def _nt_host(port: int = 36973) -> str:
    """Return the correct host for NinjaTrader ATI (handles WSL).

    If the user set `nt_host` in config, use it unconditionally — no probe,
    no cache. Otherwise probe each auto-detected candidate on first call
    per port and cache the first that accepts a connection. If no candidate
    responds, return the first candidate without caching so a later call
    can retry.
    """
    if nt_host_override:
        return nt_host_override
    cached = _nt_host_cache.get(port)
    if cached is not None:
        return cached
    candidates = _nt_host_candidates()
    for host in candidates:
        if _probe_host(host, port):
            _nt_host_cache[port] = host
            return host
    return candidates[0] if candidates else "127.0.0.1"


# NinjaTrader ends every state dump with this marker. It does NOT close the
# socket afterwards, so the marker is the only reliable end-of-response
# signal — waiting for silence is not one.
_ATI_END = (b"ATI\x00True\x00", b"ATI\x00False\x00")


def ati_response_complete(text: str) -> bool:
    """True when an ATI response carries NinjaTrader's end-of-dump marker."""
    return text.endswith(("ATI\x00True\x00", "ATI\x00False\x00"))


def _query_ati(command: str, port: int = 36973, timeout: float = 2.0,
               stall: float | None = None) -> str:
    """Send a command to NinjaTrader ATI and return the raw response text.

    Reads until NT's end-of-dump marker rather than until the stream goes
    quiet. The old "stop after 250ms of silence" rule silently truncated
    the ~10KB dump whenever NT paused mid-send — measured gaps of 300ms
    are normal — and a truncated dump does not fail loudly: it parses fine
    and simply comes back missing whatever had not arrived yet. That made
    accounts and positions blink in and out of existence.

    `timeout` caps the whole exchange; `stall` (default: the same value,
    so plain callers behave as before) is the longest silence tolerated
    between two chunks. The shared snapshot stream passes a long cap with
    a short stall: a dump that is still arriving is read to its end
    instead of being cut at a fixed wall and re-requested — the live log
    shows 40–70 KB dumps cut at 3.0 s through the busiest hours, and each
    retry added one more stream to the congestion that caused it — while
    a NinjaTrader that has gone quiet is given up on quickly.

    A response without the marker is returned anyway (callers may still
    want a partial), but is logged so truncation is never silent.
    """
    stall = timeout if stall is None else min(stall, timeout)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    started = time.monotonic()
    deadline = started + timeout
    last_rx = started
    buf = b""
    try:
        # A connect that does not complete within one stall window is a
        # NinjaTrader that is not answering — no reason to hold the caller
        # for the whole cap.
        s.settimeout(stall)
        s.connect((_nt_host(port), port))
        s.sendall(f"{command}\n".encode())
        while True:
            now = time.monotonic()
            remaining = min(deadline - now, last_rx + stall - now)
            if remaining <= 0:
                break
            s.settimeout(min(remaining, 0.5))
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                continue          # a pause is not the end — only the marker is
            if not chunk:
                break             # NT closed the connection
            buf += chunk
            last_rx = time.monotonic()
            if buf.endswith(_ATI_END):
                break
    except Exception:
        return ""
    finally:
        s.close()
    text = buf.decode("utf-8", errors="ignore")
    if text and not ati_response_complete(text):
        elapsed = time.monotonic() - started
        why = "stalled" if elapsed < timeout - 0.05 else "hit the cap"
        logger.warning(f"ATI TRUNCATED  command={command}  got={len(text)} bytes "
                       f"without end marker after {elapsed:.1f}s ({why}; "
                       f"cap {timeout}s, stall {stall}s)")
    return text


def _parse_ati_fields(text: str) -> list[tuple[str, str, str]]:
    """Parse NT ATI state-dump into a list of (field, key, value) triples.

    Wire format repeats `<Field>|<Key>\\x00<Value>\\x00` where Key is an
    account name, instrument+account, or an order UUID depending on Field.
    Value may be any string (signed integer, decimal, pipe-joined UUIDs,
    status text, or empty). We don't coerce to float here — callers that
    want numeric fields do their own parsing.
    """
    return [(m.group(1), m.group(2), m.group(3))
            for m in re.finditer(r"([A-Za-z][A-Za-z0-9_]+)\|([^\x00]*)\x00([^\x00]*)", text)]


def query_nt_accounts(port: int = 36973, timeout: float = 3.0) -> list[dict]:
    """Query NinjaTrader ATI for connected accounts with cash balances.

    Returns list of dicts: [{"name": "Sim101", "cash": 27462.14}, ...]

    Only CashValue (realized cash) is returned — NT's TCP ATI doesn't push
    a live-equity field on typical installs, so session risk is enforced
    on realized cash deltas from completed trades. Per-trade risk is
    handled by NinjaTrader's own ATM stop/target.
    """
    text = _query_ati("ACCOUNTS", port, timeout)
    if not text:
        return []
    cash_by_account: dict[str, float] = {}
    for field, key, val in _parse_ati_fields(text):
        if field != "CashValue" or not key:
            continue
        try:
            cash_by_account[key] = float(val)
        except ValueError:
            pass
    return [{"name": n, "cash": v} for n, v in cash_by_account.items()]


def _query_ati_complete(command: str, port: int, timeout: float = 2.0) -> str:
    """_query_ati with one retry when the end-of-dump marker is missing.

    A short dump parses cleanly and simply lacks whatever had not arrived,
    so a position or a working order can silently not exist. The risk paths
    that cancel orders and close positions must not act on that.
    """
    text = _query_ati(command, port, timeout)
    if text and not ati_response_complete(text):
        text = _query_ati(command, port, timeout)
    return text


def query_nt_positions(account: str, port: int = 36973) -> dict[str, int]:
    """Query NinjaTrader ATI for open positions on an account.

    Returns dict of instrument -> signed quantity (positive = long,
    negative = short). Flat positions are omitted.

    NT broadcasts the same position under multiple aliases (e.g.
    "NQ JUN26", "@NQ", "NQM26", "NQ M6"); we dedupe by preferring the
    first alias that contains a space (the human-readable "ROOT MONTHYY"
    form) since NT accepts that format back in PLACE / CLOSEPOSITION.
    """
    text = _query_ati_complete("POSITIONS", port)
    if not text:
        return {}
    logger.debug(f"ATI POSITIONS raw ({len(text)} bytes): {repr(text[:500])}")

    # NT pushes MarketPosition|<instrument>|<account>\x00<signed_int>
    # The <instrument>|<account> is one captured group; we need to split
    # off the trailing |<account> suffix to get just the instrument alias.
    suffix = f"|{account}"
    aliases: dict[str, int] = {}  # alias -> signed qty
    for field, key, val in _parse_ati_fields(text):
        if field != "MarketPosition" or not key.endswith(suffix):
            continue
        alias = key[:-len(suffix)]
        if not alias:
            continue
        try:
            qty = int(val)
        except ValueError:
            continue
        if qty == 0:
            continue
        aliases[alias] = qty

    # Dedupe: group aliases by (qty, sign) and pick a friendly representative
    # for each distinct position. This is imperfect if the user holds two
    # different instruments with identical signed qty, so also split by the
    # alias's leading "root" token to keep NQ and ES distinct.
    def root_of(alias: str) -> str:
        s = alias.strip().lstrip("@")
        if " " in s:
            return s.split()[0]
        # Continuous-contract code like "NQM26" — strip trailing
        # <month_code><1-2 digit year>. Month codes: FGHJKMNQUVXZ.
        m = re.match(r"^([A-Za-z0-9]+?)[FGHJKMNQUVXZ]\d{1,2}$", s)
        return m.group(1) if m else s
    by_root_qty: dict[tuple[str, int], list[str]] = {}
    for alias, qty in aliases.items():
        by_root_qty.setdefault((root_of(alias), qty), []).append(alias)
    result: dict[str, int] = {}
    for (root, qty), names in by_root_qty.items():
        spaced = [n for n in names if " " in n and not n.startswith("@")]
        result[(spaced or names)[0]] = qty
    logger.debug(f"ATI POSITIONS parsed: {result}")
    return result


def _alias_root(alias: str) -> str:
    """Root symbol of an NT instrument alias ("NQ SEP26"/"@NQ"/"NQU26" -> NQ)."""
    s = alias.strip().lstrip("@")
    if " " in s:
        return s.split()[0]
    m = re.match(r"^([A-Za-z0-9]+?)[FGHJKMNQUVXZ]\d{1,2}$", s)
    return m.group(1) if m else s


def _underlying_root(alias: str) -> str:
    """Underlying market for an NT instrument alias, micro folded into full.

    Handles every alias shape NinjaTrader broadcasts — "MNQ 09-26",
    "MNQ SEP26", "MNQU26", "@MNQ" — because to_full_instrument alone only
    understands the spaced form. That gap made the hedge and sync checks
    blind to a micro position whenever NT reported it as a continuous
    code, which is exactly when a cross-account hedge would go unseen.
    """
    root = _alias_root(alias).upper()
    reverse = {v: k for k, v in micro_map.items() if v != k}
    return reverse.get(root, root)


def _pick_alias(names: list[str]) -> str:
    """Prefer the human "ROOT MONYY" alias — NT accepts that form back."""
    spaced = [n for n in names if " " in n and not n.startswith("@")]
    return (spaced or names)[0]


def nt_snapshot(port: int | None = None, timeout: float = 3.0,
                retry: bool = True, stall: float | None = None) -> dict:
    """One ATI state dump parsed into everything the UI needs.

    NinjaTrader answers ACCOUNTS / POSITIONS / ORDERS with the *same* full
    state dump, so a single request yields every account's cash, realized
    P&L and buying power, every open position with its average entry, and
    each account's working orders (count in `working`, ids in
    `open_orders`). Polling one snapshot beats the old per-account
    queries: one socket round-trip instead of N.

    `req_ts` is the wall clock at the moment the request went out: the
    dump describes NinjaTrader at that instant or later, which is what a
    verifier means by "a state read taken after the closes were written".
    """
    req_ts = time.time()
    # One retry on a truncated dump: a partial response parses cleanly and
    # would otherwise look like accounts and positions having disappeared.
    # The shared snapshot stream passes retry=False — its next tick IS the
    # retry, without doubling the load on a NinjaTrader that is already
    # too slow to finish one dump.
    text = _query_ati("ACCOUNTS", port or nt_port, timeout, stall)
    if retry and text and not ati_response_complete(text):
        text = _query_ati("ACCOUNTS", port or nt_port, timeout, stall)
    complete = bool(text) and ati_response_complete(text)
    snap: dict = {"ok": complete, "accounts": {}, "positions": [],
                  "working": {}, "open_orders": {}, "ts": time.time(),
                  "req_ts": req_ts, "partial": bool(text) and not complete}
    if not text:
        return snap

    accounts: dict[str, dict] = {}
    pos_qty: dict[str, dict[str, int]] = {}    # account -> alias -> qty
    pos_price: dict[str, dict[str, float]] = {}
    order_ids: dict[str, list[str]] = {}
    order_status: dict[str, str] = {}

    def acct(name: str) -> dict:
        return accounts.setdefault(
            name, {"cash": None, "realized": None, "buying_power": None})

    for field, key, val in _parse_ati_fields(text):
        if not key:
            continue  # NT emits a blank-key aggregate row; skip it
        if field in ("CashValue", "RealizedPnL", "BuyingPower"):
            try:
                num = float(val)
            except ValueError:
                continue
            slot = {"CashValue": "cash", "RealizedPnL": "realized",
                    "BuyingPower": "buying_power"}[field]
            acct(key)[slot] = num
        elif field in ("MarketPosition", "AvgEntryPrice") and "|" in key:
            alias, _, account = key.rpartition("|")
            if not alias or not account:
                continue
            acct(account)
            try:
                num = float(val)
            except ValueError:
                continue
            if field == "MarketPosition":
                if num:
                    pos_qty.setdefault(account, {})[alias] = int(num)
            else:
                pos_price.setdefault(account, {})[alias] = num
        elif field == "Orders":
            order_ids[key] = [o for o in val.split("|") if o]
        elif field == "OrderStatus":
            order_status[key] = val

    for account, aliases in pos_qty.items():
        grouped: dict[tuple[str, int], list[str]] = {}
        for alias, qty in aliases.items():
            grouped.setdefault((_alias_root(alias), qty), []).append(alias)
        for (_root, qty), names in grouped.items():
            chosen = _pick_alias(names)
            price = None
            for n in names:
                if pos_price.get(account, {}).get(n):
                    price = pos_price[account][n]
                    break
            snap["positions"].append({
                "account": account, "instrument": chosen, "qty": qty,
                "avg_price": price})

    for account, ids in order_ids.items():
        if account:
            open_ids = [oid for oid in ids
                        if order_status.get(oid) in OPEN_ORDER_STATES]
            snap["working"][account] = len(open_ids)
            snap["open_orders"][account] = open_ids

    snap["accounts"] = accounts
    snap["positions"].sort(key=lambda p: (p["account"], p["instrument"]))
    return snap


# ---------- Shared NinjaTrader snapshot stream ----------
# Every consumer of NinjaTrader's state — the web dashboard, the P&L
# tracker, the balance monitor, fill confirmation, flattens and their
# verification — used to take its own ATI dump. That is four to five
# overlapping 40–70 KB streams a second through the busiest hours, and
# the live log shows what it cost: dumps cut at the 3 s wall 169 times in
# six days, each cut followed by a retry that added one more stream, and
# a web click that had to wait for a dump of its own before an order file
# was written (a close-all took 3 s to reach its first CLOSEPOSITION).
#
# One daemon thread now takes dumps back-to-back, never two at once, and
# publishes each with the time its request went out. Readers pick the
# freshness they need:
#   snapshot_cached(max_age)  — the latest, if requested within max_age
#   snapshot_after(t)         — block for one requested at or after t (a
#                               verifier's "read taken after the closes")
# Both fall back to the direct per-caller query when the stream is not
# running (the terminal tools, the test suite), so nothing below depends
# on it for correctness — only for speed and for keeping NinjaTrader's
# ATI down to a single stream.
SNAPSHOT_INTERVAL = 1.0        # seconds from one request to the next when nobody is waiting
SNAPSHOT_MIN_GAP = 0.25        # breathing room after a slow dump before the next one
SNAPSHOT_TIMEOUT = 10.0        # wall-clock cap on one dump …
SNAPSHOT_STALL = 2.0           # … and the silence that ends one early (see _query_ati)
SNAPSHOT_DOWN_BACKOFF = 2.0    # NinjaTrader unreachable: how long before asking again
SNAPSHOT_FIRST_WAIT = 3.0      # the first dashboard poll waits this long for a first dump

_snap_cond = threading.Condition()
_snap_latest: dict | None = None      # newest snapshot, complete or not
_snap_latest_ok: dict | None = None   # newest COMPLETE snapshot (end marker seen)
_snap_seq = 0
_snap_thread: threading.Thread | None = None
_snap_stop = threading.Event()
_snap_wake = threading.Event()        # a reader wants the next dump now


def snapshot_poller_running() -> bool:
    return _snap_thread is not None and _snap_thread.is_alive()


def _snapshot_nudge():
    """Ask the stream for its next dump now (no-op without the stream)."""
    _snap_wake.set()


def _snapshot_publish(snap: dict):
    global _snap_latest, _snap_latest_ok, _snap_seq
    with _snap_cond:
        _snap_seq += 1
        snap["seq"] = _snap_seq
        _snap_latest = snap
        if snap.get("ok"):
            _snap_latest_ok = snap
        _snap_cond.notify_all()


def _snapshot_poller_loop():
    while not _snap_stop.is_set():
        req = time.time()
        try:
            snap = nt_snapshot(nt_port, timeout=SNAPSHOT_TIMEOUT, retry=False,
                               stall=SNAPSHOT_STALL)
        except Exception as exc:
            logger.error(f"SNAPSHOT STREAM  dump failed: {exc}")
            snap = {"ok": False, "accounts": {}, "positions": [], "working": {},
                    "open_orders": {}, "ts": time.time(), "partial": False}
        snap["req_ts"] = req
        if _snap_stop.is_set():
            break                 # stopped mid-dump: never publish over a reset
        _snapshot_publish(snap)
        done = time.time()
        if not snap.get("accounts") and not snap.get("ok"):
            pause = SNAPSHOT_DOWN_BACKOFF      # nothing came back — NT is down
        else:
            pause = max(req + SNAPSHOT_INTERVAL - done, SNAPSHOT_MIN_GAP)
        if _snap_wake.wait(pause):
            _snap_wake.clear()


def start_snapshot_poller():
    """Start the shared stream (idempotent)."""
    global _snap_thread
    if snapshot_poller_running():
        return
    _snap_stop.clear()
    _snap_wake.clear()
    _snap_thread = threading.Thread(target=_snapshot_poller_loop,
                                    name="nt-snapshot", daemon=True)
    _snap_thread.start()


def stop_snapshot_poller():
    global _snap_thread, _snap_latest, _snap_latest_ok
    _snap_stop.set()
    _snap_wake.set()
    with _snap_cond:
        _snap_cond.notify_all()
    t = _snap_thread
    _snap_thread = None
    if t is not None:
        t.join(timeout=0.5)     # never waits out a dump in flight — daemon thread
    _snap_latest = None
    _snap_latest_ok = None


def snapshot_cached(max_age: float) -> dict | None:
    """The newest snapshot if its request went out within `max_age`
    seconds (age is measured from the request, the conservative end),
    else None."""
    snap = _snap_latest
    if snap is None or time.time() - snap["req_ts"] > max_age:
        return None
    return snap


def snapshot_after(after: float, timeout: float) -> dict | None:
    """Block (thread-side) for a snapshot whose request went out at or
    after wall-clock `after`, nudging the stream to start one now.
    Returns None when the stream stops or `timeout` passes first."""
    deadline = time.monotonic() + timeout
    with _snap_cond:
        while True:
            snap = _snap_latest
            if snap is not None and snap["req_ts"] >= after:
                return snap
            if _snap_stop.is_set() or not snapshot_poller_running():
                return None
            _snap_wake.set()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            _snap_cond.wait(min(remaining, 0.5))


def _snapshot_positions(snap: dict, account: str) -> dict[str, int]:
    """One account's positions from a snapshot, in query_nt_positions'
    shape (alias → signed qty)."""
    return {p["instrument"]: p["qty"] for p in snap.get("positions") or []
            if p["account"] == account and p["qty"]}


def _position_qty(positions: dict[str, int], instrument: str) -> int:
    """Signed quantity held on `instrument`, whatever alias NinjaTrader
    chose to report it under.

    Orders name the contract the OIF way ("MNQ 12-26"); the state dump
    prefers "MNQ DEC26". Six days of the live log show every fill
    confirmation arriving through the balance-delta path and none through
    the position delta — this exact mismatch — so the lookup folds both
    spellings onto the OIF form before comparing."""
    if instrument in positions:
        return positions[instrument]
    want = _nt_contract_aliases(instrument)[:1]
    if not want:
        return 0
    for alias, qty in positions.items():
        if _nt_contract_aliases(alias)[:1] == want:
            return qty
    return 0


def _stream_snapshot(max_age: float, wait: float, what: str) -> dict | None:
    """The stream's freshest snapshot for a periodic reader: one requested
    within `max_age`, waiting up to `wait` for it, else the newest there
    is (logged with its age). None only when the stream is not running.

    A reader NEVER opens a dump of its own while the stream runs: a
    NinjaTrader too slow to finish one dump is the one case where a second
    stream is exactly wrong — the harness reproduced 4 concurrent dumps
    and 18 cut-off reads from that fallback before it was removed."""
    if not snapshot_poller_running():
        return None
    snap = snapshot_after(time.time() - max_age, wait)
    if snap is None:
        snap = _snap_latest
        if snap is None:
            # Running, but no dump has landed yet — start-up under a slow
            # NinjaTrader. An empty read, exactly what a failed query gave
            # before; the harness caught the alternative (a direct query)
            # opening three extra streams against a NinjaTrader that was
            # already too slow to finish its first.
            logger.warning(f"{what}  the stream has no dump yet — reading nothing")
            return _empty_snapshot()
        logger.warning(f"{what}  no snapshot younger than {max_age + wait:.0f}s "
                       f"— reading one {time.time() - snap['req_ts']:.1f}s old")
    return snap


def _empty_snapshot() -> dict:
    now = time.time()
    return {"ok": False, "partial": False, "accounts": {}, "positions": [],
            "working": {}, "open_orders": {}, "ts": now, "req_ts": now}


def _positions_now(account: str, max_age: float) -> dict[str, int]:
    """One account's positions: from the shared stream when it is running,
    else one direct ATI query — the pre-stream path, which the tests
    drive."""
    snap = _stream_snapshot(max_age, max_age, "POSITIONS")
    if snap is not None:
        return _snapshot_positions(snap, account)
    return query_nt_positions(account, nt_port)


def _accounts_now(max_age: float) -> list[dict]:
    """Every account's CashValue, in query_nt_accounts' shape, from the
    shared stream when it is running, else one direct ATI query."""
    snap = _stream_snapshot(max_age, max_age, "BALANCES")
    if snap is not None:
        return [{"name": name, "cash": info["cash"]}
                for name, info in snap["accounts"].items()
                if isinstance(info.get("cash"), (int, float))]
    return query_nt_accounts(nt_port)


def _pre_position(account: str, instrument: str) -> int:
    """The leader's position before an entry write, for fill confirmation.

    Never costs the order a dump: with the stream running, the newest
    snapshot is already in hand and, having been requested before the
    write, is pre-fill by construction — the same reading the old blocking
    query returned 0.3–4 s later. Without the stream, one direct query as
    before; any failure reads as 0, as it always did."""
    if snapshot_poller_running():
        # A partial dump can simply lack the position; the newest COMPLETE
        # read is the baseline while it is recent, so a cut-off dump under
        # congestion cannot mint a false "FILLED" from a 0 it never held.
        snap = _snap_latest_ok
        if snap is None or time.time() - snap["req_ts"] > 5.0:
            snap = _snap_latest
        if snap is None:
            return 0
        age = time.time() - snap["req_ts"]
        if age > 5.0:
            logger.warning(f"PRE-POSITION  newest snapshot is {age:.1f}s old "
                           f"(NinjaTrader slow?) — confirming {instrument} on "
                           f"{account} against it anyway")
        return _position_qty(_snapshot_positions(snap, account), instrument)
    try:
        return _position_qty(query_nt_positions(account, nt_port), instrument)
    except Exception:
        return 0


FLATTEN_SNAPSHOT_MAX_AGE = 5.0   # a flatten acts on a snapshot at most this old …
FLATTEN_SNAPSHOT_WAIT = 2.0      # … waiting this long for a newer one before using the newest anyway


def _flatten_snapshot() -> dict | None:
    """The state a flatten works from — which orders to cancel by id and
    which instruments to close by file.

    From the shared stream: a snapshot requested within
    FLATTEN_SNAPSHOT_MAX_AGE, waiting up to FLATTEN_SNAPSHOT_WAIT for one;
    past that the newest available, logged with its age, because the
    closes matter more than a perfect inventory (the bridge's Flatten()
    needs no inventory at all, CLOSEPOSITION cancels the instrument's
    working orders itself, and session_contracts backstops the file path).
    None without the stream, and the callers query directly as before."""
    return _stream_snapshot(FLATTEN_SNAPSHOT_MAX_AGE, FLATTEN_SNAPSHOT_WAIT, "FLATTEN")


# ---------- Futures instrument catalog ----------
# The web ticket needs something to click BEFORE anything has traded, so the
# picker is driven by this catalog rather than by session history. Month sets
# are the actively-quoted cycles per product family: "HMUZ" = Mar/Jun/Sep/Dec
# (quarterly), "ALL" = every calendar month. Contracts are rendered in the
# "ROOT MM-YY" form the OIF signals use (e.g. "NQ 09-26").
MONTH_CODES = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
               7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
QUARTERLY = (3, 6, 9, 12)

FUTURES_CATALOG = [
    # root, description, micro root, active months, group
    ("ES",  "E-mini S&P 500",      "MES", QUARTERLY,            "Equity index"),
    ("NQ",  "E-mini Nasdaq-100",   "MNQ", QUARTERLY,            "Equity index"),
    ("YM",  "E-mini Dow",          "MYM", QUARTERLY,            "Equity index"),
    ("RTY", "E-mini Russell 2000", "M2K", QUARTERLY,            "Equity index"),
    ("CL",  "WTI Crude Oil",       "MCL", "ALL",                "Energy"),
    ("NG",  "Natural Gas",         "",    "ALL",                "Energy"),
    ("RB",  "RBOB Gasoline",       "",    "ALL",                "Energy"),
    ("GC",  "Gold",                "MGC", (2, 4, 6, 8, 10, 12), "Metals"),
    ("SI",  "Silver",              "SIL", (3, 5, 7, 9, 12),     "Metals"),
    ("HG",  "Copper",              "MHG", (3, 5, 7, 9, 12),     "Metals"),
    ("PL",  "Platinum",            "",    (1, 4, 7, 10),        "Metals"),
    ("ZB",  "30-Year T-Bond",      "",    QUARTERLY,            "Rates"),
    ("ZN",  "10-Year T-Note",      "",    QUARTERLY,            "Rates"),
    ("ZF",  "5-Year T-Note",       "",    QUARTERLY,            "Rates"),
    ("ZT",  "2-Year T-Note",       "",    QUARTERLY,            "Rates"),
    ("6E",  "Euro FX",             "M6E", QUARTERLY,            "FX"),
    ("6B",  "British Pound",       "M6B", QUARTERLY,            "FX"),
    ("6J",  "Japanese Yen",        "",    QUARTERLY,            "FX"),
    ("6A",  "Australian Dollar",   "M6A", QUARTERLY,            "FX"),
    ("6C",  "Canadian Dollar",     "",    QUARTERLY,            "FX"),
    ("ZC",  "Corn",                "",    (3, 5, 7, 9, 12),     "Ags"),
    ("ZS",  "Soybeans",            "",    (1, 3, 5, 7, 8, 9, 11), "Ags"),
    ("ZW",  "Wheat",               "",    (3, 5, 7, 9, 12),     "Ags"),
    ("BTC", "Bitcoin",             "MBT", "ALL",                "Crypto"),
    ("ETH", "Ether",               "MET", "ALL",                "Crypto"),
]


def _active_months(spec) -> tuple[int, ...]:
    return tuple(range(1, 13)) if spec == "ALL" else tuple(spec)


def _roll_cutoff(group: str, year: int, month: int) -> datetime:
    """ET moment after which contract (year, month) is stale for NEW entries.

    Approximates when brokers stop accepting fresh positions, not when the
    exchange delists: physically delivered metals/ags go liquidation-only
    near first notice, which falls at the END of the month BEFORE the
    delivery month (see 2026-08-27: SIL 09-26 rejected "Liquidation only,
    contract is about to be expired"). Energy expires around the 20th of
    the month before its delivery month. Financials trade well into the
    contract month — NT's own rollover is ~8 days before the 3rd Friday.
    Deliberately conservative toward rolling EARLY: the next contract is
    tradeable a little sooner, while the old one hard-rejects.
    """
    if group in ("Metals", "Ags"):
        return datetime(year, month, 1, tzinfo=ET) - timedelta(days=7)
    if group == "Energy":
        py, pm = (year - 1, 12) if month == 1 else (year, month - 1)
        return datetime(py, pm, 13, tzinfo=ET)
    return datetime(year, month, 11, tzinfo=ET)


def contract_months(root_spec, now: datetime | None = None, count: int = 3,
                    group: str = "") -> list[tuple[int, int]]:
    """The next `count` (year, month) contracts for a month cycle.

    Contracts past their family's roll cutoff (_roll_cutoff) are excluded,
    so the web picker and the signal-path month correction share one
    calendar. This is a best-effort calendar, not an exchange one — the
    bridge's NT-reported front months override it where available, and the
    ticket always accepts a typed contract for anything unusual.
    """
    now = now or datetime.now(ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    months = _active_months(root_spec)
    out: list[tuple[int, int]] = []
    year, month = now.year, now.month
    for _ in range(40):
        if month in months and now < _roll_cutoff(group, year, month):
            out.append((year, month))
            if len(out) >= count:
                break
        month += 1
        if month > 12:
            month, year = 1, year + 1
    return out


def instrument_catalog(now: datetime | None = None) -> list[dict]:
    """Clickable instruments for the web ticket: root, name, and live
    contract codes, with the micro twin when the product has one."""
    out = []
    for root, name, micro, months, group in FUTURES_CATALOG:
        codes = [f"{root} {m:02d}-{str(y)[-2:]}"
                 for y, m in contract_months(months, now, group=group)]
        if not codes:
            continue
        out.append({"root": root, "name": name, "micro": micro,
                    "group": group, "contracts": codes})
    return out


# ---------- Front months (contract expiry roll) ----------
# Root → "MM-YY" of the contract NinjaTrader itself considers current, as
# reported by the SocketTraderBridge AddOn (its instrument DB applies NT's
# rollover schedule). Synced on every bridge connect and once per ET day,
# and cached in config so a session without NT still has the last known
# fronts. The signal path never queries anything: correct_contract_month
# is a pure lookup against this map with the calendar as fallback.
front_months: dict[str, str] = {}
_front_months_date = ""          # ET date of the last successful bridge sync
_front_months_attempt_date = ""  # ET date of the last attempt (bounds retries)
_front_months_syncing = False

_CONTRACT_FORM_RE = re.compile(r"^([A-Za-z0-9]{1,6}) (\d{2})-(\d{2})$")
_MMYY_RE = re.compile(r"^\d{2}-\d{2}$")


def _catalog_row(root: str) -> tuple | None:
    """Catalog row for a full OR micro root, else None."""
    r = (root or "").strip().upper()
    for row in FUTURES_CATALOG:
        if r == row[0] or (row[2] and r == row[2]):
            return row
    return None


def front_contract(root: str, now: datetime | None = None) -> str | None:
    """Calendar-computed front 'MM-YY' for a cataloged root, else None."""
    row = _catalog_row(root)
    if not row:
        return None
    got = contract_months(row[3], now, count=1, group=row[4])
    if not got:
        return None
    y, m = got[0]
    return f"{m:02d}-{str(y)[-2:]}"


def expected_contract(root: str, now: datetime | None = None) -> str | None:
    """Front 'MM-YY' for a root: the LATER of calendar and NT-reported.

    NT rolls financials early by volume — follow it. A cached NT month
    older than the calendar front is a stale cache (NT unreachable across
    a roll) — the calendar wins there, so the map can only fail toward
    the newer, still-tradeable contract.
    """
    row = _catalog_row(root)
    if not row:
        return None
    cal = front_contract(root, now)
    r = (root or "").strip().upper()
    seen = front_months.get(r) or front_months.get(row[0]) or (
        front_months.get(row[2]) if row[2] else None)
    if seen and not _MMYY_RE.fullmatch(str(seen)):
        seen = None

    def key(tail: str) -> tuple[int, int]:
        mm, yy = tail.split("-")
        return (int(yy), int(mm))

    cands = [c for c in (cal, seen) if c]
    return max(cands, key=key) if cands else None


def correct_contract_month(instrument: str, now: datetime | None = None
                           ) -> tuple[str, str | None]:
    """('SIL 12-26', 'SIL 09-26') when the month was stale, else (input, None).

    Only 'ROOT MM-YY' instruments with a cataloged root are considered —
    typed or exotic contracts pass through untouched. Rolls only ever move
    FORWARD: a month at or beyond the known front (a deliberate back-month
    trade) is left alone.
    """
    m = _CONTRACT_FORM_RE.match((instrument or "").strip())
    if not m:
        return instrument, None
    root, mm, yy = m.group(1), m.group(2), m.group(3)
    exp = expected_contract(root, now)
    if not exp:
        return instrument, None
    exp_mm, exp_yy = exp.split("-")
    if (int(yy), int(mm)) >= (int(exp_yy), int(exp_mm)):
        return instrument, None
    return f"{root} {exp}", instrument


def load_front_months(cfg: dict):
    """Load the cached NT-reported front months from config."""
    global _front_months_date
    front_months.clear()
    raw = cfg.get("front_months")
    if isinstance(raw, dict):
        for root, tail in raw.items():
            r = str(root).strip().upper()
            t = str(tail).strip()
            if _catalog_row(r) and _MMYY_RE.fullmatch(t):
                front_months[r] = t
    _front_months_date = str(cfg.get("front_months_date") or "")


def _save_front_months():
    cfg = load_config()
    if front_months:
        cfg["front_months"] = dict(front_months)
        cfg["front_months_date"] = _front_months_date
    else:
        cfg.pop("front_months", None)
        cfg.pop("front_months_date", None)
    save_config(cfg)


def refresh_front_months() -> int:
    """Ask the AddOn for NT's current contract per catalog root (blocking).

    Sends every full and micro root in one command; the AddOn resolves
    each through Instrument.GetInstrument, which applies NT's rollover
    schedule. Returns how many roots were accepted. Older AddOn builds
    without the command refuse it — that leaves the calendar fallback in
    charge, logged once.
    """
    global _front_months_date
    roots: list[str] = []
    for row in FUTURES_CATALOG:
        roots.append(row[0])
        if row[2]:
            roots.append(row[2])
    ack = _bridge_roundtrip({"cmd": "front_months", "roots": ",".join(roots)},
                            timeout=6.0)
    if not ack:
        return 0
    if not ack.get("ack"):
        logger.info("FRONT MONTHS  AddOn refused the query — probably an "
                    "older build; calendar fallback stays in charge. "
                    "Recompile the AddOn to enable NT-synced rolls.")
        return 0
    months = ack.get("months")
    if not isinstance(months, dict):
        return 0
    updated = 0
    rolled: list[str] = []
    for root, tail in months.items():
        r = str(root).strip().upper()
        t = str(tail).strip()
        if not (_catalog_row(r) and _MMYY_RE.fullmatch(t)):
            continue
        if front_months.get(r) != t:
            rolled.append(f"{r} {front_months.get(r) or '—'}→{t}")
        front_months[r] = t
        updated += 1
    if updated:
        _front_months_date = datetime.now(ET).strftime("%Y-%m-%d")
        _save_front_months()
        logger.info(f"FRONT MONTHS  {updated} roots synced from NT"
                    + (f"  changed: {', '.join(rolled[:8])}" if rolled else ""))
    return updated


async def _front_months_sync():
    """Background bridge sync, at most one in flight, one attempt per day
    plus one per bridge (re)connect."""
    global _front_months_syncing, _front_months_attempt_date
    if _front_months_syncing:
        return
    _front_months_syncing = True
    _front_months_attempt_date = datetime.now(ET).strftime("%Y-%m-%d")
    try:
        await asyncio.to_thread(refresh_front_months)
    except Exception as exc:
        logger.warning(f"front-month sync failed: {exc}")
    finally:
        _front_months_syncing = False


DEFAULT_ACCOUNT = "Sim101"


def ask_account(cfg: dict) -> str:
    """Get NinjaTrader account name from config or prompt user.

    On first run, queries ATI for accounts or prompts manually. The
    recommended default is Sim101 — if present in the detected list it is
    pre-selected, and if ATI is unreachable pressing ENTER uses Sim101.
    """
    saved = cfg.get("account")
    if saved:
        return saved

    # First run — try to auto-detect accounts from NinjaTrader ATI
    accounts = query_nt_accounts(nt_port)
    if accounts:
        names = [a["name"] for a in accounts]
        default_idx = names.index(DEFAULT_ACCOUNT) + 1 if DEFAULT_ACCOUNT in names else 1
        default_name = accounts[default_idx - 1]["name"]
        print(Fore.CYAN + "\n┌─ NINJATRADER ACCOUNTS (auto-detected) ────────────────┐" + Style.RESET_ALL)
        for i, a in enumerate(accounts, 1):
            star = "★" if i == default_idx else " "
            line = f"{i}. {star} {a['name']}  (${a['cash']:,.2f})"
            print(Fore.CYAN + f"│  {line.ljust(54)}│" + Style.RESET_ALL)
        print(Fore.CYAN + "└───────────────────────────────────────────────────────┘" + Style.RESET_ALL)
        if len(accounts) == 1:
            print(Fore.GREEN + f"  ✔  Auto-selected: {default_name}" + Style.RESET_ALL)
            return default_name
        while True:
            choice = input(
                Fore.WHITE + f"  SELECT # [ENTER = {default_name}] ▸ " + Style.RESET_ALL
            ).strip()
            if not choice:
                return default_name
            if choice.isdigit() and 1 <= int(choice) <= len(accounts):
                return accounts[int(choice) - 1]["name"]
            print(Fore.YELLOW + f"  ⚠  Enter 1-{len(accounts)} or press ENTER for default." + Style.RESET_ALL)

    # ATI not available — manual entry with Sim101 default
    print(Fore.CYAN + "\n┌─ NINJATRADER ACCOUNT ─────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  NinjaTrader ATI unreachable — enter account name.    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  This replaces the Sim account in incoming signals.   │" + Style.RESET_ALL)
    default_line = f"Press ENTER for default: {DEFAULT_ACCOUNT}"
    print(Fore.CYAN + f"│  {default_line.ljust(54)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    if IS_WSL:
        tried = ", ".join(_nt_host_candidates()) or "127.0.0.1"
        print(Fore.YELLOW + f"  💡  WSL: tried {tried} on port {nt_port}." + Style.RESET_ALL)
        print(Fore.YELLOW + "      If NinjaTrader is running, allow inbound TCP " + str(nt_port) + " in" + Style.RESET_ALL)
        print(Fore.YELLOW + "      Windows Firewall for the WSL profile/subnet, then re-run setup." + Style.RESET_ALL)

    acct = input(Fore.WHITE + f"  ACCOUNT [{DEFAULT_ACCOUNT}] ▸ " + Style.RESET_ALL).strip()
    return acct or DEFAULT_ACCOUNT


def ask_token(cfg: dict, force: bool = False) -> str:
    """Get connection token from config or prompt user."""
    saved = cfg.get("token")
    if saved and not force:
        return saved

    if force:
        print(Fore.YELLOW + "\n  ⚠  Authentication failed. Please re-enter token." + Style.RESET_ALL)
    else:
        print(Fore.CYAN + "\n┌─ AUTHENTICATION ─────────────────────────────────────┐" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter your connection token.                        │" + Style.RESET_ALL)
        print(Fore.CYAN + "└──────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    while True:
        tk = input(Fore.WHITE + "  TOKEN ▸ " + Style.RESET_ALL).strip()
        if tk:
            return tk
        print(Fore.YELLOW + "  ⚠  Token cannot be empty." + Style.RESET_ALL)


def ask_server(cfg: dict) -> tuple[str, str]:
    """Get WebSocket server URL from config or prompt user.

    Returns (url, name) tuple.
    """
    saved = cfg.get("ws_host")
    if saved:
        # Find matching name in servers list
        for s in cfg.get("servers", []):
            if s.get("url") == saved:
                return saved, s.get("name", "Default")
        return saved, "Default"

    print(Fore.CYAN + "\n┌─ SERVER ─────────────────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Enter the WebSocket server URL.                     │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Example: ws://host:8420/ws  or  wss://host:8420/ws  │" + Style.RESET_ALL)
    print(Fore.CYAN + "└──────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    while True:
        url = input(Fore.WHITE + "  URL ▸ " + Style.RESET_ALL).strip()
        if url and (url.startswith("ws://") or url.startswith("wss://")):
            break
        print(Fore.YELLOW + "  ⚠  URL must start with ws:// or wss://" + Style.RESET_ALL)

    name = input(Fore.WHITE + "  NAME (optional) ▸ " + Style.RESET_ALL).strip()
    if not name:
        name = "Default"
    return url, name


# ---------- Cross-platform keyboard helpers ----------
_kb_stop = False  # Set True to unblock get_key threads

if os.name == "nt":  # Windows
    import msvcrt

    def get_key():
        """Non-blocking poll so the thread can exit when _kb_stop is set."""
        while not _kb_stop:
            if msvcrt.kbhit():
                ch = msvcrt.getch()
                if ch in (b"\x00", b"\xe0"):
                    # Special key — read scan code
                    if msvcrt.kbhit():
                        sc = msvcrt.getch()
                        if sc == b"H":
                            return "UP"
                        elif sc == b"P":
                            return "DOWN"
                        continue  # ignore other special keys
                return ch.decode("utf-8", errors="ignore")
            time.sleep(0.05)
        return ""

    def read_line_raw():
        return strip_terminal_input(input().strip())

else:  # POSIX
    import termios
    import tty
    import select

    _saved_termios = None

    def _ensure_raw():
        """Set terminal to raw mode once, saving original settings."""
        global _saved_termios
        fd = sys.stdin.fileno()
        if _saved_termios is None:
            _saved_termios = termios.tcgetattr(fd)
        tty.setraw(fd)

    def _restore_termios():
        """Restore original terminal settings."""
        if _saved_termios is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, _saved_termios)

    def get_key():
        fd = sys.stdin.fileno()
        _ensure_raw()
        try:
            while not _kb_stop:
                ready, _, _ = select.select([fd], [], [], 0.1)
                if ready:
                    ch = os.read(fd, 1).decode("utf-8", errors="ignore")
                    if ch == "\x1b":
                        # Escape sequence — bytes arrive together, no wait needed
                        ready2, _, _ = select.select([fd], [], [], 0.01)
                        if ready2:
                            ch2 = os.read(fd, 1).decode("utf-8", errors="ignore")
                            if ch2 == "[":
                                ready3, _, _ = select.select([fd], [], [], 0.01)
                                if ready3:
                                    ch3 = os.read(fd, 1).decode("utf-8", errors="ignore")
                                    if ch3 == "A":
                                        return "UP"
                                    elif ch3 == "B":
                                        return "DOWN"
                        return ch  # bare Escape
                    return ch
        finally:
            _restore_termios()
        return ""

    def read_line_raw():
        _restore_termios()  # Need cooked mode for readline
        try:
            line = sys.stdin.readline()
        finally:
            pass  # get_key will re-enter raw mode when called
        return strip_terminal_input(line.strip())


# ---------- Terminal helpers ----------
def term_width():
    return max(shutil.get_terminal_size().columns, 60)


def term_height():
    return max(shutil.get_terminal_size().lines, 10)


def clear():
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def move_to(row, col=1):
    sys.stdout.write(f"\033[{row};{col}H")
    sys.stdout.flush()


def hide_cursor():
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()


def show_cursor():
    sys.stdout.write("\033[?25h")
    sys.stdout.flush()


# ---------- Pinned controls bar ----------
_controls_pinned = False
_header_lines = 0  # Number of lines the pinned header occupies
_status_bar_row = 0  # Terminal row where the status bar starts
CONTROLS_TEXT = "O=ORDER  P=PAUSE  B=BAL  T=LIMITS  C=CLOSE  R=RECONN  S=SETUP  ⇧X=EXIT"


def _build_controls_line():
    """Build the controls bar text with account info on the right, truncated to terminal width."""
    left = f"  {CONTROLS_TEXT}"
    # Build account info
    acct_info = ""
    acct_info_colored = ""
    if active_account:
        # Leader label; copy-trading appends "+N" for the follower count.
        lead = active_account + (f"+{len(follower_accounts)}" if follower_accounts else "")
        start = session_start_balances.get(active_account)
        current = session_current_balances.get(active_account)
        # A quarantined feed (NT outage) freezes `current`; say so instead
        # of letting a frozen number read as live.
        stale = "  ⚠ stale" if active_account in _balance_suspect_since else ""
        stale_colored = (Fore.YELLOW + stale + Fore.CYAN + Style.DIM) if stale else ""
        if start is not None and current is not None:
            pnl = session_pnl(active_account, current)
            pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
            acct_info = f"{lead}: ${current:,.2f} (${pnl:+,.2f}){stale}"
            acct_info_colored = f"{lead}: ${current:,.2f} (" + pnl_color + f"${pnl:+,.2f}" + Fore.CYAN + Style.DIM + ")" + stale_colored
        elif current is not None:
            acct_info = f"{lead}: ${current:,.2f}{stale}"
            acct_info_colored = acct_info if not stale else f"{lead}: ${current:,.2f}" + stale_colored
        elif start is not None:
            acct_info = f"{lead}: ${start:,.2f}"
            acct_info_colored = acct_info
        else:
            acct_info = lead
            acct_info_colored = acct_info
    if not acct_info:
        acct_info = "NO ACCOUNT SET"
        acct_info_colored = acct_info
    width = term_width()
    # If controls + account info won't fit, truncate controls
    min_gap = 2
    avail_for_left = width - len(acct_info) - min_gap - 2  # 2 for trailing spaces
    if len(left) > avail_for_left > 20:
        left = left[:avail_for_left]
    gap = max(min_gap, width - len(left) - len(acct_info) - 2)
    return f"{Fore.CYAN}{Style.DIM}{left}{' ' * gap}{acct_info_colored}  {Style.RESET_ALL}"


def pin_layout():
    """Pin header at top and controls bar at bottom using scroll regions."""
    global _controls_pinned, _header_lines, _status_bar_row
    rows = term_height()

    # Draw header at the top (rows 1 through _header_lines)
    # Header content: compact banner + header box + blank + status_bar
    width = term_width()
    if pyfiglet:
        # Use compact font for pinned header to save vertical space
        for font in ["small", "standard", "big"]:
            try:
                # width=1000 prevents pyfiglet from wrapping the word onto
                # multiple "lines" when its default 80-col width is exceeded
                art = pyfiglet.figlet_format("VOIDORIGIN", font=font, width=1000)
                if max(len(l) for l in art.splitlines()) <= width:
                    break
            except Exception:
                continue
        else:
            art = "V O I D O R I G I N"
        banner_text = "\n".join(line.center(width) for line in art.rstrip("\n").splitlines())
    else:
        banner_text = "V O I D O R I G I N".center(width)
    header_text = build_header()
    pre_sb = banner_text + "\n" + header_text + "\n"
    _status_bar_row = len(pre_sb.splitlines()) + 1  # row where status bar starts
    sb = status_bar(get_session_status_text())
    header_content = pre_sb + "\n" + sb

    header_parts = header_content.splitlines()
    _header_lines = len(header_parts) + 1  # +1 for blank line after

    # Draw header
    sys.stdout.write("\033[1;1H")  # Move to top
    for i, line in enumerate(header_parts):
        sys.stdout.write(f"\033[{i + 1};1H\033[K{Fore.GREEN}{line}{Style.RESET_ALL}")

    # Set scroll region: after header, before footer
    scroll_top = _header_lines + 1
    scroll_bottom = rows - 1
    sys.stdout.write(f"\033[{scroll_top};{scroll_bottom}r")

    # Draw controls bar on last row
    sys.stdout.write(f"\033[{rows};1H")
    sys.stdout.write(f"\033[K{_build_controls_line()}")

    # Move cursor to start of scroll region
    sys.stdout.write(f"\033[{scroll_top};1H")
    sys.stdout.flush()
    _controls_pinned = True
    _dash_redraw_all()


def unpin_layout():
    """Remove pinned header/footer and restore normal scrolling."""
    global _controls_pinned
    sys.stdout.write("\033[r")
    sys.stdout.flush()
    _controls_pinned = False


def refresh_controls():
    """Redraw the pinned controls bar without disrupting scroll region."""
    if not _controls_pinned:
        return
    rows = term_height()
    # Save cursor, jump outside scroll region to last row, redraw, restore
    sys.stdout.write("\033[s")                          # save cursor
    sys.stdout.write(f"\033[{rows};1H")                 # move to last row
    sys.stdout.write(f"\033[K{_build_controls_line()}")  # clear line + draw
    sys.stdout.write("\033[u")                          # restore cursor
    sys.stdout.flush()


def refresh_header_status():
    """Redraw the status bar in the pinned header without touching scroll content."""
    if not _controls_pinned or _status_bar_row == 0:
        return
    sb_lines = status_bar(get_session_status_text()).splitlines()
    sys.stdout.write("\033[s")  # save cursor
    for i, line in enumerate(sb_lines):
        sys.stdout.write(f"\033[{_status_bar_row + i};1H\033[K{Fore.GREEN}{line}{Style.RESET_ALL}")
    sys.stdout.write("\033[u")  # restore cursor
    sys.stdout.flush()


# ---------- Dashboard layout ----------
# Fixed-row dashboard within the scroll region.  Row offsets are relative
# to scroll_top (= _header_lines + 1).  Content updates in-place via
# cursor positioning — no scrolling.
DASH_ROW_HEARTBEAT = 0       # connection / heartbeat status
DASH_ROW_SEP1 = 1            # ─── separator ───
DASH_SIGNAL_START = 2        # first signal row (newest at top)
DASH_SIGNAL_COUNT = 5        # visible signal slots
DASH_ROW_SEP2 = 7            # ─── separator ───  (SIGNAL_START + COUNT)
DASH_ROW_MOTD = 8            # server MOTD / maintenance notices
DASH_ROW_ALERT = 9           # system alerts / fill confirmations

_signal_buffer: deque[str] = deque(maxlen=DASH_SIGNAL_COUNT)
_motd_text = ""
_heartbeat_text = ""
_server_name = ""
_menu_active = False

# Alert row model: alerts are one-shot EVENTS, not states. Each is stamped
# with the time it happened so it stays truthful after the moment passes;
# ongoing conditions (connected/reconnecting/paused/stopped) belong on the
# heartbeat row and header status bar, which are actively maintained.
ALERT_EVENT = "event"  # default: one-shot occurrences (errors, fills, user actions)
ALERT_CONN = "conn"    # connection lifecycle — cleared as a group on reconnect
ALERT_TTL = 300        # seconds a non-sticky alert lives; swept on server heartbeats
_alert_text = ""
_alert_kind = ""       # kind of the alert currently shown ("" = row empty)
_alert_sticky = False  # sticky alerts (lockouts, setup gaps) never auto-expire
_alert_ts = 0.0        # when the current alert was posted


def _dash_write(row_offset: int, text: str):
    """Write text to a fixed dashboard row (no scroll, no newline)."""
    if _menu_active or not _controls_pinned:
        return
    abs_row = _header_lines + 1 + row_offset
    sys.stdout.write(f"\033[s\033[{abs_row};1H\033[K{text}\033[u")
    sys.stdout.flush()


def _dash_separator(row_offset: int):
    """Draw a dim separator line."""
    width = term_width()
    _dash_write(row_offset,
                Fore.CYAN + Style.DIM + f"  {'─' * (width - 4)}" + Style.RESET_ALL)


def _dash_add_signal(text: str):
    """Add a formatted signal line to the rolling buffer and redraw."""
    _signal_buffer.append(text)
    _web_note("signal", text)
    _redraw_signals()


def _redraw_signals():
    """Redraw signal rows (newest at top)."""
    buf = list(_signal_buffer)
    for i in range(DASH_SIGNAL_COUNT):
        idx = len(buf) - 1 - i
        _dash_write(DASH_SIGNAL_START + i,
                    buf[idx] if 0 <= idx < len(buf) else "")


def _dash_set_heartbeat(text: str):
    """Update the heartbeat / connection status line."""
    global _heartbeat_text
    _heartbeat_text = text
    _dash_write(DASH_ROW_HEARTBEAT, text)


def _dash_set_motd(text: str):
    """Update the MOTD / server-message line."""
    global _motd_text
    _motd_text = text
    _dash_write(DASH_ROW_MOTD, text)


def _dash_set_alert(text: str, kind: str = ALERT_EVENT, sticky: bool = False,
                    stamp: bool = True):
    """Update the alert / status line.

    Non-empty alerts get a dim [HH:MM:SS] stamp (unless stamp=False for
    animation frames) and expire after ALERT_TTL unless sticky.
    """
    global _alert_text, _alert_kind, _alert_sticky, _alert_ts
    if text and stamp:
        _web_note("alert", text)
        text = f"  {Style.DIM}[{time.strftime('%H:%M:%S')}]{Style.RESET_ALL}" + text
    _alert_text = text
    _alert_kind = kind if text else ""
    _alert_sticky = sticky and bool(text)
    _alert_ts = time.time()
    _dash_write(DASH_ROW_ALERT, text)


def _dash_clear_alert(kind: str | None = None):
    """Clear the alert row — with kind given, only if the current alert is that kind."""
    if kind is not None and _alert_kind != kind:
        return
    _dash_set_alert("")


def _dash_expire_alert():
    """Drop a stale non-sticky alert; called on each server heartbeat."""
    if _alert_text and not _alert_sticky and time.time() - _alert_ts > ALERT_TTL:
        _dash_set_alert("")


def _dash_redraw_all():
    """Redraw entire dashboard (after menu exit or resize)."""
    _dash_write(DASH_ROW_HEARTBEAT, _heartbeat_text)
    _dash_separator(DASH_ROW_SEP1)
    _redraw_signals()
    _dash_separator(DASH_ROW_SEP2)
    _dash_write(DASH_ROW_MOTD, _motd_text)
    _dash_write(DASH_ROW_ALERT, _alert_text)


def _dash_enter_menu():
    """Clear dashboard for menu overlay."""
    global _menu_active
    _menu_active = True
    if _controls_pinned:
        rows = term_height()
        scroll_top = _header_lines + 1
        scroll_bottom = rows - 1
        for r in range(scroll_top, scroll_bottom + 1):
            sys.stdout.write(f"\033[{r};1H\033[K")
        move_to(scroll_top)
        sys.stdout.flush()


def _dash_exit_menu():
    """Restore dashboard after menu overlay."""
    global _menu_active
    _menu_active = False
    if _controls_pinned:
        rows = term_height()
        scroll_top = _header_lines + 1
        scroll_bottom = rows - 1
        for r in range(scroll_top, scroll_bottom + 1):
            sys.stdout.write(f"\033[{r};1H\033[K")
        sys.stdout.flush()
    _dash_redraw_all()


# ---------- Dynamic ASCII banner ----------
def build_banner():
    width = term_width()
    if pyfiglet:
        for font in ["block", "banner3-D", "banner3", "doom", "larry3d", "big", "standard", "small"]:
            try:
                art = pyfiglet.figlet_format("VOIDORIGIN", font=font, width=1000)
                if max(len(l) for l in art.splitlines()) <= width:
                    break
            except Exception:
                continue
        else:
            art = pyfiglet.figlet_format("VOIDORIGIN", font="small", width=1000)
    else:
        art = "V O I D O R I G I N"
    return "\n".join(line.center(width) for line in art.splitlines())


# ---------- Box helpers ----------
def hline(width, left="╔", mid="═", right="╗"):
    return f"{left}{mid * (width - 2)}{right}"


def row(text, width, pad="║"):
    inner = width - 2
    return f"{pad}{text[:inner].center(inner)}{pad}"


def build_header():
    width = term_width()
    subtitle = "LINK ESTABLISHED  ·  SIGNAL BUS ACTIVE  ·  NODE AUTHORIZED"

    lines = [
        hline(width, "╔", "═", "╗"),
        row(f"SOCKET TRADER  v{__version__}", width),
        row(subtitle, width),
        hline(width, "╚", "═", "╝"),
    ]
    return "\n".join(lines)


# ---------- ANSI helpers ----------
_ANSI_ESCAPE = re.compile(r'\033\[[0-9;]*m')


def visible_len(s: str) -> int:
    return len(_ANSI_ESCAPE.sub('', s))


def status_bar(text):
    width = term_width()
    inner = width - 2
    dir_indicator = Fore.GREEN + "● TRADE READY" + Fore.CYAN if is_trade_ready() else Fore.RED + "● NOT READY" + Fore.CYAN
    content = f"{text}  ·  {dir_indicator}"
    if micro_mode:
        content += f"  ·  {Fore.LIGHTMAGENTA_EX}◆ MICROS{Fore.CYAN}"
    if profiles_active():
        content += f"  ·  {Fore.LIGHTCYAN_EX}◆ PROFILES{Fore.CYAN}"
    vis = visible_len(content)
    total_pad = max(0, inner - vis)
    left_pad = total_pad // 2
    right_pad = total_pad - left_pad
    return (
        f"{Fore.CYAN}┌{'─'*(width-2)}┐\n"
        f"│{' '*left_pad}{content}{' '*right_pad}{Fore.CYAN}│\n"
        f"└{'─'*(width-2)}┘{Style.RESET_ALL}"
    )


# ---------- Boot / scanline ----------
GLITCH_CHARS = "▓▒░█▄▀■□▪▫◆◇○●"


def glitch_line(text, intensity=0.15):
    return "".join(random.choice(GLITCH_CHARS) if ch.strip() and random.random() < intensity else ch for ch in text)


async def boot_sequence():
    hide_cursor()
    clear()
    # Animated banner
    banner_lines = build_banner().splitlines()
    for line in banner_lines:
        sys.stdout.write(Fore.GREEN + Style.DIM + glitch_line(line, 0.25) + "\n" + Style.RESET_ALL)
        sys.stdout.flush()
        await asyncio.sleep(0.06)
    await asyncio.sleep(0.5)
    # Clear and pin the full layout (header + scroll region + footer)
    clear()
    pin_layout()
    # Reset cursor to column 0 — Windows terminals can drift after centered text
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()
    show_cursor()


# ---------- Animated signal pulse ----------
async def signal_pulse(label="SIGNAL RECEIVED"):
    _dash_set_alert(Fore.GREEN + f"  ● {label}" + Style.RESET_ALL, stamp=False)
    await asyncio.sleep(0.4)
    _dash_set_alert("")


# ---------- Pause indicator ----------
PAUSE_FRAMES = [
    "⏸  SIGNAL OUTPUT PAUSED    ",
    "⏸  SIGNAL OUTPUT PAUSED  · ",
    "⏸  SIGNAL OUTPUT PAUSED  ··",
    "⏸  SIGNAL OUTPUT PAUSED  ··· ",
]


async def pause_indicator():
    i = 0
    while not shutdown.is_set():
        if paused and not awaiting_user_input:
            # Use MOTD line so it doesn't overwrite alerts
            _dash_set_motd(
                Fore.YELLOW + "  " + PAUSE_FRAMES[i % len(PAUSE_FRAMES)] + Style.RESET_ALL)
            i += 1
            await asyncio.sleep(0.35)
        else:
            if i > 0:
                _dash_set_motd("")  # clear pause text on resume
            i = 0
            await asyncio.sleep(0.1)


# ---------- Directory prompt ----------
async def prompt_directory():
    global output_directory, awaiting_directory_input, awaiting_user_input
    awaiting_directory_input = True
    awaiting_user_input = True
    show_cursor()
    print(Fore.CYAN + "\n┌─ SET OUTPUT DIRECTORY ──────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Paste the directory path where files will go.  │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Press ENTER to keep current.                    │" + Style.RESET_ALL)
    if output_directory:
        print(Fore.CYAN + f"│  Current: {output_directory[:39].ljust(39)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "└─────────────────────────────────────────────────┘" + Style.RESET_ALL)
    sys.stdout.write(Fore.WHITE + "  PATH ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw_path = await asyncio.to_thread(read_line_raw)

    if raw_path == "":
        print(Fore.YELLOW + "  ↩  No change — keeping current directory." + Style.RESET_ALL)
    else:
        path = Path(raw_path.strip('"').strip("'"))
        if path.is_dir():
            output_directory = str(path.resolve())
            print(Fore.GREEN + f"  ✔  Output directory set → {output_directory}" + Style.RESET_ALL)
        else:
            sys.stdout.write(Fore.YELLOW + f"  ⚠  Directory does not exist. Create? [y/N] " + Style.RESET_ALL)
            sys.stdout.flush()
            answer = await asyncio.to_thread(read_line_raw)
            if answer.lower() == "y":
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    output_directory = str(path.resolve())
                    print(Fore.GREEN + f"  ✔  Created & set → {output_directory}" + Style.RESET_ALL)
                except Exception as exc:
                    print(Fore.RED + f"  ✖  Could not create directory: {exc}" + Style.RESET_ALL)
            else:
                print(Fore.YELLOW + "  ↩  Directory unchanged." + Style.RESET_ALL)

    # Save updated directory to config
    if output_directory:
        cfg = load_config()
        cfg["output_directory"] = output_directory
        save_config(cfg)

    awaiting_directory_input = False
    awaiting_user_input = False


# ---------- Copy-trade account selection ----------
def _account_option_line(index: int, account: dict, leader: str | None, followers: list[str],
                         robins: list[str] | None = None) -> str:
    name = account["name"]
    if name == leader:
        marker = " ◀ LEADER"
    elif name in followers:
        marker = " ＋ FOLLOWER"
    elif robins and name in robins:
        marker = " ⟳ ROBIN"
    else:
        marker = ""
    return f"{index}. {name}  (${account['cash']:,.2f}){marker}"


def _account_menu_rows(accounts: list[dict], leader: str | None, followers: list[str],
                       robins: list[str] | None = None) -> tuple[list[str], int]:
    """Return printable account rows and content width for the account picker."""
    rows = [_account_option_line(i, a, leader, followers, robins) for i, a in enumerate(accounts, 1)]
    if not rows:
        return [], 49

    available = max(term_width() - 4, 49)  # left border + two-space indent + right border
    if len(rows) <= 9 or available < 74:
        width = min(max(49, max(len(r) for r in rows)), available)
        return [r[:width] for r in rows], width

    gap = "  "
    col_width = max(34, min(44, (available - len(gap)) // 2))
    split_at = (len(rows) + 1) // 2
    rendered: list[str] = []
    for i in range(split_at):
        left = rows[i][:col_width].ljust(col_width)
        right = rows[i + split_at][:col_width] if i + split_at < len(rows) else ""
        rendered.append(f"{left}{gap}{right[:col_width]}")
    return rendered, (col_width * 2) + len(gap)


def _print_account_menu(accounts: list[dict], leader: str | None, followers: list[str],
                        robins: list[str] | None = None) -> None:
    rows, width = _account_menu_rows(accounts, leader, followers, robins)
    title = "─ ACCOUNTS — LEADER · FOLLOWERS · ROUND-ROBIN "
    top = f"┌{title}{'─' * max(0, width + 2 - len(title))}┐"
    print(Fore.CYAN + "\n" + top + Style.RESET_ALL)
    if rows:
        for line in rows:
            print(Fore.CYAN + f"│  {line[:width].ljust(width)}│" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + f"│  {'NinjaTrader ATI unreachable — enter names.'.ljust(width)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {'Leader + followers copy every signal.'.ljust(width)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {'Round-robin: each entry rotates to ONE pool account.'.ljust(width)[:width]}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {'Pick with numbers/names, the word all, or ENTER=none.'.ljust(width)[:width]}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"└{'─' * (width + 2)}┘" + Style.RESET_ALL)


def _parse_follower_tokens(raw: str, names: list[str], leader: str) -> list[str]:
    """Parse a follower selection string into account names.

    Accepts space/comma-separated 1-based indexes into `names`, the word
    'all' (every account except the leader), or literal account names.
    The leader is always excluded and the result is de-duplicated in the
    order given.
    """
    raw = raw.strip()
    if not raw:
        return []
    if raw.lower() == "all":
        return [n for n in names if n != leader]
    picked: list[str] = []
    seen: set[str] = set()
    for tok in raw.replace(",", " ").split():
        name = None
        if tok.isdigit() and 1 <= int(tok) <= len(names):
            name = names[int(tok) - 1]
        elif tok in names:
            name = tok
        else:
            name = tok  # allow a manually-typed account name not in the ATI list
        if name and name != leader and name not in seen:
            seen.add(name)
            picked.append(name)
    return picked


async def prompt_accounts():
    """Select the LEADER, the FOLLOWERS that copy it, and the ROUND-ROBIN pool.

    Leader + followers fire on every signal (classic copy trading). The
    round-robin pool receives each ENTRY on exactly one member, rotating
    randomly with no repeats until the whole pool has traded a round; exits
    fan to the whole pool. An account is a follower or in the pool — never
    both. ENTER everywhere keeps the classic single-account flow unchanged.
    """
    global active_account, follower_accounts, roundrobin_accounts, awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
    names = [a["name"] for a in accounts]

    _print_account_menu(accounts, active_account, follower_accounts, roundrobin_accounts)

    # --- Leader ---
    sys.stdout.write(Fore.WHITE + f"  LEADER [{active_account or 'none'}] ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw = (await asyncio.to_thread(read_line_raw)).strip()
    if raw:
        if raw.isdigit() and names and 1 <= int(raw) <= len(names):
            active_account = names[int(raw) - 1]
        else:
            active_account = raw

    # --- Followers (copy trade) ---
    sys.stdout.write(
        Fore.WHITE + "  FOLLOWERS — copy trade (numbers/names, 'all', ENTER=none) ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw_f = await asyncio.to_thread(read_line_raw)
    follower_accounts = _parse_follower_tokens(raw_f, names, active_account)

    # --- Round-robin pool ---
    sys.stdout.write(
        Fore.WHITE + "  ROUND-ROBIN pool (numbers/names, 'all', ENTER=none) ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw_rr = await asyncio.to_thread(read_line_raw)
    rr_tokens = _parse_follower_tokens(raw_rr, names, active_account)
    overlap = [a for a in rr_tokens if a in follower_accounts]
    roundrobin_accounts = sanitize_roundrobin(rr_tokens, active_account, follower_accounts)
    if overlap and raw_rr.strip().lower() != "all":
        print(Fore.YELLOW + "  ⚠  Copy-trade and round-robin are exclusive — kept as "
              f"followers: {', '.join(overlap)}" + Style.RESET_ALL)
    _rr_reset_rotation()

    cfg = load_config()
    cfg["account"] = active_account
    cfg["follower_accounts"] = follower_accounts
    cfg["roundrobin_accounts"] = roundrobin_accounts
    save_config(cfg)

    if follower_accounts or roundrobin_accounts:
        parts_desc = []
        if follower_accounts:
            parts_desc.append(f"{len(follower_accounts)} copy ({', '.join(follower_accounts)})")
        if roundrobin_accounts:
            parts_desc.append(f"{len(roundrobin_accounts)} round-robin ({', '.join(roundrobin_accounts)})")
        print(Fore.GREEN + f"  ✔  Leader {active_account}  +  {'  ·  '.join(parts_desc)}"
              + Style.RESET_ALL)
    else:
        print(Fore.GREEN + f"  ✔  Single account → {active_account}" + Style.RESET_ALL)
    logger.info(f"ACCOUNTS SET  leader={active_account}  followers={follower_accounts}  "
                f"roundrobin={roundrobin_accounts}")
    print()
    awaiting_user_input = False


# ---------- Port prompt ----------
async def prompt_port():
    global nt_port, awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    print(Fore.CYAN + "\n┌─ NINJATRADER AT INTERFACE PORT ───────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Enter NinjaTrader AT Interface server port.      │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  Current: {str(nt_port).ljust(39)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────┘" + Style.RESET_ALL)
    sys.stdout.write(Fore.WHITE + "  PORT ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw = await asyncio.to_thread(read_line_raw)

    if raw == "":
        print(Fore.YELLOW + "  ↩  No change — keeping current port." + Style.RESET_ALL)
    else:
        try:
            port = int(raw.strip())
            if 1 <= port <= 65535:
                nt_port = port
                cfg = load_config()
                cfg["nt_port"] = nt_port
                save_config(cfg)
                invalidate_nt_host_cache()
                print(Fore.GREEN + f"  ✔  Port set → {nt_port}" + Style.RESET_ALL)
            else:
                print(Fore.RED + "  ✖  Port must be between 1 and 65535." + Style.RESET_ALL)
        except ValueError:
            print(Fore.RED + "  ✖  Invalid port number." + Style.RESET_ALL)

    print()
    awaiting_user_input = False


# ---------- ATM Strategy prompt ----------
def _strategy_option_line(index: int, name: str, current: str) -> str:
    marker = " ◀" if name == current else ""
    return f"{index}. {name}{marker}"


def _strategy_page_size() -> int:
    reserved = _header_lines + 10 if _controls_pinned else 10
    return max(6, term_height() - reserved)


def _strategy_menu_rows(available: list[str], current: str, page: int = 0) -> tuple[list[str], int, int, int]:
    """Return printable strategy rows, width, current page, and page count."""
    if not available:
        return [], 49, 0, 1

    available_width = max(term_width() - 4, 49)
    max_index_len = len(str(len(available))) + 2  # "83. "
    longest_name = max([len(n) for n in available] + [len(current)])
    min_col_width = max(18, min(34, max_index_len + min(longest_name, 24) + 2))
    columns = max(1, min(4, (available_width + 2) // (min_col_width + 2)))
    col_width = min(34, (available_width - (columns - 1) * 2) // columns)
    page_rows = _strategy_page_size()
    page_size = max(1, page_rows * columns)
    page_count = max(1, (len(available) + page_size - 1) // page_size)
    page = max(0, min(page, page_count - 1))

    start = page * page_size
    items = [
        _strategy_option_line(start + offset + 1, name, current)[:col_width]
        for offset, name in enumerate(available[start:start + page_size])
    ]
    rendered: list[str] = []
    for row_idx in range(page_rows):
        cells = []
        for col_idx in range(columns):
            item_idx = (col_idx * page_rows) + row_idx
            if item_idx < len(items):
                cells.append(items[item_idx].ljust(col_width))
        if cells:
            rendered.append("  ".join(cells).rstrip())
    width = max(49, min(available_width, (col_width * columns) + ((columns - 1) * 2)))
    return rendered, width, page, page_count


def _print_strategy_menu(available: list[str], current: str, follow_mode: bool, page: int = 0) -> tuple[int, int]:
    rows, width, page, page_count = _strategy_menu_rows(available, current, page)
    mode_label = "FOLLOW PUBLISHER" if follow_mode else "LOCKED (override)"
    title = "─ ATM STRATEGY TEMPLATE "
    top = f"┌{title}{'─' * max(0, width + 2 - len(title))}┐"
    print(Fore.CYAN + "\n" + top + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {('Mode: ' + mode_label).ljust(width)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {('Fallback: ' + current).ljust(width)[:width]}│" + Style.RESET_ALL)
    if available:
        print(Fore.CYAN + f"│  {('Templates: ' + str(len(available)) + ' · Page ' + str(page + 1) + '/' + str(page_count)).ljust(width)}│" + Style.RESET_ALL)
        for line in rows:
            print(Fore.CYAN + f"│  {line[:width].ljust(width)}│" + Style.RESET_ALL)
        hint = "#/name=set · N/P=page · T=mode · ENTER=keep"
        print(Fore.CYAN + f"│  {hint.ljust(width)[:width]}│" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + f"│  {'No templates found in AtmStrategy directory.'.ljust(width)}│" + Style.RESET_ALL)
        print(Fore.CYAN + f"│  {'Type a strategy name manually.'.ljust(width)}│" + Style.RESET_ALL)
        print(Fore.CYAN + f"│  {'T=mode · ENTER=keep'.ljust(width)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"└{'─' * (width + 2)}┘" + Style.RESET_ALL)
    return page, page_count


async def prompt_strategy():
    global atm_strategy, follow_publisher_strategy, awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    available = list_atm_strategies()

    cfg = load_config()
    page = 0
    while True:
        page, page_count = _print_strategy_menu(available, atm_strategy, follow_publisher_strategy, page)
        sys.stdout.write(Fore.WHITE + "  STRATEGY ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw = await asyncio.to_thread(read_line_raw)
        choice = raw.strip()

        if choice.lower() in {"n", "next"} and page_count > 1:
            page = (page + 1) % page_count
            _dash_enter_menu()
            continue
        if choice.lower() in {"p", "prev", "previous"} and page_count > 1:
            page = (page - 1) % page_count
            _dash_enter_menu()
            continue
        break

    if choice == "":
        print(Fore.YELLOW + "  ↩  No change — keeping current strategy." + Style.RESET_ALL)
    elif choice.lower() == "q":
        print(Fore.YELLOW + "  ↩  Cancelled." + Style.RESET_ALL)
    elif choice.lower() == "t":
        follow_publisher_strategy = not follow_publisher_strategy
        cfg["follow_publisher_strategy"] = follow_publisher_strategy
        save_config(cfg)
        new_label = "FOLLOW PUBLISHER" if follow_publisher_strategy else "LOCKED (override)"
        _dash_set_alert(Fore.GREEN + f"  ✔  Strategy mode → {new_label}  ·  Fallback: {atm_strategy}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Mode → {new_label}  ·  Fallback: {atm_strategy}" + Style.RESET_ALL)
        logger.info(f"STRATEGY MODE  follow_publisher={follow_publisher_strategy}  fallback={atm_strategy}")
    elif available and choice.isdigit() and 1 <= int(choice) <= len(available):
        atm_strategy = available[int(choice) - 1]
        cfg["atm_strategy"] = atm_strategy
        save_config(cfg)
        print(Fore.GREEN + f"  ✔  Fallback strategy set → {atm_strategy}" + Style.RESET_ALL)
    else:
        if validate_strategy(choice):
            atm_strategy = choice
            cfg["atm_strategy"] = atm_strategy
            save_config(cfg)
            print(Fore.GREEN + f"  ✔  Fallback strategy set → {atm_strategy}" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"  ✖  '{choice}' not found in templates/AtmStrategy/." + Style.RESET_ALL)
            print(Fore.YELLOW + f"  ↩  Keeping current: {atm_strategy}" + Style.RESET_ALL)

    print()
    awaiting_user_input = False


# ---------- Per-account profiles editor ----------
_PROF_INNER = 56  # inner width of profile editor boxes

_SIZE_LABELS = {"inherit": "inherit global micros toggle",
                "micros": "micros", "full": "full-size"}


def _prof_box(title: str, lines: list[str], footer: list[str] | None = None):
    """Draw a profile-editor box in the house style."""
    top = f"┌─ {title} "
    print(Fore.CYAN + "\n\r\033[K" + top + "─" * max(0, _PROF_INNER + 3 - len(top)) + "┐" + Style.RESET_ALL)
    for line in lines:
        print(Fore.CYAN + f"\r\033[K│  {line[:_PROF_INNER].ljust(_PROF_INNER)}│" + Style.RESET_ALL)
    for line in footer or []:
        print(Fore.CYAN + "\r\033[K│  " + Style.DIM
              + line[:_PROF_INNER].ljust(_PROF_INNER) + Style.NORMAL + "│" + Style.RESET_ALL)
    print(Fore.CYAN + "\r\033[K└" + "─" * (_PROF_INNER + 2) + "┘" + Style.RESET_ALL)


async def _ask_line(prompt: str) -> str:
    sys.stdout.write(Fore.WHITE + f"  {prompt} ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    return (await asyncio.to_thread(read_line_raw)).strip()


async def _ask_int(prompt: str, current: int, lo: int, hi: int) -> int:
    """Integer prompt with ENTER=keep and range clamping."""
    raw = await _ask_line(f"{prompt} (current: {current})")
    if not raw:
        return current
    try:
        val = int(float(raw))
    except ValueError:
        print(Fore.YELLOW + "  ⚠  Not a number — keeping current." + Style.RESET_ALL)
        return current
    if val < lo or val > hi:
        clamped = max(lo, min(val, hi))
        print(Fore.YELLOW + f"  ⚠  Clamped to {clamped} (allowed {lo}-{hi})." + Style.RESET_ALL)
        return clamped
    return val


async def _edit_rule_scope(rule: dict):
    """Prompt for a scoped rule's symbol / publisher-strategy filters."""
    raw = await _ask_line("SYMBOLS it applies to, e.g. 'NQ ES' (ENTER = any)")
    symbols = [t.upper() for t in raw.replace(",", " ").split() if t.strip()]
    if symbols:
        rule["symbols"] = symbols
    else:
        rule.pop("symbols", None)
    raw = await _ask_line("PUBLISHER STRATEGIES, e.g. 'NQ_Med' (ENTER = any)")
    strategies = [t for t in raw.replace(",", " ").split() if t.strip()]
    if strategies:
        rule["strategies"] = strategies
    else:
        rule.pop("strategies", None)


async def _edit_ai_gate(rule: dict):
    """Configure (or clear) a rule's AI entry gate."""
    current = rule.get("ai")
    cur_label = current["provider"] if current else "off"
    raw = (await _ask_line(
        f"AI PROVIDER [off/anthropic/openai/ollama/custom] (current: {cur_label})")).lower()
    if not raw:
        return
    if raw in ("off", "none", "0"):
        rule["ai"] = None
        print(Fore.GREEN + "  ✔  AI gate off." + Style.RESET_ALL)
        return
    if raw not in AI_PROVIDERS:
        print(Fore.YELLOW + "  ⚠  Unknown provider — unchanged." + Style.RESET_ALL)
        return
    defaults = AI_PROVIDERS[raw]
    base = current if current and current.get("provider") == raw else {}
    model = (await _ask_line(
        f"MODEL (ENTER = {base.get('model') or defaults['model'] or 'required'})")
        or base.get("model") or defaults["model"])
    endpoint = base.get("endpoint") or defaults["endpoint"]
    if raw in ("ollama", "custom"):
        endpoint = (await _ask_line(
            f"ENDPOINT URL (ENTER = {endpoint or 'required'})") or endpoint)
    key_env = base.get("api_key_env") or defaults["key_env"]
    if raw != "ollama":
        key_env = (await _ask_line(
            f"API KEY ENV VAR (ENTER = {key_env or 'none'})") or key_env)
    timeout_ms = await _ask_int("TIMEOUT ms", int(base.get("timeout_ms", 8000)), 1000, 60_000)
    on_error_raw = (await _ask_line(
        f"IF AI UNREACHABLE [skip/allow] (current: {base.get('on_error', 'skip')})")).lower()
    if on_error_raw.startswith("a"):
        on_error = "allow"
    elif on_error_raw.startswith("s"):
        on_error = "skip"
    else:
        on_error = base.get("on_error", "skip")
    instructions = (await _ask_line("EXTRA GUIDANCE for the model (ENTER = keep/none)")
                    or base.get("instructions", ""))
    ai_cfg = _coerce_ai({"provider": raw, "model": model, "endpoint": endpoint,
                         "api_key_env": key_env, "timeout_ms": timeout_ms,
                         "on_error": on_error, "instructions": instructions})
    if not ai_cfg or (raw == "custom" and not ai_cfg["endpoint"]):
        print(Fore.YELLOW + "  ⚠  Incomplete AI config — gate unchanged." + Style.RESET_ALL)
        return
    rule["ai"] = ai_cfg
    if raw == "anthropic" and anthropic is None:
        print(Fore.YELLOW + "  ⚠  anthropic SDK not installed — run: pip install anthropic" + Style.RESET_ALL)
    if ai_cfg["api_key_env"] and not os.environ.get(ai_cfg["api_key_env"]):
        print(Fore.YELLOW + f"  ⚠  ${ai_cfg['api_key_env']} is not set in this environment." + Style.RESET_ALL)
    print(Fore.GREEN + f"  ✔  AI gate → {raw} · {ai_cfg['model'] or 'default model'} · "
          f"on error: {ai_cfg['on_error']}" + Style.RESET_ALL)


async def _edit_rule_menu(rule: dict, title: str, scoped: bool):
    """Edit one rule dict in place. Keys absent from `rule` inherit (from
    the account default, then app defaults) and show without a '*'."""
    field_keys = {1: ("enabled",), 2: ("size",),
                  3: ("qty_mode", "qty_value", "max_contracts"),
                  4: ("direction",), 5: ("delay_ms", "delay_jitter_ms"),
                  6: ("stagger_entries", "stagger_interval_ms"),
                  7: ("atm",), 8: ("ai",)}
    while True:
        eff = {**DEFAULT_RULE, **{k: v for k, v in rule.items() if k in DEFAULT_RULE}}

        def mark(*keys):
            return "*" if any(k in rule for k in keys) else " "

        if eff["delay_ms"] or eff["delay_jitter_ms"]:
            delay_label = f"{eff['delay_ms']}ms" + (
                f" + 0..{eff['delay_jitter_ms']}ms jitter" if eff["delay_jitter_ms"] else "")
        else:
            delay_label = "off"
        stagger_label = ("off" if eff["stagger_entries"] <= 1 else
                         f"{eff['stagger_entries']} tranches × {eff['stagger_interval_ms']}ms")
        qty_label = _qty_label(eff) + (
            f" · cap {eff['max_contracts']}" if eff["max_contracts"] else "")
        ai = eff["ai"]
        ai_label = "off" if not ai else f"{ai['provider']} · {ai['model'] or 'default'}"
        lines = [
            f"1.{mark('enabled')} Entries       {'on' if eff['enabled'] else 'OFF — exits only'}",
            f"2.{mark('size')} Size          {_SIZE_LABELS[eff['size']]}",
            f"3.{mark('qty_mode', 'qty_value', 'max_contracts')} Contracts     {qty_label}",
            f"4.{mark('direction')} Direction     {'INVERTED' if eff['direction'] == 'invert' else 'normal'}",
            f"5.{mark('delay_ms', 'delay_jitter_ms')} Entry delay   {delay_label}",
            f"6.{mark('stagger_entries', 'stagger_interval_ms')} Stagger       {stagger_label}",
            f"7.{mark('atm')} ATM override  {eff['atm'] or 'inherit'}",
            f"8.{mark('ai')} AI gate       {ai_label}",
        ]
        if scoped:
            scope = (f"{', '.join(rule.get('symbols', [])) or 'any symbol'} · "
                     f"{', '.join(rule.get('strategies', [])) or 'any strategy'}")
            lines.append(f"9.  Scope         {scope[:40]}")
        _prof_box(title, lines, [
            "* = set here · number = edit · -number = reset field",
            "Exits are never blocked, delayed, or AI-gated.",
            "ENTER = done",
        ])
        raw = (await _ask_line("FIELD")).lower()
        if raw in ("", "q"):
            return
        reset = raw.startswith("-")
        num = raw.lstrip("-")
        if not num.isdigit():
            continue
        n = int(num)
        if reset:
            for key in field_keys.get(n, ()):
                rule.pop(key, None)
            continue
        if n == 1:
            rule["enabled"] = not eff["enabled"]
            if not rule["enabled"]:
                print(Fore.YELLOW + "  ⚠  New entries blocked for this scope — exits still flow."
                      + Style.RESET_ALL)
        elif n == 2:
            raw_s = (await _ask_line("SIZE [i]nherit / [m]icros / [f]ull")).lower()
            if raw_s.startswith("i"):
                rule["size"] = "inherit"
            elif raw_s.startswith("m"):
                rule["size"] = "micros"
            elif raw_s.startswith("f"):
                rule["size"] = "full"
            elif raw_s:
                print(Fore.YELLOW + "  ⚠  Unknown size — unchanged." + Style.RESET_ALL)
        elif n == 3:
            raw_q = (await _ask_line(
                "CONTRACTS ('copy', fixed count '2', or multiplier 'x0.5')"
            )).lower().replace(" ", "")
            if raw_q == "copy":
                rule["qty_mode"] = "copy"
                rule.pop("qty_value", None)
            elif raw_q and (raw_q.startswith("x") or raw_q.endswith("x")):
                try:
                    mult = float(raw_q.strip("x"))
                    rule["qty_mode"] = "multiple"
                    rule["qty_value"] = max(0.0, min(mult, RULE_CLAMPS["qty_value"][1]))
                except ValueError:
                    print(Fore.YELLOW + "  ⚠  Invalid multiplier — unchanged." + Style.RESET_ALL)
            elif raw_q:
                try:
                    fixed = int(raw_q)
                    rule["qty_mode"] = "fixed"
                    rule["qty_value"] = float(max(0, min(fixed, 1000)))
                except ValueError:
                    print(Fore.YELLOW + "  ⚠  Invalid count — unchanged." + Style.RESET_ALL)
            cap = await _ask_int("MAX CONTRACTS hard cap (0 = none)",
                                 eff["max_contracts"], *RULE_CLAMPS["max_contracts"])
            if cap != eff["max_contracts"] or "max_contracts" in rule:
                rule["max_contracts"] = cap
        elif n == 4:
            rule["direction"] = "invert" if eff["direction"] == "normal" else "normal"
            if rule["direction"] == "invert":
                print(Fore.YELLOW + "  ⚠  INVERTED — BUY↔SELL flipped. Non-market entries are"
                      + Style.RESET_ALL)
                print(Fore.YELLOW + "     skipped and publisher CHANGE orders are dropped."
                      + Style.RESET_ALL)
        elif n == 5:
            rule["delay_ms"] = await _ask_int(
                "DELAY ms before entries (0 = off)", eff["delay_ms"], *RULE_CLAMPS["delay_ms"])
            rule["delay_jitter_ms"] = await _ask_int(
                "RANDOM JITTER ms on top (0 = off)", eff["delay_jitter_ms"],
                *RULE_CLAMPS["delay_jitter_ms"])
        elif n == 6:
            rule["stagger_entries"] = await _ask_int(
                "TRANCHES per entry (1 = off, max 10)", eff["stagger_entries"],
                *RULE_CLAMPS["stagger_entries"])
            if rule["stagger_entries"] > 1:
                rule["stagger_interval_ms"] = await _ask_int(
                    "INTERVAL ms between tranches", eff["stagger_interval_ms"],
                    *RULE_CLAMPS["stagger_interval_ms"])
        elif n == 7:
            available = list_atm_strategies()
            if available:
                shown = ", ".join(available[:8]) + (" …" if len(available) > 8 else "")
                print(Fore.CYAN + f"\r\033[K  Installed: {shown}" + Style.RESET_ALL)
            name = await _ask_line("ATM TEMPLATE (ENTER = inherit session strategy)")
            if not name:
                rule.pop("atm", None)
            else:
                rule["atm"] = sanitize_ati(name)
                if not validate_strategy(rule["atm"]):
                    print(Fore.YELLOW + f"  ⚠  '{rule['atm']}' not in templates/AtmStrategy — "
                          "legs fall back to the session strategy until installed."
                          + Style.RESET_ALL)
        elif n == 8:
            await _edit_ai_gate(rule)
        elif n == 9 and scoped:
            await _edit_rule_scope(rule)


async def _edit_scoped_rules(account: str):
    """List / add / edit / delete an account's scoped rules."""
    while True:
        prof = account_profiles.setdefault(account, {})
        rules = prof.setdefault("rules", [])
        lines = []
        for i, r in enumerate(rules, 1):
            scope = (f"{','.join(r.get('symbols', [])) or 'any sym'} · "
                     f"{','.join(r.get('strategies', [])) or 'any strat'}")
            overrides = ", ".join(k for k in r if k in DEFAULT_RULE) or "no overrides"
            lines.append(f"{i}. [{scope[:26]}]  {overrides[:24]}")
        if not rules:
            lines.append("No scoped rules — the DEFAULT rule covers everything.")
        _prof_box(f"SCOPED RULES — {account}", lines, [
            "First matching rule wins and overrides the default rule.",
            "A = add · number = edit · D number = delete · ENTER = back",
        ])
        raw = (await _ask_line("RULES")).lower()
        if raw in ("", "q"):
            if not rules:
                prof.pop("rules", None)
            save_account_profiles()
            return
        if raw == "a":
            rule: dict = {}
            await _edit_rule_scope(rule)
            await _edit_rule_menu(rule, f"NEW RULE — {account}", scoped=True)
            if rule:
                rules.append(rule)
                print(Fore.GREEN + "  ✔  Rule added." + Style.RESET_ALL)
            save_account_profiles()
        elif raw.startswith("d") and raw[1:].strip().isdigit():
            idx = int(raw[1:].strip())
            if 1 <= idx <= len(rules):
                rules.pop(idx - 1)
                print(Fore.GREEN + f"  ✔  Rule {idx} deleted." + Style.RESET_ALL)
            save_account_profiles()
        elif raw.isdigit() and 1 <= int(raw) <= len(rules):
            await _edit_rule_menu(rules[int(raw) - 1], f"RULE {raw} — {account}", scoped=True)
            save_account_profiles()


async def _edit_account_profile(account: str):
    while True:
        prof_now = account_profiles.get(account, {})
        n_rules = len(prof_now.get("rules", []))
        allowed = prof_now.get("symbols_allowed", [])
        sym_label = ", ".join(allowed) if allowed else "all symbols"
        if prof_now.get("prop"):
            fh, fm = prop_flat_time(account)
            ch, cm = prop_cutoff_time(account)
            firm = prof_now.get("prop_firm", "")
            prop_label = (f"ON{f' ({firm})' if firm else ''} — flat {fh:02d}:{fm:02d}"
                          f" · no entries {ch:02d}:{cm:02d} ET")
        else:
            prop_label = "off"
        _prof_box(f"PROFILE — {account}", [
            f"Now: {profile_summary(account)}"[:_PROF_INNER],
            "",
            f"S. Symbols        — trades {sym_label}"[:_PROF_INNER],
            f"P. Prop account   — {prop_label}"[:_PROF_INNER],
            "D. Default rule   — applies to every signal",
            f"R. Scoped rules   — {n_rules} configured (per symbol/strategy)",
            "X. Reset          — remove this account's profile",
        ], ["ENTER = back"])
        raw = (await _ask_line("OPTION")).lower()
        if raw in ("", "q"):
            return
        if raw == "p":
            prof = account_profiles.setdefault(account, {})
            if prof.get("prop"):
                for key in ("prop", "prop_firm", "prop_flat_et", "prop_cutoff_et"):
                    prof.pop(key, None)
                if not prof:
                    account_profiles.pop(account, None)
                print(Fore.GREEN + f"  ✔  {account} is no longer a prop account."
                      + Style.RESET_ALL)
            else:
                prof["prop"] = True
                firms = ", ".join(sorted({
                    k for k in PROP_FIRM_PRESETS
                    if k not in ("myfundedfutures", "takeprofittrader", "etf")}))
                raw_f = (await _ask_line(
                    f"FIRM ({firms}; ENTER = generic)")).strip().lower()
                if raw_f:
                    prof["prop_firm"] = raw_f
                    if raw_f not in PROP_FIRM_PRESETS:
                        print(Fore.YELLOW + f"  ⚠  Unknown firm '{raw_f}' — using "
                              "generic 16:57/16:55 ET times. Set them below if "
                              "your firm closes earlier." + Style.RESET_ALL)
                (dh, dm), (dch, dcm) = _prop_preset(account)
                for key, label, dflt in (
                        ("prop_flat_et", "FLAT-BY-CLOSE", f"{dh:02d}:{dm:02d}"),
                        ("prop_cutoff_et", "ENTRY CUTOFF", f"{dch:02d}:{dcm:02d}")):
                    raw_t = await _ask_line(f"{label} ET as HH:MM (ENTER = {dflt})")
                    hm = _parse_prop_hhmm(raw_t)
                    if hm:
                        prof[key] = f"{hm[0]:02d}:{hm[1]:02d}"
                    elif raw_t.strip():
                        print(Fore.YELLOW + f"  ⚠  '{raw_t.strip()}' rejected — "
                              "need 24h ET HH:MM between 12:00 and 17:59 "
                              f"(e.g. 16:55, not 4:55). Using {dflt}."
                              + Style.RESET_ALL)
                fh, fm = prop_flat_time(account)
                print(Fore.GREEN + f"  ✔  {account} marked PROP — one position at "
                      "a time (closes are confirmed before a new entry fires),"
                      + Style.RESET_ALL)
                print(Fore.GREEN + "     opposite entries across accounts are "
                      f"blocked, and it is flattened at {fh:02d}:{fm:02d} ET."
                      + Style.RESET_ALL)
            save_account_profiles()
        elif raw == "s":
            raw_s = await _ask_line(
                "ALLOWED SYMBOLS e.g. 'GC' or 'NQ ES' (ENTER = all symbols)")
            prof = account_profiles.setdefault(account, {})
            symbols = []
            for tok in raw_s.replace(",", " ").split():
                name = tok.strip().upper()
                if name and name not in symbols:
                    symbols.append(name)
            if symbols:
                prof["symbols_allowed"] = symbols
                print(Fore.GREEN + f"  ✔  {account} only enters {', '.join(symbols)} "
                      "(micro twins included) — other signals are ignored;" + Style.RESET_ALL)
                print(Fore.GREEN + "     round-robin passes it over without losing its turn."
                      + Style.RESET_ALL)
            else:
                prof.pop("symbols_allowed", None)
                if not prof:
                    account_profiles.pop(account, None)
                print(Fore.GREEN + f"  ✔  {account} trades all symbols." + Style.RESET_ALL)
            save_account_profiles()
        elif raw == "d":
            prof = account_profiles.setdefault(account, {})
            rule = prof.setdefault("default", {})
            await _edit_rule_menu(rule, f"DEFAULT RULE — {account}", scoped=False)
            save_account_profiles()
        elif raw == "r":
            await _edit_scoped_rules(account)
        elif raw == "x":
            confirm = (await _ask_line(f"Remove profile for {account}? [y/N]")).lower()
            if confirm == "y":
                account_profiles.pop(account, None)
                save_account_profiles()
                print(Fore.GREEN + f"  ✔  {account} back to defaults." + Style.RESET_ALL)


async def _edit_strategy_symbols():
    """Global strategy → symbol filter editor (applies to every account).

    Picker-driven: strategies are numbered from the installed ATM
    templates and the publisher names seen on the wire (no transcription
    from the log), and symbols are numbered from the futures catalog.
    Filters key on ATM-template identity, so a filter set on
    'GC-MacroZoneB' also catches the wire name 'macro_zone_b'.
    """
    while True:
        choices = strategy_filter_choices()
        lines = []
        if strategy_symbols:
            for name, roots in sorted(strategy_symbols.items()):
                lines.append(f"{name[:24].ljust(24)} → {', '.join(roots)}"[:_PROF_INNER])
        else:
            lines.append("No filters — every strategy trades every symbol.")
        if choices:
            lines.append("")
            lines.append("★ installed ATM template · ≈ seen from the server")
            for i, c in enumerate(choices, 1):
                mark = {"atm": "★", "seen": "≈"}.get(c["kind"], "·")
                cur = strategy_filter_symbols(c["name"])
                tail = f" → {', '.join(cur)}" if cur else ""
                lines.append(f"{str(i).rjust(2)}. {mark} {c['name'][:32]}{tail}"[:_PROF_INNER])
        _prof_box("GLOBAL STRATEGY → SYMBOL FILTERS", lines, [
            "A listed strategy only ENTERS trades on its symbols —",
            "on EVERY account, under its template name OR the wire",
            "name. Exits never filter; a blocked reversal closes.",
            "STRATEGY number (or type a name) · ENTER = back",
        ])
        raw = (await _ask_line("STRATEGY")).strip()
        if not raw:
            return
        if raw.isdigit() and 1 <= int(raw) <= len(choices):
            raw = choices[int(raw) - 1]["name"]
        name = sanitize_ati(raw).lower()
        if not name:
            continue
        # Every key that currently governs this strategy (template-linked
        # spellings included): editing consolidates them under the picked
        # name and removing drops them ALL — leaving one behind would keep
        # gating the strategy after a "filter removed" message.
        base = atm_base_key(name)
        matching = [k for k in sorted(strategy_symbols)
                    if k == name or (base and atm_base_key(k) == base)]
        current = ", ".join(strategy_filter_symbols(name) or [])
        cat = [p["root"] for p in instrument_catalog()]
        sym_lines, row = [], []
        for i, root in enumerate(cat, 1):
            row.append(f"{str(i).rjust(2)}.{root.ljust(4)}")
            if len(row) == 6:
                sym_lines.append(" ".join(row))
                row = []
        if row:
            sym_lines.append(" ".join(row))
        _prof_box(f"SYMBOLS — {raw[:40]}", sym_lines, [
            "Numbers and/or roots, e.g. '2 8' or 'NQ GC' — micro",
            "twins included (GC covers MGC).",
            "ENTER = remove the filter" + (f" (now {current})"[:28] if current else ""),
        ])
        raw_s = await _ask_line("SYMBOLS")
        roots: list[str] = []
        bad_nums: list[str] = []
        for tok in raw_s.replace(",", " ").split():
            if tok.isdigit():
                # A number is only ever a menu pick — out of range is a
                # typo, not a symbol root named "99".
                if not 1 <= int(tok) <= len(cat):
                    bad_nums.append(tok)
                    continue
                root = cat[int(tok) - 1]
            else:
                root = sanitize_ati(tok.strip().upper())
            if root and root not in roots:
                roots.append(root)
        if bad_nums:
            print(Fore.YELLOW + f"  ⚠  Ignored out-of-range number(s): "
                  f"{', '.join(bad_nums)} — menu has 1–{len(cat)}." + Style.RESET_ALL)
        if roots:
            for k in matching:
                strategy_symbols.pop(k, None)
            strategy_symbols[name] = roots
            print(Fore.GREEN + f"  ✔  '{raw}' now only enters {', '.join(roots)} "
                  "(micro twins included) on every account." + Style.RESET_ALL)
        elif not raw_s.strip() and matching:
            for k in matching:
                strategy_symbols.pop(k, None)
            print(Fore.GREEN + f"  ✔  '{raw}' filter removed — trades every symbol."
                  + Style.RESET_ALL)
        save_strategy_symbols()


async def prompt_profiles():
    """Per-account trade profiles: size, contracts, direction, delay,
    stagger, ATM override, and AI gating — per account, scoped by symbol
    and/or publisher strategy."""
    global awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    try:
        nt_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
        names: list[str] = []
        for name in target_accounts() + [a["name"] for a in nt_accounts] + sorted(account_profiles):
            if name and name not in names:
                names.append(name)
        if not names:
            print(Fore.YELLOW + "\n  ⚠  No accounts known — set accounts first (S → 3)."
                  + Style.RESET_ALL)
            return
        while True:
            lines = []
            for i, name in enumerate(names, 1):
                if name == active_account:
                    role = "◀ leader"
                elif name in follower_accounts:
                    role = "＋ follower"
                else:
                    role = ""
                lines.append(f"{i}. {name[:13].ljust(13)} {role.ljust(10)} "
                             f"{profile_summary(name)[:29]}")
            n_filters = len(strategy_symbols)
            _prof_box("ACCOUNT PROFILES", lines, [
                "Per-account: symbol filter, micros/full, contracts,",
                "invert, delay, stagger, ATM override, AI gate.",
                f"G = global strategy→symbol filters ({n_filters} set)",
                "ACCOUNT number or name to edit · ENTER = close",
            ])
            raw = await _ask_line("ACCOUNT")
            if not raw:
                break
            if raw.strip().lower() == "g":
                await _edit_strategy_symbols()
                continue
            if raw.isdigit() and 1 <= int(raw) <= len(names):
                acct = names[int(raw) - 1]
            else:
                acct = raw
                if acct not in names:
                    names.append(acct)  # allow pre-configuring an offline account
            await _edit_account_profile(acct)
    finally:
        awaiting_user_input = False
        refresh_header_status()


# ---------- Server selector ----------
MAX_SAVED_SERVERS = 10  # cap saved server list


async def prompt_server():
    """Show saved servers and let user pick one or add a new server."""
    global awaiting_user_input
    awaiting_user_input = True
    show_cursor()

    cfg = load_config()
    servers = cfg.get("servers", [])
    current = cfg.get("ws_host", "")

    print(Fore.CYAN + "\n┌─ SERVER SELECT ───────────────────────────────────┐" + Style.RESET_ALL)
    if servers:
        for i, srv in enumerate(servers, 1):
            marker = " ◀" if srv.get("url") == current else ""
            line = f"{i}. {srv.get('name', 'unnamed')}  {srv.get('url', '')}{marker}"
            print(Fore.CYAN + f"│  {line[:49].ljust(49)}│" + Style.RESET_ALL)
    n = len(servers) + 1
    print(Fore.CYAN + f"│  {n}. + Add new server{' ' * (49 - len(str(n)) - 20)}│" + Style.RESET_ALL)
    if servers:
        d = n + 1
        print(Fore.CYAN + f"│  {d}. - Remove a server{' ' * (49 - len(str(d)) - 21)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────┘" + Style.RESET_ALL)
    sys.stdout.write(Fore.WHITE + "  SERVER ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw = (await asyncio.to_thread(read_line_raw)).strip()

    if raw == "":
        print(Fore.YELLOW + "  ↩  No change." + Style.RESET_ALL)
    elif raw.isdigit():
        choice = int(raw)
        if 1 <= choice <= len(servers):
            # Select existing server
            srv = servers[choice - 1]
            if srv["url"] != current:
                cfg["ws_host"] = srv["url"]
                save_config(cfg)
                print(Fore.GREEN + f"  ✔  Server → {srv['name']} ({srv['url']})" + Style.RESET_ALL)
                print(Fore.YELLOW + "  🔄  Reconnecting..." + Style.RESET_ALL)
                reconnect_event.set()
            else:
                print(Fore.YELLOW + "  ↩  Already connected to this server." + Style.RESET_ALL)
        elif choice == n:
            # Add new server
            sys.stdout.write(Fore.WHITE + "  NAME ▸ " + Style.RESET_ALL)
            sys.stdout.flush()
            name = (await asyncio.to_thread(read_line_raw)).strip()
            if not name:
                print(Fore.YELLOW + "  ↩  Cancelled." + Style.RESET_ALL)
            else:
                sys.stdout.write(Fore.WHITE + "  URL ▸ " + Style.RESET_ALL)
                sys.stdout.flush()
                url = (await asyncio.to_thread(read_line_raw)).strip()
                if url and (url.startswith("ws://") or url.startswith("wss://")):
                    if len(servers) >= MAX_SAVED_SERVERS:
                        print(Fore.RED + f"  ✖  Max {MAX_SAVED_SERVERS} servers. Remove one first." + Style.RESET_ALL)
                    else:
                        servers.append({"name": name, "url": url})
                        cfg["servers"] = servers
                        cfg["ws_host"] = url
                        save_config(cfg)
                        print(Fore.GREEN + f"  ✔  Added & selected → {name} ({url})" + Style.RESET_ALL)
                        print(Fore.YELLOW + "  🔄  Reconnecting..." + Style.RESET_ALL)
                        reconnect_event.set()
                else:
                    print(Fore.RED + "  ✖  URL must start with ws:// or wss://" + Style.RESET_ALL)
        elif servers and choice == n + 1:
            # Remove a server
            sys.stdout.write(Fore.WHITE + "  REMOVE # ▸ " + Style.RESET_ALL)
            sys.stdout.flush()
            rm_raw = (await asyncio.to_thread(read_line_raw)).strip()
            if rm_raw.isdigit() and 1 <= int(rm_raw) <= len(servers):
                removed = servers.pop(int(rm_raw) - 1)
                cfg["servers"] = servers
                save_config(cfg)
                print(Fore.GREEN + f"  ✔  Removed → {removed['name']}" + Style.RESET_ALL)
            else:
                print(Fore.YELLOW + "  ↩  Cancelled." + Style.RESET_ALL)

    print()
    awaiting_user_input = False


# ---------- Token prompt (runtime) ----------
async def prompt_token():
    """Prompt user to update their connection token."""
    global awaiting_user_input
    awaiting_user_input = True
    show_cursor()

    cfg = load_config()
    current = cfg.get("token", "")
    masked = "*" * len(current) if current else "not set"

    print(Fore.CYAN + "\n┌─ CONNECTION TOKEN ────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  Current: {masked[:39].ljust(39)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────┘" + Style.RESET_ALL)
    sys.stdout.write(Fore.WHITE + "  TOKEN ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw = (await asyncio.to_thread(read_line_raw)).strip()

    if raw == "":
        print(Fore.YELLOW + "  ↩  No change." + Style.RESET_ALL)
    else:
        cfg["token"] = raw
        save_config(cfg)
        print(Fore.GREEN + "  ✔  Token updated." + Style.RESET_ALL)
        print(Fore.YELLOW + "  🔄  Reconnecting..." + Style.RESET_ALL)
        reconnect_event.set()

    print()
    awaiting_user_input = False


# ---------- Setup submenu ----------
# ---------- Live-monitor prompt (optional NinjaScript AddOn) ----------
def _live_bridge_status(timeout: float = 1.0) -> tuple[str, str]:
    """Return (label, color) summarising the current live-monitor state.

    The default timeout is short because the AddOn broadcasts a snapshot
    immediately on accept; anything slower than ~1s means it's not there.
    Callers that want a more patient probe (explicit Test Connection) can
    pass a longer timeout.
    """
    if not live_bridge_enabled:
        return ("disabled", "WHITE")
    host = _nt_host(nt_port)
    reachable = probe_live_bridge(host, live_bridge_port, timeout=timeout)
    return ("enabled · active", "GREEN") if reachable else ("enabled · NOT REACHABLE", "YELLOW")


def _print_addon_install_steps():
    print(Fore.CYAN + "\n┌─ INSTALL SOCKETTRADER ADDON ───────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  The AddOn publishes live account P&L from inside  │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  NinjaTrader so stops/targets fire DURING a trade. │" + Style.RESET_ALL)
    print(Fore.CYAN + "│                                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  1. Enable (option 1). Socket Trader copies the    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│     .cs into NinjaTrader\\bin\\Custom\\AddOns\\ for    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│     you (if NT folder is accessible locally).      │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  2. In NinjaTrader:                                │" + Style.RESET_ALL)
    print(Fore.CYAN + "│     Control Center → New → NinjaScript Editor.     │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  3. Press F5 to compile. (No restart needed —      │" + Style.RESET_ALL)
    print(Fore.CYAN + "│     AddOn hot-loads on successful compile.)        │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  4. Output tab should print:                       │" + Style.RESET_ALL)
    print(Fore.CYAN + "│     'SocketTraderBridge listening on 0.0.0.0:XXX'  │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  5. Test from the menu (option 3).                 │" + Style.RESET_ALL)
    print(Fore.CYAN + "│                                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Manual install: copy addon/SocketTraderBridge.cs  │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  into Documents\\NinjaTrader 8\\bin\\Custom\\AddOns\\.  │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Remote NT? Set its LAN IP via option 5.           │" + Style.RESET_ALL)
    print(Fore.CYAN + "│                                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Full docs: addon/README.md                        │" + Style.RESET_ALL)
    print(Fore.CYAN + "└────────────────────────────────────────────────────┘" + Style.RESET_ALL)


async def prompt_live_bridge():
    """Setup submenu for the optional live-monitoring NinjaScript AddOn."""
    global live_bridge_enabled, live_bridge_port, nt_host_override, awaiting_user_input
    awaiting_user_input = True
    show_cursor()

    label, color = await asyncio.to_thread(_live_bridge_status)
    state_color = _STATE_COLORS.get(color, Fore.WHITE)
    host_display = nt_host_override or f"auto ({_nt_host(nt_port)})"

    print(Fore.CYAN + "\n┌─ LIVE TRADE MONITOR (optional AddOn) ──────────────┐" + Style.RESET_ALL)
    status_line = f"Status: {label}"
    print(Fore.CYAN + "│  " + state_color + status_line.ljust(50) + Fore.CYAN + "│" + Style.RESET_ALL)
    host_line = f"NT host: {host_display}"
    print(Fore.CYAN + f"│  {host_line[:50].ljust(50)}│" + Style.RESET_ALL)
    port_line = f"Port:    {live_bridge_port}"
    print(Fore.CYAN + f"│  {port_line[:50].ljust(50)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "│                                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  With the AddOn: session stops/targets fire DURING │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  an open trade. Without it: only after trade close.│" + Style.RESET_ALL)
    print(Fore.CYAN + "│                                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  1. Toggle enabled / disabled                      │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  2. Show install steps                             │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  3. Test connection now                            │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  4. Change port                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  5. Change NT host (for remote NT on LAN)          │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  ESC to close                                      │" + Style.RESET_ALL)
    print(Fore.CYAN + "└────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    if live_bridge_enabled and color != "GREEN":
        print(Fore.YELLOW + Style.BRIGHT +
              f"  ⚠  Live monitor is enabled but unreachable at " +
              f"{_nt_host(nt_port)}:{live_bridge_port}." + Style.RESET_ALL)
        print(Fore.YELLOW +
              "     The AddOn isn't compiled or NinjaTrader is down. "
              "Press 2 for steps." + Style.RESET_ALL)

    sys.stdout.write(Fore.WHITE + "  LIVE ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    key = await asyncio.to_thread(get_key)
    hide_cursor()

    cfg = load_config()
    host = _nt_host(nt_port)
    # Results go through _dash_set_alert because _dash_exit_menu() wipes
    # everything the submenu printed the moment control returns to the
    # outer setup_menu. The alert row is the only output that survives.
    if key == "1":
        live_bridge_enabled = not live_bridge_enabled
        cfg["live_bridge_enabled"] = live_bridge_enabled
        save_config(cfg)
        if live_bridge_enabled:
            # Auto-install SocketTraderBridge.cs into NT's AddOns folder
            # when we know where NT lives (output_directory's parent). This
            # only works when NT runs on the same box as SocketTrader or
            # the folder is mounted — remote NT installs still need manual
            # drop + F5.
            install_note = ""
            if output_directory and Path(output_directory).is_dir():
                nt_base = Path(output_directory).parent
                inst_ok, inst_msg = await asyncio.to_thread(
                    install_live_bridge_addon, nt_base)
                if inst_ok and "copied" in inst_msg.lower():
                    install_note = " · .cs copied to AddOns (F5 to compile)"
                elif not inst_ok:
                    install_note = f" · auto-install skipped: {inst_msg}"
            ok = await asyncio.to_thread(
                probe_live_bridge, host, live_bridge_port)
            if ok:
                _dash_set_alert(
                    Fore.GREEN +
                    f"  ✔  Live monitor enabled · AddOn active on "
                    f"{host}:{live_bridge_port}.{install_note}" +
                    Style.RESET_ALL)
            else:
                _dash_set_alert(
                    Fore.YELLOW +
                    f"  ⚠  Live monitor enabled.{install_note or ''}  "
                    f"Once compiled in NT (F5), S → 7 → 3 to test." +
                    Style.RESET_ALL)
        else:
            _dash_set_alert(
                Fore.CYAN + "  ✔  Live monitor disabled." + Style.RESET_ALL)
    elif key == "2":
        _print_addon_install_steps()
        sys.stdout.write(Fore.WHITE +
                         "\n  Press ENTER to return... " + Style.RESET_ALL)
        sys.stdout.flush()
        show_cursor()
        await asyncio.to_thread(read_line_raw)
        hide_cursor()
    elif key == "3":
        sys.stdout.write(Fore.WHITE +
                         "  Testing connection..." + Style.RESET_ALL)
        sys.stdout.flush()
        ok, reason = await asyncio.to_thread(
            _probe_live_bridge_detail, host, live_bridge_port, 2.5)
        if ok:
            _dash_set_alert(
                Fore.GREEN +
                f"  ✔  AddOn reachable · streaming live P&L from "
                f"{host}:{live_bridge_port}." + Style.RESET_ALL)
        else:
            _dash_set_alert(
                Fore.YELLOW +
                f"  ✖  {host}:{live_bridge_port}  →  {reason}" +
                Style.RESET_ALL)
    elif key == "4":
        show_cursor()
        sys.stdout.write(Fore.WHITE +
                         f"  NEW PORT [{live_bridge_port}] ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw = await asyncio.to_thread(read_line_raw)
        hide_cursor()
        if raw:
            try:
                p = int(raw.strip())
                if 1 <= p <= 65535:
                    live_bridge_port = p
                    cfg["live_bridge_port"] = p
                    save_config(cfg)
                    _dash_set_alert(
                        Fore.GREEN +
                        f"  ✔  Live monitor port set → {p}." + Style.RESET_ALL)
                else:
                    _dash_set_alert(
                        Fore.RED + "  ✖  Port must be 1–65535." + Style.RESET_ALL)
            except ValueError:
                _dash_set_alert(
                    Fore.RED + "  ✖  Invalid port." + Style.RESET_ALL)
    elif key == "5":
        show_cursor()
        current = nt_host_override or "(auto)"
        sys.stdout.write(Fore.WHITE +
                         f"  NT HOST [{current}]  "
                         f"(IP or hostname, blank = auto) ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw = await asyncio.to_thread(read_line_raw)
        hide_cursor()
        raw = (raw or "").strip()
        # Clear the per-port probe cache so the new host gets re-tested.
        invalidate_nt_host_cache()
        if not raw:
            # Blank input = clear override, revert to auto-detect.
            nt_host_override = ""
            cfg["nt_host"] = ""
            save_config(cfg)
            _dash_set_alert(
                Fore.CYAN + "  ✔  NT host reverted to auto-detect." +
                Style.RESET_ALL)
        else:
            nt_host_override = raw
            cfg["nt_host"] = raw
            save_config(cfg)
            # Probe the new host on the live bridge port right away so the
            # alert tells the user whether it actually reaches NT.
            ok = await asyncio.to_thread(
                probe_live_bridge, raw, live_bridge_port, 2.5)
            if ok:
                _dash_set_alert(
                    Fore.GREEN +
                    f"  ✔  NT host set → {raw} · AddOn reachable on "
                    f"port {live_bridge_port}." + Style.RESET_ALL)
            else:
                _dash_set_alert(
                    Fore.YELLOW +
                    f"  ⚠  NT host set → {raw} but AddOn not responding "
                    f"on port {live_bridge_port}.  NT running? "
                    "Firewall open for the WSL/LAN subnet?" + Style.RESET_ALL)

    awaiting_user_input = False


async def setup_menu():
    """Show the setup submenu for config changes."""
    global awaiting_user_input
    _dash_enter_menu()
    awaiting_user_input = True
    show_cursor()
    cfg = load_config()
    current_server = cfg.get("ws_host", "not set")
    masked_token = "*" * min(len(cfg.get("token", "")), 33) or "not set"
    # Probe off-loop — a blocking probe here stalls the asyncio event loop
    # long enough for the WebSocket server to time out the connection.
    live_label, live_color = await asyncio.to_thread(_live_bridge_status)
    live_color_code = _STATE_COLORS.get(live_color, "")
    print(Fore.CYAN + "\n┌─ SETUP ──────────────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  1. Server    ({current_server[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  2. Token     ({masked_token[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    acct_label = active_account or "not set"
    if follower_accounts or roundrobin_accounts:
        acct_label = active_account or "?"
        if follower_accounts:
            acct_label += f" +{len(follower_accounts)} copy"
        if roundrobin_accounts:
            acct_label += f" +{len(roundrobin_accounts)} rr"
    print(Fore.CYAN + f"│  3. Accounts  ({acct_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    mode_tag = "FOLLOW" if follow_publisher_strategy else "LOCKED"
    strat_label = f"{atm_strategy} · {mode_tag}"
    print(Fore.CYAN + f"│  4. Strategy  ({strat_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  5. Directory ({(output_directory or 'not set')[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  6. ATI Port  ({nt_port})" .ljust(53) + "│" + Style.RESET_ALL)
    micro_label = "ON — NQ→MNQ, ES→MES, …" if micro_mode else "OFF"
    print(Fore.CYAN + f"│  7. Micros    ({micro_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    n_profiles = len(account_profiles)
    prof_label = f"{n_profiles} account{'s' if n_profiles != 1 else ''} customized" if n_profiles else "none"
    print(Fore.CYAN + f"│  8. Profiles  ({prof_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    # Live monitor row with inline color on the status label. Slot 9 —
    # 7 and 8 were already taken by micros and profiles on main.
    live_row_plain = f"│  9. Live Mon. ({live_label[:33]})"
    live_pad = " " * max(0, 53 - len(live_row_plain))
    live_row = (
        Fore.CYAN + "│  9. Live Mon. (" +
        live_color_code + live_label[:33] + Fore.CYAN +
        ")" + live_pad + "│"
    )
    print(live_row + Style.RESET_ALL)
    print(Fore.CYAN + "│  ESC to close                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "└──────────────────────────────────────────────────┘" + Style.RESET_ALL)

    # Inline warning if enabled-but-not-active
    if live_bridge_enabled and live_color != "GREEN":
        print(Fore.YELLOW +
              "  ⚠  Live monitor enabled but AddOn not reachable "
              "(option 9 for steps)." + Style.RESET_ALL)

    sys.stdout.write(Fore.WHITE + "  SETUP ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    key = await asyncio.to_thread(get_key)
    hide_cursor()
    awaiting_user_input = False

    if key == "1":
        await prompt_server()
    elif key == "2":
        await prompt_token()
    elif key == "3":
        await prompt_accounts()
    elif key == "4":
        await prompt_strategy()
    elif key == "5":
        await prompt_directory()
    elif key == "6":
        await prompt_port()
    elif key == "7":
        if toggle_micro_mode():
            _dash_set_alert(Fore.GREEN + "  ✔  MICRO MODE ON — signals convert to micros (NQ→MNQ). "
                            "Toggle while flat." + Style.RESET_ALL)
        else:
            _dash_set_alert(Fore.YELLOW + "  ✔  MICRO MODE OFF — instruments sent as-is." + Style.RESET_ALL)
        refresh_header_status()
    elif key == "8":
        await prompt_profiles()
    elif key == "9":
        await prompt_live_bridge()
    _dash_exit_menu()


# ---------- Balances display ----------
async def show_balances():
    """Display all NinjaTrader account balances with session P&L."""
    accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
    if not accounts:
        _dash_set_alert(Fore.YELLOW + "  ⚠  Could not reach NinjaTrader ATI." + Style.RESET_ALL)
        return

    _dash_enter_menu()
    # Find longest account name for formatting
    max_name = max(len(a["name"]) for a in accounts)
    box_inner = max(max_name + 35, 50)

    print(Fore.CYAN + f"  ╭─ BALANCES {'─' * (box_inner - 9)}╮" + Style.RESET_ALL)
    for a in accounts:
        name = a["name"]
        cash = _held_balance(name, a["cash"])  # outage zeros → last known
        if cash is None:
            continue
        if name == active_account:
            marker = " ◀"
        elif name in follower_accounts:
            marker = " ＋"
        elif name in roundrobin_accounts:
            marker = " ⟳"
        else:
            marker = ""
        start = session_start_balances.get(name)
        if start is not None:
            pnl = session_pnl(name, cash)
            pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
            line = f"{name.ljust(max_name)}  ${cash:>12,.2f}  "
            pnl_str = f"P&L: ${pnl:+,.2f}{marker}"
            # Print with color for P&L portion
            pad = box_inner - len(line) - len(pnl_str.replace(marker, "")) - len(marker)
            print(f"\r\033[K" + Fore.CYAN + f"  │  {line}" + pnl_color + f"{pnl_str}" + " " * max(0, pad) + Fore.CYAN + "│" + Style.RESET_ALL)
        else:
            line = f"{name.ljust(max_name)}  ${cash:>12,.2f}{marker}"
            print(f"\r\033[K" + Fore.CYAN + f"  │  {line.ljust(box_inner)}│" + Style.RESET_ALL)
    print(f"\r\033[K" + Fore.CYAN + f"  ╰{'─' * (box_inner + 2)}╯" + Style.RESET_ALL)
    sys.stdout.write("\r\033[K")
    print(Fore.WHITE + Style.DIM + "  Press R to reset session P&L, or any key to close." + Style.RESET_ALL)
    sys.stdout.flush()

    global awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    key = await asyncio.to_thread(get_key)
    hide_cursor()
    awaiting_user_input = False

    if key.lower() == "r":
        reset_session_pnl()
        logger.info("MANUAL RESET  session P&L reset by user")
        # Alert will be visible after menu exit
        _dash_set_alert(Fore.GREEN + "  ✔  Session P&L reset — balances re-snapshotted." + Style.RESET_ALL)
    _dash_exit_menu()


# ---------- Close positions menu ----------
def _draw_close_menu(entries: list[tuple[str, int]], selected: int, box_inner: int, account: str):
    """Draw the close positions box with the current selection highlighted."""
    total = len(entries)  # entries = positions + "Close ALL"
    title = f"─ CLOSE POSITIONS ({account}) "
    top_dashes = max(0, box_inner - len(title) + 2)
    sys.stdout.write(f"\033[{total + 4}A")  # move cursor up to top of box
    sys.stdout.write(Fore.CYAN + f"\r\033[K┌{title}{'─' * top_dashes}┐" + Style.RESET_ALL + "\n")
    for i, (instrument, qty) in enumerate(entries):
        if instrument == "_ALL_":
            label = f"Close ALL ({qty} position{'s' if qty != 1 else ''})"
            plain = label
            colored = label
        else:
            direction = "LONG" if qty > 0 else "SHORT"
            dir_color = Fore.GREEN if qty > 0 else Fore.RED
            plain = f"{instrument}  {direction} {abs(qty)}"
            colored = f"{instrument}  {dir_color}{direction} {abs(qty)}{Fore.CYAN}"
        if i == selected:
            marker = " ◀"
            line_color = Fore.WHITE + Style.BRIGHT
            reset = Style.RESET_ALL + Fore.CYAN
        else:
            marker = ""
            line_color = Fore.CYAN
            reset = ""
        pad = box_inner - len(plain) - len(marker)
        sys.stdout.write(Fore.CYAN + f"\r\033[K│  {line_color}{colored}{reset}{marker}{' ' * max(0, pad)}│" + Style.RESET_ALL + "\n")
    hint = "↑↓ navigate · ENTER select · ESC cancel"
    sys.stdout.write(Fore.CYAN + f"\r\033[K│  " + Fore.WHITE + Style.DIM + f"{hint.ljust(box_inner)}" + Style.RESET_ALL + Fore.CYAN + "│" + Style.RESET_ALL + "\n")
    sys.stdout.write(Fore.CYAN + f"\r\033[K└{'─' * (box_inner + 2)}┘" + Style.RESET_ALL + "\n")
    sys.stdout.flush()


async def close_positions_menu():
    """Show open positions and let user close one or all via arrow keys."""
    global awaiting_user_input
    if not active_account:
        _dash_set_alert(Fore.YELLOW + "  ⚠  Set an account first (press A)." + Style.RESET_ALL)
        return
    if not output_directory:
        _dash_set_alert(Fore.YELLOW + "  ⚠  Set an output directory first (press D)." + Style.RESET_ALL)
        return

    positions = await asyncio.to_thread(query_nt_positions, active_account, nt_port)
    open_pos = {k: v for k, v in positions.items() if v != 0}

    if not open_pos:
        _dash_set_alert(
            Fore.YELLOW + "  ⚠  No open positions for " + Fore.WHITE + active_account +
            Fore.YELLOW + "." + Style.RESET_ALL)
        return

    _dash_enter_menu()
    # Build entries: individual positions + "Close ALL"
    entries = list(open_pos.items())
    entries.append(("_ALL_", len(open_pos)))
    box_inner = max(max(len(k) for k in open_pos) + 25, 42)

    awaiting_user_input = True
    hide_cursor()
    selected = 0

    # Print blank lines to reserve space, then draw
    for _ in range(len(entries) + 4):
        print()
    _draw_close_menu(entries, selected, box_inner, active_account)

    # Arrow key navigation loop
    while True:
        key = await asyncio.to_thread(get_key)
        if key == "UP":
            selected = (selected - 1) % len(entries)
            _draw_close_menu(entries, selected, box_inner, active_account)
        elif key == "DOWN":
            selected = (selected + 1) % len(entries)
            _draw_close_menu(entries, selected, box_inner, active_account)
        elif key == "\r" or key == "\n":
            # Selected — ask for confirmation
            chosen = entries[selected]
            n_acct = len(target_accounts())
            if chosen[0] == "_ALL_":
                confirm_msg = (f"Flatten EVERY managed account "
                               f"({n_acct}) — {len(open_pos)} leader "
                               f"position{'s' if len(open_pos) != 1 else ''}?"
                               if n_acct > 1 else
                               f"Close ALL {len(open_pos)} position"
                               f"{'s' if len(open_pos) != 1 else ''}?")
            else:
                direction = "LONG" if chosen[1] > 0 else "SHORT"
                confirm_msg = f"Close {chosen[0]} ({direction} {abs(chosen[1])})?"
            sys.stdout.write(f"\r\033[K" + Fore.YELLOW + f"  {confirm_msg} [y/N] " + Style.RESET_ALL)
            sys.stdout.flush()
            confirm = await asyncio.to_thread(get_key)
            if confirm.lower() == "y":
                if chosen[0] == "_ALL_":
                    # Every managed account, leader first — not just the
                    # leader. Closing only the leader leaves followers and
                    # the rotation holding the other side of the book.
                    all_closed = await asyncio.to_thread(close_all_open_positions)
                    sys.stdout.write("\r\033[K")
                    for instrument in all_closed:
                        print(Fore.RED + f"  ⛔  CLOSEPOSITION → {instrument}" + Style.RESET_ALL)
                    logger.info(f"CLOSE ALL  accounts={target_accounts()}  "
                                f"contracts={all_closed}")
                    still = await verify_flat(target_accounts())
                    if still:
                        detail = ", ".join(
                            p["instrument"] if p["instrument"] == "UNVERIFIED"
                            else f"{p['account']} {p['qty']:+d} {p['instrument']}"
                            for p in still)
                        print(Fore.RED + Style.BRIGHT +
                              f"  ⛔  NOT FLAT — still open: {detail}" + Style.RESET_ALL)
                        logger.error(f"CLOSE ALL INCOMPLETE  {detail}")
                    else:
                        print(Fore.GREEN + "  ✔  verified flat" + Style.RESET_ALL)
                else:
                    fire_close_position(active_account, chosen[0])
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + f"  ⛔  CLOSEPOSITION → {chosen[0]}" + Style.RESET_ALL)
                    logger.info(f"CLOSE  account={active_account}  contract={chosen[0]}")
            else:
                sys.stdout.write("\r\033[K")
                print(Fore.WHITE + Style.DIM + "  Cancelled." + Style.RESET_ALL)
            break
        elif key == "\x1b" or key.lower() == "q":
            break

    awaiting_user_input = False
    _dash_exit_menu()


# ---------- Keyboard loop ----------
reconnect_event = asyncio.Event()


async def keyboard_loop():
    global paused, soft_stopped
    while not shutdown.is_set():
        try:
            key = await asyncio.to_thread(get_key)
            if awaiting_directory_input or awaiting_user_input:
                continue
            if key.lower() == "p":
                if hard_stopped:
                    _dash_set_alert(
                        Fore.RED + "  ⛔  Hard stop locked for the session — B→R to reset P&L, or exit to clear." + Style.RESET_ALL)
                    continue
                paused = not paused
                if paused:
                    set_session_state("paused")
                else:
                    # Resume: clear every soft (resumable) account lockout so
                    # those accounts trade again. Hard locks are blocked above.
                    for a in [k for k, v in account_stops.items() if v == "soft"]:
                        del account_stops[a]
                    soft_stopped = False
                    _recompute_session_lock()
                    set_session_state("ready")
                    _dash_set_alert(
                        Fore.GREEN + "  ▶  SIGNAL OUTPUT RESUMED" + Style.RESET_ALL)
            elif key.lower() == "b":
                await show_balances()
            elif key.lower() == "s":
                await setup_menu()
            elif key.lower() == "t":
                await prompt_limits()
            elif key.lower() == "r":
                _dash_set_alert(
                    Fore.YELLOW + "  🔄  MANUAL RECONNECT REQUESTED" + Style.RESET_ALL,
                    kind=ALERT_CONN)
                reconnect_event.set()
            elif key.lower() == "c":
                await close_positions_menu()
            elif key.lower() == "o":
                await manual_trade_menu()
            elif key == "X":  # Shift+X only
                unpin_layout()
                clear()
                shutdown.set()
                break
        except Exception as e:
            logger.error(f"keyboard_loop error: {e}")


# ---------- Signal formatting ----------
SIGNAL_COLOURS = [Fore.GREEN, Fore.CYAN, Fore.LIGHTGREEN_EX]


# Signal tag colours for non-trade states
SIGNAL_TAG_COLOURS = {
    "PAUSED":   Fore.YELLOW,
    "LOCKED":   Fore.RED,
    "BLOCKED":  Fore.RED,
    "DUPE":     Fore.LIGHTBLACK_EX,
    "REJECTED": Fore.RED,
    "SKIPPED":  Fore.YELLOW,
    "REPLAY":   Fore.LIGHTBLACK_EX,
    "MANUAL":   Fore.LIGHTCYAN_EX,
}


def format_signal(signal_text: str, idx: int, latency_str: str = "", tag: str | None = None):
    colour = SIGNAL_COLOURS[idx % len(SIGNAL_COLOURS)]
    ts = time.strftime("%H:%M:%S")
    width = term_width()
    tag_str = ""
    if tag:
        tag_colour = SIGNAL_TAG_COLOURS.get(tag, Fore.YELLOW)
        tag_str = f"{tag_colour}[{tag}]{Style.RESET_ALL}{colour} "
        colour = Style.DIM + colour  # dim body when tagged
    prefix_plain = f"  #{idx} [{ts}] ▸  " + (f"[{tag}] " if tag else "")
    prefix = f"  #{idx} [{ts}] ▸  {tag_str}"
    suffix = f"  · {latency_str}" if latency_str else ""
    avail = width - len(prefix_plain) - len(suffix) - 2
    body = signal_text
    if len(body) > avail:
        body = body[: avail - 1] + "…"
    return f"{colour}{prefix}{body}{Style.DIM}{suffix}{Style.RESET_ALL}"


# ---------- Server message display ----------
WELCOME_FRAMES = ["◇", "◆", "◇", "◆", "●"]
HEARTBEAT_FRAMES = ["♡", "♥", "♡", "♥"]


async def display_server_message(data: dict, connect_latency: int):
    """Parse and display server messages on fixed dashboard rows."""
    global _server_name

    if "welcome" in data:
        _server_name = data.get("server", "SocketTrader")
        hb_interval = data.get("heartbeat_interval")
        ts = data.get("ts")

        # Animated welcome on heartbeat row
        for frame in WELCOME_FRAMES:
            _dash_set_heartbeat(
                f"{Fore.CYAN}  {frame} Connecting to {_server_name}...{Style.RESET_ALL}")
            await asyncio.sleep(0.15)

        # Final heartbeat line with connection details
        parts = [f"●  {_server_name}"]
        if ts:
            welcome_lat = int(time.time() * 1000) - ts
            parts.append(f"Latency: {welcome_lat}ms")
            logger.info(f"WELCOME  latency={welcome_lat}ms  handshake={connect_latency}ms")
        parts.append(f"Handshake: {connect_latency}ms")
        if hb_interval:
            parts.append(f"HB every {hb_interval // 60}min")
        _dash_set_heartbeat(
            Fore.CYAN + "  " + "  ·  ".join(parts) + Style.RESET_ALL)

    elif data.get("type") == "heartbeat":
        # Animated heartbeat pulse on fixed row
        for frame in HEARTBEAT_FRAMES:
            _dash_set_heartbeat(f"{Fore.RED}{Style.DIM}  {frame}{Style.RESET_ALL}")
            await asyncio.sleep(0.2)
        ts = time.strftime("%H:%M:%S")
        _dash_set_heartbeat(
            f"{Fore.RED}{Style.DIM}  ♥  [{ts}] heartbeat  ·  {_server_name}{Style.RESET_ALL}")
        logger.info("HEARTBEAT")
        # Connection is healthy — age out any stale one-shot alert so the
        # dashboard only shows conditions that are still true.
        _dash_expire_alert()

        # MOTD support: show server message-of-the-day, clear when absent
        motd = data.get("motd", "")
        if motd:
            _dash_set_motd(
                Fore.CYAN + f"  📢  {motd}" + Style.RESET_ALL)
        else:
            _dash_set_motd("")

    else:
        # Unknown server message — log only
        logger.info(f"SERVER  {data}")


# ---------- File output ----------
def extract_signal_string(msg: str, account: str, atm: str, follow_publisher: bool = False,
                          micros: bool = False) -> tuple[str | None, int | None, str | None, str | None]:
    """Parse JSON message and extract the raw signal string, server timestamp, signal ID, and reject reason.

    Signal format: PLACE;Account;Instrument;Action;Qty;OrderType;;;TIF;;;AtmStrategy;SignalID
    Index:           0      1        2        3     4      5     678  9  10 11          12
    - Field 1 (account) is replaced with the user's real account.
    - Field 2 (instrument): if micros is True, translated to its CME micro
      equivalent (NQ 06-26 → MNQ 06-26) via to_micro_instrument.
    - Field 11 (ATM strategy): if follow_publisher is True and the publisher's
      template exists locally, keep it; otherwise replace with the user's
      chosen strategy (`atm`) as a fallback.
    - Field 12 (last field) is the unique signal ID used for dedup.
    Returns (processed_signal, server_timestamp_ms, signal_id, reject_reason).
    A rejected-but-parsed signal returns (raw, ts, id, reason) so the UI can show it.
    """
    try:
        data = json.loads(msg)
        if isinstance(data, dict) and "signal" in data:
            # Verbatim wire capture — settles disputes about what the server
            # actually sends (field 11 vs extra envelope keys) from the log.
            logger.info(f"RAW ENVELOPE  {json.dumps(data)[:500]}")
            raw = data["signal"]
            ts = data.get("ts")
            parts = [sanitize_ati(p) for p in raw.split(";")]
            # Validate against NinjaTrader ATI spec
            error = validate_signal(parts)
            if error:
                logger.warning(f"REJECTED  {error}  raw={raw[:200]}")
                return raw[:200], ts, None, error
            # Extract signal ID (last non-empty field)
            signal_id = parts[-1] if parts else None
            # Replace account (field 1)
            if len(parts) >= 2:
                parts[1] = account
            # Resolve ATM strategy (field 11)
            if len(parts) >= 12:
                pub_strategy = parts[11].strip()
                _record_pub_strategy(pub_strategy)
                resolved = None
                if follow_publisher:
                    resolved = resolve_publisher_atm(
                        pub_strategy, parts[2] if len(parts) >= 3 else "")
                if resolved:
                    if resolved != pub_strategy:
                        logger.info(f"STRATEGY MATCH  publisher='{pub_strategy}' → '{resolved}'")
                    parts[11] = resolved
                else:
                    if follow_publisher and pub_strategy and pub_strategy != atm:
                        logger.info(
                            f"STRATEGY FALLBACK  publisher='{pub_strategy}' not installed → using '{atm}'")
                        if pub_strategy not in _pub_atm_fallback_warned:
                            _pub_atm_fallback_warned.add(pub_strategy)
                            _dash_set_alert(
                                Fore.YELLOW + f"  ⚠  No local ATM for publisher '{pub_strategy}'"
                                f" — using {atm}" + Style.RESET_ALL)
                    parts[11] = atm
            # Translate the instrument (field 2) to its micro contract.
            # Runs on every command that carries an instrument (PLACE,
            # CLOSEPOSITION, REVERSEPOSITION, ...) so exits stay consistent
            # with micro entries; a blank field stays blank.
            if micros and len(parts) >= 3 and parts[2]:
                parts[2] = to_micro_instrument(parts[2])
            return ";".join(parts), ts, signal_id, None
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None, None, None, None


_ati_write_seq = 0
_ati_seq_lock = threading.Lock()


def _next_ati_filename(prefix: str) -> str:
    """Return a unique incoming-folder filename.

    `prefix` must start with "oif": NT classifies incoming files by NAME
    and consumes-then-discards anything not matching oif*.txt with
    "ERROR: Unknown OIF file type" — no order executes. close_*/cancel_*
    names were eaten exactly this way (NT trace 2026-08-10 23:31) while
    two followers stayed in the market. Normalized here rather than
    asserted because crashing a flatten path is worse than fixing the
    name.

    A bare millisecond timestamp isn't unique — two writes in the same
    millisecond (which happens when copy-trading fans one signal out to
    several accounts, or the hard-stop loop closes multiple contracts
    back-to-back) would overwrite each other. A process-local monotonic
    counter guarantees uniqueness, and the lock makes the increment safe
    if filename generation is called from concurrent paths.
    """
    global _ati_write_seq
    if not prefix.startswith("oif"):
        prefix = "oif" + prefix
    with _ati_seq_lock:
        _ati_write_seq += 1
        seq = _ati_write_seq
        # One clock read for both fields: read separately, a write that
        # straddled a second boundary could name itself ..._120000_000
        # after ..._120000_998 and sort before the file written before it.
        now = time.time()
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime(now))
    ms = int((now % 1) * 1000)
    return f"{prefix}_{ts}_{ms:03d}_{seq:04d}.txt"


def write_signal_to_file(signal_text: str) -> str | None:
    """Write the raw signal string (not JSON) to the output directory.

    Returns the absolute path written on success, or None if there was no
    output directory or the write failed — the caller uses this to verify
    each copy-trade fan-out leg actually landed a file.
    """
    if not output_directory:
        return None
    filename = _next_ati_filename("oif")
    filepath = os.path.join(output_directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(signal_text)
        return filepath
    except Exception as exc:
        _dash_set_alert(Fore.RED + f"  ✖  File write error: {exc}" + Style.RESET_ALL)
        return None


# OIF fields that hold identifiers NinjaTrader resolves across the WHOLE
# platform instance, not per account: OCO id (9), order id (10) and ATM
# strategy id (12). CLOSESTRATEGY / CANCEL / CHANGE carry no account field
# at all — the id alone picks the target — so two accounts can never share
# one. When copy-trading reused the leader's strategy id verbatim, NT
# rejected the follower's PLACE with "strategy id 'X' already in use" and
# the follower silently missed the trade (see NT log 2026-07-23 05:10:18).
_ATI_GLOBAL_ID_FIELDS = (9, 10, 12)


def _with_account(signal_text: str, account: str) -> str:
    """Return a copy of a processed signal re-addressed to `account`.

    Field 1 (account) is swapped. When the swap re-addresses the signal to
    a DIFFERENT account than it arrived for — a copy-trade follower leg —
    every instance-global id field (OCO id, order id, strategy id) gets a
    "~<account>" suffix so the leg can't collide with the leader's ids.
    The leader's copy keeps the publisher's ids verbatim. Because the same
    transform runs on every fanned-out command, a later CLOSESTRATEGY /
    CANCEL / CHANGE naming the publisher's id still resolves per account:
    the leader leg matches the original id, each follower leg matches its
    own suffixed one.
    """
    parts = signal_text.split(";")
    if len(parts) < 2:
        return signal_text
    acct = sanitize_ati(account)
    is_follower_leg = parts[1] != acct
    parts[1] = acct
    if is_follower_leg:
        for i in _ATI_GLOBAL_ID_FIELDS:
            if len(parts) > i and parts[i]:
                parts[i] = f"{parts[i]}~{acct}"
    return ";".join(parts)


async def dispatch_signal(raw_signal: str, pub_strategy: str = "",
                          envelope: dict | None = None) -> list[str]:
    """Fan one signal out to every tradeable account through its profile.

    With no profiles configured this is the classic copy-trade fan-out: one
    identical order file per account, written immediately. Profiles can
    reshape a leg (size/qty/direction/ATM), defer it (delay, AI gate,
    staggered entry — those run as background tasks), or skip it. Returns
    the accounts whose file was written NOW; deferred legs report via the
    dashboard and log when they land.
    """
    plans, skipped = plan_signal_legs(raw_signal, pub_strategy, envelope=envelope)
    written = await execute_plans(plans)
    deferred = [p["account"] for p in plans if p["deferred"]]
    if len(target_accounts()) > 1 or deferred or skipped:
        logger.info(f"COPY DISPATCH  wrote={written}  deferred={deferred}  skipped={skipped}")
    return written


# ---------- Risk management ----------
def get_account_limits(account: str) -> dict:
    """Get target/stop for an account from config."""
    cfg = load_config()
    limits = cfg.get("account_limits", {}).get(account, {})
    return {
        "target": limits.get("target", 0),
        "target_mode": limits.get("target_mode", "soft"),
        "stop": limits.get("stop", 0),
        "stop_mode": limits.get("stop_mode", "hard"),
    }


def set_account_limits(account: str, target: float, target_mode: str, stop: float, stop_mode: str):
    """Save target/stop for an account to config."""
    cfg = load_config()
    if "account_limits" not in cfg:
        cfg["account_limits"] = {}
    cfg["account_limits"][account] = {
        "target": target, "target_mode": target_mode,
        "stop": stop, "stop_mode": stop_mode,
    }
    save_config(cfg)


def query_nt_balance(account: str) -> float | None:
    """Query current cash balance for a specific account from ATI."""
    accounts = query_nt_accounts(nt_port)
    for a in accounts:
        if a["name"] == account:
            return a["cash"]
    return None


# NT futures month codes for converting "NQ JUN26" → "NQ 06-26", the
# digit-month-dash format the file-based ATI PLACE/CLOSEPOSITION commands
# actually accept. NT broadcasts the human-readable JUN26 alias over TCP
# but rejects it as an order symbol.
_FUTURES_MONTH_CODES = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _nt_contract_aliases(name: str) -> list[str]:
    """Return contract-name aliases to try on CLOSEPOSITION.

    NT broadcasts positions under many names (NQ JUN26, @NQ, NQM26, NQ M6)
    but the ATI file-based command only accepts one specific format —
    '<root> <MM>-<YY>'. We always try that normalized form first, then
    fall back to the original in case a given install accepts it.
    Aliases are deduplicated while preserving order.
    """
    if not name:
        return []
    aliases: list[str] = []
    normalized = name
    parts = name.strip().split()
    if len(parts) == 2:
        root, suffix = parts
        suffix_up = suffix.upper()
        if "-" not in suffix and len(suffix_up) == 5 and suffix_up[:3] in _FUTURES_MONTH_CODES:
            mm = _FUTURES_MONTH_CODES[suffix_up[:3]]
            yy = suffix_up[3:]
            normalized = f"{root} {mm}-{yy}"
    # Preferred format first
    for a in (normalized, name):
        if a and a not in aliases:
            aliases.append(a)
    return aliases


def _bridge_roundtrip(cmd: dict, timeout: float = 2.0) -> dict | None:
    """One command → one ack against the SocketTraderBridge AddOn.

    Opens a short-lived TCP connection, fires one newline-delimited JSON
    object, and returns the AddOn's parsed ack dict — None when the bridge
    is off/unreachable or no parseable ack came back. The AddOn accepts
    commands on the same socket it uses for the state-push stream;
    one-shot connections keep this code simple and avoid needing to share
    state with live_bridge_task.
    """
    if not (live_bridge_enabled and _live_bridge_connected):
        return None
    host = _nt_host(nt_port)
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, live_bridge_port))
        s.sendall(bridge_auth_line())
        s.sendall((json.dumps(cmd) + "\n").encode("utf-8"))
        # Half-close so the AddOn sees end-of-request but can still reply.
        # A full close() here would RST the socket and could discard the
        # command before the AddOn's reader ever sees it.
        try:
            s.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        # WAIT FOR THE ACK. sendall() only proves the kernel accepted the
        # bytes — not that the AddOn authenticated us, parsed the command,
        # or executed it. Callers treat a reply as "this is done" and skip
        # their fallback, so it must mean the AddOn said so.
        #
        # The ack is NOT the first line back. The AddOn answers every new
        # connection with its state snapshot before its command reader even
        # starts (HandleNewClient writes the snapshot, then spawns
        # ClientReadLoop), and keeps pushing state on account events; the
        # ack is the first line carrying an "ack" key. Reading one line and
        # calling it the ack made every flatten log "REFUSED by AddOn" with
        # a state line as the refusal (live log, 2026-09-17 12:38) while
        # the AddOn had in fact flattened — so the file fallback fired
        # every time and the native path was never confirmed.
        buf = b""
        deadline = time.monotonic() + timeout
        skipped = 0
        while time.monotonic() < deadline:
            s.settimeout(max(0.05, deadline - time.monotonic()))
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break                          # AddOn closed without an ack
            buf += chunk
            while b"\n" in buf:
                raw, buf = buf.split(b"\n", 1)
                line = raw.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (ValueError, json.JSONDecodeError):
                    logger.warning(f"bridge cmd  unparseable line {line[:80]!r} "
                                   f"while waiting for the ack: {cmd}")
                    continue
                if isinstance(obj, dict) and "ack" in obj:
                    return obj
                skipped += 1                   # a state push, not the answer
            if len(buf) > 1_000_000:
                break
        logger.warning(f"bridge cmd UNCONFIRMED (no ack; {skipped} state line(s) "
                       f"read): {cmd}")
        return None
    except OSError as exc:
        logger.warning(f"bridge cmd failed: {cmd} · {exc}")
        return None
    finally:
        if s is not None:
            try:
                s.close()
            except OSError:
                pass


def bridge_send_command(cmd: dict, timeout: float = 2.0) -> bool:
    """Send a command to the AddOn; True only when it acked ok."""
    ack = _bridge_roundtrip(cmd, timeout)
    if ack is None:
        return False
    # AddOn replies {"ack":true|false,"msg":"..."} — see SendAck().
    if not ack.get("ack"):
        logger.warning(f"bridge cmd REFUSED by AddOn: {cmd} · "
                       f"{ack.get('msg') or ack}")
        return False
    logger.info(f"bridge cmd acked: {cmd}")
    return True


def fire_close_position(account: str, contract: str):
    """Write a CLOSEPOSITION command to the incoming folder.

    NT's file-based ATI wants the '<root> <MM>-<YY>' contract format
    even though it broadcasts positions as '<root> <MMM><YY>' (e.g.
    NQ JUN26). We write one command per valid alias — the first NT
    recognizes flattens the position, subsequent ones no-op because
    MarketPosition is already Flat. Harmless and covers both format
    conventions without having to guess which NT version accepts which.
    """
    if not output_directory:
        return
    _ledger_clear(account, contract)   # a targeted flatten voids attribution
    aliases = _nt_contract_aliases(contract)
    for alias in aliases:
        cmd = f"CLOSEPOSITION;{sanitize_ati(account)};{sanitize_ati(alias)};;;;;;;;;;"
        filename = _next_ati_filename("oifclose")
        filepath = os.path.join(output_directory, filename)
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(cmd)
            logger.info(
                f"CLOSEPOSITION  account={account}  contract={alias}  "
                f"file={filename}  (aliases_tried={len(aliases)})")
        except Exception as exc:
            _dash_set_alert(Fore.RED + f"  ✖  Close position write error: {exc}" + Style.RESET_ALL)
            return


# NT order states that still carry (or may still gain) working exposure
# and are therefore worth cancelling when flattening an account.
OPEN_ORDER_STATES = {
    "Working", "Accepted", "Submitted", "PartFilled",
    "TriggerPending", "ChangePending", "PendingSubmit", "PendingChange",
}


def query_nt_open_orders(account: str, port: int = 36973) -> list[str]:
    """Return the NT order IDs currently open on one account.

    The ATI state dump lists Orders|<account> (pipe-joined order IDs) and
    OrderStatus|<id> per order; only IDs in an open state are returned.
    """
    text = _query_ati_complete("POSITIONS", port)
    if not text:
        return []
    ids: list[str] = []
    status: dict[str, str] = {}
    for field, key, val in _parse_ati_fields(text):
        if field == "Orders" and key == account and val:
            ids = [o for o in val.split("|") if o]
        elif field == "OrderStatus":
            status[key] = val
    return [o for o in ids if status.get(o) in OPEN_ORDER_STATES]


def fire_cancel_order(order_id: str):
    """Write a CANCEL for a single order ID to the incoming folder."""
    if not output_directory or not order_id:
        return
    cmd = f"CANCEL;;;;;;;;;;{sanitize_ati(order_id)};;"
    filename = _next_ati_filename("oifcancel")
    filepath = os.path.join(output_directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(cmd)
        logger.info(f"CANCEL  order={order_id}  file={filename}")
    except Exception as exc:
        _dash_set_alert(Fore.RED + f"  ✖  Cancel write error: {exc}" + Style.RESET_ALL)


def fire_cancel_account_orders(account: str, snap: dict | None = None) -> int:
    """Cancel every open order on ONE account, by order ID.

    `snap` is a state dump already in hand (close_account_positions reads
    the open-order ids and the position inventory from ONE dump — they
    used to be two requests for the same bytes); without it, one query.

    Replaces the old CANCELALLORDERS file: per the NT8 OIF docs that
    command "will cancel all active orders across all accounts and broker
    connections" (its account field is not even a parameter), so firing it
    when one copy-trade account trips its stop would strip every OTHER
    account's ATM stop/target while those accounts keep trading. Instead
    the account's open orders are enumerated over the ATI TCP dump and
    cancelled one file each. If the TCP query fails, the CLOSEPOSITION
    calls that follow in close_account_positions still cancel working
    orders on each closed instrument (per the OIF docs), so protection
    degrades per-instrument rather than nuking every account.
    """
    try:
        if snap is not None:
            order_ids = list((snap.get("open_orders") or {}).get(account, []))
        else:
            order_ids = query_nt_open_orders(account, nt_port)
    except Exception as e:
        logger.error(f"cancel_account_orders {account}  query error: {e}")
        return 0
    for oid in order_ids:
        fire_cancel_order(oid)
    if order_ids:
        logger.info(f"CANCEL ACCOUNT ORDERS  account={account}  count={len(order_ids)}  ids={order_ids}")
    return len(order_ids)


def close_account_positions(account: str) -> list[str]:
    """Close every open position on one account.

    When the SocketTraderBridge AddOn is connected, sends a single
    FLATTEN command over the bridge — NT calls `account.Flatten(...)`
    natively, no file-format fragility, no contract-name-alias dance.
    The file-based CLOSEPOSITION path is retained as a fallback for
    when the bridge is disabled or unreachable.

    In either path, cancels working orders first so a pending entry
    can't refill during the close.
    """
    if not account:
        return []

    # One state read serves both the cancel-by-id sweep and the inventory
    # below — the same dump used to be requested twice, back to back, and
    # through the shared stream it is usually already in hand (see
    # _flatten_snapshot). None without the stream: each step queries.
    snap = _flatten_snapshot()

    # Cancel this account's working orders first so a pending entry can't
    # refill after we close. Scoped by order ID — never CANCELALLORDERS,
    # which NT applies globally across all accounts and connections.
    fire_cancel_account_orders(account, snap)
    # Those cancels also void any opening writes still registered as
    # in-flight — a phantom row here would keep re-triggering closes —
    # and a whole-account flatten voids every strategy attribution too.
    _clear_inflight_opens(account)
    _ledger_clear(account)

    closed: set[str] = set()

    # Capture what WAS open so the caller sees the same "Closed: ..."
    # list regardless of which path executed the flatten.
    try:
        positions = (_snapshot_positions(snap, account) if snap is not None
                     else query_nt_positions(account, nt_port))
        for instrument, qty in positions.items():
            if qty != 0 and instrument:
                closed.add(instrument)
    except Exception as e:
        logger.error(f"close_account_positions {account}  query error: {e}")
    # session_contracts is global — every account's instruments, including
    # other accounts' micro conversions. Using it wholesale made a
    # single-account close fire for contracts that account never traded and
    # report them as closed. Keep it only as a safety net for markets this
    # account is actually in, matched on the underlying so a micro/full
    # mismatch still counts.
    live_roots = {_underlying_root(c) for c in closed}
    for contract in session_contracts:
        if not contract:
            continue
        if not live_roots or _underlying_root(contract) in live_roots:
            closed.add(contract)

    # Preferred path: flatten through the AddOn bridge, which calls NT's
    # native account.Flatten() — no order-instruction file, so none of the
    # contract-name-format fragility that the file path has to work around.
    # Scoped to THIS account so the leader-first ordering in
    # close_all_open_positions still holds.
    # Only an ACKNOWLEDGED bridge flatten may skip the file path. Anything
    # else — no ack, refused, unauthenticated, AddOn wedged — must fall
    # through, because returning here while nothing closed would report a
    # successful flatten on a position that is still open.
    if bridge_send_command({"cmd": "flatten", "account": account}):
        logger.info(f"FLATTEN via bridge  account={account}  contracts={sorted(closed)}")
        return sorted(closed)
    if live_bridge_enabled:
        logger.warning(f"FLATTEN  bridge unconfirmed for {account} — "
                       "falling back to CLOSEPOSITION files")

    # Fallback: file-based CLOSEPOSITION per instrument, used whenever the
    # bridge is disabled, unreachable, or did not acknowledge.
    # `closed` is already de-duplicated (it is a set), so a contract that
    # appears in both the live-position query and session_contracts gets
    # exactly one CLOSEPOSITION write.
    for instrument in sorted(closed):
        fire_close_position(account, instrument)

    return sorted(closed)


def close_all_open_positions() -> list[str]:
    """Flatten every open position across all copy-trade target accounts.

    Safety-first: a session stop/target or manual close-all flattens every
    account the signal is copied to, not just the primary. Returns the union
    of contracts closed across accounts (single-account mode falls back to
    just the primary via target_accounts()).

    ORDER IS LOAD-BEARING — the leader is flattened FIRST, sequentially.
    target_accounts() puts the leader at the head of the list; do not
    reorder it and do not parallelise this loop. Rithmic's trade copier
    documents the hazard verbatim ("Always cancel the orders from the
    Leader Account first... If an order is canceled from a Follower
    account before canceling from the Leader Account, it may result in an
    unintended reverse position"), and an unintended position on one
    account while others sit the other way is exactly the cross-account
    hedge prop firms liquidate for.
    """
    all_closed: set[str] = set()
    for account in target_accounts():
        for contract in close_account_positions(account):
            all_closed.add(contract)
    return sorted(all_closed)


# ---------- Manual trading (terminal O key + web UI) ----------
# A manual order is a locally-built PLACE signal pushed through the SAME
# pipeline as a publisher signal: plan_signal_legs → per-account profiles
# (symbol filter, size, qty, ATM override, AI gate) → copy-trade fan-out and
# the round-robin rotation all apply. Manual orders always carry an ATM
# template and a unique signal id, and they respect session locks — but NOT
# pause, which only mutes the publisher: typing an order is deliberate.
_manual_seq = 0
_last_manual: dict = {"instrument": "", "qty": 1}


def build_manual_signal(side, instrument, qty, order_type: str = "market",
                        limit_price=None, atm: str = ""
                        ) -> tuple[str | None, str | None]:
    """Build a canonical manual PLACE signal. Returns (signal, error)."""
    global _manual_seq
    side_l = str(side or "").strip().lower()
    if side_l in ("long", "buy", "b"):
        action = "BUY"
    elif side_l in ("short", "sell", "s"):
        action = "SELL"
    else:
        return None, f"unknown side '{side}' — use long/short"
    instr = sanitize_ati(str(instrument or "").strip().upper())
    if not instr or " " not in instr:
        return None, "instrument needs an expiry, e.g. 'NQ 09-26'"
    try:
        qty_i = int(str(qty).strip())
    except (TypeError, ValueError):
        return None, f"invalid contract count '{qty}'"
    if not 1 <= qty_i <= 1000:
        return None, "contracts must be 1-1000"
    otype_l = str(order_type or "").strip().lower()
    limit_field = ""
    if otype_l in ("market", "m", "mkt"):
        otype = "MARKET"
    elif otype_l in ("limit", "l", "lmt"):
        otype = "LIMIT"
        try:
            price = float(str(limit_price).strip())
        except (TypeError, ValueError):
            return None, "limit orders need a price"
        if not math.isfinite(price) or price <= 0:
            return None, "limit price must be a positive number"
        limit_field = f"{price:.10g}"
    else:
        return None, f"unknown order type '{order_type}' — use market/limit"
    atm_name = sanitize_ati(str(atm or atm_strategy or "").strip())
    if not atm_name:
        return None, "no ATM template — set the session strategy first"
    if not validate_strategy(atm_name):
        return None, f"ATM template '{atm_name}' not installed"
    if micro_mode:
        instr = to_micro_instrument(instr)
    _manual_seq += 1
    sig_id = f"man{time.strftime('%H%M%S')}n{_manual_seq}"
    parts = ["PLACE", active_account or "", instr, action, str(qty_i), otype,
             limit_field, "", "DAY", "", "", atm_name, sig_id]
    err = validate_signal(parts)
    if err:
        return None, err
    return ";".join(parts), None


async def submit_manual_trade(side, instrument, qty, order_type: str = "market",
                              limit_price=None, atm: str = "",
                              source: str = "Manual"
                              ) -> tuple[bool, str]:
    """Fire a manual order through the normal dispatch pipeline.

    Returns (ok, message) for the terminal and web UI alike. Session locks
    block it; pause does not (pause mutes the publisher, not the trader).

    `source` names the surface that fired it and becomes the trade's label
    on the P&L calendar, so hand-fired trades stay separable by where they
    came from instead of collapsing into one bucket.
    """
    global signal_count
    if hard_stopped:
        return False, "session hard-locked — manual trading disabled"
    if not tradeable_accounts():
        return False, "every account is stopped for the session"
    if not is_trade_ready():
        return False, "system not ready — check directory, account and strategy"
    signal, err = build_manual_signal(side, instrument, qty, order_type,
                                      limit_price, atm)
    if err:
        return False, err
    plans, skipped = plan_signal_legs(signal, manual=True, source=source)
    if not plans:
        why = ", ".join(f"{a}: {r}" for a, r in skipped[:3]) or "no eligible accounts"
        return False, f"no legs to fire — {why}"
    signal_count += 1
    _dash_add_signal(format_signal(signal, signal_count, tag="MANUAL"))
    sig_id = signal.split(";")[-1]
    leader_plan = next((p for p in plans if p["account"] == active_account), None)
    pre_pos = 0
    if leader_plan and not leader_plan["deferred"]:
        # From the shared stream: the write below no longer waits on a
        # NinjaTrader dump of its own (0.3–4 s live, on every click).
        pre_pos = await asyncio.to_thread(
            _pre_position, active_account, leader_plan["instrument"])
    written = await execute_plans(plans, sig_id)
    scheduled = [p["account"] for p in plans if p["deferred"]]
    if not written and not scheduled:
        return False, "no order file written — check the output directory"
    _note_contract(signal)
    if leader_plan and not leader_plan["deferred"] and active_account in written:
        add_pending_confirm(leader_plan["signal"], sig_id,
                            leader_plan["instrument"], leader_plan["action"], pre_pos)
    p = signal.split(";")
    desc = f"MANUAL {p[3]} {p[4]} {p[2]} {p[5]}" + (f" @ {p[6]}" if p[6] else "")
    bits = [f"→ {len(written)} account{'s' if len(written) != 1 else ''}"]
    if scheduled:
        bits.append(f"{len(scheduled)} deferred")
    if skipped:
        bits.append(f"{len(skipped)} skipped")
    msg = f"{desc}  {' · '.join(bits)}"
    logger.info(f"{msg}  written={written}  deferred={scheduled}  skipped={skipped}")
    _dash_set_alert(Fore.GREEN + f"  ✔  {msg}" + Style.RESET_ALL)
    return True, msg


async def manual_trade_menu():
    """Terminal manual-order ticket (O key)."""
    global awaiting_user_input
    if hard_stopped:
        _dash_set_alert(Fore.RED + "  ⛔  Hard stop locked — manual trading disabled."
                        + Style.RESET_ALL)
        return
    awaiting_user_input = True
    show_cursor()
    try:
        micro_note = "micros ON — instrument auto-converts" if micro_mode else "micros off"
        _prof_box("MANUAL ORDER", [
            f"Leader {active_account or '—'} + copy/round-robin fan-out",
            f"ATM default: {atm_strategy or '—'} · {micro_note}",
            "Profiles (symbol filter, size, qty, AI) still apply.",
        ], ["ENTER on SIDE cancels."])
        raw_side = (await _ask_line("SIDE  [b]uy-long / [s]ell-short")).lower()
        if not raw_side:
            print(Fore.WHITE + Style.DIM + "  Cancelled." + Style.RESET_ALL)
            return
        default_instr = _last_manual["instrument"] or next(iter(sorted(session_contracts)), "")
        hint = f" [{default_instr}]" if default_instr else " e.g. NQ 09-26"
        instr = (await _ask_line(f"INSTRUMENT{hint}")) or default_instr
        qty = (await _ask_line(f"CONTRACTS [{_last_manual['qty']}]")) or _last_manual["qty"]
        otype = (await _ask_line("TYPE  [m]arket / [l]imit  [m]")).lower() or "m"
        price = None
        if otype.startswith("l"):
            price = await _ask_line("LIMIT PRICE")
        atm = (await _ask_line(f"ATM TEMPLATE [{atm_strategy}]")) or ""
        preview, err = build_manual_signal(raw_side, instr, qty, otype, price, atm)
        if err:
            print(Fore.YELLOW + f"  ⚠  {err}" + Style.RESET_ALL)
            return
        pp = preview.split(";")
        n_extra = len(target_accounts()) - 1
        fan = f" (+{n_extra} more account{'s' if n_extra != 1 else ''})" if n_extra else ""
        confirm = (await _ask_line(
            f"SUBMIT {pp[3]} {pp[4]} {pp[2]} {pp[5]}"
            + (f" @ {pp[6]}" if pp[6] else "") + f" · ATM {pp[11]}{fan}? [y/N]")).lower()
        if confirm != "y":
            print(Fore.WHITE + Style.DIM + "  Cancelled." + Style.RESET_ALL)
            return
        ok, msg = await submit_manual_trade(raw_side, instr, qty, otype, price,
                                            atm, source="Terminal")
        if ok:
            _last_manual["instrument"] = str(instr).strip().upper()
            try:
                _last_manual["qty"] = max(1, int(str(qty).strip()))
            except ValueError:
                pass
            print(Fore.GREEN + f"  ✔  {msg}" + Style.RESET_ALL)
        else:
            print(Fore.YELLOW + f"  ⚠  {msg}" + Style.RESET_ALL)
    finally:
        awaiting_user_input = False


def add_pending_confirm(signal_text: str, sig_id: str | None, instrument: str, action: str, pre_pos: int = 0):
    """Register a signal for post-trade confirmation via position check."""
    pre_balance = session_current_balances.get(active_account) if active_account else None
    with _confirms_lock:
        if len(_pending_confirms) >= MAX_PENDING_CONFIRMS:
            _pending_confirms.pop(0)  # drop oldest
        _pending_confirms.append({
            "signal": signal_text,
            "id": sig_id,
            "instrument": instrument,
            "action": action,
            "ts": time.time(),
            "pre_pos": pre_pos,
            "pre_balance": pre_balance,
        })


def check_pending_confirms():
    """Check pending signals for confirmation via position or balance change.

    Called every balance poll cycle. Position delta catches trades that
    stay open; cash-balance delta catches fast round-trips where the ATM
    stop/target closes the fill before any poll sees the open position.
    """
    if not _pending_confirms or not active_account:
        return

    positions = _positions_now(active_account, BALANCE_POLL_INTERVAL)
    cur_balance = session_current_balances.get(active_account)
    now = time.time()
    still_pending = []

    with _confirms_lock:
        for entry in _pending_confirms:
            elapsed = now - entry["ts"]
            instrument = entry["instrument"]
            pre_pos = entry["pre_pos"]
            pre_balance = entry.get("pre_balance")
            cur_pos = _position_qty(positions, instrument)

            pos_changed = cur_pos != pre_pos
            balance_changed = (
                pre_balance is not None
                and cur_balance is not None
                and abs(cur_balance - pre_balance) > 0.01
            )

            if pos_changed or balance_changed:
                if pos_changed:
                    detail = f"pos: {pre_pos}→{cur_pos}"
                else:
                    delta = cur_balance - pre_balance
                    detail = f"round-trip  balance: ${delta:+.2f}"
                _dash_set_alert(
                    Fore.GREEN +
                    f"  ✔  FILLED {instrument} {entry['action']}  {detail}" +
                    Style.RESET_ALL)
                logger.info(f"CONFIRMED  id={entry['id']}  {instrument}  "
                            f"{entry['action']}  {detail}  "
                            f"elapsed={elapsed:.1f}s")
                continue  # drop from pending

            if elapsed >= CONFIRM_TIMEOUT:
                # Timed out — no position delta and no balance delta
                _dash_set_alert(
                    Fore.YELLOW + Style.DIM +
                    f"  ⚠  No fill detected for {instrument} after {CONFIRM_TIMEOUT}s "
                    f"(ID: {entry['id']})" + Style.RESET_ALL)
                logger.warning(f"UNCONFIRMED  id={entry['id']}  {instrument}  "
                               f"{entry['action']}  pos unchanged at {pre_pos}, "
                               f"balance unchanged  elapsed={elapsed:.1f}s")
                # No position and no money moved, so there is nothing for
                # the P&L record to wait on. Left staged, this dead entry
                # keeps its slot and can hand its market and strategy to an
                # unrelated amount hours later.
                _pnl_drop_staged(active_account, instrument)
                continue  # drop from pending

            still_pending.append(entry)

        _pending_confirms.clear()
        _pending_confirms.extend(still_pending)


_TRIP_STATE = {
    ("stop", "hard"):   ("hard_stop",   Fore.RED,   "⛔", "HARD STOP", "Limit", "⇧X=EXIT"),
    ("stop", "soft"):   ("soft_stop",   Fore.RED,   "⛔", "STOP HIT",  "Limit", "P to resume"),
    ("target", "hard"): ("hard_target", Fore.GREEN, "🎯", "TARGET HIT", "Target", "⇧X=EXIT"),
    ("target", "soft"): ("soft_target", Fore.GREEN, "🎯", "TARGET HIT", "Target", "P to resume"),
}


async def _trip_account(account: str, mode: str, kind: str, pnl: float, limit: float):
    """Lock one account after its session stop/target is hit and flatten it.

    mode is "hard" (locked for the session) or "soft" (resumable via P).
    kind is "stop" or "target". Only the tripped account is flattened —
    other copy-trade accounts keep running. A leading label ("[Sim101] ")
    is shown when more than one account is in play so the user knows which.
    """
    mode = mode if mode in ("hard", "soft") else "soft"
    account_stops[account] = mode
    state, colour, icon, title, limit_word, hint = _TRIP_STATE[(kind, mode)]

    # to_thread: this does up to three blocking NT round-trips, and running
    # them on the event loop stalls signal intake and every other account's
    # risk poll at exactly the moment a limit has tripped.
    closed = await asyncio.to_thread(close_account_positions, account)

    # CONFIRM the flatten. balance_monitor skips accounts already in
    # account_stops, so this account is never looked at again — if the close
    # missed a leg (truncated dump, rejected order), that position would run
    # unmanaged for the rest of the session against the other accounts.
    still = await verify_flat([account])
    if still:
        detail = ", ".join(
            p["instrument"] if p["instrument"] == "UNVERIFIED"
            else f"{p['qty']:+d} {p['instrument']}" for p in still)
        logger.warning(f"TRIP FLATTEN INCOMPLETE  account={account}  {detail}")
        await asyncio.to_thread(close_account_positions, account)   # one retry
        still = await verify_flat([account])
    if still:
        _dash_set_alert(
            Fore.RED + Style.BRIGHT +
            f"  ⛔  {account} STOPPED BUT NOT FLAT — close it in NinjaTrader now"
            + Style.RESET_ALL, sticky=True)
        logger.error(f"TRIP FLATTEN FAILED  account={account}  still open")

    close_str = ", ".join(closed) if closed else "none"

    multi = len(target_accounts()) > 1
    who = f"[{account}] " if multi else ""
    _dash_set_alert(
        colour + Style.BRIGHT +
        f"  {icon}  {who}{title}  P&L: ${pnl:+,.2f}  {limit_word}: ${limit:+,.2f}  "
        f"Closed: {close_str}  — {hint}" + Style.RESET_ALL,
        sticky=True)
    logger.info(
        f"{title} ({mode})  account={account}  kind={kind}  pnl={pnl:.2f}  "
        f"limit={limit}  closed={closed}")
    if mode == "hard":
        save_session_state()


def _recompute_session_lock():
    """Refresh the session-aggregate lock flags from per-account stops.

    hard_stopped / soft_stopped remain the session-level view the header,
    keyboard and listen loop read: the session is hard-locked only when
    EVERY target account is hard-stopped, and once every account has
    stopped (soft or hard) the whole session pauses and its header state
    reflects the strongest lock in effect.
    """
    global hard_stopped, soft_stopped, paused
    targets = target_accounts()
    if not targets:
        hard_stopped = False
        soft_stopped = False
        return
    all_stopped = all(a in account_stops for a in targets)
    hard_stopped = all(account_stops.get(a) == "hard" for a in targets)
    soft_stopped = all_stopped and not hard_stopped
    if all_stopped:
        paused = True
        set_session_state("hard_stop" if hard_stopped else "soft_stop")
# ---------- Live P&L bridge consumer (optional SocketTraderBridge AddOn) ----------
_live_bridge_connected = False
_live_bridge_last_data_ts: float = 0.0  # wall-clock seconds of last parsed line

async def live_bridge_task():
    """Maintain a streaming connection to the SocketTraderBridge AddOn.

    When connected, feeds `equity` (cash + unrealized) from the AddOn's
    JSON stream into session_current_balances[active_account] so the
    existing balance_monitor trips stops/targets mid-trade, and caches
    every full state line as the live book (_bridge_ingest_book) so a
    prop entry can skip its pre-entry ATI snapshot when the book proves
    there is nothing to close. Falls back silently to ATI CashValue
    polling (via balance_monitor) whenever the AddOn is disabled,
    unreachable, or stale.

    Reconnects with exponential backoff up to 30s so NT restarts,
    temporary network hiccups, or AddOn recompiles auto-recover.
    """
    global _live_bridge_connected, _live_bridge_last_data_ts
    backoff = 1.0
    stale_seconds = 12.0  # no data in this long → drop + reconnect

    while not shutdown.is_set():
        if not live_bridge_enabled:
            await asyncio.sleep(2.0)
            backoff = 1.0
            continue

        host = _nt_host(nt_port)
        port = live_bridge_port
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=3.0)
        except (OSError, asyncio.TimeoutError) as exc:
            _live_bridge_connected = False
            # Say something. This path used to be completely silent, so a
            # blocked port (Windows Firewall drops WSL traffic to 36984 by
            # default) produced a session log with no bridge lines at all —
            # indistinguishable from the feature being switched off. Log the
            # first failure and then only on each backoff step, so a long
            # outage does not flood the log.
            if backoff <= 1.0 or backoff >= 30.0:
                logger.warning(
                    f"live bridge unreachable at {host}:{port} — "
                    f"{type(exc).__name__}: {exc or 'timed out'}. "
                    "If NinjaTrader is running with the AddOn compiled, this is "
                    "usually the Windows Firewall blocking the port.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue

        try:
            writer.write(bridge_auth_line())
            await writer.drain()
        except OSError:
            _live_bridge_connected = False
            try:
                writer.close()
            except OSError:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue

        # NOT connected yet: the auth write only proves the kernel took
        # the bytes. A rejected token looks exactly like a clean EOF, so
        # the flag flips only once a line actually comes back — otherwise
        # bridge_send_command would think it had a live bridge.
        got_data = False
        logger.info(f"live bridge socket open → {host}:{port} (awaiting auth)")

        try:
            while not shutdown.is_set():
                try:
                    line = await asyncio.wait_for(
                        reader.readline(), timeout=stale_seconds)
                except asyncio.TimeoutError:
                    logger.info("live bridge stale — reconnecting")
                    break
                if not line:
                    if not got_data:
                        # Closed before sending anything: the AddOn refused
                        # our token (rotated secret, stale AddOn, or no
                        # token file). Back off — retrying instantly here
                        # spins a reconnect storm against the AddOn's
                        # single-threaded accept loop.
                        logger.warning(
                            "live bridge closed before any data — token rejected? "
                            "Re-check the AddOn has SocketTraderBridge.token and "
                            "was recompiled.")
                    else:
                        logger.info("live bridge EOF — reconnecting")
                    break
                if not got_data:
                    got_data = True
                    _live_bridge_connected = True
                    backoff = 1.0
                    logger.info("live bridge authenticated and streaming")
                    # NT is reachable — pull the front months it would
                    # trade per root, so signal-time month correction is a
                    # pure map lookup (see refresh_front_months).
                    asyncio.create_task(_front_months_sync())
                elif _front_months_attempt_date != datetime.now(ET).strftime("%Y-%m-%d"):
                    # New ET day mid-session: contracts roll while the app
                    # runs, so re-sync once per day off the stream's own
                    # traffic (bounded by the attempt date, success or not).
                    asyncio.create_task(_front_months_sync())
                try:
                    obj = json.loads(line.decode("utf-8", errors="ignore"))
                except (ValueError, json.JSONDecodeError):
                    continue
                _live_bridge_last_data_ts = time.time()
                accts = obj.get("accounts") if isinstance(obj, dict) else None
                if not accts:
                    continue
                _bridge_ingest_book(accts)
                for a in accts:
                    name = a.get("name")
                    if not name:
                        continue
                    eq = a.get("equity")
                    if eq is None:
                        continue
                    # Only drive session_current_balances for the active
                    # account — other accounts still get CashValue from
                    # balance_monitor, which is enough for their display.
                    # _ingest_balance quarantines the equity:0 heartbeats NT
                    # keeps streaming while its broker connection is down.
                    if name == active_account:
                        _ingest_balance(name, eq, "bridge")
        except asyncio.CancelledError:
            try: writer.close()
            except Exception: pass
            _live_bridge_connected = False
            return
        except Exception as e:
            logger.error(f"live_bridge_task error: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            _live_bridge_connected = False


async def balance_monitor():
    """Periodically check account balance and enforce target/stop."""
    global paused, soft_stopped, hard_stopped

    while not shutdown.is_set():
        try:
            await asyncio.sleep(BALANCE_POLL_INTERVAL)
        except asyncio.CancelledError:
            return

        try:
            if not active_account:
                continue

            # Poll balances for status bar + realized-cash fallback. When
            # the live bridge AddOn is streaming, it owns the active
            # account's entry in session_current_balances (equity, not
            # cash) so mid-trade unrealized P&L reaches the stop/target
            # check. Other accounts always get ATI's CashValue.
            all_accounts = await asyncio.to_thread(_accounts_now, BALANCE_POLL_INTERVAL)
            for a in all_accounts:
                if (live_bridge_enabled and _live_bridge_connected
                        and a["name"] == active_account):
                    # Bridge owns `current` for the leader — but a session
                    # baseline can still be missing (NT was down at boot,
                    # then the bridge connected), and without one the
                    # enforcement loop below skips the account taking every
                    # trade. Seed it from the ATI cash reading; the seeder
                    # itself refuses outage zeros and never overwrites.
                    _seed_start_balance(a["name"], a["cash"])
                    continue  # bridge is authoritative for `current` here
                if not _ingest_balance(a["name"], a["cash"], "ATI poll"):
                    continue
                # Late baseline: an account with no start yet (NT was down
                # when the session began) gets one from its first real
                # reading — otherwise it would run all session with no P&L
                # and no stop/target enforcement.
                _seed_start_balance(a["name"], a["cash"])
            refresh_controls()

            # Auto-reset P&L at 4:20 PM ET (futures session boundary)
            global _last_auto_reset_date, _balance_poll_count
            now_et = datetime.now(ET)
            today_str = now_et.strftime("%Y-%m-%d")
            past_reset = ((now_et.hour == SESSION_RESET_HOUR and now_et.minute >= SESSION_RESET_MINUTE)
                          or now_et.hour > SESSION_RESET_HOUR)
            if past_reset and _last_auto_reset_date != today_str:
                _last_auto_reset_date = today_str
                reset_session_pnl()
                _dash_set_alert(
                    Fore.CYAN + Style.BRIGHT + "  🔄  Session P&L auto-reset (4:20 PM ET)" + Style.RESET_ALL)
                logger.info("AUTO RESET  session P&L reset at 4:20 PM ET")

            # Prop accounts must be flat before their firm's close deadline.
            # Spawned, not awaited: the close/verify/retry cycle can take
            # tens of seconds per stuck account, and stalling this loop
            # would suspend stop/target enforcement for every other account
            # at exactly the close. The once-per-day markers inside make
            # re-entry safe; the task guard stops overlapping sweeps.
            global _prop_flat_task
            if _prop_flat_task is None or _prop_flat_task.done():
                _prop_flat_task = asyncio.create_task(
                    _check_prop_flat_by_close(now_et))

            # Periodically persist session state for crash recovery
            _balance_poll_count += 1
            if _balance_poll_count >= SESSION_SAVE_INTERVAL:
                _balance_poll_count = 0
                save_session_state()

            if hard_stopped:
                continue

            # Check if pending signals were filled
            if _pending_confirms:
                await asyncio.to_thread(check_pending_confirms)

            # Per-account risk enforcement: each copy-trade account is
            # governed independently by its own target/stop limits. One
            # account tripping only flattens and locks that account; the
            # others keep trading the signal.
            for acct in target_accounts():
                if acct in account_stops:
                    continue  # already locked (hard) or paused-out (soft)
                if acct not in session_start_balances:
                    # An account whose cash legitimately reads ~$0.00 never
                    # seeds a baseline (the zero-refusing seeder cannot
                    # tell it from an outage artifact), so its stop/target
                    # would silently never be enforced. Losing enforcement
                    # must at least be visible: warn once per session when
                    # limits are configured and the feed is alive.
                    if (acct in session_current_balances
                            and acct not in _balance_suspect_since
                            and acct not in _no_baseline_warned):
                        limits = get_account_limits(acct)
                        if limits["target"] or limits["stop"]:
                            _no_baseline_warned.add(acct)
                            logger.warning(
                                f"NO BASELINE  {acct} has limits configured but no "
                                "session baseline (balance reads ~$0.00) — "
                                "stop/target NOT enforced until a nonzero reading")
                            _dash_set_alert(
                                Fore.YELLOW + f"  ⚠  {acct}: no session baseline "
                                "($0.00 balance) — its stop/target is NOT "
                                "enforced until NT reports a nonzero balance."
                                + Style.RESET_ALL, sticky=True)
                    continue
                current = session_current_balances.get(acct)
                if current is None:
                    continue
                limits = get_account_limits(acct)
                if limits["target"] == 0 and limits["stop"] == 0:
                    continue

                # Transfers netted out: a deposit counted as profit pushes a
                # real loss out of the stop's reach, and trips a target that
                # was never traded for.
                pnl = session_pnl(acct, current, risk=True)

                # Check stop (loss limit)
                if limits["stop"] != 0 and pnl <= limits["stop"]:
                    await _trip_account(
                        acct, limits["stop_mode"], "stop", pnl, limits["stop"])
                # Check target (profit goal)
                elif limits["target"] != 0 and pnl >= limits["target"]:
                    await _trip_account(
                        acct, limits["target_mode"], "target", pnl, limits["target"])

            _recompute_session_lock()
        except Exception as e:
            logger.error(f"balance_monitor error: {e}")


async def prompt_limits():
    """Prompt user to set session target/stop for any copy-trade account.

    With followers configured, first asks which account the limits apply
    to — the leader, one follower, or ALL target accounts at once. Before
    this picker existed, limits could only ever be attached to the leader,
    so followers ran without any session risk cap.
    """
    global awaiting_user_input, soft_stopped
    if not active_account:
        _dash_set_alert(Fore.YELLOW + "  ⚠  Set an account first (press A)." + Style.RESET_ALL)
        return

    _dash_enter_menu()
    awaiting_user_input = True
    show_cursor()

    # Pick the account (leader default). "0" applies the entered limits
    # to every target account.
    targets = target_accounts()
    acct = active_account
    apply_all = False
    if len(targets) > 1:
        print(Fore.CYAN + "\n\r\033[K  Set limits for:" + Style.RESET_ALL)
        print(Fore.CYAN + "\r\033[K    0. ALL accounts (same limits for each)" + Style.RESET_ALL)
        for i, a in enumerate(targets, 1):
            if a == active_account:
                role = "leader"
            elif a in follower_accounts:
                role = "follower"
            elif a in roundrobin_accounts:
                role = "round-robin"
            else:
                role = "account"
            print(Fore.CYAN + f"\r\033[K    {i}. {a}  ({role})" + Style.RESET_ALL)
        sys.stdout.write(Fore.WHITE + f"  ACCOUNT # (ENTER = {active_account}) ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw_a = (await asyncio.to_thread(read_line_raw)).strip()
        if raw_a == "0":
            apply_all = True
        elif raw_a.isdigit() and 1 <= int(raw_a) <= len(targets):
            acct = targets[int(raw_a) - 1]
        elif raw_a:
            print(Fore.YELLOW + f"  ⚠  Invalid choice — using {active_account}." + Style.RESET_ALL)

    limits = get_account_limits(acct)
    start_bal = session_start_balances.get(acct)
    current_bal = _held_balance(
        acct, await asyncio.to_thread(query_nt_balance, acct))

    _lim_inner = 52
    _lim_title = f"─ SESSION LIMITS ({'ALL ACCOUNTS' if apply_all else acct}) "
    _lim_top_dashes = _lim_inner - len(_lim_title)
    print(Fore.CYAN + f"\n\r\033[K┌{_lim_title}{'─' * _lim_top_dashes}┐" + Style.RESET_ALL)
    if start_bal is not None and current_bal is not None:
        pnl = current_bal - start_bal
        info = f"Balance: ${current_bal:,.2f}  ·  Session P&L: ${pnl:+,.2f}"
        print(Fore.CYAN + f"\r\033[K│  {info.ljust(_lim_inner)}│" + Style.RESET_ALL)
    if limits["target"] or limits["stop"]:
        cur = f"Target: ${limits['target']:+,.2f} ({limits['target_mode']})  ·  Stop: ${limits['stop']:+,.2f} ({limits['stop_mode']})"
        print(Fore.CYAN + f"\r\033[K│  {cur.ljust(_lim_inner)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  {'When a limit is hit, the lockout mode decides:'.ljust(_lim_inner)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  {'soft = pause signals · press P to resume'.ljust(_lim_inner)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  {'hard = flatten all positions · signals off'.ljust(_lim_inner)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  {'Enter 0 to disable. ENTER to keep current.'.ljust(_lim_inner)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K└{'─' * (_lim_inner + 2)}┘" + Style.RESET_ALL)

    # Target (profit)
    sys.stdout.write(Fore.WHITE + f"  TARGET $ (current: {limits['target']:+,.2f}) ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw_t = await asyncio.to_thread(read_line_raw)
    if raw_t.strip():
        try:
            target = float(raw_t.strip())
            if target < 0:
                print(Fore.YELLOW + "  ⚠  Target must be positive (it's a profit goal). Using absolute value." + Style.RESET_ALL)
                target = abs(target)
        except ValueError:
            print(Fore.YELLOW + "  ⚠  Invalid number — keeping current target." + Style.RESET_ALL)
            target = limits["target"]
    else:
        target = limits["target"]

    # Target mode
    target_mode = limits["target_mode"]
    if target != 0:
        sys.stdout.write(Fore.WHITE + f"  TARGET MODE (current: {limits['target_mode']}) [soft/hard] ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw_tm = await asyncio.to_thread(read_line_raw)
        tm_input = raw_tm.strip().lower()
        if tm_input in ("soft", "s"):
            target_mode = "soft"
        elif tm_input in ("hard", "h"):
            target_mode = "hard"
        elif tm_input:
            print(Fore.YELLOW + f"  ⚠  Invalid mode — keeping {target_mode}." + Style.RESET_ALL)

    # Stop
    sys.stdout.write(Fore.WHITE + f"  STOP $ (current: {limits['stop']:+,.2f}) ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw_s = await asyncio.to_thread(read_line_raw)
    if raw_s.strip():
        try:
            stop = float(raw_s.strip())
            # Positive stop = profit protection — cap at 90% of current P&L
            if stop > 0 and start_bal is not None and current_bal is not None:
                pnl = current_bal - start_bal
                if pnl <= 0:
                    print(Fore.YELLOW + f"  ⚠  Positive stop requires profit. Current P&L: ${pnl:+,.2f}. Use a negative value for loss limit." + Style.RESET_ALL)
                    stop = limits["stop"]
                else:
                    max_stop = round(pnl * 0.9, 2)
                    if stop > max_stop:
                        print(Fore.YELLOW + f"  ⚠  Positive stop capped at 90% of P&L (${max_stop:+,.2f})." + Style.RESET_ALL)
                        stop = max_stop
        except ValueError:
            print(Fore.YELLOW + "  ⚠  Invalid number — keeping current stop." + Style.RESET_ALL)
            stop = limits["stop"]
    else:
        stop = limits["stop"]

    # Stop mode
    stop_mode = limits["stop_mode"]
    if stop != 0:
        sys.stdout.write(Fore.WHITE + f"  STOP MODE (current: {limits['stop_mode']}) [soft/hard] ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw_sm = await asyncio.to_thread(read_line_raw)
        sm_input = raw_sm.strip().lower()
        if sm_input in ("soft", "s"):
            stop_mode = "soft"
        elif sm_input in ("hard", "h"):
            stop_mode = "hard"
        elif sm_input:
            print(Fore.YELLOW + f"  ⚠  Invalid mode — keeping {stop_mode}." + Style.RESET_ALL)

    # Apply to the chosen account, or every target account with "0. ALL".
    # New limits lift a soft (resumable) lockout so the account re-arms
    # against the new numbers; a hard lock stays until reset/exit.
    for a in (targets if apply_all else [acct]):
        set_account_limits(a, target, target_mode, stop, stop_mode)
        if account_stops.get(a) == "soft":
            del account_stops[a]
    soft_stopped = False
    _recompute_session_lock()
    t_label = f"${target:+,.2f} ({target_mode})" if target else "off"
    s_label = f"${stop:+,.2f} ({stop_mode})" if stop else "off"
    who = "ALL accounts" if apply_all else acct
    # Will be visible on alert line after menu exit
    _dash_set_alert(Fore.GREEN + f"  ✔  {who} → Target: {t_label}  ·  Stop: {s_label}" + Style.RESET_ALL)

    awaiting_user_input = False
    _dash_exit_menu()


# ---------- WebSocket listener with reconnection ----------
MAX_BACKOFF = 1800  # 30 minutes in seconds


def fib_backoff(prev: int, curr: int) -> tuple[int, int]:
    """Advance fibonacci sequence, clamped between 60s and MAX_BACKOFF."""
    nxt = prev + curr
    return curr, min(nxt, MAX_BACKOFF)


def fmt_wait(seconds: int) -> str:
    """Format seconds as a human-readable wait time."""
    if seconds >= 60:
        return f"{seconds // 60}m {seconds % 60}s" if seconds % 60 else f"{seconds // 60}m"
    return f"{seconds}s"


async def listen(token: str):
    global signal_count
    fib_prev, fib_curr = 60, 60  # Start at 1m, 1m → 2m → 3m → 5m → ...
    ever_connected = False       # distinguishes first boot from a reconnect
    conn_lost_at = None          # when the current outage began

    await boot_sequence()

    while not shutdown.is_set():
        manual_reconnect = False
        hb_reason = "CONNECTION LOST"  # heartbeat-row label while waiting to retry
        # Header + heartbeat row reflect the attempt in progress
        note_connection_down(reconnecting=ever_connected)
        _dash_set_heartbeat(
            Fore.CYAN + ("  ↻  Reconnecting to server..." if ever_connected
                         else "  ↻  Connecting to server...") + Style.RESET_ALL)
        try:
            # Re-read config each loop so server/token changes via Setup take effect
            cfg = load_config()
            ws_host = cfg.get("ws_host", "")
            if not ws_host:
                logger.error("No ws_host configured — cannot connect")
                return
            token = cfg.get("token", token)
            uri = f"{ws_host}?token={token}"
            connect_start = time.time()
            async with websockets.connect(uri) as ws:
                connect_latency = int((time.time() - connect_start) * 1000)
                baseline_latency = None  # First signal sets the baseline
                fib_prev, fib_curr = 60, 60  # Reset on successful connection

                # How we got here: first boot, or recovery from an outage.
                was_reconnect = ever_connected
                outage_secs = int(time.time() - conn_lost_at) if conn_lost_at is not None else None
                ever_connected = True
                conn_lost_at = None
                note_connection_up()
                note_connected()  # arm the id-less replay guard window

                # Restore persisted session if still in the same trading session
                restored = restore_session_state()

                # Snapshot account balances (realized CashValue) for the
                # session P&L baseline. When an ATM stop or target fills and
                # the position closes, CashValue jumps by the realized delta
                # — the monitor then trips the session limit if crossed.
                nt_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
                for a in nt_accounts:
                    if _ingest_balance(a["name"], a["cash"], "session snapshot"):
                        _seed_start_balance(a["name"], a["cash"])

                # Only announce a restore on first boot — mid-run reconnects
                # re-apply state that never left memory, and the row is
                # better spent confirming the reconnect below.
                if restored and not was_reconnect:
                    pnl_parts = []
                    for name in session_start_balances:
                        cur = session_current_balances.get(name)
                        if cur is not None:
                            pnl_parts.append(f"{name}: ${session_pnl(name, cur):+,.2f}")
                    _dash_set_alert(
                        Fore.CYAN + Style.BRIGHT +
                        f"  🔄  Session P&L restored ({', '.join(pnl_parts) if pnl_parts else 'no data'})" +
                        Style.RESET_ALL)
                    logger.info(f"SESSION RESTORED  id={get_session_id()}  accounts={list(session_start_balances.keys())}")

                # Context-aware welcome message
                missing = []
                if not active_account:
                    missing.append("A = set account")
                if not output_directory:
                    missing.append("D = set output directory")
                if not validate_strategy(atm_strategy):
                    missing.append(f"S = strategy '{atm_strategy}' not found")

                _dash_set_heartbeat(
                    Fore.GREEN + f"  ✔  Connected  ·  Account: {active_account or 'not set'}  ·  Handshake: {connect_latency}ms" + Style.RESET_ALL)
                logger.info(f"CONNECTED  account={active_account}  handshake={connect_latency}ms  strategy={atm_strategy}")
                if missing:
                    _dash_set_alert(
                        Fore.YELLOW + f"  ⚠  Setup incomplete: {', '.join(missing)}" + Style.RESET_ALL,
                        sticky=True)
                elif was_reconnect:
                    down = f"  ·  down {fmt_wait(outage_secs)}" if outage_secs else ""
                    _dash_set_alert(
                        Fore.GREEN + f"  ✔  Reconnected{down}" + Style.RESET_ALL,
                        kind=ALERT_CONN)
                    logger.info(f"RECONNECTED  downtime={outage_secs or 0}s")
                else:
                    # Clean first connect: drop any lingering connection alert
                    _dash_clear_alert(kind=ALERT_CONN)
                reconnect_event.clear()

                while not shutdown.is_set():
                    # Check for manual reconnect request
                    if reconnect_event.is_set():
                        reconnect_event.clear()
                        fib_prev, fib_curr = 60, 60  # Reset backoff on manual reconnect
                        manual_reconnect = True
                        conn_lost_at = time.time()
                        set_session_state("reconnecting")
                        _dash_set_alert(
                            Fore.YELLOW + "  🔄  Dropping connection for reconnect..." + Style.RESET_ALL,
                            kind=ALERT_CONN)
                        break

                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        raw_signal, server_ts, sig_id, reject_reason = extract_signal_string(
                            msg, active_account, atm_strategy, follow_publisher_strategy, micro_mode)
                        if raw_signal:
                            # Compute latency once — used by all paths below
                            lat_str = ""
                            if server_ts:
                                latency_ms = int(time.time() * 1000) - server_ts
                                if baseline_latency is None:
                                    baseline_latency = latency_ms
                                lat_str = f"{latency_ms}ms" if latency_ms < 1000 else f"{latency_ms / 1000:.1f}s"
                                logger.info(f"  latency={latency_ms}ms  baseline={baseline_latency}ms")

                            # Validation-rejected — show in dashboard and skip the trade path
                            if reject_reason:
                                signal_count += 1
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag="REJECTED"))
                                _dash_set_alert(
                                    Fore.RED + f"  ✖  Signal rejected: {reject_reason}" + Style.RESET_ALL)
                                continue

                            # Duplicate detection by signal ID — still show in dashboard so user can see it arrived
                            if sig_id and sig_id in _recent_signal_ids:
                                signal_count += 1
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag="DUPE"))
                                if not paused:
                                    _dash_set_alert(
                                        Fore.YELLOW + Style.DIM +
                                        f"  ⚠  Duplicate signal ignored (ID: {sig_id})" +
                                        Style.RESET_ALL)
                                logger.info(f"DUPLICATE IGNORED  id={sig_id}  signal={raw_signal}")
                                continue
                            if sig_id:
                                _recent_signal_ids.append(sig_id)

                            # Id-less replay guard: after a reconnect the server
                            # re-delivers recent signals; commands without a
                            # signal id (publisher CLOSEPOSITIONs) would bypass
                            # id dedup and fire twice — see 2026-08-07 04:21:58.
                            if not sig_id and _is_idless_replay(raw_signal):
                                signal_count += 1
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag="REPLAY"))
                                _dash_set_alert(
                                    Fore.YELLOW + "  ⚠  Post-reconnect replay blocked — id-less signal "
                                    "already fired. Press C to close manually if it was real."
                                    + Style.RESET_ALL)
                                logger.warning(f"REPLAY BLOCKED  {raw_signal}")
                                continue

                            signal_count += 1

                            # Non-trade states still show the signal in the dashboard, tagged
                            tag = None
                            if hard_stopped:
                                tag = "LOCKED"
                            elif paused:
                                tag = "PAUSED"
                            elif not is_trade_ready():
                                tag = "BLOCKED"
                            elif not tradeable_accounts():
                                # Every target account soft-stopped for the session
                                tag = "LOCKED"

                            if tag:
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag=tag))
                                if tag == "BLOCKED":
                                    _dash_set_alert(
                                        Fore.RED + f"  ✖  Signal blocked — system NOT READY" +
                                        Style.RESET_ALL)
                                    logger.warning(f"SIGNAL #{signal_count} (NOT READY)  {raw_signal}")
                                else:
                                    logger.info(f"SIGNAL #{signal_count} ({tag})  {raw_signal}")
                                continue

                            # Resolve per-account legs. With no profiles this
                            # is one identical leg per account (classic copy
                            # trading); profiles can reshape, defer, or skip
                            # individual accounts' legs.
                            pub_strategy = publisher_strategy_of(msg)
                            plans, skipped_legs = plan_signal_legs(
                                raw_signal, pub_strategy,
                                envelope=publisher_envelope_of(msg))
                            if not plans:
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag="SKIPPED"))
                                why = ", ".join(f"{a}: {r}" for a, r in skipped_legs[:3]) or "no tradeable accounts"
                                _dash_set_alert(
                                    Fore.YELLOW + f"  ⚠  Signal skipped by profiles — {why}" + Style.RESET_ALL)
                                logger.info(f"SIGNAL #{signal_count} (ALL LEGS SKIPPED)  {skipped_legs}  {raw_signal}")
                                continue

                            # Snapshot the LEADER's position BEFORE writing so
                            # fast fills don't make pre/post look identical.
                            # Uses the leader's TRANSFORMED instrument — its
                            # profile may size it differently. Deferred leader
                            # legs snapshot inside their own task instead.
                            leader_plan = next(
                                (p for p in plans if p["account"] == active_account), None)
                            pre_pos = 0
                            if leader_plan and not leader_plan["deferred"]:
                                pre_positions = await asyncio.to_thread(
                                    query_nt_positions, active_account, nt_port)
                                pre_pos = pre_positions.get(leader_plan["instrument"], 0)
                            # Re-check state after the await — balance_monitor could
                            # have fired a soft/hard stop while we were querying NT.
                            # Without this, a signal in-flight during a stop can race
                            # the close and open a new position right after flatten.
                            if paused or not tradeable_accounts():
                                late_tag = "PAUSED" if paused else "LOCKED"
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag=late_tag))
                                logger.info(f"SIGNAL #{signal_count} ({late_tag} post-query)  {raw_signal}")
                                continue
                            written = await execute_plans(plans, sig_id)
                            scheduled = [p["account"] for p in plans if p["deferred"]]
                            if not written and not scheduled:
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag="BLOCKED"))
                                logger.warning(f"SIGNAL #{signal_count} (NO WRITE)  {raw_signal}")
                                continue
                            _note_fired_signal(raw_signal)  # replay-guard memory
                            # Register fill confirmation on the leader if its leg
                            # fired now (deferred leader legs register in-task).
                            if (leader_plan and not leader_plan["deferred"]
                                    and active_account in written):
                                add_pending_confirm(
                                    leader_plan["signal"], sig_id,
                                    leader_plan["instrument"], leader_plan["action"], pre_pos)
                            total_legs = len(written) + len(scheduled)
                            note_bits = []
                            if total_legs > 1:
                                note_bits.append(f"→ {total_legs} accts")
                            rr_account = next(
                                (p["account"] for p in plans if p.get("rr_pick")), None)
                            if rr_account:
                                note_bits.append(f"RR→{rr_account}")
                            if scheduled:
                                note_bits.append(f"{len(scheduled)} deferred")
                            if skipped_legs:
                                note_bits.append(f"{len(skipped_legs)} skip")
                            copy_note = ("  " + " · ".join(note_bits)) if note_bits else ""
                            display_sig = leader_plan["signal"] if leader_plan else raw_signal
                            _dash_add_signal(format_signal(
                                display_sig, signal_count, (lat_str + copy_note).strip()))
                            await signal_pulse("SIGNAL RECEIVED")
                            logger.info(
                                f"SIGNAL #{signal_count}  accounts={written}  "
                                f"deferred={scheduled}  skipped={skipped_legs}  {raw_signal}")
                        else:
                            # Non-signal message (server info, heartbeat, etc.)
                            if not paused:
                                try:
                                    data = json.loads(msg)
                                    await display_server_message(data, connect_latency)
                                except json.JSONDecodeError:
                                    logger.info(f"SERVER RAW  {msg}")
                    except asyncio.TimeoutError:
                        continue

        except websockets.exceptions.InvalidURI:
            _dash_set_alert(Fore.RED + "  ⛔  INVALID SERVER URI" + Style.RESET_ALL,
                            sticky=True)
            return "shutdown"

        except Exception as e:
            if conn_lost_at is None:
                conn_lost_at = time.time()

            # Check for HTTP status rejection (old and new websockets lib)
            http_status = getattr(e, "status_code", None) or getattr(e, "status", None)

            # Check for websocket close code 1008 (policy violation = bad token)
            ws_code = getattr(e, "code", None) or getattr(e, "rcvd", None)
            if ws_code is not None and not isinstance(ws_code, int):
                # newer websockets lib: rcvd is a Close frame
                ws_code = getattr(ws_code, "code", None)

            if http_status is not None and int(http_status) in (401, 403):
                _dash_set_alert(Fore.RED + f"  ⛔  AUTH FAILED (HTTP {http_status})" + Style.RESET_ALL,
                                sticky=True)
                logger.warning(f"AUTH FAILED  http={http_status}")
                return "auth_failed"
            elif ws_code == 1008:
                _dash_set_alert(Fore.RED + "  ⛔  AUTH FAILED (invalid token)" + Style.RESET_ALL,
                                sticky=True)
                logger.warning("AUTH FAILED  ws_code=1008")
                return "auth_failed"
            elif shutdown.is_set():
                break

            # Retryable failure. Ongoing state goes on the header (RECONNECTING)
            # and heartbeat row (live countdown in the wait loop below); the
            # drop itself is recorded as a timestamped event on the alert row.
            note_connection_down()
            if http_status is not None:
                hb_reason = f"CONNECTION ERROR (HTTP {http_status})"
                logger.warning(f"CONNECTION ERROR  http={http_status}  retry={fmt_wait(fib_curr)}")
            else:
                err = (str(e).strip() or type(e).__name__)[:60]
                _dash_set_alert(
                    Fore.RED + f"  ⛔  Connection lost: {err}" + Style.RESET_ALL,
                    kind=ALERT_CONN)
                logger.warning(f"CONNECTION LOST  error={e}  retry={fmt_wait(fib_curr)}")

        if shutdown.is_set():
            break

        # Manual reconnect: brief 3s pause then reconnect (skip fib backoff)
        if manual_reconnect:
            await asyncio.sleep(3)
            continue

        # Fibonacci backoff wait (interruptible by shutdown or manual
        # reconnect) with a live countdown on the heartbeat row.
        wait_end = time.time() + fib_curr
        last_countdown = None
        while time.time() < wait_end:
            if shutdown.is_set():
                return "shutdown"
            if reconnect_event.is_set():
                reconnect_event.clear()
                fib_prev, fib_curr = 60, 60
                _dash_set_alert(
                    Fore.YELLOW + "  🔄  Manual reconnect — resetting backoff." + Style.RESET_ALL,
                    kind=ALERT_CONN)
                break
            remaining = max(0, int(wait_end - time.time()))
            if remaining != last_countdown:
                last_countdown = remaining
                _dash_set_heartbeat(
                    Fore.RED + f"  ⛔  {hb_reason}  ·  retry in {fmt_wait(remaining)}" + Style.RESET_ALL)
            await asyncio.sleep(0.5)

        fib_prev, fib_curr = fib_backoff(fib_prev, fib_curr)

    return "shutdown"


# ---------- Setup (runs before async loop) ----------
# ---------- Strategy template installer ----------
SCRIPT_DIR = Path(__file__).resolve().parent
STRATEGY_FILES = {
    "AtmStrategy": "NQ_Med.xml",
    "StopStrategy": "algoNQmed.xml",
}


def install_strategy_templates(nt_base: Path):
    """Copy ATM and Stop strategy templates into NinjaTrader 8 template dirs.

    nt_base is the NinjaTrader 8 root (parent of incoming/).
    """
    source_dir = SCRIPT_DIR / "strategy"
    if not source_dir.is_dir():
        print(Fore.YELLOW + "  ⚠  strategy/ folder not found — skipping template install." + Style.RESET_ALL)
        return

    for subdir, filename in STRATEGY_FILES.items():
        src = source_dir / filename
        dest_dir = nt_base / "templates" / subdir
        dest = dest_dir / filename

        if not src.exists():
            print(Fore.YELLOW + f"  ⚠  {filename} not found in strategy/ — skipping." + Style.RESET_ALL)
            continue

        if dest.exists():
            # Already installed
            continue

        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dest))
            print(Fore.GREEN + f"  ✔  Installed {filename} → {dest}" + Style.RESET_ALL)
        except OSError as exc:
            print(Fore.RED + f"  ✖  Could not install {filename}: {exc}" + Style.RESET_ALL)


def install_live_bridge_addon(nt_base: Path) -> tuple[bool, str]:
    """Copy SocketTraderBridge.cs from the repo into NT's AddOns folder.

    Returns (ok, message). Three outcomes:
      - Source missing → (False, reason).
      - Dest missing or differs from source → copy (backing up any existing
        local edits as .cs.bak) and return (True, "installed" message).
      - Dest already matches source → (True, "already installed" message).
    Caller surfaces the message to the user.
    """
    source = SCRIPT_DIR / "addon" / "SocketTraderBridge.cs"
    if not source.is_file():
        return False, "addon/SocketTraderBridge.cs not found in repo."
    # Publish the shared secret alongside the AddOn — it refuses every
    # connection until this file exists, so installing one without the
    # other yields a listener that never talks to anyone.
    write_bridge_token()
    dest_dir = nt_base / "bin" / "Custom" / "AddOns"
    dest = dest_dir / "SocketTraderBridge.cs"
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return False, f"cannot create AddOns dir: {exc}"

    try:
        src_bytes = source.read_bytes()
    except OSError as exc:
        return False, f"cannot read source: {exc}"

    if dest.exists():
        try:
            if dest.read_bytes() == src_bytes:
                return True, f"AddOn already installed at {dest}"
        except OSError:
            pass
        # Exists and differs — preserve any local edits before overwriting.
        backup = dest.with_suffix(".cs.bak")
        try:
            shutil.copy2(str(dest), str(backup))
        except OSError:
            pass
    try:
        dest.write_bytes(src_bytes)
        return True, f"AddOn copied to {dest} — F5 in NT to compile."
    except OSError as exc:
        return False, f"copy failed: {exc}"


def _save_server_to_list(cfg: dict, ws_host: str, server_name: str):
    """Ensure the server URL is in the saved servers list."""
    servers = cfg.get("servers", [])
    if not any(s.get("url") == ws_host for s in servers):
        servers.append({"name": server_name, "url": ws_host})
        cfg["servers"] = servers


def setup() -> tuple[str, dict]:
    """Run first-time or repeat setup. Returns (token, config).

    Only server + token are required to connect. Account, directory, and
    strategy can be configured later via the Setup menu (S key) — the app
    will connect but show "setup incomplete" until they are set.
    """
    cfg = load_config()

    print(Fore.GREEN + Style.BRIGHT)
    print("  ╔══════════════════════════════════════════╗")
    print("  ║       VOIDORIGIN  ·  SOCKET TRADER       ║")
    print("  ╚══════════════════════════════════════════╝")
    print(Style.RESET_ALL)

    changed = False

    # 1. Server — required to connect
    if not cfg.get("ws_host"):
        ws_host, server_name = ask_server(cfg)
        cfg["ws_host"] = ws_host
        _save_server_to_list(cfg, ws_host, server_name)
        changed = True

    # 2. Token — required to authenticate
    if not cfg.get("token"):
        token = ask_token(cfg)
        cfg["token"] = token
        changed = True

    # 3. Account — auto-detected via ATI when reachable, manual otherwise.
    if not cfg.get("account"):
        account = ask_account(cfg)
        cfg["account"] = account
        changed = True

    # 4. Output directory — auto-detected on Windows/WSL, manual otherwise.
    if not cfg.get("output_directory"):
        global output_directory
        output_directory = detect_or_ask_directory(cfg)
        if output_directory:
            cfg["output_directory"] = output_directory
            changed = True

    if changed:
        save_config(cfg)

    # Display current config
    print(Fore.GREEN + f"  ✔  Server: {cfg['ws_host']}" + Style.RESET_ALL)
    print(Fore.GREEN + f"  ✔  Token: {'*' * len(cfg.get('token', ''))}" + Style.RESET_ALL)
    if cfg.get("account"):
        print(Fore.GREEN + f"  ✔  Account: {cfg['account']}" + Style.RESET_ALL)
    if cfg.get("output_directory") and Path(cfg["output_directory"]).is_dir():
        print(Fore.GREEN + f"  ✔  Output: {cfg['output_directory']}" + Style.RESET_ALL)
    if not cfg.get("account") or not cfg.get("output_directory"):
        print(Fore.YELLOW + f"  ⚠  Press S after connecting to finish setup." + Style.RESET_ALL)

    # Startup warning: live monitor enabled but AddOn not reachable.
    if cfg.get("live_bridge_enabled"):
        _bridge_port = int(cfg.get("live_bridge_port", 36984))
        if probe_live_bridge(_nt_host(cfg.get("nt_port", 36973)), _bridge_port):
            print(Fore.GREEN +
                  f"  ✔  Live monitor active on port {_bridge_port}." + Style.RESET_ALL)
        else:
            print(Fore.YELLOW +
                  f"  ⚠  Live monitor enabled but AddOn not reachable on "
                  f"port {_bridge_port}." + Style.RESET_ALL)
            print(Fore.YELLOW +
                  "     Press S → 7 for install steps, or disable to silence." +
                  Style.RESET_ALL)

    # Install strategy templates if output directory is configured
    if cfg.get("output_directory") and Path(cfg["output_directory"]).is_dir():
        nt_base = Path(cfg["output_directory"]).parent
        install_strategy_templates(nt_base)

    print()
    return cfg["token"], cfg


# ---------- Embedded web UI ----------
# A localhost-only control panel started alongside the terminal UI. It is a
# stdlib ThreadingHTTPServer (no new dependencies): GET /api/state polls a
# JSON snapshot of the session; every mutating POST hops onto the asyncio
# main loop via run_coroutine_threadsafe so all trading state stays
# single-threaded. Manual orders, pause/resume, flatten, reconnect, micro
# toggle, accounts, strategy, limits and profiles all drive the exact same
# functions as the keyboard.
_WEB_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_web_loop: asyncio.AbstractEventLoop | None = None
_web_httpd = None
_web_url: str | None = None
_web_events: deque[dict] = deque(maxlen=80)


def _web_note(kind: str, text: str):
    """Mirror a dashboard line into the web event feed (ANSI stripped)."""
    plain = _WEB_ANSI_RE.sub("", text).strip()
    if plain:
        _web_events.append({"ts": time.time(), "kind": kind, "text": plain})


def web_state() -> dict:
    """JSON-safe snapshot of everything the web dashboard shows."""
    accounts = []
    for name in target_accounts():
        if name == active_account:
            role = "leader"
        elif name in follower_accounts:
            role = "follower"
        else:
            role = "round-robin"
        start = session_start_balances.get(name)
        current = session_current_balances.get(name)
        pnl = session_pnl(name, current)
        accounts.append({
            "name": name, "role": role, "start": start, "current": current,
            "pnl": pnl, "stop": account_stops.get(name),
            "stale": name in _balance_suspect_since,
            "prop": is_prop_account(name),
            "profile": profile_summary(name),
            "limits": get_account_limits(name),
        })
    atm_templates = list_atm_strategies()   # one disk glob, reused below
    return {
        "version": __version__,
        "state": _session_state,
        "status_text": _WEB_ANSI_RE.sub("", get_session_status_text()),
        "paused": paused, "hard_stopped": hard_stopped, "soft_stopped": soft_stopped,
        "trade_ready": is_trade_ready(),
        "micro_mode": micro_mode,
        "atm_strategy": atm_strategy,
        "follow_publisher_strategy": follow_publisher_strategy,
        "atm_available": atm_templates,
        "accounts": accounts,
        "leader": active_account or "",
        "followers": list(follower_accounts),
        "rr": {"pool": list(roundrobin_accounts),
               "remaining": list(_rr_remaining), "last": _rr_last},
        "signal_count": signal_count,
        "session_contracts": sorted(session_contracts),
        "last_manual": dict(_last_manual),
        "micro_map": dict(micro_map),
        "strategy_symbols": {k: list(v) for k, v in strategy_symbols.items()},
        "strategy_choices": strategy_filter_choices(atm_templates),
        # Raw wire names for the scoped-rule strategy picker: unlike the
        # global filter above, profile rules match the publisher name
        # exactly (case-insensitive), so the picker must offer the names
        # actually seen on the wire, not ATM-template spellings.
        "strategies_seen": list(pub_strategies_seen),
        "rule_defaults": dict(DEFAULT_RULE),
        "catalog": instrument_catalog(),
        "favorites": [f for f in (load_config().get("web_favorites") or [])
                      if isinstance(f, str)][:12],
        "events": list(_web_events),
        # Snapshot (the loop thread clears/repopulates this dict, and
        # json.dumps on the HTTP thread would raise mid-iteration), and
        # mask each AI gate down to its provider name: the write path
        # refuses to accept gates, so the read path should not hand out
        # their endpoints and key env vars — but the rules editor still
        # has to show which rules carry one.
        "profiles": _mask_ai_config(json.loads(json.dumps(account_profiles))),
        "server": _server_name,
        "output_directory": output_directory or "",
        "ts": time.time(),
    }


async def _web_toggle_pause(pause: bool) -> tuple[bool, str]:
    global paused, soft_stopped
    if hard_stopped:
        return False, "session hard-locked — exit to clear"
    if bool(pause) == paused:
        return True, "paused" if paused else "running"
    paused = bool(pause)
    if paused:
        set_session_state("paused")
        _dash_set_alert(Fore.YELLOW + "  ⏸  PAUSED from web UI" + Style.RESET_ALL)
        return True, "paused"
    for a in [k for k, v in account_stops.items() if v == "soft"]:
        del account_stops[a]
    soft_stopped = False
    _recompute_session_lock()
    set_session_state("ready")
    _dash_set_alert(Fore.GREEN + "  ▶  RESUMED from web UI" + Style.RESET_ALL)
    return True, "resumed"


# A flatten is WATCHED, not slept through. Success is answered the moment
# a post-close witness shows nothing open — the live bridge book on its
# next push, or the first complete state dump requested after the closes.
# Failure is not rushed: at least FLATTEN_VERIFY_TRIES post-close reads
# AND (FLATTEN_VERIFY_TRIES + 1) × FLATTEN_VERIFY_DELAY seconds of
# watching before "incomplete", because a fill can take a few seconds
# (2026-09-17: still open at +2 s, flat by +4 s). Without the stream
# (tests, terminal tools) direct dumps are spaced FLATTEN_VERIFY_DELAY
# apart, as before.
FLATTEN_VERIFY_DELAY = 1.5
FLATTEN_VERIFY_TRIES = 3
FLATTEN_VERIFY_MAX_S = 30.0     # cap on watching when NinjaTrader is slow or silent
BRIDGE_VERIFY_MARGIN_S = 0.25   # a book line is post-close only if RECEIVED this long after the closes
# How long one wait for a stream dump may take: the dump in flight when
# the wait begins (up to one cap) plus the qualifying one after it, so a
# slow-but-answering NinjaTrader still yields proof.
VERIFY_SNAPSHOT_WAIT = 2 * SNAPSHOT_TIMEOUT + SNAPSHOT_INTERVAL + 1.0
# The web budget for a flatten: the whole watch at its worst, then some.
# A button that waits this long under a congested NinjaTrader is showing
# the truth; one that times out and cancels the verification mid-flight
# would hide it.
FLATTEN_WEB_TIMEOUT = FLATTEN_VERIFY_MAX_S + 15.0


async def _snapshot_requested_after(after: float, timeout: float) -> dict | None:
    """A state dump whose request went out at or after wall-clock `after`.

    Through the shared stream when it runs — the next dump it takes, so a
    verification adds no second stream to NinjaTrader's ATI — else one
    direct dump once `after` has passed: the pre-stream path, which the
    tests drive through nt_snapshot. Exceptions propagate; each verifier
    logs its own. None means no dump arrived within `timeout`."""
    wait = after - time.time()
    if wait > 0:
        await asyncio.sleep(wait)
    if snapshot_poller_running():
        return await asyncio.to_thread(snapshot_after, after, timeout)
    return await asyncio.to_thread(nt_snapshot, nt_port)


async def verify_flat(accounts: list[str]) -> list[dict]:
    """Re-query NinjaTrader and return positions still open on `accounts`.

    "Close sent" is not "closed". A flatten that lands on some accounts and
    fails on others is worse than no flatten at all: it turns one hedged
    group into an unbalanced one, and prop firms judge cross-account
    direction, not intent. Topstep says so explicitly — a hedge is
    "prohibited even if the overlap is brief or unintentional" and "cannot
    be appealed". So the close is confirmed, not assumed.
    """
    # Minimum watch before "incomplete": (tries + 1) delays, no shorter
    # than the old sleep-then-check cadence's last look (~5.4 s live).
    gone, remaining, confirmed = await _watch_closed(
        lambda positions: [p for p in positions if p["account"] in accounts],
        lambda since: _bridge_book_shows_flat(accounts, since),
        FLATTEN_VERIFY_TRIES, (FLATTEN_VERIFY_TRIES + 1) * FLATTEN_VERIFY_DELAY,
        "FLATTEN VERIFY")
    if gone:
        return []
    if not confirmed:
        logger.warning("FLATTEN VERIFY  never got a complete dump")
        return [{"account": ", ".join(accounts), "instrument": "UNVERIFIED",
                 "qty": 0, "avg_price": None}]
    return remaining


async def _watch_closed(open_in, book_flat, tries: int, min_s: float,
                        label: str) -> tuple[bool, list[dict], bool]:
    """Watch NinjaTrader after closes were written until nothing that
    `open_in(positions)` reports is left. Returns (gone, remaining,
    confirmed); `confirmed` is False when no complete dump ever arrived,
    and an unverifiable state is NOT flat.

    Two post-close witnesses, either one enough for success:
      · the live bridge book — NinjaTrader's own Account.Positions, pushed
        by the AddOn on every position event. A line RECEIVED at least
        BRIDGE_VERIFY_MARGIN_S after the closes went out that shows
        nothing open is proof: the same negative the prop fast path
        already trusts the book for (_bridge_book_snapshot), under the
        same trust rules (bridge streaming, book fresh, account present
        with a believable balance).
      · complete ATI state dumps REQUESTED after the closes, taken one
        after another by the shared stream. The first showing nothing open
        is proof; one still showing a position only means keep watching —
        never "sleep a fixed 1.5 s and look again".
    Failure is not rushed: at least `tries` post-close reads and `min_s`
    of watching, capped by FLATTEN_VERIFY_MAX_S. A read that fails or
    comes back incomplete still counts against `tries`, as it always did,
    so a NinjaTrader that never answers cleanly cannot hold the watch
    open past the cap."""
    since = time.time()
    remaining: list[dict] = []
    confirmed = False
    attempts = 0
    # Without the stream every read is a direct dump, so they are spaced
    # FLATTEN_VERIFY_DELAY apart as before; the stream paces itself.
    spacing = 0.0 if snapshot_poller_running() else FLATTEN_VERIFY_DELAY
    mark = since + spacing         # the next dump must have been requested after this
    while True:
        elapsed = time.time() - since
        if elapsed >= FLATTEN_VERIFY_MAX_S:
            logger.warning(f"{label}  watched {elapsed:.0f}s — giving up")
            break
        if attempts >= tries and elapsed >= min_s:
            break
        if book_flat(since):
            logger.info(f"{label}  bridge book shows nothing open — confirmed")
            return True, [], True
        # The next post-mark dump, checking the book between short waits so
        # its push is not missed while a slow dump is still in flight.
        snap = None
        failed = False
        budget = time.monotonic() + VERIFY_SNAPSHOT_WAIT
        while snap is None and time.monotonic() < budget:
            if book_flat(since):
                logger.info(f"{label}  bridge book shows nothing open — confirmed")
                return True, [], True
            try:
                snap = await _snapshot_requested_after(mark, 0.25)
            except Exception as exc:
                logger.error(f"{label}  snapshot failed: {exc}")
                failed = True
                break
            if time.time() - since >= FLATTEN_VERIFY_MAX_S:
                break
        attempts += 1
        if snap is None:
            if not failed:
                logger.warning(f"{label}  no state dump arrived — cannot confirm")
            mark = time.time() + spacing
            continue
        mark = (snap.get("ts") or time.time()) + spacing
        if not snap.get("ok"):
            # A truncated dump parses cleanly and simply comes back short,
            # so "positions vanished" is indistinguishable from "flat".
            logger.warning(f"{label}  incomplete dump — cannot confirm")
            continue
        confirmed = True
        remaining = open_in(snap["positions"])
        if not remaining:
            return True, [], True
        logger.info(f"{label}  read {attempts}: still open {remaining}")
    return False, remaining, confirmed


async def _confirm_flat(accounts: list[str], closed: list[str]) -> tuple[bool, str]:
    """Shared verdict for every web flatten: verify against NT, then report.

    ALWAYS verifies, even when nothing was closed. An empty list can mean
    "already flat" or "the position query failed and we wrote no closes at
    all" — close_account_positions swallows that error — and those must
    not both report success.
    """
    still = await verify_flat(accounts)
    _live_invalidate()
    if still:
        detail = ", ".join(
            p["instrument"] if p["instrument"] == "UNVERIFIED"
            else f"{p['account']} {p['qty']:+d} {p['instrument']}" for p in still)
        logger.warning(f"FLATTEN INCOMPLETE  {detail}")
        _dash_set_alert(
            Fore.RED + f"  ⛔  FLATTEN INCOMPLETE — still open: {detail}"
            + Style.RESET_ALL, sticky=True)
        return False, (f"FLATTEN INCOMPLETE — still open: {detail}. "
                       "Close these in NinjaTrader now: a group left part "
                       "flat and part in the market is how a cross-account "
                       "hedge gets created.")
    if not closed:
        return True, "verified flat — nothing was open"
    return True, f"flat — closes confirmed for {', '.join(closed)}"


async def _web_close_all() -> tuple[bool, str]:
    accounts = target_accounts()
    closed = await asyncio.to_thread(close_all_open_positions)
    logger.info(f"WEB CLOSE ALL  contracts={closed}")
    if closed:
        _dash_set_alert(Fore.RED + f"  ⛔  WEB CLOSE ALL → {', '.join(closed)}"
                        + Style.RESET_ALL)
    return await _confirm_flat(accounts, closed)


async def _web_toggle_micro() -> tuple[bool, str]:
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    on = toggle_micro_mode()
    refresh_terminal()
    return True, f"micro mode {'ON' if on else 'off'}"


async def _web_set_accounts(leader, followers, robins) -> tuple[bool, str]:
    global active_account, follower_accounts, roundrobin_accounts
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    lead = sanitize_ati(str(leader or "").strip())
    if not lead:
        return False, "leader account required"
    fols: list[str] = []
    for f in (followers or []):
        name = sanitize_ati(str(f).strip())
        if name and name != lead and name not in fols:
            fols.append(name)
    active_account = lead
    follower_accounts = fols
    roundrobin_accounts = sanitize_roundrobin(
        [sanitize_ati(str(r).strip()) for r in (robins or [])], lead, fols)
    _rr_reset_rotation()
    cfg = load_config()
    cfg["account"] = active_account
    cfg["follower_accounts"] = follower_accounts
    cfg["roundrobin_accounts"] = roundrobin_accounts
    save_config(cfg)
    refresh_terminal()
    logger.info(f"WEB ACCOUNTS SET  leader={lead}  followers={fols}  "
                f"roundrobin={roundrobin_accounts}")
    return True, (f"leader {lead} · {len(fols)} follower(s) · "
                  f"{len(roundrobin_accounts)} round-robin")


async def _web_set_strategy(name, follow_publisher=None) -> tuple[bool, str]:
    global atm_strategy, follow_publisher_strategy
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    cfg = load_config()
    msg_bits = []
    if name is not None:
        atm = sanitize_ati(str(name).strip())
        if not atm:
            return False, "strategy name required"
        if not validate_strategy(atm):
            return False, f"template '{atm}' not in templates/AtmStrategy"
        atm_strategy = atm
        cfg["atm_strategy"] = atm
        msg_bits.append(f"strategy {atm}")
    if follow_publisher is not None:
        follow_publisher_strategy = bool(follow_publisher)
        cfg["follow_publisher_strategy"] = follow_publisher_strategy
        msg_bits.append("FOLLOW publisher" if follow_publisher_strategy else "LOCKED")
    save_config(cfg)
    refresh_terminal()
    logger.info(f"WEB STRATEGY  {'; '.join(msg_bits)}")
    return True, " · ".join(msg_bits) or "no change"


async def _web_set_limits(account, target, target_mode, stop, stop_mode) -> tuple[bool, str]:
    # Risk limits are a safety control: a hard-locked session freezes them so
    # they cannot be weakened while a lockout is in force.
    if hard_stopped:
        return False, "session hard-locked — limits are frozen"
    acct = sanitize_ati(str(account or "").strip())
    if not acct:
        return False, "account required"
    modes = {"off", "soft", "hard"}
    t_mode = str(target_mode or "off").strip().lower()
    s_mode = str(stop_mode or "off").strip().lower()
    if t_mode not in modes or s_mode not in modes:
        return False, "mode must be off, soft or hard"
    try:
        t_val, s_val = float(target or 0), float(stop or 0)
    except (TypeError, ValueError) as exc:
        return False, f"invalid limits: {exc}"
    # nan/inf would store a limit that never compares true — pnl <= nan is
    # always False — leaving a stop displayed but permanently dead, and it
    # also serialises as bare NaN which is not valid JSON.
    if not (math.isfinite(t_val) and math.isfinite(s_val)):
        return False, "limits must be finite numbers"
    try:
        set_account_limits(acct, t_val, t_mode, s_val, s_mode)
    except (TypeError, ValueError) as exc:
        return False, f"invalid limits: {exc}"
    logger.info(f"WEB LIMITS  {acct}  target={target}/{target_mode}  stop={stop}/{stop_mode}")
    return True, f"limits saved for {acct}"


def _strip_ai_config(raw):
    """Remove every `ai` key from a profiles payload arriving over HTTP.

    An AI gate names an outbound endpoint and an environment variable whose
    value is sent as a bearer token, so accepting one over the web API would
    turn a single request into "POST my API key to this host". AI gates are
    configured from the terminal editor only; existing ones are preserved
    untouched by _web_set_profiles.
    """
    if isinstance(raw, dict):
        return {k: _strip_ai_config(v) for k, v in raw.items() if k != "ai"}
    if isinstance(raw, list):
        return [_strip_ai_config(v) for v in raw]
    return raw


def _mask_ai_config(raw):
    """Reduce every `ai` config in a profiles payload to its provider name.

    The web UI needs to KNOW a rule carries an AI gate — to show it, and so
    deleting the rule visibly discards it — but the read path must not hand
    out the gate's endpoint or API-key env var (see _strip_ai_config).
    """
    if isinstance(raw, dict):
        out = {}
        for k, v in raw.items():
            if k == "ai":
                if isinstance(v, dict) and v:
                    out[k] = {"provider": str(v.get("provider", ""))}
                continue
            out[k] = _mask_ai_config(v)
        return out
    if isinstance(raw, list):
        return [_mask_ai_config(v) for v in raw]
    return raw


def _extract_ai_hints(raw: dict) -> dict[str, list]:
    """Pop the web rules editor's `_ai_idx` markers out of a profiles payload.

    The editor can reorder, delete, and insert scoped rules, so each rule it
    sends carries the index of the served rule it descends from (-1 on a rule
    added in the editor). Markers are popped here so they never reach the
    config file, and each returned list is aligned with the rules that will
    survive load_account_profiles: a rule that coerces to nothing is skipped
    by both. An account whose rules carry NO markers at all returns no entry,
    which tells _restore_ai_config to fall back to positional matching — the
    pre-marker behavior; an all-new-rules save from the editor still carries
    its -1 markers, so old gates are not resurrected positionally there.
    """
    hints: dict[str, list] = {}
    for acct, prof in raw.items():
        if not isinstance(prof, dict) or not isinstance(prof.get("rules"), list):
            continue
        per: list = []
        saw_marker = False
        for rule in prof["rules"]:
            if not isinstance(rule, dict):
                continue
            idx = rule.pop("_ai_idx", None)
            is_marker = isinstance(idx, int) and not isinstance(idx, bool)
            saw_marker = saw_marker or is_marker
            if _coerce_rule(_strip_ai_config(rule), scoped=True):
                per.append(idx if is_marker else None)
        if per and saw_marker:
            hints[str(acct)] = per
    return hints


def _restore_ai_config(cleaned: dict, previous: dict,
                       hints: dict[str, list] | None = None) -> dict:
    """Carry each rule's existing AI gate across a web profile update.

    Gates only ever come from `previous` (server state), never from the
    wire. Rules are matched through the client's `_ai_idx` markers when the
    account sent any (the web editor reorders, deletes, and inserts rules),
    positionally when it didn't (payloads predating the markers). A marker
    that is out of range or already claimed restores nothing.
    """
    for acct, prof in cleaned.items():
        old = previous.get(acct, {})
        if old.get("default", {}).get("ai") and prof.get("default") is not None:
            prof["default"]["ai"] = old["default"]["ai"]
        old_rules = old.get("rules", [])
        acct_hints = (hints or {}).get(acct)
        claimed: set[int] = set()
        for i, rule in enumerate(prof.get("rules", [])):
            if acct_hints is None:
                src = i
            elif i < len(acct_hints):
                src = acct_hints[i]
            else:
                src = None
            if src is None or not 0 <= src < len(old_rules) or src in claimed:
                continue
            claimed.add(src)
            if old_rules[src].get("ai"):
                rule["ai"] = old_rules[src]["ai"]
    return cleaned


async def _web_set_profiles(raw) -> tuple[bool, str]:
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    if not isinstance(raw, dict):
        return False, "profiles must be a JSON object keyed by account"
    previous = {a: json.loads(json.dumps(p)) for a, p in account_profiles.items()}
    hints = _extract_ai_hints(raw)
    cleaned = load_account_profiles({"account_profiles": _strip_ai_config(raw)})
    cleaned = _restore_ai_config(cleaned, previous, hints)
    account_profiles.clear()
    account_profiles.update(cleaned)
    save_account_profiles()
    refresh_terminal()
    return True, f"profiles saved for {', '.join(sorted(cleaned)) or 'no accounts'}"


async def _web_reset_pnl() -> tuple[bool, str]:
    # reset_session_pnl() clears account_stops and hard_stopped, so without
    # this the web button quietly undoes a tripped risk limit — the one
    # control the app says can only be cleared by exiting.
    if hard_stopped:
        return False, ("session hard-locked — a stop or target was hit. "
                       "Exit and restart to clear it deliberately.")
    # Same path as the terminal's B → R: re-snapshot from the balances the
    # balance monitor already keeps current.
    reset_session_pnl()
    refresh_terminal()
    _dash_set_alert(Fore.GREEN + "  ↺  SESSION P&L RESET from web UI" + Style.RESET_ALL)
    logger.info("WEB RESET P&L")
    return True, "session P&L reset — balances re-snapshotted"


async def _web_close_position(account, instrument) -> tuple[bool, str]:
    acct = sanitize_ati(str(account or "").strip())
    instr = sanitize_ati(str(instrument or "").strip())
    if not acct or not instr:
        return False, "account and instrument required"
    if acct not in target_accounts():
        return False, f"{acct} is not a managed account"
    await asyncio.to_thread(fire_close_position, acct, instr)
    _live_invalidate(fresh=True)
    _dash_set_alert(Fore.RED + f"  ⛔  CLOSE {instr} on {acct} (web)" + Style.RESET_ALL)
    return True, f"close sent — {instr} on {acct}"


async def _web_set_micro_map(raw) -> tuple[bool, str]:
    """Persist micro-contract overrides ({"GC": "MGC"} style) from the web UI."""
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    if not isinstance(raw, dict):
        return False, "micro map must be a JSON object"
    global micro_map
    cfg = load_config()
    overrides = {}
    for k, v in raw.items():
        key = sanitize_ati(str(k).strip().upper())
        val = sanitize_ati(str(v).strip().upper())
        if key and val:
            overrides[key] = val
    cfg["micro_map"] = overrides
    save_config(cfg)
    micro_map = load_micro_map(cfg)
    return True, f"{len(overrides)} micro override(s) saved"


async def _web_set_strategy_symbols(raw) -> tuple[bool, str]:
    """Persist the global strategy → symbol filter from the web UI.

    The UI sends the complete map on every save (like profiles), so an
    entry sanitized down to nothing is a removal.
    """
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    if not isinstance(raw, dict):
        return False, "strategy_symbols must be a JSON object"
    cleaned = load_strategy_symbols({"strategy_symbols": raw})
    strategy_symbols.clear()
    strategy_symbols.update(cleaned)
    save_strategy_symbols()
    return True, (f"{len(cleaned)} strategy filter(s) saved"
                  if cleaned else "strategy filters cleared")


async def _web_set_favorites(raw) -> tuple[bool, str]:
    """Pin the contracts the ticket shows first (starred in the picker)."""
    if not isinstance(raw, list):
        return False, "favorites must be a list"
    favs: list[str] = []
    for item in raw:
        name = sanitize_ati(str(item).strip().upper())
        if name and name not in favs:
            favs.append(name)
    cfg = load_config()
    cfg["web_favorites"] = favs[:12]
    save_config(cfg)
    return True, f"{len(favs[:12])} favourite(s) saved"


_live_cache: dict = {"ts": 0.0, "seq": None, "data": None}
LIVE_ZERO_FREEZE_S = 180        # how long zeroed AccountItems freeze the live view
_live_zeroed_since: float | None = None   # wall-clock start of the current zeroed run
_live_lock = threading.Lock()
LIVE_CACHE_TTL = 1.0   # seconds; several browser tabs share one ATI round-trip (no-stream path)


def _live_invalidate(fresh: bool = False):
    """Drop the derived dashboard view so the next poll rebuilds it (a role
    or profile changed), and with `fresh` also start the stream's next
    dump now (positions just changed: a close or reversal was written)."""
    _live_cache["data"] = None
    if fresh:
        _snapshot_nudge()


def web_live(force: bool = False) -> dict:
    """Live NinjaTrader view for the web UI: every account NT reports, with
    role, cash, realized P&L, session P&L, open positions and working orders.

    This is the panel's source of truth — it shows what NinjaTrader actually
    has right now, including accounts this session does not manage, so the
    grid matches the terminal's balances screen instead of only echoing
    configured names.
    """
    with _live_lock:
        if snapshot_poller_running():
            # Served from the shared stream: this request never waits on
            # NinjaTrader. The rows are rebuilt only when a new dump has
            # landed (or a role/profile edit dropped them) — otherwise the
            # last build is returned as is.
            snap = _snap_latest
            if snap is None:
                snap = snapshot_after(0.0, SNAPSHOT_FIRST_WAIT)   # first poll after start
            if snap is None:
                snap = {"ok": False, "accounts": {}, "positions": [],
                        "working": {}, "ts": time.time(), "seq": None}
            elif (not force and _live_cache["data"] is not None
                    and _live_cache["seq"] == snap.get("seq")):
                return _live_cache["data"]
        else:
            # No stream (terminal tools, the test suite): poll directly,
            # throttled so several tabs still share one round-trip.
            fresh = _live_cache["data"] is not None and (
                time.time() - _live_cache["ts"] < LIVE_CACHE_TTL)
            if fresh and not force:
                return _live_cache["data"]
            try:
                snap = nt_snapshot(nt_port)
            except Exception as exc:
                logger.error(f"WEB LIVE  snapshot failed: {exc}")
                snap = {"ok": False, "accounts": {}, "positions": [],
                        "working": {}, "ts": time.time()}
        # A truncated or failed dump must not be rendered as "these accounts
        # and positions are gone" — that is what made rows blink. Same for a
        # dump that answers with AccountItems zeroed out: that is NT with its
        # broker connection down, not accounts at $0. Serve the last good
        # view instead and let the timestamp show it is stale — but only for
        # a bounded window: past it the zeros are no longer a blip being
        # ridden out but the actual state (one broker connection dropped for
        # the day, an account the firm zeroed), and freezing every OTHER
        # account's positions forever is worse than rendering the truth
        # with held balances.
        global _live_zeroed_since
        zeroed = bool(snap.get("ok")) and any(
            isinstance(info.get("cash"), (int, float))
            and _suspect_zero_balance(name, info["cash"])
            for name, info in snap["accounts"].items())
        if zeroed:
            if _live_zeroed_since is None:
                _live_zeroed_since = time.time()
            zeroed = time.time() - _live_zeroed_since < LIVE_ZERO_FREEZE_S
        elif snap.get("ok"):
            # Only a complete, non-zeroed dump proves recovery. Truncated
            # dumps interleaving with zeroed ones must not restart the
            # freeze clock — that made the "bounded" freeze unbounded.
            _live_zeroed_since = None
        if (not snap.get("ok") or zeroed) and _live_cache.get("last_good"):
            stale = dict(_live_cache["last_good"])
            stale["stale"] = True
            _live_cache.update(ts=time.time(), seq=snap.get("seq"), data=stale)
            return stale

        managed = target_accounts()
        rows = []
        names = list(snap["accounts"]) or list(managed)
        for name in names:
            info = snap["accounts"].get(name, {})
            if name == active_account:
                role = "leader"
            elif name in follower_accounts:
                role = "follower"
            elif name in roundrobin_accounts:
                role = "round-robin"
            else:
                role = ""
            start = session_start_balances.get(name)
            cash = _held_balance(name, info.get("cash"))  # outage zeros → last known
            # Named apart from session_pnl() deliberately: binding the name
            # locally here would shadow the function for the whole scope.
            sess_pnl = session_pnl(name, cash)
            rows.append({
                "name": name,
                "role": role,
                "managed": name in managed,
                "cash": cash,
                "realized": info.get("realized"),
                "session_pnl": sess_pnl,
                "working": snap["working"].get(name, 0),
                "stop": account_stops.get(name),
                "profile": profile_summary(name),
                "limits": get_account_limits(name),
                "positions": [p for p in snap["positions"] if p["account"] == name],
            })
        rows.sort(key=lambda r: (not r["managed"], r["name"]))
        _annotate_sync(rows)
        data = {"ok": snap["ok"], "accounts": rows, "stale": False,
                "positions": snap["positions"], "ts": snap["ts"],
                "totals": _live_totals(rows)}
        _live_cache.update(ts=time.time(), seq=snap.get("seq"), data=data,
                           last_good=data)
        return data


def _position_shape(positions: list[dict]) -> set[tuple[str, int]]:
    """(market, direction) pairs an account holds.

    Copy-traded accounts legitimately differ in SIZE — that is what
    per-account multipliers are for — so sync compares which markets are
    held and on which side, not how many contracts. Micro and full-size
    are the same market here, since an account may be sized to micros.
    """
    shape = set()
    for p in positions:
        root = _underlying_root(p["instrument"])
        if root and p["qty"]:
            shape.add((root, 1 if p["qty"] > 0 else -1))
    return shape


def _annotate_sync(rows: list[dict]):
    """Mark each copy-trade account in/out of sync with the leader.

    Out-of-sync is the failure mode copy trading actually suffers: a
    follower's entry was rejected, or its exit did not land, so it now
    holds something the leader does not (or misses something the leader
    has). Every copier surfaces failures somehow, but none of them show
    the one number a copy trader wants at a glance — how many accounts
    currently match. Round-robin accounts are exempt: holding different
    positions is the entire point of a rotation.
    """
    leader_row = next((r for r in rows if r["role"] == "leader"), None)
    leader_shape = _position_shape(leader_row["positions"]) if leader_row else set()
    for r in rows:
        r["sync_detail"] = ""   # always present so the UI contract is total
        if r["role"] == "leader":
            r["sync"] = "leader"
        elif r["role"] == "follower":
            shape = _position_shape(r["positions"])
            if shape == leader_shape:
                r["sync"] = "in-sync"
            else:
                r["sync"] = "out-of-sync"
                missing = leader_shape - shape
                extra = shape - leader_shape
                bits = []
                if missing:
                    bits.append("missing " + ", ".join(
                        f"{'long' if d > 0 else 'short'} {s}" for s, d in sorted(missing)))
                if extra:
                    bits.append("holds " + ", ".join(
                        f"{'long' if d > 0 else 'short'} {s}" for s, d in sorted(extra)))
                r["sync_detail"] = "; ".join(bits)
        elif r["role"] == "round-robin":
            r["sync"] = "rotation"
        else:
            r["sync"] = ""


def _hedge_conflicts(rows: list[dict]) -> list[dict]:
    """Managed accounts holding the same underlying on OPPOSITE sides.

    Prop firms treat this as hedging and liquidate for it: Apex requires
    all funded accounts trade the same direction and bans offsetting
    positions on the same or correlated instruments; Take Profit Trader's
    rule 6 bans opposite positions across any accounts outright. The check
    is at the UNDERLYING level because MyFunded Futures states plainly
    that E-mini NQ and Micro NQ are the same underlying — so a long MNQ
    against a short NQ is a hedge, not two unrelated trades.

    _position_shape already folds micro into full-size, so comparing
    shapes across accounts gives the underlying-level view for free.
    """
    sides: dict[str, dict[int, list[str]]] = {}
    for r in rows:
        if not r["managed"]:
            continue
        for root, direction in _position_shape(r["positions"]):
            sides.setdefault(root, {}).setdefault(direction, []).append(r["name"])
    conflicts = []
    for root, by_dir in sorted(sides.items()):
        if len(by_dir) > 1:
            conflicts.append({
                "root": root,
                "long": sorted(by_dir.get(1, [])),
                "short": sorted(by_dir.get(-1, [])),
            })
    return conflicts


def _live_totals(rows: list[dict]) -> dict:
    """Roll-up tiles: exposure and copy health across managed accounts."""
    managed = [r for r in rows if r["managed"]]
    followers = [r for r in managed if r["role"] == "follower"]
    return {
        "hedges": _hedge_conflicts(rows),
        "accounts": len(managed),
        "realized": sum(r["realized"] or 0 for r in managed),
        "session_pnl": sum(r["session_pnl"] or 0 for r in managed
                           if r["session_pnl"] is not None),
        "open_positions": sum(len(r["positions"]) for r in managed),
        "contracts": sum(abs(p["qty"]) for r in managed for p in r["positions"]),
        "working": sum(r["working"] or 0 for r in managed),
        "in_sync": sum(1 for r in followers if r["sync"] == "in-sync"),
        "followers": len(followers),
        "out_of_sync": [r["name"] for r in followers if r["sync"] == "out-of-sync"],
        "locked": [r["name"] for r in managed if r["stop"]],
    }


# ---------- Daily P&L history (calendar) ----------
# A durable day-by-day trading record for the web P&L calendar, built from
# data the app already watches: the same /api/live snapshot the dashboard
# renders (ATI cash + positions) and the strategy identity each entry WRITE
# already carries. Nothing new is asked of NinjaTrader.
#
# Two layers, deliberately separate in trustworthiness:
#   · The per-day per-account NET (last cash − first cash of the session
#     date, less money that crossed the account boundary) is authoritative
#     — same source and same definition as the session P&L tile, fees
#     included, no attribution involved.
#   · Trades are the drill-down: when an (account, root) position shrinks
#     between observations, the account's cash delta over that window is
#     attributed to the root(s) that closed. One root closing (the normal
#     case) is exact; several roots closing inside one poll window split
#     the delta by closed contracts and are flagged "approx". The day's
#     Σtrades can still differ from the net by drift — the UI shows that
#     remainder as its own line instead of hiding it.
#
# Every cash movement lands in exactly one of three buckets, because a
# dollar that belongs to none of them is a dollar that makes the headline
# net and the trade-derived stats disagree with no way to see why:
#   · a window that CLOSES something → that trade's P&L;
#   · a window that only OPENS something → entry cost, parked on the
#     position and paid back into the trade when it closes;
#   · a window with no position activity at all → a transfer (deposit,
#     withdrawal, fee sweep), held out of P&L and reported separately.
#
# Days are keyed by get_session_id() (the CME close date), so Sunday-night
# trades land on Monday like every other session number in the app.

# The record is kept as ONE FILE PER MONTH under PNL_DIR, forever. Three
# reasons it is not one growing file:
#   · Nothing is ever deleted. A single file had to be pruned to stay a
#     sane size to load and rewrite; a month only ever rewrites itself, so
#     the archive can keep every year of it for the cost of one small file.
#   · Paging back a month reads that month — about 50 KB — instead of
#     every day ever recorded, so browsing stays flat no matter how deep
#     the history gets.
#   · A file that cannot be parsed costs one month, not the whole record,
#     and is never overwritten unless that month is the live one.
PNL_DIR = Path.home() / ".voidorigin_pnl"
PNL_HISTORY_FILE = Path.home() / ".voidorigin_pnl_history.json"   # legacy, migrated once
PNL_TRACK_INTERVAL = 2.0        # seconds between live-view observations
PNL_SAVE_MIN_GAP_S = 10.0       # batch disk writes; data loss window is tiny
PNL_TRADES_PER_DAY_CAP = 2000   # runaway guard — a real day is nowhere close
PNL_SETTLE_POLLS = 3            # quiet polls before the transfer identity is trusted
PNL_MONTH_CACHE = 6             # past months held in memory while paging
# How long a dispatched entry may sit unfilled before the record stops
# believing in it. NinjaTrader books an entry's commission into RealizedPnL
# before the position reaches the snapshot, so there is always a window
# where money has moved and no position explains it; inside this window it
# is read as that order's entry cost. Generous against CONFIRM_TIMEOUT so a
# slow fill keeps its strategy label, and bounded so a never-filled order
# cannot keep claiming money — or lending its name to it — all session.
PNL_STAGED_TTL_S = 120.0
# How far apart two `unseen` rows may sit and still be read as one
# rollover artifact and its negation. They were minted a poll or two
# apart; this is generous against that and far short of anything a pair
# of real fills would do.
PNL_PHANTOM_PAIR_S = 30.0

_pnl_lock = threading.Lock()    # history is read from web threads, fed by the loop
_pnl_history: dict | None = None            # LIVE month only: {"v": 2, "days": {...}}
_pnl_hot_month: str | None = None           # which month _pnl_history holds
_pnl_cache: "OrderedDict[str, dict]" = OrderedDict()   # past month -> days, for paging
_pnl_migrated = False                       # legacy split attempted this run
_pnl_prev: dict[str, dict] = {}             # account -> {"cash", "shape"} last observed
_pnl_open_meta: dict[tuple[str, str], dict] = {}  # (account, ROOT) -> entry meta
# account -> (session date, the rows written for its most recent close), so
# a fill whose cash lands a poll after the position vanished can still be
# handed to the trade that earned it instead of falling on the floor.
_pnl_recent: dict[str, tuple[str, list[dict]]] = {}
# account -> [cash, realized, consecutive polls unchanged]. Guards the
# transfer identity against being read mid-settlement — see _pnl_observe.
_pnl_stable: dict[str, list] = {}
_pnl_dirty = False
_pnl_last_save = 0.0


_PNL_SHARD_RE = re.compile(r"^\d{4}-\d\d$")


def _pnl_shard(month: str) -> Path:
    return PNL_DIR / f"{month}.json"


def _pnl_dir_ready() -> bool:
    """Make sure the archive directory exists and is private. Never raises."""
    try:
        PNL_DIR.mkdir(parents=True, exist_ok=True)
        if not IS_WINDOWS:
            os.chmod(PNL_DIR, 0o700)
        return True
    except OSError as exc:
        logger.error(f"PNL HISTORY  archive directory unusable ({exc})")
        return False


def _pnl_read_shard(month: str) -> dict:
    """One month's day records, {} when there are none. Never raises.

    A month that will not parse is reported and served empty rather than
    taken as "no history" for everything — and because only the live
    month is ever written, a damaged old month is never overwritten."""
    try:
        with open(_pnl_shard(month), "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(f"PNL HISTORY  {month} could not be read ({exc}) — that "
                     "month shows empty; the file is left alone for recovery")
        return {}
    days = data.get("days") if isinstance(data, dict) else None
    if not isinstance(days, dict):
        return {}
    healed = _pnl_strip_phantom_pairs(days)
    if healed:
        logger.info(
            f"PNL HISTORY  {month}: dropped {healed} rollover phantom row(s) "
            "left by the RealizedPnL counter restart — they cancelled, so "
            "every total is unchanged")
    return days


def _pnl_write_shard(month: str, payload: str):
    """Atomically replace one month's file. Never raises."""
    if not _pnl_dir_ready():
        return
    try:
        fd, tmp = tempfile.mkstemp(dir=str(PNL_DIR), prefix=".pnl", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
            if not IS_WINDOWS:
                os.chmod(tmp, 0o600)
            os.replace(tmp, _pnl_shard(month))
        except BaseException:
            os.unlink(tmp)
            raise
    except OSError as exc:
        logger.error(f"PNL HISTORY  save failed for {month}: {exc}")


def _pnl_months() -> list[str]:
    """Every month on record, oldest first. Never raises."""
    try:
        return sorted(p.stem for p in PNL_DIR.glob("*.json")
                      if _PNL_SHARD_RE.match(p.stem))
    except OSError:
        return []


def _pnl_migrate_legacy():
    """Split the old single-file history into months, once.

    The original file is renamed rather than deleted — it is a trading
    record, and a migration that eats one is not a migration."""
    global _pnl_migrated
    if _pnl_migrated:
        return
    _pnl_migrated = True
    try:
        if not PNL_HISTORY_FILE.exists():
            return
        with open(PNL_HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(f"PNL HISTORY  legacy file not migrated ({exc}) — it is "
                     "left in place untouched")
        return
    days = (data or {}).get("days") or {}
    by_month: dict[str, dict] = {}
    for date, rec in days.items():
        if isinstance(date, str) and len(date) >= 7:
            by_month.setdefault(date[:7], {})[date] = rec
    for month, recs in by_month.items():
        merged = dict(recs)
        merged.update(_pnl_read_shard(month))   # anything already sharded wins
        _pnl_write_shard(month, json.dumps({"v": 2, "days": merged}))
    try:
        PNL_HISTORY_FILE.rename(
            PNL_HISTORY_FILE.with_name(PNL_HISTORY_FILE.name + ".migrated"))
    except OSError:
        pass
    logger.info(f"PNL HISTORY  migrated {len(days)} day(s) into "
                f"{len(by_month)} monthly file(s) under {PNL_DIR}")


def _pnl_hot(month: str) -> dict:
    """Day records for `month`, loaded as the live month. Caller holds the lock.

    Switching months flushes the one being left, so a session that rolls
    over midnight on the 1st cannot strand the previous month's last
    trades in memory."""
    global _pnl_history, _pnl_hot_month, _pnl_dirty
    if _pnl_hot_month == month and _pnl_history is not None:
        return _pnl_history["days"]
    if _pnl_dirty and _pnl_hot_month and _pnl_history is not None:
        _pnl_write_shard(_pnl_hot_month,
                         json.dumps({"v": 2, "days": _pnl_history["days"]}))
        _pnl_dirty = False
    _pnl_migrate_legacy()
    _pnl_hot_month = month
    _pnl_history = {"v": 2, "days": _pnl_read_shard(month)}
    # _pnl_read_shard heals rollover phantoms on the way in; the live month
    # is the one that gets written back, so make sure the cleaned copy is.
    _pnl_dirty = True
    _pnl_cache.pop(month, None)          # the live copy supersedes any cached one
    return _pnl_history["days"]


def _pnl_days(month: str) -> dict:
    """Day records for any month — live from memory, past from disk.
    Caller holds the lock."""
    if _pnl_hot_month == month and _pnl_history is not None:
        return _pnl_history["days"]
    _pnl_migrate_legacy()
    cached = _pnl_cache.get(month)
    if cached is None:
        cached = _pnl_read_shard(month)
        _pnl_cache[month] = cached
        while len(_pnl_cache) > PNL_MONTH_CACHE:
            _pnl_cache.popitem(last=False)
    else:
        _pnl_cache.move_to_end(month)
    return cached


def _pnl_load(month: str | None = None) -> dict:
    """The live month's record, hot-loading `month` when one is named.

    Kept as the way to reach the in-memory day map directly; everything
    that only wants to read a month should use _pnl_month()."""
    with _pnl_lock:
        m = month or _pnl_hot_month or (get_session_id() or "")[:7]
        if not m:
            return {"v": 2, "days": {}}
        _pnl_hot(m)
        return _pnl_history


def _pnl_save(force: bool = False):
    """Atomically persist the live month when dirty (throttled unless forced).

    Only the current month is ever rewritten, so the cost of a save does
    not grow with the depth of the archive."""
    global _pnl_dirty, _pnl_last_save
    with _pnl_lock:
        if _pnl_history is None or not _pnl_dirty or not _pnl_hot_month:
            return
        if not force and time.monotonic() - _pnl_last_save < PNL_SAVE_MIN_GAP_S:
            return
        month = _pnl_hot_month
        payload = json.dumps({"v": 2, "days": _pnl_history["days"]})
        _pnl_dirty = False
        _pnl_last_save = time.monotonic()
    _pnl_write_shard(month, payload)


def _pnl_note_open(account: str, instrument: str, ident: tuple[str, str],
                   manual: bool = False, source: str = "",
                   now: float | None = None):
    """Stage strategy attribution for an entry the write path just fired.

    Called from _ledger_note_file, i.e. inside the dispatch path — it must
    never raise and never do I/O. The observed position (pnl_tracker_task)
    later adopts this label; stacked entries from different strategies on
    one root accumulate, and the close joins them ("A + B").

    Staging also starts a clock. Until a snapshot actually shows the
    position, this entry is the only explanation the record has for money
    NinjaTrader books before the fill becomes visible, and the only name it
    could lend to an orphaned amount — both of which have to expire, or an
    order that never filled goes on claiming trades hours later.

    A hand-fired entry is labelled by the surface it came from, so the
    calendar separates a click in the web UI from one in the terminal.
    Neither can be confused with a trade taken straight off a NinjaTrader
    chart: that one never reaches this function at all, and is recognised
    downstream by having no staged label despite an observed open."""
    try:
        root = _alias_root(instrument).upper()
        if not account or not root:
            return
        label = (ident[1] or "").strip() or (ident[0] or "").strip()
        if not label and manual:
            label = (source or "").strip() or "Manual"
        meta = _pnl_open_meta.setdefault(
            (account, root), {"strategies": [], "ts": None, "avg": None,
                              "cost": 0.0, "staged_ts": None})
        if meta.get("ts") is None:
            # Nothing has been observed on this root yet, so this dispatch
            # is what the record is waiting on. Adding to an already-open
            # position must not restart the clock: that position is real
            # and its meta is no longer pending anything.
            meta["staged_ts"] = time.time() if now is None else now
        if label and label not in meta["strategies"]:
            meta["strategies"].append(label)
    except Exception as exc:      # bookkeeping must never block dispatch
        logger.error(f"PNL HISTORY  note_open failed: {exc}")


def _pnl_note_close(account: str, instrument: str, now: float | None = None):
    """Record that a close was fired for a root that still holds a position.

    Trade rows are cut from the CHANGE in net position between two polls,
    which cannot see a leg that opens and closes inside one interval. Close
    one contract and take another the same way two seconds later and the
    snapshot reads -1 then -1: nothing closed, nothing opened, and two
    round trips collapse into a single row carrying the first one's entry
    price and open time, with a hold spanning both.

    Polling faster would not fix it — there is always an interval. The
    dispatch path is the only place that knows, so it says so here, and
    the observer treats the next open on this root as a fresh leg however
    the quantity happens to line up.

    Never raises and never does I/O: it is called from inside dispatch."""
    try:
        key = (account, _alias_root(instrument).upper())
        meta = _pnl_open_meta.get(key)
        if meta is not None and meta.get("ts") is not None:
            # Only a position the record believes is OPEN can be re-legged.
            meta["close_fired"] = time.time() if now is None else now
    except Exception as exc:      # bookkeeping must never block dispatch
        logger.error(f"PNL HISTORY  note_close failed: {exc}")


def _pnl_strip_phantom_pairs(days: dict) -> int:
    """Drop equal-and-opposite `unseen` rows minted moments apart.

    Before RealizedPnL was recognised as a session-scoped counter, every
    session rollover differenced it across its restart and booked the
    previous session as a fill, then booked its exact negation when the
    settle-up closed the gap that opened. One pair per account per
    rollover, four figures apiece on a one-lot book.

    The cause is fixed, but the rows are already written, and they are
    what a reader sees. They cancel, so removing both leaves every total
    in the archive exactly where it was — which is also the check: a pair
    that does not sum to zero is not one of these and is left alone.

    Runs on load, so the record heals itself on the next restart instead
    of needing a maintenance pass with the trader stopped."""
    dropped = 0
    for rec in days.values():
        trades = (rec or {}).get("trades")
        if not isinstance(trades, list) or len(trades) < 2:
            continue
        uns = [(i, t) for i, t in enumerate(trades)
               if isinstance(t, dict) and t.get("unseen")
               and isinstance(t.get("pnl"), (int, float))
               and isinstance(t.get("ts"), (int, float))]
        drop: set[int] = set()
        for a, (i, t) in enumerate(uns):
            if i in drop or abs(t["pnl"]) < 0.005:
                continue
            for j, u in uns[a + 1:]:
                if (j not in drop
                        and abs(u["ts"] - t["ts"]) <= PNL_PHANTOM_PAIR_S
                        and abs(u["pnl"] + t["pnl"]) < 0.005):
                    drop.update((i, j))
                    break
        if drop:
            rec["trades"] = [t for i, t in enumerate(trades) if i not in drop]
            dropped += len(drop)
    return dropped


def _pnl_staged_pending(account: str, shape: dict[str, int],
                        ts: float) -> list[tuple[tuple[str, str], dict]]:
    """Entries dispatched for `account` that no snapshot has shown yet.

    An entry is pending only while all three hold: no poll has ever seen
    the position (`ts` is None), the current snapshot does not show it, and
    the dispatch is still inside PNL_STAGED_TTL_S. That last clause is the
    whole point — without it a rejected order stays 'pending' forever and
    keeps answering for money it had nothing to do with."""
    out = []
    for key, meta in _pnl_open_meta.items():
        if key[0] != account or key[1] in shape:
            continue
        if meta.get("ts") is not None:
            continue
        staged = meta.get("staged_ts")
        if staged is None or ts - staged > PNL_STAGED_TTL_S:
            continue
        out.append((key, meta))
    return out


def _pnl_expire_staged(account: str, shape: dict[str, int], date: str,
                       trades: list, ts: float) -> bool:
    """Drop dispatched entries that never became a position.

    Anything parked on one is money that did leave the balance, so it is
    handed back to the late-settlement path on the way out rather than
    dropped — the day has to keep adding up even when the guess about
    which order spent it turns out to be wrong."""
    changed = False
    for key in [k for k, m in _pnl_open_meta.items()
                if k[0] == account and k[1] not in shape
                and m.get("ts") is None]:
        meta = _pnl_open_meta[key]
        staged = meta.get("staged_ts")
        # No staged_ts means dispatch never put this here — it is a
        # position adopted at startup, which is real and simply has not
        # been seen closing yet. Only a dispatched entry can go stale.
        if staged is None or ts - staged <= PNL_STAGED_TTL_S:
            continue
        _pnl_open_meta.pop(key, None)
        cost = round(float(meta.get("cost") or 0.0), 2)
        if abs(cost) >= 0.005:
            logger.info(
                f"PNL HISTORY  {account}: {key[1]} never filled — releasing "
                f"${cost:+,.2f} of entry cost back for attribution")
            if _pnl_settle_late(account, cost, date, trades, ts, shape):
                changed = True
    return changed


def _pnl_drop_staged(account: str, instrument: str) -> None:
    """Forget an entry the confirm path just declared dead.

    check_pending_confirms already knows an order produced no position and
    no balance move; without telling the record, that dead entry keeps its
    place in _pnl_open_meta and can later lend its market and strategy to
    an unrelated amount.

    Two things have to hold before anything is dropped. It must have come
    from dispatch — a position adopted at startup carries no staged_ts and
    is real however little is known about it. And the last snapshot must
    not show a position on that root: an order rejected on top of an
    existing position times out exactly the same way, and dropping there
    would take the live trade's entry price, open time and parked cost
    with it."""
    try:
        key = (account, _alias_root(instrument).upper())
        meta = _pnl_open_meta.get(key)
        if meta is None or meta.get("ts") is not None:
            return
        if meta.get("staged_ts") is None:
            return
        if key[1] in ((_pnl_prev.get(account) or {}).get("shape") or {}):
            return
        _pnl_open_meta.pop(key, None)
    except Exception as exc:      # bookkeeping must never block the poll
        logger.error(f"PNL HISTORY  drop_staged failed: {exc}")


def _pnl_shape(row: dict) -> dict[str, int]:
    """Signed contracts per root one live account row currently holds."""
    shape: dict[str, int] = {}
    for p in row.get("positions") or []:
        root = _alias_root(str(p.get("instrument") or "")).upper()
        try:
            qty = int(p.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        if root and qty:
            shape[root] = shape.get(root, 0) + qty
    return shape


def _pnl_avg_price(row: dict, root: str) -> float | None:
    """Average entry price the snapshot reports for one root, if any."""
    for p in row.get("positions") or []:
        if _alias_root(str(p.get("instrument") or "")).upper() == root:
            try:
                avg = float(p.get("avg_price"))
            except (TypeError, ValueError):
                continue
            if math.isfinite(avg) and avg:
                return avg
    return None


_pnl_unclassified: set[tuple[str, str]] = set()   # (date, account) warned


def _pnl_warn_unclassified(date: str, name: str, dcash: float):
    """Say once a day when a cash move cannot be told apart from P&L.

    Without RealizedPnL a flat account whose cash moved is either a
    transfer or a fill nobody attributed, and the two are identical from
    here. The safe reading wins — it stays in the day's P&L — but a
    feature that quietly never fires is worse than one that says it
    cannot decide, so it is said out loud, once."""
    if (date, name) in _pnl_unclassified:
        return
    if len(_pnl_unclassified) > 4000:     # only unbounded across years
        _pnl_unclassified.clear()
    _pnl_unclassified.add((date, name))
    logger.warning(
        f"PNL HISTORY  {name} {date}: ${dcash:+,.2f} moved with the account "
        "flat and no RealizedPnL reported — a transfer and an unattributed "
        "fill are indistinguishable here, so it stays in the day's P&L. "
        "Deposits and withdrawals are only held out for accounts where "
        "NinjaTrader reports RealizedPnL.")


def _pnl_settle_late(account: str, amount: float, date: str,
                     trades: list, ts: float,
                     shape: dict[str, int]) -> bool:
    """Give trading cash that arrived after its close to the trade it made.

    NinjaTrader can drop a position from the snapshot a poll before the
    fill reaches CashValue. The close is then seen with no money attached
    — the tell is a trade row of exactly $0.00 — and the money turns up in
    a later window where no position changed, which used to be discarded.

    What that loses is a whole leg, not a commission. It is why a quiet
    session of a few big trades could drift further from its balance than
    a busy one: the gap follows the size of the fills that settled late,
    never the number of trades.

    With nothing to hand it to — a whole round trip inside polls nobody
    saw, which a truncated ATI dump, an outage or a restart all produce —
    a row is minted for it instead, so the day's trades never stop adding
    up to the day's balance. That row is marked `unseen` and carries only
    what the dispatch path had already staged, so it can never be read as
    an observed fill."""
    have = _pnl_recent.get(account)
    rows = have[1] if have and have[0] == date else None
    if not rows:
        # In-memory state is gone (a restart), but the day's rows are not:
        # the account's last close is still the trade this money came
        # from. Falling back to it is what puts a late-settling winner
        # back on the trade that earned it instead of somewhere else.
        mine = [t for t in trades if t.get("account") == account
                and not t.get("unseen")]
        rows = mine[-1:] if mine else None
    if not rows:
        return _pnl_unseen_row(account, amount, date, trades, ts, shape)
    total = sum(abs(r.get("qty") or 0) for r in rows)
    for r in rows:
        share = (amount if len(rows) == 1 else
                 amount * (abs(r.get("qty") or 0)) / total if total else
                 amount / len(rows))
        r["pnl"] = round(r["pnl"] + share, 2)
        r["late"] = True
    return True


def _pnl_unseen_row(account: str, amount: float, date: str,
                    trades: list, ts: float, shape: dict[str, int]) -> bool:
    """Record realized P&L from a fill no poll ever saw, as its own row.

    Marked `unseen` and filled only from what the dispatch path had
    already staged. The amount is NinjaTrader's own; the market, side and
    size are genuinely unknown and are left empty rather than guessed,
    because a plausible-looking market on a trading record is worse than
    an admitted blank."""
    if len(trades) >= PNL_TRADES_PER_DAY_CAP:
        return False
    # Borrow the market from a staged entry only when the account is flat
    # and exactly one entry is still pending — anything else would be a
    # guess. Pending is the operative word: an order that was rejected
    # hours ago is not evidence about this money, and letting it answer
    # here is how a strategy that never filled ends up owning the day's
    # biggest win.
    pending = _pnl_staged_pending(account, shape, ts) if not shape else []
    key, meta = pending[0] if len(pending) == 1 else (None, {})
    trades.append({
        "ts": round(ts, 3),
        "account": account,
        "symbol": key[1] if key else "",
        "side": "",
        "qty": 0,
        "strategy": " + ".join(meta.get("strategies") or []),
        "entry": meta.get("avg"),
        "opened_ts": meta.get("ts"),
        "pnl": round(amount, 2),
        "approx": True,
        "unseen": True,
    })
    logger.warning(
        f"PNL HISTORY  {account} {date}: ${amount:+,.2f} of realized P&L from "
        "a fill no poll observed — recorded as an unseen trade so the day "
        "still adds up; its market and side are not known")
    return True


def _pnl_realized_restarted(name: str, acct_day: dict, prev: dict,
                            date: str, realized: float | None,
                            shape: dict[str, int]) -> bool:
    """True when RealizedPnL is no longer the counter it was last poll.

    NinjaTrader's RealizedPnL is session-scoped: it restarts at zero with
    the trading session. It is a counter, and the one thing that may never
    be done with a counter is difference it across a restart — the answer
    is the whole previous session, sign inverted, and nothing about it is
    a fill. Once a day at the rollover that produced a pair of `unseen`
    rows per account: one for the phantom delta, and its exact negation
    moments later when the settle-up closed the gap the first had opened.
    Both amounts equal to the account's previous session, which is why a
    quiet day could show a four-figure "largest win" on a one-lot book.

    Two ways the epoch ends. The session date turning over is the expected
    one and needs no evidence — that boundary is already known, and the
    new day's baseline is read off the new counter. The other is
    NinjaTrader restarting at an instant that is not this app's boundary,
    recognised only by its signature: the reading lands on zero, from
    somewhere else, with no position in either snapshot that could have
    closed to put it there.

    Deliberately strict. Missing a restart costs a pair of rows that
    cancel and are excluded from every trade statistic; mistaking a real
    fill for a restart would quietly drop money out of the record, and
    the day would stop adding up to its balance with nothing to say why.

    Re-anchors the day's realized baseline onto the new counter, carrying
    across whatever this day had already booked on the old one, so the
    transfer identity keeps describing this session and not the last."""
    pr = prev.get("realized")
    if realized is None or not isinstance(pr, (int, float)):
        return False
    if prev.get("date") != date:
        # A new session day is a new counter by definition, and the fresh
        # acct_day was seeded from the new one — nothing to re-anchor.
        return True
    if (abs(realized) >= BALANCE_ZERO_EPS or abs(pr) < BALANCE_ZERO_EPS
            or shape or prev.get("shape")):
        return False
    base = acct_day.get("start_realized")
    if isinstance(base, (int, float)):
        acct_day["start_realized"] = round(realized - (pr - base), 2)
    logger.info(
        f"PNL HISTORY  {name}: NinjaTrader's RealizedPnL restarted "
        f"(${pr:+,.2f} to ${realized:+,.2f}) — re-baselined rather than "
        "booked as a fill")
    return True


def _pnl_reconcile(name: str, acct_day: dict, cash: float,
                   realized: float | None, shape: dict[str, int],
                   settled: bool) -> bool:
    """Re-derive how much of an account's balance move was not trading.

    Watching deltas go by can only ever find a transfer the app was
    running for. This asks a different question, and asks it from scratch
    every time: NinjaTrader's RealizedPnL resets with the session and
    books commissions into itself, so across a whole session

        (cash − session baseline) − realized  ==  money from outside

    exactly. A deposit that landed while the app was down, or before any
    of this existed, still falls straight out of that subtraction.

    Derived, never accumulated, so it is idempotent — a restart, a replay
    or a double poll all land on the same number instead of stacking.

    Only while FLAT. An open position has already paid its entry
    commission out of cash while realized still books nothing, and that
    gap is not a transfer; reconciling mid-trade would flicker it into
    the calendar as one. Flat, every fill is settled and the identity is
    exact, so the value simply holds until the position closes.

    Returns True when the stored value changed."""
    if realized is None or shape or not settled or "start" not in acct_day:
        return False
    base_r = acct_day.get("start_realized")
    if base_r is None:
        # Written before this was recorded. The baseline is that session's
        # first reading and NT's realized restarts at the session too, so
        # the pair describes one window and realized began it at zero.
        base_r = 0.0
        acct_day["start_realized"] = 0.0
    adj = round((cash - acct_day["start"]) - (realized - base_r), 2)
    if abs(adj) < 0.005:
        adj = 0.0
    # The dashboard tile and the risk check read the same money from here.
    sb = session_start_balances.get(name)
    if sb is not None:
        session_adjustments[name] = round((cash - sb) - (realized - base_r), 2)
    changed = acct_day.get("adj", 0.0) != adj
    if changed:
        if adj:
            logger.info(
                f"PNL HISTORY  {name}: ${adj:+,.2f} of the session's balance "
                "move is not trading P&L (NinjaTrader's realized accounts "
                "for the rest) — booked as a transfer, held out of every "
                "P&L number")
        acct_day["adj"] = adj
    return changed


def _pnl_settle_session(name: str, acct_day: dict, cash: float,
                        shape: dict[str, int], trades: list,
                        date: str, ts: float, settled: bool) -> bool:
    """Close the gap between an account's trade rows and its day net.

    The backstop, and deliberately the LAST thing each poll does, so the
    precise per-window attribution always gets first refusal — running it
    earlier would mint a row for money the same poll was about to attach
    to a real trade.

    Flat, NinjaTrader's realized is the session's whole trading result, so
    anything the rows do not account for is P&L that never got one: fills
    lost to a truncated ATI dump, an outage, or a restart landing between
    the open and the close. It becomes an `unseen` row rather than a
    number on the unattributed line that nobody can read.

    Needs a baseline and a flat book; mid-trade the cash has already paid
    a commission that realized has not booked, and that is not a gap."""
    # Same quiet-window rule as _pnl_reconcile: mid-settlement the balance
    # is behind the rows, and "closing the gap" would move a trade's P&L
    # back and forth under the reader until the cash caught up.
    if (shape or not settled or "start" not in acct_day
            or "last" not in acct_day):
        return False
    mine = [t for t in trades if t.get("account") == name]
    # Money already parked on an entry that has not reached the snapshot is
    # spent but not yet recordable. It is not a gap, and minting a row for
    # it here would take the commission straight back off the position it
    # was just attached to.
    parked = sum(float(m.get("cost") or 0.0)
                 for _, m in _pnl_staged_pending(name, shape, ts))
    gap = round((cash - acct_day["start"] - (acct_day.get("adj") or 0.0))
                - sum(t["pnl"] for t in mine) - parked, 2)
    if abs(gap) < 0.005:
        return False
    return _pnl_settle_late(name, gap, date, trades, ts, shape)


def _pnl_observe(live: dict, now: float | None = None,
                 session_date: str | None = None):
    """Feed one /api/live view into the daily history.

    Only data that can be trusted moves the record: failed or stale
    snapshots are skipped whole, quarantined balances (_ingest_balance's
    outage guard) freeze their account's diff so a broker-feed drop can
    never mint phantom closes, and out-of-session hours record nothing
    because nothing can fill. A restart adopts open positions without
    inventing trades for them."""
    global _pnl_dirty
    if not isinstance(live, dict) or not live.get("ok") or live.get("stale"):
        return
    date = session_date if session_date is not None else get_session_id()
    if not date:
        return
    ts = now if now is not None else time.time()
    with _pnl_lock:
        # The live month, flushing any previous one on the way in.
        hist_days = _pnl_hot(date[:7])
        # Built detached and only kept if something lands in it — otherwise
        # every quiet poll would mint an empty "$0.00 day" on the calendar.
        day = hist_days.get(date)
        day_is_new = day is None
        if day_is_new:
            day = {"accounts": {}, "trades": []}
        for row in live.get("accounts") or []:
            if not row.get("managed"):
                continue
            name = row.get("name")
            cash = row.get("cash")
            if (not name or not isinstance(cash, (int, float))
                    or not math.isfinite(cash)):
                continue
            if name in _balance_suspect_since:
                continue    # outage-quarantined: hold the diff engine too
            shape = _pnl_shape(row)

            realized = row.get("realized")
            if not isinstance(realized, (int, float)) or not math.isfinite(realized):
                realized = None

            acct_day = day["accounts"].setdefault(name, {})
            # Same seeding rule as every session baseline: a ~$0.00 first
            # reading is an outage artifact or an untradeable account.
            if "start" not in acct_day and abs(cash) > BALANCE_ZERO_EPS:
                acct_day["start"] = cash
                # Pinned together so the two always describe one window.
                acct_day["start_realized"] = realized
                _pnl_dirty = True
            if acct_day.get("last") != cash and "start" in acct_day:
                acct_day["last"] = cash
                _pnl_dirty = True

            prev = _pnl_prev.get(name)
            # Cash and realized only describe the same instant once the
            # feed has gone quiet. Mid-settlement they do not: NT can book
            # a fill into RealizedPnL a poll or more before it reaches
            # CashValue, and reconciling in that window invents a transfer
            # the size of the trade. That is not a cosmetic flicker —
            # session_adjustments feeds the dashboard tile AND
            # balance_monitor's stop check, and a phantom withdrawal there
            # reads as profit, which is the direction that holds a stop
            # off. One still poll is not enough either: a lag that stalls
            # looks exactly like a quiet account, so the pair has to hold
            # still across PNL_SETTLE_POLLS of them before it is believed.
            stable = _pnl_stable.get(name)
            if stable and stable[0] == cash and stable[1] == realized:
                stable[2] += 1
            else:
                stable = _pnl_stable[name] = [cash, realized, 1]
            settled = stable[2] >= PNL_SETTLE_POLLS

            # Before the prev/delta machinery, and independent of it: this
            # is what recognises a deposit the app was not running for.
            if _pnl_reconcile(name, acct_day, cash, realized, shape, settled):
                _pnl_dirty = True

            # Independent of the delta machinery too, and on every path: an
            # order that was dispatched and never became a position has to
            # stop being treated as one, whether or not anything else
            # happened this poll.
            if _pnl_expire_staged(name, shape, date, day["trades"], ts):
                _pnl_dirty = True

            _pnl_prev[name] = {"cash": cash, "shape": shape,
                               "realized": realized, "date": date}
            if prev is None:
                # First sight after boot: adopt what is already open as
                # unattributed rather than pretending it just happened.
                for root, qty in shape.items():
                    meta = _pnl_open_meta.setdefault(
                        (name, root), {"strategies": [], "ts": None,
                                       "avg": None, "cost": 0.0})
                    if meta.get("avg") is None:
                        meta["avg"] = _pnl_avg_price(row, root)
                # Falls through to the session settle-up below: a restart
                # is one of the ways a fill goes unseen in the first place.
                if _pnl_settle_session(name, acct_day, cash, shape,
                                       day["trades"], date, ts, settled):
                    _pnl_dirty = True
                continue

            dcash = cash - prev["cash"]
            # NinjaTrader books a fill's whole result into RealizedPnL the
            # moment it closes, commissions included, so the CHANGE in
            # realized is what the trade made. Cash only agrees eventually
            # — it can lag a poll behind the position disappearing — which
            # is why realized, not the balance delta, decides attribution
            # wherever NT reports it.
            pr = prev.get("realized")
            if _pnl_realized_restarted(name, acct_day, prev, date,
                                       realized, shape):
                pr = None
            dreal = (realized - pr
                     if realized is not None and isinstance(pr, (int, float))
                     else None)
            closes: list[dict] = []
            opens: list[dict] = []
            for root in set(prev["shape"]) | set(shape):
                q0 = prev["shape"].get(root, 0)
                q1 = shape.get(root, 0)
                key = (name, root)
                flipped = bool(q0 and q1 and (q0 > 0) != (q1 > 0))
                # A close was dispatched for this root and a position is
                # still showing. The snapshot cannot tell "never left" from
                # "left and came straight back the same size" — the two are
                # the same arithmetic. Dispatch can, and said so, so this
                # is the far side of a new leg however the quantities line
                # up. Consumed on read: it describes one transition.
                fired = (_pnl_open_meta.get(key) or {}).pop("close_fired", None)
                relegged = bool(q0 and q1 and not flipped
                                and isinstance(fired, (int, float))
                                and ts - fired <= PNL_STAGED_TTL_S)
                if flipped or relegged:
                    closed, opened = abs(q0), abs(q1)
                else:
                    closed = max(0, abs(q0) - abs(q1))
                    opened = max(0, abs(q1) - abs(q0))
                if closed:
                    meta = _pnl_open_meta.get(key) or {}
                    # What it cost to get on rides back out with the
                    # contracts leaving, pro rata, so a partial close hands
                    # back its share and the runner keeps the rest.
                    cost = float(meta.get("cost") or 0.0)
                    share = cost * closed / (abs(q0) or closed)
                    if meta:
                        meta["cost"] = cost - share
                    closes.append({
                        "root": root, "qty": closed,
                        "side": "LONG" if q0 > 0 else "SHORT",
                        "opened_ts": meta.get("ts"),
                        "entry": meta.get("avg"),
                        "strategy": " + ".join(meta.get("strategies") or []),
                        "cost": share,
                    })
                if opened:
                    meta = _pnl_open_meta.setdefault(
                        key, {"strategies": [], "ts": None, "avg": None,
                              "cost": 0.0})
                    if flipped or relegged:
                        # The far side of a reversal — or of a close the
                        # poll never saw — is a fresh trade; only the label
                        # staged by the write that opened it survives.
                        staged = meta["strategies"][-1:]
                        meta.clear()
                        meta.update({"strategies": staged, "ts": None,
                                     "avg": None, "cost": 0.0})
                    if not q0 or flipped or relegged or meta.get("ts") is None:
                        meta["ts"] = ts
                    meta["avg"] = _pnl_avg_price(row, root) or meta.get("avg")
                    opens.append({"key": key, "qty": opened})
                elif q1 and _pnl_open_meta.get(key, {}).get("avg") is None:
                    _pnl_open_meta.setdefault(
                        key, {"strategies": [], "ts": None, "avg": None,
                              "cost": 0.0}
                    )["avg"] = _pnl_avg_price(row, root)
                if not q1:
                    _pnl_open_meta.pop(key, None)

            if not closes:
                # An entry whose position became visible in THIS poll is
                # the same situation as one still invisible: NinjaTrader
                # booked the commission in the very window that showed the
                # fill. Only asking "is the book flat NOW" missed exactly
                # that case, and it is the common one — which is why the
                # first entry of every session still minted a phantom row
                # for its own commission while later ones quietly wore a
                # `late` flag instead.
                cands = [k for k, _ in _pnl_staged_pending(name, shape, ts)]
                cands += [o["key"] for o in opens
                          if (_pnl_open_meta.get(o["key"]) or {})
                          .get("staged_ts") is not None]
                if (dreal is not None and dreal <= -0.005
                        and len(cands) == 1):
                    # NinjaTrader books an entry's commission into
                    # RealizedPnL as the order fills, routinely a poll or
                    # more before the position reaches the snapshot. Read
                    # literally that is "realized moved, nothing closed",
                    # and the branch below would hand it to the PREVIOUS
                    # trade — which is how every row in the log came to
                    # carry the next trade's entry bill and wear a `late`
                    # flag for it, and how the first entry of a session,
                    # with no previous trade to take it, minted a phantom
                    # row for its own commission. It belongs to the
                    # position about to open, so it waits there.
                    #
                    # Exactly one dispatched entry to answer for it, and
                    # only for money LEAVING: an entry costs, it never
                    # pays, so realized moving up here is P&L and belongs
                    # to the late path however recently an order went out.
                    # NOT "only while flat": a second market still held is
                    # no explanation for the debit — nothing closed on it
                    # (a partial exit changes its shape and lands in the
                    # closes branch above) and its ATM moving a stop or
                    # target never touches realized — and gating on the
                    # book being flat before the entry is what put the
                    # 2026-09-24 11:25 row in the log: a Gooping NQ entry
                    # filled on Sim101 while GC-ExhaustM was still held,
                    # and its $2.18 became an 'unobserved fill' (with an
                    # earlier close on the account it would have gone
                    # `late` onto that row instead). Multi-strategy mode
                    # overlaps markets as a matter of course, so that was
                    # the common case, not the edge. What stays ambiguous
                    # — a loss settling late from an earlier close while a
                    # new entry is pending — is the same ambiguity accepted
                    # while flat, and either way the day still adds up.
                    meta = _pnl_open_meta.get(cands[0])
                    if meta is not None:
                        meta["cost"] = round(
                            float(meta.get("cost") or 0.0) + dreal, 2)
                elif dreal is not None and abs(dreal) >= 0.005:
                    # Realized moved with nothing closing here: a fill that
                    # settled after its position had already gone from the
                    # snapshot. Hand it back to the trade it belongs to.
                    if _pnl_settle_late(name, dreal, date, day["trades"],
                                        ts, shape):
                        _pnl_dirty = True
                elif dreal is None and opens and dcash:
                    # Nothing closed, so this cash is what it cost to get
                    # on — commission, and whatever else the broker books
                    # against cash at entry. Park it on the position
                    # instead of dropping it: dropped, every round trip
                    # reads better than the balance does and Σtrades walks
                    # away from the day net by the entry bill.
                    total_o = sum(o["qty"] for o in opens) or 1
                    for o in opens:
                        meta = _pnl_open_meta.get(o["key"])
                        if meta is None:
                            continue
                        meta["cost"] = float(meta.get("cost") or 0.0) + (
                            dcash if len(opens) == 1
                            else dcash * o["qty"] / total_o)
                elif (dcash and realized is None
                        and not prev["shape"] and not shape):
                    # No realized feed, so _pnl_reconcile never runs and a
                    # deposit cannot be told from a fill nobody attributed.
                    # The safe reading keeps the money in the day's P&L.
                    _pnl_warn_unclassified(date, name, dcash)
                if _pnl_settle_session(name, acct_day, cash, shape,
                                       day["trades"], date, ts, settled):
                    _pnl_dirty = True
                continue
            total_q = sum(c["qty"] for c in closes) or 1
            # Realized is exact and already net of every commission — but
            # only of the commissions NinjaTrader booked inside THIS
            # window. An entry's commission was booked when the order
            # filled, polls earlier, so the delta measured across the close
            # does not contain it. Whatever was parked on the position back
            # then has to come back here, for the same reason the balance
            # fallback always needed it: leave it behind and every round
            # trip reads better than the balance does.
            pool = dcash if dreal is None else dreal
            trades = day["trades"]
            fresh: list[dict] = []
            for c in closes:
                if len(trades) >= PNL_TRADES_PER_DAY_CAP:
                    logger.warning("PNL HISTORY  trade cap reached for "
                                   f"{date} — further trades not itemized")
                    break
                pnl = pool if len(closes) == 1 else pool * c["qty"] / total_q
                pnl += c["cost"]
                fresh.append({
                    "ts": round(ts, 3),
                    "account": name,
                    "symbol": c["root"],
                    "side": c["side"],
                    "qty": c["qty"],
                    "strategy": c["strategy"],
                    "entry": c["entry"],
                    "opened_ts": c["opened_ts"],
                    "pnl": round(pnl, 2),
                    "approx": len(closes) > 1,
                })
                trades.append(fresh[-1])
                _pnl_dirty = True
            if fresh:
                # Whatever settles late belongs to these.
                _pnl_recent[name] = (date, fresh)
            # Last word each poll, after everything else had its chance.
            if _pnl_settle_session(name, acct_day, cash, shape,
                                   day["trades"], date, ts, settled):
                _pnl_dirty = True
        if day_is_new and (day["accounts"] or day["trades"]):
            hist_days[date] = day


def _pnl_month(month: str) -> dict:
    """One month of the calendar, rolled up for the web UI.

    Reads only that month's file, so paging back stays the same cost at
    any depth of history."""
    with _pnl_lock:
        src = _pnl_days(month)
        days: dict[str, dict] = {}
        transfers = 0.0
        # Rolled up per account as well as in total, because the per-account
        # view has to keep working on days dropped below — a month whose
        # only record for an account is a deposit still has to show it.
        xfer_by: dict[str, float] = {}
        names: list[str] = []
        for date in sorted(src):
            if not date.startswith(month + "-"):
                continue
            rec = src[date]
            accounts = {}
            net = 0.0
            adj_day = 0.0
            for name, a in (rec.get("accounts") or {}).items():
                start, last = a.get("start"), a.get("last")
                adj = a.get("adj") or 0.0
                # The balance moved by P&L AND by money crossing the
                # boundary; only the first half is a trading result.
                pnl = (round(last - start - adj, 2)
                       if isinstance(start, (int, float))
                       and isinstance(last, (int, float)) else None)
                if pnl is not None:
                    net += pnl
                adj_day += adj
                xfer_by[name] = round(xfer_by.get(name, 0.0) + adj, 2)
                if name not in names:
                    names.append(name)
                accounts[name] = {"start": start, "last": last, "pnl": pnl,
                                  "adj": round(adj, 2)}
            # Counted for the month even when the day itself is dropped
            # below, so a deposit never just disappears from the view.
            transfers += adj_day
            # A connected-but-idle day (baselines seeded, nothing happened)
            # is not a trading day, and neither is one whose only movement
            # was funding. Showing a $0.00 cell for every session the app
            # merely ran would bury the real record; showing a funded day
            # as a green one would invent a record that never happened.
            if not rec.get("trades") and abs(net) < 0.005:
                continue
            days[date] = {
                "pnl": round(net, 2),
                "adj": round(adj_day, 2),
                "accounts": accounts,
                "trades": list(rec.get("trades") or []),
            }
        months = _pnl_months()
        # Earliest day on record, for the UI to stop paging back at.
        oldest = _pnl_days(months[0]) if months else {}
        first = min(oldest) if oldest else None
    return {"ok": True, "month": month, "days": days, "first": first,
            # Oldest month on record, so the UI can stop paging back at the
            # beginning instead of into empty months forever.
            "first_month": months[0] if months else None,
            "transfers": round(transfers, 2),
            "accounts": sorted(names),
            "transfers_accounts": xfer_by,
            "today": datetime.now(ET).strftime("%Y-%m-%d"),
            "session": get_session_id()}


async def pnl_tracker_task():
    """Feed the daily P&L history from the shared live NinjaTrader view.

    Rides web_live()'s 1s cache, so with a dashboard tab open this costs
    zero extra ATI traffic; headless it is one state dump per interval —
    the same class of load as balance_monitor. Purely observational:
    nothing here can touch an order."""
    try:
        while not shutdown.is_set():
            try:
                await asyncio.sleep(PNL_TRACK_INTERVAL)
            except asyncio.CancelledError:
                return
            try:
                live = await asyncio.to_thread(web_live)
                _pnl_observe(live)
                await asyncio.to_thread(_pnl_save)
            except Exception as exc:
                logger.error(f"pnl_tracker error: {exc}")
    finally:
        # Best effort on the way out — the window since the last throttled
        # save is at most a few seconds of bookkeeping.
        try:
            _pnl_save(force=True)
        except Exception:
            pass


async def _web_set_sizing(account, mode, value) -> tuple[bool, str]:
    """Set one account's contract sizing straight from the grid.

    A per-account multiplier on the leader's contract count is the
    dominant idiom in futures copy trading, so it belongs in the grid
    rather than three clicks deep in a profile editor.
    """
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    acct = sanitize_ati(str(account or "").strip())
    mode = str(mode or "").strip().lower()
    if not acct:
        return False, "account required"
    if mode not in ("copy", "fixed", "multiple"):
        return False, "mode must be copy, fixed or multiple"
    prof = account_profiles.setdefault(acct, {})
    rule = prof.setdefault("default", {})
    rule["qty_mode"] = mode
    note = ""
    if mode == "copy":
        rule.pop("qty_value", None)
    else:
        try:
            val = float(value)
        except (TypeError, ValueError):
            return False, f"invalid size value '{value}'"
        lo, hi = RULE_CLAMPS["qty_value"]
        rule["qty_value"] = max(lo, min(val, hi))
        if mode == "multiple" and rule["qty_value"] < 0.5:
            # _rule_qty rounds half up, so anything under 0.5 sizes a
            # single-contract signal to zero and the leg is skipped.
            note = "  ⚠  below ×0.5 a 1-contract signal sizes to 0 and is skipped"
    save_account_profiles()
    _live_invalidate()
    label = "copy" if mode == "copy" else (
        f"×{rule['qty_value']:g}" if mode == "multiple" else f"fixed {int(rule['qty_value'])}")
    logger.info(f"WEB SIZING  {acct} -> {label}")
    return True, f"{acct} → {label}{note}"


async def _web_reverse_position(account, instrument) -> tuple[bool, str]:
    """Reverse one account's position — NinjaTrader's `Rev` action.

    Writes a REVERSEPOSITION for that account only; NT closes the current
    position and opens the same size on the other side. Deliberately
    account-scoped rather than fanned out, so it cannot silently flip a
    whole copy set from a single click.
    """
    acct = sanitize_ati(str(account or "").strip())
    instr = sanitize_ati(str(instrument or "").strip())
    if not acct or not instr:
        return False, "account and instrument required"
    if acct not in target_accounts():
        return False, f"{acct} is not a managed account"
    if hard_stopped:
        return False, "session hard-locked"
    if acct in account_stops:
        return False, f"{acct} is stopped for the session"
    if is_prop_account(acct) and _prop_entry_blocked_now(acct):
        return False, (f"{acct} is a prop account inside its flat-by-close "
                       "window — a reversal would open a position nothing "
                       "re-flattens today. Close it instead.")
    live = await asyncio.to_thread(web_live)
    if live.get("stale"):
        # The frozen last-good view can be minutes old; sizing a live
        # REVERSEPOSITION from it can reverse a quantity that no longer
        # exists and open an unintended net position.
        return False, ("live view is stale — NinjaTrader has not confirmed "
                       "current positions; refusing to size a reversal from it")
    pos = next((p for p in live["positions"]
                if p["account"] == acct and p["instrument"] == instr), None)
    if not pos:
        return False, f"no open {instr} position on {acct}"
    action = "SELL" if pos["qty"] > 0 else "BUY"
    signal = (f"REVERSEPOSITION;{acct};{instr};{action};{abs(pos['qty'])};"
              f"MARKET;;;DAY;;;{sanitize_ati(atm_strategy)};")
    err = validate_signal(signal.split(";"))
    if err:
        return False, f"could not build reversal: {err}"
    written = await asyncio.to_thread(write_signal_to_file, signal)
    _live_invalidate(fresh=True)
    if not written:
        return False, "order file write failed — check the output directory"
    logger.info(f"WEB REVERSE  {acct}  {instr}  {action} {abs(pos['qty'])}")
    _dash_set_alert(Fore.YELLOW + f"  ⇄  REVERSE {instr} on {acct}" + Style.RESET_ALL)
    if is_prop_account(acct):
        # One position at a time: sweep any OTHER market this prop account
        # still holds, trailing the reversal (exits are never delayed).
        # cross_account — this reversal was account-scoped, so another
        # prop account left on the now-opposite side of the product group
        # would be exactly the cross-account hedge the firms close accounts
        # for, and no other leg of this action exists to handle it.
        task = asyncio.create_task(_prop_reversal_cleanup(
            [{"account": acct, "instrument": instr, "action": action}],
            cross_account=True))
        _leg_tasks.add(task)
        task.add_done_callback(_leg_tasks.discard)
    return True, f"{acct}: reversing {instr} ({action} {abs(pos['qty'])})"


async def _web_flatten_account(account) -> tuple[bool, str]:
    """Flatten one account (its positions and working orders)."""
    acct = sanitize_ati(str(account or "").strip())
    if not acct:
        return False, "account required"
    if acct not in target_accounts():
        return False, f"{acct} is not a managed account"
    closed = await asyncio.to_thread(close_account_positions, acct)
    _live_invalidate(fresh=True)
    logger.info(f"WEB FLATTEN  account={acct}  contracts={closed}")
    _dash_set_alert(Fore.RED + f"  ⛔  FLATTEN {acct} (web)" + Style.RESET_ALL)
    # Same contract as FLATTEN ALL: never answer the button from the
    # request alone. This path used to reply "close sent" — true even
    # when NT discarded every close file and the account stayed in the
    # market (2026-08-10, two followers left holding NQ).
    ok, msg = await _confirm_flat([acct], closed)
    return ok, f"{acct}: {msg}"


async def _web_set_role(account, role) -> tuple[bool, str]:
    """Assign one account's role without rebuilding the whole set.

    Clicking a role in the grid is the fast path for "add this account to
    the rotation" / "stop copying to this one", so it edits in place rather
    than making the user restate every account.
    """
    global active_account, follower_accounts, roundrobin_accounts
    if hard_stopped:
        return False, "session hard-locked — settings are frozen"
    acct = sanitize_ati(str(account or "").strip())
    role = str(role or "").strip().lower()
    if not acct:
        return False, "account required"
    if role not in ("leader", "follower", "round-robin", "off"):
        return False, "role must be leader, follower, round-robin or off"

    follower_accounts = [a for a in follower_accounts if a != acct]
    roundrobin_accounts = [a for a in roundrobin_accounts if a != acct]
    if role == "leader":
        if active_account and active_account != acct:
            # the outgoing leader keeps trading as a follower rather than
            # silently dropping out of the copy set
            follower_accounts = _dedup_accounts([active_account], follower_accounts)
        active_account = acct
    elif role == "follower":
        if acct == active_account:
            return False, f"{acct} is the leader — promote another account first"
        follower_accounts = _dedup_accounts(follower_accounts, [acct])
    elif role == "round-robin":
        if acct == active_account:
            return False, f"{acct} is the leader — promote another account first"
        roundrobin_accounts = sanitize_roundrobin(
            roundrobin_accounts + [acct], active_account, follower_accounts)
    else:
        if acct == active_account:
            return False, "cannot unassign the leader — promote another account first"
    _rr_reset_rotation()
    cfg = load_config()
    cfg["account"] = active_account
    cfg["follower_accounts"] = follower_accounts
    cfg["roundrobin_accounts"] = roundrobin_accounts
    save_config(cfg)
    _live_invalidate()
    refresh_terminal()
    logger.info(f"WEB ROLE  {acct} -> {role}  leader={active_account} "
                f"followers={follower_accounts} rr={roundrobin_accounts}")
    return True, f"{acct} → {role}"


def refresh_terminal():
    """Redraw both pinned terminal regions after a web-initiated change.

    The two live in different places: the header status bar carries session
    state, and the bottom controls bar carries the leader, follower count
    and balance. A web change that touched only the header left the
    terminal advertising the old account, so both are refreshed together.
    """
    refresh_header_status()
    refresh_controls()


async def _web_shielded(coro):
    """Run `coro` so that a web timeout cannot cancel it mid-flight.

    A flatten writes its closes and then PROVES them against NinjaTrader;
    if the browser's request times out first, the proof must still finish
    and raise its verdict (the sticky FLATTEN INCOMPLETE alert) — a
    verification cancelled halfway is a silent failure. The inner task is
    tracked with the deferred legs so shutdown still cancels it."""
    task = asyncio.ensure_future(coro)
    _leg_tasks.add(task)
    task.add_done_callback(_leg_tasks.discard)
    return await asyncio.shield(task)


def _web_run(coro, timeout: float = 20.0):
    """Run a coroutine on the app loop from a web server thread.

    The timeout is a backstop for a wedged handler, NOT a shutdown wait:
    once the app is shutting down the loop stops running submitted work, so
    a request in flight at that moment would otherwise sit here for the full
    timeout — which is what made quitting take ~20 seconds. Refuse
    immediately instead.
    """
    if _web_loop is None or _web_loop.is_closed() or shutdown.is_set():
        coro.close()          # never leave the coroutine un-awaited
        raise RuntimeError("app is shutting down")
    fut = asyncio.run_coroutine_threadsafe(coro, _web_loop)
    deadline = time.monotonic() + timeout
    while True:
        if shutdown.is_set():
            fut.cancel()
            raise RuntimeError("app is shutting down")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            fut.cancel()
            raise TimeoutError("request timed out")
        try:
            return fut.result(min(remaining, 0.25))
        except concurrent.futures.TimeoutError:
            continue          # re-check the shutdown flag and keep waiting


# The web UI controls real money, so the loopback bind is NOT treated as the
# security boundary — a browser on this machine can be driven to it by any
# page the user visits. Three checks gate every request:
#   Host   — must be loopback, so a rebound DNS name can't reach the API
#   Origin — when present it must be our own origin (blocks cross-site POSTs)
#   Token  — a per-process secret embedded in the page and echoed in a custom
#            request header, which a cross-origin caller cannot set without a
#            preflight the server never approves
_web_token = ""
_WEB_TOKEN_HEADER = "X-ST-Token"


def _web_allowed_hosts() -> set[str]:
    port = _web_httpd.server_address[1] if _web_httpd else 0
    hosts = {"127.0.0.1", "localhost", "[::1]", "::1"}
    return hosts | {f"{h}:{port}" for h in hosts}


class _WebHandler(http.server.BaseHTTPRequestHandler):
    server_version = "SocketTrader"
    sys_version = ""
    # Keep-alive: the page polls twice a second and every click is a
    # POST; under HTTP/1.0 each one paid a fresh TCP connection and a
    # fresh handler thread. Every response carries Content-Length (see
    # _reply), which is what HTTP/1.1 needs to reuse the connection.
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # keep the TUI clean
        pass

    def _reply(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
            "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200):
        # allow_nan=False: bare NaN/Infinity is invalid JSON and would make
        # every poll throw in the browser, pinning the panel offline.
        try:
            body = json.dumps(obj, allow_nan=False).encode("utf-8")
        except ValueError:
            body = json.dumps({"ok": False,
                               "message": "state contains non-finite numbers"}).encode()
            code = 500
        self._reply(code, body, "application/json; charset=utf-8")

    def _host_ok(self) -> bool:
        """Reject DNS-rebinding: the Host must name loopback, not a domain."""
        host = (self.headers.get("Host") or "").strip().lower()
        return host in _web_allowed_hosts()

    def _origin_ok(self) -> bool:
        """A cross-site page's POST carries a foreign Origin — refuse it."""
        origin = (self.headers.get("Origin") or "").strip()
        if not origin:
            return True  # same-origin fetch/XHR may omit it; token still required
        try:
            netloc = urllib.parse.urlparse(origin).netloc.lower()
        except ValueError:
            return False
        return netloc in _web_allowed_hosts()

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            if not self._host_ok():
                self._json({"ok": False, "message": "forbidden host"}, 403)
                return
            if path == "/":
                page = WEB_UI_HTML.replace("__ST_TOKEN__", _web_token)
                self._reply(200, page.encode("utf-8"), "text/html; charset=utf-8")
            elif path == "/api/state":
                if not self._token_ok():
                    self._json({"ok": False, "message": "forbidden"}, 403)
                    return
                self._json(web_state())
            elif path == "/api/live":
                if not self._token_ok():
                    self._json({"ok": False, "message": "forbidden"}, 403)
                    return
                self._json(web_live())
            elif path == "/api/pnl":
                if not self._token_ok():
                    self._json({"ok": False, "message": "forbidden"}, 403)
                    return
                query = urllib.parse.parse_qs(
                    urllib.parse.urlparse(self.path).query)
                month = (query.get("month") or [""])[0]
                if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
                    self._json({"ok": False,
                                "message": "month must be YYYY-MM"}, 400)
                    return
                self._json(_pnl_month(month))
            else:
                self._json({"ok": False, "message": "not found"}, 404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            logger.error(f"WEB GET {path}  {exc}")
            try:
                self._json({"ok": False, "message": "internal error"}, 500)
            except OSError:
                pass

    def _token_ok(self) -> bool:
        sent = self.headers.get(_WEB_TOKEN_HEADER) or ""
        return bool(_web_token) and secrets.compare_digest(sent, _web_token)

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        # The refusals below answer before the body is read; on a kept-alive
        # connection those bytes would be parsed as the next request, so a
        # refused request also closes its connection.
        if not self._host_ok():
            self.close_connection = True
            self._json({"ok": False, "message": "forbidden host"}, 403)
            return
        if not self._origin_ok() or not self._token_ok():
            logger.warning(f"WEB POST {path}  rejected — bad origin or token")
            self.close_connection = True
            self._json({"ok": False, "message": "forbidden"}, 403)
            return
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ctype != "application/json":
            self.close_connection = True
            self._json({"ok": False, "message": "content-type must be application/json"}, 415)
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 1_000_000:
                raise ValueError("body too large")
            data = json.loads(self.rfile.read(length) or b"{}") if length else {}
            if not isinstance(data, dict):
                raise ValueError("body must be a JSON object")
        except (ValueError, json.JSONDecodeError) as exc:
            self._json({"ok": False, "message": f"bad request: {exc}"}, 400)
            return
        try:
            if path == "/api/trade":
                ok, msg = _web_run(submit_manual_trade(
                    data.get("side"), data.get("instrument"), data.get("qty"),
                    data.get("order_type", "market"), data.get("limit_price"),
                    data.get("atm", ""), source="Web UI"))
            elif path == "/api/pause":
                ok, msg = _web_run(_web_toggle_pause(data.get("paused", True)))
            elif path == "/api/close_all":
                ok, msg = _web_run(_web_shielded(_web_close_all()),
                                   timeout=FLATTEN_WEB_TIMEOUT)
            elif path == "/api/close_position":
                ok, msg = _web_run(_web_close_position(
                    data.get("account"), data.get("instrument")), timeout=30.0)
            elif path == "/api/flatten_account":
                ok, msg = _web_run(_web_shielded(_web_flatten_account(data.get("account"))),
                                   timeout=FLATTEN_WEB_TIMEOUT)
            elif path == "/api/role":
                ok, msg = _web_run(_web_set_role(data.get("account"),
                                                 data.get("role")))
            elif path == "/api/sizing":
                ok, msg = _web_run(_web_set_sizing(
                    data.get("account"), data.get("mode"), data.get("value")))
            elif path == "/api/reverse_position":
                ok, msg = _web_run(_web_reverse_position(
                    data.get("account"), data.get("instrument")), timeout=30.0)
            elif path == "/api/reset_pnl":
                ok, msg = _web_run(_web_reset_pnl())
            elif path == "/api/reconnect":
                _web_loop.call_soon_threadsafe(reconnect_event.set)
                ok, msg = True, "reconnect requested"
            elif path == "/api/micro":
                ok, msg = _web_run(_web_toggle_micro())
            elif path == "/api/micro_map":
                ok, msg = _web_run(_web_set_micro_map(data.get("map")))
            elif path == "/api/favorites":
                ok, msg = _web_run(_web_set_favorites(data.get("favorites")))
            elif path == "/api/accounts":
                ok, msg = _web_run(_web_set_accounts(
                    data.get("leader"), data.get("followers"),
                    data.get("roundrobin")))
            elif path == "/api/strategy":
                ok, msg = _web_run(_web_set_strategy(
                    data.get("name"), data.get("follow_publisher")))
            elif path == "/api/strategy_symbols":
                ok, msg = _web_run(_web_set_strategy_symbols(
                    data.get("strategy_symbols")))
            elif path == "/api/limits":
                ok, msg = _web_run(_web_set_limits(
                    data.get("account"), data.get("target"),
                    data.get("target_mode"), data.get("stop"),
                    data.get("stop_mode")))
            elif path == "/api/profiles":
                ok, msg = _web_run(_web_set_profiles(data.get("profiles")))
            else:
                self._json({"ok": False, "message": "not found"}, 404)
                return
            self._json({"ok": bool(ok), "message": msg})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except TimeoutError:
            # The action was dispatched; only the wait for its answer ran
            # out. Say that, not "error": a flatten keeps verifying and
            # reports in the Activity feed.
            logger.warning(f"WEB POST {path}  still running past its web timeout")
            try:
                self._json({"ok": False, "message":
                            "NinjaTrader has not confirmed in time — the request "
                            "is still running; watch Activity for the verdict"}, 504)
            except OSError:
                pass
        except Exception as exc:
            logger.error(f"WEB POST {path}  {exc}")
            try:
                self._json({"ok": False, "message": "internal error"}, 500)
            except OSError:
                pass


def start_web_ui(loop: asyncio.AbstractEventLoop, cfg: dict) -> str | None:
    """Start the localhost web UI. Returns the URL, or None when disabled."""
    global _web_loop, _web_httpd, _web_url, _web_token
    if not bool(cfg.get("webui_enabled", True)):
        return None
    try:
        port = int(cfg.get("webui_port", 8720))
    except (TypeError, ValueError):
        port = 8720
    _web_loop = loop
    _web_token = secrets.token_urlsafe(32)
    try:
        httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port), _WebHandler)
    except OSError as exc:
        logger.warning(f"WEB UI  port {port} busy ({exc}) — trying an ephemeral port")
        try:
            httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _WebHandler)
        except OSError as exc2:
            logger.error(f"WEB UI  failed to start: {exc2}")
            return None
    httpd.daemon_threads = True
    _web_httpd = httpd
    threading.Thread(target=httpd.serve_forever, name="webui", daemon=True).start()
    _web_url = f"http://127.0.0.1:{httpd.server_address[1]}"
    logger.info(f"WEB UI  serving at {_web_url}")
    return _web_url


def stop_web_ui():
    global _web_httpd
    if _web_httpd is not None:
        try:
            _web_httpd.shutdown()
            _web_httpd.server_close()
        except Exception:
            pass
        _web_httpd = None


WEB_UI_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SocketTrader</title>
<style>
:root{
--bg:#080b11;--panel:#10151f;--panel2:#161d2b;--edge:#212a3b;--edge2:#2d394f;
--fg:#dde4ef;--dim:#7f8ca5;--mute:#4a5568;
--green:#2fbf84;--green-d:#0f3527;--red:#ef5865;--red-d:#3a1216;
--yellow:#dfae57;--cyan:#4bb2d1;--violet:#8a7fd6;
--shadow:0 8px 28px rgba(0,0,0,.5);
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--fg);
font:13.5px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding-bottom:30px}
::-webkit-scrollbar{width:9px;height:9px}
::-webkit-scrollbar-thumb{background:var(--edge2);border-radius:5px}

/* top bar */
#top{position:sticky;top:0;z-index:30;background:rgba(8,11,17,.97);
border-bottom:1px solid var(--edge);padding:9px 13px;display:flex;gap:9px;
align-items:center;flex-wrap:wrap;backdrop-filter:blur(8px)}
.brand{font-weight:700;letter-spacing:2.5px;font-size:14px}
.brand span{color:var(--cyan)}
.spacer{flex:1}
.chip{border:1px solid var(--edge2);border-radius:999px;padding:3px 10px;
font-size:11.5px;color:var(--dim);white-space:nowrap}
.chip.on{color:var(--green);border-color:#1d6b50;background:var(--green-d)}
.chip.warn{color:var(--yellow);border-color:#6a5522;background:#2f2612}
.chip.bad{color:var(--red);border-color:#78262e;background:var(--red-d)}
.chip.info{color:var(--cyan);border-color:#245c6f;background:#0d2a33}

/* layout */
.wrap{padding:13px;display:grid;gap:12px;grid-template-columns:370px 1fr}
@media(max-width:1000px){.wrap{grid-template-columns:1fr}}
.col{display:grid;gap:12px;align-content:start;min-width:0}
.card{background:var(--panel);border:1px solid var(--edge);border-radius:11px;
padding:12px;box-shadow:var(--shadow);min-width:0}
.card>h2{font-size:10px;color:var(--dim);text-transform:uppercase;
letter-spacing:1.6px;margin-bottom:10px;display:flex;gap:8px;align-items:center}
.card>h2 .hint{margin-left:auto;text-transform:none;letter-spacing:0;
font-size:11px;color:var(--mute)}

/* buttons */
button{background:var(--panel2);border:1px solid var(--edge2);color:var(--fg);
border-radius:8px;padding:9px 12px;font:inherit;cursor:pointer;transition:.11s;
white-space:nowrap}
button:hover{border-color:var(--cyan);background:#1c2637}
button:active{transform:translateY(1px)}
button:disabled{opacity:.35;cursor:not-allowed}
button.sm{padding:5px 9px;font-size:12px;border-radius:6px}
button.tiny{padding:3px 7px;font-size:10.5px;border-radius:5px}
button.wide{width:100%}
button.on{background:var(--cyan);border-color:var(--cyan);color:#05202a;font-weight:700}
button.buy.on{background:var(--green);border-color:var(--green);color:#04140d}
button.sell.on{background:var(--red);border-color:var(--red);color:#180406}
button.danger{color:var(--red);border-color:#5a2028}
button.danger:hover{background:var(--red-d);border-color:var(--red)}
button.ghost{background:transparent;color:var(--dim)}
button.ghost:hover{color:var(--fg)}
button.solid-red{background:var(--red);border-color:var(--red);color:#180406;font-weight:700}
button.solid-red:hover{filter:brightness(1.1);background:var(--red)}
.btnrow{display:flex;gap:6px;flex-wrap:wrap}

/* inputs */
label{font-size:10px;color:var(--dim);display:block;margin:9px 0 4px;
text-transform:uppercase;letter-spacing:1.1px}
input,select{background:#0b101a;border:1px solid var(--edge2);color:var(--fg);
border-radius:8px;padding:8px 10px;font:inherit;width:100%}
input:focus,select:focus{outline:0;border-color:var(--cyan)}
input:disabled{opacity:.3}
.stepper{display:flex;gap:6px;align-items:center}
.stepper input{text-align:center;font-size:20px;font-weight:700;padding:6px}
.stepper button{width:42px;font-size:18px;padding:5px 0}
.row2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.seg{display:flex;border:1px solid var(--edge2);border-radius:8px;overflow:hidden}
.seg button{border:0;border-radius:0;flex:1;background:transparent}
.seg button+button{border-left:1px solid var(--edge2)}

/* instrument picker */
.pick{border:1px solid var(--edge2);border-radius:7px;padding:5px 9px;cursor:pointer;
font-size:12px;background:var(--panel2);color:var(--dim);transition:.11s;
display:inline-flex;gap:5px;align-items:center}
.pick:hover{border-color:var(--cyan);color:var(--fg)}
.pick.on{background:var(--cyan);border-color:var(--cyan);color:#05202a;font-weight:700}
.pick.star{color:var(--yellow);border-color:#6a5522}
.pick.star.on{background:var(--yellow);border-color:var(--yellow);color:#2a2005}
.chiplist{display:flex;gap:5px;flex-wrap:wrap;margin-top:5px}
#instrBox{max-height:184px;overflow-y:auto;border:1px solid var(--edge);
border-radius:8px;padding:7px;margin-top:6px;background:#0b101a}
.grp{font-size:9.5px;color:var(--mute);text-transform:uppercase;letter-spacing:1.2px;
margin:7px 0 3px}
.grp:first-child{margin-top:0}
#selected{font-size:17px;font-weight:700;letter-spacing:.5px;margin-top:7px;
display:flex;align-items:center;gap:8px}
#selected .none{color:var(--mute);font-size:13px;font-weight:400}
.starbtn{cursor:pointer;color:var(--mute);font-size:15px;user-select:none}
.starbtn.on{color:var(--yellow)}

#ticket.buy{border-color:#1d6b50}
#ticket.sell{border-color:#78262e}
#submit{width:100%;padding:15px;font-size:15px;font-weight:700;letter-spacing:1.2px;
margin-top:11px}
#submit.buy{background:var(--green);border-color:var(--green);color:#04140d}
#submit.sell{background:var(--red);border-color:var(--red);color:#180406}
#submit:hover:not(:disabled){filter:brightness(1.12)}
#tNote{text-align:center;color:var(--dim);font-size:11px;margin-top:7px;min-height:15px}

/* tables */
.tblwrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:12.5px}
th{color:var(--dim);text-align:left;font-weight:400;font-size:9.5px;letter-spacing:1px;
text-transform:uppercase;border-bottom:1px solid var(--edge);padding:5px 6px;white-space:nowrap}
td{padding:6px;border-bottom:1px solid #141a26;vertical-align:middle;white-space:nowrap}
tr:last-child td{border-bottom:0}
tr.unmanaged td{opacity:.5}
.pos{color:var(--green)}.neg{color:var(--red)}.dim{color:var(--dim)}
.num{text-align:right;font-variant-numeric:tabular-nums}
.tag{font-size:9.5px;padding:2px 6px;border-radius:4px;border:1px solid var(--edge2);color:var(--dim)}
.tag.lead{color:var(--cyan);border-color:#245c6f;background:#0d2a33}
.tag.fol{color:var(--violet);border-color:#443c76;background:#1a1730}
.tag.rr{color:var(--yellow);border-color:#6a5522;background:#2a2211}
.tag.bad{color:var(--red);border-color:#78262e;background:var(--red-d)}
.roleset{display:flex;gap:3px}
.roleset button{padding:2px 6px;font-size:10px;border-radius:4px}

/* aggregate tiles */
#tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(104px,1fr));gap:7px}
.tile{background:var(--panel2);border:1px solid var(--edge);border-radius:8px;padding:8px 9px}
.tile .k{font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:1px}
.tile .v{font-size:17px;font-weight:700;margin-top:2px;font-variant-numeric:tabular-nums}
.tile.good .v{color:var(--green)}
.tile.bad{border-color:#78262e;background:var(--red-d)}
.tile.bad .v{color:var(--red)}
.banner{margin-top:9px;border:1px solid #78262e;background:var(--red-d);color:#ffc0c5;
border-radius:8px;padding:8px 10px;font-size:12px}
.banner b{color:var(--red)}
.led{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}
.led.g{background:var(--green)}.led.y{background:var(--yellow)}
.led.r{background:var(--red)}.led.d{background:var(--mute)}
.sizecell{cursor:pointer;border-bottom:1px dotted var(--edge2)}
.sizecell:hover{color:var(--cyan)}

#feed{max-height:210px;overflow-y:auto;font-size:12px}
#feed div{padding:2px 0;border-bottom:1px solid #131926;display:flex;gap:8px}
#feed div:last-child{border-bottom:0}
#feed .t{color:var(--mute);flex-shrink:0}
.empty{color:var(--mute);font-size:12px;padding:9px 2px;text-align:center}

#veil{position:fixed;inset:0;background:rgba(3,5,9,.8);display:none;z-index:60;
padding:20px;overflow-y:auto;backdrop-filter:blur(3px)}
#veil.show{display:block}
#modal{max-width:600px;margin:0 auto;background:var(--panel);
border:1px solid var(--edge2);border-radius:12px;padding:16px;box-shadow:var(--shadow)}
#modal h3{font-size:13px;letter-spacing:1.3px}
#modal .sub{color:var(--dim);font-size:11.5px;margin:3px 0 12px}
.fieldset{border-top:1px solid var(--edge);padding-top:10px;margin-top:12px}
.fieldset:first-of-type{border-top:0;margin-top:0}
.fold{border:1px solid var(--edge);border-radius:8px;margin-top:6px}
.foldhead{display:flex;gap:6px;align-items:center;padding:7px 9px;cursor:pointer;
user-select:none;font-size:12px}
.foldhead:hover{color:var(--fg)}
.foldhead .arr{color:var(--dim);width:10px;flex:none}
.foldhead .fsum{color:var(--dim);margin-left:auto;font-size:11px;text-align:right}
.foldbody{padding:0 9px 9px;border-top:1px solid var(--edge)}

#toast{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);
background:#171f2d;border:1px solid var(--cyan);border-radius:9px;padding:10px 16px;
display:none;max-width:92vw;z-index:99;box-shadow:var(--shadow);font-size:12.5px}
#toast.bad{border-color:var(--red);color:#ffb9be}
#toast.good{border-color:var(--green);color:#a4ecca}
#toast.warn{border-color:var(--yellow);color:var(--yellow)}
#toast.info{border-color:var(--cyan);color:var(--cyan)}

/* ---- P&L calendar tab ---- */
.hide{display:none!important}
.tabs{display:flex;border:1px solid var(--edge2);border-radius:8px;overflow:hidden;
margin-left:8px}
.tabs button{border:0;border-radius:0;padding:5px 13px;font-size:11px;
letter-spacing:1.6px;background:transparent;color:var(--dim)}
.tabs button+button{border-left:1px solid var(--edge2)}
.tabs button.on{background:var(--cyan);color:#05202a;font-weight:700}
.pwrap{padding:13px;display:grid;gap:12px;max-width:1240px;margin:0 auto}
.pnav{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.pnav .pmonth{font-size:16px;font-weight:700;letter-spacing:1.2px;
min-width:172px;text-align:center}
.phero{margin-left:auto;text-align:right}
.phero .k{font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:1.5px}
.phero .v{font-size:29px;font-weight:700;line-height:1.15}
.ptiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(116px,1fr));gap:7px}
.pacct{display:flex;gap:7px;align-items:center;flex-wrap:wrap}
.pacct .k{font-size:9px;color:var(--dim);text-transform:uppercase;
letter-spacing:1.5px;margin-right:3px}
.pacct button{padding:5px 11px;font-size:11.5px;border-radius:999px;
background:transparent;color:var(--dim);border:1px solid var(--edge2)}
.pacct button:hover{color:var(--fg)}
.pacct button.on{background:var(--cyan);color:#05202a;font-weight:700;
border-color:var(--cyan)}
.pacct .amt{font-size:10px;opacity:.74;margin-left:7px;font-variant-numeric:tabular-nums}

#pcal{overflow-x:auto}
.pcal-head,.pcal-week{display:grid;gap:7px;min-width:460px;
grid-template-columns:repeat(5,1fr) minmax(96px,.72fr)}
.pcal-head{margin-bottom:7px}
.pcal-head div{font-size:9.5px;color:var(--mute);text-transform:uppercase;
letter-spacing:1.3px;text-align:center}
.pcal-week{margin-bottom:7px}
.pcal-week:last-child{margin-bottom:0}
.dcell{position:relative;border:1px solid var(--edge);border-radius:9px;
background:var(--panel2);min-height:78px;padding:7px 9px;display:flex;
flex-direction:column;align-items:flex-start;gap:2px;font:inherit;color:var(--fg);
cursor:pointer;transition:.12s;text-align:left}
.dcell:hover{border-color:var(--cyan);transform:translateY(-1px)}
.dcell:focus-visible{outline:2px solid var(--cyan);outline-offset:1px}
.dcell.off{background:transparent;border-color:transparent;visibility:hidden}
.dcell:disabled{opacity:1;cursor:default}
.dcell.mute,.dcell.future{cursor:default}
.dcell.mute:hover,.dcell.future:hover{border-color:var(--edge);transform:none}
.dcell.future{opacity:.32}
.dcell.today{box-shadow:inset 0 0 0 1px #245c6f}
.dcell.sel{border-color:var(--cyan);box-shadow:0 0 0 1px var(--cyan)}
.dnum{font-size:10px;color:var(--dim)}
.dpnl{font-size:14.5px;font-weight:700;letter-spacing:.3px}
.dsub{font-size:10px;color:var(--dim)}
.wcell{border:1px solid var(--edge);border-radius:9px;background:#0b101a;
min-height:78px;padding:7px 9px;display:flex;flex-direction:column;gap:2px}
.wcell .dnum{text-transform:uppercase;letter-spacing:1.1px;color:var(--mute)}
.wcell .dpnl{font-size:12.5px}

.eq svg{display:block;width:100%;height:auto}
.pgrid3{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(250px,1fr))}
.psec{font-size:10px;color:var(--dim);text-transform:uppercase;
letter-spacing:1.6px;margin:14px 0 7px}
.pday-title{font-size:14px;font-weight:700;letter-spacing:.5px}
.bbar{position:relative;height:8px;border-radius:4px;background:#0b101a;
min-width:64px;overflow:hidden}
.bbar i{position:absolute;top:0;bottom:0}
.bbar i.pos{background:var(--green);border-radius:0 4px 4px 0}
.bbar i.neg{background:var(--red);border-radius:4px 0 0 4px}
.bbar em{position:absolute;top:0;bottom:0;left:50%;width:1px;background:var(--edge2)}
#ptip{position:fixed;z-index:80;background:#171f2d;border:1px solid var(--edge2);
border-radius:8px;padding:8px 11px;font-size:11.5px;pointer-events:none;
box-shadow:var(--shadow);max-width:280px}
#ptip .tr{display:flex;justify-content:space-between;gap:12px;padding:1px 0}
#ptip .tv{font-weight:700}
#ptip .tk{color:var(--dim)}
@media(max-width:640px){
  .phero .v{font-size:23px}
  .dcell,.wcell{min-height:62px;padding:5px 6px}
  .dpnl{font-size:12px}
  .dsub{font-size:9px}
  .pcal-head,.pcal-week{gap:5px;min-width:430px;
    grid-template-columns:repeat(5,1fr) minmax(74px,.72fr)}
}
</style></head><body>

<div id="top">
  <div class="brand">SOCKET<span>TRADER</span></div>
  <div class="tabs"><button class="on" id="tabTrade">TRADE</button><button
    id="tabPnl">P&amp;L</button></div>
  <span class="chip" id="chState">connecting…</span>
  <span class="chip" id="chReady"></span>
  <span class="chip" id="chNt"></span>
  <span class="chip" id="chAtm"></span>
  <span class="chip" id="chMicro"></span>
  <div class="spacer"></div>
  <span class="chip dim" id="chCount"></span>
  <button class="sm" id="btnPause">PAUSE</button>
  <button class="sm solid-red" id="btnFlatten">FLATTEN ALL</button>
</div>

<div class="wrap" id="viewTrade">
  <div class="col">

    <div class="card" id="ticket">
      <h2>Order ticket <span class="hint" id="tFan"></span></h2>

      <div class="row2">
        <button class="buy" id="sideB">BUY / LONG</button>
        <button class="sell" id="sideS">SELL / SHORT</button>
      </div>

      <label>Instrument</label>
      <div id="favRow" class="chiplist"></div>
      <input id="instrSearch" placeholder="search NQ, gold, CL…" autocomplete="off">
      <div id="instrBox"></div>
      <div id="selected"><span class="none">nothing selected</span></div>
      <div id="altMonths" class="chiplist"></div>

      <label>Contracts</label>
      <div class="stepper">
        <button id="qtyMinus">&minus;</button>
        <input id="tQty" type="number" min="1" max="1000" value="1">
        <button id="qtyPlus">+</button>
      </div>
      <div class="chiplist" id="qtyChips"></div>

      <div class="row2">
        <div>
          <label>Type</label>
          <div class="seg"><button id="typeM">MARKET</button><button id="typeL">LIMIT</button></div>
        </div>
        <div>
          <label>Limit price</label>
          <input id="tPrice" type="number" step="0.25" disabled placeholder="—">
        </div>
      </div>

      <label>ATM template</label>
      <select id="tAtm"></select>

      <button id="submit" class="buy">SUBMIT</button>
      <div id="tNote"></div>
    </div>

    <div class="card">
      <h2>Session</h2>
      <div class="btnrow">
        <button class="sm" id="btnReconnect">RECONNECT</button>
        <button class="sm" id="btnMicro">MICROS</button>
        <button class="sm" id="btnResetPnl">RESET P&amp;L</button>
        <button class="sm ghost" id="btnStrategy">STRATEGY…</button>
      </div>
      <div id="rrline" class="dim" style="margin-top:8px;font-size:11px"></div>
    </div>

    <div class="card">
      <h2>Activity</h2>
      <div id="feed"></div>
    </div>

  </div>

  <div class="col">

    <div class="card">
      <h2>Copy health <span class="hint" id="liveHint"></span></h2>
      <div id="tiles"></div>
      <div id="syncWarn"></div>
    </div>

    <div class="card">
      <h2>Accounts — live from NinjaTrader
        <span class="hint" id="acctHint"></span></h2>
      <div class="tblwrap" id="acctWrap"></div>
    </div>

    <div class="card">
      <h2>Open positions <span class="hint" id="posHint"></span></h2>
      <div class="tblwrap" id="posWrap"></div>
    </div>

  </div>
</div>

<div class="pwrap hide" id="viewPnl">
  <div class="card pnav">
    <button class="sm" id="pmPrev" title="previous month">&lsaquo;</button>
    <div class="pmonth" id="pmTitle">&mdash;</div>
    <button class="sm" id="pmNext" title="next month">&rsaquo;</button>
    <button class="sm ghost" id="pmToday">TODAY</button>
    <div class="phero">
      <div class="k">Month net P&amp;L</div>
      <div class="v" id="pmNet">&mdash;</div>
    </div>
  </div>
  <div class="card pacct hide" id="pAccts"></div>
  <div class="card">
    <h2>Month statistics <span class="hint" id="pmHint"></span></h2>
    <div class="ptiles" id="pmTiles"></div>
  </div>
  <div class="card">
    <h2>Equity curve — cumulative daily P&amp;L
      <span class="hint" id="peqHint"></span></h2>
    <div class="eq" id="pmEq"></div>
  </div>
  <div class="card">
    <h2>Daily P&amp;L — trading days, session-close dated
      <span class="hint" id="pcalHint"></span></h2>
    <div id="pcal"></div>
  </div>
  <div class="card hide" id="pday"></div>
</div>

<div id="veil"><div id="modal"></div></div>
<div id="ptip" class="hide"></div>
<div id="toast"></div>

<script>
"use strict";
const TOKEN="__ST_TOKEN__";
const $=id=>document.getElementById(id);
let S=null,L=null,side="long",otype="market",instrument="",modalOpen=false,busy=false;

/* ---- safe DOM (never innerHTML with server data) ---- */
function el(tag,cls,text){const e=document.createElement(tag);
  if(cls)e.className=cls; if(text!=null)e.textContent=String(text); return e}
function clear(n){while(n.firstChild)n.removeChild(n.firstChild)}

/* Panels are rebuilt from scratch rather than diffed, so re-rendering on
   every poll would tear the DOM down twice a second: buttons visibly
   flicker, hover is lost, and an open inline editor disappears mid-use.
   Each panel therefore renders only when its own data actually changed. */
const _sig={};
function changed(key,data){
  const s=JSON.stringify(data);
  if(_sig[key]===s)return false;
  _sig[key]=s; return true}
function invalidate(key){delete _sig[key]}
let sizeEditOpen=false;
function btn(label,cls,fn){const b=el("button",cls,label);b.onclick=fn;return b}
function td(cls,text){return el("td",cls,text)}

function toast(m,kind){const t=$("toast");t.textContent=m;t.className=kind||"";
  t.style.display="block";clearTimeout(t._h);
  t._h=setTimeout(()=>t.style.display="none",5000)}

/* One request at a time — but a second click while one is in flight is
   refused OUT LOUD: silently dropping it read as a dead button. A request
   that takes longer than a beat says so (a flatten confirms against
   NinjaTrader before it answers), and a slow answer shows how long it
   took, so "sluggish" is at least never "silent". */
let busyLabel="";
async function api(path,body,label){
  if(busy){toast("still working on "+(busyLabel||"the previous request")+"…","warn");
    return{ok:false}}
  busy=true;busyLabel=label||path.replace("/api/","").replace(/_/g," ");
  const t0=performance.now();
  const slow=setTimeout(()=>toast(busyLabel+" — working… (confirming with NinjaTrader)","info"),600);
  try{
    const r=await fetch(path,{method:"POST",headers:{
      "Content-Type":"application/json","X-ST-Token":TOKEN},body:JSON.stringify(body||{})});
    const j=await r.json();
    clearTimeout(slow);
    const ms=performance.now()-t0;
    toast(j.message+(ms>1500?"  ("+(ms/1000).toFixed(1)+"s)":""),j.ok?"good":"bad");
    refresh();liveBurst();
    return j;
  }catch(e){clearTimeout(slow);toast("request failed: "+e,"bad");return{ok:false}}
  finally{busy=false;busyLabel=""}
}
async function get(path){
  const r=await fetch(path,{headers:{"X-ST-Token":TOKEN}});
  if(r.status===403){
    // The token is minted per app process, so a tab left open across a
    // restart holds a dead one and every poll 403s forever. Reload once to
    // pick up the new token instead of sitting there looking broken.
    if(!sessionStorage.getItem("st-reloaded")){
      sessionStorage.setItem("st-reloaded","1");
      location.reload();
    }
    throw new Error("stale session — reload the page");
  }
  sessionStorage.removeItem("st-reloaded");
  if(!r.ok)throw new Error(r.status);
  return r.json();
}
function fmt(v,d){return v==null?"—":Number(v).toLocaleString(undefined,
  {minimumFractionDigits:d==null?2:d,maximumFractionDigits:d==null?2:d})}
function signed(v){return v==null?"—":(v>=0?"+":"")+fmt(v)}

/* ================= order ticket ================= */
function setSide(s){side=s;
  $("sideB").classList.toggle("on",s==="long");
  $("sideS").classList.toggle("on",s==="short");
  $("ticket").classList.toggle("buy",s==="long");
  $("ticket").classList.toggle("sell",s==="short");
  $("submit").classList.toggle("buy",s==="long");
  $("submit").classList.toggle("sell",s==="short");
  note()}
function setType(t){otype=t;
  $("typeM").classList.toggle("on",t==="market");
  $("typeL").classList.toggle("on",t==="limit");
  $("tPrice").disabled=(t!=="limit");
  if(t!=="limit")$("tPrice").value="";
  note()}
function qty(){return Math.max(1,Math.min(1000,parseInt($("tQty").value||"1",10)||1))}
function bumpQty(d){$("tQty").value=qty()+d;renderQty();note()}

function pickInstrument(code){
  instrument=code;
  renderPicker();renderAlt();renderSelected();note()}

/* The ticket's chips are rebuilt only when what they show changes — the
   state poll used to tear every one of them down twice a second, which
   reset hover under the cursor and could swallow the click in flight. */
function renderSelected(){
  if(!changed("selected",[instrument,S&&S.favorites]))return;
  const box=$("selected");clear(box);
  if(!instrument){box.appendChild(el("span","none","nothing selected"));return}
  box.appendChild(el("span",null,instrument));
  const fav=(S&&S.favorites||[]).includes(instrument);
  const star=el("span","starbtn"+(fav?" on":""),fav?"★":"☆");
  star.title=fav?"unpin":"pin to favourites";
  star.onclick=()=>{
    const cur=(S&&S.favorites||[]).slice();
    const i=cur.indexOf(instrument);
    if(i<0)cur.unshift(instrument);else cur.splice(i,1);
    api("/api/favorites",{favorites:cur})};
  box.appendChild(star)}

function renderAlt(){
  if(!changed("alt",[instrument,S&&S.catalog]))return;
  const box=$("altMonths");clear(box);
  if(!instrument||!S)return;
  const root=instrument.split(" ")[0];
  for(const p of S.catalog){
    const roots=[p.root].concat(p.micro?[p.micro]:[]);
    if(!roots.includes(root))continue;
    const isMicro=(root===p.micro);
    p.contracts.forEach(c=>{
      const code=isMicro?c.replace(p.root,p.micro):c;
      if(code===instrument)return;
      box.appendChild(mkPick(code,code.split(" ")[1],false))});
    if(p.micro){
      const other=isMicro?p.root:p.micro;
      const code=instrument.replace(root,other);
      box.appendChild(mkPick(code,other,false))}
    break}}

function mkPick(code,label,star){
  const b=el("div","pick"+(star?" star":"")+(code===instrument?" on":""),label||code);
  b.onclick=()=>pickInstrument(code);
  return b}

function renderFavs(){
  if(!changed("favs",[S&&S.favorites,instrument]))return;
  const row=$("favRow");clear(row);
  const favs=(S&&S.favorites)||[];
  favs.forEach(c=>row.appendChild(mkPick(c,"★ "+c,true)))}

function renderPicker(){
  const box=$("instrBox");
  if(document.activeElement&&document.activeElement.closest("#instrBox"))return;
  const q=$("instrSearch").value.trim().toLowerCase();
  if(!changed("picker",[S&&S.catalog,instrument,q]))return;
  clear(box);
  if(!S||!S.catalog)return;
  const groups={};
  S.catalog.forEach(p=>{
    const hay=(p.root+" "+p.name+" "+p.micro+" "+p.group).toLowerCase();
    if(q&&!hay.includes(q))return;
    (groups[p.group]=groups[p.group]||[]).push(p)});
  const names=Object.keys(groups);
  if(!names.length){box.appendChild(el("div","empty","no match"));return}
  names.forEach(g=>{
    box.appendChild(el("div","grp",g));
    const row=el("div","chiplist");
    groups[g].forEach(p=>{
      const front=p.contracts[0];
      row.appendChild(mkPick(front,p.root+" "+front.split(" ")[1],false));
      if(p.micro)row.appendChild(mkPick(front.replace(p.root,p.micro),
        p.micro+" "+front.split(" ")[1],false))});
    box.appendChild(row)})}

function renderQty(){
  if(!changed("qtychips",qty()))return;
  const q=$("qtyChips");clear(q);
  [1,2,3,5,10].forEach(n=>{
    const b=el("div","pick"+(qty()===n?" on":""),String(n));
    b.onclick=()=>{$("tQty").value=n;renderQty();note()};
    q.appendChild(b)})}

function note(){
  const p=otype==="limit"&&$("tPrice").value?(" @ "+$("tPrice").value):"";
  $("submit").textContent=instrument
    ?((side==="long"?"BUY ":"SELL ")+qty()+" "+instrument+p)
    :"SELECT AN INSTRUMENT";
  const n=S?S.accounts.length:0;
  let msg="";
  if(!S)msg="";
  else if(!S.trade_ready)msg="system not ready — check directory, account, strategy";
  else if(S.hard_stopped)msg="session hard-locked";
  else msg="fans out to "+n+" managed account"+(n===1?"":"s")+
    (S.micro_mode?" · micro mode will convert to the micro contract":"");
  $("tNote").textContent=msg;
  $("submit").disabled=!instrument||!S||!S.trade_ready||S.hard_stopped}

function submitOrder(){
  if(!instrument)return toast("pick an instrument first","bad");
  api("/api/trade",{side:side,instrument:instrument,qty:qty(),order_type:otype,
    limit_price:$("tPrice").value,atm:$("tAtm").value})}

/* ================= accounts ================= */
const ROLES=[["L","leader"],["F","follower"],["R","round-robin"],["–","off"]];

/* Sizing label mirrors the profile rule so the grid reads like the editor. */
function sizeLabel(name){
  const p=(S&&S.profiles&&S.profiles[name])||{};
  const r=Object.assign({},(S&&S.rule_defaults)||{},p.default||{});
  if(r.qty_mode==="fixed")return String(parseInt(r.qty_value||1,10));
  if(r.qty_mode==="multiple")return "×"+(+r.qty_value||1);
  return "copy"}

/* A per-account multiplier on the leader's contract count is the standard
   futures copy-trading control, so it is editable inline. */
function sizeCell(a){
  const cell=el("td","num");
  const span=el("span","sizecell",sizeLabel(a.name));
  span.title="click to change this account's contract sizing";
  span.onclick=()=>{
    clear(cell);
    sizeEditOpen=true;   // freeze the table so the poll can't wipe this
    const box=el("div","chiplist");
    const close=()=>{sizeEditOpen=false;invalidate("accounts");renderAccounts()};
    const opt=(label,mode,value)=>{
      const b=el("div","pick",label);
      b.onclick=()=>{sizeEditOpen=false;
        api("/api/sizing",{account:a.name,mode:mode,value:value})};
      box.appendChild(b)};
    opt("copy","copy",0);
    [0.5,1,2,3].forEach(m=>opt("×"+m,"multiple",m));
    [1,2,3,5].forEach(n=>opt(String(n),"fixed",n));
    const x=el("div","pick","✕");x.onclick=close;
    box.appendChild(x);
    cell.appendChild(box)};
  cell.appendChild(span);
  return cell}

const SYNC_LED={"in-sync":"g","out-of-sync":"y","leader":"g","rotation":"d","":"d"};

function renderTiles(){
  if(!changed("tiles",L&&L.totals))return;
  const box=$("tiles");clear(box);
  const w=$("syncWarn");clear(w);
  if(!L||!L.totals)return;
  const T=L.totals;
  const tile=(k,v,cls)=>{const d=el("div","tile"+(cls?" "+cls:""));
    d.appendChild(el("div","k",k));d.appendChild(el("div","v",v));box.appendChild(d)};

  // Hedge first: prop firms liquidate for opposite positions across
  // accounts, so it outranks every other number on the panel.
  const hedges=T.hedges||[];
  if(hedges.length)tile("HEDGE",hedges.length,"bad");
  const syncOk=T.followers===0||T.in_sync===T.followers;
  tile("In sync",T.followers?(T.in_sync+" / "+T.followers):"n/a",
    syncOk?"good":"bad");
  tile("Accounts",T.accounts);
  tile("Realized",signed(T.realized),T.realized>=0?"good":"bad");
  tile("Session P&L",signed(T.session_pnl),T.session_pnl>=0?"good":"bad");
  tile("Contracts",T.contracts||"flat");
  tile("Working",T.working||"—");

  hedges.forEach(h=>{
    const b=el("div","banner");
    b.appendChild(el("b","","⚠ OPPOSITE POSITIONS — "+h.root+": "));
    b.appendChild(el("span",null,
      "long on "+(h.long.join(", ")||"—")+" · short on "+(h.short.join(", ")||"—")+
      ". Prop firms treat offsetting positions across accounts as hedging and "+
      "may liquidate for it — micro and full-size count as the same underlying."));
    w.appendChild(b)});
  if(T.out_of_sync&&T.out_of_sync.length){
    const b=el("div","banner");
    b.appendChild(el("b","","OUT OF SYNC: "));
    b.appendChild(el("span",null,T.out_of_sync.join(", ")+
      " — position does not match the leader. A copy leg was rejected or an exit did not land."));
    w.appendChild(b)}
  if(T.locked&&T.locked.length){
    const b=el("div","banner");
    b.appendChild(el("b","","LOCKED: "));
    b.appendChild(el("span",null,T.locked.join(", ")+" — session stop or target hit."));
    w.appendChild(b)}}

/* Numbers change on every fill; the buttons in the row must not. The
   table is rebuilt only when its SHAPE changes — which accounts, their
   roles, which markets each holds and on which side, sync state — and
   cash, P&L, working-order counts and position sizes are written into
   the existing cells, so a Flat or role button never disappears under
   the cursor mid-click. */
const acctCells={};
function acctStructure(){
  return [L&&L.ok,S&&S.profiles,(L&&L.accounts||[]).map(a=>[a.name,a.role,a.managed,
    a.stop,a.profile,a.sync,a.sync_detail,
    a.positions.map(p=>p.instrument+(p.qty>0?"+":"-"))])]}
function posText(p){return (p.qty>0?"+":"")+p.qty+" "+p.instrument+
  (p.avg_price?" @"+fmt(p.avg_price):"")}
function numCls(v){return "num "+(v==null?"dim":v>=0?"pos":"neg")}
/* Write only what differs: assigning an unchanged textContent still
   replaces the text node, which is a DOM mutation per cell per poll. */
function setText(n,v,cls){if(n.textContent!==v)n.textContent=v;
  if(cls!=null&&n.className!==cls)n.className=cls}
function updateAccountNumbers(){
  (L&&L.accounts||[]).forEach(a=>{const c=acctCells[a.name];if(!c)return;
    setText(c.cash,fmt(a.cash));
    setText(c.realized,signed(a.realized),numCls(a.realized));
    setText(c.session,signed(a.session_pnl),numCls(a.session_pnl));
    setText(c.work,String(a.working||"—"),"num"+(a.working?"":" dim"));
    a.positions.forEach((p,i)=>{if(c.pos[i])setText(c.pos[i],posText(p))})})}
function renderAccounts(){
  if(sizeEditOpen)return;                 // never yank an open inline editor
  if(!changed("accounts",acctStructure())){updateAccountNumbers();return}
  for(const k in acctCells)delete acctCells[k];
  const w=$("acctWrap");clear(w);
  if(!L){w.appendChild(el("div","empty","connecting to NinjaTrader…"));return}
  if(!L.ok){w.appendChild(el("div","empty",
    "NinjaTrader ATI not reachable — check NT is running and the ATI port"));return}
  if(!L.accounts.length){w.appendChild(el("div","empty","no accounts reported"));return}
  $("acctHint").textContent=L.accounts.length+" accounts";

  const t=el("table"),h=el("thead"),hr=el("tr");
  [["Account",""],["Role",""],["Size","num"],["Sync",""],["Cash","num"],
   ["Realized","num"],["Session","num"],["Position",""],["Work","num"],["",""]]
    .forEach(([x,c])=>hr.appendChild(el("th",c,x)));
  h.appendChild(hr);t.appendChild(h);
  const tb=el("tbody");
  L.accounts.forEach(a=>{
    const tr=el("tr",a.managed?"":"unmanaged");
    const nameTd=td(null,a.name);
    nameTd.style.cursor="pointer";
    nameTd.title="edit limits and profile";
    nameTd.onclick=()=>accountModal(a);
    tr.appendChild(nameTd);

    const rt=el("td");const rs=el("div","roleset");
    ROLES.forEach(([lbl,role])=>{
      const cur=(a.role||"off")===role;
      const b=btn(lbl,cur?"on tiny":"tiny ghost",()=>api("/api/role",
        {account:a.name,role:role}));
      b.title=role;rs.appendChild(b)});
    rt.appendChild(rs);tr.appendChild(rt);

    tr.appendChild(a.managed?sizeCell(a):td("dim","—"));

    const sy=el("td");
    if(a.sync){
      sy.appendChild(el("span","led "+(SYNC_LED[a.sync]||"d")));
      sy.appendChild(el("span",a.sync==="out-of-sync"?"":"dim",
        a.sync==="out-of-sync"?"OUT OF SYNC":a.sync));
      if(a.sync_detail)sy.title=a.sync_detail}
    tr.appendChild(sy);

    const cells={pos:[]};acctCells[a.name]=cells;
    tr.appendChild(cells.cash=td("num",fmt(a.cash)));
    tr.appendChild(cells.realized=td(numCls(a.realized),signed(a.realized)));
    tr.appendChild(cells.session=td(numCls(a.session_pnl),signed(a.session_pnl)));

    const pt=el("td");
    if(a.positions.length){
      a.positions.forEach(p=>{
        const d=el("div",p.qty>0?"pos":"neg",posText(p));
        cells.pos.push(d);pt.appendChild(d)})}
    else pt.appendChild(el("span","dim","flat"));
    tr.appendChild(pt);

    tr.appendChild(cells.work=td("num"+(a.working?"":" dim"),a.working||"—"));

    const at=el("td");at.style.textAlign="right";
    if(a.stop)at.appendChild(el("span","tag bad",a.stop.toUpperCase()));
    at.appendChild(btn("Flat","tiny danger",()=>{
      if(confirm("Flatten "+a.name+"?"))api("/api/flatten_account",{account:a.name})}));
    tr.appendChild(at);
    tb.appendChild(tr)});
  t.appendChild(tb);w.appendChild(t)}

/* Same split as the account grid: the Rev/Close buttons survive a size
   or average-price change; only a new or closed position rebuilds. */
const posCells=[];
function renderPositions(){
  const list=(L&&L.positions)||[];
  if(!changed("positions",[L&&L.ok,list.map(p=>p.account+"|"+p.instrument+"|"+(p.qty>0?"L":"S"))])){
    list.forEach((p,i)=>{const c=posCells[i];if(!c)return;
      setText(c.qty,String(Math.abs(p.qty)));
      setText(c.avg,p.avg_price?fmt(p.avg_price):"—")});
    return}
  posCells.length=0;
  const w=$("posWrap");clear(w);
  $("posHint").textContent=list.length?(list.length+" open"):"";
  if(!list.length){w.appendChild(el("div","empty","flat across all accounts"));return}
  const t=el("table"),h=el("thead"),hr=el("tr");
  [["Account",""],["Instrument",""],["Side",""],["Qty","num"],["Avg","num"],["",""]]
    .forEach(([x,c])=>hr.appendChild(el("th",c,x)));
  h.appendChild(hr);t.appendChild(h);
  const tb=el("tbody");
  list.forEach(p=>{
    const tr=el("tr"),c={};posCells.push(c);
    tr.appendChild(td(null,p.account));
    tr.appendChild(td(null,p.instrument));
    tr.appendChild(td(p.qty>0?"pos":"neg",p.qty>0?"LONG":"SHORT"));
    tr.appendChild(c.qty=td("num",Math.abs(p.qty)));
    tr.appendChild(c.avg=td("num",p.avg_price?fmt(p.avg_price):"—"));
    const act=el("td");act.style.textAlign="right";
    const grp=el("div","btnrow");grp.style.justifyContent="flex-end";
    grp.appendChild(btn("Rev","tiny",()=>{
      if(confirm("Reverse "+p.instrument+" on "+p.account+
        "?\n\nCloses "+(p.qty>0?"long":"short")+" "+Math.abs(p.qty)+
        " and opens the same size the other way."))
        api("/api/reverse_position",{account:p.account,instrument:p.instrument})}));
    grp.appendChild(btn("Close","tiny danger",()=>{
      if(confirm("Close "+p.instrument+" on "+p.account+"?"))
        api("/api/close_position",{account:p.account,instrument:p.instrument})}));
    act.appendChild(grp);
    tr.appendChild(act);tb.appendChild(tr)});
  t.appendChild(tb);w.appendChild(t)}

/* ================= modals ================= */
function openModal(title,sub,build){
  modalOpen=true;const m=$("modal");clear(m);
  m.appendChild(el("h3",null,title));
  if(sub)m.appendChild(el("div","sub",sub));
  build(m);
  const f=el("div","btnrow");f.style.marginTop="14px";
  f.appendChild(btn("CLOSE","ghost sm",closeModal));
  m.appendChild(f);
  $("veil").classList.add("show")}
function closeModal(){modalOpen=false;$("veil").classList.remove("show")}
$("veil").onclick=e=>{if(e.target===$("veil"))closeModal()};

function pickGroup(parent,options,current,onpick){
  const wrap=el("div","chiplist");const state={value:current};
  options.forEach(o=>{
    const b=el("div","pick"+(o.value===current?" on":""),o.label);
    b.onclick=()=>{state.value=o.value;
      [...wrap.children].forEach(c=>c.classList.remove("on"));
      b.classList.add("on");if(onpick)onpick(o.value)};
    wrap.appendChild(b)});
  parent.appendChild(wrap);return state}

function numField(parent,text,value){
  parent.appendChild(el("label",null,text));
  const i=el("input");i.type="number";i.value=value;parent.appendChild(i);return i}

/* Number input where an empty box means "inherit" (key absent from the
   scoped rule) rather than zero. */
function optNumField(parent,text,value){
  parent.appendChild(el("label",null,text));
  const i=el("input");i.type="number";i.placeholder="inherit";
  if(value!=null)i.value=value;parent.appendChild(i);return i}

/* Shared warning under a Direction picker, shown while INVERT is chosen. */
function invertWarn(parent){
  const w=el("div","sub");w.style.color="var(--yellow)";w.style.display="none";
  w.textContent="⚠ INVERT fades the signal: BUY↔SELL flipped, limit/stop entries "+
    "skipped, publisher CHANGE orders dropped — this side's own ATM manages its "+
    "stops. Inverting some accounts or strategies while others trade them straight "+
    "makes the fan-out hedge itself: fine when fading on your own broker account, "+
    "an account-closure event on a prop firm (the hedge guard will warn).";
  parent.appendChild(w);return w}

function fold(parent,open){
  const w=el("div","fold"),hd=el("div","foldhead"),bd=el("div","foldbody");
  const arr=el("span","arr",open?"▾":"▸"),ttl=el("span"),sum=el("span","fsum");
  hd.appendChild(arr);hd.appendChild(ttl);hd.appendChild(sum);
  if(!open)bd.style.display="none";
  hd.onclick=()=>{const vis=bd.style.display!=="none";
    bd.style.display=vis?"none":"";arr.textContent=vis?"▸":"▾"};
  w.appendChild(hd);w.appendChild(bd);parent.appendChild(w);
  return {body:bd,title:ttl,sum:sum}}

/* Mirror of Python atm_base_key: link a wire strategy name to its ATM
   template by stripping a '<known root>-' prefix and normalizing away
   case and separators, so 'GC-MacroZoneB' ≡ 'macro_zone_b'. */
function stratBase(n){
  n=(""+n).trim();const i=n.indexOf("-");
  if(i>0&&n.slice(i+1).trim()){const roots=new Set();
    (S.catalog||[]).forEach(p=>{roots.add(p.root);if(p.micro)roots.add(p.micro)});
    Object.entries(S.micro_map||{}).forEach(([k,v])=>{roots.add(k);if(v)roots.add(v)});
    if(roots.has(n.slice(0,i).trim().toUpperCase()))n=n.slice(i+1)}
  return n.toLowerCase().replace(/[^a-z0-9]/g,"")}
function stratLabel(k){
  const b=stratBase(k);
  const c=(S.strategy_choices||[]).find(c=>c.kind==="atm"&&c.base===b);
  return c?c.name:k}

function accountModal(a){
  const prof=(S.profiles&&S.profiles[a.name])||{};
  const rule=Object.assign({},S.rule_defaults,prof.default||{});
  const lim=a.limits||{};
  openModal(a.name,"Risk limits and trade profile.",m=>{
    const L1=el("div","fieldset");
    const tv=numField(L1,"Session target ($)",lim.target||0);
    const tm=pickGroup(L1,[{label:"OFF",value:"off"},{label:"SOFT",value:"soft"},
      {label:"HARD",value:"hard"}],lim.target_mode||"off");
    const sv=numField(L1,"Session stop ($)",lim.stop||0);
    const sm=pickGroup(L1,[{label:"OFF",value:"off"},{label:"SOFT",value:"soft"},
      {label:"HARD",value:"hard"}],lim.stop_mode||"off");
    L1.appendChild(btn("SAVE LIMITS","wide sm",()=>api("/api/limits",
      {account:a.name,target:tv.value,target_mode:tm.value,
       stop:sv.value,stop_mode:sm.value})));
    m.appendChild(L1);

    const P=el("div","fieldset");
    P.appendChild(el("label",null,"Symbols this account trades (none = all)"));
    const allowed=(prof.symbols_allowed||[]).slice();
    const sw=el("div","chiplist");
    (S.catalog||[]).forEach(p=>{
      const b=el("div","pick"+(allowed.includes(p.root)?" on":""),p.root);
      b.onclick=()=>{const i=allowed.indexOf(p.root);
        if(i<0)allowed.push(p.root);else allowed.splice(i,1);
        b.classList.toggle("on")};
      sw.appendChild(b)});
    P.appendChild(sw);

    P.appendChild(el("label",null,"Entries"));
    const en=pickGroup(P,[{label:"ON",value:true},{label:"OFF — exits only",value:false}],
      rule.enabled!==false);
    P.appendChild(el("label",null,"Contract size"));
    const sz=pickGroup(P,[{label:"INHERIT",value:"inherit"},{label:"MICROS",value:"micros"},
      {label:"FULL",value:"full"}],rule.size||"inherit");
    P.appendChild(el("label",null,"Contracts"));
    const qm=pickGroup(P,[{label:"COPY",value:"copy"},{label:"FIXED",value:"fixed"},
      {label:"MULTIPLE",value:"multiple"}],rule.qty_mode||"copy");
    const qv=numField(P,"Value (count, or multiplier)",rule.qty_value!=null?rule.qty_value:1);
    const cap=numField(P,"Max contracts per entry (0 = none)",rule.max_contracts||0);
    P.appendChild(el("label",null,"Direction"));
    const dir=pickGroup(P,[{label:"NORMAL",value:"normal"},{label:"INVERT",value:"invert"}],
      rule.direction||"normal",v=>{dwarn.style.display=v==="invert"?"":"none"});
    const dwarn=invertWarn(P);
    if((rule.direction||"normal")==="invert")dwarn.style.display="";
    const dl=numField(P,"Entry delay ms",rule.delay_ms||0);
    const stg=numField(P,"Stagger tranches (1 = off)",rule.stagger_entries||1);
    P.appendChild(el("label",null,"ATM override (blank = session)"));
    const at=el("select");at.appendChild(el("option","",""));
    (S.atm_available||[]).forEach(n=>{const o=el("option",null,n);o.value=n;at.appendChild(o)});
    at.value=rule.atm||"";P.appendChild(at);
    if(rule.ai)P.appendChild(el("div","sub","AI gate: "+rule.ai.provider+
      " — configure from the terminal"));

    /* ---- Scoped rules: per-symbol / per-strategy exceptions. Mirrors the
       terminal's S → 8 → account → R editor; each rule carries only the
       keys it overrides, everything else inherits from the default above.
       _ai_idx marks which served rule an edited rule descends from so the
       server can carry terminal-configured AI gates across reorders. ---- */
    const rf=fold(P,false);
    rf.title.textContent="Scoped rules";
    const rules=JSON.parse(JSON.stringify(prof.rules||[]));
    rules.forEach((r,i)=>r._ai_idx=i);
    const RB=rf.body;
    RB.appendChild(el("div","sub",
      "Exceptions to the profile above for specific symbols and/or publisher "+
      "strategies — the FIRST matching rule wins, so order matters. INHERIT "+
      "fields fall back to the account default. Exits are never blocked."));
    const rrows=el("div");RB.appendChild(rrows);
    const lower=s=>(""+s).toLowerCase();
    function rsum(){rf.sum.textContent=rules.length
      ?rules.length+" rule"+(rules.length>1?"s":"")+" — first match wins":"none"}
    function ruleScope(r){
      return ((r.symbols||[]).join(", ")||"any symbol")+" · "+
             ((r.strategies||[]).join(", ")||"any strategy")}
    function ruleBits(r){
      const b=[];
      if(r.enabled===false)b.push("entries OFF");
      if(r.enabled===true)b.push("entries on");
      if(r.direction)b.push(r.direction==="invert"?"INVERT":"normal");
      if(r.size)b.push(r.size==="inherit"?"size global":r.size);
      if(r.qty_mode==="copy")b.push("qty copy");
      if(r.qty_mode==="fixed")b.push("qty "+(r.qty_value!=null?r.qty_value:1));
      if(r.qty_mode==="multiple")b.push("qty ×"+(r.qty_value!=null?r.qty_value:1));
      if("max_contracts"in r)b.push("cap "+r.max_contracts);
      if("delay_ms"in r)b.push("delay "+r.delay_ms+"ms");
      if("stagger_entries"in r)b.push("stagger "+r.stagger_entries);
      if(r.atm!=null)b.push("ATM "+(r.atm||"session"));
      if(r.ai)b.push("AI:"+(r.ai.provider||"on"));
      return b.join(" · ")||"no overrides yet"}
    function drawRules(openIdx){
      rrows.textContent="";rsum();
      if(!rules.length)rrows.appendChild(el("div","sub",
        "No scoped rules — the profile above covers every signal."));
      rules.forEach((r,idx)=>{
        const fr=fold(rrows,idx===openIdx);
        const refresh=()=>{fr.title.textContent=(idx+1)+". "+ruleScope(r);
          fr.sum.textContent=ruleBits(r)};
        refresh();
        const B=fr.body;
        const set=(key,val,unset)=>{if(unset)delete r[key];else r[key]=val;refresh()};
        const numSet=(input,key,parse)=>{input.oninput=()=>{
          const v=parse(input.value);
          set(key,v,input.value.trim()===""||isNaN(v))}};

        B.appendChild(el("label",null,
          "Symbols it applies to (none = any — micro twins included, NQ covers MNQ)"));
        const sw2=el("div","chiplist");
        const roots=[...new Set([...(S.catalog||[]).map(p=>p.root),...(r.symbols||[])])];
        roots.forEach(root=>{
          const b=el("div","pick"+((r.symbols||[]).includes(root)?" on":""),root);
          b.onclick=()=>{const cur=r.symbols||[];const i=cur.indexOf(root);
            if(i<0)cur.push(root);else cur.splice(i,1);
            if(cur.length)r.symbols=cur;else delete r.symbols;
            b.classList.toggle("on");refresh()};
          sw2.appendChild(b)});
        B.appendChild(sw2);

        B.appendChild(el("label",null,
          "Publisher strategies it applies to (none = any) — matched by the exact name the wire sends"));
        const gw=el("div","chiplist");
        const names=[];
        [...(r.strategies||[]),...(S.strategies_seen||[])].forEach(n=>{
          if(!names.some(x=>lower(x)===lower(n)))names.push(n)});
        if(!names.length)B.appendChild(el("div","sub",
          "No strategies seen from the publisher yet — type one below."));
        names.forEach(name=>{
          const on=(r.strategies||[]).some(x=>lower(x)===lower(name));
          const b=el("div","pick"+(on?" on":""),name);
          b.onclick=()=>{const cur=r.strategies||[];
            const i=cur.findIndex(x=>lower(x)===lower(name));
            if(i<0)cur.push(name);else cur.splice(i,1);
            if(cur.length)r.strategies=cur;else delete r.strategies;
            b.classList.toggle("on");refresh()};
          gw.appendChild(b)});
        B.appendChild(gw);
        const srow=el("div","btnrow");srow.style.marginTop="6px";
        const si=el("input");si.placeholder="or type a strategy name";
        si.style.maxWidth="180px";srow.appendChild(si);
        srow.appendChild(btn("ADD","sm",()=>{
          const n=si.value.trim().replace(/;/g,"");if(!n)return;
          const cur=r.strategies||[];
          if(!cur.some(x=>lower(x)===lower(n)))cur.push(n);
          r.strategies=cur;drawRules(idx)}));
        B.appendChild(srow);

        B.appendChild(el("label",null,"Entries"));
        pickGroup(B,[{label:"INHERIT",value:"i"},{label:"ON",value:true},
          {label:"OFF — exits only",value:false}],
          "enabled"in r?r.enabled:"i",v=>set("enabled",v,v==="i"));
        B.appendChild(el("label",null,"Contract size (GLOBAL = session micro toggle)"));
        pickGroup(B,[{label:"INHERIT",value:"i"},{label:"MICROS",value:"micros"},
          {label:"FULL",value:"full"},{label:"GLOBAL",value:"inherit"}],
          "size"in r?r.size:"i",v=>set("size",v,v==="i"));
        B.appendChild(el("label",null,"Contracts"));
        pickGroup(B,[{label:"INHERIT",value:"i"},{label:"COPY",value:"copy"},
          {label:"FIXED",value:"fixed"},{label:"MULTIPLE",value:"multiple"}],
          "qty_mode"in r?r.qty_mode:"i",v=>set("qty_mode",v,v==="i"));
        numSet(optNumField(B,"Value (count, or multiplier)",
          "qty_value"in r?r.qty_value:null),"qty_value",parseFloat);
        numSet(optNumField(B,"Max contracts per entry (0 = none)",
          "max_contracts"in r?r.max_contracts:null),"max_contracts",
          v=>parseInt(v,10));
        B.appendChild(el("label",null,"Direction"));
        let rw;
        pickGroup(B,[{label:"INHERIT",value:"i"},{label:"NORMAL",value:"normal"},
          {label:"INVERT",value:"invert"}],
          "direction"in r?r.direction:"i",
          v=>{set("direction",v,v==="i");rw.style.display=v==="invert"?"":"none"});
        rw=invertWarn(B);
        if(r.direction==="invert")rw.style.display="";
        numSet(optNumField(B,"Entry delay ms","delay_ms"in r?r.delay_ms:null),
          "delay_ms",v=>parseInt(v,10));
        numSet(optNumField(B,"Stagger tranches (1 = off)",
          "stagger_entries"in r?r.stagger_entries:null),
          "stagger_entries",v=>parseInt(v,10));
        B.appendChild(el("label",null,"ATM override"));
        const at2=el("select");
        const oi=el("option",null,"inherit");oi.value="__inh";at2.appendChild(oi);
        if(r.atm===""){const o=el("option",null,"session default");o.value="";
          at2.appendChild(o)}
        (S.atm_available||[]).forEach(n=>{const o=el("option",null,n);o.value=n;
          at2.appendChild(o)});
        if(r.atm&&!(S.atm_available||[]).includes(r.atm)){
          const o=el("option",null,r.atm+" (missing)");o.value=r.atm;
          at2.appendChild(o)}
        at2.value="atm"in r?r.atm:"__inh";
        at2.onchange=()=>set("atm",at2.value,at2.value==="__inh");
        B.appendChild(at2);
        if(r.ai)B.appendChild(el("div","sub","AI gate: "+(r.ai.provider||"configured")+
          " — configure from the terminal; deleting this rule discards it"));

        const xr=el("div","btnrow");xr.style.marginTop="8px";
        xr.appendChild(btn("▲ EARLIER","sm",()=>{if(idx>0){
          rules.splice(idx-1,0,rules.splice(idx,1)[0]);drawRules(idx-1)}}));
        xr.appendChild(btn("▼ LATER","sm",()=>{if(idx<rules.length-1){
          rules.splice(idx+1,0,rules.splice(idx,1)[0]);drawRules(idx+1)}}));
        xr.appendChild(btn("DELETE RULE","sm danger",()=>{
          rules.splice(idx,1);drawRules()}));
        B.appendChild(xr)})}
    const arow=el("div","btnrow");arow.style.marginTop="8px";
    /* _ai_idx:-1 = "new rule": keeps the payload marker-aware so the server
       never falls back to positional AI-gate matching for this account. */
    arow.appendChild(btn("ADD RULE","sm",()=>{rules.push({_ai_idx:-1});
      drawRules(rules.length-1)}));
    RB.appendChild(arow);
    drawRules();

    P.appendChild(el("label",null,"Prop firm account (one trade at a time · flat by close)"));
    const pr=pickGroup(P,[{label:"OFF",value:false},{label:"PROP",value:true}],!!prof.prop);
    P.appendChild(el("label",null,"Prop firm (apex, topstep, mffu, tpt, tradeify, …) — sets safe close times"));
    const pfirm=el("input");pfirm.value=prof.prop_firm||"";P.appendChild(pfirm);
    P.appendChild(el("label",null,"Flat-by-close ET HH:MM (blank = firm preset)"));
    const pflat=el("input");pflat.value=prof.prop_flat_et||"";P.appendChild(pflat);
    P.appendChild(el("label",null,"Entry cutoff ET HH:MM (blank = firm preset)"));
    const pcut=el("input");pcut.value=prof.prop_cutoff_et||"";P.appendChild(pcut);

    P.appendChild(btn("SAVE PROFILE","wide on",()=>{
      const profiles=JSON.parse(JSON.stringify(S.profiles||{}));
      const e=profiles[a.name]||{};const d=e.default||{};
      d.enabled=en.value;d.size=sz.value;d.qty_mode=qm.value;
      d.qty_value=parseFloat(qv.value||"1");d.max_contracts=parseInt(cap.value||"0",10);
      d.direction=dir.value;d.delay_ms=parseInt(dl.value||"0",10);
      d.stagger_entries=parseInt(stg.value||"1",10);
      if(at.value)d.atm=at.value;else delete d.atm;
      e.default=d;
      if(allowed.length)e.symbols_allowed=allowed;else delete e.symbols_allowed;
      /* Drop rules with no real content (scope or override) — the server
         drops the same ones, which keeps the _ai_idx lists aligned. The
         masked `ai` marker is display-only and never counts as content. */
      const keepRules=rules.filter(r=>
        Object.keys(r).some(k=>k!=="_ai_idx"&&k!=="ai"));
      if(keepRules.length)e.rules=keepRules;else delete e.rules;
      if(pr.value){e.prop=true;
        if(pfirm.value.trim())e.prop_firm=pfirm.value.trim();else delete e.prop_firm;
        if(pflat.value.trim())e.prop_flat_et=pflat.value.trim();else delete e.prop_flat_et;
        if(pcut.value.trim())e.prop_cutoff_et=pcut.value.trim();else delete e.prop_cutoff_et}
      else{delete e.prop;delete e.prop_firm;delete e.prop_flat_et;delete e.prop_cutoff_et}
      profiles[a.name]=e;
      api("/api/profiles",{profiles:profiles}).then(closeModal)}));
    P.appendChild(btn("RESET PROFILE","wide sm danger",()=>{
      const profiles=JSON.parse(JSON.stringify(S.profiles||{}));
      delete profiles[a.name];
      api("/api/profiles",{profiles:profiles}).then(closeModal)}));
    m.appendChild(P)})}

function strategyModal(){
  openModal("STRATEGY","ATM template applied to entries.",m=>{
    const f1=fold(m,false);
    f1.title.textContent="Session ATM template";
    f1.sum.textContent=(S.atm_strategy||"—")+(S.follow_publisher_strategy?" · follow":"");
    const f=f1.body;
    f.appendChild(el("label",null,"Session ATM template"));
    let chosen;
    const list=S.atm_available||[];
    if(list.length)chosen=pickGroup(f,list.map(n=>({label:n,value:n})),S.atm_strategy);
    else{const i=el("input");i.value=S.atm_strategy||"";f.appendChild(i);
      chosen={get value(){return i.value}}}
    f.appendChild(el("label",null,"Publisher strategy"));
    const mode=pickGroup(f,[{label:"LOCKED — always mine",value:false},
      {label:"FOLLOW publisher",value:true}],S.follow_publisher_strategy);
    f.appendChild(btn("SAVE","wide on",()=>api("/api/strategy",
      {name:chosen.value,follow_publisher:mode.value}).then(closeModal)));

    const f2=fold(m,false);
    f2.title.textContent="Strategy → symbol filters";
    const G=f2.body;
    G.appendChild(el("div","sub",
      "A listed strategy only ENTERS trades on its symbols, on every account — matched by its ATM template, so the filter holds whatever name the wire uses. Exits are never filtered."));
    const map=JSON.parse(JSON.stringify(S.strategy_symbols||{}));
    const rows=el("div");G.appendChild(rows);
    const addw=el("div");
    function nFilters(){const n=Object.keys(map).length;
      f2.sum.textContent=n?n+" filter"+(n>1?"s":""):"none"}
    function redraw(openKey){
      rows.textContent="";nFilters();
      const keys=Object.keys(map).sort();
      if(!keys.length)rows.appendChild(el("div","sub","No filters — every strategy trades every symbol."));
      keys.forEach(k=>{
        const fr=fold(rows,k===openKey);
        const lbl=stratLabel(k);
        fr.title.textContent=lbl+(lbl.toLowerCase()!==k?" · "+k:"");
        const sum=()=>fr.sum.textContent=map[k].length
          ?("→ "+map[k].join(", ")):"no symbols — row is dropped on save";
        sum();
        fr.body.appendChild(el("div","sub","Symbols this strategy may enter — micro twins included (GC covers MGC)."));
        const sw=el("div","chiplist");
        const roots=[...new Set([...(S.catalog||[]).map(p=>p.root),...map[k]])];
        roots.forEach(r=>{
          const b=el("div","pick"+(map[k].includes(r)?" on":""),r);
          b.onclick=()=>{const i=map[k].indexOf(r);
            if(i<0)map[k].push(r);else map[k].splice(i,1);
            b.classList.toggle("on");sum()};
          sw.appendChild(b)});
        fr.body.appendChild(sw);
        const xr=el("div","btnrow");xr.style.marginTop="8px";
        const ci=el("input");ci.placeholder="other root, e.g. 6C";ci.style.maxWidth="130px";
        xr.appendChild(ci);
        xr.appendChild(btn("ADD SYMBOL","sm",()=>{
          const r=ci.value.trim().toUpperCase().replace(/[^A-Z0-9]/g,"");
          if(r&&!map[k].includes(r)){map[k].push(r);redraw(k)}}));
        xr.appendChild(btn("REMOVE FILTER","sm danger",()=>{delete map[k];redraw();redrawAdd()}));
        fr.body.appendChild(xr)})}
    G.appendChild(addw);
    function redrawAdd(){
      addw.textContent="";
      addw.appendChild(el("label",null,"Add a strategy — ★ installed ATM template, others were seen from the server"));
      const covered=new Set(Object.keys(map).map(stratBase));
      const av=(S.strategy_choices||[]).filter(c=>!covered.has(c.base));
      if(av.length){
        const cw=el("div","chiplist");
        av.forEach(c=>{
          const b=el("div","pick"+(c.kind==="atm"?" star":""),
            (c.kind==="atm"?"★ ":"")+c.name);
          b.onclick=()=>{const k=c.name.toLowerCase();
            if(!map[k])map[k]=[];redraw(k);redrawAdd()};
          cw.appendChild(b)});
        addw.appendChild(cw)}
      const ar=el("div","btnrow");ar.style.marginTop="6px";
      const gi=el("input");gi.placeholder="or type a strategy name";ar.appendChild(gi);
      ar.appendChild(btn("ADD","sm",()=>{
        const k=gi.value.trim().toLowerCase().replace(/;/g,"");
        if(!k)return;
        const b=stratBase(k);
        const dup=Object.keys(map).find(x=>x===k||(b&&stratBase(x)===b));
        if(dup){redraw(dup);return}   // already filtered under another spelling — open it
        map[k]=[];redraw(k);redrawAdd()}));
      addw.appendChild(ar)}
    redraw();redrawAdd();
    G.appendChild(btn("SAVE FILTERS","wide on",()=>api("/api/strategy_symbols",
      {strategy_symbols:map}).then(closeModal)))})}

/* ================= P&L calendar ================= */
/* Standalone daily record served by /api/pnl — built from the app's own
   trade tracking, no extra account data. Day net (balance-based, less any
   money that crossed the account boundary) is the authoritative number;
   per-trade rows are the drill-down and can still differ from the net by
   attribution drift, which is shown as "fees / unattributed". Deposits and
   withdrawals are never P&L and appear only under "transfers". */
const P={month:"",data:null,sel:null,timer:null,acct:null};

function pdate(s){const p=s.split("-");return new Date(+p[0],+p[1]-1,+p[2])}
function monthAdd(m,d){const p=m.split("-");
  const x=new Date(+p[0],+p[1]-1+d,1);
  return x.getFullYear()+"-"+String(x.getMonth()+1).padStart(2,"0")}
function monthLabel(m){return pdate(m+"-01")
  .toLocaleDateString(undefined,{month:"long",year:"numeric"})}
function dayLabel(s){return pdate(s)
  .toLocaleDateString(undefined,{weekday:"long",month:"long",day:"numeric"})}
function usd(v){return v==null?"—":(v<0?"-$":"+$")+Math.abs(v)
  .toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function usdc(v){const m=Math.abs(v),s=v<0?"-$":"+$";
  if(m>=1e6)return s+(m/1e6).toFixed(2)+"M";
  if(m>=1e4)return s+(m/1e3).toFixed(1)+"K";
  if(m>=1e3)return s+(m/1e3).toFixed(2)+"K";
  return s+m.toFixed(0)}
function pct(v){return v==null?"—":Math.round(v*100)+"%"}
function held(sec){if(sec==null||sec<0)return "—";
  if(sec<90)return Math.round(sec)+"s";
  if(sec<5400)return Math.round(sec/60)+"m";
  return Math.floor(sec/3600)+"h "+Math.round(sec%3600/60)+"m"}
function pnlCls(v){return v>0?"pos":v<0?"neg":"dim"}

/* ---- tooltip (enhances only — every value is also printed in a cell,
   tile or table, so nothing is gated behind hover) ---- */
function tipShow(e,rows){const t=$("ptip");clear(t);
  rows.forEach(([v,k])=>{const r=el("div","tr");
    r.appendChild(el("span","tv",v));
    if(k!=null)r.appendChild(el("span","tk",k));
    t.appendChild(r)});
  t.classList.remove("hide");tipMove(e)}
function tipMove(e){const t=$("ptip");if(t.classList.contains("hide"))return;
  const w=t.offsetWidth,h=t.offsetHeight;
  let x=e.clientX+14,y=e.clientY+12;
  if(x+w>innerWidth-8)x=e.clientX-w-10;
  if(y+h>innerHeight-8)y=e.clientY-h-10;
  t.style.left=x+"px";t.style.top=y+"px"}
function tipHide(){$("ptip").classList.add("hide")}

/* ---- per-day / per-slice statistics ---- */
function tradeStats(tr){
  /* Every statistic that describes a TRADE is built from the rows that
     were observed as one. An `unseen` row is a bare amount with no market,
     side or size — counting it is what let a session-rollover artifact
     stand as both the largest win and the largest loss of the month.
     `sum` is the deliberate exception and stays over every row: the
     "Fees / unattr" tile is exactly the remainder between the balance and
     the rows, so hiding a row from it would invent drift that isn't
     there. */
  const ob=tr.filter(t=>!t.unseen);
  const wins=ob.filter(t=>t.pnl>0),losses=ob.filter(t=>t.pnl<0);
  const gw=wins.reduce((s,t)=>s+t.pnl,0),gl=losses.reduce((s,t)=>s+t.pnl,0);
  const decided=wins.length+losses.length;
  const holds=ob.filter(t=>t.opened_ts!=null).map(t=>t.ts-t.opened_ts);
  return{n:ob.length,wins:wins.length,losses:losses.length,gw:gw,gl:gl,
    sum:tr.reduce((s,t)=>s+t.pnl,0),
    wr:decided?wins.length/decided:null,
    pf:gl<0?gw/-gl:(gw>0?Infinity:null),
    avgW:wins.length?gw/wins.length:null,
    avgL:losses.length?gl/losses.length:null,
    maxW:wins.length?Math.max.apply(null,wins.map(t=>t.pnl)):null,
    maxL:losses.length?Math.min.apply(null,losses.map(t=>t.pnl)):null,
    exp:ob.length?ob.reduce((s,t)=>s+t.pnl,0)/ob.length:null,
    longs:ob.filter(t=>t.side==="LONG"),
    shorts:ob.filter(t=>t.side==="SHORT"),
    hold:holds.length?holds.reduce((a,b)=>a+b,0)/holds.length:null}}

function ptile(box,k,v,cls){const d=el("div","tile");
  d.appendChild(el("div","k",k));
  d.appendChild(el("div","v "+(cls||""),v));
  box.appendChild(d)}

function fmtPf(pf){return pf==null?"—":pf===Infinity?"∞":pf.toFixed(2)}

/* ---- sparkline: 2px line + 10% area wash + zero hairline; crosshair
   snaps to the nearest point and drives the tooltip ---- */
function spark(parent,pts,color){
  clear(parent);
  if(pts.length<2){parent.appendChild(el("div","empty",
    "needs at least two data points"));return}
  const W=Math.max(320,parent.clientWidth||640),H=120,px=8,py=12;
  let lo=Math.min(0,...pts.map(p=>p.v)),hi=Math.max(0,...pts.map(p=>p.v));
  if(hi===lo){hi+=1;lo-=1}
  const xs=i=>px+i*(W-2*px)/(pts.length-1);
  const ys=v=>py+(hi-v)*(H-2*py)/(hi-lo);
  const ns="http://www.w3.org/2000/svg";
  const S_=(tag,at)=>{const n=document.createElementNS(ns,tag);
    for(const k in at)n.setAttribute(k,at[k]);return n};
  const svg=S_("svg",{viewBox:"0 0 "+W+" "+H});
  const zero=ys(0);
  svg.appendChild(S_("line",{x1:px,x2:W-px,y1:zero,y2:zero,
    stroke:"#212a3b","stroke-width":1}));
  let dLine="",dArea="";
  pts.forEach((p,i)=>{const c=(i?"L":"M")+xs(i).toFixed(1)+","+ys(p.v).toFixed(1);
    dLine+=c;dArea+=c});
  dArea+="L"+xs(pts.length-1).toFixed(1)+","+zero.toFixed(1)+
    "L"+xs(0).toFixed(1)+","+zero.toFixed(1)+"Z";
  svg.appendChild(S_("path",{d:dArea,fill:color,"fill-opacity":.1}));
  svg.appendChild(S_("path",{d:dLine,fill:"none",stroke:color,
    "stroke-width":2,"stroke-linejoin":"round","stroke-linecap":"round"}));
  const endV=pts[pts.length-1].v,endY=ys(endV);
  const endDot=S_("circle",{cx:xs(pts.length-1),cy:endY,
    r:4,fill:color,stroke:"#10151f","stroke-width":2});
  svg.appendChild(endDot);
  const endLbl=S_("text",{x:xs(pts.length-1)-9,
    y:endY<H/2?endY+16:endY-9,"text-anchor":"end",
    fill:"#dde4ef","font-size":11,"font-weight":700});
  endLbl.textContent=usd(endV);
  svg.appendChild(endLbl);
  const hair=S_("line",{y1:py-4,y2:H-py+4,stroke:"#2d394f",
    "stroke-width":1,visibility:"hidden"});
  const dot=S_("circle",{r:4,fill:color,stroke:"#10151f","stroke-width":2,
    visibility:"hidden"});
  svg.appendChild(hair);svg.appendChild(dot);
  svg.style.touchAction="none";
  svg.addEventListener("pointermove",e=>{
    const r=svg.getBoundingClientRect();
    const fx=(e.clientX-r.left)*W/r.width;
    let i=Math.round((fx-px)*(pts.length-1)/(W-2*px));
    i=Math.max(0,Math.min(pts.length-1,i));
    hair.setAttribute("x1",xs(i));hair.setAttribute("x2",xs(i));
    hair.setAttribute("visibility","visible");
    dot.setAttribute("cx",xs(i));dot.setAttribute("cy",ys(pts[i].v));
    dot.setAttribute("visibility","visible");
    tipShow(e,pts[i].rows||[[usd(pts[i].v),pts[i].k]])});
  svg.addEventListener("pointerleave",()=>{
    hair.setAttribute("visibility","hidden");
    dot.setAttribute("visibility","hidden");tipHide()});
  parent.appendChild(svg)}

/* ---- diverging mini-bar: grows right (profit) or left (loss) from a
   shared center baseline; rounded at the data end, square at center ---- */
function bbar(v,max){const w=el("div","bbar");w.appendChild(el("em"));
  const i=el("i",v>=0?"pos":"neg");
  const half=Math.min(50,max?Math.abs(v)/max*50:0);
  if(v>=0){i.style.left="50%";i.style.width=half+"%"}
  else{i.style.right="50%";i.style.width=half+"%"}
  w.appendChild(i);return w}

/* A trade with no staged label never went through the app's dispatch
   path — every surface that fires an order writes its own name. Watching
   the position open anyway means it came straight off a NinjaTrader
   chart, so it is Manual. Blank with no open timestamp is different: that
   is a position adopted at restart, whose label is lost rather than
   absent, and calling it Manual would be a guess. */
function stratOf(t){
  return t.strategy||(!t.unseen&&t.opened_ts!=null?"Manual":"")}

/* ---- account filter ----
   P.acct null means every managed account. Every number the page shows is
   read through these four, so one account and "all" walk the same code
   path and cannot drift apart. The payload already carries per-account
   P&L and tags each trade with its account, so filtering never refetches. */
/* `ar`, never `a` — "a." is reserved for live account rows, and the web
   UI guard checks every a.<field> against what /api/live actually sends. */
function dayPnl(rec){if(!P.acct)return rec.pnl||0;
  const ar=(rec.accounts||{})[P.acct];
  return ar&&ar.pnl!=null?ar.pnl:0}
function dayAdj(rec){if(!P.acct)return rec.adj||0;
  const ar=(rec.accounts||{})[P.acct];return ar?(ar.adj||0):0}
function dayTrades(rec){const t=rec.trades||[];
  return P.acct?t.filter(x=>x.account===P.acct):t}
/* A day the selected account sat out is not one of ITS trading days, so
   it must not count toward its day win rate or dilute its averages. */
function dayActive(rec){
  return dayTrades(rec).length>0||Math.abs(dayPnl(rec))>=0.005}

function netFor(d,acct){
  const days=d.days||{};
  return Object.keys(days).reduce((s,k)=>{
    const rec=days[k];
    if(!acct)return s+(rec.pnl||0);
    const ar=(rec.accounts||{})[acct];
    return s+(ar&&ar.pnl!=null?ar.pnl:0)},0)}

function renderAccts(d){
  const box=$("pAccts");clear(box);
  const names=d.accounts||[];
  /* One account is not a choice — don't spend a row of screen on it. */
  if(names.length<2){box.classList.add("hide");return}
  box.classList.remove("hide");
  box.appendChild(el("span","k","Account"));
  const chip=(label,val)=>{
    const b=el("button",P.acct===val?"on":null,label);
    const n=netFor(d,val);
    const amt=el("span","amt "+(P.acct===val?"":pnlCls(n)),usdc(n));
    b.appendChild(amt);
    b.onclick=()=>{P.acct=val;
      /* The drill-down may be showing a day this account sat out; re-pick
         against the new filter (pickDefaultDay reads P.acct, now set). */
      if(!P.sel||!d.days[P.sel]||!dayActive(d.days[P.sel]))
        P.sel=pickDefaultDay(d);
      renderPnl()};
    box.appendChild(b)};
  chip("All",null);
  names.forEach(n=>chip(n,n))}

function breakdown(tr,key){
  const m={};
  tr.forEach(t=>{const k=(key(t)||"unattributed");
    (m[k]=m[k]||[]).push(t)});
  return Object.keys(m).map(k=>{
    const s=tradeStats(m[k]);
    return{name:k,n:s.n,wr:s.wr,pnl:s.sum}})
    .sort((x,y)=>y.pnl-x.pnl)}

function breakTable(parent,title,rows){
  const box=el("div");
  box.appendChild(el("div","psec",title));
  if(!rows.length){box.appendChild(el("div","empty","no trades"));
    parent.appendChild(box);return}
  const max=Math.max(...rows.map(r=>Math.abs(r.pnl)));
  const t=el("table"),h=el("thead"),hr=el("tr");
  [["",""],["Trades","num"],["Win %","num"],["",""],["P&L","num"]]
    .forEach(([x,c])=>hr.appendChild(el("th",c,x)));
  h.appendChild(hr);t.appendChild(h);
  const tb=el("tbody");
  rows.forEach(r=>{
    const tr=el("tr");
    tr.appendChild(td(null,r.name));
    tr.appendChild(td("num dim",r.n));
    tr.appendChild(td("num dim",pct(r.wr)));
    const bc=el("td");bc.style.width="30%";bc.appendChild(bbar(r.pnl,max));
    tr.appendChild(bc);
    tr.appendChild(td("num "+pnlCls(r.pnl),usd(r.pnl)));
    tb.appendChild(tr)});
  t.appendChild(tb);
  const w=el("div","tblwrap");w.appendChild(t);box.appendChild(w);
  parent.appendChild(box)}

/* ---- month view ---- */
function pickDefaultDay(d){
  const keys=Object.keys(d.days||{}).sort().filter(k=>dayActive(d.days[k]));
  if(!keys.length)return null;
  if(d.session&&keys.indexOf(d.session)>=0)return d.session;
  const past=keys.filter(k=>k<=d.today);
  return past.length?past[past.length-1]:keys[keys.length-1]}

async function pnlLoad(){
  if(!P.month)P.month=localMonth();
  try{
    const d=await get("/api/pnl?month="+P.month);
    P.data=d;
    /* A month this account never traded in has no chip to stay on. */
    if(P.acct&&(d.accounts||[]).indexOf(P.acct)<0)P.acct=null;
    if(P.sel==null||!(d.days&&d.days[P.sel])||!dayActive(d.days[P.sel]))
      P.sel=pickDefaultDay(d);
    renderPnl();
  }catch(e){$("pcalHint").textContent="app offline";}}

function renderPnl(){
  const d=P.data;
  if(!d||d.month!==P.month)return;
  if(!changed("pnl",[d.month,d.days,P.sel,P.acct]))return;
  tipHide();
  $("pmTitle").textContent=monthLabel(P.month);
  const nowMonth=(d.today||"").slice(0,7);
  $("pmNext").disabled=nowMonth&&P.month>=nowMonth;
  $("pmPrev").disabled=!!(d.first_month&&P.month<=d.first_month);
  renderAccts(d);
  const days=d.days||{};
  const keys=Object.keys(days).sort().filter(k=>dayActive(days[k]));
  const allTrades=keys.reduce((acc,k)=>acc.concat(dayTrades(days[k])),[]);
  const net=keys.reduce((s,k)=>s+dayPnl(days[k]),0);

  /* hero + stat tiles */
  const hero=$("pmNet");
  hero.textContent=keys.length?usd(net):"—";
  hero.className="v "+(keys.length?pnlCls(net):"dim");
  $("pmHint").textContent=keys.length?""
    :(P.acct?"nothing recorded for "+P.acct+" this month"
            :"no trading days recorded yet");
  const tiles=$("pmTiles");clear(tiles);
  const green=keys.filter(k=>dayPnl(days[k])>0);
  const red=keys.filter(k=>dayPnl(days[k])<0);
  const ts=tradeStats(allTrades);
  const dayVals=keys.map(k=>dayPnl(days[k]));
  let streak=0;
  for(let i=keys.length-1;i>=0;i--){const v=dayPnl(days[keys[i]]);
    if(!streak){streak=v>0?1:v<0?-1:0;if(!streak)break}
    else if(streak>0&&v>0)streak++;
    else if(streak<0&&v<0)streak--;
    else break}
  ptile(tiles,"Trading days",keys.length||"—");
  ptile(tiles,"Green days",green.length,green.length?"pos":"dim");
  ptile(tiles,"Red days",red.length,red.length?"neg":"dim");
  ptile(tiles,"Day win rate",keys.length?pct(green.length/keys.length):"—");
  ptile(tiles,"Trades",ts.n||"—");
  ptile(tiles,"Trade win rate",pct(ts.wr));
  ptile(tiles,"Profit factor",fmtPf(ts.pf),
    ts.pf==null?"dim":ts.pf>=1?"pos":"neg");
  /* Profit factor and win rate are built from the trade rows; the hero net
     is built from account balances. Without this tile the two can disagree
     in sign (a positive factor over a losing month) with nothing on screen
     to say why — so the remainder between them is always shown. */
  const drift=net-ts.sum;
  ptile(tiles,"Fees / unattr",
    !keys.length?"—":Math.abs(drift)<0.005?"$0.00":usd(drift),
    !keys.length||Math.abs(drift)<0.005?"dim":pnlCls(drift));
  ptile(tiles,"Avg day",keys.length?usd(net/keys.length):"—",
    keys.length?pnlCls(net):"dim");
  ptile(tiles,"Best day",dayVals.length?usd(Math.max(...dayVals)):"—","pos");
  ptile(tiles,"Worst day",dayVals.length?usd(Math.min(...dayVals)):"—","neg");
  ptile(tiles,"Streak",streak?Math.abs(streak)+(streak>0?" green":" red"):"—",
    streak>0?"pos":streak<0?"neg":"dim");
  /* Deposits and withdrawals, held out of every P&L number above. Kept
     deliberately neutral in colour — funding an account is not a green
     day and pulling money out is not a red one. */
  const xfer=P.acct?((d.transfers_accounts||{})[P.acct]||0):(d.transfers||0);
  ptile(tiles,"Transfers",Math.abs(xfer)<0.005?"—":usd(xfer),"dim");

  /* equity curve */
  let cum=0;
  const pts=keys.map(k=>{cum+=dayPnl(days[k]);
    const lbl=pdate(k).toLocaleDateString(undefined,{month:"short",day:"numeric"});
    return{v:cum,k:lbl,rows:[[usd(cum),"cumulative"],
      [usd(dayPnl(days[k])),"day P&L"],
      [lbl,dayTrades(days[k]).length+" trades"]]}});
  $("peqHint").textContent=pts.length?"final "+usd(cum):"";
  spark($("pmEq"),pts,"#4bb2d1");

  /* calendar grid — Mon–Fri plus a week-total column (CME session dates
     are weekdays; Sunday's overnight belongs to Monday's session) */
  const cal=$("pcal");clear(cal);
  const head=el("div","pcal-head");
  ["Mon","Tue","Wed","Thu","Fri","Week"].forEach(x=>head.appendChild(el("div",null,x)));
  cal.appendChild(head);
  const first=pdate(P.month+"-01");
  const dim_=new Date(first.getFullYear(),first.getMonth()+1,0).getDate();
  const start=new Date(first);
  start.setDate(1-((first.getDay()+6)%7));   // back to Monday
  let wk=0;
  for(let ws=new Date(start);ws<=new Date(first.getFullYear(),first.getMonth(),dim_);
      ws.setDate(ws.getDate()+7)){
    wk++;
    const row=el("div","pcal-week");
    let wPnl=0,wTr=0,wAny=false;
    for(let i=0;i<7;i++){
      const dt=new Date(ws);dt.setDate(ws.getDate()+i);
      const iso=dt.getFullYear()+"-"+String(dt.getMonth()+1).padStart(2,"0")+
        "-"+String(dt.getDate()).padStart(2,"0");
      /* Filtered out for this account, so the cell is blank rather than
         showing somebody else's day. */
      const rec=days[iso]&&dayActive(days[iso])?days[iso]:null;
      const inMonth=dt.getMonth()===first.getMonth();
      if(rec&&inMonth){wPnl+=dayPnl(rec);wTr+=dayTrades(rec).length;wAny=true}
      if(i>=5)continue;                       // Sat/Sun fold into the week cell
      const cell=el("button","dcell");
      if(!inMonth){cell.className="dcell off";cell.disabled=true;
        row.appendChild(cell);continue}
      cell.appendChild(el("span","dnum",dt.getDate()));
      if(iso===d.today)cell.classList.add("today");
      if(rec){
        const dp=dayPnl(rec);
        const maxAbs=Math.max(...dayVals.map(Math.abs),1);
        const heat=(0.08+0.34*Math.min(1,Math.abs(dp)/maxAbs)).toFixed(3);
        if(dp>0)cell.style.background="rgba(47,191,132,"+heat+")";
        else if(dp<0)cell.style.background="rgba(239,88,101,"+heat+")";
        cell.appendChild(el("span","dpnl",usdc(dp)));
        const st=tradeStats(dayTrades(rec));
        cell.appendChild(el("span","dsub",
          st.n+(st.n===1?" trade":" trades")+(st.wr!=null?" · "+pct(st.wr):"")));
        if(iso===P.sel)cell.classList.add("sel");
        /* A click must visibly land somewhere: the drill-down renders in
           the card BELOW the calendar, which sits off-screen on most
           windows — without the scroll, clicking the (often already
           auto-selected) day looks like it did nothing. */
        cell.onclick=()=>{P.sel=iso;renderPnl();
          const pd=$("pday");
          if(!pd.classList.contains("hide"))
            pd.scrollIntoView({behavior:"smooth",block:"start"})};
        const rows=[[usd(dp),"net P&L"],[String(st.n),"trades"],
          [pct(st.wr),"win rate"]];
        /* Only worth breaking out when looking at the accounts together. */
        if(!P.acct)Object.keys(rec.accounts||{}).slice(0,4).forEach(n=>{
          const ap=rec.accounts[n].pnl;
          if(ap!=null)rows.push([usd(ap),n])});
        cell.addEventListener("pointerenter",e=>tipShow(e,rows));
        cell.addEventListener("pointermove",tipMove);
        cell.addEventListener("pointerleave",tipHide);
        cell.addEventListener("focus",e=>{const r=cell.getBoundingClientRect();
          tipShow({clientX:r.right,clientY:r.top},rows)});
        cell.addEventListener("blur",tipHide);
      }else{
        cell.disabled=true;
        cell.classList.add(iso>d.today?"future":"mute");
        if(iso<=d.today)cell.title="no trading recorded this day"}
      row.appendChild(cell)}
    const wc=el("div","wcell");
    wc.appendChild(el("span","dnum","Week "+wk));
    if(wAny){
      wc.appendChild(el("span","dpnl "+pnlCls(wPnl),usdc(wPnl)));
      wc.appendChild(el("span","dsub",wTr+(wTr===1?" trade":" trades")))}
    else wc.appendChild(el("span","dsub","—"));
    row.appendChild(wc);
    cal.appendChild(row)}
  $("pcalHint").textContent=keys.length
    ?keys.length+" trading day"+(keys.length>1?"s":"")
    :"records build as you trade";

  renderDayPanel(
    P.sel&&days[P.sel]&&dayActive(days[P.sel])?days[P.sel]:null,P.sel)}

/* ---- day drill-down ---- */
function renderDayPanel(day,date){
  const box=$("pday");clear(box);
  if(!day){box.classList.add("hide");return}
  box.classList.remove("hide");
  const tr=dayTrades(day).slice().sort((x,y)=>x.ts-y.ts);
  const s=tradeStats(tr);
  const dnet=dayPnl(day);
  const h=el("h2");
  h.appendChild(el("span","pday-title",dayLabel(date)
    +(P.acct?" · "+P.acct:"")));
  const hint=el("span","hint",usd(dnet)+" net · "+s.n+
    (s.n===1?" trade":" trades"));
  h.appendChild(hint);
  box.appendChild(h);

  const tiles=el("div","ptiles");
  ptile(tiles,"Net P&L",usd(dnet),pnlCls(dnet));
  ptile(tiles,"Trades",s.n||"—");
  ptile(tiles,"Win rate",pct(s.wr));
  ptile(tiles,"Profit factor",fmtPf(s.pf),
    s.pf==null?"dim":s.pf>=1?"pos":"neg");
  ptile(tiles,"Avg trade",usd(s.exp),pnlCls(s.exp||0));
  ptile(tiles,"Avg win",s.avgW==null?"—":usd(s.avgW),"pos");
  ptile(tiles,"Avg loss",s.avgL==null?"—":usd(s.avgL),"neg");
  ptile(tiles,"Largest win",s.maxW==null?"—":usd(s.maxW),"pos");
  ptile(tiles,"Largest loss",s.maxL==null?"—":usd(s.maxL),"neg");
  ptile(tiles,"Gross profit",usd(s.gw),s.gw>0?"pos":"dim");
  ptile(tiles,"Gross loss",usd(s.gl),s.gl<0?"neg":"dim");
  const drift=dnet-s.sum;
  ptile(tiles,"Fees / unattr",Math.abs(drift)<0.005?"$0.00":usd(drift),
    Math.abs(drift)<0.005?"dim":pnlCls(drift));
  /* Only on the days it actually happened — a transfer is the exception
     and earns a tile when present rather than a "$0.00" on every day. */
  const dxf=dayAdj(day);
  if(Math.abs(dxf)>=0.005)ptile(tiles,"Transfers",usd(dxf),"dim");
  ptile(tiles,"Longs",s.longs.length+(s.longs.length?" · "+usdc(
    s.longs.reduce((a,t)=>a+t.pnl,0)):""),
    pnlCls(s.longs.reduce((a,t)=>a+t.pnl,0)));
  ptile(tiles,"Shorts",s.shorts.length+(s.shorts.length?" · "+usdc(
    s.shorts.reduce((a,t)=>a+t.pnl,0)):""),
    pnlCls(s.shorts.reduce((a,t)=>a+t.pnl,0)));
  ptile(tiles,"Avg hold",held(s.hold));
  box.appendChild(tiles);

  if(tr.length>=2){
    /* This curve sums trade rows, while the Net P&L tile right above it
       comes from the balance. Letting it end on a number that silently
       contradicts that tile is exactly how "why does it say -207.74"
       happens — so when they differ, the heading says by how much. */
    const lbl="Intraday cumulative P&L (per closed trade)";
    const gap=dnet-s.sum;
    box.appendChild(el("div","psec",Math.abs(gap)<0.005?lbl
      :lbl+" — ends at "+usd(s.sum)+", "+usd(gap)+" unattributed"));
    let c=0;
    const pts=tr.map(t=>{c+=t.pnl;
      const when=new Date(t.ts*1000).toLocaleTimeString();
      return{v:c,k:when,rows:[[usd(c),"cumulative"],[usd(t.pnl),
        t.symbol+" "+t.side],[when,t.account]]}});
    const eq=el("div","eq");box.appendChild(eq);
    spark(eq,pts,"#4bb2d1")}

  const grid=el("div","pgrid3");
  breakTable(grid,"By symbol",breakdown(tr,t=>t.symbol));
  breakTable(grid,"By strategy",breakdown(tr,stratOf));
  const acctRows=Object.keys(day.accounts||{})
    .filter(n=>!P.acct||n===P.acct)
    .map(n=>{
      const mine=tr.filter(t=>t.account===n);
      return{name:n,n:mine.length,wr:tradeStats(mine).wr,
        pnl:day.accounts[n].pnl!=null?day.accounts[n].pnl:0}})
    .sort((x,y)=>y.pnl-x.pnl);
  breakTable(grid,"By account",acctRows);
  box.appendChild(grid);

  box.appendChild(el("div","psec","Trade log"));
  if(!tr.length){box.appendChild(el("div","empty",
    "no round-trips recorded — the day net came from balance changes"));return}
  const t=el("table"),th=el("thead"),hr=el("tr");
  [["Time",""],["Account",""],["Symbol",""],["Side",""],["Qty","num"],
   ["Strategy",""],["Entry","num"],["Held","num"],["P&L","num"]]
    .forEach(([x,c])=>hr.appendChild(el("th",c,x)));
  th.appendChild(hr);t.appendChild(th);
  const tb=el("tbody");
  tr.forEach(x=>{
    const r=el("tr");
    r.appendChild(td("dim",new Date(x.ts*1000).toLocaleTimeString()));
    r.appendChild(td(null,x.account));
    /* An `unseen` row is NinjaTrader's realized P&L for a fill no poll
       ever saw. The amount is real; the market, side and size are not
       known, so they are dashes rather than plausible-looking values. */
    r.appendChild(td(x.unseen?"dim":null,x.symbol||"—"));
    r.appendChild(td(x.unseen?"dim":x.side==="LONG"?"pos":"neg",
      x.unseen?"—":x.side));
    r.appendChild(td("num"+(x.unseen?" dim":""),x.unseen?"—":x.qty));
    r.appendChild(td("dim",stratOf(x)
      ||(x.unseen?"unobserved fill":"unattributed")));
    r.appendChild(td("num dim",x.entry!=null?fmt(x.entry):"—"));
    r.appendChild(td("num dim",
      !x.unseen&&x.opened_ts!=null?held(x.ts-x.opened_ts):"—"));
    const pc=td("num "+pnlCls(x.pnl),
      usd(x.pnl)+(x.approx||x.late?" ≈":""));
    if(x.late)pc.title="includes P&L that reached the balance after the "+
      "position had already closed — reconciled against NinjaTrader's own "+
      "realized total for the session";
    else if(x.unseen)pc.title="NinjaTrader's realized P&L moved while no poll "+
      "saw the position — a truncated dump, an outage or a restart. The "+
      "amount is NT's own; the market, side and size are unknown.";
    else if(x.approx)pc.title="several markets closed in the same poll window — "+
      "the cash move was split by contracts closed";
    r.appendChild(pc);
    tb.appendChild(r)});
  t.appendChild(tb);
  const w=el("div","tblwrap");w.appendChild(t);box.appendChild(w)}

/* ---- tab plumbing ---- */
function showView(v){
  $("viewTrade").classList.toggle("hide",v==="pnl");
  $("viewPnl").classList.toggle("hide",v!=="pnl");
  $("tabTrade").classList.toggle("on",v!=="pnl");
  $("tabPnl").classList.toggle("on",v==="pnl");
  try{sessionStorage.setItem("st-view",v)}catch(e){}
  clearInterval(P.timer);P.timer=null;
  if(v==="pnl"){pnlLoad();
    P.timer=setInterval(pnlLoad,15000)}
  else tipHide()}
function localMonth(){const n=new Date();
  return n.getFullYear()+"-"+String(n.getMonth()+1).padStart(2,"0")}
function pnlNav(delta){
  const nowMonth=((P.data&&P.data.today)||"").slice(0,7)||localMonth();
  const next=delta===0?nowMonth:monthAdd(P.month||nowMonth,delta);
  if(next>nowMonth)return;
  /* The archive knows where it begins, so stop there rather than let the
     reader page back through empty months forever looking for data. */
  const firstM=(P.data&&P.data.first_month)||"";
  if(firstM&&next<firstM)return;
  P.month=next;P.sel=null;P.data=null;
  invalidate("pnl");pnlLoad()}

/* ================= render ================= */
function renderChips(){
  const st=$("chState");st.textContent=S.status_text||S.state;
  st.className="chip "+(S.hard_stopped?"bad":S.paused?"warn":"on");
  const rd=$("chReady");rd.textContent=S.trade_ready?"READY":"NOT READY";
  rd.className="chip "+(S.trade_ready?"on":"bad");
  const nt=$("chNt");
  nt.textContent=(L&&L.ok)?"NT LINKED":"NT OFFLINE";
  nt.className="chip "+((L&&L.ok)?"info":"bad");
  const mc=$("chMicro");mc.textContent=S.micro_mode?"MICROS ON":"micros off";
  mc.className="chip "+(S.micro_mode?"info":"");
  $("chAtm").textContent="ATM "+(S.atm_strategy||"—")+
    (S.follow_publisher_strategy?" · follow":"");
  $("chCount").textContent=S.signal_count+" signals · v"+S.version;
  const p=$("btnPause");
  p.textContent=S.paused?"▶ RESUME":"⏸ PAUSE";
  p.className="sm "+(S.paused?"on":"");
  const rr=$("rrline");
  rr.textContent=S.rr.pool.length
    ?("rotation "+S.rr.pool.join(" → ")+" · owed: "+
      (S.rr.remaining.length?S.rr.remaining.join(", "):"(reshuffles next)")):""}

function renderFeed(){
  if(!changed("feed",S.events))return;
  const f=$("feed");clear(f);
  const ev=(S.events||[]).slice().reverse();
  if(!ev.length){f.appendChild(el("div","empty","waiting for signals…"));return}
  ev.forEach(e=>{const d=el("div");
    d.appendChild(el("span","t",new Date(e.ts*1000).toLocaleTimeString()));
    d.appendChild(el("span",null,e.text));f.appendChild(d)})}

function renderAtm(){
  const sel=$("tAtm");
  if(document.activeElement===sel)return;
  if(!changed("atm",[S.atm_available,S.atm_strategy]))return;
  const cur=sel.value||S.atm_strategy;clear(sel);
  const opts=(S.atm_available||[]).slice();
  if(S.atm_strategy&&!opts.includes(S.atm_strategy))opts.unshift(S.atm_strategy);
  opts.forEach(n=>{const o=el("option",null,n);o.value=n;sel.appendChild(o)});
  if(cur&&opts.includes(cur))sel.value=cur;else if(S.atm_strategy)sel.value=S.atm_strategy}

function render(){if(!S)return;
  renderChips();renderFeed();renderAtm();
  if(!modalOpen){renderFavs();renderPicker();renderSelected();renderAlt();renderQty()}
  note()}

/* Polls never queue behind a slow one: a tick that finds the previous
   request still in flight is skipped, so a NinjaTrader stall cannot pile
   up requests that then all land at once. */
let stateInFlight=false,liveInFlight=false,burstUntil=0;
async function refresh(){
  if(stateInFlight)return;
  stateInFlight=true;
  try{S=await get("/api/state");
    if(!instrument&&S.last_manual&&S.last_manual.instrument)
      instrument=S.last_manual.instrument;
    render()}
  catch(e){const st=$("chState");st.textContent="app offline";st.className="chip bad"}
  finally{stateInFlight=false}}

async function refreshLive(){
  if(liveInFlight)return;
  liveInFlight=true;
  try{L=await get("/api/live");
    // Outside the panel guards: the tables only redraw when their data
    // changes, but this timestamp must keep ticking so a frozen table is
    // visibly "unchanged" rather than "stopped".
    $("liveHint").textContent="updated "+new Date(L.ts*1000).toLocaleTimeString()+
      (L.stale?" · stale":"");
    if(!modalOpen){renderTiles();renderAccounts();renderPositions()}
    if(S)renderChips()}
  catch(e){$("liveHint").textContent="NinjaTrader link lost"}
  finally{liveInFlight=false}}

/* After a click NinjaTrader's state is about to change: poll the live
   view every 400ms for a few seconds so the fill or close shows the
   moment the app sees it, instead of on the next 2s tick. The live read
   is served from the app's shared snapshot stream, so this costs
   NinjaTrader nothing. */
function liveBurst(){burstUntil=performance.now()+6000;refreshLive()}
setInterval(()=>{if(performance.now()<burstUntil)refreshLive()},400);

/* ---- wiring ---- */
$("sideB").onclick=()=>setSide("long");
$("sideS").onclick=()=>setSide("short");
$("typeM").onclick=()=>setType("market");
$("typeL").onclick=()=>setType("limit");
$("qtyMinus").onclick=()=>bumpQty(-1);
$("qtyPlus").onclick=()=>bumpQty(1);
$("tQty").oninput=()=>{renderQty();note()};
$("tPrice").oninput=note;
$("instrSearch").oninput=renderPicker;
$("submit").onclick=submitOrder;
$("btnPause").onclick=()=>api("/api/pause",{paused:!S.paused});
$("btnFlatten").onclick=()=>{if(confirm("Flatten ALL positions on every managed account?"))
  api("/api/close_all",{})};
$("btnReconnect").onclick=()=>api("/api/reconnect",{});
$("btnMicro").onclick=()=>api("/api/micro",{});
$("btnResetPnl").onclick=()=>{if(confirm("Reset session P&L and re-snapshot balances?"))
  api("/api/reset_pnl",{})};
$("btnStrategy").onclick=()=>{if(S)strategyModal()};
$("tabTrade").onclick=()=>showView("trade");
$("tabPnl").onclick=()=>showView("pnl");
$("pmPrev").onclick=()=>pnlNav(-1);
$("pmNext").onclick=()=>pnlNav(1);
$("pmToday").onclick=()=>pnlNav(0);
addEventListener("resize",()=>{
  if(!$("viewPnl").classList.contains("hide")){invalidate("pnl");renderPnl()}});
document.addEventListener("keydown",e=>{if(e.key==="Escape"&&modalOpen)closeModal()});

setSide("long");setType("market");renderQty();
try{if(sessionStorage.getItem("st-view")==="pnl")showView("pnl")}catch(e){}
refresh();refreshLive();
setInterval(refresh,1500);
setInterval(refreshLive,2000);
</script></body></html>
"""


# ---------- Main ----------
async def main():
    global output_directory, active_account, atm_strategy, follow_publisher_strategy, nt_port
    global follower_accounts, roundrobin_accounts, micro_mode, micro_map, atm_aliases

    token, cfg = setup()
    active_account = cfg.get("account", "")
    follower_accounts = [a for a in cfg.get("follower_accounts", []) if a and a != active_account]
    roundrobin_accounts = sanitize_roundrobin(
        cfg.get("roundrobin_accounts", []), active_account, follower_accounts)
    atm_strategy = cfg.get("atm_strategy", "NQ_Med")
    follow_publisher_strategy = bool(cfg.get("follow_publisher_strategy", False))
    raw_aliases = cfg.get("atm_aliases")
    atm_aliases = ({str(k).strip(): str(v).strip() for k, v in raw_aliases.items()
                    if str(k).strip() and str(v).strip()}
                   if isinstance(raw_aliases, dict) else {})
    micro_mode = bool(cfg.get("micro_mode", False))
    micro_map = load_micro_map(cfg)
    nt_port = cfg.get("nt_port", 36973)
    strategy_symbols.clear()
    strategy_symbols.update(load_strategy_symbols(cfg))
    if strategy_symbols:
        logger.info(f"STRATEGY FILTERS LOADED  {strategy_symbols}")
    pub_strategies_seen.clear()
    pub_strategies_seen.extend(
        sanitize_ati(str(s).strip()) for s in (cfg.get("strategies_seen") or [])
        if isinstance(s, str) and sanitize_ati(str(s).strip()))
    del pub_strategies_seen[MAX_SEEN_STRATEGIES:]
    account_profiles.clear()
    account_profiles.update(load_account_profiles(cfg))
    if account_profiles:
        logger.info(f"PROFILES LOADED  accounts={sorted(account_profiles)}")
    load_front_months(cfg)
    if front_months:
        logger.info(f"FRONT MONTHS LOADED  {len(front_months)} roots "
                    f"(synced {_front_months_date or 'unknown'}) — bridge "
                    "refresh replaces on connect; calendar guards staleness")
    global live_bridge_enabled, live_bridge_port, nt_host_override
    live_bridge_enabled = bool(cfg.get("live_bridge_enabled", False))
    if live_bridge_enabled:
        # Refresh the token file on every start: the AddOn refuses all
        # connections without it, and NT's folder can be moved or restored
        # independently of this config.
        write_bridge_token()
    live_bridge_port = int(cfg.get("live_bridge_port", 36984))
    nt_host_override = str(cfg.get("nt_host", "") or "").strip()

    if cfg.get("output_directory"):
        output_directory = cfg["output_directory"]

    # One NinjaTrader state stream for everything that reads it — started
    # before the web UI so its first poll has a dump to answer with.
    start_snapshot_poller()
    web_url = start_web_ui(asyncio.get_running_loop(), cfg)
    if web_url:
        print(Fore.CYAN + f"  🌐  Web UI  →  {web_url}" + Style.RESET_ALL)

    while True:
        global _kb_stop
        _kb_stop = False
        shutdown.clear()
        reconnect_event.clear()

        tasks = [
            asyncio.create_task(listen(token)),
            asyncio.create_task(keyboard_loop()),
            asyncio.create_task(pause_indicator()),
            asyncio.create_task(balance_monitor()),
            asyncio.create_task(live_bridge_task()),
            asyncio.create_task(pnl_tracker_task()),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # Stop keyboard thread so it releases stdin for input()
        _kb_stop = True
        shutdown.set()

        # Take the web server down FIRST. Any request that arrives after the
        # loop stops would block its thread waiting for work the loop will
        # never run, and the browser polls twice a second — so this is the
        # difference between quitting instantly and quitting in ~20s.
        stop_web_ui()

        # Cancel remaining tasks — including in-flight deferred profile
        # legs (delayed/staggered entries) — and wait for threads to drain
        leg_tasks = list(_leg_tasks)
        for t in list(pending) + leg_tasks:
            t.cancel()
        await asyncio.gather(*pending, *leg_tasks, return_exceptions=True)

        # Check if auth failed
        result = None
        for t in done:
            try:
                result = t.result()
            except Exception:
                pass

        if result == "auth_failed":
            # Re-prompt for token — stdin is now free
            show_cursor()
            cfg = load_config()
            token = ask_token(cfg, force=True)
            cfg["token"] = token
            save_config(cfg)
            print(Fore.GREEN + "  ✔  Token updated. Reconnecting..." + Style.RESET_ALL)
            continue
        else:
            break

    stop_web_ui()
    stop_snapshot_poller()


def print_exit_summary():
    unpin_layout()
    clear()
    show_cursor()
    log_str = str(LOG_FILE)
    inner = 41  # fixed box inner width (45 total - 4 for │  ...│)
    print(f"\r\033[K" + Fore.CYAN + f"\n\r\033[K  ┌─ SESSION SUMMARY ─────────────────────────┐" + Style.RESET_ALL)
    sig_text = f"Signals received: {signal_count}"[:inner]
    print(f"\r\033[K" + Fore.CYAN + f"  │  {sig_text.ljust(inner)}│" + Style.RESET_ALL)
    # Show final P&L from cached balance (no ATI call on exit)
    if active_account and active_account in session_start_balances:
        final_bal = session_current_balances.get(active_account)
        if final_bal is not None:
            pnl = session_pnl(active_account, final_bal)
            pnl_str = f"${pnl:+,.2f}"
            pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
            pnl_line = f"Session P&L: {pnl_str}"[:inner]
            print(f"\r\033[K" + Fore.CYAN + f"  │  " + pnl_color + f"{pnl_line.ljust(inner)}" + Fore.CYAN + f"│" + Style.RESET_ALL)
    log_line = f"Log: {log_str}"[:inner]
    print(f"\r\033[K" + Fore.CYAN + f"  │  {log_line.ljust(inner)}│" + Style.RESET_ALL)
    print(f"\r\033[K" + Fore.CYAN + f"  └───────────────────────────────────────────┘" + Style.RESET_ALL)
    logger.info(f"SESSION END  signals={signal_count}")


if __name__ == "__main__":
    logger.info("SESSION START")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        # Restore terminal before printing summary
        if os.name != "nt" and _saved_termios is not None:
            _restore_termios()
        print_exit_summary()
