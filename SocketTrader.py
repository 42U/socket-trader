from __future__ import annotations

import asyncio
import websockets
import json
import logging
import logging.handlers
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
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from colorama import init, Fore, Style

# pip install pyfiglet colorama websockets
try:
    import pyfiglet
except ImportError:
    pyfiglet = None

__version__ = "0.3.2"

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
account_stops: dict[str, str] = {}  # account -> "hard" | "soft" once its session limit trips.
                                    # Absent = tradeable. NOT persisted (session-local lockout).
atm_strategy = "NQ_Med"        # ATM strategy template name (fallback)
follow_publisher_strategy = False  # If True, use the publisher's strategy per-signal when locally installed
micro_mode = False             # If True, incoming instruments are translated to their CME micro (NQ→MNQ)
nt_port = 36973                # NinjaTrader AT Interface port (default 36973)
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
session_contracts: set[str] = set()              # instruments traded this session
soft_stopped = False                              # True if soft stop triggered
hard_stopped = False                              # True if hard stop triggered
BALANCE_POLL_INTERVAL = 3                         # seconds between balance checks

# Auto-reset: futures session ends ~4:15 PM ET, reset P&L at 4:20 PM ET
ET = ZoneInfo("America/New_York")  # Eastern Time (handles EST/EDT automatically)
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
        "contracts": list(session_contracts),
        "signal_count": signal_count,
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
        global signal_count
        for name, bal in saved.get("start_balances", {}).items():
            session_start_balances[name] = bal
        session_contracts.update(saved.get("contracts", []))
        signal_count = saved.get("signal_count", 0)
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
    # Re-snapshot current balances as new starting point
    for name, bal in session_current_balances.items():
        session_start_balances[name] = bal
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
VALID_ORDER_TYPES = {"MARKET", "LIMIT", "STOP", "STOPLIMIT"}
VALID_TIF = {"DAY", "GTC"}


def sanitize_ati(value: str) -> str:
    """Strip characters that could break ATI line-based parsing or inject fields."""
    return (value
            .replace('\n', '').replace('\r', '').replace('\x00', '')
            .replace(';', ''))



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
_log_handler = logging.handlers.RotatingFileHandler(
    str(LOG_FILE), maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_log_handler)
if not IS_WINDOWS:
    try:
        os.chmod(LOG_FILE, 0o600)
    except OSError:
        pass

# ---------- Duplicate detection ----------
# Track recent signal IDs (the unique number at the end of each signal)
_recent_signal_ids: deque[str] = deque(maxlen=100)


def load_config() -> dict:
    """Load saved config from disk, or return empty dict."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
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
def target_accounts() -> list[str]:
    """Every account a signal fires on: the leader first, then each follower.

    The leader (active_account) is always traded. Followers mimic it. The
    list is de-duplicated with the leader kept first, so a follower that
    also names the leader can't get two order files for one signal. With no
    followers this is just [leader] — classic single-account mode.
    """
    seen: set[str] = set()
    out: list[str] = []
    for a in ([active_account] + list(follower_accounts)):
        if a and a not in seen:
            seen.add(a)
            out.append(a)
    return out


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


# Back-compat alias for any external callers
find_ninjatrader_incoming_windows = find_ninjatrader_incoming


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


def _nt_host(port: int = 36973) -> str:
    """Return the correct host for NinjaTrader ATI (handles WSL).

    Probes candidates on first call per port and caches the first that
    accepts a connection. If no candidate responds, returns the first
    candidate without caching so a later call can retry.
    """
    cached = _nt_host_cache.get(port)
    if cached is not None:
        return cached
    candidates = _nt_host_candidates()
    for host in candidates:
        if _probe_host(host, port):
            _nt_host_cache[port] = host
            return host
    return candidates[0] if candidates else "127.0.0.1"


def _query_ati(command: str, port: int = 36973, timeout: float = 2.0) -> str:
    """Send a command to NinjaTrader ATI and return the raw response text."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(timeout)
        s.connect((_nt_host(port), port))
        s.sendall(f"{command}\n".encode())
        parts = []
        while True:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                parts.append(chunk)
                # After first data arrives, shorten timeout — no more data = done
                s.settimeout(0.25)
            except socket.timeout:
                break
        return b"".join(parts).decode("utf-8", errors="ignore")
    except Exception:
        return ""
    finally:
        s.close()


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


def query_nt_positions(account: str, port: int = 36973) -> dict[str, int]:
    """Query NinjaTrader ATI for open positions on an account.

    Returns dict of instrument -> signed quantity (positive = long,
    negative = short). Flat positions are omitted.

    NT broadcasts the same position under multiple aliases (e.g.
    "NQ JUN26", "@NQ", "NQM26", "NQ M6"); we dedupe by preferring the
    first alias that contains a space (the human-readable "ROOT MONTHYY"
    form) since NT accepts that format back in PLACE / CLOSEPOSITION.
    """
    text = _query_ati("POSITIONS", port)
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
        m = re.match(r"^([A-Za-z]+?)[FGHJKMNQUVXZ]\d{1,2}$", s)
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
        return input().strip()

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
        return line.strip()


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
CONTROLS_TEXT = "P=PAUSE  B=BAL  T=LIMITS  C=CLOSE  R=RECONN  S=SETUP  ⇧X=EXIT"


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
        if start is not None and current is not None:
            pnl = current - start
            pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
            acct_info = f"{lead}: ${current:,.2f} (${pnl:+,.2f})"
            acct_info_colored = f"{lead}: ${current:,.2f} (" + pnl_color + f"${pnl:+,.2f}" + Fore.CYAN + Style.DIM + ")"
        elif current is not None:
            acct_info = f"{lead}: ${current:,.2f}"
            acct_info_colored = acct_info
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
DASH_TOTAL_ROWS = 10

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
PULSE_FRAMES = ["·", "•", "●", "◉", "●", "•", "·", " "]


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
def _account_option_line(index: int, account: dict, leader: str | None, followers: list[str]) -> str:
    name = account["name"]
    if name == leader:
        marker = " ◀ LEADER"
    elif name in followers:
        marker = " ＋ FOLLOWER"
    else:
        marker = ""
    return f"{index}. {name}  (${account['cash']:,.2f}){marker}"


def _account_menu_rows(accounts: list[dict], leader: str | None, followers: list[str]) -> tuple[list[str], int]:
    """Return printable account rows and content width for the account picker."""
    rows = [_account_option_line(i, a, leader, followers) for i, a in enumerate(accounts, 1)]
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


def _print_account_menu(accounts: list[dict], leader: str | None, followers: list[str]) -> None:
    rows, width = _account_menu_rows(accounts, leader, followers)
    title = "─ COPY TRADING — LEADER & FOLLOWERS "
    top = f"┌{title}{'─' * max(0, width + 2 - len(title))}┐"
    print(Fore.CYAN + "\n" + top + Style.RESET_ALL)
    if rows:
        for line in rows:
            print(Fore.CYAN + f"│  {line[:width].ljust(width)}│" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + f"│  {'NinjaTrader ATI unreachable — enter names.'.ljust(width)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {'Leader trades; followers mimic every signal.'.ljust(width)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  {'Followers: numbers/names, all, or ENTER=none.'.ljust(width)}│" + Style.RESET_ALL)
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
    """Select the LEADER account and the FOLLOWERS that mimic its trades.

    Every signal fires on the leader plus each follower. Falls back to the
    classic single-account flow (leader only, no followers) when the user
    picks no followers, keeping existing setups unchanged.
    """
    global active_account, follower_accounts, awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
    names = [a["name"] for a in accounts]

    _print_account_menu(accounts, active_account, follower_accounts)

    # --- Leader ---
    sys.stdout.write(Fore.WHITE + f"  LEADER [{active_account or 'none'}] ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw = (await asyncio.to_thread(read_line_raw)).strip()
    if raw:
        if raw.isdigit() and names and 1 <= int(raw) <= len(names):
            active_account = names[int(raw) - 1]
        else:
            active_account = raw

    # --- Followers ---
    sys.stdout.write(
        Fore.WHITE + "  FOLLOWERS (numbers/names, 'all', ENTER=none) ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw_f = await asyncio.to_thread(read_line_raw)
    follower_accounts = _parse_follower_tokens(raw_f, names, active_account)

    cfg = load_config()
    cfg["account"] = active_account
    cfg["follower_accounts"] = follower_accounts
    save_config(cfg)

    if follower_accounts:
        print(Fore.GREEN +
              f"  ✔  Leader {active_account}  +  {len(follower_accounts)} follower(s): "
              f"{', '.join(follower_accounts)}" + Style.RESET_ALL)
    else:
        print(Fore.GREEN + f"  ✔  Single account → {active_account}" + Style.RESET_ALL)
    logger.info(f"ACCOUNTS SET  leader={active_account}  followers={follower_accounts}")
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
async def setup_menu():
    """Show the setup submenu for config changes."""
    global awaiting_user_input
    _dash_enter_menu()
    awaiting_user_input = True
    show_cursor()
    cfg = load_config()
    current_server = cfg.get("ws_host", "not set")
    masked_token = "*" * min(len(cfg.get("token", "")), 33) or "not set"
    print(Fore.CYAN + "\n┌─ SETUP ──────────────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  1. Server    ({current_server[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  2. Token     ({masked_token[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    acct_label = active_account or "not set"
    if follower_accounts:
        acct_label = f"{active_account} +{len(follower_accounts)} copy"
    print(Fore.CYAN + f"│  3. Accounts  ({acct_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    mode_tag = "FOLLOW" if follow_publisher_strategy else "LOCKED"
    strat_label = f"{atm_strategy} · {mode_tag}"
    print(Fore.CYAN + f"│  4. Strategy  ({strat_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  5. Directory ({(output_directory or 'not set')[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  6. ATI Port  ({nt_port})" .ljust(53) + "│" + Style.RESET_ALL)
    micro_label = "ON — NQ→MNQ, ES→MES, …" if micro_mode else "OFF"
    print(Fore.CYAN + f"│  7. Micros    ({micro_label[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + "│  ESC to close                                    │" + Style.RESET_ALL)
    print(Fore.CYAN + "└──────────────────────────────────────────────────┘" + Style.RESET_ALL)
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
        cash = a["cash"]
        marker = " ◀" if name == active_account else (" ＋" if name in follower_accounts else "")
        start = session_start_balances.get(name)
        if start is not None:
            pnl = cash - start
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
            if chosen[0] == "_ALL_":
                confirm_msg = f"Close ALL {len(open_pos)} position{'s' if len(open_pos) != 1 else ''}?"
            else:
                direction = "LONG" if chosen[1] > 0 else "SHORT"
                confirm_msg = f"Close {chosen[0]} ({direction} {abs(chosen[1])})?"
            sys.stdout.write(f"\r\033[K" + Fore.YELLOW + f"  {confirm_msg} [y/N] " + Style.RESET_ALL)
            sys.stdout.flush()
            confirm = await asyncio.to_thread(get_key)
            if confirm.lower() == "y":
                if chosen[0] == "_ALL_":
                    for instrument, qty in open_pos.items():
                        fire_close_position(active_account, instrument)
                        sys.stdout.write("\r\033[K")
                        print(Fore.RED + f"  ⛔  CLOSEPOSITION → {instrument}" + Style.RESET_ALL)
                    logger.info(f"CLOSE ALL  account={active_account}  contracts={list(open_pos.keys())}")
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
                if follow_publisher and pub_strategy and validate_strategy(pub_strategy):
                    # Keep publisher's choice — it's installed locally
                    parts[11] = pub_strategy
                else:
                    if follow_publisher and pub_strategy and pub_strategy != atm:
                        logger.info(
                            f"STRATEGY FALLBACK  publisher='{pub_strategy}' not installed → using '{atm}'")
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

    A bare millisecond timestamp isn't unique — two writes in the same
    millisecond (which happens when copy-trading fans one signal out to
    several accounts, or the hard-stop loop closes multiple contracts
    back-to-back) would overwrite each other. A process-local monotonic
    counter guarantees uniqueness, and the lock makes the increment safe
    if filename generation is called from concurrent paths.
    """
    global _ati_write_seq
    with _ati_seq_lock:
        _ati_write_seq += 1
        seq = _ati_write_seq
    ts = time.strftime("%Y%m%d_%H%M%S")
    ms = int((time.time() % 1) * 1000)
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


def _with_account(signal_text: str, account: str) -> str:
    """Return a copy of a processed signal with field 1 (account) swapped.

    The signal that arrives already has the primary account in field 1;
    copy-trading rewrites just that field per target account, leaving the
    instrument, action, qty, strategy and signal-id fields untouched so
    every account trades the identical signal.
    """
    parts = signal_text.split(";")
    if len(parts) >= 2:
        parts[1] = sanitize_ati(account)
    return ";".join(parts)


async def dispatch_signal(raw_signal: str) -> list[str]:
    """Fan one signal out to every tradeable account, one order file each.

    Returns the accounts whose file was written successfully; failures are
    surfaced to the dashboard and logged but don't block the other legs.
    """
    accounts = tradeable_accounts()
    if not accounts:
        return []
    written: list[str] = []
    for acct in accounts:
        try:
            res = write_signal_to_file(_with_account(raw_signal, acct))
        except Exception as exc:
            res = exc
        if isinstance(res, Exception) or not res:
            logger.error(f"DISPATCH FAIL  account={acct}  result={res!r}")
            _dash_set_alert(
                Fore.RED + f"  ✖  Copy-trade write failed for {acct}" + Style.RESET_ALL)
        else:
            written.append(acct)
    if len(accounts) > 1:
        logger.info(f"COPY DISPATCH  wrote={written}  of={accounts}")
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


def fire_close_position(account: str, contract: str):
    """Write a CLOSEPOSITION command to the incoming folder."""
    if not output_directory:
        return
    cmd = f"CLOSEPOSITION;{sanitize_ati(account)};{sanitize_ati(contract)};;;;;;;;;;"
    filename = _next_ati_filename("close")
    filepath = os.path.join(output_directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(cmd)
        logger.info(f"CLOSEPOSITION  account={account}  contract={contract}  file={filename}")
    except Exception as exc:
        _dash_set_alert(Fore.RED + f"  ✖  Close position write error: {exc}" + Style.RESET_ALL)


def fire_cancel_all_orders(account: str):
    """Write a CANCELALLORDERS command so no working orders survive a hard stop."""
    if not output_directory:
        return
    cmd = f"CANCELALLORDERS;{sanitize_ati(account)};;;;;;;;;;;"
    filename = _next_ati_filename("cancelall")
    filepath = os.path.join(output_directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(cmd)
        logger.info(f"CANCELALLORDERS  account={account}  file={filename}")
    except Exception as exc:
        _dash_set_alert(Fore.RED + f"  ✖  Cancel-all write error: {exc}" + Style.RESET_ALL)


def close_account_positions(account: str) -> list[str]:
    """Close every open position on one account.

    Queries NT for actual positions and closes each one, unioned with
    any contracts tracked in session_contracts as a safety net in case
    the position query fails or is slow to refresh after a fast fill.
    Also cancels any working orders first so a pending entry can't
    leak through during the close.
    """
    if not account:
        return []

    # Cancel working orders first so they don't refill after we close
    fire_cancel_all_orders(account)

    closed: set[str] = set()

    try:
        positions = query_nt_positions(account, nt_port)
        for instrument, qty in positions.items():
            if qty != 0 and instrument:
                fire_close_position(account, instrument)
                closed.add(instrument)
    except Exception as e:
        logger.error(f"close_account_positions {account}  query error: {e}")

    # Safety net: anything tracked in session_contracts that we haven't
    # closed yet (e.g. position query was empty due to fill latency).
    for contract in session_contracts:
        if contract and contract not in closed:
            fire_close_position(account, contract)
            closed.add(contract)

    return sorted(closed)


def close_all_open_positions() -> list[str]:
    """Flatten every open position across all copy-trade target accounts.

    Safety-first: a session stop/target or manual close-all flattens every
    account the signal is copied to, not just the primary. Returns the union
    of contracts closed across accounts (single-account mode falls back to
    just the primary via target_accounts()).
    """
    all_closed: set[str] = set()
    for account in target_accounts():
        for contract in close_account_positions(account):
            all_closed.add(contract)
    return sorted(all_closed)


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

    positions = query_nt_positions(active_account, nt_port)
    cur_balance = session_current_balances.get(active_account)
    now = time.time()
    still_pending = []

    with _confirms_lock:
        for entry in _pending_confirms:
            elapsed = now - entry["ts"]
            instrument = entry["instrument"]
            pre_pos = entry["pre_pos"]
            pre_balance = entry.get("pre_balance")
            cur_pos = positions.get(instrument, 0)

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

    closed = close_account_positions(account)
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

            # Always poll balances for status bar display (non-blocking).
            # Session limits are enforced on realized CashValue deltas since
            # NT's TCP ATI doesn't push live equity; per-trade risk is handled
            # by NinjaTrader's own ATM stop/target on the entry.
            all_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
            for a in all_accounts:
                session_current_balances[a["name"]] = a["cash"]
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
                    continue
                current = session_current_balances.get(acct)
                if current is None:
                    continue
                limits = get_account_limits(acct)
                if limits["target"] == 0 and limits["stop"] == 0:
                    continue

                pnl = current - session_start_balances[acct]

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
    """Prompt user to set session target and stop for the active account."""
    global awaiting_user_input, soft_stopped
    if not active_account:
        _dash_set_alert(Fore.YELLOW + "  ⚠  Set an account first (press A)." + Style.RESET_ALL)
        return

    _dash_enter_menu()
    awaiting_user_input = True
    show_cursor()
    limits = get_account_limits(active_account)
    start_bal = session_start_balances.get(active_account)
    current_bal = await asyncio.to_thread(query_nt_balance, active_account)

    _lim_inner = 52
    _lim_title = f"─ SESSION LIMITS ({active_account}) "
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

    set_account_limits(active_account, target, target_mode, stop, stop_mode)
    # Changing the leader's limits lifts its soft (resumable) lockout so it
    # re-arms against the new numbers; a hard lock stays until reset/exit.
    if account_stops.get(active_account) == "soft":
        del account_stops[active_account]
    soft_stopped = False
    _recompute_session_lock()
    t_label = f"${target:+,.2f} ({target_mode})" if target else "off"
    s_label = f"${stop:+,.2f} ({stop_mode})" if stop else "off"
    # Will be visible on alert line after menu exit
    _dash_set_alert(Fore.GREEN + f"  ✔  {active_account} → Target: {t_label}  ·  Stop: {s_label}" + Style.RESET_ALL)

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

                # Restore persisted session if still in the same trading session
                restored = restore_session_state()

                # Snapshot account balances (realized CashValue) for the
                # session P&L baseline. When an ATM stop or target fills and
                # the position closes, CashValue jumps by the realized delta
                # — the monitor then trips the session limit if crossed.
                nt_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
                for a in nt_accounts:
                    if a["name"] not in session_start_balances:
                        session_start_balances[a["name"]] = a["cash"]
                    session_current_balances[a["name"]] = a["cash"]

                # Only announce a restore on first boot — mid-run reconnects
                # re-apply state that never left memory, and the row is
                # better spent confirming the reconnect below.
                if restored and not was_reconnect:
                    pnl_parts = []
                    for name in session_start_balances:
                        cur = session_current_balances.get(name)
                        if cur is not None:
                            pnl_parts.append(f"{name}: ${cur - session_start_balances[name]:+,.2f}")
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

                            # Track traded contracts for hard stop
                            sig_parts = raw_signal.split(";")
                            if len(sig_parts) >= 3 and sig_parts[2] and len(session_contracts) < MAX_SESSION_CONTRACTS:
                                session_contracts.add(sig_parts[2])

                            # Snapshot the LEADER's position BEFORE writing so
                            # fast fills don't make pre/post look identical.
                            # Fill confirmation tracks the leader; followers
                            # mirror the same signal.
                            instrument = sig_parts[2] if len(sig_parts) >= 3 else ""
                            action = sig_parts[3] if len(sig_parts) >= 4 else ""
                            pre_positions = await asyncio.to_thread(query_nt_positions, active_account, nt_port)
                            pre_pos = pre_positions.get(instrument, 0)
                            # Re-check state after the await — balance_monitor could
                            # have fired a soft/hard stop while we were querying NT.
                            # Without this, a signal in-flight during a stop can race
                            # the close and open a new position right after flatten.
                            if paused or not tradeable_accounts():
                                late_tag = "PAUSED" if paused else "LOCKED"
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag=late_tag))
                                logger.info(f"SIGNAL #{signal_count} ({late_tag} post-query)  {raw_signal}")
                                continue
                            # Fan the signal out to the leader + every tradeable
                            # follower.
                            written = await dispatch_signal(raw_signal)
                            if not written:
                                _dash_add_signal(format_signal(raw_signal, signal_count, lat_str, tag="BLOCKED"))
                                logger.warning(f"SIGNAL #{signal_count} (NO WRITE)  {raw_signal}")
                                continue
                            # Register fill confirmation on the leader if it fired
                            if active_account in written:
                                add_pending_confirm(raw_signal, sig_id, instrument, action, pre_pos)
                            copy_note = f"  → {len(written)} accts" if len(written) > 1 else ""
                            _dash_add_signal(format_signal(
                                raw_signal, signal_count, (lat_str + copy_note).strip()))
                            await signal_pulse("SIGNAL RECEIVED")
                            logger.info(f"SIGNAL #{signal_count}  accounts={written}  {raw_signal}")
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

    # Install strategy templates if output directory is configured
    if cfg.get("output_directory") and Path(cfg["output_directory"]).is_dir():
        nt_base = Path(cfg["output_directory"]).parent
        install_strategy_templates(nt_base)

    print()
    return cfg["token"], cfg


# ---------- Main ----------
async def main():
    global output_directory, active_account, atm_strategy, follow_publisher_strategy, nt_port
    global follower_accounts, micro_mode, micro_map

    token, cfg = setup()
    active_account = cfg.get("account", "")
    follower_accounts = [a for a in cfg.get("follower_accounts", []) if a and a != active_account]
    atm_strategy = cfg.get("atm_strategy", "NQ_Med")
    follow_publisher_strategy = bool(cfg.get("follow_publisher_strategy", False))
    micro_mode = bool(cfg.get("micro_mode", False))
    micro_map = load_micro_map(cfg)
    nt_port = cfg.get("nt_port", 36973)

    if cfg.get("output_directory"):
        output_directory = cfg["output_directory"]

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
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # Stop keyboard thread so it releases stdin for input()
        _kb_stop = True
        shutdown.set()

        # Cancel remaining tasks and wait for threads to drain
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

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
            pnl = final_bal - session_start_balances[active_account]
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
