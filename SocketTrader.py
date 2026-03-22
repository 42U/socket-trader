from __future__ import annotations

import asyncio
import websockets
import json
import logging
import logging.handlers
import shutil
import socket
import sys
import time
import tempfile
import random
import os
import re
import platform
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from colorama import init, Fore, Style

# pip install pyfiglet colorama websockets
try:
    import pyfiglet
except ImportError:
    pyfiglet = None

IS_WINDOWS = platform.system() == "Windows"

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
active_account = None          # Current NinjaTrader account name
atm_strategy = "NQ_Med"        # ATM strategy template name
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


# ---------- Risk management ----------
session_start_balances: dict[str, float] = {}   # account -> starting balance
session_current_balances: dict[str, float] = {}  # account -> latest polled balance
session_contracts: set[str] = set()              # instruments traded this session
soft_stopped = False                              # True if soft stop triggered
hard_stopped = False                              # True if hard stop triggered
BALANCE_POLL_INTERVAL = 3                         # seconds between balance checks

# Auto-reset: futures session ends ~4:15 PM ET, reset P&L at 4:20 PM ET
ET = timezone(timedelta(hours=-5))  # Eastern Time (EST; no DST handling needed — close is 4:20 either way)
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
    """Persist session P&L data to config for crash recovery."""
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
    return False


def reset_session_pnl():
    """Re-snapshot all account balances and clear session state."""
    global soft_stopped, hard_stopped, signal_count, _last_auto_reset_date
    # Re-snapshot current balances as new starting point
    for name, bal in session_current_balances.items():
        session_start_balances[name] = bal
    session_contracts.clear()
    soft_stopped = False
    hard_stopped = False
    signal_count = 0
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


# ---------- NinjaTrader incoming folder detection ----------
def find_ninjatrader_incoming_windows() -> str | None:
    """Search common Windows locations for NinjaTrader 8\\incoming."""
    candidates = []

    # Most common: Documents\NinjaTrader 8\incoming
    docs = Path.home() / "Documents" / "NinjaTrader 8" / "incoming"
    candidates.append(docs)

    # Sometimes under OneDrive\Documents
    onedrive = Path.home() / "OneDrive" / "Documents" / "NinjaTrader 8" / "incoming"
    candidates.append(onedrive)

    # Check all drives for NinjaTrader 8\incoming at root level
    for drive_letter in "CDEFGH":
        candidates.append(Path(f"{drive_letter}:/NinjaTrader 8/incoming"))

    for path in candidates:
        if path.is_dir():
            return str(path.resolve())

    # Broader search: look for any NinjaTrader 8 folder under home
    home = Path.home()
    try:
        for p in home.rglob("NinjaTrader 8"):
            incoming = p / "incoming"
            if incoming.is_dir():
                return str(incoming.resolve())
    except (PermissionError, OSError):
        pass

    return None


def detect_or_ask_directory(cfg: dict) -> str | None:
    """Determine the output directory from config, auto-detect, or user input."""
    # If already saved in config, verify it still exists
    saved = cfg.get("output_directory")
    if saved and Path(saved).is_dir():
        return saved

    if IS_WINDOWS:
        print(Fore.CYAN + "\n  🔍  Searching for NinjaTrader 8 incoming folder..." + Style.RESET_ALL)
        found = find_ninjatrader_incoming_windows()
        if found:
            print(Fore.GREEN + f"  ✔  Found: {found}" + Style.RESET_ALL)
            confirm = input(Fore.WHITE + "  Use this path? [Y/n] " + Style.RESET_ALL).strip()
            if confirm.lower() != "n":
                return found
        else:
            print(Fore.YELLOW + "  ⚠  Could not auto-detect NinjaTrader 8 incoming folder." + Style.RESET_ALL)

    # Linux or Windows fallback: ask the user
    if not IS_WINDOWS:
        print(Fore.CYAN + "\n┌─ LINUX DETECTED ─────────────────────────────────────┐" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter the path to your signal output folder.        │" + Style.RESET_ALL)
        print(Fore.CYAN + "│  (NinjaTrader incoming folder or any target dir)      │" + Style.RESET_ALL)
        print(Fore.CYAN + "└──────────────────────────────────────────────────────┘" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + "\n  Enter the NinjaTrader 8 incoming folder path manually:" + Style.RESET_ALL)

    while True:
        raw = input(Fore.WHITE + "  PATH ▸ " + Style.RESET_ALL).strip().strip('"').strip("'")
        if not raw:
            print(Fore.YELLOW + "  ↩  No directory set. You can set one later with D key." + Style.RESET_ALL)
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


def _nt_host() -> str:
    """Return the correct host for NinjaTrader ATI (handles WSL)."""
    if IS_WINDOWS:
        return "127.0.0.1"
    # WSL: NinjaTrader runs on the Windows host
    try:
        with open("/etc/resolv.conf") as f:
            for line in f:
                if line.strip().startswith("nameserver"):
                    return line.split()[1]
    except Exception:
        pass
    return "127.0.0.1"


def _query_ati(command: str, port: int = 36973, timeout: float = 2.0) -> str:
    """Send a command to NinjaTrader ATI and return the raw response text."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(timeout)
        s.connect((_nt_host(), port))
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


def query_nt_accounts(port: int = 36973, timeout: float = 3.0) -> list[dict]:
    """Query NinjaTrader ATI for connected accounts with balances.

    Returns list of dicts: [{"name": "Sim101", "cash": 28857.02}, ...]
    """
    text = _query_ati("ACCOUNTS", port, timeout)
    if not text:
        return []
    accounts = {}
    for m in re.finditer(r"CashValue\|([^\x00]+)\x00([^\x00]+)", text):
        name, val = m.group(1), m.group(2)
        try:
            float(name)  # skip bare value (no account name)
        except ValueError:
            try:
                accounts[name] = float(val)
            except ValueError:
                accounts[name] = 0.0
    return [{"name": n, "cash": v} for n, v in accounts.items()]


def query_nt_positions(account: str, port: int = 36973) -> dict[str, int]:
    """Query NinjaTrader ATI for open positions on an account.

    Returns dict of instrument -> market position quantity.
    Positive = long, negative = short, 0 = flat.
    e.g. {"NQ 06-26": 1, "ES 06-26": -2}
    """
    text = _query_ati("POSITIONS", port)
    if not text:
        return {}
    # Log raw response so we can verify/fix parsing against live ATI
    logger.debug(f"ATI POSITIONS raw ({len(text)} bytes): {repr(text[:500])}")
    # ATI POSITIONS response contains MarketPosition|Account\x00Value
    # and Quantity|Account\x00Value patterns per instrument
    positions = {}
    # Find all instrument sections with market position for our account
    # Pattern: instrument data comes in blocks separated by \x00
    # Look for MarketPosition and Quantity entries for the account
    for m in re.finditer(r"MarketPosition\|([^\x00]+)\x00([^\x00]+)", text):
        key, val = m.group(1), m.group(2)
        if account in key:
            # key format varies: could be "Account InstrumentName" or just "InstrumentName"
            instrument = key.replace(account, "").strip()
            if instrument:
                # val is typically "Long", "Short", or "Flat"
                positions[instrument] = val
    # Also look for Quantity to get actual position size
    quantities = {}
    for m in re.finditer(r"Quantity\|([^\x00]+)\x00([^\x00]+)", text):
        key, val = m.group(1), m.group(2)
        if account in key:
            instrument = key.replace(account, "").strip()
            if instrument:
                try:
                    qty = int(float(val))
                    direction = positions.get(instrument, "")
                    if "Short" in direction:
                        qty = -qty
                    elif "Flat" in direction:
                        qty = 0
                    quantities[instrument] = qty
                except ValueError:
                    pass
    logger.debug(f"ATI POSITIONS parsed: {quantities}")
    return quantities


def ask_account(cfg: dict) -> str:
    """Get NinjaTrader account name from config or prompt user.

    On first run, queries ATI for accounts or prompts manually.
    Saves choice to config and reuses it on subsequent runs.
    """
    saved = cfg.get("account")
    if saved:
        return saved

    # First run — try to auto-detect accounts from NinjaTrader ATI
    accounts = query_nt_accounts(nt_port)
    if accounts:
        print(Fore.CYAN + "\n┌─ NINJATRADER ACCOUNTS (auto-detected) ────────────────┐" + Style.RESET_ALL)
        for i, a in enumerate(accounts, 1):
            line = f"{i}. {a['name']}  (${a['cash']:,.2f})"
            print(Fore.CYAN + f"│  {line.ljust(54)}│" + Style.RESET_ALL)
        print(Fore.CYAN + "└───────────────────────────────────────────────────────┘" + Style.RESET_ALL)
        if len(accounts) == 1:
            print(Fore.GREEN + f"  ✔  Auto-selected: {accounts[0]['name']}" + Style.RESET_ALL)
            return accounts[0]["name"]
        while True:
            choice = input(Fore.WHITE + "  SELECT # ▸ " + Style.RESET_ALL).strip()
            if choice.isdigit() and 1 <= int(choice) <= len(accounts):
                return accounts[int(choice) - 1]["name"]
            print(Fore.YELLOW + f"  ⚠  Enter 1-{len(accounts)}." + Style.RESET_ALL)

    # ATI not available — manual entry
    print(Fore.CYAN + "\n┌─ NINJATRADER ACCOUNT ─────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Enter your NinjaTrader account name.                 │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  This replaces the Sim account in incoming signals.   │" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    while True:
        acct = input(Fore.WHITE + "  ACCOUNT ▸ " + Style.RESET_ALL).strip()
        if acct:
            return acct
        print(Fore.YELLOW + "  ⚠  Account cannot be empty." + Style.RESET_ALL)


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
        start = session_start_balances.get(active_account)
        current = session_current_balances.get(active_account)
        if start is not None and current is not None:
            pnl = current - start
            pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
            acct_info = f"{active_account}: ${current:,.2f} (${pnl:+,.2f})"
            acct_info_colored = f"{active_account}: ${current:,.2f} (" + pnl_color + f"${pnl:+,.2f}" + Fore.CYAN + Style.DIM + ")"
        elif current is not None:
            acct_info = f"{active_account}: ${current:,.2f}"
            acct_info_colored = acct_info
        elif start is not None:
            acct_info = f"{active_account}: ${start:,.2f}"
            acct_info_colored = acct_info
        else:
            acct_info = active_account
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
                art = pyfiglet.figlet_format("VOIDORIGIN", font=font)
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


# ---------- Dynamic ASCII banner ----------
def build_banner():
    width = term_width()
    if pyfiglet:
        for font in ["block", "banner3-D", "banner3", "doom", "larry3d", "big", "standard", "small"]:
            try:
                art = pyfiglet.figlet_format("VOIDORIGIN", font=font)
                if max(len(l) for l in art.splitlines()) <= width:
                    break
            except Exception:
                continue
        else:
            art = pyfiglet.figlet_format("VOIDORIGIN", font="small")
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
        row("SOCKET TRADER", width),
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
    for frame in PULSE_FRAMES:
        sys.stdout.write(f"\r\033[K{Fore.GREEN}{frame} {label}{Style.RESET_ALL}")
        sys.stdout.flush()
        await asyncio.sleep(0.05)
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()


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
            sys.stdout.write(Fore.YELLOW + "\r\033[K" + PAUSE_FRAMES[i % len(PAUSE_FRAMES)] + Style.RESET_ALL)
            sys.stdout.flush()
            i += 1
            await asyncio.sleep(0.35)
        else:
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

    if not output_directory or not active_account:
        print()
        print(status_bar("SESSION ACTIVE  ·  AWAITING SIGNALS"))
    print()
    awaiting_directory_input = False
    awaiting_user_input = False


# ---------- Account prompt ----------
async def prompt_account():
    global active_account, awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    # Try to auto-detect accounts from NinjaTrader ATI
    accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
    if accounts:
        print(Fore.CYAN + "\n┌─ CHANGE ACCOUNT ─────────────────────────────────┐" + Style.RESET_ALL)
        for i, a in enumerate(accounts, 1):
            marker = " ◀" if a["name"] == active_account else ""
            line = f"{i}. {a['name']}  (${a['cash']:,.2f}){marker}"
            print(Fore.CYAN + f"│  {line.ljust(49)}│" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter # to select, or type a name manually.     │" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
        print(Fore.CYAN + "└───────────────────────────────────────────────────┘" + Style.RESET_ALL)
        sys.stdout.write(Fore.WHITE + "  ACCOUNT ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw = await asyncio.to_thread(read_line_raw)
        if raw == "":
            print(Fore.YELLOW + "  ↩  No change — keeping current account." + Style.RESET_ALL)
        elif raw.strip().isdigit() and 1 <= int(raw.strip()) <= len(accounts):
            active_account = accounts[int(raw.strip()) - 1]["name"]
            cfg = load_config()
            cfg["account"] = active_account
            save_config(cfg)
            print(Fore.GREEN + f"  ✔  Account set → {active_account}" + Style.RESET_ALL)
        elif raw.strip():
            active_account = raw.strip()
            cfg = load_config()
            cfg["account"] = active_account
            save_config(cfg)
            print(Fore.GREEN + f"  ✔  Account set → {active_account}" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + "\n┌─ CHANGE ACCOUNT ─────────────────────────────────┐" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter new NinjaTrader account name.              │" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
        if active_account:
            print(Fore.CYAN + f"│  Current: {active_account[:38].ljust(38)}│" + Style.RESET_ALL)
        print(Fore.CYAN + "└───────────────────────────────────────────────────┘" + Style.RESET_ALL)
        sys.stdout.write(Fore.WHITE + "  ACCOUNT ▸ " + Style.RESET_ALL)
        sys.stdout.flush()
        raw = await asyncio.to_thread(read_line_raw)
        if raw == "":
            print(Fore.YELLOW + "  ↩  No change — keeping current account." + Style.RESET_ALL)
        else:
            active_account = raw.strip()
            cfg = load_config()
            cfg["account"] = active_account
            save_config(cfg)
            print(Fore.GREEN + f"  ✔  Account set → {active_account}" + Style.RESET_ALL)

    if not output_directory or not active_account:
        print()
        print(status_bar("SESSION ACTIVE  ·  AWAITING SIGNALS"))
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
                print(Fore.GREEN + f"  ✔  Port set → {nt_port}" + Style.RESET_ALL)
            else:
                print(Fore.RED + "  ✖  Port must be between 1 and 65535." + Style.RESET_ALL)
        except ValueError:
            print(Fore.RED + "  ✖  Invalid port number." + Style.RESET_ALL)

    print()
    awaiting_user_input = False


# ---------- ATM Strategy prompt ----------
async def prompt_strategy():
    global atm_strategy, awaiting_user_input
    awaiting_user_input = True
    show_cursor()
    available = list_atm_strategies()
    print(Fore.CYAN + "\n┌─ ATM STRATEGY TEMPLATE ───────────────────────────┐" + Style.RESET_ALL)
    if available:
        for i, name in enumerate(available, 1):
            marker = " ◀" if name == atm_strategy else ""
            line = f"{i}. {name}{marker}"
            print(Fore.CYAN + f"│  {line.ljust(49)}│" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter # to select, or type a name manually.     │" + Style.RESET_ALL)
    else:
        print(Fore.CYAN + "│  No templates found in AtmStrategy directory.     │" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Type a strategy name manually.                   │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  Current: {atm_strategy[:39].ljust(39)}│" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────┘" + Style.RESET_ALL)
    sys.stdout.write(Fore.WHITE + "  STRATEGY ▸ " + Style.RESET_ALL)
    sys.stdout.flush()
    raw = await asyncio.to_thread(read_line_raw)

    if raw == "":
        print(Fore.YELLOW + "  ↩  No change — keeping current strategy." + Style.RESET_ALL)
    elif available and raw.strip().isdigit() and 1 <= int(raw.strip()) <= len(available):
        atm_strategy = available[int(raw.strip()) - 1]
        cfg = load_config()
        cfg["atm_strategy"] = atm_strategy
        save_config(cfg)
        print(Fore.GREEN + f"  ✔  ATM Strategy set → {atm_strategy}" + Style.RESET_ALL)
    else:
        name = raw.strip()
        if validate_strategy(name):
            atm_strategy = name
            cfg = load_config()
            cfg["atm_strategy"] = atm_strategy
            save_config(cfg)
            print(Fore.GREEN + f"  ✔  ATM Strategy set → {atm_strategy}" + Style.RESET_ALL)
        else:
            print(Fore.RED + f"  ✖  '{name}' not found in templates/AtmStrategy/." + Style.RESET_ALL)
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
    awaiting_user_input = True
    show_cursor()
    cfg = load_config()
    current_server = cfg.get("ws_host", "not set")
    masked_token = "*" * min(len(cfg.get("token", "")), 33) or "not set"
    print(Fore.CYAN + "\n┌─ SETUP ──────────────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  1. Server    ({current_server[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  2. Token     ({masked_token[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  3. Account   ({(active_account or 'not set')[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  4. Strategy  ({atm_strategy[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  5. Directory ({(output_directory or 'not set')[:33]})" .ljust(53) + "│" + Style.RESET_ALL)
    print(Fore.CYAN + f"│  6. ATI Port  ({nt_port})" .ljust(53) + "│" + Style.RESET_ALL)
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
        await prompt_account()
    elif key == "4":
        await prompt_strategy()
    elif key == "5":
        await prompt_directory()
    elif key == "6":
        await prompt_port()
    else:
        sys.stdout.write("\r\033[K")


# ---------- Balances display ----------
async def show_balances():
    """Display all NinjaTrader account balances with session P&L."""
    accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
    if not accounts:
        sys.stdout.write("\r\033[K")
        print(Fore.YELLOW + "  ⚠  Could not reach NinjaTrader ATI." + Style.RESET_ALL)
        return

    # Find longest account name for formatting
    max_name = max(len(a["name"]) for a in accounts)
    box_inner = max(max_name + 35, 50)

    sys.stdout.write("\r\033[K")
    print(Fore.CYAN + f"\r\033[K  ╭─ BALANCES {'─' * (box_inner - 9)}╮" + Style.RESET_ALL)
    for a in accounts:
        name = a["name"]
        cash = a["cash"]
        marker = " ◀" if name == active_account else ""
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
        sys.stdout.write("\r\033[K")
        print(Fore.GREEN + "  ✔  Session P&L reset — balances re-snapshotted." + Style.RESET_ALL)
        logger.info("MANUAL RESET  session P&L reset by user")
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()


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
        sys.stdout.write("\r\033[K")
        print(Fore.YELLOW + "  ⚠  Set an account first (press A)." + Style.RESET_ALL)
        return
    if not output_directory:
        sys.stdout.write("\r\033[K")
        print(Fore.YELLOW + "  ⚠  Set an output directory first (press D)." + Style.RESET_ALL)
        return

    positions = await asyncio.to_thread(query_nt_positions, active_account, nt_port)
    open_pos = {k: v for k, v in positions.items() if v != 0}

    if not open_pos:
        sys.stdout.write("\r\033[K")
        print(Fore.YELLOW + "  ⚠  No open positions for " + Fore.WHITE + active_account + Fore.YELLOW + "." + Style.RESET_ALL)
        return

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
            # ESC or Q to cancel
            sys.stdout.write("\r\033[K")
            print(Fore.WHITE + Style.DIM + "  Cancelled." + Style.RESET_ALL)
            break

    awaiting_user_input = False


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
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + "  ⛔  Hard stop active — cannot resume. Press Shift+X to exit." + Style.RESET_ALL)
                    continue
                paused = not paused
                soft_stopped = False  # Reset soft stop on manual resume
                if paused:
                    set_session_state("paused")
                else:
                    set_session_state("ready")
                    sys.stdout.write("\r\033[K")
                    sys.stdout.flush()
                    print(Fore.GREEN + "▶  SIGNAL OUTPUT RESUMED" + Style.RESET_ALL)
            elif key.lower() == "b":
                await show_balances()
            elif key.lower() == "s":
                await setup_menu()
            elif key.lower() == "t":
                await prompt_limits()
            elif key.lower() == "r":
                sys.stdout.write("\r\033[K")
                print(Fore.YELLOW + "🔄  MANUAL RECONNECT REQUESTED" + Style.RESET_ALL)
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


def format_signal(signal_text: str, idx: int):
    colour = SIGNAL_COLOURS[idx % len(SIGNAL_COLOURS)]
    ts = time.strftime("%H:%M:%S")
    width = term_width()
    inner = width - 4
    body = signal_text
    if len(body) > inner:
        body = body[: inner - 3] + "..."
    return f"{colour}[{ts}] ▸  {body}{Style.RESET_ALL}"


# ---------- Server message display ----------
WELCOME_FRAMES = ["◇", "◆", "◇", "◆", "●"]
HEARTBEAT_FRAMES = ["♡", "♥", "♡", "♥"]


async def display_server_message(data: dict, connect_latency: int):
    """Parse and display server messages with styled output."""
    sys.stdout.write("\r\033[K")

    if "welcome" in data:
        # Animated welcome
        server_name = data.get("server", "SocketTrader")
        hb_interval = data.get("heartbeat_interval")
        ts = data.get("ts")

        for frame in WELCOME_FRAMES:
            sys.stdout.write(f"\r\033[K{Fore.CYAN}{frame} Connecting to {server_name}...{Style.RESET_ALL}")
            sys.stdout.flush()
            await asyncio.sleep(0.15)
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

        print(f"\r\033[K" + Fore.CYAN + "  ╭─ SERVER ───────────────────────────────────────╮" + Style.RESET_ALL)
        print(f"\r\033[K" + Fore.CYAN + f"  │  {server_name[:46].ljust(46)}│" + Style.RESET_ALL)
        if ts:
            welcome_lat = int(time.time() * 1000) - ts
            lat_line = f"Message latency: {welcome_lat}ms  ·  Handshake: {connect_latency}ms"
            print(f"\r\033[K" + Fore.CYAN + f"  │  {lat_line.ljust(46)}│" + Style.RESET_ALL)
            logger.info(f"WELCOME  latency={welcome_lat}ms  handshake={connect_latency}ms")
        if hb_interval:
            mins = hb_interval // 60
            hb_line = f"Heartbeat every {mins} min"
            print(f"\r\033[K" + Fore.CYAN + f"  │  {hb_line.ljust(46)}│" + Style.RESET_ALL)
        print(f"\r\033[K" + Fore.CYAN + "  ╰────────────────────────────────────────────────╯" + Style.RESET_ALL)
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

    elif data.get("type") == "heartbeat":
        # Animated heartbeat pulse — subtle, single line
        for frame in HEARTBEAT_FRAMES:
            sys.stdout.write(f"\r\033[K{Fore.RED}{Style.DIM}  {frame}{Style.RESET_ALL}")
            sys.stdout.flush()
            await asyncio.sleep(0.2)
        ts = time.strftime("%H:%M:%S")
        sys.stdout.write(f"\r\033[K{Fore.RED}{Style.DIM}  ♥  [{ts}] server heartbeat{Style.RESET_ALL}\n")
        sys.stdout.flush()
        logger.info("HEARTBEAT")

    else:
        # Unknown server message — log it, don't clutter terminal
        logger.info(f"SERVER  {data}")


# ---------- File output ----------
def extract_signal_string(msg: str, account: str, atm: str) -> tuple[str | None, int | None, str | None]:
    """Parse JSON message and extract the raw signal string, server timestamp, and signal ID.

    Signal format: PLACE;Account;Instrument;Action;Qty;OrderType;;;TIF;;;AtmStrategy;SignalID
    Index:           0      1        2        3     4      5     678  9  10 11          12
    - Field 1 (account) is replaced with the user's real account.
    - Field 11 (ATM strategy) is replaced with the user's chosen strategy.
    - Field 12 (last field) is the unique signal ID used for dedup.
    Returns (processed_signal, server_timestamp_ms, signal_id) or (None, None, None).
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
                return None, None, None
            # Extract signal ID (last non-empty field)
            signal_id = parts[-1] if parts else None
            # Replace account (field 1)
            if len(parts) >= 2:
                parts[1] = account
            # Replace ATM strategy (field 11)
            if len(parts) >= 12:
                parts[11] = atm
            return ";".join(parts), ts, signal_id
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None, None, None


def write_signal_to_file(signal_text: str):
    """Write the raw signal string (not JSON) to the output directory."""
    if not output_directory:
        return
    ts = time.strftime("%Y%m%d_%H%M%S")
    ms = int((time.time() % 1) * 1000)
    filename = f"signal_{ts}_{ms:03d}.txt"
    filepath = os.path.join(output_directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(signal_text)
    except Exception as exc:
        sys.stdout.write("\r\033[K")
        print(Fore.RED + f"  ✖  File write error: {exc}" + Style.RESET_ALL)


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
    ts = time.strftime("%Y%m%d_%H%M%S")
    ms = int((time.time() % 1) * 1000)
    filename = f"close_{ts}_{ms:03d}.txt"
    filepath = os.path.join(output_directory, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(cmd)
        logger.info(f"CLOSEPOSITION  account={account}  contract={contract}  file={filename}")
    except Exception as exc:
        sys.stdout.write("\r\033[K")
        print(Fore.RED + f"  ✖  Close position write error: {exc}" + Style.RESET_ALL)


def add_pending_confirm(signal_text: str, sig_id: str | None, instrument: str, action: str):
    """Register a signal for post-trade confirmation via position check."""
    # Snapshot current position for this specific instrument
    positions = query_nt_positions(active_account, nt_port)
    pre_pos = positions.get(instrument, 0)
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
        })


def check_pending_confirms():
    """Check pending signals for confirmation via position change.

    Called every balance poll cycle. Queries ATI for current positions
    and checks if the position changed for the specific instrument
    that was traded. This works even with other positions open.
    """
    if not _pending_confirms or not active_account:
        return

    positions = query_nt_positions(active_account, nt_port)
    now = time.time()
    still_pending = []

    with _confirms_lock:
        for entry in _pending_confirms:
            elapsed = now - entry["ts"]
            instrument = entry["instrument"]
            pre_pos = entry["pre_pos"]
            cur_pos = positions.get(instrument, 0)

            if cur_pos != pre_pos:
                # Position changed on this instrument — trade confirmed
                logger.info(f"CONFIRMED  id={entry['id']}  {instrument}  "
                            f"{entry['action']}  pos: {pre_pos} → {cur_pos}  "
                            f"elapsed={elapsed:.1f}s")
                continue  # drop from pending

            if elapsed >= CONFIRM_TIMEOUT:
                # Timed out — position did not change for this instrument
                sys.stdout.write("\r\033[K")
                print(Fore.YELLOW + Style.DIM +
                      f"  ⚠  No fill detected for {instrument} after {CONFIRM_TIMEOUT}s "
                      f"(ID: {entry['id']})" + Style.RESET_ALL)
                logger.warning(f"UNCONFIRMED  id={entry['id']}  {instrument}  "
                               f"{entry['action']}  pos unchanged at {pre_pos}  "
                               f"elapsed={elapsed:.1f}s")
                continue  # drop from pending

            still_pending.append(entry)

        _pending_confirms.clear()
        _pending_confirms.extend(still_pending)


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

            # Always poll balances for status bar display (non-blocking)
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
                sys.stdout.write("\r\033[K")
                print(Fore.CYAN + Style.BRIGHT + "  🔄  Session P&L auto-reset (4:20 PM ET)" + Style.RESET_ALL)
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

            if active_account not in session_start_balances:
                continue

            current = session_current_balances.get(active_account)
            if current is None:
                continue

            limits = get_account_limits(active_account)
            if limits["target"] == 0 and limits["stop"] == 0:
                continue

            start = session_start_balances[active_account]
            pnl = current - start

            # Check stop (loss limit)
            if limits["stop"] != 0 and pnl <= limits["stop"]:
                if limits["stop_mode"] == "hard":
                    hard_stopped = True
                    paused = True
                    set_session_state("hard_stop")
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + Style.BRIGHT + f"  ⛔  HARD STOP HIT  ·  P&L: ${pnl:+,.2f}  ·  Limit: ${limits['stop']:+,.2f}" + Style.RESET_ALL)
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + "  ⛔  CLOSING ALL POSITIONS..." + Style.RESET_ALL)
                    for contract in session_contracts:
                        fire_close_position(active_account, contract)
                        sys.stdout.write("\r\033[K")
                        print(Fore.RED + f"  ⛔  CLOSEPOSITION → {contract}" + Style.RESET_ALL)
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + "  ⛔  Signals LOCKED — hard stop active. Press Shift+X to exit." + Style.RESET_ALL)
                    logger.info(f"HARD STOP  pnl={pnl:.2f}  limit={limits['stop']}  contracts={list(session_contracts)}")
                elif not soft_stopped:
                    soft_stopped = True
                    paused = True
                    set_session_state("soft_stop")
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + Style.BRIGHT + f"  ⛔  STOP LIMIT HIT  ·  P&L: ${pnl:+,.2f}  ·  Limit: ${limits['stop']:+,.2f}" + Style.RESET_ALL)
                    sys.stdout.write("\r\033[K")
                    print(Fore.YELLOW + "  ⏸  Signals PAUSED — stop reached. Press P to resume." + Style.RESET_ALL)
                    logger.info(f"SOFT STOP (LOSS)  pnl={pnl:.2f}  stop={limits['stop']}")
                continue

            # Check target (profit goal)
            if limits["target"] != 0 and pnl >= limits["target"]:
                if limits["target_mode"] == "hard":
                    hard_stopped = True
                    paused = True
                    set_session_state("hard_target")
                    sys.stdout.write("\r\033[K")
                    print(Fore.GREEN + Style.BRIGHT + f"  🎯  TARGET HIT (HARD)  ·  P&L: ${pnl:+,.2f}  ·  Target: ${limits['target']:+,.2f}" + Style.RESET_ALL)
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + "  ⛔  CLOSING ALL POSITIONS..." + Style.RESET_ALL)
                    for contract in session_contracts:
                        fire_close_position(active_account, contract)
                        sys.stdout.write("\r\033[K")
                        print(Fore.RED + f"  ⛔  CLOSEPOSITION → {contract}" + Style.RESET_ALL)
                    sys.stdout.write("\r\033[K")
                    print(Fore.RED + "  ⛔  Signals LOCKED — target hard stop active. Press Shift+X to exit." + Style.RESET_ALL)
                    logger.info(f"HARD STOP (TARGET)  pnl={pnl:.2f}  target={limits['target']}  contracts={list(session_contracts)}")
                elif not soft_stopped:
                    soft_stopped = True
                    paused = True
                    set_session_state("soft_target")
                    sys.stdout.write("\r\033[K")
                    print(Fore.GREEN + Style.BRIGHT + f"  🎯  SESSION TARGET HIT  ·  P&L: ${pnl:+,.2f}  ·  Target: ${limits['target']:+,.2f}" + Style.RESET_ALL)
                    sys.stdout.write("\r\033[K")
                    print(Fore.YELLOW + "  ⏸  Signals PAUSED — target reached. Press P to resume." + Style.RESET_ALL)
                    logger.info(f"SOFT STOP (TARGET)  pnl={pnl:.2f}  target={limits['target']}")
        except Exception as e:
            logger.error(f"balance_monitor error: {e}")


async def prompt_limits():
    """Prompt user to set session target and stop for the active account."""
    global awaiting_user_input, soft_stopped
    if not active_account:
        sys.stdout.write("\r\033[K")
        print(Fore.YELLOW + "  ⚠  Set an account first (press A)." + Style.RESET_ALL)
        return

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
    soft_stopped = False  # Reset soft stop when limits change
    t_label = f"${target:+,.2f} ({target_mode})" if target else "off"
    s_label = f"${stop:+,.2f} ({stop_mode})" if stop else "off"
    print(Fore.GREEN + f"  ✔  {active_account} → Target: {t_label}  ·  Stop: {s_label}" + Style.RESET_ALL)

    print()
    awaiting_user_input = False


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

    await boot_sequence()

    while not shutdown.is_set():
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

                # Restore persisted session if still in the same trading session
                restored = restore_session_state()

                # Snapshot account balances for risk management
                nt_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
                for a in nt_accounts:
                    if a["name"] not in session_start_balances:
                        session_start_balances[a["name"]] = a["cash"]
                    session_current_balances[a["name"]] = a["cash"]

                if restored:
                    pnl_parts = []
                    for name in session_start_balances:
                        cur = session_current_balances.get(name)
                        if cur is not None:
                            pnl_parts.append(f"{name}: ${cur - session_start_balances[name]:+,.2f}")
                    sys.stdout.write("\r\033[K")
                    print(Fore.CYAN + Style.BRIGHT +
                          f"  🔄  Session P&L restored ({', '.join(pnl_parts) if pnl_parts else 'no data'})"
                          + Style.RESET_ALL)
                    logger.info(f"SESSION RESTORED  id={get_session_id()}  accounts={list(session_start_balances.keys())}")

                # Context-aware welcome message
                missing = []
                if not active_account:
                    missing.append("A = set account")
                if not output_directory:
                    missing.append("D = set output directory")
                if not validate_strategy(atm_strategy):
                    missing.append(f"S = strategy '{atm_strategy}' not found")

                if missing:
                    sys.stdout.write("\r\033[K")
                    print(Fore.YELLOW + f"⚠  Connected, but setup incomplete: {', '.join(missing)}" + Style.RESET_ALL)
                else:
                    sys.stdout.write("\r\033[K")
                    print(Fore.GREEN + f"✔  Connected  ·  Account: {active_account}  ·  Handshake: {connect_latency}ms" + Style.RESET_ALL)
                    logger.info(f"CONNECTED  account={active_account}  handshake={connect_latency}ms  strategy={atm_strategy}")
                sys.stdout.write("\r\033[K")
                sys.stdout.flush()
                reconnect_event.clear()

                while not shutdown.is_set():
                    # Check for manual reconnect request
                    if reconnect_event.is_set():
                        reconnect_event.clear()
                        fib_prev, fib_curr = 60, 60  # Reset backoff on manual reconnect
                        sys.stdout.write("\r\033[K")
                        print(Fore.YELLOW + "  🔄  Dropping connection for reconnect..." + Style.RESET_ALL)
                        break

                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1)
                        raw_signal, server_ts, sig_id = extract_signal_string(msg, active_account, atm_strategy)
                        if raw_signal:
                            # Duplicate detection by signal ID
                            if sig_id and sig_id in _recent_signal_ids:
                                if not paused:
                                    sys.stdout.write("\r\033[K")
                                    print(Fore.YELLOW + Style.DIM + f"  ⚠  Duplicate signal ignored (ID: {sig_id})" + Style.RESET_ALL)
                                logger.info(f"DUPLICATE IGNORED  id={sig_id}  signal={raw_signal}")
                                continue
                            if sig_id:
                                _recent_signal_ids.append(sig_id)

                            signal_count += 1

                            if paused:
                                # Log but don't trade or print
                                logger.info(f"SIGNAL #{signal_count} (PAUSED)  {raw_signal}")
                                continue

                            if not is_trade_ready():
                                # System not ready — log but don't trade
                                sys.stdout.write("\r\033[K")
                                print(Fore.RED + f"  ✖  Signal blocked — system NOT READY" + Style.RESET_ALL)
                                logger.warning(f"SIGNAL #{signal_count} (NOT READY)  {raw_signal}")
                                continue

                            # Track traded contracts for hard stop
                            sig_parts = raw_signal.split(";")
                            if len(sig_parts) >= 3 and sig_parts[2] and len(session_contracts) < MAX_SESSION_CONTRACTS:
                                session_contracts.add(sig_parts[2])

                            write_signal_to_file(raw_signal)
                            # Register for fill confirmation
                            instrument = sig_parts[2] if len(sig_parts) >= 3 else ""
                            action = sig_parts[3] if len(sig_parts) >= 4 else ""
                            await asyncio.to_thread(add_pending_confirm, raw_signal, sig_id, instrument, action)
                            await signal_pulse("SIGNAL RECEIVED")
                            sys.stdout.write("\r\033[K")
                            print(format_signal(raw_signal, signal_count))
                            logger.info(f"SIGNAL #{signal_count}  {raw_signal}")
                            # Latency display (first signal = baseline)
                            if server_ts:
                                latency_ms = int(time.time() * 1000) - server_ts
                                if baseline_latency is None:
                                    baseline_latency = latency_ms
                                if latency_ms < 1000:
                                    lat_str = f"{latency_ms}ms"
                                else:
                                    lat_str = f"{latency_ms / 1000:.1f}s"
                                diff = latency_ms - baseline_latency
                                if diff < 0:
                                    lat_color = Fore.GREEN   # Faster than first signal
                                elif diff <= 250:
                                    lat_color = Fore.YELLOW  # Within 250ms of first signal
                                else:
                                    lat_color = Fore.RED     # >250ms slower than first signal
                                diff_str = f" (+{diff}ms)" if diff > 0 else f" ({diff}ms)" if diff < 0 else ""
                                sys.stdout.write("\r\033[K" + lat_color + Style.DIM + f"   ├─ latency: {lat_str}{diff_str}\n" + Style.RESET_ALL)
                                sys.stdout.flush()
                                logger.info(f"  latency={latency_ms}ms  diff={diff}ms  baseline={baseline_latency}ms")
                            if output_directory:
                                sys.stdout.write("\r\033[K" + Fore.GREEN + Style.DIM + f"   └─ saved → {output_directory}\n" + Style.RESET_ALL)
                                sys.stdout.flush()
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
            sys.stdout.write("\r\033[K")
            print(Fore.RED + "⛔  INVALID SERVER URI" + Style.RESET_ALL)
            return "shutdown"

        except Exception as e:
            sys.stdout.write("\r\033[K")

            # Check for HTTP status rejection (old and new websockets lib)
            http_status = getattr(e, "status_code", None) or getattr(e, "status", None)

            # Check for websocket close code 1008 (policy violation = bad token)
            ws_code = getattr(e, "code", None) or getattr(e, "rcvd", None)
            if ws_code is not None and not isinstance(ws_code, int):
                # newer websockets lib: rcvd is a Close frame
                ws_code = getattr(ws_code, "code", None)

            if http_status is not None and int(http_status) in (401, 403):
                print(Fore.RED + f"⛔  AUTHENTICATION FAILED (HTTP {http_status})" + Style.RESET_ALL)
                logger.warning(f"AUTH FAILED  http={http_status}")
                return "auth_failed"
            elif ws_code == 1008:
                print(Fore.RED + "⛔  AUTHENTICATION FAILED (invalid token)" + Style.RESET_ALL)
                logger.warning("AUTH FAILED  ws_code=1008")
                return "auth_failed"
            elif http_status is not None:
                print(Fore.RED + f"⛔  CONNECTION ERROR (HTTP {http_status})  ·  Retrying in {fmt_wait(fib_curr)}..." + Style.RESET_ALL)
                logger.warning(f"CONNECTION ERROR  http={http_status}  retry={fmt_wait(fib_curr)}")
            elif shutdown.is_set():
                break
            else:
                print(Fore.RED + f"⛔  CONNECTION LOST  ·  {e}" + Style.RESET_ALL)
                sys.stdout.write("\r\033[K")
                print(Fore.YELLOW + f"  ↻  Reconnecting in {fmt_wait(fib_curr)}..." + Style.RESET_ALL)
                logger.warning(f"CONNECTION LOST  error={e}  retry={fmt_wait(fib_curr)}")

        if shutdown.is_set():
            break

        # Fibonacci backoff wait (interruptible by shutdown or manual reconnect)
        wait_end = time.time() + fib_curr
        while time.time() < wait_end:
            if shutdown.is_set():
                return "shutdown"
            if reconnect_event.is_set():
                reconnect_event.clear()
                fib_prev, fib_curr = 60, 60
                sys.stdout.write("\r\033[K")
                print(Fore.YELLOW + "  🔄  Manual reconnect — resetting backoff." + Style.RESET_ALL)
                break
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

    # 3. Account / directory — optional at startup, prompted later if missing
    #    Only ask during fresh first-time setup (nothing saved at all)
    if not cfg.get("account") and not cfg.get("output_directory"):
        account = ask_account(cfg)
        cfg["account"] = account

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
    global output_directory, active_account, atm_strategy, nt_port

    token, cfg = setup()
    active_account = cfg.get("account", "")
    atm_strategy = cfg.get("atm_strategy", "NQ_Med")
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
