from __future__ import annotations

import asyncio
import websockets
import json
import logging
import shutil
import socket
import sys
import time
import random
import os
import re
import platform
from collections import deque
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
WS_HOST = "ws://ec2-16-59-44-39.us-east-2.compute.amazonaws.com:8420/ws"

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

# ---------- Logging ----------
LOG_FILE = Path.home() / ".voidorigin_signals.log"
logger = logging.getLogger("sockettrader")
logger.setLevel(logging.INFO)
_log_handler = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
_log_handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_log_handler)

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
    """Persist config to disk."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
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


def query_nt_accounts(port: int = 36973, timeout: float = 3.0) -> list[dict]:
    """Query NinjaTrader ATI for connected accounts with balances.

    Returns list of dicts: [{"name": "Sim101", "cash": 28857.02}, ...]
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((_nt_host(), port))
        s.sendall(b"ACCOUNTS\n")
        parts = []
        while True:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                parts.append(chunk)
            except socket.timeout:
                break
        s.close()
        text = b"".join(parts).decode("utf-8", errors="ignore")
        # Parse accounts from CashValue|AccountName\x00Value pattern
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
    except Exception:
        pass
    return []


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


# ---------- Cross-platform keyboard helpers ----------
_kb_stop = False  # Set True to unblock get_key threads

if os.name == "nt":  # Windows
    import msvcrt

    def get_key():
        """Non-blocking poll so the thread can exit when _kb_stop is set."""
        while not _kb_stop:
            if msvcrt.kbhit():
                return msvcrt.getch().decode("utf-8", errors="ignore")
            time.sleep(0.05)
        return ""

    def read_line_raw():
        return input().strip()

else:  # POSIX
    import termios
    import tty

    def get_key():
        if _kb_stop:
            return ""
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            return sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def read_line_raw():
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
            line = sys.stdin.readline()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
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
CONTROLS_TEXT = "P=PAUSE  A=ACCT  B=BAL  S=STRAT  D=DIR  T=LIMITS  O=PORT  R=RECONN  C=CLOSE"


def _build_controls_line():
    """Build the controls bar text with account info on the right."""
    left = f"  {CONTROLS_TEXT}"
    # Build account info
    acct_info = ""
    if active_account:
        start = session_start_balances.get(active_account)
        current = session_current_balances.get(active_account)
        if start is not None and current is not None:
            pnl = current - start
            pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
            acct_info = f"{active_account}: ${current:,.2f} (" + pnl_color + f"${pnl:+,.2f}" + Fore.CYAN + Style.DIM + ")  "
        elif current is not None:
            acct_info = f"{active_account}: ${current:,.2f}  "
        elif start is not None:
            acct_info = f"{active_account}: ${start:,.2f}  "
        else:
            acct_info = f"{active_account}  "
    if not acct_info:
        acct_info = "NO ACCOUNT SET  "
    # Pad between controls and account info
    width = term_width()
    visible_left = len(left)
    visible_right = len(_ANSI_ESCAPE.sub('', acct_info))
    gap = max(2, width - visible_left - visible_right)
    return f"{Fore.CYAN}{Style.DIM}{left}{' ' * gap}{acct_info}{Style.RESET_ALL}"


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
    """Redraw the pinned controls bar and maintain scroll region."""
    if not _controls_pinned:
        return
    rows = term_height()
    scroll_top = _header_lines + 1
    scroll_bottom = rows - 1
    # Save cursor position
    sys.stdout.write("\033[s")
    # Update scroll region in case terminal resized
    sys.stdout.write(f"\033[{scroll_top};{scroll_bottom}r")
    # Move to last row and redraw controls
    sys.stdout.write(f"\033[{rows};1H")
    sys.stdout.write(f"\033[K{_build_controls_line()}")
    # Restore cursor position
    sys.stdout.write("\033[u")
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
    print(Fore.CYAN + f"\r\033[K  ╭─ BALANCES {'─' * (box_inner - 11)}╮" + Style.RESET_ALL)
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
    sys.stdout.flush()


# ---------- Keyboard loop ----------
reconnect_event = asyncio.Event()


async def keyboard_loop():
    global paused, soft_stopped
    while not shutdown.is_set():
        key = await asyncio.to_thread(get_key)
        if awaiting_directory_input or awaiting_user_input:
            continue
        if key.lower() == "p":
            if hard_stopped:
                sys.stdout.write("\r\033[K")
                print(Fore.RED + "  ⛔  Hard stop active — cannot resume. Press C to exit." + Style.RESET_ALL)
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
        elif key.lower() == "a":
            await prompt_account()
        elif key.lower() == "d":
            await prompt_directory()
        elif key.lower() == "s":
            await prompt_strategy()
        elif key.lower() == "t":
            await prompt_limits()
        elif key.lower() == "o":
            await prompt_port()
        elif key.lower() == "r":
            sys.stdout.write("\r\033[K")
            print(Fore.YELLOW + "🔄  MANUAL RECONNECT REQUESTED" + Style.RESET_ALL)
            reconnect_event.set()
        elif key.lower() == "c":
            unpin_layout()
            clear()
            shutdown.set()
            break


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
        sys.stdout.write(f"\r\033[K{Fore.RED}{Style.DIM}  ♥  [{ts}] heartbeat{Style.RESET_ALL}\n")
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
            parts = raw.split(";")
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
    cmd = f"CLOSEPOSITION;{account};{contract};;;;;;;;;;"
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


async def balance_monitor():
    """Periodically check account balance and enforce target/stop."""
    global paused, soft_stopped, hard_stopped

    while not shutdown.is_set():
        await asyncio.sleep(BALANCE_POLL_INTERVAL)

        if not active_account or hard_stopped:
            continue

        # Always poll balances for status bar display (non-blocking)
        all_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
        for a in all_accounts:
            session_current_balances[a["name"]] = a["cash"]
        refresh_controls()

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
                print(Fore.RED + "  ⛔  Signals LOCKED — hard stop active. Press C to exit." + Style.RESET_ALL)
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
                print(Fore.RED + "  ⛔  Signals LOCKED — target hard stop active. Press C to exit." + Style.RESET_ALL)
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

    print(Fore.CYAN + f"\n\r\033[K┌─ SESSION LIMITS ({active_account}) ────────────────────────┐" + Style.RESET_ALL)
    if start_bal is not None and current_bal is not None:
        pnl = current_bal - start_bal
        info = f"Balance: ${current_bal:,.2f}  ·  Session P&L: ${pnl:+,.2f}"
        print(Fore.CYAN + f"\r\033[K│  {info.ljust(52)}│" + Style.RESET_ALL)
    if limits["target"] or limits["stop"]:
        cur = f"Target: ${limits['target']:+,.2f} ({limits['target_mode']})  ·  Stop: ${limits['stop']:+,.2f} ({limits['stop_mode']})"
        print(Fore.CYAN + f"\r\033[K│  {cur.ljust(52)}│" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  When a limit is hit, the lockout mode decides:   │" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  soft = pause signals · resumable with P          │" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  hard = flatten all positions · session locked     │" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K│  Enter 0 to disable. ENTER to keep current.       │" + Style.RESET_ALL)
    print(Fore.CYAN + f"\r\033[K└───────────────────────────────────────────────────────┘" + Style.RESET_ALL)

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
    uri = f"{WS_HOST}?token={token}"
    fib_prev, fib_curr = 60, 60  # Start at 1m, 1m → 2m → 3m → 5m → ...

    await boot_sequence()

    while not shutdown.is_set():
        try:
            connect_start = time.time()
            async with websockets.connect(uri) as ws:
                connect_latency = int((time.time() - connect_start) * 1000)
                baseline_latency = None  # First signal sets the baseline
                fib_prev, fib_curr = 60, 60  # Reset on successful connection

                # Snapshot account balances for risk management
                nt_accounts = await asyncio.to_thread(query_nt_accounts, nt_port)
                for a in nt_accounts:
                    if a["name"] not in session_start_balances:
                        session_start_balances[a["name"]] = a["cash"]
                    session_current_balances[a["name"]] = a["cash"]

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

                            # Track traded contracts for hard stop
                            sig_parts = raw_signal.split(";")
                            if len(sig_parts) >= 3 and sig_parts[2]:
                                session_contracts.add(sig_parts[2])

                            write_signal_to_file(raw_signal)
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
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=fib_curr)
            break  # shutdown was set
        except asyncio.TimeoutError:
            pass  # timeout expired, retry

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


def setup() -> tuple[str, dict]:
    """Run first-time or repeat setup. Returns (token, config)."""
    cfg = load_config()

    print(Fore.GREEN + Style.BRIGHT)
    print("  ╔══════════════════════════════════════════╗")
    print("  ║       VOIDORIGIN  ·  SOCKET TRADER       ║")
    print("  ╚══════════════════════════════════════════╝")
    print(Style.RESET_ALL)

    if cfg.get("token") and cfg.get("account") and cfg.get("output_directory") and Path(cfg["output_directory"]).is_dir():
        print(Fore.GREEN + f"  ✔  Config loaded from {CONFIG_FILE}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Account: {cfg['account']}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Output: {cfg['output_directory']}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Token: {'*' * len(cfg['token'])}" + Style.RESET_ALL)
        # Install strategy templates if needed
        nt_base = Path(cfg["output_directory"]).parent
        install_strategy_templates(nt_base)
        print()
        return cfg["token"], cfg

    # Token
    token = ask_token(cfg)

    # Account
    account = ask_account(cfg)

    # Output directory
    global output_directory
    output_directory = detect_or_ask_directory(cfg)

    # Save config
    cfg["token"] = token
    cfg["account"] = account
    if output_directory:
        cfg["output_directory"] = output_directory
    save_config(cfg)

    # Install strategy templates if output directory is under NinjaTrader 8
    if output_directory:
        nt_base = Path(output_directory).parent
        install_strategy_templates(nt_base)

    print(Fore.GREEN + f"\n  ✔  Config saved to {CONFIG_FILE}" + Style.RESET_ALL)
    print()
    return token, cfg


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
        await asyncio.sleep(0.2)  # Let polling threads exit

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
    # Show final P&L if we have balance data
    if active_account and active_account in session_start_balances:
        final_bal = query_nt_balance(active_account)
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
        print_exit_summary()
