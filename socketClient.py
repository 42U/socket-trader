from __future__ import annotations

import asyncio
import websockets
import json
import shutil
import sys
import time
import random
import os
import re
import platform
from pathlib import Path
from colorama import init, Fore, Style

# pip install pyfiglet colorama websockets
try:
    import pyfiglet
except ImportError:
    pyfiglet = None

init()

# ---------- Config persistence ----------
CONFIG_FILE = Path.home() / ".voidorigin_config.json"
WS_HOST = "ws://ec2-16-59-44-39.us-east-2.compute.amazonaws.com:8420/ws"

paused = False
shutdown = asyncio.Event()
signal_count = 0
output_directory = None
active_account = None          # Current NinjaTrader account name
awaiting_directory_input = False
awaiting_user_input = False  # Block key handler during any input prompt


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

    is_windows = platform.system() == "Windows"

    if is_windows:
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
    if not is_windows:
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


def ask_account(cfg: dict) -> str:
    """Get NinjaTrader account name from config or prompt user."""
    saved = cfg.get("account")
    if saved:
        return saved

    print(Fore.CYAN + "\n┌─ NINJATRADER ACCOUNT ─────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Enter your NinjaTrader account name.                 │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  This replaces the Sim account in incoming signals.   │" + Style.RESET_ALL)
    print(Fore.CYAN + "└───────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    while True:
        acct = input(Fore.WHITE + "  ACCOUNT ▸ " + Style.RESET_ALL).strip()
        if acct:
            return acct
        print(Fore.YELLOW + "  ⚠  Account cannot be empty." + Style.RESET_ALL)


def ask_password(cfg: dict, force: bool = False) -> str:
    """Get password from config or prompt user."""
    saved = cfg.get("password")
    if saved and not force:
        return saved

    if force:
        print(Fore.YELLOW + "\n  ⚠  Authentication failed. Please re-enter password." + Style.RESET_ALL)
    else:
        print(Fore.CYAN + "\n┌─ AUTHENTICATION ─────────────────────────────────────┐" + Style.RESET_ALL)
        print(Fore.CYAN + "│  Enter your connection password.                     │" + Style.RESET_ALL)
        print(Fore.CYAN + "└──────────────────────────────────────────────────────┘" + Style.RESET_ALL)

    while True:
        pw = input(Fore.WHITE + "  PASSWORD ▸ " + Style.RESET_ALL).strip()
        if pw:
            return pw
        print(Fore.YELLOW + "  ⚠  Password cannot be empty." + Style.RESET_ALL)


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
    commands = "P = PAUSE   A = ACCOUNT   D = DIR   R = RECONNECT   C = CLOSE"

    lines = [
        hline(width, "╔", "═", "╗"),
        row("SIGNAL NODE", width),
        row(subtitle, width),
        hline(width, "╠", "═", "╣"),
        row(commands, width),
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
    dir_indicator = Fore.GREEN + "● DIR SET" + Fore.CYAN if output_directory else Fore.RED + "● DIR NOT SET" + Fore.CYAN
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
    banner_lines = build_banner().splitlines()
    for line in banner_lines:
        sys.stdout.write(Fore.GREEN + Style.DIM + glitch_line(line, 0.25) + "\n" + Style.RESET_ALL)
        sys.stdout.flush()
        await asyncio.sleep(0.06)
    await asyncio.sleep(0.5)
    for line in build_header().splitlines():
        print(Fore.GREEN + line + Style.RESET_ALL)
    print()
    print(status_bar("SESSION ACTIVE  ·  AWAITING SIGNALS"))
    print()
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
        if paused:
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
        print(Fore.CYAN + f"│  Current: {output_directory[:57].ljust(57)} │" + Style.RESET_ALL)
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
    print(Fore.CYAN + "\n┌─ CHANGE ACCOUNT ─────────────────────────────────┐" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Enter new NinjaTrader account name.              │" + Style.RESET_ALL)
    print(Fore.CYAN + "│  Press ENTER to keep current.                     │" + Style.RESET_ALL)
    if active_account:
        print(Fore.CYAN + f"│  Current: {active_account[:57].ljust(57)} │" + Style.RESET_ALL)
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

    print()
    print(status_bar("SESSION ACTIVE  ·  AWAITING SIGNALS"))
    print()
    awaiting_user_input = False


# ---------- Keyboard loop ----------
reconnect_event = asyncio.Event()


async def keyboard_loop():
    global paused
    while not shutdown.is_set():
        key = await asyncio.to_thread(get_key)
        if awaiting_directory_input or awaiting_user_input:
            continue
        if key.lower() == "p":
            paused = not paused
            if not paused:
                sys.stdout.write("\r\033[K")
                sys.stdout.flush()
                print(Fore.GREEN + "▶  SIGNAL OUTPUT RESUMED" + Style.RESET_ALL)
        elif key.lower() == "a":
            await prompt_account()
        elif key.lower() == "d":
            await prompt_directory()
        elif key.lower() == "r":
            sys.stdout.write("\r\033[K")
            print(Fore.YELLOW + "🔄  MANUAL RECONNECT REQUESTED" + Style.RESET_ALL)
            reconnect_event.set()
        elif key.lower() == "c":
            sys.stdout.write("\r\033[K")
            print(Fore.RED + "⛔  DISCONNECT REQUESTED  ·  TERMINATING SESSION" + Style.RESET_ALL)
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


# ---------- File output ----------
def extract_signal_string(msg: str, account: str) -> tuple[str | None, int | None]:
    """Parse JSON message and extract the raw signal string + server timestamp.

    Expected format: {"signal": "PLACE;SimAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;TAG;VALUE", "ts": 1234567890123}
    The account is always the second semicolon-delimited field (index 1).
    Returns (signal_with_account_replaced, server_timestamp_ms) or (None, None).
    """
    try:
        data = json.loads(msg)
        if isinstance(data, dict) and "signal" in data:
            raw = data["signal"]
            ts = data.get("ts")
            # Replace the second field (sim account) with real account
            first_semi = raw.index(";")
            second_semi = raw.index(";", first_semi + 1)
            return f"{raw[:first_semi + 1]}{account}{raw[second_semi:]}", ts
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return None, None


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


async def listen(password: str):
    global signal_count
    uri = f"{WS_HOST}?token={password}"
    fib_prev, fib_curr = 60, 60  # Start at 1m, 1m → 2m → 3m → 5m → ...

    await boot_sequence()

    while not shutdown.is_set():
        try:
            async with websockets.connect(uri) as ws:
                fib_prev, fib_curr = 60, 60  # Reset on successful connection

                # Context-aware welcome message
                missing = []
                if not active_account:
                    missing.append("A = set account")
                if not output_directory:
                    missing.append("D = set output directory")

                if missing:
                    sys.stdout.write("\r\033[K")
                    print(Fore.YELLOW + f"⚠  Connected, but setup incomplete: {', '.join(missing)}" + Style.RESET_ALL)
                else:
                    sys.stdout.write("\r\033[K")
                    print(Fore.GREEN + f"✔  Connected  ·  Account: {active_account}  ·  Signals will arrive shortly" + Style.RESET_ALL)
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
                        if not paused:
                            raw_signal, server_ts = extract_signal_string(msg, active_account)
                            if raw_signal:
                                write_signal_to_file(raw_signal)
                                await signal_pulse("SIGNAL RECEIVED")
                                print(format_signal(raw_signal, signal_count))
                                # Latency display
                                if server_ts:
                                    latency_ms = int(time.time() * 1000) - server_ts
                                    if latency_ms < 1000:
                                        lat_str = f"{latency_ms}ms"
                                    else:
                                        lat_str = f"{latency_ms / 1000:.1f}s"
                                    lat_color = Fore.GREEN if latency_ms < 200 else Fore.YELLOW if latency_ms < 1000 else Fore.RED
                                    sys.stdout.write(lat_color + Style.DIM + f"   ├─ latency: {lat_str}\n" + Style.RESET_ALL)
                                    sys.stdout.flush()
                                if output_directory:
                                    sys.stdout.write(Fore.GREEN + Style.DIM + f"   └─ saved → {output_directory}\n" + Style.RESET_ALL)
                                    sys.stdout.flush()
                                signal_count += 1
                            else:
                                # Non-signal message (server info, heartbeat, etc.)
                                try:
                                    data = json.loads(msg)
                                    # Skip server welcome — client handles its own greeting
                                    if "welcome" not in data:
                                        print(Fore.CYAN + Style.DIM + f"  [server] {data}" + Style.RESET_ALL)
                                except json.JSONDecodeError:
                                    print(Fore.CYAN + Style.DIM + f"  [server] {msg}" + Style.RESET_ALL)
                    except asyncio.TimeoutError:
                        continue

        except websockets.exceptions.InvalidURI:
            sys.stdout.write("\r\033[K")
            print(Fore.RED + "⛔  INVALID SERVER URI" + Style.RESET_ALL)
            return "shutdown"

        except Exception as e:
            # Handle HTTP rejection (auth failure or other status codes)
            # Works with both old (InvalidStatusCode) and new (InvalidStatus) websockets
            status = getattr(e, "status_code", None) or getattr(e, "status", None)
            sys.stdout.write("\r\033[K")
            if status is not None:
                status = int(status)
                if status in (401, 403):
                    print(Fore.RED + "⛔  AUTHENTICATION FAILED (HTTP {status})" + Style.RESET_ALL)
                    return "auth_failed"
                else:
                    print(Fore.RED + f"⛔  CONNECTION ERROR (HTTP {status})  ·  Retrying in {fmt_wait(fib_curr)}..." + Style.RESET_ALL)
            elif shutdown.is_set():
                break
            else:
                print(Fore.RED + f"⛔  CONNECTION LOST  ·  {e}" + Style.RESET_ALL)
                sys.stdout.write("\r\033[K")
                print(Fore.YELLOW + f"  ↻  Reconnecting in {fmt_wait(fib_curr)}..." + Style.RESET_ALL)

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
def setup() -> tuple[str, dict]:
    """Run first-time or repeat setup. Returns (password, config)."""
    cfg = load_config()

    print(Fore.GREEN + Style.BRIGHT)
    print("  ╔══════════════════════════════════════════╗")
    print("  ║         VOIDORIGIN  ·  SIGNAL NODE       ║")
    print("  ╚══════════════════════════════════════════╝")
    print(Style.RESET_ALL)

    if cfg.get("password") and cfg.get("account") and cfg.get("output_directory") and Path(cfg["output_directory"]).is_dir():
        print(Fore.GREEN + f"  ✔  Config loaded from {CONFIG_FILE}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Account: {cfg['account']}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Output: {cfg['output_directory']}" + Style.RESET_ALL)
        print(Fore.GREEN + f"  ✔  Password: {'*' * len(cfg['password'])}" + Style.RESET_ALL)
        print()
        return cfg["password"], cfg

    # Password
    password = ask_password(cfg)

    # Account
    account = ask_account(cfg)

    # Output directory
    global output_directory
    output_directory = detect_or_ask_directory(cfg)

    # Save config
    cfg["password"] = password
    cfg["account"] = account
    if output_directory:
        cfg["output_directory"] = output_directory
    save_config(cfg)

    print(Fore.GREEN + f"\n  ✔  Config saved to {CONFIG_FILE}" + Style.RESET_ALL)
    print()
    return password, cfg


# ---------- Main ----------
async def main():
    global output_directory, active_account

    password, cfg = setup()
    active_account = cfg.get("account", "")

    if cfg.get("output_directory"):
        output_directory = cfg["output_directory"]

    while True:
        global _kb_stop
        _kb_stop = False
        shutdown.clear()
        reconnect_event.clear()

        tasks = [
            asyncio.create_task(listen(password)),
            asyncio.create_task(keyboard_loop()),
            asyncio.create_task(pause_indicator()),
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
            # Re-prompt for password — stdin is now free
            show_cursor()
            cfg = load_config()
            password = ask_password(cfg, force=True)
            cfg["password"] = password
            save_config(cfg)
            print(Fore.GREEN + "  ✔  Password updated. Reconnecting..." + Style.RESET_ALL)
            continue
        else:
            break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        show_cursor()
        print(Fore.RED + "\nFORCED EXIT" + Style.RESET_ALL)
