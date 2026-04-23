<p align="center">
  <img src="images/socket-traderLOGO.png" alt="SocketTrader" width="600">
</p>

<p align="center">
  <strong>Real-time WebSocket signal gateway for NinjaTrader 8</strong>
</p>

<p align="center">
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white" alt="Python 3.10+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License"></a>
  <a href="#"><img src="https://img.shields.io/badge/platform-Windows%20%7C%20Linux-blue" alt="Platform"></a>
  <a href="#ninjatrader-setup"><img src="https://img.shields.io/badge/NinjaTrader-8-orange" alt="NinjaTrader 8"></a>
  <a href="https://voidorigin.com"><img src="https://img.shields.io/badge/VoidOrigin-signal%20server-blueviolet" alt="VoidOrigin"></a>
</p>

<p align="center">
  <a href="#installation">Installation</a> &nbsp;·&nbsp;
  <a href="#ninjatrader-setup">NinjaTrader Setup</a> &nbsp;·&nbsp;
  <a href="#usage">Usage</a> &nbsp;·&nbsp;
  <a href="#keyboard-controls">Controls</a> &nbsp;·&nbsp;
  <a href="#risk-management">Risk</a> &nbsp;·&nbsp;
  <a href="#signal-latency">Latency</a> &nbsp;·&nbsp;
  <a href="#configuration">Config</a>
</p>

---

## Overview

SocketTrader connects to a remote signal server over WebSocket, receives trading signals in real time, and writes them directly into NinjaTrader 8's `incoming/` folder for automated order execution.

```
┌────────────┐    WebSocket    ┌────────────────┐    File I/O    ┌────────────────┐
│   Signal   │ ─────────────▸  │  SocketTrader  │ ─────────────▸ │  NinjaTrader   │
│   Server   │  JSON signals   │      .py       │ raw order text │  8 incoming/   │
└────────────┘                 └────────────────┘                └────────────────┘
```

**Signal format:**
```
PLACE;<account>;<instrument>;<action>;<qty>;<order type>;<limit price>;<stop price>;<tif>;<oco id>;<order id>;<strategy>;<strategy id>
```

**Signal transformation:**
```
IN   {"signal": "PLACE;SimAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020", "ts": 1711000000000}
OUT  PLACE;YourAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020
```

The sim account is automatically swapped with your real NinjaTrader account name. Empty fields (`;;`) are optional parameters left at their defaults. Server timestamps are used to display signal latency.

---

## Features

> **Signal in. Order out. No GUI required.**
> SocketTrader runs entirely in the terminal with a keyboard-driven interface, real-time risk controls, and sub-second signal delivery.

| | |
|---|---|
| **Cross-platform** | Windows and Linux support |
| **ATI integration** | Auto-detect accounts, live balances, and position closing via NinjaTrader ATI |
| **Auto-detect directory** | Finds NinjaTrader 8 `incoming/` folder on Windows automatically |
| **Multi-server support** | Save and switch between multiple signal servers |
| **Persistent config** | Server, token, account, limits, and directory saved to `~/.voidorigin_config.json` |
| **Smart reconnect** | Fibonacci backoff: 1 &rarr; 1 &rarr; 2 &rarr; 3 &rarr; 5 &rarr; 8 &rarr; ... &rarr; 30 min max |
| **Auth handling** | Invalid token triggers re-prompt instead of infinite retry |
| **Latency monitoring** | Color-coded signal delivery time relative to baseline |
| **Risk management** | Per-account target and stop with independent soft/hard modes |
| **Live balances** | Press `B` for real-time account balances and session P&L |
| **Signal confirmation** | Verifies trade execution via ATI position tracking per instrument |
| **Trade readiness gate** | Blocks signals when account, directory, or strategy is missing |
| **Duplicate detection** | Prevents the same signal from firing twice |
| **Input validation** | Field length, count, and format checks on all incoming signals |
| **Atomic config writes** | Crash-safe config persistence via temp file + rename |
| **Log rotation** | 5 MB per log file, 3 backups kept automatically |
| **Terminal UI** | Animated boot, color-coded session states, pinned header and controls bar |

---

## Installation

```bash
git clone https://github.com/42U/socket-trader.git
cd socket-trader
pip install -r requirements.txt
```

**Requirements:** Python 3.10+ &nbsp;·&nbsp; NinjaTrader 8 (Windows) or any target directory (Linux)

---

## NinjaTrader Setup

Before SocketTrader can send orders, you must enable the **Automated Trading Interface** in NinjaTrader 8:

1. Open **NinjaTrader 8**
2. Go to **Tools** → **Settings**
3. Select **Automated trading interface** from the left panel
4. Check the **AT Interface** checkbox to enable it
5. Note the **Server port** (default: `36973`) — if you change it, go to Setup (`S`) > ATI Port in SocketTrader
6. Click **OK**

<p align="center">
  <img src="images/ninjatrader_conf.PNG" alt="NinjaTrader AT Interface Settings" width="500">
</p>

> **Important:** After clicking OK/Apply, you must **restart NinjaTrader 8** for the AT Interface changes to take effect. NinjaTrader must be running with the AT Interface enabled for signals to be processed from the `incoming/` folder.

### Strategy Templates

SocketTrader requires ATM and Stop strategy templates to be installed in NinjaTrader. On first run, the client **automatically copies** these into the correct directories:

| File | Source | Destination |
|------|--------|-------------|
| `NQ_Med.xml` | `strategy/` | `NinjaTrader 8/templates/AtmStrategy/` |
| `algoNQmed.xml` | `strategy/` | `NinjaTrader 8/templates/StopStrategy/` |

This happens automatically — no manual copying needed. If the files already exist, they are not overwritten.

### ATM Strategy Selection

The signal sent to NinjaTrader includes an **ATM Strategy template name** that controls how stops, targets, and trailing behavior are managed for each order. By default, SocketTrader uses `NQ_Med`.

If you want to use a different ATM strategy:

1. Create your custom ATM strategy template in NinjaTrader 8 (via the ATM Strategy selector on a chart or SuperDOM)
2. Press `S` in SocketTrader to open Setup, then select **Strategy** to set the name
3. The new name is saved to config and applied to all future signals

> The ATM strategy name in the signal **must match** an existing template in your NinjaTrader `templates/AtmStrategy/` folder, or the order will be rejected.

---

## Usage

```bash
python SocketTrader.py
```

On first run you'll be prompted in order:

| Prompt | Description |
|--------|-------------|
| **Server** | WebSocket server URL (`ws://` or `wss://`) and an optional name |
| **Token** | Server authentication token |
| **Account** | Auto-detected from NinjaTrader ATI, or manually entered |
| **Directory** | Auto-detected on Windows, manually entered on Linux |

Server and token are required to connect. Account and directory can also be configured later via the Setup menu (`S`).

All settings are saved and loaded automatically on subsequent runs. To change any setting mid-session, press `S` to open the Setup menu.

---

## Keyboard Controls

The controls bar is **pinned to the bottom** of the terminal at all times:

```
  P=PAUSE  B=BAL  T=LIMITS  C=CLOSE  R=RECONN  S=SETUP  ⇧X=EXIT
```

| Key | Action |
|:---:|--------|
| `P` | Pause / resume signal output |
| `B` | Show live balances and session P&L (press `R` to reset P&L) |
| `T` | Set session target and stop limits |
| `C` | Close open positions (arrow keys to select, `Y` to confirm) |
| `R` | Force immediate reconnect (resets backoff) |
| `S` | Open Setup menu (see below) |
| `Shift+X` | Exit SocketTrader |

### Setup Menu

Press `S` to open the Setup menu for all configuration options:

```
┌─ SETUP ──────────────────────────────────────────┐
│  1. Server    (wss://host:8420/ws)               │
│  2. Token     (****)                             │
│  3. Account   (Sim101)                           │
│  4. Strategy  (NQ_Med)                           │
│  5. Directory (/path/to/incoming)                │
│  6. ATI Port  (36973)                            │
│  7. Live Mon. (disabled)                         │
│  ESC to close                                    │
└──────────────────────────────────────────────────┘
```

Changing the **server** or **token** automatically triggers a reconnect. The server selector supports saving multiple servers:

---

## Optional Live Trade Monitor

NinjaTrader's built-in TCP AT Interface only publishes state transitions (open / fill / cancel / close). It does **not** push live market prices or live unrealized P&L, so by default SocketTrader's session stop / target limits can only fire *after* a trade closes — NinjaTrader's own ATM template handles per-trade risk.

If you want session limits to react **during** an open trade, install the optional `SocketTraderBridge` NinjaScript AddOn. It runs inside NinjaTrader and publishes live `cash`, `realized`, `unrealized`, `equity`, and per-position `last` price / P&L over a TCP socket on every tick.

**Install:**

1. Copy `addon/SocketTraderBridge.cs` into:
   `Documents\NinjaTrader 8\bin\Custom\AddOns\`
2. In NinjaTrader: **Control Center → New → NinjaScript Editor**.
3. Press **F5** to compile. No restart needed — the AddOn hot-loads on successful compile.
4. The Output tab should print:
   `SocketTraderBridge listening on 0.0.0.0:36984`

**Enable in SocketTrader:** `S → 7 → 1` to toggle. The menu shows three states:

- `disabled` — plain ATI only, session limits fire post-close.
- `enabled · active` — AddOn reachable, live P&L stream is flowing.
- `enabled · NOT REACHABLE` — user wants it on but the AddOn isn't responding. Usually means the `.cs` file wasn't copied, wasn't compiled, or NinjaTrader is down.

When the state is `enabled · NOT REACHABLE`, SocketTrader prints a yellow warning on startup and in the Setup menu so you don't miss it.

Full protocol, port customization, and security notes: see [`addon/README.md`](addon/README.md).

---

## Live Balances

Press `B` at any time to see all NinjaTrader accounts with real-time balances and session P&L:

```
  ╭─ BALANCES ─────────────────────────────────╮
  │  Sim101      $28,857.02  P&L: +$247.50 ◀  │
  │  SimXFAFK     $3,939.48  P&L: -$60.52      │
  ╰────────────────────────────────────────────╯
```

- Balances are queried live from NinjaTrader ATI
- Session P&L is calculated from the balance snapshot taken on connect
- `◀` marks the currently active account

---

## Risk Management

SocketTrader includes built-in risk controls that monitor your account balance in real time via the NinjaTrader ATI. Press `T` to configure.

> **There is no separate on/off toggle.** The dollar amount *is* the switch:
> - **Set a value** (e.g. `500` or `-300`) → that limit is **active**
> - **Set to `0`** → that limit is **disabled**
>
> By default both are `0` — SocketTrader will not interfere with your trading unless you explicitly set a limit. Target and stop are independent — you can use one, both, or neither.

### How It Works

1. On connect, SocketTrader snapshots your account balance from NinjaTrader ATI
2. Every 3 seconds, it polls your current balance and calculates session P&L
3. If P&L crosses your configured threshold, the appropriate action fires

### Session Stop

A **floor** for your session P&L. Triggers when P&L drops to or below this value. Set to `0` to disable.

- **Negative value** (e.g. `-300`): classic loss limit — stop out if you lose $300
- **Positive value** (e.g. `200`): profit protection — if you're up $500 and set stop to `200`, it triggers if P&L drops back to $200, locking in at least $200 of profit. Capped at 90% of current P&L to prevent accidental instant triggers

### Session Target

A **profit goal** for the session (positive dollar amount). Triggers when session P&L reaches or exceeds this value. Set to `0` to disable.

### Soft vs Hard Mode

Each limit (target and stop) can independently be set to **soft** or **hard** mode:

| Mode | Action | Recovery |
|------|--------|----------|
| **Soft** | Pauses signals · press `P` to resume | Resume any time with `P` |
| **Hard** | Flattens all positions · signals off for the session | Press `Shift+X` to exit — cannot resume |

When a **hard** limit fires, SocketTrader writes a `CLOSEPOSITION;{account};{contract};;;;;;;;;;` file to the `incoming/` folder for every instrument traded during the session. This tells NinjaTrader to flatten all positions immediately.

**Defaults:** Target defaults to **soft**, stop defaults to **hard** — but you can set any combination:

| Example | Use case |
|---------|----------|
| Target: soft, Stop: hard | Lock in profits (resumable), auto-flatten on loss |
| Target: hard, Stop: hard | Walk away — both sides close everything |
| Target: soft, Stop: soft | Gentle nudge on both sides, always resumable |
| Target: off, Stop: hard | No profit cap, hard loss protection only |

### Configuring Limits

Press `T` during a session:

```
┌─ SESSION LIMITS (Sim101) ────────────────────────┐
│  Balance: $28,857.02  ·  Session P&L: +$0.00     │
│  Target: +500.00 (soft)  ·  Stop: -300.00 (hard) │
│  When a limit is hit, the lockout mode decides:  │
│  soft = pause signals · resumable with P         │
│  hard = flatten all positions · signals off      │
│  Enter 0 to disable. ENTER to keep current.      │
└──────────────────────────────────────────────────┘
  TARGET $ (current: +500.00) ▸ 500
  TARGET MODE (current: soft) [soft/hard] ▸ soft
  STOP $ (current: -300.00) ▸ -300
  STOP MODE (current: hard) [soft/hard] ▸ hard
```

- Enter a **positive number** for target (e.g. `500` = trigger at +$500 profit)
- Enter a **negative or positive number** for stop (e.g. `-300` = loss limit, `200` = protect profit). Positive values are capped at 90% of current session P&L
- Choose **soft** or **hard** mode for each limit
- Enter `0` to **disable** either side (mode prompt is skipped)
- Press **ENTER** to keep the current value

Limits and modes are saved **per account** in your config file and persist across sessions.

### Session Summary

On exit, SocketTrader displays your session results including final P&L:

```
  ┌─ SESSION SUMMARY ───────────────────────────────────────────┐
  │  Signals received: 12                                       │
  │  Session P&L: +$247.50                                      │
  │  Log: C:\Users\trader\.voidorigin_signals.log               │
  └─────────────────────────────────────────────────────────────┘
```

---

## Signal Latency

Each signal displays delivery time from server to client. The first signal on each connection sets the **baseline** latency, and subsequent signals are color-coded relative to it:

| Color | Meaning |
|:-----:|---------|
| Green | Faster than baseline |
| Yellow | Within 250ms of baseline |
| Red | More than 250ms slower than baseline |

```
[14:32:05] ▸  PLACE;<account>;<instrument>;BUY;1;MARKET;;;DAY;;;<strategy>;<strategy id>
   ├─ latency: 47ms
   └─ saved → C:\Users\...\NinjaTrader 8\incoming

[14:33:12] ▸  PLACE;<account>;<instrument>;SELL;1;MARKET;;;DAY;;;<strategy>;<strategy id>
   ├─ latency: 312ms (+265ms)     ← red: 265ms slower than baseline
   └─ saved → C:\Users\...\NinjaTrader 8\incoming
```

---

## Configuration

Settings persist in `~/.voidorigin_config.json`:

```json
{
  "ws_host": "wss://your-server:8420/ws",
  "servers": [
    { "name": "Production", "url": "wss://your-server:8420/ws" },
    { "name": "Dev", "url": "ws://localhost:8420/ws" }
  ],
  "token": "your_connection_token",
  "account": "Sim101",
  "atm_strategy": "NQ_Med",
  "output_directory": "C:\\Users\\you\\Documents\\NinjaTrader 8\\incoming",
  "nt_port": 36973,
  "account_limits": {
    "Sim101": { "target": 500, "target_mode": "soft", "stop": -300, "stop_mode": "hard" },
    "MyLiveAccount": { "target": 1000, "target_mode": "hard", "stop": -500, "stop_mode": "hard" }
  }
}
```

| Field | Description |
|-------|-------------|
| `ws_host` | Active WebSocket server URL |
| `servers` | Saved server list (up to 10) for quick switching |
| `token` | Authentication token (rotates frequently) |
| `account` | Active NinjaTrader account name |
| `atm_strategy` | ATM strategy template name |
| `output_directory` | Path to NinjaTrader 8 `incoming/` folder |
| `nt_port` | NinjaTrader AT Interface port |
| `account_limits` | Per-account risk management settings |

> This file contains your authentication token and is excluded from version control via `.gitignore`. On non-Windows systems, file permissions are set to `0600` (owner-only).

---

## Security

SocketTrader communicates with NinjaTrader over a local TCP socket as designed by the ATI. Always run on a trusted machine.

---

## License

Distributed under the **MIT License**. See [LICENSE](LICENSE) for details.

---

<p align="center">
  <a href="https://github.com/42U"><img src="https://img.shields.io/badge/built%20by-42U-181717?logo=github" alt="Built by 42U"></a>
  <a href="https://voidorigin.com"><img src="https://img.shields.io/badge/powered%20by-VoidOrigin-blueviolet" alt="Powered by VoidOrigin"></a>
</p>
