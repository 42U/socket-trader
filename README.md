<p align="center">
  <img src="images/socket-traderLOGO.png" alt="SocketTrader" width="600">
</p>

<p align="center">
  <strong>Real-time WebSocket signal gateway for NinjaTrader 8</strong>
</p>

<p align="center">
  <a href="#installation">Installation</a> &nbsp;·&nbsp;
  <a href="#ninjatrader-setup">NinjaTrader Setup</a> &nbsp;·&nbsp;
  <a href="#usage">Usage</a> &nbsp;·&nbsp;
  <a href="#keyboard-controls">Controls</a> &nbsp;·&nbsp;
  <a href="#configuration">Config</a>
</p>

---

## Overview

SocketTrader connects to a remote signal server over WebSocket, receives trading signals in real time, and writes them directly into NinjaTrader 8's `incoming/` folder for automated order execution.

```
┌──────────┐      WebSocket      ┌──────────────┐      File I/O      ┌──────────────┐
│  Signal  │ ──────────────────▸ │ socketClient  │ ──────────────────▸ │ NinjaTrader  │
│  Server  │    JSON signals     │     .py       │   raw order text   │  8 incoming/  │
└──────────┘                     └──────────────┘                     └──────────────┘
```

**Signal transformation:**
```
IN   {"signal": "PLACE;SimAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020", "ts": 1711000000000}
OUT  PLACE;YourAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020
```

The sim account is automatically swapped with your real NinjaTrader account name. Server timestamps are used to display signal latency.

---

## Features

| | |
|---|---|
| **Cross-platform** | Windows and Linux support |
| **Auto-detect** | Finds NinjaTrader 8 `incoming/` folder on Windows automatically |
| **Persistent config** | Token, account, and directory saved to `~/.voidorigin_config.json` |
| **Smart reconnect** | Fibonacci backoff: 1m → 1m → 2m → 3m → 5m → 8m → ... → 30m max |
| **Auth handling** | Invalid token triggers re-prompt instead of infinite retry |
| **Latency monitoring** | Displays signal delivery time with color-coded thresholds |
| **Terminal UI** | Animated boot sequence, signal pulses, live status bar |

---

## Installation

```bash
git clone https://github.com/42U/socket-trader.git
cd socket-trader
pip install -r requirements.txt
```

**Requirements:** Python 3.7+ &nbsp;·&nbsp; NinjaTrader 8 (Windows) or any target directory (Linux)

---

## NinjaTrader Setup

Before SocketTrader can send orders, you must enable the **Automated Trading Interface** in NinjaTrader 8:

1. Open **NinjaTrader 8**
2. Go to **Tools** → **Options**
3. Select **Automated trading interface** from the left panel
4. Check the **AT Interface** checkbox to enable it
5. Note the **Server port** (default: `36973`) — if you change it, press `O` in SocketTrader to update
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
2. Press `S` in SocketTrader to set the strategy name to match your template
3. The new name is saved to config and applied to all future signals

> The ATM strategy name in the signal **must match** an existing template in your NinjaTrader `templates/AtmStrategy/` folder, or the order will be rejected.

---

## Usage

```bash
python socketClient.py
```

On first run you'll be prompted for:

| Prompt | Description |
|--------|-------------|
| **Token** | Server authentication token |
| **Account** | Your NinjaTrader account name (replaces `Sim*` in signals) |
| **Directory** | Auto-detected on Windows, manually entered on Linux |

All settings are saved and loaded automatically on subsequent runs.

---

## Keyboard Controls

```
╔════════════════════════════════════════════════════════════════════════╗
║  P=PAUSE  A=ACCT  S=STRAT  D=DIR  O=PORT  R=RECONN  C=CLOSE          ║
╚════════════════════════════════════════════════════════════════════════╝
```

| Key | Action |
|:---:|--------|
| `P` | Pause / resume signal output |
| `A` | Switch NinjaTrader account |
| `S` | Change ATM strategy template |
| `D` | Change output directory |
| `O` | Change NinjaTrader AT Interface port |
| `R` | Force immediate reconnect (resets backoff) |
| `C` | Close connection and exit |

---

## Configuration

Settings persist in `~/.voidorigin_config.json`:

```json
{
  "token": "your_connection_token",
  "account": "YourNTAccount",
  "atm_strategy": "NQ_Med",
  "output_directory": "C:\\Users\\you\\Documents\\NinjaTrader 8\\incoming",
  "nt_port": 36973
}
```

> This file contains your authentication token and is excluded from version control via `.gitignore`.

---

## Signal Latency

Each signal displays delivery time from server to client. The first signal on each connection sets the **baseline** latency, and subsequent signals are color-coded relative to it:

| Color | Meaning |
|:-----:|---------|
| Green | Faster than baseline |
| Yellow | Within 250ms of baseline |
| Red | More than 250ms slower than baseline |

```
[14:32:05] ▸  PLACE;MyAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020
   ├─ latency: 47ms
   └─ saved → C:\Users\...\NinjaTrader 8\incoming

[14:33:12] ▸  PLACE;MyAccount;NQ 06-26;SELL;1;MARKET;;;DAY;;;NQ_Med;1020
   ├─ latency: 312ms (+265ms)     ← red: 265ms slower than baseline
   └─ saved → C:\Users\...\NinjaTrader 8\incoming
```

---

## License

See [LICENSE](LICENSE) for details.

---

<p align="center">
  <sub>Built by <a href="https://github.com/42U">42U</a> &nbsp;·&nbsp; Powered by <a href="https://voidorigin.com">VoidOrigin</a></sub>
</p>
