<p align="center">
  <img src="logo/socket-traderLOGO.png" alt="SocketTrader" width="600">
</p>

<p align="center">
  <strong>Real-time WebSocket signal gateway for NinjaTrader 8</strong>
</p>

<p align="center">
  <a href="#installation">Installation</a> &nbsp;·&nbsp;
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
╔══════════════════════════════════════════════════════════════╗
║  P = PAUSE   A = ACCOUNT   D = DIR   R = RECONNECT   C = CLOSE  ║
╚══════════════════════════════════════════════════════════════╝
```

| Key | Action |
|:---:|--------|
| `P` | Pause / resume signal output |
| `A` | Switch NinjaTrader account |
| `D` | Change output directory |
| `R` | Force immediate reconnect (resets backoff) |
| `C` | Close connection and exit |

---

## Configuration

Settings persist in `~/.voidorigin_config.json`:

```json
{
  "token": "your_connection_token",
  "account": "YourNTAccount",
  "output_directory": "C:\\Users\\you\\Documents\\NinjaTrader 8\\incoming"
}
```

> This file contains your authentication token and is excluded from version control via `.gitignore`.

---

## Signal Latency

Each signal displays delivery time from server to client, color-coded:

| Color | Threshold |
|:-----:|-----------|
| Green | < 200ms |
| Yellow | < 1s |
| Red | > 1s |

```
[14:32:05] ▸  PLACE;MyAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020
   ├─ latency: 47ms
   └─ saved → C:\Users\...\NinjaTrader 8\incoming
```

---

## License

See [LICENSE](LICENSE) for details.

---

<p align="center">
  <sub>Built by <a href="https://github.com/42U">42U</a> &nbsp;·&nbsp; Powered by <a href="https://voidorigin.com">VoidOrigin</a></sub>
</p>
