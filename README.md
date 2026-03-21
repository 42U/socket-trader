# socket-trader

A lightweight WebSocket gateway for NinjaTrader 8 that receives trading signals and executes orders in real time.

```
  ╔══════════════════════════════════════════╗
  ║         VOIDORIGIN  ·  SIGNAL NODE       ║
  ╚══════════════════════════════════════════╝
```

## What It Does

`socket-trader` connects to a remote WebSocket signal server, receives JSON-formatted trading signals, and writes them as raw order strings directly into NinjaTrader 8's `incoming/` folder for automated execution.

**Signal flow:**
```
Server → WebSocket → socketClient.py → incoming/ → NinjaTrader 8
```

**Example:**
```
Received:  {"signal": "PLACE;SimAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020"}
Written:   PLACE;YourAccount;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020
```

The sim account in the signal is automatically replaced with your real NinjaTrader account name.

## Features

- **Cross-platform** — Windows and Linux
- **Auto-detects NinjaTrader 8** `incoming/` folder on Windows (Documents, OneDrive, drive roots)
- **Persistent config** — password, account name, and output directory saved to `~/.voidorigin_config.json` so you only configure once
- **Auto-reconnect** — exponential backoff on connection loss (1s → 2s → 4s → ... → 60s max)
- **Re-prompts on auth failure** — bad password doesn't loop forever, it asks again
- **ASCII terminal UI** — animated boot sequence, signal pulse animations, status bar

## Requirements

- Python 3.7+
- NinjaTrader 8 (Windows) or any target directory (Linux)

## Installation

```bash
git clone https://github.com/42U/socket-trader.git
cd socket-trader
pip install -r requirements.txt
```

## Usage

```bash
python socketClient.py
```

On first run, you'll be prompted for:

1. **Password** — server authentication token
2. **Account** — your NinjaTrader account name (replaces `Sim*` in signals)
3. **Output directory** — auto-detected on Windows, manually entered on Linux

These are saved to `~/.voidorigin_config.json` and loaded automatically on subsequent runs.

## Keyboard Controls

| Key | Action |
|-----|--------|
| `P` | Pause/resume signal output |
| `A` | Change NinjaTrader account |
| `D` | Change output directory |
| `R` | Force reconnect |
| `C` | Close connection and exit |

## Config

Settings are stored in `~/.voidorigin_config.json`:

```json
{
  "password": "your_token",
  "account": "YourNTAccount",
  "output_directory": "C:\\Users\\you\\Documents\\NinjaTrader 8\\incoming"
}
```

This file is excluded from version control via `.gitignore`.

## License

See [LICENSE](LICENSE) for details.
