# SocketTraderBridge — optional live-data AddOn for NinjaTrader 8

NinjaTrader's built-in TCP AT Interface only pushes state transitions
(open / fill / cancel / close). It does not push live market prices or live
unrealized P&L, so an external monitor like Socket Trader can only react
to trades *after they close*.

This AddOn solves that. It runs inside NinjaTrader and publishes a
newline-delimited JSON stream on a TCP port with live `cash`, `realized`,
`unrealized`, `equity`, and per-position `last`/`avg`/`qty`/`pl` on every
tick and account event.

## When you need it

- You want session stop / target limits to fire **during** an open trade,
  not just after the ATM template closes it.
- You want to display live P&L in the Socket Trader dashboard.
- You run **prop-marked accounts** and want entries to skip the
  close-before-open snapshot: every stream line is a full position book,
  so a book that already proves all prop accounts flat lets the entry
  fire immediately instead of waiting on a pre-entry ATI state dump.

If you're fine with NT's ATM template handling per-trade risk and
Socket Trader only locking out future signals after a trade completes,
you don't need this AddOn — the default TCP ATI path is enough.

## Install

1. Copy `SocketTraderBridge.cs` into:

   ```
   C:\Users\<you>\Documents\NinjaTrader 8\bin\Custom\AddOns\
   ```

2. In NinjaTrader: **Control Center → New → NinjaScript Editor**.
3. Right-click the editor tree → **Compile** (or press **F5**).
   There should be no errors. A compile warning about unused variables is
   fine; any red error means the file didn't copy cleanly.
4. Restart NinjaTrader (required for AddOns to load). On startup, the
   Output tab should print:

   ```
   SocketTraderBridge listening on 0.0.0.0:36984
   ```

5. Port **36984** is the AddOn; port **36973** is NT's built-in ATI.
   Socket Trader will use the AddOn when it's reachable and fall back
   to plain ATI otherwise.

## Verify it's working

From any machine on the network:

```bash
nc 192.168.1.111 36984
```

You should see a JSON line every ~5 seconds (heartbeat) plus one per
account/position/price event. Example during an open NQ short:

```json
{"t":1713885600.123,"accounts":[
  {"name":"Sim101","cash":27459.96,"realized":-2.18,
   "positions":[{"inst":"NQ 06-26","qty":-1,"avg":27068.25,"last":27074.50,"pl":-125.50}],
   "unrealized":-125.50,"equity":27334.46}
]}
```

## Commands

Socket Trader can also send one-line JSON commands over the same port
(token-authenticated, one command per connection):

- `{"cmd":"flatten","account":"…"}` — flatten every position on the account
- `{"cmd":"close_position","account":"…","instrument":"…"}` — close one position
- `{"cmd":"front_months","roots":"ES,NQ,SIL,…"}` — replies with the contract
  NT itself considers current per root under its rollover schedule, e.g.
  `{"ack":true,"msg":"front_months","months":{"SI":"12-26","ES":"09-26"}}`.
  Socket Trader sends this on every bridge connect and once per day, and
  uses the answer to auto-roll incoming signals that still name an expiring
  contract month (see *Front-Month Roll Guard* in the main README). An
  older AddOn build refuses the command harmlessly — recompile to enable.

## Changing the port

Edit the `ListenPort` constant at the top of `SocketTraderBridge.cs`,
recompile (F5), and restart NinjaTrader. If you restrict `BindAddress`
from `IPAddress.Any` to `IPAddress.Loopback`, only local connections will
be accepted — useful if NT and Socket Trader run on the same machine or
WSL with mirrored networking.

## Security notes

- The stream is plaintext TCP gated by a shared token
  (`SocketTraderBridge.token` in NT's user-data folder, written and
  rotated by Socket Trader): no valid token, no data and no commands.
  Still, don't expose the port to the public internet — keep it on your
  LAN or a VPN.
- The AddOn reads account state and accepts only the commands listed
  above — it can flatten/close existing positions and report front
  months, but it cannot place new orders. Order entry still goes through
  the existing ATI incoming folder.

## Uninstalling

Delete `SocketTraderBridge.cs` from the AddOns folder, recompile (F5)
to drop the class from NT's compiled assembly, and restart NinjaTrader.
