// SocketTraderBridge — NinjaScript AddOn for live account/position/P&L streaming
//
// Optional companion to Socket Trader's Python client. The TCP AT Interface
// only pushes state transitions (open/close/fill), so mid-trade unrealized
// P&L and live prices aren't visible to an external monitor. This AddOn runs
// inside NinjaTrader and publishes JSON lines with live data on every tick
// and account/position event.
//
// INSTALL
//   1. Drop this file into  Documents\NinjaTrader 8\bin\Custom\AddOns\
//   2. In NinjaTrader 8 →  Control Center  →  New  →  NinjaScript Editor
//   3. Press F5 to compile.
//   4. Restart NinjaTrader. The AddOn auto-loads and starts listening.
//
// WIRE FORMAT
//   Each line is a standalone JSON object (newline-delimited). Example:
//     {"t":1713885600.123,"acct":"Sim101","cash":27462.14,"realized":-2.18,
//      "unrealized":-125.50,"positions":[
//        {"inst":"NQ JUN26","qty":-1,"avg":27068.25,"last":27074.50,"pl":-125.50}
//      ]}
//
// PROTOCOL
//   - Pure push: clients connect, receive a full snapshot immediately,
//     then receive updates whenever account / position / price changes.
//   - Multiple clients supported (list of TcpClient, broadcast on each event).
//   - Binds to 0.0.0.0 by default; change BindAddress below to restrict.
//   - Default port 36974 to sit alongside NT's built-in ATI on 36973.

#region Using declarations
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript.AddOns;
#endregion

namespace NinjaTrader.NinjaScript.AddOns
{
    public class SocketTraderBridge : AddOnBase
    {
        // ---------- User-tunable ----------
        private const int ListenPort = 36974;
        private static readonly IPAddress BindAddress = IPAddress.Any;

        // Heartbeat snapshot while idle so stale-detection on the client side
        // has something to see. NT may go minutes without events when flat.
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);

        // Per-write deadline. Windows default TCP keepalive on a half-open
        // connection is ~2h — without this, one dead peer can wedge the
        // whole broadcast thread and starve every live client.
        private const int WriteTimeoutMs = 750;

        // ---------- State ----------
        private TcpListener listener;
        private Thread acceptThread;
        private Timer heartbeatTimer;
        private readonly List<TcpClient> clients = new List<TcpClient>();
        private readonly object clientsLock = new object();

        // Instruments we've subscribed to market data for (with open positions)
        private readonly Dictionary<Instrument, double> lastPrice =
            new Dictionary<Instrument, double>();
        private readonly object subsLock = new object();

        // ---------- Lifecycle ----------
        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name = "SocketTraderBridge";
                Description = "Streams live account/position/P&L JSON over TCP for Socket Trader.";
            }
            else if (State == State.Active)
            {
                StartListener();
                HookAccounts();
                SubscribeExistingPositions();
                heartbeatTimer = new Timer(_ => SafeBroadcast(), null,
                    HeartbeatInterval, HeartbeatInterval);
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge listening on {BindAddress}:{ListenPort}",
                    PrintTo.OutputTab1);
            }
            else if (State == State.Terminated)
            {
                heartbeatTimer?.Dispose();
                UnhookAccounts();
                UnsubscribeAllMarketData();
                StopListener();
            }
        }

        // ---------- TCP ----------
        private void StartListener()
        {
            try
            {
                listener = new TcpListener(BindAddress, ListenPort);
                listener.Start();
                acceptThread = new Thread(AcceptLoop) { IsBackground = true, Name = "STB-Accept" };
                acceptThread.Start();
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: failed to start listener: {ex.Message}",
                    PrintTo.OutputTab1);
            }
        }

        private void StopListener()
        {
            try { listener?.Stop(); } catch { }
            listener = null;
            lock (clientsLock)
            {
                foreach (var c in clients)
                {
                    try { c.Close(); } catch { }
                }
                clients.Clear();
            }
        }

        private void AcceptLoop()
        {
            while (listener != null)
            {
                TcpClient client;
                try { client = listener.AcceptTcpClient(); }
                catch { return; }

                // Configure the socket: disable Nagle so the first-snapshot
                // reaches the client immediately, and cap write time so a
                // half-open peer can't wedge the accept thread on the next
                // broadcast.
                try { client.NoDelay = true; } catch { }
                try { client.SendTimeout = WriteTimeoutMs; } catch { }
                try { client.ReceiveTimeout = WriteTimeoutMs; } catch { }

                // Build + send the initial snapshot DIRECTLY to the new
                // client. Doing this here (before adding to `clients`) means
                // a hang on an existing dead peer can't block a new client
                // from getting its first payload.
                string initialJson = null;
                try { initialJson = BuildSnapshotJson(); }
                catch (Exception ex)
                {
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: initial snapshot error: {ex.Message}",
                        PrintTo.OutputTab1);
                }
                if (initialJson != null)
                {
                    try
                    {
                        var bytes = Encoding.UTF8.GetBytes(initialJson + "\n");
                        var stream = client.GetStream();
                        stream.WriteTimeout = WriteTimeoutMs;
                        stream.Write(bytes, 0, bytes.Length);
                    }
                    catch (Exception ex)
                    {
                        NinjaTrader.Code.Output.Process(
                            $"SocketTraderBridge: initial write to new client failed: {ex.Message}",
                            PrintTo.OutputTab1);
                        try { client.Close(); } catch { }
                        continue;  // don't register a client we couldn't even snapshot
                    }
                }

                lock (clientsLock) { clients.Add(client); }
            }
        }

        private void SafeBroadcast()
        {
            string json;
            try { json = BuildSnapshotJson(); }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: snapshot error: {ex.Message}", PrintTo.OutputTab1);
                return;
            }
            var bytes = Encoding.UTF8.GetBytes(json + "\n");
            lock (clientsLock)
            {
                var dead = new List<TcpClient>();
                foreach (var c in clients)
                {
                    try
                    {
                        var stream = c.GetStream();
                        stream.WriteTimeout = WriteTimeoutMs;
                        stream.Write(bytes, 0, bytes.Length);
                    }
                    catch { dead.Add(c); }
                }
                foreach (var d in dead)
                {
                    try { d.Close(); } catch { }
                    clients.Remove(d);
                }
            }
        }

        // ---------- Account / position hooks ----------
        private void HookAccounts()
        {
            lock (Account.All)
            {
                foreach (var acct in Account.All)
                {
                    acct.AccountItemUpdate += OnAccountItemUpdate;
                    acct.PositionUpdate += OnPositionUpdate;
                }
            }
            // Watch for accounts added/removed after startup.
            Account.AccountStatusUpdate += OnAccountStatusUpdate;
        }

        private void UnhookAccounts()
        {
            Account.AccountStatusUpdate -= OnAccountStatusUpdate;
            lock (Account.All)
            {
                foreach (var acct in Account.All)
                {
                    acct.AccountItemUpdate -= OnAccountItemUpdate;
                    acct.PositionUpdate -= OnPositionUpdate;
                }
            }
        }

        private void OnAccountItemUpdate(object sender, AccountItemEventArgs e)
        {
            // Fires on CashValue / RealizedPnL / BuyingPower changes.
            SafeBroadcast();
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            // Fires on any position change — open, size change, close.
            EnsureMarketDataSubscription(e.Position);
            SafeBroadcast();
        }

        private void OnAccountStatusUpdate(object sender, AccountStatusEventArgs e)
        {
            // Re-hook all accounts (cheap, idempotent since += on already-hooked is noop-ish).
            // NT fires Connected / Disconnected / etc.; just rebroadcast.
            SafeBroadcast();
        }

        // ---------- Market data ----------
        private void SubscribeExistingPositions()
        {
            lock (Account.All)
            {
                foreach (var acct in Account.All)
                {
                    foreach (var pos in acct.Positions)
                    {
                        if (pos.MarketPosition != MarketPosition.Flat)
                            EnsureMarketDataSubscription(pos);
                    }
                }
            }
        }

        private void EnsureMarketDataSubscription(Position pos)
        {
            if (pos == null || pos.Instrument == null) return;
            var inst = pos.Instrument;

            if (pos.MarketPosition == MarketPosition.Flat)
            {
                UnsubscribeMarketData(inst);
                return;
            }

            lock (subsLock)
            {
                if (lastPrice.ContainsKey(inst)) return;
                lastPrice[inst] = 0;
            }
            try
            {
                inst.MarketData.Update += OnMarketDataUpdate;
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: market data subscribe failed for " +
                    $"{inst.FullName}: {ex.Message}", PrintTo.OutputTab1);
            }
        }

        private void UnsubscribeMarketData(Instrument inst)
        {
            if (inst == null) return;
            lock (subsLock)
            {
                if (!lastPrice.ContainsKey(inst)) return;
                lastPrice.Remove(inst);
            }
            try { inst.MarketData.Update -= OnMarketDataUpdate; } catch { }
        }

        private void UnsubscribeAllMarketData()
        {
            Instrument[] all;
            lock (subsLock)
            {
                all = new Instrument[lastPrice.Count];
                lastPrice.Keys.CopyTo(all, 0);
                lastPrice.Clear();
            }
            foreach (var inst in all)
            {
                try { inst.MarketData.Update -= OnMarketDataUpdate; } catch { }
            }
        }

        private void OnMarketDataUpdate(object sender, MarketDataEventArgs e)
        {
            if (e.MarketDataType != MarketDataType.Last) return;
            var md = sender as MarketDataEventArgs;  // type-safe cast below
            var inst = e.Instrument;
            if (inst == null) return;

            lock (subsLock)
            {
                if (!lastPrice.ContainsKey(inst)) return;
                lastPrice[inst] = e.Price;
            }
            SafeBroadcast();
        }

        // ---------- JSON ----------
        private string BuildSnapshotJson()
        {
            var sb = new StringBuilder(512);
            double ts = (DateTime.UtcNow - new DateTime(1970, 1, 1,
                0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
            sb.Append("{\"t\":").Append(ts.ToString("F3", CultureInfo.InvariantCulture));
            sb.Append(",\"accounts\":[");

            // Snapshot the collection so NT can mutate it during our
            // iteration without throwing InvalidOperationException.
            Account[] accountsSnapshot;
            try
            {
                var list = new List<Account>();
                foreach (var a in Account.All) list.Add(a);
                accountsSnapshot = list.ToArray();
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: Account.All snapshot failed: {ex.Message}",
                    PrintTo.OutputTab1);
                accountsSnapshot = new Account[0];
            }

            bool firstAcct = true;
            foreach (var acct in accountsSnapshot)
            {
                string accountFragment;
                try { accountFragment = BuildAccountFragment(acct); }
                catch (Exception ex)
                {
                    // One rogue account must not kill the whole response.
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: skipping account " +
                        $"'{(acct == null ? "<null>" : acct.Name)}': {ex.Message}",
                        PrintTo.OutputTab1);
                    continue;
                }
                if (accountFragment == null) continue;
                if (!firstAcct) sb.Append(',');
                firstAcct = false;
                sb.Append(accountFragment);
            }
            sb.Append("]}");
            return sb.ToString();
        }

        private string BuildAccountFragment(Account acct)
        {
            if (acct == null) return null;
            var sb = new StringBuilder(256);

            double cash = 0, realized = 0;
            try { cash = acct.Get(AccountItem.CashValue, Currency.UsDollar); } catch { }
            try { realized = acct.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar); } catch { }

            sb.Append("{\"name\":").Append(JsonString(acct.Name ?? ""));
            sb.Append(",\"cash\":").Append(D(cash));
            sb.Append(",\"realized\":").Append(D(realized));

            double unrealizedSum = 0;
            sb.Append(",\"positions\":[");

            Position[] posSnapshot;
            try
            {
                var list = new List<Position>();
                foreach (var p in acct.Positions) list.Add(p);
                posSnapshot = list.ToArray();
            }
            catch { posSnapshot = new Position[0]; }

            bool firstPos = true;
            foreach (var pos in posSnapshot)
            {
                try
                {
                    if (pos == null || pos.Instrument == null) continue;
                    if (pos.MarketPosition == MarketPosition.Flat) continue;
                    double last;
                    lock (subsLock)
                    {
                        lastPrice.TryGetValue(pos.Instrument, out last);
                    }
                    int qty = pos.Quantity;
                    if (pos.MarketPosition == MarketPosition.Short) qty = -qty;
                    double avg = pos.AveragePrice;
                    double pl = 0;
                    if (last > 0)
                    {
                        try { pl = pos.GetUnrealizedProfitLoss(PerformanceUnit.Currency, last); }
                        catch { pl = 0; }
                    }
                    unrealizedSum += pl;

                    if (!firstPos) sb.Append(',');
                    firstPos = false;
                    sb.Append("{\"inst\":").Append(JsonString(pos.Instrument.FullName ?? ""));
                    sb.Append(",\"qty\":").Append(qty);
                    sb.Append(",\"avg\":").Append(D(avg));
                    sb.Append(",\"last\":").Append(D(last));
                    sb.Append(",\"pl\":").Append(D(pl));
                    sb.Append('}');
                }
                catch (Exception ex)
                {
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: skipping position on " +
                        $"{acct.Name}: {ex.Message}", PrintTo.OutputTab1);
                }
            }
            sb.Append(']');
            sb.Append(",\"unrealized\":").Append(D(unrealizedSum));
            sb.Append(",\"equity\":").Append(D(cash + unrealizedSum));
            sb.Append('}');
            return sb.ToString();
        }

        private static string D(double v) =>
            double.IsNaN(v) || double.IsInfinity(v) ? "0"
                : v.ToString("R", CultureInfo.InvariantCulture);

        private static string JsonString(string s)
        {
            if (s == null) return "null";
            var sb = new StringBuilder(s.Length + 2);
            sb.Append('"');
            foreach (var c in s)
            {
                switch (c)
                {
                    case '"':  sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (c < 0x20) sb.AppendFormat("\\u{0:x4}", (int)c);
                        else sb.Append(c);
                        break;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }
}
