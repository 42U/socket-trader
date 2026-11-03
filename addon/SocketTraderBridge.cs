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
//     {"t":1713885600.123,"accounts":[
//       {"name":"Sim101","status":"Connected","cash":27462.14,"realized":-2.18,
//        "positions":[{"inst":"NQ 06-26","qty":-1,"avg":27068.25,"last":27074.50,"pl":-125.50}],
//        "unrealized":-125.50,"equity":27336.64}]}
//   `partial:true` is added when an account could not be read; a client
//   must not treat such a line as proof that anything is flat.
//
// PROTOCOL
//   - Pure push: clients connect, receive a full snapshot immediately,
//     then receive updates whenever account / position / price changes.
//   - Multiple clients supported (list of TcpClient, broadcast on each event).
//   - Binds to 0.0.0.0 by default; change BindAddress below to restrict.
//   - Default port 36984 to sit alongside NT's built-in ATI on 36973
//     (SocketTraderBridge.port next to the token overrides it)
//     without colliding with VSCode's auto-port-forwarder (which claims
//     36974 on loopback if your terminal session is using it).
//
// CHANGES 2026-11-03 (review fixes; each site is tagged [FIX-x])
//   A  Account.All / acct.Positions are read under NinjaTrader's lock; a read
//      that still fails ABORTS the broadcast (or marks the line partial)
//      instead of publishing an empty — i.e. "flat" — book.
//   B  One broadcaster thread builds and writes every snapshot, so lines can
//      never reach a client out of order; a new client is registered before
//      its first snapshot is built, so nothing is lost in between.
//   C  Market data is unsubscribed only when NO account still holds the
//      instrument (one account closing NQ no longer zeroes everyone's NQ P&L).
//   D  close_position matches the contract exactly (either "NQ 12-26" or
//      "NQ DEC26" spelling); no more substring hits (MES vs ES, SIL vs SI);
//      acks false when nothing matched.
//   E  Accounts that appear after start-up (connections that come up later)
//      are hooked on every AccountStatusUpdate; hooks are tracked so they are
//      never attached twice.
//   F  Each account carries its connection "status".
//   G  Snapshot building and socket writes moved off NinjaTrader's event and
//      market-data threads onto the broadcaster thread; tick storms coalesce.
//   H  Accept loop survives transient socket errors; a listener that failed
//      to bind at start-up is retried every heartbeat.
//   I  Acks and broadcasts to one client are serialised by a per-client lock.
//   J  Last price seeded from NT's last known trade when subscribing.
//   K  qty serialised culture-invariant.
//   L  Terminated flag checked by every callback; subscribe/unsubscribe
//      serialised; broadcaster joined on shutdown.
//   M  \uXXXX escapes decoded in incoming commands.
//   N  Rejected-connection log line rate-limited; concurrent unauthenticated
//      handshakes capped.
//   O  Any exception in a command only fails that command; a client whose
//      reader loop ends is closed and evicted immediately.
//   P  A position event is treated as "closed" on Operation.Remove as well
//      as on MarketPosition.Flat.
//   Q  Cash / realized read in the account's own denomination.

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
        // 36984 sits adjacent to NT's ATI on 36973 (easy to remember as a
        // pair) and deliberately avoids 36974, which VSCode's built-in
        // port auto-forwarder is prone to claiming on loopback when it
        // runs in the same session — that creates a silent loopback-only
        // listener that wins over our 0.0.0.0 binding and starves clients
        // on the same box of data.
        private const int DefaultListenPort = 36984;
        // SocketTrader writes the port it expects into this file next to
        // the token (Setup > 9 > 4 / every start); without one, or with a
        // bad value, the default above applies. So a machine whose NT ATI
        // already sits on 36984 moves the bridge without editing this file.
        private const string PortFileName = "SocketTraderBridge.port";
        private int listenPort = DefaultListenPort;
        private static readonly IPAddress BindAddress = IPAddress.Any;

        // Heartbeat snapshot while idle so stale-detection on the client side
        // has something to see. NT may go minutes without events when flat.
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);

        // [FIX-G] Minimum gap between two broadcasts. Events arriving inside
        // the gap collapse into the next snapshot, so a tick storm costs one
        // build per gap instead of one per tick. 40 ms = 25 snapshots/s max.
        private const int MinBroadcastGapMs = 40;

        // Per-write deadline. Windows default TCP keepalive on a half-open
        // connection is ~2h — without this, one dead peer can wedge the
        // whole broadcast thread and starve every live client.
        private const int WriteTimeoutMs = 750;

        // ---------- Authentication ----------
        // This socket streams account balances and accepts a FLATTEN
        // command, so an unauthenticated peer could both read the book and
        // close every position. It must bind beyond loopback (SocketTrader
        // commonly runs under WSL and reaches the host over its NAT
        // subnet), so a network boundary alone cannot be the control.
        //
        // SocketTrader writes a random secret next to NT's user data and
        // sends it as the first line of every connection. No valid token,
        // no snapshot and no commands.
        private const string TokenFileName = "SocketTraderBridge.token";
        // Every legitimate client writes its auth line immediately on
        // connect, so this only needs to cover network latency.
        private const int AuthTimeoutMs = 1000;
        // Re-read the token at most this often when one does not match, so
        // a rotated secret recovers without restarting NinjaTrader.
        private const int TokenRefreshMs = 5000;
        // [FIX-N] A scanner on the LAN must not be able to tie up the pool
        // or flood the Output window.
        private const int MaxPendingHandshakes = 8;
        private const int RejectLogIntervalMs = 10000;
        private volatile string sharedToken;
        private DateTime lastTokenRead = DateTime.MinValue;
        private readonly object tokenLock = new object();
        private int pendingHandshakes;
        private long lastRejectLogTicks;

        private string TokenPath()
        {
            try
            {
                return Path.Combine(NinjaTrader.Core.Globals.UserDataDir, TokenFileName);
            }
            catch { return null; }
        }

        private int ResolveListenPort()
        {
            try
            {
                var path = Path.Combine(NinjaTrader.Core.Globals.UserDataDir, PortFileName);
                if (!File.Exists(path))
                    return DefaultListenPort;
                int p;
                var text = File.ReadAllText(path).Trim();
                if (int.TryParse(text, out p) && p >= 1 && p <= 65535)
                    return p;
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: ignoring {path} ('{text}' is not a port) — using {DefaultListenPort}",
                    PrintTo.OutputTab1);
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: could not read port file: {ex.Message} — using {DefaultListenPort}",
                    PrintTo.OutputTab1);
            }
            return DefaultListenPort;
        }

        private void LoadToken()
        {
            string loaded = null;
            try
            {
                var path = TokenPath();
                if (path != null && File.Exists(path))
                    loaded = File.ReadAllText(path).Trim();
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: could not read token: {ex.Message}",
                    PrintTo.OutputTab1);
            }
            sharedToken = string.IsNullOrEmpty(loaded) ? null : loaded;
            lastTokenRead = DateTime.UtcNow;
        }

        /// <summary>
        /// Re-read the token file when the offered secret does not match the
        /// one held in memory. Without this the token is fixed for the whole
        /// NinjaTrader session, so writing the file — which is exactly what
        /// SocketTrader does on every start — would not take effect until NT
        /// restarted, and the client would loop on rejected connections.
        /// Rate-limited so a bad peer cannot drive constant disk reads.
        /// </summary>
        private void RefreshTokenIfStale()
        {
            lock (tokenLock)
            {
                if ((DateTime.UtcNow - lastTokenRead).TotalMilliseconds < TokenRefreshMs)
                    return;
                LoadToken();
            }
        }

        /// <summary>Length-constant compare so a wrong token leaks no timing.</summary>
        private static bool TokensMatch(string a, string b)
        {
            if (string.IsNullOrEmpty(a) || string.IsNullOrEmpty(b)) return false;
            if (a.Length != b.Length) return false;
            int diff = 0;
            for (int i = 0; i < a.Length; i++) diff |= a[i] ^ b[i];
            return diff == 0;
        }

        /// <summary>
        /// Read one newline-terminated line with a hard deadline, without
        /// consuming anything past it (a StreamReader would buffer ahead
        /// and swallow the first command).
        /// </summary>
        private static string ReadLineWithTimeout(TcpClient client, int timeoutMs)
        {
            var sb = new StringBuilder();
            var stream = client.GetStream();
            client.ReceiveTimeout = timeoutMs;
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            var one = new byte[1];
            while (DateTime.UtcNow < deadline && sb.Length < 512)
            {
                int n;
                try { n = stream.Read(one, 0, 1); }
                catch { return null; }
                if (n <= 0) return null;
                if (one[0] == (byte)'\n') return sb.ToString().Trim();
                sb.Append((char)one[0]);
            }
            return null;
        }

        // ---------- State ----------
        // [FIX-I] A connection plus the lock that serialises every write to
        // it: broadcasts come from the broadcaster thread, acks from the
        // client's own reader thread, and NetworkStream allows one writer.
        private sealed class ClientConn
        {
            public readonly TcpClient Tcp;
            public readonly object WriteLock = new object();
            public ClientConn(TcpClient tcp) { Tcp = tcp; }
        }

        private TcpListener listener;
        private Thread acceptThread;
        private readonly List<ClientConn> clients = new List<ClientConn>();
        private readonly object clientsLock = new object();

        // [FIX-G] Broadcaster: events only signal; the thread builds and
        // writes. [FIX-L] `terminated` is checked by every callback so
        // nothing runs on a torn-down instance after F5.
        private Thread broadcastThread;
        private readonly AutoResetEvent broadcastSignal = new AutoResetEvent(false);
        private volatile bool terminated;

        // Work handed from NinjaTrader's callbacks to the broadcaster thread,
        // so no callback ever takes Account.All / Positions locks while NT
        // may be holding its own (lock-order safety): instruments whose
        // subscription may now be unused ([FIX-C]) and a request to look for
        // new accounts ([FIX-E]).
        private readonly HashSet<Instrument> pendingRelease = new HashSet<Instrument>();
        private readonly object pendingLock = new object();
        private int rehookRequested;

        // Instruments we've subscribed to market data for (with open positions)
        private readonly Dictionary<Instrument, double> lastPrice =
            new Dictionary<Instrument, double>();
        private readonly object subsLock = new object();
        // [FIX-L] Serialises subscribe/unsubscribe decisions. Never taken by
        // OnMarketDataUpdate, so it cannot invert with NT's dispatch lock.
        private readonly object subOpLock = new object();

        // [FIX-E] Accounts whose events we are attached to. += is not
        // idempotent, so hooking is tracked rather than repeated.
        private readonly HashSet<Account> hooked = new HashSet<Account>();
        private readonly object hookedLock = new object();

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
                terminated = false;
                bool listening = StartListener();
                HookAccounts();
                SubscribeExistingPositions();
                broadcastThread = new Thread(BroadcastLoop) { IsBackground = true, Name = "STB-Broadcast" };
                broadcastThread.Start();
                // Say "listening" only when the bind succeeded: after a
                // failed start this line used to follow the failure and
                // read as if the port were open (2026-10-29, port held by
                // NinjaTrader's own ATI).
                NinjaTrader.Code.Output.Process(listening
                    ? $"SocketTraderBridge listening on {BindAddress}:{listenPort}"
                    : $"SocketTraderBridge NOT listening — port {listenPort} is in use " +
                      "(NinjaTrader's AT Interface on the same port?). Change the Live Monitor " +
                      "port in SocketTrader (Setup > 9 > 4) and press F5. Retrying every " +
                      $"{HeartbeatInterval.TotalSeconds:0}s.",
                    PrintTo.OutputTab1);
            }
            else if (State == State.Terminated)
            {
                // [FIX-L] Flag first so callbacks already in flight bail out,
                // then wake the broadcaster so it can exit and be joined.
                terminated = true;
                try { broadcastSignal.Set(); } catch { }
                try
                {
                    if (broadcastThread != null && broadcastThread.IsAlive)
                        broadcastThread.Join(2000);
                }
                catch { }
                UnhookAccounts();
                UnsubscribeAllMarketData();
                StopListener();
            }
        }

        // ---------- TCP ----------
        /// <summary>True when the listener is bound and accepting.</summary>
        private bool StartListener()
        {
            try
            {
                LoadToken();
                if (string.IsNullOrEmpty(sharedToken))
                    NinjaTrader.Code.Output.Process(
                        "SocketTraderBridge: no token file yet — connections will be " +
                        "refused until SocketTrader writes one (Setup > 9). " +
                        $"Expected at {TokenPath()}",
                        PrintTo.OutputTab1);
                listenPort = ResolveListenPort();
                var l = new TcpListener(BindAddress, listenPort);
                l.Start();
                listener = l;
                acceptThread = new Thread(AcceptLoop) { IsBackground = true, Name = "STB-Accept" };
                acceptThread.Start();
                return true;
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: failed to start listener: {ex.Message}",
                    PrintTo.OutputTab1);
                listener = null;
                return false;
            }
        }

        private void StopListener()
        {
            var l = listener;
            listener = null;
            try { l?.Stop(); } catch { }
            lock (clientsLock)
            {
                foreach (var c in clients)
                {
                    try { c.Tcp.Close(); } catch { }
                }
                clients.Clear();
            }
        }

        private void AcceptLoop()
        {
            while (!terminated)
            {
                var l = listener;
                if (l == null) return;
                TcpClient client;
                try { client = l.AcceptTcpClient(); }
                catch (ObjectDisposedException) { return; }
                catch (Exception ex)
                {
                    // [FIX-H] A reset during the handshake (a port scan does
                    // this on Windows) used to end the accept loop for good
                    // while the port stayed bound — every later client then
                    // hung in the backlog. Log it and keep accepting.
                    if (terminated || listener == null) return;
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: accept error (continuing): {ex.Message}",
                        PrintTo.OutputTab1);
                    Thread.Sleep(200);
                    continue;
                }

                // Hand the client off immediately. Authentication, the
                // initial snapshot build and its write all take real time
                // (up to AuthTimeoutMs + WriteTimeoutMs each), and doing
                // them inline made this single thread the bottleneck: one
                // peer that connects and stays silent would stall every
                // other connection, including the trading client's, until
                // it timed out. A worker per client keeps accept free.
                // [FIX-N] Dedicated thread (not the pool, which NT also
                // uses), and a cap on how many may sit unauthenticated.
                var c = client;
                if (Interlocked.Increment(ref pendingHandshakes) > MaxPendingHandshakes)
                {
                    Interlocked.Decrement(ref pendingHandshakes);
                    LogRejectRateLimited("too many pending connections");
                    try { c.Close(); } catch { }
                    continue;
                }
                try
                {
                    var t = new Thread(() => HandleNewClient(c)) { IsBackground = true, Name = "STB-Handshake" };
                    t.Start();
                }
                catch (Exception ex)
                {
                    Interlocked.Decrement(ref pendingHandshakes);
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: could not start client worker: {ex.Message}",
                        PrintTo.OutputTab1);
                    try { c.Close(); } catch { }
                }
            }
        }

        // [FIX-N] One line per RejectLogIntervalMs, whatever the LAN does.
        private void LogRejectRateLimited(string why)
        {
            long now = DateTime.UtcNow.Ticks;
            long last = Interlocked.Read(ref lastRejectLogTicks);
            if ((now - last) < RejectLogIntervalMs * TimeSpan.TicksPerMillisecond) return;
            if (Interlocked.CompareExchange(ref lastRejectLogTicks, now, last) != last) return;
            NinjaTrader.Code.Output.Process(
                $"SocketTraderBridge: refusing connection — {why} (further refusals muted for {RejectLogIntervalMs / 1000}s)",
                PrintTo.OutputTab1);
        }

        /// <summary>
        /// Authenticate one client, register it, then serve its commands.
        /// Its first snapshot is produced by the broadcaster right after
        /// registration ([FIX-B]), so there is no window in which a fill can
        /// slip between "snapshot built" and "client registered".
        /// Wrapped so an unexpected throw kills only this connection.
        /// </summary>
        private void HandleNewClient(TcpClient client)
        {
            bool registered = false;
            try
            {
                // Configure the socket: disable Nagle so the first snapshot
                // reaches the client immediately, and cap write time so a
                // half-open peer can't wedge the broadcaster on the next
                // broadcast.
                try { client.NoDelay = true; } catch { }
                try { client.SendTimeout = WriteTimeoutMs; } catch { }
                try { client.ReceiveTimeout = WriteTimeoutMs; } catch { }

                // AUTHENTICATE BEFORE ANYTHING IS SENT. The snapshot carries
                // account balances and open positions, so it must not go out
                // to an unverified peer — the client speaks first here.
                //
                // Started before the token file existed (fresh machine: the
                // AddOn is compiled first, SocketTrader writes the token when
                // the live monitor is enabled)? Look again now, rate-limited
                // like a mismatch — otherwise every connection is refused
                // until the next compile or NinjaTrader restart.
                if (string.IsNullOrEmpty(sharedToken))
                    RefreshTokenIfStale();
                if (string.IsNullOrEmpty(sharedToken))
                {
                    LogRejectRateLimited("no token file. Enable the live monitor in SocketTrader (Setup > 9) to create it");
                    try { client.Close(); } catch { }
                    return;
                }
                var hello = ReadLineWithTimeout(client, AuthTimeoutMs);
                var offered = ExtractJsonString(hello ?? "", "auth");
                if (!TokensMatch(offered, sharedToken))
                {
                    RefreshTokenIfStale();   // secret may have been rotated
                }
                if (!TokensMatch(offered, sharedToken))
                {
                    string peer = "?";
                    try { peer = client.Client.RemoteEndPoint.ToString(); } catch { }
                    LogRejectRateLimited($"unauthenticated client {peer}");
                    try { client.Close(); } catch { }
                    return;
                }
                if (terminated) { try { client.Close(); } catch { } return; }

                // [FIX-B] Register first, then ask the broadcaster for a
                // snapshot: the new client's first line is built after it is
                // in the list, and every later event lands after that line.
                var conn = new ClientConn(client);
                lock (clientsLock) { clients.Add(conn); }
                registered = true;
                RequestBroadcast();

                // Per-client reader thread: listens for newline-delimited
                // JSON commands from the client and dispatches them.
                // Commands let SocketTrader flatten positions via NT's
                // own account.Flatten(...) API, bypassing the file-based
                // ATI and its contract-name-format fragility.
                var readerThread = new Thread(() => ClientReadLoop(conn))
                {
                    IsBackground = true,
                    Name = "STB-ClientRead"
                };
                readerThread.Start();
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: client handler error: {ex.Message}",
                    PrintTo.OutputTab1);
                if (!registered) { try { client.Close(); } catch { } }
            }
            finally
            {
                Interlocked.Decrement(ref pendingHandshakes);
            }
        }

        private void ClientReadLoop(ClientConn conn)
        {
            var client = conn.Tcp;
            try
            {
                // Commands are expected to be infrequent; a larger read
                // timeout here lets the client connection stay open while
                // the broadcaster continues pushing data from the writer side.
                client.ReceiveTimeout = 0;  // block indefinitely on read
                var stream = client.GetStream();
                var reader = new StreamReader(stream, Encoding.UTF8);
                string line;
                while (!terminated && (line = reader.ReadLine()) != null)
                {
                    line = line.Trim();
                    if (line.Length == 0) continue;
                    // [FIX-O] A failing command must never end the reader
                    // loop — that left the client registered but deaf.
                    try { HandleCommand(line, conn); }
                    catch (Exception ex)
                    {
                        SendAck(conn, false, ex.Message);
                        NinjaTrader.Code.Output.Process(
                            $"SocketTraderBridge: command error: {ex.Message}",
                            PrintTo.OutputTab1);
                    }
                }
            }
            catch (Exception)
            {
                // connection dropped
            }
            // [FIX-O] The connection is gone (or unreadable): evict it now
            // rather than waiting for a broadcast write to fail.
            RemoveClient(conn);
        }

        private void RemoveClient(ClientConn conn)
        {
            lock (clientsLock) { clients.Remove(conn); }
            try { conn.Tcp.Close(); } catch { }
        }

        private void HandleCommand(string jsonLine, ClientConn conn)
        {
            // Tiny JSON parser — we only accept three shapes today:
            //   {"cmd":"flatten","account":"<name>"}
            //   {"cmd":"close_position","account":"<name>","instrument":"<inst>"}
            //   {"cmd":"front_months","roots":"ES,NQ,SIL,..."}
            // Using a string-contains approach keeps this dependency-free
            // and safe when the rest of NT is running on .NET Framework 4.x
            // without Newtonsoft or System.Text.Json guaranteed available.
            string cmd = ExtractJsonString(jsonLine, "cmd");
            if (cmd == "front_months")
            {
                // Account-independent: resolve each requested root against
                // NT's instrument DB and reply with the contract NT itself
                // considers current under its rollover schedule.
                SendFrontMonths(conn, ExtractJsonString(jsonLine, "roots"));
                return;
            }
            string accountName = ExtractJsonString(jsonLine, "account");
            if (string.IsNullOrEmpty(cmd) || string.IsNullOrEmpty(accountName))
            {
                SendAck(conn, false, "missing cmd or account");
                return;
            }
            Account target = null;
            lock (Account.All)
            {
                foreach (var a in Account.All)
                {
                    if (a.Name == accountName) { target = a; break; }
                }
            }
            if (target == null)
            {
                SendAck(conn, false, "account not found: " + accountName);
                return;
            }
            try
            {
                if (cmd == "flatten")
                {
                    int n = FlattenAccount(target);
                    // Both acks mean "flatten submitted", not "flat":
                    // Account.Flatten is asynchronous. The client confirms
                    // against the position book.
                    SendAck(conn, true, n > 0 ? $"flatten submitted for {n} position(s) on {accountName}"
                                              : $"nothing to flatten on {accountName}");
                }
                else if (cmd == "close_position")
                {
                    string inst = ExtractJsonString(jsonLine, "instrument");
                    if (string.IsNullOrEmpty(inst))
                    {
                        SendAck(conn, false, "close_position missing instrument");
                        return;
                    }
                    // [FIX-D] Exact match only, and an honest ack.
                    string closed = ClosePositionOnInstrument(target, inst);
                    if (closed != null)
                        SendAck(conn, true, "close submitted for " + closed + " on " + accountName);
                    else
                        SendAck(conn, false, "no open position matching " + inst + " on " + accountName);
                }
                else
                {
                    SendAck(conn, false, "unknown cmd: " + cmd);
                }
            }
            catch (Exception ex)
            {
                SendAck(conn, false, ex.Message);
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: command '{cmd}' error: {ex.Message}",
                    PrintTo.OutputTab1);
            }
        }

        /// <summary>Submits a flatten for every open position; returns how many.</summary>
        private int FlattenAccount(Account acct)
        {
            // Build the set of open-position instruments, then let NT flatten
            // each one via its own account.Flatten() API — handles ATM stop
            // cancellation and market-order exit in one call, no contract-name
            // format fragility, no file-based race.
            var toFlatten = new List<Instrument>();
            lock (acct.Positions)   // [FIX-A]
            {
                foreach (var pos in acct.Positions)
                {
                    if (pos == null || pos.Instrument == null) continue;
                    if (pos.MarketPosition == MarketPosition.Flat) continue;
                    toFlatten.Add(pos.Instrument);
                }
            }
            if (toFlatten.Count == 0) return 0;
            acct.Flatten(toFlatten.ToArray());
            NinjaTrader.Code.Output.Process(
                $"SocketTraderBridge: flattened {toFlatten.Count} position(s) on {acct.Name}",
                PrintTo.OutputTab1);
            return toFlatten.Count;
        }

        /// <summary>
        /// [FIX-D] Closes the one open position whose contract IS the
        /// requested one. Both spellings NinjaTrader uses are accepted —
        /// "NQ 12-26" (Instrument.FullName) and "NQ DEC26" (the ATI dump) —
        /// and a bare root ("NQ") closes that root's position. Nothing is
        /// matched by substring any more: "MES 12-26" is not "ES 12-26".
        /// Returns the FullName closed, or null when nothing matched.
        /// </summary>
        private string ClosePositionOnInstrument(Account acct, string instName)
        {
            string want = NormalizeContract(instName);
            Position hit = null;
            lock (acct.Positions)   // [FIX-A]
            {
                foreach (var pos in acct.Positions)
                {
                    if (pos == null || pos.Instrument == null) continue;
                    if (pos.MarketPosition == MarketPosition.Flat) continue;
                    var full = pos.Instrument.FullName ?? "";
                    var master = pos.Instrument.MasterInstrument != null
                        ? (pos.Instrument.MasterInstrument.Name ?? "")
                        : "";
                    if (NormalizeContract(full).Equals(want, StringComparison.OrdinalIgnoreCase)
                        || (master.Length > 0 && master.Equals(instName.Trim(), StringComparison.OrdinalIgnoreCase)))
                    {
                        hit = pos;
                        break;
                    }
                }
            }
            if (hit == null) return null;
            acct.Flatten(new[] { hit.Instrument });
            NinjaTrader.Code.Output.Process(
                $"SocketTraderBridge: closed {hit.Instrument.FullName} on {acct.Name}",
                PrintTo.OutputTab1);
            return hit.Instrument.FullName;
        }

        private static readonly string[] MonthCodes =
            { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };

        /// <summary>
        /// "NQ 12-26", "NQ DEC26", "nq dec-26", "NQ  12-2026" → "NQ 12-26".
        /// Anything it cannot parse is returned trimmed and upper-cased, so
        /// an exotic name still compares exactly against itself.
        /// </summary>
        internal static string NormalizeContract(string s)
        {
            if (s == null) return "";
            s = s.Trim().ToUpperInvariant();
            int sp = s.IndexOf(' ');
            if (sp <= 0) return s;
            string root = s.Substring(0, sp).Trim();
            string tail = s.Substring(sp + 1).Trim().Replace("-", "").Replace(" ", "");
            int month = 0;
            string yy = null;
            if (tail.Length >= 5 && tail.Length <= 7)
            {
                // MMMYY / MMMYYYY
                string mon = tail.Substring(0, 3);
                int idx = Array.IndexOf(MonthCodes, mon);
                if (idx >= 0)
                {
                    month = idx + 1;
                    yy = tail.Substring(3);
                }
            }
            if (month == 0 && tail.Length >= 4 && tail.Length <= 6)
            {
                // MMYY / MMYYYY
                int m;
                if (int.TryParse(tail.Substring(0, 2), NumberStyles.None, CultureInfo.InvariantCulture, out m)
                    && m >= 1 && m <= 12)
                {
                    month = m;
                    yy = tail.Substring(2);
                }
            }
            if (month == 0 || yy == null) return s;
            if (yy.Length == 4) yy = yy.Substring(2);
            int y;
            if (yy.Length != 2 || !int.TryParse(yy, NumberStyles.None, CultureInfo.InvariantCulture, out y))
                return s;
            return root + " " + month.ToString("00", CultureInfo.InvariantCulture) + "-" + yy;
        }

        private void SendFrontMonths(ClientConn conn, string rootsCsv)
        {
            // Reply: {"ack":true,"msg":"front_months","months":{"SI":"12-26",...}}
            // Instrument.GetInstrument on a bare root ("SI") resolves the
            // contract NT's rollover schedule makes current, and FullName
            // already carries it in the OIF "ROOT MM-YY" form the client
            // wants — no date math on this side. Unknown roots are simply
            // omitted so the client's calendar fallback covers them.
            var sb = new StringBuilder();
            sb.Append("{\"ack\":true,\"msg\":\"front_months\",\"months\":{");
            bool first = true;
            if (!string.IsNullOrEmpty(rootsCsv))
            {
                var seen = new HashSet<string>();
                foreach (var raw in rootsCsv.Split(','))
                {
                    if (seen.Count >= 64) break;   // bound a hostile request
                    var root = raw.Trim().ToUpperInvariant();
                    if (root.Length == 0 || root.Length > 6 || !seen.Add(root))
                        continue;
                    string tail = null;
                    try
                    {
                        var inst = Instrument.GetInstrument(root);
                        var full = inst != null ? inst.FullName : null;   // "SI 12-26"
                        if (full != null
                            && full.StartsWith(root + " ", StringComparison.OrdinalIgnoreCase))
                            tail = full.Substring(root.Length + 1).Trim();
                    }
                    catch { /* not in the instrument DB — skip */ }
                    if (string.IsNullOrEmpty(tail)) continue;
                    if (!first) sb.Append(",");
                    first = false;
                    sb.Append(JsonString(root)).Append(":").Append(JsonString(tail));
                }
            }
            sb.Append("}}\n");
            WriteToClient(conn, sb.ToString());
        }

        private void SendAck(ClientConn conn, bool ok, string message)
        {
            var sb = new StringBuilder();
            sb.Append("{\"ack\":");
            sb.Append(ok ? "true" : "false");
            sb.Append(",\"msg\":").Append(JsonString(message));
            sb.Append("}\n");
            WriteToClient(conn, sb.ToString());
        }

        /// <summary>[FIX-I] The only place bytes go to a client socket.</summary>
        private static bool WriteToClient(ClientConn conn, string text)
        {
            try
            {
                var bytes = Encoding.UTF8.GetBytes(text);
                lock (conn.WriteLock)
                {
                    var stream = conn.Tcp.GetStream();
                    stream.WriteTimeout = WriteTimeoutMs;
                    stream.Write(bytes, 0, bytes.Length);
                }
                return true;
            }
            catch { return false; }   // caller evicts on false
        }

        private static string ExtractJsonString(string json, string key)
        {
            // Locate "<key>":"<value>" and unescape the value.
            // Deliberately primitive — avoids a JSON dep and is sufficient for
            // the fixed-shape command messages this AddOn accepts.
            var needle = "\"" + key + "\"";
            int k = json.IndexOf(needle, StringComparison.Ordinal);
            if (k < 0) return null;
            int colon = json.IndexOf(':', k + needle.Length);
            if (colon < 0) return null;
            int i = colon + 1;
            while (i < json.Length && char.IsWhiteSpace(json[i])) i++;
            if (i >= json.Length || json[i] != '"') return null;
            i++;
            var val = new StringBuilder();
            while (i < json.Length)
            {
                char c = json[i];
                if (c == '\\' && i + 1 < json.Length)
                {
                    char n = json[i + 1];
                    if (n == '"' || n == '\\' || n == '/') { val.Append(n); i += 2; continue; }
                    if (n == 'n') { val.Append('\n'); i += 2; continue; }
                    if (n == 't') { val.Append('\t'); i += 2; continue; }
                    if (n == 'r') { val.Append('\r'); i += 2; continue; }
                    if (n == 'b') { val.Append('\b'); i += 2; continue; }
                    if (n == 'f') { val.Append('\f'); i += 2; continue; }
                    // [FIX-M] \uXXXX — Python's json.dumps escapes every
                    // non-ASCII character this way by default, so an
                    // account name with an accent never matched before.
                    if (n == 'u' && i + 5 < json.Length)
                    {
                        int code;
                        if (int.TryParse(json.Substring(i + 2, 4), NumberStyles.HexNumber,
                                         CultureInfo.InvariantCulture, out code))
                        {
                            val.Append((char)code); i += 6; continue;
                        }
                    }
                }
                if (c == '"') return val.ToString();
                val.Append(c);
                i++;
            }
            return null;
        }

        // ---------- Broadcasting ----------
        /// <summary>
        /// [FIX-G] Called from NT's event/market-data threads: cheap, never
        /// blocks, never touches a socket.
        /// </summary>
        private void RequestBroadcast()
        {
            if (terminated) return;
            try { broadcastSignal.Set(); } catch { }
        }

        /// <summary>Deferred housekeeping, run on the broadcaster thread only.</summary>
        private void RunDeferredWork()
        {
            if (Interlocked.Exchange(ref rehookRequested, 0) == 1)
            {
                try { HookAccounts(); }
                catch (Exception ex)
                {
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: hook error: {ex.Message}", PrintTo.OutputTab1);
                }
            }
            Instrument[] maybeFree = null;
            lock (pendingLock)
            {
                if (pendingRelease.Count > 0)
                {
                    maybeFree = new Instrument[pendingRelease.Count];
                    pendingRelease.CopyTo(maybeFree);
                    pendingRelease.Clear();
                }
            }
            if (maybeFree == null) return;
            foreach (var inst in maybeFree)
            {
                try { ReleaseMarketDataIfUnused(inst); }
                catch (Exception ex)
                {
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: release error: {ex.Message}", PrintTo.OutputTab1);
                }
            }
        }

        /// <summary>
        /// [FIX-B][FIX-G] The single producer of snapshot lines. Builds and
        /// writes under one thread, so the order clients see is the order
        /// snapshots were built; coalesces bursts; doubles as the heartbeat;
        /// retries a listener that failed to bind ([FIX-H]).
        /// </summary>
        private void BroadcastLoop()
        {
            while (!terminated)
            {
                try
                {
                    bool signalled = broadcastSignal.WaitOne(HeartbeatInterval);
                    if (terminated) return;
                    if (signalled) Thread.Sleep(MinBroadcastGapMs);   // let a burst settle into one build

                    RunDeferredWork();

                    if (listener == null && !terminated)
                    {
                        if (StartListener())
                            NinjaTrader.Code.Output.Process(
                                $"SocketTraderBridge listening on {BindAddress}:{listenPort} (retry succeeded)",
                                PrintTo.OutputTab1);
                    }

                    ClientConn[] targets;
                    lock (clientsLock) { targets = clients.ToArray(); }
                    if (targets.Length == 0) continue;   // nobody listening: no build

                    string json;
                    try { json = BuildSnapshotJson(); }
                    catch (Exception ex)
                    {
                        // [FIX-A] A snapshot that could not be read is NOT
                        // sent. The next event or the next heartbeat (≤5 s)
                        // produces a complete one; a client that wants to
                        // act on the book must wait for a line it can trust.
                        NinjaTrader.Code.Output.Process(
                            $"SocketTraderBridge: snapshot skipped: {ex.Message}", PrintTo.OutputTab1);
                        continue;
                    }
                    var line = json + "\n";
                    foreach (var c in targets)
                    {
                        if (!WriteToClient(c, line)) RemoveClient(c);
                    }
                }
                catch (Exception ex)
                {
                    if (terminated) return;
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: broadcaster error: {ex.Message}", PrintTo.OutputTab1);
                    Thread.Sleep(200);
                }
            }
        }

        // ---------- Account / position hooks ----------
        // [FIX-E] Attach to every account not yet hooked. Runs at start-up
        // and on every AccountStatusUpdate, so accounts created by a
        // connection that came up later are picked up.
        private void HookAccounts()
        {
            var fresh = new List<Account>();
            lock (Account.All)
            {
                foreach (var acct in Account.All)
                {
                    if (acct == null) continue;
                    lock (hookedLock)
                    {
                        if (!hooked.Add(acct)) continue;
                    }
                    acct.AccountItemUpdate += OnAccountItemUpdate;
                    acct.PositionUpdate += OnPositionUpdate;
                    fresh.Add(acct);
                }
            }
            foreach (var acct in fresh)
            {
                try { SubscribeAccountPositions(acct); } catch { }
            }
            // Watch for accounts added/removed after startup (attached once).
            lock (hookedLock)
            {
                if (!statusHooked)
                {
                    Account.AccountStatusUpdate += OnAccountStatusUpdate;
                    statusHooked = true;
                }
            }
        }
        private bool statusHooked;

        private void UnhookAccounts()
        {
            lock (hookedLock)
            {
                if (statusHooked)
                {
                    try { Account.AccountStatusUpdate -= OnAccountStatusUpdate; } catch { }
                    statusHooked = false;
                }
                foreach (var acct in hooked)
                {
                    try
                    {
                        acct.AccountItemUpdate -= OnAccountItemUpdate;
                        acct.PositionUpdate -= OnPositionUpdate;
                    }
                    catch { }
                }
                hooked.Clear();
            }
        }

        private void OnAccountItemUpdate(object sender, AccountItemEventArgs e)
        {
            if (terminated) return;
            // Fires on CashValue / RealizedPnL / BuyingPower / … changes.
            RequestBroadcast();
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            if (terminated) return;
            // Fires on any position change — open, size change, close.
            // [FIX-P] A removal is a close even if the Position object
            // still reads non-flat at this instant.
            bool closed = e.Operation == Operation.Remove
                || (e.Position != null && e.Position.MarketPosition == MarketPosition.Flat);
            if (closed)
            {
                // [FIX-C] Decide on the broadcaster thread whether anyone
                // still holds it — never take account locks in NT's callback.
                var inst = e.Position != null ? e.Position.Instrument : null;
                if (inst != null) lock (pendingLock) { pendingRelease.Add(inst); }
            }
            else
            {
                EnsureMarketDataSubscription(e.Position);
            }
            RequestBroadcast();
        }

        private void OnAccountStatusUpdate(object sender, AccountStatusEventArgs e)
        {
            if (terminated) return;
            // [FIX-E] A connection coming up creates accounts after start-up:
            // have the broadcaster hook whatever is new, then rebroadcast
            // (the status itself changed too).
            Interlocked.Exchange(ref rehookRequested, 1);
            RequestBroadcast();
        }

        // ---------- Market data ----------
        private void SubscribeExistingPositions()
        {
            Account[] all;
            lock (Account.All)
            {
                all = new Account[Account.All.Count];
                Account.All.CopyTo(all, 0);
            }
            foreach (var acct in all)
            {
                try { SubscribeAccountPositions(acct); } catch { }
            }
        }

        private void SubscribeAccountPositions(Account acct)
        {
            if (acct == null) return;
            Position[] snapshot;
            lock (acct.Positions)   // [FIX-A]
            {
                snapshot = new Position[acct.Positions.Count];
                acct.Positions.CopyTo(snapshot, 0);
            }
            foreach (var pos in snapshot)
            {
                if (pos != null && pos.MarketPosition != MarketPosition.Flat)
                    EnsureMarketDataSubscription(pos);
            }
        }

        private void EnsureMarketDataSubscription(Position pos)
        {
            if (pos == null || pos.Instrument == null || terminated) return;
            var inst = pos.Instrument;
            if (pos.MarketPosition == MarketPosition.Flat)
            {
                lock (pendingLock) { pendingRelease.Add(inst); }
                RequestBroadcast();
                return;
            }

            lock (subOpLock)   // [FIX-L]
            {
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
                    lock (subsLock) { lastPrice.Remove(inst); }
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: market data subscribe failed for " +
                        $"{inst.FullName}: {ex.Message}", PrintTo.OutputTab1);
                    return;
                }
                // [FIX-J] Seed from the last trade NT already knows so the
                // position does not read as zero exposure until the next
                // tick (minutes, overnight in a micro).
                try
                {
                    var last = inst.MarketData != null ? inst.MarketData.Last : null;
                    if (last != null && last.Price > 0)
                        lock (subsLock) { if (lastPrice.ContainsKey(inst)) lastPrice[inst] = last.Price; }
                }
                catch { }
            }
        }

        /// <summary>
        /// [FIX-C] Drop the subscription only when no account — not just
        /// the one that closed — still holds the instrument.
        /// </summary>
        private void ReleaseMarketDataIfUnused(Instrument inst)
        {
            if (inst == null) return;
            if (AnyOpenPositionOn(inst)) return;
            UnsubscribeMarketData(inst);
        }

        private static bool AnyOpenPositionOn(Instrument inst)
        {
            Account[] all;
            lock (Account.All)
            {
                all = new Account[Account.All.Count];
                Account.All.CopyTo(all, 0);
            }
            foreach (var a in all)
            {
                if (a == null) continue;
                lock (a.Positions)   // [FIX-A]
                {
                    foreach (var p in a.Positions)
                    {
                        if (p != null && p.Instrument == inst && p.MarketPosition != MarketPosition.Flat)
                            return true;
                    }
                }
            }
            return false;
        }

        private void UnsubscribeMarketData(Instrument inst)
        {
            if (inst == null) return;
            lock (subOpLock)   // [FIX-L]
            {
                lock (subsLock)
                {
                    if (!lastPrice.ContainsKey(inst)) return;
                    lastPrice.Remove(inst);
                }
                try { inst.MarketData.Update -= OnMarketDataUpdate; } catch { }
            }
        }

        private void UnsubscribeAllMarketData()
        {
            lock (subOpLock)
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
        }

        private void OnMarketDataUpdate(object sender, MarketDataEventArgs e)
        {
            if (terminated) return;
            if (e.MarketDataType != MarketDataType.Last) return;
            var inst = e.Instrument;
            if (inst == null) return;

            lock (subsLock)
            {
                if (!lastPrice.ContainsKey(inst)) return;
                lastPrice[inst] = e.Price;
            }
            RequestBroadcast();   // [FIX-G] no build, no socket, on NT's thread
        }

        // ---------- JSON ----------
        private string BuildSnapshotJson()
        {
            var sb = new StringBuilder(512);
            double ts = (DateTime.UtcNow - new DateTime(1970, 1, 1,
                0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
            sb.Append("{\"t\":").Append(ts.ToString("F3", CultureInfo.InvariantCulture));

            // [FIX-A] Copy the collection under NT's own lock. If that still
            // throws, the exception propagates and the broadcast is skipped —
            // an empty list here used to be published as "every account flat".
            Account[] accountsSnapshot;
            lock (Account.All)
            {
                accountsSnapshot = new Account[Account.All.Count];
                Account.All.CopyTo(accountsSnapshot, 0);
            }

            bool firstAcct = true, partial = false;
            var body = new StringBuilder(256 * Math.Max(1, accountsSnapshot.Length));
            foreach (var acct in accountsSnapshot)
            {
                string accountFragment;
                try { accountFragment = BuildAccountFragment(acct); }
                catch (Exception ex)
                {
                    // One rogue account must not kill the whole response —
                    // but the line must say that it is incomplete.
                    partial = true;
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: skipping account " +
                        $"'{(acct == null ? "<null>" : acct.Name)}': {ex.Message}",
                        PrintTo.OutputTab1);
                    continue;
                }
                if (accountFragment == null) continue;
                if (!firstAcct) body.Append(',');
                firstAcct = false;
                body.Append(accountFragment);
            }
            if (partial) sb.Append(",\"partial\":true");
            sb.Append(",\"accounts\":[").Append(body).Append("]}");
            return sb.ToString();
        }

        private string BuildAccountFragment(Account acct)
        {
            if (acct == null) return null;
            var sb = new StringBuilder(256);

            // [FIX-Q] The account's own currency, not a USD assumption.
            Currency denom = Currency.UsDollar;
            try { denom = acct.Denomination; } catch { }
            double cash = 0, realized = 0;
            try { cash = acct.Get(AccountItem.CashValue, denom); } catch { }
            try { realized = acct.Get(AccountItem.RealizedProfitLoss, denom); } catch { }

            // [FIX-F] Connection status, so a frozen book from a dropped
            // connection is recognisable as such.
            string status = "";
            try
            {
                var conn = acct.Connection;
                status = conn != null ? conn.Status.ToString() : "Disconnected";
            }
            catch { status = ""; }

            sb.Append("{\"name\":").Append(JsonString(acct.Name ?? ""));
            sb.Append(",\"status\":").Append(JsonString(status));
            sb.Append(",\"cash\":").Append(D(cash));
            sb.Append(",\"realized\":").Append(D(realized));

            double unrealizedSum = 0;
            sb.Append(",\"positions\":[");

            // [FIX-A] Under NT's lock; a failure here propagates (the
            // account is then reported missing and the line marked partial)
            // instead of being written out as "no positions".
            Position[] posSnapshot;
            lock (acct.Positions)
            {
                posSnapshot = new Position[acct.Positions.Count];
                acct.Positions.CopyTo(posSnapshot, 0);
            }

            bool firstPos = true;
            foreach (var pos in posSnapshot)
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
                sb.Append(",\"qty\":").Append(qty.ToString(CultureInfo.InvariantCulture));   // [FIX-K]
                sb.Append(",\"avg\":").Append(D(avg));
                sb.Append(",\"last\":").Append(D(last));
                sb.Append(",\"pl\":").Append(D(pl));
                sb.Append('}');
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
                        if (c < 0x20) sb.AppendFormat(CultureInfo.InvariantCulture, "\\u{0:x4}", (int)c);
                        else sb.Append(c);
                        break;
                }
            }
            sb.Append('"');
            return sb.ToString();
        }
    }
}
