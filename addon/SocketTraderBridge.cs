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
//   - Multiple clients supported (list of TcpClient; one sender thread
//     writes to all of them — NinjaTrader's own threads never block on a
//     socket).
//   - Binds 127.0.0.1 unless SocketTraderBridge.bind (next to the token)
//     says "any": SocketTrader writes "any" when it runs under WSL or
//     reaches NinjaTrader over the LAN, "loopback" otherwise.
//   - Default port 36984 to sit alongside NT's built-in ATI on 36973
//     (SocketTraderBridge.port next to the token overrides it)
//     without colliding with VSCode's auto-port-forwarder (which claims
//     36974 on loopback if your terminal session is using it).

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
        // Loopback unless SocketTraderBridge.bind says "any". This socket
        // streams balances and accepts a FLATTEN, so it is not offered to
        // the LAN by default; SocketTrader under WSL (classic NAT reaches
        // the host over the vEthernet subnet) or on another machine writes
        // "any" and the Windows Firewall rule for NinjaTrader is then the
        // boundary, as before.
        private const string BindFileName = "SocketTraderBridge.bind";
        private IPAddress bindAddress = IPAddress.Loopback;

        // Heartbeat snapshot while idle so stale-detection on the client side
        // has something to see. NT may go minutes without events when flat.
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);

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
        private volatile string sharedToken;
        private DateTime lastTokenRead = DateTime.MinValue;
        private readonly object tokenLock = new object();

        private string TokenPath()
        {
            try
            {
                return Path.Combine(NinjaTrader.Core.Globals.UserDataDir, TokenFileName);
            }
            catch { return null; }
        }

        private IPAddress ResolveBindAddress()
        {
            try
            {
                var path = Path.Combine(NinjaTrader.Core.Globals.UserDataDir, BindFileName);
                if (!File.Exists(path))
                    return IPAddress.Loopback;
                var text = File.ReadAllText(path).Trim().ToLowerInvariant();
                if (text == "any" || text == "all" || text == "0.0.0.0")
                    return IPAddress.Any;
                if (text == "loopback" || text == "local" || text == "127.0.0.1")
                    return IPAddress.Loopback;
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: ignoring {path} ('{text}' is not 'any' or 'loopback') — binding 127.0.0.1",
                    PrintTo.OutputTab1);
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: could not read bind file: {ex.Message} — binding 127.0.0.1",
                    PrintTo.OutputTab1);
            }
            return IPAddress.Loopback;
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

        // ---------- Failed-auth backoff ----------
        // A peer that keeps offering wrong tokens is refused on sight for a
        // while, and written to the Output tab once a minute instead of once
        // per attempt: the 256-bit token is not guessable, but a scanner or
        // a misconfigured client (2026-10-29: NinjaTrader's own ATI polling
        // this port) could otherwise fill the tab and cost a thread per try.
        private const int AuthFailuresBeforeBackoff = 3;
        private static readonly TimeSpan AuthFailureWindow = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan AuthBackoff = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan AuthLogEvery = TimeSpan.FromSeconds(60);
        private sealed class AuthFailures { public int Count; public DateTime First; public DateTime Until; public DateTime LastLogged; }
        private readonly Dictionary<string, AuthFailures> authFailures = new Dictionary<string, AuthFailures>();
        private readonly object authLock = new object();

        private static string PeerAddress(TcpClient client)
        {
            try
            {
                var ep = client.Client.RemoteEndPoint as IPEndPoint;
                return ep != null ? ep.Address.ToString() : "?";
            }
            catch { return "?"; }
        }

        /// <summary>True when this peer is inside its backoff window.</summary>
        private bool AuthBackedOff(string peer)
        {
            lock (authLock)
            {
                AuthFailures f;
                if (!authFailures.TryGetValue(peer, out f)) return false;
                if (DateTime.UtcNow < f.Until) return true;
                if (DateTime.UtcNow - f.First > AuthFailureWindow) authFailures.Remove(peer);
                return false;
            }
        }

        /// <summary>Record a failed auth; returns true when it should be logged.</summary>
        private bool NoteAuthFailure(string peer)
        {
            lock (authLock)
            {
                AuthFailures f;
                var now = DateTime.UtcNow;
                if (!authFailures.TryGetValue(peer, out f) || now - f.First > AuthFailureWindow)
                {
                    f = new AuthFailures { Count = 0, First = now, Until = DateTime.MinValue, LastLogged = DateTime.MinValue };
                    authFailures[peer] = f;
                }
                f.Count++;
                if (f.Count >= AuthFailuresBeforeBackoff)
                    f.Until = now + AuthBackoff;
                if (authFailures.Count > 256)      // bound the table under a scan
                    authFailures.Clear();
                if (now - f.LastLogged < AuthLogEvery) return false;
                f.LastLogged = now;
                return true;
            }
        }

        // A command line longer than this is not a command. The reader stops
        // feeding a StringBuilder past it and drops the connection, so a peer
        // cannot grow memory by never sending a newline.
        private const int MaxCommandLineBytes = 4096;

        /// <summary>
        /// Read one newline-terminated command line, byte by byte (nothing
        /// buffered past the newline, nothing kept past MaxCommandLineBytes).
        /// Returns null when the peer closed, timed out, or overran the cap.
        /// </summary>
        private static string ReadCommandLine(NetworkStream stream)
        {
            var buf = new List<byte>(128);
            var one = new byte[1];
            while (true)
            {
                int n;
                try { n = stream.Read(one, 0, 1); }
                catch { return null; }
                if (n <= 0) return null;
                if (one[0] == (byte)'\n')
                    return Encoding.UTF8.GetString(buf.ToArray()).Trim();
                if (buf.Count >= MaxCommandLineBytes) return null;
                buf.Add(one[0]);
            }
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
        private TcpListener listener;
        private Thread acceptThread;
        private Timer heartbeatTimer;
        private readonly List<TcpClient> clients = new List<TcpClient>();
        private readonly object clientsLock = new object();
        // Broadcasts happen on this thread, never on the NinjaTrader thread
        // that raised the account, position or tick event: a socket write
        // can block for WriteTimeoutMs per client, and before 1.0.4 that
        // happened on NT's own event threads under clientsLock. Events only
        // set the signal; a burst of ticks collapses into one snapshot.
        private Thread senderThread;
        private readonly AutoResetEvent broadcastSignal = new AutoResetEvent(false);
        private volatile bool senderRunning;

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
                bool listening = StartListener();
                HookAccounts();
                SubscribeExistingPositions();
                heartbeatTimer = new Timer(_ => SafeBroadcast(), null,
                    HeartbeatInterval, HeartbeatInterval);
                // Say "listening" only when the bind succeeded: after a
                // failed start this line used to follow the failure and
                // read as if the port were open (2026-10-29, port held by
                // NinjaTrader's own ATI).
                NinjaTrader.Code.Output.Process(listening
                    ? $"SocketTraderBridge listening on {bindAddress}:{listenPort}" +
                      (bindAddress.Equals(IPAddress.Loopback)
                          ? " (this machine only; SocketTraderBridge.bind = any opens it to the network)"
                          : " (all interfaces, per SocketTraderBridge.bind)")
                    : $"SocketTraderBridge NOT listening — port {listenPort} is in use " +
                      "(NinjaTrader's AT Interface on the same port?). Change the Live Monitor " +
                      "port in SocketTrader (Setup > 9 > 4) and press F5.",
                    PrintTo.OutputTab1);
            }
            else if (State == State.Terminated)
            {
                heartbeatTimer?.Dispose();
                UnhookAccounts();
                UnsubscribeAllMarketData();
                StopListener();
                StopSender();
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
                bindAddress = ResolveBindAddress();
                listener = new TcpListener(bindAddress, listenPort);
                listener.Start();
                StartSender();
                acceptThread = new Thread(AcceptLoop) { IsBackground = true, Name = "STB-Accept" };
                acceptThread.Start();
                return true;
            }
            catch (Exception ex)
            {
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: failed to start listener: {ex.Message}",
                    PrintTo.OutputTab1);
                return false;
            }
        }

        private void StartSender()
        {
            if (senderRunning) return;
            senderRunning = true;
            senderThread = new Thread(SenderLoop) { IsBackground = true, Name = "STB-Send" };
            senderThread.Start();
        }

        private void StopSender()
        {
            senderRunning = false;
            try { broadcastSignal.Set(); } catch { }
        }

        private void SenderLoop()
        {
            while (senderRunning)
            {
                try { broadcastSignal.WaitOne(); } catch { return; }
                if (!senderRunning) return;
                try { BroadcastNow(); }
                catch (Exception ex)
                {
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: broadcast error: {ex.Message}", PrintTo.OutputTab1);
                }
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

                // Hand the client off immediately. Authentication, the
                // initial snapshot build and its write all take real time
                // (up to AuthTimeoutMs + WriteTimeoutMs each), and doing
                // them inline made this single thread the bottleneck: one
                // peer that connects and stays silent would stall every
                // other connection, including the trading client's, until
                // it timed out. A worker per client keeps accept free.
                var c = client;
                try
                {
                    ThreadPool.QueueUserWorkItem(_ => HandleNewClient(c));
                }
                catch (Exception ex)
                {
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: could not start client worker: {ex.Message}",
                        PrintTo.OutputTab1);
                    try { c.Close(); } catch { }
                }
            }
        }

        /// <summary>
        /// Authenticate one client, send it the first snapshot, register it
        /// for broadcasts and then serve its commands. Runs on a pool
        /// thread — never on the accept thread. Wrapped so an unexpected
        /// throw kills only this connection, not the listener.
        /// </summary>
        private void HandleNewClient(TcpClient client)
        {
            try
            {

                // Configure the socket: disable Nagle so the first-snapshot
                // reaches the client immediately, and cap write time so a
                // half-open peer can't wedge the accept thread on the next
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
                    NinjaTrader.Code.Output.Process(
                        "SocketTraderBridge: refusing connection — no token file. " +
                        "Enable the live monitor in SocketTrader (Setup > 9) to create it.",
                        PrintTo.OutputTab1);
                    try { client.Close(); } catch { }
                    return;
                }
                string peer = PeerAddress(client);
                if (AuthBackedOff(peer))
                {
                    try { client.Close(); } catch { }   // refused on sight, nothing logged
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
                    if (NoteAuthFailure(peer))
                        NinjaTrader.Code.Output.Process(
                            $"SocketTraderBridge: rejected unauthenticated client {peer} " +
                            $"(further attempts from it are refused quietly for {AuthBackoff.TotalSeconds:0}s " +
                            $"after {AuthFailuresBeforeBackoff} failures)",
                            PrintTo.OutputTab1);
                    try { client.Close(); } catch { }
                    return;
                }

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
                        return;  // don't register a client we couldn't even snapshot
                    }
                }

                lock (clientsLock) { clients.Add(client); }

                // Per-client reader thread: listens for newline-delimited
                // JSON commands from the client and dispatches them.
                // Commands let SocketTrader flatten positions via NT's
                // own account.Flatten(...) API, bypassing the file-based
                // ATI and its contract-name-format fragility.
                var readerThread = new Thread(() => ClientReadLoop(client))
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
                try { client.Close(); } catch { }
            }
        }

        private void ClientReadLoop(TcpClient client)
        {
            try
            {
                // Commands are expected to be infrequent; a larger read
                // timeout here lets the client connection stay open while
                // SafeBroadcast continues pushing data from the writer side.
                client.ReceiveTimeout = 0;  // block indefinitely on read
                var stream = client.GetStream();
                string line;
                while ((line = ReadCommandLine(stream)) != null)
                {
                    if (line.Length == 0) continue;
                    HandleCommand(line, client);
                }
                // null: the peer closed, or sent more than MaxCommandLineBytes
                // without a newline — either way this connection is done.
                try { client.Close(); } catch { }
            }
            catch (Exception)
            {
                // connection dropped; SafeBroadcast will evict the client
                // on the next broadcast when the write fails.
            }
        }

        private void HandleCommand(string jsonLine, TcpClient client)
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
                SendFrontMonths(client, ExtractJsonString(jsonLine, "roots"));
                return;
            }
            string accountName = ExtractJsonString(jsonLine, "account");
            if (string.IsNullOrEmpty(cmd) || string.IsNullOrEmpty(accountName))
            {
                SendAck(client, false, "missing cmd or account");
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
                SendAck(client, false, "account not found: " + accountName);
                return;
            }
            try
            {
                if (cmd == "flatten")
                {
                    FlattenAccount(target);
                    SendAck(client, true, "flattened " + accountName);
                }
                else if (cmd == "close_position")
                {
                    string inst = (ExtractJsonString(jsonLine, "instrument") ?? "").Trim();
                    if (string.IsNullOrEmpty(inst))
                    {
                        SendAck(client, false, "close_position missing instrument");
                        return;
                    }
                    ClosePositionOnInstrument(target, inst);
                    SendAck(client, true, "close_position " + inst);
                }
                else
                {
                    SendAck(client, false, "unknown cmd: " + cmd);
                }
            }
            catch (Exception ex)
            {
                SendAck(client, false, ex.Message);
                NinjaTrader.Code.Output.Process(
                    $"SocketTraderBridge: command '{cmd}' error: {ex.Message}",
                    PrintTo.OutputTab1);
            }
        }

        private void FlattenAccount(Account acct)
        {
            // Build the set of open-position instruments, then let NT flatten
            // each one via its own account.Flatten() API — handles ATM stop
            // cancellation and market-order exit in one call, no contract-name
            // format fragility, no file-based race.
            var toFlatten = new List<Instrument>();
            foreach (var pos in acct.Positions)
            {
                if (pos == null || pos.Instrument == null) continue;
                if (pos.MarketPosition == MarketPosition.Flat) continue;
                toFlatten.Add(pos.Instrument);
            }
            if (toFlatten.Count == 0) return;
            acct.Flatten(toFlatten.ToArray());
            NinjaTrader.Code.Output.Process(
                $"SocketTraderBridge: flattened {toFlatten.Count} position(s) on {acct.Name}",
                PrintTo.OutputTab1);
        }

        private void ClosePositionOnInstrument(Account acct, string instName)
        {
            foreach (var pos in acct.Positions)
            {
                if (pos == null || pos.Instrument == null) continue;
                if (pos.MarketPosition == MarketPosition.Flat) continue;
                // Exact match on the contract ("NQ 12-26", NT's FullName) or
                // the product ("NQ", MasterInstrument.Name), case-insensitive.
                // The substring match this used to allow let "instrument":" "
                // close the first open position found.
                var full = (pos.Instrument.FullName ?? "").Trim();
                var master = pos.Instrument.MasterInstrument != null
                    ? (pos.Instrument.MasterInstrument.Name ?? "").Trim()
                    : "";
                if ((full.Length > 0 && full.Equals(instName, StringComparison.OrdinalIgnoreCase))
                    || (master.Length > 0 && master.Equals(instName, StringComparison.OrdinalIgnoreCase)))
                {
                    acct.Flatten(new[] { pos.Instrument });
                    NinjaTrader.Code.Output.Process(
                        $"SocketTraderBridge: closed {pos.Instrument.FullName} on {acct.Name}",
                        PrintTo.OutputTab1);
                    return;
                }
            }
        }

        private void SendFrontMonths(TcpClient client, string rootsCsv)
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
            try
            {
                var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                var stream = client.GetStream();
                stream.WriteTimeout = WriteTimeoutMs;
                stream.Write(bytes, 0, bytes.Length);
            }
            catch { /* client dropped — SafeBroadcast will evict */ }
        }

        private void SendAck(TcpClient client, bool ok, string message)
        {
            try
            {
                var sb = new StringBuilder();
                sb.Append("{\"ack\":");
                sb.Append(ok ? "true" : "false");
                sb.Append(",\"msg\":").Append(JsonString(message));
                sb.Append("}\n");
                var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                var stream = client.GetStream();
                stream.WriteTimeout = WriteTimeoutMs;
                stream.Write(bytes, 0, bytes.Length);
            }
            catch { /* client dropped — SafeBroadcast will evict */ }
        }

        private static string ExtractJsonString(string json, string key)
        {
            // Locate "<key>":"<value>" and unescape \\ and \" inside the value.
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
                    if (n == '"' || n == '\\') { val.Append(n); i += 2; continue; }
                    if (n == 'n') { val.Append('\n'); i += 2; continue; }
                    if (n == 't') { val.Append('\t'); i += 2; continue; }
                }
                if (c == '"') return val.ToString();
                val.Append(c);
                i++;
            }
            return null;
        }

        /// <summary>Ask the sender thread for a broadcast. Cheap; safe on any thread.</summary>
        private void SafeBroadcast()
        {
            if (!senderRunning) return;
            try { broadcastSignal.Set(); } catch { }
        }

        /// <summary>Build one snapshot and write it to every client (sender thread only).</summary>
        private void BroadcastNow()
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
