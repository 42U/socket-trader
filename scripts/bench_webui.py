#!/usr/bin/env python3
"""Latency harness for the SocketTrader web UI (not run in CI).

Runs a module (the committed SocketTrader.py or the working-tree one)
against a fake NinjaTrader ATI that serves a realistic ~50 KB state dump
with configurable pacing, starts the real web server (+ the snapshot
stream when the module has one), simulates the app's own dump consumers
at their real cadences, and measures what a browser click costs.

    python scripts/bench_webui.py SocketTrader.py healthy
    python scripts/bench_webui.py SocketTrader.py congested

Point it at an older copy of SocketTrader.py to compare before/after.
"""
import asyncio, http.client, importlib.util, json, os, socketserver, statistics
import sys, tempfile, threading, time
from pathlib import Path

MODULE, SCENARIO = sys.argv[1], sys.argv[2]


# ---- a realistic dump: 20 accounts, positions under 4 aliases, 200 orders
def build_dump() -> bytes:
    out = []
    for i in range(20):
        a = f"Acct{i:02d}" if i > 1 else ("Sim101", "Sim102")[i]
        out += [f"CashValue|{a}\x00{50000 + i * 13.37:.2f}\x00",
                f"RealizedPnL|{a}\x00{-12.5 + i:.2f}\x00",
                f"BuyingPower|{a}\x00{100000 + i:.2f}\x00"]
    for a in ("Sim101", "Sim102"):
        for alias in ("NQ DEC26", "@NQ", "NQZ26", "NQ Z6"):
            out += [f"MarketPosition|{alias}|{a}\x002\x00",
                    f"AvgEntryPrice|{alias}|{a}\x0023895.25\x00"]
    for a in ("Sim101", "Sim102"):
        ids = [f"{a}-o{j:03d}" for j in range(100)]
        out.append(f"Orders|{a}\x00{'|'.join(ids)}\x00")
        for j, oid in enumerate(ids):
            st_ = "Working" if j % 50 == 0 else "Filled"
            out.append(f"OrderStatus|{oid}\x00{st_}\x00")
    body = "".join(out)
    # pad with more filled-order history until ~50 KB, like a live morning
    j = 0
    while len(body) < 50_000:
        body += f"OrderStatus|hist{j:05d}\x00Filled\x00"; j += 1
    return (body + "ATI\x00True\x00").encode()


DUMP = build_dump()
FILL_DELAY_S = 1.0   # how long after a CLOSEPOSITION file appears the fake reports the account flat


def dump_now(incoming) -> bytes:
    """The dump as NinjaTrader would answer it now: accounts whose close
    files have sat in the incoming folder for FILL_DELAY_S are flat."""
    if not incoming:
        return DUMP
    flat = set(); now = time.time()
    for f in os.listdir(incoming):
        if not f.startswith("oifclose_"):
            continue
        p = os.path.join(incoming, f)
        try:
            if now - os.path.getmtime(p) >= FILL_DELAY_S:
                flat.add(open(p).read().split(";")[1])
        except (OSError, IndexError):
            pass
    if not flat:
        return DUMP
    text = DUMP.decode()
    keep = [rec for rec in text.split("\x00") if not any(
        rec.startswith(f"{fld}|") and rec.endswith(f"|{a}") for a in flat
        for fld in ("MarketPosition", "AvgEntryPrice"))]
    return "\x00".join(keep).encode()


PACING = {
    "healthy":   {"chunk": 4096, "gaps": lambda i: 0.30 if i == 1 else 0.0},   # 0.31 s live probe
    "congested": {"chunk": 1024, "gaps": lambda i: 0.09},                       # ~4.5 s trickle
}[SCENARIO]


class FakeNT:
    def __init__(self):
        outer = self
        self.lock = threading.Lock()
        self.requests = 0; self.active = 0; self.max_active = 0; self.cut = 0
        self.incoming = None

        class H(socketserver.StreamRequestHandler):
            def handle(self):
                self.rfile.readline()
                with outer.lock:
                    outer.requests += 1; outer.active += 1
                    outer.max_active = max(outer.max_active, outer.active)
                try:
                    dump = dump_now(outer.incoming)
                    for i in range(0, len(dump), PACING["chunk"]):
                        g = PACING["gaps"](i // PACING["chunk"])
                        if g: time.sleep(g)
                        self.wfile.write(dump[i:i + PACING["chunk"]]); self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    with outer.lock: outer.cut += 1
                finally:
                    with outer.lock: outer.active -= 1

        self.srv = socketserver.ThreadingTCPServer(("127.0.0.1", 0), H)
        self.srv.daemon_threads = True
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()


def load(path: str):
    spec = importlib.util.spec_from_file_location("st_bench", path)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


tmp = Path(tempfile.mkdtemp(prefix="stbench-"))
(tmp / "incoming").mkdir()
nt = FakeNT()
nt.incoming = str(tmp / "incoming")
st = load(MODULE)
st.logger.removeHandler(st._log_handler)               # never touch the live log
st.CONFIG_FILE = tmp / "cfg.json"
st.PNL_DIR = tmp / "pnl"; st.PNL_HISTORY_FILE = tmp / "pnl_hist.json"
st.nt_host_override = "127.0.0.1"; st.nt_port = nt.port
st.output_directory = str(tmp / "incoming")
st.active_account = "Sim101"; st.follower_accounts = ["Sim102"]
st.atm_strategy = "NQ_Med"
st.validate_strategy = lambda n: True
st.list_atm_strategies = lambda: ["NQ_Med"]
st.live_bridge_enabled = False
for a in ("Sim101", "Sim102"):
    st.session_start_balances[a] = 50000.0; st.session_current_balances[a] = 50000.0

loop = asyncio.new_event_loop()
threading.Thread(target=loop.run_forever, daemon=True).start()
has_stream = hasattr(st, "start_snapshot_poller")
if has_stream:
    st.start_snapshot_poller()
url = st.start_web_ui(loop, {"webui_port": 0})
host, port = url.replace("http://", "").split(":"); port = int(port)
TOKEN = st._web_token

# ---- the app's own dump consumers, at their real cadences
stop = threading.Event()
def every(period, fn):
    def run():
        while not stop.is_set():
            try: fn()
            except Exception: pass
            stop.wait(period)
    threading.Thread(target=run, daemon=True).start()
every(2.0, lambda: st.web_live())                                                     # pnl tracker
if has_stream:
    every(3.0, lambda: st._accounts_now(3.0))                                          # balance monitor
    every(3.0, lambda: st._positions_now("Sim101", 3.0))                               # confirms
else:
    every(3.0, lambda: st.query_nt_accounts(st.nt_port))
    every(3.0, lambda: st.query_nt_positions("Sim101", st.nt_port))

# ---- a browser tab
def req(method, path, body=None, timeout=60.0):
    c = http.client.HTTPConnection(host, port, timeout=timeout)
    hdrs = {"X-ST-Token": TOKEN}
    if body is not None: hdrs["Content-Type"] = "application/json"
    t0 = time.perf_counter()
    c.request(method, path, body=json.dumps(body) if body is not None else None, headers=hdrs)
    r = c.getresponse(); data = r.read(); c.close()
    return time.perf_counter() - t0, r.status, data

live_lat = []; state_lat = []; slow = []; current = {"click": "idle"}
def _live():
    t = time.perf_counter(); dt = req("GET", "/api/live")[0]; live_lat.append(dt)
    if dt > 0.05: slow.append((round(t - t_start, 1), round(dt * 1000), current["click"]))
t_start = time.perf_counter()
every(1.5, lambda: state_lat.append(req("GET", "/api/state")[0]))
every(2.0, _live)

time.sleep(4.0)                                        # let everything settle
n0 = nt.requests; t_window = time.perf_counter(); nt.cut = 0; nt.max_active = 0
results = {}
def click(name, path, body, timeout=60.0):
    current["click"] = name
    dt, status, data = req("POST", path, body, timeout)
    current["click"] = "idle"
    try: msg = json.loads(data).get("message", "")[:70]
    except Exception: msg = data[:70]
    results[name] = (dt, status, msg)
    print(f"  {name:<22} {dt*1000:8.0f} ms   {status}  {msg}", flush=True)

print(f"\n== {Path(MODULE).name}  scenario={SCENARIO}  stream={'yes' if has_stream else 'no'}")
for i in range(3):
    click(f"POST /api/trade #{i+1}", "/api/trade", {"side": "long", "instrument": "NQ 12-26", "qty": 1})
    time.sleep(1.5)
click("POST /api/close_position", "/api/close_position", {"account": "Sim101", "instrument": "NQ DEC26"})
time.sleep(1.0)
click("POST /api/flatten_account", "/api/flatten_account", {"account": "Sim102"})
time.sleep(1.0)
click("POST /api/close_all", "/api/close_all", {})
window = time.perf_counter() - t_window
stop.set(); time.sleep(0.2)
p = lambda xs, q: (statistics.quantiles(xs, n=100)[q - 1] if len(xs) >= 3 else max(xs)) * 1000
print(f"  GET /api/live   n={len(live_lat)}  p50 {p(live_lat,50):6.0f} ms  p95 {p(live_lat,95):6.0f} ms  max {max(live_lat)*1000:6.0f} ms")
print(f"  GET /api/state  n={len(state_lat)}  p50 {p(state_lat,50):6.0f} ms  p95 {p(state_lat,95):6.0f} ms  max {max(state_lat)*1000:6.0f} ms")
print(f"  ATI dumps: {(nt.requests - n0) / window:.2f}/s over {window:.0f}s   max concurrent {nt.max_active}   cut early {nt.cut}")
files = sorted(os.listdir(tmp / "incoming"))
print(f"  order files written: {len(files)}")
print(f"  slow live polls (t, ms, during): {slow}")
if has_stream: st.stop_snapshot_poller()
st.stop_web_ui(); loop.call_soon_threadsafe(loop.stop)
os._exit(0)
