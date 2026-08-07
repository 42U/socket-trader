#!/usr/bin/env python3
"""Static guards for the embedded web UI.

The dashboard is a hand-written page held in a Python string, so the normal
test suite can't see inside it: a JavaScript syntax error, a field the JS
reads that the backend stopped sending, or a re-introduced innerHTML sink
would all ship silently and only show up as a blank or broken page in the
browser. This script checks those, plus that the request-gating controls are
still wired into the HTTP handler.

Run it locally the same way CI does:

    python scripts/check_webui.py
"""
from __future__ import annotations

import inspect
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import SocketTrader as st  # noqa: E402

failures: list[str] = []
notes: list[str] = []


def check(label: str, ok: bool, detail: str = ""):
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f" — {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def js_source() -> str:
    m = re.search(r"<script>\n(.*?)</script>", st.WEB_UI_HTML, re.S)
    if not m:
        print("  FAIL  could not locate the <script> block in WEB_UI_HTML")
        failures.append("script block")
        return ""
    return m.group(1)


JS = js_source()

print("\nweb UI guards")
print("-" * 62)

# ---- 1. the page must still carry the token placeholder -------------------
# start_web_ui substitutes this per process; without it every request would
# be rejected by the token check and the UI would be inert.
check("token placeholder present", "__ST_TOKEN__" in st.WEB_UI_HTML)
check("JS sends the token header", "X-ST-Token" in JS)

# ---- 2. JavaScript must parse --------------------------------------------
node = shutil.which("node")
if node and JS:
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "ui.js"
        f.write_text(JS, encoding="utf-8")
        p = subprocess.run([node, "--check", str(f)], capture_output=True, text=True)
    check("JavaScript parses", p.returncode == 0,
          (p.stderr.strip().splitlines() or [""])[0] if p.returncode else "")
else:
    notes.append("node not found — skipped the JavaScript syntax check")

# ---- 3. no innerHTML sinks ------------------------------------------------
# Account names and profile summaries are user-controlled and reach the page;
# building rows with innerHTML re-introduces stored XSS. Rows are built with
# textContent instead, so there should be no innerHTML assignment at all.
sinks = re.findall(r"\.innerHTML\s*=", JS)
check("no innerHTML assignment in the UI", not sinks,
      f"{len(sinks)} found" if sinks else "")

# ---- 4. JS/backend field contract ----------------------------------------
# Every S.<field> the page reads must exist in web_state(), or that part of
# the dashboard silently renders undefined.
st.active_account = "Sim101"
st.follower_accounts = ["Sim102"]
st.roundrobin_accounts = ["RR1"]
st.session_start_balances["Sim101"] = 1000.0
st.session_current_balances["Sim101"] = 1100.0
state = st.web_state()

read = set(re.findall(r"\bS\.([A-Za-z_][A-Za-z0-9_]*)", JS))
missing = sorted(f for f in read if f not in state)
check(f"web_state covers all {len(read)} fields the JS reads", not missing,
      ", ".join(missing))

nested = {"rr": {"pool", "remaining", "last"}}
for parent, keys in nested.items():
    gap = sorted(k for k in keys if k not in (state.get(parent) or {}))
    check(f"web_state['{parent}'] carries {sorted(keys)}", not gap, ", ".join(gap))

# The account grid and positions table render from /api/live, so check that
# payload rather than web_state(). A stub snapshot keeps this offline-safe.
st.nt_snapshot = lambda port=None, timeout=3.0: {
    "ok": True,
    "accounts": {"Sim101": {"cash": 1000.0, "realized": 5.0, "buying_power": 0.0}},
    "positions": [{"account": "Sim101", "instrument": "NQ 09-26",
                   "qty": -1, "avg_price": 23895.25}],
    "working": {"Sim101": 2}, "ts": 0.0,
}
live = st.web_live(force=True)

# `a` is the account row inside the grid renderer; ignore DOM members in case
# a local element ever shares the name.
DOM_MEMBERS = {"appendChild", "style", "className", "textContent", "onclick",
               "value", "classList", "title", "type", "placeholder", "disabled"}
live_read = set(re.findall(r"\ba\.([A-Za-z_][A-Za-z0-9_]*)", JS)) - DOM_MEMBERS
account_keys = set((live["accounts"] or [{}])[0])
needed = {"name", "role", "managed", "cash", "realized", "session_pnl",
          "working", "stop", "profile", "limits", "positions",
          "sync", "sync_detail"}
check("live account rows carry every field the grid renders",
      needed <= account_keys, ", ".join(sorted(needed - account_keys)))
check("grid reads no field the live payload lacks",
      live_read <= account_keys | {"name"},
      ", ".join(sorted(live_read - account_keys)))

pos_keys = set((live["positions"] or [{}])[0])
pos_needed = {"account", "instrument", "qty", "avg_price"}
check("live positions carry every field the table renders",
      pos_needed <= pos_keys, ", ".join(sorted(pos_needed - pos_keys)))

# The ticket's instrument picker must never be empty — that was the whole
# failure mode of the previous build (it only listed already-traded symbols).
cat = st.instrument_catalog()
check("instrument catalog is populated", len(cat) >= 10, f"{len(cat)} products")
check("every catalog product has a live contract",
      all(p["contracts"] for p in cat))
check("contracts use the OIF 'ROOT MM-YY' form",
      all(re.fullmatch(r"[A-Z0-9]+ \d{2}-\d{2}", p["contracts"][0]) for p in cat),
      cat[0]["contracts"][0] if cat else "")

# ---- 5. request gating must stay wired ------------------------------------
# These are the controls that stop another page in the user's browser from
# driving the trading API. Losing any of them silently re-opens it.
post = inspect.getsource(st._WebHandler.do_POST)
for control, label in [("_host_ok", "Host validation (DNS rebinding)"),
                       ("_origin_ok", "Origin validation (cross-site POST)"),
                       ("_token_ok", "token check (CSRF)"),
                       ("application/json", "JSON content-type enforcement")]:
    check(f"do_POST enforces {label}", control in post)

get = inspect.getsource(st._WebHandler.do_GET)
check("do_GET validates Host", "_host_ok" in get)
check("state read requires a token", "_token_ok" in get)

profiles = inspect.getsource(st._web_set_profiles)
check("web profile writes strip AI gate config", "_strip_ai_config" in profiles)

print("-" * 62)
for n in notes:
    print(f"  note: {n}")
if failures:
    print(f"\n{len(failures)} check(s) failed: " + "; ".join(failures) + "\n")
    sys.exit(1)
print("\nall web UI guards passed\n")
