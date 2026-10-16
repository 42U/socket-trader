#!/usr/bin/env python3
"""Drive NinjaTrader's file ATI on the SimTEST account to prove how a
reset over a FILLED position behaves, before and after the sequencing fix.

    python scripts/simtest_reset_harness.py repro    # v0.21.0 order: CLOSESTRATEGY + CLOSEPOSITION back to back
    python scripts/simtest_reset_harness.py fixed    # the sequenced reset: close, prove flat, then the opposite entry
    python scripts/simtest_reset_harness.py flat     # just flatten SimTEST and verify
    python scripts/simtest_reset_harness.py closepos # probe: plain CLOSEPOSITION on a bracketed position
    python scripts/simtest_reset_harness.py reverse  # probe: NinjaTrader's REVERSEPOSITION with an ATM
    python scripts/simtest_reset_harness.py flip     # the app's own flip decision over a filled short, then the flip

Every command this script writes is addressed to SimTEST and nothing else;
it refuses to run against any other account. It imports SocketTrader under
a scratch HOME so the live config, log and P&L files are never touched, and
reads the live config only for the NinjaTrader incoming folder and port.

The NinjaTrader log lines for SimTEST in each run's window are printed at
the end: they are the evidence, the script's own clock is only a guide.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path

ACCOUNT = "SimTEST"
INSTRUMENT = "NQ 12-26"
ATM = "NQ-BellFade"      # 400-tick stop: the bracket will not fire during a run
LIVE_CONFIG = Path.home() / ".voidorigin_config.json"

# Scratch HOME before the import: SocketTrader derives its config, log and
# P&L paths from Path.home() at import time.
_scratch = tempfile.mkdtemp(prefix="simtest-home-")
os.environ["HOME"] = _scratch
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import SocketTrader as st  # noqa: E402


def _live_cfg() -> dict:
    with open(LIVE_CONFIG, encoding="utf-8") as fh:
        return json.load(fh)


def _nt_log_path(incoming: str) -> Path | None:
    log_dir = Path(incoming).parent / "log"
    names = sorted(p for p in log_dir.glob("log.*.txt") if ".en." not in p.name)
    return names[-1] if names else None


def _stamp() -> str:
    return time.strftime("%H:%M:%S") + f".{int(time.time() * 1000) % 1000:03d}"


def say(msg: str):
    print(f"{_stamp()}  {msg}", flush=True)


def positions() -> int:
    pos = st.query_nt_positions(ACCOUNT, st.nt_port)
    return st._position_qty(pos, INSTRUMENT)


def write(cmd: str) -> str:
    parts = cmd.split(";")
    if parts[1] != ACCOUNT:
        raise SystemExit(f"refusing to write a command for {parts[1]!r}: {cmd}")
    path = st.write_signal_to_file(cmd)
    if not path:
        raise SystemExit(f"write failed: {cmd}")
    say(f"WROTE  {cmd}")
    return path


def wait_for(pred, timeout: float, what: str) -> tuple[bool, list[tuple[str, int]]]:
    """Poll positions until pred(qty) holds. Returns (ok, transitions)."""
    seen: list[tuple[str, int]] = []
    last = None
    end = time.time() + timeout
    while time.time() < end:
        q = positions()
        if q != last:
            seen.append((_stamp(), q))
            say(f"POS    {ACCOUNT} {INSTRUMENT} = {q:+d}")
            last = q
        if pred(q):
            return True, seen
        time.sleep(1.0)
    say(f"TIMEOUT waiting for {what} (last seen {last:+d})" if last is not None
        else f"TIMEOUT waiting for {what}")
    return False, seen


def flatten_and_verify() -> bool:
    if positions() == 0:
        say("already flat")
        return True
    for alias in st._nt_contract_aliases(INSTRUMENT):
        write(f"CLOSEPOSITION;{ACCOUNT};{alias};;;;;;;;;;")
    ok, _ = wait_for(lambda q: q == 0, 45, "flat after cleanup")
    return ok


def print_nt_log(log_path: Path | None, since: str):
    if not log_path:
        say("no NinjaTrader log found")
        return
    print(f"\n--- NinjaTrader log lines for {ACCOUNT} since {since} ({log_path.name}) ---")
    with open(log_path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if ACCOUNT not in line:
                continue
            t = line[11:19]
            if t >= since:
                print(line.rstrip()[:260])
    print("---\n")


def place(side: str, sid: str) -> str:
    cmd = f"PLACE;{ACCOUNT};{INSTRUMENT};{side};1;MARKET;;;DAY;;;{ATM};{sid}"
    write(cmd)
    return sid


def run_repro() -> int:
    """v0.21.0 behavior: CLOSESTRATEGY for the standing entry, then
    CLOSEPOSITION (both contract sizes), written back to back."""
    sid = f"harness:repro:{int(time.time())}~{ACCOUNT}"
    since = time.strftime("%H:%M:%S")
    place("SELL", sid)
    ok, _ = wait_for(lambda q: q == -1, 60, "the short to fill")
    if not ok:
        return 2
    say("standing short is filled and bracketed — writing the old reset burst")
    write(st._close_strategy_file(ACCOUNT, sid))
    for f in st._reset_close_files(ACCOUNT, INSTRUMENT):
        write(f)
    # Watch what NinjaTrader does with two closes against a 1-lot short.
    _, seen = wait_for(lambda q: False, 40, "(observing)")
    final = positions()
    say(f"RESULT after the old burst: position = {final:+d}  transitions = {seen}")
    verdict = ("DOUBLE CLOSE REPRODUCED: the second close opened a naked position"
               if final != 0 else
               "no double close this time: the first close filled before NinjaTrader read the second")
    say(verdict)
    return 0 if flatten_and_verify() else 3


def run_fixed() -> int:
    """The sequenced reset: close, prove flat, and only then the opposite entry."""
    if not hasattr(st, "_reset_close_standing"):
        raise SystemExit("SocketTrader has no _reset_close_standing yet — apply the fix first")
    sid = f"harness:fixed:{int(time.time())}~{ACCOUNT}"
    place("SELL", sid)
    ok, _ = wait_for(lambda q: q == -1, 60, "the short to fill")
    if not ok:
        return 2
    # The app's ledger is what the reset reads its strategy ids from.
    st._ledger_note_entry(ACCOUNT, INSTRUMENT, "SELL", 1, ("harness", ATM), sid)
    say("standing short is filled and bracketed — running the sequenced reset")
    t0 = time.time()
    flat = asyncio.run(st._reset_close_standing(ACCOUNT, INSTRUMENT))
    say(f"reset proved flat={flat} after {time.time() - t0:.1f}s; position now {positions():+d}")
    if not flat:
        say("reset never proved flat within its budget — the app would place the entry "
            "NOT placed (sticky alert); this harness places nothing either")
        return 0 if flatten_and_verify() else 3
    entry_sid = f"harness:fixed-entry:{int(time.time())}~{ACCOUNT}"
    place("BUY", entry_sid)
    ok, seen = wait_for(lambda q: q == 1, 60, "the opposite entry to fill")
    final = positions()
    say(f"RESULT after the sequenced reset: position = {final:+d}  transitions = {seen}")
    say("CLEAN REVERSAL: exactly the entry's one contract, nothing naked" if final == 1
        else "UNEXPECTED position after the sequenced reset")
    hold = float(os.environ.get("HOLD_S", "0") or 0)
    if hold > 0:
        # Leave the reversal on the book so it can be watched in NinjaTrader;
        # meanwhile report what the ATI state dump says is working on SimTEST.
        say(f"holding the reversal for {hold:.0f}s — watch SimTEST's Orders / Strategies tab")
        end = time.time() + hold
        while time.time() < end:
            snap = st.nt_snapshot(st.nt_port)
            say(f"ATI dump: ok={snap.get('ok')}  SimTEST working orders={snap.get('working', {}).get(ACCOUNT)}  "
                f"ids={snap.get('open_orders', {}).get(ACCOUNT)}  position={positions():+d}")
            time.sleep(10)
    write(st._close_strategy_file(ACCOUNT, entry_sid))
    ok, _ = wait_for(lambda q: q == 0, 45, "flat after closing the entry's strategy")
    return 0 if (ok or flatten_and_verify()) else 3


def run_closepos_probe() -> int:
    """What does a plain CLOSEPOSITION do to a position that still has its
    ATM bracket: cancel first and close after, or close at once?"""
    sid = f"harness:closepos:{int(time.time())}~{ACCOUNT}"
    place("SELL", sid)
    ok, _ = wait_for(lambda q: q == -1, 60, "the short to fill")
    if not ok:
        return 2
    time.sleep(3)                         # let the bracket settle on the book
    for f in st._reset_close_files(ACCOUNT, INSTRUMENT)[:1]:
        write(f)
    ok, seen = wait_for(lambda q: q == 0, 45, "flat after the plain close")
    time.sleep(3)
    say(f"RESULT after CLOSEPOSITION: position = {positions():+d}  transitions = {seen}")
    return 0 if flatten_and_verify() else 3


def run_reverse_probe() -> int:
    """NinjaTrader's own REVERSEPOSITION with an ATM: one command that is
    supposed to close the standing position and enter the other way."""
    sid = f"harness:rev-standing:{int(time.time())}~{ACCOUNT}"
    place("SELL", sid)
    ok, _ = wait_for(lambda q: q == -1, 60, "the short to fill")
    if not ok:
        return 2
    time.sleep(3)
    rev_sid = f"harness:rev-entry:{int(time.time())}~{ACCOUNT}"
    write(f"REVERSEPOSITION;{ACCOUNT};{INSTRUMENT};BUY;1;MARKET;;;DAY;;;{ATM};{rev_sid}")
    ok, seen = wait_for(lambda q: q == 1, 60, "the reversal to show long 1")
    hold = float(os.environ.get("HOLD_S", "20") or 20)
    end = time.time() + hold
    while time.time() < end:
        snap = st.nt_snapshot(st.nt_port)
        say(f"ATI dump: ok={snap.get('ok')}  SimTEST working orders={snap.get('working', {}).get(ACCOUNT)}  "
            f"ids={snap.get('open_orders', {}).get(ACCOUNT)}  position={positions():+d}")
        time.sleep(5)
    final = positions()
    say(f"RESULT after REVERSEPOSITION: position = {final:+d}  transitions = {seen}")
    if os.environ.get("OLD_SID_CLOSE"):
        # Does the OLD strategy still exist after the flip? A publisher's late
        # CLOSESTRATEGY for it must not cover against the new long.
        say("probing: CLOSESTRATEGY for the OLD (reversed-away) strategy id")
        write(st._close_strategy_file(ACCOUNT, sid))
        _, seen2 = wait_for(lambda q: False, 10, "(observing the old-id close)")
        say(f"after the old-id CLOSESTRATEGY: position = {positions():+d}  transitions = {seen2}")
    write(st._close_strategy_file(ACCOUNT, rev_sid))
    ok, _ = wait_for(lambda q: q == 0, 45, "flat after closing the reversal's strategy")
    return 0 if (ok or flatten_and_verify()) else 3


def run_flip() -> int:
    """The app's own flip decision (_flip_mode) over a filled short, then
    whatever it chose: one REVERSEPOSITION, or close → proof → place."""
    for fn in ("_flip_mode", "_flip_signal", "_reset_close_standing"):
        if not hasattr(st, fn):
            raise SystemExit(f"SocketTrader has no {fn} yet — apply the fix first")
    sid = f"harness:flip-standing:{int(time.time())}~{ACCOUNT}"
    place("SELL", sid)
    ok, _ = wait_for(lambda q: q == -1, 60, "the short to fill")
    if not ok:
        return 2
    st._ledger_note_entry(ACCOUNT, INSTRUMENT, "SELL", 1, ("harness", ATM), sid)
    time.sleep(2)
    t0 = time.time()
    mode, why, twin = st._flip_mode(ACCOUNT, INSTRUMENT)
    say(f"_flip_mode → {mode!r} ({why}) twin={twin} in {time.time() - t0:.2f}s")
    entry_sid = f"harness:flip-entry:{int(time.time())}~{ACCOUNT}"
    entry = f"PLACE;{ACCOUNT};{INSTRUMENT};BUY;1;MARKET;;;DAY;;;{ATM};{entry_sid}"
    if mode == "reverse":
        write(st._flip_signal(entry))
    else:
        flat = asyncio.run(st._reset_close_standing(ACCOUNT, INSTRUMENT))
        say(f"sequenced close proved flat={flat} after {time.time() - t0:.1f}s")
        if not flat:
            say("flat never proven — the app would NOT place the entry; stopping here")
            return 0 if flatten_and_verify() else 3
        write(entry)
    ok, seen = wait_for(lambda q: q == 1, 60, "the flip to show long 1")
    say(f"flip complete {time.time() - t0:.1f}s after the decision: position = {positions():+d}  transitions = {seen}")
    hold = float(os.environ.get("HOLD_S", "20") or 20)
    end = time.time() + hold
    while time.time() < end:
        snap = st.nt_snapshot(st.nt_port)
        say(f"ATI dump: ok={snap.get('ok')}  SimTEST working orders={snap.get('working', {}).get(ACCOUNT)}  "
            f"ids={snap.get('open_orders', {}).get(ACCOUNT)}  position={positions():+d}")
        time.sleep(5)
    write(st._close_strategy_file(ACCOUNT, entry_sid))
    ok, _ = wait_for(lambda q: q == 0, 45, "flat after closing the flip's strategy")
    return 0 if (ok or flatten_and_verify()) else 3


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode not in ("repro", "fixed", "flat", "closepos", "reverse", "flip"):
        print(__doc__)
        return 1
    cfg = _live_cfg()
    st.output_directory = cfg["output_directory"]
    st.nt_port = int(cfg.get("nt_port", 36973))
    if not st.validate_strategy(ATM):
        raise SystemExit(f"ATM template {ATM!r} is not installed")
    say(f"incoming={st.output_directory}  port={st.nt_port}  scratch HOME={_scratch}")
    say(f"start: {ACCOUNT} {INSTRUMENT} position = {positions():+d}")
    if mode != "flat" and positions() != 0:
        raise SystemExit("SimTEST is not flat — run `flat` first")
    since = time.strftime("%H:%M:%S")
    try:
        if mode == "flat":
            rc = 0 if flatten_and_verify() else 3
        elif mode == "repro":
            rc = run_repro()
        elif mode == "closepos":
            rc = run_closepos_probe()
        elif mode == "reverse":
            rc = run_reverse_probe()
        elif mode == "flip":
            rc = run_flip()
        else:
            rc = run_fixed()
    finally:
        print_nt_log(_nt_log_path(st.output_directory), since)
    say(f"end: {ACCOUNT} {INSTRUMENT} position = {positions():+d}  rc={rc}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
