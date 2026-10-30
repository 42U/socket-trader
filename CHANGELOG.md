# Changelog

## 1.0.1 — (unreleased)

### Fixed
- **A frozen trading loop looked healthy.** 2026-10-29 21:48:58: the loop
  stopped (a click inside the console window — Windows QuickEdit pauses every
  write to the console, and the dashboard is written from the loop) while the
  web server thread kept answering from cached state for nine hours of missed
  signals. QuickEdit is now switched off for the app's window at start-up
  (`"console_quick_edit": true` keeps it), and the web header turns red with
  `MAIN LOOP STALLED <n> min — restart SocketTrader` as soon as the loop misses
  15 s of turns (`loop_stalled_s` in `/api/state`).
- Release workflow no longer appends generated notes to a hand-written release
  body (the duplicated *Full Changelog* line on 1.0.0).

## 1.0.0 — 2026-10-30

The first release that installs and updates like a Windows application
(`SocketTrader-Setup-1.0.0.exe`, in-app updates) and keeps one record across
Windows and WSL. Everything below was verified on the operator's machine or by
the test suite (1,133 tests, Linux and Windows, Python 3.10–3.13) unless
marked otherwise. Reviewed live with the operator on 2026-10-29; the QA pass
and three adversarial security sweeps are in, their remaining lower-severity
findings are listed under "Deferred to 1.0.1".

### Fixed
- **Config saves silently dropped on Windows.** A rename over a file another
  thread had open failed with "Access is denied … .tmp" and the setting was
  lost while the app believed it had saved — the cause of profiles that
  "reset" after a restart. The config is now served from memory, written under
  a lock outside the read path with retries, refused saves are kept in memory
  and retried every 2 s, and a save that does not land is reported in the
  alert line and the web reply instead of being swallowed.
- **A transient unreadable config could be rewritten from `{}`**, wiping
  profiles, token and accounts. An existing file that cannot be parsed now
  blocks saves until it can be read.
- **P&L archive lost records on a failed write** (the month was marked clean
  before the write succeeded; the exit-time save then found nothing to do).
- **Profile editor could erase other accounts' profiles**: SAVE/RESET sent the
  page's whole map, which a stale or failed poll could leave empty. The editor
  now names the one account it edits.
- **Dead accounts buried the live ones.** NinjaTrader lists every account a
  connection has ever reported; unmanaged accounts with no cash, no position
  and no working order fold away (plus a Hide button), positions sort
  managed-first.
- Menus opened only after a fresh NinjaTrader state dump (seconds on a busy
  session); they now read the shared snapshot. Dashboard polls no longer copy
  the whole config per account (web_live 25 ms → <1 ms at 81 accounts); ATM
  template listing cached; console redraws batched.
- Closing the console with the X skipped the final P&L save and left the
  instance lock stale; a console-control handler now does the orderly stop.
- **Limits set from the web UI did not lift a soft lockout** (the terminal's
  limits prompt always did). An account tripped on a stale target stayed out
  for the rest of the session after the target was raised — Sim101,
  2026-10-29, 11:50 to the 4:20 PM reset. New limits now re-arm the account
  on both paths; hard locks still hold until reset/exit.
- The "signal skipped" alert named the first three skipped legs and left out
  accounts held by a stop/target lock. It now lists every account grouped by
  reason, plus `locked: …`, and the log line carries the lock map.
- **A refused save no longer erases another writer's changes.** While the
  disk refused this app's saves, a config edit made meanwhile by something
  the instance lock does not cover (an older build, the other OS with the
  lock overridden, an edit by hand) was replaced wholesale by this app's
  memory on the next successful write. The pending save now merges: the
  file is the base, this app's changed keys go over it, and the log names
  both sides (`CONFIG … merged: this app's [...] over the file's [...]`).
- A scoped rule whose Entries is INHERIT on an ENTRIES OFF profile never
  enters, yet a strategy listed in it looked enabled. It is now flagged in
  the profile summary ("⚠ 1 never enters"), in both rule editors (with a
  confirm on save in the web UI), and in the log digest.

- **Bridge reconnect storm.** When the AddOn refused the token (or closed
  the stream), the bridge task reconnected in a tight loop — 23,000 times
  in 16 s on 2026-10-27 — and the two lines it logged each time rotated
  three files of trading history out of the log. It now backs off like an
  unreachable port (1 → 30 s), says so once per step, and retries a dropped
  stream after a second.
- **Profile import shows what it is about to do.** A weeks-old export file
  replaced a live entries-ON profile behind a generic "Replace?" prompt
  (2026-10-29 08:26; the account then sat out every signal of the session).
  The web and terminal confirms now show the account's current setup, the
  file's setup, its scoped strategies, and where/when the file was exported.

- A second launch while SocketTrader runs printed its refusal and closed the
  window in the same instant (twice on 2026-10-29); it now waits for a key.
- Instance lock: a holder whose process cannot be inspected (another user's
  or an elevated SocketTrader — access denied) counts as live, not dead.
- The Windows side no longer renames a WSL config it did not adopt.
- Switch-off overlay only on the window's X, not at logoff/shutdown.
- `build.ps1` / CI: the plain `python` fallback (no `py` launcher) was
  broken by a PowerShell descending-range slice.

- **Second machine, same NinjaTrader folder.** The bridge token file is now
  adopted when another SocketTrader already put one there (OneDrive-shared
  NinjaTrader folders), instead of each start overwriting the other's and
  taking its bridge down; a rotation from the config still publishes. The
  AddOn's port is a file (`SocketTraderBridge.port`, written from the Live
  Monitor setting) rather than a constant to edit per machine, and the probe
  names an ATI-port clash for what it is (`read error: [WinError 10053]` on a
  port NinjaTrader's own AT Interface occupies).

### Added (operability, after the 2026-10-29 stale-profile restart)
- `PROFILE DIGEST` log lines whenever profiles are loaded or saved: per
  account, what it will do and every scoped rule with its scope — a restart
  that comes up on an older profile set is visible at the minute it happens.
- `.voidorigin_profile_history/` beside the config: a dated copy of every
  profile set that reached the disk (newest 60, unchanged sets not
  duplicated), so a lost or overwritten profile can be put back by hand.

### Added
- **One data home** for Windows and WSL runs (`C:\Users\<you>`, reached from
  WSL through `/mnt/c`), with one-time merge of the old WSL-side history from
  either side; machine-specific values (NinjaTrader folder, host override)
  translated or scoped per OS. `SOCKETTRADER_HOME` overrides.
- **Instance lock** with heartbeat so two SocketTraders cannot write the same
  home; merge-on-write for the P&L month as a second line of defence.
- **SocketTrader.exe** (`build.bat`): PyInstaller build with icon and version
  resource; `install.bat` installs the exe when present (no Python on the
  target), registers in Settings → Apps with an uninstaller, stamps an
  AppUserModelID on the shortcuts; the app sets its title, icon and App ID on
  its console window at start-up.
- **`SocketTrader-Setup-X.Y.Z.exe`** — a one-click per-user installer (Inno
  Setup; `installer/SocketTrader.iss`) built and smoke-installed by the
  release workflow on a `vX.Y.Z` tag and attached to the GitHub release next
  to the bare zip, each with a SHA-256. Upgrades in place; asks to close a
  running SocketTrader (the app now holds a named mutex for it).
- **In-app updates.** The installed app checks GitHub's latest release at
  start-up and every six hours; a newer one shows in the alert line and the
  web header. `U` / the UPDATE chip downloads the installer and its
  published SHA-256, refuses a mismatch, stops cleanly and lets the silent
  installer replace the files and restart the app. `"update_check": false`
  disables the check.
- Installer installs from the folder it runs in and refuses to downgrade an
  installed copy to an older GitHub release (`-FromGitHub` to force).
- Open/close effects: digital rain on open, CRT switch-off on close — in the
  terminal, in the web UI, and (via a detached helper over the window's last
  snapshot) when the console is closed with the X. `"fx": false` disables.
- Desktop icon (`images/sockettrader.ico`, regenerated by `scripts/make_icon.py`).
- `--version` flag; `web_hidden_accounts`, `nt_host_os`, `wsl_legacy_scanned`
  config keys; `/api/hide_account`; `only` on `/api/profiles`.

### Unchanged on purpose
- The NinjaScript AddOn (`addon/SocketTraderBridge.cs`) is the build that has
  been running since October 25. A connection-status field was prototyped and
  withdrawn: it compiled against NinjaTrader.Core.dll but stalled the first
  snapshot on a live NinjaTrader; the dashboard does not need it.

### Deferred to 1.0.1 (review findings, lower severity — none affect an order that is placed)
- Replay-guard false positive on an id-less close repeated legitimately within the window.
- `CLOSESTRATEGY` fan-out can send one close per tranche id twice when a stagger was recorded
  under both ids; NinjaTrader rejects the duplicate.
- Deferred (delayed/staggered) legs are cancelled on exit but not on a mid-flight role change.
- Per-entry quantity ceiling and coercion of odd limit values in the web limits form.
- Prop flat time cap, micro-map and role validation on config load; day-record hardening in
  the P&L archive against a hand-edited month file.
- ANSI scrubbing of server-sent strings before they reach the terminal; linear
  `_parse_ati_fields` with a buffer cap; envelope type validation; strategy-name path
  characters in file names; a missing `Host` header answered 403; `RecursionError` guard on
  nested JSON; a warning when the server URL is `ws://` rather than `wss://`.
- Installer: refuse a non-empty custom `InstallDir`; `Unblock-File` on extracted scripts;
  `uninstall.cmd` OEM encoding; bundle `anthropic` in the exe for the AI gate.

### Known limits
- The in-console effects cannot show on an X-close (Windows hides the window
  first); the detached helper covers that case.
- Under Windows Terminal the X-close helper draws a dark panel instead of the
  window's own image (the real window is not visible to the process).
- The session clock treats 4:20–6:00 PM ET as closed; a fill in the 4:30–5:00
  PM reopening would be dated to the next session as an unattributed amount.
