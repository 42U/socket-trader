# Changelog

## 1.0.0 — (pending release review)

The first release that runs as an installed Windows application and keeps one
record across Windows and WSL. Everything below was verified on the operator's
machine or by the test suite (1083 tests) unless marked otherwise.

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
  its console window at start-up. Release workflow builds and attaches the
  zip on a `vX.Y.Z` tag after the test suite passes.
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

### Known limits
- The in-console effects cannot show on an X-close (Windows hides the window
  first); the detached helper covers that case.
- Under Windows Terminal the X-close helper draws a dark panel instead of the
  window's own image (the real window is not visible to the process).
- The session clock treats 4:20–6:00 PM ET as closed; a fill in the 4:30–5:00
  PM reopening would be dated to the next session as an unattributed amount.
