# Changelog

## Unreleased

### Security (from the 2026-10-30 review of the installed app)
- The updater fetches the installer and its `.sha256` only from
  `https://github.com/42U/socket-trader/releases/download/…`; any other
  URL in GitHub's release document is refused before a byte is fetched.
- A `ws://` server URL to anything but this machine is logged as
  `UNENCRYPTED SERVER URL` on every connection and shown once in the alert
  line (the token travels in the URL, every signal in cleartext). The token
  is also sent as an `Authorization: Bearer` header, so a server can stop
  reading it from the query string without a client change.
- What the server sends is data: a non-string `signal` or a non-numeric
  `ts` is a rejected frame, not an exception that dropped the connection
  into reconnect backoff; the raw text of a rejected signal and the
  server's self-declared name are scrubbed of escape sequences before the
  dashboard prints them.

## 1.0.3 — 2026-10-31

### Fixed
- **Every trade on an account whose ATM uses a simulated stop was closed
  15 s after entry.** NinjaTrader holds a simulated stop locally until its
  trigger trades and logs it as `Name='SimStop1' New state='Trigger
  pending'`. The bracket guard's naked watch counted only orders named
  `Stop…` in the states the ATI dump spells (`TriggerPending`), so it saw
  no stop at all, declared the position unprotected after
  `BRACKET_ATTACH_S` (15 s) and closed it — on every entry. Reported by a
  user on 2026-10-30. A stop now counts under any of NinjaTrader's names
  (`Stop`, `Stop1`, `SimStop1`) and in the log's own spellings (`Trigger
  pending`, `Partially filled`, `Change submitted`, `Change pending`); a
  stop mid-trail (`Change submitted`) counts too, and a rejected `SimStop1`
  is handled as a rejected stop. When the watch does act, the log now also
  lists every order NinjaTrader shows open on that root, so the next
  unfamiliar name is diagnosable from the log alone.

## 1.0.2 — 2026-10-30

### Fixed
- **The in-app update (U / UPDATE) could do nothing, silently.** 2026-10-30
  15:46 on the operator's PC: 1.0.1 was downloaded and SHA-256-verified, the
  app stopped cleanly, and then nothing — no install, no restart, the
  handover script still sitting in `%TEMP%`. The handover ran the Setup
  from a *detached* cmd with no window: Setup's failure exit code was
  echoed into a window that did not exist, `timeout`/`pause` cannot run
  without a console, `/SUPPRESSMSGBOXES` turned any question into Cancel,
  and the script quit without restarting the old version. What exactly
  made Setup quit on that PC was therefore not recorded. The handover is
  now a visible **SocketTrader update** window; Setup runs with
  `/LOG=%TEMP%\SocketTrader-Setup-<version>.log`; its exit code goes to
  `%TEMP%\SocketTrader-update.log` (one line per attempt) and to a result
  file the next start reads back and reports — `UPDATE  installed v…`, or
  `UPDATE  the last attempt … FAILED — Setup exit code N: <what that code
  means>` in the log and the alert line; SocketTrader is started again
  whether Setup succeeded (the new one) or not (the old one); the script
  waits two seconds after the app is gone; and it is written with plain
  CRLFs (text mode had doubled them).
- **A refused "already running" window held the installer's running-app
  mutex.** The AppMutex was taken before the instance-lock check, so a
  second SocketTrader left open on its "already running — this window can
  be closed" screen made any silent Setup (the in-app update) quit at its
  running-app check. Only the instance that owns the lock holds the mutex
  now.
- `SESSION START` logs the version and whether it is the installed app or
  a script run, so a log shows which build was running.

## 1.0.1 — 2026-10-30

### Fixed
- **Installing 1.0.0 made NinjaTrader show "Unable to load ATM strategy
  template from file '…\templates\AtmStrategy\algoNQmed.xml'".**
  `algoNQmed.xml` is a *stop* strategy template (the one `NQ_Med` uses);
  NinjaTrader keeps those in `templates\StopStrategy`, but the 1.0.0
  Setup.exe copied every file in `strategy\` into `templates\AtmStrategy`,
  so NinjaTrader found a stop template in its ATM folder and could not read
  it as one. Orders were never affected — the ATM templates themselves were
  in the right place — only the error dialog. The installer now sorts the
  templates by kind at build time (`scripts/sort_templates.py`; a file that
  is neither kind fails the build), installs each kind to its own
  NinjaTrader folder, and removes the misplaced copy 1.0.0 left behind
  (only a file with the name of a shipped stop template whose content is a
  stop strategy document; an operator's own ATM template of that name is
  left alone). Existing templates are still never overwritten. The release
  smoke test now installs against a fake `Documents\NinjaTrader 8` with the
  stray 1.0.0 file planted and checks the placement and the cleanup.
- **`U` (check for / install an update) was a hidden key.** It worked but was
  not listed in the controls bar. The bar now shows `U=UPDATE`, the README
  key table has the row, and a test parses the keyboard loop and fails if a
  handled key is missing from the bar.

Already on 1.0.0: the app offers this update in its alert line and web
header; `U` or the UPDATE chip installs it, and the installer cleans up the
stray template. The review findings listed under 1.0.0 as "Deferred to
1.0.1" remain open and move to the next release.

## 1.0.0 — 2026-10-30

The first release that installs and updates like a Windows application
(`SocketTrader-Setup-1.0.0.exe`, in-app updates) and keeps one record across
Windows and WSL. Everything below was verified on the operator's machine or by
the test suite (1,140 tests, Linux and Windows, Python 3.10–3.13) unless
marked otherwise. Reviewed live with the operator on 2026-10-29; the QA pass
and three adversarial security sweeps are in, their remaining lower-severity
findings are listed under "Deferred to 1.0.1".

### Fixed
- **The trading loop froze when Remote Desktop connected.** 2026-10-29
  21:48:58 to 08:54 the next morning: nine hours of missed signals while the
  web page still showed cached state. Cause, confirmed when a single Esc in the
  console window brought it back: the console was in Windows' QuickEdit
  *selection* mode — a click in the window (the one that lands on it when a
  Remote Desktop session takes over the screen) pauses every write to the
  console until Esc or Enter, and the dashboard is written from the trading
  loop. QuickEdit is now switched off for the app's window at start-up
  (`"console_quick_edit": true` keeps it). The 1.0.0 release was rebuilt with
  this on 2026-10-30; a first-day 1.0.0 install must run the Setup again by
  hand (same version number, so the in-app updater does not offer it).
- Hardening from the same incident: the X-close switch-off effect no longer
  asks the console host to render the window every 1.5 s (`PrintWindow`,
  which runs under the console lock the loop's writes need); it tracks the
  window's position only and the helper draws its panel there (see Known
  limits).
- **A frozen trading loop looked healthy.** The web header now turns red with
  `MAIN LOOP STALLED <n> min — restart SocketTrader` as soon as the loop misses
  15 s of turns (`loop_stalled_s` in `/api/state`), whatever the cause.
- Release workflow no longer appends generated notes to a hand-written release
  body (the duplicated *Full Changelog* line on 1.0.0).
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
- The X-close helper draws a dark panel at the window's last position rather
  than the window's own image: capturing that image went through the console
  host's renderer and could freeze the trading loop.
- The session clock treats 4:20–6:00 PM ET as closed; a fill in the 4:30–5:00
  PM reopening would be dated to the next session as an unattributed amount.
