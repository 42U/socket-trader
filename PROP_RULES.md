# Prop-Firm Rules — Research & Coverage

**Researched 2026-08-14** from each firm's official help center / terms (quotes and article links below; third-party blog claims are marked *aggregator*). Firms change rules without notice — Apex replaced its whole rulebook on 2026-03-01 ("Apex 4.0") — so re-verify against your firm's live pages before relying on any number here. This document backs the **Prop Firm Mode** feature (`prop: true` on an account profile, see README).

---

## 1. The question that started this: can gold and Nasdaq be held at the same time?

**Every firm surveyed allows concurrent positions in different, uncorrelated markets — same direction.** No firm has a "one open position at a time" rule.

- Apex (Scaling Levels): *"You may distribute contracts across different markets, but the total exposure must remain within your maximum contract limit."*
- Bulenox (Qualification Account): *"A trader can have several positions at the same time."*
- Elite codifies it: instruments in different "Tier 2" sector groups *"are not treated as substantially similar"* — GC (metals) + NQ (indices) is explicitly outside its hedging rule.
- MFFU's "Cross Instrument Policy" *"does not restrict holding positions in different instruments simultaneously"* — it bans only mixing minis+micros to evade contract caps.

What **is** banned everywhere is *opposite* exposure in the same or **correlated** market — and several firms scope "correlated" wide:

| Firm | Prohibited hedging scope (opposite sides) |
|---|---|
| **Apex** | Broadest: same or correlated market, *"all indices, metals, grains, currencies… no matter the size"*, in one account **or across accounts** — *"You may not be short NQ while long ES"* |
| **Topstep** | Same/correlated instrument across accounts (official examples: ES/MES, NQ/MNQ); detection weighs timing/size/duration/intent; *"prohibited even if the overlap is brief or unintentional"*, cannot be appealed |
| **Tradeify** | Whole **product group** (Equity Index, Energy, Metals, Currencies, Rates, Grains, Livestock, Volatility) — long ES / short NQ is a violation; mini = micro; single account or spread across accounts; 10-second grace |
| **Elite** | Tier 1 size-pairs + Tier 2 sector groups (ES-vs-NQ counts), one or many accounts, *"including accounts within the same household"* |
| **MFFU** | *"both buy and sell positions on the same underlying asset, at the same time"* (NQ = MNQ); different unrelated assets permitted |
| **Take Profit Trader** | Rule 6: *"opposite positions in the same or closely related products across any accounts under the same beneficial control"* (ES↔MES, NQ↔MNQ, YM↔MYM, *"not exhaustive"*) |
| **Alpha** | Cross-account inverse banned; **same-account** long NQ / short MNQ named *"strictly prohibited"* |
| **Lucid** | Same contract inverse banned everywhere; micro-vs-mini inverse allowed *in one account* but banned split across accounts; long ES / short NQ allowed in one account, banned split across accounts |
| **FundedNext** | *"Hedging correlated instruments across one or multiple accounts is not permitted"*; cross-firm hedging also named |
| **Bulenox** | No published official rule found (*aggregator*: cross-account inverse banned as circumvention) |

So the user-chosen policy SocketTrader enforces — **one position at a time per prop account** — is *stricter than any firm requires*, and that is the point: an account that never holds two markets at once can never drift into any firm's correlated-hedge definition, whatever "correlated" means this quarter.

---

## 2. What SocketTrader enforces for `prop: true` accounts

| # | Rule class | Firms' rule | How it's covered |
|---|---|---|---|
| 1 | Intra-account hedge (same market, both sides) | Banned everywhere | An entry **resets its own market**: any held same-market position (micro/full twin included, both directions) is closed and confirmed before the entry fires. Netting an opposite entry instead would leave the old ATM bracket working on a flat account (per the OIF docs only `CLOSEPOSITION` cancels the instrument's orders), and a same-direction re-entry would stack contracts toward the firm's cap. Only a `REVERSEPOSITION` keeps its own market — the reversal itself flips it |
| 2 | Concurrent markets (user's one-trade policy) | Not required by firms; safest common denominator | **Close-before-open**: every other-market position on the entering prop account — including **just-written orders NT hasn't shown as positions yet** (tracked in-flight for 2 min) — is closed and **confirmed against NinjaTrader** before the entry is written. Unconfirmable close ⇒ entry withheld, sticky alarm |
| 3 | Cross-account opposite sides (same/correlated market) | Banned everywhere; closure-grade | (a) Hedge guard **escalates to `block`** when a prop account is in an opposite-entry fan-out, overriding `warn`/`off`. (b) Before a prop entry fires, other managed prop accounts holding the **opposite side of the same product group** are flattened first, confirmed |
| 4 | Same-direction copy fan-out across own accounts | Explicitly sanctioned at every firm (own accounts, one owner) | The app's normal copy mode is exactly this pattern; leader-first flatten ordering prevents the follower-left-open shape Tradeify names a violation |
| 5 | Flat by the close | Firm-specific deadlines, some breach-grade (MFFU) | Auto-flatten at `prop_flat_et` (firm presets below), verified + retried + alarmed; entries refused from `prop_cutoff_et` until the 18:00 ET reopen; Friday cutoff holds through the weekend |
| 6 | Contract caps | Per-plan tables at every firm | Per-account `max_contracts` cap in the profile rules (set it to your plan's cap); micro/full twin stacking (MFFU's cap-evasion breach) prevented by rule 1 |
| 7 | Positions/orders left working while flat elsewhere | Working orders can refill a closed position | Whole-account preemptive flattens go through `close_account_positions`, which cancels the account's working orders first |

Everything is **proof-driven**: no step reports "flat" without a fresh NinjaTrader state dump saying so (the same doctrine as the app's stop/target flatten and web Flat button). If NinjaTrader will not prove its state, prop entries are withheld — a missed trade is recoverable, a violation is not.

### Flat-by-close presets (`prop_firm`)

| Firm key | Firm's own deadline (official) | SocketTrader auto-flat | Entry cutoff |
|---|---|---|---|
| `apex` | Auto-liquidates 4:59 PM ET (*"should not be relied upon"*) | **16:57 ET** | 16:55 |
| `topstep` | Flat by 3:10 PM CT (4:10 PM ET); staff begin flattening 3:08 PM CT; 15 min earlier on half-days | **16:05 ET** | 16:02 |
| `mffu` / `myfundedfutures` | Auto-close 4:10 PM ET — *"failure to close … will result in breaching of the account"*; **no auto-close on holiday half-days** | **16:07 ET** | 16:05 |
| `tpt` / `takeprofittrader` | Auto-closes 4:55 PM ET, no carry-over | **16:52 ET** | 16:50 |
| `tradeify` | Flat by 4:59 PM ET; auto-close is *not* a breach | **16:57 ET** | 16:55 |
| `bulenox` | *"All positions must be closed before 15:59 (CT)"* (= 4:59 PM ET) | **16:57 ET** | 16:55 |
| `elite` / `etf` | No position *"less than one (1) minute prior closing"* of that instrument | **16:57 ET** | 16:55 |
| `fundednext` | *"All positions must be closed before the end of the trading day"* | **16:57 ET** | 16:55 |
| `alpha` | *"All trades must be closed before 4:20PM EST every day"* | **16:17 ET** | 16:15 |
| `lucid` | Sim accounts auto-liquidated 4:45 PM ET (Rithmic live: 4:15 PM ET) | **16:42 ET** | 16:40 |
| *(unset/unknown)* | — | 16:57 ET | 16:55 |

Presets deliberately sit ahead of the firm's deadline so market closes fill before the firm's risk engine acts. Override with `prop_flat_et` / `prop_cutoff_et`. **Holiday early closes are not automated** — every firm moves its deadline ~15 min before an early close, and MFFU won't auto-close for you on half-days: set an earlier `prop_flat_et` on those days or flatten manually.

---

## 3. Rules an order router cannot enforce (operator responsibility)

| Rule | Firms | What to do |
|---|---|---|
| **Automation policy** | **Apex 4.0: automation prohibited outright** (*"any form of AI, Autobots, algorithms, fully automated trading systems"* → closure + forfeiture of all funds). **TPT: *"All trades must be manually executed by the trader"*** (copiers fanning out manual trades are fine — approved list: Tradesyncer, TradeCopia, Affordable Indicators, Replikanto compliance edition, platform-native). **Alpha: bots banned**, >100 automated trades/day restricted. **Elite: approved copiers only**, unapproved automation needs written approval. **Bulenox: third-party algorithms need management approval.** Topstep: automation allowed with conditions but **no VPS/VPN** and no API automation on Live Funded. Tradeify/Lucid: bots allowed (sole-owned, non-HFT). FundedNext: two official pages contradict each other — don't assume | A copier distributing a **human's** manual trades is the sanctioned pattern nearly everywhere; a fully automated publisher is not. Know which one your setup is, per firm |
| **News blackout windows** | MFFU: no positions **or orders** ±2 min around T1 events (Rapid/Pro sim-funded only). TPT: flat + no orders ±1 min (PRO/PRO+). Alpha: no orders ±2 min (Qualified non-Advanced; first violation voids profits, second breaches). Lucid Daily: ±1 min. Apex: directional news trading fine, both-sides "gambling" banned. Topstep: no flatten requirement, but don't swing full size into a release; CPI pre-release entry restrictions on index products | Needs an economic calendar — not wired into the router. Use the **AI gate** with event instructions, or `P` pause around releases you trade through |
| **Minimum hold times** | Elite: **10-second minimum per trade, no exceptions**. FundedNext: profitable <10 s trades deducted at payout. Lucid: flagged if >50 % of profits from ≤5 s holds. Tradeify: payout gate unless >50 % of trades & profit from >10 s holds | Publisher strategy design — exits are never delayed by the router (deliberately) |
| **No-DCA / averaging** | Elite bans adding to losers (*"Martingale, Dollar-Cost Averaging, or substantially similar"*). FundedNext allows structured DCA; Lucid discourages martingale | If a publisher strategy scales into losers, don't route it to Elite accounts |
| **Consistency percentages** | Payout/pass gates only — never order-flow violations: Apex 50 %, Topstep Combine 50 % / XFA fast-path 40 %, MFFU 50 % evals, TPT test 50 %, Tradeify 20–40 %, Bulenox 40 %, Elite per-plan formula, FundedNext 40 %, Alpha 20–50 %, Lucid 20–60 % | Telemetry/planning concern, not enforcement; no account is closed for these |
| **Account-count / identity caps** | Apex ≤20 PAs (payout-ineligible beyond); Topstep ≤5 XFA, 1 LFA (copier auto-unlinks at payout and must be re-enabled manually); MFFU 3–5 sim-funded; TPT ≤5 PRO; Tradeify ≤5 funded per household; Elite ≤5; FundedNext ≤5 per household; Alpha ≤3 Qualified; Lucid ≤10; Bulenox 3→11 masters, one Rithmic ID | Configuration discipline: copy only your own accounts, one identity, within caps |
| **Weekly activity minimums** | TPT PRO: ≥1 round-trip per calendar week; Tradeify: ≥1 trade/week or deletion; Apex: 2×$50-profit days per rolling 30 days; Topstep XFA/LFA: 30-day inactivity closure | Operator calendar |

---

## 4. Per-firm one-liners a router integrator should know

- **Apex (4.0, since 2026-03-01):** hedging/opposing-correlated = immediate closure; automation = closure + forfeiture; copy own accounts OK (20-PA cap); flat 4:59 PM ET; 50 % consistency payout gate; legacy pre-2026-03 accounts still carry the old 30 %/5:1/One-Direction rules. Contract cap is **portfolio-wide across instruments** and scales by daily level; oversize orders auto-reject.
- **Topstep:** current accounts are **TopstepX-only — NinjaTrader cannot connect to new accounts** (this copier can only drive legacy NT-connected ones); cross-account hedge has an escalation ladder ending in unappealable closure; flat by 4:10 PM ET; no VPS/VPN.
- **MFFU:** holding past 4:10 PM ET **breaches the account** — the single sharpest deadline in this file; ±2 min T1 news rule on sim-funded Rapid/Pro; combined mini+micro cap evasion is a breach (the router's twin-close covers the position side of this).
- **Take Profit Trader:** *all* automation banned — manual execution only; ±1 min news rule on PRO; auto-close 4:55 PM ET; counter-positions rule liquidates the account and forfeits profits.
- **Tradeify:** product-group hedging ban (ES-vs-NQ counts) with explicit warning that **a copier configured to invert, or a leader flattened while followers stay open, is itself a violation**; flat 4:59 PM ET; bots allowed if sole-owned.
- **Bulenox:** one Rithmic ID for everything; third-party algos need approval; flat 4:59 PM ET; 40 % consistency at payout.
- **Elite:** 10-second minimum hold; DCA banned; approved-copier list (Replikanto **compliance edition** only); Tier 1/Tier 2 correlation table; per-instrument flat 1 min before its close.
- **FundedNext:** micro-scalping deductions; automation pages contradict — treat bots as unsafe; cross-firm copying of your own trades explicitly allowed.
- **Alpha:** flat **4:20 PM ET**; ±2 min news on Qualified non-Advanced; bots banned; same-account NQ/MNQ inverse named banned.
- **Lucid:** flat 4:45 PM ET (sim); hedging matrix distinguishes same-account (looser) from cross-account (strict); copiers and automation allowed.

---

## 5. Verification status

High-confidence (fetched or quoted from official pages): Topstep (all articles fetched directly), MFFU, Tradeify, Elite, FundedNext, Alpha (fetched); TPT and Lucid (official articles via search snippets — help centers block fetching); Apex (official articles via search index — domain blocks fetching; 4.0 regime change corroborated by multiple independents).

**Open items marked UNVERIFIED in the underlying research:** Apex 4.0 per-size eval contract table and payout-ladder amounts; Apex semi-automation carve-out status under 4.0; Topstep NinjaTrader discontinuation/possible-return dates; TPT per-tier contract table and ATM-bracket stance; Bulenox official hedging wording and the $10K flatten exception; FundedNext automation contradiction; Lucid per-size caps.

Full research reports (with per-claim URLs and quotes) were produced on 2026-08-14 across three sweeps: Apex+Topstep; MFFU+TPT+Tradeify; Bulenox+Elite+FundedNext+Alpha+Lucid+cross-firm. Key sources: help.topstep.com (arts. 13747047, 10296582, 8284206, 8284197, …), apextraderfunding.com/help-center + support.apextraderfunding.com (arts. 40463541656603, 46729420990235, 40463473205275, …), help.myfundedfutures.com (arts. 12011241, 9558251, 10244682, 8230009, 11994562, …), takeprofittraderhelp.zendesk.com (Rule 2–6, UTP 34431153546397, copier policy 34431176505245), help.tradeify.co (arts. 10495868, 10468320, 10495876, 10495874, …), bulenox.com/help + bulenox.help, elitetraderfunding.app/terms-of-service + /help, helpfutures.fundednext.com (arts. 14298337, 14298572, 14298560, …), help.alpha-futures.com (arts. 9508585, 9492063, 9492096, …), support.lucidtrading.com (arts. 11404734, 11404729, …).
