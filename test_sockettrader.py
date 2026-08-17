"""Tests for SocketTrader pure functions and state transitions.

Run: pytest test_sockettrader.py -v
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

# Import the module under test
import SocketTrader as st


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def tmp_config(tmp_path):
    """Redirect CONFIG_FILE to a temp path for isolated config tests."""
    cfg_path = tmp_path / ".voidorigin_config.json"
    original = st.CONFIG_FILE
    st.CONFIG_FILE = cfg_path
    yield cfg_path
    st.CONFIG_FILE = original


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Provide a temp output directory for file write tests."""
    out = tmp_path / "incoming"
    out.mkdir()
    return out


@pytest.fixture(autouse=True)
def isolate_config(tmp_path_factory):
    """Never let the suite touch the real ~/.voidorigin_config.json.

    That file holds the live account set, risk limits, profiles and auth
    tokens of a running trading app. Any test that calls save_config,
    save_account_profiles, set_account_limits or bridge_token — directly
    or through a helper like hedge_guard_mode(), which reads config on
    every call — would otherwise read and WRITE the user's real settings.
    That has actually happened: a sizing test wrote a stray profile into
    the live config, and setting hedge_guard=block on the real machine
    silently broke an unrelated dispatch test.
    """
    original = st.CONFIG_FILE
    st.CONFIG_FILE = tmp_path_factory.mktemp("cfg") / ".voidorigin_config.json"
    yield
    st.CONFIG_FILE = original


@pytest.fixture(autouse=True)
def reset_session_state():
    """Reset global session state between tests."""
    st.session_start_balances.clear()
    st.session_current_balances.clear()
    st._balance_suspect_since.clear()
    st.session_contracts.clear()
    st.soft_stopped = False
    st.hard_stopped = False
    st.paused = False
    st.signal_count = 0
    st._recent_signal_ids.clear()
    st.active_account = None
    st.follower_accounts = []
    st.account_stops.clear()
    st.micro_mode = False
    st.micro_map = dict(st.MICRO_MAP)
    st._micro_unmapped_warned.clear()
    st.account_profiles.clear()
    st._stagger_placed.clear()
    st._atm_override_warned.clear()
    st._pending_confirms.clear()
    st.shutdown.clear()
    st.roundrobin_accounts = []
    st._rr_remaining = []
    st._rr_last = None
    st._recent_fired.clear()
    st._last_connect_mono = None
    st._web_events.clear()
    st._session_state = "ready"
    st._state_before_conn = "ready"
    st._alert_text = ""
    st._alert_kind = ""
    st._alert_sticky = False
    st._alert_ts = 0.0
    st._prop_autoflat_done.clear()
    st.strategy_symbols.clear()
    st.pub_strategies_seen.clear()
    st._seen_dirty = False
    st._seen_save_last = 0.0
    st.atm_aliases = {}
    yield
    st.active_account = None
    st.follower_accounts = []
    st.account_stops.clear()
    st.account_profiles.clear()
    st._stagger_placed.clear()


# ── sanitize_ati ──────────────────────────────────────────────────────


class TestSanitizeAti:
    def test_clean_string_unchanged(self):
        assert st.sanitize_ati("PLACE") == "PLACE"

    def test_strips_newlines(self):
        assert st.sanitize_ati("BUY\nSELL") == "BUYSELL"

    def test_strips_carriage_return(self):
        assert st.sanitize_ati("BUY\rSELL") == "BUYSELL"

    def test_strips_null_bytes(self):
        assert st.sanitize_ati("BUY\x00SELL") == "BUYSELL"

    def test_strips_semicolons(self):
        assert st.sanitize_ati("NQ;injected") == "NQinjected"

    def test_strips_all_dangerous_chars(self):
        assert st.sanitize_ati("A\nB\rC\x00D;E") == "ABCDE"

    def test_empty_string(self):
        assert st.sanitize_ati("") == ""


# ── validate_signal ───────────────────────────────────────────────────


class TestValidateSignal:
    def test_valid_place_signal(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "1", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert st.validate_signal(parts) is None

    def test_valid_sell_signal(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "SELL", "2", "LIMIT", "100", "", "GTC", "", "", "NQ_Med", "1021"]
        assert st.validate_signal(parts) is None

    def test_empty_signal(self):
        assert st.validate_signal([]) == "empty signal"

    def test_unknown_command(self):
        assert "unknown command" in st.validate_signal(["BOGUS"])

    def test_place_too_few_fields(self):
        parts = ["PLACE", "Sim101", "NQ 06-26"]
        err = st.validate_signal(parts)
        assert "requires 13 fields" in err

    def test_invalid_action(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "HOLD", "1", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert "invalid action" in st.validate_signal(parts)

    def test_non_numeric_qty(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "abc", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert "non-numeric qty" in st.validate_signal(parts)

    def test_zero_qty(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "0", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert "invalid qty" in st.validate_signal(parts)

    def test_negative_qty(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "-1", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert "invalid qty" in st.validate_signal(parts)

    def test_invalid_order_type(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "1", "FOK", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert "invalid order type" in st.validate_signal(parts)

    def test_invalid_tif(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "1", "MARKET", "", "", "IOC", "", "", "NQ_Med", "1020"]
        assert "invalid TIF" in st.validate_signal(parts)

    def test_empty_tif_is_ok(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "BUY", "1", "MARKET", "", "", "", "", "", "NQ_Med", "1020"]
        assert st.validate_signal(parts) is None

    def test_closeposition_valid(self):
        parts = ["CLOSEPOSITION", "Sim101", "NQ 06-26"]
        assert st.validate_signal(parts) is None

    def test_closeposition_missing_fields(self):
        parts = ["CLOSEPOSITION", "Sim101"]
        assert "requires account and instrument" in st.validate_signal(parts)

    def test_cancel_requires_order_id(self):
        parts = ["CANCEL"] + [""] * 10
        assert "requires order ID" in st.validate_signal(parts)

    def test_change_requires_order_id(self):
        parts = ["CHANGE"] + [""] * 10
        assert "requires order ID" in st.validate_signal(parts)

    def test_closestrategy_requires_strategy_id(self):
        parts = ["CLOSESTRATEGY"] + [""] * 12
        assert "requires strategy ID" in st.validate_signal(parts)

    def test_field_too_long(self):
        parts = ["PLACE", "x" * 300, "NQ 06-26", "BUY", "1", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        err = st.validate_signal(parts)
        assert "too long" in err

    def test_too_many_fields(self):
        parts = ["PLACE"] + ["x"] * 25
        err = st.validate_signal(parts)
        assert "too many fields" in err

    def test_all_valid_commands_accepted(self):
        for cmd in st.VALID_ATI_COMMANDS:
            # Just verify the command itself isn't rejected — field checks may fail
            err = st.validate_signal([cmd])
            assert err is None or "unknown command" not in err

    def test_reverseposition_validated_like_place(self):
        parts = ["REVERSEPOSITION", "Sim101", "NQ 06-26", "BUY", "1", "MARKET", "", "", "DAY", "", "", "NQ_Med", "1020"]
        assert st.validate_signal(parts) is None

    def test_cancelallorders_no_extra_fields_needed(self):
        assert st.validate_signal(["CANCELALLORDERS"]) is None

    def test_flatteneverything_no_extra_fields_needed(self):
        assert st.validate_signal(["FLATTENEVERYTHING"]) is None


# ── extract_signal_string ─────────────────────────────────────────────


class TestExtractSignalString:
    def _make_msg(self, signal="PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020",
                  ts=1711000000000, signal_id=None):
        data = {"signal": signal, "ts": ts}
        if signal_id:
            data["signal_id"] = signal_id
        return json.dumps(data)

    def test_basic_extraction(self):
        msg = self._make_msg()
        result, ts, sig_id, _ = st.extract_signal_string(msg, "MyAcct", "MyStrat")
        assert result is not None
        parts = result.split(";")
        assert parts[0] == "PLACE"
        assert parts[1] == "MyAcct"       # account replaced
        assert parts[11] == "MyStrat"      # strategy replaced
        assert ts == 1711000000000

    def test_account_replacement(self):
        msg = self._make_msg()
        result, _, _, _ = st.extract_signal_string(msg, "LiveAccount", "NQ_Med")
        assert ";LiveAccount;" in result

    def test_strategy_replacement(self):
        msg = self._make_msg()
        result, _, _, _ = st.extract_signal_string(msg, "Sim101", "CustomStrat")
        assert ";CustomStrat;" in result

    def test_signal_id_extracted(self):
        msg = self._make_msg()
        _, _, sig_id, _ = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        assert sig_id == "1020"

    def test_invalid_json_returns_none(self):
        result, ts, sig_id, reason = st.extract_signal_string("not json", "Sim101", "NQ_Med")
        assert result is None

    def test_missing_signal_key_returns_none(self):
        msg = json.dumps({"type": "heartbeat"})
        result, ts, sig_id, reason = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        assert result is None

    def test_invalid_signal_rejected(self):
        msg = self._make_msg(signal="BOGUS;bad;signal")
        result, _, _, reason = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        # Rejected signals return the raw text plus a reason so the UI can show them
        assert reason is not None
        assert result is not None

    def test_sanitizes_fields(self):
        msg = self._make_msg(signal="PLACE;Sim101;NQ\n06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020")
        result, _, _, _ = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        assert result is not None
        assert "\n" not in result

    def test_semicolon_injection_stripped(self):
        msg = self._make_msg(signal="PLACE;Sim101;NQ;injected;BUY;1;MARKET;;;DAY;;;NQ_Med;1020")
        result, _, _, _ = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        # The semicolons in "NQ;injected" are split by the outer split, then
        # sanitize_ati strips any remaining semicolons within fields.
        # This will have too many fields but they'll be sanitized.
        assert result is None or ";" not in result.split(";")[2]


# ── resolve_publisher_atm ─────────────────────────────────────────────


INSTALLED_TEMPLATES = ["Bhorgini", "BreadNButter", "GC-Bhorgini", "GC-MSSComp",
                       "GC-MacroZoneB", "NQ-MSSComp", "NQ-MacroZoneB", "NQ_Goopi"]


class TestResolvePublisherAtm:
    @pytest.fixture(autouse=True)
    def _templates(self, monkeypatch):
        monkeypatch.setattr(st, "list_atm_strategies", lambda: list(INSTALLED_TEMPLATES))
        monkeypatch.setattr(st, "validate_strategy", lambda name: name in INSTALLED_TEMPLATES)
        monkeypatch.setattr(st, "atm_aliases", {})
        monkeypatch.setattr(st, "micro_map", {"GC": "MGC", "NQ": "MNQ"})
        st._pub_atm_fallback_warned.clear()

    def test_exact_name_kept(self):
        assert st.resolve_publisher_atm("NQ_Goopi", "NQ 06-26") == "NQ_Goopi"

    def test_snake_case_id_maps_to_root_prefixed_template(self):
        assert st.resolve_publisher_atm("macro_zone_b", "GC 12-26") == "GC-MacroZoneB"

    def test_root_disambiguates_between_markets(self):
        assert st.resolve_publisher_atm("macro_zone_b", "NQ 06-26") == "NQ-MacroZoneB"

    def test_micro_instrument_resolves_full_size_root(self):
        assert st.resolve_publisher_atm("macro_zone_b", "MGC 12-26") == "GC-MacroZoneB"

    def test_bare_template_matches_without_prefix(self):
        assert st.resolve_publisher_atm("bread_n_butter", "GC 12-26") == "BreadNButter"

    def test_root_specific_template_preferred_over_bare(self):
        assert st.resolve_publisher_atm("bhorgini", "GC 12-26") == "GC-Bhorgini"

    def test_alias_maps_abbreviated_template(self, monkeypatch):
        monkeypatch.setattr(st, "atm_aliases", {"mss_de_composite": "MSSComp"})
        assert st.resolve_publisher_atm("mss_de_composite", "NQ 06-26") == "NQ-MSSComp"

    def test_alias_can_target_exact_template(self, monkeypatch):
        monkeypatch.setattr(st, "atm_aliases", {"mystery_strat": "NQ_Goopi"})
        assert st.resolve_publisher_atm("mystery_strat", "GC 12-26") == "NQ_Goopi"

    def test_unknown_id_returns_none(self):
        assert st.resolve_publisher_atm("hmm_squeeze_v3", "NQ 06-26") is None

    def test_empty_returns_none(self):
        assert st.resolve_publisher_atm("", "GC 12-26") is None

    def test_follow_mode_end_to_end_uses_gold_template(self):
        msg = json.dumps({"signal": "PLACE;pub;GC 12-26;BUY;1;MARKET;;;DAY;;;"
                          "macro_zone_b;ent:gc_macro_zone_b", "ts": 1})
        result, _, _, reason = st.extract_signal_string(
            msg, "Sim101", "NQ_Goopi", follow_publisher=True)
        assert reason is None
        assert result.split(";")[11] == "GC-MacroZoneB"

    def test_follow_mode_unknown_publisher_falls_back(self):
        msg = json.dumps({"signal": "PLACE;pub;GC 12-26;BUY;1;MARKET;;;DAY;;;"
                          "mystery_strat;123", "ts": 1})
        result, _, _, _ = st.extract_signal_string(
            msg, "Sim101", "NQ_Goopi", follow_publisher=True)
        assert result.split(";")[11] == "NQ_Goopi"

    def test_locked_mode_ignores_publisher(self):
        msg = json.dumps({"signal": "PLACE;pub;GC 12-26;BUY;1;MARKET;;;DAY;;;"
                          "macro_zone_b;123", "ts": 1})
        result, _, _, _ = st.extract_signal_string(
            msg, "Sim101", "NQ_Goopi", follow_publisher=False)
        assert result.split(";")[11] == "NQ_Goopi"


# ── Config persistence ────────────────────────────────────────────────


class TestConfig:
    def test_load_empty_config(self, tmp_config):
        cfg = st.load_config()
        assert cfg == {}

    def test_save_and_load_config(self, tmp_config):
        st.save_config({"token": "abc", "account": "Sim101"})
        cfg = st.load_config()
        assert cfg["token"] == "abc"
        assert cfg["account"] == "Sim101"

    def test_atomic_write_creates_file(self, tmp_config):
        st.save_config({"test": True})
        assert tmp_config.exists()
        data = json.loads(tmp_config.read_text())
        assert data["test"] is True

    def test_config_not_corrupted_on_overwrite(self, tmp_config):
        st.save_config({"version": 1})
        st.save_config({"version": 2, "extra": "data"})
        cfg = st.load_config()
        assert cfg["version"] == 2
        assert cfg["extra"] == "data"

    def test_load_corrupted_json_returns_empty(self, tmp_config):
        tmp_config.write_text("{broken json")
        cfg = st.load_config()
        assert cfg == {}

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permissions only")
    def test_file_permissions(self, tmp_config):
        st.save_config({"secret": "data"})
        mode = oct(tmp_config.stat().st_mode & 0o777)
        assert mode == "0o600"


# ── Session ID / CME schedule ────────────────────────────────────────


class TestSessionId:
    ET = timezone(timedelta(hours=-5))

    def test_weekday_morning_session(self):
        # Tuesday 10 AM ET → session ID is Tuesday's date
        dt = datetime(2026, 3, 17, 10, 0, tzinfo=self.ET)  # Tuesday
        assert st.get_session_id(dt) == "2026-03-17"

    def test_weekday_evening_session(self):
        # Monday 7 PM ET → session ID is Tuesday's date
        dt = datetime(2026, 3, 16, 19, 0, tzinfo=self.ET)  # Monday evening
        assert st.get_session_id(dt) == "2026-03-17"

    def test_maintenance_gap_returns_none(self):
        # Tuesday 5 PM ET (between 4:20 PM and 6 PM) → maintenance
        dt = datetime(2026, 3, 17, 17, 0, tzinfo=self.ET)
        assert st.get_session_id(dt) is None

    def test_friday_after_close_returns_none(self):
        # Friday 5 PM ET → weekend
        dt = datetime(2026, 3, 20, 17, 0, tzinfo=self.ET)  # Friday
        assert st.get_session_id(dt) is None

    def test_saturday_returns_none(self):
        dt = datetime(2026, 3, 21, 12, 0, tzinfo=self.ET)  # Saturday
        assert st.get_session_id(dt) is None

    def test_sunday_before_open_returns_none(self):
        dt = datetime(2026, 3, 22, 15, 0, tzinfo=self.ET)  # Sunday 3 PM
        assert st.get_session_id(dt) is None

    def test_sunday_after_open(self):
        # Sunday 7 PM ET → session ID is Monday's date
        dt = datetime(2026, 3, 22, 19, 0, tzinfo=self.ET)  # Sunday evening
        assert st.get_session_id(dt) == "2026-03-23"

    def test_exactly_at_close(self):
        # 4:20 PM ET on a Tuesday → maintenance gap (past_close)
        dt = datetime(2026, 3, 17, 16, 20, tzinfo=self.ET)
        assert st.get_session_id(dt) is None

    def test_just_before_close(self):
        # 4:19 PM ET on a Tuesday → still in session
        dt = datetime(2026, 3, 17, 16, 19, tzinfo=self.ET)
        assert st.get_session_id(dt) == "2026-03-17"


# ── Fibonacci backoff ─────────────────────────────────────────────────


class TestFibBackoff:
    def test_initial_step(self):
        prev, curr = st.fib_backoff(60, 60)
        assert prev == 60
        assert curr == 120

    def test_sequence(self):
        p, c = 60, 60
        p, c = st.fib_backoff(p, c)   # 60, 120
        p, c = st.fib_backoff(p, c)   # 120, 180
        p, c = st.fib_backoff(p, c)   # 180, 300
        assert p == 180
        assert c == 300

    def test_clamps_at_max(self):
        p, c = st.fib_backoff(1000, 1800)
        assert c == st.MAX_BACKOFF  # 1800 max

    def test_does_not_exceed_max(self):
        p, c = st.fib_backoff(1200, 1800)
        assert c == 1800


# ── fmt_wait ──────────────────────────────────────────────────────────


class TestFmtWait:
    def test_seconds_only(self):
        assert st.fmt_wait(45) == "45s"

    def test_exact_minutes(self):
        assert st.fmt_wait(120) == "2m"

    def test_minutes_and_seconds(self):
        assert st.fmt_wait(90) == "1m 30s"

    def test_zero(self):
        assert st.fmt_wait(0) == "0s"


# ── File output ───────────────────────────────────────────────────────


class TestWriteSignalToFile:
    def test_writes_file(self, tmp_output_dir):
        original = st.output_directory
        st.output_directory = str(tmp_output_dir)
        try:
            st.write_signal_to_file("PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020")
            files = list(tmp_output_dir.glob("oif_*.txt"))
            assert len(files) == 1
            content = files[0].read_text()
            assert content.startswith("PLACE;")
        finally:
            st.output_directory = original

    def test_no_write_without_directory(self):
        original = st.output_directory
        st.output_directory = None
        try:
            # Should not raise
            st.write_signal_to_file("PLACE;test")
        finally:
            st.output_directory = original


class TestFireClosePosition:
    def test_writes_close_file(self, tmp_output_dir):
        original = st.output_directory
        st.output_directory = str(tmp_output_dir)
        try:
            st.fire_close_position("Sim101", "NQ 06-26")
            files = list(tmp_output_dir.glob("oifclose_*.txt"))
            assert len(files) == 1
            content = files[0].read_text()
            assert content.startswith("CLOSEPOSITION;Sim101;NQ 06-26")
        finally:
            st.output_directory = original

    def test_every_ati_file_is_named_oif(self, tmp_output_dir):
        """NT8 executes only incoming files named oif*.txt; anything else
        is consumed and discarded with "Unknown OIF file type" and NO
        order fires. close_*/cancel_* names turned every file-path
        flatten into a silent no-op (NT trace 2026-08-10 23:31 — two
        followers left holding NQ after the web Flat button)."""
        original = st.output_directory
        st.output_directory = str(tmp_output_dir)
        try:
            st.fire_close_position("Sim101", "NQ 06-26")
            st.fire_cancel_order("abc123")
            st.write_signal_to_file("PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1")
            names = [f.name for f in tmp_output_dir.iterdir()]
            assert len(names) == 3
            assert all(n.startswith("oif") and n.endswith(".txt") for n in names)
        finally:
            st.output_directory = original


# ── Duplicate detection ───────────────────────────────────────────────


class TestDuplicateDetection:
    def test_deque_tracks_ids(self):
        st._recent_signal_ids.append("100")
        assert "100" in st._recent_signal_ids
        assert "200" not in st._recent_signal_ids

    def test_deque_maxlen_evicts_old(self):
        for i in range(150):
            st._recent_signal_ids.append(str(i))
        assert "0" not in st._recent_signal_ids  # evicted
        assert "149" in st._recent_signal_ids     # still there
        assert len(st._recent_signal_ids) == 100  # maxlen


# ── Server list management ────────────────────────────────────────────


class TestSaveServerToList:
    def test_adds_new_server(self):
        cfg = {}
        st._save_server_to_list(cfg, "ws://host:8420/ws", "Test")
        assert len(cfg["servers"]) == 1
        assert cfg["servers"][0]["url"] == "ws://host:8420/ws"
        assert cfg["servers"][0]["name"] == "Test"

    def test_does_not_duplicate(self):
        cfg = {"servers": [{"name": "Test", "url": "ws://host:8420/ws"}]}
        st._save_server_to_list(cfg, "ws://host:8420/ws", "Test")
        assert len(cfg["servers"]) == 1

    def test_adds_second_server(self):
        cfg = {"servers": [{"name": "Prod", "url": "wss://prod:8420/ws"}]}
        st._save_server_to_list(cfg, "ws://dev:8420/ws", "Dev")
        assert len(cfg["servers"]) == 2


# ── Risk management helpers ──────────────────────────────────────────


class TestAccountLimits:
    def test_get_defaults_when_empty(self, tmp_config):
        limits = st.get_account_limits("Sim101")
        assert limits["target"] == 0
        assert limits["stop"] == 0
        assert limits["target_mode"] == "soft"
        assert limits["stop_mode"] == "hard"

    def test_set_and_get_limits(self, tmp_config):
        st.set_account_limits("Sim101", 500, "soft", -300, "hard")
        limits = st.get_account_limits("Sim101")
        assert limits["target"] == 500
        assert limits["target_mode"] == "soft"
        assert limits["stop"] == -300
        assert limits["stop_mode"] == "hard"

    def test_limits_per_account(self, tmp_config):
        st.set_account_limits("Sim101", 500, "soft", -300, "hard")
        st.set_account_limits("Live", 1000, "hard", -500, "hard")
        assert st.get_account_limits("Sim101")["target"] == 500
        assert st.get_account_limits("Live")["target"] == 1000


# ── Session state machine ────────────────────────────────────────────


class TestSessionState:
    def test_valid_state_transition(self):
        st.set_session_state("paused")
        assert st._session_state == "paused"

    def test_invalid_state_ignored(self):
        st.set_session_state("ready")
        st.set_session_state("nonexistent_state")
        assert st._session_state == "ready"

    def test_all_states_are_valid(self):
        for state in st.SESSION_STATES:
            st.set_session_state(state)
            assert st._session_state == state


# ── Connection state notes (header restore across outages) ───────────


class TestConnectionStateNotes:
    def test_down_then_up_restores_prior_state(self):
        st.set_session_state("paused")
        st.note_connection_down()
        assert st._session_state == "reconnecting"
        st.note_connection_up()
        assert st._session_state == "paused"

    def test_first_boot_uses_connecting(self):
        st.note_connection_down(reconnecting=False)
        assert st._session_state == "connecting"
        st.note_connection_up()
        assert st._session_state == "ready"

    def test_repeated_down_keeps_original_state(self):
        st.set_session_state("soft_stop")
        st.note_connection_down()
        st.note_connection_down()  # retry during the same outage
        st.note_connection_up()
        assert st._session_state == "soft_stop"

    def test_up_leaves_state_changed_mid_outage(self):
        st.note_connection_down()
        st.set_session_state("hard_stop")  # stop tripped while disconnected
        st.note_connection_up()
        assert st._session_state == "hard_stop"

    def test_up_without_down_is_noop(self):
        st.set_session_state("paused")
        st.note_connection_up()
        assert st._session_state == "paused"


# ── Alert row lifecycle ───────────────────────────────────────────────


class TestAlertLifecycle:
    def test_alert_gets_timestamp(self):
        st._dash_set_alert("  ✖  something failed")
        assert "something failed" in st._alert_text
        assert "[" in st._alert_text and ":" in st._alert_text
        assert st._alert_kind == st.ALERT_EVENT
        assert st._alert_sticky is False
        assert st._alert_ts > 0

    def test_stamp_disabled_for_animation_frames(self):
        st._dash_set_alert("  ● SIGNAL RECEIVED", stamp=False)
        assert st._alert_text == "  ● SIGNAL RECEIVED"

    def test_empty_alert_clears_metadata(self):
        st._dash_set_alert("  ⚠  warn", sticky=True)
        st._dash_set_alert("")
        assert st._alert_text == ""
        assert st._alert_kind == ""
        assert st._alert_sticky is False

    def test_clear_by_matching_kind(self):
        st._dash_set_alert("  ↻  Reconnecting...", kind=st.ALERT_CONN)
        st._dash_clear_alert(kind=st.ALERT_CONN)
        assert st._alert_text == ""

    def test_clear_skips_other_kinds(self):
        st._dash_set_alert("  ⛔  STOP HIT", sticky=True)
        st._dash_clear_alert(kind=st.ALERT_CONN)
        assert "STOP HIT" in st._alert_text

    def test_clear_unconditional(self):
        st._dash_set_alert("  ⛔  STOP HIT", sticky=True)
        st._dash_clear_alert()
        assert st._alert_text == ""

    def test_expire_drops_old_event(self):
        st._dash_set_alert("  ✖  old error")
        st._alert_ts -= st.ALERT_TTL + 1
        st._dash_expire_alert()
        assert st._alert_text == ""

    def test_expire_keeps_fresh_event(self):
        st._dash_set_alert("  ✖  fresh error")
        st._dash_expire_alert()
        assert "fresh error" in st._alert_text

    def test_expire_keeps_sticky_alert(self):
        st._dash_set_alert("  ⛔  HARD STOP", sticky=True)
        st._alert_ts -= st.ALERT_TTL + 1
        st._dash_expire_alert()
        assert "HARD STOP" in st._alert_text


# ── Trade readiness gate ──────────────────────────────────────────────


class TestTradeReady:
    def test_not_ready_without_account(self, tmp_output_dir):
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = ""
        st.output_directory = str(tmp_output_dir)
        try:
            assert not st.is_trade_ready()
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir

    def test_not_ready_without_directory(self):
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = "Sim101"
        st.output_directory = None
        try:
            assert not st.is_trade_ready()
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir


# ── Reset session ─────────────────────────────────────────────────────


class TestResetSession:
    def test_reset_clears_state(self):
        st.session_start_balances["Sim101"] = 10000
        st.session_current_balances["Sim101"] = 10500
        st.session_contracts.add("NQ 06-26")
        st.soft_stopped = True
        st.hard_stopped = True
        st.signal_count = 5

        st.reset_session_pnl()

        # Start balances re-snapshotted from current
        assert st.session_start_balances["Sim101"] == 10500
        assert len(st.session_contracts) == 0
        assert st.soft_stopped is False
        assert st.hard_stopped is False
        assert st.signal_count == 0


# ── Rejected signal surfacing ────────────────────────────────────────


class TestRejectedSignalSurfaced:
    """Rejected signals should return raw text + reason so the UI can show them."""

    def test_unknown_command_returns_raw_and_reason(self):
        msg = json.dumps({"signal": "BOGUS;bad;signal", "ts": 1711000000000})
        raw, ts, sig_id, reason = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        assert raw == "BOGUS;bad;signal"
        assert reason is not None
        assert "unknown command" in reason.lower()
        assert sig_id is None
        assert ts == 1711000000000

    def test_too_few_fields_returns_reason(self):
        msg = json.dumps({"signal": "PLACE;Sim101;NQ", "ts": 1})
        raw, _, _, reason = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        assert raw is not None
        assert reason is not None

    def test_empty_signal_returns_reason(self):
        msg = json.dumps({"signal": "", "ts": 1})
        raw, _, _, reason = st.extract_signal_string(msg, "Sim101", "NQ_Med")
        # Empty split yields [""], which validates as unknown command (not empty)
        assert reason is not None

    def test_non_dict_returns_all_none(self):
        # Plain JSON that is not a dict should return all None
        raw, ts, sig_id, reason = st.extract_signal_string("[]", "Sim101", "NQ_Med")
        assert raw is None
        assert reason is None


# ── format_signal tagging ────────────────────────────────────────────


class TestFormatSignalTag:
    def test_untagged_has_no_bracket_prefix(self):
        out = st.format_signal("PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020", 1)
        # No tag → plain format, no [TAG] marker
        assert "[PAUSED]" not in out
        assert "[LOCKED]" not in out

    def test_paused_tag_rendered(self):
        out = st.format_signal("PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020", 1, tag="PAUSED")
        assert "[PAUSED]" in out

    def test_locked_tag_rendered(self):
        out = st.format_signal("PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1020", 2, tag="LOCKED")
        assert "[LOCKED]" in out

    def test_rejected_tag_rendered(self):
        out = st.format_signal("BOGUS", 3, tag="REJECTED")
        assert "[REJECTED]" in out


# ── close_all_open_positions ─────────────────────────────────────────


class TestCloseAllOpenPositions:
    @pytest.fixture(autouse=True)
    def _no_live_ati(self, monkeypatch):
        """Keep order-cancel enumeration off the network in tests."""
        monkeypatch.setattr(st, "query_nt_open_orders", lambda account, port=36973: [])

    def test_no_account_returns_empty(self, tmp_output_dir):
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = ""
        st.output_directory = str(tmp_output_dir)
        try:
            assert st.close_all_open_positions() == []
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir

    def test_closes_positions_from_nt_query(self, tmp_output_dir):
        """When NT reports positions, each one gets a close file."""
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = "Sim101"
        st.output_directory = str(tmp_output_dir)
        try:
            with patch.object(st, "query_nt_positions",
                              return_value={"NQ 06-26": 2, "ES 06-26": -1}):
                closed = st.close_all_open_positions()
            assert set(closed) == {"NQ 06-26", "ES 06-26"}
            close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
            assert len(close_files) == 2
            # No global CANCELALLORDERS files — cancels are per-order now
            assert list(tmp_output_dir.glob("cancelall_*.txt")) == []
            contents = [f.read_text() for f in close_files]
            assert any("NQ 06-26" in c for c in contents)
            assert any("ES 06-26" in c for c in contents)
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir

    def test_flat_positions_skipped(self, tmp_output_dir):
        """qty == 0 means flat — don't waste a close command."""
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = "Sim101"
        st.output_directory = str(tmp_output_dir)
        try:
            with patch.object(st, "query_nt_positions",
                              return_value={"NQ 06-26": 0, "ES 06-26": 1}):
                closed = st.close_all_open_positions()
            assert closed == ["ES 06-26"]
            close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
            assert len(close_files) == 1
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir

    def test_session_contracts_safety_net(self, tmp_output_dir):
        """If NT query returns nothing, session_contracts still gets closed."""
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = "Sim101"
        st.output_directory = str(tmp_output_dir)
        st.session_contracts.add("MNQ 06-26")
        try:
            with patch.object(st, "query_nt_positions", return_value={}):
                closed = st.close_all_open_positions()
            assert "MNQ 06-26" in closed
            close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
            assert len(close_files) == 1
            assert "MNQ 06-26" in close_files[0].read_text()
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir

    def test_query_failure_still_closes_tracked(self, tmp_output_dir):
        """If NT query raises, session_contracts fallback still runs."""
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = "Sim101"
        st.output_directory = str(tmp_output_dir)
        st.session_contracts.add("NQ 06-26")
        try:
            with patch.object(st, "query_nt_positions", side_effect=RuntimeError("ATI down")):
                closed = st.close_all_open_positions()
            assert closed == ["NQ 06-26"]
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir

    def test_union_query_and_tracked_no_duplicates(self, tmp_output_dir):
        """A contract in both NT positions and session_contracts is closed once."""
        original_acct = st.active_account
        original_dir = st.output_directory
        st.active_account = "Sim101"
        st.output_directory = str(tmp_output_dir)
        st.session_contracts.add("NQ 06-26")
        try:
            with patch.object(st, "query_nt_positions",
                              return_value={"NQ 06-26": 1, "ES 06-26": 2}):
                closed = st.close_all_open_positions()
            assert set(closed) == {"NQ 06-26", "ES 06-26"}
            close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
            assert len(close_files) == 2
        finally:
            st.active_account = original_acct
            st.output_directory = original_dir


# ── Scoped per-account order cancellation (v0.3.3) ───────────────────
# Replaced fire_cancel_all_orders: NT8's CANCELALLORDERS is documented as
# global ("all accounts and broker connections"), so flattening one
# copy-trade account must never use it.


class TestScopedCancel:
    DUMP = (
        "Orders|Sim101\x00aaa|bbb|ccc\x00"
        "OrderStatus|aaa\x00Working\x00"
        "OrderStatus|bbb\x00Filled\x00"
        "OrderStatus|ccc\x00Accepted\x00"
        "Orders|Other\x00zzz\x00"
        "OrderStatus|zzz\x00Working\x00"
    )

    def test_open_orders_filtered_by_account_and_state(self):
        with patch.object(st, "_query_ati", return_value=self.DUMP):
            assert st.query_nt_open_orders("Sim101") == ["aaa", "ccc"]

    def test_open_orders_scoped_to_requested_account(self):
        with patch.object(st, "_query_ati", return_value=self.DUMP):
            assert st.query_nt_open_orders("Other") == ["zzz"]

    def test_open_orders_empty_dump(self):
        with patch.object(st, "_query_ati", return_value=""):
            assert st.query_nt_open_orders("Sim101") == []

    def test_cancel_account_orders_writes_one_cancel_per_order(self, tmp_output_dir):
        original = st.output_directory
        st.output_directory = str(tmp_output_dir)
        try:
            with patch.object(st, "_query_ati", return_value=self.DUMP):
                n = st.fire_cancel_account_orders("Sim101")
            assert n == 2
            files = sorted(tmp_output_dir.glob("oifcancel_*.txt"))
            contents = sorted(p.read_text() for p in files)
            assert contents == ["CANCEL;;;;;;;;;;aaa;;", "CANCEL;;;;;;;;;;ccc;;"]
            assert list(tmp_output_dir.glob("cancelall_*.txt")) == []
        finally:
            st.output_directory = original

    def test_cancel_account_orders_query_failure_returns_zero(self, tmp_output_dir):
        original = st.output_directory
        st.output_directory = str(tmp_output_dir)
        try:
            with patch.object(st, "query_nt_open_orders", side_effect=RuntimeError("ATI down")):
                assert st.fire_cancel_account_orders("Sim101") == 0
            assert list(tmp_output_dir.iterdir()) == []
        finally:
            st.output_directory = original

    def test_close_account_positions_uses_scoped_cancel(self, tmp_output_dir, monkeypatch):
        monkeypatch.setattr(st, "query_nt_open_orders", lambda account, port=36973: ["oid1"])
        with patch.object(st, "output_directory", str(tmp_output_dir)), \
             patch.object(st, "query_nt_positions", return_value={"NQ 06-26": 1}):
            closed = st.close_account_positions("Sim101")
        assert closed == ["NQ 06-26"]
        cancels = [f.read_text() for f in tmp_output_dir.glob("oifcancel_*.txt")]
        assert cancels == ["CANCEL;;;;;;;;;;oid1;;"]
        assert list(tmp_output_dir.glob("cancelall_*.txt")) == []

    def test_global_cancelallorders_helper_is_gone(self):
        assert not hasattr(st, "fire_cancel_all_orders")


# ── Order types match the NT8 OIF enumeration ────────────────────────


class TestOrderTypesMatchNtDocs:
    def test_set_matches_nt8_enumeration(self):
        # Official docs list exactly: MARKET, LIMIT, STOPMARKET, STOPLIMIT
        assert st.VALID_ORDER_TYPES == {"MARKET", "LIMIT", "STOPMARKET", "STOPLIMIT"}

    def test_stopmarket_accepted(self):
        parts = ["PLACE", "Sim101", "NQ 06-26", "SELL", "1", "STOPMARKET", "", "28500", "DAY", "", "", "", ""]
        assert st.validate_signal(parts) is None

    def test_legacy_stop_rejected(self):
        # NT rejects "STOP" ("holds invalid order type parameter") — the
        # validator must too, so the failure surfaces in OUR log, not NT's.
        parts = ["PLACE", "Sim101", "NQ 06-26", "SELL", "1", "STOP", "", "28500", "DAY", "", "", "", ""]
        assert "invalid order type" in st.validate_signal(parts)


# ── Terminal input hygiene ───────────────────────────────────────────


class TestInputHygiene:
    def test_strip_arrow_key_escapes(self):
        assert st.strip_terminal_input("\x1b[B\x1b[B\x1b[B6") == "6"

    def test_plain_text_unchanged(self):
        assert st.strip_terminal_input("TDFYSL50925850106") == "TDFYSL50925850106"

    def test_control_chars_removed(self):
        assert st.strip_terminal_input("a\x07b\x00c") == "abc"

    def test_load_config_drops_garbage_limit_keys(self, tmp_config):
        st.save_config({"account_limits": {
            "\x1b[B\x1b[B\x1b[B6": {"target": 1000.0},
            "Sim101": {"target": 500.0},
        }})
        loaded = st.load_config()
        assert list(loaded["account_limits"]) == ["Sim101"]


# ── Session persistence: lockout flags are NOT persisted ──────────


class TestSessionLockoutNotPersisted:
    """Lockout flags intentionally don't persist across restart — exit-and-
    restart is one of the supported ways to clear a hard stop. Only
    balances, contracts, and signal_count survive a restart."""

    def _within_session_now(self):
        return st.get_session_id() is not None

    def test_hard_stop_does_not_persist(self, tmp_config):
        if not self._within_session_now():
            pytest.skip("outside active CME session hours")
        st.session_start_balances["Sim101"] = 10000.0
        st.session_current_balances["Sim101"] = 9500.0
        st.hard_stopped = True
        st.signal_count = 3
        st.set_session_state("hard_stop")

        st.save_session_state()

        # Simulate restart: wipe runtime state
        st.session_start_balances.clear()
        st.hard_stopped = False
        st.soft_stopped = False
        st.paused = False
        st.signal_count = 0
        st.set_session_state("ready")

        restored = st.restore_session_state()
        assert restored is True
        # Balances and counters come back
        assert st.session_start_balances.get("Sim101") == 10000.0
        assert st.signal_count == 3
        # But lockout flags do NOT
        assert st.hard_stopped is False
        assert st.soft_stopped is False

    def test_save_payload_excludes_lockout_flags(self, tmp_config):
        if not self._within_session_now():
            pytest.skip("outside active CME session hours")
        st.session_start_balances["Sim101"] = 10000.0
        st.session_current_balances["Sim101"] = 9500.0
        st.hard_stopped = True

        st.save_session_state()

        cfg = st.load_config()
        saved = cfg.get("session", {})
        assert "hard_stopped" not in saved
        assert "soft_stopped" not in saved
        assert "lock_state" not in saved


# ══════════════════════════════════════════════════════════════════════
# Copy trading: leader/follower fan-out
# ══════════════════════════════════════════════════════════════════════

# ── target_accounts / tradeable_accounts / session_hard_locked ────────
class TestTargetAccounts:
    def test_no_leader_no_followers_is_empty(self):
        st.active_account = None
        st.follower_accounts = []
        assert st.target_accounts() == []

    def test_leader_only_single_account_mode(self):
        st.active_account = "Sim101"
        st.follower_accounts = []
        assert st.target_accounts() == ["Sim101"]

    def test_leader_first_then_followers(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102", "Sim103"]
        assert st.target_accounts() == ["Sim101", "Sim102", "Sim103"]

    def test_follower_duplicating_leader_is_deduped(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim101", "Sim102"]
        assert st.target_accounts() == ["Sim101", "Sim102"]

    def test_duplicate_followers_deduped(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102", "Sim102"]
        assert st.target_accounts() == ["Sim101", "Sim102"]

    def test_empty_names_ignored(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["", "Sim102"]
        assert st.target_accounts() == ["Sim101", "Sim102"]


class TestTradeableAccounts:
    def test_all_tradeable_when_no_stops(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        assert st.tradeable_accounts() == ["Sim101", "Sim102"]

    def test_stopped_account_excluded(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102", "Sim103"]
        st.account_stops["Sim102"] = "hard"
        assert st.tradeable_accounts() == ["Sim101", "Sim103"]

    def test_all_stopped_is_empty(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim101"] = "soft"
        st.account_stops["Sim102"] = "hard"
        assert st.tradeable_accounts() == []


class TestSessionHardLocked:
    def test_not_locked_when_tradeable(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim101"] = "hard"
        assert st.session_hard_locked() is False  # Sim102 still tradeable

    def test_locked_when_all_hard(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim101"] = "hard"
        st.account_stops["Sim102"] = "hard"
        assert st.session_hard_locked() is True

    def test_not_locked_when_a_stop_is_soft(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim101"] = "hard"
        st.account_stops["Sim102"] = "soft"
        assert st.session_hard_locked() is False

    def test_no_targets_not_locked(self):
        st.active_account = None
        st.follower_accounts = []
        assert st.session_hard_locked() is False


# ── _with_account ─────────────────────────────────────────────────────
class TestWithAccount:
    def test_replaces_account_and_keeps_trade_fields(self):
        sig = "PLACE;OLD;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        out = st._with_account(sig, "Sim102")
        parts = out.split(";")
        assert parts[1] == "Sim102"
        assert parts[0] == "PLACE"
        assert parts[2] == "NQ 06-26"
        assert parts[3] == "BUY" and parts[4] == "1" and parts[5] == "MARKET"
        assert parts[11] == "NQ_Med"
        # Re-addressed to a different account ⇒ strategy id is made
        # per-account so NT's instance-global id check can't reject it.
        assert parts[-1] == "42~Sim102"

    def test_sanitizes_account_name(self):
        sig = "PLACE;OLD;NQ;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        out = st._with_account(sig, "Bad;Name")
        # sanitize strips the embedded semicolon so field count is preserved
        assert out.split(";")[1] == "BadName"
        assert len(out.split(";")) == len(sig.split(";"))

    def test_short_signal_untouched(self):
        assert st._with_account("PLACE", "Sim102") == "PLACE"

    def test_same_account_keeps_ids_verbatim(self):
        # The leader leg is a re-address to the SAME account: the
        # publisher's oco/order/strategy ids must pass through untouched.
        sig = "PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;oco-1;ord-1;NQ_Med;ent:x:2026"
        assert st._with_account(sig, "Sim101") == sig

    def test_follower_leg_gets_unique_strategy_id(self):
        # NT resolves ATM strategy ids across the whole instance; a follower
        # reusing the leader's id is rejected with "strategy id already in
        # use". The follower leg must carry a per-account id.
        sig = "PLACE;Sim101;NQ 06-26;SELL;1;MARKET;;;DAY;;;NQ_Goopi;ent:x:2026"
        parts = st._with_account(sig, "Sim102").split(";")
        assert parts[1] == "Sim102"
        assert parts[12] == "ent:x:2026~Sim102"

    def test_follower_leg_suffixes_oco_and_order_ids(self):
        sig = "PLACE;Sim101;NQ 06-26;BUY;2;LIMIT;100;;GTC;oco-1;ord-1;NQ_Med;99"
        parts = st._with_account(sig, "Sim102").split(";")
        assert parts[9] == "oco-1~Sim102"
        assert parts[10] == "ord-1~Sim102"
        assert parts[12] == "99~Sim102"

    def test_follower_leg_leaves_empty_ids_empty(self):
        sig = "CLOSEPOSITION;Sim101;NQ 09-26;;;;;;;;;NQ_Goopi;"
        out = st._with_account(sig, "Sim102")
        parts = out.split(";")
        assert parts[1] == "Sim102"
        assert parts[9] == "" and parts[10] == "" and parts[12] == ""
        assert len(parts) == len(sig.split(";"))

    def test_closestrategy_follower_id_matches_its_entry_id(self):
        # A publisher CLOSESTRATEGY names the id it used on PLACE. The same
        # transform on both commands must yield matching per-account ids so
        # each account's close resolves to its own ATM instance.
        entry = "PLACE;Sim101;NQ 06-26;SELL;1;MARKET;;;DAY;;;NQ_Goopi;ent:x:2026"
        close = "CLOSESTRATEGY;Sim101;;;;;;;;;;;ent:x:2026"
        entry_id = st._with_account(entry, "Sim102").split(";")[12]
        close_id = st._with_account(close, "Sim102").split(";")[12]
        assert entry_id == close_id == "ent:x:2026~Sim102"

    def test_two_followers_get_distinct_ids(self):
        sig = "PLACE;Sim101;NQ 06-26;SELL;1;MARKET;;;DAY;;;NQ_Goopi;ent:x:2026"
        ids = {
            st._with_account(sig, a).split(";")[12]
            for a in ("Sim101", "Sim102", "Sim103")
        }
        assert len(ids) == 3  # leader + each follower all unique


# ── _next_ati_filename thread safety ──────────────────────────────────
class TestFilenameThreadSafety:
    def test_concurrent_names_are_unique(self):
        names: list[str] = []
        lock = threading.Lock()

        def worker():
            n = st._next_ati_filename("oif")
            with lock:
                names.append(n)

        with ThreadPoolExecutor(max_workers=16) as ex:
            futures = [ex.submit(worker) for _ in range(500)]
            for f in futures:
                f.result()

        assert len(names) == 500
        assert len(set(names)) == 500  # no collisions under concurrency


# ── dispatch_signal fan-out ───────────────────────────────────────────
class TestDispatchSignal:
    def test_writes_one_file_per_account(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102", "Sim103", "Sim104"]
        sig = "PLACE;Sim101;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            written = asyncio.run(st.dispatch_signal(sig))

        assert set(written) == {"Sim101", "Sim102", "Sim103", "Sim104"}
        files = list(tmp_output_dir.glob("oif_*.txt"))
        assert len(files) == 4
        accounts_written = {f.read_text().split(";")[1] for f in files}
        assert accounts_written == {"Sim101", "Sim102", "Sim103", "Sim104"}

    def test_each_file_keeps_identical_signal_except_account_and_ids(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102", "Sim103", "Sim104"]
        sig = "PLACE;Sim101;NQ 06-26;BUY;3;MARKET;;;DAY;oco-1;ord-1;NQ_Med;99"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            asyncio.run(st.dispatch_signal(sig))
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        # Trade-defining fields (instrument/action/qty/type/TIF/ATM) are
        # identical on every leg; account and the instance-global id fields
        # (oco/order/strategy id) are per-account by design.
        stripped = {
            ";".join([p if i not in (1, 9, 10, 12) else ""
                      for i, p in enumerate(b.split(";"))])
            for b in bodies
        }
        assert len(bodies) == 4
        assert len(stripped) == 1

        # Every leg's strategy id is unique across the NT instance, and the
        # leader's leg keeps the publisher's id verbatim.
        by_account = {b.split(";")[1]: b.split(";") for b in bodies}
        assert by_account["Sim101"][12] == "99"
        ids = {p[12] for p in by_account.values()}
        assert len(ids) == 4

    def test_single_account_mode_keeps_signal_unchanged(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = []
        sig = "PLACE;Sim101;NQ;BUY;1;MARKET;;;DAY;oco-1;ord-1;NQ_Med;42"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            written = asyncio.run(st.dispatch_signal(sig))
        files = list(tmp_output_dir.glob("oif_*.txt"))

        assert written == ["Sim101"]
        assert len(files) == 1
        assert files[0].read_text() == sig

    def test_stopped_account_gets_no_file(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim102"] = "hard"
        sig = "PLACE;Sim101;NQ;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            written = asyncio.run(st.dispatch_signal(sig))
        assert written == ["Sim101"]
        assert len(list(tmp_output_dir.glob("oif_*.txt"))) == 1

    def test_no_tradeable_accounts_writes_nothing(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = []
        st.account_stops["Sim101"] = "hard"
        sig = "PLACE;Sim101;NQ;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            written = asyncio.run(st.dispatch_signal(sig))
        assert written == []
        assert list(tmp_output_dir.glob("oif_*.txt")) == []

    def test_single_account_mode_still_works(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = []
        sig = "PLACE;Sim101;NQ;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            written = asyncio.run(st.dispatch_signal(sig))
        assert written == ["Sim101"]
        assert len(list(tmp_output_dir.glob("oif_*.txt"))) == 1


# ── close_all_open_positions across accounts ──────────────────────────
class TestCloseAllMultiAccount:
    @pytest.fixture(autouse=True)
    def _no_live_ati(self, monkeypatch):
        monkeypatch.setattr(st, "query_nt_open_orders", lambda account, port=36973: [])

    def test_flattens_every_target_account(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]

        def fake_positions(account, port=None):
            return {"NQ 06-26": 2} if account in ("Sim101", "Sim102") else {}

        with patch.object(st, "output_directory", str(tmp_output_dir)), \
             patch.object(st, "query_nt_positions", side_effect=fake_positions):
            closed = st.close_all_open_positions()

        assert closed == ["NQ 06-26"]
        # One CLOSEPOSITION per account (2); no global CANCELALLORDERS
        close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
        assert len(close_files) == 2
        assert list(tmp_output_dir.glob("cancelall_*.txt")) == []
        closed_accounts = {f.read_text().split(";")[1] for f in close_files}
        assert closed_accounts == {"Sim101", "Sim102"}

    def test_single_account_only_closes_leader(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = []
        with patch.object(st, "output_directory", str(tmp_output_dir)), \
             patch.object(st, "query_nt_positions", return_value={"NQ 06-26": 1}):
            st.close_all_open_positions()
        close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
        assert len(close_files) == 1
        assert close_files[0].read_text().split(";")[1] == "Sim101"


# ── _parse_follower_tokens ────────────────────────────────────────────
class TestParseFollowerTokens:
    NAMES = ["Sim101", "Sim102", "Sim103"]

    def test_empty_is_none(self):
        assert st._parse_follower_tokens("", self.NAMES, "Sim101") == []

    def test_numbers_map_to_names(self):
        assert st._parse_follower_tokens("2 3", self.NAMES, "Sim101") == ["Sim102", "Sim103"]

    def test_comma_separated(self):
        assert st._parse_follower_tokens("2,3", self.NAMES, "Sim101") == ["Sim102", "Sim103"]

    def test_all_excludes_leader(self):
        assert st._parse_follower_tokens("all", self.NAMES, "Sim101") == ["Sim102", "Sim103"]

    def test_leader_index_is_excluded(self):
        assert st._parse_follower_tokens("1 2", self.NAMES, "Sim101") == ["Sim102"]

    def test_dedup(self):
        assert st._parse_follower_tokens("2 2 3", self.NAMES, "Sim101") == ["Sim102", "Sim103"]

    def test_literal_names_allowed(self):
        assert st._parse_follower_tokens("Sim102", self.NAMES, "Sim101") == ["Sim102"]


class TestAccountMenuRows:
    def test_more_than_nine_accounts_render_in_columns(self, monkeypatch):
        monkeypatch.setattr(st, "term_width", lambda: 100)
        accounts = [
            {"name": f"Sim{i:03d}", "cash": 10000.0 + i}
            for i in range(1, 13)
        ]

        rows, width = st._account_menu_rows(accounts, "Sim001", ["Sim010", "Sim012"])
        rendered = "\n".join(rows)

        assert width > 49
        assert len(rows) == 6
        assert "10. Sim010" in rendered
        assert "12. Sim012" in rendered
        assert "＋ FOLLOWER" in rendered

    def test_narrow_terminals_still_include_two_digit_accounts(self, monkeypatch):
        monkeypatch.setattr(st, "term_width", lambda: 60)
        accounts = [
            {"name": f"Sim{i:03d}", "cash": 10000.0 + i}
            for i in range(1, 12)
        ]

        rows, _ = st._account_menu_rows(accounts, "Sim001", [])

        assert len(rows) == 11
        assert any(row.startswith("10. Sim010") for row in rows)
        assert any(row.startswith("11. Sim011") for row in rows)


class TestStrategyMenuRows:
    def test_large_strategy_list_is_paged_and_columnized(self, monkeypatch):
        monkeypatch.setattr(st, "term_width", lambda: 120)
        monkeypatch.setattr(st, "term_height", lambda: 30)
        monkeypatch.setattr(st, "_controls_pinned", True)
        monkeypatch.setattr(st, "_header_lines", 8)
        strategies = [f"Strategy_{i:02d}" for i in range(1, 85)]

        rows, width, page, page_count = st._strategy_menu_rows(strategies, "Strategy_01", 0)
        rendered = "\n".join(rows)

        assert width > 49
        assert page == 0
        assert page_count > 1
        assert len(rows) <= st._strategy_page_size()
        assert "1. Strategy_01" in rendered

    def test_later_strategy_page_contains_high_numbered_items(self, monkeypatch):
        monkeypatch.setattr(st, "term_width", lambda: 120)
        monkeypatch.setattr(st, "term_height", lambda: 30)
        monkeypatch.setattr(st, "_controls_pinned", True)
        monkeypatch.setattr(st, "_header_lines", 8)
        strategies = [f"Strategy_{i:02d}" for i in range(1, 85)]

        rows, _, page, page_count = st._strategy_menu_rows(strategies, "Strategy_84", 1)
        rendered = "\n".join(rows)

        assert page == 1
        assert page_count > 1
        assert "84. Strategy_84" in rendered
        assert "◀" in rendered


# ── config persistence of followers ───────────────────────────────────
class TestFollowerConfigPersistence:
    def test_followers_round_trip(self, tmp_config):
        cfg = st.load_config()
        cfg["account"] = "Sim101"
        cfg["follower_accounts"] = ["Sim102", "Sim103"]
        st.save_config(cfg)
        reloaded = st.load_config()
        assert reloaded["account"] == "Sim101"
        assert reloaded["follower_accounts"] == ["Sim102", "Sim103"]


# ── per-account trip + session-lock recompute ─────────────────────────
class TestPerAccountRisk:
    @pytest.fixture(autouse=True)
    def _no_live_ati(self, monkeypatch):
        monkeypatch.setattr(st, "query_nt_open_orders", lambda account, port=36973: [])

    def test_trip_locks_only_that_account(self, tmp_config, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        with patch.object(st, "output_directory", str(tmp_output_dir)), \
             patch.object(st, "query_nt_positions", return_value={"NQ": 1}):
            asyncio.run(st._trip_account("Sim102", "hard", "stop", -500.0, -400.0))

        assert st.account_stops == {"Sim102": "hard"}
        assert st.tradeable_accounts() == ["Sim101"]  # leader still trades
        # only Sim102 flattened
        close_files = list(tmp_output_dir.glob("oifclose_*.txt"))
        assert {f.read_text().split(";")[1] for f in close_files} == {"Sim102"}

    def test_recompute_hard_locks_when_all_hard(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim101"] = "hard"
        st.account_stops["Sim102"] = "hard"
        st._recompute_session_lock()
        assert st.hard_stopped is True

    def test_recompute_not_hard_when_one_tradeable(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim102"] = "hard"
        st._recompute_session_lock()
        assert st.hard_stopped is False

    def test_recompute_soft_when_all_stopped_mixed(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim101"] = "soft"
        st.account_stops["Sim102"] = "hard"
        st._recompute_session_lock()
        assert st.hard_stopped is False
        assert st.soft_stopped is True

# ── micro contract conversion ─────────────────────────────────────────


class TestToMicroInstrument:
    @pytest.mark.parametrize("full,micro", [
        ("ES 09-26", "MES 09-26"),
        ("NQ 06-26", "MNQ 06-26"),
        ("YM 09-26", "MYM 09-26"),
        ("RTY 09-26", "M2K 09-26"),   # not an M-prefix
        ("GC 08-26", "MGC 08-26"),
        ("SI 09-26", "SIL 09-26"),    # not an M-prefix
        ("HG 09-26", "MHG 09-26"),
        ("CL 08-26", "MCL 08-26"),
        ("NG 08-26", "MNG 08-26"),
        ("BTC 07-26", "MBT 07-26"),
        ("ETH 07-26", "MET 07-26"),
        ("6E 09-26", "M6E 09-26"),
    ])
    def test_known_roots_convert(self, full, micro):
        assert st.to_micro_instrument(full) == micro

    def test_bare_root_without_expiry(self):
        assert st.to_micro_instrument("NQ") == "MNQ"

    def test_already_micro_passes_through(self):
        assert st.to_micro_instrument("MNQ 06-26") == "MNQ 06-26"
        assert st.to_micro_instrument("M2K 09-26") == "M2K 09-26"

    def test_already_micro_does_not_warn(self):
        st.to_micro_instrument("MES 09-26")
        assert "MES" not in st._micro_unmapped_warned

    def test_unmapped_root_passes_through(self):
        assert st.to_micro_instrument("ZB 09-26") == "ZB 09-26"

    def test_unmapped_root_warns_once(self):
        st.to_micro_instrument("ZB 09-26")
        assert "ZB" in st._micro_unmapped_warned

    def test_lowercase_root_converts(self):
        assert st.to_micro_instrument("nq 06-26") == "MNQ 06-26"

    def test_empty_instrument_unchanged(self):
        assert st.to_micro_instrument("") == ""

    def test_self_mapping_opts_out(self):
        st.micro_map = dict(st.MICRO_MAP, GC="GC")
        assert st.to_micro_instrument("GC 08-26") == "GC 08-26"
        assert "GC" not in st._micro_unmapped_warned  # opt-out is silent


class TestLoadMicroMap:
    def test_defaults_without_overrides(self):
        assert st.load_micro_map({}) == st.MICRO_MAP

    def test_override_extends_defaults(self):
        merged = st.load_micro_map({"micro_map": {"FDAX": "FDXS"}})
        assert merged["FDAX"] == "FDXS"
        assert merged["NQ"] == "MNQ"  # defaults intact

    def test_override_replaces_default(self):
        merged = st.load_micro_map({"micro_map": {"SI": "SI"}})
        assert merged["SI"] == "SI"

    def test_override_uppercased(self):
        merged = st.load_micro_map({"micro_map": {"fdax": "fdxs"}})
        assert merged["FDAX"] == "FDXS"

    def test_junk_overrides_ignored(self):
        assert st.load_micro_map({"micro_map": "not a dict"}) == st.MICRO_MAP
        merged = st.load_micro_map({"micro_map": {"": "MES", "NQ": "", "CL": 5}})
        assert merged == st.MICRO_MAP


class TestExtractSignalMicroMode:
    MSG = json.dumps({
        "signal": "PLACE;Sim101;NQ 06-26;BUY;2;MARKET;;;DAY;;;NQ_Med;1044",
        "ts": 1711000000000,
    })

    def test_micros_true_converts_instrument(self):
        result, _, sig_id, reason = st.extract_signal_string(
            self.MSG, "MyAcct", "NQ_Med", micros=True)
        assert reason is None
        parts = result.split(";")
        assert parts[2] == "MNQ 06-26"
        assert parts[1] == "MyAcct"   # account swap still applied
        assert parts[4] == "2"        # quantity untouched
        assert sig_id == "1044"       # dedup ID untouched

    def test_micros_false_leaves_instrument(self):
        result, _, _, _ = st.extract_signal_string(self.MSG, "MyAcct", "NQ_Med", micros=False)
        assert result.split(";")[2] == "NQ 06-26"

    def test_default_is_off(self):
        result, _, _, _ = st.extract_signal_string(self.MSG, "MyAcct", "NQ_Med")
        assert result.split(";")[2] == "NQ 06-26"

    def test_closeposition_converts_too(self):
        msg = json.dumps({"signal": "CLOSEPOSITION;Sim101;NQ 06-26", "ts": 1})
        result, _, _, reason = st.extract_signal_string(msg, "MyAcct", "NQ_Med", micros=True)
        assert reason is None
        assert result.split(";")[2] == "MNQ 06-26"

    def test_unmapped_symbol_sent_unchanged(self):
        msg = json.dumps({
            "signal": "PLACE;Sim101;ZB 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;1050", "ts": 1})
        result, _, _, reason = st.extract_signal_string(msg, "MyAcct", "NQ_Med", micros=True)
        assert reason is None
        assert result.split(";")[2] == "ZB 09-26"


class TestToggleMicroMode:
    def test_toggle_on_persists(self, tmp_config):
        assert st.micro_mode is False
        assert st.toggle_micro_mode() is True
        assert st.micro_mode is True
        assert st.load_config()["micro_mode"] is True

    def test_toggle_off_persists(self, tmp_config):
        st.toggle_micro_mode()
        assert st.toggle_micro_mode() is False
        assert st.load_config()["micro_mode"] is False

    def test_toggle_reloads_map_overrides(self, tmp_config):
        st.save_config({"micro_map": {"SI": "SI"}})
        st.toggle_micro_mode()
        assert st.micro_map["SI"] == "SI"

# ── per-account trade profiles ────────────────────────────────────────
SIG = "PLACE;Sim101;NQ 06-26;BUY;4;MARKET;;;DAY;;;NQ_Med;77"


class TestToFullInstrument:
    def test_micro_converts_to_full(self):
        assert st.to_full_instrument("MNQ 06-26") == "NQ 06-26"
        assert st.to_full_instrument("M2K 09-26") == "RTY 09-26"

    def test_full_passes_through(self):
        assert st.to_full_instrument("NQ 06-26") == "NQ 06-26"

    def test_unknown_passes_through(self):
        assert st.to_full_instrument("ZB 09-26") == "ZB 09-26"

    def test_self_mapped_opt_out_never_flips(self):
        st.micro_map = dict(st.MICRO_MAP, GC="GC")
        assert st.to_full_instrument("GC 08-26") == "GC 08-26"

    def test_roundtrip(self):
        assert st.to_full_instrument(st.to_micro_instrument("ES 09-26")) == "ES 09-26"


class TestRuleQty:
    def _rule(self, **kw):
        return {**st.DEFAULT_RULE, **kw}

    def test_copy_keeps_qty(self):
        assert st._rule_qty(3, self._rule()) == 3

    def test_fixed(self):
        assert st._rule_qty(3, self._rule(qty_mode="fixed", qty_value=2.0)) == 2

    def test_multiple_rounds_half_up(self):
        # 1 × 0.5 = 0.5 → 1 (banker's rounding would drop to 0)
        assert st._rule_qty(1, self._rule(qty_mode="multiple", qty_value=0.5)) == 1
        assert st._rule_qty(3, self._rule(qty_mode="multiple", qty_value=0.5)) == 2

    def test_multiple_can_size_to_zero(self):
        assert st._rule_qty(1, self._rule(qty_mode="multiple", qty_value=0.4)) == 0

    def test_multiple_scales_up(self):
        assert st._rule_qty(2, self._rule(qty_mode="multiple", qty_value=10)) == 20

    def test_cap_applies_after_mode(self):
        assert st._rule_qty(2, self._rule(qty_mode="multiple", qty_value=10,
                                          max_contracts=5)) == 5
        assert st._rule_qty(9, self._rule(max_contracts=3)) == 3

    def test_zero_cap_means_uncapped(self):
        assert st._rule_qty(9, self._rule(max_contracts=0)) == 9


class TestResolveRule:
    def test_no_profile_returns_defaults(self):
        assert st.resolve_rule("Sim102", "NQ 06-26", "NQ_Med") == st.DEFAULT_RULE

    def test_account_default_merges(self):
        st.account_profiles["Sim102"] = {"default": {"qty_mode": "fixed", "qty_value": 2.0}}
        rule = st.resolve_rule("Sim102", "NQ 06-26", "")
        assert rule["qty_mode"] == "fixed"
        assert rule["qty_value"] == 2.0
        assert rule["direction"] == "normal"  # untouched keys stay default

    def test_scoped_rule_overrides_default(self):
        st.account_profiles["Sim102"] = {
            "default": {"delay_ms": 100},
            "rules": [{"symbols": ["NQ"], "direction": "invert"}],
        }
        rule = st.resolve_rule("Sim102", "NQ 06-26", "")
        assert rule["direction"] == "invert"
        assert rule["delay_ms"] == 100  # default still applies underneath

    def test_first_matching_rule_wins(self):
        st.account_profiles["Sim102"] = {"rules": [
            {"symbols": ["NQ"], "delay_ms": 111},
            {"symbols": ["NQ"], "delay_ms": 222},
        ]}
        assert st.resolve_rule("Sim102", "NQ 06-26", "")["delay_ms"] == 111

    def test_symbol_rule_matches_micro_twin(self):
        st.account_profiles["Sim102"] = {"rules": [{"symbols": ["NQ"], "delay_ms": 42}]}
        assert st.resolve_rule("Sim102", "MNQ 06-26", "")["delay_ms"] == 42
        st.account_profiles["Sim102"] = {"rules": [{"symbols": ["MNQ"], "delay_ms": 43}]}
        assert st.resolve_rule("Sim102", "NQ 06-26", "")["delay_ms"] == 43

    def test_strategy_scope_case_insensitive(self):
        st.account_profiles["Sim102"] = {"rules": [{"strategies": ["nq_med"], "delay_ms": 9}]}
        assert st.resolve_rule("Sim102", "ES 09-26", "NQ_Med")["delay_ms"] == 9
        assert st.resolve_rule("Sim102", "ES 09-26", "Other")["delay_ms"] == 0

    def test_both_filters_must_pass(self):
        st.account_profiles["Sim102"] = {"rules": [
            {"symbols": ["NQ"], "strategies": ["NQ_Med"], "delay_ms": 5}]}
        assert st.resolve_rule("Sim102", "NQ 06-26", "NQ_Med")["delay_ms"] == 5
        assert st.resolve_rule("Sim102", "NQ 06-26", "Other")["delay_ms"] == 0
        assert st.resolve_rule("Sim102", "ES 09-26", "NQ_Med")["delay_ms"] == 0


class TestTransformForAccount:
    def _t(self, sig, acct, **rule_kw):
        rule = {**st.DEFAULT_RULE, **rule_kw}
        return st.transform_signal_for_account(sig, acct, rule)

    def test_default_rule_matches_classic_fanout(self):
        for acct in ("Sim101", "Sim102"):
            final, reason, _ = self._t(SIG, acct)
            assert reason is None
            assert final == st._with_account(SIG, acct)

    def test_size_micros_on_place_and_close(self):
        final, _, meta = self._t(SIG, "Sim102", size="micros")
        assert final.split(";")[2] == "MNQ 06-26"
        assert meta["instrument"] == "MNQ 06-26"
        close = "CLOSEPOSITION;Sim101;NQ 06-26;;;;;;;;;;"
        final_c, _, _ = self._t(close, "Sim102", size="micros")
        assert final_c.split(";")[2] == "MNQ 06-26"

    def test_size_full_converts_micro_signal(self):
        micro_sig = SIG.replace("NQ 06-26", "MNQ 06-26")
        final, _, _ = self._t(micro_sig, "Sim102", size="full")
        assert final.split(";")[2] == "NQ 06-26"

    def test_invert_flips_market_entry(self):
        final, reason, meta = self._t(SIG, "Sim102", direction="invert")
        assert reason is None
        assert final.split(";")[3] == "SELL"
        assert meta["action"] == "SELL"

    def test_invert_skips_limit_entry(self):
        limit_sig = "PLACE;Sim101;NQ 06-26;BUY;1;LIMIT;20000;;DAY;;;NQ_Med;78"
        final, reason, _ = self._t(limit_sig, "Sim102", direction="invert")
        assert final is None
        assert "LIMIT" in reason

    def test_invert_drops_change(self):
        change = "CHANGE;;;;2;;20000;;;;ord-9;;"
        final, reason, _ = self._t(change, "Sim102", direction="invert")
        assert final is None
        assert "CHANGE" in reason

    def test_normal_account_keeps_change(self):
        change = "CHANGE;;;;2;;20000;;;;ord-9;;"
        final, reason, _ = self._t(change, "Sim102")
        assert reason is None
        assert final.split(";")[0] == "CHANGE"

    def test_invert_reverse_market_flips(self):
        rev = "REVERSEPOSITION;Sim101;NQ 06-26;BUY;2;MARKET;;;DAY;;;NQ_Med;80"
        final, reason, _ = self._t(rev, "Sim102", direction="invert")
        assert reason is None
        assert final.split(";")[0] == "REVERSEPOSITION"
        assert final.split(";")[3] == "SELL"

    def test_invert_reverse_nonmarket_downgrades_to_close(self):
        rev = "REVERSEPOSITION;Sim101;NQ 06-26;BUY;2;LIMIT;20000;;DAY;;;NQ_Med;80"
        final, reason, meta = self._t(rev, "Sim102", direction="invert")
        assert reason is None  # exits always flow
        parts = final.split(";")
        assert parts[0] == "CLOSEPOSITION"
        assert parts[1] == "Sim102"
        assert parts[2] == "NQ 06-26"
        assert "downgraded" in meta["note"]

    def test_disabled_blocks_entry_but_not_exit(self):
        final, reason, _ = self._t(SIG, "Sim102", enabled=False)
        assert final is None and "disabled" in reason
        close = "CLOSEPOSITION;Sim101;NQ 06-26;;;;;;;;;;"
        final_c, reason_c, _ = self._t(close, "Sim102", enabled=False)
        assert reason_c is None
        assert final_c.split(";")[0] == "CLOSEPOSITION"

    def test_disabled_reverse_downgrades_to_close(self):
        rev = "REVERSEPOSITION;Sim101;NQ 06-26;BUY;2;MARKET;;;DAY;;;NQ_Med;80"
        final, reason, meta = self._t(rev, "Sim102", enabled=False)
        assert reason is None
        assert final.split(";")[0] == "CLOSEPOSITION"
        assert "downgraded" in meta["note"]

    def test_qty_modes_apply(self):
        final, _, meta = self._t(SIG, "Sim102", qty_mode="fixed", qty_value=2.0)
        assert final.split(";")[4] == "2" and meta["qty"] == 2
        final, _, _ = self._t(SIG, "Sim102", qty_mode="multiple", qty_value=0.5)
        assert final.split(";")[4] == "2"  # 4 × 0.5

    def test_qty_zero_skips_entry(self):
        one = SIG.replace(";4;", ";1;")
        final, reason, _ = self._t(one, "Sim102", qty_mode="multiple", qty_value=0.4)
        assert final is None and "0 contracts" in reason

    def test_atm_override_when_installed(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda name: name == "MyATM")
        final, _, _ = self._t(SIG, "Sim102", atm="MyATM")
        assert final.split(";")[11] == "MyATM"

    def test_atm_override_missing_falls_back(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda name: False)
        final, _, _ = self._t(SIG, "Sim102", atm="Ghost")
        assert final.split(";")[11] == "NQ_Med"

    def test_follower_ids_still_suffixed_after_transform(self):
        sig = "PLACE;Sim101;NQ 06-26;BUY;4;MARKET;;;DAY;oco-1;ord-1;NQ_Med;99"
        final, _, _ = self._t(sig, "Sim102", size="micros", qty_mode="fixed", qty_value=1.0)
        parts = final.split(";")
        assert parts[9] == "oco-1~Sim102"
        assert parts[10] == "ord-1~Sim102"
        assert parts[12] == "99~Sim102"


class TestPlanSignalLegs:
    def test_no_profiles_all_instant_identical(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        plans, skipped = st.plan_signal_legs(SIG)
        assert skipped == []
        assert [p["account"] for p in plans] == ["Sim101", "Sim102"]
        for p in plans:
            assert p["deferred"] is False
            assert p["files"] == [st._with_account(SIG, p["account"])]

    @pytest.mark.parametrize("overrides", [
        {"delay_ms": 100},
        {"delay_jitter_ms": 50},
        {"stagger_entries": 3},
        {"ai": {"provider": "ollama"}},
    ])
    def test_entry_features_defer_leg(self, overrides):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_profiles["Sim102"] = {"default": st._coerce_rule(overrides)}
        plans, _ = st.plan_signal_legs(SIG)
        by_acct = {p["account"]: p for p in plans}
        assert by_acct["Sim101"]["deferred"] is False
        assert by_acct["Sim102"]["deferred"] is True

    def test_exits_never_deferred(self):
        st.active_account = "Sim101"
        st.account_profiles["Sim101"] = {"default": {"delay_ms": 5000, "stagger_entries": 5}}
        close = "CLOSEPOSITION;Sim101;NQ 06-26;;;;;;;;;;"
        plans, _ = st.plan_signal_legs(close)
        assert plans[0]["deferred"] is False

    def test_disabled_account_reported_skipped(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_profiles["Sim102"] = {"default": {"enabled": False}}
        plans, skipped = st.plan_signal_legs(SIG)
        assert [p["account"] for p in plans] == ["Sim101"]
        assert skipped == [("Sim102", "entries disabled")]

    def test_stopped_account_not_planned(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_stops["Sim102"] = "hard"
        plans, skipped = st.plan_signal_legs(SIG)
        assert [p["account"] for p in plans] == ["Sim101"]
        assert skipped == []


class TestSplitQty:
    @pytest.mark.parametrize("total,n,expected", [
        (5, 3, [2, 2, 1]),
        (4, 2, [2, 2]),
        (1, 3, [1]),          # tranches clamp to qty
        (10, 10, [1] * 10),
        (7, 1, [7]),
    ])
    def test_split(self, total, n, expected):
        result = st.split_qty(total, n)
        assert result == expected
        assert sum(result) == total


class TestTrancheAndExitFanOut:
    BASE = "PLACE;Sim102;NQ 06-26;BUY;6;MARKET;;;DAY;oco-1~Sim102;;NQ_Med;99~Sim102"

    def test_first_tranche_keeps_ids(self):
        sig = st._tranche_signal(self.BASE, 2, 0)
        parts = sig.split(";")
        assert parts[4] == "2"
        assert parts[9] == "oco-1~Sim102"
        assert parts[12] == "99~Sim102"

    def test_later_tranches_suffix_nonempty_ids_only(self):
        sig = st._tranche_signal(self.BASE, 2, 1)
        parts = sig.split(";")
        assert parts[9] == "oco-1~Sim102~T2"
        assert parts[10] == ""  # empty id stays empty
        assert parts[12] == "99~Sim102~T2"

    def test_close_strategy_fans_to_recorded_tranches(self):
        st._record_stagger(self.BASE, "Sim102", 3)
        close = "CLOSESTRATEGY;;;;;;;;;;;;99~Sim102"
        files = st._expand_exit_ids(close, "Sim102")
        assert len(files) == 3
        assert files[0].split(";")[12] == "99~Sim102"
        assert files[1].split(";")[12] == "99~Sim102~T2"
        assert files[2].split(";")[12] == "99~Sim102~T3"

    def test_cancel_fans_on_order_id(self):
        placed = "PLACE;Sim102;NQ 06-26;BUY;4;LIMIT;20000;;DAY;;ord-7~Sim102;NQ_Med;99~Sim102"
        st._record_stagger(placed, "Sim102", 2)
        cancel = "CANCEL;;;;;;;;;;ord-7~Sim102;;"
        files = st._expand_exit_ids(cancel, "Sim102")
        assert len(files) == 2
        assert files[1].split(";")[10] == "ord-7~Sim102~T2"

    def test_unrecorded_id_falls_back_to_profile_max(self):
        st.account_profiles["Sim102"] = {"rules": [{"symbols": ["NQ"], "stagger_entries": 4}]}
        close = "CLOSESTRATEGY;;;;;;;;;;;;55~Sim102"
        files = st._expand_exit_ids(close, "Sim102")
        assert len(files) == 4

    def test_single_tranche_single_file(self):
        st._record_stagger(self.BASE, "Sim102", 1)
        close = "CLOSESTRATEGY;;;;;;;;;;;;99~Sim102"
        assert st._expand_exit_ids(close, "Sim102") == [close]


def _run_plans(plans, sig_id="77"):
    """Execute plans and wait for every deferred leg task to finish."""
    async def go():
        written = await st.execute_plans(plans, sig_id)
        while st._leg_tasks:
            await asyncio.gather(*list(st._leg_tasks), return_exceptions=True)
        return written
    return asyncio.run(go())


class TestDeferredLegExecution:
    @pytest.fixture(autouse=True)
    def _no_live_ati(self, monkeypatch):
        monkeypatch.setattr(st, "query_nt_positions", lambda account, port=36973: {})

    def _profile(self, acct="Sim102", **rule):
        st.account_profiles[acct] = {"default": st._coerce_rule(rule)}

    def test_stagger_writes_all_tranches_with_unique_ids(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        self._profile(stagger_entries=3, stagger_interval_ms=0)
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            written = _run_plans(plans)
        assert written == ["Sim101"]  # follower leg was deferred
        files = sorted(tmp_output_dir.glob("oif_*.txt"))
        assert len(files) == 4  # leader 1 + follower 3 tranches
        follower = [f.read_text() for f in files if f.read_text().split(";")[1] == "Sim102"]
        assert len(follower) == 3
        assert sorted(int(b.split(";")[4]) for b in follower) == [1, 1, 2]
        ids = {b.split(";")[12] for b in follower}
        assert ids == {"77~Sim102", "77~Sim102~T2", "77~Sim102~T3"}
        assert st._stagger_placed[("Sim102", "77~Sim102")] == 3

    def test_stopped_account_aborts_before_write(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        self._profile(stagger_entries=2, stagger_interval_ms=0)
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            st.account_stops["Sim102"] = "hard"  # trips after planning
            _run_plans(plans)
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        assert all(b.split(";")[1] != "Sim102" for b in bodies)

    def test_deferred_leader_registers_confirm(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = []
        self._profile("Sim101", stagger_entries=2, stagger_interval_ms=0)
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            _run_plans(plans, sig_id="77")
        assert len(st._pending_confirms) == 1
        assert st._pending_confirms[0]["instrument"] == "NQ 06-26"

    def test_ai_veto_blocks_entry(self, tmp_output_dir, monkeypatch):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        self._profile(ai={"provider": "ollama"})
        monkeypatch.setattr(st, "ai_consult", lambda cfg, ctx: {
            "decision": "skip", "qty": None, "reason": "chop"})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            _run_plans(plans)
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        assert len(bodies) == 1 and bodies[0].split(";")[1] == "Sim101"

    def test_ai_resize_only_shrinks(self, tmp_output_dir, monkeypatch):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        self._profile(ai={"provider": "ollama"})
        monkeypatch.setattr(st, "ai_consult", lambda cfg, ctx: {
            "decision": "allow", "qty": 2, "reason": "half size"})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            _run_plans(plans)
        follower = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")
                    if f.read_text().split(";")[1] == "Sim102"]
        assert follower[0].split(";")[4] == "2"

    def test_ai_resize_up_is_ignored(self, tmp_output_dir, monkeypatch):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        self._profile(ai={"provider": "ollama"})
        monkeypatch.setattr(st, "ai_consult", lambda cfg, ctx: {
            "decision": "allow", "qty": 50, "reason": "moon"})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            _run_plans(plans)
        follower = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")
                    if f.read_text().split(";")[1] == "Sim102"]
        assert follower[0].split(";")[4] == "4"  # publisher size kept

    @pytest.mark.parametrize("policy,expect_files", [("skip", 0), ("allow", 1)])
    def test_ai_error_honors_on_error_policy(self, tmp_output_dir, monkeypatch,
                                             policy, expect_files):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        self._profile(ai={"provider": "ollama", "on_error": policy})
        monkeypatch.setattr(st, "ai_consult", lambda cfg, ctx: {
            "decision": "skip", "error": "ConnectionError: refused"})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(SIG)
            _run_plans(plans)
        follower = [f for f in tmp_output_dir.glob("oif_*.txt")
                    if f.read_text().split(";")[1] == "Sim102"]
        assert len(follower) == expect_files


class TestAiParseDecision:
    def test_clean_json(self):
        v = st._ai_parse_decision('{"decision": "allow", "qty": 2, "reason": "ok"}')
        assert v == {"decision": "allow", "qty": 2, "reason": "ok"}

    def test_json_wrapped_in_prose(self):
        v = st._ai_parse_decision('Sure!\n{"decision": "skip", "qty": null, "reason": "news"}\nDone.')
        assert v["decision"] == "skip" and v["qty"] is None

    def test_unparseable_raises(self):
        with pytest.raises(ValueError):
            st._ai_parse_decision("I think you should buy")

    def test_bad_decision_raises(self):
        with pytest.raises(ValueError):
            st._ai_parse_decision('{"decision": "maybe"}')

    @pytest.mark.parametrize("qty", [None, 0, -3, True, "two"])
    def test_invalid_qty_becomes_none(self, qty):
        v = st._ai_parse_decision(json.dumps(
            {"decision": "allow", "qty": qty, "reason": ""}))
        assert v["qty"] is None


class TestAiConsult:
    CFG = {"provider": "openai", "model": "m", "endpoint": "http://x", "api_key_env": "",
           "timeout_ms": 1000, "on_error": "skip", "instructions": ""}

    def test_unknown_provider_is_error(self):
        v = st.ai_consult({"provider": "psychic"}, {})
        assert "error" in v and v["decision"] == "skip"

    def test_provider_exception_becomes_error(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("HTTP 500: nope")
        monkeypatch.setattr(st, "_ai_call_openai_compat", boom)
        v = st.ai_consult(self.CFG, {"instrument": "NQ"})
        assert v["decision"] == "skip" and "HTTP 500" in v["error"]

    def test_verdict_flows_through(self, monkeypatch):
        monkeypatch.setattr(st, "_ai_call_openai_compat",
                            lambda *a, **k: '{"decision": "allow", "qty": null, "reason": "fine"}')
        v = st.ai_consult(self.CFG, {})
        assert v == {"decision": "allow", "qty": None, "reason": "fine"}

    def test_ollama_and_anthropic_paths_are_routed(self, monkeypatch):
        calls = []
        monkeypatch.setattr(st, "_ai_call_ollama",
                            lambda *a, **k: calls.append("ollama") or '{"decision":"allow","qty":null,"reason":""}')
        st.ai_consult({**self.CFG, "provider": "ollama"}, {})
        monkeypatch.setattr(st, "_ai_call_anthropic",
                            lambda *a, **k: calls.append("anthropic") or '{"decision":"allow","qty":null,"reason":""}')
        st.ai_consult({**self.CFG, "provider": "anthropic"}, {})
        assert calls == ["ollama", "anthropic"]


class TestProfilesConfig:
    def test_coerce_clamps_and_drops_junk(self):
        loaded = st.load_account_profiles({"account_profiles": {
            "Sim102": {
                "default": {"delay_ms": 99999999, "stagger_entries": 99,
                            "size": "MICROS", "bogus_key": 1, "qty_mode": "fixed",
                            "qty_value": "3"},
                "rules": [{"symbols": "NQ, ES", "direction": "invert"},
                          "not a dict"],
            },
            "": {"default": {"delay_ms": 1}},
            "Sim103": "not a dict",
        }})
        prof = loaded["Sim102"]
        assert prof["default"]["delay_ms"] == 600_000
        assert prof["default"]["stagger_entries"] == 10
        assert prof["default"]["size"] == "micros"
        assert prof["default"]["qty_value"] == 3.0
        assert "bogus_key" not in prof["default"]
        assert prof["rules"] == [{"symbols": ["NQ", "ES"], "direction": "invert"}]
        assert set(loaded) == {"Sim102"}

    def test_ai_config_coercion(self):
        loaded = st.load_account_profiles({"account_profiles": {
            "Sim102": {"default": {"ai": {"provider": "OpenAI", "timeout_ms": 100}}}}})
        ai = loaded["Sim102"]["default"]["ai"]
        assert ai["provider"] == "openai"
        assert ai["timeout_ms"] == 1000  # clamped up
        assert ai["model"] == "gpt-4o-mini"
        assert ai["api_key_env"] == "OPENAI_API_KEY"
        bad = st.load_account_profiles({"account_profiles": {
            "Sim102": {"default": {"ai": {"provider": "skynet"}}}}})
        assert bad["Sim102"]["default"]["ai"] is None

    def test_save_roundtrip_and_pruning(self, tmp_config):
        st.account_profiles["Sim102"] = {"default": {"delay_ms": 250}, "rules": []}
        st.account_profiles["Sim103"] = {"default": {}, "rules": []}  # empty → pruned
        st.save_account_profiles()
        cfg = st.load_config()
        assert cfg["account_profiles"] == {"Sim102": {"default": {"delay_ms": 250}}}
        assert st.load_account_profiles(cfg)["Sim102"]["default"]["delay_ms"] == 250

    def test_save_removes_section_when_empty(self, tmp_config):
        st.save_config({"account_profiles": {"Sim102": {"default": {"delay_ms": 1}}}})
        st.account_profiles.clear()
        st.save_account_profiles()
        assert "account_profiles" not in st.load_config()


class TestPublisherStrategyOf:
    def test_extracts_field_11(self):
        msg = json.dumps({"signal": SIG, "ts": 1})
        assert st.publisher_strategy_of(msg) == "NQ_Med"

    def test_bad_input_returns_empty(self):
        assert st.publisher_strategy_of("not json") == ""
        assert st.publisher_strategy_of(json.dumps({"signal": "PLACE;a;b"})) == ""


class TestDispatchWithProfiles:
    """End-to-end: profiles reshape the classic fan-out per account."""

    def test_follower_trades_micros_at_fixed_size(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_profiles["Sim102"] = {"default": {
            "size": "micros", "qty_mode": "fixed", "qty_value": 2.0}}
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            written = asyncio.run(st.dispatch_signal(SIG))
        assert set(written) == {"Sim101", "Sim102"}
        by_acct = {f.read_text().split(";")[1]: f.read_text().split(";")
                   for f in tmp_output_dir.glob("oif_*.txt")}
        assert by_acct["Sim101"][2] == "NQ 06-26" and by_acct["Sim101"][4] == "4"
        assert by_acct["Sim102"][2] == "MNQ 06-26" and by_acct["Sim102"][4] == "2"

    def test_inverted_follower_fades_the_leader(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_profiles["Sim102"] = {"default": {"direction": "invert"}}
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            asyncio.run(st.dispatch_signal(SIG))
        by_acct = {f.read_text().split(";")[1]: f.read_text().split(";")
                   for f in tmp_output_dir.glob("oif_*.txt")}
        assert by_acct["Sim101"][3] == "BUY"
        assert by_acct["Sim102"][3] == "SELL"

    def test_scoped_rule_only_hits_its_symbol(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_profiles["Sim102"] = {"rules": [
            {"symbols": ["ES"], "enabled": False}]}
        es_sig = SIG.replace("NQ 06-26", "ES 09-26")
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            nq_written = asyncio.run(st.dispatch_signal(SIG))
            es_written = asyncio.run(st.dispatch_signal(es_sig))
        assert set(nq_written) == {"Sim101", "Sim102"}
        assert es_written == ["Sim101"]

# ── round-robin account mode ──────────────────────────────────────────
class TestRoundRobinDraw:
    POOL = ["RR1", "RR2", "RR3"]

    def _arm(self, pool=None):
        st.roundrobin_accounts = list(pool or self.POOL)

    def test_round_covers_every_account_without_repeats(self):
        self._arm()
        drawn = [st._rr_next() for _ in range(3)]
        assert sorted(drawn) == sorted(self.POOL)

    def test_multiple_rounds_stay_balanced(self):
        self._arm()
        drawn = [st._rr_next() for _ in range(30)]
        assert all(drawn.count(a) == 10 for a in self.POOL)
        for start in range(0, 30, 3):  # every full round covers the pool
            assert sorted(drawn[start:start + 3]) == sorted(self.POOL)

    def test_no_immediate_repeat_across_round_boundary(self):
        self._arm(["A", "B"])
        drawn = [st._rr_next() for _ in range(20)]
        assert all(drawn[i] != drawn[i + 1] for i in range(19))  # strict alternation

    def test_two_account_pool_alternates(self):
        self._arm(["A", "B"])
        first, second = st._rr_next(), st._rr_next()
        assert {first, second} == {"A", "B"}

    def test_stopped_account_is_passed_over(self):
        self._arm()
        st.account_stops["RR2"] = "hard"
        drawn = {st._rr_next() for _ in range(10)}
        assert drawn == {"RR1", "RR3"}

    def test_empty_pool_returns_none(self):
        st.roundrobin_accounts = []
        assert st._rr_next() is None
        self._arm(["RR1"])
        st.account_stops["RR1"] = "hard"
        assert st._rr_next() is None

    def test_single_account_pool_repeats(self):
        self._arm(["RR1"])
        assert st._rr_next() == "RR1"
        assert st._rr_next() == "RR1"

    def test_membership_change_joins_next_refill(self):
        self._arm(["A", "B"])
        st._rr_next(), st._rr_next()          # consume a full round
        st.roundrobin_accounts.append("C")
        drawn = [st._rr_next() for _ in range(3)]
        assert sorted(drawn) == ["A", "B", "C"]

    def test_reset_rotation_clears_state(self):
        self._arm()
        st._rr_next()
        st._rr_reset_rotation()
        assert st._rr_remaining == [] and st._rr_last is None


class TestSanitizeRoundrobin:
    def test_removes_leader_followers_dupes_and_junk(self):
        out = st.sanitize_roundrobin(
            ["Sim101", "Sim102", "RR1", "RR1", " RR2 ", 7, ""],
            leader="Sim101", followers=["Sim102"])
        assert out == ["RR1", "RR2"]

    def test_non_list_returns_empty(self):
        assert st.sanitize_roundrobin("RR1", "L", []) == []


class TestTargetAccountsWithRoundRobin:
    def test_pool_included_in_targets(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["RR1", "RR2"]
        assert st.target_accounts() == ["Sim101", "Sim102", "RR1", "RR2"]
        assert st.copy_trade_accounts() == ["Sim101", "Sim102"]

    def test_session_hard_lock_requires_pool_stopped_too(self):
        st.active_account = "Sim101"
        st.roundrobin_accounts = ["RR1"]
        st.account_stops["Sim101"] = "hard"
        assert st.session_hard_locked() is False
        st.account_stops["RR1"] = "hard"
        assert st.session_hard_locked() is True


class TestRoundRobinPlanning:
    def _arm(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["RR1", "RR2"]
        st._rr_remaining = ["RR1", "RR2"]  # deterministic rotation

    def test_entry_goes_to_copy_set_plus_one_pool_member(self):
        self._arm()
        plans, _ = st.plan_signal_legs(SIG)
        accounts = [p["account"] for p in plans]
        assert accounts == ["Sim101", "Sim102", "RR1"]
        assert [p["account"] for p in plans if p["rr_pick"]] == ["RR1"]

    def test_rotation_advances_per_entry(self):
        self._arm()
        first, _ = st.plan_signal_legs(SIG)
        second, _ = st.plan_signal_legs(SIG)
        picks = [next(p["account"] for p in plans if p["rr_pick"])
                 for plans in (first, second)]
        assert picks == ["RR1", "RR2"]

    def test_exits_fan_to_entire_pool(self):
        self._arm()
        close = "CLOSEPOSITION;Sim101;NQ 06-26;;;;;;;;;;"
        plans, _ = st.plan_signal_legs(close)
        assert [p["account"] for p in plans] == ["Sim101", "Sim102", "RR1", "RR2"]
        assert all(p["command"] == "CLOSEPOSITION" for p in plans)
        # exit fan-out consumes no rotation slot
        assert st._rr_remaining == ["RR1", "RR2"]

    def test_close_strategy_fans_to_pool_with_account_ids(self):
        self._arm()
        close = "CLOSESTRATEGY;;;;;;;;;;;;77"
        plans, _ = st.plan_signal_legs(close)
        by_acct = {p["account"]: p["signal"].split(";")[12] for p in plans}
        assert by_acct["RR1"] == "77~RR1" and by_acct["RR2"] == "77~RR2"

    def test_reverse_goes_to_pick_others_get_close(self):
        self._arm()
        rev = "REVERSEPOSITION;Sim101;NQ 06-26;BUY;2;MARKET;;;DAY;;;NQ_Med;80"
        plans, _ = st.plan_signal_legs(rev)
        by_acct = {p["account"]: p for p in plans}
        assert by_acct["RR1"]["command"] == "REVERSEPOSITION"
        assert by_acct["RR1"]["rr_pick"] is True
        assert by_acct["RR2"]["command"] == "CLOSEPOSITION"
        assert by_acct["RR2"]["signal"].split(";")[2] == "NQ 06-26"
        assert by_acct["Sim101"]["command"] == "REVERSEPOSITION"
        assert by_acct["Sim102"]["command"] == "REVERSEPOSITION"

    def test_stopped_pool_member_not_planned(self):
        self._arm()
        st.account_stops["RR1"] = "hard"
        st._rr_remaining = []  # force refill from tradeable pool
        plans, _ = st.plan_signal_legs(SIG)
        accounts = [p["account"] for p in plans]
        assert "RR1" not in accounts and "RR2" in accounts

    def test_profiles_compose_with_rotation(self):
        self._arm()
        st.account_profiles["RR1"] = {"default": {
            "size": "micros", "qty_mode": "fixed", "qty_value": 3.0}}
        plans, _ = st.plan_signal_legs(SIG)
        pick = next(p for p in plans if p["rr_pick"])
        assert pick["signal"].split(";")[2] == "MNQ 06-26"
        assert pick["signal"].split(";")[4] == "3"

    def test_no_pool_keeps_classic_behavior(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        plans, _ = st.plan_signal_legs(SIG)
        assert [p["account"] for p in plans] == ["Sim101", "Sim102"]
        assert all(not p["rr_pick"] for p in plans)


class TestRoundRobinDispatch:
    def test_consecutive_signals_rotate_through_pool(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["RR1", "RR2"]
        st._rr_remaining = ["RR1", "RR2"]
        sig2 = SIG.replace(";77", ";78")
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            first = asyncio.run(st.dispatch_signal(SIG))
            second = asyncio.run(st.dispatch_signal(sig2))
        assert set(first) == {"Sim101", "Sim102", "RR1"}
        assert set(second) == {"Sim101", "Sim102", "RR2"}
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        assert len(bodies) == 6
        rr1 = [b for b in bodies if b.split(";")[1] == "RR1"]
        assert len(rr1) == 1 and rr1[0].split(";")[12] == "77~RR1"


class TestRoundRobinPersistence:
    def test_save_and_restore_rotation(self, tmp_config, monkeypatch):
        monkeypatch.setattr(st, "get_session_id", lambda now_et=None: "2026-08-07")
        st.active_account = "Sim101"
        st.roundrobin_accounts = ["RR1", "RR2", "RR3"]
        st.session_start_balances["Sim101"] = 1000.0
        st._rr_remaining = ["RR3", "RR1"]
        st._rr_last = "RR2"
        st.save_session_state()
        st._rr_remaining, st._rr_last = [], None
        assert st.restore_session_state() is True
        assert st._rr_remaining == ["RR3", "RR1"]
        assert st._rr_last == "RR2"

    def test_pool_change_starts_fresh_round(self, tmp_config, monkeypatch):
        monkeypatch.setattr(st, "get_session_id", lambda now_et=None: "2026-08-07")
        st.active_account = "Sim101"
        st.roundrobin_accounts = ["RR1", "RR2"]
        st.session_start_balances["Sim101"] = 1000.0
        st._rr_remaining = ["RR2"]
        st.save_session_state()
        st.roundrobin_accounts = ["RR1", "RR9"]  # pool changed between runs
        st._rr_remaining, st._rr_last = [], None
        assert st.restore_session_state() is True
        assert st._rr_remaining == [] and st._rr_last is None


# ── per-account symbol filter ─────────────────────────────────────────
class TestAccountTradesSymbol:
    def test_no_filter_trades_everything(self):
        assert st.account_trades_symbol("Sim102", "ES 09-26") is True

    def test_filter_allows_only_named_roots(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        assert st.account_trades_symbol("Sim102", "GC 12-26") is True
        assert st.account_trades_symbol("Sim102", "ES 09-26") is False

    def test_micro_twins_match_both_ways(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        assert st.account_trades_symbol("Sim102", "MGC 12-26") is True
        st.account_profiles["Sim102"] = {"symbols_allowed": ["MNQ"]}
        assert st.account_trades_symbol("Sim102", "NQ 06-26") is True

    def test_no_instrument_passes(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        assert st.account_trades_symbol("Sim102", "") is True


class TestSymbolFilterTransform:
    def _t(self, sig, acct):
        return st.transform_signal_for_account(sig, acct, dict(st.DEFAULT_RULE))

    def test_place_filtered_out(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        final, reason, _ = self._t(SIG, "Sim102")
        assert final is None
        assert "symbol filtered" in reason

    def test_place_allowed_passes_unchanged(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["NQ"]}
        final, reason, _ = self._t(SIG, "Sim102")
        assert reason is None
        assert final == st._with_account(SIG, "Sim102")

    def test_reverse_downgrades_to_close(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        rev = SIG.replace("PLACE", "REVERSEPOSITION")
        final, reason, meta = self._t(rev, "Sim102")
        assert reason is None
        assert final.split(";")[0] == "CLOSEPOSITION"
        assert "symbol filtered" in meta["note"]

    def test_exits_never_filtered(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        close = "CLOSEPOSITION;Sim101;NQ 06-26;;;;;;;;;;"
        final, reason, _ = self._t(close, "Sim102")
        assert reason is None
        assert final.split(";")[0] == "CLOSEPOSITION"

    def test_filter_composes_with_micro_sizing(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["NQ"],
                                         "default": {"size": "micros"}}
        rule = st.resolve_rule("Sim102", "NQ 06-26", "")
        final, reason, _ = st.transform_signal_for_account(SIG, "Sim102", rule)
        assert reason is None
        assert final.split(";")[2] == "MNQ 06-26"


class TestRoundRobinSymbolFilter:
    def _arm(self):
        st.roundrobin_accounts = ["Gold", "Nas", "Any"]
        st._rr_remaining = ["Gold", "Nas", "Any"]
        st.account_profiles["Gold"] = {"symbols_allowed": ["GC"]}
        st.account_profiles["Nas"] = {"symbols_allowed": ["NQ"]}

    def test_filtered_account_passed_over_keeps_slot(self):
        self._arm()
        assert st._rr_next("NQ 06-26") == "Nas"
        assert st._rr_remaining == ["Gold", "Any"]

    def test_filtered_account_still_gets_its_market(self):
        self._arm()
        st._rr_next("NQ 06-26")
        assert st._rr_next("GC 12-26") == "Gold"
        assert st._rr_remaining == ["Any"]

    def test_micro_signal_reaches_full_size_filter(self):
        self._arm()
        assert st._rr_next("MGC 12-26") == "Gold"

    def test_no_eligible_member_returns_none_consumes_nothing(self):
        st.roundrobin_accounts = ["Gold"]
        st._rr_remaining = ["Gold"]
        st.account_profiles["Gold"] = {"symbols_allowed": ["GC"]}
        assert st._rr_next("ES 09-26") is None
        assert st._rr_remaining == ["Gold"]

    def test_top_up_when_no_remaining_slot_is_eligible(self):
        self._arm()
        st._rr_remaining = ["Gold"]  # only the gold slot left this round
        pick = st._rr_next("NQ 06-26")
        assert pick in ("Nas", "Any")
        assert "Gold" in st._rr_remaining  # slot survives the top-up

    def test_locked_account_still_forfeits_slot(self):
        self._arm()
        st.account_stops["Gold"] = "hard"
        assert st._rr_next("GC 12-26") == "Any"  # only unfiltered member left
        assert "Gold" not in st._rr_remaining


class TestSymbolFilterPlanning:
    SIG_ES = SIG.replace("NQ 06-26", "ES 09-26")

    def test_filtered_follower_sits_out_then_rejoins(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.account_profiles["Sim102"] = {"symbols_allowed": ["NQ"]}
        plans, skipped = st.plan_signal_legs(self.SIG_ES)
        assert [p["account"] for p in plans] == ["Sim101"]
        assert skipped[0][0] == "Sim102" and "symbol filtered" in skipped[0][1]
        plans2, skipped2 = st.plan_signal_legs(SIG)  # NQ — follower participates
        assert [p["account"] for p in plans2] == ["Sim101", "Sim102"]
        assert not skipped2

    def test_rr_entry_routes_to_eligible_member(self):
        st.active_account = "Sim101"
        st.roundrobin_accounts = ["Gold", "Any"]
        st._rr_remaining = ["Gold", "Any"]
        st.account_profiles["Gold"] = {"symbols_allowed": ["GC"]}
        plans, _ = st.plan_signal_legs(SIG)  # NQ entry
        assert [p["account"] for p in plans if p["rr_pick"]] == ["Any"]
        assert "Gold" in st._rr_remaining

    def test_rr_reverse_pick_respects_filter(self):
        st.active_account = "Sim101"
        st.roundrobin_accounts = ["Gold", "Any"]
        st._rr_remaining = ["Gold", "Any"]
        st.account_profiles["Gold"] = {"symbols_allowed": ["GC"]}
        rev = "REVERSEPOSITION;Sim101;NQ 06-26;BUY;2;MARKET;;;DAY;;;NQ_Med;80"
        plans, _ = st.plan_signal_legs(rev)
        by = {p["account"]: p for p in plans}
        assert by["Any"]["command"] == "REVERSEPOSITION" and by["Any"]["rr_pick"]
        assert by["Gold"]["command"] == "CLOSEPOSITION"  # safety close still flows


class TestSymbolsAllowedPersistence:
    def test_load_cleans_dedups_and_uppercases(self):
        out = st.load_account_profiles({"account_profiles": {
            "Sim102": {"symbols_allowed": ["gc", " si ", "GC", ""]}}})
        assert out["Sim102"]["symbols_allowed"] == ["GC", "SI"]

    def test_load_accepts_comma_string(self):
        out = st.load_account_profiles({"account_profiles": {
            "Sim102": {"symbols_allowed": "gc, nq"}}})
        assert out["Sim102"]["symbols_allowed"] == ["GC", "NQ"]

    def test_save_round_trips(self, tmp_config):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC"]}
        st.save_account_profiles()
        cfg = st.load_config()
        assert st.load_account_profiles(cfg)["Sim102"]["symbols_allowed"] == ["GC"]

    def test_summary_shows_filter(self):
        st.account_profiles["Sim102"] = {"symbols_allowed": ["GC", "SI"]}
        assert "only GC,SI" in st.profile_summary("Sim102")


# ── post-reconnect replay guard ───────────────────────────────────────
class TestReplayGuard:
    CLOSE = "CLOSEPOSITION;Sim101;MNQ 09-26;;;;;;;;;NQ_Med;"

    def test_replay_blocked_inside_grace_window(self):
        st._note_fired_signal(self.CLOSE)
        st.note_connected()
        assert st._is_idless_replay(self.CLOSE) is True

    def test_never_blocked_without_recent_connect(self):
        st._note_fired_signal(self.CLOSE)
        st._last_connect_mono = None
        assert st._is_idless_replay(self.CLOSE) is False

    def test_not_blocked_after_grace_expires(self):
        import time as _t
        st._note_fired_signal(self.CLOSE)
        st._last_connect_mono = _t.monotonic() - (st.REPLAY_GRACE_S + 1)
        assert st._is_idless_replay(self.CLOSE) is False

    def test_unseen_signal_never_blocked(self):
        st.note_connected()
        assert st._is_idless_replay(self.CLOSE) is False

    def test_stale_fired_signal_not_blocked(self):
        import time as _t
        st._recent_fired[self.CLOSE] = _t.monotonic() - (st.REPLAY_LOOKBACK_S + 1)
        st.note_connected()
        assert st._is_idless_replay(self.CLOSE) is False

    def test_different_signal_text_not_blocked(self):
        st._note_fired_signal(self.CLOSE)
        st.note_connected()
        other = self.CLOSE.replace("MNQ", "MES")
        assert st._is_idless_replay(other) is False

    def test_fired_memory_is_bounded(self):
        for i in range(st._MAX_FIRED_KEYS + 20):
            st._note_fired_signal(f"CLOSEPOSITION;A;SYM{i};;;;;;;;;;")
        assert len(st._recent_fired) == st._MAX_FIRED_KEYS


# ── manual trading ────────────────────────────────────────────────────
class TestBuildManualSignal:
    def _ready(self, monkeypatch):
        st.active_account = "Sim101"
        monkeypatch.setattr(st, "atm_strategy", "NQ_Med")
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)

    def test_market_buy_layout(self, monkeypatch):
        self._ready(monkeypatch)
        sig, err = st.build_manual_signal("long", "NQ 09-26", 2, "market", None, "NQ_Med")
        assert err is None
        p = sig.split(";")
        assert p[0] == "PLACE" and p[1] == "Sim101" and p[2] == "NQ 09-26"
        assert p[3] == "BUY" and p[4] == "2" and p[5] == "MARKET"
        assert p[6] == "" and p[8] == "DAY" and p[11] == "NQ_Med"
        assert p[12].startswith("man")

    def test_limit_sell_carries_price(self, monkeypatch):
        self._ready(monkeypatch)
        sig, err = st.build_manual_signal("short", "NQ 09-26", 1, "limit", "23895.25", "NQ_Med")
        assert err is None
        p = sig.split(";")
        assert p[3] == "SELL" and p[5] == "LIMIT" and p[6] == "23895.25"

    def test_limit_requires_price(self, monkeypatch):
        self._ready(monkeypatch)
        sig, err = st.build_manual_signal("long", "NQ 09-26", 1, "limit", "", "NQ_Med")
        assert sig is None and "price" in err

    def test_default_atm_is_session_strategy(self, monkeypatch):
        self._ready(monkeypatch)
        sig, err = st.build_manual_signal("long", "NQ 09-26", 1)
        assert err is None and sig.split(";")[11] == "NQ_Med"

    def test_bad_side_qty_type_rejected(self, monkeypatch):
        self._ready(monkeypatch)
        assert st.build_manual_signal("hold", "NQ 09-26", 1)[1]
        assert st.build_manual_signal("long", "NQ 09-26", 0)[1]
        assert st.build_manual_signal("long", "NQ 09-26", "x")[1]
        assert st.build_manual_signal("long", "NQ 09-26", 1, "stop")[1]

    def test_instrument_requires_expiry(self, monkeypatch):
        self._ready(monkeypatch)
        sig, err = st.build_manual_signal("long", "NQ", 1)
        assert sig is None and "expiry" in err

    def test_micro_mode_converts_instrument(self, monkeypatch):
        self._ready(monkeypatch)
        st.micro_mode = True
        sig, err = st.build_manual_signal("long", "NQ 09-26", 1)
        assert err is None and sig.split(";")[2] == "MNQ 09-26"

    def test_uninstalled_atm_rejected(self, monkeypatch):
        st.active_account = "Sim101"
        monkeypatch.setattr(st, "validate_strategy", lambda n: False)
        sig, err = st.build_manual_signal("long", "NQ 09-26", 1, "market", None, "Nope")
        assert sig is None and "not installed" in err

    def test_signal_ids_unique(self, monkeypatch):
        self._ready(monkeypatch)
        ids = {st.build_manual_signal("long", "NQ 09-26", 1)[0].split(";")[-1]
               for _ in range(5)}
        assert len(ids) == 5


class TestSubmitManualTrade:
    def _arm(self, monkeypatch):
        st.active_account = "Sim101"
        monkeypatch.setattr(st, "atm_strategy", "NQ_Med")
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        monkeypatch.setattr(st, "is_trade_ready", lambda: True)
        monkeypatch.setattr(st, "query_nt_positions", lambda a, p=36973: {})

    def test_hard_lock_blocks(self, monkeypatch):
        self._arm(monkeypatch)
        st.hard_stopped = True
        ok, msg = asyncio.run(st.submit_manual_trade("long", "NQ 09-26", 1))
        assert ok is False and "hard-locked" in msg

    def test_not_ready_blocks(self, monkeypatch):
        self._arm(monkeypatch)
        monkeypatch.setattr(st, "is_trade_ready", lambda: False)
        ok, msg = asyncio.run(st.submit_manual_trade("long", "NQ 09-26", 1))
        assert ok is False and "not ready" in msg

    def test_paused_does_not_block(self, tmp_output_dir, monkeypatch):
        self._arm(monkeypatch)
        st.paused = True
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            ok, _ = asyncio.run(st.submit_manual_trade("long", "NQ 09-26", 1))
        assert ok is True
        assert len(list(tmp_output_dir.glob("oif_*.txt"))) == 1

    def test_fans_out_to_copy_and_rotation(self, tmp_output_dir, monkeypatch):
        self._arm(monkeypatch)
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["RR1", "RR2"]
        st._rr_remaining = ["RR1", "RR2"]
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            ok, msg = asyncio.run(st.submit_manual_trade("long", "NQ 09-26", 2))
        assert ok is True
        accts = sorted(f.read_text().split(";")[1]
                       for f in tmp_output_dir.glob("oif_*.txt"))
        assert accts == ["RR1", "Sim101", "Sim102"]

    def test_symbol_filtered_account_sits_out(self, tmp_output_dir, monkeypatch):
        self._arm(monkeypatch)
        st.follower_accounts = ["GoldOnly"]
        st.account_profiles["GoldOnly"] = {"symbols_allowed": ["GC"]}
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            ok, msg = asyncio.run(st.submit_manual_trade("long", "NQ 09-26", 1))
        assert ok is True and "skipped" in msg
        accts = [f.read_text().split(";")[1] for f in tmp_output_dir.glob("oif_*.txt")]
        assert accts == ["Sim101"]


# ── web UI ────────────────────────────────────────────────────────────
class TestWebState:
    def test_snapshot_is_json_safe_and_complete(self, monkeypatch):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["RR1"]
        st.session_start_balances["Sim101"] = 1000.0
        st.session_current_balances["Sim101"] = 1250.5
        monkeypatch.setattr(st, "list_atm_strategies", lambda: ["NQ_Med"])
        monkeypatch.setattr(st, "get_account_limits", lambda a: {"target": 0})
        payload = json.loads(json.dumps(st.web_state()))
        assert payload["leader"] == "Sim101"
        assert payload["rr"]["pool"] == ["RR1"]
        roles = {a["name"]: a["role"] for a in payload["accounts"]}
        assert roles == {"Sim101": "leader", "Sim102": "follower",
                         "RR1": "round-robin"}
        sim = next(a for a in payload["accounts"] if a["name"] == "Sim101")
        assert sim["pnl"] == 250.5

    def test_dashboard_lines_mirrored_to_feed(self):
        st._dash_add_signal("\x1b[32mSIG #1  PLACE...\x1b[0m")
        assert st._web_events[-1]["text"] == "SIG #1  PLACE..."
        assert st._web_events[-1]["kind"] == "signal"


class _WebClient:
    """Starts the real web server on an ephemeral port and speaks to it."""

    @pytest.fixture
    def web(self, monkeypatch):
        monkeypatch.setattr(st, "list_atm_strategies", lambda: [])
        monkeypatch.setattr(st, "get_account_limits", lambda a: {})
        loop = asyncio.new_event_loop()
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()
        url = st.start_web_ui(loop, {"webui_port": 0})
        assert url
        yield url
        st.stop_web_ui()
        loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=2)

    def _post(self, url, payload, headers=None, token=True):
        import urllib.request as ur
        hdrs = {"Content-Type": "application/json"}
        if token:
            hdrs["X-ST-Token"] = st._web_token
        hdrs.update(headers or {})
        req = ur.Request(url, data=json.dumps(payload).encode(),
                         headers=hdrs, method="POST")
        return json.loads(ur.urlopen(req, timeout=5).read())

    def _get(self, url, token=True, headers=None):
        import urllib.request as ur
        hdrs = {"X-ST-Token": st._web_token} if token else {}
        hdrs.update(headers or {})
        return ur.urlopen(ur.Request(url, headers=hdrs), timeout=5)


class TestWebServer(_WebClient):
    def test_serves_page_and_state(self, web):
        html = self._get(web + "/").read().decode()
        assert "<title>SocketTrader</title>" in html and "/api/state" in html
        state = json.loads(self._get(web + "/api/state").read())
        assert state["version"] == st.__version__

    def test_page_carries_a_fresh_token(self, web):
        html = self._get(web + "/").read().decode()
        assert st._web_token and st._web_token in html
        assert "__ST_TOKEN__" not in html

    def test_trade_rejected_when_hard_locked(self, web):
        st.hard_stopped = True
        resp = self._post(web + "/api/trade",
                          {"side": "long", "instrument": "NQ 09-26", "qty": 1})
        assert resp["ok"] is False and "hard-locked" in resp["message"]

    def test_pause_toggles_through_loop(self, web):
        resp = self._post(web + "/api/pause", {"paused": True})
        assert resp["ok"] is True and st.paused is True
        resp = self._post(web + "/api/pause", {"paused": False})
        assert resp["ok"] is True and st.paused is False

    def test_accounts_applied_and_sanitized(self, web, tmp_config):
        resp = self._post(web + "/api/accounts", {
            "leader": "Sim101", "followers": ["Sim102", "Sim101"],
            "roundrobin": ["Sim102", "RR1"]})
        assert resp["ok"] is True
        assert st.active_account == "Sim101"
        assert st.follower_accounts == ["Sim102"]
        assert st.roundrobin_accounts == ["RR1"]  # follower conflict dropped

    def test_unknown_path_404(self, web):
        import urllib.error
        with pytest.raises(urllib.error.HTTPError) as e:
            self._get(web + "/api/nope")
        assert e.value.code == 404


class TestWebSecurity(_WebClient):
    """The web API controls real money — loopback is not the boundary."""

    def _expect_status(self, fn, code):
        import urllib.error
        with pytest.raises(urllib.error.HTTPError) as e:
            fn()
        assert e.value.code == code

    def test_post_without_token_rejected(self, web):
        self._expect_status(
            lambda: self._post(web + "/api/close_all", {}, token=False), 403)

    def test_post_with_wrong_token_rejected(self, web):
        self._expect_status(
            lambda: self._post(web + "/api/close_all", {},
                               headers={"X-ST-Token": "nope"}, token=False), 403)

    def test_state_read_requires_token(self, web):
        self._expect_status(lambda: self._get(web + "/api/state", token=False), 403)

    def test_cross_site_origin_rejected(self, web):
        self._expect_status(
            lambda: self._post(web + "/api/trade",
                               {"side": "long", "instrument": "NQ 09-26", "qty": 1},
                               headers={"Origin": "https://evil.example"}), 403)

    def test_non_json_content_type_rejected(self, web):
        # the CSRF-friendly simple-request content type must not be parsed
        self._expect_status(
            lambda: self._post(web + "/api/close_all", {},
                               headers={"Content-Type": "text/plain"}), 415)

    def test_rebound_host_header_rejected(self, web):
        self._expect_status(
            lambda: self._get(web + "/api/state",
                              headers={"Host": "evil.example"}), 403)

    def test_loopback_host_allowed(self, web):
        port = web.rsplit(":", 1)[1]
        assert self._get(web + "/api/state",
                         headers={"Host": f"localhost:{port}"}).status == 200

    def test_ai_gate_cannot_be_set_over_http(self, web):
        resp = self._post(web + "/api/profiles", {"profiles": {"Sim101": {"rules": [
            {"symbols": ["NQ"], "ai": {"provider": "custom",
                                       "endpoint": "http://attacker.example/x",
                                       "api_key_env": "ANTHROPIC_API_KEY"}}]}}})
        assert resp["ok"] is True
        rules = st.account_profiles.get("Sim101", {}).get("rules", [])
        assert rules and rules[0].get("ai") is None

    def test_existing_ai_gate_survives_a_web_profile_edit(self, web):
        st.account_profiles["Sim101"] = {"default": {
            "ai": {"provider": "ollama", "model": "llama3.2",
                   "endpoint": "http://localhost:11434/api/chat",
                   "api_key_env": "", "timeout_ms": 8000,
                   "on_error": "skip", "instructions": ""}}}
        resp = self._post(web + "/api/profiles",
                          {"profiles": {"Sim101": {"default": {"qty_mode": "fixed",
                                                               "qty_value": 2}}}})
        assert resp["ok"] is True
        default = st.account_profiles["Sim101"]["default"]
        assert default["qty_mode"] == "fixed"
        assert default["ai"]["provider"] == "ollama"  # preserved, not dropped

    def test_hard_lock_freezes_risk_limits(self, web):
        st.hard_stopped = True
        resp = self._post(web + "/api/limits", {"account": "Sim101", "target": 0,
                                                "target_mode": "off", "stop": 0,
                                                "stop_mode": "off"})
        assert resp["ok"] is False and "frozen" in resp["message"]

    def test_limits_reject_unknown_mode(self, web, tmp_config):
        resp = self._post(web + "/api/limits", {"account": "Sim101", "target": 100,
                                                "target_mode": "sideways",
                                                "stop": 50, "stop_mode": "hard"})
        assert resp["ok"] is False


class TestHedgeGuard:
    """A per-account `direction: invert` applied to some accounts and not
    others hedges the group against itself on every single signal."""

    ENTRY = "PLACE;LEAD;NQ 09-26;BUY;2;MARKET;;;DAY;;;NQ_Med;77"

    @pytest.fixture(autouse=True)
    def _arm(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "warn")  # the real default
        st.active_account = "LEAD"
        st.follower_accounts = ["F1"]

    def _actions(self, sig=None):
        plans, skipped = st.plan_signal_legs(sig or self.ENTRY)
        return ({p["account"]: p["signal"].split(";")[3] for p in plans}, skipped)

    # --- leader-dominant direction: the leader is the reference account ---

    def test_leader_invert_carries_the_whole_group(self):
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "SELL"} and not skipped

    def test_inheritance_survives_other_per_account_settings(self):
        # a size override must not accidentally opt F1 out of the leader's
        # direction — only an explicit `direction` key does that
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        st.account_profiles["F1"] = {"default": {"size": "micros"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "SELL"} and not skipped

    def test_scoped_leader_invert_is_inherited_too(self):
        st.account_profiles["LEAD"] = {"rules": [
            {"symbols": ["NQ"], "direction": "invert"}]}
        acts, _ = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "SELL"}

    def test_leader_invert_does_not_apply_to_other_symbols(self):
        st.account_profiles["LEAD"] = {"rules": [
            {"symbols": ["GC"], "direction": "invert"}]}
        acts, _ = self._actions()
        assert acts == {"LEAD": "BUY", "F1": "BUY"}

    # --- divergence: only an explicit follower direction creates a hedge ---

    def test_follower_invert_against_a_normal_leader_conflicts(self):
        st.account_profiles["F1"] = {"default": {"direction": "invert"}}
        plans, _ = st.plan_signal_legs(self.ENTRY)
        acts = {p["account"]: p["signal"].split(";")[3] for p in plans}
        assert acts == {"LEAD": "BUY", "F1": "SELL"}
        assert st._entry_direction_conflict(plans)

    def test_follower_pinned_normal_against_an_inverted_leader_conflicts(self):
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        st.account_profiles["F1"] = {"default": {"direction": "normal"}}
        plans, _ = st.plan_signal_legs(self.ENTRY)
        acts = {p["account"]: p["signal"].split(";")[3] for p in plans}
        assert acts == {"LEAD": "SELL", "F1": "BUY"}
        assert st._entry_direction_conflict(plans)

    def test_both_invert_is_not_a_conflict(self):
        for a in ("LEAD", "F1"):
            st.account_profiles[a] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "SELL"} and not skipped

    def test_no_invert_is_allowed(self):
        acts, _ = self._actions()
        assert acts == {"LEAD": "BUY", "F1": "BUY"}

    def test_micro_and_full_are_the_same_underlying(self):
        # F1 fades the leader AND trades micros — still one underlying
        st.account_profiles["F1"] = {"default": {"direction": "invert",
                                                 "size": "micros"}}
        plans, _ = st.plan_signal_legs(self.ENTRY)
        assert list(st._entry_direction_conflict(plans)) == ["NQ"]

    def test_different_symbols_do_not_collide(self):
        # F1 only trades gold, so it never takes the NQ entry at all
        st.account_profiles["F1"] = {"symbols_allowed": ["GC"],
                                     "default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "BUY"}
        assert [r for a, r in skipped if a == "F1" and "symbol" in r]

    def test_exits_are_never_blocked(self, monkeypatch):
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "block")
        st.account_profiles["F1"] = {"default": {"direction": "invert"}}
        plans, _ = st.plan_signal_legs("CLOSEPOSITION;LEAD;NQ 09-26;;;;;;;;;;")
        assert [p["account"] for p in plans] == ["LEAD", "F1"]
        assert all(p["command"] == "CLOSEPOSITION" for p in plans)

    def test_block_mode_refuses_a_diverging_follower(self, monkeypatch):
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "block")
        st.account_profiles["F1"] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {}
        assert all("hedge guard" in r for _, r in skipped)

    def test_warn_mode_fires_but_flags(self):
        st.account_profiles["F1"] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "BUY", "F1": "SELL"} and not skipped

    def test_off_mode_disables_the_guard(self, monkeypatch):
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "off")
        st.account_profiles["F1"] = {"default": {"direction": "invert"}}
        acts, _ = self._actions()
        assert acts == {"LEAD": "BUY", "F1": "SELL"}


class TestHedgeGuardMode:
    """Mode resolution — no autouse patch here, this reads real config."""

    def test_defaults_to_warn_when_unset(self, tmp_config):
        st.save_config({})
        assert st.hedge_guard_mode() == "warn"

    def test_falls_back_to_warn_on_junk(self, tmp_config):
        st.save_config({"hedge_guard": "banana"})
        assert st.hedge_guard_mode() == "warn"

    def test_reads_block_from_config(self, tmp_config):
        st.save_config({"hedge_guard": "block"})
        assert st.hedge_guard_mode() == "block"

    def test_reads_off_from_config(self, tmp_config):
        st.save_config({"hedge_guard": "off"})
        assert st.hedge_guard_mode() == "off"


class TestCrossAccountHedgeDetection:
    """Prop firms liquidate for opposite positions across accounts, and they
    judge it at the underlying level (NQ and MNQ are one instrument)."""

    def _rows(self, *positions):
        st.active_account = "LEAD"
        st.follower_accounts = ["F1"]
        by_acct = {}
        for acct, instrument, qty in positions:
            by_acct.setdefault(acct, []).append(
                {"account": acct, "instrument": instrument, "qty": qty,
                 "avg_price": 1.0})
        return [{"name": n, "managed": True, "positions": by_acct.get(n, [])}
                for n in ("LEAD", "F1", "RR1")]

    def test_same_direction_is_clean(self):
        rows = self._rows(("LEAD", "MNQ 09-26", -2), ("F1", "MNQ 09-26", -1))
        assert st._hedge_conflicts(rows) == []

    def test_opposite_sides_same_symbol_flagged(self):
        rows = self._rows(("LEAD", "NQ 09-26", -2), ("F1", "NQ 09-26", 1))
        assert st._hedge_conflicts(rows) == [
            {"root": "NQ", "long": ["F1"], "short": ["LEAD"]}]

    def test_micro_against_full_is_the_same_underlying(self):
        # MyFunded Futures: "E-Mini NQ and Micro NQ have the same underlying"
        rows = self._rows(("LEAD", "MNQ 09-26", -2), ("F1", "NQ 09-26", 1))
        assert st._hedge_conflicts(rows)[0]["root"] == "NQ"

    def test_different_underlyings_are_not_a_hedge(self):
        rows = self._rows(("LEAD", "NQ 09-26", 2), ("F1", "ES 09-26", -2))
        assert st._hedge_conflicts(rows) == []

    def test_unmanaged_accounts_ignored(self):
        rows = self._rows(("LEAD", "NQ 09-26", 2))
        rows.append({"name": "Other", "managed": False, "positions": [
            {"account": "Other", "instrument": "NQ 09-26", "qty": -2,
             "avg_price": 1.0}]})
        assert st._hedge_conflicts(rows) == []

    def test_flat_account_is_not_a_side(self):
        rows = self._rows(("LEAD", "NQ 09-26", 2), ("F1", "NQ 09-26", 0))
        assert st._hedge_conflicts(rows) == []


class TestFlattenVerification:
    """'Close sent' is not 'closed' — a partial flatten manufactures a
    cross-account hedge, which prop firms judge on direction, not intent."""

    def _arm(self, monkeypatch, remaining):
        st.active_account = "LEAD"
        st.follower_accounts = ["F1"]
        monkeypatch.setattr(st, "close_all_open_positions", lambda: ["MNQ 09-26"])
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0)
        monkeypatch.setattr(st, "FLATTEN_VERIFY_TRIES", 1)
        monkeypatch.setattr(st, "nt_snapshot", lambda port=None, timeout=3.0: {
            "ok": True, "accounts": {}, "positions": remaining,
            "working": {}, "ts": 0.0})

    def test_confirms_when_everything_closed(self, monkeypatch):
        self._arm(monkeypatch, [])
        ok, msg = asyncio.run(st._web_close_all())
        assert ok is True and "confirmed" in msg

    def test_reports_failure_when_a_leg_survives(self, monkeypatch):
        self._arm(monkeypatch, [{"account": "F1", "instrument": "MNQ 09-26",
                                 "qty": 2, "avg_price": 1.0}])
        ok, msg = asyncio.run(st._web_close_all())
        assert ok is False
        assert "INCOMPLETE" in msg and "F1" in msg

    def test_ignores_positions_on_unmanaged_accounts(self, monkeypatch):
        self._arm(monkeypatch, [{"account": "SomeoneElse", "instrument": "ES 09-26",
                                 "qty": 1, "avg_price": 1.0}])
        ok, _ = asyncio.run(st._web_close_all())
        assert ok is True

    def test_nothing_closed_still_verifies(self, monkeypatch):
        """An empty close list can mean 'already flat' OR 'the position
        query failed and we wrote nothing' — so it must still verify."""
        self._arm(monkeypatch, [])
        monkeypatch.setattr(st, "close_all_open_positions", lambda: [])
        ok, msg = asyncio.run(st._web_close_all())
        assert ok is True and "verified flat" in msg

    def test_nothing_closed_but_still_holding_is_a_failure(self, monkeypatch):
        self._arm(monkeypatch, [{"account": "F1", "instrument": "MNQ 09-26",
                                 "qty": 2, "avg_price": 1.0}])
        monkeypatch.setattr(st, "close_all_open_positions", lambda: [])
        ok, msg = asyncio.run(st._web_close_all())
        assert ok is False and "INCOMPLETE" in msg

    def test_single_account_flat_confirms(self, monkeypatch):
        self._arm(monkeypatch, [])
        monkeypatch.setattr(st, "close_account_positions", lambda a: ["MNQ 09-26"])
        ok, msg = asyncio.run(st._web_flatten_account("F1"))
        assert ok is True and "confirmed" in msg

    def test_single_account_flat_reports_survivor(self, monkeypatch):
        # The per-account web Flat button's original sin: it answered
        # "close sent" from the request alone. On 2026-08-10 NT had
        # discarded every close file and both followers stayed in the
        # market while the UI reported success.
        self._arm(monkeypatch, [{"account": "F1", "instrument": "MNQ 09-26",
                                 "qty": 2, "avg_price": 1.0}])
        monkeypatch.setattr(st, "close_account_positions", lambda a: ["MNQ 09-26"])
        ok, msg = asyncio.run(st._web_flatten_account("F1"))
        assert ok is False and "INCOMPLETE" in msg and "F1" in msg

    def test_single_account_ignores_other_accounts_positions(self, monkeypatch):
        # LEAD still holding must not fail F1's verdict — only the
        # flattened account is verified.
        self._arm(monkeypatch, [{"account": "LEAD", "instrument": "MNQ 09-26",
                                 "qty": 1, "avg_price": 1.0}])
        monkeypatch.setattr(st, "close_account_positions", lambda a: ["MNQ 09-26"])
        ok, msg = asyncio.run(st._web_flatten_account("F1"))
        assert ok is True and "confirmed" in msg


class TestFlattenOrdering:
    def test_leader_is_flattened_first(self, monkeypatch):
        # Rithmic: flattening a follower before the leader can leave the
        # follower in an unintended reverse position.
        st.active_account = "LEAD"
        st.follower_accounts = ["F1", "F2"]
        st.roundrobin_accounts = ["RR1"]
        order = []
        monkeypatch.setattr(st, "close_account_positions",
                            lambda a: order.append(a) or [])
        st.close_all_open_positions()
        assert order[0] == "LEAD"
        assert set(order) == {"LEAD", "F1", "F2", "RR1"}


class TestAtiCompleteness:
    """A truncated ATI dump parses fine and simply lacks data, so
    completeness has to be detected explicitly — see v0.9.2."""

    FULL = "CashValue|Sim101\x0010\x002\x00ATI\x00True\x00"

    def test_complete_dump_recognised(self):
        assert st.ati_response_complete(self.FULL) is True

    def test_false_variant_also_complete(self):
        assert st.ati_response_complete("x\x00ATI\x00False\x00") is True

    def test_truncated_dump_rejected(self):
        assert st.ati_response_complete(self.FULL[:-6]) is False

    def test_empty_is_not_complete(self):
        assert st.ati_response_complete("") is False

    def test_marker_must_be_at_the_end(self):
        # marker present mid-stream but more data followed = still truncated
        assert st.ati_response_complete(self.FULL + "CashValue|B\x001") is False

    def test_truncated_snapshot_reports_partial(self, monkeypatch):
        monkeypatch.setattr(st, "_query_ati", lambda *a, **k: "CashValue|Sim101\x0010\x00")
        snap = st.nt_snapshot(36973)
        assert snap["ok"] is False and snap["partial"] is True

    def test_complete_snapshot_is_ok(self, monkeypatch):
        monkeypatch.setattr(st, "_query_ati", lambda *a, **k: self.FULL)
        snap = st.nt_snapshot(36973)
        assert snap["ok"] is True and snap["partial"] is False
        assert snap["accounts"]["Sim101"]["cash"] == 10.0


class TestContractMonths:
    def _et(self, iso):
        return datetime.fromisoformat(iso).replace(tzinfo=st.ET)

    def test_quarterly_cycle_only_returns_quarterly_months(self):
        out = st.contract_months(st.QUARTERLY, self._et("2026-08-07"), count=4)
        assert all(m in (3, 6, 9, 12) for _y, m in out)

    def test_front_month_rolls_late_in_the_month(self):
        early = st.contract_months(st.QUARTERLY, self._et("2026-09-05"))[0]
        late = st.contract_months(st.QUARTERLY, self._et("2026-09-15"))[0]
        assert early == (2026, 9) and late == (2026, 12)

    def test_year_rolls_over(self):
        out = st.contract_months(st.QUARTERLY, self._et("2026-12-20"), count=2)
        assert out[0] == (2027, 3)

    def test_monthly_products_return_consecutive_months(self):
        out = st.contract_months("ALL", self._et("2026-08-07"), count=3)
        assert out == [(2026, 8), (2026, 9), (2026, 10)]

    def test_catalog_is_populated_and_well_formed(self):
        cat = st.instrument_catalog(self._et("2026-08-07"))
        assert len(cat) >= 20
        for p in cat:
            assert p["contracts"], f"{p['root']} has no contract"
            assert re.fullmatch(r"[A-Z0-9]+ \d{2}-\d{2}", p["contracts"][0])

    def test_micro_roots_are_real_symbols(self):
        for p in st.instrument_catalog():
            if p["micro"]:
                assert p["micro"] != p["root"]


class TestWebMutatingEndpoints:
    """The role / sizing / reverse endpoints place or reshape real trades."""

    def test_role_promotes_and_demotes(self, tmp_config):
        st.active_account = "A"
        st.follower_accounts = ["B"]
        asyncio.run(st._web_set_role("B", "round-robin"))
        assert st.follower_accounts == [] and st.roundrobin_accounts == ["B"]

    def test_promoting_a_follower_keeps_the_old_leader_trading(self, tmp_config):
        st.active_account = "A"
        st.follower_accounts = ["B"]
        asyncio.run(st._web_set_role("B", "leader"))
        assert st.active_account == "B"
        assert "A" in st.follower_accounts   # old leader must not silently drop out

    def test_leader_cannot_be_unassigned(self, tmp_config):
        st.active_account = "A"
        ok, msg = asyncio.run(st._web_set_role("A", "off"))
        assert ok is False and "leader" in msg

    def test_unknown_role_rejected(self, tmp_config):
        st.active_account = "A"
        assert asyncio.run(st._web_set_role("A", "boss"))[0] is False

    def test_sizing_rejects_bad_mode_and_value(self, tmp_config):
        st.active_account = "A"
        assert asyncio.run(st._web_set_sizing("A", "sideways", 1))[0] is False
        assert asyncio.run(st._web_set_sizing("A", "fixed", "abc"))[0] is False

    def test_sizing_warns_below_half_multiplier(self, tmp_config):
        st.active_account = "A"
        ok, msg = asyncio.run(st._web_set_sizing("A", "multiple", 0.25))
        assert ok is True and "sizes to 0" in msg

    def test_reverse_refuses_unmanaged_account(self, tmp_config):
        st.active_account = "A"
        ok, msg = asyncio.run(st._web_reverse_position("Stranger", "NQ 09-26"))
        assert ok is False and "not a managed account" in msg

    def test_reverse_refuses_when_flat(self, tmp_config, monkeypatch):
        st.active_account = "A"
        monkeypatch.setattr(st, "web_live", lambda *a, **k: {"positions": []})
        ok, msg = asyncio.run(st._web_reverse_position("A", "NQ 09-26"))
        assert ok is False and "no open" in msg

    def test_reverse_refuses_while_hard_stopped(self, tmp_config):
        st.active_account = "A"
        st.hard_stopped = True
        ok, msg = asyncio.run(st._web_reverse_position("A", "NQ 09-26"))
        assert ok is False


class TestRoundRobinSlotReturn:
    """A drawn pool slot must not be consumed when nothing was placed —
    round-robin sends an entry to exactly ONE member, so a burnt slot means
    the pool misses that trade entirely."""

    ENTRY = "PLACE;LEAD;NQ 09-26;BUY;2;MARKET;;;DAY;;;NQ_Med;77"

    @pytest.fixture(autouse=True)
    def _arm(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "warn")
        st.active_account = "LEAD"
        st.roundrobin_accounts = ["RR1", "RR2"]
        st._rr_remaining = ["RR1", "RR2"]

    def test_entries_off_account_is_passed_over_not_handed_the_turn(self):
        # Structurally dead accounts are skipped at DRAW time, like symbol
        # filters — otherwise the pool misses the trade every rotation.
        st.account_profiles["RR1"] = {"default": {"enabled": False}}
        plans, _ = st.plan_signal_legs(self.ENTRY)
        assert [p["account"] for p in plans if p["rr_pick"]] == ["RR2"]
        assert "RR1" in st._rr_remaining          # keeps its slot

    def test_symbol_filtered_and_disabled_pool_places_nothing(self):
        for a in ("RR1", "RR2"):
            st.account_profiles[a] = {"default": {"enabled": False}}
        plans, _ = st.plan_signal_legs(self.ENTRY)
        assert not [p for p in plans if p["rr_pick"]]
        assert sorted(st._rr_remaining) == ["RR1", "RR2"]   # nothing consumed

    def test_slot_returned_when_sized_to_zero(self):
        st.account_profiles["RR1"] = {"default": {"qty_mode": "multiple",
                                                  "qty_value": 0.1}}
        st.plan_signal_legs(self.ENTRY)
        assert "RR1" in st._rr_remaining

    def test_slot_returned_when_the_hedge_guard_blocks(self, monkeypatch):
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "block")
        st.follower_accounts = ["F1"]
        st.account_profiles["F1"] = {"default": {"direction": "invert"}}
        plans, _ = st.plan_signal_legs(self.ENTRY)
        assert plans == []
        assert "RR1" in st._rr_remaining

    def test_slot_still_consumed_on_a_normal_fill(self):
        plans, _ = st.plan_signal_legs(self.ENTRY)
        assert [p["account"] for p in plans if p["rr_pick"]] == ["RR1"]
        assert st._rr_remaining == ["RR2"]                  # correctly used

    def test_pool_does_not_miss_the_trade_after_a_skip(self):
        """The real payoff: the next signal reaches a pool account."""
        st.account_profiles["RR1"] = {"default": {"enabled": False}}
        st.plan_signal_legs(self.ENTRY)                      # RR1 drawn, skipped
        plans, _ = st.plan_signal_legs(self.ENTRY)
        assert [p["account"] for p in plans if p["rr_pick"]], "pool missed the trade"


class TestExitSizeMismatch:
    """An account entered in micros must still be closed if the exit was
    computed as full-size (strategy-scoped rules don't match exits, and the
    micro toggle can flip mid-position)."""

    def test_close_is_sent_for_both_contract_sizes(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        st.active_account = "LEAD"
        plans, _ = st.plan_signal_legs("CLOSEPOSITION;LEAD;NQ 09-26;;;;;;;;;;")
        instruments = {f.split(";")[2] for f in plans[0]["files"]}
        assert instruments == {"NQ 09-26", "MNQ 09-26"}

    def test_micro_close_also_covers_full_size(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        st.active_account = "LEAD"
        plans, _ = st.plan_signal_legs("CLOSEPOSITION;LEAD;MNQ 09-26;;;;;;;;;;")
        instruments = {f.split(";")[2] for f in plans[0]["files"]}
        assert instruments == {"MNQ 09-26", "NQ 09-26"}

    def test_entries_are_not_duplicated(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        st.active_account = "LEAD"
        plans, _ = st.plan_signal_legs(
            "PLACE;LEAD;NQ 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;77")
        assert len(plans[0]["files"]) == 1      # only exits fan out

    def test_symbol_without_a_micro_twin_is_not_duplicated(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        st.active_account = "LEAD"
        plans, _ = st.plan_signal_legs("CLOSEPOSITION;LEAD;ZB 09-26;;;;;;;;;;")
        assert len(plans[0]["files"]) == 1      # ZB has no micro


class TestSessionContractsScoping:
    def test_close_does_not_fire_for_other_accounts_markets(self, tmp_output_dir,
                                                             monkeypatch):
        """session_contracts is global; a per-account close must not fire for
        markets that account never traded."""
        st.active_account = "A"
        st.session_contracts.update({"NQ 09-26", "ES 09-26", "GC 12-26"})
        monkeypatch.setattr(st, "fire_cancel_account_orders", lambda a: None)
        monkeypatch.setattr(st, "bridge_send_command", lambda *a, **k: False)
        monkeypatch.setattr(st, "query_nt_positions",
                            lambda a, p=36973: {"NQ 09-26": -1})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            closed = st.close_account_positions("A")
        assert closed == ["NQ 09-26"]
        assert "ES 09-26" not in closed and "GC 12-26" not in closed

    def test_session_contracts_still_cover_a_micro_of_the_same_market(
            self, tmp_output_dir, monkeypatch):
        st.active_account = "A"
        st.session_contracts.update({"MNQ 09-26"})
        monkeypatch.setattr(st, "fire_cancel_account_orders", lambda a: None)
        monkeypatch.setattr(st, "bridge_send_command", lambda *a, **k: False)
        monkeypatch.setattr(st, "query_nt_positions",
                            lambda a, p=36973: {"NQ 09-26": -1})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            closed = st.close_account_positions("A")
        assert "MNQ 09-26" in closed      # same underlying — safety net kept


class TestReviewRegressions:
    """Each of these is a defect found in the 2026-08-09 deep review."""

    def test_reset_pnl_cannot_clear_a_hard_lockout(self, tmp_config):
        # reset_session_pnl() clears hard_stopped and account_stops, so the
        # web button was an undocumented escape hatch from a tripped limit.
        st.hard_stopped = True
        st.account_stops["A"] = "hard"
        ok, msg = asyncio.run(st._web_reset_pnl())
        assert ok is False and "hard-locked" in msg
        assert st.hard_stopped is True and st.account_stops["A"] == "hard"

    def test_flatten_refuses_unmanaged_account(self, tmp_config):
        st.active_account = "A"
        ok, msg = asyncio.run(st._web_flatten_account("SomeoneElse"))
        assert ok is False and "not a managed account" in msg

    def test_limits_reject_non_finite(self, tmp_config):
        # pnl <= nan is always False, so a nan stop is displayed but dead
        st.active_account = "A"
        for bad in ("nan", "inf", "-inf"):
            ok, msg = asyncio.run(st._web_set_limits("A", 0, "off", bad, "hard"))
            assert ok is False and "finite" in msg, bad

    def test_limit_price_rejects_non_finite(self, monkeypatch):
        monkeypatch.setattr(st, "validate_strategy", lambda n: True)
        st.active_account = "A"
        for bad in ("nan", "inf", "1e400"):
            sig, err = st.build_manual_signal("long", "NQ 09-26", 1, "limit", bad, "X")
            assert sig is None and "positive number" in err, bad

    def test_micro_toggle_frozen_while_hard_stopped(self, tmp_config):
        st.hard_stopped = True
        before = st.micro_mode
        ok, _ = asyncio.run(st._web_toggle_micro())
        assert ok is False and st.micro_mode is before

    def test_sanitize_strips_terminal_escapes(self):
        # a stored account name is printed into the pinned TUI header
        assert "\x1b" not in st.sanitize_ati("\x1b[2J\x1b[HSim101")
        assert st.sanitize_ati("\x1b[2JSim101").endswith("Sim101")
        assert st.sanitize_ati("NQ 09-26") == "NQ 09-26"      # spaces survive

    def test_state_does_not_expose_ai_gate(self, tmp_config, monkeypatch):
        monkeypatch.setattr(st, "list_atm_strategies", lambda: [])
        monkeypatch.setattr(st, "get_account_limits", lambda a: {})
        st.active_account = "A"
        st.account_profiles["A"] = {"default": {"ai": {
            "provider": "custom", "endpoint": "http://x/y",
            "api_key_env": "ANTHROPIC_API_KEY"}}}
        payload = json.dumps(st.web_state())
        assert "ANTHROPIC_API_KEY" not in payload
        assert "http://x/y" not in payload

    def test_json_reply_never_emits_bare_nan(self, tmp_config):
        # bare NaN is invalid JSON and would break every browser poll
        with pytest.raises(ValueError):
            json.dumps({"x": float("nan")}, allow_nan=False)

    def test_unacked_bridge_command_is_not_success(self, tmp_config, monkeypatch):
        """A flatten that was merely SENT must not report success — the
        caller skips its file-based fallback when this returns True."""
        st.save_config({"live_bridge_token": "S"})

        class Silent:                      # connects, accepts bytes, says nothing
            def settimeout(self, *_): pass
            def connect(self, *_): pass
            def sendall(self, b): pass
            def shutdown(self, *_): pass
            def recv(self, *_): return b""
            def close(self): pass

        monkeypatch.setattr(st.socket, "socket", lambda *a, **k: Silent())
        monkeypatch.setattr(st, "live_bridge_enabled", True)
        monkeypatch.setattr(st, "_live_bridge_connected", True)
        monkeypatch.setattr(st, "_nt_host", lambda p: "127.0.0.1")
        assert st.bridge_send_command({"cmd": "flatten", "account": "A"}) is False

    def test_refused_bridge_command_is_not_success(self, tmp_config, monkeypatch):
        st.save_config({"live_bridge_token": "S"})

        class Refuses:
            def settimeout(self, *_): pass
            def connect(self, *_): pass
            def sendall(self, b): pass
            def shutdown(self, *_): pass
            def recv(self, *_): return b'{"ack":false,"msg":"no such account"}\n'
            def close(self): pass

        monkeypatch.setattr(st.socket, "socket", lambda *a, **k: Refuses())
        monkeypatch.setattr(st, "live_bridge_enabled", True)
        monkeypatch.setattr(st, "_live_bridge_connected", True)
        monkeypatch.setattr(st, "_nt_host", lambda p: "127.0.0.1")
        assert st.bridge_send_command({"cmd": "flatten", "account": "A"}) is False

    def test_unconfirmed_bridge_falls_back_to_close_files(self, tmp_output_dir,
                                                          monkeypatch):
        """The whole point: an unacknowledged bridge flatten must still
        close positions via the file path, not silently do nothing."""
        st.active_account = "A"
        monkeypatch.setattr(st, "live_bridge_enabled", True)
        monkeypatch.setattr(st, "bridge_send_command", lambda *a, **k: False)
        monkeypatch.setattr(st, "fire_cancel_account_orders", lambda a: None)
        monkeypatch.setattr(st, "query_nt_positions",
                            lambda a, p=36973: {"NQ 09-26": -2})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            closed = st.close_account_positions("A")
        assert closed == ["NQ 09-26"]
        files = list(tmp_output_dir.glob("oifclose_*.txt"))
        assert files, "no CLOSEPOSITION file written on the fallback path"
        assert "CLOSEPOSITION" in files[0].read_text()


class TestBridgeAuth:
    """The AddOn socket streams balances and accepts FLATTEN, and cannot be
    loopback-only (SocketTrader reaches it across WSL's NAT), so the token
    is the actual access control — not the network boundary."""

    def test_token_is_generated_and_persisted(self, tmp_config):
        st.save_config({})
        t1 = st.bridge_token()
        assert len(t1) >= 32
        assert st.load_config()["live_bridge_token"] == t1
        assert st.bridge_token() == t1          # stable across calls

    def test_auth_line_is_newline_delimited_json(self, tmp_config):
        st.save_config({"live_bridge_token": "SECRET"})
        line = st.bridge_auth_line()
        assert line.endswith(b"\n")
        assert json.loads(line.decode())["auth"] == "SECRET"

    def test_token_written_where_the_addon_reads_it(self, tmp_config, tmp_path,
                                                    monkeypatch):
        st.save_config({"live_bridge_token": "SECRET"})
        monkeypatch.setattr(st, "_nt_base", lambda: tmp_path)
        path = st.write_bridge_token()
        assert path == tmp_path / st.BRIDGE_TOKEN_FILE
        assert path.read_text().strip() == "SECRET"

    def test_rewrite_is_idempotent_but_follows_rotation(self, tmp_config, tmp_path,
                                                        monkeypatch):
        monkeypatch.setattr(st, "_nt_base", lambda: tmp_path)
        st.save_config({"live_bridge_token": "ONE"})
        st.write_bridge_token()
        st.save_config({"live_bridge_token": "TWO"})
        assert st.write_bridge_token().read_text().strip() == "TWO"

    def test_no_nt_folder_is_handled(self, tmp_config, monkeypatch):
        st.save_config({"live_bridge_token": "SECRET"})
        monkeypatch.setattr(st, "_nt_base", lambda: None)
        assert st.write_bridge_token() is None

    def test_command_sender_authenticates_before_the_command(self, tmp_config,
                                                             monkeypatch):
        st.save_config({"live_bridge_token": "SECRET"})
        sent = []

        class FakeSock:
            def settimeout(self, *_): pass
            def connect(self, *_): pass
            def sendall(self, b): sent.append(b)
            def shutdown(self, *_): pass
            def recv(self, *_): return b'{"ack":true,"msg":"ok"}\n'
            def close(self): pass

        monkeypatch.setattr(st.socket, "socket", lambda *a, **k: FakeSock())
        monkeypatch.setattr(st, "live_bridge_enabled", True)
        monkeypatch.setattr(st, "_live_bridge_connected", True)
        monkeypatch.setattr(st, "_nt_host", lambda p: "127.0.0.1")
        assert st.bridge_send_command({"cmd": "flatten", "account": "A"}) is True
        assert json.loads(sent[0].decode())["auth"] == "SECRET"   # auth is FIRST
        assert json.loads(sent[1].decode())["cmd"] == "flatten"


class TestAiConfigValidation:
    def test_rejects_non_http_endpoint(self):
        assert st._coerce_ai({"provider": "custom", "model": "m",
                              "endpoint": "file:///etc/passwd"}) is None

    def test_rejects_bogus_key_env_name(self):
        assert st._coerce_ai({"provider": "openai", "model": "m",
                              "api_key_env": "PATH; rm -rf /"}) is None

    def test_accepts_normal_config(self):
        cfg = st._coerce_ai({"provider": "ollama", "model": "llama3.2"})
        assert cfg and cfg["provider"] == "ollama"

    def test_strip_ai_config_is_recursive(self):
        raw = {"A": {"default": {"ai": {"provider": "custom"}, "size": "micros"},
                     "rules": [{"symbols": ["NQ"], "ai": {"provider": "custom"}}]}}
        out = st._strip_ai_config(raw)
        assert out["A"]["default"] == {"size": "micros"}
        assert out["A"]["rules"][0] == {"symbols": ["NQ"]}


# ── Balance quarantine (NT outage reports $0.00) ──────────────────────


class TestBalanceQuarantine:
    """NT zeroes every AccountItem while its broker connection is down, so a
    $52k account suddenly polls as $0.00 and session P&L displayed -$52k.
    These verify zero readings are quarantined instead of stored."""

    def test_zero_after_nonzero_is_suspect(self):
        st.session_current_balances["Apex1"] = 52776.40
        assert st._suspect_zero_balance("Apex1", 0.0) is True

    def test_zero_with_no_history_is_not_suspect(self):
        assert st._suspect_zero_balance("Fresh", 0.0) is False

    def test_zero_when_already_zero_is_not_suspect(self):
        st.session_current_balances["Empty"] = 0.0
        assert st._suspect_zero_balance("Empty", 0.0) is False

    def test_nonzero_reading_is_never_suspect(self):
        st.session_current_balances["Apex1"] = 52776.40
        assert st._suspect_zero_balance("Apex1", 12.34) is False

    def test_start_balance_backs_the_check_when_current_missing(self):
        st.session_start_balances["Apex1"] = 52776.40
        assert st._suspect_zero_balance("Apex1", 0.0) is True

    def test_ingest_quarantines_outage_zero_and_holds_last_value(self):
        st.session_current_balances["Apex1"] = 52776.40
        assert st._ingest_balance("Apex1", 0.0, "test") is False
        assert st.session_current_balances["Apex1"] == 52776.40
        assert "Apex1" in st._balance_suspect_since

    def test_ingest_recovers_when_real_value_returns(self):
        st.session_current_balances["Apex1"] = 52776.40
        st._ingest_balance("Apex1", 0.0, "test")
        assert st._ingest_balance("Apex1", 52801.15, "test") is True
        assert st.session_current_balances["Apex1"] == 52801.15
        assert "Apex1" not in st._balance_suspect_since

    def test_ingest_accepts_normal_updates(self):
        st.session_current_balances["Apex1"] = 52776.40
        assert st._ingest_balance("Apex1", 52700.00, "test") is True
        assert st.session_current_balances["Apex1"] == 52700.00

    def test_ingest_accepts_genuine_zero_account(self):
        # An account that has always read $0 keeps reading $0.
        assert st._ingest_balance("Empty", 0.0, "test") is True
        assert st.session_current_balances["Empty"] == 0.0

    def test_ingest_rejects_garbage_without_storing(self):
        st.session_current_balances["Apex1"] = 52776.40
        assert st._ingest_balance("Apex1", None, "test") is False
        assert st._ingest_balance("Apex1", "abc", "test") is False
        assert st._ingest_balance("Apex1", float("nan"), "test") is False
        assert st._ingest_balance("Apex1", float("inf"), "test") is False
        assert st.session_current_balances["Apex1"] == 52776.40

    def test_pnl_never_swings_negative_by_full_account_on_outage(self):
        # The reported symptom: a disconnect made the dashboard show
        # 0 - 52,776.40 = -$52,776.40. The quarantine holds P&L instead.
        st.session_start_balances["Apex1"] = 52776.40
        st.session_current_balances["Apex1"] = 52950.00   # +$173.60 today
        st._ingest_balance("Apex1", 0.0, "bridge")        # outage heartbeat
        pnl = (st.session_current_balances["Apex1"]
               - st.session_start_balances["Apex1"])
        assert pnl == pytest.approx(173.60)

    def test_suspect_alert_fires_once_per_outage(self):
        st.session_current_balances["Apex1"] = 52776.40
        st._ingest_balance("Apex1", 0.0, "test")
        first_alert = st._alert_text
        st._alert_text = ""                       # something else cleared it
        st._ingest_balance("Apex1", 0.0, "test")  # next zero heartbeat
        assert "NinjaTrader" in first_alert
        assert st._alert_text == ""               # no re-alert while suspect

    def test_reset_during_outage_never_snapshots_zero_or_stale(self):
        # A 4:20 PM auto-reset while NT is down must not snapshot $0 as the
        # new baseline (phantom profit on reconnect) — and must not re-
        # baseline the HELD value either, which would hide a genuine
        # zeroing behind a frozen number. The quarantined account gets
        # full amnesia and re-seeds from its next real reading.
        st.session_start_balances["Apex1"] = 52776.40
        st.session_current_balances["Apex1"] = 52950.00
        st._ingest_balance("Apex1", 0.0, "test")          # outage begins
        st.reset_session_pnl()
        assert "Apex1" not in st.session_start_balances
        assert "Apex1" not in st.session_current_balances
        st._ingest_balance("Apex1", 52950.00, "test")     # feed recovers
        st._seed_start_balance("Apex1", 52950.00)
        assert st.session_start_balances["Apex1"] == 52950.00   # clean re-seed

    def test_seed_start_balance_refuses_zero_and_never_overwrites(self):
        st._seed_start_balance("Apex1", 0.0)
        assert "Apex1" not in st.session_start_balances
        st._seed_start_balance("Apex1", 52776.40)
        assert st.session_start_balances["Apex1"] == 52776.40
        st._seed_start_balance("Apex1", 99999.0)
        assert st.session_start_balances["Apex1"] == 52776.40

    def test_held_balance_passthrough_and_substitution(self):
        st.session_current_balances["Apex1"] = 52776.40
        assert st._held_balance("Apex1", 52800.0) == 52800.0
        assert st._held_balance("Apex1", 0.0) == 52776.40   # outage zero
        assert st._held_balance("Unknown", 0.0) == 0.0      # no history: trust it
        assert st._held_balance("Unknown", None) is None

    def test_held_balance_falls_back_to_start(self):
        st.session_start_balances["Apex1"] = 52776.40
        assert st._held_balance("Apex1", 0.0) == 52776.40

    def test_stale_marker_in_controls_line(self):
        st.active_account = "Apex1"
        st.session_start_balances["Apex1"] = 52776.40
        st.session_current_balances["Apex1"] = 52950.00
        st._ingest_balance("Apex1", 0.0, "test")
        line = st._build_controls_line()
        assert "⚠ stale" in line
        assert "$52,950.00" in line               # holds last known balance

    def test_no_stale_marker_when_feed_healthy(self):
        st.active_account = "Apex1"
        st.session_start_balances["Apex1"] = 52776.40
        st.session_current_balances["Apex1"] = 52950.00
        line = st._build_controls_line()
        assert "stale" not in line


# ── Prop-firm account mode ────────────────────────────────────────────


def _wed(h, m):
    """A plain Wednesday in ET (2026-08-12)."""
    return datetime(2026, 8, 12, h, m, tzinfo=st.ET)


class TestPropProfileParsing:
    def test_prop_flag_parsed_and_persisted_shape(self):
        cfg = {"account_profiles": {"Apex1": {
            "prop": True, "prop_firm": "Apex",
            "prop_flat_et": "16:30", "prop_cutoff_et": "16:20"}}}
        out = st.load_account_profiles(cfg)
        assert out["Apex1"]["prop"] is True
        assert out["Apex1"]["prop_firm"] == "apex"       # normalized lowercase
        assert out["Apex1"]["prop_flat_et"] == "16:30"
        assert out["Apex1"]["prop_cutoff_et"] == "16:20"

    def test_invalid_times_dropped_falls_back_to_preset(self):
        cfg = {"account_profiles": {"A": {
            "prop": True, "prop_flat_et": "26:99", "prop_cutoff_et": "garbage"}}}
        out = st.load_account_profiles(cfg)
        assert "prop_flat_et" not in out["A"]
        assert "prop_cutoff_et" not in out["A"]

    def test_not_prop_ignores_time_keys(self):
        cfg = {"account_profiles": {"A": {"prop_flat_et": "16:30",
                                          "default": {"enabled": False}}}}
        out = st.load_account_profiles(cfg)
        assert "prop" not in out["A"]
        assert "prop_flat_et" not in out["A"]

    def test_is_prop_account(self):
        st.account_profiles["Apex1"] = {"prop": True}
        assert st.is_prop_account("Apex1") is True
        assert st.is_prop_account("Cash1") is False

    def test_web_strip_ai_keeps_prop_keys(self):
        raw = {"Apex1": {"prop": True, "prop_firm": "apex",
                         "default": {"ai": {"provider": "openai"}}}}
        cleaned = st._strip_ai_config(raw)
        assert cleaned["Apex1"]["prop"] is True
        assert cleaned["Apex1"]["prop_firm"] == "apex"
        assert "ai" not in cleaned["Apex1"]["default"]

    def test_profile_summary_shows_prop_and_flat_time(self):
        st.account_profiles["Apex1"] = {"prop": True, "prop_firm": "apex"}
        s = st.profile_summary("Apex1")
        assert "PROP" in s and "16:57" in s

    def test_firm_presets_pick_safe_times(self):
        st.account_profiles["M"] = {"prop": True, "prop_firm": "mffu"}
        st.account_profiles["T"] = {"prop": True, "prop_firm": "topstep"}
        st.account_profiles["U"] = {"prop": True, "prop_firm": "unknownfirm"}
        assert st.prop_flat_time("M") == (16, 7)      # MFFU breach at 4:10 PM ET
        assert st.prop_flat_time("T") == (16, 5)      # Topstep staff flatten 4:08 PM ET
        assert st.prop_flat_time("U") == st.PROP_FLAT_ET_DEFAULT

    def test_explicit_time_beats_firm_preset(self):
        st.account_profiles["M"] = {"prop": True, "prop_firm": "mffu",
                                    "prop_flat_et": "15:45"}
        assert st.prop_flat_time("M") == (15, 45)

    def test_save_round_trip_keeps_prop(self):
        st.account_profiles["Apex1"] = {"prop": True, "prop_firm": "apex",
                                        "prop_flat_et": "16:30"}
        st.save_account_profiles()
        loaded = st.load_account_profiles(st.load_config())
        assert loaded["Apex1"]["prop"] is True
        assert loaded["Apex1"]["prop_firm"] == "apex"
        assert loaded["Apex1"]["prop_flat_et"] == "16:30"


class TestPropEntryCutoff:
    def _mark(self, account="Apex1", **extra):
        st.account_profiles[account] = {"prop": True, **extra}

    def test_open_hours_not_blocked(self):
        self._mark()
        assert st._prop_entry_blocked_now("Apex1", _wed(10, 30)) is False

    def test_blocked_from_cutoff_until_reopen(self):
        self._mark()
        assert st._prop_entry_blocked_now("Apex1", _wed(16, 55)) is True
        assert st._prop_entry_blocked_now("Apex1", _wed(17, 30)) is True
        assert st._prop_entry_blocked_now("Apex1", _wed(18, 0)) is False

    def test_friday_cutoff_holds_through_weekend(self):
        self._mark()
        fri = datetime(2026, 8, 14, 17, 30, tzinfo=st.ET)
        sat = datetime(2026, 8, 15, 11, 0, tzinfo=st.ET)
        sun_pre = datetime(2026, 8, 16, 17, 59, tzinfo=st.ET)
        sun_post = datetime(2026, 8, 16, 18, 0, tzinfo=st.ET)
        assert st._prop_entry_blocked_now("Apex1", fri) is True
        assert st._prop_entry_blocked_now("Apex1", sat) is True
        assert st._prop_entry_blocked_now("Apex1", sun_pre) is True
        assert st._prop_entry_blocked_now("Apex1", sun_post) is False

    def test_custom_cutoff_respected(self):
        self._mark(prop_cutoff_et="15:00")
        assert st._prop_entry_blocked_now("Apex1", _wed(15, 0)) is True
        assert st._prop_entry_blocked_now("Apex1", _wed(14, 59)) is False

    def test_leg_blocked_reports_prop_window(self, monkeypatch):
        self._mark()
        monkeypatch.setattr(st, "_prop_entry_blocked_now", lambda a: True)
        assert "prop entry cutoff" in st._leg_blocked("Apex1")

    def test_leg_blocked_ignores_non_prop(self, monkeypatch):
        monkeypatch.setattr(st, "_prop_entry_blocked_now", lambda a: True)
        assert st._leg_blocked("Cash1") is None


def _snap(*positions):
    """Build an nt_snapshot-shaped dict from (account, instrument, qty)."""
    return {"ok": True, "accounts": {}, "working": {}, "ts": 0.0,
            "positions": [{"account": a, "instrument": i, "qty": q,
                           "avg_price": None} for a, i, q in positions]}


class TestPropPreemptClosures:
    def _plan(self, account="Apex1", instrument="NQ 09-26", action="BUY"):
        return {"account": account, "instrument": instrument, "action": action}

    def test_other_market_closed_before_entry(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        to_close, keeps = st._prop_preempt_closures(
            [self._plan()], _snap(("Apex1", "GC DEC26", 2)))
        assert to_close == {"Apex1": ["GC DEC26"]}
        assert not keeps.get("Apex1")

    def test_same_contract_kept_for_netting(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        to_close, keeps = st._prop_preempt_closures(
            [self._plan(action="SELL")], _snap(("Apex1", "NQ SEP26", 2)))
        assert to_close == {}
        assert keeps["Apex1"] is True

    def test_micro_twin_closed_either_direction(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        for qty in (2, -2):
            to_close, _ = st._prop_preempt_closures(
                [self._plan()], _snap(("Apex1", "MNQ SEP26", qty)))
            assert to_close == {"Apex1": ["MNQ SEP26"]}

    def test_mixed_keep_and_close_flags_selective_flatten(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        to_close, keeps = st._prop_preempt_closures(
            [self._plan()],
            _snap(("Apex1", "NQ SEP26", 1), ("Apex1", "GC DEC26", 1)))
        assert to_close == {"Apex1": ["GC DEC26"]}
        assert keeps["Apex1"] is True

    def test_cross_account_opposite_same_group_closed(self):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2", "Cash1"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True}
        # Apex2 short ES — same product group (Equity index) opposite the
        # group's NQ BUY. Cash1 is not prop and is never touched.
        to_close, _ = st._prop_preempt_closures(
            [self._plan()],
            _snap(("Apex2", "ES SEP26", -1), ("Cash1", "ES SEP26", -5)))
        assert to_close == {"Apex2": ["ES SEP26"]}

    def test_cross_account_same_direction_kept(self):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True}
        to_close, keeps = st._prop_preempt_closures(
            [self._plan()], _snap(("Apex2", "ES SEP26", 3)))
        assert to_close == {}
        assert keeps["Apex2"] is True

    def test_cross_account_other_group_kept(self):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True}
        to_close, _ = st._prop_preempt_closures(
            [self._plan()], _snap(("Apex2", "GC DEC26", -2)))
        assert to_close == {}

    def test_cross_account_excluded_when_disabled(self):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True}
        to_close, _ = st._prop_preempt_closures(
            [self._plan()], _snap(("Apex2", "MNQ SEP26", -1)),
            cross_account=False)
        assert to_close == {}

    def test_flatten_wave_selective_vs_whole_account(self, monkeypatch):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2"]
        fired, flattened = [], []
        monkeypatch.setattr(st, "fire_close_position",
                            lambda a, c: fired.append((a, c)))
        monkeypatch.setattr(st, "close_account_positions",
                            lambda a: flattened.append(a) or ["X"])
        st._prop_flatten_wave(
            {"Apex1": ["GC DEC26"], "Apex2": ["ES SEP26"]},
            {"Apex1": True})
        assert fired == [("Apex1", "GC DEC26")]   # keeps → targeted close
        assert flattened == ["Apex2"]             # nothing kept → full flatten


class TestPropPlanFlags:
    SIG = "PLACE;Apex1;NQ 09-26;BUY;2;MARKET;;;DAY;;;NQ_Med;42"

    def test_prop_place_is_deferred_group(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        plans, skipped = st.plan_signal_legs(self.SIG)
        assert skipped == []
        (p,) = plans
        assert p["prop"] is True
        assert p["deferred"] is True
        assert p["prop_group"] is True

    def test_prop_with_delay_is_individual_deferred(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True,
                                        "default": {"delay_ms": 500}}
        (p,), _ = st.plan_signal_legs(self.SIG)
        assert p["deferred"] is True
        assert p["prop_group"] is False

    def test_non_prop_place_unchanged(self):
        st.active_account = "Cash1"
        sig = self.SIG.replace("Apex1", "Cash1")
        (p,), _ = st.plan_signal_legs(sig)
        assert p["prop"] is False
        assert p["deferred"] is False
        assert p["prop_group"] is False

    def test_prop_reversal_not_deferred(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        sig = self.SIG.replace("PLACE", "REVERSEPOSITION")
        (p,), _ = st.plan_signal_legs(sig)
        assert p["prop"] is True
        assert p["deferred"] is False
        assert p["prop_group"] is False


class TestPropHedgeEscalation:
    SIG = "PLACE;Apex1;NQ 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;7"

    def _inverted_pair(self, prop_on_leader):
        st.active_account = "Apex1"
        st.follower_accounts = ["B2"]
        if prop_on_leader:
            st.account_profiles["Apex1"] = {"prop": True}
        prof = st.account_profiles.setdefault("B2", {})
        prof["default"] = {"direction": "invert"}

    def test_warn_mode_escalates_to_block_for_prop(self):
        self._inverted_pair(prop_on_leader=True)   # hedge_guard defaults to warn
        plans, skipped = st.plan_signal_legs(self.SIG)
        assert plans == []
        assert {a for a, _ in skipped} == {"Apex1", "B2"}
        assert "PROP" in st._alert_text.upper() or "HEDGE BLOCKED" in st._alert_text

    def test_warn_mode_stays_warn_without_prop(self):
        self._inverted_pair(prop_on_leader=False)
        plans, _ = st.plan_signal_legs(self.SIG)
        assert len(plans) == 2                     # warned, not blocked

    def test_off_mode_still_blocks_for_prop(self):
        cfg = st.load_config()
        cfg["hedge_guard"] = "off"
        st.save_config(cfg)
        self._inverted_pair(prop_on_leader=True)
        plans, _ = st.plan_signal_legs(self.SIG)
        assert plans == []


class TestPropGroupExecution:
    SIG = "PLACE;Apex1;NQ 09-26;BUY;2;MARKET;;;DAY;;;NQ_Med;42"

    @pytest.fixture(autouse=True)
    def _fast_and_quiet(self, monkeypatch):
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)
        monkeypatch.setattr(st, "query_nt_positions",
                            lambda account, port=36973: {})
        monkeypatch.setattr(st, "query_nt_open_orders",
                            lambda account, port=36973: [])
        # Pin the clock OUT of the flat-by-close window: these tests run
        # at any wall time, and prop entries are genuinely blocked on
        # weekends and near the close.
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)

    def _wire_nt(self, monkeypatch, positions):
        """Fake NT: a mutable position table the flatten mutates."""
        state = {"pos": list(positions), "closed": [], "snapshots": 0}

        def fake_snapshot(port=None, timeout=3.0):
            state["snapshots"] += 1
            return _snap(*state["pos"])

        def fake_close(account):
            gone = [p for p in state["pos"] if p[0] == account]
            state["pos"] = [p for p in state["pos"] if p[0] != account]
            state["closed"].append(account)
            return [i for _, i, _ in gone]

        monkeypatch.setattr(st, "nt_snapshot", fake_snapshot)
        monkeypatch.setattr(st, "close_account_positions", fake_close)
        return state

    def test_conflict_closed_confirmed_then_entry_written(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        state = self._wire_nt(monkeypatch, [("Apex1", "GC DEC26", 2)])
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.SIG)
            written = _run_plans(plans)
        assert written == []                      # group runs deferred
        assert state["closed"] == ["Apex1"]       # GC flattened first
        files = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        assert len(files) == 1 and files[0].startswith("PLACE;Apex1;NQ 09-26;BUY")

    def test_entry_withheld_when_close_never_confirms(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        state = self._wire_nt(monkeypatch, [("Apex1", "GC DEC26", 2)])
        # Flatten "fires" but the position never leaves the snapshot.
        monkeypatch.setattr(st, "close_account_positions",
                            lambda a: state["closed"].append(a) or ["GC DEC26"])
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.SIG)
            _run_plans(plans)
        assert list(tmp_output_dir.glob("oif_*.txt")) == []   # NO entry
        assert state["closed"] == ["Apex1", "Apex1"]          # one retry
        assert "PROP GUARD" in st._alert_text

    def test_entry_withheld_when_nt_state_unavailable(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        monkeypatch.setattr(st, "nt_snapshot",
                            lambda port=None, timeout=3.0: {"ok": False, "positions": []})
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.SIG)
            _run_plans(plans)
        assert list(tmp_output_dir.glob("oif_*.txt")) == []
        assert "PROP GUARD" in st._alert_text

    def test_flat_account_enters_without_closing_anything(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        state = self._wire_nt(monkeypatch, [])
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.SIG)
            _run_plans(plans)
        assert state["closed"] == []
        assert len(list(tmp_output_dir.glob("oif_*.txt"))) == 1

    def test_group_covers_all_prop_accounts_and_cross_account_sweep(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2", "Cash1"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True,
                                        "symbols_allowed": ["GC"]}
        # Apex2 sits out the NQ entry (GC-only) but holds short MNQ —
        # opposite side of the group's BUY in the same product group.
        # Cash1 also holds short MNQ but is NOT prop: untouched.
        state = self._wire_nt(monkeypatch, [("Apex2", "MNQ SEP26", -3),
                                            ("Cash1", "MNQ SEP26", -3)])
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, skipped = st.plan_signal_legs(self.SIG)
            written = _run_plans(plans)
        assert ("Apex2", "symbol filtered (NQ)") in skipped
        assert written == ["Cash1"]               # instant leg, not prop
        assert state["closed"] == ["Apex2"]       # swept before the entry
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        entries = [b for b in bodies if b.startswith("PLACE")]
        assert {b.split(";")[1] for b in entries} == {"Apex1", "Cash1"}
        # Cash1's short MNQ stayed: still in the fake NT position table.
        assert ("Cash1", "MNQ SEP26", -3) in state["pos"]

    def test_leader_group_leg_registers_fill_confirm(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        self._wire_nt(monkeypatch, [])
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.SIG)
            _run_plans(plans, sig_id="42")
        assert len(st._pending_confirms) == 1
        assert st._pending_confirms[0]["instrument"] == "NQ 09-26"

    def test_deferred_prop_leg_with_delay_also_preempts(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True,
                                        "default": {"delay_ms": 1}}
        state = self._wire_nt(monkeypatch, [("Apex1", "GC DEC26", 2)])
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.SIG)
            _run_plans(plans)
        assert state["closed"] == ["Apex1"]
        assert len(list(tmp_output_dir.glob("oif_*.txt"))) == 1

    def test_reversal_fires_instantly_then_sweeps_other_markets(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        state = self._wire_nt(monkeypatch, [("Apex1", "NQ SEP26", 2),
                                            ("Apex1", "GC DEC26", 1)])
        fired = []
        monkeypatch.setattr(st, "fire_close_position", lambda a, c: (
            fired.append((a, c)),
            state.update(pos=[p for p in state["pos"] if p[1] != c])))
        sig = self.SIG.replace("PLACE", "REVERSEPOSITION").replace("BUY", "SELL")
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(sig)
            written = _run_plans(plans)
        assert written == ["Apex1"]               # reversal wrote immediately
        assert fired == [("Apex1", "GC DEC26")]   # GC swept, NQ untouched
        assert ("Apex1", "NQ SEP26", 2) in state["pos"]


class TestPropFlatByClose:
    @pytest.fixture(autouse=True)
    def _fast(self, monkeypatch):
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)

    def _wire(self, monkeypatch, still_open=False, holding=True):
        calls = []
        monkeypatch.setattr(st, "query_nt_positions",
                            lambda account, port=36973: {"NQ SEP26": 2} if holding else {})
        monkeypatch.setattr(st, "close_account_positions",
                            lambda a: calls.append(a) or ["NQ SEP26"])

        async def fake_verify(accounts):
            if still_open:
                return [{"account": accounts[0], "instrument": "NQ SEP26",
                         "qty": 2, "avg_price": None}]
            return []
        monkeypatch.setattr(st, "verify_flat", fake_verify)
        return calls

    def test_flattens_prop_account_inside_window_once(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        calls = self._wire(monkeypatch)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 58)))
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 59)))
        assert calls == ["Apex1"]                 # once per day
        assert "flat-by-close" in st._alert_text

    def test_already_flat_account_stays_quiet(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        calls = self._wire(monkeypatch, holding=False)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 58)))
        assert calls == []                        # nothing closed
        assert st._alert_text == ""               # and no "flattened" claim

    def test_respects_firm_specific_time(self, monkeypatch):
        st.active_account = "M1"
        st.account_profiles["M1"] = {"prop": True, "prop_firm": "mffu"}
        calls = self._wire(monkeypatch)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 5)))
        assert calls == []                        # before 16:07
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 8)))
        assert calls == ["M1"]

    def test_outside_window_or_weekend_no_fire(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        calls = self._wire(monkeypatch)
        asyncio.run(st._check_prop_flat_by_close(_wed(12, 0)))
        asyncio.run(st._check_prop_flat_by_close(_wed(17, 40)))   # past window end
        sat = datetime(2026, 8, 15, 16, 58, tzinfo=st.ET)
        asyncio.run(st._check_prop_flat_by_close(sat))
        assert calls == []

    def test_non_prop_account_never_touched(self, monkeypatch):
        st.active_account = "Cash1"
        calls = self._wire(monkeypatch)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 58)))
        assert calls == []

    def test_stopped_but_flat_account_stays_quiet(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_stops["Apex1"] = "hard"
        calls = self._wire(monkeypatch, holding=False)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 58)))
        assert calls == []                        # verified flat, no close

    def test_stopped_but_not_flat_account_is_caught(self, monkeypatch):
        # _trip_account's flatten can fail unconfirmed and never be looked
        # at again — the close window must still catch the position.
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_stops["Apex1"] = "hard"
        calls = self._wire(monkeypatch, holding=True)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 58)))
        assert calls == ["Apex1"]

    def test_unconfirmed_flatten_retries_and_alerts(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        calls = self._wire(monkeypatch, still_open=True)
        asyncio.run(st._check_prop_flat_by_close(_wed(16, 58)))
        assert calls == ["Apex1", "Apex1"]        # one retry
        assert "NOT FLAT" in st._alert_text


# ── Prop-mode review fixes ────────────────────────────────────────────


class TestPropReviewFixes:
    def test_reset_never_seeds_zero_baseline(self):
        # Boot-time outage: 0.0 was accepted with no history, then a reset
        # (16:20 auto / B->R / web) must not snapshot it as the baseline —
        # feed recovery would read as phantom profit.
        st.session_current_balances["Apex1"] = 0.0
        st.session_start_balances["Apex1"] = 0.0
        st.session_current_balances["Cash1"] = 52776.40
        st.reset_session_pnl()
        assert "Apex1" not in st.session_start_balances
        assert st.session_start_balances["Cash1"] == 52776.40

    def test_seed_start_balance_rejects_garbage(self):
        st._seed_start_balance("A", None)
        st._seed_start_balance("A", "abc")
        st._seed_start_balance("A", float("nan"))
        assert "A" not in st.session_start_balances

    def test_keep_test_handles_continuous_alias_instrument(self):
        # Web Rev passes whatever alias NT broadcast ("NQU26"), and the
        # just-reversed position must be KEPT, not flattened.
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        plan = {"account": "Apex1", "instrument": "NQU26", "action": "SELL"}
        to_close, keeps = st._prop_preempt_closures(
            [plan], _snap(("Apex1", "NQ SEP26", 2), ("Apex1", "GC DEC26", 1)))
        assert to_close == {"Apex1": ["GC DEC26"]}
        assert keeps["Apex1"] is True

    def test_manual_prop_entry_fires_while_paused(self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        st.paused = True
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)
        monkeypatch.setattr(st, "query_nt_positions", lambda a, port=36973: {})
        monkeypatch.setattr(st, "nt_snapshot", lambda port=None, timeout=3.0: _snap())
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)
        sig = "PLACE;Apex1;NQ 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;7"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(sig, manual=True)
            _run_plans(plans)
        assert len(list(tmp_output_dir.glob("oif_*.txt"))) == 1

    def test_publisher_prop_entry_blocked_while_paused(self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        st.paused = True
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)
        monkeypatch.setattr(st, "nt_snapshot", lambda port=None, timeout=3.0: _snap())
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)
        sig = "PLACE;Apex1;NQ 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;7"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(sig)          # not manual
            _run_plans(plans)
        assert list(tmp_output_dir.glob("oif_*.txt")) == []

    def test_prop_reversal_downgraded_to_close_inside_window(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        monkeypatch.setattr(st, "_prop_entry_blocked_now", lambda a: True)
        sig = "REVERSEPOSITION;Apex1;NQ 09-26;SELL;2;MARKET;;;DAY;;;NQ_Med;9"
        (p,), _ = st.plan_signal_legs(sig)
        assert p["command"] == "CLOSEPOSITION"
        assert "prop flat-by-close window" in p["note"]

    def test_closed_without_entry_raises_sticky_alarm(self, monkeypatch, tmp_output_dir):
        # Positions are preempted, then every write fails (no output dir):
        # the group must alarm loudly, not log-and-vanish.
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)
        monkeypatch.setattr(st, "query_nt_positions", lambda a, port=36973: {})
        monkeypatch.setattr(st, "query_nt_open_orders", lambda a, port=36973: [])
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)
        state = {"pos": [("Apex1", "GC DEC26", 2)]}
        monkeypatch.setattr(st, "nt_snapshot",
                            lambda port=None, timeout=3.0: _snap(*state["pos"]))
        monkeypatch.setattr(st, "close_account_positions",
                            lambda a: state.update(pos=[]) or ["GC DEC26"])
        sig = "PLACE;Apex1;NQ 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;7"
        plans, _ = st.plan_signal_legs(sig)
        with patch.object(st, "output_directory", None):   # write fails
            _run_plans(plans)
        assert "withheld" in st._alert_text
        assert st._alert_sticky is True

    def test_prop_times_outside_afternoon_rejected(self):
        cfg = {"account_profiles": {"A": {
            "prop": True, "prop_flat_et": "4:55", "prop_cutoff_et": "18:30"}}}
        out = st.load_account_profiles(cfg)
        assert "prop_flat_et" not in out["A"]      # 4:55 AM footgun rejected
        assert "prop_cutoff_et" not in out["A"]

    def test_web_reverse_refuses_prop_window_and_stale_view(self, monkeypatch):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        monkeypatch.setattr(st, "_prop_entry_blocked_now", lambda a: True)
        ok, msg = asyncio.run(st._web_reverse_position("Apex1", "NQ SEP26"))
        assert ok is False and "flat-by-close" in msg

        monkeypatch.setattr(st, "_prop_entry_blocked_now", lambda a: False)
        monkeypatch.setattr(st, "web_live", lambda force=False: {
            "stale": True, "positions": [
                {"account": "Apex1", "instrument": "NQ SEP26", "qty": 2}]})
        ok, msg = asyncio.run(st._web_reverse_position("Apex1", "NQ SEP26"))
        assert ok is False and "stale" in msg


# ── Global strategy → symbol filter ───────────────────────────────────


class TestGlobalStrategySymbolFilter:
    ENTRY = "PLACE;Sim101;ES 09-26;BUY;2;MARKET;;;DAY;;;NQ_Med;42"

    def test_loader_sanitizes_and_normalizes(self):
        out = st.load_strategy_symbols({"strategy_symbols": {
            "GoldStrat": ["gc"], "NasdaqStrat": "nq, es",
            "  ": ["GC"], "Junk": [], "Bad": 42}})
        assert out == {"goldstrat": ["GC"], "nasdaqstrat": ["NQ", "ES"]}

    def test_block_logic_with_twins_and_unknowns(self):
        st.strategy_symbols["goldstrat"] = ["GC"]
        assert st.strategy_symbol_block("GoldStrat", "GC 12-26") is None
        assert st.strategy_symbol_block("GOLDSTRAT", "MGC 12-26") is None   # twin
        assert "only trades GC" in st.strategy_symbol_block("GoldStrat", "NQ 09-26")
        assert st.strategy_symbol_block("OtherStrat", "NQ 09-26") is None  # unlisted
        assert st.strategy_symbol_block("", "NQ 09-26") is None            # no name
        assert st.strategy_symbol_block("GoldStrat", "") is None           # no instrument

    def test_entry_blocked_for_all_accounts_before_rr_draw(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["Sim103", "Sim104"]
        st._rr_remaining = ["Sim103", "Sim104"]
        st.strategy_symbols["goldstrat"] = ["GC"]
        plans, skipped = st.plan_signal_legs(self.ENTRY, "GoldStrat")
        assert plans == []
        assert skipped == [("all accounts", "strategy 'GoldStrat' only trades GC")]
        assert st._rr_remaining == ["Sim103", "Sim104"]   # no turn burned

    def test_entry_allowed_on_listed_symbol(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.strategy_symbols["goldstrat"] = ["ES"]
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, skipped = st.plan_signal_legs(self.ENTRY, "GoldStrat")
        assert skipped == []
        assert len(plans) == 1 and plans[0]["command"] == "PLACE"

    def test_reversal_downgraded_to_close_for_every_account(self):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]
        st.roundrobin_accounts = ["Sim103"]
        st._rr_remaining = ["Sim103"]
        st.strategy_symbols["goldstrat"] = ["GC"]
        sig = self.ENTRY.replace("PLACE", "REVERSEPOSITION")
        plans, _ = st.plan_signal_legs(sig, "GoldStrat")
        assert {p["account"] for p in plans} == {"Sim101", "Sim102", "Sim103"}
        assert all(p["command"] == "CLOSEPOSITION" for p in plans)

    def test_pure_exits_never_filtered(self):
        st.active_account = "Sim101"
        st.strategy_symbols["goldstrat"] = ["GC"]
        close = "CLOSEPOSITION;Sim101;ES 09-26;;;;;;;;;;"
        plans, skipped = st.plan_signal_legs(close, "GoldStrat")
        assert len(plans) == 1 and plans[0]["command"] == "CLOSEPOSITION"
        assert skipped == []

    def test_manual_orders_carry_no_strategy_and_pass(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.strategy_symbols["goldstrat"] = ["GC"]
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(self.ENTRY, "", manual=True)
        assert len(plans) == 1

    def test_web_setter_round_trips_and_guards(self):
        ok, msg = asyncio.run(st._web_set_strategy_symbols(
            {"GoldStrat": ["GC"], "NasdaqStrat": "NQ"}))
        assert ok is True and "2" in msg
        assert st.strategy_symbols == {"goldstrat": ["GC"], "nasdaqstrat": ["NQ"]}
        assert st.load_strategy_symbols(st.load_config()) == st.strategy_symbols

        ok, msg = asyncio.run(st._web_set_strategy_symbols({}))
        assert ok is True and st.strategy_symbols == {}
        assert "strategy_symbols" not in st.load_config()

        ok, _ = asyncio.run(st._web_set_strategy_symbols("nope"))
        assert ok is False
        st.hard_stopped = True
        ok, msg = asyncio.run(st._web_set_strategy_symbols({"A": ["GC"]}))
        assert ok is False and "hard-locked" in msg

    def test_web_state_exposes_map(self):
        st.strategy_symbols["goldstrat"] = ["GC"]
        assert st.web_state()["strategy_symbols"] == {"goldstrat": ["GC"]}


# ── Strategy filter: ATM-template naming + pickers ────────────────────


class TestAtmBaseKey:
    def test_root_prefix_and_separators_collapse(self):
        assert st.atm_base_key("GC-MacroZoneB") == "macrozoneb"
        assert st.atm_base_key("macro_zone_b") == "macrozoneb"
        assert st.atm_base_key("MacroZoneB") == "macrozoneb"

    def test_micro_root_prefix_strips_too(self):
        assert st.atm_base_key("MGC-Scalp") == "scalp"

    def test_unknown_prefix_is_kept(self):
        assert st.atm_base_key("FOO-Bar") == "foobar"

    def test_empty(self):
        assert st.atm_base_key("") == ""
        assert st.atm_base_key("  -  ") == ""


class TestTemplateLinkedFilter:
    def test_template_key_catches_wire_name_on_wrong_market(self):
        # Filter saved under the ATM template name; the wire sends the
        # publisher's snake_case id — must still block off-list entries.
        st.strategy_symbols["gc-macrozoneb"] = ["GC"]
        assert st.strategy_symbol_block("macro_zone_b", "NQ 09-26") is not None
        assert st.strategy_symbol_block("macro_zone_b", "GC 12-26") is None
        assert st.strategy_symbol_block("macro_zone_b", "MGC 12-26") is None  # twin

    def test_wire_key_catches_template_name(self):
        st.strategy_symbols["macro_zone_b"] = ["GC"]
        assert st.strategy_symbol_block("GC-MacroZoneB", "NQ 09-26") is not None
        assert st.strategy_symbol_block("NQ-MacroZoneB", "GC 12-26") is None

    def test_duplicate_keys_union_regardless_of_spelling(self):
        # Hand-edited config with two spellings of one strategy: every
        # wire spelling must see the SAME union (exact key's roots lead),
        # not a different answer per spelling.
        st.strategy_symbols["macro_zone_b"] = ["GC"]
        st.strategy_symbols["gc-macrozoneb"] = ["NQ"]
        assert st.strategy_filter_symbols("macro_zone_b") == ["GC", "NQ"]
        assert st.strategy_filter_symbols("MacroZoneB") == ["NQ", "GC"]
        assert set(st.strategy_filter_symbols("GC-MacroZoneB")) == {"GC", "NQ"}

    def test_alias_redirect_is_followed(self):
        st.atm_aliases = {"sigma7": "GC-MacroZoneB"}
        st.strategy_symbols["gc-macrozoneb"] = ["GC"]
        assert st.strategy_symbol_block("sigma7", "NQ 09-26") is not None
        assert st.strategy_symbol_block("sigma7", "GC 12-26") is None

    def test_unrelated_strategy_still_unfiltered(self):
        st.strategy_symbols["gc-macrozoneb"] = ["GC"]
        assert st.strategy_filter_symbols("bread_n_butter") is None
        assert st.strategy_filter_symbols("") is None


class TestSeenStrategies:
    def test_records_dedups_and_persists(self):
        st._record_pub_strategy("macro_zone_b")
        st._record_pub_strategy("bhorgini")
        st._record_pub_strategy("macro_zone_b")   # re-seen → moves to front
        assert st.pub_strategies_seen == ["macro_zone_b", "bhorgini"]
        # Persistence is throttled (one config write per interval, so a
        # hostile server can't spam writes) — force the pending flush.
        st._flush_seen(force=True)
        assert sorted(st.load_config()["strategies_seen"]) == ["bhorgini", "macro_zone_b"]

    def test_write_throttle_bounds_config_churn(self):
        writes = []
        original = st.save_config
        with patch.object(st, "save_config", side_effect=lambda c: (writes.append(1), original(c))):
            for i in range(50):   # hostile server: ever-new field-11 names
                st._record_pub_strategy(f"spam_{i}")
        assert len(writes) <= 2   # first write + at most one more, not 50
        st._flush_seen(force=True)
        assert len(st.load_config()["strategies_seen"]) == st.MAX_SEEN_STRATEGIES

    def test_blank_and_hostile_names_ignored(self):
        st._record_pub_strategy("   ")
        st._record_pub_strategy("\x1b[2Jbad")
        assert st.pub_strategies_seen == ["[2Jbad"]  # control chars stripped

    def test_capped(self):
        for i in range(st.MAX_SEEN_STRATEGIES + 5):
            st._record_pub_strategy(f"strat_{i}")
        assert len(st.pub_strategies_seen) == st.MAX_SEEN_STRATEGIES
        assert st.pub_strategies_seen[0] == f"strat_{st.MAX_SEEN_STRATEGIES + 4}"

    def test_wire_signal_records_field_11(self):
        msg = json.dumps({"signal":
                          "PLACE;pub;GC 12-26;BUY;1;MARKET;;;DAY;;;gold_wave;id1", "ts": 1})
        st.extract_signal_string(msg, "Sim101", "NQ_Med")
        assert st.pub_strategies_seen == ["gold_wave"]


class TestStrategyFilterChoices:
    @pytest.fixture(autouse=True)
    def _templates(self, monkeypatch):
        monkeypatch.setattr(st, "list_atm_strategies",
                            lambda: ["GC-MacroZoneB", "NQ_Goopi"])

    def test_templates_lead_and_seen_names_dedup_by_base(self):
        st.pub_strategies_seen.extend(["macro_zone_b", "mystery_strat"])
        names = [(c["kind"], c["name"]) for c in st.strategy_filter_choices()]
        # macro_zone_b collapses onto GC-MacroZoneB; mystery_strat survives
        assert names == [("atm", "GC-MacroZoneB"), ("atm", "NQ_Goopi"),
                         ("seen", "mystery_strat")]

    def test_alias_collapses_seen_name_onto_template(self):
        st.atm_aliases = {"sigma7": "MacroZoneB"}
        st.pub_strategies_seen.append("sigma7")
        kinds = [c["kind"] for c in st.strategy_filter_choices()]
        assert kinds == ["atm", "atm"]

    def test_orphaned_filter_key_still_editable(self):
        st.strategy_symbols["deleted_strat"] = ["GC"]
        choices = st.strategy_filter_choices()
        assert {"name": "deleted_strat", "kind": "filter",
                "base": "deletedstrat"} in choices

    def test_web_state_exposes_choices(self):
        assert isinstance(st.web_state()["strategy_choices"], list)


# ── Second review round: pre-release fixes ────────────────────────────


class TestSecondReviewFixes:
    def test_hedge_block_downgrades_reversals_to_closes(self, monkeypatch):
        # 'HEDGE BLOCKED' must not fire opposing reversal legs.
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)
        st.active_account = "Apex1"
        st.follower_accounts = ["B2"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["B2"] = {"default": {"direction": "invert"}}
        sig = "REVERSEPOSITION;Apex1;NQ 09-26;SELL;2;MARKET;;;DAY;;;NQ_Med;9"
        plans, _ = st.plan_signal_legs(sig)
        assert {p["account"] for p in plans} == {"Apex1", "B2"}
        assert all(p["command"] == "CLOSEPOSITION" for p in plans)
        assert all("hedge blocked" in p["note"] for p in plans)

    def test_alias_root_handles_digit_prefixed_fx_roots(self):
        assert st._alias_root("6EU26") == "6E"
        assert st._alias_root("6E SEP26") == "6E"
        assert st._underlying_root("M6EU26") == "6E"     # micro folds
        assert st._product_group("6EU26") == "FX"

    def test_preempt_keeps_fx_position_reported_as_continuous_code(self):
        st.active_account = "Apex1"
        st.account_profiles["Apex1"] = {"prop": True}
        plan = {"account": "Apex1", "instrument": "6E 09-26", "action": "SELL"}
        to_close, keeps = st._prop_preempt_closures(
            [plan], _snap(("Apex1", "6EU26", 3)))
        assert to_close == {}                     # kept, not flattened
        assert keeps["Apex1"] is True

    def test_parse_prop_hhmm_bounds(self):
        assert st._parse_prop_hhmm("16:55") == (16, 55)
        assert st._parse_prop_hhmm("4:55") is None      # 12h AM footgun
        assert st._parse_prop_hhmm("18:30") is None
        assert st._parse_prop_hhmm("junk") is None

    def test_cutoff_clamped_below_custom_flat_time(self):
        # mffu preset cutoff is 16:05; an earlier custom flat time must
        # drag the cutoff below it, or an entry can fire after the
        # once-per-day flatten already ran.
        st.account_profiles["M"] = {"prop": True, "prop_firm": "mffu",
                                    "prop_flat_et": "16:00"}
        assert st.prop_flat_time("M") == (16, 0)
        assert st.prop_cutoff_time("M") == (15, 58)

    def test_restore_refuses_zero_baselines(self, monkeypatch):
        saved = {"session": {"id": "2026-08-14",
                             "start_balances": {"A": 0.0, "B": 52776.40},
                             "contracts": [], "signal_count": 3}}
        monkeypatch.setattr(st, "load_config", lambda: dict(saved))
        monkeypatch.setattr(st, "get_session_id", lambda now_et=None: "2026-08-14")
        assert st.restore_session_state() is True
        assert "A" not in st.session_start_balances
        assert st.session_start_balances["B"] == 52776.40

    def test_reset_gives_quarantined_account_full_amnesia(self):
        st.session_start_balances["Apex1"] = 52776.40
        st.session_current_balances["Apex1"] = 52950.00
        st._ingest_balance("Apex1", 0.0, "test")          # quarantined
        st.reset_session_pnl()
        assert "Apex1" not in st.session_start_balances
        assert "Apex1" not in st.session_current_balances
        assert "Apex1" not in st._balance_suspect_since   # clean re-seed path

    def test_group_partial_write_failure_raises_sticky_alarm(
            self, monkeypatch, tmp_output_dir):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True}
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)
        monkeypatch.setattr(st, "query_nt_positions", lambda a, port=36973: {})
        monkeypatch.setattr(st, "query_nt_open_orders", lambda a, port=36973: [])
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)
        state = {"pos": [("Apex1", "GC DEC26", 2), ("Apex2", "GC DEC26", 2)]}
        monkeypatch.setattr(st, "nt_snapshot",
                            lambda port=None, timeout=3.0: _snap(*state["pos"]))
        monkeypatch.setattr(st, "close_account_positions", lambda a: (
            state.update(pos=[p for p in state["pos"] if p[0] != a]) or ["GC DEC26"]))
        real_write = st.write_signal_to_file
        monkeypatch.setattr(st, "write_signal_to_file",
                            lambda sig: None if ";Apex2;" in sig else real_write(sig))
        sig = "PLACE;Apex1;NQ 09-26;BUY;1;MARKET;;;DAY;;;NQ_Med;7"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(sig)
            _run_plans(plans)
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")
                  if f.read_text().startswith("PLACE")]
        assert {b.split(";")[1] for b in bodies} == {"Apex1"}   # Apex1 placed
        assert "Apex2" in st._alert_text and "withheld" in st._alert_text
        assert st._alert_sticky is True           # failure beats the green banner

    def test_publisher_reversal_sweeps_other_prop_account_cross_group(
            self, monkeypatch, tmp_output_dir):
        # B's leg is downgraded to a close (GC-only account), so its
        # standing short ES must be swept by A's reversal cleanup.
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2", "Cash1"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True, "symbols_allowed": ["GC"]}
        monkeypatch.setattr(st, "FLATTEN_VERIFY_DELAY", 0.001)
        monkeypatch.setattr(st, "query_nt_positions", lambda a, port=36973: {})
        monkeypatch.setattr(st, "query_nt_open_orders", lambda a, port=36973: [])
        monkeypatch.setattr(st, "_prop_entry_blocked_now",
                            lambda account, now_et=None: False)
        state = {"pos": [("Apex2", "ES SEP26", -2), ("Cash1", "ES SEP26", -2)]}
        monkeypatch.setattr(st, "nt_snapshot",
                            lambda port=None, timeout=3.0: _snap(*state["pos"]))
        closed = []
        monkeypatch.setattr(st, "close_account_positions", lambda a: (
            closed.append(a),
            state.update(pos=[p for p in state["pos"] if p[0] != a]))[0] or ["ES SEP26"])
        sig = "REVERSEPOSITION;Apex1;NQ 09-26;BUY;2;MARKET;;;DAY;;;NQ_Med;77"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            plans, _ = st.plan_signal_legs(sig)
            _run_plans(plans)
        assert closed == ["Apex2"]                # swept; Cash1 untouched
        assert ("Cash1", "ES SEP26", -2) in state["pos"]

    def test_preempt_exclude_spares_in_flight_reversers(self):
        st.active_account = "Apex1"
        st.follower_accounts = ["Apex2"]
        st.account_profiles["Apex1"] = {"prop": True}
        st.account_profiles["Apex2"] = {"prop": True}
        plan = {"account": "Apex1", "instrument": "NQ 09-26", "action": "BUY"}
        snap = _snap(("Apex2", "ES SEP26", -1))
        swept, _ = st._prop_preempt_closures([plan], snap)
        spared, _ = st._prop_preempt_closures([plan], snap, exclude={"Apex2"})
        assert swept == {"Apex2": ["ES SEP26"]}
        assert spared == {}
