"""Tests for SocketTrader pure functions and state transitions.

Run: pytest test_sockettrader.py -v
"""
from __future__ import annotations

import asyncio
import json
import os
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
def reset_session_state():
    """Reset global session state between tests."""
    st.session_start_balances.clear()
    st.session_current_balances.clear()
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
            files = list(tmp_output_dir.glob("close_*.txt"))
            assert len(files) == 1
            content = files[0].read_text()
            assert content.startswith("CLOSEPOSITION;Sim101;NQ 06-26")
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
            close_files = list(tmp_output_dir.glob("close_*.txt"))
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
            close_files = list(tmp_output_dir.glob("close_*.txt"))
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
            close_files = list(tmp_output_dir.glob("close_*.txt"))
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
            close_files = list(tmp_output_dir.glob("close_*.txt"))
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
            files = sorted(tmp_output_dir.glob("cancel_*.txt"))
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
        cancels = [f.read_text() for f in tmp_output_dir.glob("cancel_*.txt")]
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
        close_files = list(tmp_output_dir.glob("close_*.txt"))
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
        close_files = list(tmp_output_dir.glob("close_*.txt"))
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
        close_files = list(tmp_output_dir.glob("close_*.txt"))
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
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "block")
        st.active_account = "LEAD"
        st.follower_accounts = ["F1"]

    def _actions(self, sig=None):
        plans, skipped = st.plan_signal_legs(sig or self.ENTRY)
        return ({p["account"]: p["signal"].split(";")[3] for p in plans}, skipped)

    def test_inconsistent_invert_is_blocked(self):
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {}
        assert all("hedge guard" in r for _, r in skipped)

    def test_consistent_invert_is_allowed(self):
        for a in ("LEAD", "F1"):
            st.account_profiles[a] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "SELL"} and not skipped

    def test_no_invert_is_allowed(self):
        acts, _ = self._actions()
        assert acts == {"LEAD": "BUY", "F1": "BUY"}

    def test_micro_and_full_are_the_same_underlying(self):
        # F1 sized to micros still collides with the leader's full-size NQ
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        st.account_profiles["F1"] = {"default": {"size": "micros"}}
        acts, skipped = self._actions()
        assert acts == {} and skipped

    def test_different_symbols_do_not_collide(self):
        # F1 only trades gold, so it never takes the NQ entry at all
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        st.account_profiles["F1"] = {"symbols_allowed": ["GC"]}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "SELL"}
        assert [r for a, r in skipped if a == "F1" and "symbol" in r]

    def test_exits_are_never_blocked(self):
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        plans, _ = st.plan_signal_legs("CLOSEPOSITION;LEAD;NQ 09-26;;;;;;;;;;")
        assert [p["account"] for p in plans] == ["LEAD", "F1"]
        assert all(p["command"] == "CLOSEPOSITION" for p in plans)

    def test_warn_mode_fires_but_flags(self, monkeypatch):
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "warn")
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        acts, skipped = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "BUY"} and not skipped

    def test_off_mode_disables_the_guard(self, monkeypatch):
        monkeypatch.setattr(st, "hedge_guard_mode", lambda: "off")
        st.account_profiles["LEAD"] = {"default": {"direction": "invert"}}
        acts, _ = self._actions()
        assert acts == {"LEAD": "SELL", "F1": "BUY"}


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

    def test_nothing_open_skips_verification(self, monkeypatch):
        self._arm(monkeypatch, [])
        monkeypatch.setattr(st, "close_all_open_positions", lambda: [])
        ok, msg = asyncio.run(st._web_close_all())
        assert ok is True and "nothing closed" in msg


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
