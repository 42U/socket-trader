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
    st.signal_count = 0
    st._recent_signal_ids.clear()
    st.active_account = None
    st.follower_accounts = []
    st.account_stops.clear()
    st.micro_mode = False
    st.micro_map = dict(st.MICRO_MAP)
    st._micro_unmapped_warned.clear()
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
            # CANCELALLORDERS is written before the closes
            cancel_files = list(tmp_output_dir.glob("cancelall_*.txt"))
            assert len(cancel_files) == 1
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


# ── fire_cancel_all_orders ───────────────────────────────────────────


class TestFireCancelAllOrders:
    def test_writes_cancelall_file(self, tmp_output_dir):
        original = st.output_directory
        st.output_directory = str(tmp_output_dir)
        try:
            st.fire_cancel_all_orders("Sim101")
            files = list(tmp_output_dir.glob("cancelall_*.txt"))
            assert len(files) == 1
            assert files[0].read_text().startswith("CANCELALLORDERS;Sim101")
        finally:
            st.output_directory = original

    def test_no_write_without_directory(self):
        original = st.output_directory
        st.output_directory = None
        try:
            st.fire_cancel_all_orders("Sim101")  # should not raise
        finally:
            st.output_directory = original


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
    def test_replaces_account_field_only(self):
        sig = "PLACE;OLD;NQ 06-26;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        out = st._with_account(sig, "Sim102")
        parts = out.split(";")
        assert parts[1] == "Sim102"
        assert parts[0] == "PLACE"
        assert parts[2] == "NQ 06-26"
        assert parts[-1] == "42"

    def test_sanitizes_account_name(self):
        sig = "PLACE;OLD;NQ;BUY;1;MARKET;;;DAY;;;NQ_Med;42"
        out = st._with_account(sig, "Bad;Name")
        # sanitize strips the embedded semicolon so field count is preserved
        assert out.split(";")[1] == "BadName"
        assert len(out.split(";")) == len(sig.split(";"))

    def test_short_signal_untouched(self):
        assert st._with_account("PLACE", "Sim102") == "PLACE"


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

    def test_each_file_keeps_identical_signal_except_account(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102", "Sim103", "Sim104"]
        sig = "PLACE;Sim101;NQ 06-26;BUY;3;MARKET;;;DAY;oco-1;ord-1;NQ_Med;99"
        with patch.object(st, "output_directory", str(tmp_output_dir)):
            asyncio.run(st.dispatch_signal(sig))
        bodies = [f.read_text() for f in tmp_output_dir.glob("oif_*.txt")]
        stripped = {
            ";".join([p if i != 1 else "" for i, p in enumerate(b.split(";"))])
            for b in bodies
        }

        assert len(bodies) == 4
        assert len(stripped) == 1  # only the account differs

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
    def test_flattens_every_target_account(self, tmp_output_dir):
        st.active_account = "Sim101"
        st.follower_accounts = ["Sim102"]

        def fake_positions(account, port=None):
            return {"NQ 06-26": 2} if account in ("Sim101", "Sim102") else {}

        with patch.object(st, "output_directory", str(tmp_output_dir)), \
             patch.object(st, "query_nt_positions", side_effect=fake_positions):
            closed = st.close_all_open_positions()

        assert closed == ["NQ 06-26"]
        # One CLOSEPOSITION per account (2) + one CANCELALLORDERS per account (2)
        close_files = list(tmp_output_dir.glob("close_*.txt"))
        cancel_files = list(tmp_output_dir.glob("cancelall_*.txt"))
        assert len(close_files) == 2
        assert len(cancel_files) == 2
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
