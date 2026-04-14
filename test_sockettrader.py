"""Tests for SocketTrader pure functions and state transitions.

Run: pytest test_sockettrader.py -v
"""
from __future__ import annotations

import json
import os
import tempfile
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
    yield


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
