"""RED tests for Phase 81B SKILL-06 token-bucket throttle.

All imports from skills.cairo_genizah_research.scripts.throttle fail at
collection time with ModuleNotFoundError until Plan 02 lands throttle.py.
That is the intended RED state — Plan 02's acceptance criteria flips them GREEN.

Tests use monkeypatch + fake clock (never time.sleep) to keep CI fast.
Throttle state is filesystem-backed; tests use tmp_path for isolation.
"""
import importlib
import time
import pytest
from skills.cairo_genizah_research.scripts.throttle import acquire, _read_state, _write_state


# ---------------------------------------------------------------------------
# SKILL-06: Token-bucket throttle tests
# ---------------------------------------------------------------------------

def test_throttle_first_call_does_not_block(tmp_path, monkeypatch):
    """First call to acquire uses initial burst tokens — wait must be < 0.1s."""
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    wait = acquire("search", rpm=24, burst=5)
    assert wait < 0.1


def test_throttle_burst_5_then_blocks(tmp_path, monkeypatch):
    """After burst of 5 calls, 6th call must wait approximately 60/rpm seconds."""
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    # Use a fake clock to avoid actual sleeping
    _fake_time = [0.0]

    def fake_time():
        return _fake_time[0]

    monkeypatch.setattr(time, "time", fake_time)
    # Suppress actual sleep — just record it
    slept = []

    def fake_sleep(s):
        _fake_time[0] += s
        slept.append(s)

    monkeypatch.setattr(time, "sleep", fake_sleep)
    # First 5 calls consume burst — should be immediate
    for _ in range(5):
        acquire("search", rpm=24, burst=5)
    # 6th call must wait
    wait = acquire("search", rpm=24, burst=5)
    expected_min = 60.0 / 24 - 0.1  # ~2.4s
    assert wait >= expected_min, f"Expected wait >= {expected_min:.2f}s, got {wait:.4f}s"


def test_throttle_buckets_are_isolated(tmp_path, monkeypatch):
    """Exhausting 'search' bucket does not block 'browse' bucket (separate buckets)."""
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    _fake_time = [0.0]

    def fake_time():
        return _fake_time[0]

    monkeypatch.setattr(time, "time", fake_time)
    slept = []

    def fake_sleep(s):
        _fake_time[0] += s
        slept.append(s)

    monkeypatch.setattr(time, "sleep", fake_sleep)
    # Exhaust search bucket (5 burst calls)
    for _ in range(5):
        acquire("search", rpm=24, burst=5)
    # browse bucket is fresh — must not block
    browse_wait = acquire("browse", rpm=24, burst=5)
    assert browse_wait < 0.1


def test_throttle_state_persists_across_processes(tmp_path, monkeypatch):
    """Token count must decrease monotonically across two acquire calls (state persisted)."""
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    acquire("search", rpm=24, burst=5)
    state1 = _read_state(tmp_path)
    tokens_after_first = state1.get("search", {}).get("tokens", 5.0)

    acquire("search", rpm=24, burst=5)
    state2 = _read_state(tmp_path)
    tokens_after_second = state2.get("search", {}).get("tokens", 5.0)

    # After a second call at the same instant, tokens must be <= after first
    assert tokens_after_second <= tokens_after_first


def test_throttle_15_search_plus_10_browse_completes_under_60_seconds(tmp_path, monkeypatch):
    """SKILL-06 verification math: 15 search + 10 browse <= 60s total wall-clock advance.

    Burst=5 means first 5 of each endpoint are immediate. Remaining calls space out
    at 60/rpm seconds each. With rpm=24, 15 search calls: ~37.5s; 10 browse: ~12.5s.
    Total ~50s, well under 60s.
    """
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    _fake_time = [0.0]

    def fake_time():
        return _fake_time[0]

    monkeypatch.setattr(time, "time", fake_time)

    def fake_sleep(s):
        _fake_time[0] += s

    monkeypatch.setattr(time, "sleep", fake_sleep)
    start = _fake_time[0]
    for _ in range(15):
        acquire("search", rpm=24, burst=5)
    for _ in range(10):
        acquire("browse", rpm=24, burst=5)
    elapsed = _fake_time[0] - start
    assert elapsed <= 60.0, f"Total elapsed {elapsed:.2f}s exceeds 60s budget"


def test_throttle_handles_corrupt_state_file(tmp_path, monkeypatch):
    """Corrupt throttle.json must not raise — skill recovers by treating state as empty."""
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    state_file = tmp_path / "throttle.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text("not valid json }{", encoding="utf-8")
    # Should not raise
    wait = acquire("search", rpm=24, burst=5)
    assert wait < 0.1  # recovered with fresh state = immediate first call


def test_throttle_env_override_lowers_rpm(tmp_path, monkeypatch):
    """GENIZAH_SKILL_REQ_PER_MIN=12 halves the effective rpm vs default 24."""
    monkeypatch.setenv("CAIRO_GENIZAH_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("GENIZAH_SKILL_REQ_PER_MIN", "12")
    _fake_time = [0.0]

    def fake_time():
        return _fake_time[0]

    monkeypatch.setattr(time, "time", fake_time)
    slept = []

    def fake_sleep(s):
        _fake_time[0] += s
        slept.append(s)

    monkeypatch.setattr(time, "sleep", fake_sleep)
    # Exhaust burst (5 calls at rpm=12 from env)
    for _ in range(5):
        acquire("search", burst=5)  # rpm comes from env
    # 6th call — wait should reflect rpm=12 (60/12 = 5.0s per token)
    wait = acquire("search", burst=5)
    expected_min_at_12rpm = 60.0 / 12 - 0.1  # ~4.9s
    assert wait >= expected_min_at_12rpm, (
        f"Expected wait >= {expected_min_at_12rpm:.2f}s for rpm=12, got {wait:.4f}s"
    )
