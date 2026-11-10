"""Elapsed/ETA arithmetic for paused searches — pure functions, no Qt.

The headline property is that the displayed elapsed time is *constant* while a
worker is parked, and the headline regression is that none of it reads the wall
clock any more.
"""

import pytest

from genizah_app import _PauseCtx, effective_elapsed, paused_seconds


# --------------------------------------------------------------- paused_seconds

def test_paused_seconds_zero_when_never_paused():
    assert paused_seconds(0.0, 0.0, 1000.0) == 0.0


def test_paused_seconds_includes_an_in_progress_pause():
    # parked at t=100, now t=130 -> 30s of this pause, on top of 5s banked
    assert paused_seconds(5.0, 100.0, 130.0) == pytest.approx(35.0)


def test_paused_seconds_never_goes_negative_on_a_backwards_reading():
    assert paused_seconds(2.0, 100.0, 90.0) == pytest.approx(2.0)


# ------------------------------------------------------------ effective_elapsed

def test_effective_elapsed_subtracts_parked_time():
    assert effective_elapsed(200.0, 100.0, 30.0) == pytest.approx(70.0)


def test_effective_elapsed_is_zero_before_a_run_starts():
    assert effective_elapsed(200.0, None, 0.0) == 0.0


def test_a_run_starting_at_monotonic_zero_still_counts():
    """time.monotonic()'s zero point is arbitrary, so 0.0 is a legitimate
    reading — it must not be mistaken for "no run yet"."""
    assert effective_elapsed(200.0, 0.0, 0.0) == pytest.approx(200.0)


def test_effective_elapsed_clamps_at_zero():
    assert effective_elapsed(120.0, 100.0, 999.0) == 0.0


# --------------------------------------------------- the frozen-while-paused law

def test_displayed_elapsed_is_constant_while_parked():
    """Advancing the clock during a pause must not move the number.

    This is the whole point of including the in-progress pause in
    paused_seconds: the same delta is added to both sides of the subtraction.
    """
    ctx = _PauseCtx()
    ctx.reset_for_run(1, mono_start=1000.0)
    ctx.pause_started = 1060.0                  # parked after 60s of work

    readings = [ctx.elapsed(now) for now in (1060.0, 1100.0, 1400.0, 5000.0)]
    assert readings == [pytest.approx(60.0)] * 4, readings


def test_work_resumes_accumulating_after_an_unpause():
    ctx = _PauseCtx()
    ctx.reset_for_run(1, mono_start=1000.0)
    ctx.pause_started = 1060.0
    # user resumes at t=1360 -> 300s parked
    ctx.paused_total += 1360.0 - ctx.pause_started
    ctx.pause_started = 0.0

    assert ctx.elapsed(1360.0) == pytest.approx(60.0)
    assert ctx.elapsed(1380.0) == pytest.approx(80.0)


def test_two_pause_cycles_accumulate():
    ctx = _PauseCtx()
    ctx.reset_for_run(1, mono_start=0.0)
    ctx.paused_total = 10.0 + 25.0              # two completed pauses
    assert ctx.elapsed(100.0) == pytest.approx(65.0)


# --------------------------------------------------------------- the ETA property

def test_eta_ignores_parked_time():
    """on_comp_progress derives rate = curr/elapsed and remaining from it, so the
    elapsed fix is the ETA fix. 50 of 100 chunks after 60s of work and 300s
    parked should predict ~60s left, not ~360s."""
    ctx = _PauseCtx()
    ctx.reset_for_run(1, mono_start=0.0)
    ctx.paused_total = 300.0
    now = 360.0

    elapsed = ctx.elapsed(now)
    assert elapsed == pytest.approx(60.0)

    curr, total = 50, 100
    rate = curr / elapsed
    remaining = (total - curr) / rate
    assert remaining == pytest.approx(60.0)


# ------------------------------------------------------- the wall-clock regression

def test_no_elapsed_display_reads_the_wall_clock():
    """A five-minute NTP step must not move elapsed or the ETA.

    An earlier revision kept `time.time() - start` as the base and only measured
    the paused term monotonically, which fixes nothing — the base jumps with the
    clock. This asserts the property directly: elapsed is a pure function of
    monotonic inputs, so a wall-clock jump is not even an input.
    """
    import ast
    import inspect
    import textwrap

    import genizah_app

    def _calls_wall_clock(fn):
        """True if the function BODY calls time.time(). Parsed, not grepped:
        these functions discuss time.time() at length in their docstrings."""
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        return any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == 'time'
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == 'time'
            for node in ast.walk(tree)
        )

    for fn in (effective_elapsed, paused_seconds, _PauseCtx.elapsed):
        assert not _calls_wall_clock(fn), fn.__name__

    # And the five display sites no longer subtract a wall-clock start.
    src = inspect.getsource(genizah_app)
    assert 'time.time() - self.search_start_time' not in src
    assert 'time.time() - self.comp_search_start_time' not in src
