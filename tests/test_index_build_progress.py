"""One fraction and one estimate for a running index build (shared/index_build_progress.py):
the phases after the corpus lines are weighted into the bar, the estimate waits until the
rate means something, and the fraction never goes backwards."""
import pytest

from shared.index_build_progress import (
    MIN_ELAPSED_FOR_ESTIMATE_S, PHASE_WEIGHTS, BuildClock, overall_fraction, remaining_minutes,
    remaining_seconds)


def test_the_weights_cover_the_whole_build_once():
    assert abs(sum(w for _, w in PHASE_WEIGHTS) - 1.0) < 1e-9
    assert [name for name, _ in PHASE_WEIGHTS] == ['pages', 'aggregates', 'commit', 'seams', 'finish']


def test_each_phase_continues_where_the_one_before_it_ended():
    assert overall_fraction('pages', 0, 10) == 0.0
    assert overall_fraction('pages', 10, 10) == pytest.approx(overall_fraction('aggregates', 0, 7))
    assert overall_fraction('aggregates', 7, 7) == pytest.approx(overall_fraction('commit', 0, 0))
    assert overall_fraction('commit', 0, 0) < overall_fraction('seams', 1, 2) < overall_fraction('finish', 0, 0) < 1.0
    assert overall_fraction('finish', 1, 1) == 1.0


def test_an_unknown_phase_and_an_empty_total_change_nothing():
    assert overall_fraction('tea', 1, 2) is None
    assert overall_fraction('aggregates', 0, 0) == pytest.approx(overall_fraction('pages', 10, 10))
    assert overall_fraction('pages', 30, 10) == pytest.approx(overall_fraction('pages', 10, 10))   # never past its phase


def test_no_estimate_until_the_rate_means_something():
    assert remaining_seconds(5.0, 0.5) is None                              # too early
    assert remaining_seconds(MIN_ELAPSED_FOR_ESTIMATE_S + 1, 0.0) is None   # nothing done
    assert remaining_seconds(MIN_ELAPSED_FOR_ESTIMATE_S + 1, None) is None
    assert remaining_seconds(60.0, 0.25) == pytest.approx(180.0)
    assert remaining_seconds(60.0, 1.0) == 0.0


def test_minutes_for_a_sentence_round_up_and_say_under_a_minute():
    assert remaining_minutes(None) is None
    assert remaining_minutes(10) == 0
    assert remaining_minutes(44.9) == 0
    assert remaining_minutes(45) == 1
    assert remaining_minutes(61) == 2
    assert remaining_minutes(14 * 60) == 14


def test_the_clock_reads_phases_in_order_and_never_goes_back():
    clock = BuildClock(now=100.0)
    assert clock.update('pages', 55, 100, now=130.0) == pytest.approx(0.55 * 0.55)
    assert clock.minutes_left(now=130.0) == 2                   # 30 s for 30%: 70 s more
    clock.update('aggregates', 1, 2)
    at_aggregates = clock.fraction
    assert clock.update('pages', 90, 100) == at_aggregates      # a late lines tick: no step back
    assert clock.phase == 'aggregates'
    clock.update('commit', 0, 0)
    assert clock.phase == 'commit' and clock.percent() == int(100 * (0.55 + 0.08))
    clock.update('finish', 1, 1)
    assert clock.percent() == 100 and clock.minutes_left(now=10_000.0) == 0


def test_the_estimate_is_not_given_in_the_first_seconds():
    clock = BuildClock(now=0.0)
    clock.update('pages', 50, 100, now=5.0)
    assert clock.minutes_left(now=5.0) is None
    assert clock.minutes_left(now=MIN_ELAPSED_FOR_ESTIMATE_S) is not None
