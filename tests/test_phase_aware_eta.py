# -*- coding: utf-8 -*-
"""Phase 97 U-01 — _PhaseAwareETA: 4-phase EWMA smoothing with sum composition.

Tests:
  T-E-1a  test_four_phases_tracked_independently — each phase has its own
           EWMA smoothing; recording bytes on one phase does not affect another.
  T-E-1b  test_compose_overall_eta_is_sum — compose_overall_eta() returns the
           SUM of per-phase ETAs (sequential phases, Codex MEDIUM #5 resolution).
"""
def _make_eta():
    from shared.local_indexer import _PhaseAwareETA
    return _PhaseAwareETA()


# ---------------------------------------------------------------------------
# T-E-1a
# ---------------------------------------------------------------------------

def test_four_phases_tracked_independently():
    """Each phase has its own EWMA; recording on one must not alter another."""
    eta = _make_eta()
    phases = eta.PHASES
    assert len(phases) == 4, f"Expected 4 phases, got {phases}"

    # Feed bytes to only the first phase; others should remain at 0 rate.
    first_phase = phases[0]
    other_phases = phases[1:]

    eta.record(first_phase, 1_000_000)  # 1 MB in some interval
    # Rates for other phases must be zero (no samples fed)
    for p in other_phases:
        assert eta._bytes_per_sec[p] == 0.0, (
            f"Phase '{p}' rate should be 0.0 before any sample, "
            f"got {eta._bytes_per_sec[p]}"
        )

    # The sampled phase should have a non-zero rate after recording
    assert eta._bytes_per_sec[first_phase] > 0.0, (
        f"Phase '{first_phase}' rate should be > 0 after record(), "
        f"got {eta._bytes_per_sec[first_phase]}"
    )

    # Now feed the second phase with a different rate; first phase unchanged
    second_phase = phases[1]
    rate_first_before = eta._bytes_per_sec[first_phase]
    eta.record(second_phase, 500_000)
    assert eta._bytes_per_sec[first_phase] == rate_first_before, (
        "Recording on a different phase must not change first phase's EWMA"
    )
    assert eta._bytes_per_sec[second_phase] > 0.0, (
        f"Phase '{second_phase}' rate should be > 0 after record()"
    )

    # Verify ALPHA attribute is documented
    assert 0 < eta.ALPHA <= 1.0, f"ALPHA should be in (0, 1], got {eta.ALPHA}"


# ---------------------------------------------------------------------------
# T-E-1b (Codex MEDIUM #5 resolution: SUM composition)
# ---------------------------------------------------------------------------

def test_compose_overall_eta_is_sum():
    """compose_overall_eta() == sum(phase_eta_seconds(p) for active p).

    Uses set_remaining() to inject known remaining-bytes values and
    fixed EWMA rates so the sum is deterministic.
    """
    eta = _make_eta()
    phases = eta.PHASES
    assert len(phases) == 4

    # Inject known rates directly (bypassing time-based record())
    known_rates = [100.0, 200.0, 50.0, 400.0]  # bytes/sec per phase
    known_remaining = [10_000, 20_000, 5_000, 40_000]  # bytes

    for p, rate, remaining in zip(phases, known_rates, known_remaining):
        eta._bytes_per_sec[p] = rate
        eta.set_remaining(p, remaining)

    # Expected individual ETAs
    expected_etas = [remaining / rate for rate, remaining in zip(known_rates, known_remaining)]
    expected_total = sum(expected_etas)

    total_eta = eta.compose_overall_eta()
    assert abs(total_eta - expected_total) < 1e-6, (
        f"compose_overall_eta should be sum of individual ETAs: "
        f"expected {expected_total}, got {total_eta}"
    )

    # Verify it is a SUM (not max, not harmonic mean)
    for p_idx, phase in enumerate(phases):
        assert abs(eta.phase_eta_seconds(phase) - expected_etas[p_idx]) < 1e-6, (
            f"phase_eta_seconds({phase}) mismatch"
        )

    # When all remaining = 0, compose_overall_eta should return 0 (no pending work)
    for p in phases:
        eta.set_remaining(p, 0)
    assert eta.compose_overall_eta() == 0.0, (
        "compose_overall_eta with no remaining bytes should return 0.0"
    )
