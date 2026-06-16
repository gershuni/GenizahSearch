# -*- coding: utf-8 -*-
"""Phase 115 — Performance Metrics test scaffold (Wave 0).

11 test cases covering:
  - PERF-01: perf_signal emitted by SearchThread + INDEXING_COMPLETE shape
  - PERF-02: only bucketed result counts reach the payload, no raw integers
  - PERF-03: accumulate N searches -> 0 events; flush -> 1 aggregate event
  - D-05/D-06: accumulator resets after flush; GENIZAH_PERF_SAMPLE_N gate
  - D-07/D-09: scrubber allowlists perf_summary; no-consent is a no-op
  - CONSENT-08 (REVIEWS finding 1): opt-out clears the perf accumulator
  - REVIEWS finding 3: unknown mode/corpus_scope normalized to 'unknown'
  - REVIEWS finding 8: invalid env knob values clamped/defaulted

All tests are EXPECTED to be RED (ImportError / AttributeError at runtime) until
plans 02-04 land. Collection MUST succeed (symbols are referenced lazily, inside
test function bodies, so no collection-time ImportError).

Autouse fixture shape copied from tests/test_telemetry_consent_gate.py lines 24-59.
Queue-drain assertion shape from tests/test_posthog_server.py lines 53-64.
"""

from __future__ import annotations

import queue

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — resets desktop.telemetry + posthog_server state before/after each test.
# Monkeypatches load_app_config/save_app_config to use an in-memory dict,
# and replaces ph._event_queue with a fresh queue per test.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    """Reset desktop.telemetry + posthog_server state before/after each test."""
    # Use an in-memory dict as the fake config.pkl store
    fake_config: dict = {}

    def fake_load_app_config():
        return dict(fake_config)

    def fake_save_app_config(new_data: dict):
        fake_config.update(new_data)

    # Patch in both the source module AND the imported-into-telemetry names
    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    # Also patch the names as imported into desktop.telemetry
    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    # Replace ph._event_queue with a fresh per-test queue
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    # Reset desktop.telemetry module state and reload from the empty fake config.
    # tel._reset_for_tests() also clears _perf_accumulator / _perf_last_flush_time /
    # _perf_sample_counter once Phase 115 plan 02 extends it (REVIEWS finding 1).
    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    # Teardown
    tel._reset_for_tests()
    ph._reset_for_tests()


# ---------------------------------------------------------------------------
# Module-level helper — enable telemetry consent so perf tests have an active session.
# Pattern from test_telemetry_consent_gate.py.
# ---------------------------------------------------------------------------
def _enable_telemetry(tel, fake_config):
    """Enable telemetry via set_consent(True), which writes to fake_config."""
    tel.set_consent(True)
    assert tel.is_enabled()


# ===========================================================================
# Test 1: PERF-01 — SearchThread emits perf_signal on successful completion
# ===========================================================================
def test_search_thread_emits_perf_signal():
    """SearchThread emits perf_signal(elapsed_ms, result_count) on successful completion.

    perf_signal = pyqtSignal(float, int) is added to SearchThread in plan 03.
    The test constructs SearchThread with a FakeSearcher and runs it synchronously.
    """
    from gui_threads import SearchThread

    received = []

    class FakeSearcher:
        def execute_search(self, *a, **kw):
            return [1, 2, 3]

    thread = SearchThread(FakeSearcher(), 'test', 'keyword', 0)
    # perf_signal does not exist yet — this will AttributeError at runtime until plan 03 lands.
    thread.perf_signal.connect(lambda ms, rc: received.append((ms, rc)))
    thread.run()  # run synchronously (not start()) to stay in test thread

    assert len(received) == 1
    elapsed_ms, result_count = received[0]
    assert elapsed_ms > 0.0
    assert result_count == 3


# ===========================================================================
# Test 2: PERF-03 aggregate — accumulate 10x -> 0 events; flush -> exactly 1 event
# ===========================================================================
def test_no_per_search_events(_reset_telemetry_state):
    """accumulate_performance() 10x -> 0 events queued; _flush_perf_summary() -> exactly 1.

    Also asserts session_id is present on the summary (REVIEWS finding 4).
    """
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    for _ in range(10):
        tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')

    # No event should be emitted by accumulate_performance() itself
    assert ph._event_queue.qsize() == 0, (
        "accumulate_performance() must never put events on the queue directly (PERF-03)"
    )

    tel._flush_perf_summary(flush_reason='periodic')

    assert ph._event_queue.qsize() == 1, (
        f"Expected exactly 1 summary event after flush, got {ph._event_queue.qsize()}"
    )
    payload = ph._event_queue.get_nowait()
    assert payload['event'] == 'desktop_session_performance_summary'
    assert 'perf_summary' in payload['properties'], (
        "Payload must contain 'perf_summary' nested dict (D-07/KQ-8)"
    )
    assert payload['properties']['perf_summary']['keyword']['count'] == 10, (
        "Summary must reflect 10 accumulated searches"
    )
    # REVIEWS finding 4: session_id must be present on the summary for join to session_start/end
    assert payload['properties'].get('session_id', ''), (
        "desktop_session_performance_summary must include a non-empty session_id (REVIEWS finding 4)"
    )


# ===========================================================================
# Test 3: D-06 — accumulator resets after flush (no double-count across windows)
# ===========================================================================
def test_accumulator_resets_on_flush(_reset_telemetry_state):
    """Flush resets the accumulator — the second flush event reflects only post-flush data."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    for _ in range(3):
        tel.accumulate_performance(200.0, 10, 'keyword', 'genizah')
    tel._flush_perf_summary()
    ph._event_queue.get_nowait()  # drain first event

    # Accumulate a fresh (smaller) window
    for _ in range(2):
        tel.accumulate_performance(300.0, 20, 'keyword', 'genizah')
    tel._flush_perf_summary()

    payload = ph._event_queue.get_nowait()
    # Must be 2 (the second window), not 5 (both windows combined)
    assert payload['properties']['perf_summary']['keyword']['count'] == 2, (
        "Accumulator must reset after flush so windows are independent (D-06)"
    )


# ===========================================================================
# Test 4: PERF-01 indexing — INDEXING_COMPLETE event carries expected props
# ===========================================================================
def test_indexing_complete_event_shape(_reset_telemetry_state):
    """INDEXING_COMPLETE event carries operation_kind, duration_ms, doc_count_bucket."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    tel.track_performance(
        tel.DesktopEvent.INDEXING_COMPLETE,
        duration_ms=5000.0,
        operation_kind='reindex_all',
        doc_count_bucket='100+',
    )
    payload = ph._event_queue.get_nowait()
    assert payload['event'] == 'desktop_indexing_complete', (
        f"Expected 'desktop_indexing_complete', got {payload['event']!r}"
    )
    props = payload['properties']
    assert props['duration_ms'] == 5000.0
    assert props['operation_kind'] == 'reindex_all'
    assert props['doc_count_bucket'] == '100+'


# ===========================================================================
# Test 5: PERF-02 — result counts stored as buckets only, no raw integers
# ===========================================================================
def test_perf_summary_buckets_only(_reset_telemetry_state):
    """Result counts in the flushed summary are bucketed only; no raw per-search integers.

    Also asserts: no Hebrew text and no path-like string anywhere in the payload (PERF-02).
    """
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    # Accumulate a mix of result counts covering all 4 buckets
    for rc in [0, 5, 50, 500]:
        tel.accumulate_performance(100.0, rc, 'keyword', 'genizah')

    tel._flush_perf_summary(flush_reason='periodic')

    payload = ph._event_queue.get_nowait()
    mode_stats = payload['properties']['perf_summary']['keyword']

    # The four bucket counts must be present
    assert 'bucket_0' in mode_stats, "bucket_0 key missing from perf_summary"
    assert 'bucket_1_9' in mode_stats, "bucket_1_9 key missing from perf_summary"
    assert 'bucket_10_99' in mode_stats, "bucket_10_99 key missing from perf_summary"
    assert 'bucket_100plus' in mode_stats, "bucket_100plus key missing from perf_summary"

    # Bucket counts: 0→bucket_0=1, 5→bucket_1_9=1, 50→bucket_10_99=1, 500→bucket_100plus=1
    assert mode_stats['bucket_0'] == 1
    assert mode_stats['bucket_1_9'] == 1
    assert mode_stats['bucket_10_99'] == 1
    assert mode_stats['bucket_100plus'] == 1

    # PERF-02: no key in the mode dict should be a raw per-search result integer
    # (only stat keys and bucket keys are allowed)
    _ALLOWED_STAT_KEYS = {
        'count', 'median_ms', 'p95_ms', 'min_ms', 'max_ms',
        'zero_result_count',
        'bucket_0', 'bucket_1_9', 'bucket_10_99', 'bucket_100plus',
        'corpus_genizah', 'corpus_local', 'corpus_all',
    }
    for key in mode_stats:
        assert key in _ALLOWED_STAT_KEYS, (
            f"Unexpected key {key!r} in perf_summary — raw result counts must not be keys (PERF-02)"
        )

    # PERF-02: no Hebrew text and no path-like string anywhere in the serialized payload
    payload_repr = repr(payload)
    # Hebrew characters are in range ֐–׿
    for char in payload_repr:
        code = ord(char)
        assert not (0x0590 <= code <= 0x05FF), (
            f"Hebrew character {char!r} found in perf summary payload (PERF-02 privacy)"
        )
    assert ':\\' not in payload_repr and '/' not in payload_repr, (
        f"Path-like string found in perf summary payload (PERF-02 privacy): {payload_repr!r}"
    )


# ===========================================================================
# Test 6: PERF-03 / D-05 — GENIZAH_PERF_SAMPLE_N sampling gate
# ===========================================================================
def test_sample_n_skips_runs(_reset_telemetry_state, monkeypatch):
    """GENIZAH_PERF_SAMPLE_N=2 skips every other run; resulting count reflects sampling."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    monkeypatch.setenv('GENIZAH_PERF_SAMPLE_N', '2')

    # Accumulate 4 runs; with sample_n=2 only 2 should be recorded
    for _ in range(4):
        tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')

    tel._flush_perf_summary(flush_reason='periodic')

    payload = ph._event_queue.get_nowait()
    actual_count = payload['properties']['perf_summary']['keyword']['count']
    assert actual_count == 2, (
        f"With GENIZAH_PERF_SAMPLE_N=2 and 4 runs, expected count=2, got {actual_count} (D-05)"
    )


# ===========================================================================
# Test 7: D-09 — consent gate: accumulate_performance() is a no-op without consent
# ===========================================================================
def test_accumulate_disabled_when_no_consent(_reset_telemetry_state):
    """accumulate_performance() and _flush_perf_summary() are no-ops when telemetry is disabled."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    # Fixture starts with disabled telemetry — do NOT call set_consent(True)
    assert not tel.is_enabled(), "Telemetry must be disabled by default after _reset_for_tests()"

    tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')
    tel._flush_perf_summary()

    assert ph._event_queue.qsize() == 0, (
        "No events should be queued when consent is not given (D-09)"
    )


# ===========================================================================
# Test 8: D-07 / KQ-8 — perf_summary nested payload survives the scrubber + validator
# ===========================================================================
def test_perf_summary_survives_scrubber():
    """'perf_summary' top-level key is allowlisted; numeric nested dict passes scrubber."""
    from desktop.telemetry import _scrub_props, _validate_props

    props = {
        'perf_summary': {
            'keyword': {'count': 5, 'median_ms': 300.0, 'p95_ms': 800.0},
        },
        'sample_n': 1,
    }
    scrubbed = _scrub_props(props)
    validated = _validate_props(scrubbed)

    assert 'perf_summary' in validated, (
        "'perf_summary' key must survive _scrub_props + _validate_props (D-07/KQ-8)"
    )
    assert validated['perf_summary']['keyword']['count'] == 5
    assert validated['perf_summary']['keyword']['median_ms'] == 300.0


# ===========================================================================
# Test 9: REVIEWS finding 1 / CONSENT-08 — opt-out clears the perf accumulator
# ===========================================================================
def test_opt_out_clears_perf_accumulator(_reset_telemetry_state):
    """set_consent(False) must clear the in-memory perf accumulator (CONSENT-08).

    If a user opts out after searches accumulate, no stale pre-opt-out summary
    data should be flushed on a later re-opt-in (privacy regression guard).
    """
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    # Opt in and accumulate 5 searches
    _enable_telemetry(tel, _reset_telemetry_state)
    for _ in range(5):
        tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')

    # The accumulator must be non-empty at this point
    assert tel._perf_accumulator, (
        "_perf_accumulator must be non-empty after 5 accumulate_performance() calls "
        "(plan 02 adds this module-level dict)"
    )
    assert 'keyword' in tel._perf_accumulator

    # Opt out — accumulator must be cleared
    tel.set_consent(False)
    assert tel._perf_accumulator == {}, (
        "set_consent(False) must call _clear_perf_accumulator() — "
        "no stale pre-opt-out window may survive (CONSENT-08 / REVIEWS finding 1)"
    )

    # Re-opt-in and flush — no events from the pre-opt-out window
    tel.set_consent(True)
    tel._flush_perf_summary(flush_reason='periodic')

    assert ph._event_queue.qsize() == 0, (
        "After opt-out + re-opt-in, flush must emit ZERO events from the cleared accumulator"
    )


# ===========================================================================
# Test 10: REVIEWS finding 3 — unknown mode/corpus_scope normalized to 'unknown'
# ===========================================================================
def test_unknown_mode_normalized_to_unknown(_reset_telemetry_state):
    """Out-of-set mode and corpus_scope values are normalized to 'unknown' before keying.

    The scrubber recurses into dict VALUES but not KEYS (telemetry.py:264).
    An arbitrary free-string must never become a verbatim nested dict key.
    This proves: (a) the key in perf_summary is 'unknown', not the raw input,
    and (b) the raw input strings appear nowhere in the serialized payload.
    """
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    # Use an out-of-set mode and an out-of-set corpus_scope
    out_of_set_mode = 'totally-not-a-mode'
    out_of_set_corpus = '../etc/passwd'

    tel.accumulate_performance(100.0, 5, out_of_set_mode, out_of_set_corpus)
    tel._flush_perf_summary(flush_reason='periodic')

    assert ph._event_queue.qsize() == 1, "Expected exactly 1 summary event"
    payload = ph._event_queue.get_nowait()

    perf_summary = payload['properties']['perf_summary']

    # The mode key must be 'unknown', not the raw input
    assert 'unknown' in perf_summary, (
        f"Out-of-set mode must be normalized to 'unknown'; got keys: {list(perf_summary.keys())}"
    )
    assert out_of_set_mode not in perf_summary, (
        f"Raw out-of-set mode {out_of_set_mode!r} must not appear as a nested dict key "
        "(scrubber does not scrub dict keys — normalizer must prevent this; REVIEWS finding 3)"
    )

    # The raw strings must not appear anywhere in the serialized payload
    payload_repr = repr(payload)
    assert out_of_set_mode not in payload_repr, (
        f"Raw mode string {out_of_set_mode!r} leaked into payload repr"
    )
    assert out_of_set_corpus not in payload_repr, (
        f"Raw corpus string {out_of_set_corpus!r} leaked into payload repr"
    )


# ===========================================================================
# Test 11: REVIEWS finding 8 — invalid env knob values are clamped/defaulted
# ===========================================================================
def test_env_knob_invalid_values_clamped(_reset_telemetry_state, monkeypatch):
    """Invalid GENIZAH_PERF_SAMPLE_N / GENIZAH_PERF_FLUSH_INTERVAL values are clamped.

    (a) Non-numeric GENIZAH_PERF_SAMPLE_N -> default 1; accumulation NOT disabled.
    (b) GENIZAH_PERF_SAMPLE_N=0 -> clamped to 1; accumulation NOT disabled.
    (c) Non-numeric GENIZAH_PERF_FLUSH_INTERVAL -> no exception + no spurious flush.
    """
    import desktop.telemetry as tel
    import shared.posthog_server as ph

    _enable_telemetry(tel, _reset_telemetry_state)

    # --- (a) non-numeric sample_n defaults to 1 ---
    monkeypatch.setenv('GENIZAH_PERF_SAMPLE_N', 'notanumber')
    for _ in range(3):
        tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')
    tel._flush_perf_summary(flush_reason='periodic')

    assert ph._event_queue.qsize() == 1, (
        "Non-numeric GENIZAH_PERF_SAMPLE_N must default to 1 (not disable accumulation)"
    )
    payload_a = ph._event_queue.get_nowait()
    count_a = payload_a['properties']['perf_summary']['keyword']['count']
    assert count_a == 3, (
        f"With invalid sample_n (default=1), all 3 runs should be counted; got {count_a}"
    )

    # --- (b) GENIZAH_PERF_SAMPLE_N=0 clamped to 1 ---
    tel._reset_for_tests()
    tel._load_consent_state()
    tel.set_consent(True)

    monkeypatch.setenv('GENIZAH_PERF_SAMPLE_N', '0')
    for _ in range(3):
        tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')
    tel._flush_perf_summary(flush_reason='periodic')

    assert ph._event_queue.qsize() == 1, (
        "GENIZAH_PERF_SAMPLE_N=0 must clamp to 1 (not cause divide-by-zero or skip-all)"
    )
    payload_b = ph._event_queue.get_nowait()
    count_b = payload_b['properties']['perf_summary']['keyword']['count']
    assert count_b == 3, (
        f"With sample_n clamped to 1, all 3 runs should be counted; got {count_b}"
    )

    # --- (c) non-numeric flush interval -> no exception ---
    tel._reset_for_tests()
    tel._load_consent_state()
    tel.set_consent(True)

    monkeypatch.setenv('GENIZAH_PERF_FLUSH_INTERVAL', 'notanumber')
    tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')

    # flush_perf_if_due() with an invalid interval must NOT raise.
    # With a freshly reset _perf_last_flush_time (0.0) and the default 1800s interval,
    # flush_perf_if_due() should flush (monotonic since epoch > 1800s).
    # The test merely asserts no exception is raised — it does not assert queue state
    # because the actual behavior depends on whether monotonic() > interval.
    try:
        tel.flush_perf_if_due()
    except Exception as exc:
        pytest.fail(
            f"flush_perf_if_due() raised an exception with invalid GENIZAH_PERF_FLUSH_INTERVAL: {exc}"
        )
    # Drain any events produced (whether it flushed or not is implementation-dependent)
    while not ph._event_queue.empty():
        ph._event_queue.get_nowait()
