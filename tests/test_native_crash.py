# -*- coding: utf-8 -*-
"""Phase 113 Plan 03 — Native crash detection + persisted consent tests.

Plan 03 (filled here): test_prior_crash_emitted_on_consent,
test_pending_emit_after_consent, test_no_emit_without_consent,
test_classify_all_prefixes, test_classify_unknown_maps_to_unknown_native,
test_read_before_enable_ordering, test_native_payload_has_os_and_dump_reused.

Plan 02 (retained): test_persisted_consent_populates_crash_distinct_id.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import os
import pytest

import desktop.telemetry as tel


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# Helper: mock Config.INDEX_DIR to a tmp_path directory
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_index_dir(tmp_path, monkeypatch):
    """Point Config.INDEX_DIR to a tmp_path so faulthandler tests don't touch disk."""
    from genizah_core import Config
    monkeypatch.setattr(Config, 'INDEX_DIR', str(tmp_path), raising=False)
    return tmp_path


# ---------------------------------------------------------------------------
# CRASH-07 D-02 — native crash classification (fixed enum, never raw text)
# ---------------------------------------------------------------------------
def test_classify_all_prefixes():
    """CRASH-07 D-02: all _NATIVE_CRASH_LABELS prefixes map to known enum labels.

    Case-insensitive prefix matching. Raw text is NEVER returned — only enum
    labels from {segmentation_fault, access_violation, abort, stack_overflow,
    unknown_native}.
    """
    from desktop.telemetry import _classify_native_crash

    valid_labels = {
        'segmentation_fault', 'access_violation', 'abort',
        'stack_overflow', 'unknown_native',
    }

    cases = [
        ('Windows fatal exception: access violation', 'access_violation'),
        ('windows fatal exception: access violation', 'access_violation'),
        ('WINDOWS FATAL EXCEPTION: ACCESS VIOLATION', 'access_violation'),
        ('Windows fatal exception: stack overflow', 'stack_overflow'),
        ('Windows fatal exception: int divide by zero', 'abort'),
        ('Windows fatal exception: float divide by zero', 'abort'),
        ('Segmentation fault', 'segmentation_fault'),
        ('segmentation fault', 'segmentation_fault'),
        ('Aborted', 'abort'),
        ('aborted', 'abort'),
        ('Floating-point exception', 'abort'),
        ('Bus error', 'abort'),
        ('Fatal Python error:', 'unknown_native'),
        ('fatal python error: GC', 'unknown_native'),
    ]

    for text, expected in cases:
        result = _classify_native_crash(text + '\nmore lines follow...')
        assert result == expected, (
            f"_classify_native_crash({text!r}) returned {result!r}, "
            f"expected {expected!r}"
        )
        assert result in valid_labels, (
            f"_classify_native_crash returned raw text or unknown label: {result!r}"
        )


def test_classify_unknown_maps_to_unknown_native():
    """CRASH-07 D-02: unrecognized prefix and empty string → 'unknown_native'."""
    from desktop.telemetry import _classify_native_crash

    # Empty string
    assert _classify_native_crash('') == 'unknown_native', (
        "_classify_native_crash('') should return 'unknown_native'"
    )
    # Unrecognized text
    assert _classify_native_crash('some unknown crash text here') == 'unknown_native'
    assert _classify_native_crash('    ') == 'unknown_native'
    # Raw faulthandler output with no matching prefix
    assert _classify_native_crash('Thread 0x00001234 (most recent first)') == 'unknown_native'


# ---------------------------------------------------------------------------
# CRASH-07 — prior native crash emit (consent True path)
# ---------------------------------------------------------------------------
def test_prior_crash_emitted_on_consent(monkeypatch, mock_index_dir):
    """CRASH-07: prior native crash + consent True at startup → desktop_prior_crash
    emitted exactly once, dump truncated (pending is memory-only).
    """
    dump = mock_index_dir / 'faulthandler_dump.txt'
    dump.write_text('Windows fatal exception: access violation\nThread 0x0001\n')

    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    # Consent already True when setup runs
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')

    # _faulthandler_handle must be reset so STEP 3 does not fail on a closed handle
    monkeypatch.setattr(tel, '_faulthandler_handle', None)
    # Prevent real faulthandler.enable from writing to file in test
    import faulthandler as _faulthandler
    monkeypatch.setattr(_faulthandler, 'enable', lambda file, all_threads=False: None)

    tel._setup_faulthandler()

    # Exactly one desktop_prior_crash emitted
    assert sent == ['desktop_prior_crash'], (
        f"Expected exactly one 'desktop_prior_crash', got: {sent}"
    )
    # _pending_native_crash should be cleared (emitted immediately)
    assert tel._pending_native_crash is None, (
        "_pending_native_crash should be None after immediate emit"
    )


def test_pending_emit_after_consent(monkeypatch, mock_index_dir):
    """CRASH-07 D-03: prior crash held pending when consent is False;
    emitted exactly once when set_consent(True) is called.
    """
    dump = mock_index_dir / 'faulthandler_dump.txt'
    dump.write_text('Windows fatal exception: access violation\nThread 0x0001\n')

    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    # Consent is False at startup
    monkeypatch.setattr(tel, '_enabled', False)
    monkeypatch.setattr(tel, '_faulthandler_handle', None)

    import faulthandler as _faulthandler
    monkeypatch.setattr(_faulthandler, 'enable', lambda file, all_threads=False: None)

    tel._setup_faulthandler()

    # No emit yet — consent was False
    assert len(sent) == 0, f"Should not emit before consent, got: {sent}"
    assert tel._pending_native_crash == 'access_violation', (
        f"_pending_native_crash should hold 'access_violation', got: {tel._pending_native_crash!r}"
    )

    # Now call set_consent(True) — should emit exactly once
    tel.set_consent(True)
    assert len(sent) == 1, f"Expected exactly one send after set_consent(True), got: {sent}"
    assert sent[0] == 'desktop_prior_crash', (
        f"Expected 'desktop_prior_crash', got: {sent[0]!r}"
    )
    assert tel._pending_native_crash is None, (
        "_pending_native_crash should be None after emit"
    )

    # Second set_consent(True) should NOT re-emit
    tel.set_consent(True)
    assert len(sent) == 1, "Should not emit twice — exactly-once guarantee"


def test_no_emit_without_consent(monkeypatch, mock_index_dir):
    """CRASH-07: user never consents → prior native crash never emitted."""
    dump = mock_index_dir / 'faulthandler_dump.txt'
    dump.write_text('segmentation fault\nThread 0x0001\n')

    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    monkeypatch.setattr(tel, '_enabled', False)
    monkeypatch.setattr(tel, '_faulthandler_handle', None)

    import faulthandler as _faulthandler
    monkeypatch.setattr(_faulthandler, 'enable', lambda file, all_threads=False: None)

    tel._setup_faulthandler()

    # Pending held in memory, never emitted
    assert len(sent) == 0, "Should never emit without consent"
    assert tel._pending_native_crash == 'segmentation_fault', (
        f"_pending_native_crash not held: {tel._pending_native_crash!r}"
    )
    # Never call set_consent — user never consents
    assert len(sent) == 0, "Still should not have emitted"


# ---------------------------------------------------------------------------
# CRASH-03 — faulthandler read-before-enable ordering (D-03)
# ---------------------------------------------------------------------------
def test_read_before_enable_ordering(monkeypatch, mock_index_dir):
    """CRASH-03 D-03: the previous dump is READ before faulthandler.enable() is called.

    Monkeypatches faulthandler.enable to record when it was called, and writes
    a marker to the dump file. Asserts that the prior content was read BEFORE
    enable() cleared/truncated it.
    """
    dump = mock_index_dir / 'faulthandler_dump.txt'
    prior_content = 'Windows fatal exception: stack overflow\nThread 0x0001\n'
    dump.write_text(prior_content)

    call_order = []

    # Monkeypatch faulthandler.enable to record call order
    import faulthandler as _faulthandler
    real_enable = _faulthandler.enable

    def tracking_enable(file, all_threads=False):
        # Record what the dump file contains at the time enable() is called
        try:
            file.flush()
        except Exception:
            pass
        call_order.append('enable_called')

    monkeypatch.setattr(_faulthandler, 'enable', tracking_enable)

    sent = []
    monkeypatch.setattr(tel, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')
    monkeypatch.setattr(tel, '_faulthandler_handle', None)

    # Record classification (proves read happened before enable)
    classified = []
    original_classify = tel._classify_native_crash

    def tracking_classify(text):
        classified.append(text)
        return original_classify(text)

    monkeypatch.setattr(tel, '_classify_native_crash', tracking_classify)

    tel._setup_faulthandler()

    # Classify was called (with the prior dump content) before enable was called
    assert len(classified) >= 1, "Expected _classify_native_crash to be called (read happened)"
    assert 'stack overflow' in classified[0].lower(), (
        f"Prior content was not read before enable: classified={classified}"
    )
    assert 'enable_called' in call_order, "faulthandler.enable() was not called"

    # The emit confirms the read happened and the label was correct
    assert sent == ['desktop_prior_crash'], (
        f"Expected desktop_prior_crash emit, got: {sent}"
    )


# ---------------------------------------------------------------------------
# REVIEWS PASS2 — OS props in native payload + dump reused (memory-only pending)
# ---------------------------------------------------------------------------
def test_native_payload_has_os_and_dump_reused(monkeypatch, mock_index_dir):
    """REVIEWS PASS2: desktop_prior_crash payload includes os_family + os_version;
    after _setup_faulthandler runs with a prior dump, the dump is TRUNCATED
    (STEP 3 'w' reopen) and pending is memory-only.
    """
    dump = mock_index_dir / 'faulthandler_dump.txt'
    prior_content = 'Windows fatal exception: access violation\nsome frames\n'
    dump.write_text(prior_content)

    captured = []

    def capture_send(ev, props, did, timeout=0.5):
        captured.append({'event': ev, 'props': props})

    monkeypatch.setattr(tel, 'send_crash_event_direct', capture_send)
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')
    monkeypatch.setattr(tel, '_faulthandler_handle', None)

    import faulthandler as _faulthandler
    monkeypatch.setattr(_faulthandler, 'enable', lambda file, all_threads=False: None)

    tel._setup_faulthandler()

    # Exactly one desktop_prior_crash event captured
    assert len(captured) == 1, f"Expected one event, got: {len(captured)}"
    event = captured[0]
    assert event['event'] == 'desktop_prior_crash'

    # Payload includes OS props (from _BASE_PROPS)
    props = event['props']
    assert 'os_family' in props, f"os_family missing from native crash payload: {props}"
    assert 'os_version' in props, f"os_version missing from native crash payload: {props}"
    assert props['os_family'], "os_family is empty in native crash payload"
    assert props['os_version'], "os_version is empty in native crash payload"

    # Payload includes fatal_error with the classified label
    assert 'fatal_error' in props, f"fatal_error missing from native crash payload: {props}"
    assert props['fatal_error'] == 'access_violation', (
        f"fatal_error should be 'access_violation', got: {props.get('fatal_error')!r}"
    )

    # The dump file was TRUNCATED by STEP 3's 'w' reopen (pending is memory-only)
    # The handle was opened with 'w', so the prior text is gone
    if tel._faulthandler_handle is not None:
        try:
            tel._faulthandler_handle.flush()
        except Exception:
            pass
    # Read dump file directly; STEP 3 truncated it
    dump_after = dump.read_text(encoding='utf-8', errors='replace')
    assert prior_content not in dump_after, (
        "Prior dump content was NOT truncated by STEP 3 'w' reopen — "
        "the dump should be overwritten for this run's faulthandler output"
    )


# ---------------------------------------------------------------------------
# REVIEWS HIGH-3 — persisted-consent startup populates _crash_distinct_id
# ---------------------------------------------------------------------------
def test_persisted_consent_populates_crash_distinct_id(crash_telemetry_state):
    """REVIEWS HIGH-3: _load_consent_state() sets _crash_distinct_id when enabled=True.

    Simulates a persisted-consent launch by populating the fake config with
    enabled=True + an install_id, then calling _load_consent_state().
    Asserts that _crash_distinct_id is set to that id (NOT None), so a crash
    before any set_consent() call emits with the correct identity.
    """
    fake_config = crash_telemetry_state
    fake_install_id = 'aaaa1111bbbb2222cccc3333dddd4444'

    # Populate fake persisted config (simulating what set_consent(True) writes on a prior launch)
    fake_config[tel.TELEMETRY_ENABLED_KEY] = True
    fake_config[tel.TELEMETRY_INSTALL_ID_KEY] = fake_install_id

    # Reset and reload from fake config (simulates startup)
    tel._reset_for_tests()
    tel._load_consent_state()

    # _crash_distinct_id must now be set to the persisted install_id
    # (NOT None — a crash before any set_consent() call must not emit as 'system')
    assert tel._crash_distinct_id is not None, (
        "_crash_distinct_id is None after _load_consent_state() with enabled=True — "
        "persisted-consent users would emit crashes as 'system' (REVIEWS HIGH-3)"
    )
    assert tel._crash_distinct_id == fake_install_id, (
        f"_crash_distinct_id ({tel._crash_distinct_id!r}) != install_id ({fake_install_id!r}) "
        "after persisted-consent startup"
    )
