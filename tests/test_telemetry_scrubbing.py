# -*- coding: utf-8 -*-
"""Phase 111 Plan 02 — Structural scrubber tests (PRIV-01).

Tests _scrub_props() as a pure function. Covers banned-key stripping,
path redaction, Hebrew redaction, length capping, and the critical
REVIEWS MEDIUM regression: 'context' key must SURVIVE (not banned by
substring 'text') and 'traceback_scrubbed' must SURVIVE (not banned by
substring 'traceback').

No fixtures needed — _scrub_props is a pure dict-in/dict-out function.
"""

from __future__ import annotations

from desktop.telemetry import _scrub_props


# ---------------------------------------------------------------------------
# Banned key stripping
# ---------------------------------------------------------------------------

def test_banned_keys_stripped():
    """PRIV-01: query/filename/path are exact banned keys and must be dropped."""
    result = _scrub_props({
        'query': 'some search text',
        'filename': 'a.pdf',
        'path': '/home/user/data',
        'platform': 'desktop',
    })
    assert 'query' not in result, "banned key 'query' must be dropped"
    assert 'filename' not in result, "banned key 'filename' must be dropped"
    assert 'path' not in result, "banned key 'path' must be dropped"
    assert result.get('platform') == 'desktop', "'platform' must survive"


def test_context_key_survives():
    """REVIEWS MEDIUM regression: 'context' must NOT be banned by substring 'text'."""
    result = _scrub_props({'context': 'safe_constant', 'platform': 'desktop'})
    assert 'context' in result, (
        "'context' key must survive — it is allowlisted and must not be "
        "banned just because it contains 'text' as a substring"
    )
    assert result['context'] == 'safe_constant', "value must not be altered for safe strings"


def test_traceback_scrubbed_survives():
    """REVIEWS MEDIUM regression: 'traceback_scrubbed' must survive; 'traceback_raw' is banned."""
    result = _scrub_props({
        'traceback_scrubbed': 'frame1 frame2',
        'traceback_raw': 'raw stack trace',
    })
    assert 'traceback_scrubbed' in result, (
        "'traceback_scrubbed' must survive — it is an exact allowlisted key"
    )
    assert 'traceback_raw' not in result, (
        "'traceback_raw' must be dropped — it is an exact banned key"
    )


def test_text_key_still_dropped():
    """Exact 'text' key must be dropped; 'query_text' explicit token is banned."""
    result_text = _scrub_props({'text': 'some text'})
    assert result_text == {}, f"exact 'text' key must be dropped, got {result_text}"

    result_query_text = _scrub_props({'query_text': 'some query'})
    assert result_query_text == {}, f"'query_text' must be dropped, got {result_query_text}"


# ---------------------------------------------------------------------------
# Value redaction — paths
# ---------------------------------------------------------------------------

def test_windows_path_redacted():
    """A Windows path value on an allowed key must be redacted."""
    result = _scrub_props({'context': r'C:\Users\gersh\file.pdf'})
    assert 'context' in result, "'context' key must survive"
    assert '[REDACTED]' in result['context'], (
        f"Windows path must be redacted in value, got: {result['context']!r}"
    )
    # The original path must NOT appear
    assert r'C:\Users' not in result['context']


def test_posix_path_redacted():
    """A POSIX path value on an allowed key must be redacted."""
    result = _scrub_props({'context': '/home/user/secret.txt'})
    assert 'context' in result
    assert '[REDACTED]' in result['context']
    assert '/home/user' not in result['context']


def test_bare_filename_redacted():
    """A bare filename (e.g. 'report.docx') embedded in a string must be redacted."""
    result = _scrub_props({'context': 'see report.docx for details'})
    assert 'context' in result
    assert '[REDACTED]' in result['context']
    assert 'report.docx' not in result['context']


# ---------------------------------------------------------------------------
# Value redaction — Hebrew
# ---------------------------------------------------------------------------

def test_hebrew_text_redacted():
    """A value containing Hebrew characters must be fully replaced with [REDACTED]."""
    # Hebrew fragment that could be query content
    hebrew_value = 'תשובות הרמב״ם'
    result = _scrub_props({'context': hebrew_value})
    assert 'context' in result
    assert result['context'] == '[REDACTED]', (
        f"Hebrew value must be fully replaced with [REDACTED], got: {result['context']!r}"
    )


# ---------------------------------------------------------------------------
# Value length cap
# ---------------------------------------------------------------------------

def test_value_length_capped():
    """String values must be capped at 500 characters."""
    long_value = 'x' * 5000
    result = _scrub_props({'context': long_value})
    assert 'context' in result
    assert len(result['context']) <= 500, (
        f"Value must be capped at 500 chars, got {len(result['context'])}"
    )


# ---------------------------------------------------------------------------
# Non-string value passthrough
# ---------------------------------------------------------------------------

def test_non_string_values_preserved():
    """Ints, bools, and other non-string types must pass through unchanged."""
    result = _scrub_props({
        'result_count': 42,
        '$process_person_profile': True,
        'duration_ms': 3.14,
    })
    assert result.get('result_count') == 42
    assert result.get('$process_person_profile') is True
    assert result.get('duration_ms') == 3.14
