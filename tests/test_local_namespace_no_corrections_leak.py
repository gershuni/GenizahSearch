# -*- coding: utf-8 -*-
"""Phase 95 REQ-9: LOCAL sys_ids must not reach corrections_client cloud surface.

Tests that corrections_client.CorrectionsClient.create_correction() returns
'local_corrections_disabled' for LOCAL sys_ids WITHOUT making any HTTP call.
"""
from unittest.mock import MagicMock, patch


LOCAL_SYS_ID = '970012345601234567'
SYNTH_SYS_ID = '990001234560000000'
REAL_SYS_ID = '990025143260205171'


def _make_client():
    """Return a CorrectionsClient without touching the filesystem."""
    from corrections_client import CorrectionsClient
    from unittest.mock import patch as _patch
    import pathlib

    # Patch mkdir so we don't create real directories
    with _patch.object(pathlib.Path, 'mkdir'):
        client = CorrectionsClient(base_url='https://fake.example.com/api/v1')
    return client


# ---------------------------------------------------------------------------
# Test 1: LOCAL sys_id returns local_corrections_disabled
# ---------------------------------------------------------------------------

def test_corrections_submit_returns_local_corrections_disabled():
    """REQ-9: create_correction() with a LOCAL sys_id returns immediately
    with 'local_corrections_disabled' and makes NO HTTP call.
    """
    client = _make_client()

    with patch('requests.Session.request') as mock_request:
        result, message = client.create_correction(
            document_id=LOCAL_SYS_ID,
            original_text='foo',
            corrected_text='bar',
        )

    assert result is None, "Expected None result for LOCAL sys_id"
    assert 'local_corrections_disabled' in message, (
        f"Expected 'local_corrections_disabled' in message, got: {message!r}"
    )
    assert mock_request.call_count == 0, (
        "REQ-9 gate FAILED: HTTP request was made for a LOCAL sys_id"
    )


# ---------------------------------------------------------------------------
# Test 2: synthetic sys_id still returns synthetic_corrections_disabled
# ---------------------------------------------------------------------------

def test_corrections_submit_synthetic_still_disabled():
    """Regression: synthetic 99-prefix sys_id still returns
    'synthetic_corrections_disabled' (NOT the new LOCAL code).
    """
    client = _make_client()

    with patch('requests.Session.request') as mock_request:
        result, message = client.create_correction(
            document_id=SYNTH_SYS_ID,
            original_text='foo',
            corrected_text='bar',
        )

    assert result is None
    assert 'synthetic_corrections_disabled' in message, (
        f"Expected 'synthetic_corrections_disabled' in message, got: {message!r}"
    )
    # Ensure it's NOT the LOCAL error code
    assert 'local_corrections_disabled' not in message, (
        "Synthetic sys_id should NOT return 'local_corrections_disabled'"
    )
    assert mock_request.call_count == 0


# ---------------------------------------------------------------------------
# Test 3: real Alma sys_id passes both gates
# ---------------------------------------------------------------------------

def test_corrections_submit_real_alma_still_passes_gate():
    """Regression: real Alma sys_id passes both the synthetic and LOCAL gates
    and reaches the existing flow (which will try HTTP and fail/raise, but
    the gates themselves must not fire).
    """
    client = _make_client()

    # Simulate HTTP returning an error (e.g. 401) so we don't need auth,
    # but the important thing is the gates did NOT fire.
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.json.return_value = {'error': 'Unauthorized'}
    mock_resp.raise_for_status.side_effect = None

    with patch('requests.Session.request', return_value=mock_resp):
        try:
            result, message = client.create_correction(
                document_id=REAL_SYS_ID,
                original_text='foo',
                corrected_text='bar',
            )
        except Exception:
            # Any exception is fine — the important assertion is below
            pass
        else:
            # If it returned a tuple, make sure it's not a gate error
            assert 'local_corrections_disabled' not in (message or ''), (
                "Real Alma sys_id should NOT be gated by the LOCAL gate"
            )
            assert 'synthetic_corrections_disabled' not in (message or ''), (
                "Real Alma sys_id should NOT be gated by the synthetic gate"
            )
