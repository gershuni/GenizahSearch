# -*- coding: utf-8 -*-
"""Phase 95 Category 2 BLOCKER — Tantivy writer.commit() Windows os error 5 retry.

Reproduces the user-reported indexing crash:

    LocalIndexerWorker: unhandled error
    File "shared/local_indexer.py", line 1223, in _commit_batch
        self._writer.commit()
    ValueError: An IO error occurred: 'Access is denied. (os error 5)'

Tests that LocalIndexer._commit_writer_with_retry:
  1. Detects the Windows access-denied pattern.
  2. Retries with exponential backoff (3 attempts, 250 ms / 1 s / 2 s).
  3. Succeeds when a transient failure clears on retry.
  4. Raises with detailed context when all retries are exhausted.
  5. Propagates non-access-denied exceptions immediately (no retry).
"""
from __future__ import annotations

import os
import time
from unittest.mock import MagicMock, patch

import pytest

from shared.local_indexer import LocalIndexer


def _bare_indexer(tmp_path) -> LocalIndexer:
    """Build a LocalIndexer rooted at tmp_path (real DB + index dirs)."""
    index_dir = os.path.join(str(tmp_path), "idx")
    lab_dir = os.path.join(str(tmp_path), "lab")
    db_path = os.path.join(str(tmp_path), "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    return LocalIndexer(index_dir, lab_dir, db_path)


# ---------------------------------------------------------------------------
# Detection helper
# ---------------------------------------------------------------------------

def test_is_windows_access_denied_detects_os_error_5():
    """Static helper must recognise the Tantivy/Windows wording."""
    assert LocalIndexer._is_windows_access_denied(
        ValueError("An IO error occurred: 'Access is denied. (os error 5)'")
    )
    assert LocalIndexer._is_windows_access_denied(
        ValueError("os error 5 raised by writer.commit")
    )
    assert LocalIndexer._is_windows_access_denied(
        ValueError("Access is denied. (os error 5)")
    )


def test_is_windows_access_denied_rejects_unrelated_errors():
    """Other ValueErrors must NOT be classified as access-denied."""
    assert not LocalIndexer._is_windows_access_denied(
        ValueError("Bad query syntax")
    )
    assert not LocalIndexer._is_windows_access_denied(
        OSError("disk full")
    )
    assert not LocalIndexer._is_windows_access_denied(
        RuntimeError("unrelated")
    )


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

def test_commit_retry_succeeds_on_second_attempt(tmp_path):
    """Mock writer that fails first, then succeeds — retry must complete."""
    idx = _bare_indexer(tmp_path)
    try:
        mock_writer = MagicMock()
        # First call raises Windows access-denied; second call succeeds.
        mock_writer.commit.side_effect = [
            ValueError("An IO error occurred: 'Access is denied. (os error 5)'"),
            None,
        ]
        idx._writer = mock_writer

        # Patch time.sleep to make the test fast (don't actually wait).
        with patch("time.sleep", return_value=None):
            idx._commit_writer_with_retry()

        # Confirm commit was called twice.
        assert mock_writer.commit.call_count == 2
    finally:
        idx.close()


def test_commit_retry_succeeds_on_third_attempt(tmp_path):
    """Two transient failures; success on the third attempt."""
    idx = _bare_indexer(tmp_path)
    try:
        mock_writer = MagicMock()
        mock_writer.commit.side_effect = [
            ValueError("An IO error occurred: 'Access is denied. (os error 5)'"),
            ValueError("os error 5"),
            None,
        ]
        idx._writer = mock_writer

        with patch("time.sleep", return_value=None):
            idx._commit_writer_with_retry()

        assert mock_writer.commit.call_count == 3
    finally:
        idx.close()


def test_commit_retry_exhausted_raises_with_context(tmp_path):
    """All retries fail → final ValueError must mention dir + retry count + cause."""
    idx = _bare_indexer(tmp_path)
    try:
        mock_writer = MagicMock()
        mock_writer.commit.side_effect = ValueError(
            "An IO error occurred: 'Access is denied. (os error 5)'"
        )
        idx._writer = mock_writer

        with patch("time.sleep", return_value=None):
            with pytest.raises(ValueError) as exc_info:
                idx._commit_writer_with_retry()

        # Final exception should mention enough context for debugging.
        msg = str(exc_info.value)
        assert "attempts" in msg.lower(), "Retry count must be in error message"
        # The message renders the dir via repr() (Windows backslashes escape).
        # Match a stable basename that survives escaping.
        assert os.path.basename(idx._index_dir) in msg, (
            "Index dir basename must be in error message"
        )
        # Writer was called 4 times: initial + 3 retries (delays: 0.25s, 1s, 2s).
        assert mock_writer.commit.call_count == 4
    finally:
        idx.close()


def test_commit_retry_propagates_unrelated_exception(tmp_path):
    """Non-access-denied exceptions must NOT trigger retry — propagate immediately."""
    idx = _bare_indexer(tmp_path)
    try:
        mock_writer = MagicMock()
        mock_writer.commit.side_effect = ValueError("Bad query syntax")
        idx._writer = mock_writer

        with patch("time.sleep", return_value=None):
            with pytest.raises(ValueError) as exc_info:
                idx._commit_writer_with_retry()

        # Only one call — no retry on non-access-denied errors.
        assert mock_writer.commit.call_count == 1
        assert "Bad query syntax" in str(exc_info.value)
    finally:
        idx.close()


def test_commit_batch_uses_retry(tmp_path):
    """_commit_batch must route through _commit_writer_with_retry, not raw commit."""
    idx = _bare_indexer(tmp_path)
    try:
        # Populate at least one pending filepath so the early-return doesn't trip.
        idx._pending_filepaths = ["/tmp/dummy.pdf"]
        mock_writer = MagicMock()
        mock_writer.commit.side_effect = [
            ValueError("An IO error occurred: 'Access is denied. (os error 5)'"),
            None,
        ]
        idx._writer = mock_writer

        with patch("time.sleep", return_value=None):
            idx._commit_batch()

        # Retried once → commit called twice.
        assert mock_writer.commit.call_count == 2
        assert idx._pending_filepaths == [], (
            "_commit_batch must clear pending_filepaths after success"
        )
    finally:
        idx.close()


def test_commit_retry_timing_three_attempts(tmp_path):
    """Sanity check: retry uses 3 backoff delays."""
    idx = _bare_indexer(tmp_path)
    try:
        mock_writer = MagicMock()
        mock_writer.commit.side_effect = ValueError(
            "An IO error occurred: 'Access is denied. (os error 5)'"
        )
        idx._writer = mock_writer

        sleeps = []
        with patch("time.sleep", side_effect=lambda d: sleeps.append(d)):
            with pytest.raises(ValueError):
                idx._commit_writer_with_retry()

        # Expected delays before retries: 0.25, 1.0, 2.0.
        assert 0.25 in sleeps
        assert 1.0 in sleeps
        assert 2.0 in sleeps
    finally:
        idx.close()
