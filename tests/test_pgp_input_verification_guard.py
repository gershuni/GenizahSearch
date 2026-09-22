# -*- coding: utf-8 -*-
"""The input-verification guard must actually be reachable.

A stray ``return True`` sat immediately before this guard's error block, making the
messages and ``raise SystemExit(1)`` dead code. With mismatched inputs and no override it
returned success, the export ran, and the derived file was stamped with the commit from
the manifest that had just failed. Restoring the original CSV afterwards left the importer
seeing a valid upstream checksum AND a valid derived checksum for content produced from
different inputs.

It was reported, and I refuted it: I appended a byte to the real ``footnotes.csv``, saw
the script exit 1, and concluded the guard fired. It exited 1 for an unrelated reason.
**Checking the exit code is not checking the behaviour** -- so these tests assert on what
the guard does, not on what it returns to the shell.
"""
from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "pgp_transcriptions_export.py"


@pytest.fixture
def exporter():
    """Fresh module per test -- the verified flag is module state."""
    spec = importlib.util.spec_from_file_location("_pgp_tx_export", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _staged(tmp_path, tamper=False):
    """A pgp_data dir whose CSVs either match their manifest or do not."""
    files = {}
    for name in ("documents.csv", "fragments.csv", "footnotes.csv"):
        body = b"col\nvalue\n"
        (tmp_path / name).write_bytes(body + (b"tampered\n" if tamper else b""))
        import hashlib
        files[name] = {
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "rows": 1,
        }
    (tmp_path / "upstream_provenance.json").write_text(
        json.dumps({"upstream_commit": "a94528cc", "files": files}), encoding="utf-8"
    )
    return tmp_path


def test_mismatched_inputs_abort_the_run(exporter, tmp_path, monkeypatch):
    """The dead-code bug, in one test: this must RAISE, not return."""
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        exporter._require_verified_inputs(_staged(tmp_path, tamper=True))
    assert excinfo.value.code == 1


def test_matching_inputs_are_verified(exporter, tmp_path, monkeypatch):
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    assert exporter._require_verified_inputs(_staged(tmp_path)) is True
    assert exporter._INPUTS_VERIFIED is True


def test_the_override_proceeds_but_does_not_certify(exporter, tmp_path, monkeypatch):
    """`--derive anyway` must never become `and vouch for it`."""
    monkeypatch.setenv("PGP_ALLOW_UNVERIFIED_INPUTS", "1")
    assert exporter._require_verified_inputs(_staged(tmp_path, tamper=True)) is False
    assert exporter._INPUTS_VERIFIED is False


def test_an_unverified_run_records_no_commit_and_clears_a_stale_stamp(exporter, tmp_path,
                                                                     monkeypatch):
    monkeypatch.setenv("PGP_ALLOW_UNVERIFIED_INPUTS", "1")
    staged = _staged(tmp_path, tamper=True)
    (staged / "derived_provenance.json").write_text(
        json.dumps({"derived_from_commit": "stale"}), encoding="utf-8"
    )
    (staged / "transcriptions_linked.csv").write_bytes(b"pgpid\n1\n")

    exporter._require_verified_inputs(staged)
    exporter._record_derived_provenance(staged, verified=exporter._INPUTS_VERIFIED)

    assert not (staged / "derived_provenance.json").exists(), (
        "a stale stamp must not survive a run whose inputs were never verified"
    )


def test_a_verified_run_does_record_the_commit(exporter, tmp_path, monkeypatch):
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    staged = _staged(tmp_path)
    (staged / "transcriptions_linked.csv").write_bytes(b"pgpid\n1\n")

    exporter._require_verified_inputs(staged)
    exporter._record_derived_provenance(staged, verified=exporter._INPUTS_VERIFIED)

    recorded = json.loads((staged / "derived_provenance.json").read_text(encoding="utf-8"))
    assert recorded["derived_from_commit"] == "a94528cc"
    assert "sha256" in recorded["files"]["transcriptions_linked.csv"]
