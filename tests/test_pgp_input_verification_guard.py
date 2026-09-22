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
import io
import json
import pathlib
from contextlib import redirect_stderr, redirect_stdout

import pytest

import pgp_pipeline_fixtures as fx

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


# ── through main(): the call site, not the helpers ────────────────────────────
#
# With the module default `_INPUTS_VERIFIED = True` and the `verified=` kwarg dropped at
# the one call site, an override run wrote a fully certified stamp -- and every test above
# still passed, because every one of them calls the helpers and passes `verified=` itself.


def _run_main(exporter, root):
    exporter.__file__ = str(root / "scripts" / "pgp_transcriptions_export.py")
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        try:
            rc = exporter.main()
        except SystemExit as exc:
            rc = exc.code
    return rc, out.getvalue(), err.getvalue()


def _verifier():
    spec = importlib.util.spec_from_file_location(
        "_fetch_pgp_metadata", REPO_ROOT / "scripts" / "fetch_pgp_metadata.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_verified_flag_starts_false_and_an_unflagged_stamp_is_never_written(exporter,
                                                                                tmp_path):
    """The fail-open default: any caller that skipped the guard got a certified stamp."""
    assert exporter._INPUTS_VERIFIED is False
    staged = _staged(tmp_path)
    (staged / "transcriptions_linked.csv").write_bytes(b"pgpid\n1\n")
    exporter._record_derived_provenance(staged)  # no verified= at all
    assert not (staged / "derived_provenance.json").exists()


def test_main_refuses_mismatched_inputs_and_leaves_the_derived_file_alone(exporter, tmp_path,
                                                                          monkeypatch):
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    pgp_data = fx.build_tree(tmp_path, derived=False)
    (pgp_data / "transcriptions_linked.csv").write_text("OLD", encoding="utf-8")
    fx.tamper(pgp_data / "footnotes.csv")

    rc, _out, err = _run_main(exporter, tmp_path)
    assert rc == 1
    assert "do not match upstream_provenance.json" in err
    assert (pgp_data / "transcriptions_linked.csv").read_text(encoding="utf-8") == "OLD"
    assert not (pgp_data / "derived_provenance.json").exists()


def test_main_under_the_override_derives_but_writes_no_stamp(exporter, tmp_path, monkeypatch):
    """The 609ff402 finding, through the call site. A stale stamp is seeded too: it must
    not survive to vouch for the new file."""
    monkeypatch.setenv("PGP_ALLOW_UNVERIFIED_INPUTS", "1")
    pgp_data = fx.build_tree(tmp_path, derived=False)
    (pgp_data / "derived_provenance.json").write_text('{"derived_from_commit": "stale"}',
                                                      encoding="utf-8")
    fx.tamper(pgp_data / "footnotes.csv")

    rc, out, _err = _run_main(exporter, tmp_path)
    assert rc == 0
    assert "Wrote 2 records" in out
    assert not (pgp_data / "derived_provenance.json").exists(), (
        "derive anyway must never become 'and vouch for it'"
    )
    # ...and the importer's verifier agrees there is nothing to trust.
    problems = _verifier().verify_against_provenance(str(pgp_data), check_derived=True)
    assert any("transcriptions_linked.csv" in p for p in problems)


def test_main_with_verified_inputs_stamps_the_file_and_its_inputs(exporter, tmp_path,
                                                                  monkeypatch):
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    pgp_data = fx.build_tree(tmp_path, derived=False)

    rc, _out, _err = _run_main(exporter, tmp_path)
    assert rc == 0
    stamp = json.loads((pgp_data / "derived_provenance.json").read_text(encoding="utf-8"))
    assert stamp["derived_from_commit"] == fx.COMMIT
    assert set(stamp["inputs"]) == {"libraries.csv", "fist_shelfmarks_supplement.csv"}
    assert stamp["inputs"]["libraries.csv"]["bytes"] == (tmp_path / "libraries.csv").stat().st_size

    verifier = _verifier()
    assert verifier.verify_against_provenance(str(pgp_data), check_derived=True) == []

    # libraries.csv decides which manuscript each transcription is attributed to. It is not
    # an upstream file, so nothing else can see it change.
    fx.tamper(tmp_path / "libraries.csv")
    problems = verifier.verify_against_provenance(str(pgp_data), check_derived=True)
    assert any("libraries.csv has changed" in p for p in problems), problems

    # A supplement that was present at derivation and is gone now is a divergence too.
    fx.build_tree(tmp_path, derived=False)
    assert _run_main(exporter, tmp_path)[0] == 0
    (pgp_data / "fist_shelfmarks_supplement.csv").unlink()
    problems = verifier.verify_against_provenance(str(pgp_data), check_derived=True)
    assert any("fist_shelfmarks_supplement.csv" in p and "missing now" in p for p in problems)


def test_an_absent_supplement_is_recorded_and_its_later_appearance_is_a_divergence(
        exporter, tmp_path, monkeypatch):
    """The documented escape hatch (PGP_ALLOW_MISSING_FIST_SUPPLEMENT=1) produced a different
    derivation -- ~2,900 fewer fragments at full scale -- with the same clean stamp."""
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    monkeypatch.setenv("PGP_ALLOW_MISSING_FIST_SUPPLEMENT", "1")
    pgp_data = fx.build_tree(tmp_path, derived=False, supplement=False)

    assert _run_main(exporter, tmp_path)[0] == 0
    stamp = json.loads((pgp_data / "derived_provenance.json").read_text(encoding="utf-8"))
    assert stamp["inputs"]["fist_shelfmarks_supplement.csv"] == "absent"

    verifier = _verifier()
    assert verifier.verify_against_provenance(str(pgp_data), check_derived=True) == []
    (pgp_data / "fist_shelfmarks_supplement.csv").write_text(fx.SUPPLEMENT_CSV, encoding="utf-8")
    problems = verifier.verify_against_provenance(str(pgp_data), check_derived=True)
    assert any("was absent when" in p for p in problems), problems


def test_an_empty_derivation_refuses_to_overwrite_and_keeps_the_valid_stamp(exporter, tmp_path,
                                                                            monkeypatch):
    """An upstream column rename produced zero qualifying footnote rows. The old code
    replaced a 10,000-row file with a header, then crashed in the report
    (ZeroDivisionError) with the PREVIOUS stamp still on disk."""
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    pgp_data = fx.build_tree(tmp_path)  # a valid derived file + stamp already present
    good_stamp = (pgp_data / "derived_provenance.json").read_bytes()
    good_linked = (pgp_data / "transcriptions_linked.csv").read_bytes()
    # Every footnote row is now a Discussion: nothing qualifies as a transcription.
    (pgp_data / "footnotes.csv").write_text(
        fx.FOOTNOTES_CSV.replace("Digital Edition", "Discussion")
                        .replace("Digital Translation", "Discussion"),
        encoding="utf-8",
    )
    fx.write_upstream_manifest(pgp_data)  # the edit is legitimate upstream content

    rc, _out, err = _run_main(exporter, tmp_path)
    assert rc == 1
    assert "refusing to overwrite transcriptions_linked.csv" in err
    assert (pgp_data / "transcriptions_linked.csv").read_bytes() == good_linked
    assert (pgp_data / "derived_provenance.json").read_bytes() == good_stamp, (
        "the old file is intact, so its stamp still describes it"
    )


def test_a_crash_after_the_write_leaves_no_stamp_describing_the_old_file(exporter, tmp_path,
                                                                        monkeypatch):
    """The ZeroDivisionError route, generalised. The linked CSV is written, then something
    between the write and the re-stamp fails (here: the report path is a directory). The
    OLD stamp must already be gone -- it described a file that no longer exists."""
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    pgp_data = fx.build_tree(tmp_path)  # valid derived file + stamp on disk
    report = pgp_data / "export_report.txt"
    report.mkdir()  # open() for writing now raises

    with pytest.raises(OSError):
        exporter.__file__ = str(tmp_path / "scripts" / "pgp_transcriptions_export.py")
        with redirect_stdout(io.StringIO()):
            exporter.main()

    assert not (pgp_data / "derived_provenance.json").exists(), (
        "a stamp that outlives the file it described will certify the next thing written "
        "at that path"
    )


# ── the verifier itself ───────────────────────────────────────────────────────


def test_a_manifest_that_does_not_cover_every_file_is_rejected(tmp_path):
    """Two entries certified an impostor at the third name."""
    pgp_data = fx.build_tree(tmp_path)
    manifest_path = pgp_data / "upstream_provenance.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    del manifest["files"]["footnotes.csv"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    problems = _verifier().verify_against_provenance(str(pgp_data), check_derived=False)
    assert any("does not cover footnotes.csv" in p for p in problems), problems


def test_two_missing_commits_are_not_a_match(tmp_path):
    """None == None used to read as 'same commit'."""
    pgp_data = fx.build_tree(tmp_path)
    for name, key in (("upstream_provenance.json", "upstream_commit"),
                      ("derived_provenance.json", "derived_from_commit")):
        path = pgp_data / name
        data = json.loads(path.read_text(encoding="utf-8"))
        del data[key]
        path.write_text(json.dumps(data), encoding="utf-8")

    problems = _verifier().verify_against_provenance(str(pgp_data), check_derived=True)
    assert any("records no upstream commit" in p for p in problems), problems


def test_an_input_edited_while_the_derivation_ran_cannot_verify_clean(exporter, tmp_path,
                                                                     monkeypatch):
    """gpt-6-astra, round 5: the fingerprints were taken AFTER the derivation read the
    files. Editing libraries.csv between the read and the stamp produced output from the
    old mapping stamped with the new file's hash -- and it verified clean. Fingerprinted
    before the read, the same edit leaves the file on disk disagreeing with the stamp."""
    monkeypatch.delenv("PGP_ALLOW_UNVERIFIED_INPUTS", raising=False)
    pgp_data = fx.build_tree(tmp_path, derived=False)
    real_loader = exporter.load_genizahsearch_shelfmarks

    def load_then_edit(libraries_path, supplement_path=None):
        lookup = real_loader(libraries_path, supplement_path)
        fx.tamper(tmp_path / "libraries.csv")  # the race, made deterministic
        return lookup

    monkeypatch.setattr(exporter, "load_genizahsearch_shelfmarks", load_then_edit)
    assert _run_main(exporter, tmp_path)[0] == 0

    problems = _verifier().verify_against_provenance(str(pgp_data), check_derived=True)
    assert any("libraries.csv has changed" in p for p in problems), (
        "the stamp must describe the file the derivation CONSUMED, not the one on disk "
        "afterwards: %r" % problems
    )
