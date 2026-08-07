"""Tests for the novelty-reuse measurement's pinning (Codex round 5, HIGH).

Round 4 asked for the inputs to be recorded; round 5 found that recording is not
pinning: `--population` was "freely selected by the caller and is not derived from,
or validated against, the input asset", `_hash` turned an unreadable input into a
report STRING while still exiting zero, and hashing happened only AFTER the
minutes-long candidate build.

The stake is specific: this report is the intended evidence for a spend decision.
A number that can be mislabelled is the exact failure that let the retracted ~$4
figure stand.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.v3_measure_novelty_reuse import (  # noqa: E402
    MeasurementError,
    _hash_or_die,
    _verify_population,
)


def _asset(path: Path, *, coverage_routing=None, version="discovery-v1-real"):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    rows = [("sidecar_version", version)]
    if coverage_routing is not None:
        rows.append(("coverage_routing", coverage_routing))
    conn.executemany("INSERT INTO meta VALUES (?,?)", rows)
    conn.commit()
    conn.close()
    return path


def test_a_PRE_V3_asset_cannot_be_labelled_pinned(tmp_path):
    """THE round-5 property: the label is verified, not asserted.

    A pre-v3 asset carries NO `meta.coverage_routing` row -- only a v3-era
    `finalize_build` writes one -- so it cannot be a v3 population whatever the
    caller types.
    """
    legacy = _asset(tmp_path / "legacy.db")
    assert _verify_population(str(legacy), "legacy")["meta_coverage_routing"] is None
    with pytest.raises(MeasurementError, match="no `meta.coverage_routing` row"):
        _verify_population(str(legacy), "pinned")


def test_a_DELIBERATE_lever1_v3_build_is_a_v3_population(tmp_path):
    """Codex round 6 (MEDIUM), correcting my own first definition.

    I had equated "v3 population" with `coverage_routing == 'gen2_router'`. That
    mislabels a build the pipeline SUPPORTS: `allow_lever1_coverage=True` records
    `coverage_routing = 'lever1_cliff'`, and such a build still has a v3 assembly and
    a v3 work set -- its candidate population is v3 even though its routing is the
    legacy cliff. Rejecting it made the measurement unavailable for a supported
    choice. The routing MODE is now reported separately instead, so neither case is
    mislabelled.
    """
    cliff = _asset(tmp_path / "cliff.db", coverage_routing="lever1_cliff")
    got = _verify_population(str(cliff), "pinned")
    assert got["routing_mode"] == "lever1_cliff", (
        "a deliberately cliff-routed v3 build is rejected as a v3 population, or its "
        "routing mode is not reported -- a `pinned` number would then be ambiguous "
        "about which routing produced it"
    )
    # ...and it must NOT be passable as legacy either.
    with pytest.raises(MeasurementError, match="Label it `pinned`"):
        _verify_population(str(cliff), "legacy")


def test_a_router_asset_cannot_be_labelled_legacy(tmp_path):
    """The other direction, which matters for a different reason: an under-claimed
    v3 measurement gets ignored as stale, so the real price never reaches the owner."""
    routed = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    got = _verify_population(str(routed), "pinned")
    assert got["meta_coverage_routing"] == "gen2_router"
    assert got["routing_mode"] == "gen2_router"
    with pytest.raises(MeasurementError, match="Label it `pinned`"):
        _verify_population(str(routed), "legacy")


def test_an_unreadable_asset_halts_rather_than_defaulting(tmp_path):
    with pytest.raises(MeasurementError, match="cannot read `meta`"):
        _verify_population(str(tmp_path / "does-not-exist.db"), "legacy")


def test_an_unhashable_input_halts_instead_of_becoming_a_report_string(tmp_path):
    """Codex R5: `_hash` "converts an unreadable input into a report string and
    still exits zero", so a report could describe candidates whose inputs were not
    all hash-bound while still looking like option-0 evidence."""
    good = tmp_path / "f.bin"
    good.write_bytes(b"abc")
    assert len(_hash_or_die(str(good), "good")) == 64

    with pytest.raises(MeasurementError, match="cannot hash input"):
        _hash_or_die(str(tmp_path / "missing.bin"), "missing")


def test_a_cache_changed_after_loading_HALTS(tmp_path, monkeypatch):
    """Codex round 6 (HIGH), and my previous test could not have caught it.

    That test asserted three SOURCE substrings around `build_all_candidates`. Round 6
    was right that it "does not execute the branch, mutate an input, or establish
    that every reader's state is covered" -- keeping those strings while retaining the
    cache-read-before-hash race stayed green.

    This EXECUTES `main()` with a real cache file that is MUTATED during candidate
    construction, and requires the run to halt. Before the fix the counters would come
    from the pre-mutation object while the report recorded the post-mutation hash, and
    a stable second hash would falsely confirm it.
    """
    import scripts.v3_measure_novelty_reuse as mod

    asset = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    cache = tmp_path / "verdicts.json"
    cache.write_text('{"s1::w000001": {"novelty_status": "fills_gap"}}', encoding="utf-8")
    for name in ("libraries.csv", "fjms.db", "fgp.db", "pgp.db"):
        (tmp_path / name).write_bytes(b"x")

    # Mutate the cache in the EXACT race window: between the moment it is read and
    # the moment it is hashed. Patching `json.load` is what pins the ORDER -- mutating
    # during `build_all_candidates` (my first attempt) is caught by the post-run
    # re-verification whichever order the code uses, so it could not distinguish
    # hash-then-load from load-then-hash. Mutation testing found that: swapping the
    # two left the test green.
    real_load = mod.json.load

    def load_then_mutate(fh):
        loaded = real_load(fh)
        cache.write_text('{"s1::w000001": {"novelty_status": "confirms"}}',
                         encoding="utf-8")
        return loaded

    monkeypatch.setattr(mod.json, "load", load_then_mutate)
    monkeypatch.setattr(mod, "build_all_candidates", lambda **_kw: ([], {}, {}))
    monkeypatch.setattr(mod, "run_heuristic_funnel", lambda _c: ({}, []))
    with pytest.raises(MeasurementError, match="CHANGED during the measurement"):
        mod.main([
            "--asset", str(asset), "--cache", str(cache), "--population", "pinned",
            "--libraries-csv", str(tmp_path / "libraries.csv"),
            "--fjms-db", str(tmp_path / "fjms.db"),
            "--fgp-db", str(tmp_path / "fgp.db"),
            "--pgp-db", str(tmp_path / "pgp.db"),
            "--report", str(tmp_path / "r.json"),
        ])
    assert not (tmp_path / "r.json").exists(), (
        "a report was written despite an input changing mid-measurement"
    )


def test_a_SQLITE_JOURNAL_appearing_mid_measurement_HALTS(tmp_path, monkeypatch):
    """The second round-6 hole: SQLite can serve committed content from sibling
    `-journal`/`-wal` files, which the first version never hashed -- so candidate
    input could change while both main-file hashes agreed.

    A sidecar appearing mid-run is the detectable form of that, which is why an
    ABSENT sidecar is recorded as `(absent)` rather than skipped: skipping it would
    make the second pass agree with the first.
    """
    import scripts.v3_measure_novelty_reuse as mod

    asset = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    cache = tmp_path / "verdicts.json"
    cache.write_text("{}", encoding="utf-8")
    for name in ("libraries.csv", "fjms.db", "fgp.db", "pgp.db"):
        (tmp_path / name).write_bytes(b"x")

    def fake_build(**_kw):
        # A WAL file appears beside the asset, as it would under a concurrent writer.
        (tmp_path / "v3.db-wal").write_bytes(b"committed-but-not-in-the-main-file")
        return [], {}, {}

    monkeypatch.setattr(mod, "build_all_candidates", fake_build)
    with pytest.raises(MeasurementError, match="CHANGED during the measurement"):
        mod.main([
            "--asset", str(asset), "--cache", str(cache), "--population", "pinned",
            "--libraries-csv", str(tmp_path / "libraries.csv"),
            "--fjms-db", str(tmp_path / "fjms.db"),
            "--fgp-db", str(tmp_path / "fgp.db"),
            "--pgp-db", str(tmp_path / "pgp.db"),
            "--report", str(tmp_path / "r.json"),
        ])


def test_an_UNCHANGED_measurement_writes_its_report(tmp_path, monkeypatch):
    """The control. Without it, "halt on everything" would satisfy both tests above
    while making the measurement impossible to run."""
    import json as _json

    import scripts.v3_measure_novelty_reuse as mod

    asset = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    cache = tmp_path / "verdicts.json"
    cache.write_text("{}", encoding="utf-8")
    for name in ("libraries.csv", "fjms.db", "fgp.db", "pgp.db"):
        (tmp_path / name).write_bytes(b"x")

    monkeypatch.setattr(mod, "build_all_candidates", lambda **_kw: ([], {}, {}))
    monkeypatch.setattr(mod, "run_heuristic_funnel", lambda _c: ({}, []))
    rc = mod.main([
        "--asset", str(asset), "--cache", str(cache), "--population", "pinned",
        "--libraries-csv", str(tmp_path / "libraries.csv"),
        "--fjms-db", str(tmp_path / "fjms.db"),
        "--fgp-db", str(tmp_path / "fgp.db"),
        "--pgp-db", str(tmp_path / "pgp.db"),
        "--report", str(tmp_path / "r.json"),
    ])
    assert rc == 0
    report = _json.loads((tmp_path / "r.json").read_text(encoding="utf-8"))
    assert report["population"] == "pinned"
    # The sidecar keys must be present and recorded as absent -- that is what makes
    # a later appearance detectable.
    assert report["input_sha256"]["asset-wal"] == "(absent)", report["input_sha256"]


def test_a_failed_measurement_exits_nonzero(tmp_path, capsys):
    """An operator running this for a spend decision must not read a traceback as
    a result, nor a zero exit as a pass."""
    from scripts.v3_measure_novelty_reuse import _cli

    rc = _cli(["--asset", str(tmp_path / "nope.db"), "--population", "pinned"])
    assert rc == 1
    assert "fail-closed" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Codex ROUND 7 findings.
# ---------------------------------------------------------------------------

def test_an_asset_changed_between_verification_and_hashing_HALTS(tmp_path, monkeypatch):
    """Codex R7 (HIGH): `_verify_population` read the asset BEFORE `_hash_all`.

    So an asset changed in that window left the report carrying a population claim
    derived from the OLD state while its hashes -- and the final re-verification --
    described the new one, and the two agreed. The fix is ordering: hash first, then
    derive the claim from the hashed state.

    Reproduced by mutating the asset from inside `_verify_population`, i.e. exactly
    the window. With the fix the mutation lands AFTER the first hash, so the closing
    re-verification catches it; before the fix it landed between the read and the
    hash and nothing did.
    """
    import scripts.v3_measure_novelty_reuse as mod

    asset = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    cache = tmp_path / "verdicts.json"
    cache.write_text("{}", encoding="utf-8")
    for name in ("libraries.csv", "fjms.db", "fgp.db", "pgp.db"):
        (tmp_path / name).write_bytes(b"x")

    real_verify = mod._verify_population

    def verify_then_mutate(path, claimed):
        out = real_verify(path, claimed)
        with open(path, "ab") as fh:          # change the asset in the window
            fh.write(b"\x00appended")
        return out

    monkeypatch.setattr(mod, "_verify_population", verify_then_mutate)
    monkeypatch.setattr(mod, "build_all_candidates", lambda **_kw: ([], {}, {}))
    monkeypatch.setattr(mod, "run_heuristic_funnel", lambda _c: ({}, []))

    with pytest.raises(MeasurementError, match="CHANGED during the measurement"):
        mod.main([
            "--asset", str(asset), "--cache", str(cache), "--population", "pinned",
            "--libraries-csv", str(tmp_path / "libraries.csv"),
            "--fjms-db", str(tmp_path / "fjms.db"),
            "--fgp-db", str(tmp_path / "fgp.db"),
            "--pgp-db", str(tmp_path / "pgp.db"),
            "--report", str(tmp_path / "r.json"),
        ])
    assert not (tmp_path / "r.json").exists()


def test_an_unrecognised_routing_mode_is_REFUSED(tmp_path):
    """Codex R7 (MEDIUM): accepting any non-null value "replaces one unsound proxy
    with another" -- notably `coverage_routing='none'`, which `finalize_build` writes
    whenever D-17 is inactive, was silently accepted as `pinned`.

    The vocabulary is now closed. `none` is still a v3 build (the pipeline wrote the
    row) but its mode is reported, so a `pinned` number says which routing produced
    it; an UNKNOWN value is refused outright, because it means the writer changed or
    the row was hand-edited.
    """
    from scripts.v3_measure_novelty_reuse import _V3_ROUTING_MODES

    none_asset = _asset(tmp_path / "none.db", coverage_routing="none")
    got = _verify_population(str(none_asset), "pinned")
    assert got["routing_mode"] == "none", (
        "a v3 build with no coverage routing is no longer reported as such -- a "
        "`pinned` number would not say which routing produced its population"
    )

    bogus = _asset(tmp_path / "bogus.db", coverage_routing="hand_edited_value")
    with pytest.raises(MeasurementError, match="not.*one of"):
        _verify_population(str(bogus), "pinned")
    assert "hand_edited_value" not in _V3_ROUTING_MODES


def test_the_routing_modes_match_what_finalize_build_writes():
    """The closed vocabulary must track the WRITER, or it rejects a real asset.

    Derived from the writer's own expression rather than restated, so adding a fourth
    mode to `finalize_build` without updating the checker fails here instead of at a
    measurement.
    """
    import re

    import build_discovery_sidecar as bds
    from scripts.v3_measure_novelty_reuse import _V3_ROUTING_MODES

    src = Path(bds.__file__).read_text(encoding="utf-8")
    # Locate the ("coverage_routing", <expr>) tuple and harvest EVERY string
    # literal the expression can yield. Formatting-agnostic on purpose: the
    # previous version pinned one exact single-line shape and broke the moment the
    # expression grew a fourth alternative and wrapped, which made a real
    # vocabulary change look like a missing expression.
    start = src.index('("coverage_routing"')
    depth = 0
    end = start
    for i in range(start, len(src)):
        if src[i] == "(":
            depth += 1
        elif src[i] == ")":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    expr = src[start:end]
    assert expr.startswith('("coverage_routing"'), (
        "could not locate the coverage_routing meta expression")
    written = set(re.findall(r'"([a-z0-9_]+)"', expr)) - {"coverage_routing"}
    assert written, f"no candidate values found in the writer expression: {expr!r}"
    assert written == set(_V3_ROUTING_MODES), (
        f"the checker's closed vocabulary and the writer disagree: "
        f"writer-only={sorted(written - set(_V3_ROUTING_MODES))}, "
        f"checker-only={sorted(set(_V3_ROUTING_MODES) - written)}"
    )


def test_a_REAL_WAL_change_during_the_measurement_HALTS(tmp_path, monkeypatch):
    """Codex R7 (LOW): my WAL test wrote arbitrary bytes named `*.db-wal`, so "no
    SQLite reader observes a valid WAL-backed state" -- it proved the key comparison
    notices an appearing file, which is easier than the claim.

    This uses a REAL WAL-mode SQLite database and commits a change through SQLite
    during the measurement, so the sidecar content is genuine WAL state that a reader
    would observe.
    """
    import scripts.v3_measure_novelty_reuse as mod

    asset = _asset(tmp_path / "v3.db", coverage_routing="gen2_router")
    cache = tmp_path / "verdicts.json"
    cache.write_text("{}", encoding="utf-8")
    (tmp_path / "libraries.csv").write_bytes(b"x")
    (tmp_path / "fgp.db").write_bytes(b"x")
    (tmp_path / "pgp.db").write_bytes(b"x")

    # A genuine WAL-mode database as one of the candidate inputs.
    fjms = tmp_path / "fjms.db"
    conn = sqlite3.connect(str(fjms))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t (v TEXT)")
    conn.execute("INSERT INTO t VALUES ('before')")
    conn.commit()
    conn.close()
    before = mod._hash_or_die(str(fjms), "fjms_db")

    held = []

    observed = []

    def commit_through_sqlite(**_kw):
        # The connection is LEFT OPEN deliberately. Closing it checkpoints the WAL
        # into the main file and deletes the sidecar, which changes the main file --
        # so a closed writer would be caught by main-file hashing alone and the
        # fixture would not demonstrate why sidecars matter. An open connection is
        # also the realistic case: a concurrent writer holding the database.
        writer = sqlite3.connect(str(fjms))
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("INSERT INTO t VALUES ('committed during the measurement')")
        writer.commit()
        held.append(writer)
        # THEN READ IT BACK THROUGH SQLITE (Codex round 8, LOW). Without this the test
        # proved only that `_hash_all` notices a changed sidecar; it did not show that
        # a reader in the candidate-build path OBSERVES the committed row, which is
        # the reason unhashed sidecar state is dangerous rather than merely untidy.
        reader = sqlite3.connect(str(fjms))
        try:
            observed.extend(v for (v,) in reader.execute("SELECT v FROM t"))
        finally:
            reader.close()
        return [], {}, {}

    monkeypatch.setattr(mod, "build_all_candidates", commit_through_sqlite)
    monkeypatch.setattr(mod, "run_heuristic_funnel", lambda _c: ({}, []))

    with pytest.raises(MeasurementError, match="CHANGED during the measurement"):
        mod.main([
            "--asset", str(asset), "--cache", str(cache), "--population", "pinned",
            "--libraries-csv", str(tmp_path / "libraries.csv"),
            "--fjms-db", str(fjms),
            "--fgp-db", str(tmp_path / "fgp.db"),
            "--pgp-db", str(tmp_path / "pgp.db"),
            "--report", str(tmp_path / "r.json"),
        ])
    # The point of the fixture: a WAL commit can leave the MAIN file byte-identical,
    # so main-file hashing alone would have seen nothing.
    # A candidate-path reader really did see the WAL-committed row -- so the change
    # this halts on is one that WOULD have altered the candidate population.
    assert "committed during the measurement" in observed, (
        f"no SQLite reader observed the committed WAL row, so this fixture proves "
        f"only that a sidecar file changed: {observed}"
    )
    assert "before" in observed, "the reader did not see the pre-existing row either"

    try:
        assert mod._hash_or_die(str(fjms), "fjms_db") == before, (
            "the WAL commit also changed the main file, so this fixture no longer "
            "demonstrates why sidecars must be hashed -- it would pass without them"
        )
    finally:
        for writer in held:
            writer.close()
