# -*- coding: utf-8 -*-
"""SEED-029 Lever-1 coverage REPLICATION gate (Phase 135, plan 135-07, PART 2).

Pre-registered held-out replication (Codex requirement): proves the ported
`norm_stream` normalizer + coverage metric in
`scripts/build_discovery_sidecar.py` is a FAITHFUL reimplementation of the
SEED-029 metric BEFORE any production re-bake runs. Two independent checks over
the 200 catalogue-blind page-level grades:

  1. ROW-LEVEL REPLAY: for each graded `(page, work)` unit, recompute
     `coverage = min(1.0, matched_letters / len(norm_stream(page_text)))` with
     the PART-1 build-script code and confirm it reproduces the graded `cov`
     within a tiny float tolerance (the stored `cov` is rounded to 3 decimals).
  2. BAND REPRODUCTION: bin the grades by the RECOMPUTED coverage
     (high >= 0.60 / med 0.45-0.60 / low < 0.45) and confirm the SEED-029
     precision bands (94.0% / 91.7% / 37.5%) reproduce within tolerance.

If EITHER check fails the gate FAILS (nonzero) and the bake MUST NOT proceed.
Do NOT "adjust" thresholds to force a pass -- a failure means the normalizer
port diverged from `normalize.py` and the coverage denominator is wrong.

MASKING / DATA DISCIPLINE
  - This test reads GITIGNORED real artifacts at RUN time only; it NEVER commits
    them and NEVER prints a graded `id`, a work id, a raw source id, or any
    Hebrew/M-source content -- diagnostics are aggregate counts + numeric
    coverage values ONLY.
  - It skips CLEANLY (pytest.skip) when either artifact is absent on the box,
    exactly like the other real-artifact smokes in the discovery suite.

Inputs (both gitignored):
  - grades:  discovery_data/track1_id_grades (3).json
             (fields: id, stratum, tbucket, cls, tier, cov, letters, grade;
              `letters` == matched_letters (numerator);
              `id` == "<page_id>|<raw_work_id>"; page_id is the join key)
  - research DB (pages.text denominator source): the gen-2 corpus DB, located
    via env DISCOVERY_RESEARCH_DB, else the default path below. Only the derived
    integer letter count is ever taken from `pages.text`.
"""
import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:  # pytest: `scripts` resolves to the repo-root package (suite-consistent identity)
    from scripts import build_discovery_sidecar as sidecar_build  # noqa: E402
except ImportError:
    # Standalone (`python tests/test_discovery_coverage_replication.py`): the
    # regular `tests/scripts` package shadows the repo-root namespace `scripts`.
    # Load the build module by explicit file path (it inserts repo_root +
    # scripts/ onto sys.path at import time, so its own flat imports resolve).
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        "build_discovery_sidecar",
        str(_REPO_ROOT / "scripts" / "build_discovery_sidecar.py"))
    sidecar_build = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(sidecar_build)

# The 200 catalogue-blind page-level grades (gitignored, masking-sensitive).
_GRADES_PATH = _REPO_ROOT / "discovery_data" / "track1_id_grades (3).json"

# Research DB carrying pages.text (the coverage denominator source). Overridable
# for boxes where the gen-2 corpus lives elsewhere; default is the standard
# gen-2 corpus path.
_RESEARCH_DB_DEFAULT = _REPO_ROOT / "same_work_spike" / "probe" / "data" / "fullcorpus_v2.db"


def _research_db_path() -> Path:
    override = os.environ.get("DISCOVERY_RESEARCH_DB")
    return Path(override) if override else _RESEARCH_DB_DEFAULT


# Witness-precision numerator grades, frozen in the page-level manifest
# (`track1_pagelevel_manifest.json` "witness_grades"). A graded unit counts as a
# true witness iff its grade is one of these; everything else (citation /
# formula / wrong / print / junk / unsure) is a non-witness.
_WITNESS_GRADES = frozenset({"correct", "cowitness"})

# Row-level tolerance: the stored `cov` is rounded to 3 decimals, so a faithful
# port lands within ~5e-4; 1.5e-3 keeps the rounding margin while still failing
# hard on a wrong normalizer (a divergent denominator shifts cov by >= ~1e-2).
_ROW_TOL = 1.5e-3

# Band-precision targets (SEED-029, discovery-band-labels-v1.md §3.1) + tolerance
# in PERCENTAGE POINTS.
_BAND_TARGETS_PCT = {"high": 94.0, "med": 91.7, "low": 37.5}
_BAND_TOL_PCT = 0.6


def _coverage_band(cov: float) -> str:
    if cov >= 0.60:
        return "high"
    if cov >= 0.45:
        return "med"
    return "low"


def _artifacts_present() -> bool:
    return _GRADES_PATH.exists() and _research_db_path().exists()


def _run_replication_gate():
    """Execute both replication checks. Returns a dict report:
        {ok, n, row_mismatches, worst_row_diff, bands:{band:{n,witness,pct,
         target,ok}}, band_ok}
    Raises no exception on data mismatch -- the caller (test / __main__) decides.
    Prints NO restricted content."""
    grades = json.loads(_GRADES_PATH.read_text(encoding="utf-8"))
    conn = sqlite3.connect(f"file:{_research_db_path().as_posix()}?mode=ro", uri=True)
    try:
        row_mismatches = 0
        worst_row_diff = 0.0
        band_tot = {"high": 0, "med": 0, "low": 0}
        band_wit = {"high": 0, "med": 0, "low": 0}
        for g in grades:
            page_id = str(g["id"]).split("|", 1)[0]
            row = conn.execute(
                "SELECT text FROM pages WHERE page_id = ?", (page_id,)
            ).fetchone()
            text = row[0] if row is not None else None
            page_norm_letters = sidecar_build.norm_stream_letter_count(text)
            cov = sidecar_build.compute_page_coverage(g["letters"], page_norm_letters)
            cov = 0.0 if cov is None else cov
            diff = abs(cov - g["cov"])
            worst_row_diff = max(worst_row_diff, diff)
            if diff > _ROW_TOL:
                row_mismatches += 1
            band = _coverage_band(cov)
            band_tot[band] += 1
            if g["grade"] in _WITNESS_GRADES:
                band_wit[band] += 1
    finally:
        conn.close()

    bands = {}
    band_ok = True
    for b in ("high", "med", "low"):
        n = band_tot[b]
        pct = (100.0 * band_wit[b] / n) if n else 0.0
        target = _BAND_TARGETS_PCT[b]
        ok = abs(pct - target) <= _BAND_TOL_PCT
        band_ok = band_ok and ok and n > 0
        bands[b] = {"n": n, "witness": band_wit[b], "pct": pct, "target": target, "ok": ok}

    report = {
        "n": len(grades),
        "row_mismatches": row_mismatches,
        "worst_row_diff": worst_row_diff,
        "bands": bands,
        "band_ok": band_ok,
        "ok": row_mismatches == 0 and band_ok,
    }
    return report


def _format_report(r) -> str:
    lines = [
        "SEED-029 coverage replication gate",
        f"  graded units: {r['n']}",
        f"  row-level: mismatches={r['row_mismatches']} "
        f"worst_diff={r['worst_row_diff']:.5f} (tol={_ROW_TOL})",
        "  band reproduction (recomputed coverage):",
    ]
    for b in ("high", "med", "low"):
        d = r["bands"][b]
        lines.append(
            f"    {b:>4}: {d['witness']}/{d['n']} = {d['pct']:.1f}% "
            f"(target {d['target']:.1f}%, {'OK' if d['ok'] else 'FAIL'})"
        )
    lines.append(f"  GATE: {'PASS' if r['ok'] else 'FAIL'}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# pytest entrypoints
# ---------------------------------------------------------------------------

def test_coverage_replication_row_level_matches_graded_cov():
    """Row-level replay: the ported normalizer reproduces every graded unit's
    stored `cov`."""
    if not _artifacts_present():
        pytest.skip("SEED-029 grades and/or research DB not present on this box")
    r = _run_replication_gate()
    print("\n" + _format_report(r))
    assert r["row_mismatches"] == 0, (
        f"{r['row_mismatches']}/{r['n']} graded units did not reproduce their stored "
        f"cov within {_ROW_TOL} -- the norm_stream port diverged from normalize.py"
    )


def test_coverage_replication_reproduces_precision_bands():
    """Band reproduction: binning by recomputed coverage reproduces the
    SEED-029 94.0% / 91.7% / 37.5% precision bands."""
    if not _artifacts_present():
        pytest.skip("SEED-029 grades and/or research DB not present on this box")
    r = _run_replication_gate()
    print("\n" + _format_report(r))
    for b in ("high", "med", "low"):
        d = r["bands"][b]
        assert d["n"] > 0, f"band {b} drew 0 graded units"
        assert d["ok"], (
            f"band {b}: reproduced {d['pct']:.1f}% vs target {d['target']:.1f}% "
            f"(tol {_BAND_TOL_PCT}pp)"
        )


if __name__ == "__main__":  # standalone gate: nonzero exit iff the gate FAILS
    if not _artifacts_present():
        print("SKIP: SEED-029 grades and/or research DB not present "
              f"(grades={_GRADES_PATH.exists()}, db={_research_db_path().exists()})")
        sys.exit(0)
    report = _run_replication_gate()
    print(_format_report(report))
    sys.exit(0 if report["ok"] else 1)
