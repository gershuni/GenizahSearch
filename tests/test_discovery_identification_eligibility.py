"""Identification eligibility: which reference material may originate a claim.

A reference can match a fragment truthfully and still not identify it. The
measured case: a prayer-book compendium earned 6,869 live matches across 6,227
manuscripts, ~84% of them in its order-of-prayer divisions, and it SHADOWED
6,948 rows -- more than it earned -- including 220 belonging to the dedicated
Amidah reference built to hold exactly that text.

So the load-bearing behaviour is not "drop some rows". It is that an ineligible
row must stop COMPETING: the page it was taking has to go back to the reference
that can identify it. That is what ``test_an_ineligible_row_stops_shadowing_an
_eligible_one`` pins, and it is the reason the shadow competition runs over the
filtered set rather than the full table.

Ineligible rows are preserved, marked, and given a per-row reason, so the
suppression is auditable rather than a silent deletion.

Every fixture is synthetic: made-up raw ids, made-up offsets, made-up Hebrew.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

import scripts.discovery_v4_match as v4match
from scripts.discovery_identification_eligibility import (
    EligibilityError,
    SCHEMA_VERSION,
    content_hash_for_entries,
    ineligible_reason,
    load_eligibility_artifact,
    widest_span,
)

CORPUS_SHA = "a" * 64
RULE = (
    "A structurally delimited division whose primary function is to transmit a "
    "performable order of prayer is ineligible to originate ordinary source "
    "identifications."
)


def _work_entry(raw_id="REF9:prayerbook", **overrides) -> dict:
    entry = {
        "raw_reference_id": raw_id,
        "scope": "work",
        "classification": "order of prayer",
        "basis": "owner ruling test-ruling (2026-08-17)",
        "rationale": "Synthetic prayer book: the whole work is an order of prayer.",
    }
    entry.update(overrides)
    return entry


def _division_entry(raw_id="REF9:code", start=1000, end=2000, **overrides) -> dict:
    entry = {
        "raw_reference_id": raw_id,
        "scope": "divisions",
        "classification": "order of prayer",
        "basis": "owner ruling test-ruling (2026-08-17)",
        "rationale": "One appended prayer order inside an otherwise halakhic work.",
        "divisions": [
            {"label_he": "סדר התפילה", "start_offset": start, "end_offset": end}
        ],
    }
    entry.update(overrides)
    return entry


def _write_artifact(tmp_path: Path, entries, **overrides) -> Path:
    doc = {
        "schema_version": SCHEMA_VERSION,
        "rule": RULE,
        "ruled_on": "2026-08-17",
        "reference_corpus_sha256": CORPUS_SHA,
        "entries": entries,
        "content_hash": content_hash_for_entries(entries),
    }
    doc.update(overrides)
    path = tmp_path / "eligibility.json"
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return path


def _spans(rg0: int, rg1: int) -> str:
    return json.dumps([{"p0": 0, "p1": rg1 - rg0, "dens": 0.32, "rg0": rg0, "rg1": rg1}])


# --------------------------------------------------------------------------
# loader: fail closed on every defect
# --------------------------------------------------------------------------


def test_loader_accepts_a_valid_artifact(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry(), _division_entry()])
    doc = load_eligibility_artifact(path, reference_corpus_sha256=CORPUS_SHA)
    assert set(doc["by_work"]) == {"REF9:prayerbook", "REF9:code"}
    assert doc["by_work"]["REF9:code"]["ranges"] == [(1000, 2000)]
    assert doc["by_work"]["REF9:prayerbook"]["ranges"] is None


def test_loader_enforces_the_sha256_when_given(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry()])
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    load_eligibility_artifact(path, sha256=actual)
    with pytest.raises(EligibilityError, match="SHA-256 mismatch"):
        load_eligibility_artifact(path, sha256="f" * 64)


def test_loader_rejects_a_different_reference_corpus(tmp_path):
    """The gate that matters most: offsets are positions in ONE letter stream.

    Applied to another corpus they would suppress whatever now happens to lie
    at those offsets -- silently, and in the wrong work.
    """
    path = _write_artifact(tmp_path, [_division_entry()])
    with pytest.raises(EligibilityError, match="do not transfer"):
        load_eligibility_artifact(path, reference_corpus_sha256="b" * 64)


def test_loader_rejects_wrong_schema_version(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry()], schema_version="something-else")
    with pytest.raises(EligibilityError, match="unsupported eligibility artifact schema"):
        load_eligibility_artifact(path)


def test_loader_rejects_top_level_key_drift(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry()], extra_key="nope")
    with pytest.raises(EligibilityError, match="top-level key drift"):
        load_eligibility_artifact(path)


def test_loader_rejects_a_content_hash_that_does_not_match(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry()], content_hash="sha256:" + "0" * 64)
    with pytest.raises(EligibilityError, match="content hash"):
        load_eligibility_artifact(path)


def test_loader_rejects_an_empty_rule(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry()], rule="   ")
    with pytest.raises(EligibilityError, match="must carry the rule"):
        load_eligibility_artifact(path)


def test_loader_rejects_entry_key_drift(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry(note="not a key of this schema")])
    with pytest.raises(EligibilityError, match="entry key drift"):
        load_eligibility_artifact(path)


def test_loader_rejects_an_unknown_scope(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry(scope="sometimes")])
    with pytest.raises(EligibilityError, match="unknown scope"):
        load_eligibility_artifact(path)


@pytest.mark.parametrize("field", ["classification", "basis", "rationale"])
def test_loader_requires_a_stated_reason(tmp_path, field):
    """A suppression with no recorded rationale is exactly what the rule forbids."""
    path = _write_artifact(tmp_path, [_work_entry(**{field: ""})])
    with pytest.raises(EligibilityError, match=f"{field} must not be empty"):
        load_eligibility_artifact(path)


def test_loader_rejects_work_scope_carrying_divisions(tmp_path):
    entry = _work_entry()
    entry["divisions"] = [{"label_he": "x", "start_offset": 0, "end_offset": 5}]
    path = _write_artifact(tmp_path, [entry])
    with pytest.raises(EligibilityError, match="must not list divisions"):
        load_eligibility_artifact(path)


def test_loader_rejects_division_scope_with_no_divisions(tmp_path):
    path = _write_artifact(tmp_path, [_division_entry(divisions=[])])
    with pytest.raises(EligibilityError, match="no divisions listed"):
        load_eligibility_artifact(path)


def test_loader_rejects_overlapping_or_unsorted_divisions(tmp_path):
    entry = _division_entry()
    entry["divisions"] = [
        {"label_he": "א", "start_offset": 0, "end_offset": 1000},
        {"label_he": "ב", "start_offset": 500, "end_offset": 1500},
    ]
    path = _write_artifact(tmp_path, [entry])
    with pytest.raises(EligibilityError, match="overlap or are unsorted"):
        load_eligibility_artifact(path)


def test_loader_rejects_an_inverted_division(tmp_path):
    path = _write_artifact(tmp_path, [_division_entry(start=2000, end=1000)])
    with pytest.raises(EligibilityError, match="empty or inverted"):
        load_eligibility_artifact(path)


def test_loader_rejects_a_duplicate_reference(tmp_path):
    path = _write_artifact(tmp_path, [_work_entry(), _work_entry()])
    with pytest.raises(EligibilityError, match="appears twice"):
        load_eligibility_artifact(path)


def test_loader_rejects_an_empty_entry_list(tmp_path):
    path = _write_artifact(tmp_path, [])
    with pytest.raises(EligibilityError, match="lists no entries"):
        load_eligibility_artifact(path)


def test_loader_rejects_a_missing_file(tmp_path):
    with pytest.raises(EligibilityError, match="not found"):
        load_eligibility_artifact(tmp_path / "absent.json")


# --------------------------------------------------------------------------
# the per-row decision
# --------------------------------------------------------------------------


def test_widest_span_picks_the_widest(tmp_path):
    payload = json.dumps(
        [
            {"rg0": 0, "rg1": 10},
            {"rg0": 500, "rg1": 900},
            {"rg0": 100, "rg1": 120},
        ]
    )
    assert widest_span(payload) == (500, 900)
    assert widest_span(None) is None
    assert widest_span("[]") is None


def test_a_whole_work_entry_bars_every_row(tmp_path):
    doc = load_eligibility_artifact(_write_artifact(tmp_path, [_work_entry()]))
    assert ineligible_reason(doc, "REF9:prayerbook", _spans(0, 200)) is not None
    # ... including a row with no reference offsets at all.
    assert ineligible_reason(doc, "REF9:prayerbook", None) is not None


def test_a_row_outside_the_artifact_is_eligible(tmp_path):
    doc = load_eligibility_artifact(_write_artifact(tmp_path, [_work_entry()]))
    assert ineligible_reason(doc, "REF9:something_else", _spans(0, 200)) is None


def test_a_division_bars_a_row_that_mostly_sits_inside_it(tmp_path):
    doc = load_eligibility_artifact(_write_artifact(tmp_path, [_division_entry()]))
    reason = ineligible_reason(doc, "REF9:code", _spans(1200, 1800))
    assert reason is not None
    assert "סדר התפילה" in reason


def test_a_division_lets_through_a_row_that_mostly_sits_outside_it(tmp_path):
    """A match that runs on out of the prayer order is evidence about the prose.

    Span [900, 1100): 100 letters inside the division, 100 outside -- exactly at
    the boundary, so it stays eligible. Only a MAJORITY inside suppresses.
    """
    doc = load_eligibility_artifact(_write_artifact(tmp_path, [_division_entry()]))
    assert ineligible_reason(doc, "REF9:code", _spans(900, 1100)) is None
    assert ineligible_reason(doc, "REF9:code", _spans(0, 500)) is None
    # One letter more inside than outside is a majority.
    assert ineligible_reason(doc, "REF9:code", _spans(901, 1100)) is not None


# --------------------------------------------------------------------------
# promote: the mechanism in place
# --------------------------------------------------------------------------

_PROMOTED_COLUMNS = v4match.TRACK1_PROMOTED_COLUMNS


def _promotable_db(tmp_path: Path, staged_rows: list[dict], tag: str = "elig") -> Path:
    db_path = tmp_path / "research.sqlite3"
    staged = v4match.staged_table(tag)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"CREATE TABLE {staged} (page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, "
            "genre TEXT, author TEXT, title TEXT, matched_letters INTEGER, "
            "best_density REAL, n_spans INTEGER, spans_json TEXT, generation TEXT, "
            "ref_spans_json TEXT, shadowed_by TEXT)"
        )
        for row in staged_rows:
            conn.execute(
                f"INSERT INTO {staged} ({', '.join(_PROMOTED_COLUMNS)}) "
                f"VALUES ({', '.join('?' * len(_PROMOTED_COLUMNS))})",
                tuple(row[column] for column in _PROMOTED_COLUMNS),
            )
        # promote preserves an existing V2 snapshot and requires its frozen
        # row count, so the fixture supplies one of exactly that size.
        conn.execute("CREATE TABLE track1_matches_v2_snapshot (x INTEGER)")
        conn.execute(
            "INSERT INTO track1_matches_v2_snapshot SELECT i FROM ("
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<381341) "
            "SELECT i FROM c)"
        )
        v4match._ensure_batch_ledger_table(conn)
        conn.executemany(
            f"INSERT INTO {v4match.BATCH_LEDGER_TABLE} VALUES (?, ?, ?)",
            [(tag, v4match.GENERATION, 0)],
        )
        conn.commit()
    return db_path


def _staged_row(page_id, work_id, density, span, ref_span) -> dict:
    start, end = span
    return {
        "page_id": page_id,
        "sys_id": "s1",
        "work_id": work_id,
        "cat": "Sefaria",
        "genre": "Genre",
        "author": "Author",
        "title": work_id,
        "matched_letters": end - start,
        "best_density": density,
        "n_spans": 1,
        "spans_json": json.dumps([[start, end, density]]),
        "generation": v4match.GENERATION,
        "ref_spans_json": _spans(*ref_span),
        "shadowed_by": None,
    }


def _promote(tmp_path, db_path, artifact: Path | None, tag="elig", monkeypatch=None):
    registry_path = _registry(tmp_path)
    status = {
        "db": str(db_path),
        "table": v4match.staged_table(tag),
        "complete": True,
        "missing_ref_offsets": 0,
        "duplicate_pairs": 0,
        "expected_batches": 1,
        "row_count": None,  # filled by the caller below
        "run_id": "0" * 64,
        "reference_sha256": CORPUS_SHA,
        "masks_sha256": "b" * 64,
        "source_db_seed_sha256": "c" * 64,
        "pilot_sha256": "d" * 64,
        "calibration_sha256": "e" * 64,
        "fingerprint": "f" * 40,
        "page_count": 667_411,
        "page_batch": 2_000,
    }
    with sqlite3.connect(db_path) as conn:
        status["row_count"] = conn.execute(
            f"SELECT COUNT(*) FROM {status['table']}"
        ).fetchone()[0]
    monkeypatch.setattr(v4match, "inspect_stage", lambda args: status)
    args = argparse.Namespace(
        tag=tag,
        cohort_registry=str(registry_path),
        report=None,
        contract=None,
        eligibility=str(artifact) if artifact else None,
        eligibility_sha256=(
            hashlib.sha256(artifact.read_bytes()).hexdigest() if artifact else None
        ),
    )
    return v4match.promote(args)


def _registry(tmp_path: Path) -> Path:
    (tmp_path / "ref6_map.json").write_text(
        json.dumps(
            {
                "schema_version": "discovery-v4-sources-v1",
                "sources": [
                    {"key": "prayerbook", "identity_mode": "public_first"},
                    {"key": "other", "identity_mode": "public_first"},
                ],
            }
        ),
        encoding="utf-8",
    )
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "discovery-routing-cohorts-v1",
                "cohorts": [
                    {
                        "namespace": "REF6",
                        "cohort": "extrapolated",
                        "identity_mode": "per_entry",
                        "source_map": "ref6_map.json",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_an_ineligible_row_stops_shadowing_an_eligible_one(tmp_path, monkeypatch):
    """THE point of the mechanism: the page goes back to the reference that can
    identify it.

    Both rows sit on the same page with a full overlap and a density gap, so
    under the frozen algorithm the prayer-book row shadows the other. Once the
    prayer book is ineligible it is out of the competition, and the row it was
    suppressing becomes live.
    """
    rows = [
        _staged_row("p1", "REF6:prayerbook", 0.30, (0, 100), (0, 100)),
        _staged_row("p1", "REF6:other", 0.40, (0, 100), (0, 100)),
    ]
    db_path = _promotable_db(tmp_path, rows)
    artifact = _write_artifact(tmp_path, [_work_entry("REF6:prayerbook")])

    # First, without the artifact: the eligible row IS shadowed.
    baseline = _promote(tmp_path, db_path, None, monkeypatch=monkeypatch)
    with sqlite3.connect(db_path) as conn:
        state = dict(
            conn.execute("SELECT work_id, shadowed_by FROM track1_matches").fetchall()
        )
    assert state == {"REF6:prayerbook": None, "REF6:other": "REF6:prayerbook"}
    assert baseline["live_rows"] == 1
    assert "eligibility" not in baseline

    # Then with it: the prayer book is out, and the other row is live.
    report = _promote(tmp_path, db_path, artifact, monkeypatch=monkeypatch)
    with sqlite3.connect(db_path) as conn:
        state = dict(
            conn.execute("SELECT work_id, shadowed_by FROM track1_matches").fetchall()
        )
        recorded = conn.execute(
            f"SELECT work_id, reason FROM {v4match.INELIGIBLE_TABLE}"
        ).fetchall()
    assert state == {
        "REF6:prayerbook": v4match.INELIGIBLE_MARKER,
        "REF6:other": None,
    }
    assert report["live_rows"] == 1
    assert report["promoted_rows"] == 2  # preserved, not deleted
    assert recorded == [("REF6:prayerbook", "order of prayer: whole work ineligible")]
    assert report["eligibility"]["ineligible_rows"] == 1
    assert report["eligibility"]["ineligible_rows_by_work"] == {"REF6:prayerbook": 1}


def test_the_contract_names_the_eligibility_filter(tmp_path, monkeypatch):
    rows = [_staged_row("p1", "REF6:other", 0.40, (0, 100), (0, 100))]
    db_path = _promotable_db(tmp_path, rows)
    artifact = _write_artifact(tmp_path, [_work_entry("REF6:prayerbook")])

    plain = _promote(tmp_path, db_path, None, monkeypatch=monkeypatch)
    assert plain["release_contract"]["shadow_algorithm"] == v4match.SHADOW_ALGORITHM_FACT

    filtered = _promote(tmp_path, db_path, artifact, monkeypatch=monkeypatch)
    fact = filtered["release_contract"]["shadow_algorithm"]
    assert fact.startswith(v4match.SHADOW_ALGORITHM_FACT + "/eligibility:")
    assert filtered["eligibility"]["content_hash"] in fact


def test_promote_requires_the_artifact_and_its_hash_together(tmp_path, monkeypatch):
    rows = [_staged_row("p1", "REF6:other", 0.40, (0, 100), (0, 100))]
    db_path = _promotable_db(tmp_path, rows)
    artifact = _write_artifact(tmp_path, [_work_entry("REF6:prayerbook")])
    status = {"reference_sha256": CORPUS_SHA}
    args = argparse.Namespace(eligibility=str(artifact), eligibility_sha256=None)
    with pytest.raises(ValueError, match="must be supplied together"):
        v4match.load_promotion_eligibility(args, status)
    args = argparse.Namespace(eligibility=None, eligibility_sha256="a" * 64)
    with pytest.raises(ValueError, match="must be supplied together"):
        v4match.load_promotion_eligibility(args, status)
    # Neither is the pre-existing behaviour, and stays available.
    args = argparse.Namespace(eligibility=None, eligibility_sha256=None)
    assert v4match.load_promotion_eligibility(args, status) is None


def test_promotion_binds_the_artifact_to_the_matched_corpus(tmp_path, monkeypatch):
    """A promotion may not apply offsets written against another corpus."""
    artifact = _write_artifact(tmp_path, [_division_entry("REF6:other")])
    args = argparse.Namespace(
        eligibility=str(artifact),
        eligibility_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
    )
    with pytest.raises(EligibilityError, match="do not transfer"):
        v4match.load_promotion_eligibility(args, {"reference_sha256": "9" * 64})
