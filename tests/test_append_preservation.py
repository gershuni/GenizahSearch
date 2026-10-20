# -*- coding: utf-8 -*-
"""The append-preservation gate (OPS-02): every way it must pass, and every way it must fail.

Every fixture is FABRICATED (small temp SQLite files with synthetic ids and titles), never
research content. The baseline pair passes first; without it the failure cases below
would prove nothing.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import verify_append_preservation as vap

REPO_ROOT = Path(__file__).resolve().parent.parent

S1, S2, S3, S4, S5 = (f"99000000000000000{i}" for i in range(1, 6))
W1, W2, W3 = "w000001", "w000002", "w000003"

# (sys_id, canonical_work_id, main_pool, main_pool_reason, max_coverage_ppm)
LIVE_IDS = [
    (S1, W1, 1, None, 900_000),
    (S2, W2, 1, None, 800_000),
    (S3, W1, 1, None, 700_000),
    (S4, W3, 0, "low_coverage", 100_000),
]
# (sys_id, a_page_id, routing_status, work_id)
LIVE_EVIDENCE = [
    (S1, "p1", "shipped", W1),
    (S2, "p2", "shipped", W2),
    (S3, "p3", "shipped", W1),
    (S4, "p4", "review_only", W3),
]


def make_db(path, ids, evidence, *, audience="public", drop_table=None, drop_column=None):
    conn = sqlite3.connect(path)
    schema = {
        "meta": "key TEXT, value TEXT",
        "works": "work_id TEXT, canonical_work_id TEXT, neutral_title TEXT",
        "discovery_claim": "claim_id TEXT, work_id TEXT",
        "discovery_evidence": "claim_id TEXT, sys_id TEXT, a_page_id TEXT, routing_status TEXT",
        "discovery_identification": (
            "sys_id TEXT, canonical_work_id TEXT, display_work_id TEXT, main_pool INTEGER, "
            "main_pool_reason TEXT, page_count INTEGER, max_coverage_ppm INTEGER, "
            "novelty_status TEXT"),
        "manuscript_display": "sys_id TEXT, library_code TEXT, shelfmark_display TEXT",
    }
    for table, cols in schema.items():
        conn.execute(f"CREATE TABLE {table} ({cols})")
    conn.execute("INSERT INTO meta VALUES ('audience', ?)", (audience,))
    for work in (W1, W2, W3):
        conn.execute("INSERT INTO works VALUES (?, ?, ?)", (work, work, f"<Title {work}>"))
    for i, (sys_id, page, status, work) in enumerate(evidence):
        conn.execute("INSERT INTO discovery_claim VALUES (?, ?)", (f"c{i}", work))
        conn.execute("INSERT INTO discovery_evidence VALUES (?, ?, ?, ?)",
                     (f"c{i}", sys_id, page, status))
    for sys_id, work, main_pool, reason, coverage in ids:
        conn.execute(
            "INSERT INTO discovery_identification VALUES (?, ?, ?, ?, ?, 1, ?, 'confirms')",
            (sys_id, work, work, main_pool, reason, coverage))
    for sys_id in {i[0] for i in ids}:
        conn.execute("INSERT INTO manuscript_display VALUES (?, 'CUL', ?)",
                     (sys_id, f"T-S {sys_id[-1]}"))
    if drop_table:
        conn.execute(f"DROP TABLE {drop_table}")
    if drop_column:
        conn.execute(f"ALTER TABLE {drop_column[0]} DROP COLUMN {drop_column[1]}")
    conn.commit()
    conn.close()
    return str(path)


@pytest.fixture
def live(tmp_path):
    return make_db(tmp_path / "live.db", LIVE_IDS, LIVE_EVIDENCE)


def candidate(tmp_path, ids=None, evidence=None, name="candidate.db", **kw):
    return make_db(tmp_path / name, LIVE_IDS if ids is None else ids,
                   LIVE_EVIDENCE if evidence is None else evidence, **kw)


def without(rows, sys_id, work=None):
    return [r for r in rows if not (r[0] == sys_id and (work is None or work in r))]


def delta_of(live_path, cand_path):
    return vap.classify_losses(
        vap.open_artifact(live_path, "live"), vap.open_artifact(cand_path, "candidate"),
        vap.sha256_file(live_path), vap.sha256_file(cand_path))


def gate(*argv):
    return vap.run([str(a) for a in argv])


# ---------------------------------------------------------------------------
# Passes
# ---------------------------------------------------------------------------

def test_an_identical_candidate_passes(live, tmp_path, capsys):
    assert gate(live, candidate(tmp_path)) == 0
    assert "PASS: no live main-pool identification is lost" in capsys.readouterr().out


def test_gains_and_promotions_alone_pass(live, tmp_path):
    ids = [r if r[0] != S4 else (S4, W3, 1, None, 600_000) for r in LIVE_IDS]
    ids.append((S5, W2, 1, None, 500_000))
    cand = candidate(tmp_path, ids=ids)
    assert gate(live, cand) == 0
    assert delta_of(live, cand).main_pool_gained == 2


def test_losing_an_identification_outside_the_main_pool_is_counted_but_does_not_block(
        live, tmp_path):
    cand = candidate(tmp_path, ids=without(LIVE_IDS, S4), evidence=without(LIVE_EVIDENCE, S4))
    assert gate(live, cand) == 0
    assert delta_of(live, cand).other_pool_deleted == 1


# ---------------------------------------------------------------------------
# Losses: each fails, each gets its cause
# ---------------------------------------------------------------------------

def test_a_demotion_below_the_main_pool_bar_is_a_loss(live, tmp_path, capsys):
    ids = [r if r[0] != S1 else (S1, W1, 0, "low_coverage", 400_000) for r in LIVE_IDS]
    cand = candidate(tmp_path, ids=ids)
    assert gate(live, cand) == 1
    out = capsys.readouterr().out
    assert "FAIL: 1 live main-pool identification(s) lost and no acceptance given" in out
    assert "demoted with coverage lower than live       1" in out
    (loss,) = delta_of(live, cand).losses
    assert (loss.sys_id, loss.cause, loss.detail) == (S1, vap.CAUSE_DEMOTED, "low_coverage")
    assert (loss.live_coverage_ppm, loss.candidate_coverage_ppm) == (900_000, 400_000)


def test_a_deletion_with_nothing_left_for_the_pair(live, tmp_path):
    cand = candidate(tmp_path, ids=without(LIVE_IDS, S1), evidence=without(LIVE_EVIDENCE, S1))
    assert gate(live, cand) == 1
    (loss,) = delta_of(live, cand).losses
    assert loss.cause == vap.CAUSE_NO_PAIR_EVIDENCE


def test_a_deletion_whose_page_now_ships_another_work(live, tmp_path):
    evidence = without(LIVE_EVIDENCE, S1) + [(S1, "p1", "shipped", W2)]
    ids = without(LIVE_IDS, S1) + [(S1, W2, 1, None, 900_000)]
    cand = candidate(tmp_path, ids=ids, evidence=evidence)
    assert gate(live, cand) == 1
    (loss,) = delta_of(live, cand).losses
    assert loss.cause == vap.CAUSE_PAGE_REATTRIBUTED


def test_another_work_on_a_different_page_is_not_a_same_page_reattribution(live, tmp_path):
    evidence = without(LIVE_EVIDENCE, S1) + [(S1, "p9", "shipped", W2)]
    cand = candidate(tmp_path, ids=without(LIVE_IDS, S1), evidence=evidence)
    (loss,) = delta_of(live, cand).losses
    assert loss.cause == vap.CAUSE_NO_PAIR_EVIDENCE


def test_a_deletion_whose_evidence_is_kept(live, tmp_path):
    evidence = without(LIVE_EVIDENCE, S1) + [(S1, "p1", "review_only", W1)]
    cand = candidate(tmp_path, ids=without(LIVE_IDS, S1), evidence=evidence)
    assert gate(live, cand) == 1
    (loss,) = delta_of(live, cand).losses
    assert loss.cause == vap.CAUSE_EVIDENCE_KEPT


def test_every_loss_gets_exactly_one_cause_from_the_closed_list(live, tmp_path):
    ids = [r for r in LIVE_IDS if r[0] not in (S1, S2)] + [(S2, W2, 0, "shared_wording", 1)]
    cand = candidate(tmp_path, ids=ids, evidence=without(LIVE_EVIDENCE, S1))
    losses = delta_of(live, cand).losses
    assert len(losses) == 2
    assert all(x.cause in vap.CAUSES for x in losses)


# ---------------------------------------------------------------------------
# Acceptance
# ---------------------------------------------------------------------------

@pytest.fixture
def lossy(live, tmp_path):
    ids = [r if r[0] != S1 else (S1, W1, 0, "low_coverage", 1) for r in LIVE_IDS]
    return candidate(tmp_path, ids=ids)


def ruled(live, cand, tmp_path, name="acceptance.json", **changes):
    acc = vap.draft_acceptance(delta_of(live, cand))
    acc.update(ruled_by="Test Owner", ruled_on="2026-10-20", ruling="fabricated test ruling")
    acc.update(changes)
    path = tmp_path / name
    path.write_text(json.dumps(acc), encoding="utf-8")
    return path


def test_a_ruled_acceptance_of_exactly_these_losses_passes(live, lossy, tmp_path, capsys):
    assert gate(live, lossy, "--acceptance", ruled(live, lossy, tmp_path)) == 0
    assert "PASS: all 1 losses accepted by Test Owner on 2026-10-20" in capsys.readouterr().out


def test_an_unruled_draft_fails(live, lossy, tmp_path, capsys):
    draft = tmp_path / "draft.json"
    assert gate(live, lossy, "--draft-acceptance", draft) == 1
    assert gate(live, lossy, "--acceptance", draft) == 1
    out = capsys.readouterr().out
    assert "acceptance has no ruled_by (a draft is not a ruling)" in out
    assert "acceptance ruled_on is not a YYYY-MM-DD date" in out


def test_an_acceptance_is_void_for_any_other_candidate_file(live, lossy, tmp_path, capsys):
    acc = ruled(live, lossy, tmp_path)
    # Same single loss, one unrelated gain: different bytes, so a different build.
    ids = [r if r[0] != S1 else (S1, W1, 0, "low_coverage", 1) for r in LIVE_IDS]
    rebuilt = candidate(tmp_path, ids=ids + [(S5, W2, 1, None, 1)], name="rebuilt.db")
    assert gate(live, rebuilt, "--acceptance", acc) == 1
    assert "different CANDIDATE artifact" in capsys.readouterr().out


def test_an_acceptance_is_void_for_any_other_live_file(live, lossy, tmp_path, capsys):
    acc = ruled(live, lossy, tmp_path)
    other_live = make_db(tmp_path / "other_live.db", LIVE_IDS + [(S5, W3, 0, "x", 1)],
                         LIVE_EVIDENCE)
    assert gate(other_live, lossy, "--acceptance", acc) == 1
    assert "different LIVE artifact" in capsys.readouterr().out


def test_an_acceptance_missing_a_loss_fails(live, tmp_path, capsys):
    ids = [r for r in LIVE_IDS if r[0] not in (S1, S2)]
    cand = candidate(tmp_path, ids=ids, evidence=without(LIVE_EVIDENCE, S1))
    acc = vap.draft_acceptance(delta_of(live, cand))
    acc.update(ruled_by="Test Owner", ruled_on="2026-10-20", ruling="r", lost=acc["lost"][:1])
    path = tmp_path / "short.json"
    path.write_text(json.dumps(acc), encoding="utf-8")
    assert gate(live, cand, "--acceptance", path) == 1
    assert "this candidate has 1 loss(es) the acceptance does not name" in capsys.readouterr().out


def test_an_acceptance_naming_a_loss_that_did_not_happen_fails(live, lossy, tmp_path, capsys):
    acc = vap.draft_acceptance(delta_of(live, lossy))
    acc.update(ruled_by="Test Owner", ruled_on="2026-10-20", ruling="r",
               lost=acc["lost"] + [[S2, W2, vap.CAUSE_DEMOTED]])
    path = tmp_path / "extra.json"
    path.write_text(json.dumps(acc), encoding="utf-8")
    assert gate(live, lossy, "--acceptance", path) == 1
    assert "acceptance names 1 loss(es) this candidate does not have" in capsys.readouterr().out


@pytest.mark.parametrize("bad", ["[]", "not json", json.dumps({"schema_version": "v0"})])
def test_a_malformed_acceptance_fails(live, lossy, tmp_path, bad):
    path = tmp_path / "bad.json"
    path.write_text(bad, encoding="utf-8")
    assert gate(live, lossy, "--acceptance", path) == 1


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------

def test_a_missing_artifact_fails_and_leaves_no_stub(live, tmp_path, capsys):
    missing = tmp_path / "missing.db"
    assert gate(live, missing) == 1
    assert "candidate artifact not found" in capsys.readouterr().out
    assert not missing.exists()


@pytest.mark.parametrize("kw,message", [
    ({"drop_table": "discovery_evidence"}, "has no table discovery_evidence"),
    ({"drop_column": ("discovery_identification", "main_pool")}, "lacks column(s) main_pool"),
    ({"audience": "private"}, "is not a public projection"),
])
def test_an_unusable_candidate_fails_closed(live, tmp_path, capsys, kw, message):
    assert gate(live, candidate(tmp_path, **kw)) == 1
    assert message in capsys.readouterr().out


def test_duplicate_identification_keys_fail_closed(live, tmp_path, capsys):
    cand = candidate(tmp_path, ids=LIVE_IDS + [LIVE_IDS[0]])
    assert gate(live, cand) == 1
    assert "1 duplicate (sys_id, canonical_work_id) key(s)" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Manifest mode (the web-box, pre-swap form)
# ---------------------------------------------------------------------------

def manifest_for(db_path, name, content_hash=None):
    db_path = Path(db_path)
    path = db_path.parent / name
    path.write_text(json.dumps({
        "asset_basename": db_path.stem,
        "content_hash": content_hash or vap.sha256_file(str(db_path)),
    }), encoding="utf-8")
    return path


def test_manifest_mode_resolves_the_exact_named_files(live, lossy, tmp_path):
    live_m = manifest_for(live, "manifest.json")
    assert gate("--live-manifest", live_m,
                "--candidate-manifest", manifest_for(live, "same.candidate")) == 0
    assert gate("--live-manifest", live_m,
                "--candidate-manifest", manifest_for(lossy, "lossy.candidate")) == 1


def test_manifest_mode_fails_when_the_bytes_are_not_the_manifests_file(live, lossy, tmp_path,
                                                                       capsys):
    bad = manifest_for(lossy, "bad.candidate", content_hash="0" * 64)
    assert gate("--live-manifest", manifest_for(live, "manifest.json"),
                "--candidate-manifest", bad) == 1
    assert "do not match the candidate manifest's content_hash" in capsys.readouterr().out


def test_manifest_mode_refuses_a_path_shaped_basename(live, tmp_path, capsys):
    bad = tmp_path / "traversal.candidate"
    bad.write_text(json.dumps({"asset_basename": "../live", "content_hash": "0" * 64}),
                   encoding="utf-8")
    assert gate("--live-manifest", manifest_for(live, "manifest.json"),
                "--candidate-manifest", bad) == 1
    assert "not a bare filename stem" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# CLI and outputs
# ---------------------------------------------------------------------------

def test_outputs_never_overwrite_an_existing_file(live, lossy, tmp_path):
    existing = tmp_path / "existing.json"
    existing.write_text("keep me", encoding="utf-8")
    assert gate(live, lossy, "--draft-acceptance", existing) == 2
    assert gate(live, lossy, "--report-html", existing) == 2
    assert existing.read_text(encoding="utf-8") == "keep me"


def test_mixing_paths_and_manifests_is_a_usage_error(live, lossy, tmp_path):
    with pytest.raises(SystemExit) as exc:
        gate(live, lossy, "--live-manifest", manifest_for(live, "manifest.json"))
    assert exc.value.code == 2


def test_the_report_lists_every_loss_escaped(live, tmp_path):
    ids = [r for r in LIVE_IDS if r[0] not in (S1, S2)] + [(S2, W2, 0, "shared_wording", 1)]
    cand = candidate(tmp_path, ids=ids, evidence=without(LIVE_EVIDENCE, S1))
    report = tmp_path / "report.html"
    assert gate(live, cand, "--report-html", report) == 1
    page = report.read_text(encoding="utf-8")
    assert page.count("<tr><td>") == 2
    assert "&lt;Title w000001&gt;" in page and "<Title" not in page


def test_the_script_runs_standalone_and_exits_nonzero_on_loss(live, lossy):
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "verify_append_preservation.py"), live, lossy],
        capture_output=True, text=True, cwd=REPO_ROOT,
        env={**os.environ, "PYTHONUTF8": "1"})
    assert proc.returncode == 1
    assert "FAIL: 1 live main-pool identification(s) lost" in proc.stdout
