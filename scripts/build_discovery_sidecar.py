#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline build for the Phase 134 Discovery Data Spine sidecar (`discovery.db`).

Implements the FROZEN two-table claim model from
`docs/specs/discovery-sidecar-schema-v1.md` (Phase 134, plan 134-01): the
`create_schema()` DDL (works, discovery_claim, discovery_evidence,
witness_units, witness_unit_members, meta, band_precision) plus a synthetic
fixture-generation mode (`synthetic_discovery_dataset` / `--golden PATH` /
`--smoke N`) from 134-03, PLUS (134-04) the REAL offline distillation --
shown-work selection + opaque work_id minting (`select_shown_works` /
`assign_opaque_work_ids`), the masked CANDIDATE review artifact + the NEW
fail-closed `--from-approved` reader (`emit_review_artifact` /
`load_approved_works`), the unified witness family across the
evidence_source axis (`build_claims_and_evidence`: track1_direct 4-disjoint-
source banding + propagated corroborated/weak) + the shared_text family +
the family-router collections, `build_witness_units` (DATA-10), and
`finalize_build` -- the full real-mode orchestration behind the BLOCKING
masking gate (`--from-approved`/`--crosswalk`/`--source-db`).

Masking note (synthetic path, 134-03): every identifier/title/span value
fabricated in `synthetic_discovery_dataset` is SYNTHETIC -- never derived
from real research data (mirrors `scripts/build_atlas_asset.py`'s
`synthetic_dataset`/`golden_dataset` convention). `source_corpus` values are
the masked codes {sefaria, ja, msource} only; work_ids are minted via
`scripts.discovery_ids.mint_work_id` (a plain zero-padded counter, never a
raw M:/J:/REF token). The REAL distillation path (134-04) mints opaque
work_ids the SAME way (never echoing a raw research work_id) and runs the
BLOCKING `check_atlas_masking.scan_sqlite` gate over the finalized `.db`
before it is ever considered buildable output (aborts + deletes on any hit).

Usage:
    python scripts/build_discovery_sidecar.py --golden tests/fixtures/discovery/discovery-v1-fixture.db
    python scripts/build_discovery_sidecar.py --smoke 10
    python scripts/build_discovery_sidecar.py <source_db_path> --from-approved <APPROVED.csv> \\
        --crosswalk <crosswalk.json> [--init-crosswalk] [--research-data-dir <DIR>] \\
        [--libraries-csv libraries.csv] [--fjms-db fist_data/fjms_enrichment.db] \\
        [--out discovery_data/discovery-v1.db] [--review-artifact discovery_data/candidates.csv]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import discovery_ids as ids  # scripts/discovery_ids.py -- FROZEN id/enum/routing primitives
import check_atlas_masking as _cam  # scripts/check_atlas_masking.py -- DATA-05 masking gate

SCHEMA_VERSION = "discovery-v1"
SIDECAR_VERSION = "discovery-v1-synthetic-fixture"

# Real-mode (134-04) distillation's own sidecar_version -- kept DISTINCT from the
# synthetic-fixture constant above so the two build paths can never be confused by
# a reader inspecting discovery_claim.sidecar_version.
REAL_SIDECAR_VERSION = "discovery-v1-real"

# Frozen constant timestamps (F13/determinism) -- NEVER wall-clock, so a
# rebuild in any environment reproduces byte-identical output.
FROZEN_BUILD_DATE = "2026-07-22T00:00:00Z"
FROZEN_DATA_AS_OF = "2026-07-21"
FROZEN_HTR_SNAPSHOT_HASH = hashlib.sha256(b"discovery-v1-synthetic-htr-corpus").hexdigest()

_RULE_VERSION = "discovery-v1-synthetic"


# ---------------------------------------------------------------------------
# 1. Schema DDL (docs/specs/discovery-sidecar-schema-v1.md SS1, verbatim)
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE works (
  work_id             TEXT PRIMARY KEY,
  canonical_work_id    TEXT NOT NULL,
  neutral_title        TEXT NOT NULL,
  author               TEXT,
  genre                TEXT,
  source_corpus        TEXT NOT NULL CHECK (source_corpus IN ('sefaria','ja','msource'))
);
CREATE INDEX ix_works_canonical ON works(canonical_work_id);

CREATE TABLE discovery_claim (
  page_id             TEXT NOT NULL,
  work_id             TEXT NOT NULL REFERENCES works(work_id),
  claim_id            TEXT NOT NULL UNIQUE,
  claim_type          TEXT NOT NULL CHECK (claim_type IN ('direct_witness','quotes_this_work','shared_text')),
  display_evidence_id TEXT NOT NULL,
  source_corpus       TEXT NOT NULL,
  sidecar_version     TEXT NOT NULL,
  PRIMARY KEY (page_id, work_id)
);
CREATE INDEX ix_discovery_claim_work_id ON discovery_claim(work_id);
CREATE INDEX ix_discovery_claim_page_id ON discovery_claim(page_id);

CREATE TABLE discovery_evidence (
  evidence_id       TEXT PRIMARY KEY,
  claim_id          TEXT NOT NULL REFERENCES discovery_claim(claim_id),
  evidence_kind     TEXT NOT NULL CHECK (evidence_kind IN ('witness','shared_text')),
  evidence_source   TEXT NOT NULL CHECK (evidence_source IN ('track1_direct','propagated')),
  confidence_band   TEXT NOT NULL,
  adjudication_status TEXT NOT NULL CHECK (adjudication_status IN ('human_confirmed','provisional','unreviewed')),
  audit_status      TEXT NOT NULL CHECK (audit_status IN ('audit_pending','audit_passed','n/a')),
  routing_status    TEXT NOT NULL CHECK (routing_status IN ('shipped','review_only')),
  routing_reason    TEXT NOT NULL CHECK (routing_reason IN ('impurity','runner_up_conflict','co_citation','none')),
  is_new            INTEGER NOT NULL DEFAULT 0,

  a_page_id         TEXT NOT NULL,
  sys_id            TEXT NOT NULL,

  tier              TEXT,
  aligned_len       INTEGER,
  occ_class         TEXT,
  cross_language    INTEGER,
  n_seed_ms         INTEGER,
  trials            INTEGER,
  runner_up         REAL,
  community         TEXT,
  ge3               INTEGER,
  rung              TEXT,
  router_bucket     TEXT,
  matched_letters   INTEGER,
  density           REAL,
  n_spans           INTEGER,

  span_start        INTEGER NOT NULL,
  span_end          INTEGER NOT NULL,
  text_layer        TEXT,
  snapshot_hash     TEXT,

  seed_spans        TEXT,
  seed_ms_ids       TEXT,

  other_page_id     TEXT,
  b_start           INTEGER,
  b_end             INTEGER,
  text_layer_b      TEXT,
  snapshot_hash_b   TEXT,

  rule_version      TEXT,
  community_id      TEXT,

  UNIQUE(claim_id, evidence_id)
);
CREATE INDEX ix_discovery_evidence_claim_id     ON discovery_evidence(claim_id);
CREATE INDEX ix_discovery_evidence_a_page_id    ON discovery_evidence(a_page_id);
CREATE INDEX ix_discovery_evidence_other_page_id ON discovery_evidence(other_page_id);

CREATE TABLE witness_units (
  unit_id  TEXT PRIMARY KEY
);
CREATE TABLE witness_unit_members (
  unit_id  TEXT NOT NULL REFERENCES witness_units(unit_id),
  sys_id   TEXT NOT NULL,
  merge_basis TEXT NOT NULL CHECK (merge_basis IN ('oxford_part','physical_join')),
  UNIQUE(sys_id)
);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE band_precision (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  scope           TEXT NOT NULL CHECK (scope IN ('collection','band')),
  collection_id   TEXT NOT NULL,
  evidence_source TEXT,
  confidence_band TEXT,
  numerator       INTEGER,
  denominator     INTEGER,
  precision       REAL,
  ci_low          REAL,
  ci_high         REAL,
  method          TEXT,
  sampling_frame  TEXT,
  ins_policy      TEXT,
  weighting       TEXT,
  notes           TEXT
);
"""


def create_schema(conn: sqlite3.Connection) -> None:
    """Emit the FROZEN DDL exactly (docs/specs/discovery-sidecar-schema-v1.md SS1)."""
    conn.executescript(_DDL)


# ---------------------------------------------------------------------------
# 2. Membership-based frame_content_hash (SS1.5/SS7 -- key_link: meta.frame_content_hash)
#
# Reused (imported, never duplicated) by scripts/verify_discovery_sidecar.py
# so build-time and verify-time recomputation can never drift apart.
# ---------------------------------------------------------------------------

def compute_frame_content_hash(conn: sqlite3.Connection) -> str:
    """Recompute the deduped per-claim + per-evidence membership hash.

    Deliberately NOT a raw-byte hash of the file -- it hashes the ordered
    (page_id, work_id, claim_type, display_evidence_id, evidence_id,
    evidence_kind, evidence_source, confidence_band) tuple set, so mutating
    one evidence row's band OR dropping a claim changes the digest, while
    build metadata (timestamps, manifest cruft) never affects it.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT dc.page_id, dc.work_id, dc.claim_type, dc.display_evidence_id,
               de.evidence_id, de.evidence_kind, de.evidence_source, de.confidence_band
        FROM discovery_claim dc
        JOIN discovery_evidence de ON de.claim_id = dc.claim_id
        ORDER BY dc.page_id, dc.work_id, de.evidence_id
        """
    )
    parts = [
        "|".join("" if v is None else str(v) for v in row)
        for row in cur.fetchall()
    ]
    key = "discovery_frame_v1\n" + "\n".join(parts)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 3. Synthetic dataset -- fabricated, masking-safe, NEVER real research data
# ---------------------------------------------------------------------------

_WITNESS = ids.EVIDENCE_KIND_WITNESS
_SHARED_TEXT = ids.EVIDENCE_KIND_SHARED_TEXT
_TRACK1 = ids.EVIDENCE_SOURCE_TRACK1_DIRECT
_PROPAGATED = ids.EVIDENCE_SOURCE_PROPAGATED

_EXPERT_VERIFIED = ids.CONFIDENCE_BAND_EXPERT_VERIFIED
_TIER_A = ids.CONFIDENCE_BAND_TIER_A
_SCREENING_RB = ids.CONFIDENCE_BAND_SCREENING_RB
_SCREENING_CANON = ids.CONFIDENCE_BAND_SCREENING_CANON
_CORROBORATED = ids.CONFIDENCE_BAND_CORROBORATED
_WEAK = ids.CONFIDENCE_BAND_WEAK
_NOT_EVALUATED = ids.CONFIDENCE_BAND_NOT_EVALUATED

_HUMAN_CONFIRMED = ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED
_PROVISIONAL = ids.ADJUDICATION_STATUS_PROVISIONAL
_UNREVIEWED = ids.ADJUDICATION_STATUS_UNREVIEWED

_AUDIT_PENDING = ids.AUDIT_STATUS_AUDIT_PENDING
_NA = ids.AUDIT_STATUS_NA

_SHIPPED = ids.ROUTING_STATUS_SHIPPED
_REVIEW_ONLY = ids.ROUTING_STATUS_REVIEW_ONLY

_NONE_REASON = ids.ROUTING_REASON_NONE
_CO_CITATION = ids.ROUTING_REASON_CO_CITATION

_OXFORD_PART = ids.MERGE_BASIS_OXFORD_PART
_PHYSICAL_JOIN = ids.MERGE_BASIS_PHYSICAL_JOIN

# Fabricated, neutral, masking-safe synthetic titles -- never a real research title.
_WORKS = [
    ("w000001", "sefaria", "Synthetic Neutral Title Alpha", "Synthetic Author A", "Synthetic Genre A"),
    ("w000002", "sefaria", "Synthetic Neutral Title Beta", "Synthetic Author B", "Synthetic Genre A"),
    ("w000003", "ja", "Synthetic Neutral Title Gamma", "Synthetic Author C", "Synthetic Genre B"),
    ("w000004", "ja", "Synthetic Neutral Title Delta", None, "Synthetic Genre B"),
    ("w000005", "msource", "Synthetic Neutral Title Epsilon", "Synthetic Author D", "Synthetic Genre C"),
    ("w000006", "msource", "Synthetic Neutral Title Zeta", "Synthetic Author E", "Synthetic Genre C"),
    ("w000007", "sefaria", "Synthetic Neutral Title Eta", None, None),
    ("w000008", "ja", "Synthetic Neutral Title Theta", "Synthetic Author F", "Synthetic Genre B"),
]


def _fake_hash(seed_text: str) -> str:
    return hashlib.sha256(f"discovery-v1-synthetic|{seed_text}".encode("utf-8")).hexdigest()


def _page_sys(n: int):
    return f"p{n:03d}", f"990000000000000{n:03d}"


def _mk_evidence(
    *, page_id, work_id, sys_id, evidence_kind, evidence_source, confidence_band,
    adjudication_status, audit_status, routing_status, routing_reason,
    span_start, span_end, text_layer="htr", snapshot_hash=None,
    is_new=0, other_page_id=None, b_start=None, b_end=None, text_layer_b=None,
    snapshot_hash_b=None, tier=None, aligned_len=None, occ_class=None,
    cross_language=None, n_seed_ms=None, trials=None, runner_up=None,
    community=None, ge3=None, rung=None, router_bucket=None,
    matched_letters=None, density=None, n_spans=None, seed_spans=None,
    seed_ms_ids=None, rule_version=_RULE_VERSION, community_id=None,
) -> Dict:
    if snapshot_hash is None:
        snapshot_hash = _fake_hash(f"{page_id}|{sys_id}|a")
    if evidence_kind == _SHARED_TEXT and other_page_id and snapshot_hash_b is None:
        snapshot_hash_b = _fake_hash(f"{other_page_id}|b")
    return {
        "page_id": page_id, "work_id": work_id, "sys_id": sys_id,
        "evidence_kind": evidence_kind, "evidence_source": evidence_source,
        "confidence_band": confidence_band, "adjudication_status": adjudication_status,
        "audit_status": audit_status, "routing_status": routing_status,
        "routing_reason": routing_reason, "is_new": is_new,
        "span_start": span_start, "span_end": span_end, "text_layer": text_layer,
        "snapshot_hash": snapshot_hash, "other_page_id": other_page_id,
        "b_start": b_start, "b_end": b_end, "text_layer_b": text_layer_b,
        "snapshot_hash_b": snapshot_hash_b, "tier": tier, "aligned_len": aligned_len,
        "occ_class": occ_class, "cross_language": cross_language, "n_seed_ms": n_seed_ms,
        "trials": trials, "runner_up": runner_up, "community": community, "ge3": ge3,
        "rung": rung, "router_bucket": router_bucket, "matched_letters": matched_letters,
        "density": density, "n_spans": n_spans, "seed_spans": seed_spans,
        "seed_ms_ids": seed_ms_ids, "rule_version": rule_version, "community_id": community_id,
    }


def synthetic_discovery_dataset():
    """Build the fixed, deterministic, fabricated evidence-row list + unit specs
    covering EVERY corrected-model case (docs SS4/SS5/SS6, 134-CONTEXT C-1..C-9).

    Returns (works, evidence_specs, unit_specs) -- see module docstring / the
    134-03-SUMMARY.md claim inventory for the full case-by-case mapping.
    """
    p001, s001 = _page_sys(1)
    p002, s002 = _page_sys(2)
    p003, s003 = _page_sys(3)
    p004, s004 = _page_sys(4)
    p005, s005 = _page_sys(5)
    p006, s006 = _page_sys(6)
    p007, s007 = _page_sys(7)
    p008, s008 = _page_sys(8)
    p009, s009 = _page_sys(9)
    p010, s010 = _page_sys(10)
    p011, s011 = _page_sys(11)
    p012, s012 = _page_sys(12)
    p013, s013 = _page_sys(13)
    p014, s014 = _page_sys(14)
    p015, s015 = _page_sys(15)
    p016, s016 = _page_sys(16)
    p017, s017 = _page_sys(17)
    p018, s018 = _page_sys(18)

    evidence_specs: List[Dict] = []

    # -- C1 (p001/w1): combo (c) -- corroborated vs UNREVIEWED expert_verified -> expert_verified wins
    evidence_specs.append(_mk_evidence(
        page_id=p001, work_id="w000001", sys_id=s001,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_EXPERT_VERIFIED,
        adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=10, span_end=60,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p001, work_id="w000001", sys_id=s001,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_CORROBORATED,
        adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=15, span_end=55, trials=2, is_new=1,
        seed_spans=[{"occ0": 15, "occ1": 55, "occ_class": "core",
                     "seed_page_ids": ["p901"], "seed_sys_ids": ["990000000000000901"]}],
        seed_ms_ids=["p901"],
    ))

    # -- C2 (p002/w2): combo (b) -- tier_a vs corroborated -> tier_a wins; MULTI-seed R4 row
    evidence_specs.append(_mk_evidence(
        page_id=p002, work_id="w000002", sys_id=s002,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_TIER_A,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=100, span_end=400, matched_letters=300, density=0.92, n_spans=1,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p002, work_id="w000002", sys_id=s002,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_CORROBORATED,
        adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=100, span_end=250, trials=3, is_new=1,
        seed_spans=[
            {"occ0": 100, "occ1": 250, "occ_class": "core",
             "seed_page_ids": ["p910", "p911"], "seed_sys_ids": ["990000000000000910", "990000000000000911"]},
            {"occ0": 10, "occ1": 60, "occ_class": "flank",
             "seed_page_ids": ["p912"], "seed_sys_ids": ["990000000000000912"]},
        ],
        seed_ms_ids=["p910", "p911", "p912"],
    ))

    # -- C3 (p003/w1): combo (a) -- corroborated vs screening_rb -> corroborated wins
    evidence_specs.append(_mk_evidence(
        page_id=p003, work_id="w000001", sys_id=s003,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_SCREENING_RB,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=5, span_end=45,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p003, work_id="w000001", sys_id=s003,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_CORROBORATED,
        adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=5, span_end=40, trials=2, is_new=1,
        seed_spans=[{"occ0": 5, "occ1": 40, "occ_class": "core",
                     "seed_page_ids": ["p920"], "seed_sys_ids": ["990000000000000920"]}],
        seed_ms_ids=["p920"],
    ))

    # -- C4 (p004/w3): F7 witness+shared_text COLLISION; combo (d) not_evaluated never chosen
    evidence_specs.append(_mk_evidence(
        page_id=p004, work_id="w000003", sys_id=s004,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_WEAK,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=20, span_end=50, rung="A", is_new=1,
    ))
    p004b, _ = _page_sys(104)
    evidence_specs.append(_mk_evidence(
        page_id=p004, work_id="w000003", sys_id=s004,
        evidence_kind=_SHARED_TEXT, evidence_source=_PROPAGATED, confidence_band=_NOT_EVALUATED,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=20, span_end=50, tier="T2", aligned_len=150, occ_class="core",
        n_seed_ms=1, cross_language=0, is_new=0, other_page_id=p004b,
    ))

    # -- C5 (p005/w4): human_confirmed screening-band dominance (totality case)
    evidence_specs.append(_mk_evidence(
        page_id=p005, work_id="w000004", sys_id=s005,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_SCREENING_CANON,
        adjudication_status=_HUMAN_CONFIRMED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=8, span_end=48,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p005, work_id="w000004", sys_id=s005,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_CORROBORATED,
        adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=8, span_end=40, trials=2, is_new=1,
        seed_spans=[{"occ0": 8, "occ1": 40, "occ_class": "core",
                     "seed_page_ids": ["p930"], "seed_sys_ids": ["990000000000000930"]}],
        seed_ms_ids=["p930"],
    ))

    # -- C6 (p006/w2): individually-adjudicated expert_verified (R6, single row)
    evidence_specs.append(_mk_evidence(
        page_id=p006, work_id="w000002", sys_id=s006,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_EXPERT_VERIFIED,
        adjudication_status=_HUMAN_CONFIRMED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=12, span_end=72,
    ))

    # -- C7 (p007/w5): plain screening_canon (D-10 canon caveat)
    evidence_specs.append(_mk_evidence(
        page_id=p007, work_id="w000005", sys_id=s007,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_SCREENING_CANON,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=3, span_end=33,
    ))

    # -- C8 (p008/w6): plain weak
    evidence_specs.append(_mk_evidence(
        page_id=p008, work_id="w000006", sys_id=s008,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_WEAK,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=6, span_end=26, rung="B2", is_new=1,
    ))

    # -- C9 (p009/w7): plain corroborated
    evidence_specs.append(_mk_evidence(
        page_id=p009, work_id="w000007", sys_id=s009,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_CORROBORATED,
        adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=9, span_end=59, trials=2, is_new=1,
        seed_spans=[{"occ0": 9, "occ1": 59, "occ_class": "core",
                     "seed_page_ids": ["p940"], "seed_sys_ids": ["990000000000000940"]}],
        seed_ms_ids=["p940"],
    ))

    # -- C10 (p010/w8): family-router row (R3, NON-witness bucket)
    p010b, _ = _page_sys(110)
    evidence_specs.append(_mk_evidence(
        page_id=p010, work_id="w000008", sys_id=s010,
        evidence_kind=_SHARED_TEXT, evidence_source=_PROPAGATED, confidence_band=_NOT_EVALUATED,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_REVIEW_ONLY, routing_reason=_CO_CITATION,
        span_start=4, span_end=44, router_bucket="tafsir_targum",
        other_page_id=p010b, is_new=0,
    ))

    # -- C11 (p011/w1): plain shared_text (non-router)
    p011b, _ = _page_sys(111)
    evidence_specs.append(_mk_evidence(
        page_id=p011, work_id="w000001", sys_id=s011,
        evidence_kind=_SHARED_TEXT, evidence_source=_PROPAGATED, confidence_band=_NOT_EVALUATED,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=0, span_end=300, tier="T1", aligned_len=300, occ_class="core",
        n_seed_ms=2, cross_language=0, is_new=1, other_page_id=p011b,
    ))

    # -- C12a/C12b (p012, SAME page/sys, TWO works): multi-work-per-MS + claim_type dominance
    evidence_specs.append(_mk_evidence(
        page_id=p012, work_id="w000003", sys_id=s012,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_TIER_A,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=10, span_end=500, matched_letters=490, density=0.88, n_spans=2,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p012, work_id="w000004", sys_id=s012,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_SCREENING_RB,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=520, span_end=560,
    ))

    # -- C13/C14 (p013/p014, w5): DATA-10 oxford_part unit projection
    evidence_specs.append(_mk_evidence(
        page_id=p013, work_id="w000005", sys_id=s013,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_TIER_A,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=1, span_end=31, matched_letters=30, density=0.70, n_spans=1,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p014, work_id="w000005", sys_id=s014,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_TIER_A,
        adjudication_status=_UNREVIEWED, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=1, span_end=41, matched_letters=40, density=0.75, n_spans=1,
    ))

    # -- C15/C16 (p015/p016, w6): DATA-10 physical_join unit projection
    evidence_specs.append(_mk_evidence(
        page_id=p015, work_id="w000006", sys_id=s015,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_SCREENING_RB,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=2, span_end=22,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p016, work_id="w000006", sys_id=s016,
        evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_SCREENING_RB,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=2, span_end=27,
    ))

    # -- C17/C18 (p017/p018): a "same scribe" pair -- DATA-10 requires these NEVER merge
    evidence_specs.append(_mk_evidence(
        page_id=p017, work_id="w000007", sys_id=s017,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_WEAK,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=3, span_end=13, rung="A", is_new=1,
    ))
    evidence_specs.append(_mk_evidence(
        page_id=p018, work_id="w000008", sys_id=s018,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_WEAK,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=4, span_end=14, rung="B2", is_new=1,
    ))

    unit_specs = [
        {"members": [s013, s014], "merge_basis": _OXFORD_PART},
        {"members": [s015, s016], "merge_basis": _PHYSICAL_JOIN},
        # s017/s018 (the "same scribe" pair) are DELIBERATELY absent from any
        # unit -- DATA-10 forbids merge_basis='scribe' entirely.
    ]

    return list(_WORKS), evidence_specs, unit_specs


# ---------------------------------------------------------------------------
# 4. band_precision population (C-7/G8) -- BEFORE final hashing (F13)
# ---------------------------------------------------------------------------

def _band_precision_rows() -> List[Dict]:
    """SYNTHETIC-mode-ONLY band_precision rows (H3) -- used exclusively by
    `populate_synthetic` (and therefore the pinned 134-03 golden fixture,
    which MUST stay byte-identical). Carries a placeholder `tier_a`
    precision (0.90) that is fabricated and must NEVER reach a real/release
    build -- `finalize_build`'s real-mode path uses
    `_frozen_real_band_precision_rows` instead (tier_a precision NULL, per
    the FROZEN band_precision contract, docs/specs/discovery-sidecar-schema-v1.md
    SS1.6: only expert_verified/screening_rb/screening_canon carry a
    measured track1_direct precision; tier_a does not)."""
    rows = [
        {
            "scope": "collection", "collection_id": "propagated_witness_collection_v1",
            "evidence_source": None, "confidence_band": None,
            "numerator": 176, "denominator": 190, "precision": 0.926,
            "ci_low": 0.875, "ci_high": 0.968, "method": "work-cluster bootstrap",
            "sampling_frame": "held-out 200-card draw (90 corroborated + 110 weak); "
                               "frame_size 2109 after ledger+neighborhood+shelf exclusion",
            "ins_policy": "locked-rule evaluation", "weighting": "unweighted",
            "notes": "C-7/R1 frozen collection-level precision over corroborated UNION weak; "
                     "NEVER a corroborated-only (81/86) or weak-only (95/104) split.",
        },
        {
            "scope": "band", "collection_id": "propagated_witness_collection_v1",
            "evidence_source": _PROPAGATED, "confidence_band": _CORROBORATED,
            "numerator": None, "denominator": None, "precision": None,
            "ci_low": None, "ci_high": None, "method": "locked-rule evaluation",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "No valid band-specific measurement exists (G8) -- see the "
                     "scope='collection' row for the single measured collection-level number.",
        },
        {
            "scope": "band", "collection_id": "propagated_witness_collection_v1",
            "evidence_source": _PROPAGATED, "confidence_band": _WEAK,
            "numerator": None, "denominator": None, "precision": None,
            "ci_low": None, "ci_high": None, "method": "locked-rule evaluation",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "No valid band-specific measurement exists (G8) -- see the "
                     "scope='collection' row for the single measured collection-level number.",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _EXPERT_VERIFIED,
            "numerator": None, "denominator": None, "precision": 0.889,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-A figure; synthetic placeholder in 134-03, finalized in 134-07.",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _TIER_A,
            "numerator": None, "denominator": None, "precision": 0.90,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "Synthetic placeholder in 134-03, finalized in 134-07.",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _SCREENING_RB,
            "numerator": None, "denominator": None, "precision": 0.859,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-B figure; synthetic placeholder in 134-03, finalized in 134-07.",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _SCREENING_CANON,
            "numerator": None, "denominator": None, "precision": 0.647,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-CANON figure; D-10 canon caveat (known Targum-confusion class).",
        },
    ]
    return rows


def _frozen_real_band_precision_rows() -> List[Dict]:
    """The FROZEN real/release-mode band_precision default spec (H3, C-7/G8,
    docs/specs/discovery-sidecar-schema-v1.md SS1.6). Used by
    `finalize_build`'s real-mode path whenever the caller does not supply a
    custom `--precision-spec` -- unlike `_band_precision_rows` (the
    SYNTHETIC-mode-only fixture rows), `tier_a` here carries NULL precision:
    there is NO measured tier_a interval in the frozen contract, so a real
    build must never write the 0.90 synthetic placeholder. The three
    MEASURED track1_direct bands (expert_verified/screening_rb/
    screening_canon) and the collection-level 0.926 are the frozen contract
    values documented in the schema doc; the owner may override this whole
    spec at 134-07 via an explicit `--precision-spec <json>` file."""
    rows = [
        {
            "scope": "collection", "collection_id": "propagated_witness_collection_v1",
            "evidence_source": None, "confidence_band": None,
            "numerator": 176, "denominator": 190, "precision": 0.926,
            "ci_low": 0.875, "ci_high": 0.968, "method": "work-cluster bootstrap",
            "sampling_frame": "held-out 200-card draw (90 corroborated + 110 weak); "
                               "frame_size 2109 after ledger+neighborhood+shelf exclusion",
            "ins_policy": "locked-rule evaluation", "weighting": "unweighted",
            "notes": "C-7/R1 frozen collection-level precision over corroborated UNION weak; "
                     "NEVER a corroborated-only (81/86) or weak-only (95/104) split.",
        },
        {
            "scope": "band", "collection_id": "propagated_witness_collection_v1",
            "evidence_source": _PROPAGATED, "confidence_band": _CORROBORATED,
            "numerator": None, "denominator": None, "precision": None,
            "ci_low": None, "ci_high": None, "method": "locked-rule evaluation",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "No valid band-specific measurement exists (G8) -- see the "
                     "scope='collection' row for the single measured collection-level number.",
        },
        {
            "scope": "band", "collection_id": "propagated_witness_collection_v1",
            "evidence_source": _PROPAGATED, "confidence_band": _WEAK,
            "numerator": None, "denominator": None, "precision": None,
            "ci_low": None, "ci_high": None, "method": "locked-rule evaluation",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "No valid band-specific measurement exists (G8) -- see the "
                     "scope='collection' row for the single measured collection-level number.",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _EXPERT_VERIFIED,
            "numerator": None, "denominator": None, "precision": 0.889,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-A figure (frozen contract, docs/specs SS1.6/SS4.1).",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _TIER_A,
            "numerator": None, "denominator": None, "precision": None,
            "ci_low": None, "ci_high": None, "method": None,
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "H3: tier_a carries NO measured precision in the frozen contract -- "
                     "NEVER a fabricated number in a real/release build (unlike the "
                     "SYNTHETIC-fixture-only 0.90 placeholder in _band_precision_rows).",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _SCREENING_RB,
            "numerator": None, "denominator": None, "precision": 0.859,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-B figure (frozen contract, docs/specs SS1.6/SS4.1).",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _SCREENING_CANON,
            "numerator": None, "denominator": None, "precision": 0.647,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-CANON figure (frozen contract, docs/specs SS1.6/SS4.1); "
                     "D-10 canon caveat (known Targum-confusion class).",
        },
    ]
    return rows


# ---------------------------------------------------------------------------
# 5. populate_synthetic -- the full insert pipeline
# ---------------------------------------------------------------------------

def populate_synthetic(conn: sqlite3.Connection, source_db_hash: str) -> Dict:
    works, evidence_specs, unit_specs = synthetic_discovery_dataset()
    work_corpus = {w[0]: w[1] for w in works}

    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [(w[0], ids.canonical_work_id(w[0]), w[2], w[3], w[4], w[1]) for w in works],
    )

    # Group witness evidence by page_id to compute the largest-span-dominates
    # claim_type per row (docs SS3) -- keyed on ALL witness rows sharing a
    # page, regardless of which work they belong to.
    spans_by_page: Dict[str, List[int]] = {}
    for e in evidence_specs:
        if e["evidence_kind"] == _WITNESS:
            spans_by_page.setdefault(e["page_id"], []).append(e["span_end"] - e["span_start"])
    for e in evidence_specs:
        if e["evidence_kind"] == _WITNESS:
            this_len = e["span_end"] - e["span_start"]
            e["_row_claim_type"] = ids.claim_type_for_work_witness(spans_by_page[e["page_id"]], this_len)

    # Group evidence specs into claims keyed on (page_id, work_id).
    claims: Dict[tuple, List[Dict]] = {}
    for e in evidence_specs:
        claims.setdefault((e["page_id"], e["work_id"]), []).append(e)

    # Insert discovery_claim rows with a placeholder display_evidence_id --
    # backfilled in a second pass once evidence rows exist (SS1.3 circular-FK note).
    claim_rows = []
    for (page_id, work_id), rows in claims.items():
        claim_id = ids.claim_id(page_id, work_id)
        resolver_input = [
            {"evidence_kind": r["evidence_kind"], "claim_type": r.get("_row_claim_type")}
            for r in rows
        ]
        claim_type = ids.resolve_claim_type(resolver_input)
        claim_rows.append((
            page_id, work_id, claim_id, claim_type, "", work_corpus[work_id], SIDECAR_VERSION,
        ))
    cur.executemany(
        "INSERT INTO discovery_claim "
        "(page_id, work_id, claim_id, claim_type, display_evidence_id, source_corpus, sidecar_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        claim_rows,
    )

    claim_id_by_key = {(p, w): ids.claim_id(p, w) for (p, w) in claims}

    evidence_rows = []
    for (page_id, work_id), rows in claims.items():
        claim_id = claim_id_by_key[(page_id, work_id)]
        for e in rows:
            evidence_id = ids.evidence_id(
                work_id=e["work_id"], a_page_id=e["page_id"], sys_id=e["sys_id"],
                evidence_kind=e["evidence_kind"], evidence_source=e["evidence_source"],
                confidence_band=e["confidence_band"], span_start=e["span_start"],
                span_end=e["span_end"], other_page_id=e.get("other_page_id"),
                seed_spans=e.get("seed_spans"),
            )
            e["_evidence_id"] = evidence_id
            evidence_rows.append((
                evidence_id, claim_id, e["evidence_kind"], e["evidence_source"], e["confidence_band"],
                e["adjudication_status"], e["audit_status"], e["routing_status"], e["routing_reason"],
                int(e["is_new"]), e["page_id"], e["sys_id"],
                e.get("tier"), e.get("aligned_len"), e.get("occ_class"), e.get("cross_language"),
                e.get("n_seed_ms"), e.get("trials"), e.get("runner_up"), e.get("community"),
                e.get("ge3"), e.get("rung"), e.get("router_bucket"), e.get("matched_letters"),
                e.get("density"), e.get("n_spans"),
                e["span_start"], e["span_end"], e.get("text_layer"), e.get("snapshot_hash"),
                json.dumps(e["seed_spans"]) if e.get("seed_spans") else None,
                json.dumps(e["seed_ms_ids"]) if e.get("seed_ms_ids") else None,
                e.get("other_page_id"), e.get("b_start"), e.get("b_end"),
                e.get("text_layer_b"), e.get("snapshot_hash_b"),
                e.get("rule_version"), e.get("community_id"),
            ))
    cur.executemany(
        """
        INSERT INTO discovery_evidence (
            evidence_id, claim_id, evidence_kind, evidence_source, confidence_band,
            adjudication_status, audit_status, routing_status, routing_reason,
            is_new, a_page_id, sys_id,
            tier, aligned_len, occ_class, cross_language, n_seed_ms, trials, runner_up,
            community, ge3, rung, router_bucket, matched_letters, density, n_spans,
            span_start, span_end, text_layer, snapshot_hash,
            seed_spans, seed_ms_ids,
            other_page_id, b_start, b_end, text_layer_b, snapshot_hash_b,
            rule_version, community_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        evidence_rows,
    )

    # Backfill display_evidence_id via the frozen TOTAL selector (SS6).
    display_choices: Dict[str, str] = {}
    for (page_id, work_id), rows in claims.items():
        claim_id = claim_id_by_key[(page_id, work_id)]
        selector_rows = [
            {"evidence_id": e["_evidence_id"], "evidence_source": e["evidence_source"],
             "confidence_band": e["confidence_band"], "adjudication_status": e["adjudication_status"]}
            for e in rows
        ]
        winner = ids.select_display_evidence(selector_rows)
        display_choices[claim_id] = winner
        cur.execute(
            "UPDATE discovery_claim SET display_evidence_id = ? WHERE claim_id = ?",
            (winner, claim_id),
        )

    # witness_units / witness_unit_members (DATA-10 projection).
    unit_rows = []
    member_rows = []
    for spec in unit_specs:
        unit_id = ids.unit_id(spec["members"])
        unit_rows.append((unit_id,))
        for sys_id in spec["members"]:
            member_rows.append((unit_id, sys_id, spec["merge_basis"]))
    cur.executemany("INSERT INTO witness_units (unit_id) VALUES (?)", unit_rows)
    cur.executemany(
        "INSERT INTO witness_unit_members (unit_id, sys_id, merge_basis) VALUES (?, ?, ?)",
        member_rows,
    )

    # band_precision (F13 -- populated BEFORE final hashing).
    bp_rows = _band_precision_rows()
    cur.executemany(
        """
        INSERT INTO band_precision (
            scope, collection_id, evidence_source, confidence_band, numerator, denominator,
            precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes
        ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
                   :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
                   :ins_policy, :weighting, :notes)
        """,
        bp_rows,
    )

    (n_works,) = cur.execute("SELECT COUNT(*) FROM works").fetchone()
    (n_claims,) = cur.execute("SELECT COUNT(*) FROM discovery_claim").fetchone()
    (n_evidence,) = cur.execute("SELECT COUNT(*) FROM discovery_evidence").fetchone()
    (n_units,) = cur.execute("SELECT COUNT(*) FROM witness_units").fetchone()

    frame_content_hash = compute_frame_content_hash(conn)

    meta_rows = [
        ("schema_version", SCHEMA_VERSION),
        ("sidecar_version", SIDECAR_VERSION),
        ("source_db_sha256", source_db_hash),
        ("build_date", FROZEN_BUILD_DATE),
        ("data_as_of", FROZEN_DATA_AS_OF),
        ("htr_snapshot_hash", FROZEN_HTR_SNAPSHOT_HASH),
        ("expected_rows_claims", str(n_claims)),
        ("expected_rows_evidence", str(n_evidence)),
        ("expected_rows_works", str(n_works)),
        ("expected_rows_units", str(n_units)),
        ("frame_content_hash", frame_content_hash),
    ]
    cur.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)

    return {
        "row_counts": {
            "works": n_works, "discovery_claim": n_claims,
            "discovery_evidence": n_evidence, "witness_units": n_units,
        },
        "frame_content_hash": frame_content_hash,
        "display_evidence_choices": display_choices,
        "band_precision_rows": len(bp_rows),
    }


# ---------------------------------------------------------------------------
# 6. REAL-MODE DISTILLATION (Phase 134, plan 134-04)
#
# Consumes the gitignored research DB (`fullcorpus_v2.db`) + the Q2/E1
# collections + `libraries.csv` + `fjms_enrichment.db` to build the masked
# `discovery.db`. Deliberately INDEPENDENT of `populate_synthetic` above (a
# small amount of INSERT-statement duplication is accepted here) so the
# pinned 134-03 golden fixture can never drift as a side effect of a change
# made in this section.
# ---------------------------------------------------------------------------

class CrosswalkAbortError(RuntimeError):
    """Raised when a raw->opaque work_id crosswalk is REQUIRED but absent.

    DC2: an absent crosswalk must never be silently treated as "first build" --
    that would re-mint fresh opaque ids for works that may already have shipped
    under a prior opaque id, breaking id stability across rebuilds. Callers
    that really mean "this is the very first build" must say so explicitly via
    `assign_opaque_work_ids(..., create_if_missing=True)` (CLI: --init-crosswalk).
    """


class MaskingGateFailure(RuntimeError):
    """Raised when the BLOCKING masking scan (DC13) finds ANY hit in the
    finalized `.db`. `finalize_build` deletes the offending file before
    raising -- a half-finalized leaking artifact must never linger on disk."""


class CrosswalkValidationError(RuntimeError):
    """Raised (M1) when a PERSISTED raw->opaque work_id crosswalk value fails
    format or 1:1-uniqueness validation. Defense-in-depth: a corrupted or
    hand-edited crosswalk file could otherwise place a raw identifier or a
    filename stem straight into an emitted surface (the review artifact's
    `work_id` column, or `works.work_id` in the shipped sidecar) -- this
    aborts BEFORE any review-artifact or sidecar emission."""


class EvidenceIdCollisionError(RuntimeError):
    """Raised (L2) when two evidence rows collide on the SAME `evidence_id`
    at EQUAL routing priority (both shipped, or both review_only) but carry
    SEMANTICALLY DIFFERENT content. The FROZEN `evidence_id` recipe has no
    "which source collection" discriminator by design (see the
    shipped-over-review_only preference above), so a genuine equal-priority
    collision with differing content would otherwise be resolved by
    first-seen-in-`evidence_specs` order -- an accident of ingestion order,
    not a deterministic build property. An IDENTICAL-content collision (a
    true duplicate row, e.g. a repeated JSONL line) is harmless and is
    deduped silently without raising; only a content-DIVERGENT equal-priority
    collision raises here, fail-closed, so the build never silently discards
    real evidence based on input order."""


# ---------------------------------------------------------------------------
# 6.1 Shown-work selection (D-05/D-06) + cat/genre masking policy (Landmine 2)
# ---------------------------------------------------------------------------

# Raw research `cat` values that are the ALREADY-OPEN reference corpora
# (auto-adopt, D-05) -- NEVER the masked corpus's own raw name (Landmine 2):
# any `cat` value that is not one of these, and not the JA corpus, is treated
# as the SINGLE masked M-source bucket by construction (an else-branch), so
# this module never needs to name that corpus at all.
_OPEN_CORPUS_CATS_SEFARIA = frozenset({
    "Sefaria", "Bible", "Bavli", "Mishnah", "Tosefta", "Yerushalmi", "Targum", "Liturgy",
})
_OPEN_CORPUS_CAT_JA = "JA"


def _map_cat_to_source_corpus(cat: Optional[str]) -> Optional[str]:
    """Map a raw research `cat` value to a masked `source_corpus` code.

    `cat` in the open-corpus set -> 'sefaria'; `cat == 'JA'` -> 'ja'; any
    OTHER non-empty `cat` -> 'msource' (the masked bucket, reached purely by
    elimination -- the corpus's own raw name is never compared against or
    written here, Landmine 2). Empty/None `cat` -> None (excluded, unmapped).
    """
    if not cat:
        return None
    if cat == _OPEN_CORPUS_CAT_JA:
        return ids.SOURCE_CORPUS_JA
    if cat in _OPEN_CORPUS_CATS_SEFARIA:
        return ids.SOURCE_CORPUS_SEFARIA
    return ids.SOURCE_CORPUS_MSOURCE


# D-06 exclude-by-genre curation policy over the masked bucket's OWN `genre`
# column values (RESEARCH.md "Genre signal", verified against the research
# corpus): keeps the Geonic / Talmud&Midrash / Karaite / rabbinic /
# belles-lettres / science / philology / Arabic-translation classes as
# literary CANDIDATES (owner is the final gate, D-06 -- this only produces
# the pre-owner-review candidate set); drops piyyut / documentary /
# modern-other / unrecognized classes. These are standard Hebrew
# bibliographic genre-taxonomy labels (not a corpus name or siglum).
_GENRE_CLASS_LITERARY_KEEP = frozenset({
    "ספרות הגאונים",                     # Geonic literature
    "תלמוד ומדרש",                        # Talmud and Midrash
    "ספרות הקראים",                      # Karaite literature
    "ספרות רבנית",                        # rabbinic literature
    "ספרות יפה",                          # belles-lettres
    "ספרות מדע",                          # science literature
    "בלשנות׃ מסורה, דקדוק ומילונות",       # philology (masorah/grammar/lexicography)
    "ספרות התרגומים מערבית",              # translations from Arabic
})


def _is_literary_genre(genre: Optional[str]) -> bool:
    """True iff `genre` is in the D-06 literary-keep class; False (fail-closed
    conservative default) for piyyut/documentary/modern-other/unknown/empty."""
    return bool(genre) and genre in _GENRE_CLASS_LITERARY_KEEP


def select_shown_works(conn: sqlite3.Connection) -> List[Dict]:
    """Curation POLICY producing shown-work CANDIDATES (D-05/D-06) -- NOT the
    final shipped set (the owner is the final gate via the review artifact,
    134-07). Auto-adopts ALL open-corpus (Sefaria/JA, incl the canonical
    strata cat values) works; selects the M-source large-literary subset via
    the exclude-by-genre policy. Reads ONE representative row per raw
    work_id (first by page_id, for determinism) from `track1_matches WHERE
    shadowed_by IS NULL` (Landmine 9) -- the OQ2-frozen shown-work source
    (reference-catalogue identification, never unsupervised clustering).

    Returns a list of dicts: {raw_work_id, cat, genre, author, title,
    source_corpus} -- sorted by raw_work_id for deterministic downstream
    opaque-id assignment ordering.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT work_id, cat, genre, author, title, page_id "
        "FROM track1_matches WHERE shadowed_by IS NULL "
        "ORDER BY work_id, page_id"
    )
    seen: Dict[str, Dict] = {}
    for work_id, cat, genre, author, title, _page_id in cur.fetchall():
        if work_id in seen:
            continue
        seen[work_id] = {
            "raw_work_id": work_id, "cat": cat, "genre": genre,
            "author": author, "title": title,
        }

    candidates = []
    for raw_work_id in sorted(seen):
        info = seen[raw_work_id]
        source_corpus = _map_cat_to_source_corpus(info["cat"])
        if source_corpus is None:
            continue
        if source_corpus == ids.SOURCE_CORPUS_MSOURCE and not _is_literary_genre(info["genre"]):
            continue
        candidates.append({**info, "source_corpus": source_corpus})
    return candidates


# ---------------------------------------------------------------------------
# 6.2 Opaque work_id minting (crosswalk-anchored, F16/DC2)
# ---------------------------------------------------------------------------

# Mirrors (never imports/edits) the FROZEN opaque work_id shape minted by
# `scripts/discovery_ids.py::mint_work_id` -- a plain "w" + a zero-padded
# 6-digit counter (e.g. "w000001"). Used ONLY as a defensive re-validation
# pattern for PERSISTED crosswalk values (M1); the frozen recipe module
# itself stays untouched.
_OPAQUE_WORK_ID_PATTERN = re.compile(r"^w\d{6}$")


def _validate_crosswalk(crosswalk: Dict[str, str]) -> None:
    """Defense-in-depth (M1): validate every crosswalk value matches the
    frozen opaque work_id format AND that the mapping is 1:1 (no two raw
    work_ids sharing the same opaque id). Raises `CrosswalkValidationError`
    on ANY malformed or duplicated value -- a corrupted/hand-edited
    crosswalk file could otherwise place a raw identifier or filename stem
    straight into an emitted surface (the review artifact's `work_id`
    column, or `works.work_id` in the shipped sidecar); this must run
    BEFORE any review-artifact or sidecar emission."""
    malformed = sorted(
        raw_id for raw_id, opaque in crosswalk.items()
        if not (isinstance(opaque, str) and _OPAQUE_WORK_ID_PATTERN.match(opaque))
    )
    if malformed:
        raise CrosswalkValidationError(
            f"crosswalk contains {len(malformed)} value(s) not matching the frozen "
            f"opaque work_id format (^w\\d{{6}}$) for raw_work_id(s): {malformed[:5]} "
            "(M1 -- refusing to emit any review artifact/sidecar with a "
            "potentially-raw crosswalk value)"
        )
    seen_opaque: Dict[str, str] = {}
    duplicates: List[str] = []
    for raw_id, opaque in sorted(crosswalk.items()):
        prior_raw = seen_opaque.get(opaque)
        if prior_raw is None:
            seen_opaque[opaque] = raw_id
        elif prior_raw != raw_id:
            duplicates.append(f"{opaque} <- {prior_raw!r} AND {raw_id!r}")
    if duplicates:
        raise CrosswalkValidationError(
            f"crosswalk is not 1:1 -- {len(duplicates)} opaque work_id(s) are shared "
            f"by multiple raw work_ids (M1): {duplicates[:5]}"
        )


def assign_opaque_work_ids(
    candidates: List[Dict], crosswalk_path, *, create_if_missing: bool = False
) -> List[Dict]:
    """Mint stable opaque work_ids 1:1 per raw research work_id, anchored on a
    PERSISTED raw->opaque crosswalk file (gitignored, dev-box) so the SAME raw
    work keeps the SAME opaque id across rebuilds (F16). Mutates + returns
    `candidates` with a `work_id` key added; persists the updated crosswalk
    back to `crosswalk_path`.

    If the crosswalk file is absent and `create_if_missing` is False (the
    default), raises `CrosswalkAbortError` -- an absent crosswalk NEVER
    silently re-mints (DC2). Every crosswalk value (on load AND again before
    persisting) is validated via `_validate_crosswalk` (M1) -- a malformed or
    non-1:1 value aborts BEFORE any candidate/work_id is ever assigned or
    written out.
    """
    crosswalk_file = Path(crosswalk_path)
    if crosswalk_file.exists():
        crosswalk: Dict[str, str] = json.loads(crosswalk_file.read_text(encoding="utf-8"))
        _validate_crosswalk(crosswalk)  # M1: re-validate the PERSISTED file on load
    elif create_if_missing:
        crosswalk = {}
    else:
        raise CrosswalkAbortError(
            f"crosswalk file not found: {crosswalk_path} -- refusing to mint fresh opaque "
            "work_ids without an explicit create_if_missing=True / --init-crosswalk "
            "(DC2: an absent-but-required crosswalk aborts, never silently re-mints)"
        )

    counter = 0
    for opaque in crosswalk.values():
        counter = max(counter, int(opaque[1:]))  # already format-validated above

    for c in candidates:
        raw_id = c["raw_work_id"]
        if raw_id in crosswalk:
            c["work_id"] = crosswalk[raw_id]
        else:
            counter += 1
            opaque = ids.mint_work_id(counter)
            crosswalk[raw_id] = opaque
            c["work_id"] = opaque

    _validate_crosswalk(crosswalk)  # M1: re-validate BEFORE persisting/returning

    crosswalk_file.parent.mkdir(parents=True, exist_ok=True)
    crosswalk_file.write_text(
        json.dumps(crosswalk, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return candidates


# ---------------------------------------------------------------------------
# 6.3 Source-masked CANDIDATE review artifact + NEW --from-approved reader (F5)
# ---------------------------------------------------------------------------

CANDIDATE_HEADER = [
    "work_id", "candidate_neutral_title", "author", "genre",
    "source_corpus", "review_status", "review_notes",
]
APPROVED_HEADER = [
    "work_id", "neutral_title", "author", "genre", "source_corpus", "review_status",
]


def _validate_csv_header(actual_fieldnames, expected_header, csv_kind: str) -> None:
    if list(actual_fieldnames or []) != list(expected_header):
        raise ValueError(
            f"{csv_kind} CSV header mismatch: expected {expected_header}, got {actual_fieldnames}"
        )


def emit_review_artifact(candidates: List[Dict], out_csv_path) -> List[Dict]:
    """Write the source-MASKED CANDIDATE review CSV (D-08). The FROZEN exact
    header is `CANDIDATE_HEADER`; `source_corpus` is the masked code only (no
    M-source/R-source name or siglum in any cell). Open-corpus (sefaria/ja)
    rows auto-fill `candidate_neutral_title` + `review_status='approved'`
    (D-08's "light spot-check", never a full review); the M-source literary
    subset is left with an EMPTY `candidate_neutral_title` + `review_status`
    for the owner to fill in during full manual review.

    Modeled (EMISSION-shape only, F5) on
    `scripts/export_translation_audit_sample.py`'s `write_csv`/AUDIT_COLUMNS
    convention (utf-8-sig, trailing review_status/review_notes columns).
    """
    rows = []
    for c in candidates:
        is_open = c["source_corpus"] in (ids.SOURCE_CORPUS_SEFARIA, ids.SOURCE_CORPUS_JA)
        rows.append({
            "work_id": c["work_id"],
            "candidate_neutral_title": (c.get("title") or "") if is_open else "",
            "author": c.get("author") or "",
            "genre": c.get("genre") or "",
            "source_corpus": c["source_corpus"],
            "review_status": "approved" if is_open else "",
            "review_notes": "auto-adopted (open corpus, light spot-check)" if is_open else "",
        })
    out_path = Path(out_csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CANDIDATE_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def load_approved_works(approved_csv_path, *, valid_work_ids: Optional[Iterable[str]] = None) -> List[Dict]:
    """NEW fail-closed `--from-approved` reader (F5 -- genuinely new behavior;
    no existing approved-round-trip reader analog). Reads ONLY rows meeting
    ALL rejection-rule gates:
      - `review_status == 'approved'`
      - a non-empty `neutral_title`
      - `work_id` present AND (if `valid_work_ids` given) crosswalk-known
      - a valid `source_corpus` masked code

    Anything else is EXCLUDED -- never a research-title fallback (D-07).
    Enforces the FROZEN exact `APPROVED_HEADER`.
    """
    valid_ids = set(valid_work_ids) if valid_work_ids is not None else None
    approved = []
    with open(approved_csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        _validate_csv_header(reader.fieldnames, APPROVED_HEADER, "APPROVED")
        for row in reader:
            if (row.get("review_status") or "").strip() != "approved":
                continue
            neutral_title = (row.get("neutral_title") or "").strip()
            if not neutral_title:
                continue
            work_id = (row.get("work_id") or "").strip()
            if not work_id:
                continue
            if valid_ids is not None and work_id not in valid_ids:
                continue
            source_corpus = row.get("source_corpus")
            try:
                ids.validate_source_corpus_code(source_corpus)
            except ValueError:
                continue
            approved.append({
                "work_id": work_id,
                "neutral_title": neutral_title,
                "author": (row.get("author") or None),
                "genre": (row.get("genre") or None),
                "source_corpus": source_corpus,
            })
    return approved


# ---------------------------------------------------------------------------
# 6.4 Per-page OUR-side text lookup (text_layer + snapshot_hash, OQ3)
# ---------------------------------------------------------------------------

class PageTextIndex:
    """Lazy, cached (text_layer, snapshot_hash) lookup over the research DB's
    `pages` table. NEVER exposes/copies the raw `text` value into the sidecar
    -- only its per-page sha256 digest + `provenance` (-> `text_layer`), per
    OQ3. Duck-typed: any object exposing `.get(page_id) -> (text_layer,
    snapshot_hash)` can stand in for this (tests use tiny in-memory doubles).
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

    def get(self, page_id: str) -> Tuple[Optional[str], Optional[str]]:
        if page_id in self._cache:
            return self._cache[page_id]
        row = self._conn.execute(
            "SELECT provenance, text FROM pages WHERE page_id = ?", (page_id,)
        ).fetchone()
        if row is None:
            result: Tuple[Optional[str], Optional[str]] = (None, None)
        else:
            provenance, text = row
            snapshot_hash = hashlib.sha256((text or "").encode("utf-8")).hexdigest()
            result = (provenance, snapshot_hash)
        self._cache[page_id] = result
        return result


# ---------------------------------------------------------------------------
# 6.5 Span-selection helpers (R7 tier_a largest-span; R4 propagated seed_spans)
# ---------------------------------------------------------------------------

def _largest_track1_span(spans_json_str: str) -> Tuple[int, int]:
    """Parse `track1_matches.spans_json` (a JSON list of `[start, end,
    density]` TRIPLES) and return `(start, end)` of the largest span by
    `end - start`, tie-broken `start` ASC then `end` ASC (R7). The density
    element `[2]` is NEVER used for selection -- `track1_matches` has no
    `alen`/`offsets` columns."""
    spans = json.loads(spans_json_str)
    best_key = None
    best_span = (0, 0)
    for s in spans:
        start, end = s[0], s[1]
        key = (-(end - start), start, end)
        if best_key is None or key < best_key:
            best_key = key
            best_span = (start, end)
    return best_span


def _largest_occurrence_span(seeds: List[Dict]) -> Tuple[int, int, Optional[str]]:
    """Return `(occ0, occ1, occ_class)` of the largest DISTINCT candidate-side
    occurrence among `seeds[]`, tie-broken `(occ1-occ0)` DESC, `occ0` ASC,
    `occ1` ASC (the frozen R4 primary-span tie-break)."""
    best_key = None
    best = (0, 0, None)
    for s in seeds:
        occ0, occ1 = s["occ0"], s["occ1"]
        key = (-(occ1 - occ0), occ0, occ1)
        if best_key is None or key < best_key:
            best_key = key
            best = (occ0, occ1, s.get("occ_class"))
    return best


def _distinct_seed_spans(seeds: List[Dict]) -> List[Dict]:
    """Collapse raw seed records to DISTINCT `(occ0, occ1)` occurrences (R4),
    each carrying `occ_class` + the sorted distinct `seed_page_ids`/
    `seed_sys_ids` that contributed to it. Up to 14 distinct occurrences per
    row (raw seeds up to 32 collapse by `(occ0, occ1)`)."""
    by_occ: Dict[Tuple[int, int], Dict] = {}
    for s in seeds:
        key = (s["occ0"], s["occ1"])
        entry = by_occ.setdefault(key, {
            "occ0": s["occ0"], "occ1": s["occ1"], "occ_class": s.get("occ_class"),
            "seed_page_ids": set(), "seed_sys_ids": set(),
        })
        if s.get("seed_page") is not None:
            entry["seed_page_ids"].add(s["seed_page"])
        if s.get("seed_sys") is not None:
            entry["seed_sys_ids"].add(s["seed_sys"])
    spans = []
    for (occ0, occ1) in sorted(by_occ):
        entry = by_occ[(occ0, occ1)]
        spans.append({
            "occ0": entry["occ0"], "occ1": entry["occ1"], "occ_class": entry["occ_class"],
            "seed_page_ids": sorted(entry["seed_page_ids"]),
            "seed_sys_ids": sorted(entry["seed_sys_ids"]),
        })
    return spans


def _seed_ms_ids(seeds: List[Dict]) -> List[str]:
    """Distinct OUR-side seed page_ids/sys_ids (both), sorted."""
    ids_set = set()
    for s in seeds:
        if s.get("seed_page") is not None:
            ids_set.add(s["seed_page"])
        if s.get("seed_sys") is not None:
            ids_set.add(s["seed_sys"])
    return sorted(ids_set)


def _selected_other_page_for_occurrence(seeds: List[Dict], occ0: int, occ1: int) -> Optional[str]:
    """The `seed_page` of the seed(s) contributing the SELECTED occurrence
    `(occ0, occ1)`; when multiple seeds contribute the SAME occurrence, pick
    the lexicographically-MIN `seed_page` (deterministic, family-router rows)."""
    candidates = sorted(
        s["seed_page"] for s in seeds
        if s.get("occ0") == occ0 and s.get("occ1") == occ1 and s.get("seed_page") is not None
    )
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# 6.6 Per-source ingestion (C-4 band map; §4 of the frozen schema doc)
# ---------------------------------------------------------------------------

def _ingest_e1_rows(
    rows: Iterable[Dict], *, work_index: Dict[str, Dict], page_index,
    confidence_band: str, adjudication_status: str, audit_status: str,
) -> List[Dict]:
    """Ingest ONE of the four DISJOINT E1 track1_direct source populations
    (e1_ra_confirmed / e1_adjudicated_a / e1_rb_screening / e1_r3_frame) --
    all four share the same row shape (page_id, sys_id, work_id, o0, o1, ml,
    dens, n_spans); band assignment is BY-SOURCE (the caller fixes
    `confidence_band`), no within-track1_direct fall-through (F1)."""
    out = []
    for row in rows:
        work = work_index.get(row["work_id"])
        if work is None:
            continue
        text_layer, snapshot_hash = page_index.get(row["page_id"])
        out.append(_mk_evidence(
            page_id=row["page_id"], work_id=work["work_id"], sys_id=row["sys_id"],
            evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=confidence_band,
            adjudication_status=adjudication_status, audit_status=audit_status,
            routing_status=_SHIPPED, routing_reason=_NONE_REASON,
            span_start=row["o0"], span_end=row["o1"],
            matched_letters=row.get("ml"), density=row.get("dens"), n_spans=row.get("n_spans"),
            text_layer=text_layer, snapshot_hash=snapshot_hash,
        ))
    return out


def _ingest_tier_a(conn: sqlite3.Connection, work_index: Dict[str, Dict], page_index) -> List[Dict]:
    """Ingest `track1_matches WHERE shadowed_by IS NULL` (Landmine 9) -> the
    `tier_a` band; offsets = the largest `spans_json` span (R7)."""
    out = []
    cur = conn.cursor()
    cur.execute(
        "SELECT page_id, sys_id, work_id, matched_letters, best_density, n_spans, spans_json "
        "FROM track1_matches WHERE shadowed_by IS NULL"
    )
    for page_id, sys_id, work_id, matched_letters, best_density, n_spans, spans_json in cur:
        work = work_index.get(work_id)
        if work is None:
            continue
        start, end = _largest_track1_span(spans_json)
        text_layer, snapshot_hash = page_index.get(page_id)
        out.append(_mk_evidence(
            page_id=page_id, work_id=work["work_id"], sys_id=sys_id,
            evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_TIER_A,
            adjudication_status=_UNREVIEWED, audit_status=_NA,
            routing_status=_SHIPPED, routing_reason=_NONE_REASON,
            span_start=start, span_end=end,
            matched_letters=matched_letters, density=best_density, n_spans=n_spans,
            text_layer=text_layer, snapshot_hash=snapshot_hash,
        ))
    return out


def _ingest_propagated_witness(
    rows: Iterable[Dict], work_index: Dict[str, Dict], page_index
) -> List[Dict]:
    """Ingest `q2_witness_collection.jsonl` (keyed cpage/csys) -> corroborated
    (via the LITERAL `ids.corroborated_predicate`) or weak; R4 multi-occurrence
    seed_spans + seed_ms_ids; primary a-side span = the largest distinct
    occurrence (tie-break per `_largest_occurrence_span`)."""
    out = []
    for row in rows:
        work = work_index.get(row["work_id"])
        if work is None:
            continue
        seeds = row.get("seeds") or []
        span_start, span_end, occ_class = _largest_occurrence_span(seeds)
        seed_spans = _distinct_seed_spans(seeds)
        seed_ms_ids = _seed_ms_ids(seeds)
        if ids.corroborated_predicate(row):
            band = _CORROBORATED
            adjudication_status, audit_status = _UNREVIEWED, _AUDIT_PENDING
        else:
            band = _WEAK
            adjudication_status, audit_status = _PROVISIONAL, _NA
        text_layer, snapshot_hash = page_index.get(row["cpage"])
        ge3_val = None
        if "ge3" in row:
            ge3_val = 1 if row.get("ge3") else 0
        out.append(_mk_evidence(
            page_id=row["cpage"], work_id=work["work_id"], sys_id=row["csys"],
            evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=band,
            adjudication_status=adjudication_status, audit_status=audit_status,
            routing_status=_SHIPPED, routing_reason=_NONE_REASON,
            span_start=span_start, span_end=span_end, occ_class=occ_class,
            is_new=1 if row.get("is_new") else 0,
            trials=row.get("trials"), runner_up=row.get("runner_up"),
            community=row.get("community"), ge3=ge3_val, rung=row.get("rung"),
            seed_spans=seed_spans, seed_ms_ids=seed_ms_ids,
            text_layer=text_layer, snapshot_hash=snapshot_hash,
        ))
    return out


def _ingest_family_router(
    rows: Iterable[Dict], work_index: Dict[str, Dict], page_index, *, router_bucket: str
) -> List[Dict]:
    """Ingest a NON-witness family-router collection (tafsir_targum /
    with_arabic, R3 -- `corroborated_predicate` is NEVER run on these) as
    `evidence_kind=shared_text` / `confidence_band=not_evaluated` /
    `routing_status=review_only` / `routing_reason=co_citation` (F9), with
    the FULL shared_text two-side shape (G2): a-side = the largest seed
    occurrence into `cpage`; `other_page_id` = the seed_page of the seed(s)
    contributing that SAME occurrence (lex-min tie-break); b-side offsets
    LEFT NULL (the router collections carry no b-side span)."""
    out = []
    for row in rows:
        work = work_index.get(row["work_id"])
        if work is None:
            continue
        seeds = row.get("seeds") or []
        span_start, span_end, _occ_class = _largest_occurrence_span(seeds)
        other_page_id = _selected_other_page_for_occurrence(seeds, span_start, span_end)
        text_layer, snapshot_hash = page_index.get(row["cpage"])
        text_layer_b, snapshot_hash_b = (
            page_index.get(other_page_id) if other_page_id else (None, None)
        )
        out.append(_mk_evidence(
            page_id=row["cpage"], work_id=work["work_id"], sys_id=row["csys"],
            evidence_kind=_SHARED_TEXT, evidence_source=_PROPAGATED, confidence_band=_NOT_EVALUATED,
            adjudication_status=_UNREVIEWED, audit_status=_NA,
            routing_status=_REVIEW_ONLY, routing_reason=_CO_CITATION,
            span_start=span_start, span_end=span_end, router_bucket=router_bucket,
            is_new=1 if row.get("is_new") else 0,
            text_layer=text_layer, snapshot_hash=snapshot_hash,
            other_page_id=other_page_id, text_layer_b=text_layer_b, snapshot_hash_b=snapshot_hash_b,
        ))
    return out


def _ingest_shared_text(
    rows: Iterable[Dict], work_index: Dict[str, Dict], page_index
) -> List[Dict]:
    """Ingest `q2_shared_text.jsonl` (keyed cpage/csys) -> `evidence_kind=
    shared_text` / `evidence_source=propagated` / `confidence_band=
    not_evaluated`, ACTUAL attributes only (tier/aligned_len/occ_class/
    n_seed_ms/cross_language/is_new -- never flank_class/ge3/cluster_size/
    router_bucket/rung, C-4); a-side occ0/occ1 into cpage; b-side
    `other_page_id=seed_page` (page id ONLY -- b_start/b_end stay NULL)."""
    out = []
    for row in rows:
        work = work_index.get(row["work_id"])
        if work is None:
            continue
        text_layer, snapshot_hash = page_index.get(row["cpage"])
        other_page_id = row.get("seed_page")
        text_layer_b, snapshot_hash_b = (
            page_index.get(other_page_id) if other_page_id else (None, None)
        )
        out.append(_mk_evidence(
            page_id=row["cpage"], work_id=work["work_id"], sys_id=row["csys"],
            evidence_kind=_SHARED_TEXT, evidence_source=_PROPAGATED, confidence_band=_NOT_EVALUATED,
            adjudication_status=_UNREVIEWED, audit_status=_NA,
            routing_status=_SHIPPED, routing_reason=_NONE_REASON,
            span_start=row["occ0"], span_end=row["occ1"],
            tier=row.get("tier"), aligned_len=row.get("aligned_len"), occ_class=row.get("occ_class"),
            n_seed_ms=row.get("n_seed_ms"), cross_language=1 if row.get("cross_language") else 0,
            is_new=1 if row.get("is_new") else 0,
            text_layer=text_layer, snapshot_hash=snapshot_hash,
            other_page_id=other_page_id, text_layer_b=text_layer_b, snapshot_hash_b=snapshot_hash_b,
        ))
    return out


# ---------------------------------------------------------------------------
# 6.6b L2: equal-priority evidence_id collision content comparison
# ---------------------------------------------------------------------------

# The full set of PERSISTED evidence-row fields (mirrors the discovery_evidence
# INSERT column list, minus the identity/bookkeeping keys) -- used ONLY to
# decide whether an equal-routing-priority evidence_id collision is a
# harmless true duplicate (identical content) or a genuine content-divergent
# collision that must raise (L2), never to alter what gets persisted.
_EVIDENCE_CONTENT_FIELDS = (
    "evidence_kind", "evidence_source", "confidence_band",
    "adjudication_status", "audit_status", "routing_status", "routing_reason",
    "is_new", "page_id", "sys_id",
    "tier", "aligned_len", "occ_class", "cross_language", "n_seed_ms", "trials",
    "runner_up", "community", "ge3", "rung", "router_bucket", "matched_letters",
    "density", "n_spans", "span_start", "span_end", "text_layer", "snapshot_hash",
    "seed_spans", "seed_ms_ids", "other_page_id", "b_start", "b_end",
    "text_layer_b", "snapshot_hash_b", "rule_version", "community_id",
)


def _evidence_content_key(e: Dict) -> tuple:
    """A comparable snapshot of an evidence-spec dict's PERSISTED content
    (L2) -- two rows with an equal key are semantically identical (a
    harmless true duplicate); a differing key at equal routing priority is
    a genuine, non-deterministic-to-resolve collision."""
    return tuple(e.get(f) for f in _EVIDENCE_CONTENT_FIELDS)


# ---------------------------------------------------------------------------
# 6.7 Claim/evidence assembly (independent of populate_synthetic, fixture-safety)
# ---------------------------------------------------------------------------

def assemble_claims_and_evidence(
    evidence_specs: List[Dict], work_source_corpus: Dict[str, str],
    *, sidecar_version: str = REAL_SIDECAR_VERSION,
) -> Dict:
    """Group a flat evidence-spec list (the `_mk_evidence` shape) into
    `discovery_claim` + `discovery_evidence` rows: resolves `claim_type`
    (largest-span-dominates across ALL witness evidence sharing a `page_id`,
    regardless of which work it belongs to) and the `display_evidence_id`
    (the frozen TOTAL selector). Claims key on the REAL `(page_id, work_id)`
    -- NO physical-MS collapse (R5).

    Independent of `populate_synthetic`'s inline synthetic-fixture assembly
    loop by design (a small amount of duplication is accepted) so the pinned
    134-03 golden fixture can never drift as a side effect of a change made
    here.
    """
    spans_by_page: Dict[str, List[int]] = {}
    for e in evidence_specs:
        if e["evidence_kind"] == _WITNESS:
            spans_by_page.setdefault(e["page_id"], []).append(e["span_end"] - e["span_start"])
    for e in evidence_specs:
        if e["evidence_kind"] == _WITNESS:
            this_len = e["span_end"] - e["span_start"]
            e["_row_claim_type"] = ids.claim_type_for_work_witness(spans_by_page[e["page_id"]], this_len)

    claims: Dict[tuple, List[Dict]] = {}
    for e in evidence_specs:
        claims.setdefault((e["page_id"], e["work_id"]), []).append(e)

    claim_id_by_key = {(p, w): ids.claim_id(p, w) for (p, w) in claims}

    claim_type_by_key = {}
    for (page_id, work_id), rows in claims.items():
        resolver_input = [
            {"evidence_kind": r["evidence_kind"], "claim_type": r.get("_row_claim_type")}
            for r in rows
        ]
        claim_type_by_key[(page_id, work_id)] = ids.resolve_claim_type(resolver_input)

    # Deduplicate on evidence_id (discovery_evidence's actual PK). A genuine
    # content-hash collision has been OBSERVED in the real research corpus
    # (deferred-items.md "evidence_id collision: shared_text vs family-router
    # same-span"): a plain q2_shared_text.jsonl row and a family-router
    # (tafsir_targum/with_arabic) row can independently resolve to the IDENTICAL
    # (work_id, a_page_id, sys_id, evidence_kind=shared_text,
    # evidence_source=propagated, confidence_band=not_evaluated, span_start,
    # span_end, other_page_id) tuple -- the FROZEN evidence_id recipe
    # (docs/specs/discovery-sidecar-schema-v1.md SS2) has no "which source
    # collection" discriminator by design, so this is NOT a bug in the
    # recipe's implementation; it is a real-data gap flagged for a future
    # dated schema amendment (never silently patched here). Deterministic
    # resolution: prefer the SHIPPED row over a review_only one (never let a
    # co-citation-only signal silently displace a shipped recall-widening
    # row); otherwise keep the first-seen row. Both source rows are never
    # dropped silently -- the collision count is returned for visibility.
    _ROUTING_PRIORITY = {_SHIPPED: 0, _REVIEW_ONLY: 1}
    evidence_by_id: Dict[str, Dict] = {}
    evidence_id_collisions = 0
    for (page_id, work_id), rows in claims.items():
        claim_id = claim_id_by_key[(page_id, work_id)]
        for e in rows:
            evidence_id = ids.evidence_id(
                work_id=e["work_id"], a_page_id=e["page_id"], sys_id=e["sys_id"],
                evidence_kind=e["evidence_kind"], evidence_source=e["evidence_source"],
                confidence_band=e["confidence_band"], span_start=e["span_start"],
                span_end=e["span_end"], other_page_id=e.get("other_page_id"),
                seed_spans=e.get("seed_spans"),
            )
            e["_evidence_id"] = evidence_id
            e["_claim_id"] = claim_id
            existing = evidence_by_id.get(evidence_id)
            if existing is None:
                evidence_by_id[evidence_id] = e
                continue
            evidence_id_collisions += 1
            existing_priority = _ROUTING_PRIORITY.get(existing["routing_status"], 99)
            new_priority = _ROUTING_PRIORITY.get(e["routing_status"], 99)
            if new_priority < existing_priority:
                evidence_by_id[evidence_id] = e
            elif new_priority == existing_priority:
                # L2: an EQUAL-priority collision (both shipped, or both
                # review_only) is only safe to resolve by first-seen order
                # when the two rows are semantically IDENTICAL (a true
                # duplicate, e.g. a repeated JSONL line -- harmless, either
                # is fine to keep). A content-DIVERGENT equal-priority
                # collision has no deterministic winner under the frozen
                # evidence_id recipe (which carries no "which source
                # collection" discriminator) -- raise fail-closed rather
                # than silently pick one based on ingestion order.
                if _evidence_content_key(existing) != _evidence_content_key(e):
                    raise EvidenceIdCollisionError(
                        f"evidence_id {evidence_id} collision at EQUAL routing "
                        f"priority (routing_status={e['routing_status']!r}) between "
                        f"two semantically DIFFERENT evidence rows for claim "
                        f"(page_id={page_id!r}, work_id={work_id!r}) -- refusing to "
                        "silently pick one based on ingestion order (L2)."
                    )

    evidence_rows_by_claim: Dict[str, List[Dict]] = {}
    for e in evidence_by_id.values():
        evidence_rows_by_claim.setdefault(e["_claim_id"], []).append(e)

    evidence_rows = []
    for e in evidence_by_id.values():
        evidence_rows.append((
            e["_evidence_id"], e["_claim_id"], e["evidence_kind"], e["evidence_source"], e["confidence_band"],
            e["adjudication_status"], e["audit_status"], e["routing_status"], e["routing_reason"],
            int(e["is_new"]), e["page_id"], e["sys_id"],
            e.get("tier"), e.get("aligned_len"), e.get("occ_class"), e.get("cross_language"),
            e.get("n_seed_ms"), e.get("trials"), e.get("runner_up"), e.get("community"),
            e.get("ge3"), e.get("rung"), e.get("router_bucket"), e.get("matched_letters"),
            e.get("density"), e.get("n_spans"),
            e["span_start"], e["span_end"], e.get("text_layer"), e.get("snapshot_hash"),
            json.dumps(e["seed_spans"]) if e.get("seed_spans") else None,
            json.dumps(e["seed_ms_ids"]) if e.get("seed_ms_ids") else None,
            e.get("other_page_id"), e.get("b_start"), e.get("b_end"),
            e.get("text_layer_b"), e.get("snapshot_hash_b"),
            e.get("rule_version"), e.get("community_id"),
        ))

    display_choices: Dict[str, str] = {}
    for claim_id, rows in evidence_rows_by_claim.items():
        selector_rows = [
            {"evidence_id": e["_evidence_id"], "evidence_source": e["evidence_source"],
             "confidence_band": e["confidence_band"], "adjudication_status": e["adjudication_status"]}
            for e in rows
        ]
        display_choices[claim_id] = ids.select_display_evidence(selector_rows)

    claim_rows = []
    for (page_id, work_id), claim_id in claim_id_by_key.items():
        claim_rows.append((
            page_id, work_id, claim_id, claim_type_by_key[(page_id, work_id)],
            display_choices[claim_id], work_source_corpus[work_id], sidecar_version,
        ))

    return {
        "claim_rows": claim_rows,
        "evidence_rows": evidence_rows,
        "display_choices": display_choices,
        "evidence_id_collisions": evidence_id_collisions,
    }


def build_claims_and_evidence(
    *, conn: Optional[sqlite3.Connection], works: List[Dict], page_index,
    e1_ra_confirmed: Iterable[Dict] = (), e1_adjudicated_a: Iterable[Dict] = (),
    e1_rb_screening: Iterable[Dict] = (), e1_r3_frame: Iterable[Dict] = (),
    q2_witness_collection: Iterable[Dict] = (), q2_collection_tafsir_targum: Iterable[Dict] = (),
    q2_collection_with_arabic: Iterable[Dict] = (), q2_shared_text: Iterable[Dict] = (),
    sidecar_version: str = REAL_SIDECAR_VERSION,
) -> Dict:
    """Assemble the UNIFIED witness family (track1_direct 4-disjoint-source
    banding + propagated corroborated/weak) + the shared_text family + the
    family-router collections into claim/evidence rows. `works` = the
    SHOWN-work set (list of dicts carrying `raw_work_id`/`work_id`/
    `source_corpus`) -- any row whose raw work_id is not in this set is
    EXCLUDED (no claim can exist without a `works` FK anchor, §10).

    `conn` (the research DB connection) is used ONLY for the `tier_a` read
    (`track1_matches WHERE shadowed_by IS NULL`); pass None to skip it (unit
    tests that only exercise the E1/Q2 JSONL-shaped sources).
    """
    work_index = {w["raw_work_id"]: w for w in works}
    work_source_corpus = {w["work_id"]: w["source_corpus"] for w in works}

    evidence_specs: List[Dict] = []
    evidence_specs += _ingest_e1_rows(
        e1_ra_confirmed, work_index=work_index, page_index=page_index,
        confidence_band=_EXPERT_VERIFIED, adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
    )
    evidence_specs += _ingest_e1_rows(
        e1_adjudicated_a, work_index=work_index, page_index=page_index,
        confidence_band=_EXPERT_VERIFIED, adjudication_status=_HUMAN_CONFIRMED, audit_status=_AUDIT_PENDING,
    )
    evidence_specs += _ingest_e1_rows(
        e1_rb_screening, work_index=work_index, page_index=page_index,
        confidence_band=_SCREENING_RB, adjudication_status=_PROVISIONAL, audit_status=_NA,
    )
    evidence_specs += _ingest_e1_rows(
        e1_r3_frame, work_index=work_index, page_index=page_index,
        confidence_band=_SCREENING_CANON, adjudication_status=_PROVISIONAL, audit_status=_NA,
    )
    if conn is not None:
        evidence_specs += _ingest_tier_a(conn, work_index, page_index)
    evidence_specs += _ingest_propagated_witness(q2_witness_collection, work_index, page_index)
    evidence_specs += _ingest_family_router(
        q2_collection_tafsir_targum, work_index, page_index, router_bucket="tafsir_targum"
    )
    evidence_specs += _ingest_family_router(
        q2_collection_with_arabic, work_index, page_index, router_bucket="with_arabic"
    )
    evidence_specs += _ingest_shared_text(q2_shared_text, work_index, page_index)

    return assemble_claims_and_evidence(evidence_specs, work_source_corpus, sidecar_version=sidecar_version)


# ---------------------------------------------------------------------------
# 6.8 Witness units (DATA-10) -- Oxford codicological parts + physical joins
# ---------------------------------------------------------------------------

_MERGEABLE_JOIN_TYPES = frozenset({
    "Physical Join", "Codex join", "Partial Physical Join", "Unspecified join",
})


def build_witness_units(
    oxford_parts: Iterable[Tuple[str, str]], physical_joins: Iterable[Tuple[str, int, Optional[str]]]
) -> List[Dict]:
    """Merge sys_ids into witness units via (a) catalogued Oxford
    codicological parts (`libraries.csv` `(sys_id, oxford_part_id)` pairs,
    non-empty part id only) then (b) physical joins (`(sys_id,
    join_group_id, join_type)` triples) where `join_type` in
    `_MERGEABLE_JOIN_TYPES` -- NEVER `'Scribe join'`, and NEVER on
    NULL/ambiguous `join_type` (conservative, DATA-10). Each sys_id lands in
    AT MOST ONE unit: Oxford-part groups are formed FIRST; any sys_id already
    assigned is excluded from a subsequent physical-join group. Returns unit
    specs `[{"members": [...], "merge_basis": ...}, ...]` (>=2 members each).
    """
    assigned: set = set()
    unit_specs: List[Dict] = []

    by_part: Dict[str, set] = {}
    for sys_id, part_id in oxford_parts:
        if not part_id:
            continue
        by_part.setdefault(part_id, set()).add(sys_id)
    for part_id in sorted(by_part):
        members = sorted(by_part[part_id])
        if len(members) < 2:
            continue
        unit_specs.append({"members": members, "merge_basis": ids.MERGE_BASIS_OXFORD_PART})
        assigned.update(members)

    by_group: Dict[int, set] = {}
    for sys_id, join_group_id, join_type in physical_joins:
        if join_type not in _MERGEABLE_JOIN_TYPES:
            continue
        if sys_id in assigned:
            continue
        by_group.setdefault(join_group_id, set()).add(sys_id)
    for group_id in sorted(by_group):
        members = sorted(m for m in by_group[group_id] if m not in assigned)
        if len(members) < 2:
            continue
        unit_specs.append({"members": members, "merge_basis": ids.MERGE_BASIS_PHYSICAL_JOIN})
        assigned.update(members)

    return unit_specs


def _load_oxford_parts(libraries_csv_path) -> List[Tuple[str, str]]:
    """Read `(system_number, oxford_part_id)` pairs from `libraries.csv`
    (col 0 = sys_id, col 1 = oxford_part_id per CLAUDE.md); non-empty
    part id only."""
    pairs = []
    with open(libraries_csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for row in reader:
            if len(row) < 2:
                continue
            sys_id, part_id = row[0], row[1]
            if part_id and part_id.strip():
                pairs.append((sys_id, part_id.strip()))
    return pairs


def _load_physical_joins(fjms_db_path) -> List[Tuple[str, int, Optional[str]]]:
    """Read `(AlmaId, JoinGroupId, JoinType)` from `fjms_enrichment.db`'s
    `joins` table (AlmaId == sys_id)."""
    conn = sqlite3.connect(f"file:{Path(fjms_db_path).resolve().as_posix()}?mode=ro", uri=True)
    try:
        cur = conn.execute("SELECT AlmaId, JoinGroupId, JoinType FROM joins")
        return [(alma_id, join_group_id, join_type) for alma_id, join_group_id, join_type in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 6.9 Misc real-mode helpers
# ---------------------------------------------------------------------------

def load_jsonl(path) -> List[Dict]:
    """Read a newline-delimited JSON collection into a list of dicts."""
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _compute_htr_snapshot_hash(conn: sqlite3.Connection) -> str:
    """ONE corpus-level "did the underlying HTR corpus change" signal (OQ3) --
    a cheap deterministic digest over the page count + total char count (the
    cheapest sufficient granularity per OQ3; per-page drift is separately
    covered by each evidence row's own `snapshot_hash`/`snapshot_hash_b`)."""
    (n_pages, total_chars) = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(n_chars), 0) FROM pages"
    ).fetchone()
    key = f"htr_snapshot_v1|{n_pages}|{total_chars}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _connect_research_ro(db_path) -> sqlite3.Connection:
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _insert_works_real(cur: sqlite3.Cursor, works: List[Dict]) -> None:
    rows = [
        (w["work_id"], ids.canonical_work_id(w["work_id"]), w["neutral_title"],
         w.get("author"), w.get("genre"), w["source_corpus"])
        for w in works
    ]
    cur.executemany(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def _insert_claims_and_evidence_real(cur: sqlite3.Cursor, claim_rows, evidence_rows) -> None:
    cur.executemany(
        "INSERT INTO discovery_claim "
        "(page_id, work_id, claim_id, claim_type, display_evidence_id, source_corpus, sidecar_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        claim_rows,
    )
    cur.executemany(
        """
        INSERT INTO discovery_evidence (
            evidence_id, claim_id, evidence_kind, evidence_source, confidence_band,
            adjudication_status, audit_status, routing_status, routing_reason,
            is_new, a_page_id, sys_id,
            tier, aligned_len, occ_class, cross_language, n_seed_ms, trials, runner_up,
            community, ge3, rung, router_bucket, matched_letters, density, n_spans,
            span_start, span_end, text_layer, snapshot_hash,
            seed_spans, seed_ms_ids,
            other_page_id, b_start, b_end, text_layer_b, snapshot_hash_b,
            rule_version, community_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        evidence_rows,
    )


def _insert_witness_units_real(cur: sqlite3.Cursor, unit_specs: List[Dict]) -> int:
    unit_rows = []
    member_rows = []
    for spec in unit_specs:
        unit_id = ids.unit_id(spec["members"])
        unit_rows.append((unit_id,))
        for sys_id in spec["members"]:
            member_rows.append((unit_id, sys_id, spec["merge_basis"]))
    cur.executemany("INSERT INTO witness_units (unit_id) VALUES (?)", unit_rows)
    cur.executemany(
        "INSERT INTO witness_unit_members (unit_id, sys_id, merge_basis) VALUES (?, ?, ?)",
        member_rows,
    )
    return len(unit_rows)


# ---------------------------------------------------------------------------
# 6.10 finalize_build -- the full real-mode orchestration (DATA-05/08, F13)
# ---------------------------------------------------------------------------

# Frozen real-data contract facts (verified against the research corpus this
# rework, docs/specs C-4/§4.2) -- a hard integrity assertion ONLY when the
# collection is actually loaded from its real file path (never fired against
# a test's small synthetic slice, since tests never pass these *_path args).
_EXPECTED_TAFSIR_TARGUM_ROWS = 106
_EXPECTED_WITH_ARABIC_ROWS = 108

# H2: the FULL frozen release-input contract (docs/specs/discovery-sidecar-
# schema-v1.md SS4.1/SS4.2/SS4.3, C-1). Enforced ONLY when finalize_build is
# called with release=True -- a missing/short collection must never
# silently ingest as empty and produce a tier-A-only sidecar that still
# passes every OTHER gate. Non-release calls (unit tests, --allow-partial-
# sources smoke builds) are exempt by design.
_EXPECTED_E1_RA_CONFIRMED_ROWS = 1570
_EXPECTED_E1_ADJUDICATED_A_ROWS = 174
_EXPECTED_E1_RB_SCREENING_ROWS = 7498
_EXPECTED_E1_R3_FRAME_ROWS = 9996
_EXPECTED_Q2_WITNESS_COLLECTION_ROWS = 4367
_EXPECTED_Q2_SHARED_TEXT_ROWS = 60156
_EXPECTED_TIER_A_ROWS = 275894  # track1_matches WHERE shadowed_by IS NULL


class ReleaseInputsIncompleteError(RuntimeError):
    """Raised (H2) when `finalize_build(release=True, ...)` is missing any
    frozen release input, or an input's row count drifts from the frozen
    contract. A missing collection must never silently ingest as empty --
    that would produce a tier-A-only sidecar that still passes every other
    release gate. `--allow-partial-sources` (never combined with
    `release=True`) is the ONLY sanctioned escape hatch, reserved for the
    smoke/unit path."""


def _count_tier_a_rows(conn: sqlite3.Connection) -> int:
    (n,) = conn.execute(
        "SELECT COUNT(*) FROM track1_matches WHERE shadowed_by IS NULL"
    ).fetchone()
    return n


def _assert_release_inputs_complete(
    *,
    release: bool,
    allow_partial_sources: bool,
    e1_ra_confirmed: List[Dict],
    e1_adjudicated_a: List[Dict],
    e1_rb_screening: List[Dict],
    e1_r3_frame: List[Dict],
    q2_witness_collection: List[Dict],
    q2_shared_text: List[Dict],
    q2_collection_tafsir_targum: List[Dict],
    q2_collection_with_arabic: List[Dict],
    tier_a_row_count: Optional[int],
) -> None:
    """H2: in release mode, REQUIRE every frozen release input present at
    its EXACT expected count -- abort (raise) on any absent/short/long
    input, BEFORE any ingest of the (possibly-partial) collections into
    claims/evidence. This is the PRIMARY fix (not the verifier's
    release-contract check, which only re-checks build-written `meta`
    counts against the ACTUAL row counts in the finished .db -- a
    self-consistent-but-wrong check that a partial-but-internally-
    consistent build would still pass)."""
    if not release:
        return
    if allow_partial_sources:
        raise ValueError(
            "--allow-partial-sources cannot be combined with --release (H2) -- "
            "a release build must never silently accept a partial source set"
        )
    checks = [
        ("e1_ra_confirmed", len(e1_ra_confirmed), _EXPECTED_E1_RA_CONFIRMED_ROWS),
        ("e1_adjudicated_a", len(e1_adjudicated_a), _EXPECTED_E1_ADJUDICATED_A_ROWS),
        ("e1_rb_screening", len(e1_rb_screening), _EXPECTED_E1_RB_SCREENING_ROWS),
        ("e1_r3_frame", len(e1_r3_frame), _EXPECTED_E1_R3_FRAME_ROWS),
        ("q2_witness_collection", len(q2_witness_collection), _EXPECTED_Q2_WITNESS_COLLECTION_ROWS),
        ("q2_shared_text", len(q2_shared_text), _EXPECTED_Q2_SHARED_TEXT_ROWS),
        ("q2_collection_tafsir_targum", len(q2_collection_tafsir_targum), _EXPECTED_TAFSIR_TARGUM_ROWS),
        ("q2_collection_with_arabic", len(q2_collection_with_arabic), _EXPECTED_WITH_ARABIC_ROWS),
        (
            "tier_a (track1_matches WHERE shadowed_by IS NULL)",
            tier_a_row_count if tier_a_row_count is not None else 0,
            _EXPECTED_TIER_A_ROWS,
        ),
    ]
    problems = [
        f"{name}: expected {expected}, got {actual}"
        for name, actual, expected in checks
        if actual != expected
    ]
    if problems:
        raise ReleaseInputsIncompleteError(
            "release build (H2) requires every frozen input present at its EXACT "
            "expected row count -- mismatches: " + "; ".join(problems)
        )


def _resolve_band_precision_spec(
    *, precision_spec: Optional[List[Dict]], frozen_precision_defaults: bool, release: bool,
) -> List[Dict]:
    """H3: resolve the band_precision rows to write, BEFORE any further
    build work begins. An explicit `precision_spec` (owner-supplied at
    134-07) always wins; otherwise an explicit `frozen_precision_defaults`
    acknowledgement uses the documented frozen-contract defaults (tier_a
    precision NULL -- never the SYNTHETIC-mode-only 0.90 placeholder). A
    `release=True` build with NEITHER supplied is refused outright -- a
    real/release payload must never silently fabricate a number. A
    non-release call (unit tests, `--allow-partial-sources` smoke builds)
    defaults to the SAME frozen-contract rows when neither is supplied.
    Extracted as its own function so H3's raise path is directly
    unit-testable without needing to satisfy the (unrelated) H2 input-
    completeness gate."""
    if precision_spec is not None:
        return precision_spec
    if frozen_precision_defaults:
        return _frozen_real_band_precision_rows()
    if release:
        raise ValueError(
            "--release requires --precision-spec <json> or an explicit "
            "--frozen-precision-defaults acknowledgement (H3) -- a real/release "
            "build must never silently fabricate band_precision numbers"
        )
    return _frozen_real_band_precision_rows()


def finalize_build(
    *,
    source_db_path,
    from_approved_path,
    crosswalk_path,
    out_db_path,
    review_artifact_path=None,
    libraries_csv_path=None,
    fjms_db_path=None,
    e1_ra_confirmed_path=None,
    e1_adjudicated_a_path=None,
    e1_rb_screening_path=None,
    e1_r3_frame_path=None,
    q2_witness_collection_path=None,
    q2_collection_tafsir_targum_path=None,
    q2_collection_with_arabic_path=None,
    q2_shared_text_path=None,
    precision_spec=None,
    frozen_precision_defaults: bool = False,
    masking_patterns=None,
    create_crosswalk_if_missing: bool = False,
    data_as_of: Optional[str] = None,
    release: bool = False,
    allow_partial_sources: bool = False,
) -> Dict:
    """Orchestrate the REAL distillation end to end (F13 order): distill
    (claims/evidence, NO physical-MS collapse) -> `build_witness_units` ->
    populate `band_precision` -> write `meta` (incl `frame_content_hash`) ->
    `PRAGMA integrity_check` -> commit -> BLOCKING masking scan (aborts +
    deletes the `.db` on ANY hit, DC13) -> non-blocking registered-token scan
    of the review artifact (surfaces only) -> file content_hash + manifest.

    Raises `CrosswalkAbortError` (via `assign_opaque_work_ids`) if the
    crosswalk is required-but-absent, `MaskingGateFailure` if the BLOCKING
    scan finds any hit, `ReleaseInputsIncompleteError` (H2) if `release=True`
    and any frozen input is absent/short/long, and `ValueError` (H3) if
    `release=True` and neither `precision_spec` nor
    `frozen_precision_defaults=True` was supplied.

    `release` (H2/H3): when True, REQUIRES every frozen Q2/E1 input present
    at its exact expected row count (see `_assert_release_inputs_complete`)
    AND an explicit precision-spec choice (see below) -- never combined with
    `allow_partial_sources`. `allow_partial_sources` is the ONLY sanctioned
    way to ingest a partial/subset source set, reserved for the smoke/unit
    path (never `release=True`).

    `precision_spec`/`frozen_precision_defaults` (H3): a real/release build
    NEVER fabricates a `tier_a` precision number. If `precision_spec` (an
    explicit list of band_precision row dicts, owner-supplied at 134-07) is
    given, it is used verbatim. Otherwise, if `frozen_precision_defaults=True`
    is explicitly acknowledged, `_frozen_real_band_precision_rows()` (the
    documented frozen-contract defaults, tier_a precision NULL) is used. A
    `release=True` build with NEITHER supplied raises -- a real/release
    payload must never silently default. A non-release call (unit tests,
    `--allow-partial-sources` smoke builds) defaults to
    `_frozen_real_band_precision_rows()` when neither is supplied.
    """
    out_path = Path(out_db_path)
    if out_path.exists():
        out_path.unlink()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn_research = _connect_research_ro(source_db_path)
    try:
        candidates = select_shown_works(conn_research)
        candidates = assign_opaque_work_ids(
            candidates, crosswalk_path, create_if_missing=create_crosswalk_if_missing
        )
        if review_artifact_path:
            emit_review_artifact(candidates, review_artifact_path)

        crosswalk = json.loads(Path(crosswalk_path).read_text(encoding="utf-8"))
        valid_work_ids = set(crosswalk.values())

        approved = load_approved_works(from_approved_path, valid_work_ids=valid_work_ids)
        raw_by_opaque = {c["work_id"]: c["raw_work_id"] for c in candidates}
        works = []
        for a in approved:
            raw_work_id = raw_by_opaque.get(a["work_id"])
            if raw_work_id is None:
                continue
            works.append({**a, "raw_work_id": raw_work_id})

        page_index = PageTextIndex(conn_research)

        def _load(path):
            return load_jsonl(path) if path else []

        e1_ra_confirmed = _load(e1_ra_confirmed_path)
        e1_adjudicated_a = _load(e1_adjudicated_a_path)
        e1_rb_screening = _load(e1_rb_screening_path)
        e1_r3_frame = _load(e1_r3_frame_path)
        q2_witness_collection = _load(q2_witness_collection_path)
        q2_collection_tafsir_targum = _load(q2_collection_tafsir_targum_path)
        q2_collection_with_arabic = _load(q2_collection_with_arabic_path)
        q2_shared_text = _load(q2_shared_text_path)

        if q2_collection_tafsir_targum_path and len(q2_collection_tafsir_targum) != _EXPECTED_TAFSIR_TARGUM_ROWS:
            raise ValueError(
                "q2_collection_tafsir_targum row count drifted from the frozen contract "
                f"(expected {_EXPECTED_TAFSIR_TARGUM_ROWS}, got {len(q2_collection_tafsir_targum)})"
            )
        if q2_collection_with_arabic_path and len(q2_collection_with_arabic) != _EXPECTED_WITH_ARABIC_ROWS:
            raise ValueError(
                "q2_collection_with_arabic row count drifted from the frozen contract "
                f"(expected {_EXPECTED_WITH_ARABIC_ROWS}, got {len(q2_collection_with_arabic)})"
            )

        # H2: in release mode, REQUIRE every frozen input present at its
        # EXACT expected count BEFORE any ingest -- a missing collection
        # must never silently ingest as empty and produce a tier-A-only
        # sidecar that still passes every other gate.
        tier_a_row_count = _count_tier_a_rows(conn_research) if release else None
        _assert_release_inputs_complete(
            release=release,
            allow_partial_sources=allow_partial_sources,
            e1_ra_confirmed=e1_ra_confirmed, e1_adjudicated_a=e1_adjudicated_a,
            e1_rb_screening=e1_rb_screening, e1_r3_frame=e1_r3_frame,
            q2_witness_collection=q2_witness_collection, q2_shared_text=q2_shared_text,
            q2_collection_tafsir_targum=q2_collection_tafsir_targum,
            q2_collection_with_arabic=q2_collection_with_arabic,
            tier_a_row_count=tier_a_row_count,
        )

        # H3: resolve the band_precision spec BEFORE any further work -- a
        # real/release build must NEVER fabricate a tier_a number.
        bp_rows = _resolve_band_precision_spec(
            precision_spec=precision_spec,
            frozen_precision_defaults=frozen_precision_defaults,
            release=release,
        )

        result = build_claims_and_evidence(
            conn=conn_research, works=works, page_index=page_index,
            e1_ra_confirmed=e1_ra_confirmed, e1_adjudicated_a=e1_adjudicated_a,
            e1_rb_screening=e1_rb_screening, e1_r3_frame=e1_r3_frame,
            q2_witness_collection=q2_witness_collection,
            q2_collection_tafsir_targum=q2_collection_tafsir_targum,
            q2_collection_with_arabic=q2_collection_with_arabic,
            q2_shared_text=q2_shared_text,
            sidecar_version=REAL_SIDECAR_VERSION,
        )

        oxford_parts = _load_oxford_parts(libraries_csv_path) if libraries_csv_path else []
        physical_joins = _load_physical_joins(fjms_db_path) if fjms_db_path else []
        unit_specs = build_witness_units(oxford_parts, physical_joins)

        source_db_sha256 = _hash_file(Path(source_db_path))
        crosswalk_sha256 = _hash_file(Path(crosswalk_path))
        htr_snapshot_hash = _compute_htr_snapshot_hash(conn_research)
    finally:
        conn_research.close()

    out_conn = sqlite3.connect(str(out_path))
    out_conn.execute("PRAGMA foreign_keys = ON")
    cur = None
    try:
        create_schema(out_conn)
        cur = out_conn.cursor()
        _insert_works_real(cur, works)
        _insert_claims_and_evidence_real(cur, result["claim_rows"], result["evidence_rows"])
        n_units = _insert_witness_units_real(cur, unit_specs)

        # bp_rows was already resolved above (H3), BEFORE any research-DB
        # work began -- reused here unchanged, now that the output schema
        # exists to insert it into.
        cur.executemany(
            """
            INSERT INTO band_precision (
                scope, collection_id, evidence_source, confidence_band, numerator, denominator,
                precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes
            ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
                       :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
                       :ins_policy, :weighting, :notes)
            """,
            bp_rows,
        )

        (n_works,) = cur.execute("SELECT COUNT(*) FROM works").fetchone()
        (n_claims,) = cur.execute("SELECT COUNT(*) FROM discovery_claim").fetchone()
        (n_evidence,) = cur.execute("SELECT COUNT(*) FROM discovery_evidence").fetchone()

        # F13: band_precision is ALREADY committed by the time frame_content_hash
        # / meta / the file-level content_hash are computed below.
        frame_content_hash = compute_frame_content_hash(out_conn)

        build_date = _now_iso()
        meta_rows = [
            ("schema_version", SCHEMA_VERSION),
            ("sidecar_version", REAL_SIDECAR_VERSION),
            ("source_db_sha256", source_db_sha256),
            ("crosswalk_sha256", crosswalk_sha256),
            ("build_date", build_date),
            ("data_as_of", data_as_of or build_date[:10]),
            ("htr_snapshot_hash", htr_snapshot_hash),
            ("expected_rows_claims", str(n_claims)),
            ("expected_rows_evidence", str(n_evidence)),
            ("expected_rows_works", str(n_works)),
            ("expected_rows_units", str(n_units)),
            ("frame_content_hash", frame_content_hash),
        ]
        cur.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)

        (integrity_result,) = out_conn.execute("PRAGMA integrity_check").fetchone()
        if integrity_result != "ok":
            raise RuntimeError(f"PRAGMA integrity_check failed: {integrity_result}")

        out_conn.commit()
    finally:
        # Windows sqlite3 gotcha: a live Cursor object with an un-finalized
        # statement can keep the underlying OS file handle open even after
        # Connection.close() -- explicitly close the cursor FIRST so the
        # BLOCKING masking-gate scan (and any caller-side unlink-on-abort)
        # below never races a stale handle on the just-written .db file.
        if cur is not None:
            cur.close()
        out_conn.close()

    # BLOCKING masking gate (DC13/M2) -- runs over the FINALIZED,
    # already-committed .db. M2: pattern-loading and the scan itself are
    # wrapped in the SAME try/except as the hit check, so ANY exception
    # from this block (a ScanError from _require_patterns/scan_sqlite --
    # e.g. an empty pattern set, an unreadable pattern file, or a scan
    # failure -- not just an actual masking HIT) deletes the just-written
    # output .db before propagating. Previously only a HIT deleted the
    # file; a pattern-load or scan-time exception left a half-finalized
    # (unscanned, therefore unproven-clean) artifact on disk.
    try:
        patterns = masking_patterns if masking_patterns is not None else _cam.load_patterns()
        patterns = _cam._require_patterns(patterns)
        issues = _cam.scan_sqlite(str(out_path), patterns)
        if issues:
            raise MaskingGateFailure(
                f"blocking masking scan found {len(issues)} issue(s) -- finalization ABORTED "
                f"(DC13); {out_path} removed"
            )
    except Exception:
        try:
            out_path.unlink()
        except OSError:
            pass
        raise

    # Non-blocking registered-token scan of the gitignored review artifact --
    # surfaces (returned in stats) but NEVER blocks finalization (DC13).
    artifact_issue_count = 0
    if review_artifact_path and os.path.exists(review_artifact_path):
        try:
            artifact_issue_count = len(_cam.scan_asset(review_artifact_path, patterns))
        except _cam.ScanError:
            artifact_issue_count = 0

    content_hash = _hash_file(out_path)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "asset_basename": out_path.stem,
        "content_hash": content_hash,
        "frame_content_hash": frame_content_hash,
    }
    manifest_path = out_path.parent / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {
        "row_counts": {
            "works": n_works, "discovery_claim": n_claims,
            "discovery_evidence": n_evidence, "witness_units": n_units,
        },
        "frame_content_hash": frame_content_hash,
        "content_hash": content_hash,
        "band_precision_rows": len(bp_rows),
        "artifact_masking_issues": artifact_issue_count,
        "evidence_id_collisions": result.get("evidence_id_collisions", 0),
        "manifest_path": str(manifest_path),
        "db_path": str(out_path),
    }


_DEFAULT_COLLECTION_FILENAMES = {
    "e1_ra_confirmed": "e1_ra_confirmed.jsonl",
    "e1_adjudicated_a": "e1_adjudicated_a.jsonl",
    "e1_rb_screening": "e1_rb_screening.jsonl",
    "e1_r3_frame": "e1_r3_frame.jsonl",
    "q2_witness_collection": "q2_witness_collection.jsonl",
    "q2_collection_tafsir_targum": "q2_collection_tafsir_targum.jsonl",
    "q2_collection_with_arabic": "q2_collection_with_arabic.jsonl",
    "q2_shared_text": "q2_shared_text.jsonl",
}


def _resolve_collection_paths(research_data_dir) -> Dict[str, Optional[str]]:
    paths: Dict[str, Optional[str]] = {}
    for key, filename in _DEFAULT_COLLECTION_FILENAMES.items():
        if not research_data_dir:
            paths[key] = None
            continue
        candidate = Path(research_data_dir) / filename
        paths[key] = str(candidate) if candidate.exists() else None
    return paths


# ---------------------------------------------------------------------------
# 7. CLI
# ---------------------------------------------------------------------------

def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_golden(path: str) -> int:
    db_path = Path(path)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        create_schema(conn)
        stats = populate_synthetic(conn, source_db_hash="golden-synthetic-v1")
        conn.commit()
    finally:
        conn.close()

    content_hash = _hash_file(db_path)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "asset_basename": "discovery-v1-fixture",
        "content_hash": content_hash,
        "frame_content_hash": stats["frame_content_hash"],
    }
    manifest_path = db_path.parent / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    expected = dict(stats)
    expected["content_hash"] = content_hash
    expected_path = db_path.with_name(db_path.stem + "-expected.json")
    expected_path.write_text(
        json.dumps(expected, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    print(f"wrote golden fixture: {db_path} ({db_path.stat().st_size} bytes), "
          f"{manifest_path}, {expected_path}")
    print(f"content_hash={content_hash}")
    print(f"frame_content_hash={stats['frame_content_hash']}")
    return 0


def _run_smoke(n: int) -> int:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        create_schema(conn)
        stats = populate_synthetic(conn, source_db_hash=f"smoke-synthetic-{n}")
        conn.commit()
    finally:
        conn.close()
    print(f"smoke build OK: {stats['row_counts']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("db_path", nargs="?", default=None,
                        help="Path to the gitignored research DB (fullcorpus_v2.db) for "
                             "real-mode distillation (134-04; requires --from-approved + --crosswalk)")
    parser.add_argument("--golden", metavar="PATH", default=None,
                        help="Write the deterministic committed fixture to PATH "
                             "(+ manifest.json + PATH-expected.json alongside)")
    parser.add_argument("--smoke", type=int, default=None, metavar="N",
                        help="Build the synthetic dataset in-memory and print stats "
                             "(N accepted for CLI-shape parity with build_atlas_asset.py; "
                             "the curated synthetic dataset is fixed/comprehensive, not scaled by N)")
    real_group = parser.add_argument_group("real-mode distillation (134-04)")
    real_group.add_argument("--from-approved", metavar="PATH", default=None,
                        help="Owner-APPROVED neutral-title CSV (APPROVED_HEADER) -- required")
    real_group.add_argument("--crosswalk", metavar="PATH", default=None,
                        help="Persisted raw->opaque work_id crosswalk JSON -- required")
    real_group.add_argument("--init-crosswalk", action="store_true",
                        help="Allow creating a NEW crosswalk file if --crosswalk doesn't exist yet "
                             "(first-ever build only; otherwise an absent crosswalk aborts, DC2)")
    real_group.add_argument("--research-data-dir", metavar="DIR", default=None,
                        help="Directory containing the Q2/E1 *.jsonl collections "
                             "(filenames resolved via _DEFAULT_COLLECTION_FILENAMES)")
    real_group.add_argument("--libraries-csv", metavar="PATH", default=None,
                        help="libraries.csv path (DATA-10 Oxford codicological parts)")
    real_group.add_argument("--fjms-db", metavar="PATH", default=None,
                        help="fjms_enrichment.db path (DATA-10 physical joins)")
    real_group.add_argument("--out", metavar="PATH", default=None,
                        help="Output discovery.db path (default: discovery_data/discovery-v1.db)")
    real_group.add_argument("--review-artifact", metavar="PATH", default=None,
                        help="Output CANDIDATE review-artifact CSV path "
                             "(default: discovery_data/discovery-review-candidates.csv)")
    real_group.add_argument("--release", action="store_true",
                        help="H2/H3: real RELEASE build -- REQUIRES every frozen Q2/E1 "
                             "input present at its exact expected row count, and an "
                             "explicit --precision-spec or --frozen-precision-defaults "
                             "choice. Mutually exclusive with --allow-partial-sources.")
    real_group.add_argument("--allow-partial-sources", action="store_true",
                        help="H2: explicitly allow ingesting a partial/subset set of "
                             "Q2/E1 collections -- ONLY for the smoke/unit path, NEVER "
                             "combined with --release.")
    real_group.add_argument("--precision-spec", metavar="PATH", default=None,
                        help="H3: JSON file with an explicit list of band_precision row "
                             "dicts (owner-supplied at 134-07) -- overrides the frozen "
                             "real-mode defaults.")
    real_group.add_argument("--frozen-precision-defaults", action="store_true",
                        help="H3: explicitly acknowledge using the frozen-contract "
                             "band_precision defaults (docs/specs/discovery-sidecar-"
                             "schema-v1.md SS1.6, tier_a=NULL) instead of a custom "
                             "--precision-spec. Required (together with --precision-spec "
                             "being one-or-the-other) for --release.")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.golden is None and args.smoke is None and args.db_path is None:
        parser.error("db_path is required unless --smoke N or --golden PATH is given")

    if args.golden is not None:
        return _write_golden(args.golden)
    if args.smoke is not None:
        return _run_smoke(args.smoke)

    # Real-mode distillation (134-04).
    if not args.from_approved or not args.crosswalk:
        parser.error("--from-approved and --crosswalk are required for real-mode distillation")

    # H2: there is NO silent default -- the operator must explicitly choose
    # between a strict --release build or an --allow-partial-sources
    # smoke/unit build. Never both.
    if args.release and args.allow_partial_sources:
        parser.error("--release and --allow-partial-sources are mutually exclusive (H2)")
    if not args.release and not args.allow_partial_sources:
        parser.error(
            "real-mode distillation requires an explicit --release or "
            "--allow-partial-sources choice (H2) -- no default is silently permitted"
        )
    # H3: a --release build must explicitly choose a precision source.
    if args.release and not args.precision_spec and not args.frozen_precision_defaults:
        parser.error(
            "--release requires --precision-spec <json> or --frozen-precision-defaults (H3)"
        )

    precision_spec = None
    if args.precision_spec:
        precision_spec = json.loads(Path(args.precision_spec).read_text(encoding="utf-8"))

    out_db_path = args.out or str(Path(_REPO_ROOT) / "discovery_data" / "discovery-v1.db")
    review_artifact_path = args.review_artifact or str(
        Path(_REPO_ROOT) / "discovery_data" / "discovery-review-candidates.csv"
    )
    collection_paths = _resolve_collection_paths(args.research_data_dir)

    stats = finalize_build(
        source_db_path=args.db_path,
        from_approved_path=args.from_approved,
        crosswalk_path=args.crosswalk,
        out_db_path=out_db_path,
        review_artifact_path=review_artifact_path,
        libraries_csv_path=args.libraries_csv,
        fjms_db_path=args.fjms_db,
        create_crosswalk_if_missing=args.init_crosswalk,
        e1_ra_confirmed_path=collection_paths["e1_ra_confirmed"],
        e1_adjudicated_a_path=collection_paths["e1_adjudicated_a"],
        e1_rb_screening_path=collection_paths["e1_rb_screening"],
        e1_r3_frame_path=collection_paths["e1_r3_frame"],
        q2_witness_collection_path=collection_paths["q2_witness_collection"],
        q2_collection_tafsir_targum_path=collection_paths["q2_collection_tafsir_targum"],
        q2_collection_with_arabic_path=collection_paths["q2_collection_with_arabic"],
        q2_shared_text_path=collection_paths["q2_shared_text"],
        precision_spec=precision_spec,
        frozen_precision_defaults=args.frozen_precision_defaults,
        release=args.release,
        allow_partial_sources=args.allow_partial_sources,
    )
    print(f"real build OK: {stats['row_counts']}")
    print(f"content_hash={stats['content_hash']}")
    print(f"frame_content_hash={stats['frame_content_hash']}")
    print(f"evidence_id_collisions={stats['evidence_id_collisions']}")
    print(f"db_path={stats['db_path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
