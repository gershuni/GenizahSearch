#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline build for the Phase 134 Discovery Data Spine sidecar (`discovery.db`).

Implements the FROZEN two-table claim model from
`docs/specs/discovery-sidecar-schema-v1.md` (Phase 134, plan 134-01): the
`create_schema()` DDL (works, discovery_claim, discovery_evidence,
witness_units, witness_unit_members, meta, band_precision) plus a synthetic
fixture-generation mode (`synthetic_discovery_dataset` / `--golden PATH` /
`--smoke N`). This plan (134-03) ships ONLY the DDL + synthetic mode; the
real-mode distillation (consuming the gitignored research DB) is deferred to
134-04 -- calling this script with a bare `db_path` raises `NotImplementedError`.

Masking note: every identifier/title/span value fabricated here is
SYNTHETIC -- never derived from real research data (mirrors
`scripts/build_atlas_asset.py`'s `synthetic_dataset`/`golden_dataset`
convention). `source_corpus` values are the masked codes {sefaria, ja,
msource} only; work_ids are minted via `scripts.discovery_ids.mint_work_id`
(a plain zero-padded counter, never a raw M:/J:/REF token).

Usage:
    python scripts/build_discovery_sidecar.py --golden tests/fixtures/discovery/discovery-v1-fixture.db
    python scripts/build_discovery_sidecar.py --smoke 10
    python scripts/build_discovery_sidecar.py <db_path>   # NotImplementedError until 134-04
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)
for _p in (_REPO_ROOT, _SCRIPTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import discovery_ids as ids  # scripts/discovery_ids.py -- FROZEN id/enum/routing primitives

SCHEMA_VERSION = "discovery-v1"
SIDECAR_VERSION = "discovery-v1-synthetic-fixture"

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
# 6. CLI
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
                        help="Path to the research DB for real-mode distillation "
                             "(NotImplementedError until 134-04)")
    parser.add_argument("--golden", metavar="PATH", default=None,
                        help="Write the deterministic committed fixture to PATH "
                             "(+ manifest.json + PATH-expected.json alongside)")
    parser.add_argument("--smoke", type=int, default=None, metavar="N",
                        help="Build the synthetic dataset in-memory and print stats "
                             "(N accepted for CLI-shape parity with build_atlas_asset.py; "
                             "the curated synthetic dataset is fixed/comprehensive, not scaled by N)")
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

    raise NotImplementedError(
        "real-mode distillation (consuming the gitignored research DB) is "
        "deferred to Phase 134 plan 134-04; this plan (134-03) ships only the "
        "DDL + synthetic/--golden fixture mode."
    )


if __name__ == "__main__":
    sys.exit(main())
