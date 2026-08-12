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
`assign_opaque_work_ids`), the masked, impact-prioritized CANDIDATE review
artifact (134-07 Task A: `compute_work_impact_counts` / `emit_review_artifact`
/ `build_candidate_review_artifact` / `--emit-review-artifact-only`) + the
fail-closed owner-verdict `--from-approved` reader (134-07 Task B:
`load_approved_works` -- ships iff `owner_verdict` in {approve, edit} AND a
non-empty resolved title), the unified witness family across the
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
    python scripts/build_discovery_sidecar.py <source_db_path> --emit-review-artifact-only \\
        --crosswalk <crosswalk.json> [--init-crosswalk] --research-data-dir <DIR> \\
        [--review-artifact discovery_data/candidates.csv]
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
import unicodedata
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

# 136-11 (T-136-11-01): the main-pool bucket rule has exactly ONE implementation
# across the bake, the panel and the corpus-wide findings page. This builder
# CALLS it -- there is deliberately no `main_pool`-shaped function anywhere in
# this file, so the three surfaces cannot disagree about which bucket an
# identification belongs to.
from shared.discovery_locus import (  # noqa: E402
    parse_ref_span_alignments,
    select_primary_alignment,
)
# Deliberate re-export: this error was raised from this module before the parsing moved
# to `shared.discovery_locus`, and callers -- including the frozen gate-14 tests --
# import it from here. Re-exporting keeps it the SAME class, so `isinstance` and
# `pytest.raises` both continue to hold.
from shared.discovery_locus import RefSpanProjectionError  # noqa: E402,F401
from shared.discovery_main_pool import (  # noqa: E402
    MAIN_POOL_REASONS,
    Identification as MainPoolIdentification,
    main_pool_decision,
)

# 136-11 (D-10a): the MATERIALIZED `band_rank` sort key must be the SAME lattice
# the runtime service sorts by -- so it is IMPORTED here, never re-declared. A
# second literal band order in this file would mean the stored key and
# `shared.discovery_service._band_rank` could silently disagree, and rows would
# then appear in a different order depending on which code path produced the
# ordering (T-136-11-02). `shared/discovery_service.py` imports nothing from
# `web/` and nothing from this script, so there is no cycle and no heavy import.
from shared.discovery_service import (  # noqa: E402
    _BAND_RANK_ORDER,
    _band_rank as _runtime_band_rank,
)

# 136-12 (NOVEL-01/02): the novelty axis has exactly ONE vocabulary, ONE
# verdict->column mapping, ONE identity key and ONE masking table, all owned by
# `shared/discovery_novelty.py` (contract: `docs/specs/discovery-novelty-v1.md`).
# They are IMPORTED here, never restated -- a second literal shade list in this
# file is precisely how the builder and the verifier would come to disagree
# about what `fills_gap` selects.
from shared.discovery_novelty import (  # noqa: E402
    DEFAULT_STATUS as NOVELTY_DEFAULT_STATUS,
    MASKED_PROVENANCE_LABELS,
    NOVELTY_STATUSES,
    load_alias_groups,
    masked_provenance_label,
    novelty_columns_for,
    novelty_work_key,
)

# 136-12 (VIS-01 / D-22): the two visibility axes are DERIVED by the shared
# module, never by a second `source_corpus == 'sefaria'` test written here. The
# builder's job is to call them at the point the RAW evidence origin is still in
# scope and to STORE the results -- `is_public` (the ONE eligibility rule) is
# deliberately NOT imported: the builder stores the axes and lets
# `scripts/project_discovery_public.py` apply the conjunction, so the
# eligibility rule has exactly one caller-facing home.
from shared.discovery_visibility import (  # noqa: E402
    VISIBILITY_PRIVATE,
    VISIBILITY_PUBLIC,
    assertion_visibility as derive_assertion_visibility,
    identity_visibility as derive_identity_visibility,
    reconcile_launch_scope,
)

# C-track / Contract 1: the precedence matrix. IMPORTED, never restated -- the
# release verifier recomputes `rendered_relation` from the same module and
# asserts equality row-for-row, so a second implementation here would turn that
# gate into a comparison of this file against itself.
from shared import discovery_relation_matrix as relation_matrix  # noqa: E402
SCHEMA_VERSION = "discovery-v1"
SIDECAR_VERSION = "discovery-v1-synthetic-fixture"

# Real-mode (134-04) distillation's own sidecar_version -- kept DISTINCT from the
# synthetic-fixture constant above so the two build paths can never be confused by
# a reader inspecting discovery_claim.sidecar_version.
REAL_SIDECAR_VERSION = "discovery-v1-real"

# CD batch / schema Amendment 2026-08-12 (U): the unconditional post-batch
# marker. NOT a schema_version bump (that marker stays `discovery-v1`, pinned
# by test) -- a loader that sees this key requires every table, column and
# count key the amendment adds; an asset without it is a pre-batch asset and
# validates exactly as before.
LOCUS_SCHEMA_VERSION = "locus-v1"

# Frozen constant timestamps (F13/determinism) -- NEVER wall-clock, so a
# rebuild in any environment reproduces byte-identical output.
FROZEN_BUILD_DATE = "2026-07-22T00:00:00Z"
FROZEN_DATA_AS_OF = "2026-07-21"
FROZEN_HTR_SNAPSHOT_HASH = hashlib.sha256(b"discovery-v1-synthetic-htr-corpus").hexdigest()

_RULE_VERSION = "discovery-v1-synthetic"

# 136-12 / schema Amendment 2026-08-02 (C1): the closed `meta.audience` enum.
# THIS builder only ever writes the private value -- `scripts/project_discovery_public.py`
# is the sole writer of `public`, and the two must never converge into one
# configurable knob (that is what makes the boundary structural rather than
# procedural). Mirrors `web/discovery_assets.py::_AUDIENCES`.
ASSET_AUDIENCE_PRIVATE = "private"
ASSET_AUDIENCE_PUBLIC = "public"
ASSET_AUDIENCES = frozenset({ASSET_AUDIENCE_PRIVATE, ASSET_AUDIENCE_PUBLIC})


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
  source_corpus        TEXT NOT NULL CHECK (source_corpus IN ('sefaria','ja','msource')),
  -- 136-12 / schema Amendment 2026-08-02 (A). VIS-01 axis TWO (D-22): the
  -- visibility of this WORK's own identity source. Derived by
  -- `shared.discovery_visibility.identity_visibility` -- the ONE derivation,
  -- never a second `source_corpus == 'sefaria'` test written here.
  --
  -- NOT NULL DEFAULT 'private' is the fail-closed posture: public eligibility
  -- requires BOTH axes public (`is_public`), so a row some future insert path
  -- forgets to derive is private, never public by omission.
  identity_visibility  TEXT NOT NULL DEFAULT 'private'
                       CHECK (identity_visibility IN ('public','private'))
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
  -- discovery-v3 / schema Amendment 2026-08-07 (E): the two gen-2 coverage-router
  -- reasons APPENDED to the enum (see discovery_ids.ROUTING_REASONS). An enum a
  -- writer can produce but the CHECK constraint rejects is not a vocabulary
  -- extension, it is a build that dies at INSERT -- which is exactly what Codex
  -- round 2 found: the router mapping was written and tested against in-memory
  -- tuples only, so no test ever reached this constraint.
  routing_reason    TEXT NOT NULL CHECK (routing_reason IN ('impurity','runner_up_conflict','co_citation','none','later_shared_text','low_coverage','gen2_parallel_surface','gen2_router_not_shipped')),
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

  -- 136-11 / schema Amendment 2026-08-02 (A). APPENDED at the end of the column
  -- list on purpose: every pre-existing column keeps its ordinal position, so a
  -- positional reader (and the D-02b rebuild-preservation diff) sees an additive
  -- change, never a reshuffle.
  --
  -- coverage_ppm: `round(matched_letters / page_norm_letters * 1_000_000)`,
  -- DIRECT FAMILY ONLY (D-08a). Propagated rows carry NO coverage value -- not
  -- because it is withheld for display reasons, but because those rows have NO
  -- page-length denominator at all: `_ingest_propagated_witness` never computes
  -- matched_letters against a page, and all 42,776 shipped propagated evidence
  -- rows in the live v2 asset have NULL matched_letters.
  --
  -- coverage_status is the VALIDITY axis and must never collapse into the value
  -- axis (T-136-11-04): `compute_page_coverage` returns 0.0 on a MISSING
  -- denominator, which is not the same fact as a genuine near-zero match. So a
  -- missing/zero denominator stores coverage_ppm NULL + 'no_denominator', and a
  -- real measurement stores an integer + 'measured'. A reader can therefore
  -- always tell "we measured almost nothing" from "we could not measure".
  coverage_ppm      INTEGER,
  coverage_status   TEXT CHECK (coverage_status IN ('measured','no_denominator','not_applicable')
                                OR coverage_status IS NULL),
  -- band_rank: the materialized (evidence_source, confidence_band) sort key --
  -- see `_runtime_band_rank` at the top of this file. Lower is stronger.
  band_rank         INTEGER,
  -- novelty_status: the TEN-VALUE closed shade enum (owner rulings E/E'/F/G/H,
  -- `136-GATE1-DECISIONS.md` SS E-H; restated in the schema doc's Amendment
  -- 2026-08-02 (A)). The COLUMN + its D-10a index are created here because the
  -- authorized index set names it; the VALUES are computed and written by plan
  -- 136-12. Until then every row carries the fail-closed default `not_checked`
  -- -- which is exactly what `not_checked` means, never "novel by default".
  novelty_status    TEXT NOT NULL DEFAULT 'not_checked' CHECK (novelty_status IN (
                        'confirms','refines_granularity','aid_more_specific','diverges_work',
                        'diverges_part','container_predicts','fills_gap','extends','alias_merge',
                        'not_checked')),
  -- 136-12 / schema Amendment 2026-08-02 (A). The MASKED provenance label only:
  -- one of `shared.discovery_novelty.MASKED_PROVENANCE_LABELS`, never the raw
  -- provenance value (NOVEL-02 / D-25). NULL on `fills_gap` (nothing to name)
  -- and `not_checked` (nothing was checked) -- gated by
  -- `novelty_columns_for`'s SOURCE_LABEL_ELIGIBLE_SHADES, never by a second
  -- membership test written here.
  novelty_source_label TEXT,
  -- A SEPARATE sibling column, never part of the shade enum (owner ruling F --
  -- correctness is orthogonal to shade). The CHECK below is the schema doc's
  -- own literal, reproduced VERBATIM and identical to the mirrored
  -- discovery_identification CHECK.
  --
  -- WHAT IT ACTUALLY ENFORCES, stated precisely because the prose reads
  -- stronger than SQL delivers: under SQL three-valued logic a CHECK passes
  -- unless it evaluates to FALSE, and `NULL IN (...)` is NULL, not FALSE. So a
  -- `diverges_work` row with a NULL correctness PASSES, while a non-NULL value
  -- on a non-divergence shade, or an out-of-vocabulary value on a divergence
  -- shade, is REJECTED. The constraint is therefore the one-directional rule
  -- "non-NULL implies a divergence shade AND an in-vocabulary value" -- which
  -- is exactly what `shared.discovery_novelty.novelty_columns_for` (the
  -- designated tie-breaker) independently enforces, and exactly what owner
  -- ruling L REQUIRES.
  --
  -- OWNER RULING L (2026-08-03, `136-GATE1-DECISIONS.md` SS L): this column is
  -- HUMAN/OWNER ANNOTATION ONLY. The model no longer produces it
  -- (`resolve_model_output` ALWAYS returns None for it), so the build's own
  -- verdict-cache ingestion never writes a value here -- the column stays NULL
  -- until a separate human/owner annotation artifact (not yet built) supplies
  -- one. The column, its CHECK and its vocabulary are UNCHANGED by ruling L;
  -- only the SOURCE of a value changed.
  divergence_correctness TEXT CHECK (
                        (novelty_status IN ('diverges_work','diverges_part')
                         AND divergence_correctness IN ('catalogue_correct','claim_correct','unclear'))
                        OR
                        (novelty_status NOT IN ('diverges_work','diverges_part')
                         AND divergence_correctness IS NULL)
                      ),
  -- 136-12 / schema Amendment 2026-08-02 (A). VIS-01 axis ONE (D-22): the
  -- visibility of the ORIGIN OF THIS ASSERTION -- the specific evidence
  -- occurrence -- NOT of the identified work's corpus.
  --
  -- This column exists because the origin of the displayed assertion is not
  -- otherwise representable in the shipped schema: SS1.2 REQUIRES
  -- `discovery_claim.source_corpus` to equal the identified work's, so
  -- `works.source_corpus` is exactly the proxy D-22 measured insufficient
  -- (the restricted-corpus id prefix maps to 656 restricted-identity works AND
  -- 235 open ones -- it mislabels in BOTH directions).
  --
  -- The DERIVED masked enum only. The raw origin is consumed at ingest, while
  -- it is still in scope, and never stored, logged or interpolated -- once the
  -- private asset is built the raw provenance is gone, and this stored value is
  -- what `scripts/project_discovery_public.py` reads. NOT NULL DEFAULT
  -- 'private': fail-closed, exactly as on `works.identity_visibility`.
  assertion_visibility TEXT NOT NULL DEFAULT 'private'
                       CHECK (assertion_visibility IN ('public','private')),

  -- discovery-v3 / schema Amendment 2026-08-07 (F): the WORK-SIDE offsets
  -- (Codex blocker 1, bake plan §3.2 stage 1). APPENDED at the end, like every
  -- amendment before it, so positional readers see an additive change.
  --
  -- COORDINATE SPACE, named here because the plan requires it and because
  -- getting it wrong is the specific trap the v2 bake plan warned about: these
  -- index the reference work's `norm_stream` -- the SAME normalized stream
  -- `span_start`/`span_end` index on the page side. They are NOT offsets into
  -- the readability-oriented `body` that the Sefaria versemaps index, so
  -- resolving them to a human-readable locus (chapter/verse) needs a
  -- `body <-> norm_stream` map that does not exist yet. That resolution is
  -- DEFERRED; the raw offsets are shipped now because the our-text-only
  -- highlight does not wait on it.
  --
  -- NULLABLE by necessity, not by laxity: only `track1_direct` witnesses carry
  -- a work-side coordinate at all. The propagated and shared_text families have
  -- no reference-side span in their inputs, so a NOT NULL default would either
  -- fabricate a zero or block the build. Gate 3 asserts non-NULL on the
  -- track1_direct population specifically.
  w_start           INTEGER,
  w_end             INTEGER,
  -- discovery-v3 / schema Amendment 2026-08-07 (G): the PAGE side of the SAME
  -- producer alignment `w_start`/`w_end` came from (Codex R3 BLOCKER).
  --
  -- Why this exists rather than reusing `span_start`/`span_end`. Those are frozen
  -- inputs to the `evidence_id` recipe, so they cannot be changed without
  -- regenerating every track1_direct id and breaking the D-02b
  -- rebuild-preservation diff. But they hold the largest `spans_json` span, which
  -- is a HULL over the producer's paired alignments -- on the real fixture, page
  -- hull [981,1772] while the selected pair's page side is [981,1705]. Emitting
  -- the hull beside a work interval from a NARROWER entry claims a
  -- correspondence that does not exist: round 3's finding was exact, and it
  -- matters for the page highlight, the D-17 overlap computation and any consumer
  -- reading the four columns as one alignment.
  --
  -- So the coherent pair is `(aligned_page_start, aligned_page_end, w_start,
  -- w_end)` -- all four from ONE producer alignment. `span_start`/`span_end`
  -- remain what they always were (the hull, an id input and a coarse locator) and
  -- are NOT a work-side correspondence. A consumer wanting a two-sided alignment
  -- must read these four; that is stated in the schema doc, not left to be
  -- inferred.
  aligned_page_start INTEGER,
  aligned_page_end   INTEGER,

  UNIQUE(claim_id, evidence_id)
);
CREATE INDEX ix_discovery_evidence_claim_id     ON discovery_evidence(claim_id);
CREATE INDEX ix_discovery_evidence_a_page_id    ON discovery_evidence(a_page_id);
CREATE INDEX ix_discovery_evidence_other_page_id ON discovery_evidence(other_page_id);
-- D-10a index set (schema Amendment 2026-08-02 (D)), part 1: the two new
-- sort/filter keys, and the novelty STATUS column -- deliberately NOT the legacy
-- `is_new` boolean, which stays in the schema for read-compat but is no longer
-- the query target.
CREATE INDEX ix_discovery_evidence_coverage_ppm   ON discovery_evidence(coverage_ppm);
CREATE INDEX ix_discovery_evidence_band_rank      ON discovery_evidence(band_rank);
CREATE INDEX ix_discovery_evidence_novelty_status ON discovery_evidence(novelty_status);
-- D-10a's measured findings-query fix. UNIQUE is a real invariant, not just an
-- index hint: each claim selects its display evidence from its OWN evidence
-- rows, and `evidence_id` is the PK of discovery_evidence, so two claims can
-- never name the same display row (verified on the live v2 asset: 268,361
-- claims / 268,361 distinct display_evidence_id).
CREATE UNIQUE INDEX ux_discovery_claim_display_evidence_id
  ON discovery_claim(display_evidence_id);

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
  notes           TEXT,

  -- CERT-01 measurement-registry columns (Phase 135, plan 135-05). All
  -- NULLABLE: the CERT-01 grading write fills them later (135-09+). The
  -- CLOSED-vocab measurement_status CHECK (Codex #B3) mirrors
  -- shared/discovery_band_labels.MEASUREMENT_STATUSES exactly, so a free-text
  -- status can never reach the default-eligibility predicate.
  measurement_status TEXT CHECK (measurement_status IN
                        ('not_measured','measured_pass','measured_fail','insufficient_evidence')
                        OR measurement_status IS NULL),
  measurement_date   TEXT,
  grader             TEXT,
  audit_status       TEXT,
  report_id          TEXT
);

-- Masking-safe D-17 chronological-routing audit trail (Phase 135, plan
-- 135-05 DDL; ROWS written by the v2 bake demotion in 135-06). Opaque work
-- ids + numeric years ONLY -- no title, no reference text, no raw id: this
-- table carries no restricted content by construction. `decision` and
-- `routing_reason` record which demotion rule fired; `routing_reason` is a
-- plain annotation column here (the constrained routing_reason enum lives on
-- discovery_evidence).
CREATE TABLE discovery_routing_audit (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id         TEXT,
  kept_work_id    TEXT,
  demoted_work_id TEXT,
  kept_year       INTEGER,
  demoted_year    INTEGER,
  delta_years     INTEGER,
  decision        TEXT CHECK (decision IN ('demoted','kept_tie','fail_safe_unknown_date')),
  routing_reason  TEXT
);

-- ---------------------------------------------------------------------------
-- 136-11 / schema Amendment 2026-08-02 (B): the IDENTIFICATION grain.
--
-- ONE row per (sys_id, canonical_work_id) -- the unit the main-pool rule and
-- the corpus-wide findings page both operate on. Because the grain is the
-- CANONICAL work, the D-13a duplicate collapse is STRUCTURAL here: two `works`
-- rows recording the same canonical_work_id produce ONE identification row, so
-- every count derived from this table is already deduplicated.
-- ---------------------------------------------------------------------------
CREATE TABLE discovery_identification (
  identification_id   TEXT PRIMARY KEY,   -- sha256("discovery_identification_v1|{sys_id}|{canonical_work_id}")
  sys_id              TEXT NOT NULL,
  canonical_work_id   TEXT NOT NULL,
  -- NEVER canonical_work_id: `canonical_work_id` is NOT unique on `works` (15
  -- duplicated groups on the live asset), so joining the identification grain
  -- to `works` on it FANS OUT 64,509 rows to 65,587. `display_work_id` is the
  -- deterministic representative chosen by the schema's SS(B1) ordered total
  -- rule; every identity join (title, author, identity_visibility) reads it.
  display_work_id     TEXT NOT NULL REFERENCES works(work_id),
  main_pool           INTEGER NOT NULL,   -- boolean (0/1), from shared.discovery_main_pool
  main_pool_reason    TEXT NOT NULL CHECK (main_pool_reason IN (
                         'shared_wording','overlapping_tie','low_coverage',
                         'insufficient_length','missing_signal',
                         'main_multifolio','main_full_coverage','main_human_confirmed')),
  best_band_rank      INTEGER NOT NULL,
  page_count          INTEGER NOT NULL,
  max_coverage_ppm    INTEGER,            -- NULL when no direct-family evidence contributes
  relation_kind       TEXT NOT NULL,      -- the STORED claim_type basis of the display relation
  -- ⚠ NOT YET IN THE SCHEMA DOC. Required by plan 136-11's own action text
  -- ("carry a column recording which of the two admitted it so the surface can
  -- render the coverage note") but absent from the schema's Amendment
  -- 2026-08-02 (B) column list, which predates that D-13g analysis. Deliberately
  -- NULLABLE, so `scripts/project_discovery_public.py` -- which inserts only the
  -- 14 columns the schema doc names -- keeps working unchanged; a NOT NULL
  -- column here would break the public projection outright. A dated schema
  -- amendment adding this column is OWED at 136-12 (the plan that already edits
  -- the schema contract); see 136-11-SUMMARY.md.
  eligibility_basis   TEXT CHECK (eligibility_basis IN ('shipped','human_confirmed')
                                  OR eligibility_basis IS NULL),
  -- The TEN-VALUE shade enum (owner rulings E/E'/F/G/H). This CHECK and the
  -- mirrored discovery_evidence.novelty_status CHECK above are the two places
  -- the vocabulary is unavoidably restated as a SQL literal (a SQLite CHECK
  -- cannot import a shared constant); `shared/discovery_novelty.py::
  -- NOVELTY_STATUSES` is the tie-breaker if they ever disagree.
  novelty_status      TEXT NOT NULL CHECK (novelty_status IN
                         ('confirms','refines_granularity','aid_more_specific','diverges_work',
                          'diverges_part','container_predicts','fills_gap','extends','alias_merge',
                          'not_checked')),
  -- A SEPARATE sibling column, never part of the shade enum (owner ruling F --
  -- correctness is orthogonal to shade). Required NULL outside the two
  -- divergence shades; the CHECK enforces that direction in both directions.
  divergence_correctness TEXT CHECK (
                         (novelty_status IN ('diverges_work','diverges_part')
                          AND divergence_correctness IN ('catalogue_correct','claim_correct','unclear'))
                         OR
                         (novelty_status NOT IN ('diverges_work','diverges_part')
                          AND divergence_correctness IS NULL)
                       ),
  assertion_visibility TEXT NOT NULL CHECK (assertion_visibility IN ('public','private')),
  identity_visibility  TEXT NOT NULL CHECK (identity_visibility IN ('public','private')),
  -- CD batch / schema Amendment 2026-08-12 (P): the routing_reason of this
  -- identification's OWN best evidence row (lowest band_rank, then the D-13b
  -- lexicographic evidence_id tie-break -- the same `best` the display basis
  -- and novelty inheritance already use). Codex pre-flight finding 2's rule,
  -- verbatim; matrix step 2 reads it. DEFAULT 'none' exists only for additive
  -- ALTER compatibility -- the materializer always writes an explicit value.
  routing_reason      TEXT NOT NULL DEFAULT 'none'
                      CHECK (routing_reason IN
                        ('impurity','runner_up_conflict','co_citation','none',
                         'later_shared_text','low_coverage','gen2_parallel_surface',
                         'gen2_router_not_shipped')),
  -- CD batch / schema Amendment 2026-08-12 (O): the Contract-1 rendered
  -- relation -- the stored output of the frozen precedence matrix
  -- (docs/specs/discovery-relation-matrix-v1.md §2), recomputed per asset
  -- after public pruning.
  --
  -- ⟨AMENDED 2026-08-12, C-track⟩ This comment used to end "NO surface reads
  -- this column before C-track". THREE now do, unconditionally: the claims
  -- query, the manuscript-summary query and the findings query. That is why
  -- `web/discovery_assets.py::_REQUIRED_COLUMNS` requires the column of EVERY
  -- asset rather than only of post-batch ones -- an asset without it does not
  -- degrade, it loads and then errors on every read.
  rendered_relation   TEXT NOT NULL DEFAULT 'uncertain'
                      CHECK (rendered_relation IN
                        ('direct_witness','shared_text','quotes_this_work',
                         'work_quotes_page','uncertain')),
  UNIQUE (sys_id, canonical_work_id)
);

-- ---------------------------------------------------------------------------
-- 136-11 / schema Amendment 2026-08-02 (B): manuscript display keys.
--
-- Sourced ONLY from libraries.csv (masking-safe catalogue metadata, the same
-- source the existing panel/browse surfaces already read). Carries NO work
-- title, NO reference text and NO locus (T-136-11-03) -- it exists purely so
-- the findings page and the panel can sort by library + shelfmark and show a
-- real total, neither of which the paged result set can do today (D-17a).
-- ---------------------------------------------------------------------------
CREATE TABLE manuscript_display (
  sys_id              TEXT PRIMARY KEY,
  library_code        TEXT NOT NULL,
  library_sort_key    TEXT NOT NULL,
  shelfmark_display   TEXT NOT NULL,
  shelfmark_sort_key  TEXT NOT NULL
);

-- D-10a index set (schema Amendment 2026-08-02 (D)), part 2.
CREATE INDEX ix_discovery_identification_order
  ON discovery_identification(main_pool, best_band_rank, max_coverage_ppm);
CREATE INDEX ix_discovery_identification_canonical_work_id
  ON discovery_identification(canonical_work_id);
CREATE INDEX ix_discovery_identification_sys_id
  ON discovery_identification(sys_id);
CREATE INDEX ix_manuscript_display_sort
  ON manuscript_display(library_sort_key, shelfmark_sort_key);

-- ---------------------------------------------------------------------------
-- CD batch / schema Amendment 2026-08-12 (N): the locus address tables.
-- Created EMPTY by the batch; POPULATED by the D-track import, which re-keys
-- the build_work_divisions.py artifact from raw locus_ref_id to the OPAQUE
-- work_id via the crosswalk (fail-closed: no crosswalk entry -> the row is
-- skipped and counted, never guessed). The raw locus_ref_id is NEVER stored
-- -- opaque ids only, the same posture as every other table (the verifier's
-- raw-work-id sweep covers these tables too); provenance lives in the import
-- report. locus_unit.start_offset indexes the work's norm_stream -- the SAME
-- coordinate space as w_start/w_end (rule (G)).
-- ---------------------------------------------------------------------------
CREATE TABLE locus_work (
  work_id       TEXT PRIMARY KEY REFERENCES works(work_id),
  -- family vocabulary MEASURED against the locus artifact 2026-08-12 (the
  -- first CHECK draft omitted msource_daf and would have rejected the
  -- D-track import outright): sefaria/ja/msource_header/msource_daf.
  family        TEXT NOT NULL CHECK (family IN
                  ('sefaria','ja','msource_header','msource_daf')),
  grain         TEXT NOT NULL,
  stream_len    INTEGER NOT NULL,
  unit_count    INTEGER NOT NULL
);
CREATE TABLE locus_unit (
  work_id      TEXT NOT NULL REFERENCES locus_work(work_id),
  unit_ord     INTEGER NOT NULL,
  start_offset INTEGER NOT NULL,
  part_key     TEXT NOT NULL,
  label_he     TEXT NOT NULL,
  citation_pos INTEGER,
  PRIMARY KEY (work_id, unit_ord)
);
CREATE INDEX ix_locus_unit_part ON locus_unit(work_id, part_key);
CREATE TABLE locus_edition (
  work_id         TEXT PRIMARY KEY REFERENCES locus_work(work_id),
  title_he        TEXT NOT NULL,
  title_original  TEXT NOT NULL,
  author_short    TEXT NOT NULL,
  author_full     TEXT NOT NULL,
  publisher       TEXT NOT NULL,
  publisher_city  TEXT NOT NULL,
  publisher_year  TEXT NOT NULL,
  editor          TEXT NOT NULL,
  edition         TEXT NOT NULL
);

-- CD batch / schema Amendment 2026-08-12 (R): Contract-1's two input tables.
-- Matrix step 3 reads discovery_region_map at the single region_version named
-- in meta; step 4's curated half reads discovery_curated_quoter at the single
-- curated_quoter_version. `discriminative` is tri-state ON PURPOSE: NULL is an
-- 'open' card and fails closed under the frozen step-3 semantics (a unit with
-- no row at all ALWAYS blocks the demotion -- the map is partial). Both ship
-- EMPTY from the batch; C-track populates (region: the owner's 2026-08-11
-- input; curated v1: both Yalkut works, owner ruling 2026-08-12).
CREATE TABLE discovery_region_map (
  region_version TEXT NOT NULL,
  work_id        TEXT NOT NULL REFERENCES locus_work(work_id),
  unit_ord       INTEGER NOT NULL,
  discriminative INTEGER CHECK (discriminative IN (0,1) OR discriminative IS NULL),
  source         TEXT NOT NULL CHECK (source IN
                   ('ruling','derived','superseded_by_derivation','open')),
  basis          TEXT,
  PRIMARY KEY (region_version, work_id, unit_ord)
);
CREATE TABLE discovery_curated_quoter (
  list_version      TEXT NOT NULL,
  canonical_work_id TEXT NOT NULL,
  ruled_date        TEXT NOT NULL,
  note              TEXT,
  PRIMARY KEY (list_version, canonical_work_id)
);

-- CD batch / schema Amendment 2026-08-12 (Q): Contract-4 storage. Withholding
-- is DISPLAY-LAYER ONLY -- a withheld row renders no locus and falls to the
-- matrix's fail-closed state; the row itself remains on every surface and in
-- every count, and withholding never mutates CERT-01 membership (the frame
-- hash reads claim/evidence membership fields only -- proven by test). Both
-- ship EMPTY; the compiled eligibility read lands with C-track's shared SQL
-- builder, the bijection gates with the audit frames (E).
CREATE TABLE discovery_withholding (
  withhold_version TEXT NOT NULL,
  scope_id         TEXT NOT NULL,
  predicate_json   TEXT NOT NULL,
  frame_version    TEXT,
  stratum_id       TEXT,
  reason           TEXT NOT NULL,
  created_date     TEXT NOT NULL,
  PRIMARY KEY (withhold_version, scope_id)
);
CREATE TABLE discovery_stratum_membership (
  frame_version     TEXT NOT NULL,
  stratum_id        TEXT NOT NULL,
  identification_id TEXT NOT NULL REFERENCES discovery_identification(identification_id),
  PRIMARY KEY (frame_version, stratum_id, identification_id)
);
CREATE INDEX ix_stratum_membership_identification
  ON discovery_stratum_membership(identification_id);
"""


# ---------------------------------------------------------------------------
# v2 re-distill (Phase 135, plan 135-06): hash-pinned build inputs (strict
# JSON parse), the census -> cross_corpus_map merge loader, the two date
# inputs, the D-17 chronological co-claim demotion, and the CERT-01
# FAIL-branch reband. All masking-safe: opaque w000xxx ids + numeric years
# only ever cross into these structures -- never a title, a raw descriptive
# date string, or the restricted codename.
# ---------------------------------------------------------------------------


class CanonicalMergesError(ValueError):
    """Raised when the hash-pinned `--canonical-merges` census input fails its
    SHA-256 pin, its FROZEN exact-shape schema, its transitivity guard, or
    (for a release build) its semantic-ratification assertion."""


class CompositionDatesError(ValueError):
    """Raised when the hash-pinned `--composition-dates` input fails its
    SHA-256 pin or its FROZEN exact schema, or a composition-date value is
    unparseable / normalizes out of the [500, 1600] CE window."""


class SeftjaDatesError(ValueError):
    """Raised when the hash-pinned `--seftja-dates` input fails its SHA-256
    pin or its FROZEN exact `{year:int, basis:str}` schema."""


class ConflictingSameMemberDateError(ValueError):
    """Raised (bake plan §4.3, Codex R5-HIGH) when, WITHIN a single date map,
    TWO OR MORE distinct raw ids crosswalk to the SAME opaque `w000xxx` (the
    representative OR any other group member) carrying TWO OR MORE DISTINCT
    normalized years. Crosswalk injectivity is FORWARD-ONLY, so this reverse
    collision is a data-quality defect in the pinned date input requiring
    upstream correction -- NEVER resolved by first-row/minimum precedence. A
    `--release` HALT. Masking: the message carries the opaque id + the
    conflicting numeric years ONLY, never a raw id."""


class DateCoverageError(RuntimeError):
    """Raised (Codex #5) when the production date-join coverage gate HALTs a
    `--release` build: a zero candidate universe, or `pair_coverage` below
    the absolute floor."""


def _reject_duplicate_keys(pairs):
    """`object_pairs_hook` that HARD-REJECTS a repeated key at ANY nesting
    level (bake plan §4 shared JSON-parsing requirement, Codex R4-MEDIUM) --
    the bare stdlib decoder would silently last-write-wins."""
    seen: Dict = {}
    for k, v in pairs:
        if k in seen:
            raise ValueError(f"duplicate JSON key {k!r} rejected (strict parse)")
        seen[k] = v
    return seen


def _json_loads_strict(text: str):
    return json.loads(text, object_pairs_hook=_reject_duplicate_keys)


# The CLOSED top-level key allowlist for the census file (bake plan §4.1).
# Only `merges` + `dropped_by_135` are READ; the other ten are
# tolerated-but-ignored (present in the real handoff, never consumed). A
# top-level key OUTSIDE this set is REJECTED.
_CANONICAL_MERGES_TOP_KEYS = frozenset({
    "merges", "dropped_by_135", "source", "canonical_priority", "owner_ratified",
    "ratified_at", "relations_policy", "chronological_rule_examples", "contested",
    "provisional_relations_measurement_only", "residual_direct", "notes",
})
_MERGE_ENTRY_KEYS = frozenset({"members_w", "canonical_w", "owner_verdict"})
_W_ID_RE = re.compile(r"^w\d{6}$")
_MERGE_APPROVE = "approve"
# D-14 semantic-ratification constants (bake plan §2/§4.1) -- the one merge
# whose canonical rep is the M-source side because the Sefaria copy is dropped.
_D14_MEMBERS = frozenset({"w000452", "w001239"})
_D14_CANONICAL = "w000452"
_D14_DROPPED = "w001239"
_EXPECTED_APPROVE_MERGES = 16


def _is_w_id(x) -> bool:
    return isinstance(x, str) and bool(_W_ID_RE.match(x))


def load_canonical_merges(
    path, *, sha256: Optional[str] = None, require_release_semantics: bool = False,
) -> Dict:
    """Load + validate the REQUIRED hash-pinned `--canonical-merges` census
    input (bake plan §4.1, Codex #B2). Returns
    `{cross_corpus_map, dropped, sha256, approve_count}`.

    Order of enforcement: SHA-256 pin (if supplied) -> strict duplicate-key
    JSON parse -> closed top-level key allowlist -> per-entry FROZEN exact
    shape (EXACTLY `members_w`/`canonical_w`/`owner_verdict`, w000xxx-shaped,
    >=2 DISTINCT members, canonical_w a member) -> approve-only load ->
    transitivity guard (no id in two approved groups) -> (release only) the
    16-merge / drop=={w001239} / D-14-flip semantic-ratification assertion.

    Masking: members are referenced ONLY by opaque w000xxx id -- never a title
    or the restricted codename.
    """
    p = Path(path)
    if not p.exists():
        raise CanonicalMergesError(f"--canonical-merges file not found: {path}")
    actual_sha = _hash_file(p)
    if sha256 is not None and actual_sha != sha256:
        raise CanonicalMergesError(
            "--canonical-merges SHA-256 pin mismatch (hash gate) -- refusing to load"
        )
    try:
        doc = _json_loads_strict(p.read_text(encoding="utf-8"))
    except ValueError as e:
        raise CanonicalMergesError(f"--canonical-merges parse error: {e}") from e
    if not isinstance(doc, dict):
        raise CanonicalMergesError("--canonical-merges top-level value must be a JSON object")
    extra = set(doc) - _CANONICAL_MERGES_TOP_KEYS
    if extra:
        raise CanonicalMergesError(
            f"--canonical-merges has {len(extra)} top-level key(s) outside the closed allowlist"
        )
    merges = doc.get("merges", [])
    dropped_raw = doc.get("dropped_by_135", [])
    if not isinstance(merges, list):
        raise CanonicalMergesError("--canonical-merges 'merges' must be a list")
    if not isinstance(dropped_raw, list):
        raise CanonicalMergesError("--canonical-merges 'dropped_by_135' must be a list")

    dropped = set()
    for d in dropped_raw:
        if not _is_w_id(d):
            raise CanonicalMergesError("--canonical-merges dropped_by_135 entry not w000xxx-shaped")
        dropped.add(d)

    cross_corpus_map: Dict[str, str] = {}
    seen_ids: set = set()
    approve_count = 0
    d14_ok = False
    for i, m in enumerate(merges):
        if not isinstance(m, dict):
            raise CanonicalMergesError(f"merges[{i}] must be a JSON object")
        if set(m) != _MERGE_ENTRY_KEYS:
            raise CanonicalMergesError(
                f"merges[{i}] must carry EXACTLY {sorted(_MERGE_ENTRY_KEYS)} (no missing/extra field)"
            )
        members = m["members_w"]
        canon = m["canonical_w"]
        verdict = m["owner_verdict"]
        if not isinstance(verdict, str):
            raise CanonicalMergesError(f"merges[{i}] owner_verdict must be a string")
        if not (isinstance(members, list) and all(_is_w_id(x) for x in members)):
            raise CanonicalMergesError(f"merges[{i}] members_w must be a list of w000xxx-shaped strings")
        distinct = set(members)
        if len(distinct) < 2:
            raise CanonicalMergesError(f"merges[{i}] members_w must have >=2 DISTINCT ids")
        if not _is_w_id(canon):
            raise CanonicalMergesError(f"merges[{i}] canonical_w must be w000xxx-shaped")
        if canon not in distinct:
            raise CanonicalMergesError(f"merges[{i}] canonical_w must be an element of its own members_w")
        if verdict != _MERGE_APPROVE:
            continue
        group_ids = distinct | {canon}
        overlap = group_ids & seen_ids
        if overlap:
            raise CanonicalMergesError(
                f"transitivity guard: {len(overlap)} id(s) appear in >1 approved merge group"
            )
        seen_ids |= group_ids
        approve_count += 1
        for member in members:
            cross_corpus_map[member] = canon
        if distinct == _D14_MEMBERS and canon == _D14_CANONICAL:
            d14_ok = True

    if require_release_semantics:
        problems = []
        if approve_count != _EXPECTED_APPROVE_MERGES:
            problems.append(f"expected {_EXPECTED_APPROVE_MERGES} approve merges, got {approve_count}")
        if dropped != {_D14_DROPPED}:
            problems.append(f"dropped_by_135 must equal {{{_D14_DROPPED!r}}}")
        if not d14_ok:
            problems.append("D-14 flip absent (the w000452/w001239 group must have canonical_w=w000452)")
        if problems:
            raise CanonicalMergesError(
                "--canonical-merges semantic-ratification assertion failed: " + "; ".join(problems)
            )

    return {
        "cross_corpus_map": cross_corpus_map,
        "dropped": dropped,
        "sha256": actual_sha,
        "approve_count": approve_count,
    }


# ---------------------------------------------------------------------------
# Hash-pinned date inputs (bake plan §4.3, Codex #5) -- each parsed against a
# FROZEN exact schema that REJECTS anything else. Every date normalizes to a
# NUMERIC YEAR before any use -- the raw descriptive string is NEVER persisted.
# ---------------------------------------------------------------------------

# The plausible-composition window (bake plan §4.3): a normalized year outside
# this inclusive bound HALTS the build (never silently UNKNOWN / clamped).
# This bound governs the M-source `--composition-dates` corpus.
# WIDENED 500 -> 100 (135-07 amendment, owner-directed 2026-07-26): the
# original floor assumed the corpus is entirely medieval, but that was an
# artifact of the upstream date-emitter's own [500,1600] filter silently
# dropping the CLASSICAL strata (Mishnaic/Talmudic-era base texts) whose
# dates DO exist in the owner date source. Those recovered years now enter
# this table directly, so the floor matches the decoupled SEF/JA floor below.
# ANTIQUITY-CLAMP convention (same amendment): a work whose composition
# predates 100 CE (biblical / Second-Temple era) is recorded AT the floor
# (100) -- order-preserving for every D-17 comparison against a co-claimant
# dated >= 200 (delta >= 100 still demotes), while pairs wholly inside
# [100,199] resolve kept_tie (conservative fail-safe, never a wrong
# demotion). The anti-corruption rationale of the original R6-HIGH gate is
# preserved: the floor still rejects near-zero / negative / absurd values.
_COMPOSITION_YEAR_MIN = 100
_COMPOSITION_YEAR_MAX = 1600
# 135-07 recovered-strata RELEASE contract (Codex window-widen review, item 3):
# a regenerated upstream table that silently regressed the classical-strata
# recovery (the upstream emitter's own [500,1600] window dropping them again)
# must never pass `--release`, even if an operator re-pins its SHA. These
# minima pin the known-good distribution of the recovered strata (7,277
# delivered + 166 recovered = 7,443; 166 pre-500 incl. 39 antiquity-floor
# clamps); a legitimate future refresh can only GROW them. Applied ONLY to a
# `--release` build with a `--composition-dates` input (fixtures unaffected).
_COMPOSITION_RELEASE_MIN_ENTRIES = 7443
_COMPOSITION_RELEASE_MIN_PRE500 = 166
_COMPOSITION_RELEASE_MIN_AT_FLOOR = 39
# DECOUPLED SEF/JA window (135-07 amendment, owner decision 2026-07-24):
# the interim SEF/JA `--seftja-dates` corpus legitimately includes CLASSICAL
# base texts (Mishnaic ~150, Talmudic/Amoraic ~300) that the medieval M-source
# corpus does not, so its floor is decoupled from `_COMPOSITION_YEAR_MIN` and
# lowered to admit those genuine early-canonical anchors (they act as
# earlier-side D-17 chronological demoters). The upper bound is unchanged. The
# anti-corruption rationale of the original R6-HIGH gate is preserved: a floor
# still rejects near-zero / negative / absurd values; only the medieval-only
# assumption is corrected. See bake plan §4.3 (dated amendment).
_SEFTJA_YEAR_MIN = 100
_SEFTJA_YEAR_MAX = 1600
# The FIXED, HARDCODED range-separator set (bake plan §4.3 -- data-file-driven
# designators, but the separator must be identical across every date-table
# revision). U+002D HYPHEN-MINUS, U+2013 EN DASH, U+2014 EM DASH.
_RANGE_SEPARATORS = frozenset({"-", "–", "—"})
# The FIXED allowed residual punctuation (bake plan §4.3): comma, period, parens.
_ALLOWED_RESIDUAL = frozenset({",", ".", "(", ")"})
_SEFTJA_ENTRY_KEYS = frozenset({"year", "basis"})
_COMPOSITION_TOP_KEYS = frozenset({
    "century_designators", "range_designators", "era_qualifiers", "dates",
})

# D-17 constants (bake plan §4.3). DELTA=100y cited to chrono_date_coverage.md;
# MIN_ML=200 = the frozen minimum distinctive span.
D17_DELTA_YEARS = 100
D17_MIN_ML = 200
LEVER1_COVERAGE_CLIFF = 0.45
# Mirrors shared/discovery_band_labels.STRICT_FLOOR (D-07) -- the CERT-01 pass
# threshold. Kept as a local literal so this stdlib-only build script never
# imports the values module (avoids a build->shared coupling); the verifier's
# gate 12 keeps the SAME literal mirror for its own checks (see
# scripts/verify_discovery_sidecar.py's own `_STRICT_FLOOR`).
STRICT_FLOOR_FROZEN = 0.85
# Mirrors shared/discovery_band_labels.MEASUREMENT_STATUSES (135-05 closed
# vocab) as a local literal, same rationale as STRICT_FLOOR_FROZEN above --
# used by D-02a's widened `_validate_precision_spec` cross-check (136-06,
# docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment 2026-08-02).
MEASUREMENT_STATUSES_FROZEN = frozenset({
    "not_measured", "measured_pass", "measured_fail", "insufficient_evidence",
})


# ---------------------------------------------------------------------------
# SEED-029 page-coverage normalizer (bake plan §4.4 -- 135-07 fix).
#
# The Lever-1 routing basis is `coverage = matched_letters / page_norm_letters`
# (frozen manifest `coverage_def`, discovery-band-labels-v1.md §3.1). This is a
# FAITHFUL, letter-count-only port of
# `same_work_spike/probe/scripts/normalize.py::norm_stream` -- the SEED-029
# union-view normalizer that DEFINED the denominator when the page-level bands
# (94.0% / 91.7% / 37.5%) were validated. The port is proven byte-exact by the
# 135-07 PART-2 replication gate (tests/test_discovery_coverage_replication.py):
# it reproduces every graded unit's stored `cov` and the three precision bands.
#
# Masking-safe: this is a GENERIC Hebrew base-letter normalizer expressed
# ENTIRELY in ASCII code points -- no restricted content, no literal Hebrew
# glyph in this source file. Only the derived INTEGER stream length ever leaves
# the function (same masking posture as `matched_letters`); the normalized text
# itself is never returned or persisted.
# ---------------------------------------------------------------------------

# Hebrew base-letter range: U+05D0 (alef) .. U+05EA (tav).
_HEB_MIN, _HEB_MAX = 0x05D0, 0x05EA
# Final-form -> base-form fold, keyed BY CODE POINT so no literal Hebrew glyph
# appears here (final kaf/mem/nun/pe/tsadi -> their base forms). Verbatim to
# normalize.py's FINAL_FOLD. Every final form (U+05DA/DD/DF/E3/E5) already lies
# inside [_HEB_MIN, _HEB_MAX], so the fold does not change the stream LENGTH --
# it is ported for exact fidelity to the source normalizer, not for the count.
_FINAL_FOLD = {
    0x05DA: chr(0x05DB),  # final kaf   -> kaf
    0x05DD: chr(0x05DE),  # final mem   -> mem
    0x05DF: chr(0x05E0),  # final nun   -> nun
    0x05E3: chr(0x05E4),  # final pe    -> pe
    0x05E5: chr(0x05E6),  # final tsadi -> tsadi
}


def norm_stream_letter_count(text: Optional[str]) -> int:
    """`len(norm_stream(text))` -- the SEED-029 space-free normalized Hebrew
    base-letter stream length (= `page_norm_letters`, the Lever-1 coverage
    denominator).

    Faithful port of `same_work_spike/probe/scripts/normalize.py::norm_stream`'s
    letter selection: NFC-normalize -> fold final letters to base -> KEEP ONLY
    the Hebrew base letters U+05D0..U+05EA. EVERYTHING else is a separator and
    is dropped -- nikud / cantillation / ALL Unicode combining marks (incl. the
    Judeo-Arabic upper dot U+0307), geresh / gershayim / quotes / apostrophes,
    brackets, spaces, digits, and Latin. Returns the integer stream length only.
    """
    if not text:
        return 0
    n = 0
    for ch in unicodedata.normalize("NFC", text):
        folded = _FINAL_FOLD.get(ord(ch))
        code = ord(ch) if folded is None else ord(folded)
        if _HEB_MIN <= code <= _HEB_MAX:
            n += 1
    return n


def compute_page_coverage(matched_letters: Optional[int], page_norm_letters: Optional[int]) -> Optional[float]:
    """The Lever-1 coverage metric: `min(1.0, matched_letters /
    page_norm_letters)`, clamped to [0, 1] (bake plan §4.4 `coverage_def`).

    Returns None when `matched_letters` is unknown (no routing basis). Guards
    the zero / missing denominator (an all-non-Hebrew page, or a page whose text
    is absent from the research DB) -> coverage 0.0 (the row routes to
    review_only under the 0.45 cliff -- fail-closed, never a ZeroDivisionError).
    """
    if matched_letters is None:
        return None
    if not page_norm_letters:  # None or 0
        return 0.0
    return min(1.0, matched_letters / page_norm_letters)


# ---------------------------------------------------------------------------
# 136-11 (D-08a / D-10a): PERSISTING the two metrics above.
#
# `compute_page_coverage` and `norm_stream_letter_count` are DELIBERATELY left
# untouched -- the computation was always right, only its persistence was
# missing (the value was computed at ingestion, used for Lever-1 routing, and
# then thrown away because `_mk_evidence`'s returned dict had no `coverage`
# key). Everything below is about STORING it, never about recomputing it.
# ---------------------------------------------------------------------------

COVERAGE_STATUS_MEASURED = "measured"
COVERAGE_STATUS_NO_DENOMINATOR = "no_denominator"
COVERAGE_STATUS_NOT_APPLICABLE = "not_applicable"

# The closed validity vocabulary, mirroring the `coverage_status` CHECK in the
# DDL above and the schema doc's Amendment 2026-08-02 (A).
COVERAGE_STATUSES = frozenset({
    COVERAGE_STATUS_MEASURED,
    COVERAGE_STATUS_NO_DENOMINATOR,
    COVERAGE_STATUS_NOT_APPLICABLE,
})

COVERAGE_PPM_SCALE = 1_000_000


def coverage_ppm_and_status(
    evidence_source: Optional[str],
    coverage: Optional[float],
    page_norm_letters: Optional[int],
) -> Tuple[Optional[int], str]:
    """`(coverage_ppm, coverage_status)` for one evidence row.

    The validity axis is SEPARATE from the value axis on purpose (T-136-11-04).
    `compute_page_coverage` returns `0.0` when the denominator is missing, which
    is NOT the same fact as a genuine near-zero match -- storing that 0.0 as a
    real coverage would understate a match that was never measurable. So:

      * propagated family -> `(None, 'not_applicable')`. Not a display choice:
        those rows have no page-length denominator at all (D-08a).
      * direct family, denominator present and coverage computed ->
        `(round(coverage * 1e6), 'measured')`.
      * direct family, denominator zero/missing (or coverage never computed for
        want of a matched-letters basis) -> `(None, 'no_denominator')`. Both
        sub-cases mean the SAME thing to a reader -- we could not measure -- and
        the schema's closed three-value enum has no fourth token for them; a
        finer split would need its own dated schema amendment.

    Direct family ONLY, per D-08a: a surface shows, sorts and filters the
    percentage for `track1_direct` rows and shows nothing at all for propagated
    ones.
    """
    if evidence_source != ids.EVIDENCE_SOURCE_TRACK1_DIRECT:
        return None, COVERAGE_STATUS_NOT_APPLICABLE
    if coverage is None or not page_norm_letters:
        return None, COVERAGE_STATUS_NO_DENOMINATOR
    ppm = int(round(coverage * COVERAGE_PPM_SCALE))
    # `compute_page_coverage` already clamps to [0, 1]; clamp again so a
    # rounding artefact can never store an out-of-range fixed-point value.
    return max(0, min(COVERAGE_PPM_SCALE, ppm)), COVERAGE_STATUS_MEASURED


def evidence_band_rank(evidence_source: Optional[str], confidence_band: Optional[str]) -> int:
    """The MATERIALIZED band-rank sort key -- delegating to the runtime
    lattice, never to a second copy of it.

    `_BAND_RANK_ORDER` / `_band_rank` are imported from
    `shared.discovery_service` at the top of this file, so the stored key and
    the ordering the service sorts by are the same object by construction
    (T-136-11-02). Lower is stronger; an unknown pair ranks last, exactly as it
    does at runtime.
    """
    return _runtime_band_rank(evidence_source, confidence_band)


# The lattice is imported, never redeclared -- this reference exists so a reader
# (and a grep) can see WHICH ordering table is in force here.
BAND_RANK_LATTICE_SIZE = len(_BAND_RANK_ORDER)


def _verify_input_sha256(path, expected, *, exc, label) -> str:
    p = Path(path)
    if not p.exists():
        raise exc(f"{label} file not found: {path}")
    actual = _hash_file(p)
    if expected is not None and actual != expected:
        raise exc(f"{label} SHA-256 pin mismatch (hash gate) -- refusing to load")
    return actual


def parse_seftja_dates(path, *, sha256: Optional[str] = None) -> Dict[str, int]:
    """Parse the hash-pinned `--seftja-dates` input against its FROZEN exact
    schema (bake plan §4.3): a JSON object mapping a raw source-side id to an
    object with EXACTLY `{year:int, basis:str}`. `basis` is validated then
    DISCARDED (only the numeric year is used, never persisted). A missing/
    non-integer year, a missing/non-string basis, a third key, or a year
    outside the DECOUPLED SEF/JA window [_SEFTJA_YEAR_MIN, _SEFTJA_YEAR_MAX]
    (= [100, 1600]; lower than the M-source `--composition-dates` floor of 500
    so genuine classical base texts are admitted -- 135-07 amendment) is
    REJECTED. Returns `{raw_id: year}`."""
    _verify_input_sha256(path, sha256, exc=SeftjaDatesError, label="--seftja-dates")
    try:
        doc = _json_loads_strict(Path(path).read_text(encoding="utf-8"))
    except ValueError as e:
        raise SeftjaDatesError(f"--seftja-dates parse error: {e}") from e
    if not isinstance(doc, dict):
        raise SeftjaDatesError("--seftja-dates top-level value must be a JSON object")
    out: Dict[str, int] = {}
    for raw_id, val in doc.items():
        if not isinstance(val, dict) or set(val) != _SEFTJA_ENTRY_KEYS:
            raise SeftjaDatesError(
                f"--seftja-dates entry must carry EXACTLY {sorted(_SEFTJA_ENTRY_KEYS)}"
            )
        year = val["year"]
        basis = val["basis"]
        # bool is a subclass of int -- reject it explicitly (year is never a flag).
        if not isinstance(year, int) or isinstance(year, bool):
            raise SeftjaDatesError("--seftja-dates 'year' must be a JSON integer")
        if not isinstance(basis, str):
            raise SeftjaDatesError("--seftja-dates 'basis' must be a JSON string")
        if not (_SEFTJA_YEAR_MIN <= year <= _SEFTJA_YEAR_MAX):
            raise SeftjaDatesError(
                f"--seftja-dates 'year' {year} outside the SEF/JA composition window "
                f"[{_SEFTJA_YEAR_MIN}, {_SEFTJA_YEAR_MAX}]"
            )
        out[raw_id] = year  # basis discarded
    return out


def normalize_composition_date(
    value: str, *, century_designators, range_designators, era_qualifiers,
) -> int:
    """FROZEN normalizer (bake plan §4.3): convert ONE M-source composition
    date STRING to ONE integer CE year via EXACTLY three designator-driven
    categories -- (i) explicit year -> that year; (ii) century form -> midpoint
    100*(N-1)+50; (iii) bounded range -> midpoint floor((earliest+latest)/2).
    TOKENIZE-FIRST (digit runs located directly in the original string, never
    punctuation-stripped first). A present-but-unparseable value OR one
    normalizing outside [500, 1600] raises `CompositionDatesError` (a HALT,
    never a silent UNKNOWN). Designator vocabularies are DATA (owner-held,
    hash-pinned) -- never hardcoded here."""
    if not isinstance(value, str):
        raise CompositionDatesError("composition-date value must be a JSON string")
    s = value.strip()

    def _matches(designators):
        return [d for d in designators if d and d in s]

    century_hits = _matches(century_designators)
    range_hits = _matches(range_designators)
    if century_hits and range_hits:
        raise CompositionDatesError(f"ambiguous dual-designator match in composition date {value!r}")
    if century_hits:
        category = "century"
    elif range_hits:
        category = "range"
    else:
        category = "explicit"

    runs = re.findall(r"\d+", s)  # maximal decimal-digit runs, left-to-right
    if category == "century":
        if len(runs) != 1:
            raise CompositionDatesError("century form requires EXACTLY one digit run")
        n = int(runs[0])
        if not (1 <= n <= 16):
            raise CompositionDatesError(f"century ordinal {n} outside [1, 16]")
        year = 100 * (n - 1) + 50
    elif category == "range":
        if len(runs) != 2:
            raise CompositionDatesError("range form requires EXACTLY two digit runs")
        earliest, latest = int(runs[0]), int(runs[1])
        if earliest >= latest:
            raise CompositionDatesError("range earliest must be < latest")
        # the SOLE non-whitespace char between the two runs must be a pinned dash.
        i0 = s.index(runs[0])
        between = s[i0 + len(runs[0]):s.index(runs[1], i0 + len(runs[0]))]
        between_core = "".join(ch for ch in between if not ch.isspace())
        if between_core not in _RANGE_SEPARATORS:
            raise CompositionDatesError(
                "range separator between digit runs is not one of the pinned {-, en-dash, em-dash}"
            )
        year = (earliest + latest) // 2
    else:  # explicit
        if len(runs) != 1:
            raise CompositionDatesError("explicit-year form requires EXACTLY one digit run")
        year = int(runs[0])

    # Anchoring: after removing matched designators + era_qualifiers + digit
    # runs (+ range separator), only allowed punctuation may remain.
    residual = s
    for d in century_hits + range_hits + _matches(era_qualifiers):
        residual = residual.replace(d, " ")
    for run in runs:
        residual = residual.replace(run, " ", 1)
    for sep in _RANGE_SEPARATORS:
        residual = residual.replace(sep, " ")
    leftover = {ch for ch in residual if not ch.isspace()}
    stray = leftover - _ALLOWED_RESIDUAL
    if stray:
        raise CompositionDatesError(
            f"composition date {value!r} has unaccounted-for residual character(s)"
        )

    if not (_COMPOSITION_YEAR_MIN <= year <= _COMPOSITION_YEAR_MAX):
        raise CompositionDatesError(
            f"normalized year {year} outside [{_COMPOSITION_YEAR_MIN}, {_COMPOSITION_YEAR_MAX}]"
        )
    return year


def parse_composition_dates(path, *, sha256: Optional[str] = None) -> Dict[str, int]:
    """Parse the hash-pinned `--composition-dates` input, accepting EITHER of two
    schemas (branch selection is robust + unambiguous), and return `{raw_id: year}`:

    (A) **FROZEN designator+string form** (bake plan §4.3): a JSON object with
        EXACTLY the four keys `century_designators` / `range_designators`
        (non-empty lists of non-empty strings), `era_qualifiers` (a list --
        possibly empty -- of non-empty strings), and `dates` (raw_id -> date
        STRING). Each date value is normalized to ONE integer year via
        `normalize_composition_date`; the recognized designator vocabulary is
        READ FROM THIS SAME PINNED FILE (never hardcoded, never in a fixture).

    (B) **FLAT pre-normalized form** (Amendment 2026-07-24 — the delivered
        production artifact): a NON-EMPTY JSON object mapping raw source-side
        ids to **integer** CE years. The production chrono pipeline already did
        the (range-aware) anchoring and hands over explicit anchored years, so
        no descriptive strings enter the input (masking-cleaner). Each value is
        validated as an `int` (a `bool` -- an `int` subclass -- is rejected)
        within `[_COMPOSITION_YEAR_MIN, _COMPOSITION_YEAR_MAX]`; an out-of-range
        or non-int value HALTs (never a silent skip).

    Anything else (an empty object, mixed/typed values, or extra keys) is
    ambiguous/malformed and HALTs with `CompositionDatesError`."""
    _verify_input_sha256(path, sha256, exc=CompositionDatesError, label="--composition-dates")
    try:
        doc = _json_loads_strict(Path(path).read_text(encoding="utf-8"))
    except ValueError as e:
        raise CompositionDatesError(f"--composition-dates parse error: {e}") from e
    if not isinstance(doc, dict):
        raise CompositionDatesError("--composition-dates top-level value must be a JSON object")

    # --- Branch (A): FROZEN designator+string form -------------------------
    if set(doc) == _COMPOSITION_TOP_KEYS:
        def _nonempty_str_list(name, *, allow_empty_list):
            v = doc[name]
            if not isinstance(v, list):
                raise CompositionDatesError(f"--composition-dates '{name}' must be a list")
            if not allow_empty_list and not v:
                raise CompositionDatesError(f"--composition-dates '{name}' must be non-empty")
            for el in v:
                if not isinstance(el, str) or el == "":
                    raise CompositionDatesError(
                        f"--composition-dates '{name}' elements must be non-empty strings"
                    )
            return v

        century = _nonempty_str_list("century_designators", allow_empty_list=False)
        ranges = _nonempty_str_list("range_designators", allow_empty_list=False)
        eras = _nonempty_str_list("era_qualifiers", allow_empty_list=True)
        dates = doc["dates"]
        if not isinstance(dates, dict):
            raise CompositionDatesError("--composition-dates 'dates' must be an object")

        out: Dict[str, int] = {}
        for raw_id, val in dates.items():
            if not isinstance(val, str):
                raise CompositionDatesError("--composition-dates 'dates' values must be JSON strings")
            out[raw_id] = normalize_composition_date(
                val, century_designators=century, range_designators=ranges, era_qualifiers=eras)
        return out

    # --- Branch (B): FLAT pre-normalized {raw_id: int CE year} form --------
    # (bool is an int subclass; detection routes a bool-carrying doc here so the
    # per-value guard below HALTs on it with a precise message.)
    if doc and all(isinstance(v, int) for v in doc.values()):
        flat: Dict[str, int] = {}
        for raw_id, year in doc.items():
            if not isinstance(year, int) or isinstance(year, bool):
                raise CompositionDatesError(
                    f"--composition-dates flat value for {raw_id!r} must be a JSON integer"
                )
            if not (_COMPOSITION_YEAR_MIN <= year <= _COMPOSITION_YEAR_MAX):
                raise CompositionDatesError(
                    f"--composition-dates flat year {year} for {raw_id!r} outside "
                    f"[{_COMPOSITION_YEAR_MIN}, {_COMPOSITION_YEAR_MAX}]"
                )
            flat[raw_id] = year
        return flat

    raise CompositionDatesError(
        "--composition-dates must be EITHER an object with EXACTLY "
        f"{sorted(_COMPOSITION_TOP_KEYS)} (designator+string form) OR a non-empty "
        "object mapping raw ids to integer CE years (flat pre-normalized form)"
    )


def assert_composition_release_contract(dates_map: Dict[str, int]) -> None:
    """135-07 recovered-strata release contract (Codex window-widen review,
    item 3): HALT a `--release` build whose composition-date table regressed
    below the known-good recovered distribution -- the failure mode is an
    upstream re-emit with the old [500,1600] window silently dropping the
    classical strata again, then an operator re-pinning the regressed file.
    The SHA pin cannot catch a deliberate re-pin; this semantic gate can."""
    n = len(dates_map)
    n_pre500 = sum(1 for v in dates_map.values() if v < 500)
    n_floor = sum(1 for v in dates_map.values() if v == _COMPOSITION_YEAR_MIN)
    if (n < _COMPOSITION_RELEASE_MIN_ENTRIES
            or n_pre500 < _COMPOSITION_RELEASE_MIN_PRE500
            or n_floor < _COMPOSITION_RELEASE_MIN_AT_FLOOR):
        raise CompositionDatesError(
            "--composition-dates release contract violated: table regressed below "
            f"the 135-07 recovered-strata minima (entries {n} < "
            f"{_COMPOSITION_RELEASE_MIN_ENTRIES}, pre-500 {n_pre500} < "
            f"{_COMPOSITION_RELEASE_MIN_PRE500}, or at-floor {n_floor} < "
            f"{_COMPOSITION_RELEASE_MIN_AT_FLOOR}) -- likely an upstream re-emit "
            "with the old [500,1600] window; HALT"
        )


# ---------------------------------------------------------------------------
# Lever-1 coverage routing (bake plan §4.4) -- runs BEFORE D-17.
# ---------------------------------------------------------------------------

def apply_lever1_coverage(evidence_specs: List[Dict], *, threshold: float = LEVER1_COVERAGE_CLIFF) -> int:
    """Route each track1_direct witness spec whose page COVERAGE is `< threshold`
    to `routing_status='review_only'` with `routing_reason='low_coverage'`,
    recoverable (bake plan §4.4).

    135-07 field-name-collision FIX: the routing input is the per-spec
    `coverage` = `matched_letters / len(norm_stream(page_text))` (computed at
    ingestion, see `_ingest_tier_a` / `_ingest_e1_rows`), NOT the `density`
    column. The `density` column is a SEPARATE signal -- the normalized
    Levenshtein edit-DISTANCE match quality (`track1_match.accept_density`
    HARD-REJECTS > 0.35, so it is capped at 0.35 and can NEVER reach the 0.45
    cliff). Feeding `density` in as "coverage" demoted ~100% of witnesses and
    orphaned every shipped page (the bug this fixes). The cliff (0.45) is
    unchanged -- only the metric was wrong.

    `routing_reason='low_coverage'` (Codex R3-BLOCKER, bake plan §4.4) makes a
    Lever-1 demotion reconstructable from the shipped asset alone, distinct from
    a D-17 `'later_shared_text'` demotion. Returns the count demoted."""
    n = 0
    for e in evidence_specs:
        if e.get("evidence_source") != _TRACK1 or e.get("evidence_kind") != _WITNESS:
            continue
        cov = e.get("coverage")
        if cov is None:
            continue
        # Bake plan §4.4 cross-check (NON-fatal): the recomputed coverage and
        # the stored `density` edit-distance are DIFFERENT signals, but a
        # coverage that is impossibly high relative to a near-zero density can
        # indicate a matched_letters / denominator mismatch. Log, never route.
        _lever1_density_crosscheck(e, cov)
        if cov < threshold and e.get("routing_status") == _SHIPPED:
            e["routing_status"] = _REVIEW_ONLY
            e["routing_reason"] = _LOW_COVERAGE
            n += 1
    return n


def _lever1_density_crosscheck(e: Dict, coverage: float) -> None:
    """Bake plan §4.4 sanity cross-check (NON-fatal, never a routing input):
    a shipped-tier coverage (>= cliff) computed against a page whose stored
    `density` edit-distance signal is entirely absent is worth a one-line
    diagnostic on stderr. Deliberately conservative -- it NEVER raises and NEVER
    changes routing (that is the field-collision bug this plan removes)."""
    density = e.get("density")
    if density is None and coverage >= LEVER1_COVERAGE_CLIFF:
        print(
            f"[lever1-crosscheck] page={e.get('page_id')!r} work={e.get('work_id')!r} "
            f"coverage={coverage:.3f} ships but has no density edit-distance signal",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# D-17 chronological co-claim demotion (bake plan §4.3) -- pure pairwise over
# DISTINCT canonical_work_id groups; keeps the earliest SHIPPED, demotes each
# materially-later (>= DELTA) SHIPPED co-claimant on the shared span.
# ---------------------------------------------------------------------------

def _spans_overlap(a_start, a_end, b_start, b_end) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)


def _footprints_overlap(fa: List[Tuple[int, int]], fb: List[Tuple[int, int]]) -> bool:
    return any(_spans_overlap(x0, x1, y0, y1) for (x0, x1) in fa for (y0, y1) in fb)


def apply_d17_demotion(
    evidence_specs: List[Dict], *, cross_corpus_map: Optional[Dict[str, str]],
    year_by_canonical: Dict[str, Optional[int]], delta: int = D17_DELTA_YEARS,
    ml_floor: int = D17_MIN_ML, will_reband_tier_a: bool = False,
) -> List[Dict]:
    """Ordered-stateful, per-spec, earliest-first D-17 co-claim demotion (bake
    plan §4.3; 135-07 cascade fix).

    Groups the currently-SHIPPED track1_direct witness population by page ->
    `canonical_work_id` (Codex #4 -- a merged twin is never compared against
    itself) and processes each page's canonical groups EARLIEST-FIRST by
    resolved year, mutating `routing_status` in place as it goes. A later work
    W's spec `s` is demoted (`routing_status=review_only`,
    `routing_reason=later_shared_text`) iff SOME earlier work `k` (with
    `year[k] <= year[W] - delta`) has a CURRENTLY-SHIPPED spec whose span
    overlaps `s`. Because processing is earliest-first, every candidate `k` was
    fully processed before W, so k's shipped footprint is FINAL at the moment W
    is evaluated:
      * a FULLY-demoted k has an EMPTY shipped footprint -> it can never demote
        (fixes the Codex three-work counterexample: w1 demotes w2; the demoted
        w2 cannot then demote w3; w3 STAYS shipped);
      * a PARTIALLY-demoted k's still-shipped specs REMAIN valid demoters on
        their own spans -- a naive whole-group skip would wrongly leave some
        later specs shipped (the multi-footprint test guards this).

    Returns one masking-safe `discovery_routing_audit` row dict per considered
    decision (opaque ids + numeric years only):
      * every considered overlapping pair with an UNDATED member -> one
        'fail_safe_unknown_date' row (kept_work_id = lower canonical id);
      * every considered overlapping DATED pair WITHIN delta -> one 'kept_tie'
        row -- both UNCHANGED from the prior pairwise pass;
      * each ACTUAL demotion -> EXACTLY ONE 'demoted' row per demoted work,
        attributed to its EARLIEST valid demoter (earliest year, then id).

    DEFERRED to v2.1 (Option A, owner-ratified): a materially-later DATED pair
    that does NOT demote because the earlier reference is itself INVALID (no
    currently-shipped overlapping spec) emits NO audit row. The
    `kept_invalid_reference` provenance is intentionally NOT modelled here (no
    new `decision` enum value, no DDL change); its only effect is that those
    (bounded, negligible) pairs are absent from the coverage U/R. The verifier
    fail-closed cascade gate (commit 4a52641c) stays as the DB-level backstop.

    Population = currently-SHIPPED track1_direct witness specs with
    `matched_letters >= ml_floor`, EXCLUDING rows a §4.5 reband will condemn
    (`will_reband_tier_a` + `confidence_band==tier_a`, bake plan §6 step-0
    consultation)."""
    ccm = cross_corpus_map or {}

    def _canon(spec):
        return ids.canonical_work_id(spec["work_id"], ccm)

    # Build the shipped population, keyed per page -> canonical -> [specs].
    by_page: Dict[str, Dict[str, List[Dict]]] = {}
    for e in evidence_specs:
        if e.get("evidence_source") != _TRACK1 or e.get("evidence_kind") != _WITNESS:
            continue
        if e.get("routing_status") != _SHIPPED:
            continue
        ml = e.get("matched_letters")
        if ml is None or ml < ml_floor:
            continue
        if will_reband_tier_a and e.get("confidence_band") == _TIER_A:
            continue
        by_page.setdefault(e["page_id"], {}).setdefault(_canon(e), []).append(e)

    audit_rows: List[Dict] = []
    for page_id in sorted(by_page):
        groups = by_page[page_id]
        footprints = {
            c: [(s["span_start"], s["span_end"]) for s in specs]
            for c, specs in groups.items()
        }
        canons = sorted(groups)

        # (A) Pairwise fail_safe / kept_tie classification -- UNCHANGED pairwise
        # semantics. Materially-later DATED pairs (abs(delta) >= `delta`) emit
        # NO row here; the demotion decision AND its single 'demoted' row are
        # produced by the ordered per-spec pass (B) below.
        for i in range(len(canons)):
            for j in range(i + 1, len(canons)):
                lo, hi = canons[i], canons[j]
                if not _footprints_overlap(footprints[lo], footprints[hi]):
                    continue
                yr_lo = year_by_canonical.get(lo)
                yr_hi = year_by_canonical.get(hi)
                if yr_lo is None or yr_hi is None:
                    audit_rows.append({
                        "page_id": page_id, "kept_work_id": lo, "demoted_work_id": None,
                        "kept_year": yr_lo, "demoted_year": yr_hi, "delta_years": None,
                        "decision": "fail_safe_unknown_date", "routing_reason": None,
                    })
                    continue
                d = abs(yr_hi - yr_lo)
                if d < delta:
                    audit_rows.append({
                        # 136-12 / schema Amendment 2026-08-02 (F): a `kept_tie`
                        # row MUST name the OTHER member of the tie pair. It used
                        # to write NULL here, which made the pair
                        # unreconstructable from the audit table alone -- there
                        # was no way to tell WHICH two works were tied. That
                        # matters concretely: the main-pool rule's competition
                        # gate reads exactly these ties
                        # (`_page_competition_index`). Neither work is demoted by
                        # a tie (that is what "tie" means); `demoted_work_id`
                        # simply records the pair's second member, `hi`, against
                        # `kept_work_id = lo` -- and `lo`/`hi` are the sorted
                        # canonical ids, so the pairing is deterministic.
                        "page_id": page_id, "kept_work_id": lo, "demoted_work_id": hi,
                        "kept_year": yr_lo, "demoted_year": yr_hi, "delta_years": d,
                        "decision": "kept_tie", "routing_reason": None,
                    })
                # else: materially-later DATED pair -> deferred to pass (B); if
                # its earlier reference turns out to be invalid there, NO row is
                # emitted (the v2.1-deferred kept_invalid_reference provenance).

        # (B) Ordered-stateful, per-spec demotion. Process dated canonicals
        # EARLIEST-FIRST (year, then id) so a spec demoted here can no longer
        # act as a demoter for any later work evaluated afterwards.
        dated = sorted(
            (c for c in canons if year_by_canonical.get(c) is not None),
            key=lambda c: (year_by_canonical[c], c),
        )
        for w_idx, W in enumerate(dated):
            yW = year_by_canonical[W]
            attributed: Optional[Tuple[int, str]] = None  # earliest (year, id)
            for s in groups[W]:
                for k in dated[:w_idx]:  # strictly-earlier works, earliest-first
                    yk = year_by_canonical[k]
                    if yk > yW - delta:  # not materially earlier -> not a demoter
                        continue
                    if any(
                        ks["routing_status"] == _SHIPPED
                        and _spans_overlap(s["span_start"], s["span_end"],
                                           ks["span_start"], ks["span_end"])
                        for ks in groups[k]
                    ):
                        s["routing_status"] = _REVIEW_ONLY
                        s["routing_reason"] = _LATER_SHARED_TEXT
                        cand = (yk, k)
                        if attributed is None or cand < attributed:
                            attributed = cand
                        break  # spec demoted; attribute it to its earliest `k`
            if attributed is not None:
                kept_year, kept_id = attributed
                audit_rows.append({
                    "page_id": page_id, "kept_work_id": kept_id, "demoted_work_id": W,
                    "kept_year": kept_year, "demoted_year": yW,
                    "delta_years": abs(yW - kept_year),
                    "decision": "demoted", "routing_reason": _LATER_SHARED_TEXT,
                })
    return audit_rows


class RoutingAuditError(RuntimeError):
    """Raised when a `discovery_routing_audit` row would ship in a shape that
    makes the decision it records unreplayable -- today, a `kept_tie` row with
    no `demoted_work_id`."""


def assert_kept_tie_rows_name_their_pair(audit_rows: Iterable[Dict]) -> int:
    """Schema Amendment 2026-08-02 (F): no `kept_tie` row may be written with a
    NULL `demoted_work_id`.

    Asserted on the ROWS, before the INSERT -- a DB-level check would only
    catch it after the audit trail had already been written. `fail_safe_unknown_date`
    rows are deliberately NOT covered: the amendment scopes this rule to
    `kept_tie`, and a fail-safe row records that a date was missing, not that a
    pair competed."""
    offenders = [
        a for a in audit_rows
        if a.get("decision") == "kept_tie" and a.get("demoted_work_id") is None
    ]
    if offenders:
        raise RoutingAuditError(
            f"{len(offenders)} discovery_routing_audit row(s) with decision='kept_tie' carry a "
            "NULL demoted_work_id -- the tie pair would be unreconstructable from the audit "
            "alone, and the main-pool competition gate reads exactly these ties "
            "(schema Amendment 2026-08-02 (F))"
        )
    return len(offenders)


def compute_pair_coverage(audit_rows: List[Dict]) -> Tuple[int, int, float]:
    """Production coverage gate arithmetic (bake plan §4.3/§7 gate 9). `U` =
    every audit row; `R` = rows where BOTH years resolved (decision in
    demoted/kept_tie). Returns `(|R|, |U|, pair_coverage)`. Raises
    `DateCoverageError` on the zero-candidate case (never a 0/0 division)."""
    u = len(audit_rows)
    if u == 0:
        raise DateCoverageError(
            "date-coverage gate: zero candidate pairs (|U|=0) -- likely broken "
            "candidate generation upstream (never silently treated as 100%)"
        )
    r = sum(1 for a in audit_rows if a["decision"] in ("demoted", "kept_tie"))
    return r, u, r / u


class WorkOffsetsMissingError(RuntimeError):
    """A release build emitted track1_direct rows with no work-side offsets."""


# Positions of the fields this gate reads inside an emitted evidence-row TUPLE.
# Indexed rather than named because `assemble_claims_and_evidence` emits tuples
# for `executemany`; the constants are asserted against the INSERT column list by
# `test_the_release_offsets_gate_reads_the_right_tuple_positions`, so a column
# added in the middle cannot silently shift what this gate checks.
_EVIDENCE_TUPLE_EVIDENCE_SOURCE = 3
_EVIDENCE_TUPLE_W_START = -4
_EVIDENCE_TUPLE_W_END = -3
_EVIDENCE_TUPLE_ALIGNED_PAGE_START = -2
_EVIDENCE_TUPLE_ALIGNED_PAGE_END = -1


def assert_release_work_offsets(evidence_rows) -> int:
    """Gate 3, enforced on the EMITTED rows of a release build.

    Every `track1_direct` row must carry all four coordinates of one producer
    alignment (`w_start`/`w_end` plus the `aligned_page_*` side, Amendment (G)).
    A NULL means either the research DB had no `ref_spans_json` (a v2-era DB fed
    to a v3 release) or the projection failed silently -- both of which the plan
    says must halt, and neither of which anything checked.

    Returns the number of rows verified, so a caller can record that the gate saw
    a non-zero population: an assertion that passes over zero rows is the
    canonical false green, and this gate would otherwise pass on an empty build.

    Masking (D-25): counts only. No page id, work id or offset is echoed.
    """
    checked = 0
    missing = 0
    for row in evidence_rows:
        if row[_EVIDENCE_TUPLE_EVIDENCE_SOURCE] != _TRACK1:
            continue
        checked += 1
        if any(row[i] is None for i in (
            _EVIDENCE_TUPLE_W_START, _EVIDENCE_TUPLE_W_END,
            _EVIDENCE_TUPLE_ALIGNED_PAGE_START, _EVIDENCE_TUPLE_ALIGNED_PAGE_END,
        )):
            missing += 1
    if missing:
        raise WorkOffsetsMissingError(
            f"{missing} of {checked} track1_direct evidence row(s) in a RELEASE build "
            f"carry no work-side offsets. Either the research DB lacks "
            f"`ref_spans_json` (a v2-era DB cannot satisfy a v3 release) or the "
            f"page-span -> reference-span projection failed. Halting rather than "
            f"shipping an asset whose offsets column is silently unpopulated."
        )
    if not checked:
        raise WorkOffsetsMissingError(
            "a RELEASE build emitted ZERO track1_direct evidence rows, so the "
            "work-offsets gate verified nothing. A gate that passes over an empty "
            "population is a false green, not a pass."
        )
    return checked


def assert_pair_coverage_floor(audit_rows: List[Dict], *, floor: float) -> float:
    r, u, cov = compute_pair_coverage(audit_rows)
    if cov < floor:
        raise DateCoverageError(
            f"date-coverage gate: pair_coverage {cov:.4f} ({r}/{u}) below absolute floor {floor} -- HALT"
        )
    return cov


# ---------------------------------------------------------------------------
# CERT-01 FAIL-branch reband to screening_rb (bake plan §4.5, Codex #7/#B2) --
# a REBUILD INPUT consumed at band-assignment time, never a bare in-place
# UPDATE: apply_reband mutates specs BEFORE evidence_id + display selection.
# ---------------------------------------------------------------------------

_REBAND_TRIGGER_FIELDS = ("precision", "ci_low", "ci_high", "numerator", "denominator")


def resolve_reband_decision(precision_spec) -> Optional[Dict]:
    """Bake plan §6 step-0 pre-flight: inspect an optional `--precision-spec`
    for a tier_a `measurement_status='measured_fail'` outcome. Returns
    `{'trigger': {...5 fields...}}` when a valid FAIL reband is triggered,
    else None (absent spec, or a non-triggering `measured_pass`/
    `insufficient_evidence`). PREFLIGHT-GATED (Codex round-8 HIGH-2): a
    `measured_fail` row MUST carry all five non-NULL fields AND `ci_low<0.85`,
    else `InvalidPrecisionSpecError` (a HALT before any reband logic)."""
    if not precision_spec:
        return None
    fail_rows = [
        r for r in precision_spec
        if isinstance(r, dict)
        and r.get("confidence_band") == _TIER_A
        and r.get("measurement_status") == "measured_fail"
    ]
    if not fail_rows:
        return None
    r = fail_rows[0]
    missing = [f for f in _REBAND_TRIGGER_FIELDS if r.get(f) is None]
    if missing:
        raise InvalidPrecisionSpecError(
            f"measured_fail tier_a spec missing required field(s) {missing} "
            "(Codex round-8 HIGH-2: an inconsistent measured_fail must NEVER fire the reband)"
        )
    if not (isinstance(r["ci_low"], (int, float)) and r["ci_low"] < STRICT_FLOOR_FROZEN):
        raise InvalidPrecisionSpecError(
            "measured_fail tier_a spec must have ci_low < 0.85 (contradicts its own CI otherwise)"
        )
    return {"trigger": {f: r[f] for f in _REBAND_TRIGGER_FIELDS}}


def apply_reband(evidence_specs: List[Dict]) -> int:
    """Materialize the §4.5 reband as a REBUILD INPUT: every
    `confidence_band='tier_a'` spec becomes `screening_rb` +
    `routing_status='review_only'`, mutated BEFORE `assemble_claims_and_evidence`
    recomputes each row's `evidence_id` (over the NEW band, part of the frozen
    §2 id tuple) and each claim's routing-aware `display_evidence_id`. Leaves
    the row's PRE-EXISTING `routing_reason` unchanged (never `later_shared_text`
    -- that is exclusively D-17's output). Returns the rebanded-row count."""
    n = 0
    for e in evidence_specs:
        if e.get("confidence_band") == _TIER_A:
            e["confidence_band"] = _SCREENING_RB
            e["routing_status"] = _REVIEW_ONLY
            n += 1
    return n


def invalidate_reband_band_precision(bp_rows: List[Dict], decision: Dict) -> Tuple[List[Dict], Dict]:
    """Atomically invalidate BOTH the SOURCE `tier_a` and TARGET `screening_rb`
    band_precision rows (bake plan §4.5, Codex R3-HIGH/#B2): each gets
    `measurement_status='not_measured'` + NULL precision/ci_low/ci_high/
    numerator/denominator -- the rebanded rows changed both populations, so
    the legacy numbers are invalid (never a fabricated combined number). The
    triggering measured_fail numbers are preserved SEPARATELY in `meta` (never
    in the live band_precision table). Returns `(new_bp_rows, meta_extra)`."""
    trig = decision["trigger"]
    new_rows = []
    for r in bp_rows:
        r = dict(r)
        if r.get("evidence_source") == _TRACK1 and r.get("confidence_band") in (_TIER_A, _SCREENING_RB):
            r["measurement_status"] = "not_measured"
            for f in _REBAND_TRIGGER_FIELDS:
                r[f] = None
        new_rows.append(r)
    meta_extra = {
        "tier_a_reband_target": _SCREENING_RB,
        "tier_a_reband_count": None,  # filled in after apply_reband counts rows
        "tier_a_reband_trigger_precision": str(trig["precision"]),
        "tier_a_reband_trigger_ci_low": str(trig["ci_low"]),
        "tier_a_reband_trigger_ci_high": str(trig["ci_high"]),
        "tier_a_reband_trigger_numerator": str(trig["numerator"]),
        "tier_a_reband_trigger_denominator": str(trig["denominator"]),
    }
    return new_rows, meta_extra


def resolve_year_by_canonical(
    date_maps: List[Dict[str, int]], *, crosswalk: Dict[str, str],
    cross_corpus_map: Optional[Dict[str, str]],
    dropped_ids: frozenset = frozenset(),
) -> Dict[str, int]:
    """Resolve ONE year per canonical group from the raw-id -> year date maps,
    per the RATIFIED bake plan §4.3 year-resolution contract (NOT the former
    last-write-wins stub). Join is raw research id -> opaque work_id (crosswalk)
    -> canonical (census cross_corpus_map). Returns `{canonical_work_id: year}`
    -- numeric years only.

    Contract (bake plan §4.3):
      1. Dropped-member exclusion FIRST (Codex round-8 HIGH-1): any opaque in
         `dropped_ids` (§4.2 `dropped_by_135`) is removed BEFORE the
         representative AND the sibling lookups -- a dropped id's date can
         NEVER supply the year that drives a D-17 demotion.
      2. Representative-first: use the year keyed to the canonical
         REPRESENTATIVE's own opaque (an opaque is representative iff
         `canonical_work_id(opaque, ccm) == opaque`).
      3. Sibling-minimum fallback: if the representative has NO resolved date,
         use the MINIMUM resolved year among the group's OTHER (non-dropped)
         member opaques.
      4. Unknown: if no non-dropped member has a date, the canonical is ABSENT
         from the output (D-17 then no-ops that pair via fail_safe_unknown_date).
      5. Same-member-conflict HALT: if, WITHIN a single date map, MULTIPLE
         distinct raw ids crosswalk to the SAME (non-dropped) opaque with >=2
         DISTINCT years -> `ConflictingSameMemberDateError` (`--release` HALT).
         Same year across those raw ids is fine (no HALT).

    `min()` as the multi-year tie-break within `rep_years` is a determinism
    safeguard (a representative's date lives in exactly one map empirically, so
    `rep_years` is usually a single value)."""
    ccm = cross_corpus_map or {}
    rep_years: Dict[str, set] = {}   # canonical -> {years where opaque == canonical}
    sib_years: Dict[str, set] = {}   # canonical -> {years where opaque != canonical}
    for dm in date_maps:
        # (5) Same-member conflict detection WITHIN this single map, AFTER
        # dropped-exclusion (1): collapse each map to opaque -> year, HALTing on
        # a second DISTINCT year for the same opaque.
        year_by_opaque: Dict[str, int] = {}
        for raw_id, year in dm.items():
            opaque = crosswalk.get(raw_id)
            if opaque is None or opaque in dropped_ids:
                continue
            prev = year_by_opaque.get(opaque)
            if prev is not None and prev != year:
                raise ConflictingSameMemberDateError(
                    f"conflicting same-member dates within one date map for opaque "
                    f"{opaque}: {sorted({prev, year})} -- crosswalk injectivity is "
                    "forward-only; a same-member date conflict is an upstream "
                    "data-quality defect, never resolved by precedence (§4.3)"
                )
            year_by_opaque[opaque] = year
        # (2)/(3) Accumulate representative vs sibling years across maps.
        for opaque, year in year_by_opaque.items():
            canonical = ids.canonical_work_id(opaque, ccm)
            if opaque == canonical:
                rep_years.setdefault(canonical, set()).add(year)
            else:
                sib_years.setdefault(canonical, set()).add(year)
    out: Dict[str, int] = {}
    for canonical in set(rep_years) | set(sib_years):
        rep = rep_years.get(canonical)
        if rep:
            out[canonical] = min(rep)          # (2) representative-first
        else:
            sib = sib_years.get(canonical)
            if sib:
                out[canonical] = min(sib)      # (3) sibling-minimum fallback
        # (4) neither -> canonical stays ABSENT from `out` (unknown)
    return out


def create_schema(conn: sqlite3.Connection) -> None:
    """Emit the FROZEN DDL exactly (docs/specs/discovery-sidecar-schema-v1.md SS1)."""
    conn.executescript(_DDL)


# CD batch / schema Amendment 2026-08-12 (U): the seven tables the amendment
# adds, in their release-contract count-key order. Zero is a legitimate count
# -- the batch creates every one of them EMPTY (D-track populates the locus
# tables, C-track the region map + curated list, the frames the rest).
AMENDMENT_2026_08_12_COUNT_TABLES = (
    "locus_work",
    "locus_unit",
    "locus_edition",
    "discovery_region_map",
    "discovery_curated_quoter",
    "discovery_stratum_membership",
    "discovery_withholding",
)


def ingest_curated_quoter(conn: sqlite3.Connection, path: str) -> List[Tuple[str, str]]:
    """Ingest the tracked curated-quoter list (Amendment 2026-08-12 (R)) into
    `discovery_curated_quoter` and return its meta rows.

    Fail-closed: a canonical id the asset does not carry is a typo in an
    owner ruling, never a silent no-op -- the build stops. (Projection copies
    the table verbatim; a row whose work is later pruned publicly is inert
    there, which is documented at the projection rule.)"""
    with open(path, encoding="utf-8") as fh:
        curated = json.load(fh)
    list_version = str(curated["list_version"])
    ruled_date = str(curated["ruled_date"])
    known = {r[0] for r in conn.execute("SELECT DISTINCT canonical_work_id FROM works")}
    rows = []
    for entry in curated["entries"]:
        canonical_work_id = str(entry["canonical_work_id"])
        if canonical_work_id not in known:
            raise ValueError(
                f"curated quoter list names canonical_work_id {canonical_work_id!r} "
                "which this asset does not carry -- fail closed, never a silent no-op"
            )
        rows.append((list_version, canonical_work_id, ruled_date,
                     entry.get("note")))
    conn.executemany(
        "INSERT INTO discovery_curated_quoter "
        "(list_version, canonical_work_id, ruled_date, note) VALUES (?, ?, ?, ?)",
        rows,
    )
    return [("curated_quoter_version", list_version)]


def ingest_region_map(
    conn: sqlite3.Connection, path: str, crosswalk: Dict[str, str],
) -> List[Tuple[str, str]]:
    """Ingest the owner's region input (Amendment 2026-08-12 (R)) into
    `discovery_region_map`, re-keyed raw locus_ref_id -> opaque work_id via
    the crosswalk, and return its meta rows.

    PRECONDITION (fail-closed, stated in the error): the locus tables must
    already be populated -- a region ruling is ABOUT a locus unit, so this
    input is supplied together with the D-track locus import, never before.
    Every row must resolve: an unresolvable ref id or a (work, unit) pair
    absent from locus_unit is a data defect in an owner ruling, never skipped.

    The input's Hebrew `unit`/`work` display labels are deliberately NOT
    stored -- the sidecar carries opaque ids + the tri-state + `basis` only;
    labels live on locus_unit."""
    with open(path, encoding="utf-8") as fh:
        region = json.load(fh)
    region_version = str(region["frame"])
    unit_keys = {
        (w, o) for w, o in conn.execute("SELECT work_id, unit_ord FROM locus_unit")
    }
    if not unit_keys:
        raise ValueError(
            "region map supplied but locus_unit is EMPTY -- the region input rules "
            "on locus units, so --region-map is supplied together with the locus "
            "import (D-track), never before it"
        )
    rows = []
    for r in region["rows"]:
        work_id = crosswalk.get(r["locus_ref_id"])
        if work_id is None:
            raise ValueError(
                "region input references a locus_ref_id with no crosswalk entry "
                "(ref id withheld from this message; see the input file) -- fail closed"
            )
        unit_ord = int(r["unit_ord"])
        if (work_id, unit_ord) not in unit_keys:
            raise ValueError(
                f"region input rules on (work {work_id!r}, unit_ord {unit_ord}) "
                "which locus_unit does not carry -- an unanchored ruling, fail closed"
            )
        discriminative = r.get("discriminative")
        rows.append((
            region_version, work_id, unit_ord,
            None if discriminative is None else (1 if discriminative else 0),
            str(r["source"]), r.get("basis"),
        ))
    conn.executemany(
        "INSERT INTO discovery_region_map "
        "(region_version, work_id, unit_ord, discriminative, source, basis) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    return [("region_map_version", region_version)]


def population_lock_meta_rows(lock: Dict, lock_sha256: str) -> List[Tuple[str, str]]:
    """The population lock's COPIED meta constants (Amendment 2026-08-12 (S)).

    Copied, never recomputed -- recomputing them per asset would un-lock them;
    the projector's copy-vs-recompute table carries the same rule. The
    verifier's retention gate recomputes the CURRENT population and enforces
    the lock's own floors against these constants."""
    import shared.discovery_family as _fam

    families_in_lock = set(lock["by_family"])
    unknown = families_in_lock - set(_fam.FAMILIES)
    if unknown or lock.get("family_version") != _fam.FAMILY_VERSION:
        raise ValueError(
            "population lock does not match the fam-v1 contract "
            f"(family_version={lock.get('family_version')!r}, "
            f"unknown families={sorted(unknown)})"
        )
    rows: List[Tuple[str, str]] = [
        ("population_lock_version", str(lock["lock_version"])),
        ("population_lock_family_version", str(lock["family_version"])),
        ("population_lock_sha256", lock_sha256),
        ("population_lock_total", str(int(lock["total"]))),
        ("population_lock_retention_floor_overall",
         repr(float(lock["retention_floor_overall"]))),
        ("population_lock_retention_floor_per_family",
         repr(float(lock["retention_floor_per_family"]))),
        ("population_lock_daf_overrides",
         json.dumps(sorted(lock["daf_override_canonical_ids"]))),
    ]
    for family in _fam.FAMILIES:
        rows.append((f"population_lock_family_{family}",
                     str(int(lock["by_family"].get(family, 0)))))
    return rows


def amendment_2026_08_12_meta_rows(
    conn: sqlite3.Connection, *, reference_corpus_sha256: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """The CD-batch meta rows (schema Amendment 2026-08-12 (U)), computed by
    ONE function shared by the synthetic and real build paths so the two can
    never drift: the unconditional `locus_schema_version` marker, one
    release-contract count per new table, and -- when the bake was given one --
    the Contract-0 bake-side coordinate-basis pin (`reference_corpus_sha256`,
    the stream the evidence w_start/w_end offsets index). The locus-side twin
    (`locus_reference_corpus_sha256`) is written by the D-track import; the
    verifier's Contract-0 gate asserts the two EQUAL whenever locus_unit is
    populated."""
    rows: List[Tuple[str, str]] = [("locus_schema_version", LOCUS_SCHEMA_VERSION)]
    for table in AMENDMENT_2026_08_12_COUNT_TABLES:
        (n,) = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        rows.append((f"expected_rows_{table}", str(n)))
    if reference_corpus_sha256:
        rows.append(("reference_corpus_sha256", reference_corpus_sha256))
    # C-track: which parameterization this asset's `rendered_relation` values
    # were produced under. The verifier reconstructs it from HERE rather than
    # assuming deploy 1 -- a gate that recomputes under its own assumptions
    # would silently pass rows stored under different ones.
    rows.extend(relation_matrix.parameterization_meta_rows(
        relation_matrix.DEPLOY_1_PARAMETERIZATION))
    return rows


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
# v2-vocabulary constants (Phase 135, 135-05). Referenced by the v2 build
# logic (135-06: the D-17 later_shared_text demotion + the v2 band rename).
# Established here in lockstep with the frozen enum + DDL. As of the 135-06
# amendment (2026-07-24) the real-mode E1 track1_direct top tier IS flipped to
# `_HIGH_CONFIDENCE_ALGORITHMIC` in a v2 build (`build_claims_and_evidence(
# v2_bands=True)`, set by finalize_build when a `--canonical-merges` census is
# supplied); the synthetic byte-identical fixture and every v1 build keep
# writing `_EXPERT_VERIFIED`. The 135-07 pre-bake hardening COMPLETED the
# rename cascade: the frozen band_precision default
# (`_frozen_real_band_precision_rows(v2_bands=True)`, below) now ALSO keys the
# E1 top-tier row on `_HIGH_CONFIDENCE_ALGORITHMIC` in a v2 build, threaded
# from finalize_build's single `band_vocab_version` signal, so a pure-v2 asset
# passes the FULL (version-aware) verifier and the pure-v1 build stays
# byte-identical.
_LATER_SHARED_TEXT = ids.ROUTING_REASON_LATER_SHARED_TEXT
# Lever-1 coverage-demotion reason (bake plan §4.4, 135-07 coverage-metric fix).
_LOW_COVERAGE = ids.ROUTING_REASON_LOW_COVERAGE
_HIGH_CONFIDENCE_ALGORITHMIC = ids.CONFIDENCE_BAND_HIGH_CONFIDENCE_ALGORITHMIC

_OXFORD_PART = ids.MERGE_BASIS_OXFORD_PART
_PHYSICAL_JOIN = ids.MERGE_BASIS_PHYSICAL_JOIN

# Fabricated, neutral, masking-safe synthetic titles -- never a real research title.
#
# 136-12: the `genre` column now carries the CURATED value shape (Amendment
# (C)) -- the full "{parent} / {leaf}" path, or the explicit `Unassigned`
# sentinel. The parent/leaf names stay FABRICATED (never a real FJMS vocabulary
# node), because this dataset's whole discipline is that nothing in it derives
# from real data; only the SHAPE is contractual. `w000005`/`w000006` carry
# `Unassigned` so the golden fixture proves the sentinel survives into an asset
# as a real, queryable value rather than disappearing.
#
# `w000007` previously kept a NULL genre "so the pre-rebuild not-populated state
# stays exercised". That conflated two different states: the pre-rebuild state is
# an ENTIRELY unpopulated column, which check_works_genre_vocabulary already
# early-returns on, whereas one NULL among seven populated rows is PARTIAL
# population -- and on a work reachable from a public surface that is exactly the
# NULL-as-absent the contract forbids (Codex code review 2026-08-03, finding 3).
# The real artifacts satisfy the rule: 0 of their 58 public / 181 private
# NULL-genre works are reachable. This fixture did not, because everything in a
# 8-work fixture is reachable. The genuinely-unpopulated state is now exercised
# where it belongs -- by blanking the whole column on a COPY, in
# tests/test_discovery_release_contract.py.
_WORKS = [
    ("w000001", "sefaria", "Synthetic Neutral Title Alpha", "Synthetic Author A",
     "Synthetic Parent A / Synthetic Leaf A"),
    ("w000002", "sefaria", "Synthetic Neutral Title Beta", "Synthetic Author B",
     "Synthetic Parent A / Synthetic Leaf A"),
    ("w000003", "ja", "Synthetic Neutral Title Gamma", "Synthetic Author C",
     "Synthetic Parent B / Synthetic Leaf B"),
    ("w000004", "ja", "Synthetic Neutral Title Delta", None,
     "Synthetic Parent B / Synthetic Leaf B"),
    ("w000005", "msource", "Synthetic Neutral Title Epsilon", "Synthetic Author D",
     "Unassigned"),
    ("w000006", "msource", "Synthetic Neutral Title Zeta", "Synthetic Author E",
     "Unassigned"),
    ("w000007", "sefaria", "Synthetic Neutral Title Eta", None, "Unassigned"),
    ("w000008", "ja", "Synthetic Neutral Title Theta", "Synthetic Author F",
     "Synthetic Parent B / Synthetic Leaf B"),
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
    coverage=None, page_norm_letters=None, assertion_source_corpus=None,
    # discovery-v3 / schema Amendment 2026-08-07 (F): the WORK-side offsets, in
    # the reference work's `norm_stream`. Only `track1_direct` witnesses have
    # them; every other family leaves them None (see the DDL comment).
    w_start=None, w_end=None,
    # Amendment (G): the PAGE side of the same producer alignment as w_start/w_end.
    aligned_page_start=None, aligned_page_end=None,
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
        "span_start": span_start, "span_end": span_end,
        "w_start": w_start, "w_end": w_end,
        "aligned_page_start": aligned_page_start, "aligned_page_end": aligned_page_end,
        "text_layer": text_layer,
        "snapshot_hash": snapshot_hash, "other_page_id": other_page_id,
        "b_start": b_start, "b_end": b_end, "text_layer_b": text_layer_b,
        "snapshot_hash_b": snapshot_hash_b, "tier": tier, "aligned_len": aligned_len,
        "occ_class": occ_class, "cross_language": cross_language, "n_seed_ms": n_seed_ms,
        "trials": trials, "runner_up": runner_up, "community": community, "ge3": ge3,
        "rung": rung, "router_bucket": router_bucket, "matched_letters": matched_letters,
        "density": density, "n_spans": n_spans, "seed_spans": seed_spans,
        "seed_ms_ids": seed_ms_ids, "rule_version": rule_version, "community_id": community_id,
        # 136-11: `coverage` USED to be attached post-hoc by `_attach_coverage`
        # and then silently dropped at the INSERT, because this dict had no
        # `coverage` key -- that is exactly where the metric was lost. Both keys
        # default to None, so `apply_lever1_coverage`'s `e.get("coverage") is
        # None -> skip` behaviour is byte-for-byte unchanged for every caller
        # that does not supply one. `page_norm_letters` is carried alongside so
        # the persistence layer can tell a MISSING denominator from a genuine
        # near-zero coverage (see `coverage_ppm_and_status`).
        "coverage": coverage, "page_norm_letters": page_norm_letters,
        # 136-12 (D-22): the RAW origin of THIS evidence occurrence, carried on
        # the spec ONLY until `assemble_claims_and_evidence` derives the masked
        # `assertion_visibility` from it. It is a build-time-only key -- it is
        # never inserted, never logged and has no column. `None` (no caller
        # supplied one) fails closed to `private` at derivation time.
        "assertion_source_corpus": assertion_source_corpus,
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

    # -- C8 (p008/w6): plain weak.
    #    136-12 (D-22) MISLABELLING DIRECTION A: w000006's identity corpus is
    #    the restricted one, so the corpus field ALONE would call this
    #    assertion private -- but THIS occurrence originates in an open corpus.
    #    Only a build-time derivation from the raw origin can see that; a
    #    post-hoc `works.source_corpus` join cannot.
    evidence_specs.append(_mk_evidence(
        page_id=p008, work_id="w000006", sys_id=s008,
        evidence_kind=_WITNESS, evidence_source=_PROPAGATED, confidence_band=_WEAK,
        adjudication_status=_PROVISIONAL, audit_status=_NA,
        routing_status=_SHIPPED, routing_reason=_NONE_REASON,
        span_start=6, span_end=26, rung="B2", is_new=1,
        assertion_source_corpus=ids.SOURCE_CORPUS_SEFARIA,
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
        # 136-12 (D-22) MISLABELLING DIRECTION B: w000008's identity corpus is
        # open, so the corpus field ALONE would call this assertion public --
        # but THIS occurrence originates in the restricted corpus. Public
        # eligibility requires BOTH axes, so this row is correctly NOT public.
        assertion_source_corpus=ids.SOURCE_CORPUS_MSOURCE,
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


def _frozen_real_band_precision_rows(v2_bands: bool = False) -> List[Dict]:
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
    spec at 134-07 via an explicit `--precision-spec <json>` file.

    v2 rename (135-07): when `v2_bands=True`, the E1 top-tier row's
    `confidence_band` KEY is `_HIGH_CONFIDENCE_ALGORITHMIC` instead of the v1
    `_EXPERT_VERIFIED` -- SAME 0.889 measured value, ONLY the key renames (the
    tier is algorithmic, not human-verified). Every other row is byte-identical
    across v1/v2, so a v1 build stays exactly as before."""
    top_tier_band = _HIGH_CONFIDENCE_ALGORITHMIC if v2_bands else _EXPERT_VERIFIED
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
            "evidence_source": _TRACK1, "confidence_band": top_tier_band,
            "numerator": None, "denominator": None, "precision": 0.889,
            "ci_low": None, "ci_high": None, "method": "E1 registry pre-registered measurement",
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "notes": "R-A figure (frozen contract, docs/specs SS1.6/SS4.1).",
        },
        {
            "scope": "band", "collection_id": "e1_certification_registry_v1",
            "evidence_source": _TRACK1, "confidence_band": _TIER_A,
            "numerator": None, "denominator": None, "precision": None,
            "ci_low": 0.9084, "ci_high": None, "method": None,
            "sampling_frame": None, "ins_policy": None, "weighting": None,
            "measurement_status": "measured_pass",
            "notes": "D-02a (docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment "
                     "2026-08-02, 136-GATE1-DECISIONS.md SS D-02a): stores ONLY the CERT-01 "
                     "AUTHORIZATION that is_default_eligible() reads -- "
                     "measurement_status='measured_pass', ci_low=0.9084 -- NEVER a measured "
                     "precision number. 'precision' STAYS NULL; this is not a fabricated "
                     "number in a real/release build (unlike the SYNTHETIC-fixture-only 0.90 "
                     "placeholder in _band_precision_rows).",
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

    # 136-12 (D-22): give every spec that did not NAME its own assertion origin
    # the matched work's corpus -- the exact fallback `_assertion_source_corpus`
    # applies on the real path for sources that carry no per-row `cat` (rule 2).
    # The two specs that DO name one (C8/C10) are the deliberate mislabelling
    # fixtures and are left untouched, so the golden fixture exercises both
    # directions in which a corpus-keyed shortcut gets the answer wrong.
    for e in evidence_specs:
        if e.get("assertion_source_corpus") is None:
            e["assertion_source_corpus"] = work_corpus.get(e["work_id"])

    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, "
        "source_corpus, identity_visibility) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(w[0], ids.canonical_work_id(w[0]), w[2], w[3], w[4], w[1],
          derive_identity_visibility({"source_corpus": w[1]})) for w in works],
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
    #
    # 136-11: the placeholder is the claim's OWN claim_id, not the empty string
    # it used to be. D-10a's UNIQUE index on discovery_claim(display_evidence_id)
    # would reject the second row carrying `""`, and the placeholder is
    # overwritten by the backfill below before anything reads it. (claim_id and
    # evidence_id are sha256 digests from different recipes, so a placeholder can
    # never collide with a real winner either.)
    claim_rows = []
    for (page_id, work_id), rows in claims.items():
        claim_id = ids.claim_id(page_id, work_id)
        resolver_input = [
            {"evidence_kind": r["evidence_kind"], "claim_type": r.get("_row_claim_type")}
            for r in rows
        ]
        claim_type = ids.resolve_claim_type(resolver_input)
        claim_rows.append((
            page_id, work_id, claim_id, claim_type, claim_id, work_corpus[work_id], SIDECAR_VERSION,
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
            coverage_ppm, coverage_status = coverage_ppm_and_status(
                e["evidence_source"], e.get("coverage"), e.get("page_norm_letters")
            )
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
                coverage_ppm, coverage_status,
                evidence_band_rank(e["evidence_source"], e["confidence_band"]),
                # 136-12 (D-22): axis ONE, derived while the spec still carries
                # its raw origin. The synthetic dataset supplies none, so every
                # fixture row fails closed to `private` unless the spec names an
                # `assertion_source_corpus` -- never public by omission.
                derive_assertion_visibility(e),
                # discovery-v3 Amendments (F) + (G): the work-side offsets and
                # the PAGE side of that same producer alignment, LAST.
                e.get("w_start"), e.get("w_end"),
                e.get("aligned_page_start"), e.get("aligned_page_end"),
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
            rule_version, community_id,
            coverage_ppm, coverage_status, band_rank, assertion_visibility,
            w_start, w_end, aligned_page_start, aligned_page_end
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        evidence_rows,
    )

    # Backfill display_evidence_id via the frozen TOTAL selector (SS6).
    display_choices: Dict[str, str] = {}
    for (page_id, work_id), rows in claims.items():
        claim_id = claim_id_by_key[(page_id, work_id)]
        selector_rows = [
            {"evidence_id": e["_evidence_id"], "evidence_source": e["evidence_source"],
             "confidence_band": e["confidence_band"], "adjudication_status": e["adjudication_status"],
             "routing_status": e.get("routing_status")}
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

    # 136-11 Task 2: the identification grain + the manuscript display keys.
    # AFTER band_precision, because the main-pool rule's gate 2 reads the
    # `tier_a` certificate authorization (measurement_status/ci_low) out of that
    # registry. The synthetic fixture has no libraries.csv, so
    # `manuscript_display` stays empty here by design -- never back-filled from
    # some other source.
    # 136-12 Task 1: the novelty axis. The synthetic fixture has no verdict
    # cache, so this resolves EVERY row to the fail-closed `not_checked` -- and
    # still runs both build-time invariants, so the fixture proves the assertions
    # are wired rather than merely present.
    novelty_stats = apply_novelty_verdicts(conn, None, alias_groups={})

    identification_stats = populate_discovery_identification(conn)
    identification_stats.update(populate_manuscript_display(conn, None))

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
        # 136-11 / schema Amendment 2026-08-02 (C1): the two new tables need the
        # same release-contract count validation the four existing ones have.
        ("expected_rows_discovery_identification", str(identification_stats["identifications"])),
        ("expected_rows_manuscript_display", str(identification_stats["manuscript_display"])),
        # 136-12 / schema Amendment 2026-08-02 (C1) -- see finalize_build.
        ("audience", ASSET_AUDIENCE_PRIVATE),
        # 136-12 (Amendment (C)): a POPULATED genre column must name the pinned
        # artifact that produced it. The synthetic path has no curated artifact,
        # so it pins its OWN synthetic assignment rows through the identical
        # recipe -- a real hash over real (fabricated) content, never a
        # placeholder digest.
        ("work_domains_content_hash", curated_content_hash(
            [{"canonical_work_id": w[0], "genre": w[4]} for w in works if w[4]])),
        ("frame_content_hash", frame_content_hash),
    ]
    # CD batch / Amendment 2026-08-12 (U): same helper as the real build --
    # the synthetic fixture carries the marker + zero counts, so the loader's
    # conditional requirements and the verifier's new gates are exercised by
    # the fixture rather than first met in production.
    meta_rows.extend(amendment_2026_08_12_meta_rows(conn))
    cur.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)

    return {
        "row_counts": {
            "works": n_works, "discovery_claim": n_claims,
            "discovery_evidence": n_evidence, "witness_units": n_units,
            "discovery_identification": identification_stats["identifications"],
            "manuscript_display": identification_stats["manuscript_display"],
        },
        "frame_content_hash": frame_content_hash,
        "display_evidence_choices": display_choices,
        "band_precision_rows": len(bp_rows),
        "identification": identification_stats,
        "novelty": novelty_stats,
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
# `scripts/discovery_ids.py::mint_work_id` -- `f"w{int(counter):06d}"`,
# i.e. a plain "w" + a zero-padded 6-digit ASCII-decimal counter (e.g.
# "w000001"). Used ONLY as a defensive re-validation pattern for PERSISTED
# crosswalk values (M1); the frozen recipe module itself stays untouched.
#
# Codex R2 masking fix: `[0-9]` here, NEVER bare `\d` -- Python's `\d` (with
# the default str-pattern UNICODE flag) matches ANY Unicode decimal-digit
# codepoint (Nd category), e.g. fullwidth digits U+FF10-FF19 or
# Arabic-Indic digits, not just ASCII '0'-'9'. `format(int, "06d")` (what
# `mint_work_id` actually calls) always emits ASCII '0'-'9' ONLY -- so a
# value matching `\d` but NOT `[0-9]` is PROVABLY not a real mint_work_id
# output and must be rejected, never silently admitted as "digit-shaped".
#
# Codex R3 fix: this pattern is applied via `re.fullmatch` (below), NOT
# `.match` with a `$` anchor. Python's `$` matches just before a TERMINAL
# newline, so "w000000\n" (which `format(int, "06d")` can never emit) would
# have passed a `^w[0-9]{6}$`.match() check and leaked a non-frozen opaque id
# into the crosswalk/review artifact. `fullmatch` on an unanchored pattern
# requires the WHOLE string to be consumed, rejecting any trailing newline.
_OPAQUE_WORK_ID_PATTERN = re.compile(r"w[0-9]{6}")


def _validate_crosswalk(crosswalk: Dict[str, str]) -> None:
    """Defense-in-depth (M1): validate every crosswalk value matches the
    frozen opaque work_id format AND that the mapping is 1:1 (no two raw
    work_ids sharing the same opaque id). Raises `CrosswalkValidationError`
    on ANY malformed or duplicated value -- a corrupted/hand-edited
    crosswalk file could otherwise place a raw identifier or filename stem
    straight into an emitted surface (the review artifact's `work_id`
    column, or `works.work_id` in the shipped sidecar); this must run
    BEFORE any review-artifact or sidecar emission.

    Masking (Codex R2): the raised message NEVER echoes a raw crosswalk key
    or value -- a malformed opaque VALUE could itself be, or embed, a raw
    restricted M-source identifier, and a raw_id KEY could carry one too.
    Only counts and positional indices (stable dict-iteration-order
    position) are reported, so a CLI invocation or an uncaught traceback
    can never surface a restricted string via this validation path."""
    items = list(crosswalk.items())

    malformed_positions = [
        i for i, (_raw_id, opaque) in enumerate(items)
        if not (isinstance(opaque, str) and _OPAQUE_WORK_ID_PATTERN.fullmatch(opaque))
    ]
    if malformed_positions:
        raise CrosswalkValidationError(
            f"crosswalk contains {len(malformed_positions)} value(s) not matching the frozen "
            f"opaque work_id format (whole-string w[0-9]{{6}}) at crosswalk position(s) "
            f"{malformed_positions[:5]} (M1/masking -- refusing to emit any review "
            "artifact/sidecar with a potentially-raw crosswalk value; the raw key/value "
            "is deliberately NOT included in this message)"
        )

    seen_opaque: Dict[str, int] = {}
    duplicate_positions: List[int] = []
    for i, (_raw_id, opaque) in enumerate(items):
        if opaque in seen_opaque:
            duplicate_positions.append(i)
        else:
            seen_opaque[opaque] = i
    if duplicate_positions:
        raise CrosswalkValidationError(
            f"crosswalk is not 1:1 -- {len(duplicate_positions)} opaque work_id(s) at "
            f"crosswalk position(s) {duplicate_positions[:5]} are shared with an earlier "
            "entry (M1/masking -- the raw key/value is deliberately NOT included in "
            "this message)"
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
# 6.3 Source-masked, impact-prioritized CANDIDATE review artifact + the
# owner-verdict `--from-approved` reader (134-07 Task A/B).
#
# The CANDIDATE csv IS the APPROVED csv shape (APPROVED_HEADER ==
# CANDIDATE_HEADER): the owner receives the emitted file, fills in the three
# trailing owner_* columns in place, and returns the SAME 11-column file --
# there is no separate round-trip schema to keep in sync.
# ---------------------------------------------------------------------------

# `confidence_basis` fixed vocabulary (134-07 Task A) -- do NOT invent a
# basis this module doesn't actually compute:
#   - open-corpus-title: the candidate is already-open-corpus (sefaria/ja)
#     AND carries a non-empty research `title` -- auto-adopted verbatim.
#   - none-owner-supplies: EVERY other case (M-source, or an open-corpus
#     candidate with no title) -- fail-closed, candidate_title stays BLANK,
#     never a restricted-title fallback.
CONFIDENCE_BASIS_OPEN_CORPUS_TITLE = "open-corpus-title"
CONFIDENCE_BASIS_NONE_OWNER_SUPPLIES = "none-owner-supplies"

CANDIDATE_HEADER = [
    "work_id", "candidate_title", "author", "genre", "source_label",
    "confidence_basis", "tier_a_witnesses", "claim_count",
    "owner_title", "owner_verdict", "owner_note",
]
APPROVED_HEADER = CANDIDATE_HEADER

# Owner verdicts that ship a work (134-07 Task B) -- 'reject'/'suppress'/a
# blank verdict all EXCLUDE, fail-closed.
_SHIP_OWNER_VERDICTS = frozenset({"approve", "edit"})


def _validate_csv_header(actual_fieldnames, expected_header, csv_kind: str) -> None:
    if list(actual_fieldnames or []) != list(expected_header):
        raise ValueError(
            f"{csv_kind} CSV header mismatch: expected {expected_header}, got {actual_fieldnames}"
        )


def compute_work_impact_counts(claim_rows: List[Tuple], evidence_rows: List[Tuple]) -> Dict[str, Dict[str, int]]:
    """Compute the review-artifact impact signal (134-07 Task A) DIRECTLY
    from the ASSEMBLED claim/evidence rows produced by
    `build_claims_and_evidence`/`assemble_claims_and_evidence` -- never a
    hand-rolled divergent counter, so the review CSV's `tier_a_witnesses`/
    `claim_count` columns can never drift from what the real distillation
    actually ships.

    `claim_rows`: `(page_id, work_id, claim_id, claim_type,
    display_evidence_id, source_corpus, sidecar_version)` tuples (the
    `assemble_claims_and_evidence` "claim_rows" shape).
    `evidence_rows`: `(evidence_id, claim_id, evidence_kind, evidence_source,
    confidence_band, ...)` tuples (the "evidence_rows" shape).

    Returns `{work_id: {"claim_count": int, "tier_a_witnesses": int}}` --
    `claim_count` = distinct `(page_id, work_id)` claims for that work;
    `tier_a_witnesses` = count of that work's evidence rows with
    `evidence_source == 'track1_direct'` AND `confidence_band == 'tier_a'`
    (the `track1_matches WHERE shadowed_by IS NULL` band).
    """
    claim_id_to_work_id: Dict[str, str] = {}
    counts: Dict[str, Dict[str, int]] = {}
    for row in claim_rows:
        work_id, claim_id = row[1], row[2]
        claim_id_to_work_id[claim_id] = work_id
        entry = counts.setdefault(work_id, {"claim_count": 0, "tier_a_witnesses": 0})
        entry["claim_count"] += 1
    for row in evidence_rows:
        claim_id, evidence_source, confidence_band = row[1], row[3], row[4]
        if evidence_source == _TRACK1 and confidence_band == _TIER_A:
            work_id = claim_id_to_work_id.get(claim_id)
            if work_id is None:
                continue
            entry = counts.setdefault(work_id, {"claim_count": 0, "tier_a_witnesses": 0})
            entry["tier_a_witnesses"] += 1
    return counts


def _pick_public_first_value(
    fjms_value, raw_value, *, is_open: bool, include_masked_metadata: bool,
) -> str:
    """FJMS-first with a masking-gated raw-research fallback (owner decision
    2026-07-22).

    The FJMS value (public Genizah catalog vocabulary -- domain / composition
    author) is masking-SAFE for ANY row, so it is used verbatim whenever
    non-empty, with NO gating. Only when FJMS has nothing do we fall back to
    the raw-research value, and that fallback is masking-gated:
      - open-corpus (sefaria/ja) raw is public -> ALWAYS allowed;
      - M-source raw is restricted -> allowed ONLY under the explicit
        --include-masked-metadata owner opt-in (`include_masked_metadata`).
    Returns '' when there is no safe value to surface.

    NOTE (masking-correctness): the gate keys on `is_open` (== source_corpus
    in {sefaria, ja}), NOT on confidence_basis. A title-less open-corpus row
    (confidence_basis='none-owner-supplies' but still public sefaria/ja)
    therefore still gets its public raw fallback -- resolving that edge in
    the masking-correct direction while matching the owner's rule ("open-corpus
    raw always; M-source raw gated")."""
    fjms_value = (fjms_value or "").strip()
    if fjms_value:
        return fjms_value
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return ""
    if is_open:
        return raw_value
    return raw_value if include_masked_metadata else ""


def emit_review_artifact(
    candidates: List[Dict], out_csv_path, *, impact_counts: Dict[str, Dict[str, int]],
    fjms_meta: Optional[Dict[str, Dict[str, str]]] = None,
    include_masked_metadata: bool = False,
) -> List[Dict]:
    """Write the enriched, source-MASKED, impact-prioritized CANDIDATE review
    CSV (134-07 Task A). The exact header is `CANDIDATE_HEADER`;
    `source_label` is the masked `source_corpus` code only (no M-source/
    R-source name or siglum in any cell).

    SCOPE: the PRIORITIZED FULL set -- only candidates carrying >=1 claim in
    the assembled real distillation (`impact_counts`, as returned by
    `compute_work_impact_counts` over the SAME `build_claims_and_evidence`/
    `assemble_claims_and_evidence` assembly the real build uses) are
    emitted; a work with zero claims will never surface in the real
    distillation and is silently excluded rather than shown as a dead row.
    This is NOT a sample -- every surfacing work is included.

    `candidate_title` is auto-derived (verbatim) ONLY when the candidate is
    already open-corpus (sefaria/ja) AND carries a non-empty research `title`
    (`confidence_basis='open-corpus-title'`); it is ALWAYS blank for a
    `none-owner-supplies` row (the owner supplies the neutral title --
    regardless of any flag).

    `genre`/`author` are sourced FJMS-FIRST from the CANONICAL PUBLIC FJMS
    vocabularies (owner decision 2026-07-22), passed in via `fjms_meta`
    (`{work_id: {"genre": <modal FJMS domain>, "author": <modal FJMS
    composition-author EngDesc>}}` from `compute_fjms_enrichment`). FJMS
    domain/composition-author are public Genizah catalog data (NOT restricted
    M-source), so the FJMS value is masking-safe for ALL rows and is used
    whenever non-empty with NO gating. When FJMS has nothing, fall back to
    the raw-research value (`select_shown_works` genre/author) -- gated for
    masking safety by `_pick_public_first_value`: open-corpus (sefaria/ja)
    raw is public and always allowed; M-source raw is allowed ONLY under the
    explicit `include_masked_metadata` owner opt-in (the flag now gates the
    RAW M-SOURCE FALLBACK specifically, since the FJMS-sourced value needs no
    gating). The finished CSV is ALWAYS subject to the blocking masking scan
    (never waived -- the scan decides, not the flag).

    Rows are sorted by `(tier_a_witnesses DESC, claim_count DESC, work_id
    ASC)` -- deterministic, highest-impact titles first, long tail still
    included.

    `owner_title`/`owner_verdict`/`owner_note` are ALWAYS blank in this
    CANDIDATE emission -- even an auto-derived open-corpus title still
    requires the owner's explicit verdict; `load_approved_works` (Task B)
    is the fail-closed gate applied to the file the owner RETURNS.

    Modeled (EMISSION-shape only) on
    `scripts/export_translation_audit_sample.py`'s `write_csv` convention
    (utf-8-sig, trailing review columns).
    """
    fjms_meta = fjms_meta or {}
    rows = []
    for c in candidates:
        work_id = c["work_id"]
        counts = impact_counts.get(work_id)
        claim_count = counts.get("claim_count", 0) if counts else 0
        if claim_count < 1:
            continue  # SCOPE: excluded -- never surfaces in the real distillation
        tier_a_witnesses = counts.get("tier_a_witnesses", 0) if counts else 0

        is_open = c["source_corpus"] in (ids.SOURCE_CORPUS_SEFARIA, ids.SOURCE_CORPUS_JA)
        title = (c.get("title") or "").strip()
        confidence_basis = (
            CONFIDENCE_BASIS_OPEN_CORPUS_TITLE if (is_open and title)
            else CONFIDENCE_BASIS_NONE_OWNER_SUPPLIES
        )
        # candidate_title UNCHANGED: the verbatim open-corpus title only,
        # blank for every none-owner-supplies row (owner supplies it).
        candidate_title = title if confidence_basis == CONFIDENCE_BASIS_OPEN_CORPUS_TITLE else ""

        # genre/author: FJMS PUBLIC vocabulary FIRST (safe for ALL rows),
        # raw-research FALLBACK only when FJMS is empty (masking-gated -- see
        # _pick_public_first_value).
        work_fjms = fjms_meta.get(work_id) or {}
        genre = _pick_public_first_value(
            work_fjms.get("genre"), c.get("genre"),
            is_open=is_open, include_masked_metadata=include_masked_metadata,
        )
        author = _pick_public_first_value(
            work_fjms.get("author"), c.get("author"),
            is_open=is_open, include_masked_metadata=include_masked_metadata,
        )

        rows.append({
            "work_id": work_id,
            "candidate_title": candidate_title,
            "author": author,
            "genre": genre,
            "source_label": c["source_corpus"],
            "confidence_basis": confidence_basis,
            "tier_a_witnesses": tier_a_witnesses,
            "claim_count": claim_count,
            "owner_title": "",
            "owner_verdict": "",
            "owner_note": "",
        })

    rows.sort(key=lambda r: (-r["tier_a_witnesses"], -r["claim_count"], r["work_id"]))

    out_path = Path(out_csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CANDIDATE_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def load_approved_works(approved_csv_path, *, valid_work_ids: Optional[Iterable[str]] = None) -> List[Dict]:
    """Fail-closed `--from-approved` reader (134-07 Task B) over the owner's
    RETURNED enriched review csv (`APPROVED_HEADER == CANDIDATE_HEADER` --
    the owner edits the SAME 11-column file in place).

    SHIP iff `owner_verdict` in `{'approve', 'edit'}` AND the RESOLVED title
    is non-empty, where resolved title = `owner_title` if non-empty else
    `candidate_title`. Everything else -- `owner_verdict` in
    `{'reject', 'suppress'}`, a blank verdict, or an empty resolved title --
    is EXCLUDED. NO research-title fallback (D-07): a row can never ship
    with an empty resolved title.

    Enforces the FROZEN exact `APPROVED_HEADER`; never renders any CSV cell
    value in a raised message (masking) -- the header-mismatch message
    below names only expected/actual COLUMN NAMES, never row data.
    """
    valid_ids = set(valid_work_ids) if valid_work_ids is not None else None
    approved = []
    with open(approved_csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        _validate_csv_header(reader.fieldnames, APPROVED_HEADER, "APPROVED")
        for row in reader:
            verdict = (row.get("owner_verdict") or "").strip()
            if verdict not in _SHIP_OWNER_VERDICTS:
                continue
            owner_title = (row.get("owner_title") or "").strip()
            candidate_title = (row.get("candidate_title") or "").strip()
            neutral_title = owner_title or candidate_title
            if not neutral_title:
                continue
            work_id = (row.get("work_id") or "").strip()
            if not work_id:
                continue
            if valid_ids is not None and work_id not in valid_ids:
                continue
            source_corpus = row.get("source_label")
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
        self._norm_letters_cache: Dict[str, int] = {}

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

    def norm_letters(self, page_id: str) -> int:
        """`page_norm_letters` for a page (bake plan §4.4 Lever-1 coverage
        denominator) = `len(norm_stream(pages.text))`. Cached per page_id. The
        raw `text` is read transiently and IMMEDIATELY reduced to the derived
        integer letter count -- it is NEVER exposed, copied, or persisted into
        the sidecar (same masking posture as `get`'s snapshot digest). A page
        absent from `pages` -> 0 (coverage 0.0, routes to review_only)."""
        cached = self._norm_letters_cache.get(page_id)
        if cached is not None:
            return cached
        row = self._conn.execute(
            "SELECT text FROM pages WHERE page_id = ?", (page_id,)
        ).fetchone()
        count = norm_stream_letter_count(row[0]) if row is not None else 0
        self._norm_letters_cache[page_id] = count
        return count


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


def project_ref_span(ref_spans_json_str: Optional[str]) -> Tuple[
    Optional[int], Optional[int], Optional[int], Optional[int]
]:
    """Project a gen-2 match row's paired spans onto ONE dual-side selection.

    Returns `(page_start, page_end, w_start, w_end)`, or four `None`s when the
    row carries no reference spans (a v2-era row, which has no work-side
    coordinate at all).

    THE CONTRACT IS UNCHANGED. The parsing and the selection now live in
    `shared.discovery_locus` -- `parse_ref_span_alignments` and
    `select_primary_alignment` -- because the locus stage needs EVERY alignment a row
    carries, not just the one an evidence row has room for, and two copies of this
    reading would drift. What ships from here is byte-identical: verified over all
    381,341 real rows against the pre-refactor implementation.

    THE RULE (Codex blocker 1 -- and it is *discovered*, not invented). gen-2's
    `ref_spans_json` is a list of `{p0, p1, dens, rg0, rg1, cigar}` objects in
    which the producer has ALREADY paired the two sides: `p0/p1` index the page's
    `norm_stream`, `rg0/rg1` index the reference work's. So the projection is a
    SELECTION among the producer's own pairs, never an alignment this build has
    to compute.

    Selection: the entry with the largest PAGE-side extent (`p1 - p0`),
    tie-broken `p0` ASC, `p1` ASC, `rg0` ASC, `rg1` ASC -- a total order over
    integers, so the result is deterministic for any input.

    WHY NOT `spans_json` (the trap this avoids). The obvious move is to reuse
    `_largest_track1_span`'s R7 selection and look its `(start, end)` up among
    the ref entries. Measured on all 381,341 gen-2 rows, that FAILS on 12.2% of
    them: `spans_json` is a coarser HULL over the ref entries, so its largest
    span is frequently a range no ref entry carries (e.g. page hull `[981,1772]`
    over ref entries at `[981,1705]` and `[1142,1772]`). Keying on the hull would
    have silently emitted NULL work offsets for 46,472 rows.

    VERIFIED AGAINST THE PRODUCER, not against itself (gate 14). The producer's
    own `discovery_evidence` rows carry `page_start/page_end/ref_start/ref_end`,
    and those tuples are drawn EXACTLY from `ref_spans_json` (100.00% of 200,000
    sampled rows). This rule's selection reproduces one of the producer's own
    evidence rows on **381,341 of 381,341 rows (100.00%)** -- so the offsets this
    ships are the producer's offsets, not a plausible reconstruction of them.

    MULTIPLICITY is preserved, not hidden: 22.0% of rows carry more than one
    distinct work-side span, and this deliberately keeps ONE (the match row is a
    single row, so it has room for one dual-side span). The others are no longer
    discarded at the source -- `parse_ref_span_alignments` returns all of them for
    the locus stage -- but nothing about them is asserted by THIS column.

    MASKING (D-25): consumes integer offsets only. The `cigar` alignment string
    is never read, never stored, and never logged -- it is reference-text-derived
    and has no place in the shipped asset.
    """
    primary = select_primary_alignment(parse_ref_span_alignments(ref_spans_json_str))
    if primary is None:
        return (None, None, None, None)
    return (primary.page_start, primary.page_end, primary.w_start, primary.w_end)


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

def _attach_coverage(spec: Dict, page_index, page_id: str, matched_letters: Optional[int]) -> Dict:
    """Set the Lever-1 routing input `spec['coverage']` = `matched_letters /
    page_norm_letters` (bake plan §4.4), computed at INGESTION for every
    track1_direct witness (135-07 coverage-metric fix).

    `page_norm_letters` comes from `page_index.norm_letters(page_id)` (cached,
    integer-only). A `page_index` WITHOUT a `norm_letters` method (a lightweight
    in-memory test double that supplies only `.get()`) yields NO coverage --
    the spec's `coverage` stays absent, so `apply_lever1_coverage` skips it and
    the row keeps its ingested routing (backward-compatible with the E1/tier_a
    unit tests that never exercise Lever-1). Mutates and returns `spec`.

    136-11: the resolved denominator is recorded on the spec too, so
    `coverage_ppm_and_status` can distinguish "the page had no normalized
    letters" (coverage 0.0 is a SENTINEL there, not a measurement) from a real
    measured near-zero. Nothing about the computation changes."""
    norm_fn = getattr(page_index, "norm_letters", None)
    if norm_fn is None:
        return spec
    page_norm_letters = norm_fn(page_id)
    spec["coverage"] = compute_page_coverage(matched_letters, page_norm_letters)
    spec["page_norm_letters"] = page_norm_letters
    return spec


def _assertion_source_corpus(row: Dict, work: Dict) -> Optional[str]:
    """The RAW origin of ONE evidence occurrence, resolved at INGEST -- while
    the raw research row is still in scope and before it is discarded (D-22,
    T-136-12-05).

    Order, most specific first:

    1. **The occurrence's OWN raw `cat`**, mapped through
       `_map_cat_to_source_corpus`. `track1_matches` carries a `cat` per ROW,
       and `select_shown_works` deliberately reads only ONE representative row
       per raw work id ("first by page_id") to fix the work's IDENTITY corpus.
       So an individual occurrence's origin genuinely can differ from the
       work's -- which is precisely the case a `works.source_corpus` join
       cannot see, and precisely the mislabelling D-22 measured in both
       directions.
    2. **Else the matched reference work's own ingest-time `source_corpus`** --
       still the RAW-derived value carried on the shown-work record (the
       sources that reach us as JSONL, e.g. the E1/Q2 collections, carry no
       per-row `cat` of their own), NOT a post-hoc `works` table join.
    3. **Else `None`** -> `assertion_visibility` fails closed to `private`.

    Returns a MASKED corpus code or `None`. The raw `cat` value never leaves
    this function.
    """
    if "cat" in row:
        mapped = _map_cat_to_source_corpus(row.get("cat"))
        if mapped is not None:
            return mapped
    return work.get("source_corpus")


def _ingest_e1_rows(
    rows: Iterable[Dict], *, work_index: Dict[str, Dict], page_index,
    confidence_band: str, adjudication_status: str, audit_status: str,
    e1_ref_spans: Optional[Dict[Tuple[str, str], str]] = None,
) -> List[Dict]:
    """Ingest ONE of the four DISJOINT E1 track1_direct source populations
    (e1_ra_confirmed / e1_adjudicated_a / e1_rb_screening / e1_r3_frame) --
    all four share the same row shape (page_id, sys_id, work_id, o0, o1, ml,
    dens, n_spans); band assignment is BY-SOURCE (the caller fixes
    `confidence_band`), no within-track1_direct fall-through (F1).

    `e1_ref_spans` (2026-08-08) maps RAW `(page_id, work_id)` -> the producer's
    `ref_spans_json` for that pair, so E1 rows carry the same four dual-side
    coordinates `_ingest_tier_a` emits and can satisfy gate 3.

    Why it is a PARAMETER and not read here. These rows come from tier B, which
    the production matcher never ref-instrumented: `mapv2_track1_run.py:424`
    drops the reference coordinate it just computed. The map is produced by a
    separate, frame-parity-gated regeneration (`e1_tierb_frame.py`) and carried
    in the research DB. Passing it in keeps this function a pure transform and
    keeps the *provenance* decision -- which regeneration, gated how -- at the
    call site where it can be seen.

    Absent map => four `None`s, exactly as before. That is correct for a v2 or
    smoke build and is NOT a silent release hazard: gate 3 halts a `--release`
    build on precisely that condition.
    """
    out = []
    ref_spans = e1_ref_spans or {}
    for row in rows:
        work = work_index.get(row["work_id"])
        if work is None:
            continue
        text_layer, snapshot_hash = page_index.get(row["page_id"])
        matched_letters = row.get("ml")
        # Keyed on the RAW work id, which is what the matcher wrote and what the
        # JSONL carries -- NOT `work["work_id"]` (the minted `w######`). Using the
        # minted id would miss every row, and the failure would look like "the
        # regeneration produced nothing" rather than a key mismatch.
        aligned_page_start, aligned_page_end, w_start, w_end = project_ref_span(
            ref_spans.get((row["page_id"], row["work_id"]))
        )
        spec = _mk_evidence(
            page_id=row["page_id"], work_id=work["work_id"], sys_id=row["sys_id"],
            evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=confidence_band,
            adjudication_status=adjudication_status, audit_status=audit_status,
            routing_status=_SHIPPED, routing_reason=_NONE_REASON,
            span_start=row["o0"], span_end=row["o1"],
            matched_letters=matched_letters, density=row.get("dens"), n_spans=row.get("n_spans"),
            text_layer=text_layer, snapshot_hash=snapshot_hash,
            assertion_source_corpus=_assertion_source_corpus(row, work),
            w_start=w_start, w_end=w_end,
            aligned_page_start=aligned_page_start, aligned_page_end=aligned_page_end,
        )
        out.append(_attach_coverage(spec, page_index, row["page_id"], matched_letters))
    return out


E1_REF_SPANS_TABLE = "e1_ref_spans"


def load_e1_ref_spans(conn: Optional[sqlite3.Connection]) -> Dict[Tuple[str, str], str]:
    """Read the frame-parity-gated tier-B reference spans out of the research DB.

    Returns `{}` when the table is absent -- a v2-era research DB legitimately has
    no tier-B instrumentation, and a smoke fixture has no need of one. The release
    path does not rely on this being non-empty: gate 3 is what refuses to ship a
    `track1_direct` row with no work-side coordinate, and it counts rows rather
    than trusting a table to exist.
    """
    if conn is None:
        return {}
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (E1_REF_SPANS_TABLE,),
    ).fetchone()
    if row is None:
        return {}
    return {
        (page_id, work_id): ref_spans_json
        for page_id, work_id, ref_spans_json in conn.execute(
            f"SELECT page_id, work_id, ref_spans_json FROM {E1_REF_SPANS_TABLE}"
        )
    }


def _ingest_tier_a(conn: sqlite3.Connection, work_index: Dict[str, Dict], page_index) -> List[Dict]:
    """Ingest `track1_matches WHERE shadowed_by IS NULL` (Landmine 9) -> the
    `tier_a` band; offsets = the largest `spans_json` span (R7).

    136-12: `cat` is selected alongside, because this is the ONE ingest path
    whose rows carry a PER-OCCURRENCE raw origin. `select_shown_works` reads a
    single representative row per raw work id to fix the work's IDENTITY
    corpus, so an individual match's own `cat` can differ from it -- and that
    difference is exactly what `assertion_visibility` has to see while it is
    still in scope (D-22 / T-136-12-05). The raw value is consumed by
    `_assertion_source_corpus` and never stored."""
    out = []
    cur = conn.cursor()
    # discovery-v3 (2026-08-07, Codex blocker 1): `ref_spans_json` is read when
    # the research DB carries it (a gen-2 slim DB does; a v2-era one does not).
    # Probed rather than assumed so a v2 rebuild against the old research DB
    # still works -- it simply emits NULL work-side offsets, which is the honest
    # answer for a population that has none.
    have_ref_spans = "ref_spans_json" in {
        r[1] for r in conn.execute("PRAGMA table_info(track1_matches)")
    }
    ref_col = ", ref_spans_json" if have_ref_spans else ""
    cur.execute(
        "SELECT page_id, sys_id, work_id, matched_letters, best_density, n_spans, spans_json, cat"
        f"{ref_col} FROM track1_matches WHERE shadowed_by IS NULL"
    )
    for row in cur:
        (page_id, sys_id, work_id, matched_letters, best_density,
         n_spans, spans_json, cat) = row[:8]
        ref_spans_json = row[8] if have_ref_spans else None
        work = work_index.get(work_id)
        if work is None:
            continue
        start, end = _largest_track1_span(spans_json)
        # The dual-side projection. Its PAGE side is deliberately NOT used to
        # overwrite `span_start`/`span_end`: those are frozen inputs to the
        # `evidence_id` recipe, so changing them would regenerate every
        # track1_direct id and break the D-02b rebuild-preservation diff. Only
        # the work-side coordinate is new.
        # BOTH sides of the selected producer alignment (Codex R3): the page side
        # is retained rather than discarded, because `span_start`/`span_end` hold
        # the coarser hull and pairing that with this work interval would assert a
        # correspondence the producer never made.
        aligned_p0, aligned_p1, w_start, w_end = project_ref_span(ref_spans_json)
        text_layer, snapshot_hash = page_index.get(page_id)
        spec = _mk_evidence(
            page_id=page_id, work_id=work["work_id"], sys_id=sys_id,
            evidence_kind=_WITNESS, evidence_source=_TRACK1, confidence_band=_TIER_A,
            adjudication_status=_UNREVIEWED, audit_status=_NA,
            routing_status=_SHIPPED, routing_reason=_NONE_REASON,
            span_start=start, span_end=end, w_start=w_start, w_end=w_end,
            aligned_page_start=aligned_p0, aligned_page_end=aligned_p1,
            matched_letters=matched_letters, density=best_density, n_spans=n_spans,
            text_layer=text_layer, snapshot_hash=snapshot_hash,
            assertion_source_corpus=_assertion_source_corpus({"cat": cat}, work),
        )
        out.append(_attach_coverage(spec, page_index, page_id, matched_letters))
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
            assertion_source_corpus=_assertion_source_corpus(row, work),
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
            assertion_source_corpus=_assertion_source_corpus(row, work),
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
            assertion_source_corpus=_assertion_source_corpus(row, work),
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
    # discovery-v3 Amendment (F). Included so an equal-priority evidence_id
    # collision between two rows that differ ONLY in their work-side offsets is
    # treated as content-divergent (raise) rather than as a harmless duplicate:
    # the offsets are not in the frozen evidence_id recipe, so two genuinely
    # different alignments CAN collide, and silently keeping whichever arrived
    # first would pick an alignment by ingestion order.
    "w_start", "w_end", "aligned_page_start", "aligned_page_end",
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
        coverage_ppm, coverage_status = coverage_ppm_and_status(
            e["evidence_source"], e.get("coverage"), e.get("page_norm_letters")
        )
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
            # 136-11 (schema Amendment (A)): the two persisted sort keys. See
            # `coverage_ppm_and_status` / `evidence_band_rank`.
            coverage_ppm, coverage_status,
            evidence_band_rank(e["evidence_source"], e["confidence_band"]),
            # 136-12 (D-22): axis ONE, derived HERE -- the last point at which
            # the spec still carries its raw `assertion_source_corpus`. After
            # this the spec dicts are discarded and the axis is unrecoverable:
            # `works.source_corpus` is exactly the proxy D-22 rejects.
            derive_assertion_visibility(e),
            # discovery-v3 Amendment (F): the work-side offsets, LAST in the
            # tuple to match the column list. NULL for every family but
            # track1_direct.
            e.get("w_start"), e.get("w_end"),
            e.get("aligned_page_start"), e.get("aligned_page_end"),
        ))

    display_choices: Dict[str, str] = {}
    for claim_id, rows in evidence_rows_by_claim.items():
        selector_rows = [
            {"evidence_id": e["_evidence_id"], "evidence_source": e["evidence_source"],
             "confidence_band": e["confidence_band"], "adjudication_status": e["adjudication_status"],
             "routing_status": e.get("routing_status")}
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
    cross_corpus_map: Optional[Dict[str, str]] = None,
    year_by_canonical: Optional[Dict[str, Optional[int]]] = None,
    apply_lever1: bool = False,
    lever1_threshold: float = LEVER1_COVERAGE_CLIFF,
    gen2_router: Optional[Dict] = None,
    raw_work_by_minted: Optional[Dict[str, str]] = None,
    reband_tier_a: bool = False,
    v2_bands: bool = False,
    d17_delta: int = D17_DELTA_YEARS,
    d17_ml_floor: int = D17_MIN_ML,
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
    # discovery-v3 (Codex R3, HIGH): containment at the works ANCHOR, checked here
    # because this is the single point every evidence family's claims pass through
    # -- a row whose raw work id is absent from `works` is excluded outright (§10),
    # so gating this set covers the E1/Q2 JSONL sources that the research-DB gate
    # structurally cannot see.
    assert_works_contain_no_excluded_corpus(works)
    work_index = {w["raw_work_id"]: w for w in works}
    work_source_corpus = {w["work_id"]: w["source_corpus"] for w in works}

    # v2 (135-06 amendment 2026-07-24): the E1 track1_direct top tier is an
    # ALGORITHMIC top-score, NOT human approval, so a v2 build bands it
    # `high_confidence_algorithmic` (the 135-05 rename); a v1 build keeps the
    # legacy `expert_verified` literal. Gated by the EXPLICIT `v2_bands` flag
    # (finalize_build sets it True whenever a v2 census is supplied), matching
    # the existing v2-signal precedent (cross_corpus_map / apply_lever1 /
    # reband_tier_a) -- never inferred implicitly from cross_corpus_map.
    expert_tier_band = _HIGH_CONFIDENCE_ALGORITHMIC if v2_bands else _EXPERT_VERIFIED

    evidence_specs: List[Dict] = []
    # The tier-B reference coordinates (2026-08-08). Read ONCE and shared by all
    # four collections -- they are disjoint populations over one keyspace, so a
    # per-collection read would be four scans of the same table.
    e1_ref_spans = load_e1_ref_spans(conn)
    evidence_specs += _ingest_e1_rows(
        e1_ra_confirmed, work_index=work_index, page_index=page_index,
        confidence_band=expert_tier_band, adjudication_status=_UNREVIEWED, audit_status=_AUDIT_PENDING,
        e1_ref_spans=e1_ref_spans,
    )
    evidence_specs += _ingest_e1_rows(
        e1_adjudicated_a, work_index=work_index, page_index=page_index,
        confidence_band=expert_tier_band, adjudication_status=_HUMAN_CONFIRMED, audit_status=_AUDIT_PENDING,
        e1_ref_spans=e1_ref_spans,
    )
    evidence_specs += _ingest_e1_rows(
        e1_rb_screening, work_index=work_index, page_index=page_index,
        confidence_band=_SCREENING_RB, adjudication_status=_PROVISIONAL, audit_status=_NA,
        e1_ref_spans=e1_ref_spans,
    )
    evidence_specs += _ingest_e1_rows(
        e1_r3_frame, work_index=work_index, page_index=page_index,
        confidence_band=_SCREENING_CANON, adjudication_status=_PROVISIONAL, audit_status=_NA,
        e1_ref_spans=e1_ref_spans,
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

    # v2 (135-06) corrected order-of-operations (bake plan §6): Lever-1
    # coverage routing (step 4) BEFORE D-17 chronological demotion (step 5);
    # the §4.5 reband (step 6 materialization) is consumed as a REBUILD INPUT
    # here -- BEFORE assemble computes evidence_id / display_evidence_id -- so
    # each rebanded row's id regenerates over its new confidence_band and the
    # display pointer recomputes over the rebanded+demoted set.
    routing_audit_rows: List[Dict] = []
    # discovery-v3 (2026-08-07, owner decision + Codex blocker 2): when a gen-2
    # coverage ROUTER is supplied, its decision REPLACES Lever-1 entirely -- the
    # two are mutually exclusive by construction, not merely ordered. Measured
    # on the real artifact: applying Lever-1's 0.45 cliff to the router's basis
    # demotes 30,899 of 160,095 `same_work` rows (19.3%), one-way, so running
    # both would discard exactly the surface the router's grading validated.
    # Round 2 of the Codex review found the ingest unwired and therefore inert;
    # this is that wiring, and `assert_emitted_parity` proves it took effect.
    #
    # THE ORDER, stated rather than "re-derived" (Codex round 2, correctly: "'re-
    # derive' is not an order"). v3 runs, in this sequence:
    #
    #   3. ingest sources          (already done above)
    #   4. ROUTER routing          <- occupies Lever-1's slot exactly
    #   5. D-17 chronological demotion
    #   6. §4.5 reband (rebuild input)
    #
    # The router takes Lever-1's position for a substantive reason, not merely to
    # minimise the diff: `apply_d17_demotion` groups "the currently-SHIPPED
    # track1_direct witness population" and mutates `routing_status` as it walks
    # each page earliest-first. Running it BEFORE the router would let it
    # arbitrate between rows the router is about to demote -- so a work could be
    # demoted for being chronologically later than a competitor that never ships
    # at all, and the surviving row would carry `later_shared_text` as its reason
    # while the actual cause was a phantom. Running the router first makes D-17's
    # input exactly the population that ships, which is the same invariant the v2
    # Lever-1-before-D-17 order existed to hold.
    #
    # Pinned by `test_the_router_runs_before_d17_not_after`.
    if gen2_router is not None:
        if apply_lever1:
            raise RoutingConflictError(
                "gen2_router and apply_lever1 are mutually exclusive: the router "
                "IS the coverage routing decision, and Lever-1 would re-derive it "
                "with a different threshold (0.45 vs the router's fitted value), "
                "demoting ~19% of the validated headline surface"
            )
        from v3_routing_ingest import apply_router_routing, assert_emitted_parity
        if raw_work_by_minted is None:
            raw_work_by_minted = {
                w["work_id"]: w["raw_work_id"] for w in works if w.get("raw_work_id")
            }
        router_report = apply_router_routing(
            evidence_specs, gen2_router,
            raw_work_by_minted=raw_work_by_minted,
            track1_source=_TRACK1, witness_kind=_WITNESS,
        )
        assert_emitted_parity(router_report, gen2_router)
    elif apply_lever1:
        apply_lever1_coverage(evidence_specs, threshold=lever1_threshold)
    if year_by_canonical is not None:
        routing_audit_rows = apply_d17_demotion(
            evidence_specs, cross_corpus_map=cross_corpus_map,
            year_by_canonical=year_by_canonical, delta=d17_delta,
            ml_floor=d17_ml_floor, will_reband_tier_a=reband_tier_a,
        )
    if reband_tier_a:
        apply_reband(evidence_specs)

    result = assemble_claims_and_evidence(
        evidence_specs, work_source_corpus, sidecar_version=sidecar_version)
    result["routing_audit_rows"] = routing_audit_rows

    # discovery-v3 (Codex R3 HIGH, corrected per R4): PER-KEY parity against the
    # ASSEMBLED rows. Runs here because this is the first point they exist -- after
    # dedup on evidence_id and collision resolution, which is where a row could
    # silently acquire a routing the router never gave it.
    #
    # ALWAYS runs when a router is supplied. An earlier version skipped it whenever
    # D-17 demoted anything; round 4 was right that this disables parity on exactly
    # the builds that matter, since one valid demotion suppresses the check for
    # every row. D-17 is now RECONCILED instead: a `review_only` row the router
    # shipped is permitted only when a D-17 audit row names that same
    # (page_id, work). Unmatched audit rows fail too.
    if gen2_router is not None:
        from v3_routing_ingest import assert_assembled_parity
        result["router_parity"] = assert_assembled_parity(
            result["evidence_rows"], gen2_router, result["claim_rows"],
            routing_status_idx=7, claim_id_idx=1, evidence_source_idx=3,
            track1_source=_TRACK1, raw_work_by_minted=raw_work_by_minted,
            d17_audit_rows=routing_audit_rows,
            # The D-17 audit rows key on the BUILDER's canonical id (over the
            # minted work_id), while the router keys on gen-2's. Supply the
            # translation rather than letting the gate guess -- it is computed
            # here anyway, and an omitted map makes every demotion read as a
            # parity failure, loudly.
            canonical_by_minted={
                w["work_id"]: ids.canonical_work_id(w["work_id"], cross_corpus_map or {})
                for w in works
            },
        )
    return result


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


class RestrictedCorpusLeakError(RuntimeError):
    """A research DB carries rows from a corpus excluded from this asset."""


# Work-id prefixes excluded from the v3 asset by decision. Mirrors
# `v3_build_research_db.EXCLUDED_WORK_PREFIXES` -- the slim builder DROPS them at
# the source; this is the CONSUMER-side gate for the case where the slim builder
# was never run.
_EXCLUDED_WORK_PREFIXES: Tuple[str, ...] = ("RS:",)


def assert_research_db_contains_no_excluded_corpus(
    conn: sqlite3.Connection, *, prefixes: Tuple[str, ...] = _EXCLUDED_WORK_PREFIXES,
) -> Dict:
    """Gate 12, at the CONSUMER boundary (Codex round 2, HIGH).

    Round 2's finding was exact: the slim builder's prefix filter and post-build
    scan are "useful defense in depth, not the required gate", because
    `select_shown_works`, the review-only path and `--from-approved` each build
    their candidate set from whatever database path the operator supplies. So a
    different research DB -- notably the gen-2 corpus file, whose own
    `track1_matches` table is the V2-ERA one carrying 349 restricted-corpus works
    -- reaches a sidecar or a review artifact without `v3_build_research_db.py`
    ever running. `select_shown_works` has no prefix rejection: those rows are
    classified through the ordinary `cat`/genre path and ship.

    Placed in `_connect_research_ro` so EVERY entrypoint is covered by
    construction. A gate each caller has to remember to invoke is a gate that a
    future caller will not invoke.

    Returns a fingerprint of what was checked -- row count and the table's own
    column set -- so a build record can show WHICH source table was gated rather
    than merely that a gate ran.

    Masking (D-25): counts and prefixes only. No work id, title or reference text
    is returned, logged or interpolated into the error -- a leak report must not
    itself leak.
    """
    tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
    }
    if "track1_matches" not in tables:
        # Nothing to gate: a caller passing a DB with no match table (some unit
        # tests) is not a containment risk. Recorded rather than silently passed.
        return {"gated": False, "reason": "no track1_matches table"}
    total = conn.execute("SELECT COUNT(*) FROM track1_matches").fetchone()[0]
    offending = 0
    for prefix in prefixes:
        offending += conn.execute(
            "SELECT COUNT(*) FROM track1_matches WHERE work_id LIKE ? || '%'",
            (prefix,),
        ).fetchone()[0]
    if offending:
        raise RestrictedCorpusLeakError(
            f"the research DB carries {offending} of {total} match row(s) whose "
            f"work id is from a corpus EXCLUDED from this asset. This is the "
            f"signature of pointing the build at the gen-2 corpus file (whose own "
            f"`track1_matches` is the v2-era table) instead of at a slim DB built "
            f"by scripts/v3_build_research_db.py. Refusing to read it. "
            f"(No work id is echoed here -- D-25.)"
        )
    return {
        "gated": True,
        "track1_matches_rows": total,
        "excluded_prefix_rows": 0,
        # The source-table identity round 2 asked for: a caller can record WHICH
        # table shape was gated, so "the gate ran" is checkable against "the gate
        # ran on the thing that was built".
        "track1_matches_columns": sorted(
            r[1] for r in conn.execute("PRAGMA table_info(track1_matches)")
        ),
    }


def assert_works_contain_no_excluded_corpus(
    works, *, prefixes: Tuple[str, ...] = _EXCLUDED_WORK_PREFIXES,
) -> Dict:
    """Containment at the WORKS ANCHOR (Codex R3, HIGH).

    Round 3's finding was that the research-DB gate reads `track1_matches` only,
    so it cannot see a restricted row arriving through the E1/Q2 JSONL families:
    those ingests key on `work_id` and `cpage`, not on the match table.

    This gate closes that class in one place by using a structural fact rather
    than enumerating input surfaces. `build_claims_and_evidence` builds
    `work_index` from `works` and EXCLUDES any row whose raw work id is absent
    (§10: no claim can exist without a `works` FK anchor). So every claim in every
    family -- track1_direct, propagated witness, family-router, shared_text --
    passes through this set. Gating it therefore covers the JSONL paths without a
    per-source check that a future fifth source would arrive after.

    What it does NOT cover, stated rather than implied: a restricted PAGE reached
    by an allowed work. `pages` carries no work id, so page-level provenance is
    not derivable here; the slim builder's page copy is bounded by the gen-2
    corpus, and closing that class properly needs a page-provenance column the
    producer does not currently emit. Recorded in the plan as owed, not claimed
    closed.

    Masking (D-25): counts and prefixes only.
    """
    offending = 0
    total = 0
    for w in works:
        total += 1
        raw = w.get("raw_work_id") or ""
        if raw.startswith(prefixes):
            offending += 1
    if offending:
        raise RestrictedCorpusLeakError(
            f"{offending} of {total} approved work(s) are from a corpus EXCLUDED from "
            f"this asset. Every claim requires a `works` FK anchor, so these would "
            f"admit restricted rows through ANY evidence family -- including the "
            f"E1/Q2 JSONL sources, which the research-DB gate cannot see. Refusing to "
            f"build. (No work id is echoed here -- D-25.)"
        )
    return {"works_checked": total, "excluded_prefix_works": 0}


def _connect_research_ro(db_path) -> sqlite3.Connection:
    """Open a research DB read-only, GATED for excluded-corpus containment.

    The gate runs here rather than at each call site because there are four
    entrypoints (real build, review-only, `--from-approved`, and tests) and a
    future fifth would otherwise arrive ungated.
    """
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        assert_research_db_contains_no_excluded_corpus(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def _insert_works_real(
    cur: sqlite3.Cursor, works: List[Dict], cross_corpus_map: Optional[Dict[str, str]] = None,
) -> None:
    # v2 (135-06): the hash-pinned census `cross_corpus_map` is threaded into
    # the FROZEN `ids.canonical_work_id` recipe so a merged twin's two source
    # copies collapse to ONE canonical_work_id (soft merge -- work_id itself
    # is preserved for provenance). None/{} => v1 identity (each work is its
    # own canonical), so existing callers are unaffected.
    rows = [
        (w["work_id"], ids.canonical_work_id(w["work_id"], cross_corpus_map), w["neutral_title"],
         w.get("author"), w.get("genre"), w["source_corpus"],
         # 136-12 (D-22): axis TWO. `works.source_corpus` is an insufficient
         # proxy for the ASSERTION axis but is exactly right for THIS one -- it
         # answers "does this work's own identity originate in an open or a
         # restricted corpus", which is what the column records. Derived by the
         # shared module so there is no second mapping to keep in step.
         derive_identity_visibility(w))
        for w in works
    ]
    cur.executemany(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, "
        "source_corpus, identity_visibility) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
            rule_version, community_id,
            coverage_ppm, coverage_status, band_rank, assertion_visibility,
            w_start, w_end, aligned_page_start, aligned_page_end
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


# ===========================================================================
# 6.8b 136-11 Task 2: the IDENTIFICATION grain + the manuscript display keys
# (schema Amendment 2026-08-02 (B)/(B1)).
#
# Both tables are materialized by READING the already-inserted works / claims /
# evidence out of the output DB, so the synthetic and the real build paths --
# which assemble their rows through deliberately independent code -- share ONE
# implementation of the grain, the bucket rule and the display-work selection.
# ===========================================================================

class IdentificationGrainError(RuntimeError):
    """Raised when a build-time consistency assertion over the materialized
    identification grain fails: the row count disagrees with the distinct
    `(sys_id, canonical_work_id)` pair count, or the `works` identity join is
    not exactly 1:1.

    Both are HARD build failures on purpose. A grouping or join error here does
    not look like an error downstream -- it looks like a different corpus
    total, silently."""


# The frozen id recipe (schema SS(B)): sha256 over the canonical UTF-8
# serialization of this prefix + sys_id + canonical_work_id, backed by the
# table's own UNIQUE(sys_id, canonical_work_id) constraint.
_IDENTIFICATION_ID_PREFIX = "discovery_identification_v1"

# Schema SS(B1) step 2: public-before-private, so a mixed-visibility canonical
# group's representative is public whenever a public member exists.
_SOURCE_CORPUS_RANK = {
    ids.SOURCE_CORPUS_SEFARIA: 0,
    ids.SOURCE_CORPUS_JA: 1,
    ids.SOURCE_CORPUS_MSOURCE: 2,
}

# "The strongest claim type present", using the SAME precedence
# `ids.resolve_claim_type` already applies within one claim -- witness beats
# shared wording, and a dominant span beats a quotation.
_CLAIM_TYPE_STRENGTH = {
    ids.CLAIM_TYPE_DIRECT_WITNESS: 0,
    ids.CLAIM_TYPE_QUOTES_THIS_WORK: 1,
    ids.CLAIM_TYPE_SHARED_TEXT: 2,
}

# 136-11 wrote the STRUCTURE; 136-12 computes the values. Both visibility axes
# fail CLOSED to `private` here -- public eligibility requires BOTH to be
# `public` (D-22), so an un-derived row can never leak public by default.
# `not_checked` is novelty's own fail-closed default, never "novel"; it now
# survives only as the fallback for an evidence row that somehow carries no
# shade at all (the column is NOT NULL, so this is defence in depth).
_IDENTIFICATION_NOVELTY_DEFAULT = NOVELTY_DEFAULT_STATUS


def identification_id(sys_id: str, canonical_work_id: str) -> str:
    """The deterministic content key for one identification.

    Deliberately reproduces the recipe frozen in
    `docs/specs/discovery-sidecar-schema-v1.md` SS(B) verbatim, exactly as
    `scripts/project_discovery_public.py` already does -- there is no
    `identification_id()` in `scripts/discovery_ids.py` yet (the table is new in
    the Phase-136 amendment). `tests/test_discovery_build.py` asserts the two
    implementations produce identical ids, so the duplication is a red suite
    rather than a silent divergence until a later plan centralizes it."""
    key = f"{_IDENTIFICATION_ID_PREFIX}|{sys_id}|{canonical_work_id}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def select_display_work_id(canonical_work_id: str, members: Iterable[Tuple[str, str]]) -> str:
    """The schema SS(B1) ORDERED, TOTAL representative rule over a canonical
    group -- never "whichever row the join returns".

    `members` is an iterable of `(work_id, source_corpus)`. `canonical_work_id`
    is NOT unique on `works` (15 duplicated groups on the live asset, three with
    different titles AND mixed source corpora), so joining the identification
    grain to `works` on it FANS OUT 64,509 rows to 65,587. Left unaddressed,
    title selection and `identity_visibility` are undefined for those groups,
    and a PRIVATE work in a duplicated group could influence what looks like a
    shared public aggregate.

    1. the canonical anchor (`work_id == canonical_work_id`), if it is itself a
       member of its own group;
    2. else the LOWEST `source_corpus` in the fixed order sefaria < ja < msource;
    3. else the lexicographically SMALLEST `work_id`.
    """
    members = list(members)
    if not members:
        raise IdentificationGrainError(
            f"canonical group {canonical_work_id!r} has no member works -- "
            "cannot select a display_work_id"
        )
    for work_id, _corpus in members:
        if work_id == canonical_work_id:
            return work_id
    ranked = sorted(
        members, key=lambda m: (_SOURCE_CORPUS_RANK.get(m[1], 99), m[0])
    )
    return ranked[0][0]


def _canonical_group_index(conn: sqlite3.Connection) -> Dict[str, List[Tuple[str, str]]]:
    groups: Dict[str, List[Tuple[str, str]]] = {}
    for work_id, canonical_work_id, source_corpus in conn.execute(
        "SELECT work_id, canonical_work_id, source_corpus FROM works"
    ):
        groups.setdefault(canonical_work_id, []).append((work_id, source_corpus))
    return groups


def compute_launch_scope_reconciliation(conn: sqlite3.Connection) -> Dict:
    """Run `shared.discovery_visibility.reconcile_launch_scope` over the BUILT
    rows and return its result for the build report (136-12 Task 2).

    VIS-01's own prose describes a corpus/family shortcut ("launch scope:
    Sefaria-direct matches union all MS-relationship/propagated claims") that is
    EXACTLY the proxy D-22 proves insufficient. This reports both counts and the
    symmetric difference by corpus x family; it does NOT resolve the
    disagreement, does not drop, keep or relabel a single row, and is never
    consulted by any eligibility decision. Plan 136-13 puts the real number in
    front of the owner.

    **Why the assertion axis is fed back in as a corpus code.** By this point
    the raw evidence origin is deliberately gone -- that is the entire property
    D-22 asks the build to guarantee -- so the STORED `assertion_visibility` is
    the only surviving authority. `reconcile_launch_scope` derives that axis
    itself from a masked corpus code, so each row is handed the code that
    reproduces its stored value. The round-trip is faithful for this
    comparison's purposes because `_corpus_code_to_visibility` maps BOTH open
    codes (`sefaria`, `ja`) to `public`: the public/private outcome is
    preserved exactly, even though the specific open code is not. The VIS-01
    shortcut's own inputs (`evidence_source`, the claim's `source_corpus`) are
    read straight from the columns, unmodified."""
    rows = [
        {
            "evidence_source": evidence_source,
            "source_corpus": source_corpus,
            # `reconcile_launch_scope` derives axis one via
            # `assertion_visibility(row)`, which reads `assertion_source_corpus`.
            # The raw origin no longer exists at this point -- by design -- so we
            # hand it the ALREADY-DERIVED masked corpus code that produced the
            # stored value: `sefaria` reproduces `public`, and the restricted
            # code reproduces `private`. The stored column stays the authority;
            # this only feeds the comparison the report exists to print.
            "assertion_source_corpus": (
                ids.SOURCE_CORPUS_SEFARIA if assertion_vis == VISIBILITY_PUBLIC
                else ids.SOURCE_CORPUS_MSOURCE
            ),
        }
        for evidence_source, source_corpus, assertion_vis in conn.execute(
            """
            SELECT de.evidence_source, dc.source_corpus, de.assertion_visibility
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            """
        )
    ]
    result = reconcile_launch_scope(rows)
    # JSON-safe: the shared module keys the breakdown on a (corpus, family)
    # TUPLE, which no JSON report can carry.
    result["symmetric_difference_by_corpus_family"] = {
        f"{corpus}|{family}": count
        for (corpus, family), count in
        result["symmetric_difference_by_corpus_family"].items()
    }
    return result


def _identity_visibility_index(conn: sqlite3.Connection) -> Dict[str, str]:
    """`work_id -> identity_visibility`, READ back from the stored column rather
    than re-derived (136-12 / D-22).

    Schema SS(B1) is explicit that every identity join -- title, author,
    `identity_visibility` -- reads `display_work_id`, NEVER
    `canonical_work_id`: the latter is not unique on `works` (15 duplicated
    groups on the live asset, three with mixed source corpora), so joining on
    it fans out AND leaves `identity_visibility` undefined for exactly the
    groups where a PRIVATE member could otherwise influence a shared public
    aggregate."""
    return {
        work_id: visibility
        for work_id, visibility in conn.execute(
            "SELECT work_id, identity_visibility FROM works"
        )
    }


def _band_measurement_index(
    conn: sqlite3.Connection,
) -> Dict[Tuple[str, str], Tuple[Optional[str], Optional[float]]]:
    """`(evidence_source, confidence_band) -> (measurement_status, ci_low)` from
    the `band_precision` registry -- the two fields `is_default_eligible` reads
    for its `tier_a` certificate gate (D-02a). Rows must therefore already be
    inserted when the identification grain is materialized."""
    index: Dict[Tuple[str, str], Tuple[Optional[str], Optional[float]]] = {}
    for evidence_source, confidence_band, measurement_status, ci_low in conn.execute(
        "SELECT evidence_source, confidence_band, measurement_status, ci_low "
        "FROM band_precision WHERE scope = 'band'"
    ):
        index[(evidence_source, confidence_band)] = (measurement_status, ci_low)
    return index


def _page_competition_index(
    rows: List[sqlite3.Row], kept_tie_pages: set
) -> Dict[str, Dict[str, bool]]:
    """`page_id -> {canonical_work_id -> has_unresolved_competitor}`.

    Gate 3 of the main-pool rule
    (`.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md`):
    *"Unresolved competition on every matched page (an overlapping near-tie span
    from another canonical work, or a `kept_tie` page)"*. Both halves are
    implemented here: a `kept_tie` audit row marks the whole page contested, and
    otherwise a canonical work is contested on a page when its span overlaps a
    span claimed there by a DIFFERENT canonical work.

    Cheap by construction: the live v2 asset carries at most 20 evidence rows
    and 9 distinct canonical works on any one page."""
    spans_by_page: Dict[str, List[Tuple[str, int, int]]] = {}
    for row in rows:
        spans_by_page.setdefault(row["a_page_id"], []).append(
            (row["canonical_work_id"], row["span_start"], row["span_end"])
        )

    index: Dict[str, Dict[str, bool]] = {}
    for page_id, spans in spans_by_page.items():
        works_on_page = {cwid for cwid, _s, _e in spans}
        page_is_kept_tie = page_id in kept_tie_pages
        per_work: Dict[str, bool] = {}
        for cwid in works_on_page:
            if page_is_kept_tie:
                per_work[cwid] = True
                continue
            mine = [(s, e) for w, s, e in spans if w == cwid]
            others = [(s, e) for w, s, e in spans if w != cwid]
            per_work[cwid] = any(
                _spans_overlap(s0, e0, s1, e1) for (s0, e0) in mine for (s1, e1) in others
            )
        index[page_id] = per_work
    return index


def populate_discovery_identification(conn: sqlite3.Connection) -> Dict:
    """Materialize `discovery_identification` -- ONE row per
    `(sys_id, canonical_work_id)`, grouping all of that identification's
    page-claims.

    **Eligibility is `shipped` OR `human_confirmed`, not `shipped` alone.** This
    is the second half of the D-13g fix. The service restores review-only
    human-confirmed rows to the page query; if this table held only shipped
    identifications, an inner join would drop those rows a second time and a
    left join would leave them with no bucket and no reason -- either way the
    fix would be undone one layer down. `eligibility_basis` records WHICH of the
    two rules admitted each row, so a surface can render the coverage note
    D-13g calls for.

    `main_pool`/`main_pool_reason` come from
    `shared.discovery_main_pool.main_pool_decision` -- the shared rule, never a
    second implementation (T-136-11-01).

    Two build-time assertions run before returning; either failing raises
    `IdentificationGrainError` rather than silently changing the corpus total.
    """
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT de.evidence_id     AS evidence_id,
                   de.sys_id          AS sys_id,
                   de.a_page_id       AS a_page_id,
                   de.evidence_source AS evidence_source,
                   de.confidence_band AS confidence_band,
                   de.adjudication_status AS adjudication_status,
                   de.routing_status  AS routing_status,
                   de.routing_reason  AS routing_reason,
                   de.band_rank       AS band_rank,
                   de.coverage_ppm    AS coverage_ppm,
                   de.matched_letters AS matched_letters,
                   de.span_start      AS span_start,
                   de.span_end        AS span_end,
                   de.novelty_status  AS novelty_status,
                   de.divergence_correctness AS divergence_correctness,
                   de.assertion_visibility AS assertion_visibility,
                   dc.claim_type      AS claim_type,
                   w.canonical_work_id AS canonical_work_id
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w            ON w.work_id  = dc.work_id
            WHERE de.routing_status = ? OR de.adjudication_status = ?
            """,
            (ids.ROUTING_STATUS_SHIPPED, ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED),
        ).fetchall()
        kept_tie_pages = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT page_id FROM discovery_routing_audit "
                "WHERE decision = 'kept_tie' AND page_id IS NOT NULL"
            )
        }
        canonical_groups = _canonical_group_index(conn)
        identity_visibilities = _identity_visibility_index(conn)
        band_measurements = _band_measurement_index(conn)
    finally:
        conn.row_factory = None

    competition = _page_competition_index(rows, kept_tie_pages)

    groups: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for row in rows:
        groups.setdefault((row["sys_id"], row["canonical_work_id"]), []).append(row)

    insert_rows = []
    reason_counts: Dict[str, int] = {}
    n_main = 0
    n_restored = 0
    for (sys_id, canonical_work_id), group in sorted(groups.items()):
        # The identification's OWN best evidence row: lowest band_rank, then
        # lexicographic evidence_id (the D-13b tie-break, reused verbatim).
        best = min(group, key=lambda r: ((r["band_rank"] if r["band_rank"] is not None
                                          else BAND_RANK_LATTICE_SIZE), r["evidence_id"]))
        best_band_rank = (best["band_rank"] if best["band_rank"] is not None
                          else BAND_RANK_LATTICE_SIZE)
        measurement_status, ci_low = band_measurements.get(
            (best["evidence_source"], best["confidence_band"]), (None, None)
        )

        page_ids = {r["a_page_id"] for r in group}
        page_competition = {
            page_id: competition.get(page_id, {}).get(canonical_work_id, False)
            for page_id in page_ids
        }

        coverage_values = [r["coverage_ppm"] for r in group if r["coverage_ppm"] is not None]
        max_coverage_ppm = max(coverage_values) if coverage_values else None
        direct_matched = [
            r["matched_letters"] for r in group
            if r["evidence_source"] == ids.EVIDENCE_SOURCE_TRACK1_DIRECT
            and r["matched_letters"] is not None
        ]
        max_matched_letters = max(direct_matched) if direct_matched else None

        any_human_confirmed = any(
            r["adjudication_status"] == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED for r in group
        )
        any_shipped = any(r["routing_status"] == ids.ROUTING_STATUS_SHIPPED for r in group)
        # D-13g: which rule admitted this identification. `human_confirmed` is
        # recorded only when routing alone would NOT have admitted it -- those
        # are exactly the rows the service restores and the surface annotates.
        eligibility_basis = (
            ids.ROUTING_STATUS_SHIPPED if any_shipped
            else ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED
        )
        if not any_shipped:
            n_restored += 1

        in_main_pool, main_pool_reason = main_pool_decision(MainPoolIdentification(
            has_same_work_claim=any(
                r["claim_type"] == ids.CLAIM_TYPE_DIRECT_WITNESS for r in group
            ),
            any_human_confirmed=any_human_confirmed,
            best_evidence_source=best["evidence_source"],
            best_confidence_band=best["confidence_band"],
            best_adjudication_status=best["adjudication_status"],
            best_routing_status=best["routing_status"],
            best_measurement_status=measurement_status,
            best_ci_low=ci_low,
            page_has_unresolved_competitor=page_competition,
            max_matched_letters=max_matched_letters,
            max_coverage=(None if max_coverage_ppm is None
                          else max_coverage_ppm / COVERAGE_PPM_SCALE),
        ))
        if main_pool_reason not in MAIN_POOL_REASONS:  # pragma: no cover -- defensive
            raise IdentificationGrainError(
                f"main_pool_reason {main_pool_reason!r} is outside the closed vocabulary"
            )
        reason_counts[main_pool_reason] = reason_counts.get(main_pool_reason, 0) + 1
        if in_main_pool:
            n_main += 1

        relation_kind = min(
            (r["claim_type"] for r in group),
            key=lambda ct: _CLAIM_TYPE_STRENGTH.get(ct, 99),
        )

        display_work_id = select_display_work_id(
            canonical_work_id, canonical_groups.get(canonical_work_id, []))

        # 136-12 (D-22), at the AGGREGATE grain. An identification row is a
        # summary over several evidence rows, so it may call its assertion
        # origin public only when EVERY contributing assertion is public --
        # otherwise a "public" label would sit on an aggregate (page_count,
        # best_band_rank, max_coverage_ppm) that a restricted assertion helped
        # produce. Conservative by construction: `all()` over an empty group is
        # unreachable here (a group exists because it has rows), and any row
        # that failed closed to `private` carries the whole identification with
        # it. The identity axis reads the DISPLAY work, per schema SS(B1).
        assertion_vis = (
            VISIBILITY_PUBLIC
            if all(r["assertion_visibility"] == VISIBILITY_PUBLIC for r in group)
            else VISIBILITY_PRIVATE
        )
        identity_vis = identity_visibilities.get(display_work_id, VISIBILITY_PRIVATE)

        insert_rows.append((
            identification_id(sys_id, canonical_work_id),
            sys_id,
            canonical_work_id,
            display_work_id,
            1 if in_main_pool else 0,
            main_pool_reason,
            best_band_rank,
            len(page_ids),
            max_coverage_ppm,
            relation_kind,
            eligibility_basis,
            # 136-12: INHERITED from this identification's own BEST evidence row
            # (lowest band_rank, then the D-13b lexicographic evidence_id
            # tie-break -- already resolved as `best` above), never re-derived
            # and never defaulted once the axis is wired. A canonical group can
            # span two `works` rows with different reviewed identities, so
            # "whichever row the group returns" would be nondeterministic; the
            # existing total order is reused rather than a second one invented.
            best["novelty_status"] or _IDENTIFICATION_NOVELTY_DEFAULT,
            # Ruling L: human/owner annotation only -- carried through from the
            # evidence row (which is NULL on every row today) rather than
            # hardcoded, so an annotation pathway lands here for free.
            best["divergence_correctness"],
            assertion_vis,
            identity_vis,
            # CD batch / Amendment 2026-08-12 (P): the SAME best row's own
            # routing_reason (Codex finding 2's rule) -- the reading under
            # which the A0a-2 census counted the 173 router-flagged rows.
            best["routing_reason"],
            # C-track: inserted fail-closed and OVERWRITTEN a few lines below by
            # `relation_matrix.recompute_and_store`, in this same transaction.
            # Contract 1's step 4 is a work-level aggregate over the asset's own
            # rows, so no single row can know its relation while the table is
            # still being filled; recomputing from the COMMITTED table is also
            # what the projector and the verifier do, which is what keeps the
            # three from drifting.
            ids.RENDERED_RELATION_FAIL_CLOSED,
        ))

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO discovery_identification (
            identification_id, sys_id, canonical_work_id, display_work_id,
            main_pool, main_pool_reason, best_band_rank, page_count,
            max_coverage_ppm, relation_kind, eligibility_basis,
            novelty_status, divergence_correctness,
            assertion_visibility, identity_visibility,
            routing_reason, rendered_relation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        insert_rows,
    )

    # C-track / Contract 1: replace the fail-closed placeholder with the
    # matrix's verdict, computed over the now-complete table by the ONE shared
    # implementation the verifier recomputes with.
    relation_counts = relation_matrix.recompute_and_store(
        conn, relation_matrix.DEPLOY_1_PARAMETERIZATION
    )

    counts = assert_identification_grain_consistent(conn)
    n_rows = counts["identifications"]

    n_duplicate_groups = sum(1 for members in canonical_groups.values() if len(members) > 1)
    return {
        "identifications": n_rows,
        "identifications_shipped_only": counts["identifications_shipped_only"],
        "identifications_restored_by_human_confirmed": n_restored,
        "identifications_main_pool": n_main,
        "identifications_more_matches": n_rows - n_main,
        "main_pool_reason_counts": reason_counts,
        "duplicate_canonical_groups": n_duplicate_groups,
        # C-track: the rendered-relation census, so a build log shows what the
        # matrix actually moved instead of only that it ran.
        "rendered_relation_counts": relation_counts,
    }


def assert_identification_grain_consistent(conn: sqlite3.Connection) -> Dict:
    """The TWO build-time consistency assertions over the materialized grain.

    Extracted as its own callable so a grouping/join error fails the BUILD (and
    is directly testable) instead of silently changing the corpus total.

    1. The number of distinct `(sys_id, canonical_work_id)` pairs across claims
       eligible under `shipped OR human_confirmed` equals the
       `discovery_identification` row count. The shipped-ONLY figure is measured
       alongside it and RETURNED (never enforced), so the delta the D-13g fix
       restores stays visible rather than absorbed into one number.
    2. `SELECT COUNT(*)` over `discovery_identification JOIN works ON
       display_work_id` equals the row count EXACTLY. A fan-out here is the
       65,587-row failure and it is invisible without this assertion -- the
       query simply returns more rows than there are identifications, and every
       downstream count inherits the inflation.
    """
    (n_pairs,) = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT de.sys_id, w.canonical_work_id
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w            ON w.work_id  = dc.work_id
            WHERE de.routing_status = ? OR de.adjudication_status = ?
        )
        """,
        (ids.ROUTING_STATUS_SHIPPED, ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED),
    ).fetchone()
    (n_rows,) = conn.execute("SELECT COUNT(*) FROM discovery_identification").fetchone()
    if n_pairs != n_rows:
        raise IdentificationGrainError(
            f"discovery_identification row count {n_rows} != distinct "
            f"(sys_id, canonical_work_id) pair count {n_pairs} over claims eligible "
            "under shipped OR human_confirmed -- a grouping error would otherwise "
            "change the corpus total silently"
        )

    (n_shipped_only,) = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT de.sys_id, w.canonical_work_id
            FROM discovery_evidence de
            JOIN discovery_claim dc ON dc.claim_id = de.claim_id
            JOIN works w            ON w.work_id  = dc.work_id
            WHERE de.routing_status = ?
        )
        """,
        (ids.ROUTING_STATUS_SHIPPED,),
    ).fetchone()

    (n_joined,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_identification di "
        "JOIN works w ON w.work_id = di.display_work_id"
    ).fetchone()
    if n_joined != n_rows:
        raise IdentificationGrainError(
            f"discovery_identification JOIN works ON display_work_id produced "
            f"{n_joined} rows for {n_rows} identifications -- the identity join "
            "must be exactly 1:1 (schema SS(B1)); a duplicated canonical_work_id "
            "group is fanning out"
        )

    (n_null_display,) = conn.execute(
        "SELECT COUNT(*) FROM discovery_identification WHERE display_work_id IS NULL"
    ).fetchone()
    if n_null_display:
        raise IdentificationGrainError(
            f"{n_null_display} discovery_identification row(s) carry a NULL "
            "display_work_id -- every identification must resolve a representative"
        )

    return {
        "identifications": n_rows,
        "identifications_shipped_only": n_shipped_only,
        "identification_pairs": n_pairs,
        "identification_works_join_rows": n_joined,
    }


# ---------------------------------------------------------------------------
# manuscript_display -- libraries.csv ONLY (T-136-11-03).
# ---------------------------------------------------------------------------

_SORT_KEY_DIGITS = re.compile(r"\d+")
_SORT_KEY_PAD = 8


def normalize_sort_key(text: Optional[str]) -> str:
    """A defensible ordering key for a shelfmark or a library code.

    Raw lexical order is wrong for shelfmarks: "T-S 12.123" sorts BEFORE
    "T-S 12.9" because '1' < '9' character-wise. Zero-padding every digit run to
    a fixed width fixes that while staying a plain TEXT column an index can use.
    Case is folded and whitespace collapsed so trivial transcription differences
    do not scatter neighbours."""
    if not text:
        return ""
    collapsed = " ".join(str(text).split()).upper()
    return _SORT_KEY_DIGITS.sub(lambda m: m.group(0).zfill(_SORT_KEY_PAD), collapsed)


def _load_manuscript_catalogue(libraries_csv_path) -> Dict[str, Tuple[str, str]]:
    """`sys_id -> (library_code, shelfmark_display)` from libraries.csv.

    Mirrors `shared/metadata_manager.py`'s own reader exactly -- digits-only
    sys_id normalization, library_code at column 3, and the SHORTEST non-empty
    pipe-separated `call_numbers` variant as the display shelfmark -- so the
    shelfmark this table stores is the same string the existing panel/browse
    surfaces already show. Reads NOTHING else from the file: no title (column
    7), no locus, no reference text (T-136-11-03)."""
    out: Dict[str, Tuple[str, str]] = {}
    with open(libraries_csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for row in reader:
            if not row or len(row) < 3:
                continue
            raw_sys_id = str(row[0]).strip()
            if raw_sys_id.startswith("#"):  # synthetic marker-block lines
                continue
            digits_sys_id = "".join(ch for ch in raw_sys_id if ch.isdigit())
            if not raw_sys_id and not digits_sys_id:
                continue
            raw_shelves = row[2].split("|")
            shelf = raw_shelves[0].strip() if raw_shelves else ""
            for candidate in raw_shelves:
                candidate = candidate.strip()
                if candidate and (not shelf or len(candidate) < len(shelf)):
                    shelf = candidate
            library_code = row[3].strip() if len(row) > 3 else ""
            if not shelf and not library_code:
                continue
            # Index under the digits-only form (what production sys_ids look
            # like, and what metadata_manager keys on) AND under the raw value,
            # so a non-numeric identifier still resolves. The raw form wins a
            # collision -- an exact match is never displaced by a normalized one.
            if digits_sys_id:
                out.setdefault(digits_sys_id, (library_code, shelf))
            out[raw_sys_id] = (library_code, shelf)
    return out


def populate_manuscript_display(conn: sqlite3.Connection, libraries_csv_path) -> Dict:
    """Materialize `manuscript_display` for every sys_id carrying at least one
    eligible claim.

    This table exists because the paged result set today has no shelfmark, no
    library sort key and no total, which makes server-side sorting by library
    and a visible real total impossible (D-17a).

    Eligibility matches `populate_discovery_identification` exactly (`shipped`
    OR `human_confirmed`) -- a strict superset of "at least one shipped claim",
    chosen so a D-13g-restored identification can never end up with no shelfmark
    to render. A sys_id absent from libraries.csv gets NO row (the columns are
    NOT NULL and there is no catalogue metadata to put in them); the count of
    such manuscripts is returned so the gap is visible rather than silent.

    With no libraries.csv supplied the table stays EMPTY -- the only sanctioned
    source is that file (T-136-11-03), never a fallback that could pull a work
    title or a locus into a masking-scanned asset."""
    claim_sys_ids = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT sys_id FROM discovery_evidence "
            "WHERE routing_status = ? OR adjudication_status = ?",
            (ids.ROUTING_STATUS_SHIPPED, ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED),
        )
    }
    if not libraries_csv_path:
        return {
            "manuscript_display": 0,
            "manuscript_display_sys_ids_with_claims": len(claim_sys_ids),
            "manuscript_display_missing_from_libraries_csv": len(claim_sys_ids),
        }

    catalogue = _load_manuscript_catalogue(libraries_csv_path)
    rows = []
    for sys_id in sorted(claim_sys_ids):
        entry = catalogue.get(sys_id)
        if entry is None:
            continue
        library_code, shelfmark = entry
        rows.append((
            sys_id,
            library_code,
            normalize_sort_key(library_code),
            shelfmark,
            normalize_sort_key(shelfmark),
        ))
    conn.executemany(
        "INSERT INTO manuscript_display "
        "(sys_id, library_code, library_sort_key, shelfmark_display, shelfmark_sort_key) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    return {
        "manuscript_display": len(rows),
        "manuscript_display_sys_ids_with_claims": len(claim_sys_ids),
        "manuscript_display_missing_from_libraries_csv": len(claim_sys_ids) - len(rows),
    }


# ===========================================================================
# 6.9b 136-12 Task 1: NOVELTY SHADE ingestion (the ten-value enum) across BOTH
# evidence families, at the centralized (sys_id, novelty_work_key) grain, with
# MASKED provenance.
#
# Why this exists at all: the frozen v2 asset only ever computed novelty for
# the `propagated` family, so all 144,294 shipped `track1_direct` rows sit at
# `is_new = 0` -- a value that means UNCHECKED, not "already recorded". A
# two-state read of that data would tell a reader 144,294 findings are already
# in the finding aids, which is false on the flagship surface.
#
# EVERY vocabulary decision here is delegated to `shared/discovery_novelty.py`
# (the contract module, `docs/specs/discovery-novelty-v1.md`) -- this file
# holds no second copy of the ten shades, no second masked-label table and no
# second `divergence_correctness` applicability test.
# ===========================================================================

class NoveltyVerdictCacheError(RuntimeError):
    """Raised when the hash-pinned novelty verdict cache fails its SHA-256 pin,
    is absent, is unparseable, or is not the frozen `{key: {...}}` shape.

    The pin is the whole point (T-136-12-03): the verdict cache is an external,
    expensive build input, and a cache that is not the cache that was MEASURED
    is not a pinned input. Refusing is always correct -- every row it would have
    resolved simply falls back to `not_checked`, which is what `not_checked`
    means."""


class NoveltyIngestError(RuntimeError):
    """Raised when a build-time novelty invariant fails: an unmasked
    `novelty_source_label` reached a column (T-136-12-02), or the evidence rows
    of ONE claim disagree about their novelty result (D-23a).

    Both are HARD build failures. The live v1 asset already contains 665 claims
    whose own evidence rows disagree on the legacy `is_new` boolean; the build
    must not be able to produce a 666th."""


# The frozen verdict-cache shape. `scripts/discovery_novelty_production_run.py`
# writes exactly this: a JSON object keyed `"{sys_id}::{ref_work_id}"`, each
# value an object carrying at least `novelty_status`.
_NOVELTY_KEY_SEPARATOR = "::"

# Keys a verdict entry may carry. `divergence_correctness` is TOLERATED and
# then DROPPED -- see `load_novelty_verdicts` (owner ruling L).
_NOVELTY_ENTRY_KEYS = frozenset({
    "novelty_status", "divergence_correctness", "source_code",
    # discovery-v3 (2026-08-07, Codex blocker 3): the per-pair INPUT fingerprint
    # (`scripts/discovery_novelty_funnel.candidate_input_fingerprint`). Covers
    # every field `render_case` sends -- claimed title, claimed author, the five
    # per-source evidence texts -- plus the pinned model/version/effort/prompt/
    # normalization identifiers. TOLERATED as absent so a pre-v3 cache still
    # loads, but then the entry is a MISS (see `load_novelty_verdicts`): an
    # unfingerprinted verdict cannot prove the question it answered.
    "input_fingerprint",
})


def novelty_grain_key(sys_id: str, work_key: str) -> str:
    """The centralized (manuscript, reviewed-work) grain key (D-23a/D-23d).

    `work_key` is a `shared.discovery_novelty.novelty_work_key` result -- the
    ALIAS-GROUP representative, never a raw id and never the over-collapsed
    `canonical_work_id` (one collapsed id covers 39 Bible books). Keying on the
    reviewed identity is what makes "known via ANY alias implies confirms"
    hold without the caller testing every spelling."""
    return f"{sys_id}{_NOVELTY_KEY_SEPARATOR}{work_key}"


def _load_novelty_fingerprints(path) -> Optional[Dict[str, str]]:
    """Read the `{grain_key: input_fingerprint}` map for the CLI path.

    Fails closed on a malformed file rather than degrading to "no fingerprints":
    a caller who supplied the flag intends the gate to run, and silently skipping
    it would be worse than not offering the flag at all.
    """
    if path is None:
        return None
    doc = _json_loads_strict(Path(path).read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or not doc:
        raise NoveltyVerdictCacheError(
            "novelty input-fingerprint file must be a non-empty JSON object of "
            "{'{sys_id}::{work_id}': fingerprint}"
        )
    bad = sum(
        1 for k, v in doc.items()
        if not isinstance(k, str) or not isinstance(v, str) or not v
    )
    if bad:
        raise NoveltyVerdictCacheError(
            f"{bad} novelty input-fingerprint entr(ies) are not a non-empty "
            f"string-to-string mapping (keys/values withheld -- masking)"
        )
    return doc


def load_novelty_verdicts(
    path, *, sha256: Optional[str],
    expected_fingerprints: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Dict], Dict]:
    """Load the hash-pinned novelty verdict cache. Returns `(entries, stats)`.

    Order of enforcement, all BEFORE a single verdict is read into the build:
    file present -> SHA-256 pin -> strict duplicate-key JSON parse -> top-level
    object -> per-entry frozen shape -> per-entry INPUT FINGERPRINT.

    **The fingerprint gate (discovery-v3, Codex blocker 3).** The whole-file
    SHA-256 above proves the file is the measured FILE. It cannot prove any entry
    answers the QUESTION this build is asking: entries key on
    `(sys_id, work_key)`, while the prompt also carries the claimed title, the
    claimed author and the assembled finding-aid text -- all sourced from
    artifacts that change between runs. So a title correction, an alias-group
    rebuild or a finding-aid refresh would silently reuse an answer to a
    different question.

    When `expected_fingerprints` is supplied (`{grain_key: fingerprint}` from
    `candidate_input_fingerprint`), every entry must carry a matching
    `input_fingerprint`. An entry that does not is treated as a MISS -- resolved
    to the fail-closed `not_checked` default and counted, exactly like an
    out-of-vocabulary status. NOT an error: a legitimately-changed input SHOULD
    produce a miss, and raising would turn a routine input refresh into a build
    failure. What it must never do is answer.

    Passing `None` keeps the pre-v3 behaviour (no fingerprint check) so a v2
    rebuild against the v2-era cache still works. A v3 build MUST pass it; gate
    13 asserts a changed title becomes a miss.

    `sha256` is REQUIRED (a `None` raises). The verdict cache is the single
    most expensive build input in this phase and the one whose substitution
    would silently change the flagship "Candidates for new finds" filter, so
    there is no unpinned load path at all -- not even a warned one.

    FAIL-CLOSED per entry, never fatal: a missing, non-string or
    out-of-vocabulary `novelty_status` resolves to
    `shared.discovery_novelty.DEFAULT_STATUS` (`not_checked`) and is COUNTED,
    rather than raising. A machine-produced cache is allowed to contain a row
    the pipeline could not answer; what it is NOT allowed to do is turn that
    into a positive verdict.

    **Owner ruling L (`136-GATE1-DECISIONS.md` SS L, 2026-08-03).** Any
    `divergence_correctness` key in the cache is DROPPED here and counted --
    never carried into the build. The model no longer produces this field
    (`resolve_model_output` always returns `None` for it), so a value appearing
    in a cache can only be a stale pre-ruling-L entry or a hallucinated key;
    either way it is not owner-supplied and must not reach the asset. The
    column is populated ONLY by a human/owner annotation pathway, which does
    not exist yet.

    Masking (NOVEL-02 / T-136-12-02): `source_code` is consumed here and never
    stored. Only `masked_provenance_label`'s pre-written output crosses into
    the returned structure, so a restricted-corpus code cannot reach a column,
    a log line or an error message from this function -- including its error
    paths, which never echo a key or a value.
    """
    if sha256 is None:
        raise NoveltyVerdictCacheError(
            "novelty verdict cache supplied without a SHA-256 pin -- refusing to load. "
            "A build input that is not the input that was measured is not a pinned input "
            "(136-12 Task 1 / T-136-12-03)."
        )
    p = Path(path)
    if not p.exists():
        raise NoveltyVerdictCacheError(f"novelty verdict cache not found: {path}")
    actual_sha = _hash_file(p)
    if actual_sha != sha256:
        raise NoveltyVerdictCacheError(
            "novelty verdict cache SHA-256 pin mismatch (hash gate) -- refusing to read "
            "any verdict. Re-pin against the run record (136-NOVELTY-RUN.md) or re-run "
            "the gate; never build against an unproven cache."
        )
    try:
        doc = _json_loads_strict(p.read_text(encoding="utf-8"))
    except ValueError as e:
        raise NoveltyVerdictCacheError(f"novelty verdict cache parse error: {e}") from e
    if not isinstance(doc, dict):
        raise NoveltyVerdictCacheError(
            "novelty verdict cache top-level value must be a JSON object"
        )

    entries: Dict[str, Dict] = {}
    stats = {
        "verdict_cache_sha256": actual_sha,
        "verdict_entries": 0,
        "verdict_entries_malformed_key": 0,
        "verdict_entries_failed_closed": 0,
        "verdict_entries_divergence_correctness_dropped": 0,
        # discovery-v3 (Codex blocker 3). Split so an operator can tell a cache
        # built before the fingerprint existed (all `unfingerprinted`) from one
        # whose INPUTS moved under it (`fingerprint_mismatch`) -- the first is a
        # one-off migration, the second means an artifact changed and the spend
        # is real. Reported, so "how much of the cache still applies" is a number
        # rather than an assumption.
        "verdict_entries_unfingerprinted": 0,
        "verdict_entries_fingerprint_mismatch": 0,
        "verdict_entries_fingerprint_ok": 0,
        "verdict_fingerprint_checked": expected_fingerprints is not None,
    }
    for raw_key, raw_entry in doc.items():
        if not isinstance(raw_key, str) or _NOVELTY_KEY_SEPARATOR not in raw_key:
            stats["verdict_entries_malformed_key"] += 1
            continue
        if not isinstance(raw_entry, dict) or set(raw_entry) - _NOVELTY_ENTRY_KEYS:
            raise NoveltyVerdictCacheError(
                "novelty verdict cache entry is not the frozen "
                f"{sorted(_NOVELTY_ENTRY_KEYS)} shape (entry key withheld -- masking)"
            )
        if raw_entry.get("divergence_correctness") is not None:
            stats["verdict_entries_divergence_correctness_dropped"] += 1
        status = raw_entry.get("novelty_status")
        if status not in NOVELTY_STATUSES:
            status = NOVELTY_DEFAULT_STATUS
            stats["verdict_entries_failed_closed"] += 1
        # THE FINGERPRINT GATE. Applied AFTER the status is resolved and BEFORE
        # the entry is stored, so a mismatched entry cannot contribute a positive
        # verdict by any path. Demoting to the default rather than dropping the
        # key keeps the row present-and-unanswered, which is what the surfaces
        # already handle; dropping it would make a changed input look like a
        # candidate that was never generated.
        if expected_fingerprints is not None:
            got = raw_entry.get("input_fingerprint")
            want = expected_fingerprints.get(raw_key)
            if not isinstance(got, str) or not got:
                stats["verdict_entries_unfingerprinted"] += 1
                if status != NOVELTY_DEFAULT_STATUS:
                    status = NOVELTY_DEFAULT_STATUS
                    stats["verdict_entries_failed_closed"] += 1
            elif want is None or got != want:
                # `want is None` means this build did not generate the pair at
                # all, so no question was asked and no answer applies.
                stats["verdict_entries_fingerprint_mismatch"] += 1
                if status != NOVELTY_DEFAULT_STATUS:
                    status = NOVELTY_DEFAULT_STATUS
                    stats["verdict_entries_failed_closed"] += 1
            else:
                stats["verdict_entries_fingerprint_ok"] += 1
        sys_id, _sep, ref_work_id = raw_key.partition(_NOVELTY_KEY_SEPARATOR)
        entries[raw_key] = {
            "sys_id": sys_id,
            "ref_work_id": ref_work_id,
            "novelty_status": status,
            # The MASKED label only -- the raw code dies here. `None` when the
            # cache named no source, which `novelty_columns_for` then stores as
            # NULL for the eligible shades too.
            "source_label": (
                masked_provenance_label(raw_entry["source_code"])
                if raw_entry.get("source_code") is not None else None
            ),
            # Ruling L: structurally absent, never read from the cache.
            "divergence_correctness": None,
        }
        stats["verdict_entries"] += 1
    return entries, stats


def build_novelty_grain_index(
    entries: Dict[str, Dict], alias_groups: Optional[Dict] = None,
) -> Tuple[Dict[str, Dict], Dict]:
    """Collapse raw `(sys_id, ref_work_id)` verdict entries onto the CENTRALIZED
    `(sys_id, novelty_work_key)` grain (D-23a/D-23d).

    A work with no reviewable identity at all (`novelty_work_key` returns
    `None`) is DROPPED and counted -- its evidence rows then fall through to
    `not_checked`, never to a guessed key.

    When two raw ids of the SAME curated alias group carry DIFFERENT shades for
    one manuscript, the grain FAILS CLOSED to `not_checked` and the conflict is
    counted. Alias-group members are by curation the same work, so a
    disagreement is an input defect; resolving it by picking a side would
    manufacture a verdict nobody produced."""
    index: Dict[str, Dict] = {}
    stats = {"grain_keys": 0, "grain_unkeyable_works": 0, "grain_alias_conflicts": 0}
    for entry in entries.values():
        work_key = novelty_work_key({"work_id": entry["ref_work_id"]}, alias_groups)
        if work_key is None:
            stats["grain_unkeyable_works"] += 1
            continue
        key = novelty_grain_key(entry["sys_id"], work_key)
        existing = index.get(key)
        if existing is None:
            index[key] = dict(entry)
            continue
        if existing["novelty_status"] != entry["novelty_status"]:
            stats["grain_alias_conflicts"] += 1
            index[key] = {
                **existing,
                "novelty_status": NOVELTY_DEFAULT_STATUS,
                "source_label": None,
            }
    stats["grain_keys"] = len(index)
    return index, stats


def apply_novelty_verdicts(
    conn: sqlite3.Connection,
    grain_index: Optional[Dict[str, Dict]] = None,
    *,
    alias_groups: Optional[Dict] = None,
) -> Dict:
    """Write `novelty_status` / `novelty_source_label` / `divergence_correctness`
    onto EVERY `discovery_evidence` row of BOTH families.

    Reads the already-inserted claims/evidence/works out of the output DB, so
    the synthetic and the real build paths -- which assemble their rows through
    deliberately independent code -- share ONE implementation of the ingest,
    exactly as they already share `populate_discovery_identification`.

    Called with `grain_index=None` (no verdict cache supplied) this is a
    deliberate NO-OP over the values -- every row keeps the DDL default
    `not_checked` -- while still running both build-time invariants. "The gate
    did not run" and "the gate ran and found nothing" must never be the same
    stored fact, and `not_checked` is the token that says the former.

    `divergence_correctness` is NEVER written here (owner ruling L): the model
    no longer produces it and no human/owner annotation pathway exists yet, so
    the column stays NULL on every row. It is passed to `novelty_columns_for`
    as `None` explicitly rather than omitted, so the day an annotation artifact
    DOES exist, this is the one line that changes.
    """
    grain_index = grain_index or {}
    rows = conn.execute(
        """
        SELECT de.evidence_id, de.sys_id, de.evidence_source, dc.work_id
        FROM discovery_evidence de
        JOIN discovery_claim dc ON dc.claim_id = de.claim_id
        """
    ).fetchall()

    updates = []
    per_shade: Dict[str, int] = {}
    per_family_resolved: Dict[str, int] = {}
    n_unkeyable = 0
    n_no_verdict = 0
    for evidence_id, sys_id, evidence_source, work_id in rows:
        work_key = novelty_work_key({"work_id": work_id}, alias_groups)
        if work_key is None:
            # No reviewable identity -> not_checked. Never a guessed key.
            n_unkeyable += 1
            verdict = None
        else:
            verdict = grain_index.get(novelty_grain_key(sys_id, work_key))
            if verdict is None:
                n_no_verdict += 1

        status = verdict["novelty_status"] if verdict else NOVELTY_DEFAULT_STATUS
        source_label = verdict["source_label"] if verdict else None
        cols = novelty_columns_for(
            status,
            divergence_correctness=None,  # ruling L -- human/owner annotation only
            source_label=source_label,
        )
        per_shade[cols["novelty_status"]] = per_shade.get(cols["novelty_status"], 0) + 1
        if cols["novelty_status"] != NOVELTY_DEFAULT_STATUS:
            per_family_resolved[evidence_source] = per_family_resolved.get(evidence_source, 0) + 1
        updates.append((
            cols["novelty_status"], cols["novelty_source_label"],
            cols["divergence_correctness"], evidence_id,
        ))

    conn.executemany(
        "UPDATE discovery_evidence SET novelty_status = ?, novelty_source_label = ?, "
        "divergence_correctness = ? WHERE evidence_id = ?",
        updates,
    )

    assert_novelty_source_labels_masked(conn)
    assert_one_novelty_result_per_claim(conn)

    return {
        "novelty_evidence_rows": len(updates),
        "novelty_shade_counts": per_shade,
        "novelty_resolved_by_family": per_family_resolved,
        "novelty_rows_without_a_reviewable_work_key": n_unkeyable,
        "novelty_rows_without_a_verdict": n_no_verdict,
    }


def assert_novelty_source_labels_masked(conn: sqlite3.Connection) -> int:
    """T-136-12-02: every distinct `novelty_source_label` actually written must
    be a member of `shared.discovery_novelty.MASKED_PROVENANCE_LABELS`.

    This is the assertion that makes the masking property a BUILD failure
    rather than a shipping accident. `masked_provenance_label` is already
    structurally incapable of returning an unmasked value (it never echoes its
    input), so a violation here means some OTHER code path wrote this column --
    which is exactly the thing worth failing on.

    The violation message names the table, the column and the COUNT, never the
    offending value: echoing it would perform the very leak the check exists to
    prevent."""
    labels = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT novelty_source_label FROM discovery_evidence "
            "WHERE novelty_source_label IS NOT NULL"
        )
    ]
    unmasked = [v for v in labels if v not in MASKED_PROVENANCE_LABELS]
    if unmasked:
        raise NoveltyIngestError(
            f"discovery_evidence.novelty_source_label carries {len(unmasked)} distinct value(s) "
            "outside the masked label set (values withheld -- naming them here would be the "
            "leak this assertion exists to prevent; NOVEL-02 / D-25)"
        )
    return len(labels)


def assert_one_novelty_result_per_claim(conn: sqlite3.Connection) -> int:
    """D-23a: every evidence row of ONE claim inherits ONE novelty result.

    The result is all THREE stored columns, not the shade alone -- a claim
    whose rows agree on `fills_gap` but disagree on which source label
    justified it is just as incoherent to a reader as one that disagrees on the
    shade.

    A HARD build failure. The live v1 asset already carries 665 claims whose
    own evidence rows disagree on the legacy `is_new` boolean (of 29,054
    multi-evidence claims) -- this build must not be able to produce a 666th.
    """
    (n_disagreeing,) = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT claim_id FROM discovery_evidence
            GROUP BY claim_id
            HAVING COUNT(DISTINCT novelty_status) > 1
                OR COUNT(DISTINCT COALESCE(novelty_source_label, '')) > 1
                OR COUNT(DISTINCT COALESCE(divergence_correctness, '')) > 1
        )
        """
    ).fetchone()
    if n_disagreeing:
        raise NoveltyIngestError(
            f"{n_disagreeing} claim(s) carry evidence rows that DISAGREE about their novelty "
            "result -- every evidence row of one claim must inherit exactly one "
            "(novelty_status, novelty_source_label, divergence_correctness) (D-23a)"
        )
    return n_disagreeing


# ===========================================================================
# 6.9c 136-12 Task 3: the CURATED artifacts (plan 136-09), loaded by content
# hash through the SAME fail-closed mechanism the existing curated build inputs
# use -- verify the hash, validate the shape, refuse to build on a mismatch.
#
# `works.genre` ALREADY EXISTS (schema SS1.1); Amendment 2026-08-02 (C) is
# explicit that this rebuild does NOT add it and that no ALTER TABLE may target
# it. What changes is that the column becomes POPULATED from the curated,
# hash-pinned artifact and CONSTRAINED to the closed FJMS vocabulary or the
# explicit `Unassigned` value -- never silently NULL-as-absent.
# ===========================================================================

class CuratedArtifactError(ValueError):
    """Raised when a hash-pinned curated artifact (the work-domain assignments
    or the author alias map) fails its content-hash pin, its declared identity,
    its frozen row shape, or -- for the domain artifact -- the fail-closed
    release gate that holds an unruled `needs-ruling` row back."""


# `Unassigned` is a REAL value with its own parent, not missing data: a work the
# controlled vocabulary cannot place stays VISIBLE in the corpus view rather
# than disappearing from the facet. Mirrors
# `scripts/curate_work_domains.py::UNASSIGNED`.
GENRE_UNASSIGNED = "Unassigned"

# The stored `works.genre` separator. The value is the FULL PATH
# (`"{domain_parent} / {domain_leaf}"`) because a bare leaf is not identifying --
# several parents carry an `Other` leaf, and the owner's own rulings name these
# nodes as paths ("Rabbinic Literature / Other"). The `Unassigned` bucket is the
# one exception and stores the bare sentinel, so the value a reader filters on
# is literally `Unassigned`.
GENRE_PATH_SEPARATOR = " / "

_DOMAIN_ARTIFACT_NAME = "work_domains"
_DOMAIN_ARTIFACT_VERSION = "v1"
_ALIAS_ARTIFACT_NAME = "work_author_aliases"
_ALIAS_ARTIFACT_VERSION = "v1"
_DOMAIN_ROW_REQUIRED = frozenset(
    {"canonical_work_id", "domain_parent", "domain_leaf", "confidence", "provenance"})


def curated_content_hash(payload) -> str:
    """The curated artifacts' content-hash recipe, reproduced verbatim from
    `scripts/curate_work_domains.py::compute_content_hash`: `sha256:` + a
    SHA-256 over the payload ARRAY only (`sort_keys=True`, `ensure_ascii=False`),
    so the digest is stable under later changes to the artifact's own header
    fields.

    Reproduced rather than imported because that module imports
    `shared.fjms_service` (and through it the 1.59 GB FJMS sidecar) to read the
    live domain tree -- a dependency this stdlib-only build script must not
    acquire. `tests/test_discovery_schema.py` asserts the two implementations
    agree, so the duplication is a red suite rather than a silent divergence."""
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def work_genre_value(row) -> str:
    """The stored `works.genre` value for ONE curated assignment row.

    The `Unassigned` bucket stores the bare sentinel (so a reader filters on
    literally `Unassigned`); every other row stores the full
    `"{parent} / {leaf}"` path."""
    parent = row.get("domain_parent")
    leaf = row.get("domain_leaf")
    if parent == GENRE_UNASSIGNED and leaf == GENRE_UNASSIGNED:
        return GENRE_UNASSIGNED
    return f"{parent}{GENRE_PATH_SEPARATOR}{leaf}"


def load_work_domains(path, *, content_hash: Optional[str]) -> Tuple[Dict[str, str], Dict]:
    """Load the hash-pinned curated work-domain artifact. Returns
    `(genre_by_canonical_work_id, stats)`.

    Order of enforcement, all BEFORE a single value is written: file present ->
    strict duplicate-key JSON parse -> declared artifact identity -> the
    artifact's OWN declared content hash recomputed and matched -> the caller's
    pin matched -> per-row frozen shape -> the release gate.

    **The release gate keys on `owner_ruling`, NEVER on `confidence`** (plan
    136-09 / `136-DOMAIN-CURATION.md` item 4, owner rulings P and Q). The 29
    ruled rows deliberately RETAIN `confidence: "needs-ruling"`; the
    `owner_ruling` citation is what marks them settled. Reading `confidence`
    here would reject all 29 owner-ruled rows and refuse a build the owner has
    already authorized -- and, worse, a future artifact could flip a row to
    `high` without a ruling and slip through. A `needs-ruling` row carrying no
    `owner_ruling` is HELD and REFUSES the build, exactly as
    `curate_work_domains.py --validate --release` does.
    """
    doc, declared = _load_curated_artifact(
        path, content_hash=content_hash, payload_key="assignments",
        artifact=_DOMAIN_ARTIFACT_NAME, version=_DOMAIN_ARTIFACT_VERSION,
    )
    assignments = doc["assignments"]

    genre_by_work: Dict[str, str] = {}
    held: List[str] = []
    n_unassigned = 0
    for row in assignments:
        missing = _DOMAIN_ROW_REQUIRED - set(row)
        if missing:
            raise CuratedArtifactError(
                f"work_domains assignment row is missing {sorted(missing)} "
                "(row key withheld -- masking)"
            )
        work_id = row["canonical_work_id"]
        if not _is_w_id(work_id):
            raise CuratedArtifactError(
                "work_domains assignment row is keyed on a non-opaque work id")
        if work_id in genre_by_work:
            raise CuratedArtifactError(
                f"work_domains carries a duplicate assignment for {work_id}")
        if row.get("confidence") == "needs-ruling" and not row.get("owner_ruling"):
            held.append(work_id)
            continue
        if row.get("domain_parent") is None or row.get("domain_leaf") is None:
            raise CuratedArtifactError(
                f"work_domains row {work_id} carries a null domain with no held posture")
        value = work_genre_value(row)
        if value == GENRE_UNASSIGNED:
            n_unassigned += 1
        genre_by_work[work_id] = value

    if held:
        raise CuratedArtifactError(
            f"RELEASE GATE: {len(held)} work_domains row(s) are still HELD for owner ruling "
            "(confidence='needs-ruling' with no owner_ruling citation) -- refusing to load "
            "works.genre. The 'ship as Unassigned' default was explicitly DECLINED by the "
            "owner (136-GATE1-DECISIONS.md SS D)."
        )

    return genre_by_work, {
        "work_domains_content_hash": declared,
        "work_domains_assignments": len(assignments),
        "work_domains_assigned": len(genre_by_work),
        "work_domains_unassigned": n_unassigned,
    }


def load_work_author_aliases(path, *, content_hash: Optional[str]) -> Tuple[Dict[str, Dict], Dict]:
    """Load the hash-pinned curated author-alias artifact. Returns
    `(alias_by_normalized_author, stats)`.

    **Deliberately does NOT write a column.** Schema Amendment 2026-08-02 states
    "Nothing outside this list is authorized to appear in the asset", and no
    author-key column is on that list -- `works.author` is the only author
    field, and 136-09 explicitly deferred author CORRECTIONS. So the author key
    is bound to the asset by an ENFORCED COVERAGE CHECK
    (`assert_author_key_coverage`) plus a recorded provenance pin, rather than
    by inventing an unauthorized column. See 136-12-SUMMARY.md; a future plan
    that wants the FJMS person id materialized owes a dated schema amendment
    first."""
    doc, declared = _load_curated_artifact(
        path, content_hash=content_hash, payload_key="aliases",
        artifact=_ALIAS_ARTIFACT_NAME, version=_ALIAS_ARTIFACT_VERSION,
    )
    aliases = doc["aliases"]

    by_normalized: Dict[str, Dict] = {}
    matched = 0
    for row in aliases:
        author = row.get("author")
        normalized = row.get("normalized")
        if not isinstance(author, str) or not isinstance(normalized, str):
            raise CuratedArtifactError(
                "work_author_aliases row carries a non-string author/normalized "
                "(row content withheld -- masking)")
        by_normalized[normalized] = row
        # 2026-08-03 (136-13): ALSO key each row under its RAW author string.
        # `assert_author_key_coverage` compares the asset's own `works.author`
        # values -- which are RAW -- against this index, and 16 of the 96 curated
        # rows normalize to something other than their raw form, so a
        # normalized-ONLY index made the coverage check unsatisfiable by
        # construction (it rejected the live production asset). Two rows also
        # collide on one normalized key, so the raw keys additionally keep the
        # 96th row reachable. Both spellings map to the same row object.
        by_normalized.setdefault(author, row)
        if row.get("match") in ("exact", "containment"):
            matched += 1

    return by_normalized, {
        "work_author_aliases_content_hash": declared,
        "work_author_alias_rows": len(aliases),
        "work_author_alias_matched": matched,
    }


def _load_curated_artifact(path, *, content_hash, payload_key, artifact, version):
    """Shared fail-closed load for both curated artifacts. Returns
    `(doc, declared_content_hash)`."""
    if content_hash is None:
        raise CuratedArtifactError(
            f"{artifact} artifact supplied without a content-hash pin -- refusing to load. "
            "A curated artifact edited after pinning is exactly the tampering case the pin "
            "exists to catch (T-136-12-04)."
        )
    p = Path(path)
    if not p.exists():
        raise CuratedArtifactError(f"{artifact} artifact not found: {path}")
    try:
        doc = _json_loads_strict(p.read_text(encoding="utf-8"))
    except ValueError as e:
        raise CuratedArtifactError(f"{artifact} artifact parse error: {e}") from e
    if not isinstance(doc, dict):
        raise CuratedArtifactError(f"{artifact} artifact top-level value must be a JSON object")
    if doc.get("artifact") != artifact or doc.get("artifact_version") != version:
        raise CuratedArtifactError(
            f"{artifact} artifact identity mismatch -- expected artifact={artifact!r} "
            f"artifact_version={version!r}"
        )
    payload = doc.get(payload_key)
    if not isinstance(payload, list) or not payload:
        raise CuratedArtifactError(f"{artifact} artifact {payload_key!r} must be a non-empty list")
    if not all(isinstance(r, dict) for r in payload):
        raise CuratedArtifactError(f"{artifact} artifact {payload_key!r} rows must be objects")

    declared = doc.get("content_hash")
    recomputed = curated_content_hash(payload)
    if not declared:
        raise CuratedArtifactError(
            f"{artifact} artifact carries no content_hash -- an unpinned artifact is not pinned")
    if declared != recomputed:
        raise CuratedArtifactError(
            f"{artifact} artifact SELF content_hash mismatch -- the payload was edited after "
            "the artifact declared its own hash")
    if declared != content_hash:
        raise CuratedArtifactError(
            f"{artifact} artifact content-hash pin mismatch (hash gate) -- refusing to load. "
            "Re-pin against 136-09-SUMMARY.md; note the PRE-RULING work_domains hash "
            "(sha256:4cc103ff...) is superseded and must never be accepted."
        )
    return doc, declared


def apply_work_genres(conn: sqlite3.Connection, genre_by_work: Dict[str, str]) -> Dict:
    """Write `works.genre` at the CANONICAL grain.

    Assignment is keyed on `canonical_work_id` (136-09's own assignment axis),
    so a D-13a duplicate is never assigned twice and two `works` rows sharing a
    canonical id always agree. No DDL is emitted: the column already exists
    (Amendment (C) forbids re-adding it)."""
    rows = conn.execute("SELECT work_id, canonical_work_id FROM works").fetchall()
    updates = [
        (genre_by_work[canonical], work_id)
        for work_id, canonical in rows if canonical in genre_by_work
    ]
    conn.executemany("UPDATE works SET genre = ? WHERE work_id = ?", updates)
    return {
        "works_genre_written": len(updates),
        "works_genre_unmatched": len(rows) - len(updates),
    }


def assert_author_key_coverage(conn: sqlite3.Connection, alias_index: Dict[str, Dict]) -> Dict:
    """Bind the curated author key to the built asset: every distinct non-NULL
    `works.author` string must appear in the hash-pinned alias artifact.

    This is what "load the author artifact and apply it" means in the absence
    of an authorized destination column -- the curated identity is ENFORCED
    against the asset rather than merely sitting in a file. An author the
    artifact has never seen means the artifact and the asset were built from
    different work sets, which is precisely the drift a pin cannot catch on its
    own.

    The violation message names the COUNT, never the author strings.

    Scope, in two corrections.

    (2026-08-03, 136-13) The check originally scanned ALL of `works`, demanding
    coverage the artifact never claimed to provide -- 12 author strings live
    only on works with zero shipped claims. `curate_work_domains.load_worklist`
    is documented as "every CANONICAL work carrying at least one shipped claim",
    so that was a scope error in the CHECK, not drift in the asset.

    (2026-08-04, Codex code review finding 6) But "shipped" was then too narrow
    in the other direction. Under D-13g a work reaches a public surface if it has
    shipped evidence **OR** human-confirmed evidence, and review-opt-in surfaces
    can return non-shipped works. The human-confirmed-only works happened to be
    covered -- accidentally, not by enforcement.

    The scope is now REACHABILITY, matching `check_works_genre_vocabulary` in the
    verifier: present in `discovery_identification`, or carrying shipped or
    human-confirmed evidence. The predicate is mirrored there rather than shared,
    per the standing independence rule for checks.

    Measured when the scope was widened: reachable and shipped resolve to the
    SAME author set on both live artifacts (47 public, 96 private), so this
    costs nothing today. It is the enforcement that changes, not the outcome --
    a human-confirmed-only work with an uncovered author now fails instead of
    passing by luck.
    """
    has_identification = bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_identification'"
    ).fetchone())
    reachable_clause = (
        """EXISTS (SELECT 1 FROM discovery_identification di
                    WHERE di.canonical_work_id = w.canonical_work_id)
           OR """ if has_identification else ""
    )
    authors = {
        r[0] for r in conn.execute(
            f"""
            SELECT DISTINCT w.author
              FROM works w
             WHERE w.author IS NOT NULL AND w.author != ''
               AND ({reachable_clause}
                    EXISTS (SELECT 1 FROM discovery_claim dc
                              JOIN discovery_evidence e ON e.claim_id = dc.claim_id
                             WHERE dc.work_id = w.work_id
                               AND (e.routing_status = 'shipped'
                                    OR e.adjudication_status = 'human_confirmed')))
            """)
    }
    uncovered = sorted(a for a in authors if a not in alias_index)
    if uncovered:
        raise CuratedArtifactError(
            f"{len(uncovered)} distinct works.author value(s) on works REACHABLE from a "
            "public surface are absent from the pinned author-alias artifact -- the "
            "artifact and the asset were built from different work sets (author strings "
            "withheld -- masking)"
        )
    return {"works_author_strings": len(authors), "works_author_strings_covered": len(authors)}


# ---------------------------------------------------------------------------
# 6.9a FJMS public-vocabulary enrichment (owner decision 2026-07-22): genre =
# modal FJMS domain, author = modal FJMS composition-author, over each work's
# witness sys_ids. Batched `IN (...)` loads (mirrors fjms_service._batch_domains
# / _query_browse_authors_v5), modal computed in Python -- NEVER one round-trip
# per work. FJMS domain/composition-author are public Genizah catalog data, so
# these values are masking-safe for ALL rows (see _pick_public_first_value).
# ---------------------------------------------------------------------------

# The REAL 1.59 GB fjms_enrichment.db lives under fist_data/ (resolved at
# runtime by shared/fjms_service.py); the repo-root fjms_enrichment.db is a
# 0-byte placeholder and must NEVER be used. Mirror fjms_service's
# _SIDECAR_DIR/_SIDECAR_FILENAME rather than importing the (web-framework-
# dependent) service class into this stdlib-only build script.
_FJMS_SIDECAR_DIR = "fist_data"
_FJMS_SIDECAR_FILENAME = "fjms_enrichment.db"
_FJMS_BATCH_SIZE = 500


def resolve_fjms_db_path(explicit_path=None) -> Optional[str]:
    """Resolve the REAL fjms_enrichment.db (never the 0-byte repo-root
    placeholder). An explicit path always wins; otherwise prefer
    `fist_data/fjms_enrichment.db` under the repo root, then the LOCALAPPDATA
    user sidecar (mirroring shared/fjms_service.py). Returns None if no
    candidate is a NON-EMPTY file -- callers then skip FJMS enrichment
    entirely (genre/author fall back to the masking-gated raw path)."""
    if explicit_path:
        return explicit_path
    candidates = [Path(_REPO_ROOT) / _FJMS_SIDECAR_DIR / _FJMS_SIDECAR_FILENAME]
    localappdata = os.environ.get("LOCALAPPDATA")
    if localappdata:
        candidates.append(
            Path(localappdata) / "GenizahSearchPro" / "data"
            / _FJMS_SIDECAR_DIR / _FJMS_SIDECAR_FILENAME
        )
    for c in candidates:
        try:
            if c.is_file() and c.stat().st_size > 0:
                return str(c)
        except OSError:
            continue
    return None


def _connect_fjms_ro(fjms_db_path) -> sqlite3.Connection:
    """Read-only connection to fjms_enrichment.db (join key AlmaId == sys_id)."""
    uri = Path(fjms_db_path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def collect_work_witness_sys_ids(claim_rows: List[Tuple], evidence_rows: List[Tuple]) -> Dict[str, set]:
    """Map each work_id -> the SET of its WITNESS-evidence sys_ids, straight
    from the in-memory distillation (no physical-MS collapse). `evidence_rows`
    is the `assemble_claims_and_evidence` tuple shape: index 1 = claim_id,
    index 2 = evidence_kind, index 11 = sys_id; `claim_rows` index 1 =
    work_id, index 2 = claim_id. shared_text evidence rows are ignored -- the
    genre/author signal is a property of the physical WITNESS manuscripts."""
    claim_id_to_work_id = {row[2]: row[1] for row in claim_rows}
    work_sys_ids: Dict[str, set] = {}
    for ev in evidence_rows:
        if ev[2] != _WITNESS:
            continue
        work_id = claim_id_to_work_id.get(ev[1])
        if work_id is None:
            continue
        work_sys_ids.setdefault(work_id, set()).add(str(ev[11]))
    return work_sys_ids


def _batch_fjms_domains(conn: sqlite3.Connection, sys_ids: List[str]) -> Dict[str, set]:
    """sys_id -> SET of distinct FJMS `domains.Domain` (English) values.
    Batched `IN (...)` (mirrors fjms_service._batch_domains). A set per
    sys_id inherently dedups the 22,006 exact-duplicate (AlmaId, Domain)
    rows, so a later per-work tally over these sets counts COUNT(DISTINCT
    AlmaId) per domain."""
    result: Dict[str, set] = {}
    for i in range(0, len(sys_ids), _FJMS_BATCH_SIZE):
        batch = sys_ids[i:i + _FJMS_BATCH_SIZE]
        placeholders = ",".join("?" * len(batch))
        cur = conn.execute(
            f"SELECT AlmaId, Domain FROM domains WHERE AlmaId IN ({placeholders})",
            batch,
        )
        for alma_id, domain in cur.fetchall():
            if domain is None or str(domain).strip() == "":
                continue
            result.setdefault(str(alma_id), set()).add(domain)
    return result


def _batch_fjms_composition_authors(conn: sqlite3.Connection, sys_ids: List[str]) -> Dict[str, set]:
    """sys_id -> SET of (person_id, EngDesc) COMPOSITION authors, via the
    verified two-path UNION (the `_query_browse_authors_v5` pattern):
      Path 1: catalog(DISTINCT AlmaId, GenizahTitleId, Author)
              JOIN genizah_titles gt ON GenizahTitleId
              JOIN genizah_persons gp ON gt.AuthorId = gp.GenizahPersonId
              WHERE gp.GenizahPersonId > 0
      Path 2: catalog.Author -> genizah_persons.GenizahPersonId
              WHERE GenizahTitleId IS NULL AND Author > 0
    This is the COMPOSITION author (Saadiah Gaon etc.) -- NOT the scribe
    (CopyName) or mentioned-persons (catalog_mentions). Author is SPARSE
    (~10-23% coverage) so most works legitimately resolve to none."""
    result: Dict[str, set] = {}
    for i in range(0, len(sys_ids), _FJMS_BATCH_SIZE):
        batch = sys_ids[i:i + _FJMS_BATCH_SIZE]
        placeholders = ",".join("?" * len(batch))
        sql = f"""
            WITH dc AS (
                SELECT DISTINCT AlmaId, GenizahTitleId, Author
                FROM catalog WHERE AlmaId IN ({placeholders})
            )
            SELECT dc.AlmaId AS alma_id, gp.GenizahPersonId AS person_id, gp.EngDesc AS eng_desc
            FROM dc
            INNER JOIN genizah_titles gt ON dc.GenizahTitleId = gt.GenizahTitleId
            INNER JOIN genizah_persons gp ON gt.AuthorId = gp.GenizahPersonId
            WHERE gp.GenizahPersonId > 0
            UNION
            SELECT dc.AlmaId AS alma_id, gp.GenizahPersonId AS person_id, gp.EngDesc AS eng_desc
            FROM dc
            INNER JOIN genizah_persons gp ON dc.Author = gp.GenizahPersonId
            WHERE dc.GenizahTitleId IS NULL AND dc.Author IS NOT NULL AND dc.Author > 0
        """
        cur = conn.execute(sql, batch)
        for alma_id, person_id, eng_desc in cur.fetchall():
            if person_id is None:
                continue
            result.setdefault(str(alma_id), set()).add((int(person_id), eng_desc or ""))
    return result


def _modal_domain(work_sys_ids: set, sys_domains: Dict[str, set]) -> str:
    """Modal FJMS domain across the work's witness sys_ids, counting
    COUNT(DISTINCT AlmaId) per domain (each sys_id contributes at most 1 per
    domain -- sys_domains values are sets); tie-break domain name ASC."""
    counts: Dict[str, int] = {}
    for sid in work_sys_ids:
        for domain in sys_domains.get(sid, ()):
            counts[domain] = counts.get(domain, 0) + 1
    if not counts:
        return ""
    return min(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))[0]


def _modal_author(work_sys_ids: set, sys_authors: Dict[str, set]) -> str:
    """Modal FJMS composition-author across the work's witness sys_ids,
    counting COUNT(DISTINCT AlmaId) per (person_id, EngDesc); tie-break
    person_id ASC. Returns the EngDesc of the winner ('' if none)."""
    counts: Dict[tuple, int] = {}
    for sid in work_sys_ids:
        for author_key in sys_authors.get(sid, ()):
            counts[author_key] = counts.get(author_key, 0) + 1
    if not counts:
        return ""
    return min(counts.items(), key=lambda kv: (-kv[1], kv[0][0]))[0][1]


def compute_fjms_enrichment(
    conn: sqlite3.Connection, work_witness_sys_ids: Dict[str, set],
) -> Dict[str, Dict[str, str]]:
    """Return `{work_id: {"genre": <modal FJMS domain>, "author": <modal FJMS
    composition-author EngDesc>}}`. Batch-loads domains + composition authors
    ONCE for ALL witness sys_ids (no per-work round-trip), then computes each
    work's modal value in Python. Empty string where FJMS has nothing."""
    all_sys_ids = sorted({sid for sids in work_witness_sys_ids.values() for sid in sids})
    sys_domains = _batch_fjms_domains(conn, all_sys_ids)
    sys_authors = _batch_fjms_composition_authors(conn, all_sys_ids)
    out: Dict[str, Dict[str, str]] = {}
    for work_id, sids in work_witness_sys_ids.items():
        out[work_id] = {
            "genre": _modal_domain(sids, sys_domains),
            "author": _modal_author(sids, sys_authors),
        }
    return out


def _value_provenance(final_value: str, fjms_value) -> str:
    """Report-only provenance of an emitted genre/author cell: 'fjms' when the
    FJMS value was non-empty (FJMS-first always wins then), 'raw' when FJMS
    was empty but a raw fallback filled the cell, 'blank' otherwise."""
    if (fjms_value or "").strip():
        return "fjms"
    return "raw" if (final_value or "").strip() else "blank"


# ---------------------------------------------------------------------------
# 6.9b Enriched CANDIDATE review-artifact emission (134-07 Task A) -- a
# SHARED helper so `finalize_build`'s own optional re-emission and the
# standalone `build_candidate_review_artifact` (the owner title-review gate's
# actual entry point, Task 1) can never compute the impact signal two
# divergent ways.
# ---------------------------------------------------------------------------

def _emit_enriched_review_artifact(
    conn_research: sqlite3.Connection, candidates: List[Dict], out_csv_path, *,
    page_index,
    e1_ra_confirmed: Iterable[Dict] = (), e1_adjudicated_a: Iterable[Dict] = (),
    e1_rb_screening: Iterable[Dict] = (), e1_r3_frame: Iterable[Dict] = (),
    q2_witness_collection: Iterable[Dict] = (), q2_collection_tafsir_targum: Iterable[Dict] = (),
    q2_collection_with_arabic: Iterable[Dict] = (), q2_shared_text: Iterable[Dict] = (),
    fjms_conn: Optional[sqlite3.Connection] = None,
    include_masked_metadata: bool = False,
) -> Dict:
    """Assemble claims/evidence over ALL `candidates` (pre-owner-review --
    NOT just a prior approved set) so the emitted CANDIDATE csv's
    `tier_a_witnesses`/`claim_count` columns reflect the ACTUAL real
    distillation, then FJMS-enrich (genre/author) and emit the review
    artifact. Returns the emitted rows, the `evidence_id_collisions` count,
    and per-column genre/author provenance tallies (fjms/raw/blank) for
    reporting -- none of which blocks this emission step.

    `fjms_conn` (optional): a read-only fjms_enrichment.db connection. When
    given, genre/author are sourced FJMS-first (public Genizah vocabulary,
    masking-safe for all rows) via `compute_fjms_enrichment`, with a
    masking-gated raw-research fallback. When None, genre/author resolve
    purely from the (masking-gated) raw path.

    `include_masked_metadata` (default False) gates ONLY the RAW M-source
    fallback (the FJMS-sourced value is never gated) -- always subject to
    the finished-CSV blocking masking scan."""
    result = build_claims_and_evidence(
        conn=conn_research, works=candidates, page_index=page_index,
        e1_ra_confirmed=e1_ra_confirmed, e1_adjudicated_a=e1_adjudicated_a,
        e1_rb_screening=e1_rb_screening, e1_r3_frame=e1_r3_frame,
        q2_witness_collection=q2_witness_collection,
        q2_collection_tafsir_targum=q2_collection_tafsir_targum,
        q2_collection_with_arabic=q2_collection_with_arabic,
        q2_shared_text=q2_shared_text,
        sidecar_version=REAL_SIDECAR_VERSION,
    )
    impact_counts = compute_work_impact_counts(result["claim_rows"], result["evidence_rows"])

    fjms_meta: Dict[str, Dict[str, str]] = {}
    if fjms_conn is not None:
        work_witness_sys_ids = collect_work_witness_sys_ids(
            result["claim_rows"], result["evidence_rows"]
        )
        fjms_meta = compute_fjms_enrichment(fjms_conn, work_witness_sys_ids)

    rows = emit_review_artifact(
        candidates, out_csv_path, impact_counts=impact_counts,
        fjms_meta=fjms_meta, include_masked_metadata=include_masked_metadata,
    )

    genre_prov: Dict[str, int] = {"fjms": 0, "raw": 0, "blank": 0}
    author_prov: Dict[str, int] = {"fjms": 0, "raw": 0, "blank": 0}
    for r in rows:
        wf = fjms_meta.get(r["work_id"]) or {}
        genre_prov[_value_provenance(r["genre"], wf.get("genre"))] += 1
        author_prov[_value_provenance(r["author"], wf.get("author"))] += 1

    return {
        "rows": rows,
        "evidence_id_collisions": result.get("evidence_id_collisions", 0),
        "genre_provenance": genre_prov,
        "author_provenance": author_prov,
    }


def build_candidate_review_artifact(
    *,
    source_db_path,
    crosswalk_path,
    out_csv_path,
    e1_ra_confirmed_path=None,
    e1_adjudicated_a_path=None,
    e1_rb_screening_path=None,
    e1_r3_frame_path=None,
    q2_witness_collection_path=None,
    q2_collection_tafsir_targum_path=None,
    q2_collection_with_arabic_path=None,
    q2_shared_text_path=None,
    fjms_db_path=None,
    create_crosswalk_if_missing: bool = False,
    include_masked_metadata: bool = False,
) -> Dict:
    """134-07 Task 1/A entry point: emit ONLY the enriched, source-masked,
    impact-prioritized CANDIDATE review csv against the real research corpus
    -- does NOT write a discovery.db and does NOT require an
    `--from-approved` csv (there isn't one yet; that is the whole point of
    the owner review gate this feeds). REUSES the persisted crosswalk
    (`create_crosswalk_if_missing=False` by default, DC2) so opaque work_ids
    stay stable with any prior/subsequent real build.

    `fjms_db_path` (optional): path to the REAL fjms_enrichment.db for the
    FJMS-first genre/author enrichment. Auto-resolved via `resolve_fjms_db_path`
    (the fist_data/ sidecar, never the 0-byte repo-root placeholder) when not
    given; enrichment is silently skipped when no non-empty DB is found.

    `include_masked_metadata` (default False) is the explicit owner opt-in
    (owner decision 2026-07-22) gating ONLY the RAW M-source genre/author
    fallback -- the FJMS-sourced (public) value is never gated. Default OFF
    keeps the conservative fail-closed artifact for anyone running the tool
    without the flag.

    Raises `CrosswalkAbortError` if the crosswalk is required-but-absent
    (mirrors `assign_opaque_work_ids`'s own DC2 contract).
    """
    resolved_fjms = resolve_fjms_db_path(fjms_db_path)
    fjms_conn = _connect_fjms_ro(resolved_fjms) if resolved_fjms else None
    conn_research = _connect_research_ro(source_db_path)
    try:
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

        candidates = select_shown_works(conn_research)
        candidates = assign_opaque_work_ids(
            candidates, crosswalk_path, create_if_missing=create_crosswalk_if_missing,
        )
        page_index = PageTextIndex(conn_research)
        outcome = _emit_enriched_review_artifact(
            conn_research, candidates, out_csv_path, page_index=page_index,
            e1_ra_confirmed=e1_ra_confirmed, e1_adjudicated_a=e1_adjudicated_a,
            e1_rb_screening=e1_rb_screening, e1_r3_frame=e1_r3_frame,
            q2_witness_collection=q2_witness_collection,
            q2_collection_tafsir_targum=q2_collection_tafsir_targum,
            q2_collection_with_arabic=q2_collection_with_arabic,
            q2_shared_text=q2_shared_text,
            fjms_conn=fjms_conn,
            include_masked_metadata=include_masked_metadata,
        )
    finally:
        conn_research.close()
        if fjms_conn is not None:
            fjms_conn.close()

    return {
        "candidate_count": len(candidates),
        "emitted_row_count": len(outcome["rows"]),
        "evidence_id_collisions": outcome["evidence_id_collisions"],
        "fjms_db_used": resolved_fjms or "(none -- raw-only fallback)",
        "genre_provenance": outcome["genre_provenance"],
        "author_provenance": outcome["author_provenance"],
        "out_csv_path": str(out_csv_path),
    }


# ---------------------------------------------------------------------------
# 6.10 finalize_build -- the full real-mode orchestration (DATA-05/08, F13)
# ---------------------------------------------------------------------------

# Frozen real-data contract facts (verified against the research corpus this
# rework, docs/specs C-4/§4.2) -- a hard integrity assertion ONLY when the
# collection is actually loaded from its real file path (never fired against
# a test's small synthetic slice, since tests never pass these *_path args).
_EXPECTED_TAFSIR_TARGUM_ROWS = 106
_EXPECTED_WITH_ARABIC_ROWS = 108
# Frozen two-seed (trials>=2) subset counts within each router collection (C-1):
# 106 total / 18 two-seed, 108 total / 57 two-seed. The release gate (H2) must
# pin the SUBSET counts + bucket identity, not just the totals -- a shape-drifted
# input with the same total but a different trials>=2 distribution must not pass.
_EXPECTED_TAFSIR_TARGUM_TWO_SEED = 18
_EXPECTED_WITH_ARABIC_TWO_SEED = 57

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
# PROVENANCE (Codex round 2, HIGH -- and the finding was right).
#
# The v3 slim DB's own tier-A count is 275,894: EXACTLY this frozen value. The
# bake plan read that as evidence the release contract had been pinned against
# the gen-2 population. **That inference is not sound**, and is withdrawn:
# numerical agreement between a constant and one transformation's output proves
# only that this transformation currently produces that number. The constant
# carried no source-table fingerprint, no derivation query and no dated origin,
# so nothing distinguishes "deliberately pinned" from "coincidence".
#
# What IS established, and all that is claimed:
#   - the query is stated (`WHERE shadowed_by IS NULL`), so the count is
#     reproducible from any research DB;
#   - measured 2026-08-07 on the v3 slim DB built from
#     `track1_matches_pilot_glaunch3_live`: 275,894 unshadowed of 381,341 total,
#     with 105,447 rows in wholly-shadowed units;
#   - `derive_shadowed_by` HALTS on a mixed unit and on a non-injective
#     reduction, so the derivation is not silently order-dependent.
#
# The operative rule, which matters more than the number's history: **this value
# is NEVER edited to make a run pass.** A mismatch means the population changed,
# and a changed population needs a decision, not a new constant. The build
# records the actual count in its release-contract check either way, so a
# divergence is visible rather than absorbed.


class ReleaseInputsIncompleteError(RuntimeError):
    """Raised (H2) when `finalize_build(release=True, ...)` is missing any
    frozen release input, or an input's row count drifts from the frozen
    contract. A missing collection must never silently ingest as empty --
    that would produce a tier-A-only sidecar that still passes every other
    release gate. `--allow-partial-sources` (never combined with
    `release=True`) is the ONLY sanctioned escape hatch, reserved for the
    smoke/unit path."""


class RoutingConflictError(RuntimeError):
    """Raised when a gen-2 coverage router AND Lever-1 are both requested.

    They are two different decisions about the same question (is this page a
    witness of this work?) over the same quantity, with different thresholds.
    Running both silently applies the stricter one, which for discovery-v3 would
    demote 30,899 of 160,095 router-shipped witness rows -- measured, one-way.
    A caller that asks for both has not decided, so this refuses rather than
    picking."""


class InvalidPrecisionSpecError(ValueError):
    """Raised (Codex R2 HIGH) when an explicit `--precision-spec` does not
    match the EXACT frozen release band_precision row-set (docs/specs/
    discovery-sidecar-schema-v1.md SS1.6). Without this check, an
    owner-supplied spec carrying a fabricated `tier_a` precision, a missing
    frozen row, or an extra/duplicate measured band could reach a
    finalized real/release `.db` + manifest BEFORE the separate
    `scripts/verify_discovery_sidecar.py` process (run only AFTER this one
    exits) ever gets a chance to catch it."""


class BandVocabPreflightError(RuntimeError):
    """Raised (135-07 hardening) when a build's band-vocabulary input set is
    inconsistent with the operator's declared intent: a SHA-256 pin supplied
    WITHOUT its input path (any mode -- a dangling pin means the operator
    believes an input is wired that is not); a v2 `--release` missing any of
    the THREE hash-pinned inputs or their SHA pins; or a v1 `--release`
    carrying any v2-only input. Fail-closed: v1-vs-v2 operator intent must be
    unambiguous and fully pinned before any output is produced (the deferred
    rename cascade's root cause was that v2 was inferred, never asserted)."""


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

    # Codex R5 MED: pin each router collection's frozen SHAPE, not just its
    # total -- the two-seed (trials>=2) subset count AND per-row bucket identity.
    # (Masking: messages name only the param name, the frozen-safe expected
    # bucket literal, and counts -- never a supplied row value.)
    def _two_seed_count(collection_rows) -> int:
        n = 0
        for r in collection_rows:
            t = r.get("trials") if isinstance(r, dict) else None
            try:
                if t is not None and float(t) >= 2:
                    n += 1
            except (TypeError, ValueError):
                pass
        return n

    def _wrong_bucket_count(collection_rows, expected_bucket) -> int:
        return sum(
            1 for r in collection_rows
            if not (isinstance(r, dict) and r.get("_bucket") == expected_bucket)
        )

    for name, coll, expected_bucket, expected_two_seed in (
        ("q2_collection_tafsir_targum", q2_collection_tafsir_targum,
         "tafsir_targum", _EXPECTED_TAFSIR_TARGUM_TWO_SEED),
        ("q2_collection_with_arabic", q2_collection_with_arabic,
         "with_arabic", _EXPECTED_WITH_ARABIC_TWO_SEED),
    ):
        actual_two_seed = _two_seed_count(coll)
        if actual_two_seed != expected_two_seed:
            problems.append(
                f"{name}: expected {expected_two_seed} two-seed (trials>=2) rows, "
                f"got {actual_two_seed}"
            )
        wrong_bucket = _wrong_bucket_count(coll, expected_bucket)
        if wrong_bucket:
            problems.append(
                f"{name}: {wrong_bucket} row(s) have _bucket != {expected_bucket!r}"
            )

    if problems:
        raise ReleaseInputsIncompleteError(
            "release build (H2) requires every frozen input present at its EXACT "
            "expected row count -- mismatches: " + "; ".join(problems)
        )


def _assert_band_vocab_release_preflight(
    *,
    release: bool,
    v2_build: bool,
    canonical_merges_path,
    canonical_merges_sha256,
    composition_dates_path,
    composition_dates_sha256,
    seftja_dates_path,
    seftja_dates_sha256,
) -> None:
    """135-07 band-vocabulary release preflight -- a pure argument-validation
    gate with NO file I/O, run at the very TOP of `finalize_build` (before any
    input load or output mutation). Enforces:

      * (ALL modes) a SHA-256 pin supplied WITHOUT its input path is a hard
        error -- a dangling pin means the operator believes an input is wired
        that is not (fail-closed, never silently ignored);
      * (release + v2) all THREE hash-pinned inputs (`--canonical-merges`,
        `--composition-dates`, `--seftja-dates`) present WITH their SHA pins;
      * (release + v1) NO v2-only input present at all -- a v1 release that
        carries a v2 input has ambiguous intent and is refused.

    The `--release` gates are inert for the fixture/unit path (135-06's
    "v2 inputs opt-in when not --release" behavior is preserved); only the
    universal dangling-pin check applies there, which the fixtures satisfy."""
    dangling = [
        name
        for name, path, sha in (
            ("--canonical-merges", canonical_merges_path, canonical_merges_sha256),
            ("--composition-dates", composition_dates_path, composition_dates_sha256),
            ("--seftja-dates", seftja_dates_path, seftja_dates_sha256),
        )
        if sha is not None and path is None
    ]
    if dangling:
        raise BandVocabPreflightError(
            "SHA-256 pin(s) supplied without the corresponding input path: "
            + ", ".join(sorted(dangling))
        )
    if not release:
        return
    if v2_build:
        problems = []
        for name, path, sha in (
            ("--canonical-merges", canonical_merges_path, canonical_merges_sha256),
            ("--composition-dates", composition_dates_path, composition_dates_sha256),
            ("--seftja-dates", seftja_dates_path, seftja_dates_sha256),
        ):
            if path is None:
                problems.append(f"{name} path missing")
            if sha is None:
                problems.append(f"{name} SHA-256 pin missing")
        if problems:
            raise BandVocabPreflightError(
                "v2 --release requires all three hash-pinned inputs present WITH "
                "their SHA-256 pins: " + "; ".join(problems)
            )
    else:
        v2_only = [
            name
            for name, path in (
                ("--canonical-merges", canonical_merges_path),
                ("--composition-dates", composition_dates_path),
                ("--seftja-dates", seftja_dates_path),
            )
            if path is not None
        ]
        if v2_only:
            raise BandVocabPreflightError(
                "v1 --release must not carry v2-only input(s): "
                + ", ".join(sorted(v2_only))
            )


_PRECISION_SPEC_TOLERANCE = 1e-6


def _validate_precision_spec(rows: List[Dict], *, v2_bands: bool = False) -> None:
    """Codex R2 HIGH: validate an explicit `--precision-spec` against the
    EXACT frozen release band_precision row-set (docs/specs/discovery-
    sidecar-schema-v1.md SS1.6) BEFORE it is used for any output/artifact
    write. Cross-checked against `_frozen_real_band_precision_rows(v2_bands)`
    -- the ONE source of truth for the frozen row-set already defined in this
    module -- rather than a second hardcoded copy of the same contract
    (which would risk drifting out of sync).

    v2 rename (135-07): `v2_bands` selects the frozen key-set the supplied spec
    must match. A v2 build REQUIRES the v2 top-tier key
    (`high_confidence_algorithmic`) and REJECTS the v1 key (`expert_verified`),
    and vice-versa for a v1 build -- the frozen row-set is the sole gate, so
    this falls out automatically from the version-threaded cross-check below.

    Requires EXACTLY: the collection row (`propagated_witness_collection_v1`,
    precision ~= 0.926, non-null numerator/denominator/ci_low/ci_high/method);
    both propagated bands (corroborated, weak) present with NULL precision;
    the three measured track1_direct bands (expert_verified/screening_rb/
    screening_canon) present at their frozen values; `tier_a` present with
    NULL precision. Any missing/duplicate/extra row, or a value outside the
    frozen tolerance, raises `InvalidPrecisionSpecError` -- a spec with a
    fabricated `tier_a` precision or a missing frozen row must never reach
    a real/release build's output before the separate verifier ever runs.

    D-02a (docs/specs/discovery-sidecar-schema-v1.md SS1.6 amendment
    2026-08-02): the per-band loop ALSO asserts `ci_low` and
    `measurement_status` match the frozen row exactly on every band --
    `tier_a`'s frozen row now carries the CERT-01 AUTHORIZATION pair
    (`ci_low=0.9084`, `measurement_status='measured_pass'`) while `precision`
    stays NULL; every other band's frozen `ci_low`/`measurement_status`
    remain unchanged (NULL/None), so a spec asserting `measured_pass` on any
    band other than `tier_a` is rejected here, and a `measurement_status`
    outside the closed vocabulary (shared/discovery_band_labels.
    MEASUREMENT_STATUSES, mirrored locally as MEASUREMENT_STATUSES_FROZEN)
    is rejected independently of whether it happens to also mismatch.
    """
    frozen = _frozen_real_band_precision_rows(v2_bands=v2_bands)
    frozen_collection = next(r for r in frozen if r["scope"] == "collection")
    frozen_band_by_key = {
        (r["evidence_source"], r["confidence_band"]): r
        for r in frozen if r["scope"] == "band"
    }

    problems: List[str] = []

    def _precision_matches(actual, expected) -> bool:
        if expected is None:
            return actual is None
        return (
            actual is not None
            and isinstance(actual, (int, float))
            and not isinstance(actual, bool)
            and abs(actual - expected) <= _PRECISION_SPEC_TOLERANCE
        )

    # Codex R3 HIGH / R4 MED (masking): enforce the EXACT frozen key MULTISET
    # up front and FAIL FAST, separately from the value checks below. Those
    # checks key collection rows by collection_id and band rows by
    # (evidence_source, confidence_band) ONLY, so an EXTRA collection row, a
    # band on the WRONG collection_id, a duplicated key, a non-dict row, or an
    # unknown scope would otherwise slip through.
    #
    # Masking (Codex R4): the supplied spec is potentially owner-/hand-authored,
    # so a malformed key field could embed restricted text. This structural
    # diagnostic therefore NEVER renders a SUPPLIED value -- unexpected/duplicate
    # rows are reported by POSITION only; only MISSING keys (which come from the
    # FROZEN row-set, never the supplied spec) are named. Same discipline as
    # _validate_crosswalk. It raises immediately, before any value check runs, so
    # the value-check messages below only ever render frozen-safe keys.
    from collections import Counter

    def _spec_key(r: Dict) -> Tuple:
        return (
            r.get("scope"), r.get("collection_id"),
            r.get("evidence_source"), r.get("confidence_band"),
        )

    structural: List[str] = []
    non_dict_positions = [i for i, r in enumerate(rows) if not isinstance(r, dict)]
    if non_dict_positions:
        structural.append(
            f"{len(non_dict_positions)} row(s) are not dicts at position(s) "
            f"{non_dict_positions[:5]}"
        )
    bad_scope_positions = [
        i for i, r in enumerate(rows)
        if isinstance(r, dict) and r.get("scope") not in ("collection", "band")
    ]
    if bad_scope_positions:
        structural.append(
            f"{len(bad_scope_positions)} row(s) have an unknown/invalid scope at "
            f"position(s) {bad_scope_positions[:5]}"
        )

    frozen_key_counts = Counter(_spec_key(r) for r in frozen)
    seen = Counter()
    unexpected_positions: List[int] = []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            continue
        key = _spec_key(r)
        seen[key] += 1
        if seen[key] > frozen_key_counts.get(key, 0):
            unexpected_positions.append(i)
    if unexpected_positions:
        structural.append(
            f"{len(unexpected_positions)} unexpected/duplicate precision-spec row(s) at "
            f"position(s) {unexpected_positions[:5]} (outside the frozen row-set)"
        )
    missing_keys = sorted((frozen_key_counts - seen).elements(), key=str)
    if missing_keys:
        structural.append(
            f"missing {len(missing_keys)} required frozen row-key(s): {missing_keys}"
        )

    if structural:
        # fail fast: never proceed to value validation with a structurally-wrong
        # key set (and never having rendered a supplied value -- masking)
        raise InvalidPrecisionSpecError(
            "explicit --precision-spec does not match the frozen release band_precision "
            "key-set (docs/specs/discovery-sidecar-schema-v1.md SS1.6): "
            + "; ".join(structural)
        )

    collection_rows = [r for r in rows if isinstance(r, dict) and r.get("scope") == "collection"]
    matching_collection = [
        r for r in collection_rows if r.get("collection_id") == frozen_collection["collection_id"]
    ]
    if len(matching_collection) != 1:
        problems.append(
            f"expected exactly 1 scope='collection' row with collection_id="
            f"{frozen_collection['collection_id']!r}, found {len(matching_collection)}"
        )
    else:
        c = matching_collection[0]
        if not _precision_matches(c.get("precision"), frozen_collection["precision"]):
            # masking (Codex R5): never render the SUPPLIED precision -- a spec
            # could supply an arbitrary string there; name only the frozen value.
            problems.append(
                "scope='collection' precision mismatch (expected frozen "
                f"{frozen_collection['precision']})"
            )
        if c.get("evidence_source") is not None or c.get("confidence_band") is not None:
            problems.append(
                "scope='collection' row must carry NULL evidence_source/confidence_band"
            )
        for field in ("numerator", "denominator", "ci_low", "ci_high", "method"):
            if c.get(field) is None:
                problems.append(f"scope='collection' row missing required field {field!r}")

    band_rows = [r for r in rows if isinstance(r, dict) and r.get("scope") == "band"]
    band_rows_by_key: Dict[Tuple[Optional[str], Optional[str]], List[Dict]] = {}
    for r in band_rows:
        band_rows_by_key.setdefault((r.get("evidence_source"), r.get("confidence_band")), []).append(r)

    for key, frozen_row in frozen_band_by_key.items():
        matches = band_rows_by_key.get(key, [])
        if len(matches) != 1:
            problems.append(
                f"expected exactly 1 scope='band' row for (evidence_source, "
                f"confidence_band)={key}, found {len(matches)}"
            )
            continue
        actual_precision = matches[0].get("precision")
        expected_precision = frozen_row["precision"]
        if not _precision_matches(actual_precision, expected_precision):
            # masking (Codex R5): `key` is frozen-safe (iterated from
            # frozen_band_by_key) but the SUPPLIED precision is not -- name only
            # the frozen expected value, never the supplied one.
            problems.append(
                f"band {key}: precision mismatch (expected frozen {expected_precision!r})"
            )

        # D-02a (136-06, docs/specs/discovery-sidecar-schema-v1.md SS1.6
        # amendment 2026-08-02): the `tier_a` row now also carries a frozen
        # `ci_low`/`measurement_status` AUTHORIZATION pair -- widen this loop
        # to assert those two fields match the frozen row EXACTLY, same
        # masking discipline as the precision check above (never render the
        # supplied value, only the frozen expected one).
        actual_ci_low = matches[0].get("ci_low")
        expected_ci_low = frozen_row.get("ci_low")
        if not _precision_matches(actual_ci_low, expected_ci_low):
            problems.append(
                f"band {key}: ci_low mismatch (expected frozen {expected_ci_low!r})"
            )

        # Closed-vocabulary cross-check FIRST (Codex #B3 discipline, mirrors
        # shared/discovery_band_labels.MEASUREMENT_STATUSES): a supplied
        # `measurement_status` outside the frozen enum is its own build error,
        # independent of whether it happens to also mismatch the frozen row --
        # never named an unexpected value not in this closed set may reach
        # the D-18 default-eligibility predicate.
        actual_measurement_status = matches[0].get("measurement_status")
        expected_measurement_status = frozen_row.get("measurement_status")
        if (
            actual_measurement_status is not None
            and actual_measurement_status not in MEASUREMENT_STATUSES_FROZEN
        ):
            problems.append(
                f"band {key}: measurement_status is outside the closed vocabulary "
                f"{sorted(MEASUREMENT_STATUSES_FROZEN)}"
            )
        elif actual_measurement_status != expected_measurement_status:
            # masking: never render the supplied value, only the frozen one.
            problems.append(
                f"band {key}: measurement_status mismatch (expected frozen "
                f"{expected_measurement_status!r})"
            )

    # NOTE: an extra/wrong-collection band row can never reach here -- the
    # structural key-multiset check above fails fast on it (Codex R3/R4/R5), so
    # once we get here the band key-set is EXACTLY the frozen set. The old
    # `extra_keys` diagnostic (which rendered supplied-derived keys) is therefore
    # both unreachable-with-content and a masking risk, and has been removed.

    if problems:
        raise InvalidPrecisionSpecError(
            "explicit --precision-spec does not match the frozen release band_precision "
            "row-set (docs/specs/discovery-sidecar-schema-v1.md SS1.6): " + "; ".join(problems)
        )


def _resolve_band_precision_spec(
    *, precision_spec: Optional[List[Dict]], frozen_precision_defaults: bool, release: bool,
    v2_bands: bool = False,
) -> List[Dict]:
    """H3: resolve the band_precision rows to write, BEFORE any further
    build work begins. An explicit `precision_spec` (owner-supplied at
    134-07) always wins -- but is FIRST validated against the exact frozen
    release row-set (Codex R2 HIGH, `_validate_precision_spec`) so a
    fabricated/incomplete spec is rejected before any output/artifact
    write, never merely relying on the separate `verify_discovery_sidecar.py`
    process to catch it after the fact; otherwise an explicit
    `frozen_precision_defaults` acknowledgement uses the documented
    frozen-contract defaults (tier_a precision NULL -- never the
    SYNTHETIC-mode-only 0.90 placeholder). A `release=True` build with
    NEITHER supplied is refused outright -- a real/release payload must
    never silently fabricate a number. A non-release call (unit tests,
    `--allow-partial-sources` smoke builds) defaults to the SAME
    frozen-contract rows when neither is supplied. Extracted as its own
    function so H3's raise path is directly unit-testable without needing
    to satisfy the (unrelated) H2 input-completeness gate.

    v2 rename (135-07): `v2_bands` is threaded to BOTH the explicit-spec
    validation and the frozen-default resolution so a v2 build's band_precision
    top tier is keyed `high_confidence_algorithmic` (and a supplied v2 spec is
    required to match that key), while a v1 build stays byte-identical."""
    if precision_spec is not None:
        _validate_precision_spec(precision_spec, v2_bands=v2_bands)
        return precision_spec
    if frozen_precision_defaults:
        return _frozen_real_band_precision_rows(v2_bands=v2_bands)
    if release:
        raise ValueError(
            "--release requires --precision-spec <json> or an explicit "
            "--frozen-precision-defaults acknowledgement (H3) -- a real/release "
            "build must never silently fabricate band_precision numbers"
        )
    return _frozen_real_band_precision_rows(v2_bands=v2_bands)


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
    canonical_merges_path=None,
    canonical_merges_sha256: Optional[str] = None,
    composition_dates_path=None,
    composition_dates_sha256: Optional[str] = None,
    seftja_dates_path=None,
    seftja_dates_sha256: Optional[str] = None,
    novelty_verdicts_path=None,
    novelty_verdicts_sha256: Optional[str] = None,
    novelty_alias_groups_path=None,
    # discovery-v3 (Codex R3 BLOCKER): `{grain_key: input_fingerprint}` for the
    # pairs this build generated. REQUIRED whenever a verdict cache is supplied,
    # unless explicitly waived below -- round 3 found the fingerprint gate
    # implemented but never reached from here, which is the identical
    # correct-function-nobody-calls failure round 2 found in the router ingest.
    # discovery-v3 (Codex R3 BLOCKER, the router bypass). Path to the gen-2
    # evidence DB carrying `coverage_route` + `coverage_route_meta`. Supplied =>
    # gen-2's fitted routing decision is INGESTED and REPLACES the Lever-1 cliff.
    # Round 3 found `finalize_build` had no router input at all, so the real build
    # applied the legacy 0.45 threshold precisely when D-17 ran -- demoting the
    # ~19% of the validated headline surface the ingest exists to preserve. The
    # helper accepted a router; nothing here ever built one.
    gen2_router_evidence_db=None,
    # The ONLY way to run the legacy coverage cliff on a v3 build. Named, like the
    # novelty waiver, so it cannot be reached by omission -- see the guard below.
    allow_lever1_coverage: bool = False,
    # SPLIT-GRAIN RE-GRAINING (option 4, owner-approved 2026-08-07). Default TRUE
    # because on the real v3 population the build CANNOT COMPLETE without it: gen-2
    # scored collapsed canonical ids, so 138,800 of 275,894 tier-A rows have no
    # decision at their own key and the wipe-out guard halts. A default of False
    # would make the documented v3 path a build that always fails.
    #
    # It is still a named parameter, and set False it restores the exact
    # pre-re-grain behaviour -- which is what the v2-population fixtures and the
    # existing router tests exercise, and what a caller deliberately baking at the
    # COLLAPSED grain would want. Whichever path runs is recorded in `meta`
    # (`coverage_threshold_grain_applied`), so a reader needs no build log.
    regrain_split_grain: bool = True,
    novelty_input_fingerprints: Optional[Dict[str, str]] = None,
    # The ONLY way to load a verdict cache without the fingerprint gate. Named
    # for what it actually does, so it cannot be passed absent-mindedly and shows
    # up in a build record. A v2 rebuild against the v2-era cache legitimately
    # needs it; a v3 build must never set it.
    novelty_allow_unfingerprinted_cache: bool = False,
    work_domains_path=None,
    work_domains_content_hash: Optional[str] = None,
    work_author_aliases_path=None,
    work_author_aliases_content_hash: Optional[str] = None,
    coverage_floor: float = 0.99,
    # CD batch / schema Amendment 2026-08-12 (T), Contract 0: the SHA-256 of
    # the reference-corpus stream the evidence w_start/w_end offsets index.
    # Supplied by the bake pipeline; written to meta as
    # `reference_corpus_sha256`. The D-track locus import writes the twin
    # `locus_reference_corpus_sha256`, and the verifier asserts the two EQUAL
    # whenever locus_unit is populated -- so a reference refresh can no longer
    # silently move every citation in the corpus.
    reference_corpus_sha256: Optional[str] = None,
    # CD batch / schema Amendment 2026-08-12 (S): the tracked population-lock
    # JSON (scripts/emit_population_lock.py). Its constants are COPIED into
    # meta; the verifier's retention gate enforces the lock's own floors
    # against every shipped asset's recomputed population.
    population_lock_path=None,
    # CD batch / schema Amendment 2026-08-12 (R): Contract-1's two input
    # tables. The curated list can be supplied on any post-batch bake; the
    # region map REQUIRES the locus tables populated (its ingest fails closed
    # otherwise), so it travels with the D-track locus import.
    curated_quoter_path=None,
    region_map_path=None,
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

    Codex R2 MED (gate-ordering fix): the H2 (`_assert_release_inputs_complete`)
    and H3 (`_resolve_band_precision_spec`) gates now run BEFORE ANY output
    mutation -- before the prior output `.db` is deleted, before the output
    directory is created, before the crosswalk is persisted (inside
    `assign_opaque_work_ids`), and before the review artifact is emitted.
    A failed release build (either gate) leaves every prior artifact (the
    existing output `.db`, the crosswalk file, the review CSV) COMPLETELY
    untouched -- previously those four mutations ran FIRST, so a release
    build that failed H2/H3 had already deleted the prior `.db`, persisted
    a crosswalk update, and (if requested) overwritten the review artifact
    before ever raising.
    """
    # v2 STEP -1 (135-07 ordering fix, Codex #1): derive the band-vocabulary
    # version ONCE, at the very TOP of the body -- BEFORE band_precision is
    # resolved. A supplied hash-pinned `--canonical-merges` census is a v2-ONLY
    # concept, so it is the definitive "this is a v2 build" signal;
    # `band_vocab_version` is the explicit, durable operator-intent marker
    # recorded in meta (below) and cross-checked by the verifier. Previously
    # `v2_build` was computed AFTER `bp_rows` was resolved, so the frozen
    # band_precision default kept the v1 `expert_verified` top-tier key even in
    # a v2 build (the deferred-rename cascade's ordering bug).
    v2_build = canonical_merges_path is not None
    band_vocab_version = "v2" if v2_build else "v1"

    # 135-07 band-vocabulary release preflight (pure arg validation, NO I/O):
    # dangling SHA pins (any mode) + v2/v1 --release input-set completeness.
    _assert_band_vocab_release_preflight(
        release=release,
        v2_build=v2_build,
        canonical_merges_path=canonical_merges_path,
        canonical_merges_sha256=canonical_merges_sha256,
        composition_dates_path=composition_dates_path,
        composition_dates_sha256=composition_dates_sha256,
        seftja_dates_path=seftja_dates_path,
        seftja_dates_sha256=seftja_dates_sha256,
    )

    # v2 STEP 0 (135-06, bake plan §6): resolve the optional --precision-spec
    # FAIL-branch reband BEFORE any DB write. A `measured_fail` tier_a outcome
    # is a REBUILD INPUT (never a bare in-place UPDATE) -- decided here, then
    # consumed at band-assignment time inside build_claims_and_evidence.
    reband_decision = resolve_reband_decision(precision_spec)
    reband_tier_a = reband_decision is not None

    # H3 -- a pure argument-validation gate with NO file I/O at all. When the
    # spec triggers a reband, the strict frozen-row-set validation does NOT
    # apply (that spec carries measurement_status semantics, not the frozen
    # numbers); the reband path resolves + invalidates band_precision itself.
    # v2_bands is threaded into BOTH branches (135-07) so the E1 top-tier
    # band_precision row is keyed `high_confidence_algorithmic` in a v2 build
    # (the reband branch grabs the frozen rows BEFORE invalidating tier_a/
    # screening_rb, so without the version thread it would keep the OLD key).
    if reband_tier_a:
        bp_rows = _frozen_real_band_precision_rows(v2_bands=v2_build)
        bp_rows, reband_meta_extra = invalidate_reband_band_precision(bp_rows, reband_decision)
    else:
        bp_rows = _resolve_band_precision_spec(
            precision_spec=precision_spec,
            frozen_precision_defaults=frozen_precision_defaults,
            release=release,
            v2_bands=v2_build,
        )
        reband_meta_extra = {}

    # v2 STEP 0 (bake plan §4.1/§4.3): load the hash-pinned merge census + the
    # two hash-pinned date inputs. In 135-06 (fixtures-only, no production
    # bake) the v2 inputs are OPT-IN and fully validated + hash-verified when
    # supplied; the "required-for-release" enforcement rides the 135-07
    # production-bake path (this keeps the legacy v1-release finalize_build
    # tests, out of this plan's file scope, green). A mismatched SHA or a
    # malformed shape still HALTs (raised by the loaders below) whenever the
    # input IS supplied, and semantic-ratification is enforced on --release.
    # `v2_build` (the "this is a v2 build" signal) was derived at the TOP of
    # the body (v2 STEP -1) so it could thread the band_precision rename; its
    # verified SHA is recorded in meta (`canonical_merges_sha256`) below,
    # giving the shipped asset a clean v2 marker alongside band_vocab_version.
    if v2_build:
        merges_loaded = load_canonical_merges(
            canonical_merges_path, sha256=canonical_merges_sha256,
            require_release_semantics=release,
        )
        cross_corpus_map = merges_loaded["cross_corpus_map"]
        dropped_work_ids = merges_loaded["dropped"]
        canonical_merges_sha256 = merges_loaded["sha256"]
    else:
        cross_corpus_map = {}
        dropped_work_ids = set()

    run_d17 = composition_dates_path is not None or seftja_dates_path is not None

    # ---------------------------------------------------------------------
    # discovery-v3 coverage routing (Codex R3 BLOCKER): the router, loaded HERE
    # with the other pinned inputs, BEFORE any output mutation.
    #
    # Round 3's finding was that `apply_lever1=run_d17` below was the ONLY
    # coverage-routing path a real build could take, so the entire router ingest
    # -- module, mapping, parity gate, order justification, tests -- had no effect
    # on any artifact. The third instance of the same failure in this work, so the
    # guard is structural rather than a new default: when D-17 runs (the case that
    # routes coverage at all), the caller must either supply the router or say out
    # loud that it wants the legacy cliff.
    # ---------------------------------------------------------------------
    gen2_router = None
    regrain_report = None
    e1_route_report = None
    if gen2_router_evidence_db is not None:
        from v3_routing_ingest import load_router as _load_gen2_router
        gen2_router = _load_gen2_router(str(gen2_router_evidence_db))
        # NOTE: the split-grain re-grain runs LATER, once `conn_research` is open
        # (it needs `pages.n_chars` and the tier-A key list from the research DB).
        # Search for `regrain_router_to_split` below.
    if run_d17 and gen2_router is None and not allow_lever1_coverage:
        raise RoutingConflictError(
            "coverage routing is unspecified: D-17 is active (dates were supplied) but "
            "neither `gen2_router_evidence_db` nor `allow_lever1_coverage` was given. "
            "Defaulting to the legacy Lever-1 cliff is what this build must never do "
            "silently -- measured on the real artifact, it demotes 30,899 of 160,095 "
            "`same_work` rows (19.3%) that gen-2's fitted router ships, one-way, so the "
            "asset would not contain the population the grading validated. Supply the "
            "gen-2 evidence DB, or set `allow_lever1_coverage=True` to choose the legacy "
            "cliff deliberately."
        )

    composition_dates_map = {}
    seftja_dates_map = {}
    if composition_dates_path is not None:
        composition_dates_map = parse_composition_dates(
            composition_dates_path, sha256=composition_dates_sha256)
        composition_dates_sha256 = _hash_file(Path(composition_dates_path))
        if release:
            # 135-07 recovered-strata semantic gate (release builds only --
            # fixtures/smoke paths pass no composition table or no --release).
            assert_composition_release_contract(composition_dates_map)
    if seftja_dates_path is not None:
        seftja_dates_map = parse_seftja_dates(
            seftja_dates_path, sha256=seftja_dates_sha256)
        seftja_dates_sha256 = _hash_file(Path(seftja_dates_path))

    # 136-12 Task 1: the hash-pinned novelty verdict cache. Loaded HERE, with
    # the other pinned inputs, BEFORE any output mutation -- so a bad pin fails
    # the build while every prior artifact is still untouched, exactly like the
    # H2/H3 gates below. Absent => every evidence row keeps the fail-closed
    # `not_checked`; that is a real, honest state, not a degraded one.
    novelty_alias_groups = load_alias_groups(novelty_alias_groups_path)
    novelty_grain_index: Optional[Dict[str, Dict]] = None
    novelty_input_stats: Dict = {}
    if novelty_verdicts_path is not None:
        # discovery-v3 (Codex R3 BLOCKER). The fingerprint gate has to be
        # UNSKIPPABLE-BY-OMISSION here: a caller that simply forgets the argument
        # would silently accept stale positive verdicts, which is exactly the
        # reuse-across-a-changed-question hole blocker 3 closed. So the choice is
        # forced -- supply the fingerprints, or say out loud that you are not
        # gating. There is no third, quiet option.
        if novelty_input_fingerprints is None and not novelty_allow_unfingerprinted_cache:
            raise NoveltyVerdictCacheError(
                "a novelty verdict cache was supplied without `novelty_input_fingerprints`. "
                "Without them an entry cannot prove WHICH question it answered, so a stale "
                "verdict (changed title, rebuilt alias group, refreshed finding aid) would "
                "be reused silently. Pass the fingerprints computed by "
                "`discovery_novelty_funnel.candidate_input_fingerprint`, or -- only for a "
                "v2-era rebuild -- set `novelty_allow_unfingerprinted_cache=True` explicitly."
            )
        verdict_entries, verdict_stats = load_novelty_verdicts(
            novelty_verdicts_path, sha256=novelty_verdicts_sha256,
            expected_fingerprints=novelty_input_fingerprints)
        novelty_grain_index, grain_stats = build_novelty_grain_index(
            verdict_entries, novelty_alias_groups)
        novelty_input_stats = {**verdict_stats, **grain_stats}

    # 136-12 Task 3: the curated artifacts (plan 136-09), loaded with the other
    # hash-pinned inputs and BEFORE any output mutation. Both are OPT-IN: a
    # build without them leaves `works.genre` exactly as it is today (NULL),
    # which is honest, rather than half-populating it from a guess.
    curated_genres: Dict[str, str] = {}
    author_alias_index: Dict[str, Dict] = {}
    curated_stats: Dict = {}
    if work_domains_path is not None:
        curated_genres, domain_stats = load_work_domains(
            work_domains_path, content_hash=work_domains_content_hash)
        curated_stats.update(domain_stats)
    if work_author_aliases_path is not None:
        author_alias_index, alias_stats = load_work_author_aliases(
            work_author_aliases_path, content_hash=work_author_aliases_content_hash)
        curated_stats.update(alias_stats)

    out_path = Path(out_db_path)

    conn_research = _connect_research_ro(source_db_path)
    try:
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
        # EXACT expected count BEFORE any ingest AND before any output/
        # crosswalk/review-artifact mutation below -- a missing collection
        # must never silently ingest as empty and produce a tier-A-only
        # sidecar that still passes every other gate, and a failed release
        # build must never have already deleted/overwritten prior artifacts.
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

        # SPLIT-GRAIN RE-GRAINING (option 4, owner-approved 2026-08-07). Runs
        # HERE: after every input gate (so a bad input still fails while prior
        # artifacts are untouched) and before any mutation, but inside the
        # `conn_research` scope it needs.
        #
        # gen-2 scored COLLAPSED canonical ids (`M:Ytext1000` = 39 Bible books as
        # ONE work); the v3 slim table carries the SPLIT ids. So 138,800 of
        # 275,894 tier-A rows have no decision at their own key and the wipe-out
        # guard halts -- correctly. This OVERLAYS a decision for each of them,
        # computed with the router's OWN estimand and OWN threshold. Full
        # rationale and the three hazards it closes:
        # `v3_routing_ingest.regrain_router_to_split`.
        #
        # OVERLAY, NEVER REPLACE. Every key gen-2 already decided keeps its
        # verdict verbatim (`kept_exact` = 134,536), so this cannot move a row the
        # 400-card grading measured. A full recompute would instead move 6,276
        # rows out of `same_work` AND 2,725 into it, and would discard gen-2's
        # `not_shipped` -- which is shadowing PLUS gen-2's own chronological
        # demotion, a signal no coverage recompute can reproduce.
        if gen2_router is not None and regrain_split_grain:
            from v3_routing_ingest import (
                load_split_grain_coverage as _load_split_cov,
                regrain_router_to_split as _regrain,
            )
            _split_max = _load_split_cov(str(gen2_router_evidence_db))
            # `pages.n_chars` is the RAW character length, and that is
            # DELIBERATE. `matched_letters` is a NORMALIZED Hebrew-letter width,
            # so `page_coverage` is a mixed-unit ratio ~23% "too low" -- and the
            # threshold was calibrated against that same mismatch. Feeding the
            # builder's own `compute_page_coverage` (normalized denominator) into
            # this threshold over-promotes 3.7% of tier-A rows with NO error
            # signal. Measured 2026-08-07; do NOT "correct" this to
            # `norm_stream_letter_count`. This file has already shipped one
            # field-collision bug of exactly this shape (135-07, `density` fed in
            # as `coverage` -- see `apply_lever1_coverage`).
            _page_chars = {
                p: n for p, n in conn_research.execute(
                    "SELECT page_id, n_chars FROM pages")
            }
            _tier_a_keys = conn_research.execute(
                "SELECT page_id, work_id FROM track1_matches "
                "WHERE shadowed_by IS NULL"
            ).fetchall()
            regrain_report = _regrain(
                gen2_router, _split_max, _page_chars, _tier_a_keys)
            if regrain_report["undecided"]:
                raise RoutingConflictError(
                    f"split-grain re-graining left {regrain_report['undecided']} "
                    f"tier-A row(s) with no routing decision -- they would keep the "
                    f"ingest default and silently bypass coverage routing. Halting "
                    f"rather than defaulting."
                )

            # E1 witness routing (2026-08-08, owner-authorized). The four E1
            # collections are drawn ENTIRELY from the matcher's tier B, which
            # gen-2's router never scored at ANY grain -- measured 0 of 19,238
            # pairs present in `coverage_route`, `discovery_claim` or
            # `discovery_evidence`. The split-grain re-grain above does not reach
            # them (it is fed tier-A keys only), so without this they keep the
            # ingest default and `assert_emitted_parity` halts on 16,097 rows.
            #
            # Same threshold, same raw-`n_chars` unit, same `>=`, same
            # impossible-coverage refusal as the tier-A path. It is an
            # EXTRAPOLATION onto a population the calibration never saw, and it
            # is recorded as such in meta rather than presented as fitted.
            _e1_ml: Dict[Tuple[str, str], int] = {}
            _e1_keys = []
            for _rows in (e1_ra_confirmed, e1_adjudicated_a,
                          e1_rb_screening, e1_r3_frame):
                for _r in (_rows or ()):
                    _k = (_r["page_id"], _r["work_id"])
                    _ml = _r.get("ml")
                    if _ml is not None:
                        _e1_ml[_k] = _ml
                    _e1_keys.append(_k)
            if _e1_keys:
                from v3_routing_ingest import route_e1_by_coverage as _route_e1
                e1_route_report = _route_e1(
                    gen2_router, _e1_keys, _e1_ml, _page_chars)
                if e1_route_report["undecided"]:
                    raise RoutingConflictError(
                        f"E1 coverage routing left "
                        f"{e1_route_report['undecided']} row(s) with no decision "
                        f"-- they would keep the ingest default and silently "
                        f"bypass coverage routing. Halting rather than defaulting."
                    )

        # Every gate passed -- ONLY NOW is it safe to mutate: delete any
        # prior output .db, create the output directory, mint/persist
        # opaque work_ids (crosswalk write, inside assign_opaque_work_ids),
        # and emit the review artifact.
        if out_path.exists():
            out_path.unlink()
        out_path.parent.mkdir(parents=True, exist_ok=True)

        candidates = select_shown_works(conn_research)
        candidates = assign_opaque_work_ids(
            candidates, crosswalk_path, create_if_missing=create_crosswalk_if_missing
        )
        page_index = PageTextIndex(conn_research)
        if review_artifact_path:
            # Reuses the SAME build_claims_and_evidence assembly (over ALL
            # candidates, not just the approved subset below) so the
            # re-emitted CANDIDATE csv's impact columns can never diverge
            # from the real distillation (134-07 Task A). FJMS enrichment is
            # applied ONLY when fjms_db_path was explicitly supplied (unit
            # tests pass None -> no enrichment, no 1.59 GB DB opened).
            review_fjms_conn = _connect_fjms_ro(fjms_db_path) if fjms_db_path else None
            try:
                _emit_enriched_review_artifact(
                    conn_research, candidates, review_artifact_path, page_index=page_index,
                    e1_ra_confirmed=e1_ra_confirmed, e1_adjudicated_a=e1_adjudicated_a,
                    e1_rb_screening=e1_rb_screening, e1_r3_frame=e1_r3_frame,
                    q2_witness_collection=q2_witness_collection,
                    q2_collection_tafsir_targum=q2_collection_tafsir_targum,
                    q2_collection_with_arabic=q2_collection_with_arabic,
                    q2_shared_text=q2_shared_text,
                    fjms_conn=review_fjms_conn,
                )
            finally:
                if review_fjms_conn is not None:
                    review_fjms_conn.close()

        crosswalk = json.loads(Path(crosswalk_path).read_text(encoding="utf-8"))
        valid_work_ids = set(crosswalk.values())

        approved = load_approved_works(from_approved_path, valid_work_ids=valid_work_ids)
        raw_by_opaque = {c["work_id"]: c["raw_work_id"] for c in candidates}
        works = []
        for a in approved:
            raw_work_id = raw_by_opaque.get(a["work_id"])
            if raw_work_id is None:
                continue
            # v2 drop-list (bake plan §4.2): a dropped opaque work_id emits NO
            # works/claim/evidence rows -- excluded BEFORE claim-gen sees it.
            if a["work_id"] in dropped_work_ids:
                continue
            works.append({**a, "raw_work_id": raw_work_id})

        # v2 D-17 year resolution (bake plan §4.3): join the hash-pinned date
        # tables to canonical_work_id via the crosswalk + census map. numeric
        # years only -- the raw descriptive string never leaves the parser.
        year_by_canonical = None
        if run_d17:
            year_by_canonical = resolve_year_by_canonical(
                [composition_dates_map, seftja_dates_map],
                crosswalk=crosswalk, cross_corpus_map=cross_corpus_map,
                dropped_ids=dropped_work_ids,
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
            cross_corpus_map=cross_corpus_map,
            year_by_canonical=year_by_canonical,
            # discovery-v3 (Codex R3): the router REPLACES Lever-1 when supplied.
            # `build_claims_and_evidence` refuses both at once, so passing
            # `apply_lever1` only when there is no router is required, not tidy.
            gen2_router=gen2_router,
            apply_lever1=run_d17 and gen2_router is None,
            reband_tier_a=reband_tier_a,
            v2_bands=v2_build,
        )
        routing_audit_rows = result.get("routing_audit_rows", [])
        # Production coverage gate (Codex #5) -- only on a --release build.
        if release and run_d17:
            assert_pair_coverage_floor(routing_audit_rows, floor=coverage_floor)
        # discovery-v3 gate 3 (Codex R3 BLOCKER): work-side offsets non-NULL on
        # every track1_direct row of a RELEASE build. Round 3 found the gate
        # existed only in the plan: `_ingest_tier_a` probes for `ref_spans_json`
        # and emits NULL when it is absent, so a v2-era research DB could produce
        # a release artifact carrying precisely the missing coordinates gate 3 says
        # must halt -- and the tests asserted NULL was the expected v2 result, so
        # nothing contradicted it.
        #
        # Release-only, deliberately: a v2 rebuild is a legitimate operation whose
        # population genuinely has no work-side coordinate, and failing it would
        # block a real task to enforce a v3 property. A RELEASE build is where the
        # claim is made.
        if release:
            assert_release_work_offsets(result["evidence_rows"])

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
        _insert_works_real(cur, works, cross_corpus_map=cross_corpus_map)
        _insert_claims_and_evidence_real(cur, result["claim_rows"], result["evidence_rows"])
        n_units = _insert_witness_units_real(cur, unit_specs)

        # v2 (135-06): persist every D-17 pairwise decision to the masking-safe
        # discovery_routing_audit table (opaque ids + numeric years only) so
        # each demotion is replayable DB-only (gate 10).
        if routing_audit_rows:
            # 136-12 (schema Amendment (F)): assert BEFORE the insert -- a
            # DB-level check would only catch an unreconstructable tie after the
            # audit trail had already been written.
            assert_kept_tie_rows_name_their_pair(routing_audit_rows)
            cur.executemany(
                """
                INSERT INTO discovery_routing_audit
                    (page_id, kept_work_id, demoted_work_id, kept_year, demoted_year,
                     delta_years, decision, routing_reason)
                VALUES (:page_id, :kept_work_id, :demoted_work_id, :kept_year, :demoted_year,
                        :delta_years, :decision, :routing_reason)
                """,
                routing_audit_rows,
            )

        # bp_rows was already resolved above (H3 / v2 reband), BEFORE any
        # research-DB work began -- reused here unchanged, now that the output
        # schema exists to insert it into. measurement_status is written too
        # (v2, gate 12/13): NULL for a normal build; 'not_measured' for a
        # reband-invalidated band.
        # D-02a (136-06): the `{"measurement_status": None, **r}` dict-literal
        # below lets a row's OWN `measurement_status` key (e.g. the frozen
        # `tier_a` row's `measured_pass`, set in
        # `_frozen_real_band_precision_rows`) win over this `None` default --
        # `_frozen_real_band_precision_rows` is the ONE source of truth for
        # that value; do not "fix" this line to always write NULL.
        cur.executemany(
            """
            INSERT INTO band_precision (
                scope, collection_id, evidence_source, confidence_band, numerator, denominator,
                precision, ci_low, ci_high, method, sampling_frame, ins_policy, weighting, notes,
                measurement_status
            ) VALUES (:scope, :collection_id, :evidence_source, :confidence_band, :numerator,
                       :denominator, :precision, :ci_low, :ci_high, :method, :sampling_frame,
                       :ins_policy, :weighting, :notes, :measurement_status)
            """,
            [{"measurement_status": None, **r} for r in bp_rows],
        )

        # 136-11 Task 2: materialize the identification grain + the manuscript
        # display keys. Ordered AFTER band_precision on purpose -- the main-pool
        # rule's gate 2 reads the `tier_a` certificate authorization
        # (measurement_status/ci_low) out of that registry, so materializing
        # earlier would silently evaluate every tier_a identification against a
        # missing authorization and demote it.
        # 136-12 Task 3: the curated genre + author key. Written BEFORE the
        # identification grain, which joins `works` for the identity axis.
        if curated_genres:
            curated_stats.update(apply_work_genres(out_conn, curated_genres))
        if author_alias_index:
            curated_stats.update(assert_author_key_coverage(out_conn, author_alias_index))

        # 136-12 Task 1: the novelty axis, written BEFORE the identification
        # grain is materialized -- `populate_discovery_identification` inherits
        # each identification's shade from its own evidence rows, so a grain
        # materialized first would freeze the pre-ingest `not_checked` default.
        novelty_stats = apply_novelty_verdicts(
            out_conn, novelty_grain_index, alias_groups=novelty_alias_groups
        )

        # CD batch / Amendment 2026-08-12 (R): the Contract-1 input tables.
        # Ingested BEFORE the identification grain, because C-track's matrix
        # runs as the last step of `populate_discovery_identification` and step 4
        # reads `discovery_curated_quoter` -- ingesting afterwards would render
        # every row as though the owner's curated list were still empty, and the
        # build would pass while asserting the pre-ruling relations. (Also still
        # before the count/meta block, so the release-contract counts include
        # their rows.) Each returns its version meta row.
        contract1_input_meta_rows: List[Tuple[str, str]] = []
        if curated_quoter_path:
            contract1_input_meta_rows.extend(
                ingest_curated_quoter(out_conn, curated_quoter_path))
        if region_map_path:
            with open(crosswalk_path, encoding="utf-8") as fh:
                _cw = json.load(fh)
            contract1_input_meta_rows.extend(ingest_region_map(
                out_conn, region_map_path, _cw.get("crosswalk", _cw)))

        identification_stats = populate_discovery_identification(out_conn)
        identification_stats.update(
            populate_manuscript_display(out_conn, libraries_csv_path)
        )

        # 136-12 (VIS-01 vs D-22): REPORT the disagreement, never resolve it.
        launch_scope_reconciliation = compute_launch_scope_reconciliation(out_conn)

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
            # 136-11 / schema Amendment 2026-08-02 (C1).
            ("expected_rows_discovery_identification",
             str(identification_stats["identifications"])),
            ("expected_rows_manuscript_display",
             str(identification_stats["manuscript_display"])),
            # 136-12 / schema Amendment 2026-08-02 (C1): the audience marker
            # on the PRIVATE artifact. The public projection writes `public` on
            # its own output; nothing wrote the private value, so the private
            # artifact would have shipped without a field its own contract
            # defines. This does not change the security property -- the runtime
            # loader treats MISSING and `private` identically and fails closed
            # either way -- but a contract field that is defined and never
            # written is a contract that decays.
            ("audience", ASSET_AUDIENCE_PRIVATE),
            ("frame_content_hash", frame_content_hash),
            # 135-07: the explicit, durable band-vocabulary version marker
            # ("v1"/"v2"). The verifier keys its version-aware expected top-tier
            # band KEY on this (cross-checked against the ACTUAL bands present +
            # the real canonical_merges_sha256), and `--require-v2` proves the
            # bake matched OPERATOR INTENT rather than letting the asset choose
            # its own contract.
            ("band_vocab_version", band_vocab_version),
            # discovery-v3 (Codex R3 BLOCKER): WHICH coverage routing produced this
            # asset. The two answers describe materially different populations --
            # the legacy cliff demotes 19.3% of what the router ships -- and a
            # reader has no build log, so without this the asset cannot say which
            # one it contains. `gen2_router` (not the flag) drives it: intent is not
            # evidence.
            ("coverage_routing", (
                "gen2_router_split_regrained" if regrain_report is not None
                else ("gen2_router" if gen2_router is not None
                      else ("lever1_cliff" if run_d17 else "none")))),
        ]
        # SPLIT-GRAIN RE-GRAINING provenance (option 4). The honesty-critical rows:
        # the threshold was calibrated at the COLLAPSED grain and APPLIED at the
        # split grain, and the asset must say so rather than imply a validated
        # calibration at the grain it shipped. Measured basis for reusing it: of the
        # 1,395 graded claims the threshold was fitted on, 340 (24.2%) fall on the
        # three works v3 splits; refitting on the grain-clean 1,055 gives 0.2531, so
        # reusing 0.2984 costs 0.47pp accuracy there and is the CONSERVATIVE choice
        # (the refit would ship 10,224 MORE rows). Defensible -- but a JUDGEMENT,
        # not a calibration, which is exactly what these rows record.
        if regrain_report is not None:
            from v3_routing_ingest import (
                REGRAIN_META_APPLIED_GRAIN as _RM_APPLIED,
                REGRAIN_META_CALIBRATED_GRAIN as _RM_CALIB,
                REGRAIN_META_SOURCE as _RM_SRC,
                REGRAIN_META_THRESHOLD as _RM_THR,
            )
            meta_rows.extend([
                (_RM_THR, repr(regrain_report["threshold"])),
                (_RM_CALIB, regrain_report["threshold_grain_calibrated"]),
                (_RM_APPLIED, regrain_report["threshold_grain_applied"]),
                # WHERE the threshold came from -- read from the router's own meta
                # row, never a literal. Truncated variants (0.298, 0.2984) exist in
                # the tree and each moves 90 rows across the line.
                (_RM_SRC, "coverage_route_meta.threshold"),
                ("coverage_regrain_kept_exact", str(regrain_report["kept_exact"])),
                ("coverage_regrain_recomputed", str(
                    regrain_report["recomputed_same_work"]
                    + regrain_report["recomputed_parallel"])),
                ("coverage_regrain_disagrees_with_parent",
                 str(regrain_report["disagrees_with_parent"])),
            ])
        if e1_route_report is not None:
            # The E1 extrapolation, recorded so a reader of the ASSET can see
            # that this population was routed by a threshold fitted on a
            # different tier -- not inferred from the absence of a note.
            meta_rows.extend([
                ("coverage_e1_routing", "threshold_extrapolated_tier_b"),
                ("coverage_e1_considered", str(e1_route_report["considered"])),
                ("coverage_e1_routed", str(e1_route_report["added"])),
                ("coverage_e1_same_work", str(e1_route_report["same_work"])),
                ("coverage_e1_parallel", str(e1_route_report["parallel"])),
            ])
        # v2 provenance (bake plan §7 gate 11, Codex #B2/#5): record the
        # verified SHA-256 of every supplied hash-pinned input in meta.
        if canonical_merges_sha256 is not None:
            meta_rows.append(("canonical_merges_sha256", canonical_merges_sha256))
        if composition_dates_sha256 is not None:
            meta_rows.append(("composition_dates_sha256", composition_dates_sha256))
        if seftja_dates_sha256 is not None:
            meta_rows.append(("seftja_dates_sha256", seftja_dates_sha256))
        # 136-12: the verdict cache's own verified SHA, recorded in meta so a
        # shipped asset proves WHICH measured cache produced its novelty column.
        # The cache itself is a BUILD-TIME artifact and never ships (NOVEL-02).
        if novelty_input_stats.get("verdict_cache_sha256"):
            meta_rows.append(
                ("novelty_verdicts_sha256", novelty_input_stats["verdict_cache_sha256"]))
            # discovery-v3 (Codex R3 BLOCKER): record IN THE ASSET whether the
            # fingerprint gate ran. The cache SHA proves which FILE was read; only
            # this proves whether each verdict was checked against the question it
            # answered. An asset built through the waiver is a materially
            # different claim, and a reader must be able to tell without the build
            # log -- which is not shipped.
            meta_rows.append((
                "novelty_input_fingerprint_checked",
                "1" if novelty_input_stats.get("verdict_fingerprint_checked") else "0",
            ))
        # 136-12: the curated artifacts' verified content hashes. A POPULATED
        # `works.genre` column must be able to name the pinned artifact that
        # produced it -- the release verifier checks exactly that, and it is the
        # only genre provenance an independent verifier can see (the artifact
        # itself is gitignored).
        if curated_stats.get("work_domains_content_hash"):
            meta_rows.append(
                ("work_domains_content_hash", curated_stats["work_domains_content_hash"]))
        if curated_stats.get("work_author_aliases_content_hash"):
            meta_rows.append(
                ("work_author_aliases_content_hash",
                 curated_stats["work_author_aliases_content_hash"]))
        # v2 reband markers (bake plan §4.5, gate 13): the target band + the
        # rebanded-row count + the preserved trigger provenance.
        if reband_tier_a:
            # A screening_rb row can ONLY reach review_only via the reband
            # (bake plan §4.3 reband-routing-reason clarification) -- so this
            # counts EXACTLY the rebanded rows, never the original shipped
            # screening_rb population.
            (n_rebanded,) = out_conn.execute(
                "SELECT COUNT(*) FROM discovery_evidence WHERE confidence_band=? AND routing_status=?",
                (_SCREENING_RB, _REVIEW_ONLY),
            ).fetchone()
            reband_meta_extra["tier_a_reband_count"] = str(n_rebanded)
            for k, v in reband_meta_extra.items():
                meta_rows.append((k, v))
        # CD batch / Amendment 2026-08-12 (U): marker + per-table counts +
        # the Contract-0 bake-side basis pin (when supplied).
        meta_rows.extend(amendment_2026_08_12_meta_rows(
            out_conn, reference_corpus_sha256=reference_corpus_sha256))
        # Amendment 2026-08-12 (S): the population lock's copied constants.
        if population_lock_path:
            with open(population_lock_path, encoding="utf-8") as fh:
                _lock = json.load(fh)
            meta_rows.extend(
                population_lock_meta_rows(_lock, _hash_file(Path(population_lock_path))))
        # Amendment 2026-08-12 (R): the active region/curated versions.
        meta_rows.extend(contract1_input_meta_rows)
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
            "discovery_identification": identification_stats["identifications"],
            "manuscript_display": identification_stats["manuscript_display"],
        },
        # The measured identification grain, alongside the shipped-ONLY figure,
        # so the delta the D-13g `shipped OR human_confirmed` fix restores is
        # visible in the build summary rather than absorbed into one total.
        "identification": identification_stats,
        # 136-12: the novelty ingest's own measured shape -- the per-shade
        # counts, the per-family resolved counts (so the direct-family coverage
        # gap this rebuild closes is a NUMBER in the report, not a claim), and
        # every fail-closed fallback the cache triggered.
        "novelty": {**novelty_input_stats, **novelty_stats},
        # 136-12: VIS-01's launch-scope shortcut vs the D-22 two-axis
        # conjunction over the SAME rows -- both counts and the symmetric
        # difference by corpus x family. REPORTED, never resolved: a projection
        # silently narrowing or widening the public asset is the failure this
        # number exists to prevent.
        "launch_scope_reconciliation": launch_scope_reconciliation,
        # 136-12: the curated genre/author load -- both content hashes, the
        # assigned/unassigned split, and the author-key coverage.
        "curated": curated_stats,
        "frame_content_hash": frame_content_hash,
        "content_hash": content_hash,
        "band_precision_rows": len(bp_rows),
        "artifact_masking_issues": artifact_issue_count,
        "evidence_id_collisions": result.get("evidence_id_collisions", 0),
        # SPLIT-GRAIN RE-GRAINING (option 4) and the parity gate's HONEST split.
        # `router_parity.checked_regrained` is compared against the re-grainer's own
        # output (same dict), so it is self-consistency; only
        # `checked_independent` is cross-artifact verification. Surfaced here so the
        # build summary states both rather than one total that reads like the whole
        # population was independently verified.
        "router_parity": result.get("router_parity"),
        "coverage_regrain": regrain_report,
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
                             "real-mode defaults. A tier_a measurement_status='measured_fail' "
                             "row triggers the v2 CERT-01 FAIL-branch reband (135-06).")
    v2_group = parser.add_argument_group("v2 re-distill hash-pinned inputs (135-06)")
    v2_group.add_argument("--canonical-merges", metavar="PATH", default=None,
                        help="Hash-pinned census: opaque w000xxx merge map + drop-list "
                             "(REQUIRED for --release; Codex #B2).")
    v2_group.add_argument("--canonical-merges-sha256", metavar="HEX", default=None,
                        help="SHA-256 pin verified before --canonical-merges is used.")
    v2_group.add_argument("--composition-dates", metavar="PATH", default=None,
                        help="Hash-pinned M-source composition dates (REQUIRED for --release "
                             "D-17; Codex #5). Frozen schema; normalized to a numeric year.")
    v2_group.add_argument("--composition-dates-sha256", metavar="HEX", default=None,
                        help="SHA-256 pin verified before --composition-dates is used.")
    v2_group.add_argument("--seftja-dates", metavar="PATH", default=None,
                        help="Hash-pinned interim SEF/JA composition dates (REQUIRED for "
                             "--release D-17; Codex #5). Frozen {year:int, basis:str} schema.")
    v2_group.add_argument("--seftja-dates-sha256", metavar="HEX", default=None,
                        help="SHA-256 pin verified before --seftja-dates is used.")
    novelty_group = parser.add_argument_group("novelty axis hash-pinned inputs (136-12, NOVEL-01/02)")
    novelty_group.add_argument("--novelty-verdicts", metavar="PATH", default=None,
                        help="Hash-pinned novelty verdict cache -- the funnel+gate output, "
                             "keyed '{sys_id}::{ref_work_id}'. Omit and every evidence row "
                             "keeps the fail-closed `not_checked` (a real state, never "
                             "'novel by default'). The cache is a BUILD-TIME artifact and "
                             "is NEVER shipped inside the sidecar (NOVEL-02).")
    novelty_group.add_argument("--novelty-verdicts-sha256", metavar="HEX", default=None,
                        help="SHA-256 pin, REQUIRED whenever --novelty-verdicts is given: "
                             "a cache that is not the cache that was MEASURED is not a "
                             "pinned input (T-136-12-03).")
    v2_group.add_argument("--gen2-router-evidence-db", metavar="PATH", default=None,
                        help="gen-2 evidence DB carrying coverage_route + "
                             "coverage_route_meta (discovery-v3, Codex R3). Supplied => "
                             "gen-2's FITTED routing decision is ingested and REPLACES the "
                             "legacy Lever-1 0.45 cliff. REQUIRED whenever dates are given "
                             "(i.e. whenever D-17 runs) unless "
                             "--allow-lever1-coverage is set: measured on the real "
                             "artifact, the legacy cliff demotes 30,899 of 160,095 "
                             "`same_work` rows (19.3%%) the router ships, one-way.")
    v2_group.add_argument("--allow-lever1-coverage", action="store_true",
                        help="Run the LEGACY Lever-1 coverage cliff instead of ingesting "
                             "gen-2's router. Choose it deliberately or not at all; it is "
                             "recorded in the asset as coverage_routing=lever1_cliff.")
    novelty_group.add_argument("--novelty-input-fingerprints", metavar="PATH", default=None,
                        help="JSON {'{sys_id}::{work_id}': fingerprint} from "
                             "discovery_novelty_funnel.candidate_input_fingerprint. REQUIRED "
                             "with --novelty-verdicts (discovery-v3, Codex R3): the cache SHA "
                             "proves which FILE was read, never which QUESTION each entry "
                             "answered, so without these a stale verdict (changed title, "
                             "rebuilt alias group, refreshed finding aid) is reused silently. "
                             "A pair whose fingerprint is absent or differs resolves to the "
                             "fail-closed `not_checked` and is COUNTED.")
    novelty_group.add_argument("--novelty-allow-unfingerprinted-cache", action="store_true",
                        help="Load a verdict cache WITHOUT the fingerprint gate. Only for a "
                             "v2-era rebuild against the v2-era cache; a v3 build must never "
                             "use it. Named for what it does so it cannot be passed "
                             "absent-mindedly, and recorded in the asset's meta as "
                             "novelty_input_fingerprint_checked=0.")
    novelty_group.add_argument("--work-domains", metavar="PATH", default=None,
                        help="Hash-pinned curated work-domain artifact (plan 136-09) -> "
                             "works.genre at the CANONICAL grain. A row still HELD for owner "
                             "ruling refuses the build.")
    novelty_group.add_argument("--work-domains-content-hash", metavar="SHA", default=None,
                        help="'sha256:<hex>' content-hash pin, REQUIRED with --work-domains. "
                             "The PRE-RULING hash is superseded and must not be used.")
    novelty_group.add_argument("--work-author-aliases", metavar="PATH", default=None,
                        help="Hash-pinned curated author-alias artifact (plan 136-09). Binds "
                             "the curated author key to the asset by an enforced coverage "
                             "check; it writes no column (none is authorized).")
    novelty_group.add_argument("--work-author-aliases-content-hash", metavar="SHA", default=None,
                        help="'sha256:<hex>' content-hash pin, REQUIRED with "
                             "--work-author-aliases.")
    novelty_group.add_argument("--novelty-alias-groups", metavar="PATH", default=None,
                        help="Curated work-id alias groups (D-23d). Absent => every work is "
                             "its own singleton reviewed identity (fail-closed), never a "
                             "guessed grouping.")
    v2_group.add_argument("--curated-quoter", metavar="PATH", default=None,
                        help="Tracked curated-quoter list JSON (Amendment 2026-08-12 (R); "
                             "docs/specs/discovery-curated-quoter-v1.json). Matrix step 4's "
                             "curated half; rows relabel, never delete. A canonical id the "
                             "asset does not carry fails the build.")
    v2_group.add_argument("--region-map", metavar="PATH", default=None,
                        help="Owner region-input JSON (Amendment 2026-08-12 (R)). Matrix "
                             "step 3's input, re-keyed via the crosswalk. REQUIRES the "
                             "locus tables populated (supply with the D-track locus "
                             "import); fails closed otherwise.")
    v2_group.add_argument("--population-lock", metavar="PATH", default=None,
                        help="Tracked population-lock JSON (schema Amendment 2026-08-12 (S), "
                             "scripts/emit_population_lock.py). Constants are COPIED into "
                             "meta -- never recomputed -- and the verifier enforces the "
                             "lock's retention floors against every shipped asset.")
    v2_group.add_argument("--reference-corpus-sha256", metavar="HEX", default=None,
                        help="Contract 0 (schema Amendment 2026-08-12 (T)): SHA-256 of the "
                             "reference-corpus stream the evidence w_start/w_end offsets "
                             "index. Written to meta as reference_corpus_sha256; the "
                             "verifier asserts equality with the locus build's own "
                             "locus_reference_corpus_sha256 whenever locus_unit is "
                             "populated. Omit only on a build with no work-side offsets.")
    real_group.add_argument("--frozen-precision-defaults", action="store_true",
                        help="H3: explicitly acknowledge using the frozen-contract "
                             "band_precision defaults (docs/specs/discovery-sidecar-"
                             "schema-v1.md SS1.6, tier_a=NULL) instead of a custom "
                             "--precision-spec. Required (together with --precision-spec "
                             "being one-or-the-other) for --release.")
    real_group.add_argument("--emit-review-artifact-only", action="store_true",
                        help="134-07 Task 1/A: emit ONLY the enriched, impact-prioritized "
                             "CANDIDATE review csv (reusing the real distillation assembly) "
                             "and exit -- does NOT require --from-approved and does NOT "
                             "write a discovery.db. Reuses --crosswalk WITHOUT re-minting "
                             "(pass --init-crosswalk only for a genuinely first-ever build).")
    real_group.add_argument("--include-masked-metadata", action="store_true",
                        help="Owner opt-in (owner decision 2026-07-22): populate author + "
                             "genre for masked (none-owner-supplies) rows of the gitignored "
                             "CANDIDATE review csv as skim signals. candidate_title stays "
                             "blank regardless (owner supplies the neutral title). DEFAULT "
                             "OFF (fail-closed -- author/genre blank for masked rows). The "
                             "finished CSV is still subject to the blocking masking scan.")
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

    if args.emit_review_artifact_only:
        # 134-07 Task 1/A: emit ONLY the enriched CANDIDATE csv, no
        # --from-approved required (there isn't one yet), no .db written.
        if not args.crosswalk:
            parser.error("--crosswalk is required for --emit-review-artifact-only")
        review_artifact_path = args.review_artifact or str(
            Path(_REPO_ROOT) / "discovery_data" / "discovery-review-candidates.csv"
        )
        collection_paths = _resolve_collection_paths(args.research_data_dir)
        stats = build_candidate_review_artifact(
            source_db_path=args.db_path,
            crosswalk_path=args.crosswalk,
            out_csv_path=review_artifact_path,
            e1_ra_confirmed_path=collection_paths["e1_ra_confirmed"],
            e1_adjudicated_a_path=collection_paths["e1_adjudicated_a"],
            e1_rb_screening_path=collection_paths["e1_rb_screening"],
            e1_r3_frame_path=collection_paths["e1_r3_frame"],
            q2_witness_collection_path=collection_paths["q2_witness_collection"],
            q2_collection_tafsir_targum_path=collection_paths["q2_collection_tafsir_targum"],
            q2_collection_with_arabic_path=collection_paths["q2_collection_with_arabic"],
            q2_shared_text_path=collection_paths["q2_shared_text"],
            fjms_db_path=args.fjms_db,
            create_crosswalk_if_missing=args.init_crosswalk,
            include_masked_metadata=args.include_masked_metadata,
        )
        print(f"review artifact OK: {stats}")
        return 0

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
        canonical_merges_path=args.canonical_merges,
        canonical_merges_sha256=args.canonical_merges_sha256,
        composition_dates_path=args.composition_dates,
        composition_dates_sha256=args.composition_dates_sha256,
        seftja_dates_path=args.seftja_dates,
        seftja_dates_sha256=args.seftja_dates_sha256,
        gen2_router_evidence_db=args.gen2_router_evidence_db,
        allow_lever1_coverage=args.allow_lever1_coverage,
        novelty_verdicts_path=args.novelty_verdicts,
        novelty_verdicts_sha256=args.novelty_verdicts_sha256,
        novelty_input_fingerprints=_load_novelty_fingerprints(
            args.novelty_input_fingerprints),
        novelty_allow_unfingerprinted_cache=args.novelty_allow_unfingerprinted_cache,
        novelty_alias_groups_path=args.novelty_alias_groups,
        work_domains_path=args.work_domains,
        work_domains_content_hash=args.work_domains_content_hash,
        work_author_aliases_path=args.work_author_aliases,
        work_author_aliases_content_hash=args.work_author_aliases_content_hash,
        reference_corpus_sha256=args.reference_corpus_sha256,
        population_lock_path=args.population_lock,
        curated_quoter_path=args.curated_quoter,
        region_map_path=args.region_map,
    )
    print(f"real build OK: {stats['row_counts']}")
    print(f"novelty={stats['novelty']}")
    print(f"content_hash={stats['content_hash']}")
    print(f"frame_content_hash={stats['frame_content_hash']}")
    print(f"evidence_id_collisions={stats['evidence_id_collisions']}")
    print(f"db_path={stats['db_path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
