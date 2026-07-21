# Discovery Sidecar Schema v1

**Status: FROZEN (2026-07-22).** This document is the authoritative, frozen
contract for the `discovery.db` sidecar's claim model. It replaces the
superseded PARTIAL draft (commits `f6127f9a`/`6188ce81`) which modeled only
Track-1 direct work-witness bands plus a `flank_class`-based MS-MS heuristic —
that draft entirely missed the SEED-029 Q2 recall-widening / cluster-
propagation layer and was corrected at the 134-01 owner gate. This document
implements the **CONTRACT CORRECTION (2026-07-21)** recorded in
`134-CONTEXT.md` (clauses C-1..C-9 + the ROUND-3 amendments R1-R7), pinned to
the REAL research-data field names/keys/predicates. Every later plan in
Phase 134 (fixture, distillation, loader, service, frame) consumes the
recipes and tables frozen here.

**Provenance-masking note:** every gitignored-artifact filename cited below
(`e1_ra_confirmed.jsonl`, `track1_matches`, `q2_witness_collection.jsonl`,
etc.) lives in the gitignored `same_work_spike/probe/` research tree
(dev-box only, never committed, never shipped). This document and all
product code reference the restricted reference corpora ONLY by the masked
codenames **M-source** and **R-source** — never by real name, path, or
provenance. The filenames quoted here are artifact identifiers for the
offline build step, not restricted content; no raw `work_id` VALUE, no
reference text, and no corpus sigla appear anywhere in this document.
`check_atlas_masking.py --scan-repo` gates this doc on every commit (the
real gate is fail-safe/exit-1 on this machine while
`MASKING_SCAN_PATTERNS_FILE` is unset — see the executor's SUMMARY for the
deferred-gate note; this doc is masking-clean BY CONSTRUCTION).

---

## Frozen Enum Vocabularies

```
claim_type        in {direct_witness, quotes_this_work, shared_text}
                  (old "textual-parallel" / "direct-text-overlap" codes DROPPED, C-2 — they fold into shared_text)
evidence_kind     in {witness, shared_text}                    (R2 discriminator on discovery_evidence)
evidence_source   in {track1_direct, propagated}                (shared_text is propagated + not_evaluated, NOT a third source, F7)
confidence_band   track1_direct -> {expert_verified, tier_a, screening_rb, screening_canon}
                  propagated    -> {corroborated, weak, not_evaluated}   (not_evaluated is a REAL enum value, never NULL, C-4)
adjudication_status in {human_confirmed, provisional, unreviewed}
audit_status        in {audit_pending, audit_passed, n/a}   (NO audit_passed at launch — registry-gated, C-6; "certified" PROHIBITED)
routing_status      in {shipped, review_only}
routing_reason      in {impurity, runner_up_conflict, co_citation, none}
source_corpus       in {sefaria, ja, msource}   (masked codes; internal-only; NEVER displayed, D-03a)
merge_basis         in {oxford_part, physical_join}   (NEVER scribe, DATA-10)
```

These are the ONE source of truth; `scripts/discovery_ids.py` exposes them as
module-level frozen constants so the build, the service, and the tests never
duplicate the vocab.

---

## 1. Two-Table Claim Model (C-5)

The model is a **two-table split**: one row per **catalogued work anchor**
(`works`), one row per **real (page, work) claim** (`discovery_claim`), and a
**1-to-many** evidence table (`discovery_evidence`) carrying every piece of
supporting evidence for that claim — witness identifications AND shared-text
citations both attach here, discriminated by `evidence_kind`. Physical-MS
deduplication is **NOT** a claim-key collapse; it is a display/projection
concern handled entirely by `witness_units` (§1.4 / DATA-10). This SUPERSEDES
the historical 6-table sketch in `134-RESEARCH.md` (the historical
work-witness-claim / work-witness-page / MS-MS-claim / MS-MS-alignment
single-family tables are HISTORICAL research-notes framing, not the shipped
schema — none of those table names is defined anywhere in this document).

### 1.1 `works`

```sql
CREATE TABLE works (
  work_id             TEXT PRIMARY KEY,   -- opaque product id (minted 1:1; NEVER raw M:/J:/REF or a filename stem)
  canonical_work_id    TEXT NOT NULL,      -- cross-corpus dedup key; DEFAULTS to work_id (F16) until a gen-2 rebuild sets it
  neutral_title        TEXT NOT NULL,      -- human-reviewed (D-07); fail-closed EXCLUDE if unreviewed (never a research title fallback)
  author               TEXT,
  genre                TEXT,
  source_corpus        TEXT NOT NULL CHECK (source_corpus IN ('sefaria','ja','msource'))  -- masked; internal-only; NEVER displayed
);
CREATE INDEX ix_works_canonical ON works(canonical_work_id);
```

### 1.2 `discovery_claim`

`page_id` is the **REAL page** — never a collapsed physical-MS
representative (R5). `claim_id` is stable per real `(page_id, work_id)`
forever, independent of how many evidence rows the claim later accumulates.

```sql
CREATE TABLE discovery_claim (
  page_id             TEXT NOT NULL,
  work_id             TEXT NOT NULL REFERENCES works(work_id),
  claim_id            TEXT NOT NULL UNIQUE,   -- sha256(namespace|page_id|work_id) — see §2; NOT a function of claim_type (G5)
  claim_type          TEXT NOT NULL CHECK (claim_type IN ('direct_witness','quotes_this_work','shared_text')),
  display_evidence_id TEXT NOT NULL,          -- deterministic TOTAL selection — see §6
  source_corpus       TEXT NOT NULL,          -- MUST equal works.source_corpus for this work_id (F4 cross-table consistency)
  sidecar_version     TEXT NOT NULL,
  PRIMARY KEY (page_id, work_id)
);
CREATE INDEX ix_discovery_claim_work_id ON discovery_claim(work_id);
CREATE INDEX ix_discovery_claim_page_id ON discovery_claim(page_id);
```

### 1.3 `discovery_evidence`

One row per piece of supporting evidence. `evidence_kind` (R2) is the
discriminator the VALID-combination matrix keys on (§5) — NOT the parent
`claim_type` — so a `direct_witness` claim carrying a `not_evaluated`
shared_text evidence row (the 43,046-row collision, F7) validates cleanly.
Every row carries its OWN `a_page_id`/`sys_id` (indexed, R5) so PANEL-02
("pages related to this page") is served by-page WITHOUT any physical-MS
claim-key collapse.

```sql
CREATE TABLE discovery_evidence (
  evidence_id       TEXT PRIMARY KEY,     -- sha256 digest — see §2
  claim_id          TEXT NOT NULL REFERENCES discovery_claim(claim_id),
  evidence_kind     TEXT NOT NULL CHECK (evidence_kind IN ('witness','shared_text')),
  evidence_source   TEXT NOT NULL CHECK (evidence_source IN ('track1_direct','propagated')),
  confidence_band   TEXT NOT NULL,        -- validated against evidence_source's enum — see §4/§5
  adjudication_status TEXT NOT NULL CHECK (adjudication_status IN ('human_confirmed','provisional','unreviewed')),
  audit_status      TEXT NOT NULL CHECK (audit_status IN ('audit_pending','audit_passed','n/a')),
  routing_status    TEXT NOT NULL CHECK (routing_status IN ('shipped','review_only')),
  routing_reason    TEXT NOT NULL CHECK (routing_reason IN ('impurity','runner_up_conflict','co_citation','none')),
  is_new            INTEGER NOT NULL DEFAULT 0,  -- FLAG (C-8) — new? is a flag, NOT a surface

  -- this evidence row's OWN origin (R5) — normally == claim.page_id, but
  -- indexed independently so by-page queries never need a claim-key collapse
  a_page_id         TEXT NOT NULL,
  sys_id            TEXT NOT NULL,

  -- attribute columns — a SUPERSET union across families; each family
  -- populates ONLY its own available fields, all others stay NULL. Do NOT
  -- assert flank_class/cluster_size/router_bucket/rung/ge3 on shared_text
  -- rows — those live on the propagated WITNESS family only (C-4).
  tier              TEXT,      -- shared_text: T1 (>=250 letters) / T2 (100-250) / T3 (40-100)
  aligned_len       INTEGER,   -- shared_text
  occ_class         TEXT,      -- shared_text + propagated witness
  cross_language    INTEGER,   -- shared_text (boolean flag)
  n_seed_ms         INTEGER,   -- shared_text
  trials            INTEGER,   -- propagated witness (>=2 => corroborated-eligible)
  runner_up         REAL,      -- propagated witness (impurity input; see is_impure)
  community         TEXT,      -- reserved: cluster/community label attribute, populated only where the
                                -- source collection itself carries one (unused at v1 launch, kept for totality)
  ge3               INTEGER,   -- propagated witness (boolean: >=3-member cluster; no separate band interval, C-4)
  rung              TEXT,      -- propagated witness, one-seed rows: 'A' | 'B2'
  router_bucket     TEXT,      -- propagated family-router rows: 'tafsir_targum' | 'with_arabic'
  matched_letters   INTEGER,   -- track1_direct (tier_a / E1 rows)
  density           REAL,      -- track1_direct (tier_a best_density / E1 dens)
  n_spans           INTEGER,   -- track1_direct (tier_a spans_json span count)

  -- primary a-side span (the OUR-side offsets into a_page_id's text layer)
  span_start        INTEGER NOT NULL,
  span_end          INTEGER NOT NULL,
  text_layer        TEXT,
  snapshot_hash     TEXT,      -- per-page HTR drift signal (OQ3) for a_page_id

  -- provenance (propagated witness family, R4/G3): every DISTINCT
  -- candidate-side (occ0,occ1) occurrence, up to 14 per row (raw seeds up
  -- to 32 collapse by (occ0,occ1)). span_start/span_end above = the largest
  -- one (tie-break: (occ1-occ0) DESC, then occ0 ASC, then occ1 ASC).
  -- track1_evidence_id is DROPPED (absent from the source seed object).
  seed_spans        TEXT,      -- JSON list: [{occ0,occ1,occ_class,seed_page_ids,seed_sys_ids}, ...]
  seed_ms_ids       TEXT,      -- JSON list of distinct OUR-side seed page_ids/sys_ids

  -- OPTIONAL b-side (shared_text only) — REQUIRED NON-NULL together on every
  -- shared_text row: other_page_id + snapshot_hash_b (computed over the
  -- WHOLE other_page_id page). b_start/b_end are NULLABLE/ABSENT because the
  -- source collection carries only the a-side occurrence span + a b-side
  -- page id — b-side drift therefore fails closed independently at PAGE
  -- granularity, not span granularity (OQ3).
  other_page_id     TEXT,
  b_start           INTEGER,
  b_end             INTEGER,
  text_layer_b      TEXT,
  snapshot_hash_b   TEXT,

  rule_version      TEXT,
  community_id      TEXT,      -- nullable graph-community link (distinct from the `community` attribute above)

  UNIQUE(claim_id, evidence_id)
);
CREATE INDEX ix_discovery_evidence_claim_id     ON discovery_evidence(claim_id);
CREATE INDEX ix_discovery_evidence_a_page_id    ON discovery_evidence(a_page_id);
CREATE INDEX ix_discovery_evidence_other_page_id ON discovery_evidence(other_page_id);
```

**FK ownership (F12):** `discovery_claim.display_evidence_id` MUST reference
a `discovery_evidence` row whose `claim_id` equals that SAME claim's
`claim_id` — a display pointer can never point at another claim's evidence.
This is enforced by a composite ownership constraint (an application-level
CHECK at build time, backed by the `UNIQUE(claim_id, evidence_id)` index
above, since SQLite cannot express a cross-column composite FK against a
non-composite-unique target without duplicating `claim_id` onto the
pointing side). `PRAGMA foreign_keys=ON` is enabled at BOTH build and load
time. The circular FK (`discovery_claim.display_evidence_id` <->
`discovery_evidence.claim_id`) is intentional and resolved at build time:
evidence rows are inserted first (uncommitted `display_evidence_id`), then
`select_display_evidence` (§6) computes and back-fills the claim's pointer
in a second pass.

Physical-MS dedup does **NOT** collapse claims (R5) — every real
`(page_id, work_id)` is its own claim row; the DATA-10 unit×work
projection (Phase 134-06) shows a physical MS once via `witness_units`.
`discovery_evidence` never carries an aggregated "supporting_page_ids" list
as a claim-key-collapse mechanism.

### 1.4 `witness_units` + `witness_unit_members` (DATA-10 projection)

```sql
CREATE TABLE witness_units (
  unit_id  TEXT PRIMARY KEY   -- sha256 over sorted member sys_ids — see §2
);
CREATE TABLE witness_unit_members (
  unit_id  TEXT NOT NULL REFERENCES witness_units(unit_id),
  sys_id   TEXT NOT NULL,
  merge_basis TEXT NOT NULL CHECK (merge_basis IN ('oxford_part','physical_join')),  -- NEVER scribe
  UNIQUE(sys_id)
);
```

### 1.5 `meta`

Release-contract key/value store (DATA-08), read at startup and verified
against actuals: `schema_version`, `sidecar_version`, `source_db_sha256`,
`build_date`, `data_as_of`, `htr_snapshot_hash` (ONE corpus-level hash, OQ3
FROZEN default — see §9), `expected_rows_claims`, `expected_rows_evidence`,
`expected_rows_works`, `expected_rows_units`, `frame_content_hash`.

```sql
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
```

### 1.6 `band_precision` (C-7, TABLE not meta JSON — G8)

Populated DURING the build, BEFORE final hashing/VACUUM/manifest emission
(F13), from placeholder precision in the 134-03 synthetic mode and the
FINALIZED C-7 numbers in 134-07. This is the surface Phase 135 BAND-02
reads data-driven with NO code change.

```sql
CREATE TABLE band_precision (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  scope           TEXT NOT NULL CHECK (scope IN ('collection','band')),
  collection_id   TEXT NOT NULL,     -- e.g. 'propagated_witness_collection_v1'
  evidence_source TEXT,              -- NULL at scope='collection'; set at scope='band'
  confidence_band TEXT,              -- NULL at scope='collection'; set at scope='band'
  numerator       INTEGER,
  denominator     INTEGER,
  precision       REAL,              -- NULL where no valid band-specific measurement exists (C-7/G8)
  ci_low          REAL,
  ci_high         REAL,
  method          TEXT,              -- e.g. 'work-cluster bootstrap' | 'locked-rule evaluation'
  sampling_frame  TEXT,
  ins_policy      TEXT,
  weighting       TEXT,
  notes           TEXT
);
```

**Frozen population rule (R1/C-7/G8):** the collection-level **0.926
[0.875,0.968]** — a work-cluster bootstrap over the FULL router-cleaned
propagated WITNESS COLLECTION (corroborated UNION weak), held-out 200-card
draw = 90 corroborated + 110 weak, determinate 176/190, frame_size 2,109
after ledger+neighborhood+shelf exclusion — is stored **EXACTLY ONCE** at
`scope='collection'` with `collection_id='propagated_witness_collection_v1'`
(or equivalent stable id), `evidence_source`/`confidence_band` both NULL.
The `scope='band'` rows for `corroborated` and `weak`
(`evidence_source='propagated'`) carry **NULL** `precision`/`ci_low`/
`ci_high` — per the owner's no-pooling discipline there is NO valid
propagated-band-specific measurement (the 0.926 is a single mixed-collection
draw, never a corroborated-only 81/86 or weak-only 95/104 split — those
post-hoc splits are explicitly NOT valid band precisions). The
`track1_direct` `scope='band'` rows (`expert_verified`, `screening_rb`,
`screening_canon`) DO carry their own E1-certification-registry
band-specific precisions (legitimately band-specific, pre-registered
measurements, distinct from the pooled 0.926 — feeding CERT-01). So
Phase 135 BAND-02 can never surface the 0.926 as a propagated-band estimate.

---

## 2. Frozen ID Recipes (DATA-01/DATA-10)

Freeze exact field order, the `|` delimiter, UTF-8 encoding, and SHA-256.
Implemented byte-for-byte in `scripts/discovery_ids.py`, pinned by the
golden test in `tests/test_discovery_ids.py`.

- **`claim_id(page_id, work_id)`** — over the STABLE canonical key
  `"discovery_claim_v1|{page_id}|{work_id}"`. **NOT** a function of
  `claim_type` — `claim_type` is a stored DERIVED attribute that can flip
  when evidence is added (e.g. a new dominant witness identification on the
  same page), so hashing it would break `claim_id` stability (G5). Backed by
  the `discovery_claim` PK `(page_id, work_id)` uniqueness constraint.
- **`evidence_id(work_id, a_page_id, sys_id, evidence_kind, evidence_source, confidence_band, span_start, span_end, other_page_id, seed_spans)`**
  — over the frozen tuple `"discovery_evidence_v1|{work_id}|{a_page_id}|{sys_id}|{evidence_kind}|{evidence_source}|{confidence_band}|{span_start}|{span_end}|{other_page_id or ''}|{seed_spans_digest}"`,
  where `seed_spans_digest` is a deterministic SHA-256 digest over the
  `seed_spans` list SORTED by `(occ0, occ1)` (each element serialized as
  `occ0:occ1:occ_class:sorted(seed_page_ids):sorted(seed_sys_ids)`) — this
  keeps `evidence_id` collision-free within a claim ACROSS the R4
  multi-span expansion (1,912 rows carry >=2 distinct occurrences). Backed
  by `UNIQUE(claim_id, evidence_id)` on `discovery_evidence`.
- **`unit_id(member_sys_ids)`** — over `"unit|" + "|".join(sorted(member_sys_ids))`,
  sorted as STRINGS (byte/lexicographic collation) — order-invariant over
  its input membership. Backed by the `witness_units` PK.
- **`mint_work_id(...)`** — opaque work_id minting, 1:1 per raw research
  work_id, via a persisted raw->opaque crosswalk that is a CALLER
  responsibility (never embedded in the module's return value). The FROZEN
  v1 scheme is a monotonic zero-padded counter form (`"w000001"`,
  `"w000002"`, ...) — this provably never emits a raw `M:`/`J:`/`REF` token
  or a filename-shaped stem, since the function only ever sees and echoes an
  integer counter.
- **`canonical_work_id(work_id, cross_corpus_map=None)`** — DEFAULTS to
  `work_id` (F16); cross-corpus canonical merges are a gen-2 versioned
  REBUILD, never a v1 migration (D-03b).

---

## 3. Frozen `claim_type` Routing (C-2)

The old "textual-parallel" / "direct-text-overlap" claim_type codes are DROPPED;
`flank_class` no longer maps to a `claim_type` at all (that MS-MS/Track-2
framing is superseded — Track 2's `accepted_pairs_canonmask` is an
ATLAS-layer graph input, not a claim-model input, per the frozen OQ2).

- **Witness evidence** (`evidence_source` = `track1_direct` OR
  `propagated`): `claim_type` is resolved via `claim_type_for_work_witness`
  — the largest-span-dominates rule, keyed on relative span dominance
  within a `page_id`: the largest matched span among ALL witness evidence
  on that page -> `direct_witness`; a smaller embedded span (a different
  work's identification dominates the page) -> `quotes_this_work`. A page
  with exactly one live witness identification always yields
  `direct_witness` (no competing span). For `tier_a` the span is the
  largest `spans_json` pair (see §4); for E1 rows (expert_verified /
  screening_rb / screening_canon), the `(o0, o1)` offset pair; for
  propagated rows, the primary a-side `seeds[].occ` span (§1.3/§4).
- **shared_text evidence**: contributes `claim_type=shared_text`.
- **Parent-claim collision resolver (`resolve_claim_type`, F7):** a
  `(page_id, work_id)` claim's `claim_type` is the WITNESS rule
  (`direct_witness`/`quotes_this_work`) whenever the claim carries ANY
  `track1_direct` OR `propagated` WITNESS evidence row; `claim_type =
  shared_text` ONLY when the claim has `shared_text` evidence and NO
  witness evidence. Both evidence rows persist on the claim regardless of
  which one wins the parent `claim_type`. This resolver is TOTAL over every
  input combination (witness-only / shared-text-only / mixed).
- **Release-verifier cross-check (134-03, G9):** claim_type validation keys
  on the COMPLETE CHILD `evidence_kind` set, not just the stored
  `claim_type` column: a `claim_type=shared_text` parent carrying ANY
  witness `evidence_kind` row is INVALID; a witness `claim_type`
  (`direct_witness`/`quotes_this_work`) requires >=1 witness
  `evidence_kind` row.

---

## 4. Per-`evidence_source` Confidence Bands — Ground-Truth Source Map (C-4)

### 4.1 `track1_direct` — four DISJOINT populations (verified zero pairwise
`(page_id, work_id)` intersection; band assignment is BY-SOURCE, no
within-track1_direct fall-through — the global band-rank in §6 still
governs cross-source DISPLAY precedence):

| Band | Source artifact | Rows | Join key | OUR-side offsets |
|---|---|---|---|---|
| `expert_verified` | `e1_ra_confirmed.jsonl` (band-evaluated single-expert population; `adjudication_status=unreviewed`) | 1,570 | `(page_id, sys_id, work_id)` | `(o0, o1)` |
| `expert_verified` | `e1_adjudicated_a.jsonl` (the ONLY individually-adjudicated cards; `adjudication_status=human_confirmed`; DISJOINT from `e1_ra_confirmed.jsonl`, verified zero key intersection) | 174 | `(page_id, sys_id, work_id)` | `(o0, o1)` |
| `tier_a` | `track1_matches` WHERE `shadowed_by IS NULL` | 275,894 rows / 198,238 pages / 52,497 sys_id / 4,093 work_id | `(page_id, sys_id, work_id)` | the LARGEST span in `spans_json` — a JSON list of `[start, end, density]` TRIPLES (R7); selection compares elements `[0]` and `[1]` (`end - start`), ignoring the density element `[2]`; `track1_matches` has NO `alen`/`offsets` columns |
| `screening_rb` | `e1_rb_screening.jsonl` | 7,498 | `(page_id, sys_id, work_id)` | `(o0, o1)` |
| `screening_canon` | `e1_r3_frame.jsonl` (ALL rows `quilt_flag==0`; D-10 canon caveat: closed at screening, ships as leads) | 9,996 | `(page_id, sys_id, work_id)` | `(o0, o1)` |

Both `expert_verified` source populations band as `expert_verified` AND both
carry `audit_status=audit_pending` — **band != adjudication** (R6, C-3
orthogonal-columns discipline). Only the 174 individually-adjudicated
`e1_adjudicated_a.jsonl` rows ever carry `adjudication_status=human_confirmed`
in v1.

### 4.2 `propagated` (witness family — C-1/C-4/R1/R3/R4)

- **`corroborated`** <- `q2_witness_collection.jsonl` (4,367 router-cleaned
  rows: all `_bucket=='witness'`, `is_new==True`, `impurity==False`). The
  LITERAL `corroborated_predicate`:
  ```
  row['_bucket'] == 'witness'
    AND row.get('is_new')
    AND NOT row.get('impurity')
    AND ('trials' in row AND row['trials'] >= 2)
  ```
  — the two-seed frame, ~2,336 rows. `impurity == (runner_up >= 0.5*support
  AND support > 0)`; "no runner-up-conflict" is the SAME test (non-impure),
  NOT `runner_up==0`. Key: `cpage` (OUR `page_id`) + `csys` (OUR `sys_id`).
- **`weak`** <- the router-cleaned witness rows LACKING `trials` (carry
  `rung` in `{A, B2}`; one-seed; ~2,031 rows). Provisional, no shipped
  precision number of its own.
- **R1 (precision attribution):** the held-out **0.926 [0.875, 0.968]** is a
  **COLLECTION-level** number over the WHOLE router-cleaned propagated
  witness collection (corroborated UNION weak; 200-card draw = 90
  corroborated + 110 weak, determinate 176/190), **NOT** a corroborated-only
  interval. `corroborated` ranks above `weak` STRUCTURALLY (two-seed vs
  one-seed evidence strength) but NEITHER band carries a separate manufactured
  precision interval — see `band_precision` §1.6.
- **R4 (multi-occurrence provenance):** 1,912 of the 4,367 rows carry
  MULTIPLE distinct candidate-side spans (up to 14). OUR-side PRIMARY a-side
  span = the largest `seeds[].occ0/occ1` into `cpage` (tie-break:
  `(occ1-occ0)` DESC, then `occ0` ASC, then `occ1` ASC); a structured
  `seed_spans` list — ONE element per DISTINCT `(occ0, occ1)` occurrence —
  `{occ0, occ1, occ_class, seed_page_ids, seed_sys_ids}` — carries every
  distinct occurrence (raw seeds up to 32 collapse by `(occ0,occ1)` to <=14
  distinct; `occ_class` is unambiguous per occurrence). `track1_evidence_id`
  is DROPPED — ABSENT from the source seed object, not deterministically
  derivable. `seed_ms_ids` = the distinct `seeds[].seed_sys`/`seed_page`
  (OUR-side) identifiers.
- **Family routers (R3, NON-witness — `corroborated_predicate` NEVER run on
  these):** `q2_collection_tafsir_targum.jsonl` (106 rows, `_bucket ==
  'tafsir_targum'`, 18 with `trials>=2`) + `q2_collection_with_arabic.jsonl`
  (108 rows, `_bucket == 'with_arabic'`, 57 with `trials>=2`). Ingested as
  `evidence_kind=shared_text`, `confidence_band=not_evaluated`,
  `routing_status=review_only`, `routing_reason=co_citation` — a co-citation
  signal, never a witness band, regardless of `trials` count.

### 4.3 `shared_text` (`evidence_source=propagated`, `confidence_band=not_evaluated`)

<- `q2_shared_text.jsonl` (60,156 rows). Key `cpage` (OUR `page_id`) +
`csys`. **ACTUAL attribute fields ONLY:** `tier` (T1 >=250 / T2 100-250 /
T3 40-100 by aligned span), `aligned_len`, `occ_class`, `n_seed_ms`,
`cross_language`, `is_new`, and `cat` -> `source_corpus`. A-side offsets =
`occ0`/`occ1` into `cpage`; b-side = `other_page_id` = `seed_page` (a page
id ONLY — the collection carries NO b-side offset span, so `b_start`/`b_end`
are NULLABLE/ABSENT on shared_text rows). `shared_text` carries **NO**
`flank_class`/`ge3`/`cluster_size`/`router_bucket`/`rung` — those attributes
exist ONLY on the propagated WITNESS family (§4.2), never on shared_text; do
NOT assert them here. `is_new==False` for 43,046 rows — those rows are ALSO
a live track1 identification for the same `(cpage, work_id)`, so they
COLLIDE with a `track1_direct` claim on that key: ONE `discovery_claim`, TWO
`discovery_evidence` rows (F7 — see §5).

---

## 5. Evidence-Row-Combination Invariants

**DROPS** the historical "exactly one distinct band per claim key"
invariant (it contradicted C-5 — a claim legitimately carries multiple
evidence rows with DIFFERENT bands). REPLACED with invariants keyed on
`evidence_kind` (R2 — not the parent `claim_type`, which is what lets a
witness + shared_text collision validate cleanly):

1. **Valid `(evidence_kind, evidence_source, confidence_band)` combinations:**
   - `witness` x `track1_direct` x `{expert_verified, tier_a, screening_rb, screening_canon}`
   - `witness` x `propagated` x `{corroborated, weak}`
   - `shared_text` x `propagated` x `{not_evaluated}`
   A single `(page_id, work_id)` claim MAY carry BOTH a `witness` evidence
   row AND a `shared_text`/`not_evaluated` evidence row (the 43,046-row
   collision, §4.3); the parent `claim_type` is resolved by
   `resolve_claim_type` (§3) INDEPENDENTLY of any single row's `evidence_kind`.
2. **No duplicate evidence keys on a claim** — `UNIQUE(claim_id, evidence_id)`.
3. **Every claim has >=1 evidence row** (nonempty).
4. **Exactly ONE deterministic `display_evidence_id` per claim**, belonging
   to that SAME claim (F12 composite ownership, §1.3).

---

## 6. Display-Evidence Precedence Lattice (`display_evidence_id` selection, C-5)

`select_display_evidence(evidence_rows)` is a TOTAL, deterministic selector
over EVERY `(evidence_source, confidence_band, adjudication_status)`
combination. Sort key, in priority order:

1. **Family-specific human_confirmed dominance:** any `track1_direct` row
   with `adjudication_status=human_confirmed` outranks EVERY
   non-`(human_confirmed, track1_direct)` row — holds across ALL four
   track1_direct bands (the human_confirmed elevation is FAMILY-specific
   AND adjudication-specific; only individually-adjudicated rows carry
   `human_confirmed` — in v1 only the 174 `e1_adjudicated_a.jsonl`
   `expert_verified` rows ever reach this cell, R6). The
   propagated/shared_text human_confirmed cells are unreachable-but-defined
   for totality.
2. **Global band-rank** over `(evidence_source, confidence_band)`, strongest
   first:
   ```
   1. track1_direct / expert_verified
   2. track1_direct / tier_a
   3. propagated    / corroborated
   4. track1_direct / screening_rb
   5. track1_direct / screening_canon
   6. propagated    / weak
   7. propagated    / not_evaluated
   ```
3. **`adjudication_status`** tie-break: `human_confirmed` < `provisional` <
   `unreviewed`.
4. **`evidence_id`** lexicographic tie-break (deterministic final
   disambiguation).

This EXPLICITLY resolves:
- `track1_direct tier_a` OUTRANKS `propagated corroborated`.
- `track1_direct expert_verified` OUTRANKS `propagated corroborated` EVEN
  WHEN `unreviewed` (the 1,570-row `e1_ra_confirmed.jsonl` population, R6).
- `propagated corroborated` OUTRANKS BOTH `track1_direct` screening bands
  (`screening_rb`, `screening_canon`).
- `not_evaluated` (shared_text) is the LOWEST band-rank — never chosen as
  `display_evidence_id` when any witness evidence co-exists on the claim.

Both evidence rows always persist regardless of which one is selected for
display — no band-suppression, no manufactured cross-source scalar.

---

## 7. `adjudication_status` / `audit_status` / Routing Matrices (F9/F10, C-6)

| Band / population | `adjudication_status` | `audit_status` |
|---|---|---|
| `expert_verified` <- `e1_adjudicated_a.jsonl` (174) | `human_confirmed` | `audit_pending` |
| `expert_verified` <- `e1_ra_confirmed.jsonl` (1,570) | `unreviewed` | `audit_pending` |
| `tier_a` | `unreviewed` | `n/a` |
| `screening_rb` | `provisional` | `n/a` |
| `screening_canon` | `provisional` | `n/a` (+ D-10 canon caveat) |
| `corroborated` | `unreviewed` | `audit_pending` |
| `weak` | `provisional` | `n/a` |
| `not_evaluated` | `unreviewed` | `n/a` |

No band carries `audit_status=audit_passed` at launch — registry-gated
(C-6); the word **"certified" is PROHIBITED** in code/UI until the
independent-audit gate (kappa >= 0.60) passes.

**Routing (F9/R3):** all four `track1_direct` bands + `propagated`
`corroborated` + `propagated` `weak` -> `routing_status=shipped`,
`routing_reason=none`. The family-router collections (`tafsir_targum`,
`with_arabic`) are NON-witness buckets — `corroborated_predicate` is NEVER
run on them (R3) — ingested as `evidence_kind=shared_text` /
`confidence_band=not_evaluated` with `routing_status=review_only`,
`routing_reason=co_citation`. `review_only` rows PERSIST in the sidecar
(queryable) and are excluded from the default shipped surface only by
`routing_status`, never deleted. `impurity`/`runner_up_conflict` routing
reasons are RESERVED (unused at launch — the shipped collections are all
router-cleaned non-impure).

---

## 8. `source_corpus` Codes, Source-Extensibility, Reserved Future Pathway

`source_corpus in {sefaria, ja, msource}` — masked codes; internal-only;
NEVER displayed (D-03a). The sidecar is built source-extensible (D-03): a
gen-2 refresh (adding R-source + the deferred M-source piyyut/documentary
works) is a **versioned REBUILD, never a schema migration** — the masked
`source_corpus` field and the `canonical_work_id` cross-corpus dedup key
(§1.1/§2) are both already in place for this. R-source name/aliases/sigla
are pre-registered in the DATA-05 masking pattern set now (defense in
depth) even though R-source text enters no launch surface.

**C-9 (reserved future pathway, document only, NO v1 data):** a THIRD
work-ID mechanism reserves a future `evidence_source = catalog_propagated`
— catalog/title/FGP-identity propagation over MS-to-MS connections, using a
Track-2 connection plus an external NLI/FJMS/FGP identity on a connected
fragment to identify works NOT in the reference corpus. This would be added
via a versioned REBUILD per D-03, never a v1 migration; catalog-identity
works are a new work SOURCE, distinct from OQ2's excluded
clustering-invented works. Candidate future spike/phase (135-139).

---

## 9. Retained Resolved Design Questions (OQ2, OQ3 — FROZEN defaults, unchanged)

### OQ2 — Shown-set derivation: reference-catalogue works only (FROZEN default)

**Decision (FROZEN):** the launch shown-set's *works* come exclusively from
the reference-catalogue identification tables — `track1_matches` /
`track1_candidates` / `work_query_hits_fullv2` — never from unsupervised
clustering (Louvain / connected-components) over `accepted_pairs_canonmask`.
Rationale (RESEARCH.md Landmine 8 / Assumption A3): the launch shown-set is
Sefaria + JA + M-source-literary (134-CONTEXT D-05), and every one of those
reference works already has a `work_id`/page-identification record in
Track 1 — there is no need to *invent* works via graph clustering.
Clustering (Louvain / force-layout) is an ATLAS-layout concern (Phase 133),
not a claim-model concern, and is explicitly out of scope for the discovery
spine.

**Owner-confirmable flag:** if a future gen-2 build wants discovery-only
works (works with NO reference-catalogue identification, discovered purely
from MS-MS connectivity), that is a new, separately-versioned frame — never
a retrofit of the v1 frame (mirrors D-03(d)).

### OQ3 — HTR snapshot-hash granularity (FROZEN default)

**Decision (FROZEN):** the release contract carries ONE corpus-level
`htr_snapshot_hash` in `meta` (the release-contract-level "did the
underlying HTR corpus change since this build" signal) **PLUS** a per-page
hash-or-char-count on every evidence row, for page-scoped drift detection at
render time (Phase 136):

- `discovery_evidence.snapshot_hash` carries a per-page snapshot hash (or
  `htr_n_chars` equivalent) alongside `text_layer` for the `a_page_id` side.
- `discovery_evidence.snapshot_hash_b` carries the SAME thing for
  `other_page_id` (the shared_text b-side) — a shared_text row spans two
  pages, so drift on EITHER side must be independently detectable (DATA-03
  fail-closed-on-drift covers both sides).

This is the cheapest sufficient granularity: a single corpus-wide hash lets
the release contract detect "the whole corpus text changed, do not trust
these offsets," while the per-page value lets a render-time check reject
ONLY the specific page(s) that drifted, without invalidating the whole
sidecar.

---

## 10. DATA-01(b) Mapping Note

DATA-01(b)'s originally-scoped "MS-MS relation claims" family is REPLACED
under this model by `shared_text` claims (`claim_type=shared_text`,
`evidence_source=propagated`, `confidence_band=not_evaluated`) — which, like
EVERY `discovery_claim`, still require a `works.work_id` FK (a catalogued
shown-work anchor). MS-MS-only connections with NO catalogued work anchor
are RESIDUE and are EXCLUDED per C-8 + the frozen OQ2 above — they are an
ATLAS-layer graph concern (Phase 133+), never a `discovery_claim`.

---

*This document is FROZEN as of 2026-07-22 (plan 134-01, Task 1). Later
Phase 134 plans (fixture/distillation/loader/service/frame) implement
against this contract; any correction requires a new dated amendment
section here, never a silent edit.*
