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

## Amendment 2026-07-24 (Phase 135, plan 135-05 — v2 vocabulary + registry lockstep, D-17)

This dated amendment ADDS to the frozen contract above; the "Frozen Enum
Vocabularies" block, §1.5, and §1.6 are left UNTOUCHED in place (dated-amendment
discipline, never a silent edit). It establishes the v2 vocabulary + schema the
`discovery-v2` bake logic (135-06) and CERT-01 grading (135-09+) write against.
NO row is populated by 135-05 — this is DDL + vocabulary only.

1. **`routing_reason` gains `later_shared_text`.** The frozen `routing_reason`
   vocab becomes `{impurity, runner_up_conflict, co_citation, none,
   later_shared_text}` (5 members). `later_shared_text` records the D-17 coarse
   chronological demotion: the later of two co-claiming works whose text is
   shared is routed to `routing_status='review_only'` with this reason. The
   `discovery_evidence.routing_reason` DDL CHECK mirrors the frozen
   `scripts.discovery_ids.ROUTING_REASONS` frozenset exactly.

2. **`band_precision` gains five NULLABLE CERT-01 registry columns** (filled by
   the CERT-01 grading write later, never by 135-05):

   ```sql
   measurement_status TEXT CHECK (measurement_status IN
                         ('not_measured','measured_pass','measured_fail','insufficient_evidence')
                         OR measurement_status IS NULL),
   measurement_date   TEXT,
   grader             TEXT,
   audit_status       TEXT,
   report_id          TEXT
   ```

   The **CLOSED-vocab `measurement_status` CHECK (Codex #B3)** mirrors
   `shared/discovery_band_labels.MEASUREMENT_STATUSES` exactly, so a free-text
   status can never reach the D-18 default-eligibility predicate — a
   `measured_pass` that contradicts its own `ci_low` is still fail-closed at the
   predicate layer, and a status outside the closed vocab is rejected at the DB
   layer.

3. **New `discovery_routing_audit` table** (masking-safe by construction —
   opaque work ids + numeric years only; NO title, reference text, or raw id):

   ```sql
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
   ```

   The v2 bake (135-06) WRITES one row per D-17 routing decision; `routing_reason`
   is a plain annotation column here (the constrained routing_reason enum lives
   on `discovery_evidence`).

4. **New `meta` provenance keys written at the v2 bake** (Codex #B2/#5), added to
   the §1.5 release-contract key set for the v2 asset: `canonical_merges_sha256`
   (content hash of the twin/canonical-merge seed), `composition_dates_sha256`
   (Sefaria composition-date input), and `seftja_dates_sha256` (the SEF/JA date
   swap-in input). These pin the D-17 chronology + canonical-merge inputs so a
   rebuild is reproducible and its provenance is auditable.

5. **v2 band rename — v1-read-compat.** The stored track1_direct top band adds
   the v2 key `high_confidence_algorithmic`; the v1 key `expert_verified` is
   RETAINED through the transition (the live v1 asset + the v1 fixture tests read
   it) and is dropped only once the v2 manifest is live (135-08). See
   `docs/specs/discovery-band-labels-v1.md` §5 (asset/bake-level atomicity) and §2.

---

## Amendment 2026-08-02 (Phase 136)

This dated amendment ADDS to the frozen contract above (the "Frozen Enum Vocabularies" block, §1-§9,
and the 2026-07-24 amendment are left UNTOUCHED in place — dated-amendment discipline, never a silent
edit). It defines, as contract, EVERY field and table the ONE authorized Phase-136 rebuild adds.
**Nothing outside this list is authorized to appear in the asset.** `scripts/build_discovery_sidecar.py`
and `scripts/verify_discovery_sidecar.py` implement against this section; plans 136-05/136-06/136-11/
136-12 consume it.

### (A) `discovery_evidence` / `discovery_claim` additions

- **`coverage_ppm`** (`discovery_evidence`, INTEGER, indexed) — an indexed fixed-point integer:
  `round(matched_letters / page_norm_letters * 1_000_000)`, DIRECT FAMILY ONLY
  (`evidence_source='track1_direct'`). Propagated rows carry NO coverage value — `coverage_ppm IS
  NULL` for every `evidence_source='propagated'` row (all shipped propagated evidence rows have NULL
  `matched_letters` today). The number is MATCHED-LETTER coverage, never bare "of page" — display
  wording must say so explicitly. A companion **`coverage_status`** TEXT validity enum (`{measured,
  no_denominator, not_applicable}` — `not_applicable` for propagated rows, `no_denominator` when
  `page_norm_letters` is zero/missing for a direct row) records why a value is or is not present, so an
  absent `coverage_ppm` is never misread as zero coverage.
- **`band_rank`** (`discovery_evidence`, INTEGER, indexed) — a MATERIALIZED integer sort key mirroring
  the existing §6 global band-rank lattice (`track1_direct`/`expert_verified` = 1 … `propagated`/
  `not_evaluated` = 7), computed once at build time so the findings page and the panel never recompute
  the CASE-based rank at query time (D-10a).
- **`novelty_status`** (`discovery_evidence`, TEXT, indexed on the status, not on a boolean) —
  **⟨AMENDED 2026-08-02, owner rulings E/E′/F/G/H — `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-GATE1-DECISIONS.md`
  §§ E, E′, F, G, H — supersedes the tri-state this amendment originally recorded, which was never
  propagated past this dated block; `.planning/REQUIREMENTS.md`'s NOVEL-01 amendment trail carries the
  full rationale for each widening step and is the amendment-by-amendment record; `docs/specs/discovery-novelty-v1.md`
  (plan 136-04) is the CANONICAL, single restatement of this list going forward — cite that file rather
  than re-deriving the list from this schema doc's own prose.⟩** a TEN-VALUE CLOSED vocabulary:
  `confirms` / `refines_granularity` / `aid_more_specific` / `diverges_work` / `diverges_part` /
  `container_predicts` / `fills_gap` / `extends` / `alias_merge` / `not_checked`, **DEFAULTING to
  `not_checked`** (fail-closed, unchanged in meaning across every widening). `fills_gap` is the SOLE
  value the public "Candidates for new finds" predicate selects. `diverges_work`/`diverges_part` carry a
  default-hidden, explicit-warned-toggle display posture (ruling F); `container_predicts` does NOT —
  ruling H is explicit that F's default-hidden rationale (rows the owner has measured reason to believe
  are OUR false positives) does not apply to a container relationship, where there is no disagreement to
  warn about. Computed for ALL evidence families (`track1_direct` AND `propagated`) — this is the
  coverage-gap fix the frozen v2 asset left open (all 254,612 `track1_direct` rows shipped at
  `is_new = 0`, meaning UNCHECKED, not "known"). This is the field a read path filters/groups on; the
  legacy `is_new` boolean stays in the schema unmodified for read-compat but is no longer the query
  target.
- **`novelty_source_label`** (`discovery_evidence`, TEXT, nullable) — populated when `novelty_status` is
  any shade where SOME finding aid says something nameable about this fragment-work pair (`confirms` /
  `refines_granularity` / `aid_more_specific` / `alias_merge` / `extends` / `diverges_work` /
  `diverges_part` / `container_predicts`); values are the MASKED label set ONLY — name the source where
  nameable (e.g. "recorded in the FJMS catalogue"), otherwise the fixed fallback "recorded in another
  reference source". `NULL` on `fills_gap` (nothing to name) and `not_checked` (nothing was checked).
  The raw provenance value (which finding aid, which restricted corpus) is NEVER stored in the asset —
  masked at build time, before this column is written (NOVEL-02).
- **`divergence_correctness`** (`discovery_evidence`, TEXT, nullable) — **⟨ADDED 2026-08-02, owner ruling
  F — `136-GATE1-DECISIONS.md` § F⟩** a SEPARATE, sibling closed vocabulary — `catalogue_correct` /
  `claim_correct` / `unclear` — recording which side is right on a divergence row. Populated
  IF-AND-ONLY-IF `novelty_status IN ('diverges_work', 'diverges_part')`; `NULL` for every other shade (a
  `confirms`/`fills_gap`/etc. row has no divergence to adjudicate correctness on). NOT part of the
  `novelty_status` enum — correctness is orthogonal to shade because the owner's own review of real
  divergence cases found BOTH directions occur under the identical shade token, so one column cannot
  carry both meanings.
- **`assertion_visibility`** and **`identity_visibility`** (`discovery_evidence` / `works`
  respectively, TEXT, closed `{public, private}` enums) — the VIS-01 two-axis derivation (D-22):
  `assertion_visibility` is derived from the raw evidence origin (per evidence row),
  `identity_visibility` from the displayed work's origin (per work). **Public eligibility requires
  BOTH to be `public`.** Neither axis is a proxy for the other — `works.source_corpus` alone is
  insufficient (a restricted-corpus id prefix maps to both restricted-identity AND open-identity works
  in the live asset — 656 restricted-identity works AND 235 open (Sefaria) ones).

### (B) New tables

```sql
CREATE TABLE discovery_identification (
  identification_id   TEXT PRIMARY KEY,   -- deterministic content key — see the ID recipe below
  sys_id              TEXT NOT NULL,
  canonical_work_id   TEXT NOT NULL,
  display_work_id     TEXT NOT NULL REFERENCES works(work_id),  -- see (B1) — NEVER canonical_work_id
  main_pool           INTEGER NOT NULL,   -- boolean (0/1)
  main_pool_reason    TEXT NOT NULL CHECK (main_pool_reason IN (
                         'shared_wording','overlapping_tie','low_coverage',
                         'insufficient_length','missing_signal',
                         'main_multifolio','main_full_coverage','main_human_confirmed')),
  best_band_rank      INTEGER NOT NULL,
  page_count          INTEGER NOT NULL,
  max_coverage_ppm    INTEGER,            -- NULL when no direct-family evidence contributes
  relation_kind       TEXT NOT NULL,      -- the display relation ("direct match"/"partial match"/"shared text") basis
  -- ⟨AMENDED 2026-08-02, owner rulings E/E′/F/G/H — 136-GATE1-DECISIONS.md §§ E-H⟩ widened from the
  -- original three-value tri-state to the current TEN-value shade enum. This CHECK is one of the two
  -- places (the other is the mirrored discovery_evidence.novelty_status column, § A above) where the
  -- vocabulary is UNAVOIDABLY restated as a SQL literal rather than cited by reference — SQLite CHECK
  -- constraints cannot import a shared constant. `docs/specs/discovery-novelty-v1.md` (plan 136-04) is
  -- the CANONICAL prose statement of this list; if this literal and that file's list ever disagree, that
  -- is a build/verifier bug, and `shared/discovery_novelty.py::NOVELTY_STATUSES` is the tie-breaker
  -- (a test there asserts equality against this schema's vocabulary).
  novelty_status      TEXT NOT NULL CHECK (novelty_status IN
                         ('confirms','refines_granularity','aid_more_specific','diverges_work',
                          'diverges_part','container_predicts','fills_gap','extends','alias_merge',
                          'not_checked')),
  -- ⟨ADDED 2026-08-02, owner ruling F — 136-GATE1-DECISIONS.md § F⟩ a SEPARATE sibling column, never
  -- part of the novelty_status enum above (correctness is orthogonal to shade). NULL required outside
  -- the two divergence shades; the CHECK enforces that direction. Populated only on divergence rows,
  -- drawn from its own three-value vocabulary when non-NULL.
  divergence_correctness TEXT CHECK (
                         (novelty_status IN ('diverges_work','diverges_part')
                          AND divergence_correctness IN ('catalogue_correct','claim_correct','unclear'))
                         OR
                         (novelty_status NOT IN ('diverges_work','diverges_part')
                          AND divergence_correctness IS NULL)
                       ),
  assertion_visibility TEXT NOT NULL CHECK (assertion_visibility IN ('public','private')),
  identity_visibility  TEXT NOT NULL CHECK (identity_visibility IN ('public','private')),
  UNIQUE (sys_id, canonical_work_id)
);
CREATE TABLE manuscript_display (
  sys_id              TEXT PRIMARY KEY,
  library_code        TEXT NOT NULL,
  library_sort_key    TEXT NOT NULL,
  shelfmark_display   TEXT NOT NULL,
  shelfmark_sort_key  TEXT NOT NULL
);
```

`discovery_identification` is one row per `(sys_id, canonical_work_id)` — the identification grain the
main-pool bucket rule (`.claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md`) and
the findings page both operate on. `identification_id` is a deterministic content key, recipe frozen in
the same style as §2's existing ID recipes: SHA-256 over
`"discovery_identification_v1|{sys_id}|{canonical_work_id}"`, UTF-8 encoded, backed by the
`UNIQUE(sys_id, canonical_work_id)` constraint above. `main_pool_reason` is a CLOSED vocabulary — five
reasons that send an identification to "more matches" (mirroring the four main-pool-rule gates plus the
shared-wording no-same-work-claim case) and three AFFIRMATIVE reasons recording why an identification
landed in the main pool (`main_multifolio`, `main_full_coverage`, `main_human_confirmed`) — a value
outside this list is a build error, never a silent default. `manuscript_display` is sourced ONLY from
`libraries.csv` (masking-safe catalogue metadata — the same source the existing panel/browse surfaces
already read) and carries NO work title, NO reference text, and NO locus; it exists purely so the
findings page and panel can sort/display library + shelfmark without a per-row `libraries.csv` lookup
at request time.

#### (B1) `display_work_id` — the canonical-identity grain is NOT unambiguous

Measured on the live asset: `works` contains **15 duplicated `canonical_work_id` groups**, three of
them carrying DIFFERENT titles AND mixed source corpora. Joining the 64,509-row identification grain
to `works` on `canonical_work_id` therefore yields **65,587 rows** — a FAN-OUT, not a lookup. Left
unaddressed, title selection and `identity_visibility` are undefined for those groups, and a PRIVATE
work in a duplicated group could influence what looks like a shared public aggregate.

`display_work_id` is the deterministic REPRESENTATIVE of the canonical group, selected by an ORDERED,
TOTAL rule — never "whichever row the join returns":

1. Prefer the row whose `work_id == canonical_work_id` (the canonical anchor, if it is itself a
   member of its own group).
2. Else prefer the row with the LOWEST `source_corpus` in the fixed order `sefaria < ja < msource`
   (public-before-private, so a mixed-visibility group's representative is public whenever a public
   member exists).
3. Else (a tie within the same `source_corpus`) the lexicographically SMALLEST `work_id`.

This selection is deterministic and total over every duplicated group. Every identity join — title,
author, `identity_visibility` — reads `display_work_id`, NEVER `canonical_work_id`; the join from
`discovery_identification` to `works` is REQUIRED to be exactly 1:1 (`COUNT(*)` after the join must
equal the identification row count — a release-verifier check). The public projection RECOMPUTES each
identification's `main_pool`/`best_band_rank`/`novelty_status`/etc. from its OWN surviving public
claims rather than copying the private row's values, so a private contribution can never survive into
the public asset as part of a shared aggregate.

### (C) `works.genre` — an EXISTING column, populated and constrained

`genre` ALREADY EXISTS on `works` (§1.1 above) and is NULL on all 1,269 rows today; this amendment does
**NOT** add it — no schema migration that introduces a new `genre` column (an `ALTER TABLE` DDL
statement targeting this field) may ever be emitted, because the column already exists. The change this
rebuild makes is that the column BECOMES populated from a curated, hash-pinned artifact (the ~1,088-work
one-time domain-curation pass) and CONSTRAINED to the FJMS closed domain vocabulary (39 parents / 202
leaves, bilingual) or to an explicit `Unassigned` value — never silently NULL-as-absent. Assignment is
at the CANONICAL work level (via `display_work_id`, so a duplicate is never assigned twice), and a
value outside the closed vocabulary is a BUILD ERROR, never a new ad hoc domain. The same curation pass
also produces the author alias map referenced in plan 136-09.

### (C1) `meta.audience`

Add **`meta.audience`** — a closed enum, `public` | `private`, written by the private build (`private`)
and by the public-projection step (`public`). This is the field the RUNTIME LOADER gates on so a public
route can never resolve a private artifact by accident — without it the public/private exclusion is
procedural (a code-review discipline) rather than STRUCTURAL (a fact the loader itself can check). Add
the release-contract count meta keys for the two new tables alongside it, following the existing §1.5
convention: `expected_rows_discovery_identification`, `expected_rows_manuscript_display` — the startup
readiness contract validates counts from `meta`, and these two new tables need the same validation as
`expected_rows_claims`/`expected_rows_evidence`/`expected_rows_works`/`expected_rows_units`.

### (D) Index set (D-10a)

- A composite ordering index over `discovery_identification(main_pool, best_band_rank,
  max_coverage_ppm)` — the findings-page default sort (main pool first, tier-first within it).
- Lookup indexes on `discovery_identification(canonical_work_id)` and
  `discovery_identification(sys_id)`.
- An index on `manuscript_display(library_sort_key, shelfmark_sort_key)` — the deterministic
  library-then-shelfmark sort the work page and the findings page both need.
- A UNIQUE index on `discovery_claim(display_evidence_id)` — D-10a's measured findings-query fix.
- An index on `discovery_evidence(novelty_status)` — the STATUS column, replacing the legacy `is_new`
  boolean as the indexed filter target (the boolean stays in the schema for read-compat, unindexed as a
  filter basis).

### (E) The narrow §1.6 `tier_a` amendment (D-02a)

Amend the FROZEN `band_precision` row-set (§1.6 above, left otherwise untouched) to permit — for the
`tier_a` band ONLY (`scope='band'`, `evidence_source='track1_direct'`, `confidence_band='tier_a'`) —
`measurement_status = 'measured_pass'` and `ci_low = 0.9084` (the CERT-01 measured lower confidence
bound), while **`precision` STAYS NULL**. No other row in the frozen §1.6 set changes.

**Reasoning, stated in the contract:** this stores the AUTHORIZATION that `is_default_eligible()` reads
(`measurement_status == 'measured_pass' AND ci_low is not None AND ci_low >= STRICT_FLOOR`) — NOT an
estimate. `tier_a` becomes default-visible because its certificate PASSED, while the asset still stores
zero numeric precision for it, which is exactly what keeps the asset consistent with the D-06
no-numbers posture: the band goes default-shown on the strength of a pass/fail authorization, never on
the strength of a displayed number.

**Lockstep sites (enumerate — all six move together, one bake, per the discipline in §5 above):**

1. This §1.6 row-set (the frozen contract, amended here).
2. `scripts/build_discovery_sidecar.py::_frozen_real_band_precision_rows` — the ONE source of truth
   the builder reads (must emit the same `measurement_status`/`ci_low` for the `tier_a` row).
3. `scripts/build_discovery_sidecar.py::_validate_precision_spec` — the cross-check gate for any
   explicit `--precision-spec` (must accept the amended row and continue to reject a non-NULL `tier_a`
   `precision`).
4. `scripts/verify_discovery_sidecar.py`'s release-strict tier-A check (M4) — must be updated to
   accept `measurement_status='measured_pass'`/`ci_low=0.9084` on `tier_a` while continuing to REJECT
   any non-NULL `tier_a` `precision`.
5. The `band_precision` INSERT column list (`scripts/build_discovery_sidecar.py`, the
   `measurement_status` column already exists per the 2026-07-24 amendment above) — must carry the
   amended `tier_a` values through to the built row.
6. The schema/builder/verifier tests — REQUIRE fixtures proving BOTH branches: a `tier_a` row at
   `measurement_status='measured_pass'`/`ci_low=0.9084` PASSES verification and reads
   default-eligible=True; a `tier_a` row with any non-NULL `precision`, OR a `ci_low` below
   `STRICT_FLOOR` (0.85), OR a `measurement_status` other than `measured_pass` FAILS verification /
   reads default-eligible=False.

### (F) `discovery_routing_audit` — `kept_tie` rows must carry `demoted_work_id`

The 2026-07-24-amendment `discovery_routing_audit` table (§ above) is amended: every row with
`decision='kept_tie'` MUST carry a non-NULL `demoted_work_id` — a NULL `demoted_work_id` on a
`kept_tie` decision makes the tie pair unreconstructable from the audit table alone (there is no way to
tell which two works were tied). The build must populate `demoted_work_id` for every `kept_tie` row it
writes; the release verifier gains a check rejecting any `kept_tie` row with a NULL `demoted_work_id`.

### (G) A standing schema rule — every offset column names its coordinate space

**Every offset column in this schema names the coordinate space it indexes, at the point of
definition.** This is stated here as a STANDING RULE, not a one-off note, because the same trap has
already been found twice (the manuscript side in Phase 136, the work side deferred to
discovery-v2.1). Applied RETROACTIVELY to the existing §1.3 columns: **`span_start`/`span_end` index
the NORMALIZED Hebrew-letter stream** (the `norm_stream_letter_count`/`compute_page_coverage` space in
`scripts/build_discovery_sidecar.py`), **NOT raw page text** — slicing raw text at these offsets lands
in the wrong place (652 characters off on the sampled case). Any future offset column (e.g. a
discovery-v2.1 `w_start`/`w_end` on the work side) MUST state its coordinate space in the same
sentence that defines it.

### (H) Explicitly OUT of this rebuild

`w_start`/`w_end` (work-side match offsets) and the Sefaria versemap reference-resolution stage are
DEFERRED to discovery-v2.1 by owner decision 2026-08-02 (`136-CONTEXT.md` RE-SCOPE block) — they serve
only the reference-side locus and the side-by-side evidence view, both moved to Phase 136.1/v2.1, and
they carried the build's hardest work (the `body` ↔ `norm_stream` coordinate mapping). **No field for
either may appear in this asset.** A schema reviewer finding a `w_start`/`w_end`-shaped column, or a
versemap-derived reference field, on the `discovery_evidence`/`discovery_claim`/`works` tables in this
rebuild has found a build error.

---

*This document is FROZEN as of 2026-07-22 (plan 134-01, Task 1). Later
Phase 134 plans (fixture/distillation/loader/service/frame) implement
against this contract; any correction requires a new dated amendment
section here, never a silent edit. Dated amendments: 2026-07-24 (Phase 135,
plan 135-05 — v2 vocabulary + registry lockstep); 2026-08-02 (Phase 136,
plan 136-01 — the trimmed-rebuild new-field contract: coverage_ppm,
band_rank, novelty_status/novelty_source_label, the VIS-01 visibility axes,
discovery_identification, manuscript_display, display_work_id, the
works.genre population rule, meta.audience, the D-10a index set, the narrow
§1.6 tier_a authorization, the discovery_routing_audit demoted_work_id fix,
the offset coordinate-space standing rule, and the explicit v2.1 deferral of
w_start/w_end + versemap resolution); 2026-08-02 (Phase 136, plan 136-03
continuation — novelty shade-enum reconciliation, prompted by
`136-NOVELTY-PRIOR-ART.md`'s finding that owner rulings E/E′/F/G had been
recorded in `136-GATE1-DECISIONS.md` but never propagated here: the
`novelty_status` vocabulary widens from the plan-136-01-era three-value
tri-state to the current TEN-value shade enum (`confirms` /
`refines_granularity` / `aid_more_specific` / `diverges_work` /
`diverges_part` / `container_predicts` / `fills_gap` / `extends` /
`alias_merge` / `not_checked`), in both the `discovery_evidence` prose bullet
and the `discovery_identification` CHECK constraint; a new sibling
`divergence_correctness` column is added (owner ruling F), nullable, CHECK'd
to `catalogue_correct`/`claim_correct`/`unclear` and required NULL outside
`diverges_work`/`diverges_part`. `docs/specs/discovery-novelty-v1.md`
(plan 136-04, not yet written as of this amendment) is designated the
CANONICAL single-cited prose statement of the shade enum going forward; the
SQL CHECK literals in this document are the one place a second restatement
is unavoidable, per this amendment's own inline note at the point of
definition).*
