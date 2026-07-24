# Discovery v2 Bake Plan (data-quality re-distill)

**Status:** RATIFIED DRAFT — census landed (2026-07-23, owner-ratified); this
rewrite implements the full owner decision set. Codex adversarial re-review
REQUIRED before any code change (phase-134 discipline: `build_discovery_sidecar.py`
is Codex-reviewed across multiple rounds; this rewrite gets its own re-review,
tracked in `135-BAKEPLAN-CODEX-REVIEW.md`).
**Owner-ratified decisions:** 2026-07-23 (see `.planning/STATE.md` "V2 BAKE
SEQUENCING LOCKED" + the census handoff artifact).
**Blocking input status:** RESOLVED. The complete twin census (see §2) has
landed and is owner-ratified. The only remaining gate before any v2 build code
is written (135-05+) is the Codex adversarial re-review of THIS document
(Task 2 of plan 135-04).

---

## 1. Purpose & scope

Re-distill the discovery sidecar **v1 → v2** to fix the three data-quality
defects the owner found in the v1 build (2026-07-23):

1. **Cross-corpus duplicate works** — the same work appears once per source
   corpus (M-source + Sefaria), `canonical_work_id` unpopulated.
2. **Band-label overclaim** — `expert_verified` conflated top-algorithmic-score
   with human approval (only 121 rows are `human_confirmed`; 1,067 of the 1,188
   `expert_verified` are `unreviewed`).
3. **Anthology/quotation false positives** — a folio that *quotes* a work is
   claimed as a *witness* to it.

**In scope for v2:**
- Canonical merge (soft, `canonical_work_id` only) from the full owner-ratified
  census — **16 text-confirmed merges**, **1 drop** (`w001239`), the **D-14
  canonical flip** (§2).
- **NO `work_relations` table.** The previously-proposed enumerated
  containment/relation table is dropped entirely (§2, §4.3). It is replaced by
  ONE general rule.
- The **D-17 chronological co-claim demotion rule** (§4.3) — replaces the
  killed relation table. Hardcoded `DELTA=100y`, cited to
  `chrono_date_coverage.md`.
- **THREE hash-pinned build inputs**, each with a required
  `--<name>`/`--<name>-sha256` pair, a FROZEN exact-shape validating parser,
  and recorded provenance: the census (`--canonical-merges`), the M-source
  composition dates (`--composition-dates` + a production coverage gate), and
  the SEF/JA interim dates (`--seftja-dates`).
- **Lever-1 coverage routing** (`cov < 0.45 → review_only`), re-enabled ahead
  of D-17 in the corrected order-of-operations (§6).
- The **(B) band-label enum rename** (`expert_verified →
  high_confidence_algorithmic`) plus the NEW `routing_reason` amendment
  (`later_shared_text`) as an eighth lockstep item (§5).
- The masking-safe `discovery_routing_audit` replayability table (§4.3).
- The **tested CERT-01 FAIL-branch reband** to `screening_rb` (a REAL frozen
  band key), applied as a rebuild input with an atomic legacy-precision
  invalidation (§4.5).
- New verifier gates (§7 gates 7–15).

**NOT in scope (explicitly deferred):**
- **(C) direction-aware shadow router = v2.1.** It would reclassify the
  remaining high-coverage `direct_witness → quotes_this_work` residual and
  needs a full re-instrumented Track-1 re-run + a producer-side direction
  router. Defect #3 is only *partially* addressed in v2: Lever-1 coverage
  routing catches low-coverage quotations, and D-17 catches materially-later
  co-claimants on shared spans; the direction-aware residual waits for v2.1.
- **Production deploy (134-08 Task 3)** — deploy the FINAL v2 sidecar ONCE, as
  a Phase 135 prerequisite. Do not deploy v1 then re-deploy v2.

**The spine is unchanged.** v2 runs through the SAME pipeline, schema, loader,
service, and masking guard as v1. This is a data refresh, not an architecture
change — which is why it does not re-open the Phase 134 spine deliverables
(see §9).

---

## 2. The census — RESOLVED: 16 merges, 1 drop, D-14 canonical flip, NO `work_relations` table

The canonical merge (§4.1) is driven by the complete, owner-ratified twin
census (`rsource/results/mask2_v2_cross_census*.md` / the machine-readable
`rsource/data/v2_canonical_merges.json` — v2-internal Sefaria/M/JA twins,
masking-clean, population = all launch works, cross-source pairs, witnessed
[>= 8 shared Genizah pages]). Ratified 2026-07-23 (owner, in-chat review).

**Format:** each merge entry lists one or more opaque `w000xxx` member ids
that collapse to a single canonical `w000xxx` representative, gated by an
`owner_verdict` field — ONLY `approve` rows load. All 16 delivered merges
carry `owner_verdict='approve'`.

**Full merge set (16, all `owner_verdict='approve'`):**

| # | member w-ids | canonical rep |
|---|---|---|
| 1 | w000190, w001382 | w001382 |
| 2 | w000192, w001269 | w001269 |
| 3 | w000193, w001267 | w001267 |
| 4 | w000191, w001337 | w001337 |
| 5 | w000465, w001238 | w001238 |
| 6 | w000459, w001242 | w001242 |
| 7 | w000452, w001239 | w000452 (D-14 flip — see below) |
| 8 | w000464, w001226 | w001226 |
| 9 | w000456, w001241 | w001241 |
| 10 | w000460, w001234 | w001234 |
| 11 | w000467, w001240 | w001240 |
| 12 | w000454, w001236 | w001236 |
| 13 | w000458, w001237 | w001237 |
| 14 | w000453, w001229 | w001229 |
| 15 | w000455, w001243 | w001243 |
| 16 | w000461, w001235 | w001235 |

Canonical = the Sefaria representative for every merge EXCEPT #7 (public/
citable identity; masking-aligned — the displayed work is never the
restricted-source copy).

**The drop list:** `dropped_by_135 = [w001239]` (§3 — the Sefaria copy of
merge #7, RCh Shabbat, is dropped, not merged).

**D-14 canonical flip:** merge #7 (`w000452` ↔ `w001239`) IS one of the 16
owner-approved merges, but its Sefaria member `w001239` is separately dropped
(§3) — leaving `w000452` (the M-source id) standing alone as the group's
canonical representative. This is the one merge whose canonical rep is NOT
the Sefaria side; every other merge keeps the standard Sefaria-first priority.
*(Note: "RCh-Shabbat" is the SEFARIA-side neutral canonical short name used
in this document and the census artifact — it is NOT the restricted
M-source title, so the abbreviation is masking-safe per the Codex-preflight
disposition on this point.)*

**NO work_relations table.** The v2 build does not create, populate, or
read any enumerated relation table. Containment / embedding / abridgment
semantics between DIFFERENT (non-merged) works are handled entirely by the
general D-17 chronological co-claim demotion rule (§4.3), which never names
a relation — it only demotes the later co-claimant on a shared span. The
census artifact's own `relations_policy` field states this explicitly: hand-
enumerated containment relations are not built because containment is
many-to-many and open-ended; chronology needs no enumeration.

**174 provisional + 8 residual — NOT loaded (D-15).** The census additionally
carries `provisional_relations_measurement_only` (174 rows) and
`residual_direct` (8 rows). Neither is loaded into the shipped v2 sidecar.
Per Codex blocker #2's candidates-list treatment, the 174 provisional rows are
candidates for the `later_shared_text` DIAGNOSTIC measurement ONLY — never
ratified relations, never product data. The 8 residual_direct rows are
zero/near-zero-co-witness twin candidates the census could not fully confirm;
they are recorded for a future census pass, not acted on in v2.

**Pitfall #7 (self-erasure) — closed.** The delivered census is the COMPLETE
list (owner-ratified, chain-checked); it is not a partial merge that would
silently leave duplicates. A transitivity guard (§4.1) still asserts at build
time that no work appears in two merge groups.

---

## 3. Contested case — RESOLVED by drop (no schema needed)

Owner adjudication 2026-07-23 ("drop it and we'll be ok"): the Hai-Gaon /
RCh-Shabbat contested-author group is resolved by **dropping the Sefaria RCh
Shabbat copy (`w001239`)**, NOT by a contested-author merge. The census
records this verdict as `contested_drop_w001239` on the single `contested`
entry (`w000451` ↔ `w001239`).

In-DB evidence (`tmp/discovery-contested-study*.txt`, gitignored): Hai Gaon
(`w000451`) co-occurred ONLY with `w001239` (11 shared direct-witness folios,
byte-identical `matched_letters`), and ZERO with the M-source RCh Shabbat
(`w000452`). After the drop, all 11 shared folios KEEP a `tier_a` Hai Gaon
identification (0 loss on contested folios). Hai Gaon then has zero RCh
overlap → the contested question vanishes.

**Consequence:** the previously-planned v2 `works` disputed-author schema
(primary + alt + disputed flag) is DROPPED — there is no case left to model.
Reinstate ONLY if a future census pass surfaces OTHER ≥2-author groups.

**Cost accepted by owner:** RCh Shabbat coverage 65 → 31 direct-witness
folios; of 34 Sefaria-only folios, 12 are rescued by another work and ~22
lose their (unreviewed, Sefaria-reference-only) RCh ID (~10 lose any claim).
Integrity: `w001239` is canonical for no other work and
`witness_unit_members` has no `work_id` FK, so it is a clean bake-time
exclusion (129 claims / 144 evidence rows).

**Do NOT generalize the drop.** Every other twin MERGES — there is no
contested-author reason to delete, and dropping always forfeits the dropped
copy's unique folios.

---

## 4. Build changes to `build_discovery_sidecar.py`

Five subsections. Each must land behind the strict masking gate and the
all-invariant verifier (§7). Document order below is 4.1→4.5; the EXECUTION
order at bake time is different and is fixed in §6 (4.4 Lever-1 coverage
routing runs BEFORE 4.3 D-17 chronological demotion).

### 4.1 Canonical merge (soft merge) — REQUIRED hash-pinned `--canonical-merges` input

- Define the REQUIRED hash-pinned census input `--canonical-merges <path>
  --canonical-merges-sha256 <hex>` (owner-held handoff, the machine-readable
  `v2_canonical_merges.json`; masking-sensitive — never committed).
- **FROZEN exact-shape validating parser** (masking-safe, EXACT — not
  "merely functional"): the file is a JSON object with EXACTLY these
  top-level keys read by the build: `merges` (a list) and `dropped_by_135`
  (a list); any OTHER top-level key present in the real handoff (`source`,
  `canonical_priority`, `owner_ratified`, `ratified_at`, `relations_policy`,
  `chronological_rule_examples`, `contested`,
  `provisional_relations_measurement_only`, `residual_direct`, `notes`) is
  present in the real file but IGNORED by the parser — reading `merges` and
  `dropped_by_135` only, never any other key, is itself part of the frozen
  contract (this is what keeps the 174 provisional / 8 residual / 1
  contested entries out of the shipped v2 build, §2). Each entry in `merges`
  is a JSON object with EXACTLY the field names `members_w` (a list of one
  or more `w000xxx`-shaped opaque id strings), `canonical_w` (a single
  `w000xxx`-shaped opaque id string), and `owner_verdict` (a string) — these
  are the REAL field names as implemented in the delivered handoff, used
  verbatim here (no renaming/abstraction layer). ONLY entries with
  `owner_verdict=='approve'` load; any other `owner_verdict` value (e.g. the
  single `contested_drop_w001239` entry) is SKIPPED by this parser (that
  entry is handled entirely by the separate `dropped_by_135` exclusion,
  §4.2/§3, never by the merge loader). `dropped_by_135` is a list of
  `w000xxx`-shaped opaque id strings. The parser REJECTS: any `merges` entry
  missing `members_w`, `canonical_w`, or `owner_verdict`; any entry whose
  `members_w`/`canonical_w` values are not `w000xxx`-shaped strings
  (regex `^w\d{6}$`); a `dropped_by_135` entry that is not `w000xxx`-shaped;
  or a top-level JSON value that is not an object. NEVER a title, NEVER the
  restricted codename — the parser and this document reference members only
  by their opaque `w000xxx` id.
- **Semantic-ratification assertion (defense-in-depth, MEDIUM):** structural
  validity + a matching SHA-256 pin is not sufficient on its own to prove
  the PINNED file is the intended census — an operator could hash-pin a
  structurally-valid but wrong census revision. The build additionally
  asserts, after parsing: exactly 16 entries carry `owner_verdict=='approve'`;
  `dropped_by_135` equals exactly `{w001239}`; and the entry whose
  `members_w` contains `w000452` and `w001239` has `canonical_w=='w000452'`
  (the D-14 flip — the one merge whose canonical rep is NOT its Sefaria
  member). A file that passes SHA-256 + structural validation but fails any
  of these three semantic counts is REJECTED (`--release` HALTS) — this is
  the build's own defense against a correctly-hashed but substantively wrong
  census file.
- **Transitivity guard:** reject a census where a work appears as a member of
  more than one merge group (a chained collapse is a hard build error, not a
  best-effort union).
- The build MUST verify the file's SHA-256 against the pin BEFORE use (fail
  on missing/mismatch), validate the schema, build a `cross_corpus_map` from
  the `approve` rows PLUS the D-14 flip (§2), and thread it THREE ways:
  1. into `ids.canonical_work_id(work_id, cross_corpus_map)` at the real-mode
     `works` insert;
  2. into claim assembly (so display/aggregation can group by
     `canonical_work_id`, per the existing consumer contract below);
  3. into the D-17 router (§4.3), **grouping D-17 candidates by
     `canonical_work_id`** so a merged twin is never chrono-compared against
     itself (Codex #4 — closes preflight HIGH #10's self-loop concern:
     canonicalize BEFORE generating co-claim candidates, not after).
- Record the VERIFIED `--canonical-merges` SHA-256 in `meta` AND the v2 frame
  doc (Codex #B2 — meta + frame, NOT the minimal deploy manifest, which stays
  schema/basename/content-hash/frame-hash only per `discovery-deploy.md`).
- **Soft merge only** — do NOT rewrite `discovery_claim.work_id`. Provenance
  (which source copy) is preserved in `work_id`; display collapses via
  `canonical_work_id`.
- **Consumer contract (enforced downstream in 135/136):** all display /
  aggregation groups by `canonical_work_id`, and de-dups `(page_id,
  canonical_work_id)` so a folio witnessed under both copies shows ONCE. The
  spine keeps both rows (PK `(page_id, work_id)`); the collapse is a
  projection.

### 4.2 Drop-list exclusion — BEFORE claim-gen

- A hard exclusion set, `dropped_by_135` from the census (start: `{w001239}`).
  Excluded works emit NO `works`, `discovery_claim`, or `discovery_evidence`
  rows.
- This runs BEFORE `build_claims_and_evidence` — the drop-list exclusion must
  happen before claim-gen sees the excluded works, never as a post-hoc filter.
- Orphan check: after exclusion, assert no surviving claim/evidence/unit-member
  references a dropped work_id (should be structurally impossible via the
  two-table build, but assert it — a HARD FAIL).

### 4.3 The D-17 chronological co-claim demotion (replaces the killed relation table)

Runs AFTER §4.1 canonical merge, §4.2 drop-list exclusion, AND §4.4 Lever-1
coverage routing (see §6 for the full corrected order — this is the
sequencing fix for RESEARCH.md Pitfall 3).

**The rule** (per `same_work_spike/probe/rsource/results/chronological_demotion_rule.md`,
gitignored bake spec): for a page P, consider all works with a Track-1 claim
whose matched span on P overlaps another work's span (a co-claim cluster on
shared text — the candidate-universe definition below), GROUPED BY
`canonical_work_id` (Codex #4) — i.e. the cluster is built over DISTINCT
`canonical_work_id` values present on P, so two claims that collapse to the
SAME canonical group never compete against each other at all (they are one
entity for D-17 purposes).

**Deterministic year resolution per canonical group.** A canonical group's
year is resolved ONCE, not per raw member: primarily from the date table
entry keyed to the canonical representative's OWN raw work id (via
crosswalk); if the representative's own raw id has no resolved date, fall
back to the MINIMUM resolved year among the group's OTHER (merged) member
raw ids. If NO member of the group has a resolved date, the group's year is
UNKNOWN. This single resolved year is used for every claim under that
`canonical_work_id`, so conflicting per-member raw dates are a
resolution-ORDER rule (representative first, else earliest sibling), never
an ambiguity — there is exactly one year per canonical group by
construction.

**Deterministic single-anchor comparison (not pairwise-among-all-members).**
Within a cluster (2 or more distinct canonical groups on the same overlapping
span): let B = the cluster member with the MINIMUM resolved year (a
deterministic tie-break — if 2+ members share the minimum year, B is the
lexicographically smallest `canonical_work_id` among them; since all
minimum-year members are then within `DELTA` of each other by construction,
the tie-break never changes who gets demoted). For EVERY OTHER member A in
the cluster:
- if A has no resolved year → A is NEVER demoted (fail-safe, rule 5);
- else `diff = year(A) - year(B)` (always `>= 0` since B is the minimum);
  if `diff >= DELTA` → demote A on the shared span
  (`routing_status='review_only'`, `routing_reason='later_shared_text'`);
  if `diff < DELTA` → A is a TIE with B, NOT demoted.

Every comparison is against the single anchor B — members are NEVER compared
pairwise against each other. This resolves the apparent tension between
"later than the earliest" and "within-threshold ties": a tie is ALWAYS a
tie against the anchor B, never a separate pairwise check among the later
members (two non-anchor members that are both `>= DELTA` past B are both
demoted independently of each other, regardless of how close they are to
each other). The rule NEVER names a relation (embed / abridge / quote) — only
demotes. A work with no reliable date is NEVER demoted (fail-safe).

**Candidate-universe definition — FROZEN (Codex #14).** "Shared text" /
"co-claim cluster" is defined exactly as follows, reusing the SAME frame the
delivered date-coverage audit already measured its 69.1%/30.7%/0.2% firing
rates against (`chrono_date_coverage.md`, `MIN_ML=200`) — this is not a new
threshold invented for this rewrite, it is the audited one:
- **Population:** the CURRENTLY-SHIPPED (post-Lever-1, §4.4) `discovery_claim`
  rows, restricted to `evidence_source='track1_direct'` witness evidence
  (the same population the audit computed over).
- **Overlap metric:** two claims on the SAME `page_id` co-claim when each
  carries a witness evidence row with `matched_letters >= 200` on that page
  (the frozen minimum distinctive span — `MIN_ML=200`, identical to the
  audited frame; a match shorter than 200 letters is NOT a candidate pair
  member, on EITHER side).
- **Boundary rule:** no additional span-overlap geometry test beyond the
  `>=200`-letter threshold above is applied in v2 (both works' evidence
  spans are on the SAME page — the co-occurrence itself, gated by the
  200-letter floor, is the candidate signal; sub-span positional overlap
  refinement is a Lever-2/v2.1 concern, not v2).
- **Deduplication:** ONE candidate pair per unordered `{canonical_work_id_a,
  canonical_work_id_b}` PER `page_id` — repeated occurrences of the same
  canonical-group pair on the SAME page collapse to a single considered
  pair (already implied by the canonical-group clustering above); the SAME
  pair recurring on a DIFFERENT page is a SEPARATE considered pair (and gets
  its own `discovery_routing_audit` row, §below).
- **Formulaic-text exclusion:** NOT applied in v2 — no boilerplate/liturgical-
  formula filter narrows the candidate universe beyond the 200-letter floor.
  This is an explicit, documented v2 limitation (a common short liturgical
  formula shared verbatim across many works could inflate low-value
  candidate pairs); if corpus-scale fan-out proves this materially noisy, a
  formulaic-text exclusion is a v2.1/Lever-2 follow-up, not silently added
  here.
- **Fan-out cap:** NONE at v2 launch — the real corpus's co-claim universe is
  bounded (10,837 pairs at the audited frame size), well within any
  practical build budget; no artificial per-work or per-page cap is applied.

This candidate-universe definition is frozen BEFORE the v2 frame freezes
(§7 gate 6) — a later change to the 200-letter floor, the dedup rule, or the
no-cap/no-formulaic-filter choices is a NEW versioned frame (`discovery-v2.1`
or later), never a silent in-place edit to an already-shipped v2 frame.

**`DELTA=100y` — hardcoded, cited.** Per
`same_work_spike/probe/rsource/results/chrono_date_coverage.md` (the
date-coverage audit, owner-delivered 2026-07-23): composition-date coverage
is 99.9% corpus-wide (M 100.0%, SEF 100.0%, JA 92.7%); at `DELTA=100y` the
rule can order + demote 69.1% of real co-claim work-pairs, 30.7% are
within-DELTA ties, and 0.2% have an undated side (fail-safe, cannot fire).
This is the owner-set, delivered value — this document does NOT propose
re-running the audit or re-tuning DELTA.

**TWO REQUIRED hash-pinned date inputs** (Codex #5 — BOTH pinned, neither
silently optional; SHA-256 verified before use; recorded in `meta` + frame):

- **`--seftja-dates <path> --seftja-dates-sha256 <hex>`** (owner-held,
  masking-sensitive — the interim SEF/JA composition-date proxies). **FROZEN
  exact schema:** a JSON object mapping a RAW source-side work id (e.g. a
  Sefaria/JA reference id — masking-sensitive; joined to a `w000xxx` via
  `discovery_data/crosswalk.json` at build time, NOT itself an opaque product
  id; validated ONLY by successful crosswalk resolution, §7 D-16(c) — no
  separate raw-id namespace/regex is imposed here) to an object carrying
  EXACTLY the two keys `year` and `basis`, nothing more and nothing less. The
  parser REJECTS: a missing `year` key; a `year` value that is not a JSON
  integer (a numeric string, float, or null is rejected — `basis` and `year`
  are never interchangeable); a missing `basis` key (its presence is
  mandatory even though its content is discarded post-validation); a `basis`
  value that is not a JSON string (an EMPTY string `""` IS a valid `basis`
  value — only a missing key or a non-string type is rejected); or any
  object carrying a THIRD top-level key beyond exactly `{year, basis}`. The
  `year` value itself carries no additional numeric-range constraint in this
  input (the SEF/JA interim dates were already range-sanity-checked when
  generated); only the shape above is enforced by this parser.
- **`--composition-dates <path> --composition-dates-sha256 <hex>`**
  (owner-held, masking-sensitive — the M-source authored-works composition
  dates). **FROZEN exact schema:** a JSON object mapping a raw M-source work
  id (masking-sensitive; joined to `w000xxx` via crosswalk, same
  crosswalk-only validation as above) to a single composition-date value.
  The mapped value MUST be a JSON string (a number, null, object, list, or
  boolean is REJECTED outright — not merely "unparseable," a structural type
  violation). **FROZEN normalizer contract** (masking-safe — the STRUCTURAL
  grammar is exact and frozen here; the literal era/range/century designator
  vocabulary is the owner-held M-source string content itself and is NEVER
  enumerated/quoted in this committed document, since quoting it would
  reproduce owner-held source material): after stripping leading/trailing
  whitespace, the normalizer classifies the string into EXACTLY one of three
  structural categories by counting the decimal integer tokens (each 3–4
  digits) it contains and whether a recognized-but-unquoted era/range/century
  designator decorates them:
  1. **Explicit single year** — the string carries decoration recognized as
     an era/qualifier marker (or none) around EXACTLY ONE integer token →
     that integer is the normalized year directly (no arithmetic).
  2. **Century form** — the string carries decoration recognized as a
     century marker around EXACTLY ONE integer token `N` (the century
     ordinal, itself a small 1–2 digit number, NOT a 3–4 digit year) → the
     normalized year is the century MIDPOINT `100*(N-1)+50` (e.g. ordinal 10
     → 950).
  3. **Bounded year range** — the string carries decoration recognized as a
     range marker around EXACTLY TWO integer tokens `earliest`, `latest` (in
     that textual order) → the normalized year is the MIDPOINT
     `floor((earliest+latest)/2)`, REJECTED if `earliest >= latest` (a
     non-increasing or degenerate pair is not a valid range, regardless of
     designator match).

  A string containing zero integer tokens, more than two integer tokens, or
  decoration matching more than one designator category (an ambiguous mix)
  is UNPARSEABLE. The normalized integer year MUST satisfy the EXACT
  inclusive bound `500 <= year <= 1600` (a normalized value of 499 or 1601 is
  OUT OF RANGE, not merely "implausible" — the predicate is a plain integer
  comparison, no fuzz). **REJECT/HALT:** a present-but-unparseable value
  (matching NONE of the three categories, or matching more than one) OR one
  normalizing to a year failing the `500 <= year <= 1600` predicate HALTS the
  `--release` build (a hard build error) — it NEVER silently becomes UNKNOWN
  (an UNKNOWN would no-op D-17 on that pair and evaporate the audited
  coverage). This is DISTINCT from a MISSING row (absent from the file
  entirely), which the production coverage gate (below) handles separately.
  The exact literal era/range/century designator vocabulary is validated at
  execution against the owner table; the three-category/integer-count/
  midpoint-arithmetic/inclusive-range-bound/reject contract above is FROZEN
  and tested in the build (135-06): one test per accepted category (explicit
  year / century→midpoint / range→midpoint) plus near-miss rejection of an
  unparseable value (zero or >2 integer tokens) AND an out-of-range year
  (499 and 1601 boundary cases).

**Production coverage gate — EXACT predicate (Codex #5).** Let `U` = the
FROZEN candidate universe above (the currently-shipped, post-Lever-1
co-claim pairs at the `>=200`-letter floor, deduplicated per `(page_id,
canonical_work_id_a, canonical_work_id_b)`). Let `R` = the subset of `U`
where BOTH sides resolve a composition year (§4.3's deterministic
per-canonical-group year resolution). `pair_coverage = |R| / |U|`. This is
the SAME metric the delivered audit computed (10,837 pairs at `MIN_ML=200`,
21 unknown-date pairs = 99.8061% pair coverage — the pair-level number, NOT
the 99.9% corpus-wide all-works number quoted above for DELTA's own
citation; the two are DIFFERENT denominators and this gate uses the PAIR one
exclusively). The `--release` build computes `pair_coverage` over the ACTUAL
v2-shipped pair universe and HALTS (hard build error, before any bake
proceeds) if `pair_coverage < 0.990` — a fixed floor with deliberate headroom
below the audited 99.8061% (tolerating ordinary corpus growth/drift without
becoming a false-trip trap on an unrelated small change, while still hard-
catching a genuine broken date-join or a missing/short input source). This
gate NEVER silently degrades a missing source to UNKNOWN-for-all — a
HALT is the only outcome on a materially degraded join.

**`discovery_routing_audit` — masking-safe, page-scoped replayability table
(Codex #5).** A new table in the shipped sidecar recording every D-17
decision — one row per considered `(page_id, canonical_work_id_a,
canonical_work_id_b)` occurrence (matching the dedup key above, so repeated
occurrences of the SAME work pair on DIFFERENT pages each get their own row,
and `verify_discovery_sidecar.py` can reconstruct every individual routing
decision, not merely prove that SOME compatible pair-level record exists):

```
discovery_routing_audit (
  page_id                  TEXT NOT NULL,
  canonical_work_id_a      TEXT NOT NULL,  -- the anchor B (min-year / tie-break winner)
  canonical_work_id_b      TEXT NOT NULL,  -- the other member A of this considered pair
  year_a                   INTEGER,        -- B's resolved year; NULL only if B itself
                                            -- has no date (fail_safe_unknown_date case)
  year_b                   INTEGER,        -- A's resolved year; NULL if A is undated
  delta                    INTEGER,        -- year_b - year_a; NULL if either year is NULL
  decision                 TEXT NOT NULL CHECK (decision IN
                              ('demoted','kept_tie','fail_safe_unknown_date')),
  demoted_canonical_work_id TEXT,          -- set to canonical_work_id_b when decision='demoted';
                                            -- NULL for 'kept_tie' and 'fail_safe_unknown_date'
  UNIQUE(page_id, canonical_work_id_a, canonical_work_id_b)
);
```

Both canonical work ids are ALWAYS recorded, regardless of decision type
(including the fail-safe/unknown-date case) — a `NULL` `year_a`/`year_b`
records WHICH side was undated, never a dropped row. No title, no raw source
id, no restricted codename ever enters this table — opaque `w000xxx` ids and
integers only.

**Provenance recorded in `meta` + frame:** the verified `--composition-dates`,
`--seftja-dates`, and `discovery_data/crosswalk.json` SHA-256 hashes (Codex
#5/#B2 — alongside the `--canonical-merges` hash from §4.1; all four join the
existing `docs/specs/discovery-deploy.md` §4 pinned-input list at the
implementation phase).

**Invariants:**
- Merges FIRST (§4.1/§4.2 before this step) — never chrono-compare a work
  against its own cross-corpus twin.
- Per shared span — demotion is per co-claimed span, not the whole work; a
  work's DISTINCTIVE (non-co-claimed) text is unaffected.
- **Never orphans a shipped row** — the surviving row is by construction the
  earliest (kept), never the demoted one; no shipped row is left with only a
  `review_only` sibling as its sole evidence.
- Unknown/unreliable date → never demoted (fail-safe).
- Within-DELTA ties → NONE demoted (or `contested` if the surface later needs
  a single default, not required at launch).
- Recoverable in `review_only`, never deleted.
- **The D-17 candidate/keeper universe = CURRENTLY-SHIPPED spans** — i.e. the
  population AFTER Lever-1 coverage routing (§4.4) has already run (§6). A
  claim that Lever-1 already demoted to `review_only` is not part of the
  "shipped" population D-17 considers, so D-17 never touches (and never
  overwrites) a pre-existing Lever-1 `routing_reason` — this is a STRUCTURAL
  consequence of the corrected order-of-operations, not a runtime
  conflict-resolution rule (Codex #6: Lever-1-then-D-17 precedence is
  guaranteed by sequencing, never by a tie-break function).
- Promotion PROHIBITED — D-17 only ever moves a row from `shipped` to
  `review_only`, never the reverse.
- **`select_display_evidence` routing_status tier (Pitfall 2 — resolved as an
  investigate-then-decide item in 135-06 Task 2):** if real v1-candidate data
  shows Track-1 can emit >1 witness evidence row for the same `(page_id,
  work_id)` claim on distinct spans (one demoted by D-17, one still shipped),
  `scripts/discovery_ids.py::select_display_evidence`'s `_display_sort_key`
  gains a `routing_status` tier (shipped ranks above `review_only`) so a
  demoted sibling can never become the `display_evidence_id` of a claim that
  also has a shipped row; if Track-1 never does this in practice, this is
  documented as structurally unreachable rather than a required code change.
  Either way, the build MUST normalize every date to a numeric year BEFORE
  any row is written — the raw descriptive date string is NEVER persisted
  to the shipped sidecar (Pitfall 6, masking).

### 4.4 Lever-1 coverage routing — runs BEFORE §4.3 in execution order (see §6)

- Coverage = `matched_letters / len(norm_stream(page_text))`, computed at
  bake. `matched_letters` is populated on 254,729 evidence rows; the
  denominator is the normalized source page text; verify the computed value
  against the stored `density` column.
- Routing: `cov ≥ 0.45 → routing_status='shipped'`; `cov < 0.45 →
  routing_status='review_only'` (recoverable — the claim is retained, just
  not surfaced). Cliff is at 0.45 (validated: page-level catalogue-blind deck,
  `track1_pagelevel_manifest.json` — high 94.0% / med 91.7% / low 37.5%; 0.50
  reference point 94.3%, one-sided 95% LB 90.1%; see
  `docs/specs/discovery-band-labels-v1.md` §3.1).
- **INVARIANT (never route on catalogue):** coverage routing uses ONLY the
  coverage metric. Catalogue mismatch (52%, coverage-confounded) NEVER
  demotes a claim.
- **INVARIANT (review_only never dominates shipped):** if a page's display
  claim would be `review_only` while a shipped claim exists for the same
  `(page_id, canonical_work_id)`, the shipped claim wins the
  `display_evidence_id`. A `review_only` row must never orphan a shipped
  base.
- This step runs BEFORE §4.3 D-17 chronological demotion — D-17's candidate
  universe is defined over the population THIS step has already shipped
  (§4.3 invariants, §6).

### 4.5 The TESTED CERT-01 FAIL-branch reband to `screening_rb` (Codex #7 + #B2)

**Scope boundary — explicit deferral to the measurement protocol (Codex F6/
F12).** This section consumes a `--precision-spec` measurement OUTCOME as an
opaque, already-decided build input. This bake plan does NOT define, govern,
or duplicate ANY part of how that outcome is produced — blinding, sample
isolation from the D-17/Lever-1 diagnostic families, the survey-design
estimator (physMS-cluster bootstrap, effective sample size, per-stratum
minimums), or the pass/fail decision rule itself. ALL of that is governed
EXCLUSIVELY by `docs/specs/discovery-cert01-protocol.md` (135-03, already
Codex-reviewed and tracked). The `--precision-spec` value this build accepts
MUST be traceable to that protocol's frozen, pre-registered artifacts (its
`report_id`/frame hashes) — this build never re-validates the measurement's
statistical design, it only consumes its OUTCOME (`measurement_status` +
`ci_low`/`ci_high`/`numerator`/`denominator`) and applies the mechanical
reband/invalidation described below.

- A measured-below-floor `tier_a` outcome (fed post-135 via
  `--precision-spec`, `measurement_status='measured_fail'`) rebands `tier_a
  → screening_rb` — a REAL frozen band key (the rule-based algorithmic
  screening band, `discovery-band-labels-v1.md` §2 "Screening — rule-based");
  NEVER the non-existent `screening`, and NOT the D-10 canon-caveat
  `screening_canon`.
- AND flips affected rows' `routing_status` to `review_only` (drop from
  default).
- AND, in the SAME transaction, ATOMICALLY INVALIDATES the target
  `screening_rb` `band_precision` row: `measurement_status='not_measured'` +
  NULL `precision`/`ci_low`/`ci_high`/`numerator`/`denominator` — because the
  rebanded rows CHANGE the `screening_rb` population, so its legacy
  pre-registered number (0.859, measured on the ORIGINAL screening
  population) is no longer a valid estimate for the new, larger population.
  DO NOT fabricate a combined number (Codex #B2).
- AND writes a `meta` marker (`tier_a_reband_target='screening_rb'` + a
  rebanded-row count) the verifier keys on.
- **This is NOT a `band_precision`-only relabel, NOR a bare in-place `UPDATE
  discovery_evidence SET confidence_band=...`.** Because `confidence_band` is
  part of the FROZEN §2 `evidence_id` tuple (`discovery-sidecar-schema-v1.md`
  §2: `work_id|a_page_id|sys_id|evidence_kind|evidence_source|
  confidence_band|span_start|span_end|other_page_id|seed_spans_digest`) AND
  the §6 display-precedence key, the reband MUST be consumed as a REBUILD
  INPUT at band-assignment time (BEFORE `ids.evidence_id(...)` generation AND
  BEFORE `select_display_evidence()`'s display-pointer back-fill) — so each
  rebanded row's `evidence_id` REGENERATES over the new `confidence_band` and
  the claim's `display_evidence_id` RECOMPUTES over the rebanded+demoted
  evidence set, in the SAME build transaction. A bare post-assembly `UPDATE
  confidence_band=...` on an already-built DB is FORBIDDEN — it would leave
  content-inconsistent evidence_ids (no longer matching the frozen recipe)
  AND a stale `display_evidence_id` still pointing at a now-`review_only` row
  (Codex-R4 new-HIGH). When a rebanded `tier_a` claim ALSO carries a
  COMPETING shipped sibling evidence row (e.g. a `propagated corroborated`
  witness on the same `(page_id, work_id)`), the recomputed
  `display_evidence_id` moves to the surviving shipped sibling — never the
  demoted `screening_rb`/`review_only` row (§6 lattice + routing_status
  tier).
- Distinct from the **insufficient-evidence branch**
  (`measurement_status='insufficient_evidence'`), which keeps `tier_a`
  non-default via the **D-18 default-eligibility predicate**
  (`is_default_eligible()`, defined in `shared/discovery_band_labels.py`,
  135-01: a band is default-eligible ONLY when
  `measurement_status=='measured_pass'` AND `ci_low` is present AND
  `ci_low >= STRICT_FLOOR` (0.85); an `insufficient_evidence` status
  therefore fails this predicate and the band stays non-default WITHOUT any
  permanent relabel or precision invalidation — it can still pass on a later
  confirmation draw).
- This is a build capability 135-06 implements + tests; no production bake
  runs from this plan.

---

## 5. Band-label honesty (B) + the eighth lockstep item

The (B) contract (`docs/specs/discovery-band-labels-v1.md`) LANDED. v2 must
emit its enum rename in lockstep across the 7 files listed in that doc's §5:
`scripts/discovery_ids.py`, `scripts/build_discovery_sidecar.py`,
`scripts/verify_discovery_sidecar.py`, `web/discovery_assets.py`,
`shared/discovery_service.py`, `docs/specs/discovery-sidecar-schema-v1.md` +
`discovery-frames.md`, and this file's §2 label map:

- `expert_verified → high_confidence_algorithmic` (Track-1 top tier is an
  ALGORITHMIC score, not human approval).
- "Verified" / "confirmed" / "reviewed" are reserved for
  `adjudication_status='human_confirmed'` ONLY (121 rows corpus-wide) — they
  MUST NOT appear as a name or label for any `confidence_band` value.
- **"Certified" is PROHIBITED OUTRIGHT** — unlike the three words above, it
  is NEVER used anywhere in this document, the v2 build, or any v2 surface,
  including for `human_confirmed` rows (pre-existing frame rule,
  independent-audit gate not yet passed). This document itself uses
  "certified" ONLY inside this sentence, to state the prohibition — it never
  appears as a label, a variable name, or a description of any band.
- Bilingual EN/HE band labels per that doc's §2; estimated band precision
  `[CI]` presentation per §3, never per-item.

**Eighth lockstep item — the `routing_reason` `later_shared_text` amendment
(Pitfall 1).** The D-17 rule (§4.3) tags evidence `routing_reason=
'later_shared_text'`, but the FROZEN `routing_reason` enum in
`docs/specs/discovery-sidecar-schema-v1.md` / `scripts/discovery_ids.py` /
the `discovery_evidence` DDL `CHECK` constraint is currently `{impurity,
runner_up_conflict, co_citation, none}` — no fifth value exists yet. This
lands as an EIGHTH lockstep change, alongside (not instead of) the 7-file
rename above, in the SAME bake/commit:
- `scripts/discovery_ids.py` — add `ROUTING_REASON_LATER_SHARED_TEXT =
  "later_shared_text"` to `ROUTING_REASONS`.
- `scripts/build_discovery_sidecar.py` — add `'later_shared_text'` to the
  `discovery_evidence.routing_reason` CHECK constraint DDL.
- `scripts/verify_discovery_sidecar.py` — extend the enum-invariant
  validators.
- `docs/specs/discovery-sidecar-schema-v1.md` — a NEW dated amendment section
  (never a silent edit to the frozen block), adding `later_shared_text` to
  the Frozen Enum Vocabularies.

**Asset/bake-level atomicity (Codex #8).** `discovery-band-labels-v1.md` §5
gains its own dated amendment in plan 135-05 clarifying that the "one
commit / one bake" discipline means the BUILT v2 ASSET must be entirely v2
with NO mixed v1/v2 enum state (the literal v1 string `expert_verified`
ABSENT from the shipped v2 DB, the v2 key `high_confidence_algorithmic`
present) — not that every touched source file must land in one literal git
commit. v2 build code enforces this at the asset level (§7 gate 14); source
edits may still land across separately-committed, sequentially-dependent
plans/tasks, as long as no v2 bake ever runs with a partial rename applied.

---

## 6. Order of operations (corrected — D-17 AFTER Lever-1, not before)

1. Canonical / vgroup resolution (§4.1) + drop-list exclusion (§4.2).
2. Span-paired claim generation.
3. Distinctive / shared routing.
4. **Lever-1 coverage routing** (§4.4 — `cov < 0.45 → review_only`).
5. **D-17 chronological co-claim demotion** (§4.3 — over the now-shipped
   population from step 4, grouped by `canonical_work_id`).
6. Tier-A assignment.
7. Bake + verify + masking + manifest.

This corrects the STALE plan's ordering (which placed relation-table
population before Lever-1); `chronological_demotion_rule.md`'s own text is
explicit: "Applied AFTER merges (unify same-work) and Lever-1 coverage
routing, at bake time." A co-claim cluster's "kept" (earliest) work must
already have survived coverage routing before chronology compares against
it — otherwise the chronology step could be comparing against a work that
was never going to ship anyway.

---

## 7. Gates (all must pass before ship)

1. **Codex review of the build-script diff** — REQUIRED, precedes code merge
   (phase-134 discipline).
2. **All-invariant verifier** (standalone, from 134-03) — two-table
   integrity, offsets-only evidence, per-source bands, frozen enums,
   release-contract counts, `PRAGMA integrity_check`, schema_version.
3. **Strict masking gate** — `MASKING_SCAN_PATTERNS_FILE=.masking_patterns
   python scripts/check_atlas_masking.py --scan-repo --scan-sqlite
   --scan-asset <v2.db> --strict` → exit 0. Independent re-scan of every
   hand-edited/AI-generated artifact, including the M-source date-table
   vocabulary registered defensively (Pitfall 6).
4. **Band-enum absence** — v1 name (`expert_verified`) absent from the v2
   DB.
5. **Coverage sanity** — recomputed coverage matches stored `density` within
   tolerance; the 0.45 routing split reproduces the expected
   shipped/review_only counts.
6. **Frame doc** — write `docs/specs/discovery-frames-v2.md` with corrected
   per-band / per-evidence_source counts, merge/drop/D-17 summary, and the
   new `frame_content_hash` + DB `content_hash`; update
   `discovery_data/manifest.json`.
7. **`routing_reason` enum amendment landed in lockstep** (§5 eighth item) —
   `later_shared_text` present in `scripts/discovery_ids.py`, the DDL CHECK
   constraint, the verifier, and the dated schema amendment, all in the SAME
   bake.
8. **Never-orphan-shipped verifier invariant — EXACT assertion:** for EVERY
   claim that owns at least one `discovery_evidence` row with
   `routing_status='shipped'`, that claim's `display_evidence_id` MUST
   reference one of ITS OWN `shipped` evidence rows — a claim with any
   shipped evidence row can NEVER have `display_evidence_id` pointing at a
   `review_only` row (D-17 or Lever-1 demoted). Directly asserted per claim,
   not inferred from an absence of counterexamples.
9. **Composition-date coverage gate** — the `--release` build HALTS if
   `pair_coverage` (§4.3's exact `|R|/|U|` predicate over the frozen
   candidate universe) is `< 0.990` (a fixed floor with headroom below the
   audited 99.8061% pair-level baseline — NOT the 99.9% corpus-wide all-works
   figure, a different denominator used only to justify DELTA's citation).
10. **Routing-audit replayability check** — every `discovery_routing_audit`
    row is individually reconstructable: for `decision='demoted'`,
    `demoted_canonical_work_id` is non-NULL, both years are non-NULL, and
    `delta >= DELTA`; for `decision='kept_tie'`, both years are non-NULL and
    `delta < DELTA`; for `decision='fail_safe_unknown_date'`, at least one of
    `year_a`/`year_b` is NULL and `demoted_canonical_work_id` is NULL. Every
    `routing_reason='later_shared_text'` evidence row cross-checks against
    an audit row with `decision='demoted'` on the same `(page_id,
    canonical_work_id_a, canonical_work_id_b)` key.
11. **Frozen-input-hash provenance** — `--canonical-merges`,
    `--composition-dates`, `--seftja-dates`, and
    `discovery_data/crosswalk.json` SHA-256 hashes recorded in `meta` AND the
    v2 frame doc (Codex #B2/#5 — NOT the minimal deploy manifest).
12. **`measurement_status` ↔ interval consistency — EXHAUSTIVE over the
    closed vocabulary** `{not_measured, measured_pass, measured_fail,
    insufficient_evidence}` (any OTHER stored value is a HARD FAIL): a row
    with `measurement_status='measured_pass'` MUST have
    `precision`/`ci_low`/`ci_high`/`numerator`/`denominator` ALL non-NULL AND
    `ci_low >= 0.85`; `measurement_status='measured_fail'` MUST have those
    same five fields ALL non-NULL AND `ci_low < 0.85`; BOTH
    `measurement_status='not_measured'` AND
    `measurement_status='insufficient_evidence'` MUST have ALL FIVE of those
    fields NULL (no partial intervals under any status — Codex #B3).
13. **Reband-precision-invalidation** — when `meta` carries
    `tier_a_reband_target='screening_rb'`, the target band's `band_precision`
    row MUST have `measurement_status='not_measured'` AND ALL FIVE of
    `precision`/`ci_low`/`ci_high`/`numerator`/`denominator` NULL (not
    `precision` alone), AND `meta` MUST ALSO carry the paired rebanded-row
    count key alongside `tier_a_reband_target` — the verifier asserts BOTH
    the full five-field nulling AND the count key's presence (Codex #B2).
14. **Asset/bake-level no-mixed-enum atomicity** — the built v2 asset
    contains NO v1 enum literal `expert_verified` AND uses the v2 key
    `high_confidence_algorithmic` — ANY mixed v1/v2 state is a HARD FAIL
    (Codex #8).
15. **Evidence_id-content-consistency** — every stored `evidence_id`
    recomputes to the frozen §2 recipe, AND every claim's
    `display_evidence_id` recomputes via `select_display_evidence` over its
    current evidence rows — a reband applied as a bare in-place `UPDATE
    confidence_band` (not a rebuild input, §4.5) leaves a stale id/display
    pointer and HARD FAILs (Codex-R4 new-HIGH).

**D-16 resolutions:**
- **(a) Census JSON → `canonical_work_id` only, NOT `work_relations`.** The
  census's ONLY product-schema effect is populating `works.canonical_work_id`
  (§4.1). No relation table is built (§2).
- **(b) Filtering merges to the shipped work set.** The census population
  (all launch works, cross-source, witnessed) is a SUPERSET of the ~1,270
  shipped works. At build time, a census merge entry whose member(s) are not
  present in the v2 shown-set is SKIPPED (logged, not hard-failed) — it is
  harmless because there is no shipped row to affect. A merge entry is only
  APPLIED when at least one member resolves to a shipped `works.work_id`.
- **(c) `ref_corpus_v2.pkl` stability check.** Both date inputs (§4.3) key
  off raw source-side ids drawn from `ref_corpus_v2.pkl` at the time the
  census/date tables were generated. The build asserts every raw id
  referenced by the census, `--composition-dates`, and `--seftja-dates`
  successfully resolves via `discovery_data/crosswalk.json` — any raw id
  that fails to resolve is a HARD FAIL (never a silent skip), since a
  crosswalk miss on a previously-registered id would indicate
  `ref_corpus_v2.pkl` drifted between census-generation time and build time.
  This reuses the existing crosswalk join-safety discipline (Codex #9 —
  join only via stable ids, never normalized titles, hard-fail unexpected
  cardinalities).

---

## 8. Pitfalls (carry forward + new for v2)

- **Self-erasure (closed)** — the delivered census is complete and
  chain-checked (§2); the transitivity guard (§4.1) is the remaining
  build-time defense.
- **Soft-merge display gap** — if a consumer forgets to group by
  `canonical_work_id`, the duplicate reappears. Enforce in the 135/136
  display layer.
- **`review_only` orphaning** — a demoted row (Lever-1 OR D-17) must never be
  the sole `display_evidence_id` for a page that also has a shipped claim
  (§7 gate 8).
- **Coverage denominator** — `len(norm_stream(page_text))` must use the SAME
  normalization as the aligner, or coverage is wrong. Verify vs stored
  `density`.
- **Masking coverage gap (M-source date table)** — the composition-date
  table is a brand-new external input; never store the raw descriptive date
  string anywhere in the shipped sidecar (normalize to a numeric year at
  build time, §4.3); register the date table's known vocabulary in the
  masking pattern file defensively; re-run the full masking gate against the
  v2 asset AND every newly-authored doc (this rewrite, `discovery-frames-v2.md`)
  before any deploy step.
- **Worked-example validation (Pitfall 5)** — `chronological_demotion_rule.md`'s
  "Worked examples" table lists four illustrative cases; only ONE (work pair
  `w001159`/`w000177`, the single `chronological_rule_examples` entry in the
  census) is currently operationalized against real data with concrete
  `span_jac`/`co_pages`/`breadth` evidence. The other three remain qualitative
  illustrations. 135-06/135-07 spot-check at least one worked example against
  the real bake output as a MEDIUM (not hard-gating) check; the v2 frame doc
  documents which examples remain qualitative/unverified pending a full
  corpus-scale review.

---

## 9. Relationship to Phase 134 closure

This plan is a **data refresh carried forward**, not a re-opening of the
Phase 134 spine. All three Phase 134 success criteria (two-table schema +
contract; permanent masking guard; async DiscoveryService with budgets) are
MET by the v1 build and its code, and none of the three data-quality defects
violate them. The v2 re-distill runs through the unchanged pipeline and MUST
complete before Phase 135 grading and Phase 136 read surfaces — it is a Phase
135 prerequisite, tracked here. The 134-08 Task 3 production deploy remains
DEFERRED until the v2 build is ready to ship (deploy the FINAL v2 sidecar
ONCE, never v1 then v2).
