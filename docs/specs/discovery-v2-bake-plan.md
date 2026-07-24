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

**The spine is unchanged; the schema gets an ADDITIVE, explicitly-declared
amendment (Codex R3-HIGH — "spine unchanged" does not mean "zero DDL
change").** v2 runs through the SAME pipeline, loader, service, and masking
guard as v1 — the CORE two-table claim model (`works` / `discovery_claim` /
`discovery_evidence`, §1 of `discovery-sidecar-schema-v1.md`) is untouched.
This bake plan DOES require two additive, backward-compatible schema
changes, both landing as a NEW dated amendment section in
`discovery-sidecar-schema-v1.md` (§11, alongside the `routing_reason` enum
amendment, §5): (1) the NEW `discovery_routing_audit` table (§4.3); (2) a
NEW `band_precision.measurement_status` column (closed vocabulary
`{not_measured, measured_pass, measured_fail, insufficient_evidence}`,
required by gates 12/13 — flagged as a needed-but-not-yet-frozen column by
the 135-03 plan's own SUMMARY). Both are ADDITIVE (a new table, a new
nullable-defaulted column) — an OLD v1 asset remains structurally readable
by OLD code unchanged; the v2 loader/service degrades gracefully
(`measurement_status`/`discovery_routing_audit` absent ⇒ treated as a v1
asset, mirroring the existing `_index_has_field`-style compat-gate
convention elsewhere in the codebase) rather than crashing. "The spine is
unchanged" refers to the CORE claim-model architecture and pipeline shape —
not a literal zero-DDL-change claim, which this section makes explicit
rather than leaving implicit.

**FIRST-BUILD `measurement_status` population — FROZEN default (Codex
R5-HIGH fix, closing the "initial population unspecified" gap).** On the
FIRST v2 build (no `--precision-spec` supplied; §6 step 0 resolves "no
override"), EVERY `band_precision` row's `measurement_status` is populated
by ONE rule, applied uniformly so gate 12's exact five-field consistency
predicate holds for EVERY row with NO exception: default
`measurement_status='not_measured'` with ALL FIVE of
`precision`/`ci_low`/`ci_high`/`numerator`/`denominator` set NULL, for EVERY
band — INCLUDING `screening_rb`'s pre-existing legacy figure
(`discovery-band-labels-v1.md` §3.1's `0.859 (pre-registered; CI pending
Ph135)`). That legacy value is a precision POINT ESTIMATE WITHOUT a computed
`ci_low`/`ci_high` (explicitly "CI pending" — not yet a real confidence
interval), so it CANNOT satisfy gate 12's `measured_pass`/`measured_fail`
five-field-non-NULL-plus-`ci_low`-comparison requirement; the v2 first build
therefore NULLS this legacy point estimate alongside its status,
intentionally resetting `screening_rb`'s displayed precision to "not
measured" until a REAL CI-bearing measurement (a future `screening_rb`-
scoped precision protocol, or a later CERT-01-style measurement) completes
and writes a genuine `measured_pass`/`measured_fail` row with all five
fields populated. The ONLY row eligible for a non-`not_measured` initial
status on the FIRST build is one that ALREADY carries a genuine, complete,
previously-computed CI (non-NULL `precision` AND `ci_low` AND `ci_high` AND
`numerator` AND `denominator` from a prior, already-completed measurement) —
none exists at v2 first-build time for ANY band, so in practice EVERY
`band_precision` row starts the v2 era as `measurement_status='not_measured'`
with all five fields NULL, no exception. This is a ONE-TIME first-build
migration rule, distinct from (and never invoked by) the §4.5 reband's own
atomic invalidation, which applies ONLY on a LATER reband-triggering
rebuild.

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

**Shared JSON-parsing requirement across ALL THREE hash-pinned inputs (Codex
R4-MEDIUM):** `--canonical-merges`, `--composition-dates`, and
`--seftja-dates` are each parsed with STRICT duplicate-key rejection at
EVERY object nesting level — a source document carrying a repeated key
anywhere (top-level or nested inside a `merges`/`dates` entry) is REJECTED
(`--release` HALTS), never silently resolved by a default last-write-wins
JSON decoder. This is implemented via a duplicate-checking `object_pairs_hook`
(or equivalent) passed to every JSON parse in the build, not the bare
stdlib `json.loads` default — a real gap the default decoder would
otherwise silently mask, undermining the "exact/closed shape" guarantees
specified for all three parsers below.

### 4.1 Canonical merge (soft merge) — REQUIRED hash-pinned `--canonical-merges` input

- Define the REQUIRED hash-pinned census input `--canonical-merges <path>
  --canonical-merges-sha256 <hex>` (owner-held handoff, the machine-readable
  `v2_canonical_merges.json`; masking-sensitive — never committed).
- **FROZEN exact-shape validating parser** (masking-safe, EXACT — not
  "merely functional"; CLOSED key set, Codex R2-MEDIUM fix): the file is a
  JSON object whose top-level keys MUST be a SUBSET of the CLOSED, named
  set `{merges, dropped_by_135, source, canonical_priority, owner_ratified,
  ratified_at, relations_policy, chronological_rule_examples, contested,
  provisional_relations_measurement_only, residual_direct, notes}` — of
  these twelve, ONLY `merges` (a list) and `dropped_by_135` (a list) are
  READ by the build; the other ten are TOLERATED-BUT-IGNORED (present in
  the real handoff, never consumed — this is what keeps the 174 provisional
  / 8 residual / 1 contested entries out of the shipped v2 build, §2). A
  top-level key OUTSIDE this named twelve-key set is REJECTED (`--release`
  HALTS) — the allowlist is closed, not merely "ignore anything unexpected."
  **The ten TOLERATED-BUT-IGNORED keys carry NO declared value-shape
  constraint whatsoever, and this is INTENTIONAL, not an oversight (Codex
  R5-MEDIUM fix):** `source`, `canonical_priority`, `owner_ratified`,
  `ratified_at`, `relations_policy`, `chronological_rule_examples`,
  `contested`, `provisional_relations_measurement_only`, `residual_direct`,
  and `notes` may EACH hold ANY valid JSON value of ANY shape (string,
  number, object, list, null, arbitrarily nested) — the parser performs ZERO
  validation on their CONTENTS; only their KEY NAMES are checked against the
  closed twelve-key allowlist above (a key name outside the twelve is
  rejected regardless of its value's shape; a key name inside the twelve, of
  ANY value shape, is accepted and discarded unread). This is safe precisely
  BECAUSE these ten keys are never read: a malformed or unexpected VALUE
  under a tolerated key can never influence the shipped build. The
  "exact-shape" guarantee this parser provides is therefore scoped to the
  TWO consumed keys (`merges`, `dropped_by_135`, each frozen exactly below)
  plus the closed key-NAME allowlist for the file's other ten top-level
  keys — not to the full recursive shape of every top-level value.
  Each entry in `merges` is a JSON object with EXACTLY the three field names
  `members_w` (a list of one or more `w000xxx`-shaped opaque id strings),
  `canonical_w` (a single `w000xxx`-shaped opaque id string), and
  `owner_verdict` (a string) — these are the REAL field names as implemented
  in the delivered handoff, used verbatim here (no renaming/abstraction
  layer); a `merges` entry carrying ANY field beyond exactly these three is
  REJECTED (extra fields inside a merge entry are NOT tolerated, unlike the
  file's own top-level key set above). ONLY entries with
  `owner_verdict=='approve'` load; any other `owner_verdict` value (e.g. the
  single `contested_drop_w001239` entry) is SKIPPED by this parser (that
  entry is handled entirely by the separate `dropped_by_135` exclusion,
  §4.2/§3, never by the merge loader). `dropped_by_135` is a list of
  `w000xxx`-shaped opaque id strings. The parser REJECTS: any `merges` entry
  missing `members_w`, `canonical_w`, or `owner_verdict`, or carrying an
  extra field; any entry whose `members_w`/`canonical_w` values are not
  `w000xxx`-shaped strings (regex `^w\d{6}$`); a `dropped_by_135` entry that
  is not `w000xxx`-shaped; a top-level JSON value that is not an object; or
  any top-level key outside the closed twelve-key set above. NEVER a title,
  NEVER the restricted codename — the parser and this document reference
  members only by their opaque `w000xxx` id.
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
gitignored bake spec): the rule is evaluated as a set of PURE PAIRWISE
comparisons — never a whole-cluster "anchor" — over DISTINCT
`canonical_work_id` values (Codex #4: two claims that collapse to the SAME
canonical group never form a pair; they are one entity for D-17 purposes).
This pairwise formulation (rather than a single per-cluster anchor) is
deliberate: it removes any ambiguity about which member "anchors" a cluster
of 3+ co-claimed works, and every pairwise decision is self-contained and
independently replayable (§ candidate-universe + `discovery_routing_audit`
below).

**Deterministic year resolution per canonical group.** A canonical group's
year is resolved ONCE, not per raw member: primarily from the date table
entry keyed to the canonical representative's OWN raw work id (via
crosswalk); if the representative's own raw id has no resolved date, fall
back to the MINIMUM resolved year among the group's OTHER (merged) member
raw ids. If NO member of the group has a resolved date, the group's year is
UNKNOWN. This single resolved year is used for every claim under that
`canonical_work_id`, so conflicting per-member raw dates ACROSS DIFFERENT
members are a resolution-ORDER rule (representative first, else earliest
sibling), never an ambiguity — there is exactly one year per canonical group
by construction.

**Crosswalk injectivity is ONE-DIRECTIONAL — the SAME-member conflicting-date
case is a HALT, not an ambiguity resolved by the rule above (Codex R5-HIGH
fix).** §7 D-16(c)'s crosswalk guarantee is FORWARD-ONLY: every RAW id
resolves to EXACTLY ONE `w000xxx`. It does NOT guarantee the REVERSE — a
single `w000xxx` (the representative OR any other group member) MAY be the
crosswalk target of MULTIPLE DISTINCT raw ids appearing in the SAME date
table (e.g. two source-side reference-segment ids that both crosswalk to the
same work). This is DIFFERENT from the "representative vs. sibling"
resolution-order rule above, which resolves conflicts ACROSS DIFFERENT
canonical-group MEMBERS — it does NOT cover multiple raw ids landing on the
SAME member. When multiple raw ids crosswalk to the SAME `w000xxx` within the
SAME date table: if they ALL normalize to the SAME year, that year is used
(no ambiguity, no HALT). If they normalize to TWO OR MORE DISTINCT years, the
build HALTS (`--release` fails) with a dedicated conflicting-same-member-date
error — NEVER resolved by "first row wins," "minimum year," or any other
precedence rule; a same-member date conflict is treated as a data-quality
defect in the pinned date input requiring upstream correction, structurally
distinct from the (legitimate, resolution-ordered) across-member case above.
This HALT applies identically whether the SAME-member conflict lands on the
representative's own id or on a fallback sibling id.

**Namespace-family restriction on the two date inputs (Codex R5-HIGH fix —
closes the round-5 "not explicitly constrained" gap):** `--seftja-dates` keys
are restricted to raw ids matching the `sefaria` OR `ja` namespace-prefix
pattern (§7 D-16(c)) ONLY — a key matching the `msource` pattern present in
`--seftja-dates` is REJECTED (`--release` HALTS, a hard structural error,
BEFORE any crosswalk join is attempted for that key). Symmetrically,
`--composition-dates` keys are restricted to raw ids matching the `msource`
namespace-prefix pattern ONLY — a key matching `sefaria` or `ja` present in
`--composition-dates` is REJECTED. This check runs BEFORE the crosswalk join
(defense-in-depth alongside D-16(c)'s existing zero/multiple-match
cardinality checks) and prevents either date table from silently supplying
dates sourced from the WRONG corpus family.

**Deterministic pairwise decision — processed in ONE deterministic pass,
ORDERED to prevent a demoted reference from itself demoting anything
further (Codex R4-HIGH fix — closes the span-orphaning gap):** compute
EVERY qualifying pair `{X, Y}` in the candidate universe (below) up front,
order EACH pair lexicographically by `canonical_work_id` into `(lo, hi)` —
a stable, date-independent addressing key, never implying which side is
"kept" — then process ALL pairs in a SINGLE FIXED ORDER: ascending by the
NUMERICALLY LATER side's resolved year (ties broken by the lexicographic
`(page_id, lo, hi)` key). For pair `{lo, hi}`:
- if EITHER `lo` or `hi` has no resolved year → `decision =
  fail_safe_unknown_date`; NEITHER side is demoted (fail-safe, rule 5 — an
  unknown date on either side means no direction can be established, so the
  KNOWN-dated side is not entitled to demote the unknown one either);
- else let `E` = the EARLIER-dated side, `L` = the LATER-dated side (equal
  years — a true tie — fall through to `kept_tie` directly, below), `delta =
  ABS(year(L) - year(E))`;
  - **if `E` has ALREADY been marked `demoted` by an earlier-PROCESSED pair
    in THIS SAME PASS** (i.e., `E` itself lost to some still-earlier
    reference) → `decision = kept_invalid_reference` REGARDLESS of `delta`
    (Codex R5-BLOCKER fix — a DISTINCT fourth closed decision value, NEVER
    `kept_tie`: `kept_tie` is reserved EXCLUSIVELY for a delta-below-floor
    outcome measured against a VALID, still-surviving reference, so every
    `kept_tie` row is GUARANTEED `delta < DELTA` by construction — this is
    what makes gate 10's per-decision predicate, updated below, exhaustive
    and internally consistent rather than contradicted by this branch) —
    `L` is NEVER demoted relative to an ALREADY-HIDDEN reference; a claim can
    only ever be demoted relative to a reference that is ITSELF surviving
    (shipped or tied), never to one that has already lost its own comparison.
    (Processing in ascending order of the LATER side's year guarantees `E`'s
    own demotion status against ITS earlier references is ALREADY FINALIZED
    by the time this pair is evaluated, since `E < L` means `E`'s own pairs
    were processed earlier in this SAME ascending pass — no fixed-point
    iteration is needed, one deterministic forward pass suffices.)
  - **otherwise** (E is a valid, surviving reference — i.e. E was NOT itself
    marked `demoted` by an earlier-processed pair in this pass): if
    `delta >= DELTA` → `L` is demoted (`decision = demoted`); if
    `delta < DELTA` → `decision = kept_tie` (neither demoted). **`kept_tie` is
    reachable ONLY from this "valid reference" branch — never from the
    "`E` already demoted" branch above** — which is exactly what guarantees
    `delta < DELTA` on every `kept_tie` row (gate 10).

This closes the exact gap Codex R4 found: WITHOUT the "is `E` already
demoted" check, a middle-dated claim `B` that itself lost to an earlier
claim `A` could still be used as the "kept" reference that hides a later
claim `C` — leaving a shared span where NEITHER the earliest witness (`A`,
which may not even overlap `C`'s specific span) NOR `B` NOR `C` is visible.
With the ordered, reference-validity-checked pass above, `C` can only ever
be demoted relative to a reference that is ITSELF still shipped (or tied),
so a chain of demotions can never orphan a span.

**Physical mutation — the ENTIRE canonical group's evidence footprint on the
page, not one arbitrarily-chosen row (Codex R4-BLOCKER fix):** because soft
merge (§4.1) preserves EVERY raw `discovery_claim`/`discovery_evidence` row
(a canonical group MAY have 2+ raw member `work_id`s, each with its OWN
possible witness evidence on the SAME `page_id`), a `decision='demoted'`
pairwise outcome for canonical group `G` on page `P` demotes EVERY
`discovery_evidence` row on `P` whose (canonicalized) `work_id` resolves to
`G`, whose `evidence_source='track1_direct'`, and whose primary span
satisfies the overlap + 200-letter floor test against the winning side — NOT
a single representative row. This is a per-CANONICAL-GROUP, per-PAGE
mutation (potentially touching >1 physical evidence row when a group has
multiple raw members each separately witnessing that page), never a
single-row operation. The `discovery_routing_audit` reverse/forward match
(§below) is therefore MANY-TO-MANY, not 1:1 in either direction: one audit
row can explain MULTIPLE physical evidence rows (when the demoted canonical
group has multiple raw-member witnesses on that page), and one evidence row
can be explained by MULTIPLE audit rows (when it lost to 2+ earlier
co-claimants in a 3+-way cluster, §above).

**Demotion target-set — EXACT and independently verifiable (Codex R5-HIGH
fix, closing the "entire footprint" vs. per-row overlap-qualifier
ambiguity).** The phrase "ENTIRE canonical group's evidence footprint" above
means EXACTLY the following CLOSED target set — never an unconditional
"every row under G" set. For canonical group `G` demoted relative to winning
side `L` on page `P`, the target set `T(G,P)` = every `discovery_evidence`
row `R` such that (i) `R`'s canonicalized `work_id` equals `G`, (ii)
`R.evidence_source='track1_direct'`, (iii) `R`'s primary span NUMERICALLY
OVERLAPS `L`'s primary span under the EXACT SAME overlap test used to qualify
the candidate pair itself (`max(start_R, start_L) < min(end_R, end_L)` —
never a looser "same page" test), and (iv) `R.matched_letters >= 200` (the
same floor gating candidacy, above). A row on page `P` canonicalizing to `G`
whose span does NOT overlap `L` (e.g. a disjoint-register witness, per the
multi-register invariant above) is OUTSIDE `T(G,P)` and is NEVER touched by
this demotion, even though it shares page `P` and canonical group `G` with
the demoted rows. `T(G,P)` MAY contain more than one row (when `G` has 2+ raw
members each separately witnessing an overlapping span on `P`) — this is
what "the entire qualifying footprint" means; it is NOT "every row
regardless of overlap."

**"Already demoted" tracking is PAGE-SCOPED (Codex R5-HIGH fix, answering the
round-5 question directly).** The reference-validity check in the pairwise
pass (above) tracks a canonical group's demoted status PER
`(page_id, canonical_work_id)` — exactly the granularity the candidate
universe itself is keyed at (`(page_id, canonical_work_id_lo,
canonical_work_id_hi)`, below). A group demoted on page `P1` is NOT thereby
"already demoted" when its OWN pairs on a DIFFERENT page `P2` are evaluated;
each page's ordered pass is independent. There is no per-overlapping-span-
component or cross-page demotion state.

A claim's FINAL routing outcome for a given shared span is `review_only` if
its canonical group was the demoted side in ANY qualifying pair touching
that span, per the ordered pass above; the rule NEVER names a relation
(embed / abridge / quote) — only demotes.

**Candidate-universe definition — FROZEN (Codex #14), WITH a required
span-overlap safety refinement (BLOCKER, multi-register invariant).** "Shared
text" / co-claim pair is defined exactly as follows. The delivered
date-coverage audit's own candidate count (10,837 pairs, `chrono_date_coverage.md`)
used a PAGE-CO-OCCURRENCE approximation (`MIN_ML=200` letters per side, with
NO span-position-overlap test) purely to validate DELTA's firing rate; the v2
BUILD CONTRACT below is STRICTER — it additionally REQUIRES the two claims'
primary witness spans to numerically overlap, which the audit script did not
test. This is a deliberate, honest divergence: a stricter candidate universe
can only be a SUBSET of the audited 10,837 (never a superset), so the
audited 69.1%/30.7%/0.2% firing-rate split is a valid UPPER BOUND on how much
the v2 build's stricter rule could ever demote, never an exact prediction of
the v2 build's own (smaller) candidate count.
- **Population:** the CURRENTLY-SHIPPED (post-Lever-1, §4.4) `discovery_claim`
  rows, restricted to `evidence_source='track1_direct'` witness evidence.
- **Primary span:** each claim's PRIMARY witness interval on its page — the
  largest `spans_json` pair for `tier_a` rows, the `(o0, o1)` pair for E1
  rows (§4 of `discovery-sidecar-schema-v1.md`).
- **Span-overlap requirement (Codex BLOCKER — multi-register safety):** two
  claims on the SAME `page_id` qualify as a candidate pair ONLY IF their
  primary intervals NUMERICALLY OVERLAP: `max(start_x, start_y) <
  min(end_x, end_y)`. Two claims whose primary spans are DISJOINT (occupy
  different, non-overlapping positions on the page — e.g. Bible verses vs.
  an interleaved Targum translation, or Bible vs. Onkelos vs. a
  Judeo-Arabic Tafsir, each in its own non-overlapping run) are NEVER a
  candidate pair, regardless of both being present on the same page. This
  directly preserves the `discovery-band-labels-v1.md` §4 multi-register
  invariant: legitimate, co-existing, non-competing witnesses in different
  registers/scripts occupy disjoint spans and are structurally excluded from
  D-17 comparison — chronology only ever compares works that are ACTUALLY
  contending for the SAME text.
- **Overlap magnitude floor:** in ADDITION to the overlap requirement above,
  EACH claim's own `matched_letters >= 200` (the frozen minimum distinctive
  span, `MIN_ML=200` — the same floor the audit used, applied per side).
- **Deduplication:** ONE candidate pair row per `(page_id, canonical_work_id_lo,
  canonical_work_id_hi)` — the lexicographic pair key above. Repeated
  overlap of the SAME canonical-group pair on the SAME page collapses to a
  single considered pair; the SAME pair recurring on a DIFFERENT page is a
  SEPARATE considered pair (its own `discovery_routing_audit` row, below).
  For a page carrying 3+ mutually-overlapping canonical groups, EVERY
  qualifying unordered pair among them is independently considered (there is
  no single per-page "cluster anchor" — see the pure-pairwise decision
  above), so population size and decision count are always in 1:1
  correspondence, with no separate cluster-level bookkeeping.
- **Formulaic-text exclusion:** NOT applied in v2 — no boilerplate/liturgical-
  formula filter narrows the candidate universe beyond the overlap +
  200-letter floor above. This is an explicit, documented v2 limitation (a
  common short liturgical formula shared verbatim across many works could
  inflate low-value candidate pairs); if corpus-scale fan-out proves this
  materially noisy, a formulaic-text exclusion is a v2.1/Lever-2 follow-up,
  not silently added here.
- **Fan-out cap:** NONE at v2 launch — the real corpus's co-claim universe is
  bounded (at most 10,837 pairs under the looser page-co-occurrence
  approximation; the span-overlap-gated v2 universe is smaller), well within
  any practical build budget; no artificial per-work or per-page cap is
  applied.

This candidate-universe definition is frozen BEFORE the v2 frame freezes
(§7 gate 6) — a later change to the overlap rule, the 200-letter floor, the
dedup key, or the no-cap/no-formulaic-filter choices is a NEW versioned frame
(`discovery-v2.1` or later), never a silent in-place edit to an
already-shipped v2 frame.

**Reband/D-17 interaction — see §6 for the single unified sequence (Codex
R2-HIGH, tightened at R3).** §6 defines ONE numbered order-of-operations
valid for BOTH the first v2 build and any later reband-triggering rebuild:
a pre-flight step 0 resolves an optional `--precision-spec` BEFORE the
pipeline runs; D-17 (step 5) CONSULTS that resolution when building its
currently-shipped candidate population (so a row already condemned to the
§4.5 reband is excluded from D-17's population even though the reband's
DB write happens later, at step 6); this guarantees a page can never end up
with BOTH its D-17-kept co-claimant AND its only alternative hidden by the
SAME build.

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
  id; validated via the SAME fixed namespace-prefix-family check PLUS
  crosswalk resolution defined once in §7 D-16(c) — this is the ONE
  raw-id validation mechanism used by every input in this document, never a
  separate or contradictory rule) to an object carrying
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
  dates). **FROZEN exact schema — the grammar vocabulary is DATA bound by the
  SAME hash pin, not a separate out-of-band contract (Codex R2-BLOCKER
  fix):** the file is a single JSON object with EXACTLY these four top-level
  keys — `century_designators` (a list of one or more non-empty strings —
  owner-held recognized substrings that decorate a century-ordinal token),
  `range_designators` (a list of one or more non-empty strings — owner-held
  recognized substrings that decorate a two-integer range), `era_qualifiers`
  (a list of zero or more non-empty strings — owner-held recognized
  decoration that does NOT change how a lone year token is read; MAY be an
  empty list, but the key itself is REQUIRED), and `dates` (a JSON object
  mapping a raw M-source work id — masking-sensitive; joined to `w000xxx`
  via the SAME namespace-prefix-family check PLUS crosswalk resolution
  defined once in §7 D-16(c) — to a single composition-date STRING value).
  The parser REJECTS: any file missing one of these four keys;
  `century_designators`/`range_designators` empty or
  containing a non-string/empty-string element; `era_qualifiers` containing
  a non-string element (empty list IS valid); `dates` not an object, or any
  `dates` value that is not a JSON string (a number, null, object, list, or
  boolean value is REJECTED outright — a structural type violation, not
  merely "unparseable"). Because the recognized designator vocabulary is
  READ FROM THIS SAME FILE (never hardcoded in build code, never quoted in
  this document), the SHA-256 pin on `--composition-dates` covers BOTH the
  raw date strings AND the exact grammar used to parse them — the "vocabulary
  validated against the owner table" ambiguity from the previous draft is
  eliminated: there is no separate, unpinned owner table; the vocabulary IS
  the pinned file.
  **FROZEN normalizer contract — TOKENIZE-FIRST (never punctuation-strip-
  first), designator-driven, fully anchored (Codex R2-BLOCKER fix for the
  token-count ambiguity + Codex R3-HIGH fix for unanchored residual text +
  Codex R4-HIGH fix for the punctuation/tokenization order contradiction and
  the undefined multi-designator-match behavior):** the normalizer NEVER
  strips or removes punctuation before locating digit tokens — digit tokens
  are located FIRST, directly against the ORIGINAL (whitespace-trimmed)
  string, via a fixed maximal-decimal-run scan (every maximal contiguous run
  of ASCII digit characters, in left-to-right textual order); punctuation is
  examined ONLY afterward, to validate that nothing UNACCOUNTED-FOR remains.
  This ordering removes the round-4 contradiction entirely: a range
  separator or any other punctuation character can NEVER cause two adjacent
  digit runs to fuse into one token, because digit-run extraction never
  looks at surrounding punctuation at all.
  1. **Designator classification (category selection):** test the string for
     a substring match against EVERY entry in `century_designators`, then
     EVERY entry in `range_designators` (fixed order). If ONE OR MORE
     `century_designators` entries match (regardless of how many DISTINCT
     entries from that SAME list match — multiple synonymous designators for
     the SAME category is fine and not ambiguous), category = **century**.
     Else if one or more `range_designators` entries match, category =
     **range**. Else category = **explicit year**. If the string matches
     AT LEAST ONE entry from BOTH `century_designators` AND
     `range_designators` (a genuine cross-list ambiguity, not merely
     multiple matches within one list), the value is UNPARSEABLE.
  2. **Digit-token extraction:** scan the ORIGINAL string (not
     punctuation-stripped) for every maximal decimal-digit run, in
     left-to-right order. Century category requires EXACTLY ONE run,
     interpreted as the century ORDINAL `N` (a plausible ordinal in `[1,16]`
     for the composition window below); range category requires EXACTLY TWO
     runs, `earliest` then `latest` in textual order (each a plausible 3–4
     digit year); explicit-year category requires EXACTLY ONE run (a
     plausible 3–4 digit year). Zero runs, or a run-count other than what
     the category requires, is UNPARSEABLE (this applies BEFORE any
     punctuation/residual-text check below).
  3. **Anchoring — validate the residual (Codex R3-HIGH, restated without the
     R4 contradiction; Codex R5-HIGH fix pins the EXACT range-separator
     character set + a deterministic removal order for overlapping matches):**
     remove FROM THE ORIGINAL STRING, in this FIXED order, (a) the matched
     designator substring(s) that determined the category in step 1, (b)
     every matched `era_qualifiers` substring, (c) every extracted digit-run
     substring (step 2), and (d) — range category ONLY — exactly one
     range-separator character (below); then remove all whitespace.
     **Deterministic removal order for overlapping matches:** if two
     candidate substrings to remove (from the `century_designators`/
     `range_designators` list at (a), or the `era_qualifiers` list at (b))
     overlap in character span within the original string (one is a
     substring of, or partially overlaps, another), the LONGEST matching
     substring is removed FIRST; a SHORTER match fully contained within an
     already-removed character span is treated as already consumed — it is
     NOT removed a second time, and it does NOT count as a SEPARATE match for
     the "one or more entries match" test in step 1.
     **The range-separator character set is FIXED and HARDCODED in build
     code — NOT data-file-driven, unlike the designator lists (Codex R5-HIGH
     fix), precisely because the separator must be identical across every
     possible date-table revision:** when category=range, the separator MUST
     be EXACTLY ONE character from the CLOSED set `{U+002D HYPHEN-MINUS,
     U+2013 EN DASH, U+2014 EM DASH}`, and — AFTER removals (a)-(c) above —
     it MUST be the SOLE remaining non-whitespace character positioned
     TEXTUALLY BETWEEN the `earliest` and `latest` digit-run positions (never
     before the `earliest` run, never after the `latest` run, never a second
     occurrence anywhere else in the residual); a range-category string whose
     between-digit-runs character is NOT one of these three is UNPARSEABLE.
     The REMAINDER, after (a)-(d) and whitespace removal, MUST consist ONLY
     of characters from the FIXED allowed punctuation set `{',', '.', '(',
     ')'}` (comma, period, parentheses) PLUS, for range category ONLY, the
     single consumed range-separator character above (a distinct set — the
     separator is never also counted as a comma/period/parenthesis). ANY
     other leftover character or word (including an unrecognized qualifier
     not present in `era_qualifiers`) makes the WHOLE value UNPARSEABLE — an
     unrecognized semantic label merely co-occurring with a valid-looking
     year token is REJECTED, never silently treated as harmless decoration.
  4. **Value computation:** century → normalized year = the MIDPOINT
     `100*(N-1)+50` (e.g. ordinal 10 → 950; an ordinal outside `[1,16]` is
     UNPARSEABLE); range → normalized year = the MIDPOINT
     `floor((earliest+latest)/2)`, REJECTED if `earliest >= latest`;
     explicit year → that integer directly (no arithmetic).

  The normalized integer year MUST satisfy the EXACT inclusive bound
  `500 <= year <= 1600` (a normalized value of 499 or 1601 is OUT OF RANGE,
  not merely "implausible" — the predicate is a plain integer comparison, no
  fuzz). **REJECT/HALT:** a present-but-unparseable value (matching zero of
  the three categories per the designator-driven rule above, or an internal
  integer-token-count violation within its matched category) OR one
  normalizing to a year failing the `500 <= year <= 1600` predicate HALTS
  the `--release` build (a hard build error) — it NEVER silently becomes
  UNKNOWN (an UNKNOWN would no-op D-17 on that pair and evaporate the
  audited coverage). This is DISTINCT from a MISSING row (absent from
  `dates` entirely), which the production coverage gate (below) handles
  separately. The three-category designator-driven contract above (checked
  in the fixed order century → range → explicit-year) is FROZEN and tested
  in the build (135-06) against FABRICATED, masking-safe designator lists
  and date strings (never the real owner-held vocabulary in test fixtures):
  one test per accepted category (century→midpoint / range→midpoint /
  explicit year) plus near-miss rejection of an unparseable value (wrong
  integer-token count for its matched category, or a century ordinal outside
  `[1,16]`) AND an out-of-range normalized year (499 and 1601 boundary
  cases) AND the ambiguous-dual-designator-match rejection AND a
  non-pinned-range-separator rejection (Codex R5-HIGH — a range-shaped value
  whose between-digit-runs character is NOT one of the three pinned
  `{U+002D, U+2013, U+2014}` separators, e.g. a comma or a designator word
  positioned between the two digit runs instead of a dash character).

**Production coverage gate — EXACT predicate (Codex #5).** Let `U` = the
FROZEN candidate universe above (the currently-shipped, post-Lever-1,
span-overlap-gated co-claim pairs, deduplicated per `(page_id,
canonical_work_id_lo, canonical_work_id_hi)` — every row of
`discovery_routing_audit`, §below, IS exactly one element of `U`, by
construction; `|U|` = `COUNT(*)` of that table). Let `R` = the subset of `U`
where BOTH sides resolve a composition year (`decision IN ('demoted',
'kept_tie')`, i.e. NOT `fail_safe_unknown_date`). **Zero-candidate case
(Codex R3-MEDIUM):** if `|U| = 0` (no candidate pairs exist at all), the
build HARD-FAILS EXPLICITLY with a dedicated error — `pair_coverage` is
NEVER computed as a 0/0 division, and an empty universe is NEVER silently
treated as 100% (trivially "fully covered") or skipped; `|U| = 0` almost
certainly indicates a broken candidate-generation step upstream (§4.3), not
a legitimately clean corpus, and must be surfaced as a hard build error
distinct from the `pair_coverage < 0.990` HALT. Otherwise, `pair_coverage =
|R| / |U|`. **This is an ABSOLUTE production floor, NOT a same-basis
regression check against the delivered audit's number (Codex R4-MEDIUM —
the two are computed over DIFFERENT, non-comparable populations and must
not be conflated as "before vs after"):** the delivered audit's 99.8061%
figure (10,837 pairs at `MIN_ML=200`, 21 unknown-date pairs) used a LOOSER
page-co-occurrence approximation with NO span-overlap test; the v2 build's
`|U|` is computed over the STRICTER, span-overlap-gated candidate universe
(§ above) — a stricter subset can legitimately show a LOWER or HIGHER
coverage PROPORTION than the looser approximation even when the underlying
date-join itself has not regressed at all, simply because the denominator
population differs. `0.990` is therefore set and enforced as a fixed,
STANDALONE, absolute quality floor for the v2 build's OWN
`pair_coverage` (chosen with headroom below the audited number purely as a
sanity anchor, not as a strict "must not regress from X" comparison) — it
HALTS (hard build error, before any bake proceeds) whenever the ACTUAL
v2-shipped `discovery_routing_audit` population's `pair_coverage` is
`< 0.990`, catching a genuine broken date-join or a missing/short input
source, without requiring or implying like-for-like comparability to the
older approximation's number. This gate NEVER silently degrades a missing
source to
UNKNOWN-for-all — a HALT is the only outcome on a materially degraded join.

**`discovery_routing_audit` — masking-safe, page-scoped, fully replayable
table (Codex #5).** A new table in the shipped sidecar recording EVERY
pairwise D-17 decision — one row per `(page_id, canonical_work_id_lo,
canonical_work_id_hi)` (the lexicographic pair key from §4.3's pairwise
rule; `lo`/`hi` is a stable ADDRESSING order, never a date/kept-vs-demoted
implication):

```
discovery_routing_audit (
  page_id                   TEXT NOT NULL,
  canonical_work_id_lo      TEXT NOT NULL,  -- lexicographically smaller of the pair
  canonical_work_id_hi      TEXT NOT NULL,  -- lexicographically larger of the pair
  year_lo                   INTEGER,        -- canonical_work_id_lo's resolved year; NULL if undated
  year_hi                   INTEGER,        -- canonical_work_id_hi's resolved year; NULL if undated
  delta                     INTEGER,        -- ABS(year_hi - year_lo); NULL if either year is NULL
  decision                  TEXT NOT NULL CHECK (decision IN
                               ('demoted','kept_tie','kept_invalid_reference',
                                'fail_safe_unknown_date')),  -- 4th value: Codex R5-BLOCKER fix
  demoted_canonical_work_id TEXT,           -- the actual demoted member (equals canonical_work_id_lo
                                             -- OR canonical_work_id_hi); NULL for 'kept_tie' and
                                             -- 'fail_safe_unknown_date'
  UNIQUE(page_id, canonical_work_id_lo, canonical_work_id_hi)
);
```

Both canonical work ids are ALWAYS recorded, regardless of decision type
(including the fail-safe/unknown-date case) — a `NULL` `year_lo`/`year_hi`
records WHICH side was undated, never a dropped row. No title, no raw source
id, no restricted codename ever enters this table — opaque `w000xxx` ids and
integers only.

**Gate 10 (routing-audit replayability) — EXHAUSTIVE predicate (Codex R2-HIGH,
fixing the round-1 gate):** for EVERY `discovery_routing_audit` row:
- `demoted_canonical_work_id`, when non-NULL, MUST equal EXACTLY
  `canonical_work_id_lo` OR `canonical_work_id_hi` (no third value);
- `decision='demoted'` REQUIRES: both `year_lo`/`year_hi` non-NULL,
  `delta = ABS(year_hi - year_lo)` exactly (recomputed and compared, not
  merely present), `delta >= DELTA`, and `demoted_canonical_work_id`
  non-NULL and equal to whichever of `lo`/`hi` has the LARGER year;
- `decision='kept_tie'` REQUIRES: both years non-NULL, `delta = ABS(year_hi
  - year_lo)` exactly, `delta < DELTA`, and `demoted_canonical_work_id` IS
  NULL;
- **`decision='kept_invalid_reference'`** (Codex R5-BLOCKER fix — the fourth
  closed decision value, distinct from `kept_tie`) **REQUIRES:** both years
  non-NULL (the earlier-dated side `E` must itself have HAD a resolved year
  to have been marked `demoted` by an earlier-processed pair in the first
  place), `delta = ABS(year_hi - year_lo)` exactly recomputed — `delta` here
  carries NO magnitude constraint relative to `DELTA` (it may be `>= DELTA`
  or `< DELTA`; UNLIKE `kept_tie`, which is bound to `delta < DELTA`), and
  `demoted_canonical_work_id` IS NULL (this pair's later side `L` is NOT
  demoted here — it is merely withheld from demoting an already-hidden `E`).
  **Page-scoped replay check:** `E` (whichever of `canonical_work_id_lo`/`_hi`
  is the earlier-dated side of THIS pair) MUST itself appear as the
  `demoted_canonical_work_id` of AT LEAST ONE OTHER `decision='demoted'` row
  on the SAME `page_id` — a `kept_invalid_reference` row with no such
  same-page corroborating `demoted` row is a HARD FAIL (it would mean the
  "already demoted" branch fired without a real, page-scoped prior demotion
  to justify it);
- `decision='fail_safe_unknown_date'` REQUIRES: `year_lo IS NULL OR year_hi
  IS NULL`, `delta IS NULL`, and `demoted_canonical_work_id IS NULL`;
- **Population equality — defined over the RECONSTRUCTED PRE-D-17 population
  (Codex R3-BLOCKER fix, not the naively-current `shipped` set):** because
  D-17 itself changes some rows FROM `shipped` TO `review_only`, the
  candidate universe `U` cannot be recomputed from the CURRENT
  `routing_status='shipped'` rows alone (every successful demotion would
  vanish from that recomputation). Instead, define **pre-D-17-eligible** as:
  a `discovery_evidence` row that is EITHER currently `routing_status=
  'shipped'` OR currently `routing_status='review_only' AND
  routing_reason='later_shared_text'` (the `low_coverage` review_only
  reason, §4.4, and the `co_citation` review_only reason, pre-existing v1,
  are EXCLUDED from this reconstruction — those rows were ALREADY not
  pre-D-17-shipped, so D-17 never considered them in the first place; the
  §4.5 reband ALSO never assigns `later_shared_text` — see the dedicated
  reband-routing-reason clarification below — so it never enters this
  reconstruction either). The SET of `(page_id, canonical_work_id_lo,
  canonical_work_id_hi)` triples in `discovery_routing_audit` EQUALS the
  frozen candidate-universe `U` recomputed over this PRE-D-17-ELIGIBLE
  population (span-overlap + 200-letter floor, §4.3) — no audit row without
  a corresponding real candidate pair, and no real candidate pair without an
  audit row.
- **Reverse/forward match — MANY-TO-MANY in BOTH directions (Codex R3-BLOCKER
  + R4-BLOCKER fixes, corrected from an impossible 1:1 forward claim):**
  because (a) a single evidence row can lose to MULTIPLE earlier co-claimants
  when 3+ canonical groups co-claim the same page (the pure-pairwise design,
  §4.3), AND (b) a single `decision='demoted'` audit row can correspond to
  MULTIPLE physical `discovery_evidence` rows when the demoted canonical
  group has 2+ raw members each separately witnessing the SAME page (§4.3's
  physical-mutation rule — soft merge preserves every raw row), the
  audit↔evidence match is many-to-many in BOTH directions, joined on
  `(page_id, canonicalized work_id)`:
  - every `discovery_evidence` row carrying `routing_reason=
    'later_shared_text'` has AT LEAST ONE corresponding
    `discovery_routing_audit` row with `decision='demoted'` naming its
    (canonicalized) `work_id` as `demoted_canonical_work_id` on the same
    `page_id`;
  - every `discovery_routing_audit` row with `decision='demoted'` has AT
    LEAST ONE corresponding `discovery_evidence` row on the same `page_id`
    whose canonicalized `work_id` equals `demoted_canonical_work_id` and
    carries `routing_reason='later_shared_text'`.
  A demotion recorded in one table with ZERO counterparts in the other (in
  EITHER direction) is a HARD FAIL; MULTIPLE rows on either side explaining
  the SAME demotion (whether multiple audit rows for one evidence row, or
  multiple evidence rows for one audit row) is EXPECTED and VALID, never a
  failure — the invariant is "zero" that is forbidden, not "more than one".
- **Target-set completeness — a NEW exhaustive check (Codex R5-HIGH fix,
  IN ADDITION TO, not a replacement for, the "at least one counterpart"
  reverse/forward match above).** The reverse/forward match above proves NO
  orphaned audit/evidence row exists at all; it does NOT by itself prove a
  demotion was COMPLETE. This check closes that gap: for EVERY
  `discovery_routing_audit` row with `decision='demoted'` naming canonical
  group `G` on page `P` (with winning side `L` = whichever of `lo`/`hi` is
  NOT `demoted_canonical_work_id`), the SET of `discovery_evidence` rows with
  `routing_status='review_only' AND routing_reason='later_shared_text'` whose
  canonicalized `work_id` equals `G` on page `P` MUST equal EXACTLY the
  target set `T(G,P)` defined above (§4.3 "Demotion target-set") — every row
  satisfying (i)-(iv) there is demoted (a qualifying row accidentally left
  `shipped` is a HARD FAIL: partial mutation), and no row failing any of
  (i)-(iv) is demoted (a non-overlapping or sub-floor row wrongly caught is a
  HARD FAIL: over-mutation).
- **Reband routing-reason clarification (Codex R4-HIGH fix — closes an
  undefined-value gap; Codex R5-MEDIUM fix removes a now-impossible branch):**
  the §4.5 FAIL-branch reband flips affected rows' `routing_status` to
  `review_only` but does NOT assign `routing_reason='later_shared_text'`
  (that value is EXCLUSIVELY D-17's own output) — the reband LEAVES the
  row's PRE-EXISTING `routing_reason` UNCHANGED (whatever it already was:
  `none` for a normal previously-shipped `tier_a` row, or `low_coverage` if
  Lever-1 had ALREADY demoted it before the reband ran — Lever-1 coverage
  routing, §4.4, runs on EVERY claim independently of any `--precision-spec`
  resolution and is NOT excluded by §6 step 0, so a `tier_a` row CAN already
  carry `low_coverage` at reband time). **`later_shared_text` is NOT a
  possible pre-existing value here** — unlike `low_coverage`, this is not
  merely unlikely but STRUCTURALLY IMPOSSIBLE under the corrected §6
  ordering: step 0's resolution EXCLUDES every row already condemned to this
  reband from D-17's (step 5) shipped candidate population, so D-17 NEVER
  processes — and therefore never assigns `later_shared_text` to — a row
  that THIS SAME build is about to reband; the two mechanisms cannot both
  touch the same row in one build, so the round-5-flagged "or
  later_shared_text ... if D-17 had ALREADY demoted it" disjunct is removed
  as a stale, unreachable branch. The row's REBANDED status is fully
  identified WITHOUT a new `routing_reason` value by the COMBINATION of
  `confidence_band='screening_rb'` + `routing_status='review_only'` +
  `meta.tier_a_reband_target='screening_rb'` being set — a screening_rb row
  can ONLY reach `review_only` via this reband mechanism (a normal,
  non-rebanded `screening_rb` row is ALWAYS `routing_status='shipped'`,
  `routing_reason='none'`, per the pre-existing v1 routing matrix,
  `discovery-sidecar-schema-v1.md` §7) — so NO third new `routing_reason`
  enum value is needed; the schema, build behavior, and this replayability
  description now agree exactly.

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
- Routing: `cov ≥ 0.45 → routing_status='shipped', routing_reason='none'`;
  `cov < 0.45 → routing_status='review_only', routing_reason='low_coverage'`
  (recoverable — the claim is retained, just not surfaced). `low_coverage`
  is a NEW `routing_reason` enum value (Codex R3-BLOCKER — see the enum
  amendment note under §4.3's `discovery_routing_audit` gate: WITHOUT a
  distinguishing reason, a `review_only` row's cause — Lever-1 vs D-17 vs
  the §4.5 reband — cannot be reconstructed from the shipped asset alone,
  which is exactly what breaks gate 10's replayability). Cliff is at 0.45
  (validated: page-level catalogue-blind deck, `track1_pagelevel_manifest.json`
  — high 94.0% / med 91.7% / low 37.5%; 0.50 reference point 94.3%, one-sided
  95% LB 90.1%; see `docs/specs/discovery-band-labels-v1.md` §3.1).
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
  `--precision-spec`, `measurement_status='measured_fail'`) rebands **the
  ENTIRE currently `confidence_band='tier_a'` population** (`--precision-spec`
  is a scope-wide result over the whole `tier_a` stratified sample — there is
  no valid SUB-population distinction the measurement design supports, so
  "affected rows" = ALL rows currently banded `tier_a`, never a partial
  subset) → `screening_rb` — a REAL frozen band key (the rule-based
  algorithmic screening band, `discovery-band-labels-v1.md` §2 "Screening —
  rule-based"); NEVER the non-existent `screening`, and NOT the D-10
  canon-caveat `screening_canon`.
- AND flips affected rows' `routing_status` to `review_only` (drop from
  default).
- AND, in the SAME transaction, ATOMICALLY INVALIDATES the CURRENT
  `band_precision` row for BOTH bands whose population just changed — TARGET
  `screening_rb` AND SOURCE `tier_a` (Codex R3-HIGH: invalidating only the
  target and silently preserving the source's now-stale-population number in
  the LIVE `band_precision` table was itself a B2 violation — `tier_a`'s row
  described a population that changed the instant the reband applied, exactly
  like `screening_rb`'s did). BOTH rows get
  `measurement_status='not_measured'` + NULL
  `precision`/`ci_low`/`ci_high`/`numerator`/`denominator` in the SAME
  transaction — `screening_rb`'s legacy pre-registered number (0.859,
  measured on the ORIGINAL screening population) is no longer valid for its
  new, larger population; `tier_a`'s number is no longer valid for a NOW-EMPTY
  population (zero rows remain — the verifier asserts `SELECT COUNT(*) FROM
  discovery_evidence WHERE confidence_band='tier_a'` = 0 whenever
  `meta.tier_a_reband_target` is set). DO NOT fabricate a combined number for
  either band (Codex #B2).
- **The triggering measurement is preserved SEPARATELY, in `meta`, never in
  the LIVE `band_precision` table (Codex R3-HIGH fix):** the actual
  CERT-01 `measured_fail` numbers that TRIGGERED this reband (the precision/
  `ci_low`/`ci_high`/numerator/denominator the `--precision-spec` supplied)
  are recorded as historical provenance under DEDICATED `meta` keys —
  `tier_a_reband_trigger_precision`, `_ci_low`, `_ci_high`, `_numerator`,
  `_denominator` — and in the v2 frame doc, NOT as the live `tier_a`
  `band_precision` row (which, per the bullet above, is nulled like every
  other invalidated row). This keeps `band_precision` a table of ONLY
  currently-valid numbers (no exceptions), while the measurement that caused
  the reband remains fully auditable via `meta` + the frame doc.
- AND writes a `meta` marker (`tier_a_reband_target='screening_rb'` + a
  rebanded-row count) the verifier keys on, ALONGSIDE the trigger-provenance
  keys above.
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
`discovery-frames.md` (Codex R5-MEDIUM fix: the trailing "and this file's §2
label map" reference is REMOVED here — that was a leftover pointer to a
label map §2 no longer contains; §2 is now the census, and this document
carries no separate duplicate band-label mapping of its own to rename):

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

**Eighth lockstep item — the `routing_reason` amendment, TWO new values
(Pitfall 1 + Codex R3-BLOCKER).** The D-17 rule (§4.3) tags evidence
`routing_reason='later_shared_text'`, and Lever-1 (§4.4) tags its own
demotions `routing_reason='low_coverage'` — a SECOND new value, discovered
as necessary during the Codex R3 adversarial round: without it, a
`review_only` row's cause (Lever-1 vs D-17) cannot be reconstructed from the
shipped asset alone, which is exactly what gate 10's replayability requires
(§4.3). The FROZEN `routing_reason` enum in
`docs/specs/discovery-sidecar-schema-v1.md` / `scripts/discovery_ids.py` /
the `discovery_evidence` DDL `CHECK` constraint is currently `{impurity,
runner_up_conflict, co_citation, none}` — neither new value exists yet. Both
land as ONE EIGHTH lockstep change (adding two sibling enum values in the
SAME amendment), alongside (not instead of) the 7-file rename above, in the
SAME bake/commit:
- `scripts/discovery_ids.py` — add `ROUTING_REASON_LATER_SHARED_TEXT =
  "later_shared_text"` AND `ROUTING_REASON_LOW_COVERAGE = "low_coverage"` to
  `ROUTING_REASONS`.
- `scripts/build_discovery_sidecar.py` — add `'later_shared_text'` AND
  `'low_coverage'` to the `discovery_evidence.routing_reason` CHECK
  constraint DDL.
- `scripts/verify_discovery_sidecar.py` — extend the enum-invariant
  validators for BOTH new values.
- `docs/specs/discovery-sidecar-schema-v1.md` — a NEW dated amendment section
  (never a silent edit to the frozen block), adding BOTH `later_shared_text`
  and `low_coverage` to the Frozen Enum Vocabularies, AND separately adding
  the NEW `discovery_routing_audit` table definition and the NEW
  `band_precision.measurement_status` column (§1's schema-amendment note) in
  the SAME dated section.

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

## 6. Order of operations (corrected — D-17 AFTER Lever-1, not before) — ONE unified sequence for both the initial build and a reband-triggering rebuild (Codex R3-HIGH fix)

**Step 0 — precision-spec resolution (pre-flight, no DB writes; a no-op for
the FIRST build):** parse and validate an optional `--precision-spec`
BEFORE step 1. If absent (the first v2 build) or present-but-not-triggering
(e.g. `measurement_status='insufficient_evidence'`), resolution = "no
override" and every step below runs exactly as originally banded. If it
triggers the §4.5 FAIL-branch reband (`measurement_status='measured_fail'`),
resolution DECIDES (in memory, at this pre-flight point) the FINAL set of
rows that will end this build as `confidence_band='screening_rb',
routing_status='review_only'` instead of their original `tier_a`/`shipped`
state — this decision is CONSULTED (read-only) by step 5 below and
MATERIALIZED (written to the DB) at step 6; it is decided ONCE, here, never
re-derived mid-pipeline.

1. Canonical / vgroup resolution (§4.1) + drop-list exclusion (§4.2).
2. Span-paired claim generation (this is where each claim's initial
   `confidence_band`, including `tier_a`, is populated from its source
   population — e.g. `track1_matches WHERE shadowed_by IS NULL` for
   `tier_a` — independently of any later reband).
3. Distinctive / shared routing.
4. **Lever-1 coverage routing** (§4.4 — `cov < 0.45 → routing_status=
   'review_only', routing_reason='low_coverage'`).
5. **D-17 chronological co-claim demotion** (§4.3 — over the currently-
   shipped population from step 4, grouped by `canonical_work_id`, PURE
   PAIRWISE). CRITICALLY: D-17's "currently-shipped" population CONSULTS
   step 0's resolution — any row step 0 already decided will end this build
   rebanded to `screening_rb`/`review_only` is EXCLUDED from D-17's shipped
   population here, even though step 6 has not yet physically written that
   change to the row. This is how the reband's effect is guaranteed to
   precede D-17 causally while `write` of the reband still happens later, at
   step 6, in the SAME build transaction — D-17 never treats a row step 0
   has already condemned as a valid "kept" side of a pairwise comparison.
6. **Tier-A assignment (reband materialization point):** for the FIRST
   build (step 0 resolved "no override"), this step is a no-op — `tier_a`
   rows keep the band step 2 gave them. For a reband-triggering rebuild,
   THIS is where step 0's decision is physically WRITTEN: the affected
   rows' `confidence_band` becomes `screening_rb`, `routing_status` becomes
   `review_only`, `evidence_id` regenerates over the new band (§4.5), and
   `display_evidence_id` recomputes.
7. Bake + verify + masking + manifest.

This ONE sequence is valid for BOTH build types — there is no separate
"exception note" for the reband case; step 0's read-only resolution +
step 5's consultation of it + step 6's materialization is the single
mechanism that keeps D-17 and the reband mutually consistent regardless of
which build is running. This also corrects the STALE original plan's
ordering (which placed relation-table population before Lever-1);
`chronological_demotion_rule.md`'s own text is explicit: "Applied AFTER
merges (unify same-work) and Lever-1 coverage routing, at bake time." A
co-claim pair's earlier-dated side must already have survived coverage
routing (and any reband) before chronology compares against it — otherwise
D-17 could be comparing against a work that was never going to ship anyway.

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
   BOTH `later_shared_text` AND `low_coverage` present in
   `scripts/discovery_ids.py`, the DDL CHECK constraint, the verifier, and
   the dated schema amendment (which also adds `discovery_routing_audit` +
   `band_precision.measurement_status`), all in the SAME bake.
8. **Never-orphan-shipped verifier invariant — EXACT assertion:** for EVERY
   claim that owns at least one `discovery_evidence` row with
   `routing_status='shipped'`, that claim's `display_evidence_id` MUST
   reference one of ITS OWN `shipped` evidence rows — a claim with any
   shipped evidence row can NEVER have `display_evidence_id` pointing at a
   `review_only` row (D-17 or Lever-1 demoted). Directly asserted per claim,
   not inferred from an absence of counterexamples.
9. **Composition-date coverage gate** — the `--release` build HALTS
   EXPLICITLY (a dedicated error, never a 0/0 division) if `|U| = 0` (no
   candidate pairs at all — a likely upstream candidate-generation bug);
   otherwise it HALTS if `pair_coverage` (§4.3's exact `|R|/|U|` predicate
   over the frozen candidate universe) is `< 0.990` — an ABSOLUTE,
   standalone production floor for the v2 build's OWN population, NOT a
   same-basis regression check against the audited 99.8061% pair-level
   baseline (that number was computed over a LOOSER, non-span-overlap-gated
   population and is not proportionally comparable to the v2 build's
   stricter `|U|`); NOT the 99.9% corpus-wide all-works figure either, a
   third, different denominator used only to justify DELTA's citation.
10. **Routing-audit replayability check** — the full EXHAUSTIVE predicate
    defined inline in §4.3 (over `canonical_work_id_lo`/`_hi`, `year_lo`/
    `year_hi`, exact `delta` recomputation, population equality against the
    RECONSTRUCTED PRE-D-17-ELIGIBLE candidate universe — `shipped` UNION
    `review_only`-with-`routing_reason='later_shared_text'`, never the
    naively-current `shipped` set alone — and the reverse match against every
    `routing_reason='later_shared_text'` evidence row) — summarized here:
    every `discovery_routing_audit` row is individually reconstructable per
    its `decision`, the audit population exactly equals the recomputed
    pre-D-17-eligible candidate universe (no extra, no missing rows), and
    every demotion is matched AT LEAST ONCE in BOTH directions
    (audit→evidence AND evidence→audit are both many-to-many, not 1:1 — a
    demoted canonical group with 2+ raw witnessing members on one page, or a
    row demoted by 2+ earlier co-claimants in a 3+-way cluster, are both
    expected, not failures; only a ZERO-count match in either direction is a
    HARD FAIL).
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
    fields NULL (no partial intervals under any status — Codex #B3). The
    FIRST-build default population rule (§1, Codex R5-HIGH) guarantees every
    row satisfies this predicate trivially via `not_measured` + all-NULL
    until a real measurement lands.
13. **Reband-precision-invalidation — BOTH bands (Codex R3-HIGH, corrected
    from a target-only check):** when `meta` carries
    `tier_a_reband_target='screening_rb'`, BOTH the target `screening_rb` row
    AND the source `tier_a` row in `band_precision` MUST have
    `measurement_status='not_measured'` AND ALL FIVE of `precision`/`ci_low`/
    `ci_high`/`numerator`/`denominator` NULL (not `precision` alone, and not
    the target band alone); AND `meta` MUST carry the paired rebanded-row
    count key alongside `tier_a_reband_target`, AND the
    `tier_a_reband_trigger_*` provenance keys (precision/ci_low/ci_high/
    numerator/denominator) recording the triggering measurement — the
    verifier asserts the full five-field nulling on BOTH bands, the count
    key's presence, AND the trigger-provenance keys' presence (Codex #B2).
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
- **(c) `ref_corpus_v2.pkl` stability check + namespaced-id join safety —
  the namespace-prefix families are HASH-PINNED DATA, not a described-but-
  unfrozen mechanism (Codex #9/F9, tightened at R4 — "the same three
  families the crosswalk is keyed by" is not itself an implementable
  contract; the exact patterns must be pinned).** Both date inputs (§4.3)
  key off raw source-side ids drawn from `ref_corpus_v2.pkl` at the time the
  census/date tables were generated. `discovery_data/crosswalk.json` itself
  carries a REQUIRED top-level key `namespace_prefixes`: a JSON object with
  EXACTLY three keys `sefaria`, `ja`, `msource`, each mapping to a
  non-empty regex-pattern STRING (owner-authored, anchored — e.g. matched
  via `re.match` against the start of the raw id — and, like the rest of
  `crosswalk.json`, covered by the SAME SHA-256 pin already required for
  this file per the existing D-16(c)/Codex #9 discipline, so the recognized
  prefix patterns are never a separate, unpinned, out-of-band contract).
  The parser REJECTS a `crosswalk.json` missing `namespace_prefixes`, missing
  any of the three named keys, or with a non-string/empty-string pattern
  value. The build validates that the three patterns are MUTUALLY EXCLUSIVE
  by construction (a self-test over a synthetic id set at build start: no
  fabricated test id may match more than one pattern) — a HARD FAIL if they
  are not. Before attempting ANY crosswalk join, the build validates every
  raw id referenced by the census, `--composition-dates`, and
  `--seftja-dates` against these three PINNED patterns — never a normalized
  title string, and never an ad hoc pattern invented at parse time. A raw id
  matching NONE of the three PINNED patterns is REJECTED before the
  crosswalk join is even attempted (a HARD FAIL, defense-in-depth ahead of
  the join itself); a raw id matching MORE than one is likewise a HARD FAIL
  (should be structurally impossible given the mutual-exclusivity self-test,
  but asserted defensively per-id too). For every raw id that DOES match
  exactly one recognized family, the crosswalk join MUST resolve to EXACTLY
  ONE `w000xxx`; the join HARD-FAILS (never silently disambiguates, never
  picks the first match) if it resolves to zero matches (a HARD FAIL, never
  a silent skip — a crosswalk miss on a previously-registered id indicates
  `ref_corpus_v2.pkl` drifted between census-generation time and build
  time) OR to more than one match (unexpected cardinality — a HARD FAIL).
  This reuses and sharpens the existing crosswalk join-safety discipline
  (Codex #9 — join only via stable, PINNED, namespaced ids, never normalized
  titles, hard-fail unexpected cardinalities in EITHER direction).

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
- **Worked-example validation (Pitfall 5) — EXPLICIT PRODUCTION PRECONDITION
  (Codex R2-MEDIUM: elevated from an advisory note to a required gate).**
  `chronological_demotion_rule.md`'s "Worked examples" table lists four
  illustrative cases; only ONE (work pair `w001159`/`w000177`, the single
  `chronological_rule_examples` entry in the census) is currently
  operationalized against real data with concrete
  `span_jac`/`co_pages`/`breadth` evidence. The other three remain
  qualitative illustrations. **Before the v2 asset ships (i.e. before 135-07's
  production bake is treated as ready for Phase 135 grading), 135-06/135-07
  MUST spot-check AT LEAST ONE FURTHER worked example (beyond `w001159`/
  `w000177`) against the REAL bake output** — this is a required production
  precondition, not an optional/advisory suggestion; the v2 frame doc
  (`discovery-frames-v2.md`) MUST record which examples were checked against
  real data and which remain qualitative/unverified, and 135-07's own
  verification step is INCOMPLETE without this record present.

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
