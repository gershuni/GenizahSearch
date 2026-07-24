# CERT-01 Tier-A Precision Certificate — Pre-Registered Protocol (v1)

**Status: PRE-REGISTERED, awaiting v2 frame freeze.** Authored 2026-07-24 (Phase
135, plan 135-03, Track A — census-independent). This document fixes every
CERT-01 measurement parameter in writing, BEFORE any card is drawn, so the
actual measurement (135-09, run against the built `discovery-v2` frame) is
honest. It is the WRITTEN half of CERT-01; the frame-freeze artifact
(`cert01_prereg.json`), the OC-table computation, and the card draw itself all
run later in 135-09 — this document specifies exactly how, so 135-09 has no
discretion left to exercise.

**Masking:** every reference-corpus work is cited only by its opaque `w000xxx`
product id (never a title), and by count or numeric year where relevant. The
restricted corpus is referenced only by its internal codename, never its real
name. This document is committed and gated by `check_atlas_masking.py
--scan-repo` on every commit.

**Tunable ONLY by versioning this artifact** (same discipline as
`docs/specs/discovery-budgets.md` / `discovery-band-labels-v1.md`). Any
correction after this document is committed requires a new dated amendment
section (§12), never a silent edit — and if a correction changes anything a
hash covers (§5), that hash necessarily changes too, which is the whole point
of the freeze discipline.

**Precedent reused wholesale (D-09):** this protocol is CERT-01's instance of
the `same_work_spike/probe` E1/Q2 adjudication harness — the same reveal-lock
deck mechanics, gold-repeatability gate, physMS-clustered bootstrap, pre-outcome
OC table, and freeze-manifest discipline that produced R-A (0.889, confirmed
Broad ≥0.60) and R-CANON-NQ (0.647, FAILED Broad ≥0.60 → shipped screening).
Template sources: `same_work_spike/probe/results/PLAN-e1-round2.md` (§ headings,
freeze-manifest shape, OC-table method) and `PLAN-e1-round3-canon.md` (gold
repeatability gate + measured-below-floor outcome branch — the exact pattern
CERT-01's FAIL branch mirrors, at a stricter floor). Worked outcome precedent:
`E1-ROUND2-RELEASE.md` (PASS example) and `E1-ROUND3-RELEASE.md` (FAIL example
— "Broad 0.60 certification: FAIL → band designated SCREENING", the FAIL wording
this protocol generalizes to Strict 0.85 and a real routing flip).

---

## 0. Objective

Measure the precision of the shipped `tier_a` band — ~89% of the `discovery-v2`
spine, currently `unreviewed`/`n/a` audit status, no precision number — against
ONE pre-registered decision rule (**Strict**, physMS-clustered lower confidence
bound ≥ **0.85**; D-07), so:

(a) `tier_a` can either promote out from behind the "show more possible matches"
    toggle (D-18) with an honest, CI-backed precision number, or
(b) the FAIL branch fires a TESTED, pre-registered reband (never an ad hoc
    relabel) that keeps the default view trustworthy, or
(c) an insufficient-evidence / wide-CI outcome keeps `tier_a` exactly where it
    already is (behind the toggle, no promotion, no reband) pending the
    pre-reserved confirmation draw.

No outcome may ever use the word "certified" (D-06; `docs/specs/discovery-band-labels-v1.md`
§0/Rule 1). The published posture on PASS is **"expert-measured · independent
audit pending"** — parity with R-A's existing 0.889 status, never a stronger claim.

---

## 1. Frozen estimand

### 1.1 Population membership (D-05)

**Estimand = the shipped, display-deduplicated `(page_id, canonical_work_id)`
population, sampled AFTER:**

1. canonical merges (`v2_canonical_merges.json` — 16 owner-ratified merges,
   each populating `works.canonical_work_id`; D-13),
2. the w001239 drop (RCh-Shabbat Sefaria copy excluded entirely at bake; the
   canonical representative for that merge group flips to the M-source id per
   D-14 — the one documented exception to "canonical = Sefaria representative"),
3. Lever-1 coverage routing (`docs/specs/discovery-band-labels-v1.md` §3.1 —
   page-level coverage < 0.45 → `routing_status='review_only'`; ≥ 0.45 → shipped),
4. the D-17 chronological co-claim demotion (later-dated co-claimants on an
   overlapping shared span demoted to `routing_status='review_only'`, tagged
   `later_shared_text`; DELTA = 100 years, per the 2026-07-23 date-coverage
   audit, D-19), and
5. the display-evidence dedup selection below (§1.2).

Only rows that survive ALL FIVE steps AND resolve to `confidence_band='tier_a'`
AND `routing_status='shipped'` enter the CERT-01 sampling frame. A row that is
`review_only` for any reason (coverage, `later_shared_text`, or any other
routing_reason) is excluded from the frame — CERT-01 measures what a user
actually sees by default once `tier_a` promotes, not the full unfiltered
`tier_a` band as originally baked.

### 1.2 Frozen dedup/ranking SQL — multi-raw-claim collisions after canonical merge

A canonical merge can put TWO (or more) raw `discovery_claim` rows — each its
own `(page_id, work_id)` — under the SAME `(page_id, canonical_work_id)` key
(e.g. one raw claim via the M-source-side work_id, one via the Sefaria-side
work_id, both merged to one `canonical_work_id`). The estimand is built by
picking exactly ONE surviving representative claim per `(page_id,
canonical_work_id)`, using the SAME deterministic display-evidence precedence
lattice already frozen in `docs/specs/discovery-sidecar-schema-v1.md` §6 (no new
ranking rule is invented — the collision-resolution rule below is that lattice
applied ACROSS claims instead of across evidence rows within one claim):

```sql
WITH claim_display AS (
  -- one row per surviving (page_id, work_id) claim, carrying its own
  -- already-selected display_evidence_id's band attributes (§6 of the
  -- sidecar schema; NOT recomputed here, just joined)
  SELECT
    dc.page_id,
    w.canonical_work_id,
    dc.work_id,
    dc.claim_id,
    de.evidence_id           AS display_evidence_id,
    de.evidence_source,
    de.confidence_band,
    de.adjudication_status,
    de.routing_status,
    de.routing_reason,
    w.source_corpus
  FROM discovery_claim dc
  JOIN works w               ON w.work_id = dc.work_id
  JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
  WHERE de.routing_status = 'shipped'
    AND w.work_id NOT IN (/* w001239 drop-list, §1.1 step 2 */)
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY page_id, canonical_work_id
      ORDER BY
        -- (i) family-specific human_confirmed dominance (schema §6 rule 1)
        CASE WHEN evidence_source = 'track1_direct'
               AND adjudication_status = 'human_confirmed' THEN 0 ELSE 1 END,
        -- (ii) global band-rank, strongest first (schema §6 rule 2, verbatim)
        CASE evidence_source || ':' || confidence_band
          WHEN 'track1_direct:expert_verified'          THEN 1  -- v2: high_confidence_algorithmic
          WHEN 'track1_direct:high_confidence_algorithmic' THEN 1
          WHEN 'track1_direct:tier_a'                    THEN 2
          WHEN 'propagated:corroborated'                 THEN 3
          WHEN 'track1_direct:screening_rb'              THEN 4
          WHEN 'track1_direct:screening_canon'           THEN 5
          WHEN 'propagated:weak'                         THEN 6
          WHEN 'propagated:not_evaluated'                THEN 7
          ELSE 99
        END,
        -- (iii) adjudication_status tie-break (schema §6 rule 3)
        CASE adjudication_status
          WHEN 'human_confirmed' THEN 0
          WHEN 'provisional'     THEN 1
          WHEN 'unreviewed'      THEN 2
          ELSE 9
        END,
        -- (iv) evidence_id lexicographic tie-break (schema §6 rule 4 — final,
        -- deterministic, never reached in practice because evidence_id is
        -- already unique per claim and (ii)/(iii) resolve almost every case)
        display_evidence_id ASC
    ) AS rn
  FROM claim_display
)
SELECT * FROM ranked WHERE rn = 1;
```

The **CERT-01 sampling frame** is this result set filtered to
`confidence_band = 'tier_a'`. Both the losing raw claim(s) at a collided key
and the evidence rows behind them persist in the sidecar (nothing is deleted);
they are simply not the DISPLAY row a grader would ever see, so they are
correctly excluded from what CERT-01 measures.

### 1.3 Page → physical-MS cluster mapping, frozen and hashed

The confidence interval is clustered by physical manuscript, not by page
(pages within one physical MS are not independent — E1 precedent, §1.5). The
cluster key for a page is the SAME `unit_key` DATA-10 already uses for
display-time projection (`docs/specs/discovery-sidecar-schema-v1.md` §1.4 /
`discovery-frames.md` §6):

```
unit_key(a_page_id) = COALESCE(
    (SELECT wum.unit_id FROM witness_unit_members wum
       WHERE wum.sys_id = sys_id_of(a_page_id)),
    'sys:' || sys_id_of(a_page_id)
)
```

i.e. a page whose `sys_id` belongs to a multi-part `witness_units` cluster maps
to that cluster's `unit_id`; a page whose `sys_id` is not a unit member maps to
a synthetic per-manuscript singleton key `'sys:' + sys_id`.

**`cluster_map_hash`** = SHA-256 over the sorted list of
`(page_id, canonical_work_id, unit_key)` triples for every row in the CERT-01
estimand (§1.1/§1.2), serialized one triple per line as
`{page_id}|{canonical_work_id}|{unit_key}`, sorted lexicographically by the
whole line, UTF-8 encoded, newline-joined, then SHA-256'd.

**Why this hash exists as its own field (Codex #13):** `frame_content_hash`
(the sidecar's existing membership hash) and `population_hash` (§5) both key on
`(page_id, canonical_work_id, stratum)` — neither one encodes WHICH physical-MS
cluster a page belongs to. A future change to `witness_units` /
`witness_unit_members` (a re-run of the physical-join/oxford-part clustering)
could silently change the physMS-clustered bootstrap's variance — and therefore
its confidence interval and the Strict 0.85 pass/fail decision — while
`frame_content_hash` and `population_hash` stay bit-identical. `cluster_map_hash`
is the ONLY hash that would catch that. `verify_cert01_grading.py` (135-09)
recomputes it from the deployed sidecar and compares (§5.5).

### 1.4 Cross-corpus stratum tie-break

Strata are keyed on `source corpus × coverage band` (§3). For the SURVIVING
representative claim at a given `(page_id, canonical_work_id)` key (§1.2), the
corpus label is `works.source_corpus` of that winning row's own `work_id` — this
is unambiguous in the overwhelming majority of cases, because the winning row is
one specific raw claim with one specific `works.source_corpus`.

The tie-break is needed only for the residual case: a canonical work whose
merged raw members span more than one `source_corpus` (e.g. a 12-tractate
Rabbeinu-Chananel-style merge spanning both an M-source raw id and a Sefaria raw
id) needs ONE fixed corpus label to stratify the canonical work as a whole
(e.g. when pre-declaring a card's stratum before its specific per-page winning
claim is looked at, or in the rare case where the §1.2 lattice ties EXACTLY
across raw claims from different corpora at the same page). Frozen rule,
applied deterministically:

```
corpus_rank = {sefaria: 1, ja: 2, msource: 3}
resolved_stratum_corpus(canonical_work_id) =
    the source_corpus, among all source_corpus values contributing at least
    one SHIPPED raw claim to this canonical_work_id, with the LOWEST
    corpus_rank (i.e. sefaria wins over ja, ja wins over msource)
```

This is a fixed priority order (sefaria < ja < msource), never a probabilistic
or per-card choice, and is computed once per `canonical_work_id` at frame-freeze
time — it is part of the frozen frame, not re-derived per card.

### 1.5 Unit of measurement and CI method

**Unit = `(page, work)`** — deliberately NOT whole-manuscript (D-05): a
manuscript can carry many different identifications across its pages
(citation-heavy MSS especially), so a whole-MS verdict blurs which claim is
being judged. Each `(page_id, canonical_work_id)` claim is graded independently.
**Multi-register is preserved** — several identifications can be TRUE for one
page (e.g. Bible + Targum + Judeo-Arabic Tafsir occupying non-overlapping
spans); CERT-01 never collapses a page to "one work."

**CI method = physical-MS-clustered percentile bootstrap, B=10,000**, over the
`unit_key` clusters of §1.3 — the SAME mechanism E1 used
(`same_work_spike/probe/scripts/e1_deck.py::components_of` builds the
cluster→card membership map; `e1_deck.py::comp_bootstrap` resamples clusters
with replacement B=10,000 times and reports the 2.5th-percentile one-sided
lower bound). `wilson_bounds` (same module) is retained only for the fallback
degenerate-cluster diagnostic (E1 round-3 precedent: when a bipartite
work↔physMS dependency structure degenerates into one giant component, switch
the clustering unit — not applicable here since CERT-01's clustering unit is
already fixed to physMS from the start, but the fallback check is run anyway as
a sanity gate: if `K_eff` on the realized draw is implausibly low relative to
the drawn card count, that is reported as a deviation, §11).

---

## 2. Decision rule

**ONE pre-registered decision rule, Strict: physMS-clustered lower confidence
bound ≥ 0.85** (D-07). Tested ONCE, k=1, no multiple-testing correction needed
because there is exactly one gate. This mirrors `shared/discovery_band_labels.py::STRICT_FLOOR
= 0.85` — the code-side constant the values module already reads (Phase 135
plan 135-01); CERT-01 is the measurement that eventually feeds a real
`ci_low`/`ci_high` pair to compare against it.

Balanced/Broad thresholds are NOT computed as separate gates here — unlike the
E1 rounds, which used Broad (≥0.60) as their primary certifiable gate and
treated Strict as descriptive-only, CERT-01's OWNER-CHOSEN gate for the bulk
`tier_a` band is Strict from the start (D-07: "owner accepts the demotion risk").
Descriptive percentile clearances at 0.60/0.75 MAY still be reported alongside
the primary result for context (as E1 always did), but they carry NO decision
weight.

---

## 3. Strata and weights

**Strata = source corpus (Sefaria / Judeo-Arabic / M-source) × coverage band
(high ≥ 0.60 / medium 0.45–0.60)** — both reliably known BEFORE grading and
strongly predictive of outcome (D-08). Both axes are mutually exclusive and
exhaustive over the frame (§1.1 step 3 already excludes coverage < 0.45 from the
frame entirely, so only the high/medium coverage bands remain to stratify on).

Weights are the INVERSE draw-inclusion design weights (E1 convention):
`w_stratum = N_stratum / n_scored_stratum`, where `N_stratum` is the frozen
frame's stratum size and `n_scored_stratum` is the number of cards actually
drawn+scored from that stratum (any verdict including indeterminate/INS
counts as scored). Weights are recorded at freeze from the REALIZED stratum
sizes of the shipped tier_a frame (§1.1), allocated proportionally across the
~200–250-card deck (§7) so no stratum is starved.

**Work-category is a DIAGNOSTIC breakdown only, never a weighting stratum**
(D-08) — genre/category signal is unreliable (dropped as a weighting input in
134-07 for the same reason) and is reported descriptively (per-category raw
counts, E1 round-3 convention: "raw counts only, no CIs, no subgroup claims").

**Later-shared-text quotation-FP diagnostic family (D-08):** every drawn card is
ALSO tagged (internally, never shown to the grader) with whether it was a
`later_shared_text`-demotion CANDIDATE that survived as shipped anyway (i.e. it
was NOT demoted because no overlapping earlier co-claimant existed on its span)
versus a card with no D-17 exposure at all. This tag is never a grading input
and never influences the stratum weights above — it exists solely to feed the
blinded diagnostic sample (§8).

---

## 4. Seed, blindness, gold treatment, exclusions

- **Blindness:** catalogue-blind with logged reveals (E1 convention) — graders
  see the manuscript image + transcription and the claimed work's neutral
  title, NEVER the catalogue's own cataloguer-assigned identification, and
  NEVER whether a card carries the `later_shared_text` tag (D-08 — graders stay
  blind to the demotion classifier; §8 explains why).
- **Gold treatment:** every rendered deck embeds a gold block of
  previously-adjudicated cards re-presented blind, scored against the grader's
  own locked prior verdict (intra-rater repeatability, "necessary, not
  sufficient" — PLAN-e1-round3-canon.md §3 convention). Gold-block sizing and
  quota cells are computed at freeze from the actual cluster-disjoint gold
  inventory available at that time (135-09; not fixed here because the
  inventory depends on the final v2 frame).
- **Exclusion/indeterminate rules:** the standard E1 rubric (A / B / C / INS),
  A vs (B∪C) as the primary outcome; INS (indeterminate — image illegible,
  ambiguous claim, ties in the grader's judgment) is excluded from the
  determinate denominator but reported as an INS rate; every non-A verdict
  requires exactly one reason code (E1 convention), never used in the primary
  analysis, retained for diagnostic reading only.
- **Seed:** the discovery-deck RNG seed, gold-shuffle seed, and bootstrap seed
  are frozen integers recorded in `cert01_prereg.json` at freeze (135-09) — not
  invented here, because they must be drawn fresh for the v2 frame and then
  immutably fixed; this document specifies that they MUST exist as named,
  hashed fields in the pre-registration payload (§5.2) before any card renders.

---

## 5. Freeze discipline — immutable pre-registration payload

**The freeze is proven by a content hash, NOT by git-commit ordering (Codex
#B1).** A git commit timestamp is not evidence that a parameter was fixed
before a card was drawn — files can be amended, rebased, or their commit dates
otherwise made unreliable as an audit trail. The freeze instead rests on an
IMMUTABLE PAYLOAD whose own hash is verifiable independently of any git history.

### 5.1 `cert01_prereg.json` (written by 135-09, specified here)

135-09 writes a TRACKED, committed, immutable JSON file `cert01_prereg.json`
recording, at minimum:

- `protocol_sha256` — the SHA-256 of THIS protocol document's file contents at
  the moment `cert01_prereg.json` is written (so the pre-registration is
  cryptographically bound to this exact written spec, not a future edit of it).
- `seed` — the frozen RNG seeds (§4).
- `frame_content_hash` — the sidecar's existing membership-based hash
  (`docs/specs/discovery-sidecar-schema-v1.md`), recomputed over the deployed
  v2 sidecar.
- `population_hash` — SHA-256 over the sorted `(page_id, canonical_work_id,
  stratum)` triples of the CERT-01 estimand (§1.1/§1.2/§3) — narrower than
  `frame_content_hash` (which covers the WHOLE sidecar's claim membership, not
  just the tier_a CERT-01 frame).
- **The FOUR frozen input hashes:**
  1. `canonical_merges_sha256` — SHA-256 of `v2_canonical_merges.json` (the
     owner-ratified twin-merge/drop handoff, D-13).
  2. `composition_dates_sha256` — SHA-256 of the M-source composition-date
     table used by the D-17 demotion rule (owner-held, external; referenced
     functionally only, never rendered).
  3. `seftja_dates_sha256` — SHA-256 of `seftja_dates.json` (the 407 interim
     Sefaria/JA dates, D-19), the OTHER date input the D-17 rule joins.
  4. `db_content_hash` — the DEPLOYED v2 sidecar's own whole-file `content_hash`
     (the same field named `DB content_hash` in `discovery-frames.md` §1) — NOT
     `frame_content_hash` (a membership hash); this is the literal content hash
     of the shipped `.db` asset actually being measured against.
- `crosswalk_sha256` — SHA-256 of `discovery_data/crosswalk.json` (the raw→opaque
  `w000xxx` id mapping every hash above and the estimand itself depend on).
- `cluster_map_hash` — §1.3, computed over the estimand rows.
- All cutoffs — the Strict 0.85 floor (§2), the Lever-1 coverage cliff (0.45,
  already baked upstream but restated here for audit completeness), the D-17
  `DELTA=100yr` demotion threshold (already baked upstream, restated here).
- Strata weights (§3) as realized at freeze.
- Gold/confirmation allocations (§4, §7) as realized at freeze.

### 5.2 The `report_id` construction (self-referential, finite, well-defined)

`report_id` is computed by: (1) serializing the payload above to canonical JSON
(sorted keys, no extraneous whitespace) WITH the `report_id` key itself OMITTED;
(2) taking the SHA-256 hex digest of that serialization; (3) inserting the
resulting digest back into the payload as the `report_id` field. This is finite
and well-defined — the hash is computed over the payload minus its own field,
so there is no circularity, and the same procedure re-run over the SAME
(minus-report_id) payload always reproduces the same `report_id`, which is
exactly what makes it a useful integrity check.

**The immutable, citable report identifier is `cert-tier_a-<report_id>`**
(the `cert-tier_a-<hash>` form named in D-10), where `<report_id>` is the hex
digest from step (2). This is the identifier the BAND-05 methods page cites,
stable from the moment the pre-registration is written through however long
grading takes to complete — the id does not change when the measurement lands,
only its `status` field does (methods-page convention already established by
BAND-02/BAND-05).

### 5.3 The pre-registration is never mutated after the draw

Once `cert01_prereg.json` is committed and its `report_id` computed, the file is
never edited in place. If a genuine deviation occurs (an unforeseen data issue,
a necessary protocol adjustment), it is recorded in this document's Deviations
Register (§11) as a NEW dated entry — never a retroactive change to the frozen
payload. `cert01_prereg.json`'s own immutability is what lets the freeze-before-
draw claim be verified by hash recomputation rather than trusted on the
strength of a stated intention.

### 5.4 The deck manifest is a SEPARATE tracked artifact

The deck binding — which specific cards were drawn, in what order, under which
stratum allocation — lives in a SEPARATE tracked file, `cert01_deck_manifest.json`,
written AFTER `cert01_prereg.json` exists. `cert01_deck_manifest.json` records
its own `deck_manifest_hash` and REFERENCES the pre-registration's `report_id`
by value (Codex #B1). This separation is deliberate: it lets
`verify_cert01_grading.py` prove "the deck was drawn against an
ALREADY-FROZEN, hash-stable pre-registration" by checking that the deck
manifest's referenced `report_id` matches the RECOMPUTED `report_id` from
`cert01_prereg.json` — a proof by hash equality, not by inspecting commit
timestamps.

### 5.5 `verify_cert01_grading.py` (135-09) — recompute, never trust

135-09 ships `verify_cert01_grading.py`, which RECOMPUTES and compares every
one of the hashes above against the deployed sidecar and the frozen input
artifacts (Codex #B3/#13):

- Recomputes `frame_content_hash` and `db content_hash` from the deployed
  `discovery-v2` `.db` file and compares against the values stored in
  `cert01_prereg.json`.
- Recomputes `population_hash` and `cluster_map_hash` from the estimand SQL
  (§1.2) and the cluster mapping (§1.3) run live against the deployed sidecar,
  and compares.
- Recomputes `canonical_merges_sha256`, `composition_dates_sha256`,
  `seftja_dates_sha256`, and `crosswalk_sha256` from the actual input files on
  disk and compares.
- Recomputes the self-referential `report_id` from the stored payload (minus
  its own `report_id` field) and confirms it equals the stored `report_id`
  (proves the file was not hand-edited after the fact).
- Confirms `cert01_deck_manifest.json`'s referenced `report_id` equals the
  recomputed one.

Any mismatch is a HARD FAIL — grading cannot be scored as certifiable evidence
against a pre-registration that does not match the artifacts it claims to
describe. This is exactly the defense `cluster_map_hash` exists for (§1.3): a
`witness_units` rebuild that changes physMS clustering would leave
`frame_content_hash`/`population_hash` unchanged but would flip
`cluster_map_hash`, and `verify_cert01_grading.py` would catch it.

---

## 6. Pre-outcome operating-characteristics (OC) table

**Mandatory before any card is drawn** (E1 convention, honored without
exception — Pitfall 8 in `135-RESEARCH.md` names this explicitly). CERT-01's
Strict 0.85 floor at ~200–250 cards is a MATERIALLY HARDER statistical target
than anything the E1 harness has cleared to date: `PLAN-e1-round2.md`'s own
Objective section notes that with a ~0.788 tier-A control ceiling, "a ≤400-card
confirmation needs ~0.81 discovery lower bound for Balanced, ~0.90 for Strict —
implausible," which is exactly why E1 targeted Broad (≥0.60) as its primary
certifiable gate. CERT-01 deliberately takes on the harder Strict floor because
`tier_a` is the bulk 89%-of-spine band and the owner wants a stricter bar before
it becomes the default view (D-07) — but the sample-size math means the
pre-outcome table must be run and published BEFORE the deck renders, and a low
joint pass-probability is a signal to negotiate card count or accept the
insufficient-evidence branch — never a reason to skip the OC step (Pitfall 8's
own "how to avoid").

**Method (135-09, cited here so 135-09 has no discretion to skip it):** reuse
`same_work_spike/probe/scripts/e1_confirm_sizing.py` in full —
`anova_icc` (one-way ANOVA intraclass correlation on the realized physMS
component structure of the frozen `tier_a` frame), `n_det_required` (smallest
determinate-card count clearing the target floor at ~80% power via exact
Wilson-bound enumeration), `wilson_lower_one_sided` (the closed-form one-sided
bound the sizing search tests against), and `size_confirmation` (the full
frozen sizing algorithm — screens for a discovery lower bound below the Strict
threshold, computes ICC-adjusted design effect, returns either a workable
confirmation `n_drawn` or a `screening=True` flag with a stated reason).

**Grid (135-09 computes the actual cells against the real v2 frame; the grid
SHAPE is frozen here):** true precision `p ∈ {0.80, 0.85, 0.90, 0.95}` (bracketing
the 0.85 floor from below and above) × physMS-cluster ICC as REALIZED on the
frozen `tier_a` frame (not assumed) × INS rate `∈ {0%, 10%, 20%}` (E1's own
bracketing convention). For each cell, report: (i) the joint probability of
clearing the discovery-stage Strict-lower-bound gate at ~200–250 cards, (ii) the
`size_confirmation` sizing outcome (a finite confirmation `n_drawn`, or
`screening=True`), (iii) the conditional confirmation pass probability given
(i). This is EXPECTATIONS-SETTING ONLY (E1 convention) — the deck runs
regardless of what the table says; the table is published pre-outcome so the
Strict floor's difficulty is visible in writing before any verdict exists, and
so a structurally-unwinnable cell (e.g. `p=0.85` with realized ICC pushing
`size_confirmation` to `screening=True` at every INS rate) is a known, disclosed
risk rather than a surprise discovered after grading has already begun.

---

## 7. Deck and confirmation draw

**~200–250 cards**, stratified per §3, drawn from the frozen `tier_a` estimand
(§1). A pre-reserved CONFIRMATION draw is sequestered at the SAME time as the
discovery deck (E1 convention: the reserve's row order is frozen and hashed
into `cert01_prereg.json` before any discovery card renders) — entered only if
discovery clears the Strict 0.85 lower bound; its exact size is set by
`size_confirmation`'s output (§6), not invented ad hoc after discovery results
land.

All standard E1 draw discipline applies: SRSWOR within stratum, ledger-on-draw
(every drawn row, scored or not, is recorded so it can never be redrawn),
reveal-lock (primary verdict locks before the catalogue/reference reveal, when
a reveal occurs at all), and a zero-overlap audit between the discovery draw,
the confirmation reserve, and the blinded diagnostic sample (§8) — any
prohibited overlap fails closed (draw discarded, incident recorded per §11).

---

## 8. Blinded diagnostic sample (demoted + retained `later_shared_text`)

A SEPARATELY-IDENTIFIED sample (or a reserved oversample) spans BOTH:

- **Demoted** cards — rows the D-17 chronological demotion rule routed to
  `routing_status='review_only'`, tagged `later_shared_text`, because a
  materially-earlier (≥ 100yr) co-claimant shares the overlapping span; and
- **Retained** cards — rows that were CANDIDATES for the same demotion logic
  (an overlapping shared-span co-claim exists) but SURVIVED as shipped because
  no materially-earlier co-claimant existed on that span, or the co-claim fell
  within the 100yr tie window.

Both groups are drawn and rendered to the SAME grader under the SAME blind
conditions as the main deck — the grader never sees the `later_shared_text`
tag, before or after grading (§4). Only AFTER the grader's verdicts are LOCKED
(reveal-lock honored) is the hidden classifier tag joined back in, off-line,
by the analysis script — never by the grader, never before verdict lock.

This produces, purely as a POST-HOC classifier-validation exercise:

- **Coverage** — of all pages that genuinely warranted demotion (per the
  grader's blind verdict, not the D-17 heuristic), what fraction did D-17
  actually demote?
- **PPV (precision of the demotion)** — of all pages D-17 demoted, what
  fraction did the grader agree were genuinely a later-work quotation, not a
  real independent witness?
- **Sensitivity** — same construction from the opposite direction.

**This is reported ONLY as classifier validation, never as adjudication
evidence for the tier_a precision estimate itself** (D-08, avoids circularity:
using the demotion tag's own accuracy to justify the demotion tag would be
self-referential). The tier_a estimand (§1.1) already EXCLUDES all
`later_shared_text`-demoted rows before grading begins — this diagnostic sample
exists purely to measure whether that upstream exclusion is doing its job, on a
population that was never part of the CERT-01 precision estimand itself.

---

## 9. Outcome branches and release copy

### 9.1 PASS — Strict lower bound ≥ 0.85

Posture: **"expert-measured · independent audit pending"** (D-06) — never
"certified." The measured number + CI populate `band_precision` at
`scope='band'`, `evidence_source='track1_direct'`, `confidence_band='tier_a'`
(no code change needed — BAND-02 already reads this table data-driven,
`docs/specs/discovery-sidecar-schema-v1.md` §1.6). `tier_a` promotes out from
behind the "show more possible matches" toggle (D-18) and joins the default
view alongside `human_confirmed` rows and the already-measured 0.889 top
algorithmic band.

### 9.2 FAIL — measured below the Strict floor

**FAIL action (pre-registered, TESTED, never an ad hoc relabel — Codex #7,
#B2):** the SAME 135-06 build path that populates `band_precision`
(`scripts/build_discovery_sidecar.py`, its existing `--precision-spec` /
`_resolve_band_precision_spec` machinery) is re-run with an explicit reband
input that:

1. **Flips `routing_status` to `review_only`** on every `discovery_evidence`
   row belonging to the affected `tier_a` claims — this drops them from the
   default-shown surface (`docs/specs/discovery-band-labels-v1.md` §4).
2. **Rebands those rows' `confidence_band` from `tier_a` to `screening_rb`.**
   `screening_rb` is the ONE semantically-correct target: the frozen
   `track1_direct` band enum (`docs/specs/discovery-sidecar-schema-v1.md`,
   "Frozen Enum Vocabularies") is
   `{expert_verified/high_confidence_algorithmic, tier_a, screening_rb,
   screening_canon}` — there is **NO `screening`** key anywhere in the frozen
   vocabulary, and the FAIL branch never emits one. `screening_canon` is
   reserved for the canon-caveated recovery lane (D-10's canon caveat, distinct
   population, distinct pre-registered precision 0.647) and is NOT the target
   here. `screening_rb` — "Screening — rule-based" — is the algorithmic,
   density/rule-based screening band, which is exactly what a
   measured-below-Strict `tier_a` population becomes: still algorithmically
   scored, no longer trusted at the default-shown Strict bar. Both
   `screening_rb` and `screening_canon` sit behind the same "show more possible
   matches" toggle (`docs/specs/discovery-band-labels-v1.md` §4), so either
   would technically drop `tier_a` from the default view — but only
   `screening_rb` is the semantically-honest destination.
3. **Is fed as a REBUILD INPUT, never a bare in-place `UPDATE
   confidence_band`.** The reband must run BEFORE `evidence_id` generation and
   BEFORE `display_evidence_id` selection in the SAME build pass (Codex-R4
   new-HIGH): `evidence_id` is a frozen hash over a tuple that INCLUDES
   `confidence_band` (`scripts/discovery_ids.py::evidence_id` —
   `"...{evidence_source}|{confidence_band}|..."`), so changing a row's band
   without regenerating its `evidence_id` would leave a content-inconsistent id
   (an `evidence_id` whose hash input no longer matches its own stored
   `confidence_band`) and a stale `display_evidence_id` pointer on the parent
   claim. The FAIL rebuild therefore reruns the full evidence-id-mint →
   display-evidence-select pipeline (`select_display_evidence`,
   `docs/specs/discovery-sidecar-schema-v1.md` §6) over the rebanded rows, so
   every downstream `evidence_id` and `display_evidence_id` recomputes
   consistently. A direct `UPDATE discovery_evidence SET confidence_band=...`
   is explicitly PROHIBITED as a FAIL-branch implementation.
4. **Atomically invalidates the target `screening_rb` band's LEGACY
   precision** (Codex #B2): because the rebanded rows CHANGE the
   `screening_rb` population (it now contains additional rows that were never
   part of the population the pre-registered 0.859 was measured on,
   `docs/specs/discovery-band-labels-v1.md` §3), the existing
   `band_precision` row for `scope='band'`, `evidence_source='track1_direct'`,
   `confidence_band='screening_rb'` is set to a `measurement_status` of
   `'not_measured'` with `precision`/`ci_low`/`ci_high` all set to `NULL`, IN
   THE SAME BUILD TRANSACTION as the reband — never a separate follow-up step,
   and never left stale. **No combined/pooled number is ever fabricated**
   (Codex #B2's core prohibition) — the legacy 0.859 was measured on the
   ORIGINAL `screening_rb` population and is invalid the instant that
   population changes; there is no valid post-hoc arithmetic that produces a
   correct combined estimate from the old measurement plus the newly-added
   rows. `screening_rb` ships with an explicit "precision not yet measured"
   status (mirroring the existing `tier_a` "not yet measured" convention,
   `docs/specs/discovery-band-labels-v1.md` §3 row) until a future measurement
   round re-certifies it. **Implementation note for 135-06:** the frozen
   `band_precision` schema (`docs/specs/discovery-sidecar-schema-v1.md` §1.6)
   does not yet carry a `measurement_status` column — 135-06 must land a
   versioned amendment adding it (a new dated amendment section in that spec,
   never a silent edit), OR represent invalidation via the existing NULL
   `precision`/`ci_low`/`ci_high` columns plus a `notes` field recording the
   invalidation event and its cause; whichever representation 135-06 adopts,
   the OBSERVABLE CONTRACT is identical — a UI reading `band_precision` for
   `screening_rb` after a FAIL reband must render "not yet measured," never the
   stale 0.859.

### 9.3 Insufficient evidence / wide CI

Distinguished from a genuine FAIL (Codex #13/MEDIUM-13's "measured-below-floor
vs insufficient-evidence" distinction, mirrored from E1's own OC-driven
disposition language): if the discovery draw's lower bound straddles 0.85 with
a CI too wide to call (e.g. the point estimate sits near or above 0.85 but the
lower bound has not yet cleared it, and `size_confirmation` (§6) returns a
workable, not-yet-exhausted confirmation `n_drawn`), `tier_a` is NOT rebanded.
It stays exactly where D-18 already put it — behind the "show more possible
matches" toggle, non-default, un-relabeled — pending the pre-reserved
confirmation draw (§7). No permanent action is taken on an inconclusive result;
only a definitive measured-below-floor outcome (§9.2) triggers the reband.

---

## 10. Phase-closing signal — "grading STARTED"

Per D-02 (Phase 135 closes when grading has STARTED, not completed) and
`135-RESEARCH.md` Pitfall 7's recommended objective definition, the phase-close
signal for CERT-01 is the conjunction of THREE mechanically-checkable artifacts,
none of which require waiting for the measurement to finish:

1. `cert01_prereg.json` exists, is committed, and its `report_id` recomputes
   correctly (§5) — the pre-registration is FROZEN.
2. `cert01_deck_manifest.json` exists, is committed, references the SAME
   `report_id`, and its `deck_manifest_hash` recomputes correctly — the deck is
   RENDERED against the v2 frame.
3. At least one verdict is recorded in the deck's verdicts/ledger file — grading
   has genuinely BEGUN, not merely been scheduled.

All three are keyed off tracked, hash-verifiable artifacts (never a verbal
assertion at review time) — exactly Pitfall 7's own prescription.

---

## 11. Deviations register

None at authoring time (2026-07-24). Any deviation discovered during 135-09's
execution (frame freeze, OC computation, or the actual card draw) MUST be
appended here as a NEW dated entry, in the same discipline as the E1 templates'
own deviations registers (`PLAN-e1-round2.md` §"Deviations register",
`PLAN-e1-round3-canon.md` §11) — never a silent edit to the sections above.

---

## 12. Cross-references

- `docs/specs/discovery-sidecar-schema-v1.md` — frozen enum vocab (§"Frozen
  Enum Vocabularies"), two-table claim model (§1), frozen id recipes (§2),
  display-evidence precedence lattice (§6), `band_precision` table (§1.6).
- `docs/specs/discovery-band-labels-v1.md` — band display labels (§2),
  precision presentation rules (§3), Lever-1 coverage bands (§3.1),
  default-shown policy + toggle (§4), the v2 enum-rename lockstep (§5).
- `docs/specs/discovery-frames.md` — the v1 reference build (SUPERSEDED-PENDING);
  `docs/specs/discovery-frames-v2.md` (to be written at the v2 bake) is the
  frame this protocol's estimand actually measures against.
- `docs/specs/discovery-v2-bake-plan.md` — STALE per `135-CONTEXT.md`
  (superseded by the D-13..D-19 decisions cited throughout this document — the
  16-merge census, the D-17 chronological demotion, and the abandonment of any
  enumerated `work_relations` table); the bake-plan rewrite is a separate,
  later task and is not authored by this plan.
- `.planning/phases/135-precision-certificate-confidence-bands/135-CONTEXT.md`
  — D-05 (estimand), D-06 (posture), D-07 (Strict gate + FAIL action), D-08
  (strata + blindness), D-09 (harness reuse), D-10 (report id + methods page),
  D-13/D-14 (canonical merges + RCh-Shabbat resolution), D-17 (chronological
  demotion rule), D-18 (tier_a sequencing behind the toggle), D-19 (DELTA=100yr
  date-coverage audit).
- `.planning/phases/135-precision-certificate-confidence-bands/135-RESEARCH.md`
  — Pitfall 7 (grading-STARTED signal, §10 above), Pitfall 8 (Strict-floor
  statistical power, §6 above), Open Question 1 (report-id hash recipe, §5.2
  above).
- `same_work_spike/probe/scripts/e1_deck.py` — `components_of`, `comp_bootstrap`,
  `wilson_bounds` (§1.5).
- `same_work_spike/probe/scripts/e1_confirm_sizing.py` — `size_confirmation`,
  `n_det_required`, `anova_icc`, `wilson_lower_one_sided` (§6).
- `same_work_spike/probe/results/PLAN-e1-round2.md`,
  `same_work_spike/probe/results/PLAN-e1-round3-canon.md` — pre-registration
  protocol templates this document mirrors.
- `same_work_spike/probe/results/E1-ROUND2-RELEASE.md`,
  `same_work_spike/probe/results/E1-ROUND3-RELEASE.md` — worked PASS/FAIL
  outcome precedent.
- `scripts/build_discovery_sidecar.py` — the FAIL-branch reband/rebuild path
  (§9.2); `scripts/discovery_ids.py` — the frozen `evidence_id`/`claim_id`
  recipes the FAIL branch must regenerate rather than patch in place.
- `shared/discovery_band_labels.py` — `STRICT_FLOOR = 0.85` (§2), the code-side
  constant this protocol's decision rule feeds.
- `feedback_catalogue_never_evidence` (project memory) — catalogue is a recall
  yardstick, never acceptance evidence; adjudication stays catalogue-blind
  throughout this protocol (§4).

---

*This document is FROZEN as of 2026-07-24 (Phase 135, plan 135-03). Any
correction requires a new dated amendment section above (§11 for measurement
deviations, or a new numbered section here for a protocol-design correction),
never a silent edit.*
