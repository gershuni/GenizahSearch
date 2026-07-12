# HANDOFF — MAPV2 full rebuild + 25h autonomous battery (2026-07-10 → 07-11)

Session: Hillel's directive "run all the tests and re-tests with Opus agents and
Codex reviews, then run the MAPV2 with all the corpus in all tracks as planned."
Everything below is DONE unless marked open. This file + the memory entry in
`project_seed029_probe_go.md` are the complete state; the task list carries
MAPV2-8 (partially open) and MAPV2-9 (next cycle).

## 1. What ran (all complete)

**Overnight chain, 14/14 steps OK** (launched 17:02, done 23:47; log
`results/MAPV2-RUN-LOG.md`, per-step logs `results/overnight/mapv2-*.log`):
final CAL-1 → Track-1 v2 full (197 min) → shadow → testimonies → review →
Track-2 canonmask (50 min) → map → atlas → graph → chains → units → motifs →
motif-query (96 min) → work-query (39 min). Then the morning product chain
(`scripts/mapv2_morning.py`, detached watcher pattern) ran the delta report +
full deck build automatically.

## 2. Key numbers (v2 state, all verified)

- **Census (tier A): 270,080 rows → 180,562 identified pages** (+4,118 pages
  net vs v1); shadowing 51,396 rows; census 87,547 (ms,work):
  testimony 48,076 / partial 22,400 / citation 17,071; new? 9,988 /
  new?known 7,646 (4-channel gate). Targum gap CLOSED (Onkelos 175
  witnesses; +252/+197/+170/+164/+113 by book).
- **Tier B: 1,335,320 P-stamped candidate rows on 321,452 pages** (48% of
  corpus) in `track1_candidates` (census never reads it).
- **Track-2 map: 1,361,749 accepted pairs → 455,673 clean MS pairs / 63,616
  MSS** (+4% vs v1). A2 work-query: 23%→95% pairing, +12,834 memberships.
- **Track-2 WIDE tier (MAPV2-6, new): 552,403 scored pairs** in
  `data/track2_wide.db` (decoy-calibrated, 12K decoys × 2 chunk cohorts,
  conservative fallback nulls, global chance rate <1%); **100,926 pairs with
  a side ≤300 letters** (the small-fragment population).
- **Delta report** (`results/mapv2_tierA_delta.md`): gains 18,859
  (ref-new-work 9,786 / text 4,330 / mesirah 3,963 / engine 738); "losses"
  87.5% re-attribution (version migration to REF-2 units), page-level
  persistence PASSES. 5/6 invariants pass; the 6th was mis-specified.

## 3. Deliverables sent to Hillel (paths)

- `review/full_deck/COVER-NOTE.md` — plain-language cover (Hebrew).
- `review/full_deck/mapv2_discovery_deck.html` — **v10 discovery deck**
  (88 cards; measured top-band precision 55%; liturgy/piyyut ~82-84%).
- `review/full_deck/mapv2_blinded_deck.html` + `mapv2_blinded_key.json` —
  55-card blind deck; **Hillel graded all 55**
  (`review/full_deck/BlindedDeckGrading.txt`), scored in
  `results/blinded_deck_scoring.md`.
- `review/track2_wide_small_fragments.html` — 45 sample small-fragment pairs.
- `review/rehearsal_fullv2_atlas.html` — visual map (+ graph
  `review/rehearsal_fullv2_graph.html`, 14.5MB).
- `results/mapv2_tierA_verse_audit.md` — census self-audit (see §5).

## 4. Deck lineage + what the blind grading proved

Precision lineage (agent-graded, same rubric): v6 84% → v7 82.5% (liturgy
smoke) → **v8 FULL 45%** (liturgy guards did NOT transfer to rabbinic/JA
content) → v9 55% → v10 55% (shipped; 3 consecutive zero-over-kill rounds;
flagships intact: Solomon-throne midrash T-S C 2.198, gaon-kaddish T-S NS
309.86, Hidayat al-Qari ×2, Perek Shira, ר"ח ב"מ T-S NS 311.35).

Guards now in `scripts/mapv2_deck.py` (all measured): rarity = q92 of tier-A
witness counts (derived at runtime, 45 on full corpus), Bible span-union
cover, span-union margin w/ per-span competitor density, canonical-rendering
trimmed hulls (chunked + NDJSON checkpoint + input fingerprint), whole-slice
Bible align ≥60, canon-claim coverage gate ≥0.45 (killed the pitum-haketoret
class), cite-formula gate (window [-38,+30) around span start; exemption =
works whose OWN stream uses formulas, len≥5 markers only), Targum same-book
sibling gate, modern-era gate (date ≥1500; 5,211 rows), substitution-risk
exclude-list (270 rows), tier-A-known pair exclusion, per-work + per-ms caps,
statutory routing, honesty display rules (singleton caps 0.799, range chips
for small-margin mid-range, display cap 0.99).

**Blind grading readout** (`results/blinded_deck_scoring.md`): W+P (witness
OR genuine parallel) tracks hidden P monotonically 60/30/27/20/20% by
quintile — P ranks RELATIONSHIP strength. Witnesses concentrate in singleton
(30%) + m_ge_010 (40%); small-margin bands 0-5% witness → demote them from
top sections next cycle. Hillel's method gifts: ואמרו במדרש marker missing;
JA citation family (לקו'=לקולה) entirely absent; card #23 = REVERSED
direction (edition quotes the page's source) = potential-find class;
#17/18/19 = Karaite↔Rabbanite shared verse-chains = new parallel product.

## 5. Census self-audit (MAPV2-7, done)

`scripts/mapv2_tierA_audit.py` ran the deck guards over 75,045 non-canonical
live census rows (3.3h): 24,507 suspects (32.7%), **testimony-grade dangerous
slice 4,321 (10.2%)**. TWO populations (interpretation note in the report):
(a) audit artifacts — statutory-liturgy editions overlapping the guard by
construction (next cycle: exempt works whose own stream ≥0.45 guard-covered);
(b) TRUE defects — halakhic codes credited via embedded canon: **ספר אהבה
2,883 rows (witnesses 3,124→1,545), זמנים 1,028, טור או"ח 521, רי"ף שבת 424**
= Hillel's day-one complaint quantified. Flags in
`data/tierA_verse_suspects.json`; removal needs per-work flank review.

## 6. Other battery results

- **CAL-1 final2 refit** (isotonic by alen, effective-works pooling):
  holdout mean |gap| 0.161→0.082; Codex 0-blocker; ADOPTED for deck ranking
  (`data/p_calibration_final2.json`; runner-stored tier-B P remains final).
- **Stage-0 substitution risk** (MAPV2-8): F-SQL-1 recompute over all 18,982
  substituted pages → 595 risky (3.13%), 152 severe, 0 catastrophic
  (`results/mapv2_substitution_risk.md`, exclude-list
  `data/substitution_risk_pages.json`, wired into the deck). OPEN remainder:
  tighten the stage0 gate next run (fidelity-weighted coverage, persist
  coverage/case, gap markers, Case-A check) + revert the 152 severe pages to
  v1 HTR before the next census build.
- **Codex gates run this session:** deck guard R1 (7 findings) + deck R2
  (4) → all fixed; MAPV2-6 design (8 findings incl. 2 BLOCKER) → redesigned;
  MAPV2-6 code R1 (7 findings incl. 2 BLOCKER) → fixed → **R2 APPROVE, zero
  findings**; CAL-1 refit (4 findings, 0 blockers) → adopted with notes.
- **Opus agent passes:** final-cal audit, stage-0 gate audit, v6/v7/v8/v9/v10
  deck gradings (one agent, full context lineage in
  `results/agent_deck_review_content.md`), MAPV2-6 implementation, tierA
  audit implementation, statutory Sefaria fetch (16→17 guard-only units,
  manifest 75 entries; `ref2_build.py` now SKIPS guard_only).

## 7. Open items (task list)

- **MAPV2-8 (in_progress):** stage0 gate tightening + revert 152 severe
  substitutions at the next corpus build.
- **MAPV2-9 (new, from blind grading + v10 grade):** host-work-keyed
  cite-formula exemption (aligned-position test) — the current exemption is
  keyed on the WRONG side and re-admits the geonic digest family;
  NLI-catalog-mismatch flag (~16/18 leaks show it); JA + HTR-tolerant
  citation markers (ואמרו במדרש, לקו'/לקולה, וגדסי'); widen rabbinic guard;
  demote small-margin bands from top sections; NEW products: reversed-
  direction finds + Karaite↔Rabbanite parallels; relabel P as relationship
  strength.
- Deferred (unchanged): FRAG-3 glossary/lemma matcher; 6 competing residue
  clusters (Hillel parked); Track-3 RamBERT experiment (parked per goal
  pivot).

## 8. Operational notes for the next session

- Machine idle now; all DBs free. fullcorpus_v2.db = the v2 state
  (rollback journal — NEVER read it while a chain writer runs).
- Deck rebuild is cheap when guard params unchanged: checkpoint fingerprint
  (`review/full_deck/mapv2_deck_guard_ckpt.ndjson`) → 24-90s reruns; any
  guard-unit/param change invalidates → ~55 min full guard requery.
- `scripts/mapv2_morning.py` = reusable wait-then-run product driver
  (state `data/mapv2_morning_state.json`; note: delta exit-1-on-check-fail
  stops it — mark step done in the state json to skip).
- Gotchas hit tonight: PowerShell Start-Process multi-word args need
  embedded quotes; os.remove inside `with open()` = WinError 32; 3-letter
  normalized Hebrew markers false-match inside words; agents ending turns
  "awaiting monitor" answer stale wakeups (verify file mtimes; SendMessage
  resume may reply from stale context); Opus agent credits can exhaust
  (reset 12:30pm LA) — Codex + Fable-inline are the fallback.
