# v9.0.0 Discovery — Publication Strategy (owner decisions, 2026-07-27)

**Status:** Owner-decided 2026-07-27 in a consult session. This file is the `.planning/`-resident
record of decisions that were previously held only in a LOCAL-ONLY probe-tree handoff
(`same_work_spike/probe/OPEN-FIRST-HANDOFF.md`, a git repo with no remote) and in conversation.
Anything not written here does not survive a context reset.

**Read this before `/gsd-plan-phase 136` and before any 137/138/139 planning.**

**Masking:** restricted corpora appear ONLY as **M-source** and **R-source**. Works appear only as
opaque `w000xxx` ids, counts, or public names. Same rule as every tracked file.

---

## 0. Why this file exists

Three deliverables had all been informally called "the discovery release", and work drifted between
them without a decision ever being recorded:

1. the GSD milestone path (Phases 136–139),
2. a standalone faceted register of discoveries ("the deck"), and
3. the gen-2 engine rebuild (`same_work_spike/probe/rsource/`, run **G** → discovery-v2.1).

A cleaner four-part framing (adopted 2026-07-27, from the Codex second opinion in §7):

| Layer | What it does | Who owns it |
|---|---|---|
| ① Research engine | generates candidate relationships | gen-2 track (probe tree) |
| ② Publication data contract | masks, bands, routes, versions, verifies | `scripts/build_discovery_sidecar.py` + the frozen schema |
| ③ Product surfaces | browse panel, work pages, voting, atlas | Phases 136–139 |
| ④ Editorial publication | a selected, cited, stable, explainable presentation of findings | **nobody, until now** |

**The v9.0.0 goal is ③.** ④ is an optional presentation layer ON TOP of ③, not a replacement for it.
The internal 159 MB register is *source material* for ④, not a finished version of it.

---

## 1. DECISION — the destination is the website

**The milestone's purpose stands exactly as originally set: surface BOTH work→MS and MS→MS
identifications on genizahsearch.com.** That is already specified, verbatim, as PANEL-02:

> (1) "Other manuscripts of ⟨work⟩" — MS-to-MS, derived from shared work–witness claims;
> (2) "Pages in other manuscripts related to this page" — direct page-to-page alignment claims

plus PANEL-01 (the browse entry point), PANEL-03 (evidence view) and WORK-01/02 (`/work/{id}`).
**That is Phase 136.** A standalone deck is NOT a substitute for it and must not be planned as one.

**Consequence for planning:** Phase 136 is smaller than "four unplanned phases" implies. The read
spine is already built, tested and unused — `shared/discovery_service.py` carries all four query
paths (PANEL-01/02/03 + DATA-10) and `web/discovery.py` carries fail-open async wrappers with
**zero callers**. What is missing is UI plus the 135-08 deploy, not data-layer design.

**Where the deck fits now (reduced scope):** the **private** full register — which substantially
exists and mainly needs a sidecar-sourced rebuild plus masking — and, optionally, a public
*exhibition* of selected finds as an announcement artifact layered on top of the live surfaces.
Not on the critical path.

---

## 2. DECISION — open-first, visibility-gated output (two assets, one pipeline)

One private pipeline emits **two** assets:

- **PUBLIC asset** — open-licensable provenance only. First public scope = **Sefaria-direct matches
  ∪ all MS-relationship (propagated) claims**. Candidate set as measured against the frozen v2 asset:
  **240,566 evidence rows**, 842 distinct works, 219,711 with an NLI catalogue title,
  ~61K distinct (work, catalogue-title) pairs.
- **PRIVATE asset** — the full register, owner's eyes only.

**M-source / JA / R-source remain private INPUTS.** They legitimately date, deduplicate and
canonicalize works, and removing them would degrade the public asset's *quality*. They are gated out
of public **outputs**, not out of the pipeline.

Deferred out of the first public scope: the "M-source-derived-from-Genizah" slice (using our own
transcriptions where an M-source witness is itself a Genizah fragment).

**Carrier fields already exist in the frozen schema** — `works.source_corpus` /
`discovery_claim.source_corpus ∈ {sefaria, ja, msource}` and
`discovery_evidence.evidence_source ∈ {track1_direct, propagated}`. What does not exist is the
labeling/projection pass.

**Structural, not cosmetic:** private rows must be **absent from the public artifact**, not merely
hidden in the UI. A public asset that contains private rows behind a UI filter does not satisfy this.

---

## 3. DECISION — the visibility gate is built ONCE, at the packaging boundary

Both tracks had independently planned a source-keyed public/private gate. **It is built once, in the
bake (`scripts/build_discovery_sidecar.py`), never in the engine.**

Reasons, in order of force:

1. **Visibility is release policy, not matching logic.** Policy will change independently of the
   algorithm, and private analysis needs the full output regardless.
2. **`source_corpus` alone may be insufficient** after cross-corpus dedup, dating and
   canonicalization. The gate must distinguish *the origin of the displayed assertion* from private
   inputs that merely influenced identity or chronology. The engine's job is to preserve lineage
   granular enough for that decision; the bake's job is to make it.
3. Gen-2 output must pass through the bake anyway — it is not publish-shaped (see §5).

**Architecture:** ONE normalized bake, then **deterministic public/private projections** with
separate manifests, separate content hashes, and invariant tests per projection. Explicitly NOT two
loosely divergent `--emit public|private` build paths that can drift.

---

## 4. DECISION — the novelty axis ships, with a precise name

**What it actually is** (this was mis-described in an earlier brief and must not be re-mis-described):
a per-`(sys_id, work)` check against **every available finding aid** — FJMS and NLI catalogue and
bibliography, titles, PGP/FGP, and M-source shelfmark attributions — asking *"does any known
apparatus already tie THIS fragment to THIS work?"* It is **not** a catalogue-title classifier.
Owner estimate: ~85% confidence that a flagged item is previously unknown, and near-certainty that a
researcher would not reach it through the existing apparatus even where some publication records it.

**The public label states the defensible claim, not a stronger one:**

> **"Not identified in any available finding aid"** / *"לא מזוהה באמצעי העזר הקיימים"*
> — with the checked sources **enumerated and dated**, and the ~85% figure stated as an estimate.

Rejected framings: **"new discovery" / "is_new"** as public wording (over-claims — invites announcing
already-known identifications as new, the single most reputationally damaging failure available here);
and "not predicted by the current catalogue title" (under-claims — describes only one of the checked
sources).

**Hard constraints:**

- **Orthogonal to bands.** Novelty must NEVER feed band assignment, precision copy, ranking weights,
  or certified styling. Absence from a catalogue is not evidence a finding is right. (This is the
  catalogue-never-evidence rule applied in the other direction.)
- **`known_source` provenance must be masked on the public side.** The boolean flag is publishable.
  The provenance *value* is not, when the source is M-source — it must collapse to something like
  "recorded in a restricted corpus" / "recorded elsewhere", never the name. Decide this before the
  column is designed: it is the difference between "filter **and explain**" and "filter only" publicly.
- **Coverage gap to close first.** In the frozen v2 asset `is_new` is computed ONLY for the propagated
  family (14,441 flagged new). **All 254,612 `track1_direct` rows are `is_new = 0`** — novelty was
  never computed for the direct matches. It must run for ALL families.

**Cost (measured, not estimated):** ~**$27 one-time**, ~15 min parallelized, over the ~61K distinct
pairs — `google/gemini-3.6-flash` with `reasoning:{effort:"low"}` (100% verdict agreement, 40/40,
with a baseline that itself scored 99% against 103 owner grades). Verdicts are cached, so re-bakes pay
only for new pairs. **Do not downgrade the model to save money** — a weaker flash model scored 62.5%
agreement. Always request `usage:{include:true}` and read real `usage.cost`; naive token×price
estimates were off by ~50×. Recorded in memory `reference_discovery_llm_gate_cost`.

---

## 5. DECISION — gen-2 run G is GO, concurrent, off the critical path

**GO given 2026-07-27.** G is the later **discovery-v2.1** evidence refresh. It does not block Phase
136, and Phase 136 does not wait for it. ~2.75–3.75 h single-machine wall-clock over 667,411 pages,
**no LLM/API cost**.

**Preconditions before launching (all small, all real):**

1. **There is no full-corpus launcher.** `gen2_discovery_run.py::__main__` only runs `_selftest()`;
   the sole working driver (`gen2_e1l_run.run_coarse`) is hardcoded to the E1-L sample regen table and
   a fixed `RUN_ID`. Needs a real entry point: fresh run_id, `abandon_stale_runs`, full staging spec,
   lock path.
2. **Use the launch3 canonical mask** `ref_canon_masks_v2.json` (522 works) — **NOT**
   `mask2_hardmask.json`. E1-L proved the latter is the R-source mask, masks zero launch3 works, and
   inflated the frame by 211 spurious rows + 75 column mismatches. This footgun has already fired once.
3. **Scope G to launch3 only** (Sefaria + JA + M-source reference, live generation, coarse mode). The
   R-source half is blocked behind E1-R / Gate-0 / A2 / F-R, none run — and the R-source tokens are
   still absent from `.masking_patterns`, which HARD-BLOCKS any R-source build or scan (owner action).
4. **Fold in forward-ledger item 10** — the only ledger item carrying a mandate, *"MUST ride any gen-2
   heavy re-run"*: revert the 152 severe HTR-substitution pages; re-key the cite-formula exemption that
   currently re-admits the geonic-digest family. Skipping it means running G twice.

**Operational notes:** the matcher loads ALL page normalized streams into RAM up front and there is
documented OOM history at pilot scale. Matching is resumable (2,000-page batches, fingerprint-checked);
the post-match DB pipeline is **not** resumable — a crash is abandoned for a fresh run_id.

**Also owed by the gen-2 track (or its next re-emit regresses a frozen input):** widen its composition
window `[500,1600] → [100,1600]`, adopt the antiquity clamp @100, sync the 410-entry SEF/JA table, and
note the composition SHA `2b46b470…`. The new `assert_composition_release_contract` will HALT a
regressed build.

**Publishing gen-2 requires real schema work, not a copy.** Its `SCHEMA_VERSION='discovery/1'` carries
tables the sidecar lacks (`discovery_run`, `discovery_version_pointer`, `chrono_pair`), a
`routing_status='contested'` value the shipped enum forbids, sha1 identity keys instead of the frozen
`evidence_id()` tuple the verifier recomputes row-by-row, and it never populates shipped `NOT NULL`
fields (`confidence_band`, `adjudication_status`, `audit_status`, `evidence_kind`,
`display_evidence_id`, `a_page_id`, `sys_id`, `span_start/end`, `works.neutral_title`). Per
`discovery-sidecar-schema-v1.md`, a gen-2 refresh is *"a versioned REBUILD, never a schema migration"*,
and per `discovery-coordination.md` §2 gen-2 vocabulary enters the shipped sidecar only via a dated
amendment that 135-SHIP owns.

**Retraction exposure to plan for:** the reported precision transitions imply gen-2 would
demote/reclassify roughly **32K–45K of the 240,566 publishable rows** — a defensible range, not a
forecast (different frames and weighting). Most would be *semantic* corrections ("same work" →
quotation / directional reuse / formula), not evidence retractions — but they ARE retractions if first
published as witnesses. Mitigations: prefer versioned claims with published relation changes over
silent replacement, and adjudicate individually anything headlined before gen-2 lands.

---

## 6. Precision posture (settled unless the owner objects)

- **`tier_a` is 230,267 of 268,361 display claims (86%) and has NO measured precision** in the frozen
  release contract. That is deliberate — never fabricated. CERT-01 (135-09) is the only valid route to
  a `tier_a` population number.
- **Publish no population precision number for `tier_a`** until CERT-01 lands, per CERT-02. Band label
  + the BAND-05 methods page, no percentage.
- **The two existing owner-graded estimates cannot be reconciled from aggregates**, and neither
  governs a public `tier_a` claim:
  - SEED-029, 200 grades, at the Lever-1-routed *(page, work)* unit: high coverage **94.0%**,
    medium (0.45–0.60) **91.7%**, low (<0.45) **37.5%**.
  - gen-2 E1-L, 1,402 grades, design-weighted per claim on the gen-2 frame: gen-1 shipped **0.73**,
    gen-2 shipped **0.77**, gen-2 headline tier **0.89**.
  - **The key to why they differ:** E1-L's raw split is **48.7% strict same-work** but **99.8% "some
    genuine textual relationship"** (of 717 strict negatives, only **3** lacked any real textual
    relationship; 714 were quotation or formula). Different truth predicates, different units,
    different weighting. A governing public number must come from the exact public eligible frame with
    quotation counted as not-same-work — which is precisely CERT-01's design.
- **Already licensed for display:** collection-scope propagated (corroborated ∪ weak) **0.926**
  [0.875, 0.968]; band `high_confidence_algorithmic` 0.889; `screening_rb` 0.859; `screening_canon`
  0.647 (with its known Targum-confusion caveat).

---

## 7. Second opinion of record (Codex, 2026-07-27)

An adversarial review of this strategy returned **AGREE-WITH-CHANGES**. Brief and full log:
`tmp/CODEX-BRIEF-discovery-publish-strategy.md`, `tmp/CODEX-REVIEW-discovery-publish-strategy.log`
(gitignored; brief verified masking-clean before the run). Its three priority changes, and their
disposition:

| Codex change | Disposition |
|---|---|
| Document a bounded REL-01 exception with per-card editorial review | **ACCEPTED in part** — see §8, still an OPEN owner decision. The exception mechanism is accepted; per-card review for the *website surfaces* is not settled and is partly obviated by §4's label change. |
| Replace "repoint the deck builder" with a provenance-aware public-deck materializer | **ACCEPTED** — see §8. Verified in code: `_largest_track1_span()` (`build_discovery_sidecar.py:2555`) keeps only `(start, end)` of the LARGEST span and reduces the disjoint `spans_json` list to an `n_spans` count; the sidecar also has `snapshot_hash` but no HTR text, so rendering needs a fail-closed join to the live text layer. |
| Treat novelty as triage; run G concurrently; publish no `tier_a` number without a frame-matched audit | **ACCEPTED for G (§5) and the `tier_a` number (§6). PARTLY REJECTED for novelty (§4)** — Codex was reviewing a brief that under-described the gate as a catalogue-title classifier. The real multi-source construct supports a stronger, still-honest claim. |

**Codex finding worth carrying regardless — selection bias.** Any curated/exhibition surface must
publish its funnel: frozen input asset + eligible-universe counts, every deterministic filter with
survivor counts, stratification (work / genre / collection / library / relation type), whether final
picks were random-within-strata or editorial, review requirements and rejection reasons, a
machine-readable manifest of included claim/evidence IDs, and an explicit "not a random sample;
cannot estimate corpus precision."

---

## 8. STILL OPEN — decide before or during 136 planning

1. **REL-01 exception posture (highest priority — sets 136's success criteria).** REL-01 is a gate on
   public claim surfaces, not merely on the three switches it names; shipping claims without an
   exception would violate it in substance. Proposed instrument: a documented **CURATED-SURFACE
   EXCEPTION** on the same pattern as the ATLAS-PREVIEW EXCEPTION, allowing Phase 136 surfaces to go
   live behind their own flag, `noindex`, with no `tier_a` number, before CERT-01 completes — main
   discovery flag / homepage band / sitemap-SEO staying OFF. Draft conditions in
   `v9-REQUIREMENTS-ADDENDUM-DRAFT.md`.
2. **Which slice of 136 is v1?** Suggested: the browse panel's MS-to-MS list + `/work/{id}` first
   (the two the owner named), deferring the page-to-page view and PANEL-03's on-demand evidence pane
   (PANEL-03 carries the fail-closed HTR-drift join — the expensive part).
3. **The 101,824 review_only-display claims (38%).** They must never render as identifications. The
   simplest honest answer is that they do not render at all in v1 — but a meaningful share of the
   novel finds may sit there, so this needs a decision rather than a default.
4. **Whether a public *exhibition* surface (④) is built at all**, and if so whether selection is
   stratified-random-then-editorially-pruned or openly hand-picked (either is fine if labeled
   correctly and the funnel is published).
5. **Promotion path:** do M-source / R-source rows ever move private → public later? This decides
   whether §3's projection is a permanent structural filter or a revisable policy label.
6. **SEED-032** (surface new/uncataloged discoveries above known works) still has no owner and needs an
   explicit disposition at 136 or 138 planning.

---

## 9. Blockers carried forward (not strategy, but they gate the same path)

- **135-08 has never run.** The sidecar has never been in production; nothing on-site can render
  without it. Mechanical and rollback-drilled, `DISCOVERY_ENABLED` held OFF. Also unblocks the
  prod-box RSS measurement PERF-01 needs.
- **`PERF-01` cannot be signed off.** The only measured actuals were taken against the *v1* asset on a
  dev box (browse p95 0.57 ms vs a 150 ms cap; added RSS 11.1 MB vs a 250 MB cap — large headroom, but
  the wrong artifact). Prod-box RSS is unmeasured; `/work` + `/leads` request-time caps are pending
  until those surfaces exist. The frozen v2 asset (~370 MiB) has had no size re-acceptance.
- **~~Commit `97cad7df` blocks pushing~~ — NO LONGER A BLOCKER (owner decision, 2026-07-28).**
  Measured that day: the repo is **PUBLIC**, and the restricted name — plus excerpt-level reference
  TEXT (`ref_txt`, `ref_snippet`) — has been in its pushed **history** since **2026-01-30** (91
  commits, through the 2026-07-20 cleanup; paths under `same_work_spike/**` and `corpus_mapper/**`,
  NOT `.planning/`). `97cad7df` would add a 92nd to 91 already public. **Owner accepted the historical
  exposure and declined a rewrite** — the window is closed, distribution is low (13 stars, 0 watchers,
  3 forks all created 2026-01-09/12 *before* the window opened), and a rewrite would change every SHA
  and risk the desktop updater, which distributes the installer via tag-bound GitHub Releases.
  **Do not gate 135-08 or any deploy on this.** Forward discipline is the whole control: never commit
  M-source/R-source names, titles or text; research trees stay untracked + gitignored.
  **Known blind spot to remember:** DATA-05 scopes the guard to *"git index/HEAD"* — a green
  `--scan-repo` says nothing about history, which is exactly how this ran undetected for six months.
  Full measurement: memory `project_git_history_msource_exposure_accepted`.
- **R-source tokens absent from `.masking_patterns`** — owner-only action, hard-blocks R-source work.
- **Pending UX discuss-phase** (already a Pending Todo in `STATE.md`) gates 136 planning: relation
  vocabulary display wording, the `/catalog` Browse-by-Identification extension shape, the BAND-03
  Hebrew toggle wording, per-surface BAND-04 disclaimer wording, ATLAS-02's primary graph object.
- **Naming hazard:** `web/pages/discoveries.py` is the PRE-EXISTING community "Discoveries Center"
  (user-submitted discoveries/questions in Supabase), unrelated to the v9 discovery module
  (`web/discovery.py`, `web/discovery_assets.py`). The v9 claim surfaces have no page module yet.

---

*Recorded 2026-07-27. Supersedes, for `.planning/` purposes, the strategy sections of the local-only
`same_work_spike/probe/OPEN-FIRST-HANDOFF.md`; that file remains the fuller working record of the
packaging track, and `same_work_spike/probe/rsource/GEN2-HANDOFF.md` of the engine track. Neither is
in a repo with a remote — do not rely on them surviving.*
