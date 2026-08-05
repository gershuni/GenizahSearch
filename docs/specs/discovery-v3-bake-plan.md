# discovery-v3 bake plan — the gen-2 evidence refresh

**Status: DRAFT FOR REVIEW, 2026-08-05. No heavy run has started.** Written by the gen-2 bake session
before touching the pipeline, per the standing rule that a plan naming its costs is reviewed before spend.

**Companion:** `discovery-v3-naming.md` (why this is v3 and not v2.1). **Predecessor:**
`discovery-v2-bake-plan.md` (the pipeline this reuses). **Input spec:**
`same_work_spike/probe/rsource/HANDOFF-TO-135.md`.

---

## 0. Verdict up front

The bake is **feasible and much cheaper than the planning record says**, but it is **not** the
"cache-makes-it-nearly-free" job the brief describes, and one of the three headline debts is **already
paid** by the input artifact.

| | finding |
|---|---|
| **Cheaper than recorded** | The novelty LLM gate's real measured cost is **$40.12**, not the `~$301` the ROADMAP carries. Batching 10 cases per call is the whole difference. |
| **One debt already paid** | `w_start`/`w_end` — the item Phase 136.1 actually waits on — is **already persisted, 100% populated, on all 502,498 gen-2 evidence rows** as `ref_start`/`ref_end`. The expensive half of that debt does not need doing. |
| **Not as cheap as briefed** | The verdict cache is keyed on grain alone (`sys_id::work_key`) with a whole-file hash pin. It has **no mechanism to notice that a question changed**, so reusing it across a membership change is a correctness hazard, not a saving. Recommend computing fresh. |

Everything below is measured against the real artifacts on this machine, with the measurement named. Where
I could not measure, I say so rather than estimating quietly.

---

## 1. Measured facts (and three corrections to the record)

All figures read directly from the artifacts, read-only, on 2026-08-05.

### 1.1 The input artifact is present and intact

`same_work_spike/probe/rsource/data/g_launch3.db` — 672 MB, run `g_launch3`.

| table | rows |
|---|---|
| `discovery_claim` | 358,206 |
| `discovery_evidence` | 502,498 |
| `coverage_route` | `same_work` 160,095 · `parallel` 75,644 · `not_shipped` 118,789 |
| distinct `ref_work` | **4,160** |
| distinct `page_id` | 198,238 |

`ix_evidence_claim` **is present** on `discovery_evidence` — the index whose absence caused the documented
O(n²) finalize hang at corpus scale. That hazard is covered *on the source side*; it says nothing about the
destination bake, which is a different pipeline (§6).

Scale versus what is serving now: **358,206 claims against v2's 268,361 private / 231,244 public**, and
**4,160 reference works against 1,269 private / 613 public**. This is a membership replacement, roughly 1.3×
the claims at ~3.3× the work-axis granularity.

### 1.2 CORRECTION 1 — `w_start`/`w_end` is already done

The brief calls this "the highest-value item here", and it is — but the work-side offsets the Phase 136
rebuild trimmed out **already exist in the gen-2 evidence**:

```
discovery_evidence.ref_start INTEGER   -- 0 NULL of 502,498
discovery_evidence.ref_end   INTEGER   -- 0 NULL of 502,498
```

The v2 bake plan's §"Why it is cheap" predicted exactly this ("the work-side coordinate exists at match time
and is discarded at ingest") — the re-instrumented gen-2 Track-1 **stopped discarding it**. So the corpus-wide
persistence half of the debt is free: it is an ingest mapping, not a computation.

**The coordinate space is `norm_stream`**, confirmed at `gen2_track1_run.py:140`
(`streams = [norm_stream(r[2])[0] for r in rows]`) — i.e. the matching-oriented normalisation, *not* the
readability-oriented `body` the versemaps index. **The coordinate trap the v2 bake plan warned about is real
and unavoidable**, and it is the only genuinely hard part left in this debt (§3.2).

### 1.3 CORRECTION 2 — the novelty gate's real cost is $40.12, not ~$301

Read from the run's own cost log (`discovery_data/novelty_production_cost_log.jsonl`, 5,528 logged calls,
summing the real per-call `cost` field — never an estimate):

| | |
|---|---|
| **measured total spend** | **$40.12** against a `cost_ceiling_usd` of 45.0 |
| model / effort | `gemini-3.6-flash` / `reasoning: low` (as recommended) |
| **batch size** | **10 cases per call** |
| LLM-decided cases | 55,184 |
| heuristically resolved (free) | 10,016 |
| **unit cost** | **$0.000727 per case** |

**This reconciles the `~$27` and `~$301` figures in the record.** `~$301` was the per-case unbatched
re-derivation; batching ten cases per call brings it to $40. Neither figure was wrong about its own contract.

> **⚠ An authorization discrepancy I cannot resolve and am not going to paper over.**
> `136-NOVELTY-RUN.md` states plainly: *"The ~$301 production run remains UNAUTHORIZED. It was NOT executed
> by this session."* Yet a production run **did** complete — `novelty_production_manifest.json` (started
> 2026-08-03T12:57), a 55,184-entry checkpoint, a $40.12 cost log, and the resulting cache
> `novelty_production_verdicts.json` whose SHA-256 is `eb6fc4f8…`, **which is the exact hash the shipped v2.1
> asset pins and ingested**. The most plausible reading is that the run was re-scoped to `batch_size=10`
> and authorized *after* that document was written, which is precisely the "authorize at a lower cost" option
> its own §5 offers. **Owner: please confirm that reading before v3 spends anything on this gate.** I am not
> treating a $40 receipt as evidence of authorization.

### 1.4 CORRECTION 3 — the verdict cache is NOT keyed the way the brief describes

My brief states the cache is keyed "in fixed order on pinned model / version / effort / prompt hash /
input-normalization hash FIRST, then `sys_id`, `ref_work_id`, then the normalized claimed title/author and
per-source free text", and concludes *"a cache hit therefore provably means the identical question was already
asked and answered."*

**That is the spec's intent. The artifact does not implement it.** The real shape, from the builder's own
frozen contract (`build_discovery_sidecar.py:4555-4573`) and verified against the pinned file:

```
{ "990000413480205171::w001159": {"novelty_status": "...", "divergence_correctness": null}, ... }
```

The key is **grain only** — `f"{sys_id}::{work_key}"`, where `work_key` is the alias-group representative.
Model, effort, prompt hash and input text appear **nowhere in the key**. What actually protects integrity is
different and coarser: a **whole-file SHA-256 pin** (`sha256` is required; `None` raises — there is no
unpinned load path) plus the model/prompt/effort recorded in a **sibling manifest**.

Three consequences, and they run *opposite* to the brief's reassurance:

1. **A cache hit does not prove the identical question was asked.** It proves only that some run once answered
   for that (manuscript, alias-work) pair.
2. **Changing the model would NOT invalidate entries.** The brief warns against changing the model because
   "those fields are first in the key by design". In this artifact a model change is silently invisible to
   lookup — a *stale-verdict* hazard, not a cost event. The protection is procedural (re-pin the file), not
   structural.
3. **Therefore the cache cannot safely be reused across the gen-2 membership change.** Gen-2 alters the input
   free text and the work grain by construction; a gen-2 row can collide with a v2 key while asking a
   materially different question, and nothing in the load path can detect it.

This lands in the same place the standing rule already requires — *novelty is granularity-relative; recompute
at the new granularity, never migrate* — but for a sharper reason than economy. **Recommend: compute v3
novelty fresh. Do not top up or key-reuse the v2 cache.** §4 shows this is affordable.

One thing the brief gets exactly right, and it is load-bearing: unverified rows land as `not_checked`,
**fail-closed per entry, never fatal** (`load_novelty_verdicts`) — excluded from contribution figures, shown as
"not yet checked", never as a candidate. Skipping the gate is honest by construction, so cost can be traded
against coverage without dishonesty.

### 1.5 The gen-2 artifact's own novelty table must NOT be ingested

`g_launch3.db` carries `novelty` (92,684 rows) and `novelty_meta` declaring
`novelty-gate/1-heuristic` over sources `["bib(Friedberg)", "catalog_refs", "fgp", "pgp"]`.

**This is the flawed prototype, and its own handoff says so** — §6.1 of HANDOFF-TO-135 specifies
`catalog.TitleHeb`/`GenizahTitleOrgTitle` and records that `catalog_refs` **"matched ZERO"**. It is reference
material, not production. Ingesting it would import a known-wrong axis. Flagging explicitly because it sits
in the source DB looking authoritative and the ingest step will walk right past it.

### 1.6 The granularity stage is partly already done

Gen-2's raw `ref_work` ids are **already split below the collapsed canonical grain**. The top of the evidence
distribution is Bible books carrying their own ids (`M:Ytext1000_NN`, one per book) rather than one collapsed
Bible id:

- top 10 `ref_work` = **30.6%** of all evidence rows; the top 8 are all book-level ids under one M-source text.
- 4,160 distinct `ref_work` vs 1,269 milestone works.

So handoff §6.3's "Bible → chapter, Talmud → folio+amud" stage is **not** starting from zero: book/tractate
level is present in the raw ids today. Chapter/folio level is not. This materially reduces that item's cost
(§3.6).

---

## 2. What the bake owes — the ledger at a glance

| # | debt | cost | blocks | droppable? |
|---|---|---|---|---|
| 1 | gen-2 evidence ingest (the bake itself) | **L** — the irreducible core | everything | no — it *is* the bake |
| 2 | `w_start`/`w_end` **persistence** | **XS** — already in the input | Phase 136.1 PANEL-03 | no, and no reason to |
| 2b | Sefaria versemap resolution + `body↔norm_stream` map | **M** — the hardest remaining work | PANEL-03's *reference locus* only | **yes** — stage it |
| 3 | novelty recompute at v3 grain | **S** — ~$40–110 measured | findings-page candidacy filter | partially — `not_checked` is honest |
| 4 | GEN2 emitter sync (date tables) | **S** | D-17 chronological demotion | no — cheap and load-bearing |
| 5 | MAPV2-8/-9 engine debts | **UNKNOWN — must be scoped first** | possibly a second heavy re-run | **decision, not a given** (§3.5) |
| 6 | 58 NULL-genre works | **XS** — curated CSV already exists | the release verifier **fails today** | no — it is a hard gate |
| 7 | `band_precision` re-bake | **S**, but gated on a **human** step | CERT-01 closure, Phase 139 | defer — needs owner grading, not compute |

`XS` < 1h · `S` hours · `M` 1–3 days · `L` ≥ a week.

---

## 3. The ledger in detail

### 3.1 The ingest (debt 1) — the irreducible core

The handoff is explicit that **the schemas differ**: probe `discovery_claim`/`discovery_evidence` versus the
milestone `discovery.db`. *"This is a mapping/ingest, not a file swap."* Two id-space translations are
required, and both are the crux of everything else:

| axis | gen-2 | milestone | note |
|---|---|---|---|
| manuscript | `page_id`, 48-char opaque hash | `sys_id`, 18-digit Alma | `discovery_data/crosswalk.json` is the existing bridge |
| work | `ref_work` — `M:` / `REF2:` / `J:` prefixed raw | `w######` canonical + alias groups | census + canonical-merge mapping |

Neither axis matches on either side. **Every downstream figure in this plan depends on getting these two
maps right**, and a silent partial map is the failure mode to fear: it would not error, it would just
under-populate. Gate accordingly (§6, gate 2).

### 3.2 `w_start`/`w_end` and the locus (debts 2 / 2b) — split them

**Stage 1 — persistence (do it, it is nearly free).** Carry `ref_start`/`ref_end` through the ingest as
`w_start`/`w_end` on `discovery_evidence`, for **all corpora**, and **name the indexed stream
(`norm_stream`) at the point of definition** in `discovery-sidecar-schema-v1.md`. The v2 plan's rule — *every
offset in this system needs its coordinate space named* — is the whole lesson of the D-12 sketch finding and
of the 652-char miss on the manuscript side. This alone delivers the internal wins: containment detection,
shadowing, join sequencing, leaf ordering, work-coverage statistics.

**Stage 2 — human-readable reference (the real work).** This needs the `body ↔ norm_stream` offset map per
work, because the 322 staged versemaps index `body` and the offsets index `norm_stream`. Both are
deterministic functions of the same source text, so it is mechanical — but it is per-work, it is the item the
owner trimmed out of Phase 136 precisely because it "carried the build's hardest work", and nothing else in
this bake depends on it.

**Recommendation: ship stage 1 in the v3 bake; run stage 2 as its own follow-on.** Phase 136.1's
*our-text-only* evidence highlight does not wait on either (it uses page-side `span_start`/`span_end`, which
already ship). Only PANEL-03's reference-side locus waits on stage 2 — so staging costs one deferred surface
element, not a phase. Also unchanged: the acquisition gaps (2 liturgy bodies; 322 staged versemaps against
451 Sefaria works with claims) are re-runnable fetcher work, not engineering.

### 3.3 Novelty (debt 3) — recompute; see §4 for the money

Per §1.4, compute fresh rather than reusing the v2 cache. Grain: per `(sys_id, reviewed alias-work)` using
gen-2's **raw** `ref_work`, never the over-collapsed canonical id — which the builder's own
`novelty_grain_key` docstring already insists on ("one collapsed id covers 39 Bible books"). Catalogue source:
`catalog.TitleHeb` / `GenizahTitleOrgTitle`, **not** `catalog_refs` (§1.5). Keep `divergence_correctness`
**out of the model's job** — ruling L, measured at or below chance; the builder already drops it if present,
and it must stay a human-only annotation.

### 3.4 GEN2 emitter sync (debt 4)

The date tables are shared frozen inputs already in the milestone (`composition_dates.json` `2b46b470…`,
`seftja_dates.json` 410 / `0076028…`) and D-17's chronological co-claim demotion runs on them. The sync is
re-emitting from the gen-2 side so the emitter and the pinned artifact agree, and re-pinning. Cheap,
load-bearing, and it must happen **before** the D-17 step, whose ordering (Lever-1 → D-17, not the reverse) the
v2 plan §6 fixed once already after a Codex round. Do not re-derive that order; inherit it.

### 3.5 MAPV2-8/-9 (debt 5) — the one item I will not cost without scoping

The forward ledger says these **MUST** ride any gen-2 heavy re-run: revert 152 severe HTR-substitution pages,
re-key the cite-formula exemption (it currently re-admits the geonic-digest family), add JA/HTR-tolerant
citation markers.

**What I found, and why it is a question rather than a line item.** The re-keyed exemption exists — as
`cite-formula gate v11 (aligned host-side exemption)` in `same_work_spike/probe/scripts/mapv2_deck.py`. But
that is the **MAPV2 deck/product path**, a different lineage from the gen-2 engine
(`rsource/scripts/gen2_track1_run.py` → `gen2_discovery_run.py`) that produced `g_launch3.db` on 2026-07-29.
I did **not** find these fixes in the gen-2 engine path.

So exactly one of three is true, and which one decides whether this is hours or another heavy re-run:

1. the gen-2 engine already incorporates them by another name → no cost;
2. they are post-hoc filters applicable at ingest → **S**, do it in this bake;
3. they are matcher-level and need a **fresh heavy Track-1 run** → **L**, and it changes the whole shape of
   this plan.

**Recommendation: scope this first, cheaply, before anything else** — a targeted diff of the two engine paths
plus a check for the 152 flagged pages in `g_launch3`. Half a day of reading buys the difference between a
one-week and a multi-week bake. **I have not assumed an answer, and this plan is not costable until it is
known.**

### 3.6 The 58 NULL-genre works (debt 6) — the curated file already exists

**The release verifier FAILS on the artifacts as built today**
(`verify_discovery_sidecar.py::check_works_genre_vocabulary`; control
`tests/test_discovery_release_contract.py::test_null_genre_reachable_only_through_the_review_opt_in_is_a_violation`).
58 of 613 public / 181 of 1,269 private works reachable through the review opt-in carry NULL genre against a
contract that says *"an explicit `Unassigned` bucket, never NULL-as-absent"*. Owner decided 2026-08-04 to
**curate**, not backfill `Unassigned`.

**Found before regenerating anything, as instructed:** `_tmp/genre-curation-58-COMPLETE.csv` — **58 rows,
zero blanks**, carrying `genre_to_assign` plus `basis` and `evidence` columns (family-unanimous /
family-majority reasoning). Its sibling `-RESOLVED.csv` still has 4 blanks, so **`-COMPLETE.csv` is the one to
use.** Do not regenerate; feed it through the 136-09 curated-artifact path (`apply_work_genres`, at
`canonical_work_id` grain, re-pinning the content hash).

**Two live gaps to close, not one:**
- **The 123 restricted works are NOT curated** — `_tmp/genre-curation-restricted.PRIVATE.csv` is still the
  blank template (123 rows, empty `genre_to_assign`). 58 public + 123 restricted = the 181 private. So the
  **public** release gate can pass while the **private** artifact still fails. Both need an answer.
- **`apply_work_genres` writes only works matched in the curated artifact** — that omission is the root cause
  of this bug. At v3's grain there are **4,160 reference works**, not 1,269, so the curated set will cover a
  much smaller fraction. **Verify coverage against every claim-bearing work in the v3 asset rather than
  assuming it, and expect this debt to be bigger at v3 than at v2.** This is the one item where v3 makes an
  existing problem worse rather than better.

### 3.7 `band_precision` (debt 7) — defer, and say why

Open since Phase 135: `tier_a` carries no number, and a real build refuses to fabricate one
(`--release` requires `--precision-spec`; `_validate_precision_spec` pins it to the exact frozen row-set).
The blocker is **not compute** — it is a pre-registered measurement over owner grading, and v3 changes the
population, which re-registers CERT-01 (population change is exactly the cascade cost the coordination doc
listed).

**Recommendation: defer past the v3 bake.** Bake v3 with the frozen precision defaults and no `tier_a`
number, exactly as today. Nothing user-facing regresses, because **no precision number may reach a surface
anyway** — tiers only. Re-registering a certificate against a new population is a Phase 139 conversation, and
sequencing it before the bake would serialise the bake behind owner grading time, the scarcest resource in
this project.

---

## 4. Novelty economics, done honestly

Unit cost is **measured, not modelled**: $40.12 ÷ 55,184 = **$0.000727 per LLM-decided case**, with **15.4%**
of cases resolved free by the heuristic funnel. Cost for a population of P:

`cost ≈ P × 0.846 × $0.000727 ≈ P × $0.000615`

| v3 novelty population P | projected cost |
|---|---|
| 65,200 (v2's, if grain collapses similarly) | **$40** |
| 100,000 | **$62** |
| 150,000 | **$92** |
| 173,564 — page-grain headline ceiling, an **over-count** at `(sys_id, work)` grain | **$107** |

**Recommend a $150 ceiling**, which the run enforces itself (`cost_ceiling_usd`, as the $45 one did). That is
half the `~$301` figure currently in the ROADMAP for a *larger* corpus.

**What I could not measure, and why.** The exact P needs the `page_id → sys_id` crosswalk and the
`ref_work → alias-work` map — i.e. §3.1, which the bake builds anyway. **P is therefore a build output, not an
input**, and the honest sequence is: build the ingest, count P, then authorize the spend against the real
number. The measured bounds above are for deciding whether it is worth starting, and they say yes.

**Do not touch model, effort or prompt to save money.** Not for the brief's stated reason (they are not in the
key), but for a better one: the $0.000727 unit cost and the 78.3%-agreement re-measurement are both *of that
configuration*. Changing it discards the only validation this gate has, and the saving is at most tens of
dollars.

---

## 5. Recommended scope

**DO in the v3 bake**
1. Scope MAPV2-8/-9 first (§3.5) — it can invalidate the rest of this plan.
2. The gen-2 ingest with both id-space maps (§3.1).
3. `w_start`/`w_end` stage 1, corpus-wide, coordinate space named in the schema doc (§3.2).
4. GEN2 emitter sync + re-pin, before D-17 (§3.4).
5. Genre curation from `-COMPLETE.csv`, plus a coverage check at v3's 4,160-work grain (§3.6).
6. Novelty recomputed fresh at v3 grain, under a $150 self-enforced ceiling (§3.3, §4).

**DEFER, with the cost of deferring stated**
- **Versemap resolution / `body↔norm_stream`** (§3.2 stage 2) → PANEL-03's reference *locus* only.
- **`band_precision` + CERT-01 re-registration** (§3.7) → blocked on owner grading, not on this bake.
- **The 123 restricted-work genres** (§3.6) → blocks the *private* verifier, not the public release.
- **JA divisions** — the v2 plan already deferred these and says explicitly *"do not block stage 1 on any of
  this."* Inherit that.

**DROPPABLE if the schedule demands**
- Novelty coverage may be **partial**: unverified rows land `not_checked` and are excluded from contribution
  figures and never shown as candidates. Honest by construction — the one place in this bake where scope can
  be cut without lying.
- The handoff's **conservative headline option** — gating heavily-quoted mega-works (Talmud/Bible/Tosefta) out
  of the same-work headline surface until the witness-vs-quoter lever exists. Costs recall, buys a uniformly
  clean headline. **Owner call**, and it is a routing decision, not a compute one.

**NOT droppable**
- The genre gate (the verifier fails today), the two id-space maps, the D-17 ordering, the masking gate.

---

## 6. Gates — and every one must be shown able to fail

This project has a measured history of checks that reported success without performing their check
(Phase 136 shipped seven of them). So each gate below carries **how it is proven able to fail**, and no gate
is recorded as passed without that demonstration first. Precedent already set this session: before trusting
the masking scan on my own two files, I ran it with the pattern file unset (**exit 1**, fail-closed) and
`--self-test` (synthetic needle **caught**), *then* the real scan (clean).

| # | gate | proven able to fail by |
|---|---|---|
| 1 | **Row-count / preservation** vs the pinned expectation, taken **before** the bake | mutate one expected count → must fail |
| 2 | **Id-map completeness** — every gen-2 `page_id` and `ref_work` resolves, or the build HALTS | drop one crosswalk row → must halt, not under-populate |
| 3 | **`w_start`/`w_end` non-NULL on every `track1_direct` row**, all corpora | null one row → must fail |
| 4 | **Release verifier**, both audiences (`--audience public`) | must fail on today's NULL-genre artifact **before** the fix, pass after — this gate is currently RED, which is the control |
| 5 | **Masking, `--strict --scan-repo --scan-asset --scan-sqlite`**, `MASKING_SCAN_PATTERNS_FILE` set | unset it → exit 1; `--self-test` → needle caught. **Never a skip.** |
| 6 | **Golden fixture + discovery suites** | — |
| 7 | **Performance** vs `discovery-budgets.md` caps | — |
| 8 | **Novelty fail-closed** — out-of-vocab → `not_checked`, counted, never a positive verdict | inject a bad status → must resolve `not_checked` |
| 9 | **`divergence_correctness` NULL on every row** (ruling L, human-only) | inject a value in the cache → must be dropped + counted |

**Order of operations: inherit `discovery-v2-bake-plan.md` §6 unchanged** — one unified sequence, Lever-1
coverage routing **before** D-17. That order was corrected once already after a Codex round; re-deriving it is
how it gets broken again.

---

## 7. Operational plan

- **Produce a NEW asset.** `discovery-v1-e9365edc…` is what production and local previews read. Nothing here
  overwrites it; swapping is a later, explicit, owner-authorized step.
- **Do not modify `discovery_data/manifest.json`** — `tests/test_cert01_grading_validator.py` resolves the
  real artifact through it. Stage v3 in its **own** directory and point `GENIZAH_DISCOVERY_DATA_DIR` at it.
  `discovery_data/live/` already holds the serving artifact by hardlink; do not overwrite it either.
- **Detached, never as a Claude Code child process** — a CC-child job dies with the session. `screen`/`nohup`
  or a scheduled task, then poll.
- **Checkpoint everything.** Prior runs were lost to restarts; the novelty run's own
  `*_checkpoint.jsonl` + `*_cost_log.jsonl` pattern is the model to copy, and its cost log is the only reason
  the real $40.12 is knowable today. Every long stage writes a resumable checkpoint and a real-cost log.
- **Check `ix_evidence_claim` exists on the destination before the finalize step**, not after. It is present on
  the source (§1.1); that proves nothing about the destination.
- **py-spy for a live CPU-bound hang**, not guesswork.
- **Cannot run in a worktree** — no gitignored data there: no sidecars, no source DBs, no `.env`, no
  `.masking_patterns`. This runs on `master-main` in the main tree.
- **Coordination with the concurrent code session:** do not edit `web/pages/findings.py`,
  `web/components/discovery_panel.py`, `shared/discovery_panel_model.py`, `shared/discovery_service.py`. If
  the bake needs a service change, write it up and hand it over. Stage explicit paths on every commit; never
  `git add -A`.

---

## 8. Owner questions

**Blocking — the bake should not start without these**

1. **§1.3 authorization discrepancy.** Was the 2026-08-03 novelty production run authorized at
   `batch_size=10`? Its output is what the serving asset pins, and the record says the run was unauthorized.
   Also: authorize v3's fresh novelty run under a **$150** self-enforced ceiling.
2. **§3.5 MAPV2-8/-9.** Confirm the scoping pass is the first task, and that finding "matcher-level, needs a
   fresh heavy Track-1" is an acceptable outcome that comes back for re-planning rather than being absorbed.

**Non-blocking — needed before the corresponding step**

3. **§3.6** The 123 restricted-work genres: curate now, or accept a private-verifier failure while the public
   release passes?
4. **§5** The conservative headline option — gate heavily-quoted mega-works out of the same-work headline
   surface at launch, or ship the measured surface as-is?
5. **§3.7** Confirm `band_precision` / CERT-01 re-registration is deferred past this bake.
6. **`discovery-v3-naming.md`** — acknowledge the rename, which supersedes wording in the owner-ratified
   `discovery-coordination.md` §1.

---

## 9. Masking

Tracked file. Restricted corpora appear here only as **M-source** / **R-source**, never by name. Opaque work
ids (`M:` / `REF2:` / `J:` prefixed, `w######`) and Hebrew content are safe per the codename rule; the corpus
name is not, in code, comments, specs, fixtures, logs, error paths or commit messages. `--strict` requires
BOTH `--scan-repo` and `--scan-asset`; SQLite needs `--scan-sqlite`; unset `MASKING_SCAN_PATTERNS_FILE` fails
closed (exit 1) and is **never** a silent green.
