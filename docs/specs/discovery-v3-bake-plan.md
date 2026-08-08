# discovery-v3 bake plan — the gen-2 evidence refresh

**Status: 🛑 CODEX ROUND 2 = CHANGES-REQUIRED (2026-08-07). DO NOT EXECUTE. 3 BLOCKERs, all three of round 1's
BLOCKERs re-opened as NOT CLOSED** (`discovery-v3-bake-plan.CODEX-REVIEW-R2.md`).

> **The central finding, and it is correct: my router ingest is not wired to anything.**
> `scripts/build_discovery_sidecar.py` neither imports nor calls `v3_routing_ingest`; `_ingest_tier_a` still
> marks every tier-A row shipped and `build_claims_and_evidence` still calls `apply_lever1_coverage`.
> **Verified by grep: zero references.** So the 30,899 demotions I measured would happen anyway, and the
> module I described as "closing blocker 2 in code" is, in Codex's word, decoration. I reported a decision as
> implemented when only its reader existed.
>
> Codex's round-1 disposition, verbatim: **BLOCKER 1 NOT CLOSED · BLOCKER 2 NOT CLOSED · BLOCKER 3 NOT
> CLOSED**; five HIGH items PARTIALLY closed. Two of those three I had already labelled "engineering owed" —
> which round 2 rightly calls laundering an unsolved problem into a promise.
>
> **New HIGH/MEDIUM findings worth naming, because each refutes something I asserted:**
> - The parity report is **not a parity gate**: it compares thresholds inside the source, never an *emitted*
>   result, so nothing checks that the built asset matches the router.
> - `derive_shadowed_by` halts on a mixed producer unit and then **reduces to `(page_id, ref_work)`
>   unchecked** — two distinct claims on one page/work can silently overwrite each other.
> - R-source containment is **not enforced at the consumer boundary**: a caller pointed at any other research
>   DB bypasses my filter entirely.
> - `_EXPECTED_TIER_A_ROWS == 275,894` is **numerical agreement, not evidence of causation.** I read a
>   coincidence as proof the contract was pinned against this population. Codex is right that I cannot know
>   that without the recorded derivation.
> - The state ledger is **single-writer safe only**; two processes sharing a directory can lose a step, and the
>   temp sweep could unlink a live writer's temp.
> - My "the builder's own reader accepts the slim DB" test calls **only `select_shown_works`** — not
>   `_ingest_tier_a`, `PageTextIndex`, `_compute_htr_snapshot_hash`, or either build mode. It cannot establish
>   the compatibility I claimed.
>
> Codex found no literally un-failable test this round, but judged several **insufficient** — the router suite
> never reaches the builder, the research-DB test never reaches real ingestion, the shadow suite omits the
> page/work collision, the ledger suite omits concurrent writers. Caveat on its own verdict: it **could not
> execute** the suites (no working interpreter in its sandbox), so its review is static and source-grounded.
>
> **Nothing is approved. No heavy run, no spend.** The honest state: three modules exist and pass their own
> tests, none is connected to the pipeline, and the three original blockers are open.

> **⚠ Correction, 2026-08-07.** This line previously read "🟢 CLEARED TO BUILD", which **overstated the
> record**: the only Codex verdict on file is **CHANGES-REQUIRED**
> (`discovery-v3-bake-plan.CODEX-REVIEW.md`). I folded the findings in and then marked it green myself; the
> owner then authorized execution. That is an owner go-ahead, **not** an external approval, and this project's
> convention is a convergence loop — re-review until no HIGH remains. Round 2 is now running over the amended
> plan **and** the three modules actually built since (`v3_bake_state`, `v3_build_research_db`,
> `v3_routing_ingest`). Two of the three blockers are rewritten as *engineering owed* rather than closed, so a
> reviewer may still reject the approach; the third was closed in code Codex has not seen.
> **No heavy run and no spend until round 2 returns.** Owner asked for this explicitly.
Codex review 2026-08-05 returned 3 BLOCKERs + 5 HIGH + 2 MEDIUM + 1 LOW
(`discovery-v3-bake-plan.CODEX-REVIEW.md`); all are now either **decided** (§5.0 decision table) or **converted
into engineering owed before the corresponding step**, with five new gates (§6 gates 10–14) covering exactly the
three blockers. Owner authorized execution 2026-08-05 ("Let's go").

**What the blockers changed, in one line each:** the routing is now **ingested, not recomputed** (so the
handoff's validated quality actually transfers, and the D-17 order is re-derived rather than inherited);
`w_start`/`w_end` needs a **declared multi-span projection**, not the scalar join originally assumed; and
novelty reuse is gated on a **per-pair input fingerprint** rather than the grain key alone.

**Read §5.0 first — it is the operative scope and supersedes any conflicting prose elsewhere in this file.**
Sections that were reversed during planning are kept in `<details>` blocks so the reversals stay auditable.

| Codex finding | what it refutes | my assessment |
|---|---|---|
| **BLOCKER 1** — the adapter cannot deliver `w_start`/`w_end` | §1.2's "one debt already paid" | **Conceded.** The offsets exist in gen-2's evidence, but `_ingest_tier_a` reads `spans_json` and emits only the **largest page-side span**; it never reads `ref_spans_json`/`ref_start`/`ref_end`. Gen-2 stores *multiple dual-side spans per match row*, so a scalar join has **no defined choice**. The data is free; carrying it is not. Needs a declared page-span→reference-span projection + a multi-span parity gate. |
| **BLOCKER 2** — the bake drops gen-2's coverage-router semantics | §3.1a's "translation layer", §3.4's inherited D-17 order, and every quality figure borrowed from the handoff | **Conceded, and it is the most consequential.** The v3 two-surface split lives in gen-2's `coverage_route` table (own grain, own threshold t=0.2984). **The builder never reads it** — it recomputes coverage from `pages.text` + `matched_letters` and applies v2's Lever-1 rule. So feeding gen-2 rows through this builder yields **v2 routing**, and the handoff's validated ~0.89 headline precision would **not** transfer. This is a real decision, not an adapter detail. |
| **BLOCKER 3** — cache reuse does not prove an unchanged question | §1.4a's reversal | **Partly conceded.** Codex **confirms** my core point: band, coverage, routing, matched-letters, competing works and span text are *not* in the prompt, so the engine change genuinely cannot move the answer. But `render_case` also sends the **claimed title and author**, read from the baked `works` row — which `sys_id::w######` does not pin — and neither the alias-group artifact nor the finding-aid DBs are pinned. Reuse survives, but only behind a **per-pair input fingerprint**, which is what my brief's spec described and the artifact does not implement. |

**Five HIGH findings, all accepted:** the read surface needs more columns than §3.1a lists and `source_corpus`
is **never read** (so that derived column was pointless); the "52-work gap" is a *current-policy classification*,
not a demonstrated cause of the missing crosswalk entries; **MAPV2-8 is internally inconsistent across §3.5 / §5
/ §8** (the DO list says 152 while carrying the 595 blast radius); `shadowed_by` must be derived at the
producer's `(claim_id, ref_work)` grain with a **halt on any mixed group** rather than resting on this run's
observation; and R-source needs a **fail-closed input gate on the slim table** (gate 2 checks mapping
completeness, not absence, so a stray row would resolve and pass). Plus MEDIUM: gates 6/7 carry no failure
demonstration, and the masking self-test uses a *synthetic* pattern so it cannot attest the real pattern set is
complete or current.

**Net effect on the estimate:** the ~2-day figure assumed an adapter. Blockers 1 and 2 make it a build with
real decisions in it.

### ✅ OWNER DECISION 2026-08-05 — blocker 2: ingest gen-2's router

*"yes of course"* — **the builder learns gen-2's sorting.** The two-surface split (`same_work` witness vs
`parallel` quotation) is the improvement the whole refresh exists to deliver, and it is what the 400-card
grading actually measured; shipping v2 routing over gen-2 evidence would be effort spent to lose the gain.
So: **ingest `coverage_route` with a declared mapping and parity checks**, per Codex's first option — do NOT
recompute coverage and do NOT inherit the v2 Lever-1 → D-17 order by assertion; re-derive that order against
the ingested router.

### Owner access to the PRIVATE (M-source) items — verified 2026-08-05

**On the website: no, and there is no switch.** `web/discovery_assets.py` hard-rejects any artifact whose
`meta.audience` is not `public` — `_PUBLIC_LOADER_AUDIENCE = "public"` is a module constant, not a setting, and
a missing/empty audience is rejected too ("reject-incompatible"). The only environment variable in that module
selects the data *directory*, so even a local instance pointed at the private file refuses to load it. There is
no per-user unlock and no admin view, by design: an owner-visible web surface would put the private artifact on
the web box, which is the thing the masking posture exists to prevent.

**Locally: yes — and this is already the established pattern.** The private artifact (all 1,269 works incl.
every M-source work) lives on this machine, and owner review has always run through local `*.PRIVATE.*`
artifacts rather than a web surface — currently ten of them, including full HTML decks
(`discovery-v2-REVIEW`, `PREDEPLOY-candidates-by-work`, `novelty-FILLSGAP-by-work`), a local review server
(`discovery-v2-review-server.PRIVATE.py`) and `msource-owner-title-map.PRIVATE.csv`.

**v3 therefore owes a private review deck**, generated locally and never deployed, covering the M-source items.
Added to §5 as a DO item.

**Worth noting, since it is nearly free:** the 2,686 D-06-excluded works (§3.1) will be in *neither* artifact
under the recommended scope — but a **local review deck over the excluded set** would let the owner see the
liturgy findings and judge whether the coverage justifies building the containment fix. Precedent exists in
exactly that shape: `EXCLUDED-STRONG-nonbible.PRIVATE.html`. It touches no masking posture, because it never
leaves the machine. **✅ Owner asked for this 2026-08-05 — it is a DO item.**

### 🟡 Owner request 2026-08-05: an internally-shareable private artifact, INCLUDING R-source

Two parts, and the first is settled while the second is not.

**(a) Internal sharing of the private artifact — supported, and verified rather than assumed.** I ran the
cell-by-cell masking scan (`--scan-sqlite`) over the existing private asset
`discovery-v1-136rebuild.db`: **CLEAN**, with the fail-closed control confirming exit 1 when the pattern file
is unset. So the private artifact carries **no restricted corpus name in any cell** — the masking rule is
already satisfied *inside* it; what makes it private is the *works and assertions* it exposes, not leaked
provenance strings. That is what makes internal team sharing feasible at all.

Two conditions on sharing it, both real:

1. **It must never reach the web box.** The public loader refuses it (`meta.audience != 'public'` →
   reject-incompatible), so an accident is caught — but the deploy path is the risk, not the loader.
2. **The recipients inherit the masking obligation.** "Internal" means people who may see restricted-corpus
   *works*; the corpus *name* is still masked from them, since the artifact does not contain it.

**Recommendation:** ship the private DB **plus** a generated `PRIVATE.html` deck, since a browsable deck is
what internal reviewers actually use (three exist today) and a 470 MB SQLite file is not a sharing format.

**(b) Adding R-source to the private artifact — this cannot ride along, for a reason that is not policy.**

Measured, not assumed:

| | |
|---|---|
| R-source in the **v2-era** matcher | **349 works · 289,080 match rows · 110,203 unshadowed · 112,630 pages** |
| R-source in **gen-2** (`g_launch3` evidence) | **0 rows** |
| R-source through the current builder policy | **349 of 349 dropped** by the D-06 genre filter (15 distinct genre values, none in the literary keep-set) |

**The blocker is that gen-2 never matched R-source.** `g_launch3` — the evidence this whole bake ingests — has
**zero** R-source rows. So there is nothing to include: including R-source means **running the matcher over it**,
which is the heavy Track-1 run this plan was scoped to avoid, and which the handoff explicitly puts on a
separate downstream track (*"R-source (`RS:`) is the separate downstream G-R, NOT in this handoff"*).

Falling back to the v2-era R-source rows instead is the wrong shape and should not be done quietly: they were
produced by the **old** engine, so they carry none of gen-2's routing, shadowing or chronology — mixing them
into a v3 artifact would put un-routed old-engine assertions beside routed new-engine ones under one label,
which is precisely the "new data sorted the old way" failure the owner just rejected for the corpus as a whole.

**And the eligibility question is unresolved, independently of engineering:** R-source is **~86% post-Genizah**
(responsa, Shulchan-Aruch tradition, Rishonim→Acharonim, Hasidut, modern encyclopedias). A Genizah page
"matching" a 19th-century commentary is usually the *commentary quoting the source*, not a witness — the same
containment failure as the liturgy class, but far more prevalent. Its own memo says it "needs careful
eligibility handling before ingest."

**Three honest options:**

- **(i) v3 ships without R-source** (recommended) — the private artifact still gains everything else, and R-source
  gets its own run rather than a rushed inclusion.
- **(ii) A separate R-source Track-1 run**, then a v3.1 private refresh — real work (a heavy run), and it needs
  the eligibility rule first.
- **(iii) A local R-source review deck built from the EXISTING v2-era rows**, clearly labelled old-engine and
  never merged into the artifact — cheap, gives the owner visibility now, decides nothing.

**Recommendation: (i) + (iii).** That gives the internal artifact promptly and puts R-source in front of the
owner without smuggling old-engine assertions into a new-engine release.

### Promote-to-public — DEFERRED to a follow-on (owner, 2026-08-05)

Recorded so the mechanism is not re-derived later. Visibility is **derived, not set**: `identity_visibility`
reads `works.source_corpus` and `assertion_visibility` reads the build-time `assertion_source_corpus`, both
through a fail-closed `_corpus_code_to_visibility` where anything not exactly `sefaria`/`ja` is `private`.
**There is no per-work override hook** (grepped: no `override` / `allowlist` / `force_public` in
`shared/discovery_visibility.py` or `scripts/project_discovery_public.py`).

Promotion is nonetheless cheap because the public asset is a **projection** of the private one
(`scripts/project_discovery_public.py`), a separate build step over the same membership. An owner-approved
promote-list would be a small, well-shaped addition to that projection, matching the existing D-08 pattern
where owner approval gates what ships. **No re-bake, no re-match, no re-grading** — regenerate the projection
and redeploy.

Two things a future session must not skip: each promoted work needs its **title cleared as publishable**
(D-08's whole purpose), and promoting a work makes `is_public` non-derivable from corpus alone — so the
projection's closed-graph and leak-control checks must treat the promote-list as an input, not a bypass.
**Do NOT implement in the v3 bake.**

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
| **Cache reuse IS valid** | ⟨§1.4a, **reversing §1.4**⟩ The cache is keyed on grain alone with a whole-file hash pin — but both model inputs (finding-aid text; work identity) are unchanged for any pair reached through the same crosswalk, and novelty is orthogonal to band by contract. Measured **87.6% reuse** at headline scope → **≈$4**, not $40. One guardrail: never re-grain an existing `w######` in place. |
| **No second heavy run, and no week** | MAPV2-9's mechanism does not exist in the gen-2 lineage; MAPV2-8 is a 301-claim ingest exclusion (§3.5). And the ingest is **S, not L** (§3.1a): the bake runs in ~5 min, and the builder reads only `track1_matches` + `pages`, which gen-2 already provides at 12-of-14 columns. **~2 days end to end**, not a week. |
| **No corpus expansion** | gen-2's 4,160 works are a strict **subset** of the v2-era matcher's 4,509. **CERT-01 needs no re-registration on that ground** (§3.1). The 349-work difference is **R-source**, correctly absent — and it must stay out (§3.1, R-source). |
| **🛑 One decision REOPENED** | The owner's "mint the 2,738" was given on my wrong premise that they were an accidental gap. **2,686 of them are D-05/D-06 exclusions** — 82% liturgy and poetry — dropped for masking risk, title-curation burden and claim quality. The real accidental gap is **52 works / 0.03% / zero shipped claims**. Adding the rest is the deferral D-05 pointed at *this* track, but it needs neutral titles, a masking review and a containment fix — none of which exist. **Recommended default: honour D-06 for this bake.** |

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

> **✅ RESOLVED 2026-08-05 — the owner confirms the run happened.** So `136-NOVELTY-RUN.md`'s "remains
> UNAUTHORIZED / was NOT executed" is a true statement about *that session*, superseded by events, and the
> re-scope-to-`batch_size=10` reading below was correct. The trail corroborates it independently:
> `136-REBUILD-GATES.md` records the cache `eb6fc4f8…` as a hash-pinned gated PASS, and that is the hash the
> live asset pins. **`.planning/ROADMAP.md` SC-6 corrected the same day** on both stale claims (the run's
> status and the `~$301` figure). Nothing is owed here; the paragraph below is kept as the record of how the
> discrepancy was found and what it looked like before it was answered.
>
> **⚠ The original discrepancy, as written before the owner confirmed it.**
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

**⚠ REVERSED 2026-08-05 — see §1.4a. The "do not reuse" recommendation below was wrong**, and it would have
spent ~$30–55 to re-answer questions that had not changed. The cache-key *description* above is accurate; the
conclusion I drew from it was not.

<details>
<summary>The superseded recommendation, kept because the reasoning is instructive</summary>

This lands in the same place the standing rule already requires — *novelty is granularity-relative; recompute
at the new granularity, never migrate* — but for a sharper reason than economy. **Recommend: compute v3
novelty fresh. Do not top up or key-reuse the v2 cache.** §4 shows this is affordable.

</details>

### 1.4a Reuse IS valid — what actually changes between v2 and v3, input by input

I argued against reuse because "gen-2 alters the input free text and the work grain by construction." **The
free-text half of that is simply false**, and it is the half that mattered.

The model is asked: *does any finding aid already record this manuscript as carrying this work, and how does
its wording relate to our claim?* Two inputs:

| input | changes v2 → v3? |
|---|---|
| the finding-aid evidence (FJMS `catalog.TitleHeb`/`GenizahTitleOrgTitle`, bib, FGP, PGP free text) | **No.** These are external reference sidecars. Untouched by a discovery re-bake. |
| the `(manuscript, work)` identity | **No, for any pair reached through the same crosswalk** — same `w######`, so the same alias group, so the same `work_key`. |

What v3 *does* change is **whether we assert a pair, and with what band, coverage and routing**. Novelty is
**orthogonal to band by design** — the frozen contract says it never feeds "band assignment, ranking, precision
copy or styling." So the engine change cannot change the novelty answer for a pair that exists in both.

**Therefore a cache hit on an unchanged grain is a valid answer to an unchanged question**, and keying on grain
— which I criticised — is exactly what makes that checkable. Pairs on the 2,738 newly minted works have no
entry and are honest misses, which is the "recompute at the new granularity" rule enforcing itself.

#### The one guardrail that makes this safe

**Never re-grain an existing `w######` in place.** If a work's identity is refined — handoff §6.3's
Bible→chapter, Talmud→folio+amud — it must receive a **new** id so the key changes and the cache misses. Keep
the id while changing what it means and cached verdicts become silently wrong, and **the 4,052
`aid_more_specific` verdicts would flip first**, because that shade is *precisely* a statement about relative
granularity. Concretely: **do not run the granularity stage in the same bake as the novelty reuse** unless
re-grained works are minted as new ids.

**A second reason not to touch the model, better than the cost one.** A model/prompt/effort change would leave
a **mixed** cache — some entries from the old configuration, some new, indistinguishable by key, with only the
sibling manifest (which describes one configuration) to vouch for all of them. Homogeneity, not price, is the
argument. Re-pinning the file's SHA-256 after adding entries is expected and fine; the pin exists for
reproducibility, not immutability.

**Checked:** the cached values are already the current **ten-shade** vocabulary (`confirms`, `diverges_work`,
`refines_granularity`, `aid_more_specific`, `container_predicts`, `fills_gap`, `diverges_part`, `alias_merge`,
`not_checked`), not the superseded five-way one — so the reused entries and any new ones are the same contract.

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
| 1 | gen-2 evidence ingest (the bake itself) | ~~**L**~~ → **S**, re-measured 2026-08-05 (§3.1a) | everything | no — it *is* the bake |
| 2 | `w_start`/`w_end` **persistence** | **XS** — already in the input | Phase 136.1 PANEL-03 | no, and no reason to |
| 2b | Sefaria versemap resolution + `body↔norm_stream` map | **M** — the hardest remaining work | PANEL-03's *reference locus* only | **yes** — stage it |
| 3 | novelty recompute at v3 grain | **S** — ~$40–110 measured | findings-page candidacy filter | partially — `not_checked` is honest |
| 4 | GEN2 emitter sync (date tables) | **S** | D-17 chronological demotion | no — cheap and load-bearing |
| 5 | MAPV2-8/-9 engine debts | **S** — **scoped 2026-08-05, no heavy re-run** | headline cleanliness | no — cheap now (§3.5) |
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

**MEASURED 2026-08-05.** Both maps were tested against the real artifacts. One is trivial; the other has a
large, quantified hole.

**Manuscript axis — a string split, not a lookup.** Gen-2's `page_id` *embeds* the sys_id:
`{sys_id}_IE{ie}_P{p}_FL{fl}`. **100.00% of all 198,238 distinct page_ids match that pattern, zero
exceptions.** No crosswalk is involved and this axis carries no risk. (It is also why the MAPV2-8
exclude-list drops straight in — same id space.)

**Work axis — `discovery_data/crosswalk.json` maps raw `ref_work` → `w######`, and covers 34% of gen-2's
works:**

| | distinct works | evidence rows |
|---|---|---|
| resolve via crosswalk | 1,422 of 4,160 (**34.2%**) | 439,127 of 502,498 (**87.39%**) |
| **UNRESOLVED** | **2,738 (65.8%)** | **63,371 (12.61%)** |

The gap is almost entirely one corpus: **2,733 of the 2,738 unresolved works are M-source**, plus 5 Sefaria
and 0 JA. So the public corpora are essentially fully mapped, and the hole is the M-source long tail —
low-claim works, which is why 66% of works is only 13% of rows.

**This is the silent-under-population failure mode, now quantified rather than feared.** An ingest that
resolves what it can and moves on would drop 12.6% of the evidence and emit no error. Three ways out, and it
is an owner decision because they differ in kind, not degree:

1. **Mint** new `w######` ids for the 2,738 → the works table goes from 1,269 to ~4,000. This is what a
   membership expansion *means*, but it cascades into §3.6: each new work needs a neutral title and a genre.
2. ~~**Map up** to a parent/canonical work where one exists~~ — **TESTED 2026-08-05 and NOT AVAILABLE.**
   Stripping a trailing `_NN` sub-division suffix finds a crosswalked parent for **0 of the 2,738** works
   (0 evidence rows). The id ranges are simply disjoint: the crosswalk's mapped M-source works are
   `M:Ytext#####` (5-digit, 1,003 of them) while **2,622 of the unresolved are `M:Ytext######` (6-digit)**,
   plus 67 five-digit, 37 seven-digit and 3 Sefaria stragglers. These are **not finer grain on known works.**
3. **Drop** → lose 63,371 evidence rows (12.6%), almost all M-source. **Not as cheap as it sounds:** M-source
   *work titles already ship and already render* (the masked thing is the corpus codename and excerpt-level
   reference text, not the works' existence — v2 bake plan, §"M-source: store, do not display the locus"). So
   these are displayable findings, not internal-only rows.
### 🛑 RETRACTION 2026-08-05 — the 2,738 are POLICY EXCLUSIONS, not an accidental gap

**The owner's "we add them" was given on a premise I got wrong, so it does not cover what the code actually
does. This needs re-deciding before the bake.**

I explained the missing ids as bookkeeping — "only works whose claims survived into v2's published set ever
earned a `w######`". That is **false**. `select_shown_works` (the builder's own curation policy, D-05/D-06)
derives each work's corpus from its `cat` column and then **deliberately drops** M-source works whose `genre`
is outside a literary keep-set (Geonic · Talmud & Midrash · Karaite · rabbinic · belles-lettres · science ·
philology · translations-from-Arabic). Piyyut, documentary, modern-other and unrecognised genres are excluded
**by design, with the owner as the final gate.**

Running the 2,738 through that exact policy:

| bucket | works | evidence rows | shipped claims |
|---|---|---|---|
| **dropped by the D-06 genre policy** | **2,686** | **63,230 (12.58%)** | 42,258 |
| policy would KEEP (msource) | 44 | 114 (0.02%) | **0** |
| policy would KEEP (sefaria) | 8 | 27 (0.01%) | **0** |

**Two conclusions, both reversing what I told the owner:**

1. **Nothing is being "lost".** The 12.58% I warned about is material v2 *declined on purpose*. Honouring
   D-06, the remainder is **52 works / 141 evidence rows / 0.03% of the corpus**. Those 52 are worth a look;
   they are not a decision.

   **WORDING CORRECTED TWICE (Codex R2 then R3, both right).** Two claims are withdrawn, not softened:

   - *"the real gap"* → **current-policy drops**. Applying today's `cat`/genre selector to the missing ids
     *partitions* them; it does not establish that they lacked crosswalk entries **because** of the historical
     D-05/D-06 decision. The selector picks one representative occurrence while `cat`/genre are
     occurrence-level fields, and no audit of crosswalk creation or approval history was done. Calling the
     result "the real gap" asserts a cause that was never measured.
   - *"zero shipped claims, i.e. inert"* → **not yet established**. The zero was measured under the routing in
     force at the time, which was the legacy Lever-1 cliff. Decision 1 replaces that with gen-2's router, which
     ships a **different and larger** population (the cliff demotes 19.3% of what the router ships). Until the
     figure is recomputed under the router actually shipped, "inert" is an inference from a superseded
     measurement — and it is the load-bearing reason this population is being set aside, so it is worth
     recomputing rather than restating.
2. **"Adding them" would reverse D-06** — folding ~2,686 piyyut/documentary/unclassified works and 42,258
   shipped claims into the discovery corpus. That is a substantive product decision about what the corpus is
   *for*, not a gap fix, and it is precisely the call D-06 reserves to the owner.

**Also corrected: my "the genre debt is 99.8% already answered" finding was true but misleading.** Those works
do carry genres — and those genres are *the reason they are excluded*. Reading their presence as "curation is
nearly done" inverted their meaning.

**What survives from the earlier analysis:** the corpus did not expand (gen-2's 4,160 works ⊂ the v2-era 4,509;
the 349 difference is **R-source**, correctly absent from gen-2 and to be kept out of v3). So CERT-01 still
needs no re-registration on corpus-expansion grounds. Only the *explanation* for the missing ids was wrong.

#### What is actually in the 2,686, and why each part is dropped

Measured per genre (work counts match `134-RESEARCH.md` §"Genre signal" exactly — 2,208 piyyut / 446
documentary / 32 modern, so this reproduces the figure the policy was written against):

| genre | works | shipped claims | headline |
|---|---|---|---|
| piyyut & prayer | 1,617 | 36,753 | 22,186 |
| Andalusian Hebrew poetry | 590 | 4,076 | 2,940 |
| letters | 426 | 818 | 636 |
| essays / journalism / publicistic | 32 | 464 | 316 |
| Judean-Desert documents | 6 | 99 | 56 |
| deeds · epigraphy | 14 | 24 | 19 |

**Overwhelmingly liturgy and poetry: 82% of the works, 87% of the shipped claims.** "Piyyut/documentary" as a
label undersells what is being excluded.

**Three different reasons, not one policy:**

1. **Liturgy/piyyut — a MEASURED precision failure, not a taste judgement.** CERT-01 measured that **one
   liturgical work caused 45% of all error, and the top three 68%** (`135-09-CERT01-MEASUREMENT.md` §4). The
   mechanism, owner-identified during grading: a later halakhic code *embeds the full liturgy*, so a Genizah
   page carrying a common prayer matches **the code rather than the prayer-book** — "containment, not
   coincidence", hence systematic. And the existing safeguard cannot catch it: D-17 demotes the chronologically
   later work, but here the code **postdates the liturgy it quotes**, so demoting it is backwards. A
   containment-aware rule is the real fix and is already logged as a v3 candidate. **Note the error above was
   measured on liturgical works that ARE included — so admitting 1,617 more would amplify the system's
   single worst known failure mode.**
2. **Documentary (letters, deeds, epigraphy, Judean-Desert) — the machinery does not apply.** Discovery asks
   "which manuscripts witness this work"; a letter or a deed is a *unique document*, not a text copied across
   witnesses. *(This is the evident rationale rather than a documented one — `134-RESEARCH.md` records the
   exclusion without arguing it, so treat it as weaker-sourced than reason 1.)*
3. **Modern (32 works) — post-Genizah.** Study/journalism/publicistic material. `134-RESEARCH.md` marks these
   "owner call — likely exclude as post-Genizah", and `docs/OPEN_ISSUES.md` already flags a 19th-century
   maskilic memoir carrying 6 shipped claims as possibly not belonging in the reference corpus at all. Closer
   to a reference-corpus data-quality issue than a scope choice.

**What is genuinely lost by excluding them:** piyyut is one of the largest and most celebrated bodies of Cairo
Genizah material, so this is a real coverage gap — 22,186 headline claims is not a rounding error. The
exclusion is defensible *because the system currently gets this class wrong in a specific, measured way*, not
because the material is uninteresting.

**And gen-2's two-surface router does NOT solve it.** The obvious hope — let the coverage router send
quotations to the weaker "parallel" surface and keep the headline clean — fails on measurement: **61% of
liturgical shipped claims (22,186 of 36,753) are routed to `same_work`, i.e. the headline.** So admitting
liturgy would need an explicit liturgy/containment gate, which is the handoff's "conservative headline option"
(§5) rather than something the router gives free.

#### Yes — the exclusion IS a masking decision, and v3 is where D-05 said it would be revisited

I led with the CERT-01 precision finding. **That was not the original reason, and it understated the masking
half.** D-05 (`134-CONTEXT.md`) states the rationale verbatim:

> *"large literary works have obvious neutral titles + **low masking sensitivity** + cleaner same-work claims;
> piyyut (incipit-keyed micro-units) and documentary (letters/legal docs, often untitled) are both a
> **title-curation nightmare** AND the **highest masking risk**."*

So three reasons, and **two of them are about masking and curation, not accuracy**:

1. **Masking risk — directly the M-source privacy posture.** A large literary work's title is a well-known
   public name and reveals nothing about where it came from. A piyyut is keyed by **incipit**, and documentary
   items are often untitled and described. Those strings are exactly the kind that could de-anonymize the
   corpus — the same hazard as the signature-vocabulary rule (never annotate the codename with the corpus's fingerprint terms): pairing the codename with fingerprint text
   defeats it as effectively as the name would.
2. **Title-curation burden.** Every M-source displayable field must pass **owner review before it can ship**
   (D-08, fail-closed), because that metadata is provenance-sensitive and lives off-repo. Reviewing 2,208
   incipit-keyed works by hand is the "nightmare"; D-06 exists to keep the review set tractable.
3. **Claim quality** — "cleaner same-work claims" for literary works. Present from the start, and **CERT-01's
   45%-of-error liturgical-containment finding arrived a phase later as independent confirmation**, not as the
   founding reason.

**And the correction that matters most to the owner's instinct:** D-05 does not exclude piyyut and documentary
permanently. It excludes them **"at launch (deferred to the fast-follow / gen-2 track alongside R-source)"** —
and *this bake is the gen-2 track*. So "we add them" is **not** reversing a decision; it is the deferral
arriving at its designated venue. My retraction was right that it is a substantive decision and wrong to frame
it as a policy reversal.

What the deferral requires before it can land, none of which exists yet:

| blocker | status |
|---|---|
| neutral titles for ~2,208 incipit-keyed works, owner-reviewed (D-08) | not started — the largest item |
| a masking review of those titles (highest masking risk class) | not started |
| a containment-aware rule for the liturgy precision failure | logged as a v3 candidate, unbuilt |

The first two are bigger obstacles than the precision fix I emphasised, and the first is owner grading time —
this project's scarcest resource.

#### R-source: out of v3, and verified out

Deferred by D-05 in the same breath as piyyut/documentary, and by the handoff explicitly — R-source (`RS:`) is
the separate downstream **G-R**, *"NOT in this handoff"*. Also ~86% post-Genizah, so its relevance is a real
question rather than a scheduling one. **Verified rather than assumed:**

- `track1_matches_pilot_glaunch3_live` (the gen-2 table): **M 3,776 · REF2 299 · J 85 — zero `RS:`**.
- `g_launch3.db` evidence rows matching `RS:%`: **0**.
- the v2-era `track1_matches`: **349 `RS:` works** — i.e. R-source *is* present in that table.

**That last line is a live trap for this bake.** `select_shown_works` reads a table literally named
`track1_matches`; if the v3 research DB carried the v2-era one, 349 R-source works would enter silently. The
research DB must therefore be built with `track1_matches` **materialised from the glaunch3 rows**, and
`fullcorpus_gen2.db`'s own `track1_matches` / `track1_matches_rs*` tables must not be carried over — which is
why §7 builds a *slim* research DB rather than copying the corpus file. Gate 2 (id-map completeness, HALT on
unresolved) is what catches a mistake here.

`.masking_patterns` holds **8** patterns (count only — contents are secret; attested per run, §5.0a),
consistent with DATA-05's requirement that **R-source tokens are pre-registered** alongside M-source, so a leak
of either is caught by the same gate. *(This line said 15 until 2026-08-07. The 15 was never measured — it was
carried forward from an earlier draft. The attested count is 8, and the arithmetic reconciles exactly: 6
original → 10 after the owner added four fingerprint forms on 2026-08-06 → **8** after the two bare-Hebrew
forms were removed, those being ordinary Hebrew that matched 48 innocent occurrences in our own transcriptions.
Nothing is missing; the number in this file was.)*

**Three real options, in increasing cost:**
- **(a) keep excluded** until containment-aware routing exists — recommended for this bake;
- **(b) admit to the parallel surface only**, behind an explicit liturgy gate (not the router);
- **(c) build the containment fix in v3, then admit** — the honest full solution, and a phase of work.

**Recommended default: honour D-06** — take the 52, leave the 2,686. It costs 0.03% of evidence and no shipped
claims, keeps v3 comparable to v2, and leaves the "should discovery cover piyyut and documentary material?"
question to be asked on its own terms rather than smuggled in through a bake.

<details>
<summary>The superseded decision block, kept because the owner acted on it</summary>

### ✅ OWNER DECISION 2026-08-05: **mint them, and M-source stays private for now**

Option 1. Both halves measured before proceeding, and both make this much cheaper than the plan feared:

**(i) The mint cannot affect the public release gate.** Measured on the currently-serving public artifact: its
`works` table is **507 Sefaria + 106 JA = 613, and ZERO M-source**. M-source is entirely withheld by the
visibility axes, which is the "stays private" posture already implemented. So 2,738 new M-source works land in
the **private** artifact only; the public genre gate still sees exactly the 58 already curated
(56 Sefaria + 2 JA). **Public release is not blocked by this decision at all.**

**(ii) The genre debt is ~99.8% already answered, not 47× the work.** `track1_matches` carries `title`,
`author` and `genre` columns per work, and for the 2,738:

| | coverage |
|---|---|
| title | **2,738 / 2,738 (100%)** |
| author | 2,735 (99.9%) |
| **genre** | **2,733 (99.8%)** |

Only **5 works** lack a genre. And the source vocabulary is **34 distinct values**, so the task is a one-time
**34-row mapping** from source-genre → the curated `Parent / Leaf` taxonomy, not 2,738 individual curations.
`Unassigned` is then needed for **5 works**, comfortably inside what the frozen contract sanctions.

**Retired by this measurement:** my recommendation to mint with blanket `Unassigned`, and the warning that this
was "~47× the 58 just curated". Both were written before checking whether the source metadata carried genres.
It does.

**Still true and worth stating:** these are masked-corpus catalogue strings. The 34-value mapping and any
worklist stay on this machine — never committed, never pasted into a shared channel — and the genre *values*
must not be quoted in tracked files.

</details>

#### RESOLVED 2026-08-05 — there is NO corpus expansion (but the *reason* below is wrong; see the retraction above)

> **⚠ The "it was bookkeeping" explanation in this subsection is RETRACTED** — the missing ids are the D-06
> genre policy, not claim survival. **The conclusion still holds**: gen-2's work set is a strict subset of the
> v2-era matcher's, so there is no corpus expansion and CERT-01 needs no re-registration on that ground. Only
> the causal story was wrong. The 349-work difference is **R-source**, correctly absent from gen-2.

I raised this as possibly outranking the mint/drop decision: if v3 matched a *wider* M-source corpus than v2,
CERT-01 would be pre-registered on a different population. **Checked directly against the two match tables in
`fullcorpus_gen2.db`, and the answer is clean:**

| | distinct `work_id` |
|---|---|
| v2-era matcher (`track1_matches`) | **4,509** |
| gen-2 (`track1_matches_pilot_glaunch3_live`) | **4,160** |
| in gen-2 but **not** in the v2-era table | **0** |
| in the v2-era table but not in gen-2 | 349 |

**gen-2's work set is a strict SUBSET of v2's.** And of the 2,738 works with no crosswalk entry, **2,738
(100%) were already in the v2-era match table** — zero are new. So v3 matches a slightly *narrower* reference
corpus, not a wider one. **HANDOFF-TO-135's "same launch3 scope" claim is correct**; my inference from the
disjoint id ranges was wrong.

The real explanation is reading **(a), bookkeeping**: the crosswalk and the works table only ever received
`w######` ids for works whose claims *survived* into v2's published set. The other 2,738 were in the reference
corpus all along and simply never earned an id. **Minting them is catch-up, not expansion.**

**Consequence for CERT-01:** the corpus-expansion ground for re-registration is **withdrawn**. The population
still shifts because the engine and routing changed — an already-anticipated cascade — but not because the
corpus grew. §3.7's recommendation to defer stands, and is now better supported.

*(My earlier "evidence leans (b), weakly" read — unresolved works carrying shipped claims at 21.7% vs 20.1%
— was a red herring. It measures how often those works match, not whether they were in scope. The ratio was
never able to answer the question, which is why it needed the match tables.)*

Gate accordingly (§6, gate 2): the build must **HALT** on an unresolved id rather than skip it.

### 3.1a Re-measured: the ingest is **S**, not **L** — and "roughly a week" was wrong

I sized debt 1 as **L (≥ a week)** and called it "the irreducible core" before looking at what the builder
actually reads. Challenged on the estimate, I measured it. Four findings, each cutting the same way:

1. **The bake takes ~5 minutes.** The last comparable rebuild ran 17:13→17:17 for 268,361 claims / 297,415
   evidence rows (`discovery_data/136_rebuild.log` + artifact mtimes). Compute was never the cost.
2. **The builder does NOT consume the probe's `discovery_claim`/`discovery_evidence` schema.** Its entire
   research-DB surface is **two tables**: `track1_matches` (via `select_shown_works`, `_count_tier_a_rows`)
   and `pages` (via `PageTextIndex`, `_compute_htr_snapshot_hash`). HANDOFF-TO-135 §5's framing — *"the
   schemas differ (probe `discovery_claim` vs milestone `discovery.db`) — this is a mapping/ingest, not a
   file swap"* — points at tables the builder never opens.
3. **Gen-2 already writes a `track1_matches`-shaped table, in the same file as `pages`.**
   `track1_matches_pilot_glaunch3_live` shares **12 of 14 columns** with `track1_matches`, and
   `fullcorpus_gen2.db` holds `pages` (667,411 rows) alongside it. Missing exactly two, both cheap:
   - `shadowed_by` — join it from `g_launch3.db::discovery_evidence.shadowed_by`;
   - `source_corpus` — derive from the `work_id` prefix (`M:` / `REF2:` / `J:`), the mapping §3.1 already uses.

   It also *adds* `ref_spans_json`, the match-level work-side spans that complement `ref_start`/`ref_end`.
4. **Minting rides along.** The works table is built **from `track1_matches`**, whose rows already carry
   `title` / `author` / `genre` / `cat` — which is why §3.1's genre coverage came out at 99.8%. The 2,738 new
   works arrive through the same path as the existing ones rather than needing a separate pipeline.

**So the ingest is a translation layer — a `track1_matches`-shaped view with two derived columns — not a
rewrite.** Revised: **~1 day of engineering, one unattended overnight for the novelty run, a few hours of
gates. ~2 days end to end.**

**Where an overrun would actually come from,** stated so it is not a surprise: the verifier and the release
contract are strict and fail-closed by design, so expect rejections, and each one is a round trip. That is a
schedule risk in the *gates*, not in the bake. The one substantive unknown left is whether the two derived
columns reproduce v2's shadowing semantics exactly — gate 1 (row-count preservation) is what would catch a
divergence.

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

### 3.5 MAPV2-8/-9 (debt 5) — SCOPED 2026-08-05: **no heavy re-run needed**

> **This section was written as "uncostable until scoped", then scoped the same day. The scoping pass ran
> (reading only — no pipeline touched) and the answer is the good one: neither debt requires a fresh heavy
> Track-1 run.** The original three-way question is kept below the answer, because it records what was
> actually uncertain and what settled it.

**MAPV2-9 (cite-formula exemption + JA/HTR-tolerant citation markers) — architecturally moot for v3.**
There is **no cite-formula gate in the gen-2 engine at all**: no `CITE_MARKERS`, no `guard_cite`, no cite
gate anywhere in `rsource/scripts/`. The only `cite`/`formula` matches are an unrelated `batch-formula guard`
comment in `gen2_g_launch.py` and, in `gen2_coverage_router.py`, a `QUOTE` set of **grade labels** (the 714
graded quotations) rather than pipeline output. Gen-2 replaced the guard with a different mechanism entirely —
the **coverage router** (page-coverage ≥ 0.2984 → `same_work`; below → `parallel`). Confirmed downstream:
`span_class` in `g_launch3` takes only `distinctive` / `unknown` / `shared`, never the
`quote_ab`/`formula`/`citation` vocabulary. **So there is no wrongly-keyed exemption to re-key.** What remains
is an **outcome** check, not a code port: verify the router does not re-admit the geonic-digest family the old
exemption let through. That is a query against `g_launch3`. Cost: **S**.

**MAPV2-8 (revert 152 severe HTR-substitution pages) — did NOT ride `g_launch3`; applying it is an
ingest-time filter.** The gen-2 engine *does* read MAPV2 flag tables — `gen2_track1_run.py` reads
`mapv2_page_flags` and `stage0_sys_flags` from `fullcorpus_gen2.db` — but **neither carries substitution
severity** (`mapv2_page_flags` is `page_id, sys_id, merge_flag, weak_two_work_flag`; `stage0_sys_flags` is
`sys_id, n_pages, n_fgp_rows, fgp_disagree`). The actual exclude-list is
`same_work_spike/probe/data/substitution_risk_pages.json` (2026-07-11; 18,982 substitutions audited → 595
risky), and it is read **only by MAPV2-lineage scripts** (`audit_stage0_coverage.py`, `mapv2_deck.py`) —
**no gen-2 consumer**. Three facts make this cheap:

1. The list exists and is machine-readable.
2. Its ids are already in **gen-2's exact 48-char `page_id` space** (`{sys_id}_IE…_P…_FL…`) — no translation.
3. **Blast radius measured: 301 `g_launch3` claim rows** fall on those 595 pages (the 152 "severe" are a
   subset, so fewer still).

Cost: **S** — an exclusion at ingest.

#### The 152 cannot be reproduced from what was persisted (measured 2026-08-05)

I tried to re-derive it and could not, and the reason matters. The report defines severity by *matched-char
coverage of the HTR page*: `<0.50` → 0 pages, **`0.50–0.70` → 152**, `0.70–0.85` → 441. But the persisted
JSON stores values **rounded to 3 dp**, and the rounding is provably lossy here: 4 records store
`faithful` as exactly `0.750` while the band predicate that admitted them is `faithful < 0.75` — so their
unrounded values were below 0.75 and the file cannot represent the cut that produced it.

| cut, from the persisted file | pages | gen-2 claim rows |
|---|---|---|
| `matched_frac < 0.70` | **151** | 46 |
| `matched_frac <= 0.70` | **154** | — |
| the full persisted risky list | **595** | **301** |
| *the report's figure* | *152* | *not reproducible* |

**So there is no honest way to apply "the 152" from the artifacts on hand** — only 151, 154, or 595. The
unrounded values live in `fullcorpus_gen2.db` and the audit is re-runnable over its 18,982 substituted pages,
but that is a disproportionate amount of work to settle a one-page ambiguity affecting roughly one claim row.

**Recommendation: exclude at the full 595-risky level (301 claim rows, 0.084% of 358,206).** It is the
*persisted, authoritative* artifact rather than a derived statistic; it is conservative in the safe direction;
and it removes the reproducibility problem instead of fudging it. My earlier caution — "do not silently apply
all 595 in place of the 152, that is a larger decision" — was right to flag it and wrong about the magnitude:
measured, the two differ by 255 claim rows out of 358,206.

**One distinction I should not gloss:** MAPV2-8 asks to **revert** those pages to v1 HTR, not to exclude them.
Reverting recovers the finding with better text; excluding drops it. At 595 pages a re-match is cheap, so
revert stays available as a later enhancement — but excluding achieves the *safety* goal (no claim resting on
damaged text) without a matcher re-run, and that is the right trade for 301 rows.

<details>
<summary>The original three-way question, and what settled it</summary>

The forward ledger says these **MUST** ride any gen-2 heavy re-run. The re-keyed exemption exists — as
`cite-formula gate v11 (aligned host-side exemption)` in `same_work_spike/probe/scripts/mapv2_deck.py` — but
that is the **MAPV2 deck/product path**, a different lineage from the gen-2 engine
(`rsource/scripts/gen2_track1_run.py` → `gen2_discovery_run.py`) that produced `g_launch3.db` on 2026-07-29.
Exactly one of three had to be true: (1) gen-2 already incorporates them under another name → no cost;
(2) they are post-hoc filters applicable at ingest → **S**; (3) they are matcher-level and need a fresh heavy
Track-1 run → **L**, reshaping this plan. **Measured answer: MAPV2-9 is (1)-by-replacement, MAPV2-8 is (2).
Neither is (3).**

</details>

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
  of this bug, and it still has to be closed for the newly minted works. **But the scare in the first draft is
  retired (2026-08-05):** I wrote that minting the 2,738 would arrive with 2,738 NULL genres, "~47× the work",
  and called it the strongest argument against minting. Measured, **2,733 of the 2,738 already carry a genre**
  in `track1_matches`, from a **34-value** source vocabulary — so the job is one 34-row mapping into the curated
  taxonomy plus **5** works needing `Unassigned` (§3.1). It is hours, not weeks. Two things nonetheless hold:
  **verify coverage against every claim-bearing work in the v3 asset rather than assuming it** (that assumption
  is what caused this bug), and remember the newly minted works are **private-only**, so they gate the private
  verifier and never the public release.

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

**P is now MEASURED, superseding the illustrative rows above (2026-08-05).** I had written that P "needs the
`page_id → sys_id` crosswalk and the `ref_work → alias-work` map, so P is a build output, not an input." That
was wrong: §3.1 established both maps are computable **today** — the manuscript axis is a string split and the
work axis is `crosswalk.json` — so P was computed directly as distinct `(sys_id, w-id)` pairs. Alias grouping
(`novelty_work_key`) only collapses further, so each figure is an **upper bound**:

| scope | P (resolved) | cost | + unresolved-work pairs (§3.1) |
|---|---|---|---|
| **same_work / headline only** | 51,476 | **$32** | +13,718 |
| **shipped claims** | 86,073 | **$53** | +23,895 |
| all evidence | 153,606 | **$94** | +38,604 |

If the 2,738 unresolved M-source works are minted (§3.1 option 1), add the right-hand column: the shipped
scope becomes ~110k → **≈$68**.

**With cache reuse (§1.4a), measured against the real 65,200-entry cache — this is the operative table:**

| scope | reuse | pay for | **cost, honouring D-06** | if the 2,686 were added |
|---|---|---|---|---|
| **headline (`same_work`)** | **87.6%** (45,070 of 51,476) | 6,406 | **$3.94** | $12.38 |
| **shipped** | 57.1% (49,145 of 86,073) | 36,928 | **$22.71** | $37.41 |

**The D-06 retraction cuts this again.** The higher right-hand column paid in full for every pair on the 2,738
unmapped works (13,718 headline / 23,895 shipped). Honouring D-06 removes almost all of them — the 52 works the
policy *would* keep carry **zero shipped claims**, so they add ~nothing — leaving only genuine cache misses on
existing works. **Headline novelty becomes ≈$4.**

The headline surface reuses far better because it is the stable, high-coverage population v2 already asked
about; the shipped scope reaches further into pairs v2 never assessed.

The intersection is computed on the raw `w######` id, while the real key is the **alias-group representative** —
alias collapsing can only merge keys, so **actual reuse is ≥ these figures and the costs are upper bounds.**

**Recommend: headline scope first at ≈$4**, then extend to shipped only if the wider surface proves wanted —
that sequencing spends four dollars to learn whether the extra ~$19 is worth it. Keep the $150 ceiling as the
backstop; at these figures it is a formality rather than a constraint.

**Do not touch model, effort or prompt to save money.** Not for the brief's stated reason (they are not in the
key), but for a better one: the $0.000727 unit cost and the 78.3%-agreement re-measurement are both *of that
configuration*. Changing it discards the only validation this gate has, and the saving is at most tens of
dollars.

---

## 5. Recommended scope

### 5.0 THE OPERATIVE DECISION TABLE — dated 2026-08-05, supersedes every "DO/DEFER" line below

Closes Codex's LOW finding ("current scope is internally stale — needs one unambiguous, dated decision table").
**Where this table and any prose elsewhere in this file disagree, this table wins.**

| # | question | DECISION | authority |
|---|---|---|---|
| 1 | **Routing** — recompute v2-style, or ingest gen-2's? | **INGEST `coverage_route`** with a declared mapping + parity checks. Do NOT recompute coverage. **CLOSED IN CODE 2026-08-07** (`v3_routing_ingest.apply_router_routing`, wired in `build_claims_and_evidence`, mutually exclusive with Lever-1). **THE ORDER IS NOW STATED, not "re-derived"** (Codex R2: "'re-derive' is not an order"): ingest → **ROUTER** → D-17 → §4.5 reband. The router occupies Lever-1's slot because `apply_d17_demotion` arbitrates among the *currently-shipped* witnesses — running it first lets a work be demoted against a competitor the router is about to remove, stamping `later_shared_text` for a cause that never existed. Pinned by `test_the_router_runs_before_d17_not_after` (mutation-verified: swapping the order reproduces exactly that phantom demotion). **Two R2 corrections folded in:** the two new reason codes are now in `ROUTING_REASONS` *and* the `discovery_evidence` CHECK constraint (schema Amendment 2026-08-07 (E)) — without that every router-demoted row would have died at INSERT; and `parallel` maps to **`review_only`**, not `shipped`, because `claim_type` comes from witness span dominance and the panel's relation chip reads `claim_type`, never `routing_reason`, so a shipped quotation would have rendered as a direct witness. | owner, "yes of course"; Codex R2 |
| 2 | **Selected population** — the 2,686 D-06 works? | **HONOUR D-05/D-06** — exclude them; take only the 52 policy-keeps. Liturgy/piyyut needs the containment fix first. **WORDING CORRECTED (Codex R2, MEDIUM):** these are **current-policy drops**, not "the real gap". Applying today's `cat`/genre selector to the missing ids partitions them; it does **not** establish that they lacked crosswalk entries *because* of the historical D-05/D-06 decision — the selector picks one representative occurrence while `cat`/genre are occurrence-level, and no audit of crosswalk-creation or approval history was done. The decision to honour the policy stands; the causal claim is withdrawn. The **zero-shipped-claims** figure must also be recomputed under the router actually shipped (decision 1), since it was measured pre-router. | plan rec., owner not yet contradicted; wording per Codex R2 |
| 3 | **Novelty mode** | **REUSE the cache behind a per-pair input fingerprint**, NOT blanket reuse and NOT a full re-run. **CLOSED IN CODE 2026-08-07** (`candidate_input_fingerprint`; `load_novelty_verdicts(expected_fingerprints=...)`; both model arms record it; the RESUME path re-asks a stale checkpoint line rather than resuming it). The machinery was already specified — `build_cache_key`/`CACHE_KEY_FIELDS`/`INPUT_NORMALIZATION_SPEC` — and had no caller but a demo block; this is the bridge. Unfingerprinted or mismatched → **counted MISS**, not an error (a legitimately-changed input *should* miss, and raising would turn a routine refresh into a build failure). Manifest pins the fingerprint version, prompt hash, normalization hash and field list. Gate 13 mutation-verified: dropping `claimed_title` from the fingerprint turns it red. | Codex blocker 3 |
| 4 | **Novelty scope + ceiling** | **🛑 MEASURED 2026-08-07: reuse through the gate is 0.0%, and the ~$4 estimate is retracted.** See §5.0b. Over the **legacy** population, 0 of 55,184 residual pairs survive the fingerprint gate (every cache entry predates it), against **100%** bare key overlap — which is precisely the quantity the old "87.6% → ≈$4" figure was reporting. The comparable actual spend is **$40.12** (2026-08-03, 5,528 calls at batch 10, read from the real `usage.cost` log). **But per Codex R4 that is the LEGACY price, not the v3 price** — the v3 candidate set does not exist until the router and final work set are fixed. **Recommended: option 0 (§5.0b), a $0 re-measurement against the real v3 inputs once assembly has run**, and only then an owner decision. The owner's "$12 → go" no longer applies at either number. | owner ("$12 → go") superseded; Codex R2 + R4 |
| 5 | **MAPV2-8** — 152 or 595? | **595-risky exclusion at ingest** (301 claim rows, 0.084%). The 152 is NOT reproducible from the persisted file. Named as an *exclusion*, not the requested revert. **Owner confirmation owed** (Codex HIGH). | §3.5; supersedes the "152" in the old DO list |
| 6 | **`w_start`/`w_end`** | **Stage 1 only. CLOSED IN CODE 2026-08-07** (`project_ref_span`; schema Amendment 2026-08-07 (F)). The rule turned out to be **discoverable, not inventable**: gen-2's `ref_spans_json` carries `{p0,p1,rg0,rg1}` objects in which the producer has already PAIRED the two sides, so the projection is a *selection* among the producer's own pairs. Rule = largest page-side extent, tie-break `p0,p1,rg0,rg1` ASC. **Verified against the producer, not against itself:** its `discovery_evidence.page_start/page_end/ref_start/ref_end` tuples are drawn exactly from `ref_spans_json` (100.00% of 200,000 sampled), and this rule reproduces a producer evidence row on **381,341 of 381,341 rows (100.00%)**. The trap avoided, measured: keying on `spans_json`'s largest span (the existing R7 page-side rule) matches **no** ref entry on 12.2% of rows (46,472) because `spans_json` is a coarser HULL — that implementation would have emitted NULL offsets silently. Two ref ranges under one hull can sit 3.4M chars apart (p90 13,113), so a work-side hull would be meaningless. 22.06% of `(page, work)` pairs have multiple producer alignments; one is kept and the multiplicity is documented, never implied away. Coordinate space **named**: the reference work's `norm_stream`. Gate 14 uses a real multi-span row with the producer's real evidence rows; three mutations (order flipped / unwired / sides swapped) all turn it red. | Codex blocker 1 |
| 7 | **R-source** | **OUT of v3.** gen-2 has zero R-source evidence; including it means a new heavy run. Plus a **local old-engine-labelled review deck** for owner visibility. | owner request + §3.1 measurement |
| 8 | **Private artifact** | **BUILD IT**, internally shareable: private DB + generated `PRIVATE.html` deck. Never to the web box. | owner |
| 9 | **Excluded-set deck** | **BUILD IT** — local deck over the 2,686, so the liturgy coverage can be judged. | owner, "Yes" |
| 10 | **Promote-to-public** | **DEFERRED** to a follow-on. Do not implement here. | owner |
| 11 | **`band_precision` / CERT-01** | **DEFERRED** — blocked on owner grading, not compute. No `tier_a` number; no re-registration on corpus-expansion grounds (measured: no expansion). | §3.1, §3.7 |
| 12 | **Versemap resolution, JA divisions** | **DEFERRED.** Affects PANEL-03's reference *locus* only; the our-text-only highlight does not wait. | §3.2 |
| 13 | **Genre curation** | From `_tmp/genre-curation-58-COMPLETE.csv` (58 rows, zero blanks). Coverage **verified** against every claim-bearing v3 work, not assumed. 123 restricted-work genres deferred (gates the private verifier only). | §3.6 |
| 14 | **GEN2 emitter sync** | **DO**, and re-pin, before the D-17 step. | §3.4 |

### 5.0a Two findings from the first build step (2026-08-06) — one is a masking-gate hole

Found while enumerating the slim DB's column list (Codex's HIGH), before writing the adapter.

**🛑 FINDING A — the masking scanner does NOT catch the signature-vocabulary term, and gen-2 hands it to us
as a column name.** `track1_matches_pilot_glaunch3_live` carries a column whose name **is** the restricted
corpus's distinctive source-record term — the exact thing
[[project_msource_codename_rule]]'s 2026-07-27 refinement forbids pairing with the codename, because a
fingerprint term de-anonymizes as effectively as the name. **Measured:** a file containing only that word,
scanned with `--scan-asset` and the pattern file set, returns **"no matches — clean", exit 0.** So the D-25
gate would not stop it reaching a committed file, a shipped asset, or a log.

Consequences, in order:
1. **The slim DB must not carry that column** — it is in the "extras" the builder never reads, so dropping it
   costs nothing. Same for any derived artifact, deck, or log line.
2. **`.masking_patterns` is missing a pattern.** This is an owner-held secret file (15 patterns), so I am not
   editing it — **owner action: add the signature term.** Until then this specific leak class is uncaught, and
   the gate's green is narrower than it looks. This is exactly the MEDIUM Codex raised (the self-test needle is
   synthetic, so a passing scan does not attest the pattern set is complete) with a *concrete instance*.
3. It reinforces the owed **non-disclosing attestation** (pattern count + hash per run) — a count of 15 would
   not have revealed the gap, but a reviewed inventory would.

**✅ CLOSED 2026-08-07 — the attestation now exists** (`check_atlas_masking.py --attest`, and
`pattern_set_attestation()` for programmatic use). Codex round 2 held this open twice, correctly: a synthetic
self-test needle "demonstrates only scanner mechanics" and cannot attest that the pattern set is COMPLETE.
That remains literally true and is not claimed — completeness is unknowable from inside, since the scanner
cannot enumerate terms nobody told it about. What was missing and IS attestable is **identity**: which pattern
set ran. Emitted per run: `pattern_count`, a KEYED `pattern_set_hmac` over the sorted set (stable under reordering,
moves on any add/remove/**edit**), and the first 8 hex chars of each pattern's own digest so a reviewer can see
*which* entry changed. Non-disclosure is the design constraint — no pattern text, prefix or length, because for
a short restricted term a length plus a known alphabet is a real narrowing. Fails closed: `--attest` with no
pattern file exits 1, since an attestation of an empty set is the worst possible artifact (a build record that
looks like evidence while attesting nothing). Printed BEFORE the scan, so it is present exactly when a failing
run is being diagnosed.

**⚠️ AND IT IMMEDIATELY FOUND SOMETHING.** The live set attests **`pattern_count = 8`**, not the 15 this
document recorded. The count changed after the owner's 2026-08-06 edits (transliterations added, then the Hebrew
forms removed following 48 innocent hits), so 8 is plausibly correct and intended — but the discrepancy was
invisible until something printed the number, which is precisely the argument for the attestation.

**✅ RECONCILED 2026-08-07 — 8 is correct, and the "15" was the error.** The count was never measured; it was
carried from an earlier draft. The arithmetic closes exactly: **6** original → **10** after the owner added four
fingerprint forms (Hebrew singular + plural, transliterated singular + plural) → **8** after the two bare-Hebrew
forms were withdrawn as unusable (ordinary Hebrew; 48 innocent hits in our own transcriptions). Every stale
"15" in this file has been corrected to 8. Nothing is absent from the set — the number in the document was
wrong, not the file.

**🛑 THE DIGEST FIELD, CORRECTED 2026-08-07 after Codex round 5 (MEDIUM).** An earlier revision of this section
recorded a fixed `pattern_set_sha256` value "so a later run can prove it used the same reviewed set". That is
**withdrawn, and the value is deleted**, because it cannot do what it claimed:

- The attestation no longer emits a plain SHA-256 at all. Round 3 established that an unkeyed digest is a
  membership **oracle** (hash a candidate term, compare the prefix), so `pattern_set_attestation` now emits
  **keyed HMACs** — `pattern_set_hmac` and keyed per-pattern prefixes — and **omits every digest** when no key
  is present. A recorded unkeyed value is therefore not comparable with any current run's output, and an
  unkeyed run cannot produce one to compare.
- **The build-record contract is now:** with `MASKING_ATTESTATION_KEY` set, record `pattern_count` **and**
  `pattern_set_hmac`; a later run under the SAME key reproduces both, which is what proves the reviewed set ran.
  Without the key, record `pattern_count` only, and state plainly in the build record that **no identity digest
  is available** — a count alone cannot distinguish two 8-pattern sets.
- The key must therefore be as durable as the pattern file itself: a rotated or forgotten key makes every
  earlier `pattern_set_hmac` unverifiable, which converts the attestation back into a bare count. Treat it with
  the same handling as `.masking_patterns` (env-held, gitignored, not rotated casually).

**✅ GATE 16 IS NOW GREEN (2026-08-07) — measured, not assumed.** The earlier verdict ("the signature-vocabulary
term is still absent from the scanned pattern set") is **falsified**. The owner's addition DID land; only this
document was stale. Probed by writing each candidate form to a scratch file and scanning it with the live
pattern set — the term itself is never printed, only the exit code:

| probe | scanner verdict |
|---|---|
| transliterated singular | **CAUGHT** (exit 1) |
| transliterated plural | **CAUGHT** (exit 1) |
| bare 5-char stem | passes clean (exit 0) — *expected*: this is the `FORBIDDEN_COLUMN_SUBSTRINGS` value, cleartext in committed code by design |
| neutral replacement `src_attr_note` | passes clean (exit 0) — the control |

So the leak class the finding was about **is** caught by the scan now, in both Latin-script forms. The bare stem
is deliberately *not* a pattern: it is the denylist substring in `v3_build_research_db.py`, and making it one
would turn the repo scan red on the guard itself. The bare-Hebrew forms remain excluded by measured decision
(48 innocent hits), so for that script the slim-DB column denylist stays the operative control — which is what
it was always documented to be. Both owner actions on this gate are **discharged**.

**✅ FINDING B — RESOLVED 2026-08-06: not a coincidence. The frozen constant was pinned against the GEN-2
population.** The slim research DB was built from the real artifacts and fed to the builder's own reader:
`_count_tier_a_rows()` returns **275,894 — exactly `_EXPECTED_TIER_A_ROWS`**, on the first attempt, with no
tuning. Since the slim DB is materialised *only* from `track1_matches_pilot_glaunch3_live`, that number is a
property of the gen-2 rows, and the release contract already expects it.

So the constant does **not** need re-pinning, and gate 15's concern is answered in the reassuring direction:
the v3 tier-A population and the frozen contract agree by construction. Two things still follow:

- **Gate 15 stays**, now as a *preservation* check rather than a re-pin: if a later change to the ingest moves
  that count, the release gate must fail with the number named rather than being quietly re-pinned to whatever
  the build produced.
- **It implies the v2-era `track1_matches` was NOT what the contract was frozen against** (that table yields
  364,178 unshadowed, or 253,975 excluding R-source). Worth knowing: it means the v2 release contract and the
  v2-era research table were already out of step, which is a v2 records question, not a v3 blocker.

<details>
<summary>The original finding, before it was measured (kept — the caution was right even though the alarm was not)</summary>

**⚠ FINDING B — a frozen release constant coincidentally equals a gen-2 figure. Do not read it as agreement.**
`build_discovery_sidecar.py:5558` freezes `_EXPECTED_TIER_A_ROWS = 275894`, commented
"`track1_matches WHERE shadowed_by IS NULL`" — and 275,894 is **exactly** the gen-2 unshadowed
`(page_id, ref_work)` pair count measured in §3.1. It is **not** either v2-era figure (364,178 with R-source,
253,975 without). The likely reading is that the v2 release contract was frozen against a *narrower*
population than today's v2-era table, and the collision with gen-2 is chance. **Either way the release gate
will compare v3's tier-A count against 275,894 and either pass for the wrong reason or fail without
explanation.** Owed: establish what that constant was frozen against, and re-pin it deliberately for v3 with
the derivation recorded — never let it match by luck. Added as gate 15.

</details>

#### FINDING A — RESOLVED, and it produced a second finding the owner must know about (2026-08-06)

**Owner added the term (4 forms: Hebrew singular + plural, transliterated singular + plural) — pattern set 6 →
10.** Verified: all four forms now scan as hits (exit 1), including an UPPERCASE transliteration, which
confirms the documented casefolding; the neutral replacement name `src_attr_note` correctly passes (exit 0).
The gate hole is closed.

**Prerequisite done first:** the term was already in **four tracked files** (7 occurrences) — a fixture column
name mirroring the research table, plus two planning-doc column lists. Adding the pattern without clearing
those would have turned the gate red on already-pushed content. The fixture column was renamed to
`src_attr_note` (safe: the builder never reads it, and every fixture INSERT is positional), the doc mentions
redacted; 289 tests green.

**🛑 SECOND FINDING — the Hebrew form is an ordinary Hebrew word, and it now matches 48 INNOCENT occurrences
in tracked content.** The full `--scan-repo` is **RED**, and every hit is a false positive:

| file | hits | what it actually is |
|---|---|---|
| `web/Transcriptions_part.txt` | **47** | **our own manuscript transcriptions** — Talmudic text on modes of acquisition, where the word is the standard legal term (e.g. *"a ship is acquired by drawing… R. Nathan says"*) |
| `web/pages/help.py` | 1 (+1 as a URL-decoded form) | the Hebrew **help text**: "deleting a local join document *removes* it automatically from the community too" |

Neither has anything to do with the restricted corpus. The word is common Hebrew (*handing over / transmission
/ delivery*), so as a bare substring it cannot distinguish the fingerprint sense from ordinary usage — and
`web/Transcriptions_part.txt` is 15 MB of exactly the Hebrew this project exists to search.

**This matters beyond one red scan: `.github/workflows/ci.yml` provisions the same pattern set from the
`MASKING_SCAN_PATTERNS` secret and runs `--scan-repo` in `render-smoke-tests`. If the Hebrew forms were added
to that secret, CI is now RED on every run** — and the standing rule (correctly) forbids "fixing" that by
skipping the scan.

**Recommended fix — keep the transliterations, drop the bare Hebrew forms.**

1. **Keep the two Latin-script (transliterated) forms.** In Latin script the word appears only as a
   *technical/database* term — this
   is the form that actually leaked (a column name), and it produced **zero** false positives across the whole
   repo.
2. **Remove the two bare Hebrew forms** from `.masking_patterns` *and* from the CI secret. As a bare substring
   they are unusable: 48 innocent hits, 0 real ones.
3. If the Hebrew must be covered, use a **discriminating** form rather than the bare word — the scanner does
   substring matching, so a longer phrase pairing it with a corpus-specific context word would match the
   fingerprint sense without matching Talmudic prose. That needs a real example of the leaking phrase to design
   against; the column-name leak does not provide one.

**The compensating control stands regardless** (gate 16): the slim DB drops that column outright, so the term
never enters a v3 artifact by that route.

**Owner action:** revert the two Hebrew lines (keep the two Latin ones) locally and in the CI secret, then
re-run `--scan-repo` — it should return green. **I have not edited `.masking_patterns`;** it is owner-held.

**Still owed before execution** — the remaining Codex items, none of them owner decisions:
blocker 1's projection spec · blocker 2's declared routing mapping + parity checks · blocker 3's fingerprint ·
the full column list for the two research tables (HIGH — and drop the pointless `source_corpus` column, which is
never read) · `shadowed_by` derived at the producer's `(claim_id, ref_work)` grain with a **halt on any mixed
group** · a fail-closed R-source input gate on the slim table · failure demonstrations for gates 6 and 7.

<details>
<summary>The superseded DO list (kept for audit; items 1, 2, 3 and 6 are wrong per the table above)</summary>

**DO in the v3 bake**
1. **MAPV2-8** exclude-list applied at ingest, at the 152-severe cut (§3.5) — 301-claim blast radius, ids
   already in gen-2's page space. *(The scoping this originally called for is done — see §3.5.)*
2. The gen-2 ingest with both id-space maps (§3.1).
3. `w_start`/`w_end` stage 1, corpus-wide, coordinate space named in the schema doc (§3.2).
4. GEN2 emitter sync + re-pin, before D-17 (§3.4).
5. Genre curation from `-COMPLETE.csv`, plus a coverage check at v3's 4,160-work grain (§3.6).
6. Novelty recomputed fresh at v3 grain, under a $150 self-enforced ceiling (§3.3, §4).

</details>

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

### 5.0b MEASURED 2026-08-07 — the novelty cache reuse rate is ZERO, and the price is $40 not $4

Ran `scripts/v3_measure_novelty_reuse.py` against the current asset, the current finding-aid DBs and the
existing 65,200-entry verdict cache. Report: `_tmp/v3-novelty-reuse-measurement.json`. It calls no model and
spends nothing.

| quantity | value |
|---|---|
| cache entries | 65,200 |
| candidates from the current asset | 65,200 |
| heuristically resolved (no model, free) | 10,016 |
| **residual — would reach the model** | **55,184** |
| `residual_present_but_unfingerprinted` | **55,184 (all of them)** |
| **reuse rate THROUGH THE GATE** | **0.0%** |
| key-overlap rate (what the old figure measured) | **100.0%** |

> **⚠️ CORRECTED 2026-08-07 after Codex round 4 (HIGH), and the correction matters.** The measurement
> above was run against the **LEGACY v2 asset and finding-aid DBs** (the script's defaults), so it
> measures *"how much of the existing cache still answers the questions it was built for"* — which is
> exactly why its residual, 55,184, agrees to the row with the 2026-08-03 run. That is a real and useful
> number: it settles whether the cache survives at all, and the answer is that it does not.
>
> **It is NOT a measurement of the v3 population**, and the ~$40 is therefore not yet the v3 price. The
> v3 candidate set does not exist to measure: it depends on the router, the crosswalk and the final work
> set, none of which is fixed until the bake's earlier steps run. Round 4 was right that presenting the
> historical spend as "the price of the v3 choice" repeats the error that produced the ~$4 figure — a
> number detached from the population it describes.
>
> The script now records every input path **with its content hash** and a `population: legacy|pinned`
> label, so no future reader can mistake one for the other.
>
> **This adds a fourth option, and it is now the recommended one — option 0, at $0:** run the bake's
> earlier steps (slim DB → router → work set → assembly), then re-run
> `scripts/v3_measure_novelty_reuse.py --population pinned` against those actual v3 inputs. That yields
> the v3 residual and hence the real price, still without spending anything. Only then is the
> re-run-vs-`not_checked` decision a decision about a known number. Options 1–3 below remain the choices
> *after* that measurement; their costs should be read as the legacy-population figures they are.

**Why zero, and why that is the correct answer rather than a bug.** The cache was produced on 2026-08-03,
before the fingerprint existed, so no entry carries one. Under the gate an unfingerprinted verdict cannot prove
which question it answered, so it does not answer. The 100% key-overlap figure is precisely the number the
"87.6% reusable" claim was reporting — Codex's objection was that key overlap is not question identity, and the
gap between these two rows is that objection quantified.

**The price.** The identical run on 2026-08-03 cost a *measured* **$40.12** over 5,528 calls (batch 10,
`gemini-3.6-flash`, effort low — read from the real `usage.cost` log, never estimated). So re-establishing the
verdicts costs about **$40**, an order of magnitude above the "≈$4" the plan carried. Both the 87.6% and the ≈$4
are retracted; the owner's "$12 → go" authorization was given against a number that no longer holds.

**Why a fingerprint cannot honestly be back-filled.** The tempting shortcut is to stamp today's fingerprint onto
the existing verdicts. That asserts exactly the thing never checked — that the inputs behind each verdict are
today's inputs — and it would convert an unprovable reuse into a provable-looking one, which is worse than no
gate at all. It is not offered as an option.

**OWNER DECISION OWED — three options, with what each buys and costs:**

1. **Re-run the model arm (~$40).** Every verdict is then fingerprinted, and every later run's reuse is real and
   provable. Buys a cache that keeps working; costs $40 once. *This is the recommendation* — the fingerprint's
   whole value is future runs, and $40 is the last time this population is unpriced.
2. **Ship v3 with `not_checked` novelty on the 55,184 residual rows ($0).** Honest and free, but the "Candidates
   for new finds" surface loses its model-derived shades. The 10,016 heuristically-resolved rows are unaffected
   and stay populated — including the 8,327-row bypass path, the largest single source of that surface.
3. **Run the gate against the OLD prompt hash instead ($0, NOT recommended).** The single-case and batch prompts
   fingerprint differently by design; nothing here would let a cache built under one framing be reused under the
   other. Mentioned only to record that it was considered and rejected: it re-opens exactly the reuse-across-a-
   changed-question hole blocker 3 closed.

Nothing in the bake is blocked by this except the novelty step itself — the router, the offsets, the slim DB and
every gate above are independent of it.

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
| 6 | **Golden fixture + discovery suites** | ⟨Codex MEDIUM — was blank⟩ run the suite against a deliberately mis-mapped ingest (one `page_id` prefix corrupted) → **must go red**; a suite that passes on a broken map is testing nothing |
| 7 | **Performance** vs `discovery-budgets.md` caps | ⟨Codex MEDIUM — was blank⟩ set one cap to 0 ms → the harness must **report over-cap and fail**, proving it compares rather than records |
| 8 | **Novelty fail-closed** — out-of-vocab → `not_checked`, counted, never a positive verdict | inject a bad status → must resolve `not_checked` |
| 9 | **`divergence_correctness` NULL on every row** (ruling L, human-only) | inject a value in the cache → must be dropped + counted |
| **10** | **✅ CLOSED 2026-08-07 — routing parity** (Codex blocker 2), now asserted on the **EMITTED** result rather than on the source. R2's finding was exact: the first `parity_report` compared two thresholds *inside the source DB* and never checked the built asset. `assert_emitted_parity` now fails on any undecided spec (the fallback was `shipped` — the very bypass this replaces), on any reason code outside the declared mapping, and on a wipe-out relative to what the mapping would ship for the rows actually considered. `load_router` additionally HALTS on a duplicate `(page_id, canonical_work_id)` (agreeing duplicates inflated `counts` while replacing the entry, so the report was at neither grain) and on a row whose `shipped` flag contradicts its `surface` (the column was read and thrown away). Tests drive `build_claims_and_evidence` itself, and one drives a REAL INSERT against the real DDL — the gap that let an unwired module pass 8 tests. Mutation-verified. |
| **11** | **✅ CLOSED 2026-08-07 — `shadowed_by` grain**, both halts. The mixed-unit halt existed; R2 found the **reduction** to `(page_id, ref_work)` unchecked and named the two cases a mixed-unit test structurally cannot see: (a) a wholly-unshadowed unit plus a wholly-shadowed one on the same key — neither is "mixed", and the shadowed one silently drops the other's rows out of tier A; (b) two shadowed units with different values → last-write-wins. Both now halt as "not injective". Measured: ZERO such collisions on `g_launch3` today, which is exactly why it is asserted rather than assumed. Fixtures cover both cases plus two controls (agreeing units, and different works on one page, must still pass). Mutation-verified. |
| **12** | **✅ CLOSED 2026-08-07 — R-source gate, at the CONSUMER boundary.** R2 was right that the slim builder's filter is defense in depth, not the gate: `select_shown_works`, the review-only path and `--from-approved` each build from whatever DB path the operator supplies, and `select_shown_works` has no prefix rejection — so pointing the build at the gen-2 corpus file (whose own `track1_matches` is the v2-era table with 349 restricted works) reaches a sidecar without the slim builder running at all. The gate now lives in `_connect_research_ro`, the ONE place every entrypoint opens a research DB, and returns a source-table fingerprint (row count + column set). Tested by direct invocation with a planted prefix; a third test asserts the error names no work id, because a containment report that leaks defeats itself. Mutation-verified. |
| **13** | **✅ CLOSED 2026-08-07 — novelty input fingerprint.** `candidate_input_fingerprint` bridges a real candidate to the already-specified `build_cache_key`/`CACHE_KEY_FIELDS` (whose only prior caller was a demo block). Consumer: unfingerprinted or mismatched → **counted MISS**, never an error. Producer: both arms record it, and the RESUME path re-asks a stale checkpoint line rather than resuming it — the one place a stale answer is indistinguishable from a finished one. Manifest pins version + prompt hash + normalization hash + field list. Mutation-verified three ways including the exact case R2 named (dropping `claimed_title` → red). One test imports the REAL renderer and requires that any field which changes the rendered prompt also changes the fingerprint, so a future prompt field cannot quietly escape. |
| **15** | **⚠️ PARTIAL 2026-08-07 — provenance recorded, inference WITHDRAWN.** R2 was right that exact agreement between the constant and the slim DB's count "is not evidence of causation" — it proves only that this transformation currently produces that number. The plan's inference is withdrawn. Recorded at the constant: the query, the dated measurement (275,894 unshadowed of 381,341; 105,447 in shadowed units), the halting derivation, and the operative rule — **the value is NEVER edited to make a run pass**. A mismatch means the population changed, which needs a decision, not a new constant. Still owed: the pre-build source-identity record in the build manifest. |
| **16** | **✅ CLOSED 2026-08-07 — the signature-vocabulary term** (§5.0a finding A) appears in no slim-DB column **name**, shipped artifact, deck or log. *(Scope corrected the same day: the term is absent from column NAMES, but the restricted corpus's own name IS a `cat` **value** on 106,887 slim-DB rows — see §7a. That is a separate, contained finding, not this gate.)* The earlier "currently FAILS this control" verdict is **falsified**: the owner's addition had landed and only this document was stale. Proven by the control itself — each candidate form written to a scratch file and scanned with the live set (the term is never printed, only the exit code): **transliterated singular → CAUGHT (exit 1)**, **transliterated plural → CAUGHT (exit 1)**, neutral replacement `src_attr_note` → clean (the control), bare 5-char stem → clean *by design* (it is the `FORBIDDEN_COLUMN_SUBSTRINGS` value, cleartext in committed code, so making it a pattern would redden the repo scan on the guard itself). Bare-Hebrew forms stay excluded by measured decision (48 innocent hits in our own transcriptions), so for that script the column denylist remains the operative control — as always documented. | plant the term in a scanned file → must be reported. **Done, both Latin-script forms.** |
| **14** | **✅ CLOSED 2026-08-07 — multi-span offset parity** (Codex blocker 1). R2 called the promised gate "a placeholder" for specifying no selection rule, tie-break, or source relation; all three are fixed, and the rule is **discovered, not invented** — gen-2's `ref_spans_json` already pairs the sides, so the projection is a selection among the producer's own pairs (largest page-side extent, tie-break `p0,p1,rg0,rg1` ASC). Verified against the producer: its evidence tuples come exactly from `ref_spans_json` (100.00% of 200,000 sampled) and this rule reproduces one on **381,341/381,341 rows (100.00%)**. Fixture is a real multi-span row whose `spans_json` hull matches NO ref entry — the 12.2% (46,472-row) case a hull-keyed projection would have silently NULLed, kept as an explicit control. Three mutations (order flipped / unwired / sides swapped) all turn it red. |

**On the masking gate (Codex MEDIUM — attestation DELIVERED 2026-08-07).** The fail-closed control and
`--self-test` prove the *mechanism* runs and can return non-zero; they do **not** prove the loaded pattern set
is complete, because the needle is synthetic. That limit is permanent and is not claimed away. What was owed and
now exists: `check_atlas_masking.py --attest` emits a **non-disclosing** attestation per run — `pattern_count`,
a KEYED `pattern_set_hmac` over the sorted set (stable under reordering, moves on any add/remove/**edit**), and each
pattern's own 8-char KEYED digest prefix so a reviewer can see *which* entry changed. No pattern text, prefix or
length is emitted; length is omitted deliberately, because for a short restricted term a length plus a known
alphabet is a real narrowing. Fails closed (no pattern file → exit 1) and prints **before** the scan, so it is
present exactly when a failing run is being diagnosed. **It immediately found a discrepancy: the live set is 8
patterns, not the 15 recorded here** — see §5.0a. Still owed: recording the scanned asset/sqlite paths and
post-build hashes alongside it.

**Order of operations: RE-DERIVED AND STATED (2026-08-07).** Codex R2's objection to the previous wording was
exact — *"'re-derive' is not an order"*. The v3 sequence is:

> 3. ingest sources → 4. **ROUTER routing** → 5. D-17 chronological demotion → 6. §4.5 reband

The router takes Lever-1's slot for a substantive reason, not to minimise the diff: `apply_d17_demotion` groups
*the currently-SHIPPED* track1_direct witnesses and mutates `routing_status` as it walks each page
earliest-first. Running it before the router would let it arbitrate between rows the router is about to demote —
so a work could be demoted for being chronologically later than a competitor that never ships at all, and the
survivor would carry `later_shared_text` naming a cause that never existed. Running the router first makes
D-17's input exactly the population that ships, which is the invariant the v2 Lever-1-before-D-17 order held.
Pinned by `test_the_router_runs_before_d17_not_after`; swapping the order in the source reproduces precisely
that phantom demotion. The v2 §6 rationale still applies to everything downstream of routing.

<details>
<summary>Superseded: "inherit §6 unchanged" (kept — it was right for a recomputing builder, wrong for an ingesting one)</summary>

**Order of operations: inherit `discovery-v2-bake-plan.md` §6 unchanged** — one unified sequence, Lever-1
coverage routing **before** D-17. That order was corrected once already after a Codex round; re-deriving it is
how it gets broken again.

</details>

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

### 7a. MEASURED 2026-08-07 — the slim research DB carries the restricted corpus NAME in `cat` values

Found by actually running `--scan-sqlite` against the built slim DB rather than reasoning about it.
**106,892 hits, exit 1:**

| where | hits | what it is |
|---|---|---|
| `track1_matches.cat` | **106,887** | one of ten distinct `cat` values IS the restricted corpus's own name, used as gen-2's source label |
| `pages.text` | 5 | our own manuscript transcriptions — the same innocent-Hebrew class as the 48 hits in §5.0a |

**This is contained, and the shipped sidecar is unaffected.** Verified on the artifact, not asserted:

- `_map_cat_to_source_corpus` masks **by elimination** — the open-corpus set maps to `sefaria`, `JA` to `ja`,
  and *everything else* falls through an else-branch to `msource`. So the builder never compares against or
  writes the restricted name. Confirmed by running the real function over all ten live values: the restricted
  one → `msource`.
- The slim DB lives in `_tmp/`, is **gitignored** (`.gitignore:184`) and **untracked**; `--scan-repo` stays
  clean with it on disk.

**The operative rule this establishes:** the slim research DB is an **intermediate**, inside the masking
boundary — not a publishable artifact. It must never be committed, attached, scp'd, or included in a deck or
handoff. Only the built sidecar crosses the boundary, and it crosses because `source_corpus` is a masked
enum rather than a copied label.

**Also a correction to my own earlier claim.** Gate 16's line said the term "appears in no slim-DB column".
The *column* is indeed dropped (`FORBIDDEN_COLUMN_SUBSTRINGS`), but a column-name guard says nothing about
**values**, and the corpus name is a value on 106,887 rows. The gate wording is corrected above. The lesson is
the familiar one: the denylist I wrote guarded the axis I was thinking about, and the scan found the axis I
was not.

**Owed at bake time:** run `--scan-sqlite` against the **built v3 sidecar** (not the slim DB) and require
clean, which is the assertion that actually matters. `--scan-sqlite` on the slim DB is expected to be RED and
must not be "fixed" by dropping `cat` — the builder needs it to derive `source_corpus`.

---

### 7b. RESOLVED 2026-08-07 — the assembly's exact input set, and two gates that earned their keep

Recorded because none of it was written down and I had to derive it. The v3 assembly's twelve inputs:

| input | resolved path | pin |
|---|---|---|
| source DB | `_tmp/v3_research_slim.db` (built §7a) | `tier_a_unshadowed=275894`, matches the frozen constant |
| approved works | `discovery_data/discovery-review-approved-final.csv` | — |
| crosswalk | `discovery_data/crosswalk.json` | `bcde04bd…` |
| canonical merges | `same_work_spike/probe/rsource/data/v2_canonical_merges.build.json` | `cc054d11…` |
| composition dates | `discovery_data/composition_dates.json` | `2b46b470…` |
| seftja dates | `same_work_spike/probe/rsource/data/seftja_dates.json` | `00760289…` |
| novelty verdicts | `discovery_data/novelty_production_verdicts.json` | `eb6fc4f8…` |
| work domains | `discovery_data/work_domains-v1.json` | `sha256:57393773…` |
| work author aliases | `discovery_data/work_author_aliases-v1.json` | `sha256:ddb2f644…` **(see below)** |
| Q2/E1 collections (8 files) | `same_work_spike/probe/data/*.jsonl` | all eight match their frozen counts exactly |
| gen-2 router evidence | `same_work_spike/probe/rsource/data/g_launch3.db` | 19.3% demotion reproduced |
| libraries / FJMS | `libraries.csv`, `fist_data/fjms_enrichment.db` | — |

**The author-alias pin MOVED, legitimately.** v2's meta records `acce47f6…`; the artifact on disk declares
`ddb2f644…`. Traced rather than assumed: commit `5e38ee74` (2026-08-04, one day AFTER v2 baked) regenerated it
— *"the author alias map now matches the catalogue's English name too"*, the Maimonides-rendered-twice fix,
scope 2 of 610 authored works. The file is gitignored (regenerated, not committed), which is why only the hash
records the change. Both curation artifacts were validated with their OWN `compute_content_hash` and are
self-consistent; `work_domains` still matches v2's pin exactly, and its ruling state is clean
(`needs_ruling_held: 0`, all 29 needs-ruling rows ruled, `unassigned: 0`). **The v3 build therefore pins the
NEWER alias hash deliberately, not by drift.**

**Two gates did real work, on the first two attempts:**

1. **H2 refused to default.** `real-mode distillation requires an explicit --release or
   --allow-partial-sources choice (H2) -- no default is silently permitted`. Correct: this is the full corpus,
   `--allow-partial-sources` is documented "ONLY for the smoke/unit path", so `--release` is the honest flag —
   and it is the only path that exercises the release-only `w_start`/`w_end` offsets gate (gate 3) on real data.
2. **`ReleaseInputsIncompleteError` caught a wrong path.** I pointed `--research-data-dir` at
   `probe/rsource/data`; the eight Q2/E1 collections live in `probe/data`. Every collection loaded as **0 rows**
   and the release gate named all eight with expected-vs-got. Under `--allow-partial-sources` this would have
   produced a *successful* build missing 84,000 evidence rows — the exact "passes for the wrong reason" failure
   the row-count pins exist to prevent. This is gate 1 (row-count preservation) demonstrating it can fail
   without anyone mutating anything.

---

### 7c. 🛑 BLOCKED 2026-08-07 — the router and the slim table are at DIFFERENT GRAINS. Owner decision owed.

The assembly halted on the wipe-out guard (gate 10), which is the guard working:

> `RoutingIngestError: 154897 tier-A witness spec(s) got no router decision -- they would keep the
> ingest default and silently bypass gen-2's routing.`

**Diagnosed to the row. This is NOT a missing router run and NOT a scope decision — it is a grain mismatch:**

| measurement | value |
|---|---|
| router rows / distinct works | 354,528 / **3,985** |
| slim tier-A rows / distinct works | 275,894 / **4,093** |
| tier-A pairs with no router decision | **141,358 (51.2% of rows)** |
| …whose **page** is absent from the router | **0** — every page is scored |
| works present in BOTH, their tier-A rows missing | **0 of 134,536** — coverage is perfect where grains agree |
| missing pairs whose work ends in `_NN` | 138,800 |
| …that resolve at the **collapsed parent** `(page_id, parent)` | **138,800 = 98.2% of all missing** |

The router scored `M:Ytext1000`; the slim table carries `M:Ytext1000_00 … _38`. The handoff names this exact
id as the over-collapsed case (*"`M:Ytext1000` = 39 Bible books"*), and
[[project_novelty_is_granularity_relative]] says novelty must be RECOMPUTED at the bake's grain, never
migrated. **The same now demonstrably applies to routing.** The unscored works are the mega-works: p50 **40**
rows/work vs **3** for scored, max **17,590** vs 5,462.

**The residual, separately:** 2,447 pairs over just **15** works, no `_NN` suffix, absent from the router at
any grain (all pages scored, so not a page gap). `matched_letters` p50 163 — small matches. These need their
own answer; a parent-grain mapping would not touch them.

**I did NOT write a suffix-stripping fallback, and it should not be written casually.** Mapping a sub-work's
routing decision from its collapsed parent asserts that the parent's `page_coverage` describes the child —
and coverage is *precisely* the quantity that changes with grain: a page matching one Bible book at 0.75 of
the page does not match the 39-book collapsed work at 0.75 of anything comparable. Silently inheriting the
parent's verdict would reintroduce the error the router exists to fix, on 51.2% of rows, invisibly.

**Three real options for the owner:**

1. **Re-run the coverage router at the split grain** — the honest fix, and the only one whose thresholds mean
   what they say. Cost: a gen-2 router re-run over 4,093 works, not a bake step. The threshold (0.2984) was
   calibrated on 1,395 graded cases at the COLLAPSED grain, so it would need re-deriving too.
2. **Route every unscored pair to `review_only`** (reason `gen2_router_not_shipped` — the code already exists
   for exactly this). $0, ships now, honest: nothing claims a routing decision that was never made. Cost: the
   mega-works leave the same-work headline surface — **which is what the handoff's own conservative option
   recommends** (*"gate the collapsed/heavily-quoted mega-works out of the same-work HEADLINE surface"*).
3. **Bake at the collapsed grain** — feed the router's own work ids through, no split. Coherent, but discards
   the granularity gain that motivated v3 in the first place.

**Recommendation: option 2.** It is free, it is the handoff's own advice, it uses machinery already built and
tested, and it keeps every claim truthful — an unscored pair is marked unscored rather than guessed. Option 1
is the right eventual answer and belongs in the gen-2 track, not in this bake.

**Consequence for the novelty price:** the v3 candidate population depends on which option is chosen, so the
$0 re-measurement cannot produce a meaningful number until it is. Option 2 makes it runnable immediately.

#### 7c-bis. MEASURED 2026-08-07, and it corrects two claims I made above

The owner asked how heavy option 1 really is, and whether granularity-novelty ("is *this book* attested in the
catalogue?") is this phase or the next. Both are now measured, and the answers move the recommendation.

**(a) `page_coverage` is arithmetic I already have — the re-run is NOT a matcher re-run.** Verified: the
router's own `page_coverage` equals `matched_letters / page_chars` on **5,000/5,000** sampled rows, and the
slim table carries **both numbers at the split grain on 100.0% of 275,894 tier-A rows**. Recomputing coverage
per book/tractate is a SQL pass, not a gen-2 job. What is NOT free is the *threshold*: 0.2984 was calibrated on
1,395 graded cases at the collapsed grain (`coverage_route_meta`), so honest re-derivation needs grading at the
split grain. **Split the cost in two: recompute = trivial; re-calibrate = owner grading.** My "a gen-2 router
re-run" framing overstated the compute and understated the grading.

**(b) My "51.2% invisibly" was wrong — the real exposure is 4.5%.** Measured against the parent decision on the
138,800 comparable rows:

| | |
|---|---|
| split coverage **identical** to parent | 121,318 (87.4%) |
| split coverage **lower** | 17,482 (12.6%) |
| split coverage **higher** | **0** (coverage never rises when the unit narrows) |
| median delta | **+0.0000**; p10 −0.0337 |
| parent said `same_work`, split falls **below** threshold | **6,233 = 4.5% of comparable** |
| parent said `parallel`, split **above** threshold | **0** (asserted, not assumed) |

So inheritance is wrong in exactly ONE direction — over-promotion — on 6,233 rows. Of those, **46.1% sit below
half the threshold** (coverage p50 0.158, clearly quotation) and 16.1% are within 0.05 of the line. Their works
are the expected ones: Isaiah 634, Jeremiah 594, Psalms 567, Deuteronomy 431. The mechanism I described was
real; the magnitude I asserted was not, and 4.5% is a materially different decision from 51%.

**(c) The grain question is ALREADY SETTLED, in the split's favour — and the router is the stale artifact.**
`crosswalk.json` maps the 39 Bible books to **39 distinct minted ids** (`M:Ytext1000_00 → w000086`, …), and
**`M:Ytext1000` is not in the crosswalk at all** — it exists only inside `coverage_route`. So the project's
established id space is already per-book; the collapsed id is an artifact local to the router. 2,738 of the
router's 3,985 works are absent from the crosswalk. This reframes 7c: the slim table is not "ahead of" the
router at some future granularity — **the router is behind the id space everything else already uses.**

**(d) Granularity-novelty is THIS phase, and it is largely already paid for.** Correcting my own id-space slip
(I first compared raw ids to the cache's minted `w######` keys and read 0% — the same mistake the D-17 audit
hit twice): mapping through the crosswalk, the cache holds verdicts for **141 of the 164 split-grain works
(86.0%)**. Novelty was therefore already asked *per book*, not per Bible — so "is Genesis attested here?" is a
question the cache answers, and the 86% is a floor on how much of the granularity-novelty work is done.
The handoff's §6.3 lists reference-granularity as a stage this milestone owns and explicitly frames it as
**actionability, not precision** (*"it does NOT raise same-work precision"*), which agrees with (b): the labels
barely move.

**Revised recommendation.** Option 2 still ships today at $0 and is still the safe default. But (b), (c) and
(d) together make a fourth option the better one if the owner wants the mega-works in the headline:

**Option 4 — recompute coverage at the split grain, reusing the existing threshold, and route the 6,233
one-way disagreements to `review_only`.** $0 compute, no grading, no inheritance of a parent verdict: every row
gets a coverage number computed from ITS OWN unit. The one place the old threshold could mislead is the
direction it can err (over-promotion), and those 6,233 rows are precisely the ones sent to review rather than
shipped. Applying 0.2984 at the split grain yields **165,459 same_work (60.0%) / 110,435 parallel (40.0%)** over
the full 275,894. Threshold re-calibration then becomes a later *refinement* of a shipped, honest surface
instead of a blocker — and it is the only option that keeps the actionable per-book labels the granularity
stage exists to deliver.

**Not certified.** Reusing a collapsed-grain threshold at book grain is a judgement, not a validated
calibration; these numbers are descriptive. Per [[feedback_discovery_vibe_not_experiment]] no precision claim
is attached, and none of this reaches a user-facing surface as a percentage.

---

## 7d. The bake RAN. Both blockers closed, and the novelty price is measured (2026-08-08)

`ASSEMBLE_EXIT=0`. The first complete v3 artifact exists: `works 1,269 · discovery_claim 268,361 ·
discovery_evidence 297,415 · witness_units 5,547 · discovery_identification 66,388 · manuscript_display 45,241`,
`content_hash de7896c3…`. Two blockers stood between §7c and this, and they were sequential — closing the
first only revealed the second.

### Blocker 1 — routing (closed by option 4, §7c-bis)

Recorded in the asset itself: `coverage_routing=gen2_router_split_regrained`, `kept_exact 134,536`,
`recomputed 138,796`, `disagrees_with_parent 6,233`, threshold `0.2984126984126984` read from
`coverage_route_meta.threshold`, both grains named. The 6,233 figure reproduces §7c-bis's prediction exactly.

### Blocker 2 — gate 3 had NO work-side offsets for E1, and nothing had noticed

**The finding.** The four E1 collections are `track1_direct`, so release gate 3
(`assert_release_work_offsets`) demands `w_start`/`w_end`/`aligned_page_*` on every one. `_ingest_e1_rows`
never set them; `_mk_evidence` defaults all four to `None`. Reproduced end to end: **16,105 of 16,105**
emitted E1 rows would have halted the build. It was invisible until now only because the routing gate at
`build_claims_and_evidence` fires ~40 lines earlier — so **fixing routing alone would not have unblocked the
bake**, it would have advanced the halt by one gate.

**Why E1 has no offsets, and why that is structural rather than an oversight.** The matcher partitions every
`(page, work)` pair at `mapv2_track1_run.assign_page`: tier A (verifies at the production boundary) →
`track1_matches`, tier B (verifies only in the wide band) → `track1_candidates`. One `if/continue/else`, so the
two are disjoint by construction — measured `381,341 + 1,900,171 = 2,281,512` exact union, intersection **0**,
over 156,612 shared pages and 3,550 shared works. **E1 draws 19,238/19,238 (100.0%) from tier B and 0 from
tier A.** The ref-instrumented pilot enriches `m['accepted_works']` only; its own docstring says *"tier-B is
not ref-instrumented here."*

**The consequence for reading the two lanes against each other, stated because it is easy to get backwards:**
E1's zero overlap with tier A carries **no information about agreement or disagreement** — the two lanes were
never given the opportunity to agree. It is not a quality signal in either direction. What it *does* mean is
that E1 is the pile which did **not** clear the primary bar, subsequently filtered, scored, and — for 174 rows
only — individually adjudicated. (`e1_ra_confirmed`'s 1,570 are band-evaluated and `unreviewed`; band ≠
adjudication, 134-CONTEXT R6.)

**The fix, and why it was cheap.** The coordinate was never unobtainable: `mapv2_track1_run.py:411-412`
computes `r0`/`r1` per hull and line 424 discards them, while `seg_off` — bound at line 293 — is never read
again. It was one array lookup from being stored. `gen2_track1_pilot.py` already carries `off + r0, off + r1`
on every hull for tier A, so a `GEN2_TIER=b` emitter (default stays `a`; existing invocations byte-identical)
re-runs the SAME shared membership kernel over exactly the E1 pages. **5.1 minutes**, not the "hardest
remaining work" this file previously implied.

**Frame parity, BLOCKING, and it is the load-bearing gate** — `span_start`/`span_end` are frozen inputs to the
E1 `evidence_id` recipe, so a one-letter page-side drift regenerates every E1 id:

| | |
|---|---|
| pages in scope | 18,779 |
| frozen tier-B rows on them | 61,270 |
| regenerated | **61,270** |
| frozen-only / regenerated-only / column-unequal | **0 / 0 / 0** |
| E1 pairs absent from the regeneration | **0 of 19,238** |
| regenerated rows with no reference span | **0** |

Result in the artifact: **254,612 `track1_direct` rows, 0 with a NULL work-side offset.** Gate 3 satisfied
with data, not waived.

### E1 routing — an extrapolation, labelled as one

`route_e1_by_coverage` decides keys gen-2 never scored, using the same threshold, the same RAW `n_chars`
denominator and the same impossible-coverage refusal as the split-grain path. **19,236 routed — 13,166
`same_work` (68.4%) / 6,070 `parallel`; 0 undecided.** Two pairs turned out to carry a genuine gen-2
`coverage_route` verdict (both `parallel`) and are **preserved verbatim**, which is why that branch was written
defensively rather than assuming the measured zero. (The earlier "0 of 19,238" was measured against
`discovery_claim`/`discovery_evidence`; against `coverage_route` it is 2.)

The asset records this as `coverage_e1_routing=threshold_extrapolated_tier_b`, so a reader sees that the
threshold was fitted on a different tier — rather than having to infer it from the absence of a note. Owner-
authorized 2026-08-08; descriptive, no precision claim, and the rows keep their own `confidence_band`.

### Novelty: the v3 price, which is the number §5.0b option 0 existed to produce

Measured against the real v3 asset (`--population pinned`, verified against the asset's own
`coverage_routing`), $0 spent, no model called:

| | |
|---|---|
| candidates | 67,079 |
| heuristically resolved (no model) | 10,267 (**15.3%**) |
| **residual — would reach the model** | **56,812** |
| reuse through the fingerprint gate | **0.0%** (every cache entry predates the fingerprint) |
| bare key overlap | 95.5% — *this is the quantity the retracted "87.6% → ≈$4" was reporting* |
| residual breakdown | 54,254 present-but-unfingerprinted + 2,558 absent from cache |

At the **measured** unit cost ($40.12 ÷ 55,184 = **$0.000727** per LLM-decided case, §1.3), the v3 novelty pass
is **≈ $41.30**. The 15.3% heuristic rate reproduces the legacy 15.4% almost exactly, which is the main reason
to trust the projection. **This supersedes both retracted figures (≈$4 and ≈$68) and is the first number that
describes the v3 population rather than the legacy one.** It remains an owner spend decision.

### Gate discipline

Every gate added here was proven able to fail, per [[feedback_gates_must_be_proven_able_to_fail]] and
[[feedback_gates_must_change_the_artifact]]. Tier-B emitter equivalence: 5/6 mutations RED (the 6th is
provably a no-op — `margin_band` returns `'singleton'` whenever `n_competitors == 0`, and `margin` is `None`
exactly then). Frame parity: perturbed the REAL regenerated table three ways — a one-letter `matched_letters`
drift, a deleted row, a NULLed reference span — all RED, green again after restore. Offset wiring: 5/5 RED,
including one written specifically to catch "the builder never calls the loader", which was a **real** gap —
the first test round passed while the builder ignored the data entirely. One of those tests was also not
running at all (`-k e1` did not match its name), which hid a genuine failure.

---

## 7e. The novelty gate never read M-source's own attribution — and what that was worth (2026-08-08)

Found by the owner asking whether the gate considers M-source's version references. It does not, and it
never did. `NoveltyCandidate.m_source_shelfmark_text` is threaded into `assemble_evidence_bundle`, listed in
`_SOURCE_ORDER` and is a **fingerprint input** — while the sole production assignment was the literal `None`.
The only non-`None` assignments in the tree were two test fixtures, so every test passed over an empty source.

**Wired** via `scripts/emit_work_attributions.py` (5,077 works) → `--work-attributions` → crosswalk-translated
to minted ids in `build_all_candidates`. Kept OUT of the shipped asset: the gate's verdicts ship, its inputs
need not. The emitter resolves the restricted field name **by elimination** rather than writing it — the first
draft hardcoded it and the masking scan correctly rejected the file; output verified byte-identical.

### It made the bake MORE expensive, and that is the finding

| | before | after |
|---|---:|---:|
| heuristically resolved | 10,267 | 4,479 |
| residual (reaches the model) | 56,812 | **62,600** |
| projected spend @ $0.000727 | $41.30 | **$45.51** |

**5,788 candidates were qualifying for the funnel's rule 2** — *"no checked source says anything, therefore
this ships as `fills_gap` automatically"* — **only because a source we hold was invisible to it.** They were
auto-declared discoveries because nobody was looking. The delta is exactly conserved, and adding a source can
only ADD name-matches, so the loss can come only from that auto-`fills_gap` bucket.

### The flip rate: 0.5–0.9% corpus-wide, ~21–26% where the comparison is possible

Measured by THREE independent methods (token-normalization, numeric-signature, reverse-lookup), each attacked
by a skeptic who re-derived the number rather than re-running the script.

| method | flips | rate of D0 (197,093) |
|---|---:|---:|
| token-normalized | 1,375 | 0.698% |
| numeric-signature | 1,217 | 0.617% |
| reverse-lookup (confident) | 987 | 0.501% |
| skeptic's independent matcher, self-corrected | ~1,387 | 0.704% |

**All three skeptics found the methods err STRICT, never loose.** Zero false positives were found in any
method's confident set. Library-stratified permutation nulls: observed 1,083 hits vs permuted mean 5.0 (218×),
and 400–2000× on the other arms — the signal is manuscript-specific, not an artifact. One skeptic confirmed
131 additional missed flips (+10.8%) from institution routing, so the honest reading is a **floor**.

**Why the corpus-wide rate is so low, and it is structural rather than encouraging:** 142,047 claims (72% of
D0) belong to the 39 Bible works, whose attribution is one complete non-Genizah codex — independently verified
absent from `libraries.csv` (0 rows for the series). The classical strata point at single non-Genizah codices
likewise. Those claims **cannot** flip. Structural ceiling corpus-wide: **0.54%**.

**Where the attribution names a manuscript that IS in our corpus (4,921 claims), 20.6–25.6% flip.** Per
institution, within the comparable subset: AIU 85.5%, Kaufmann 44.0%, JTS 31.4%, **CUL ~31%**, Bodleian 16.7%.

**The reframing all three methods reached independently, which bounds everything above:** this field is **not
a witness list**. It records the SINGLE base manuscript the edition was transcribed from (887 of 896 parsed
segments name exactly one). A work may have twenty catalogued witnesses and this names one. So it establishes
a **lower bound** on "already known" and can never establish an upper one.

**Practical scale, stated so the fix is not oversold:** of the 987 confident flips, only **20 currently carry
`fills_gap`** — the rest were already caught by other aids. This source's UNIQUE contribution to the current
artifact is therefore small. The larger effect is the 5,788 auto-`fills_gap` candidates that now get examined
at all, which the flip measurement does not capture.

Largest known blind spot, unmeasured: **28,342 claims (14.4% of D0)** whose attribution cites a bare
Neubauer/Margoliouth catalogue number, for which `libraries.csv` carries a variant on only 33 of 255,725
records. The flip rate there cannot currently be measured.

---

## 8. Owner questions

> **✅ CODEX APPROVED 2026-08-07, round 9** — `VERDICT: APPROVE`, recorded verbatim in
> `discovery-v3-bake-plan.CODEX-APPROVAL-R9.md`. Nine rounds; rounds 2–8 returned CHANGES-REQUIRED and
> found, among much else, that four separate gates were **written but never called** by a real build (the
> router ingest, the novelty fingerprint gate, the router inside `finalize_build`, and parity disabled on
> D-17 builds) — plus eight of my own tests that passed without checking anything. Round 9: *"I found no new
> production-code defect and no remaining un-failable test... I found no remaining production-code
> blocker."* Every fix is mutation-proven: the defect is re-introduced and the test must go red.
>
> The owner actions and run-time records below are **unchanged by that approval** — they are not code.
>
> **🛑 EXECUTION READINESS — the authoritative statement (2026-08-07, Codex rounds 7–9).** The sentence below
> once read "Nothing is blocking any more", which was false and is corrected here rather than deleted, because
> two review rounds had to point at the contradiction. The four *owner questions* below really are closed by
> measurement. **The BAKE is not ready to execute as a release-quality run.** Three lists, deliberately
> separated:
>
> **MUST FIX IN CODE — nothing outstanding.** Every Codex blocker and HIGH through round 7 is closed with
> mutation-verified tests (rounds 2–7: the router ingest and its wiring into `finalize_build`, the work-side
> offsets and their coherent pairing, gate 3's release enforcement, per-key parity reconciled with D-17, the
> novelty fingerprint and its `finalize_build` bypass, the shadow-grain halts, consumer-boundary containment,
> the destructive liveness probe, and the measurement's hash races).
>
> **OWNER ACTION OWED — blocks a release-quality run:**
> 1. Add the missing restricted pattern to `.masking_patterns` (§5.0a finding A). **Gate 16 is NOT green**
>    without it; the slim-DB column denylist is a compensating control for the one arrival point we know of,
>    not the scan.
> 2. Confirm the intended pattern count — the attestation reports **8**, this file elsewhere says 15.
> 3. Run the full strict scan (`--strict --scan-repo --scan-asset --scan-sqlite`) with that set, and confirm
>    it passes.
> 4. Choose the novelty option (§5.0b). **Recommended: option 0 first** — the $0 re-measurement against real
>    v3 inputs, which is now genuinely pinned. No spend is authorized at either the retracted ≈$4 or the
>    legacy-population $40.12.
>
> **MUST RECORD AT RUN TIME:** the keyed attestation (`pattern_count` + `pattern_set_hmac` under a retained
> key — without the key there is no identity digest at all), the scanned asset/SQLite paths with their
> post-build hashes, and the pre-build source-identity record gate 15 still owes.

**Four owner questions were closed by measurement rather than by asking**, which is the
posture this file should have started in:

- **§3.5** the MAPV2-8 severity cut — the 152 is not reproducible from the persisted file; use the 595.
- **§4** the novelty population — P is measured, not a build output.
- **§3.1** mint vs drop — **owner decided 2026-08-05: mint, M-source stays private.** Measured consequences:
  the public release gate is untouched (zero M-source works are public) and the genre debt is 99.8% already
  answered from source metadata.
- **§3.1** the corpus-expansion worry — **withdrawn.** gen-2's work set is a strict subset of v2's; all 2,738
  were already in the v2-era matcher. No CERT-01 re-registration on that ground.

- **§1.3** the authorization discrepancy — **resolved 2026-08-05: the owner confirms the run happened.** The
  stale claims in `.planning/ROADMAP.md` SC-6 (run "UNAUTHORIZED", cost `~$301`) are corrected in place.

**No further owner QUESTIONS are owed on the four items above** — but see the execution-readiness box: four
owner ACTIONS are owed before a release-quality run, and the novelty spend needs a decision at a number that
does not yet exist. The **≈$68** figure once quoted here is **retracted** along with the ≈$4: §5.0b measures
0.0% cache reuse through the fingerprint gate over the legacy population, and the only comparable actual spend
is $40.12 — which is *also* not the v3 price, because the v3 candidate population does not exist until the
router and final work set are built. Option 0 produces that number for $0.

**Non-blocking — needed before the corresponding step**

3. **§3.5** Confirm exclusion at the 595-risky level (301 claim rows, 0.084%) rather than a re-derived
   152; and that **exclude-now / revert-later** is the right trade versus reverting those pages to v1 HTR.
4. **§3.6** The 123 restricted-work genres: curate now, or accept a private-verifier failure while the public
   release passes?
5. **§5** The conservative headline option — gate heavily-quoted mega-works out of the same-work headline
   surface at launch, or ship the measured surface as-is?
6. **§3.7** Confirm `band_precision` / CERT-01 re-registration is deferred past this bake.
7. **`discovery-v3-naming.md`** — acknowledge the rename, which supersedes wording in the owner-ratified
   `discovery-coordination.md` §1.

---

## 9. Masking

Tracked file. Restricted corpora appear here only as **M-source** / **R-source**, never by name. Opaque work
ids (`M:` / `REF2:` / `J:` prefixed, `w######`) and Hebrew content are safe per the codename rule; the corpus
name is not, in code, comments, specs, fixtures, logs, error paths or commit messages. `--strict` requires
BOTH `--scan-repo` and `--scan-asset`; SQLite needs `--scan-sqlite`; unset `MASKING_SCAN_PATTERNS_FILE` fails
closed (exit 1) and is **never** a silent green.
