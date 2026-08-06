# discovery-v3 bake plan — the gen-2 evidence refresh

**Status: 🟢 CLEARED TO BUILD, 2026-08-06 — with every Codex finding folded in and the scope frozen in §5.0.**
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
   D-06, the **real gap is 52 works / 141 evidence rows / 0.03% of the corpus — and zero shipped claims**, i.e.
   inert. Those 52 are worth a look as genuine omissions; they are not a decision.
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

`.masking_patterns` holds 15 patterns (count only — contents are secret), consistent with DATA-05's requirement
that **R-source tokens are pre-registered** alongside M-source, so a leak of either is caught by the same gate.

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
| 1 | **Routing** — recompute v2-style, or ingest gen-2's? | **INGEST `coverage_route`** with a declared mapping + parity checks. Do NOT recompute coverage. Re-derive the D-17 order against the ingested router; do not inherit it. | owner, "yes of course" |
| 2 | **Selected population** — the 2,686 D-06 works? | **HONOUR D-05/D-06** — exclude them; take only the 52 policy-keeps. Liturgy/piyyut needs the containment fix first. | plan rec., owner not yet contradicted |
| 3 | **Novelty mode** | **REUSE the cache behind a per-pair input fingerprint** (Codex blocker 3), NOT blanket reuse and NOT a full re-run. Fingerprint = every rendered prompt field incl. claim title/author + evidence text, plus alias-group and model/prompt/effort hashes. Unfingerprinted → treat as miss. | Codex blocker 3 |
| 4 | **Novelty scope + ceiling** | **Headline (`same_work`) first**, $150 self-enforced ceiling. Est. ≈$4 at measured reuse; recompute the estimate once the fingerprint is in, since it can only *lower* reuse. | owner ("$12 → go"), revised by #3 |
| 5 | **MAPV2-8** — 152 or 595? | **595-risky exclusion at ingest** (301 claim rows, 0.084%). The 152 is NOT reproducible from the persisted file. Named as an *exclusion*, not the requested revert. **Owner confirmation owed** (Codex HIGH). | §3.5; supersedes the "152" in the old DO list |
| 6 | **`w_start`/`w_end`** | **Stage 1 only**, and it needs a **declared page-span→reference-span projection** + multi-span parity gate — not the scalar join §1.2 assumed (Codex blocker 1). Coordinate space (`norm_stream`) named in the schema doc. | Codex blocker 1 |
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

**⚠ FINDING B — a frozen release constant coincidentally equals a gen-2 figure. Do not read it as agreement.**
`build_discovery_sidecar.py:5558` freezes `_EXPECTED_TIER_A_ROWS = 275894`, commented
"`track1_matches WHERE shadowed_by IS NULL`" — and 275,894 is **exactly** the gen-2 unshadowed
`(page_id, ref_work)` pair count measured in §3.1. It is **not** either v2-era figure (364,178 with R-source,
253,975 without). The likely reading is that the v2 release contract was frozen against a *narrower*
population than today's v2-era table, and the collision with gen-2 is chance. **Either way the release gate
will compare v3's tier-A count against 275,894 and either pass for the wrong reason or fail without
explanation.** Owed: establish what that constant was frozen against, and re-pin it deliberately for v3 with
the derivation recorded — never let it match by luck. Added as gate 15.

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
| **10** | **NEW — routing parity** (Codex blocker 2): the ingested `same_work`/`parallel` split reproduces gen-2's `coverage_route` **exactly** at its own grain | flip one route label in the staged input → must fail. Absent this, "we ingested the router" is unproven and the handoff's quality figures do not transfer |
| **11** | **NEW — `shadowed_by` mixed-group halt** (Codex HIGH): derived at the producer's `(claim_id, ref_work)` grain, all constituent rows must agree | synthesise a mixed group → **must halt**, never silently ANY/ALL it |
| **12** | **NEW — R-source input gate** (Codex HIGH): the slim research DB is asserted to contain **zero** `RS:`-prefixed rows, and its source-table identity is fingerprinted, before every build and review-artifact invocation | plant one `RS:` row → must refuse. Gate 2 checks completeness, not absence, so it cannot catch this |
| **13** | **NEW — novelty input fingerprint** (Codex blocker 3): a reused verdict requires an exact per-pair input fingerprint match | mutate a work's title → the pair must become a **miss**, not a hit |
| **15** | **NEW — `_EXPECTED_TIER_A_ROWS` re-pinned deliberately** (§5.0a finding B): the frozen count's derivation is recorded and re-derived for v3, never inherited | change the ingest population by one row → must fail with the count named. Today's value coincidentally equals a gen-2 figure, so a pass would otherwise prove nothing |
| **16** | **NEW — the signature-vocabulary term** (§5.0a finding A) appears in no slim-DB column, artifact, deck or log | plant the term in a scanned file → must be reported. **Currently FAILS this control** — the pattern is absent from `.masking_patterns`; owner action owed. Until then, an explicit column-name denylist in the slim-DB builder is the compensating control |
| **14** | **NEW — multi-span offset parity** (Codex blocker 1): the page-span→reference-span projection is deterministic and correct on rows carrying multiple dual-side spans | pick a known multi-span row; assert the chosen `w_start`/`w_end` against the producer's own evidence rows, not merely non-NULL |

**On the masking gate (Codex MEDIUM, accepted).** The fail-closed control and `--self-test` prove the
*mechanism* runs and can return non-zero — they do **not** prove the loaded pattern set is complete or current,
because the self-test needle is synthetic. Owed: a **non-disclosing attestation** of the pattern set (count +
hash, never contents — 15 patterns today) recorded per run, plus the exact asset/sqlite paths and post-build
hashes scanned, and a real-pattern positive control that does not print the pattern.

**Order of operations: NO LONGER inherited.** The v2 §6 sequence (Lever-1 coverage routing **before** D-17) was
written for a builder that *computes* coverage. Decision #1 replaces that step with an **ingest** of gen-2's
router, so the order must be **re-derived against the ingested router** rather than carried over (Codex blocker
2's closing instruction). The v2 §6 rationale still applies to everything downstream of routing.

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

## 8. Owner questions

**Nothing is blocking any more. Four questions were closed by measurement rather than by asking**, which is the
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

**Nothing is owed.** One courtesy confirmation before money moves: v3's fresh novelty run at **≈$68** (shipped
scope, $150 self-enforced ceiling) — same model/prompt/effort, so the validated configuration is unchanged.

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
