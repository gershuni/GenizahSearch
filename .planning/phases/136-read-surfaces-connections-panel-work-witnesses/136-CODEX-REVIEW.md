# Phase 136 CONTEXT — Codex adversarial review (2026-07-30)

**Verdict: REWORK** — 3 BLOCKER, 9 HIGH, 3 MEDIUM.
**Brief:** `tmp/CODEX-BRIEF-136-context.md` (masking-scanned clean before the run).
**Full run log:** `tmp/CODEX-REVIEW-136-context.log` (gitignored; ~947 KB, mostly tool trace).
Codex opened both sidecar databases read-only, ran aggregate queries and `EXPLAIN QUERY PLAN`,
and traced the id / frame / certificate / manifest / default-visibility / service-query logic.
No files or databases were modified.

Disposition of every finding is recorded in `136-CONTEXT.md` (see its Codex-disposition table).

---

| ID | severity | area | finding | what to change |
|---|---|---|---|---|
| F-01 | BLOCKER | D-02 / precision payload | The proposed tier-A measurement cannot pass the current builder or release verifier. The builder requires tier-A precision to remain null, the verifier independently requires null, and the insert statement drops `measurement_date`, `grader`, `audit_status`, and `report_id`. | Amend the frozen precision contract, builder validation/insertion, release verifier, schema tests, and methods reader together. Add a complete owner-approved precision-spec fixture and prove both PASS and FAIL branches. |
| F-02 | BLOCKER | Certificate / public projection | The certificate cannot authorize the structurally different public projection. The measurement report explicitly says that projection is a different population and its subgroup analysis is descriptive, not pre-registered. | Keep the existing pre-registration immutable. Either pre-register and measure the public projection, or formally amend the release requirement to justify transfer. Never copy the all-source measurement into the public asset as if frame-matched. |
| F-03 | BLOCKER | D-06 / requirements | “No precision percentages anywhere” conflicts now—not merely in Phase 139—with BAND-03, BAND-05, CERT-02, and REL-01. It also contradicts the existing methods implementation. | Before planning, amend all affected requirements and the frozen presentation contract, or withdraw D-06. Define exactly which non-percentage measurement facts remain public: status, unit, sample size, date, method, audit state, and report identifier. |
| F-04 | HIGH | Rebuild preservation / deploy | Equality of `population_hash` and `cluster_map_hash` is necessary but insufficient. They omit claim/evidence IDs, selected spans, snapshot hashes, and within-stratum changes to `matched_letters`. The deploy verifier compares the candidate against the candidate manifest, not against the certified asset. The documented rebuild command also omits the live v2-specific pinned inputs. | Add an old-versus-new allowlisted database diff, an estimand-payload hash, deck-card resolution checks, and an externally pinned expected certified-frame hash. Update the runbook to reproduce the actual live v2 inputs. |
| F-05 | HIGH | D-22 / visibility | “Origin of the displayed assertion” is not represented. `discovery_claim.source_corpus` is required to equal the work’s identity source; evidence rows have only a family code, not assertion-origin or license provenance. A safe projection cannot be reconstructed from the shipped schema. | Derive `assertion_visibility` from the raw evidence origin before identifiers are discarded, and derive `identity_visibility` separately for the displayed work. Public eligibility must require both. Project reachable works and auxiliary rows as a closed graph. |
| F-06 | HIGH | Novelty construct | A boolean cannot distinguish “checked and not found” from source failure, missing identifiers, incomplete coverage, stale cache, or LLM abstention. The phrase “any available finding aid” is broader than the finite, versioned set actually checked. Existing evidence already contains 665 claims with conflicting novelty values. | Use `known / not_found / indeterminate`, fail closed to `indeterminate`, and display “Not found in the finding aids checked,” with versions and dates. Centralize assessment at a versioned manuscript–scholarly-work key and verify all evidence inherits one result. |
| F-07 | HIGH | Novelty grain / ranking | D-24 and NOVEL-01 prohibit novelty from feeding ranking, while D-19 makes it the primary ordering. Raw-work grain also risks false novelty where aliases identify the same scholarly work, while the existing canonical key is acknowledged to over-collapse. | Define a separate, reviewed `novelty_work_key` with aliases. Either make tier the default ordering or amend “no ranking” to permit an explicit user-selected novelty grouping that never changes confidence rank or styling. |
| F-08 | HIGH | Span coverage / honesty | The proposed percentage is not generally “span coverage.” It is normalized Hebrew base-letter coverage, capped at 100%; 9,549 shipped main-family rows have multiple spans, while all 42,776 shipped propagated evidence rows lack `matched_letters`. The displayed largest span can therefore differ from the percentage’s aggregate numerator. | Freeze a family-complete numerator and denominator definition, or restrict the metric to the family for which it is valid. Store an indexed fixed-point `coverage_ppm` plus a validity status. Label it explicitly as matched Hebrew-letter coverage, not bare “68% of page.” |
| F-09 | HIGH | PANEL-03 | The leading evidence design cannot satisfy PANEL-03 for both sides of a page relation: shared-text rows deliberately lack b-side offsets. When the viewed page is the b-side, there is nothing to highlight. The existing `highlight_text()` helper highlights search terms, not stored offsets. | Add verified b-side offsets during the rebuild or amend/defer PANEL-03. Implement offset highlighting by slicing raw snapshot text before HTML escaping; fail closed independently on each side’s snapshot hash. |
| F-10 | HIGH | PERF-01 / indexes | The corpus findings query cannot meet the current work-page budget. A representative novelty/tier/coverage ordering scanned all display claims, used a temporary sort, and took about 3.4 seconds repeatedly on this machine; the documented cap is 1.5 seconds. | Materialize sortable `band_rank` and `coverage_ppm`; add an index for findings ordering/filtering and a unique index on `discovery_claim(display_evidence_id)`. Add a manuscript index only if replacing the already-indexed `page_id IN (…)` plan. Benchmark every filter/sort/count combination before deployment. |
| F-11 | HIGH | Work page / WORK-02 | Current work results contain no shelfmark, library sort key, novelty, coverage, or total. Server-side sorting after pagination is therefore impossible. The context also mistakes claim rows for carriers: the maximum is 13,038 claims, not manuscripts; medians are 9 claims, 5 manuscript IDs, and 4 witness units. | Add a sidecar manuscript-display lookup with normalized library/shelfmark sort keys, plus a count query using the identical grouped predicates. Define title aliases, bilingual normalization, and duplicate-title handling before claiming WORK-02. |
| F-12 | HIGH | Scope / sequencing | This is no longer one deliverable phase: it contains contract amendments, LLM-assisted novelty, dual projection, certificate compatibility, schema/index changes, deploy changes, four UI surfaces, catalogue vocabulary mapping, and an unresolved evidence design. | Keep Phase 136 as an umbrella only if necessary, but split execution gates: contract/data semantics; offline build and verification; owner approval and deploy; minimal panel/work surfaces; then catalogue/findings and evidence UI. The public-projection certificate mismatch is the single highest-risk item. |
| F-13 | MEDIUM | Related-page count | The stated 20,435 is the count of display claims whose selected evidence is shared text, not the relation evidence population. The asset has 40,968 shipped shared-text evidence rows, 37,397 directed page pairs, and 30,539 unordered pairs. “N pages” needs a per-anchor distinct-page definition. | Specify directed versus unordered semantics, deduplicate by the opposite page, and label the header “unevaluated candidate alignments” so an aggregate does not evade the screening disclosure. |
| F-14 | MEDIUM | Availability semantics | Current wrappers collapse timeout, overload, unavailable sidecar, and genuine zero results to `[]`. D-13 would consequently hide the panel during an outage as though the manuscript had no claims. | Return an envelope such as `{status, items, total}` and hide the control only for a successful zero. Preserve a visible temporary-unavailable state and retry behavior. |
| F-15 | MEDIUM | Factual data claims | The gen-2 deferral is substantively correct, but the reviewed-title count is stale or uses an undocumented join. The current approved list and crosswalk cover 80,993 of 236,497 shipped claims (34.25%), not 82,156 (34.7%). | Recompute and document the exact join recipe. Correct D-17’s “carriers/manuscripts” terminology and D-11’s shared-text population description. |

## Evidence for BLOCKER and HIGH findings

**F-01.** `scripts/build_discovery_sidecar.py:3867-3890` says an explicit release precision spec must keep tier-A precision null; lines 4019-4027 enforce that value comparison. `scripts/verify_discovery_sidecar.py:553-560` independently rejects a non-null tier-A precision. The real build insert at `scripts/build_discovery_sidecar.py:4419-4427` writes only `measurement_status` from the five registry columns. The live tier-A row has all measurement and registry fields null.

**F-02.** The measured-result document states at lines 94-114 that the public projection is a different population, its subgroup estimate is not pre-registered, and a clean publication path requires a public estimand registered before the public bake. `scripts/verify_cert01_grading.py:206-212` also requires the literal database-content hash pinned by the original pre-registration, so every legitimate rebuilt byte stream fails check 10.

**F-03.** `.planning/REQUIREMENTS.md:42-46` requires screening precision to be reachable, BAND-05 to publish estimates and intervals, and each band’s measured number to display with status; line 87 requires tier A to go public with its number. The context acknowledges only the CERT-02/REL-01 portion at `136-CONTEXT.md:88-98`. The existing help page still renders estimates and intervals at `web/pages/help.py:245-260`.

**F-04.** `scripts/cert01_frame.py:293-306` confirms the two cited hashes contain only `(page_id, canonical_work_id, stratum)` and `(page_id, canonical_work_id, unit_key)`. `scripts/build_discovery_sidecar.py:1217-1241` shows the frame hash also omits novelty, routing, precision, snapshot hash, and `matched_letters`. Thus `matched_letters` can drift without detection if it remains in the same stratum. The deploy runbook reads the expected frame from the new manifest (`docs/specs/discovery-deploy.md:79-91, 123-138`), while its rebuild command at lines 211-231 omits the live v2 pinned inputs.

The correct preservation gate is an exact old/new comparison in which `works`, claims, witness units, routing audit, and all pre-existing evidence columns are byte-identical except explicitly authorized novelty changes; only the new coverage columns and the specified tier-A registry row may differ. In addition, recompute the original population, stratum, and cluster hashes and bind every graded card to the same work, claim, display evidence, span, and snapshot. Keep the original database hash and report ID in the immutable pre-registration; publish a separate compatibility attestation for the rebuilt full asset.

**F-05.** The schema requires claim source to equal work source (`docs/specs/discovery-sidecar-schema-v1.md:89-100`), and the verifier enforces that at `scripts/verify_discovery_sidecar.py:319-332`. `discovery_evidence` has no assertion-origin or license field (`discovery-sidecar-schema-v1.md:114-180`). Consequently, `works.source_corpus` is exactly the proxy D-22 says is insufficient.

Projection must happen while raw evidence provenance remains available. The public artifact must contain only masked/public enums, not the raw origin. Verification must cover the FK closure, unreachable works, routing-audit rows, counts, aggregates, sort behavior, and auxiliary tables—not only claim rows.

**F-06.** The frozen schema has only `is_new` (`discovery-sidecar-schema-v1.md:124`) and no provenance, assessment status, source-set identifier, or per-source success state. The live asset contains 29,054 multi-evidence claims, 665 of which currently disagree internally on the novelty boolean. A paid title judgement cannot repair failed shelfmark normalization, absent catalogue records, incomplete source snapshots, or an invalid manuscript identifier.

The LLM gate also needs a pinned prompt hash, exact model/version, normalized input hash, structured abstention, cache-key specification, and a substantially larger owner-labelled hard-case evaluation. Agreement on 40 cases is too weak for a default ordering over tens of thousands of assessments, particularly when false novelty is the reputationally costly error.

**F-07.** `136-CONTEXT.md:161-165` makes findings novelty-first, while lines 212-214 say novelty must never feed ranking weight; `.planning/REQUIREMENTS.md:35` repeats the prohibition. That is not structural orthogonality. Even with separate columns, users will infer importance or correctness if lower-tier “novel” rows dominate the first page.

Raw source-work IDs and the existing canonical key are both unsuitable as the sole scholarly identity key: one splits aliases, the other is documented as over-collapsing. Novelty needs its own reviewed equivalence relation and a deterministic “known via any alias means not novel” rule.

**F-08.** `scripts/build_discovery_sidecar.py:532-568` defines coverage as matched letters divided by the normalized Hebrew base-letter stream, not literal character or displayed-span coverage. Tier-A stores only the largest span (`scripts/build_discovery_sidecar.py:2555-2570, 2689-2713`). Read-only queries found 9,549 shipped main-family rows with multiple spans; all 40,968 shipped propagated shared-text rows and 1,808 propagated witness rows have null `matched_letters`.

The safe first release is either main-family-only coverage or a separately frozen metric per family. Bare “68% of page” beside a match label is especially likely to be read as confidence when precision percentages are withheld.

**F-09.** PANEL-03 requires each side’s own span (`.planning/REQUIREMENTS.md:52`), but the schema explicitly allows shared-text `b_start`/`b_end` to be absent (`discovery-sidecar-schema-v1.md:165-175`). `web/pages/browse.py:1577-1601` escapes the full text and performs case-insensitive search-term substitution; it does not consume offsets and cannot safely be reused for snapshot offsets. The context’s leading implementation at `136-CONTEXT.md:122-128` is therefore not implementable for both orientations with the proposed rebuild payload.

**F-10.** The live asset has no `sys_id` index; `EXPLAIN QUERY PLAN` confirms a direct manuscript lookup scans `discovery_evidence`. The existing page-list approach does use `ix_discovery_claim_page_id`, so it is acceptable if retained. The more serious gap is corpus browsing: the representative 50-row novelty/tier/coverage-style query scanned all claims, performed primary-key evidence lookups, used a temporary B-tree, and took 3.41–3.55 seconds over four runs. The count alone took roughly 0.50–0.55 seconds. `docs/specs/discovery-budgets.md:35-42` gives work/leads a 1.5-second p95 cap, and there is no versioned findings-browse cap yet.

At minimum add a unique display-pointer index and a covering/indexable findings ordering. If novelty becomes tri-state, index that status rather than the legacy boolean. Add the new query shapes and their worst-case filtered totals to the versioned budget artifact.

**F-11.** `shared/discovery_service.py:209-226` shows the work CTE exposes only unit, page/work/claim IDs, manuscript ID, evidence family, band, and computed band rank. The paged result at lines 797-842 still has no total, shelfmark, library, coverage, or novelty. The existing catalogue page uses a different title-ID vocabulary and resolves shelfmarks only after retrieving a page (`web/pages/catalog_browse.py:262-309, 376-438`), which cannot produce globally correct server-side shelfmark sorting.

Read-only counts show why terminology matters: the heaviest work has 13,038 claim rows but 4,796 distinct manuscript IDs and 4,637 witness units. Across the 1,088 works, medians are respectively 9, 5, and 4; only 64 works exceed 200 witness units, not 120. WORK-01 totals must count the post-DATA-10 unit projection.

**F-12.** The context combines the rebuild/redeploy (`136-CONTEXT.md:60-69`), four surfaces, catalogue integration, a new findings browse, novelty, and an unresolved evidence design. These are coupled by immutable asset identity and masking, so “data first” does not make the downstream risk small.

Recommended sequencing:

1. Amend requirements and freeze visibility, novelty, coverage, and evidence semantics.
2. Build both projections offline; run exact compatibility, masking-positive-control, reconciliation, and performance gates.
3. Obtain owner approval, then perform the paired asset-first deployment.
4. Ship the page/manuscript panel and tier-first work page.
5. Move catalogue integration and corpus findings browse to the next wave.
6. Ship PANEL-03 only after b-side evidence is available or the requirement is amended.

## What I verified and what I could not

I verified the requested documents and code, opened both databases read-only, inspected schemas and indexes, ran aggregate queries and query plans, and traced the ID, frame, certificate, manifest, default-visibility, and service-query logic.

Key confirmed live-asset facts include:

- 1,269 works, 268,361 claims, 297,415 evidence rows, and 166,537 shipped display claims.
- All 144,294 shipped main-family evidence rows have novelty false because it was not computed.
- Propagated display rows contain exactly 14,003 novelty flags.
- The manuscript statistics in D-09 are accurate: 44,375 manuscripts, median 2 claims/1 work, maximum 427 claims/47 works, and 429 above 50 claims.
- The gen-2 evidence is not a drop-in replacement: 358,206 claims, 236,497 shipped, all direct, with band and novelty null. D-01’s deferral is sound.
- `population_hash` and `cluster_map_hash` use the triples described in the brief, but they do not prove card-content or full-row identity.

I could not validate the proposed novelty result because no complete all-family novelty artifact, source-version manifest, LLM cache, or assessment audit exists. I also could not measure production p95 for the new surfaces or inspect final mockups because neither exists. No files or databases were modified.

VERDICT: REWORK
