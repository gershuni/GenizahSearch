---
phase: 62
reviewers: [gemini, codex]
reviewed_at: 2026-04-03
plans_reviewed: [62-01-PLAN.md, 62-02-PLAN.md, 62-03-PLAN.md]
---

# Cross-AI Plan Review -- Phase 62

## Gemini Review

This review evaluates the plans for Phase 62 (Investigation & Validation) of the GenizahSearch image caching milestone.

### 1. Summary
The context document is of high quality and demonstrates a mature, risk-averse approach to infrastructure planning. It correctly prioritizes empirical data (rate tests, storage sampling) over assumptions and identifies the crucial "NLI-only" subset to optimize storage. The plan is well-integrated with the existing codebase (`nli_crossref.db`) and provides clear success criteria. However, there are minor gaps in the definition of the "NLI-only" subset and a potential conflict between the "hard gate" requirement for TOS and the implementation decision to proceed without a formal response.

### 2. Strengths
* **Empirical Foundation:** Decision D-02 (ramp-up testing) and D-04 (multi-resolution sampling) ensure that the final architecture is based on real-world NLI behavior rather than guesswork.
* **Resource Efficiency:** Identifying the "NLI-only" subset (D-05, D-06) is a critical optimization that prevents redundant caching of images already hosted by stable providers like Cambridge or Manchester.
* **Safe Execution:** The use of a residential IP (D-01) and specific block detection triggers (D-03) protects the production EC2 environment from early blacklisting.
* **Artifact Continuity:** D-14 ensures that investigation scripts are not throwaway code but are transitioned into the project's tooling for the next phase.

### 3. Concerns
* **TOS "Hard Gate" Ambiguity (HIGH):** The requirement INV-04 describes a "hard go/no-go gate," but D-11 suggests a "conditional go" if the TOS is merely silent and NLI doesn't respond. This creates a risk where "no answer" is interpreted as "yes," which may conflict with the user's intent for a "hard gate" before fetching 815K images.
* **Incomplete Exclusion List (MEDIUM):** D-06 lists Cambridge, Manchester, JTS, and DPUL as exclusions. However, significant Genizah collections like the **Russian National Library (RNL)**, **British Library (BL)**, **AIU**, and **Mosseri** are missing. If these aren't excluded, the "NLI-only" subset will be significantly larger and more redundant than necessary.
* **Filesystem Design Vagueness (MEDIUM):** INV-03 requires a "hierarchical directory structure designed," but D-08 leaves this to "Claude's Discretion." Without a specific decision to *test* inode limits or directory lookup speeds during the investigation, the "design" might be theoretical rather than validated.
* **Cost Calculation Omission (LOW):** While storage *size* is sampled (INV-02), there is no explicit decision to translate that size into a **projected monthly EBS/S3 cost**. For 815K images, even small differences in average file size can lead to significant monthly cost variations.

### 4. Suggestions
* **Expand Exclusion Logic:** Update D-06 to explicitly include all non-NLI sources found in `fist_data/fjms_enrichment.db` (e.g., RNL, BL, AIU, etc.) to ensure the NLI-only subset is truly minimal.
* **Define TOS "Silent" Protocol:** Clarify D-11. If the "hard gate" requires NLI contact, define a waiting period (e.g., 5 business days) or a specific criteria for what constitutes a "Conditional Go" (e.g., "TOS does not explicitly prohibit caching AND request was sent to NLI").
* **Inode/Filesystem Stress Test:** Add a decision (D-15) to create a "dummy" directory structure on the target filesystem with ~100k empty files to measure `ls` and `find` performance before finalizing the hierarchical layout.
* **Financial Validation:** Add a deliverable to D-12: "Estimated monthly storage and bandwidth cost report based on sample data."
* **Sequence TOS First:** Move D-09/D-10 to the top of the execution flow. If the TOS review (D-09) reveals an explicit prohibition, the rate tests (D-01 to D-04) should not be performed.

### 5. Risk Assessment: LOW/MEDIUM
The risk is **LOW** for technical failure (the testing plan is solid) but **MEDIUM** for project-level alignment. The primary risks are:
1. Caching images that are available elsewhere (due to the incomplete exclusion list).
2. Proceeding with a "conditional go" on TOS that the user might consider too legally risky for a "hard gate."

Addressing the exclusion list and tightening the TOS decision logic would move this to **LOW** risk.

---

## Codex Review

Grounded against the current repo patterns in web/api.py and scripts/import_nli_crossref.py, the plans are mostly scoped correctly for an investigation phase and avoid obvious production over-engineering. The main weaknesses are cross-plan handoff ambiguity, one dependency bug, and several places where the measurement definition is still too loose to support a confident go/no-go.

### Plan 62-01: NLI-Only Subset + TOS Gate

**Summary**
This is the strongest of the three plans. It puts the human TOS decision in the loop, keeps the implementation small, and targets the right prerequisite dataset for the later phases. The main risk is data correctness: the plan says what sources to cross-reference, but not exactly how records are matched or how ambiguity/duplicates are handled.

**Strengths**
- Puts the TOS decision early and makes it an explicit stop condition.
- Correctly avoids using `library_code` as a proxy for image availability.
- Keeps deliverables pragmatic: one script, unit tests, JSON output.
- Includes a sanity check against a rough expected count, which is useful for catching obvious logic errors.

**Concerns**
- HIGH: The matching strategy is underspecified. If `nli_images`, provider tables, and Oxford JSON do not align on a single canonical key, subset counts can be materially wrong.
- MEDIUM: "7 test behaviors" is probably too thin for this dataset. It misses likely edge cases such as duplicates, null IDs, one manuscript excluded by multiple providers, and Oxford normalization mismatches.
- MEDIUM: The output contract is not defined tightly enough for downstream reuse. Later plans need a stable machine-readable artifact, not just summary counts.
- LOW: `no-FGP stats` is unclear and may be a typo or overloaded term.

**Suggestions**
- Define the canonical join strategy up front: preferred key order, fallback matching, and how conflicts are resolved.
- Require exclusion-reason counts in the output, not just final totals.
- Save a deterministic sample list for downstream rate/storage tests, using a fixed seed.
- Add at least one fixture-based regression test from a small real-data snapshot, not only synthetic unit cases.

**Risk Assessment:** MEDIUM.

### Plan 62-02: Rate Test + Storage Sampling

**Summary**
This plan addresses the right questions, but it currently mixes several distinct measurements into one wave without fully defining what is being measured. The biggest issue is that rate testing can be distorted if manifest resolution and image downloads are lumped together.

**Strengths**
- Conservative ramp-up with clear abort conditions is appropriate for an external service.
- `--dry-run` is useful and keeps development/testing from hitting NLI unnecessarily.
- Reusing downloaded images is a good anti-waste design choice.
- Dual-resolution sampling maps directly to the actual product decision.

**Concerns**
- HIGH: The plan conflates FL-ID resolution traffic with image-fetch traffic. If the rate test measures both together, the sustainable cache-ingest rate will be misestimated.
- HIGH: "500+ manuscripts at both 800px and 1200px = 1000+ total images" is ambiguous. Manuscripts and images are not the same sampling unit.
- MEDIUM: Abort on `3+` timeouts may treat residential ISP noise as an upstream block. That can produce false pessimism.
- MEDIUM: Sampling is not explicitly stratified. If the sample skews toward one image type, the quality/storage decision will be weak.
- MEDIUM: The plan does not explicitly require persistent request logs and resolved FL-ID artifacts.
- LOW: Hard-coding a storage-cost number is brittle.

**Suggestions**
- Split measurements: manifest/FL-ID resolution rate, then image-download rate with resolved FL IDs cached.
- Define the sample unit explicitly as image pages, not manuscripts.
- Record per-request outcomes, latency percentiles, and failure types.
- Add a hard ceiling on total live requests and a cooldown between ramp stages.
- Make live execution opt-in with `--live`; default modes should never touch NLI.

**Risk Assessment:** HIGH.

### Plan 62-03: EC2 Filesystem + Investigation Report

**Summary**
The benchmark script is appropriately pragmatic and portable, and the reporting deliverables are sensible. The main problem is structural: the plan claims to depend only on 62-01 even though its report step explicitly needs 62-02 outputs.

**Strengths**
- Uses stdlib-only Python, which is the right call for EC2 portability.
- Keeps the benchmark as a disposable investigation tool.
- Includes both internal and public-facing report outputs.
- Tests real EC2 behavior rather than assuming filesystem performance.

**Concerns**
- HIGH: Dependency ordering is wrong. Task 3 depends on Plan 62-02 data but the plan only declares dependency on 62-01.
- HIGH: Extrapolating 300K-file behavior from a single 50K-file run is weak evidence for an eventual 815K+ target.
- MEDIUM: The 2-level 65K-directory layout may itself create meaningful inode/metadata overhead; no comparison point.
- MEDIUM: Thresholds like `ls/stat < 10ms` are not defined as warm-cache, cold-cache, median, or p95.
- MEDIUM: The plan omits rsync-like tree traversal/write behavior.
- LOW: `--cleanup` needs explicit guardrails.

**Suggestions**
- Make Plan 62-03 formally depend on both 62-01 and 62-02.
- Benchmark at two scales, not one, so scaling claims are empirical.
- Compare at least one alternate fan-out layout.
- Add ingest-relevant operations: file creation, tree walk, rsync-like copy/scan.
- Define acceptance metrics as percentiles with warm vs cold cache conditions.

**Risk Assessment:** MEDIUM.

### Overall Codex Assessment

The plans are close to usable, but would benefit from fixes before execution. The most important:
- Tighten artifact handoffs between waves: stable JSON/CSV outputs, fixed seeds, reusable sample manifests.
- Correct the 62-03 dependency on 62-02.
- Define measurement units precisely, especially for rate testing and storage sampling.
- Separate "safe upstream behavior" from "storage economics" from "filesystem behavior" so the final go/no-go is defensible.

---

## Consensus Summary

Phase 62 plans reviewed by Gemini and Codex. Both reviewers see the investigation approach as sound but flag specific gaps.

### Agreed Strengths
* Plans are appropriately scoped for investigation (not production code)
* TOS gate is correctly sequenced as a blocking prerequisite
* NLI-only subset optimization is the right approach
* Empirical testing (rate, storage, filesystem) over guesswork
* Script deliverables are pragmatic and reusable

### Agreed Concerns (priority order)

1. **[HIGH] TOS gate ambiguity persists.** Gemini flags "conditional go" as conflicting with "hard gate." This was already addressed in the revised CONTEXT.md but Gemini's review was against the pre-revision context prompt. The plans themselves implement the revised conditional-go interpretation correctly.

2. **[HIGH] Exclusion list / matching strategy.** Gemini notes RNL/BL/AIU/Mosseri missing from exclusion list (these are NLI-only collections -- they should be INCLUDED, not excluded). Codex flags that the matching strategy between tables is underspecified. The plans do cross-reference all 4 provider tables + Oxford, but join key semantics could be clearer.

3. **[HIGH] Plan 62-03 dependency on 62-02.** Codex correctly identifies that Task 3 of Plan 62-03 reads 62-02-SUMMARY.md but the plan only declares depends_on: [62-01]. This is a structural bug -- Task 3 cannot run until Plan 02 completes.

4. **[HIGH] Rate test conflates manifest resolution with image fetch (Codex).** The ramp-up measures both FL ID resolution and image download together, which could skew the sustainable rate estimate.

5. **[MEDIUM] Measurement definitions too loose.** Codex wants explicit sample units (images vs manuscripts), latency percentiles (not just averages), and warm/cold cache conditions. Gemini wants clearer cost projection methodology.

6. **[MEDIUM] Filesystem benchmark extrapolation weak.** Codex wants two-scale testing (e.g., 50K and 150K) instead of single-point extrapolation.

### Divergent Views

* **Risk level:** Gemini rates LOW/MEDIUM overall; Codex rates Plan 62-02 as HIGH risk due to measurement conflation. Codex is more demanding on measurement rigor.
* **Exclusion logic:** Gemini misunderstands the exclusion model (suggests excluding RNL/BL/AIU, which are NLI-only collections that SHOULD be in the subset). Codex correctly focuses on join-key ambiguity rather than library names.
* **Scope:** Gemini reviews appear to be against the CONTEXT.md rather than the PLAN.md files (references D-xx decisions). Codex reviews the actual plan tasks.

### Recommended Plan Updates (priority order)

1. **Fix 62-03 dependency** -- Add 62-02 to depends_on list
2. **Separate FL ID resolution from image rate test** -- Resolve FL IDs first (phase 1), then measure pure image fetch rate (phase 2) within the rate test script
3. **Clarify join strategy in 62-01** -- Document canonical join keys for each provider table
4. **Add persistent request logs** -- Rate test should emit per-request CSV for post-hoc analysis
5. **Define sample units** -- Explicitly state: "1000+ images" = 500 manuscripts x 2 resolutions, where "images" counts individual FL-ID page downloads
6. **Benchmark at two scales** -- EC2 test at 50K and 150K files for empirical scaling validation
7. **Add fixed seed for reproducible sampling** -- Downstream reuse and audit trail

---
*Review completed: 2026-04-03*
*Reviewers: Gemini, Codex (2/2 assessed)*
