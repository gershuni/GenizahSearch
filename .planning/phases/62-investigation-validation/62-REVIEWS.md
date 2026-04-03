---
phase: 62
reviewers: [gemini, codex]
reviewed_at: 2026-04-03
plans_reviewed: [62-CONTEXT.md (pre-plan review)]
---

# Cross-AI Context Review -- Phase 62

## Gemini Review

This review evaluates the **CONTEXT.md** for Phase 62 (Investigation & Validation) of the GenizahSearch image caching milestone.

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

### Summary

This is a strong investigation-phase context in terms of scope, repo grounding, and deliverable shape, but it is not yet safe for downstream planning as written. The biggest problems are that it weakens the stated hard TOS gate, leaves the actual ingest path implicit even though EC2 is known to be blocked by NLI, and defines the "NLI-only" subset too narrowly for the current codebase's real image-source model.

### Strengths
* The phase boundary is clear: this is validation work, not feature delivery.
* The document usefully ties rate testing, storage sampling, and resolution choice together instead of treating them as separate tracks.
* The deliverables are concrete and appropriately investigation-oriented: report plus scripts.
* The canonical references are mostly good, especially `shared/nli_crossref_service.py` and `web/api.py`, which are the right places to ground image-source behavior.
* D-04 is a good idea: testing both `800px` and `1200px` directly supports INV-05 with real data instead of opinion.
* The focus on the NLI-only subset is directionally right; it keeps the storage estimate tied to the real caching target rather than the whole `815K` image universe.

### Concerns
* **[HIGH] INV-04 is contradicted by D-09 through D-11.** The requirements say "NLI contacted" and describe a hard go/no-go gate before large-scale fetch begins, but the context turns that into "review terms first, email only if needed, and proceed on conditional go." That will push downstream agents toward planning against a softer gate than the milestone actually allows.
* **[HIGH] The ingest topology is missing.** The milestone is "pre-cache on EC2," but the context also says NLI blocks datacenter IPs and rate testing must happen from a residential IP. It never states whether the intended Phase 63 path is "fetch from home PC then transfer to EC2," "wait for NLI approval/whitelisting," or something else. That is a core feasibility assumption, not an implementation detail.
* **[HIGH] The NLI-only subset definition is incomplete.** D-06 excludes Cambridge, Manchester, and JTS/DPUL, but the current app also has Oxford as a separate non-NLI image path in `web/api.py`, and Cambridge can apply to Mosseri via label construction in `genizah_core.py`. "NLI-only" should be defined by actual non-NLI image-provider availability, not just a short list of library names or tables.
* **[MEDIUM] INV-03 is too underspecified.** D-08 says the EC2 layout is "Claude's Discretion," but it does not define the actual investigation questions a planner needs answered: filesystem type, inode budgeting, per-directory file-count target, EBS assumptions, and what evidence is enough to call `815K+` files feasible.
* **[MEDIUM] The rate-test success condition is still mushy.** D-02's `100-200` images technically meets the "100+" success criterion, but it does not clearly separate ramp-up from steady-state. A planner will still have to ask what "sustainable rate" means operationally.
* **[MEDIUM] D-07 is ambiguous when combined with D-04.** It is unclear whether `1000+` means per resolution, total across both resolutions, or only after a provisional resolution choice. That ambiguity matters because INV-02 requires the storage estimate to be grounded in actual sample data at the target resolution.
* **[MEDIUM] INV-05 is only partially addressed.** The document names `800` vs `1200`, but it does not say how quality will be judged, which manuscript types should be included in the comparison, or who decides "good enough for research use."
* **[LOW] "Claude's Discretion" is not good cross-agent wording.** This is a context file for downstream agents, so the discretion language should be model-neutral.
* **[LOW] "Scripts should be reusable by Phase 63"** is reasonable, but stated too strongly it may bias the investigation toward production hardening instead of fastest validation.

### Suggestions
* Replace D-09 through D-11 with explicit gate wording: review published terms immediately, contact NLI immediately if bulk academic caching permission is not explicit, and do not record a go decision until the gate outcome is written down.
* Add a decision that states the assumed ingest path end-to-end. Example: "Phase 63 assumes residential fetching plus transfer/sync to EC2 unless NLI explicitly permits direct server-side acquisition."
* Redefine "NLI-only subset" as "manuscripts with NLI images and no usable non-NLI image source in the current app architecture." Ground that in `shared/nli_crossref_service.py` and `web/api.py`, not only ad hoc SQL.
* Explicitly call out Oxford in the subset logic, and treat Mosseri carefully since some Mosseri manuscripts map to Cambridge rather than remaining NLI-only.
* Clarify that libraries like `RNL`, `AIU`, and many others are collection owners, not automatically alternate image providers. Conversely, `Mosseri` may have Cambridge-backed coverage, so `library_code` alone is not a safe proxy.
* Tighten INV-01 with a precise definition of success: final plateau rate, minimum successful fetch count at that plateau, allowed error budget, and whether retries count as failures.
* Clarify the sampling plan for INV-02/INV-05: either sample the same `1000+` images at both `800` and `1200`, or do a smaller dual-resolution pilot first and then a full `1000+` sample at the chosen candidate.
* Add a lightweight quality-review protocol for INV-05: representative manuscript mix, side-by-side outputs, and a named signoff criterion.
* Replace "Claude's Discretion" with "Implementation Discretion" or "Investigator Discretion."
* Start TOS review/outreach first or in parallel with the technical tests, since reply latency is likely to be the longest path in the phase.

### Risk Assessment: HIGH
The context is close to useful, but not yet planning-safe. One hard requirement is currently softened in a way that conflicts with the milestone, and two central assumptions remain implicit: what exactly counts as "NLI-only," and how data gets from an allowed IP to EC2. Fix those, and the document becomes much more reliable for researcher/planner/executor handoff.

---

## Consensus Summary

Phase 62 context reviewed by Gemini and Codex. Both reviewers agree on the core issues.

### Agreed Strengths
* Phase boundary is clear: investigation, not feature delivery
* Dual-resolution testing (D-04) is well-designed for INV-05
* NLI-only subset focus is directionally correct
* Deliverables (report + scripts) are concrete and appropriate
* Canonical references are well-chosen

### Agreed Concerns (HIGH priority -- raised by both reviewers)

1. **TOS gate contradicts requirements.** Both reviewers flag that D-09 through D-11 soften INV-04's "hard go/no-go gate" into a "conditional go." Gemini calls it ambiguous; Codex calls it a direct contradiction that makes the context not planning-safe.

2. **NLI-only subset definition is incomplete.** Both note that D-06 only excludes Cambridge/Manchester/JTS/DPUL. Gemini suggests adding RNL, BL, AIU, Mosseri. Codex goes further: Oxford is a separate non-NLI image path, Mosseri can map to Cambridge, and `library_code` alone is not a safe proxy for image availability. The definition should be grounded in actual image-provider availability in the codebase, not a hand-curated library list.

3. **Filesystem validation is underspecified.** Both flag INV-03/D-08 as too vague. Gemini suggests a dummy inode stress test; Codex wants explicit investigation questions (filesystem type, inode budget, per-directory targets, EBS assumptions).

### Codex-Only Concerns (not raised by Gemini)

4. **[HIGH] Ingest topology is missing.** The document never states how images get from a residential IP to EC2. This is a core feasibility assumption that downstream agents need.

5. **[MEDIUM] Rate-test success criteria are mushy.** D-02 doesn't separate ramp-up from steady-state. "Sustainable rate" is undefined operationally.

6. **[MEDIUM] D-07 sampling ambiguity.** Unclear whether 1000+ means per resolution, total, or after resolution choice. Matters for INV-02.

7. **[MEDIUM] INV-05 quality criteria missing.** No protocol for who judges "good enough" or how quality is evaluated.

### Divergent Views

* **Risk level:** Gemini rates LOW/MEDIUM, Codex rates HIGH. Codex's higher rating is driven by the ingest topology gap and TOS contradiction, which it considers blocking for planning. Gemini's lower rating reflects confidence in the technical approach.
* **Exclusion nuance:** Gemini treats it as a simple list expansion (add more library codes). Codex argues library_code is fundamentally the wrong proxy -- the definition should be based on actual image-provider availability in code, not collection ownership.

### Recommended CONTEXT.md Updates (priority order)

1. **Fix TOS gate** -- Align D-09/D-11 with INV-04's "hard gate" wording, or explicitly acknowledge the softening
2. **Add ingest topology decision** -- State the assumed residential-fetch-then-transfer path
3. **Redefine NLI-only subset** -- Ground in actual image-provider availability (nli_crossref_service.py + web/api.py), not ad-hoc library list. Include Oxford; handle Mosseri/Cambridge overlap
4. **Tighten INV-03** -- Add specific investigation questions for filesystem validation
5. **Clarify sampling plan** -- Whether 1000+ is per-resolution or total; how quality is judged
6. **Replace "Claude's Discretion"** -- Use model-neutral "Implementation Discretion"
7. **Add cost projection** -- Monthly EBS cost estimate as a report deliverable
8. **Start TOS first/parallel** -- Reply latency is the longest path

---
*Review completed: 2026-04-03*
*Reviewers: Gemini, Codex (2/2 assessed)*
