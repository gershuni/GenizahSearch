---
phase: 62
reviewers: [gemini]
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

Codex CLI invocation failed (auth/syntax issue). Skipped.

---

## Consensus Summary

Only one reviewer (Gemini) succeeded. Key feedback:

### Concerns (prioritized)

1. **HIGH -- TOS gate mismatch:** INV-04 says "hard go/no-go gate" but D-11 allows "conditional go" without NLI response. The user decided TOS review is sufficient, but this should be explicitly acknowledged as a softening of the original requirement.

2. **MEDIUM -- Incomplete NLI-only exclusion list:** D-06 only excludes Cambridge/Manchester/JTS/DPUL. Other libraries with their own image sources (RNL, BL, AIU, Mosseri, Oxford/Bodleian) are not mentioned. This would inflate the "NLI-only" subset with manuscripts that have alternative image sources.

3. **MEDIUM -- Filesystem validation is theoretical:** D-08 delegates directory design to Claude's discretion but doesn't require actual inode/performance testing on EC2.

4. **LOW -- No cost projection:** Storage sampling covers size but not projected monthly EBS cost.

### Suggestions for CONTEXT.md Update

- Expand D-06 exclusion list to cover ALL libraries with alternative image sources
- Add D-15: TOS review should happen FIRST in execution order (before rate tests)
- Add cost projection as a report deliverable
- Consider inode stress test as part of INV-03 validation

---
*Review completed: 2026-04-03*
*Reviewers: Gemini (1/1 succeeded)*
