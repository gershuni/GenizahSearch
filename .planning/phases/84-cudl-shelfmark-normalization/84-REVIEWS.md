---
phase: 84
reviewers: [gemini, codex]
reviewed_at: 2026-05-06
plans_reviewed:
  - 84-01-PLAN.md
  - 84-02-PLAN.md
  - 84-03-PLAN.md
  - 84-04-PLAN.md
  - 84-05-PLAN.md
self_skipped: claude (running inside Claude Code)
unavailable: [opencode, qwen, cursor]
---

# Cross-AI Plan Review — Phase 84: CUDL Shelfmark Normalization

## Gemini Review

This review evaluates the implementation plans for **Phase 84: CUDL Shelfmark Normalization**.

### Summary
The plans are exceptionally well-structured and technically sound, providing a surgical yet comprehensive solution to the CUDL shelfmark gap. By adopting a "layered fallback" architecture (D-01, D-02), the bridge module achieves the goal of recovering thousands of Mosseri and Or. records without risking regressions in the core search or joins logic. The strategy of "auditing collisions first" for leading-zero collapse is a professional approach to data safety.

### Strengths
- **Safe Architecture:** Leaving `genizah_core.normalize_shelfmark()` untouched and using the bridge only as a fallthrough (Plan 04) effectively isolates the risk to specific cross-system lookup sites.
- **Data Integrity:** The "Audit-first / Fail-loud" strategy for leading-zero collisions (Plan 01/03) ensures that distinct fragments are never accidentally merged due to aggressive normalization.
- **Symmetry:** Reusing the existing forward `construct_mosseri_cudl_label` for the reverse index (Plan 02) avoids maintaining two separate parsers for the complex Mosseri pattern.
- **Rigorous Validation:** Plan 05's three-layer guard (golden fixture, scan-diff, and byte-identical canonical check) is a gold standard for defending against regressions in large-scale data processing.

### Concerns
- **Startup Latency (LOW):** Building the alias index (Plan 02) involves walking ~140K CUL rows. While O(1) lookups are efficient once built, the initial build time should be monitored to ensure it doesn't noticeably delay application startup on lower-end hardware.
- **Late Import Hygiene (LOW):** Plan 04 uses late imports to break circular dependencies between `genizah_core` and the bridge. While correct, it can sometimes make debugging harder if imports fail silently inside a `try...except`.

### Suggestions
- **Pre-computed Collision Set:** In Plan 01, consider hardcoding a small initial set of known collisions in addition to the dynamic CSV loader to ensure basic safety even if the audit report is missing.
- **`lookup_cudl` Return Type:** For D-04, the plan to return `{'sys_id': ..., 'shelfmark': ...}` is optimal. Callers like the search engine can use the ID directly, while the UI can use the canonical shelfmark to provide better feedback.

### Risk Assessment: LOW
The overall risk is low because the bridge is implemented as an opt-in fallback layer. Even in the worst-case scenario where the bridge module contains a bug, the existing 140,000 correctly matching records remain untouched. The most sensitive part—the leading-zero rule—is protected by a mandatory pre-run audit.

**Approval:** Approved for execution.

---

## Codex Review

### Summary

The plan set is thoughtfully layered and mostly aligned with NORM-01..NORM-04: it preserves the canonical normalizer, isolates CUDL behavior in `shared/shelfmark_bridge.py`, audits leading-zero risks before runtime wiring, and wires the bridge only as fallback at high-value cross-system boundaries. The main risks are in the alias-index semantics, the scan-diff test assumptions, and some platform/test fragility. I would treat this as a solid plan with several medium-to-high fixes needed before execution.

### Strengths
- Clear phase boundary: no `libraries.csv` schema changes and no synthetic rows, keeping Phase 85 out of scope.
- Good D-02 discipline: `normalize_shelfmark()` is explicitly frozen and bridge calls are fallback-only.
- Mosseri strategy is sound: reusing `construct_mosseri_cudl_label()` avoids a second parser of record.
- Collision-first thinking is correct for D-06.
- The four D-08 wiring sites match the stated runtime surfaces.
- Plan sequencing is reasonable: foundation → alias index → Or/forward helpers → runtime wiring → regression guard.
- Golden fixture plus scan diff plus canonical-untouched tests provide useful overlapping protection.

### Concerns

**HIGH: Scan-diff "post orphan set is subset of pre" is probably invalid.** After Plan 04, `scripts/scan_cudl_orphans.py` changes its normalizer. That can change the identity of normalized keys and candidate matching behavior, not just remove previously orphaned labels. A strict `post ⊆ pre` assertion may fail even when behavior improves. Better invariant: preserve a baseline of CUDL manifest labels and assert every previously matched label still matches under the new runtime lookup.

**HIGH: Alias-index collision handling is too quiet outside leading-zero report.** Plan 02 uses `index.setdefault(...)`, allowing first-wins for duplicate normalized keys unless they are already in `_COLLISION_KEYS`. That can silently map a CUDL classmark to the wrong sys_id for non-leading-zero collisions, duplicate shelfmarks, or mixed Mosseri/CUL aliases. `build_alias_index()` should collect all duplicate key claims with distinct sys_ids, log them, and exclude ambiguous keys by default.

**HIGH: `shelfmark_to_cudl_label()` may overgeneralize non-Mosseri URLs.** For non-Mosseri CUL shelfmarks, returning `cudl_normalize(shelfmark)` assumes the normalized key is always the CUDL viewer slug. That is true for cited patterns, but risky for all CUL subcollections. Make it conservative: return Mosseri-specific slugs, known Or transforms, and known CUL patterns only. For uncertain forms, return `None` so browse.py keeps the v7.10 fallback.

**MEDIUM: Collision audit uses full `cudl_normalize()`, not isolated leading-zero collapse.** The audit claims to detect leading-zero collisions, but `cudl_normalize()` also applies slash, comma, dash, quote, and dot-after-letter normalization. That may produce a broader collision set than intended. Better: audit both `base_key_without_zero_collapse` and `zero_collapsed_key` and report only collisions introduced by the zero-collapse delta.

**MEDIUM: Plan 03 numeric-collapse may affect non-Or keys.** `_collapse_numeric_runs()` runs for every variant. Any collection with three numeric groups will get an added collapsed alias, not just `Or.`. Constrain numeric-collapse indexing to Cambridge Or. variants, or add duplicate-key exclusion strong enough to make this safe.

**MEDIUM: NLI wrapper caller migration is underspecified.** Plan 04 says add `get_cambridge_manifest_with_bridge()` and maybe update callers depending on grep results. If callers are not updated, D-08 site #3 is only partially satisfied. The plan should require identifying the actual browse/image call path and updating it, not just adding an opt-in wrapper.

**MEDIUM: Tests depend on real `MetadataManager()` startup.** That may be slow, environment-sensitive, or trigger unrelated side effects. Keep that integration test, but add a deterministic unit fixture that builds a small csv_bank directly.

**MEDIUM: Fixture authoring asks agents to invent/guess 50 real rows.** Plan 05 tells the implementer to create ~50 rows and "use real rows where possible." The plan should require generating candidates from `libraries.csv` plus CUDL orphan reports, then validating every row before writing the fixture.

**LOW: Windows environment mismatch.** Plans use `grep`, `cp`, `tee`, bash snippets. The stated environment is PowerShell on Windows.

**LOW: Plan 05 modifies generated reports as part of tests.** Prefer writing post-phase outputs to separate files, with explicit baseline files checked in.

### Suggestions

1. Add an ambiguity policy to `build_alias_index()`: collect `key -> set[(sys_id, shelfmark)]`, exclude keys with multiple sys_ids, log/write `reports/cudl_alias_collisions.csv`.
2. Split collision audits: leading-zero-introduced, all bridge-normalization, committed collision keys consumed by runtime.
3. Make `shelfmark_to_cudl_label()` pattern-aware and conservative for non-Mosseri CUL shelfmarks.
4. Require actual NLI call-site migration in Plan 04. Do not leave the wrapper unused.
5. Replace scan-diff subset assertion with a stronger direct "previously matched still resolves" check.
6. In Plan 05, generate the golden fixture from validated real data; include a small deterministic fixture for unit tests.
7. Add explicit tests for ambiguous/collision keys.
8. Add a canonical-normalizer source guard: hash or snapshot the `normalize_shelfmark()` function text, or use targeted literal-output tests.

### Risk Assessment: MEDIUM-HIGH

The architecture is right, and the phase is well-scoped, but correctness depends on normalization details where false positives can silently route users to the wrong manuscript. The biggest risks are ambiguous alias handling, overbroad forward URL generation, and scan-diff assertions that may not actually prove NORM-04.

---

## Consensus Summary

Both reviewers approve the layered architecture, the D-02 freeze on `normalize_shelfmark()`, and the symmetry of reusing `construct_mosseri_cudl_label()` for the reverse index. They diverge sharply on residual risk: Gemini reads the fallback design as inherently low-risk; Codex flags several places where the fallback can silently route the wrong manuscript despite the design intent.

### Agreed Strengths
- Layered fallback architecture preserves the canonical normalizer (D-02).
- Forward-parser reuse for the Mosseri reverse index avoids two parsers of record.
- Audit-first / fail-loud collision strategy for leading-zero collapse.
- Three-layer regression guard (golden fixture + scan-diff + canonical-untouched).

### Agreed Concerns
None at the same severity — but Gemini's "Pre-computed Collision Set" suggestion and Codex's HIGH "alias-index collision handling is too quiet" both touch the same concern: the runtime should not depend on the audit report being present and complete.

### Codex-only HIGH-severity items (worth addressing before execution)
1. **Scan-diff invariant is too strict.** `post ⊆ pre` may fail when normalizer behavior changes legitimately. Replace with "every previously matched CUDL label still resolves" check.
2. **Alias-index ambiguity policy.** `setdefault` first-wins is unsafe for non-leading-zero collisions. Collect all `(sys_id, shelfmark)` claims per key, exclude any key with >1 distinct sys_id, write `reports/cudl_alias_collisions.csv`.
3. **`shelfmark_to_cudl_label()` overgeneralization.** For non-Mosseri CUL shelfmarks, fall back to `None` rather than returning `cudl_normalize(shelfmark)`, so browse.py keeps the v7.10 behavior on uncertain forms.

### Codex-only MEDIUM items
- Collision audit should isolate the leading-zero delta, not full `cudl_normalize()`.
- Numeric-collapse should be constrained to Or. variants only.
- NLI wrapper caller migration must be made unconditional, not opt-in based on grep count.
- Add a small deterministic unit fixture alongside the integration test that uses real `MetadataManager()`.
- Generate the golden fixture from validated real data, not invented rows.

### Divergent Views
- **Risk level:** Gemini → LOW; Codex → MEDIUM-HIGH. The disagreement turns on whether the bridge can silently route to a wrong sys_id. Codex's collision-policy fix would close this gap and bring both reviewers into alignment.
- **Late imports:** Gemini sees the silent-failure risk; Codex doesn't flag it. Worth adding an explicit log on `try/except ImportError` paths.

### Recommended Next Step

Run `/gsd-plan-phase 84 --reviews` to incorporate Codex's three HIGH-severity items and the consensus collision-policy hardening before execution. Gemini's two LOW items (startup latency budget assertion, late-import logging) can be folded into the same revision pass.
