---
phase: 102
round: 2
reviewers: [codex]
reviewed_at: 2026-05-29T14:40:00Z
plans_reviewed: [102-01-PLAN.md, 102-02-PLAN.md, 102-03-PLAN.md, 102-04-PLAN.md, 102-05-PLAN.md]
verdict: MEDIUM risk — minor-patch-then-execute (all round-1 HIGH blockers resolved)
supersedes: 102-REVIEWS-round1.md
---

# Cross-AI Plan Review — Phase 102 (Round 2 / re-review)

> Reviewer: **Codex** (`codex exec`, default model, read the live codebase + installed PyMuPDF).
> Single-reviewer run (`--codex`); `claude` CLI skipped for independence (we run inside Claude Code).
> This is a RE-REVIEW of the plans AFTER they were revised in `--reviews` mode against the round-1
> findings (archived in `102-REVIEWS-round1.md`). Codex's task: verify each round-1 finding is now
> resolved and catch any new drift introduced by the revisions. Codex's Python launcher still failed
> in its sandbox (environmental — our own toolchain runs pytest fine), so it verified PyMuPDF and
> code claims by reading installed package files + live source.

## Codex Review

**Summary**

Verdict: **MEDIUM risk, minor-patch-then-execute**. The revised plans resolve the original HIGH
blockers in substance: D-06 is now coherent under the new "strip nikud for display too" decision,
corrupt PDFs are buffered and classified before writes, and the glyph metadata contract is much
stronger. I would not replan the phase again, but I would patch a few plan gaps before execution,
mostly around existing tests, block-boundary preservation, and one subtle RTL letter-order
ambiguity.

**Round-1 Findings Resolution**

| Round-1 finding | Status | Evidence |
|---|---:|---|
| PyMuPDF rawdict flags valid | RESOLVED | PyMuPDF `1.27.2.3`; `TEXTFLAGS_RAWDICT = TEXTFLAGS_DICT` and `TEXT_PRESERVE_IMAGES` exist. Plan 02 uses `_RAWDICT_FLAGS = fitz.TEXTFLAGS_RAWDICT & ~fitz.TEXT_PRESERVE_IMAGES`. |
| `_attach_nikud_page` exists | RESOLVED | Live `pdf_to_docx.py:791`; Plan 02 calls it before metrics. |
| D-06 display assumption was false | RESOLVED | New design intentionally strips both `content` and `cached_text`; no display lookup change needed. Live display still reads Tantivy `content` at `genizah_core.py:7166`/`:9631` — now consonantal by design. |
| Corrupt pages written before classification | RESOLVED | Plan 02 exposes `page_flags` before write; Plan 03 buffers pages and returns `corrupt_encoding` before `_write_page_doc`, with zero-write tests. |
| Atomic rebuild would reintroduce nikud | RESOLVED | With stripped `cached_text`, live rebuild path `:3054`→`:3072` reindexes stripped text. Plan 03 also allows defensive `strip_nikud`. |
| Fresh DB stamp left at user_version 2 | RESOLVED | Plan 04 bumps `_LATEST_VERSION` and `init_sqlite` stamp to 3, with tests. |
| Existing fallback tests would break | RESOLVED (named file) | Plan 02 updates `tests/test_local_pdf_extraction_fallback.py` to rawdict-primary. See new concern for an additional uncovered AST test. |
| Missing D-08 status surfaces | RESOLVED | Plan 03 covers in-file surfaces; Plan 04 covers desktop static/live UI + migration `_KEPT_STATUSES`. |
| `strip_nikud` broader behavior | RESOLVED/ACKNOWLEDGED | Live pattern `[֑-׏]` at `genizah_core.py:157`; Plan 03 notes it strips nikud + cantillation. |
| Glyph-order contract underspecified | **PARTIALLY RESOLVED** | Plans preserve `original_order` and forbid destructive x-sort. Remaining ambiguity: intra-word letter order if rawdict emits per-letter Hebrew in visual LTR order. |
| De-space lacked span/font metadata | RESOLVED | Rich glyph contract includes `font`, `size`, `span_id`, `original_order`; hysteresis uses `span_id/font`. |
| Block/paragraph boundary risk | **PARTIALLY RESOLVED** | Plan 02 claims block separation but still groups `all_line_dicts` globally. Needs per-block grouping. |
| Migration test did not exercise 1→2 prune | RESOLVED | Plan 04 seeds `corrupt_encoding` before the 1→2 prune and asserts survival. |
| Python env broken in Codex sandbox | UNCHANGED (not a plan defect) | Verified PyMuPDF by installed package files instead. |

**New Concerns**

- **MEDIUM (M1):** Additional existing tests will fail and are not named in the plans.
  `tests/test_format_rtl_invariant.py:88-111` asserts `_fix_sort_true_rtl_page` is called from
  `extract_pdf_pages` in the sort=True branch; Phase 102 removes that branch as primary behavior.
  Also `tests/test_phase_97_2_reset_my_library_full_cycle.py:69-71` asserts `user_version == 2`;
  Plan 04 only mentions `tests/test_local_indexer_migrations.py`.

- **MEDIUM (M2):** Plan 02's block preservation is contradictory. It says collect all page lines and
  group globally (`102-02:179`), then join "blocks" with `\n\n` (`:189`). Patch to group lines **per
  original rawdict block**, then join blocks.

- **MEDIUM (M3):** RTL de-space may preserve reversed letters inside a word if rawdict emits
  letter-spaced Hebrew glyphs in visual LTR order. Plan 01 builds word text by `original_order`,
  while reorder only moves whole word units. Add a fixture where a single Hebrew word's glyphs are
  emitted left-to-right and require correct consonant order, or specify that RTL word units order
  their letters by descending x when needed.

- **MEDIUM (M4):** D-06 strip at `_write_page_doc` affects **all** LOCAL formats, not just PDFs. Live
  `_write_page_doc` is shared by PDF, DOCX, TXT, HTML, XLSX, CSV. Either explicitly accept "all LOCAL
  display is consonantal" or gate stripping by file extension/status and test the chosen behavior.
  **(Scope decision — needs the user, see Recommended Action.)**

- **MEDIUM (M5):** Buffer-then-decide drops cancellation during the final write loop. Plan 03 checks
  `cancel_check()` while buffering but not while writing. Add a cancel check in the write loop with
  `_rollback_partial(sys_id)` if cancellation happens after any write.

- **LOW (L1):** Plan 03 adds `from genizah_core import strip_nikud` at module import time, but
  `shared/local_indexer.py` is currently Qt-free/shared and has no `genizah_core` import. Prefer a
  small shared normalization helper or a lazy import in `_write_page_doc`/rebuild to avoid tightening
  module coupling.

**Plan↔Code Drift**

Most cited live line references are accurate: `extract_pdf_pages` at `shared/local_indexer.py:794`,
`_write_page_doc` at `:2420`, `_extract_and_write_pdf` at `:2495`, rebuild content write at `:3072`,
migration `_LATEST_VERSION` at `shared/local_indexer_migrations.py:33`, desktop status sites at
`desktop/my_library_tab.py:333`/`:486`/`:519`. The main drift is missing affected tests
(`test_format_rtl_invariant.py`, `test_phase_97_2_reset_my_library_full_cycle.py`) that still encode
old Phase 101/97.2 assumptions and are not covered by the revised plan set.

**Risk Assessment**

Overall: **MEDIUM**. The original HIGH-risk architectural defects are resolved well enough to proceed
after small plan edits. Recommendation: **minor-patch-then-execute**, not execute-as-is and not
replan-again. Patch the five medium concerns into the relevant plans, then the phase is safe to run.

---

## Consensus Summary

Single reviewer (Codex). Round-2 verdict is a clear improvement over round-1 (HIGH → MEDIUM): every
round-1 HIGH/MED blocker is confirmed RESOLVED against live code. No remaining HIGH items. The
phase is safe to execute after a light patch pass.

### Confirmed resolved (round-1 → round-2)
All 14 round-1 findings RESOLVED, except two now downgraded to PARTIAL (glyph intra-word letter
order; per-block line grouping) — both addressed by the new MEDIUM patches below.

### Agreed Concerns (round-2 — patch before execution)
1. **M1 — uncovered legacy tests:** add `tests/test_format_rtl_invariant.py` and
   `tests/test_phase_97_2_reset_my_library_full_cycle.py` to the relevant plan(s) and update their
   stale assertions (`_fix_sort_true_rtl_page` primary call; `user_version == 2`).
2. **M2 — per-block grouping:** Plan 02 must group lines per original rawdict block, then join blocks
   with `\n\n` (resolves the global-grouping vs block-join contradiction).
3. **M3 — intra-word RTL letter order:** add a visual-LTR-glyph Hebrew-word fixture + specify
   descending-x letter ordering within RTL word units (Plan 01).
4. **M4 — D-06 strip scope (NEEDS USER DECISION):** `_write_page_doc` is shared by ALL LOCAL formats.
   Decide: strip nikud for all LOCAL formats, or gate to PDF only.
5. **M5 — cancellation in write loop:** add cancel check + `_rollback_partial(sys_id)` to Plan 03's
   buffer-then-decide write loop.
6. **L1 — import coupling:** lazy-import `strip_nikud` in `shared/local_indexer.py` (keep the shared
   module Qt-/genizah_core-free at import time).

### Divergent Views
None (single reviewer).

---

## Recommended Action

The phase is **MEDIUM risk — minor-patch-then-execute**. One concern (M4) is a scope decision only
the user can make; the other five (M1, M2, M3, M5, L1) are mechanical patches with clear fixes.

After resolving M4, incorporate all patches via:

```
/gsd-plan-phase 102 --reviews
```

This re-runs the planner in reviews mode against this round-2 REVIEWS.md, then re-verifies via
gsd-plan-checker. (Round-1 review preserved at `102-REVIEWS-round1.md`.)
