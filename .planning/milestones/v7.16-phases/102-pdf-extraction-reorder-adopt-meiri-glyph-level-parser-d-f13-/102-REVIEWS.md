---
phase: 102
round: 3
reviewers: [codex]
reviewed_at: 2026-05-29T14:17:56Z
plans_reviewed: [102-01-PLAN.md, 102-02-PLAN.md, 102-03-PLAN.md, 102-04-PLAN.md, 102-05-PLAN.md]
verdict: MEDIUM risk — minor-patch-then-execute (1 new HIGH: buffer-phase cancellation rollback)
supersedes: 102-REVIEWS-round2.md
---

# Cross-AI Plan Review — Phase 102 (Round 3 / re-review)

> Reviewer: **Codex** (`codex exec`, default model). Read the live codebase at commit `3576bf31`
> and inspected installed PyMuPDF `1.27.2.3`.
> Single-reviewer run (`--codex`); `claude` CLI skipped for independence (we run inside Claude Code).
> This is a RE-REVIEW of the plans AFTER they were revised again following the round-2 review
> (`102-REVIEWS-round2.md`). Goal: confirm round-2 concerns were resolved and surface NEW issues
> introduced by the revisions.

## Codex Review

**Summary**
Verdict: **MEDIUM risk - minor-patch-then-execute**. The round-2 concerns are mostly resolved in the current plan set: stale tests are named, per-block grouping is specified, D-06 is now explicitly all-format strip at `_write_page_doc`, corrupt PDFs are buffer-then-decide, and migration 2->3 avoids auto-reindex.

I verified the live code at commit `3576bf31`. Python launchers in this sandbox point at missing installs, but package inspection confirms PyMuPDF `1.27.2.3`; `TEXTFLAGS_RAWDICT`, `TEXTFLAGS_DICT`, and `TEXT_PRESERVE_IMAGES` exist in `.venv\Lib\site-packages\pymupdf\__init__.py:17588` and `:17631-17639`.

**Strengths**
- Round-2 M1 is addressed: Plan 02 updates `tests/test_format_rtl_invariant.py`; Plan 04 updates `tests/test_phase_97_2_reset_my_library_full_cycle.py`.
- Round-2 M2 is addressed: Plan 02 now requires per-rawdict-block grouping and `\n\n` block joins.
- D-06 is coherent in the main plans: Plan 03 strips once at `_write_page_doc`, for all LOCAL formats, with `content == cached_text == stripped`.
- Corrupt encoding flow is much safer: Plan 03 buffers pages, decides `corrupt_encoding` before writes, keeps `encoding_error` classification unchanged, and adds status-surface tests.
- Migration 2->3 is conservative: Plan 04 is a stamp/no-op DDL migration, updates fresh DB stamping to user_version 3, and explicitly forbids pending flips.

**Concerns**
- **HIGH:** Plan 03 introduces a cancellation regression during the buffer phase. It says cancellation while buffering returns `"cancelled"` with “no rollback needed” (`102-03-PLAN.md:234-241`). But live `_index_one_file` pre-inserts `processed_files` and `local_files` before extraction (`shared/local_indexer.py:2223`, `:2227`, `:2245`), and `_commit_batch` later marks pending `processed_files` rows as committed (`shared/local_indexer.py:2833`). Existing rollback deletes `local_pages` and `processed_files` (`shared/local_indexer.py:2669-2670`). Patch Plan 03 so buffer-phase cancellation also calls `_rollback_partial(sys_id)`, and add a test for cancellation while buffering, not only during the final write loop.

- **MEDIUM:** The M3 fixture wording contradicts the proposed descending-x fix. Plan 01 says `intra_word_visual_ltr.json` should have `"שלום"` glyph centers ascending `ש<ל<ו<ם` left-to-right and still expect `"שלום"` after descending center-x (`102-01-PLAN.md:169`, `:223`). Descending x over that fixture yields `םולש`. For a visual-LTR emitted correct Hebrew word, the ascending-x emitted order should be `ם<ו<ל<ש`, expected output `שלום`.

- **MEDIUM:** Referenced support docs still contain stale D-06 divergence instructions. `102-PATTERNS.md:282`, `:315-316`, and `:516-521` still say `content` should be stripped while `cached_text` keeps nikud. `102-CONTEXT.md:201` and `:206` also still say D-06 keeps original cached text / deliberately diverges. Since plans instruct executors to read these files, patch or delete those stale sections before execution. Same issue for `encoding_error`: Plan 03 correctly keeps it indexed (`102-03-PLAN.md:275`), while `102-PATTERNS.md:364` still says `encoding_error` goes to errors.

- **LOW:** Plan 05 is wave 4 but only depends on `102-02` and `102-03` (`102-05-PLAN.md:6`). Because it runs `pytest tests/ -q -k "local"` (`102-05-PLAN.md:158`), explicitly add `102-04` to `depends_on` so dependency-only runners cannot start it before migration/UI status tests land.

- **LOW:** Plan 01 says `normalize_punctuation_spacing` handles Hebrew sof-pasuq/maqaf, but the regex shown only covers ASCII punctuation (`102-01-PLAN.md:277`). Add explicit Hebrew punctuation chars/tests or narrow the claim.

**Suggestions**
1. Patch Plan 03 buffer cancellation to rollback, and add a buffer-cancel test.
2. Fix the M3 fixture description and expected rawdict emission order.
3. Update `102-PATTERNS.md` and the stale `102-CONTEXT.md` code-context lines to match D-06 FINAL and the `encoding_error` decision.
4. Add `102-04` to Plan 05 dependencies.
5. Add Hebrew punctuation coverage for F-B.

**Overall Risk**
**MEDIUM - minor-patch-then-execute.** No replan needed. The remaining issues are localized, but I would not execute as-is because the buffer-phase cancellation bug can leave inconsistent committed state for a cancelled PDF.

---

## Consensus Summary

Single external reviewer (Codex) this round. Verdict: **MEDIUM — minor-patch-then-execute.**
Round-2 blockers (M1 stale-test naming, M2 per-block grouping, D-06 all-format strip clarity,
corrupt buffer-then-decide, migration 2→3 no-auto-flip) are confirmed resolved in the current plans.

### Agreed Concerns (action items before execution)

1. **HIGH — buffer-phase cancellation leaves inconsistent state.** Plan 03 (`102-03-PLAN.md:234-241`)
   says cancellation while buffering returns `"cancelled"` with "no rollback needed", but live
   `_index_one_file` pre-inserts `processed_files`/`local_files` BEFORE extraction
   (`shared/local_indexer.py:2223,:2227,:2245`) and `_commit_batch` later flips pending rows to
   committed (`:2833`). Buffer-phase cancel must call `_rollback_partial(sys_id)` (existing rollback
   at `:2669-2670`) + add a buffer-cancel test (not only final-write-loop cancel).
2. **MEDIUM — M3 fixture contradicts the descending-x fix.** `intra_word_visual_ltr.json`
   (`102-01-PLAN.md:169,:223`) describes ascending centers `ש<ל<ו<ם` but expects `שלום` after
   descending-x sort — that yields `םולש`. For a visual-LTR-emitted correct word the ascending
   emission should be `ם<ו<ל<ש` → expected `שלום`. Fix fixture description + expected order.
3. **MEDIUM — stale support docs still teach the old D-06 divergence.** `102-PATTERNS.md:282,:315-316,
   :516-521` and `102-CONTEXT.md:201,:206` still say `content` strips while `cached_text` keeps nikud
   (deliberate divergence) — contradicts D-06 FINAL. Also `102-PATTERNS.md:364` says `encoding_error`
   goes to errors, but Plan 03 (`:275`) correctly keeps it indexed. Patch/delete stale sections since
   executors are told to read these files.
4. **LOW — Plan 05 dependency gap.** Wave-4 Plan 05 depends only on 102-02/102-03 (`102-05-PLAN.md:6`)
   but runs `pytest tests/ -q -k "local"` (`:158`) which now includes 102-04's migration/UI status
   tests. Add `102-04` to `depends_on`.
5. **LOW — F-B Hebrew punctuation claim overstated.** `normalize_punctuation_spacing`
   (`102-01-PLAN.md:277`) claims sof-pasuq/maqaf handling but the regex is ASCII-only. Add Hebrew
   punctuation chars + tests, or narrow the claim.

### Divergent Views

N/A — single reviewer this round.
