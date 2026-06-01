# Phase 101: LOCAL PDF text extraction RTL fix and Phase 100 remnant cleanup - Context

**Gathered:** 2026-05-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Clear the four remnant issues blocking a clean v7.15 release:

1. **RTL/bidi word-order fix** (primary) — LOCAL PDF text extraction reverses word order per line on Hebrew/Judeo-Arabic books (each line shows last-word-first). Fix the text layer so transcriptions index and display in correct reading order. Image rendering is already correct and out of scope.
2. **WR-01** — `genizah_app.py::_open_local_browse_page` computes `is_pdf` and `filepath` from two separate `_lookup_local_filepath` calls that could diverge; collapse to one lookup.
3. **WR-02** — add a regression test asserting `PdfImageController._pending` is empty immediately after `discard_scope`.
4. **Test-isolation flake** — `tests/test_local_indexer.py::test_txt_undecodable_marked_encoding_error` passes in isolation, fails under batch ordering (global-state pollution from a sibling test). Pre-existing; not introduced by Phase 100.

Clarifies HOW to implement these four items. New capabilities (OCR for image-only PDFs, side-by-side PDF view, etc.) belong in other phases.

</domain>

<decisions>
## Implementation Decisions

### RTL Fix Technique

> **⚠ DECISION OVERRIDE (2026-05-27, post-research — these supersede the original D-01/D-02/D-05 text below).**
> Research empirically proved `python-bidi`'s `get_display` is a logical→visual transform that reverses *characters* (corrupts already-correct Hebrew letters: `שלם`→`מלש`). The actual bug is **word-order** reversal introduced by PyMuPDF's `get_text("text", sort=True)` fallback (sorts words by ascending x = LTR visual order). User confirmed the corrected approach:
> - **D-01 (OVERRIDDEN):** Use **per-line word-token reversal** — `' '.join(line.split()[::-1])` — NOT `python-bidi`. Letters within each word stay in correct logical Unicode order; only word tokens are reversed.
> - **D-02 (VOIDED):** **Do NOT add `python-bidi`.** It is unused under the word-reversal approach; the existing `_rtl_ratio` helper handles RTL detection. No `requirements.txt` change, no `GenizahSearchPro.spec` `collect_all('bidi')`, no Rust `.pyd` packaging.
> - **D-05 (REINTERPRETED):** Apply the reversal **only inside the `sort=True` fallback branch** (`if _detect_single_word_per_line(text):`), gated on `_rtl_ratio > 0.4`. Do NOT touch the primary `get_text("blocks")` path — it already returns correct RTL order, and applying reversal there would break correctly-ordered professional RTL PDFs. The `_rtl_ratio > 0.4` gate is a true no-op on pure-LTR/numeric lines, satisfying D-05's "no fragile threshold" intent.
> See `101-RESEARCH.md` "The RTL Failure Mechanism" + Pitfalls 1–2 for the empirical basis.

- **~~D-01~~ (OVERRIDDEN — see box above):** ~~Use `python-bidi` to recover reading order.~~
- **~~D-02~~ (VOIDED — see box above):** ~~Add python-bidi to requirements.txt + GenizahSearchPro.spec.~~
- **D-03:** Fix lives in the **shared extraction path** (`shared/local_indexer.py::extract_pdf_pages`). Both the search index and the displayed transcription read the same stored text, so a single extraction-layer fix corrects both surfaces. **(STILL VALID — the word-reversal fix lands in this chokepoint, specifically in the sort=True fallback branch.)**
- **Research note (RESOLVED):** Failure confirmed as word-order reversal via PyMuPDF `sort=True`; letters are already correct logical Unicode. Word-token reversal restores reading order. See `101-RESEARCH.md`.

### Backfill of Already-Indexed PDFs
- **D-04:** **Auto-reindex via version bump.** Bump the extractor/index schema version so existing LOCAL libraries are detected as stale and re-indexed automatically on next launch, reusing the existing recovery/rebuild machinery (`.meta.json` freshness markers + recovery probe). User does nothing; reversed text self-corrects.
- **Research note:** Confirm the exact staleness-detection hook — whether there is an extractor-version constant that, when bumped, forces reindex, or whether this requires touching the `.meta.json` schema-version / recovery-scan logic. Reuse existing rebuild paths; do not bulk-render or re-render images (text-layer reindex only).

### RTL Detection Gate
- **D-05 (REINTERPRETED — see override box under "RTL Fix Technique"):** Apply word-token reversal only inside the `sort=True` fallback branch, gated on `_rtl_ratio > 0.4` (a true no-op on pure-LTR/numeric/mixed-LTR lines). The original "unconditionally to every line" wording is superseded: research showed the primary `get_text("blocks")` path is already correct and must not be reordered. D-05's *intent* (no fragile threshold for correct output) holds — pure-RTL always passes the gate, pure-LTR never does.
- **Research note (RESOLVED):** `_rtl_ratio` empirically confirmed a no-op on LTR/numeric/empty lines. `tests/fixtures/local_indexer/single_word_per_line.pdf` serves as the LTR regression guard.

### RTL Verification Fixture
- **D-06:** Verify with a **real Hebrew PDF excerpt** from the actual book that surfaced the bug in Phase 100 UAT. **The user (Hillel) will provide a small excerpt** — this is a prerequisite the planner/executor needs before the RTL test can be finalized. Commit a 1–2 page excerpt as the fixture (mirroring the `single_word_per_line.pdf` pattern); the test asserts extracted text matches known-correct reading order. Add a copyright/provenance note in the fixture README or test docstring.

### Cleanup Trio (prescriptive — execute as specified in 100-REVIEW.md)
- **D-07 (WR-01):** In `_open_local_browse_page`, compute `filepath = self._lookup_local_filepath(sys_id)` once and derive `is_pdf = bool(filepath) and filepath.lower().endswith('.pdf')` from it, so the pane-reveal decision and the `controller.request()` decision can never diverge. See `100-REVIEW.md` WR-01 for the exact patch shape.
- **D-08 (WR-02):** Add a regression test that opens + closes a ResultDialog (or controller scope) while a render is queued and asserts `ctrl._pending` is empty immediately after `discard_scope`.
- **D-09 (flake):** Fix the test-isolation flake by identifying the polluting sibling test and isolating the shared global state (proper teardown/fixture scoping). The failing test is `tests/test_local_indexer.py::test_txt_undecodable_marked_encoding_error`; the planner/researcher pins the exact polluting source.

### Claude's Discretion
- Exact module placement of the bidi-reorder helper within `local_indexer.py`, function naming, and whether to retire/replace the dead-code `_fix_rtl_*` helpers (D-02 from Phase 95 marked them a regression-prevention contract — decide whether they remain or are superseded by the live python-bidi path).
- Precise mechanism for the flake fix once the polluting sibling is identified.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### RTL / PDF text extraction
- `shared/local_indexer.py` — `extract_pdf_pages` (lines ~672-721, current `get_text("blocks")` + `sort=True` fallback), dead-code RTL helpers `_rtl_ratio`/`_fix_rtl_line`/`_fix_rtl_page`/`_join_fragmented_lines` (lines ~302-358), `_detect_single_word_per_line` (live, ~373).
- `docs/OPEN_ISSUES.md` (line ~220) — the RTL PDF text-extraction issue row: root-cause analysis (PyMuPDF visual-order glyph runs) and recommended fix paths (python-bidi / x-coord reordering).
- `tests/fixtures/local_indexer/single_word_per_line.pdf` — existing PDF-extraction regression fixture (Phase 96 D-F4); pattern to mirror for the new RTL fixture + serves as LTR no-op guard.
- `seewald_addition/genizah_make_index.py:67-105` — original source of the ported dead-code RTL helpers.

### Packaging
- `GenizahSearchPro.spec` — PyInstaller spec; `hiddenimports` list (line ~11) must gain `bidi`/`collect_all('bidi')`.
- `requirements.txt` (loose deps) — must add `python-bidi`; `requirements-lock.txt` already pins `python-bidi==0.6.7`.

### Cleanup trio
- `.planning/phases/100-local-pdf-image-in-resultdialog-browse/100-REVIEW.md` — WR-01 (with exact fix snippet) and WR-02 (regression-test recommendation).
- `genizah_app.py::_open_local_browse_page` (~lines 19151-19272) — WR-01 site.
- `desktop/pdf_image_controller.py` (~227-234, `_pending`, `discard_scope`, `cancel`) and `desktop/result_dialog.py` (~3286-3293) — WR-02 site.
- `tests/test_local_indexer.py::test_txt_undecodable_marked_encoding_error` (~line 266) — the flaky test.

### Backfill machinery
- Phase 97/97.2/97.3 recovery + rebuild code (`sweep_running_scan_runs`, `start_recovery_probe`, LAB rebuild abort-on-empty-source guard, `.meta.json` freshness markers) — the existing reindex/staleness machinery to reuse for D-04.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/local_indexer.py::_rtl_ratio` — fraction of RTL chars among alpha chars; available as a fallback gate if unconditional bidi proves unsafe (D-05).
- Existing recovery/rebuild pipeline (Phases 97.x) — reuse for auto-reindex on version bump (D-04); do not invent new reindex paths.
- `single_word_per_line.pdf` fixture + its test — template for the new RTL PDF fixture and assertion style (D-06).

### Established Patterns
- PDF extraction is per-page via `get_text("blocks")` with a per-page `get_text("text", sort=True)` fallback (Phase 96 D-F4). The bidi reorder should slot into this same per-page flow.
- Encoding/extraction failures set `local_files.extraction_status`; tests assert status + zero emitted docs.
- Desktop-only constraint: My Library does not exist on web, so the RTL fix and reindex are desktop-surfaced (though the code lives in `shared/`).

### Integration Points
- `extract_pdf_pages` is the single chokepoint — both index and display consume its output, so the fix lands in one place (D-03).
- `GenizahSearchPro.spec` is the desktop packaging boundary where the new Rust dependency must be collected (D-02).

</code_context>

<specifics>
## Specific Ideas

- Real failure signature: each RTL line shows words in reversed order ("last-word-first") with letters apparently correct — this is the discriminator that rules out plain string reversal and favors a true bidi pass.
- User will hand over a small real PDF excerpt from the Phase 100 UAT book for the test fixture (D-06) — planning should treat this as an inbound asset.

</specifics>

<deferred>
## Deferred Ideas

- **PDF OCR (D-F2)** — scanned/image-only PDFs with no text layer still yield nothing; OCR (Tesseract or similar) remains out of scope for v7.15. Tracked in `docs/OPEN_ISSUES.md`.
- None of the discussion introduced new scope beyond the four roadmap-scoped remnant items.

</deferred>

---

*Phase: 101-local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea*
*Context gathered: 2026-05-27*
