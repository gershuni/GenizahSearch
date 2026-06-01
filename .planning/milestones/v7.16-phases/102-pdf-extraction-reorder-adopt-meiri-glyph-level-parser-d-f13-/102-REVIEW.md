---
phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-
reviewed: 2026-05-29T00:00:00Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - shared/local_indexer_rtl.py
  - shared/local_indexer.py
  - shared/local_indexer_migrations.py
  - desktop/my_library_tab.py
  - tests/scripts/build_phase102_fixtures.py
  - tests/test_local_pdf_rtl_helpers.py
  - tests/test_local_pdf_rawdict_extraction.py
  - tests/test_local_pdf_nikud_strip.py
  - tests/test_local_pdf_corrupt_status.py
findings:
  critical: 0
  warning: 3
  info: 5
  total: 8
status: issues_found
---

# Phase 102: Code Review Report

**Reviewed:** 2026-05-29
**Depth:** standard
**Files Reviewed:** 9
**Status:** issues_found

## Summary

Phase 102 rewrites the LOCAL PDF text-layer extractor onto a PyMuPDF `rawdict`
(per-glyph bbox) foundation with RTL-gated word-unit reordering, adaptive
letter-spacing de-collapse, a conservative corrupt-encoding detector, and a
D-06 all-format nikud strip at the shared `_write_page_doc` write site. The
implementation is careful and well-documented, with the four areas flagged for
attention all handled correctly:

1. **bbox / center-x geometry and RTL gating** — `_center_x`, `_unit_center_x`,
   `_bbox_union`, and the descending-center-x M3 letter ordering are all
   geometrically sound. Every reorder/despace/bracket/punctuation helper is
   gated by `rtl_ratio(...) <= 0.4` and returns the input untouched on LTR text.
2. **buffer-then-decide `_extract_and_write_pdf` + `_rollback_partial`** — the
   three-phase flow (buffer → corrupt-decision → write) is correct; the
   corrupt path returns without rollback (preserving sibling files' uncommitted
   docs), and `_rollback_partial` now deletes `local_pages`, `processed_files`,
   AND `local_files` for the sys_id (the round-3 fix). The cancel/corrupt
   contracts match the tests.
3. **lazy `strip_nikud` import** — function-local inside `_write_page_doc`,
   applied uniformly to all formats, with `content == cached_text == stripped`
   (both derived from the same `stripped` variable). The AST guard test pins
   the no-module-top-import invariant.
4. **LTR-regression risk** — Latin rows take the `line_text_raw.strip()`
   pass-through branch in `_extract_one_page_rawdict`, and the D-03
   `_ltr_damage_guard` falls back to the proven blocks path on token-count or
   Jaccard divergence. LTR text is structurally protected.

No Critical issues found. The Warnings below are correctness edge cases worth
confirming or guarding; the Info items are minor robustness/clarity notes.

## Warnings

### WR-01: `_rollback_partial` rolls back the ENTIRE writer, discarding sibling files' uncommitted Tantivy docs

**File:** `shared/local_indexer.py:3071-3090`
**Issue:** On a mid-batch cancel, `_rollback_partial` calls
`self._writer.rollback()`, which discards *all* documents added to the writer
since the last `_commit_batch()` — not just the cancelled file's pages.
`_write_page_doc` only commits SQLite per page (`self._conn.commit()`); the
Tantivy `add_document` calls accumulate in the writer until `_commit_batch`
fires (byte/count/time trigger). So when file N is cancelled, files
`last_commit+1 .. N-1` that already finished and were appended to
`_pending_filepaths` have their SQLite `local_pages` rows committed but their
Tantivy docs rolled back. This creates a transient SQLite↔Tantivy divergence
for those sibling files.

In practice the damage is bounded because (a) cancel halts the scan
immediately, (b) the final `_commit_batch` is skipped on cancel
(`scan_all:2326` guards `if not result["cancelled"]`), and (c) those siblings'
`processed_files.status` stays `'pending'`, so the next scan re-indexes them.
But the `local_pages` rows for those siblings are now orphaned (cached_text
present, no matching Tantivy doc) until the re-scan overwrites them. This is a
pre-existing pattern (the `writer.rollback()` predates Phase 102), but Phase 102
expanded `_rollback_partial`'s SQLite deletions to `local_files`, making the
asymmetry more visible.

**Fix:** Prefer per-document deletion over a full writer rollback so sibling
docs survive. The `delete_documents("unique_id", uid)` loop already targets
exactly this file's pages — drop the `writer.rollback()` / re-open and let the
deletes stand (they take effect on the next commit). If `rollback()` must stay
for heap-pressure reasons, document that pending sibling `local_pages` rows are
knowingly orphaned-until-rescan, e.g.:

```python
for uid_row in uid_rows:
    self._writer.delete_documents("unique_id", uid_row["uid"])
# Do NOT writer.rollback() here — it would discard sibling files' uncommitted
# docs added since the last _commit_batch. The per-uid deletes above scope the
# rollback to THIS file's pages only.
self._conn.execute("DELETE FROM local_pages WHERE sys_id = ?", (sys_id,))
self._conn.execute("DELETE FROM processed_files WHERE sys_id = ?", (sys_id,))
self._conn.execute("DELETE FROM local_files WHERE sys_id = ?", (sys_id,))
self._conn.commit()
```

### WR-02: corrupt-encoding decision counts only pages PRESENT in `page_flags`, but the threshold divides by `len(buffered)`

**File:** `shared/local_indexer.py:2900-2908`
**Issue:** The corrupt decision is
`corrupt_count >= len(buffered) * 0.5` where `corrupt_count` sums pages whose
`page_flags.get(p[0], {}).get("corrupt", False)` is True. If `extract_pdf_pages`
yields a page but (due to the `try/except` around the multicolumn/flag block at
`local_indexer.py:1055-1073`) fails to populate `page_flags[page_num]`, that
page silently counts as non-corrupt in the numerator while still counting in the
denominator. A file where the flag-collection path raises on the genuinely
corrupt pages could therefore dodge the ≥50% threshold. The flag-collection
`get_text("rawdict", ...)` is a second extraction call that can fail
independently of the primary one.

This is a low-probability edge (the same rawdict call already succeeded once for
the primary text), but the asymmetry — denominator always counts the page,
numerator only counts pages that got flagged — means missing flags bias *away*
from the corrupt verdict.

**Fix:** Either compute `corrupt` inline from the already-extracted `text`
during buffering (avoiding the second `get_text` and the missing-key path), or
make the threshold robust to missing flags by only dividing over pages that have
a flag entry:

```python
flagged = [p for p in buffered if p[0] in page_flags]
if flagged:
    corrupt_count = sum(1 for p in flagged if page_flags[p[0]].get("corrupt"))
    if corrupt_count >= len(flagged) * 0.5:
        return (0, "corrupt_encoding", display_title)
```

### WR-03: `extract_pdf_pages` calls `page.get_text("rawdict", ...)` twice per page when `page_flags` is supplied

**File:** `shared/local_indexer.py:1043` and `1058`
**Issue:** `_extract_one_page_rawdict(page, ...)` already calls
`page.get_text("rawdict", flags=_RAWDICT_FLAGS)` (line 1120), and then when
`page_flags is not None` the caller re-runs the *same* expensive parse
(line 1058) solely to collect per-line x-bbox ranges for the multicolumn
detector. Since `_extract_and_write_pdf` always passes `page_flags`, every
production PDF page is parsed twice. Although v1 performance is out of review
scope, this is a correctness-adjacent duplication: the two `rawdict` calls could
in principle observe different output, and the line bboxes feeding the
multicolumn flag are derived from a *different* parse than the text feeding the
corrupt flag.

**Fix:** Have `_extract_one_page_rawdict` return (or accept an out-param for) the
line x-ranges it already iterated, so the caller reuses the single parse rather
than re-invoking `get_text("rawdict")`. This removes the double-parse and
guarantees the flags and text describe the same glyph data.

## Info

### IN-01: `_detect_corrupt_encoding` denominator includes allowlisted/skipped codepoints, diluting both ratios

**File:** `shared/local_indexer.py:614-618`
**Issue:** `garbage_ratio` and `wordlike_ratio` both divide by `total = len(text)`,
but the loop `continue`s past allowlisted ranges (Arabic, Greek, Hebrew
presentation forms, bidi marks, all `P*` punctuation, `Sm`) without incrementing
either counter. A page that is 70% punctuation/bidi and 30% genuine garbage
would show `garbage_ratio = 0.30` only if those garbage chars are 30% of the
*whole* string — the punctuation mass dilutes the signal. This is intentionally
conservative (the stated design goal), so it is correct-as-designed, but the
`wordlike_ratio < 0.40` arm is harder to trigger than the comment implies on
punctuation-heavy pages. No change required; noting for future tuning.

### IN-02: `_order_unit_text_rtl` nikud-to-base attachment uses `min(... key=abs distance)` with no tie-break

**File:** `shared/local_indexer_rtl.py:236-238`
**Issue:** Each nikud mark attaches to the nearest base by `abs(center_x diff)`.
When a mark sits exactly between two equidistant bases, `min` returns the first
in `bases` order (rawdict reading order), which is deterministic but arbitrary.
For tightly-set vocalized Hebrew this is fine in practice; flagging only because
a future denser-nikud fixture could expose an off-by-one attachment. Consider
preferring the base immediately to the mark's right (RTL: the consonant a vowel
sits under/before) on ties.

### IN-03: digit-run re-reversal in `reorder_word_units_rtl` keys on `text.isdigit()` only

**File:** `shared/local_indexer_rtl.py:406-408`
**Issue:** `_is_digit_unit` treats a unit as a number only when the whole
stripped token `isdigit()`. Mixed tokens common in citations — `"12a"`,
`"12.3"`, `"1,234"`, folio `"3r"` — are not recognized as LTR numeric runs, so
their relative order may be flipped inside RTL reflow. The `_fix_sort_true_rtl_line`
fallback path has the same limitation. Acceptable for v1 (pure page/footnote
numbers dominate), but worth a note for the Hebrew-bibliography corpus where
`"12.3"`-style references appear.

### IN-04: fixture builder mutates a private PyMuPDF API (`page._set_contents`)

**File:** `tests/scripts/build_phase102_fixtures.py:267`
**Issue:** The corrupt-sample builder falls back to
`page._set_contents([xref])  # type: ignore[attr-defined]`, a private/underscore
API that may be removed or renamed across PyMuPDF releases (currently 1.27.2).
The primary path (`update_stream(contents_xrefs[0], ...)`) is public and is what
runs when `get_contents()` returns a non-empty list (the normal case), so the
private branch is rarely hit. Since the builder is committed precisely so
fixtures can be regenerated after a PyMuPDF upgrade (Codex LOW-11), pin the
PyMuPDF version in a comment or guard the private call with a clear failure
message if it disappears.

### IN-05: `_RAWDICT_FLAGS` AttributeError fallback yields 0 (no flags), not `TEXTFLAGS_DICT`

**File:** `shared/local_indexer.py:103-106`
**Issue:** The docstring/intent (lines 99-102) says "fall back to
`TEXTFLAGS_DICT`", but the `except AttributeError` branch computes
`fitz.TEXT_PRESERVE_IMAGES ^ fitz.TEXT_PRESERVE_IMAGES` which is `0` — a "no
flags" sentinel, not `TEXTFLAGS_DICT`. The comment labels it a "no-op fallback,"
so the code matches what it does, but it contradicts the module-import comment
two lines above. On any PyMuPDF where `TEXTFLAGS_RAWDICT` is missing,
`get_text("rawdict", flags=0)` still requests rawdict mode (the `"rawdict"`
string drives the parse), so behavior is acceptable; only the comment is
misleading. Align the comment with the actual `0`-flags fallback.

---

_Reviewed: 2026-05-29_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
