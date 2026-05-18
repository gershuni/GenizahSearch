---
phase: 999.4-line-numbering
verified: 2026-05-18T23:15:00Z
status: passed
score: 13/13 truths verified
re_verification:
  previous_status: none
  note: initial verification
---

# Phase 999.4: Line numbering — Verification Report

**Phase Goal:** Display an RTL-aware line-number gutter alongside transcription text on 5 surfaces (web Browse single-page, Quick View, Full Manuscript View; desktop Browse tab `browse_text`, ResultDialog `text_ms`) with `text.split('\n')` semantics (D-10), copy-paste safe (D-04), and a user-controllable toggle persisted across sessions (D-07/D-09).

**Verified:** 2026-05-18 (initial). **Status:** PASSED. **Score:** 13/13.

## Observable Truths (Goal-Backward)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Web helper `_render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers)` at module scope; CSS-grid two-column layout with `user-select: none` gutter; `show_line_numbers=False` returns gutterless passthrough | PASS | `web/pages/browse.py:41-157` — signature matches; gutter span has `user-select: none; -webkit-user-select: none;` (:126); separate grid column via `display: grid; grid-template-columns: max-content 1fr` (:152); `show_line_numbers=False` branch at :80-93 returns plain `<div class="transcription-text">` with no gutter |
| 2 | Web wired at all 3 surfaces: Browse `render_text_content`, Quick View `render_text_section`, FMV per-page loop | PASS | `web/pages/browse.py:4348` (Browse `render_text_content` calls helper); `web/pages/search_results.py:1789` (imports + :1800 calls helper in `render_text_section`); `web/pages/browse.py:2714,2722` (FMV per-page calls in the `state.full_manuscript` loop) |
| 3 | Web toggle persisted via `safe_user_get('ui.show_line_numbers', True)` + `safe_user_set` in both surfaces; default True (D-07) | PASS | `web/pages/browse.py:4343,4364-4365` (render reads `bool(safe_user_get('ui.show_line_numbers', True))`; toggle handler reads + flips + writes); `web/pages/search_results.py:1794,1892-1894` (Quick View parallel). FMV render :2711 also reads same key. Default `True` literal present at all 4 read sites |
| 4 | Web copy-paste invariant (D-04): gutter has `user-select: none` AND separate CSS-grid column; test passes | PASS | Both invariants verified in #1 above. `tests/test_line_numbers_web.py::test_render_line_numbered_html_copy_paste_invariant` passes (`pytest -x -q` green) |
| 5 | Desktop widget `LineNumberArea(QWidget)` as sibling pattern (not subclass of body, not part of QTextDocument) | PASS | `desktop/widgets/line_number_text_edit.py:53` `class LineNumberArea(QWidget)`; `:60-66` `__init__(self, body_widget)` calls `super().__init__(body_widget)` (parent=body) but holds body via `self._body` — gutter is a child widget painted via `paintEvent`, NOT part of `body.document()`. WA_TransparentForMouseEvents (:72) keeps clicks falling through |
| 6 | Desktop wired at all 4+6 surfaces: 7 sites in `genizah_app.py:self.browse_text` and 4 setHtml sites in `desktop/result_dialog.py:self.text_ms` | PASS | `genizah_app.py` calls `apply_line_numbered_text` at 7 sites: :3372 (edit cancel), :3643 (transcription HTML), :6628 (browse init), :8627, :9395 (Reading Desk per-page via `pages=raw_text_parts`), :10044 (View All per-page via `pages=raw_text_parts`), :21572. `desktop/result_dialog.py` at :486 (init), :1294, :1307, :1974, :2166 = 5 sites total (Plan SUMMARY says "4 setHtml" — init+4 setHtml; verified by grep) |
| 7 | Desktop toggle persisted via `load_app_config({'show_line_numbers': True})` / `save_app_config`; `is_line_numbers_enabled()` + `set_line_numbers_enabled(bool)` exist | PASS | `desktop/widgets/line_number_text_edit.py:34` `_CONFIG_KEY = "show_line_numbers"`; :41-45 `is_line_numbers_enabled()` reads with default True (D-07); :48-50 `set_line_numbers_enabled` writes via `save_app_config({_CONFIG_KEY: bool(enabled)})`. Wired in `genizah_app.py:6527/6532/6533` (Browse find row) and `desktop/result_dialog.py:461/466/467` (RD find row) |
| 8 | Desktop copy-paste invariant (D-04): gutter is sibling QWidget; `body.toPlainText()` cannot contain digits; test passes | PASS | Structural invariant by widget hierarchy (#5). `tests/test_line_numbers_desktop.py::test_clipboard_isolation_invariant` passes (12/12 in headless Qt run) |
| 9 | D-10 split semantics shared (LINE-NUM-09): web uses `text.split('\n')`; desktop walks `block.text().split(' ')` (Qt's U+2028 for `<br>`); blank lines and trailing empties numbered | PASS | `web/pages/browse.py:110,113` both call `.split('\n')` (NOT splitlines). `desktop/widgets/line_number_text_edit.py:139,157` both call `block.text().split(' ')` (` ` is U+2028 LINE SEPARATOR — Qt's `<br>` representation). Test `test_render_line_numbered_html_blank_count_matches` + `test_line_number_area_line_count_matches_split` cover blanks/trailing empties (both pass) |
| 10 | Per-page restart on View All across both pillars | PASS | Web: `web/pages/browse.py:2693-2729` — the `for idx, doc_page in enumerate(state.full_manuscript)` loop makes a fresh `_render_line_numbered_html` call per page; counter naturally restarts at 1 in each call. Desktop: `genizah_app.py:10044` passes `pages=raw_text_parts` to `apply_line_numbered_text`, which calls `_mark_blocks_for_pages` (line_number_text_edit.py:289) tagging each QTextBlock with `setUserState(page_idx)`; painter (:127-151) resets `line_in_page = 0` when `state != current_page` |
| 11 | All 24 tests pass (12 web + 12 desktop) | PASS | `python -m pytest tests/test_line_numbers_web.py tests/test_line_numbers_desktop.py -x -q` → `24 passed in 2.33s` |
| 12 | REQUIREMENTS + ROADMAP + STATE flipped | PASS | `REQUIREMENTS.md:96-111` LINE-NUM-01..10 all `[x]`; Traceability rows :170-179 all `Complete`. `ROADMAP.md:405` reads `Phase 999.4: Line numbering (BACKLOG — SHIPPED 2026-05-18)` with both plan checkboxes `[x]` and `LINE-NUM-01..10 ✅`. `STATE.md:5,28,57` status=idle, Phase 999.4 Complete (2026-05-18) in Backlog table |
| 13 | No cross-pillar contamination | PASS | `git log --name-only` for the 13 commits: web commits (5ce115f5, 69a48986, ba666564, 9bde739e, e63d0e91) touched ONLY `web/pages/browse.py`, `web/pages/search_results.py`, and `tests/test_line_numbers_web.py`. Desktop commits (dbd3b96f, 30cc144e, 7a93d4eb, b3a491a9, 346546ad, cbb4a3fb, 0c164687, 05a5740b) touched ONLY `desktop/widgets/line_number_text_edit.py`, `desktop/widgets/__init__.py`, `genizah_app.py`, `desktop/result_dialog.py`, `genizah_translations.py` (the latter is the intentional shared translations file, allowed). Grep confirms no `line_number` references in `web/` outside `browse.py` + `search_results.py` |

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/pages/browse.py::_render_line_numbered_html` | Module-scope pure helper | VERIFIED | 117 lines (:41-157); pure str→str; XSS-escapes raw `text`; normalizes pre-converted `<br>`→`\n` for counting |
| `desktop/widgets/line_number_text_edit.py` | New module with sibling-QWidget gutter | VERIFIED | 410 lines; exports `LineNumberArea`, `apply_line_numbered_text`, `refresh_visibility`, `is_line_numbers_enabled`, `set_line_numbers_enabled`, `_mark_blocks_for_pages`, `_normalize_block_text` |
| `tests/test_line_numbers_web.py` | 12 structural tests | VERIFIED | 12/12 pass |
| `tests/test_line_numbers_desktop.py` | 12 headless Qt tests | VERIFIED | 12/12 pass |

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| Browse render | helper | Direct call at `web/pages/browse.py:4348` | WIRED | `_render_line_numbered_html(text=..., highlight_html=..., show_line_numbers=show_ln)` |
| Quick View render | helper | Cross-module import + call | WIRED | `from web.pages.browse import _render_line_numbered_html` at `search_results.py:1789`; call at :1800 |
| FMV per-page render | helper | Per-page call in loop | WIRED | `browse.py:2714/2722` called per `doc_page` |
| Desktop Browse render | `apply_line_numbered_text` | 7 wired sites | WIRED | All 7 sites from `Grep` line numbers match the SUMMARY's planned list (plus an init at :6628 not enumerated in the SUMMARY but inert/empty) |
| Desktop RD render | `apply_line_numbered_text` | 5 sites | WIRED | result_dialog.py :486 (init), :1294/:1307/:1974/:2166 |
| Toggle (web) | `safe_user_set('ui.show_line_numbers', ...)` | 2 surfaces | WIRED | browse.py:4365; search_results.py:1894 |
| Toggle (desktop) | `set_line_numbers_enabled` + `refresh_line_number_visibility` | 2 surfaces | WIRED | genizah_app.py:6532-6533; result_dialog.py:466-467 |

## Data-Flow Trace (Level 4)

- Web: `safe_user_get('ui.show_line_numbers', True)` → `show_ln` (bool) → `_render_line_numbered_html(show_line_numbers=show_ln)` → returns HTML with/without gutter rows → `ui.html(html_str, sanitize=False)`. Real data: source text comes from `page.text` / `doc_page.text` / `display_text` (real Supabase / PGP transcription data, not stubbed). FLOWING.
- Desktop: `load_app_config().get('show_line_numbers', True)` → `is_line_numbers_enabled()` → `area.setVisible(enabled)` after `apply_line_numbered_text`. Line count from `source_text.split('\n')` or `pages=raw_text_parts`. Real data: transcription HTML from PGP `pgp_translations`/edition pipeline. FLOWING.

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Web + desktop test suites pass | `python -m pytest tests/test_line_numbers_web.py tests/test_line_numbers_desktop.py -x -q` | `24 passed in 2.33s` | PASS |
| `_render_line_numbered_html` importable + pure | (covered by test import at suite start) | covered | PASS |
| `apply_line_numbered_text` + sibling-widget pattern | (covered by `test_clipboard_isolation_invariant`) | covered | PASS |

## Requirements Coverage

LINE-NUM-01..10 all `[x]` in `REQUIREMENTS.md:96-111` and `Complete` in Traceability `:170-179`. All 10 mapped to specific source-of-truth files/lines verified above.

## Anti-Patterns Found

None blocking. The desktop module contains an intentional duplicate of the `_BIDI_CONTROLS` literal (`line_number_text_edit.py:258-262` AND :265-269) — a harmless cosmetic redundancy from the 4-round smoke-check iteration. Not a blocker; not a stub; both definitions identical. Noted for follow-up housekeeping if/when this file is next edited.

## Human Verification

None required for this verification pass — the SUMMARY documents 4 rounds of human smoke-checks with Hillel (web round 1+2; desktop rounds 1–4) all approved. Re-verification by user not requested.

## Gap Summary

No gaps. Phase 999.4 achieves its stated goal across all 5 surfaces with copy-paste invariant (D-04), split-semantics invariant (D-10), per-page restart (D-11), default-ON toggle (D-07), per-user persistence (D-09), and no cross-pillar contamination. The known divergence noted in the SUMMARY — desktop View All originally rendered continuous numbering and was corrected to per-page restart at commit `cbb4a3fb` via the `pages=raw_text_parts` keyword + `_mark_blocks_for_pages` block-tagging — is fully resolved in the shipped code (Truth-10).

---

*Verified: 2026-05-18T23:15:00Z*
*Verifier: Claude (gsd-verifier)*
