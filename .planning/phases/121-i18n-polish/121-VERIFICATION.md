---
phase: 121-i18n-polish
verified: 2026-06-21T17:45:00Z
status: human_needed
score: 3/3 must-haves verified
overrides_applied: 0
human_verification:
  - test: "SEED-010 — Joins Lab image-resolution + zoom defect (language-independent)"
    expected: "CUDL/Oxford images resolve consistently across anchor/grid/Compare panes; zoom works when image fails to load"
    why_human: "Defect is language-independent and was explicitly deferred during Hillel's 2026-06-21 HE-UAT pass (untestable during NLI outage). Logged as SEED-010 / docs/OPEN_ISSUES.md P2. Out of i18n scope but still an open defect in the Joins Lab that must be resolved before v8.2.0 ships."
---

# Phase 121: i18n Polish — Verification Report

**Phase Goal:** Every visible string in the Joins Lab is available in both English and Hebrew; RTL layout is correct throughout — anchor transcription, builder rows, candidate grid/table, Compare panes, dialogs and toasts; no Hebrew string leaks to the English interface and no English string to the Hebrew interface.

**Verified:** 2026-06-21T17:45:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Every UI string in the Joins Lab has both EN and HE keys in tr(); switching language updates all strings without page reload | VERIFIED | `python -m pytest tests/test_joins_lab_i18n.py -q` → 6 passed. Direct probe confirmed: all 17 gap keys present in TRANSLATIONS with valid Hebrew values; all tr() literal keys across the 8 FULL_SCAN_FILES resolve in TRANSLATIONS; 3 badge_and_tooltip() strings covered; 4 entry-point keys wired in host files. |
| 2 | The Hebrew-interface layout is fully RTL: anchor transcription right-aligned, builder rows RTL, candidate grid/table RTL, Compare panes mirrored, no element clipped/overlapping | VERIFIED (conditional) | SC#2 has two halves: (a) automated structural: `test_rtl_flex_row_reverse_pagination_and_compare` passes — flex-row-reverse confirmed in pagination row (candidate_grid.py:1363) and Compare nav bar (compare_modal.py:792, triple-token signature). (b) Human acceptance: 121-HE-UAT-CHECKLIST.md signed off PASS by Hillel on 2026-06-21, all 8 surfaces PASS. Inline fixes applied during UAT (bfc658fa, 1a8c9aca). One deferred defect (SEED-010: image-resolution + zoom, language-independent) logged to OPEN_ISSUES.md P2. |
| 3 | A static/AST audit confirms no raw Hebrew literal appears in the Joins Lab page/component Python files outside tr() | VERIFIED | Direct AST scan of all 8 FULL_SCAN_FILES: zero raw Hebrew literals outside tr() (ALLOWLIST correctly excludes the 8 D-04 operator-tuple literals from joins_builder.py:344-351). CI guard test_no_raw_hebrew_literals passes and was proven to bite via sanity injection. |

**Score:** 3/3 truths verified

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `genizah_translations.py` | 17 new HE keys + 3 drift fixes in TRANSLATIONS | VERIFIED | Commit 85330c9e: 56 insertions / 3 deletions. All 17 keys confirmed present with valid Hebrew (non-key) values. Drift fixes confirmed: 'Open in Joins Lab' → 'פתח במעבדת הצירופים', 'Clear Joins Lab' → 'נקה את מעבדת הצירופים', 'Clear all Joins Lab state: anchor, builder, triage, filters' → correct form. Post-UAT: commit bfc658fa fixed 'Add as Join' duplicate-key drift and 'View in Browse' wording. |
| `web/pages/joins_lab.py` | tr()-wrapped XLSX sheet name | VERIFIED | `ws.title = tr('Candidates')` present; raw `ws.title = 'Candidates'` absent. Commit 047dab5f. |
| `tests/test_joins_lab_i18n.py` | Permanent dual-check AST guard + badge-string list + scoped entry-point key-check + drift-value pin | VERIFIED | File exists, 384 lines (exceeds min_lines: 120). Contains BADGE_STRINGS, HEBREW_LITERAL_ALLOWLIST, ENTRY_POINT_KEYS, FULL_SCAN_FILES, DRIFT_PINNED_VALUES. Six test functions confirmed. |
| `tests/render_smoke/test_joins_lab_render_smoke.py` | RTL structural render-smoke assertions for HE mode | VERIFIED | Contains set_language, flex-row-reverse, justify-between; test_rtl_flex_row_reverse_pagination_and_compare present. |
| `.planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md` | Per-surface HE-mode RTL UAT checklist + sign-off block | VERIFIED | File exists with all 8 surfaces, per-surface PASS/FAIL items. Sign-off block: overall PASS, signed by Hillel, dated 2026-06-21. |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `web/components/candidate_grid.py:760` | `genizah_translations.TRANSLATIONS` | `tr(tooltip_text)` on badge_and_tooltip() return | WIRED | `tr(tooltip_text)` confirmed in candidate_grid.py |
| `web/pages/lists.py:709` | `genizah_translations.TRANSLATIONS['Open in Joins Lab']` | `tr('Open in Joins Lab')` | WIRED | `tr('Open in Joins Lab')` confirmed in lists.py; TRANSLATIONS value = 'פתח במעבדת הצירופים' |
| `web/components/compare_modal.py:470-471` | `genizah_translations.TRANSLATIONS` | `tr(tooltip_text)` for badge strings | WIRED | `tr(tooltip_text)` confirmed in compare_modal.py (2nd badge call site noted in REVIEWS #5) |
| `tests/test_joins_lab_i18n.py` | `genizah_translations.TRANSLATIONS` | `from genizah_translations import TRANSLATIONS` | WIRED | Confirmed in test file |
| `tests/render_smoke/test_joins_lab_render_smoke.py` | `web.translations.set_language` | `set_language('he')` before `user.open('/joins-lab')` | WIRED | Confirmed, with finally-block restore for test isolation |

---

### Data-Flow Trace (Level 4)

Not applicable — this phase modifies only i18n strings (TRANSLATIONS dict entries and tr() wraps) and test files. No dynamic data rendering components introduced.

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 17 gap keys present with valid Hebrew values | `PYTHONUTF8=1 python -c "from genizah_translations import TRANSLATIONS as T; ..."` | 0 missing, 0 bad values | PASS |
| Drift fixes correct ('הצירופים' form) | Key value assertions | All 3 drift keys verified | PASS |
| XLSX sheet tr()-wrapped | Source string grep | `ws.title = tr('Candidates')` present | PASS |
| SC#3 — zero raw Hebrew outside tr() | Direct AST scan of 8 files | 0 leaks found | PASS |
| SC#1 — all tr() literal keys resolve | Direct AST extraction + TRANSLATIONS lookup | 0 missing keys across 8 files | PASS |
| Entry-point keys wired in host files | Source grep | All 4 keys in TRANSLATIONS + tr()-wrapped in host | PASS |
| Drift-prone keys pin to correct values | DRIFT_PINNED_VALUES check | Add as Join=הוסף כצירוף, Open in Joins Lab=פתח במעבדת הצירופים, etc. | PASS |
| Ruff clean on Phase 121 modified files | `python -m ruff check tests/test_joins_lab_i18n.py tests/render_smoke/... web/pages/joins_lab.py genizah_translations.py web/components/joins_builder.py` | All checks passed | PASS |

---

### Probe Execution

| Probe | Command | Result | Status |
|-------|---------|--------|--------|
| Permanent i18n guard | `python -m pytest tests/test_joins_lab_i18n.py -q` | 6 passed in 0.64s | PASS |
| Render-smoke (full, incl. RTL) | `python -m pytest tests/render_smoke/test_joins_lab_render_smoke.py -q` | 35 passed, 1 skipped in 67.34s | PASS |
| Combined wave gate | `python -m pytest tests/test_joins_lab_i18n.py tests/render_smoke/ -q` | 41 passed, 1 skipped in 68.81s | PASS |

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| FND-07 | 121-01, 121-02, 121-03 | Entire Joins Lab UI is bilingual (EN/HE) with correct RTL layout | SATISFIED | REQUIREMENTS.md line 114: "FND-07 \| Phase 121 \| Complete". SC#1 (all tr() keys covered), SC#2 (RTL layout signed off), SC#3 (zero raw HE literals) all verified. |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | No TBD/FIXME/XXX/placeholder patterns found in Phase 121 modified files | — | — |

**Code-review findings (from 121-REVIEW.md), both resolved in commit b569d13b:**

- WR-01 (CI breaker): `import pytest` unused in tests/test_joins_lab_i18n.py → RESOLVED (removed, ruff clean)
- WR-02 (maintainability): duplicate 'Add as Join' key with definition-order coupling → RESOLVED (test_glossary_drift_values_pinned added, pins resolved values for 4 drift-prone keys)
- IN-01 (nit): missing trailing newline in genizah_translations.py → reported; negligible impact
- IN-02 (info): allowlist narrow theoretical false-negative → informational, no action required

---

### Human Verification Required

#### 1. SEED-010: Joins Lab Image-Resolution and Zoom Defect

**Test:** On `/joins-lab`, load an anchor with a CUDL or Oxford image, run a search, open Compare. Verify images load consistently in anchor pane, candidate grid, and both Compare panes. Verify zoom works when an image loads successfully. Test edge case: observe behavior when an image fails to load — zoom should degrade gracefully, not crash.

**Expected:** All provider images (CUDL/Oxford/NLI/Cambridge/Manchester/JTS) resolve and display consistently across anchor pane, candidate grid thumbnails, and Compare panes. Zoom is functional when image loads; a failed image load does not leave zoom in a permanently broken state.

**Why human:** This is a language-independent functional defect (not an i18n issue). It was found during the HE-UAT pass but was untestable at the time due to a concurrent NLI outage. The defect is logged as SEED-010 in docs/OPEN_ISSUES.md (P2). It is out of scope for this i18n phase but remains an open defect in the Joins Lab that must be resolved before v8.2.0 ships. Automated tests cannot verify image-provider resolution consistency or zoom state machines without a live IIIF endpoint.

---

### Gaps Summary

No blockers. All three success criteria are verified by code evidence:

- **SC#1 (string coverage):** Fully met. 17 missing keys added, all tr() literals in the 8 dedicated files resolve in TRANSLATIONS, badge strings covered via explicit BADGE_STRINGS list, entry-point keys wired in host files. The permanent CI guard (6 tests) locks this forever including a glossary-drift value-pin test.
- **SC#3 (static audit):** Fully met. AST scan confirms zero raw Hebrew literals outside tr() across all 8 FULL_SCAN_FILES. Guard proven to bite via sanity injection.
- **SC#2 (RTL layout):** Met via two halves — automated structural render-smoke (flex-row-reverse in pagination row and Compare nav bar), plus Hillel's signed-off live HE-UAT (all 8 surfaces PASS, 2026-06-21). Inline fixes committed during the UAT pass (bfc658fa, 1a8c9aca).

The one open item is SEED-010 (image-resolution + zoom), which is language-independent, explicitly deferred by the developer, logged in OPEN_ISSUES.md P2, and out of scope for this i18n phase. It does not block the i18n goal but is flagged for human follow-up before v8.2.0 ships.

---

_Verified: 2026-06-21T17:45:00Z_
_Verifier: Claude (gsd-verifier)_
