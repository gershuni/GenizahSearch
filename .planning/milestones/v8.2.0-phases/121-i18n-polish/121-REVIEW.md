---
phase: 121-i18n-polish
reviewed: 2026-06-21T14:26:34Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - genizah_translations.py
  - web/pages/joins_lab.py
  - web/components/joins_builder.py
  - tests/test_joins_lab_i18n.py
  - tests/render_smoke/test_joins_lab_render_smoke.py
findings:
  critical: 0
  warning: 2
  info: 2
  total: 4
status: issues_found
---

# Phase 121: Code Review Report

**Reviewed:** 2026-06-21T14:26:34Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

Phase 121 ("i18n-polish", FND-07) closes a Hebrew-translation gap for the web Joins Lab,
fixes a glossary drift (חיבור → צירוף), wraps the XLSX `Candidates` sheet name in `tr()`,
adds a permanent AST-based i18n CI guard (`tests/test_joins_lab_i18n.py`), adds RTL
render-smoke assertions, and swaps one builder icon (`tune` → `settings`).

The functional substance of the change is **sound**. I verified by direct execution:

- All 23 new/edited Hebrew values are valid Hebrew with no garbled characters, no stray
  bidi marks, and no reversed text.
- `tr()` is language-gated (`web/translations.py:41` — returns the English key verbatim
  when `_current_lang == 'en'`), so EN users are structurally unaffected.
- Runtime placeholders are preserved: `{a}`/`{b}` survive the `.replace()` chain at
  `joins_lab.py:1685-1687` (verified — no leftover braces after substitution); the literal
  `N` token in `"Mark N selected as:"` survives `.replace("N", ...)` (the HE value has no
  other Latin `N`); both U+2026 ellipses are byte-for-byte preserved.
- The glossary-drift winner resolves correctly: `"Add as Join"` has three definitions
  (lines 3399/3977/4271) but the **last** one (4271, the Phase-121-edited line) wins and is
  now `הוסף כצירוף`.
- The AST guard is conservative (catches Hebrew in f-strings, non-`tr()` calls, and
  concatenations); the D-04 allowlist matches `joins_builder.py:344-351` byte-for-byte.
- The `tune → settings` icon change is localized to the per-word modifier button; `settings`
  is a valid Material icon, no test asserts on it, and other `tune` usages elsewhere are
  untouched.
- The new XLSX sheet title `tr('Candidates')` → `מועמדים` (7 chars, no forbidden
  characters) is accepted by openpyxl.
- All 6 tests in `test_joins_lab_i18n.py` pass and the new RTL render-smoke test passes.

Two issues should be fixed: an unused-import lint error that will break CI, and a noted
(pre-existing) duplicate-key smell around the glossary-drift fix that the change relied on.

## Warnings

### WR-01: Unused `import pytest` breaks CI (F401)

**File:** `tests/test_joins_lab_i18n.py:27`
**Issue:** `import pytest` is never used in the file — no `pytest.skip`, `pytest.mark`,
`pytest.raises`, `pytest.fixture`, or `pytest.param` references anywhere (verified by grep
for `pytest.`). `ruff check` reports this as `F401 [*] pytest imported but unused`. Per the
project's documented release convention (`feedback_pre_release_must_run_ruff` —
"v7.12.0 CI failed on 18 F401 errors"), CI runs `python -m ruff check .` over the whole repo
and **fails on F401**. This new test file would break the next CI run / release pre-flight.
**Fix:**
```python
# tests/test_joins_lab_i18n.py — remove line 27
import ast
import pathlib
import re
# (delete the line:  import pytest)
```
Confirmed with `python -m ruff check`: this is the only ruff finding across all 5 Phase-121
files; removing the import clears it.

### WR-02: `"Add as Join"` has three duplicate dict entries; the glossary fix depends on definition order

**File:** `genizah_translations.py:3399, 3977, 4271`
**Issue:** `"Add as Join"` is defined three times in `TRANSLATIONS`. In the base revision the
values were `הוסף כצירוף` (3399), `הוסף כצירוף` (3977), and `הוסף כחיבור` (4271 — the OLD
glossary term, which **won** because it is the last `TRANSLATIONS.update()` write). Phase 121
fixed the drift by editing **only line 4271** to `הוסף כצירוף`. This is correct *today* —
the last write still wins and all three now agree — but the correctness is silently coupled
to definition order. If a future edit changes 3399 or 3977 without touching 4271 (or
re-orders the update blocks), the displayed value will diverge from the edited line with no
test catching it (the guard only asserts the key *resolves*, not that duplicates agree). This
is a maintainability/robustness smell, not a current functional defect. Note this is one of
160 duplicate keys already present in the file, so it is a pre-existing condition the phase
inherited rather than introduced — but the phase's fix relies on it.
**Fix:** De-duplicate `"Add as Join"` to a single authoritative entry (keep the
Phase-121 `TRANSLATIONS.update()` one at 4271, delete 3399 and 3977), or — lower effort —
extend the i18n guard with a duplicate-key detector that fails when any key has conflicting
values across dict literals / `update()` blocks:
```python
def test_no_conflicting_duplicate_translation_keys():
    import ast
    tree = ast.parse(open('genizah_translations.py', encoding='utf-8').read())
    seen = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Dict):
            for k, v in zip(n.keys, n.values):
                if isinstance(k, ast.Constant) and isinstance(k.value, str) \
                   and isinstance(v, ast.Constant):
                    if k.value in seen and seen[k.value] != v.value:
                        raise AssertionError(
                            f"Conflicting duplicate key {k.value!r}: "
                            f"{seen[k.value]!r} vs {v.value!r}")
                    seen[k.value] = v.value
```

## Info

### IN-01: `genizah_translations.py` ends without a trailing newline

**File:** `genizah_translations.py:4421` (end of file)
**Issue:** The appended Phase-121 `TRANSLATIONS.update({...})` block ends with `})` and **no
trailing newline** (`git diff` shows `\ No newline at end of file`; confirmed the raw bytes
end with `})`). This is a minor POSIX/style nit — it produces a noisy diff for the next edit
to the file and trips some editors/linters. Ruff did not flag it under the current config.
**Fix:** Add a single trailing newline after the closing `})`.

### IN-02: D-04 allowlist matches by exact value — a narrow theoretical false-negative

**File:** `tests/test_joins_lab_i18n.py:80-89` (`HEBREW_LITERAL_ALLOWLIST`)
**Issue:** The guard allowlists the 8 operator-example literals by exact string value
(e.g. `'#מילה'`). This is correct and byte-for-byte matches `joins_builder.py:344-351`
(verified). The narrow caveat: if a developer later introduced a genuinely user-facing raw
Hebrew literal whose value happened to equal one of these 8 distinctive operator strings, the
guard would silently allow it. The collision risk is negligible given the distinctiveness of
the operator tokens, so this is informational only — no action required unless the allowlist
grows to include common/short Hebrew words.
**Fix:** If the allowlist ever expands, prefer scoping exclusions to a specific file +
line range rather than a global value-set, to avoid value-collision false-negatives.

---

_Reviewed: 2026-06-21T14:26:34Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
