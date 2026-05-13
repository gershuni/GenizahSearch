---
phase: 87
plan: 03
type: execute
wave: 1
depends_on: [87-01]
files_modified:
  - web/components/text_editor.py
  - web/components/translation_report.py
  - web/pages/home.py
  - web/pages/settings.py
  - web/pages/search_results.py
autonomous: true
requirements:
  - FOUND-02
tags:
  - phase87
  - migration
  - safe-storage
  - leaf-files
  - components
  - pages
must_haves:
  truths:
    - "web/components/text_editor.py has 0 raw app.storage.user accesses (was 3 at lines 35, 50, 66)"
    - "web/components/translation_report.py has 0 raw app.storage.user accesses (was 1 at line 152)"
    - "web/pages/home.py has 0 raw app.storage.user accesses (was 2 at lines 40, 59)"
    - "web/pages/settings.py has 0 raw app.storage.user accesses (was 7 at lines 61, 76, 94, 109, 119, 134, 149)"
    - "web/pages/search_results.py has 0 raw app.storage.user accesses (was 3 at lines 483, 1577, 1635)"
    - "Each migrated file imports from web.safe_storage"
    - "All non-FOUND-04 tests pass"
  artifacts:
    - path: "web/components/text_editor.py"
      provides: "Migrated local edits store: get/set/delete now via safe_storage helpers"
      contains: "from web.safe_storage import"
    - path: "web/components/translation_report.py"
      provides: "Migrated user_id read in report saver"
      contains: "safe_user_get"
    - path: "web/pages/home.py"
      provides: "Migrated dismiss-banner writes (manual + auto-dismiss paths)"
      contains: "safe_user_set"
    - path: "web/pages/settings.py"
      provides: "Migrated 7 setting writes (theme, results_per_page, default_search_mode, default_gap, lab_mode_default, session_persistence_enabled, search_history_limit)"
      contains: "safe_user_set"
    - path: "web/pages/search_results.py"
      provides: "Migrated 3 show_translations reads"
      contains: "safe_user_get"
  key_links:
    - from: "All 5 migrated files"
      to: "web.safe_storage"
      via: "import statement at module top"
      pattern: "from web\\.safe_storage import"
---

<objective>
Migrate 5 leaf files (no cross-file state dependencies, no aliases, no allowlist conflicts) from raw `app.storage.user.*` access to `web.safe_storage` helpers. Total: 16 raw access sites migrated.

Purpose: These files are the lowest-risk migration targets per research R-09 — each has a tiny number of sites in a single module with no cross-file refactoring concerns. Doing them in one plan keeps the migration sweep focused and reduces context-switching cost for the executor.

Output: 5 files modified, each importing `safe_user_get`/`safe_user_set`/`safe_user_pop` and using them in place of raw `app.storage.user.*`. Zero new test failures.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@web/safe_storage.py
@web/components/text_editor.py
@web/components/translation_report.py
@web/pages/home.py
@web/pages/settings.py
@web/pages/search_results.py

<interfaces>
<!-- Existing safe_storage API — use these in the migrations. Plan 02 may already have landed get_session_uuid; this plan does NOT use it. -->

```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

# Read (returns default on any failure):
value = safe_user_get(key, default)  # default defaults to None

# Write (returns False on prune-race; usually ignored):
safe_user_set(key, value)

# Pop (returns default on any failure):
value = safe_user_pop(key, default)
```

Reference pattern for component files (already-migrated example):
```python
# From web/components/filter_panel.py (cca23db3 — DO NOT touch this file):
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

def persist_value(key, value):
    if safe_user_get('session_persistence_enabled', True):
        safe_user_set(key, value)
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/components/text_editor.py (3 sites) and web/components/translation_report.py (1 site)</name>
  <read_first>
    - web/components/filter_panel.py (REFERENCE — already migrated in cca23db3; see lines 229-231, 249, 300, 336 for the import + usage pattern; DO NOT modify this file)
    - web/components/text_editor.py (FULL FILE — verify lines 35, 50, 66 contain `app.storage.user.get(LOCAL_EDITS_KEY, {})`, `app.storage.user[LOCAL_EDITS_KEY] = edits`, and another `app.storage.user[LOCAL_EDITS_KEY] = edits`)
    - web/components/translation_report.py (FULL FILE — verify line 152 contains `app.storage.user.get('user_id', '')`)
    - web/safe_storage.py (so you know the helpers exist and their signatures)
  </read_first>
  <files>web/components/text_editor.py, web/components/translation_report.py</files>
  <action>
**File 1: `web/components/text_editor.py`** — has 3 raw accesses at lines 35, 50, 66 (all touching `LOCAL_EDITS_KEY`).

Step 1.1: Add import at module top (after the existing `from nicegui import app` line — find it via grep before editing):
```python
from web.safe_storage import safe_user_get, safe_user_set
```

Step 1.2: At line 35 in `get_local_edits()`, replace:
```python
return app.storage.user.get(LOCAL_EDITS_KEY, {})
```
with:
```python
return safe_user_get(LOCAL_EDITS_KEY, {})
```

Step 1.3: At line 50 in (the function setting the entire edits dict), replace:
```python
app.storage.user[LOCAL_EDITS_KEY] = edits
```
with:
```python
safe_user_set(LOCAL_EDITS_KEY, edits)
```

Step 1.4: At line 66 (inside the function that deletes a key then re-saves), replace:
```python
app.storage.user[LOCAL_EDITS_KEY] = edits
```
with:
```python
safe_user_set(LOCAL_EDITS_KEY, edits)
```

NOTE: Per Codex round 4 MEDIUM-2, the second write at line 66 is in an auto-save deferred callback path. The `safe_user_set` wrapper gracefully handles prune-mid-flight (returns False; logs at debug; no exception bubbles). This is the intentional Phase 87 fix for that specific Codex finding.

If the `from nicegui import app` import is now ONLY used for `app.storage.user`, remove the import. Otherwise leave it (other `app.*` accesses may exist — verify with `grep -c "\\bapp\\." web/components/text_editor.py` AFTER your changes; if returns 0 occurrences of `app.` outside imports, remove the import).

**File 2: `web/components/translation_report.py`** — has 1 raw access at line 152.

Step 2.1: At the top of the file, find the existing imports and add (after `from nicegui import app` if it exists, or at the top of the imports block):
```python
from web.safe_storage import safe_user_get
```

Step 2.2: At line 152, replace:
```python
user_id = app.storage.user.get('user_id', '')
```
with:
```python
user_id = safe_user_get('user_id', '')
```

If the `from nicegui import app` import is now ONLY used for `app.storage.user`, remove the import (run the same check as above).

After editing both files, verify:
```bash
grep -c "app\.storage\.user" web/components/text_editor.py     # expect 0
grep -c "app\.storage\.user" web/components/translation_report.py  # expect 0
grep -c "from web.safe_storage import" web/components/text_editor.py     # expect 1
grep -c "from web.safe_storage import" web/components/translation_report.py  # expect 1
pytest tests/ -x --timeout 60 -k "text_editor or translation_report" --tb=short  # may have 0 tests; that's OK
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/components/text_editor.py web/components/translation_report.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/components/text_editor.py` returns 0
    - `grep -c "app\.storage\.user" web/components/translation_report.py` returns 0
    - `grep -c "safe_user_get\|safe_user_set\|safe_user_pop" web/components/text_editor.py` returns at least 3 (1 import + 2 set + 1 get = 4, or imports counted differently — at minimum 3)
    - `grep -c "safe_user_get" web/components/translation_report.py` returns at least 2 (1 import + 1 use)
    - `grep -c "from web.safe_storage import" web/components/text_editor.py` returns 1
    - `grep -c "from web.safe_storage import" web/components/translation_report.py` returns 1
    - Both files parse as valid Python: `python -c "import ast; ast.parse(open('web/components/text_editor.py').read()); ast.parse(open('web/components/translation_report.py').read())"` exits 0
    - `ruff check web/components/text_editor.py web/components/translation_report.py` exits 0
    - No import of `app` from nicegui remains UNLESS the file uses `app.*` for something other than `app.storage.user` (verify with `grep -E "\\bapp\\.[a-z_]" {file}` after migration; if zero matches outside imports, the `from nicegui import app` line should be removed)
  </acceptance_criteria>
  <done>Both component files have 0 raw `app.storage.user` and use safe_storage helpers; files parse cleanly; ruff happy.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/pages/home.py (2 sites) and web/pages/search_results.py (3 sites)</name>
  <read_first>
    - web/pages/home.py (FULL FILE — verify lines 40, 59 are the only raw access sites; line 40 is inside `dismiss_banner()` callback; line 59 is inside `_auto_dismiss_ocr()` deferred callback wrapped in try/except)
    - web/pages/search_results.py (read AT MINIMUM lines 475-500, 1570-1590, 1625-1645 for context around the 3 raw reads of 'show_translations')
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/home.py" and "web/pages/search_results.py" sections)
    - web/components/filter_panel.py (REFERENCE — the cca23db3 migration pattern for value reads)
  </read_first>
  <files>web/pages/home.py, web/pages/search_results.py</files>
  <action>
**File 1: `web/pages/home.py`** — has 2 raw writes at lines 40 and 59 (both write `app.storage.user['ocr_disclaimer_dismissed'] = True`).

Step 1.1: At the top of the file (after `from nicegui import ...`), add OR extend the safe_storage import. Check first whether `safe_user_get` is already imported (the file has a `_safe_get` reference per research lines 29-30 — verify before editing). If `from web.safe_storage import safe_user_get as _safe_get` exists, change it to:
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```
If no safe_storage import exists yet, add:
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```

Step 1.2: At line 40 in `dismiss_banner()`, replace:
```python
app.storage.user['ocr_disclaimer_dismissed'] = True
```
with:
```python
_safe_set('ocr_disclaimer_dismissed', True)
```

Step 1.3: At line 59 in `_auto_dismiss_ocr()`, the current pattern is (per research):
```python
try:
    app.storage.user['ocr_disclaimer_dismissed'] = True
except Exception:
    pass
```

Replace this entire try/except block (LINES around 57-61 — verify by reading context first) with the single line:
```python
_safe_set('ocr_disclaimer_dismissed', True)
```
The `safe_user_set` wrapper absorbs the exception internally; the outer try/except wrapper around `ocr_banner.delete()` (per PATTERNS.md home.py section) should be preserved because it protects the `ocr_banner.delete()` call, NOT the storage write.

Carefully read the existing 57-65 block before editing to confirm the exact structure — the PATTERNS.md description says:
```
def _auto_dismiss_ocr():
    try:
        ocr_banner.delete()
    except Exception:
        return
    try:
        app.storage.user['ocr_disclaimer_dismissed'] = True
    except Exception:
        pass
```

Should become:
```
def _auto_dismiss_ocr():
    try:
        ocr_banner.delete()
    except Exception:
        return
    _safe_set('ocr_disclaimer_dismissed', True)
```

**File 2: `web/pages/search_results.py`** — has 3 raw reads of `'show_translations'` at lines 483, 1577, 1635.

Step 2.1: Add import at top of file (or extend an existing safe_storage import — check first with `grep "from web.safe_storage" web/pages/search_results.py`):
```python
from web.safe_storage import safe_user_get as _safe_get
```
If `_safe_get` is already imported, no change needed.

Step 2.2: At line 483, replace:
```python
_show_trans = app.storage.user.get('show_translations', False)
```
with:
```python
_show_trans = _safe_get('show_translations', False)
```

Step 2.3: At line 1577, replace:
```python
_show_type_trans = app.storage.user.get('show_translations', False)
```
with:
```python
_show_type_trans = _safe_get('show_translations', False)
```

Step 2.4: At line 1635, replace:
```python
_show_trans_adv = app.storage.user.get('show_translations', False)
```
with:
```python
_show_trans_adv = _safe_get('show_translations', False)
```

After editing both files, verify:
```bash
grep -c "app\.storage\.user" web/pages/home.py            # expect 0
grep -c "app\.storage\.user" web/pages/search_results.py  # expect 0
grep -c "_safe_set\|_safe_get" web/pages/home.py          # expect at least 3 (1 import + 2 sets)
grep -c "_safe_get" web/pages/search_results.py           # expect at least 4 (1 import + 3 reads)
python -c "import ast; ast.parse(open('web/pages/home.py').read()); ast.parse(open('web/pages/search_results.py').read())"
ruff check web/pages/home.py web/pages/search_results.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/home.py web/pages/search_results.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/home.py` returns 0
    - `grep -c "app\.storage\.user" web/pages/search_results.py` returns 0
    - `grep -c "_safe_set\|safe_user_set" web/pages/home.py` returns at least 2 (the 2 migrated write sites)
    - `grep -c "_safe_get\|safe_user_get" web/pages/search_results.py` returns at least 3 (the 3 migrated read sites)
    - `grep -c "from web.safe_storage import" web/pages/home.py` returns at least 1
    - `grep -c "from web.safe_storage import" web/pages/search_results.py` returns at least 1
    - Both files parse: `python -c "import ast; ast.parse(open('web/pages/home.py').read()); ast.parse(open('web/pages/search_results.py').read())"` exits 0
    - `ruff check web/pages/home.py web/pages/search_results.py` exits 0
    - `pytest tests/ -k "home or search_results" --tb=short` exits 0 if any such tests exist; otherwise no-op acceptable
  </acceptance_criteria>
  <done>home.py + search_results.py have 0 raw access; 5 sites migrated; files parse cleanly.</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/pages/settings.py (7 sites)</name>
  <read_first>
    - web/pages/settings.py (FULL FILE — read all lines 55-160; verify the 7 raw writes at lines 61, 76, 94, 109, 119, 134, 149 are inside event callbacks for: theme, results_per_page, default_search_mode, default_gap, lab_mode_default, session_persistence_enabled, search_history_limit)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/settings.py" section — confirms reads already migrated via _safe_get at line 22; only writes need migration)
  </read_first>
  <files>web/pages/settings.py</files>
  <action>
**File: `web/pages/settings.py`** — has 7 raw write sites, all inside event callbacks for settings dropdowns/switches.

Step 1: Find the existing safe_storage import (should be at top of file, something like `from web.safe_storage import safe_user_get as _safe_get`). Extend to include `safe_user_set`:
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```

Step 2: Migrate the 7 write sites. Read each context (a few lines around each line number) to understand which callback owns the write, then replace:

| Line | Before | After |
|------|--------|-------|
| 61 | `app.storage.user['theme'] = theme` | `_safe_set('theme', theme)` |
| 76 | `app.storage.user['results_per_page'] = rpp_select.value` | `_safe_set('results_per_page', rpp_select.value)` |
| 94 | `app.storage.user['default_search_mode'] = mode_select.value` | `_safe_set('default_search_mode', mode_select.value)` |
| 109 | `app.storage.user['default_gap'] = int(gap_input.value) if gap_input.value else 0` | `_safe_set('default_gap', int(gap_input.value) if gap_input.value else 0)` |
| 119 | `app.storage.user['lab_mode_default'] = lab_switch.value` | `_safe_set('lab_mode_default', lab_switch.value)` |
| 134 | `app.storage.user['session_persistence_enabled'] = persist_switch.value` | `_safe_set('session_persistence_enabled', persist_switch.value)` |
| 149 | `app.storage.user['search_history_limit'] = int(history_limit_input.value) if history_limit_input.value else 20` | `_safe_set('search_history_limit', int(history_limit_input.value) if history_limit_input.value else 20)` |

NOTE: Each callback may have surrounding code (notifications, UI updates) — preserve all of that. Only the single `app.storage.user[KEY] = VALUE` line changes per callback.

After editing, verify the `from nicegui import app` import. If `app.*` is no longer used outside `app.storage.user` (which is now zero), check whether removing the import is appropriate:
```bash
grep -E "\bapp\.[a-z_]" web/pages/settings.py | grep -v "app.storage.user"
```
If this returns 0 matches AND `from nicegui import app` was only imported for the storage access, remove that import line. (NiceGUI's `ui.*` is imported separately; do not remove `from nicegui import ui` if present.)

Final verification:
```bash
grep -c "app\.storage\.user" web/pages/settings.py            # expect 0
grep -c "_safe_set" web/pages/settings.py                     # expect 8 (1 import + 7 writes)
grep -c "_safe_get" web/pages/settings.py                     # expect 1+ (existing reads still use it)
python -c "import ast; ast.parse(open('web/pages/settings.py').read())"
ruff check web/pages/settings.py
```
  </action>
  <verify>
    <automated>grep -c "app\.storage\.user" web/pages/settings.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "app\.storage\.user" web/pages/settings.py` returns 0
    - `grep -c "_safe_set\|safe_user_set" web/pages/settings.py` returns at least 7 (the 7 migrated write sites; 8 if import line is counted)
    - `grep -c "_safe_get\|safe_user_get" web/pages/settings.py` returns at least 1 (existing reads preserved)
    - `grep -c "from web.safe_storage import" web/pages/settings.py` returns 1
    - File parses: `python -c "import ast; ast.parse(open('web/pages/settings.py').read())"` exits 0
    - `ruff check web/pages/settings.py` exits 0
    - All 7 specific settings keys still referenced (no accidental rename): `grep -c "'theme'" web/pages/settings.py` ≥ 1, `grep -c "'results_per_page'" web/pages/settings.py` ≥ 1, `grep -c "'default_search_mode'" web/pages/settings.py` ≥ 1, `grep -c "'default_gap'" web/pages/settings.py` ≥ 1, `grep -c "'lab_mode_default'" web/pages/settings.py` ≥ 1, `grep -c "'session_persistence_enabled'" web/pages/settings.py` ≥ 1, `grep -c "'search_history_limit'" web/pages/settings.py` ≥ 1
    - `pytest tests/ -k settings --tb=short` exits 0 if any settings tests exist; otherwise no-op
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 tests remain green; this plan should not affect them)
  </acceptance_criteria>
  <done>settings.py has 0 raw access; all 7 setting keys migrated; file parses cleanly; ruff happy.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| UI event callbacks → safe_storage helpers | Migration target: callbacks now write through wrapped helpers instead of raw access |
| Auto-save deferred callbacks → safe_storage helpers | text_editor.py:66 specifically — Codex round 4 MEDIUM-2 site; safe_user_set absorbs prune-race AssertionError |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Lint scanner pattern matching — N/A (no allowlist entries changed) | accept | Plan 03 files are all migrated (not allowlisted); they appear in the lint scan's negative space |
| T-87-05 | Information disclosure | Alias resolution — N/A (these files use `from nicegui import app`, no alias) | accept | Standard `app` alias; no special handling needed |
| — | Tampering | Auto-save mid-prune race | mitigate | `safe_user_set` returns False on AssertionError; caller (text_editor:66) ignores return value; no exception bubbles to UI |

This plan does not directly mitigate T-87-01/02/03 — those are Plan 02's concern. No new security surface introduced.
</threat_model>

<verification>
After all 3 tasks:

```bash
# Verify zero raw access in all 5 files
for f in web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py; do
  count=$(grep -c "app\.storage\.user" "$f")
  echo "$f: $count raw access (expect 0)"
done

# Verify all 5 files import safe_storage helpers
for f in web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py; do
  count=$(grep -c "from web.safe_storage import" "$f")
  echo "$f: $count safe_storage imports (expect 1)"
done

# Verify all 5 files parse and pass ruff
python -c "
import ast
files = ['web/components/text_editor.py', 'web/components/translation_report.py', 'web/pages/home.py', 'web/pages/settings.py', 'web/pages/search_results.py']
for f in files:
    ast.parse(open(f).read(), filename=f)
print('All 5 files parse OK')
"
ruff check web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py

# Verify Plan 02 tests still green
pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Verify no regression in any existing tests touching these files
pytest tests/ --tb=short -q 2>&1 | tail -20
```
</verification>

<success_criteria>
1. `grep -rc "app\.storage\.user" web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py` returns 0 for every file (total raw access count: 0)
2. Each of the 5 files has at least 1 `from web.safe_storage import` line
3. Total of 16 raw access sites migrated: 3 (text_editor) + 1 (translation_report) + 2 (home) + 7 (settings) + 3 (search_results)
4. `ruff check` clean on all 5 files
5. All 5 files parse as valid Python
6. `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariant preserved)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-03-SUMMARY.md` summarizing:
- 5 files migrated (with site counts: text_editor=3, translation_report=1, home=2, settings=7, search_results=3, total=16)
- Each file's raw access count before/after (16 → 0)
- Any `from nicegui import app` imports removed (note which files had no other `app.*` usage)
- Verification: `pytest tests/test_safe_storage.py tests/test_session_uuid.py` still 11 passed
</output>
