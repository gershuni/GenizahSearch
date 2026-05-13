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
    - "test_no_raw_storage_access scanner reports zero violations for these 5 files (verified via pytest, not grep — per M1)"
    - "Existing defensive try/except blocks that catch JSON/value-decoding errors are PRESERVED (M3); only AssertionError-only wrappers are collapsed"
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

**REVISION (M1, M3, M4 from 87-REVIEWS.md):**
- **M1:** All `<acceptance_criteria>` use `pytest tests/test_no_raw_storage_access.py` invocations instead of `grep -c` gates. The AST scanner is authoritative; grep counts comments and docstrings, producing false failures.
- **M3:** Adds explicit per-task instruction to PRESERVE defensive try/except blocks that catch JSON parsing / malformed-data exceptions. Only collapse wrappers whose ONLY caught exception is `AssertionError` from storage prune-mid-flight.
- **M4:** All shell snippets use Windows-safe Python one-liners or `ruff`/`pytest` invocations. No `/tmp`, `grep | wc -l`, `tail`, `sha256sum`, or POSIX-only commands.

Purpose: These files are the lowest-risk migration targets per research R-09 — each has a tiny number of sites in a single module with no cross-file refactoring concerns. Doing them in one plan keeps the migration sweep focused and reduces context-switching cost for the executor.

Output: 5 files modified, each importing `safe_user_get`/`safe_user_set`/`safe_user_pop` and using them in place of raw `app.storage.user.*`. Zero new test failures.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@web/safe_storage.py
@web/components/text_editor.py
@web/components/translation_report.py
@web/pages/home.py
@web/pages/settings.py
@web/pages/search_results.py

<interfaces>
<!-- Existing safe_storage API — use these in the migrations. Plan 02 has already landed get_session_uuid; this plan does NOT use it. -->

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

<defensive_wrapper_preservation>
<!-- M3 from 87-REVIEWS.md: SOME existing try/except blocks catch more than just
     AssertionError from storage prune-mid-flight. They may also catch:
       - json.JSONDecodeError (when persisted value is corrupt)
       - KeyError / TypeError / ValueError (when value shape differs from expected)
     For these blocks, REPLACE the inner storage call with safe_user_get/set/pop
     but KEEP THE WRAPPER. The wrapper is doing two jobs: (1) absorb storage
     prune AssertionError, (2) absorb downstream parsing failure. safe_user_get
     handles job (1); the wrapper still handles job (2).

     RULE: collapse the wrapper ONLY if the except clause's caught types are
     exactly one of:
       - `except AssertionError:`
       - `except (AssertionError, Exception):` (assuming the Exception was for
         the same prune-race-related raise paths — confirm by reading what the
         except body does; if it just returns a default, it's safe to collapse)

     KEEP the wrapper if the except clause catches any of:
       - `except json.JSONDecodeError:`
       - `except (json.JSONDecodeError, ValueError):`
       - `except (TypeError, KeyError, ValueError, AttributeError):`
       - Any other type combo where at least one type is NOT AssertionError or
         generic Exception.

     When in doubt: KEEP the wrapper. False-positive preservation is a no-op;
     false-negative collapse can corrupt session state.
-->
</defensive_wrapper_preservation>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Migrate web/components/text_editor.py (3 sites) and web/components/translation_report.py (1 site)</name>
  <read_first>
    - web/components/filter_panel.py (REFERENCE — already migrated in cca23db3; see lines 229-231, 249, 300, 336 for the import + usage pattern; DO NOT modify this file)
    - web/components/text_editor.py (FULL FILE — verify lines 35, 50, 66 contain `app.storage.user.get(LOCAL_EDITS_KEY, {})`, `app.storage.user[LOCAL_EDITS_KEY] = edits`, and another `app.storage.user[LOCAL_EDITS_KEY] = edits`. ALSO inspect the surrounding try/except: per M3, determine whether the wrapper catches AssertionError-only or also catches parsing failures.)
    - web/components/translation_report.py (FULL FILE — verify line 152 contains `app.storage.user.get('user_id', '')`; check for surrounding try/except)
    - web/safe_storage.py (so you know the helpers exist and their signatures)
    - The `<defensive_wrapper_preservation>` block in this plan's `<context>` section for M3 guidance
  </read_first>
  <files>web/components/text_editor.py, web/components/translation_report.py</files>
  <action>
**File 1: `web/components/text_editor.py`** — has 3 raw accesses at lines 35, 50, 66 (all touching `LOCAL_EDITS_KEY`).

**M3 STEP — Defensive wrapper audit (REQUIRED before editing):**

Read the surrounding 10 lines around each of lines 35, 50, 66. For each site, identify the enclosing try/except (if any). Classify the wrapper:

- **Class A (collapse):** `except AssertionError:` or `except (AssertionError, Exception):` where the body is just `pass`, `return default`, or `logger.debug + return default`.
- **Class B (preserve):** `except (json.JSONDecodeError, ...):` or any except clause that catches type-conversion / parsing errors.

If site 35 is Class B (e.g., the storage returns a dict and the caller does `edits = ...; for k, v in edits.items()` inside the try, catching TypeError if edits is None), keep the wrapper but replace the raw `app.storage.user.get(...)` with `safe_user_get(...)`. If it is Class A, collapse the wrapper.

**Step 1.1: Add import at module top.**

Locate the existing `from nicegui import app` line (use Python regex check: `python -c "import re; print(re.search(r'from nicegui import app', open('web/components/text_editor.py').read()))"`). Add the safe_storage import on the NEXT line (or below other `from web.*` imports if any exist):

```python
from web.safe_storage import safe_user_get, safe_user_set
```

**Step 1.2: Migrate line 35** (read site).

Current code (verify by reading the actual file first):
```python
return app.storage.user.get(LOCAL_EDITS_KEY, {})
```

Replace with:
```python
return safe_user_get(LOCAL_EDITS_KEY, {})
```

If the surrounding code has a Class B wrapper (M3), keep it. Replace ONLY the storage call.

**Step 1.3: Migrate line 50** (write).

Current:
```python
app.storage.user[LOCAL_EDITS_KEY] = edits
```

Replace with:
```python
safe_user_set(LOCAL_EDITS_KEY, edits)
```

**Step 1.4: Migrate line 66** (write inside auto-save deferred callback — Codex round 4 MEDIUM-2 site).

Current:
```python
app.storage.user[LOCAL_EDITS_KEY] = edits
```

Replace with:
```python
safe_user_set(LOCAL_EDITS_KEY, edits)
```

NOTE: Per Codex round 4 MEDIUM-2, the second write at line 66 is in an auto-save deferred callback path. The `safe_user_set` wrapper gracefully handles prune-mid-flight (returns False; logs at debug; no exception bubbles). This is the intentional Phase 87 fix for that specific Codex finding.

**Step 1.5: Audit whether `from nicegui import app` is still needed.**

Check Windows-safely:
```
python -c "import re; src = open('web/components/text_editor.py').read(); non_storage_uses = re.findall(r'\\bapp\\.[a-zA-Z_]', src); print('non-app.storage.user app.* uses:', [u for u in non_storage_uses if not u.startswith('app.storage.user')])"
```

If output is `[]` AND `from nicegui import app` is not needed for type annotations or other purposes, you may remove the import line. If unsure, leave the import — an unused-import ruff warning is fixable later, but accidentally removing an import that IS used would break imports.

**File 2: `web/components/translation_report.py`** — has 1 raw access at line 152.

**M3 STEP:** Read 10 lines around line 152. The site reads `user_id` (a primitive string). The surrounding code likely uses `user_id` directly without parsing — unlikely to have a Class B wrapper. Verify by reading; if wrapper is Class A or absent, proceed; if Class B, preserve.

**Step 2.1: Add import.**

```python
from web.safe_storage import safe_user_get
```

**Step 2.2: Migrate line 152.**

Current:
```python
user_id = app.storage.user.get('user_id', '')
```

Replace with:
```python
user_id = safe_user_get('user_id', '')
```

**Step 2.3: Audit `from nicegui import app` removability (same approach as Step 1.5).**

**Final verification (Windows-safe):**
```
python -c "import ast; ast.parse(open('web/components/text_editor.py').read()); ast.parse(open('web/components/translation_report.py').read()); print('both parse OK')"
ruff check web/components/text_editor.py web/components/translation_report.py
python -m pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x
```
  </action>
  <verify>
    <automated>python -c "import pathlib; print('text_editor file_violations:', len([v for v in __import__('tests.test_no_raw_storage_access', fromlist=['_scan_file'])._scan_file(pathlib.Path('web/components/text_editor.py'), pathlib.Path('web/components/text_editor.py').read_text(encoding='utf-8'))]))"</automated>
  </verify>
  <acceptance_criteria>
    - Both files parse: `python -c "import ast; ast.parse(open('web/components/text_editor.py').read()); ast.parse(open('web/components/translation_report.py').read())"` exits 0
    - `ruff check web/components/text_editor.py web/components/translation_report.py` exits 0
    - The AST scanner reports zero violations for these 2 files. Verify via: `python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file, _find_app_aliases; import ast; [print(p, len(_scan_file(pathlib.Path(p), pathlib.Path(p).read_text(encoding='utf-8')))) for p in ['web/components/text_editor.py', 'web/components/translation_report.py']]"` prints two lines each ending in `0`
    - Both files import safe_storage: `python -c "import re; assert re.search(r'from web\\.safe_storage import', open('web/components/text_editor.py').read()); assert re.search(r'from web\\.safe_storage import', open('web/components/translation_report.py').read()); print('OK')"` prints `OK`
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariants preserved)
    - M3 — defensive wrappers preserved: any try/except in these files that previously caught `json.JSONDecodeError`, `KeyError`, `ValueError`, or `TypeError` is still present (manual verification noted in SUMMARY)
  </acceptance_criteria>
  <done>Both component files have 0 raw `app.storage.user` AST nodes (verified by scanner); files parse cleanly; ruff happy; defensive wrappers preserved per M3.</done>
</task>

<task type="auto">
  <name>Task 2: Migrate web/pages/home.py (2 sites) and web/pages/search_results.py (3 sites)</name>
  <read_first>
    - web/pages/home.py (FULL FILE — verify lines 40, 59 are the only raw access sites; line 40 is inside `dismiss_banner()` callback; line 59 is inside `_auto_dismiss_ocr()` deferred callback wrapped in try/except. Apply M3 audit to the line-59 wrapper.)
    - web/pages/search_results.py (read AT MINIMUM lines 475-500, 1570-1590, 1625-1645 for context around the 3 raw reads of 'show_translations')
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md (read "web/pages/home.py" and "web/pages/search_results.py" sections)
    - web/components/filter_panel.py (REFERENCE — the cca23db3 migration pattern for value reads)
    - The `<defensive_wrapper_preservation>` block in this plan
  </read_first>
  <files>web/pages/home.py, web/pages/search_results.py</files>
  <action>
**File 1: `web/pages/home.py`** — has 2 raw writes at lines 40 and 59.

**Step 1.1: Audit/add safe_storage import.**

Check first:
```
python -c "import re; print(re.search(r'from web\\.safe_storage import', open('web/pages/home.py').read()))"
```

If the import does not exist, add it after the other web.* imports:
```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```

If a partial import already exists (e.g., only `safe_user_get as _safe_get`), extend it to include `safe_user_set as _safe_set`.

**Step 1.2: Migrate line 40 in `dismiss_banner()`.**

Current:
```python
app.storage.user['ocr_disclaimer_dismissed'] = True
```

Replace with:
```python
_safe_set('ocr_disclaimer_dismissed', True)
```

**Step 1.3: Migrate line 59 in `_auto_dismiss_ocr()` — APPLY M3 wrapper audit.**

Read 10 lines around line 59. The existing pattern per research:
```python
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

M3 classification of the SECOND try/except (around the storage write):
- It catches `except Exception:` (broad), but the only thing inside the try is the storage write. The wrapper is doing ONLY the prune-race absorption job.
- Class A → safe to collapse.

The FIRST try/except (around `ocr_banner.delete()`) catches a non-storage failure (element-already-removed). KEEP IT.

Replace the entire block with:
```python
def _auto_dismiss_ocr():
    try:
        ocr_banner.delete()
    except Exception:
        return
    _safe_set('ocr_disclaimer_dismissed', True)
```

The `_safe_set` wrapper absorbs AssertionError internally; no outer try/except needed for the write.

**File 2: `web/pages/search_results.py`** — has 3 raw reads at lines 483, 1577, 1635.

**Step 2.1: Add or extend the safe_storage import.**

Check:
```
python -c "import re; print(re.search(r'from web\\.safe_storage import', open('web/pages/search_results.py').read()))"
```

Add or extend:
```python
from web.safe_storage import safe_user_get as _safe_get
```

**Step 2.2-2.4: Migrate the 3 reads.**

All three sites have the same pattern: a read of `'show_translations'` with a `False` default.

| Line | Before | After |
|------|--------|-------|
| 483 | `_show_trans = app.storage.user.get('show_translations', False)` | `_show_trans = _safe_get('show_translations', False)` |
| 1577 | `_show_type_trans = app.storage.user.get('show_translations', False)` | `_show_type_trans = _safe_get('show_translations', False)` |
| 1635 | `_show_trans_adv = app.storage.user.get('show_translations', False)` | `_show_trans_adv = _safe_get('show_translations', False)` |

M3 audit: these are bare reads of a primitive boolean. Unlikely to have wrappers. If any do (verify by reading), keep Class B wrappers (e.g., a wrapper catching ValueError if `_show_trans` were used as `int(_show_trans)` later — unlikely but possible).

**Final verification (Windows-safe):**
```
python -c "import ast; ast.parse(open('web/pages/home.py').read()); ast.parse(open('web/pages/search_results.py').read()); print('both parse OK')"
ruff check web/pages/home.py web/pages/search_results.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; [print(p, len(_scan_file(pathlib.Path(p), pathlib.Path(p).read_text(encoding='utf-8')))) for p in ['web/pages/home.py', 'web/pages/search_results.py']]"
```
Both files should report `0` violations.
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; total = sum(len(_scan_file(pathlib.Path(p), pathlib.Path(p).read_text(encoding='utf-8'))) for p in ['web/pages/home.py', 'web/pages/search_results.py']); assert total == 0, f'{total} violations remain'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - Both files parse: `python -c "import ast; ast.parse(open('web/pages/home.py').read()); ast.parse(open('web/pages/search_results.py').read())"` exits 0
    - `ruff check web/pages/home.py web/pages/search_results.py` exits 0
    - AST scanner reports 0 violations for both files (the `python -c` invocation in `<verify>` exits 0 and prints `OK`)
    - Both files import safe_storage: `python -c "import re; assert re.search(r'from web\\.safe_storage import', open('web/pages/home.py').read()); assert re.search(r'from web\\.safe_storage import', open('web/pages/search_results.py').read()); print('OK')"` prints `OK`
    - M3: The outer try/except around `ocr_banner.delete()` in home.py:_auto_dismiss_ocr() is PRESERVED (verify by reading the function body and confirming the try: ocr_banner.delete() except: return idiom is intact)
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariants preserved)
  </acceptance_criteria>
  <done>home.py + search_results.py have 0 AST violations; 5 sites migrated; files parse cleanly; M3 preserved the non-storage wrapper in home.py.</done>
</task>

<task type="auto">
  <name>Task 3: Migrate web/pages/settings.py (7 sites)</name>
  <read_first>
    - web/pages/settings.py (FULL FILE — read all lines 55-160; verify the 7 raw writes at lines 61, 76, 94, 109, 119, 134, 149 are inside event callbacks for: theme, results_per_page, default_search_mode, default_gap, lab_mode_default, session_persistence_enabled, search_history_limit. Apply M3 audit to each callback.)
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md ("web/pages/settings.py" section — confirms reads already migrated via _safe_get at line 22; only writes need migration)
  </read_first>
  <files>web/pages/settings.py</files>
  <action>
**File: `web/pages/settings.py`** — has 7 raw write sites, all inside event callbacks for settings dropdowns/switches.

**Step 1: Extend the existing safe_storage import.**

Per research, the file already has `from web.safe_storage import safe_user_get as _safe_get` (verify with the Python regex check pattern used in prior tasks). Extend to include `safe_user_set`:

```python
from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
```

If the existing import is on a single line, use Edit to replace it. If it spans multiple lines (parenthesized), add `, safe_user_set as _safe_set` to the names list.

**Step 2: Migrate the 7 write sites.**

For EACH line, read 5 lines of context. Apply M3 audit:
- All 7 sites are inside event handlers triggered by UI controls. Each handler typically: reads control value, writes to storage, calls a notify or refresh.
- Wrappers (if any) are Class A — they catch `Exception` and just `pass`. Safe to collapse.

| Line | Before | After |
|------|--------|-------|
| 61 | `app.storage.user['theme'] = theme` | `_safe_set('theme', theme)` |
| 76 | `app.storage.user['results_per_page'] = rpp_select.value` | `_safe_set('results_per_page', rpp_select.value)` |
| 94 | `app.storage.user['default_search_mode'] = mode_select.value` | `_safe_set('default_search_mode', mode_select.value)` |
| 109 | `app.storage.user['default_gap'] = int(gap_input.value) if gap_input.value else 0` | `_safe_set('default_gap', int(gap_input.value) if gap_input.value else 0)` |
| 119 | `app.storage.user['lab_mode_default'] = lab_switch.value` | `_safe_set('lab_mode_default', lab_switch.value)` |
| 134 | `app.storage.user['session_persistence_enabled'] = persist_switch.value` | `_safe_set('session_persistence_enabled', persist_switch.value)` |
| 149 | `app.storage.user['search_history_limit'] = int(history_limit_input.value) if history_limit_input.value else 20` | `_safe_set('search_history_limit', int(history_limit_input.value) if history_limit_input.value else 20)` |

NOTE on line 109 and 149: these contain `int(...)` conversion which could in principle raise ValueError. The CURRENT inline expression `int(gap_input.value) if gap_input.value else 0` already guards against empty string but does NOT guard against `int('abc')`. If the original code wraps this in a try/except ValueError, that wrapper is **Class B (preserve)** because the int conversion is the failure mode. If there is no wrapper, the migration is a straightforward substitution.

NOTE: Each callback may have surrounding code (notifications, UI updates) — preserve all of that. Only the single `app.storage.user[KEY] = VALUE` line changes per callback.

**Step 3: Verify the `from nicegui import app` removability.**

After all 7 migrations, check whether `app.*` is still used outside `app.storage.user` (which is now zero in this file):
```
python -c "import re; src = open('web/pages/settings.py').read(); non_storage = [m for m in re.findall(r'\\bapp\\.[a-zA-Z_]+', src) if not m.startswith('app.storage')]; print('non-storage app.* uses:', non_storage)"
```

If the output is `[]`, you MAY remove `from nicegui import app` (verify it doesn't appear in `from nicegui import app, ui` — if so, remove only `app,` and keep `ui`). Otherwise leave the import.

**Final verification (Windows-safe):**
```
python -c "import ast; ast.parse(open('web/pages/settings.py').read()); print('parses OK')"
ruff check web/pages/settings.py
python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; print('settings.py violations:', len(_scan_file(pathlib.Path('web/pages/settings.py'), pathlib.Path('web/pages/settings.py').read_text(encoding='utf-8'))))"
# Expected: 0
```

All 7 storage keys must still appear in the migrated file (sanity check via Python regex; each key should be referenced via `_safe_set('key', ...)`).
  </action>
  <verify>
    <automated>python -c "import sys, pathlib; sys.path.insert(0, '.'); from tests.test_no_raw_storage_access import _scan_file; v = _scan_file(pathlib.Path('web/pages/settings.py'), pathlib.Path('web/pages/settings.py').read_text(encoding='utf-8')); assert len(v) == 0, v; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File parses: `python -c "import ast; ast.parse(open('web/pages/settings.py').read())"` exits 0
    - `ruff check web/pages/settings.py` exits 0
    - AST scanner reports 0 violations for settings.py (verified by `<verify>` block)
    - safe_storage import present: `python -c "import re; src = open('web/pages/settings.py').read(); assert re.search(r'from web\\.safe_storage import.*safe_user_set', src); print('OK')"` prints `OK`
    - All 7 settings keys still referenced in migrated file (string literals preserved): `python -c "import re; src = open('web/pages/settings.py').read(); keys = ['theme', 'results_per_page', 'default_search_mode', 'default_gap', 'lab_mode_default', 'session_persistence_enabled', 'search_history_limit']; missing = [k for k in keys if f\"'{k}'\" not in src]; assert not missing, missing; print('OK')"` prints `OK`
    - M3: any try/except ValueError around int() conversions is PRESERVED (manual verification noted in SUMMARY)
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariants preserved)
  </acceptance_criteria>
  <done>settings.py has 0 AST violations; all 7 setting keys migrated; file parses cleanly; ruff happy; M3 preserved any int()-conversion wrappers.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| UI event callbacks -> safe_storage helpers | Migration target: callbacks now write through wrapped helpers instead of raw access |
| Auto-save deferred callbacks -> safe_storage helpers | text_editor.py:66 specifically — Codex round 4 MEDIUM-2 site; safe_user_set absorbs prune-race AssertionError |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Lint scanner pattern matching — N/A (no allowlist entries changed) | accept | Plan 03 files are all migrated (not allowlisted); they appear in the lint scan's negative space |
| T-87-05 | Information disclosure | Alias resolution — N/A (these files use `from nicegui import app`, no alias) | accept | Standard `app` alias; no special handling needed |
| -- | Tampering | Auto-save mid-prune race | mitigate | `safe_user_set` returns False on AssertionError; caller (text_editor:66) ignores return value; no exception bubbles to UI |
| -- | Data corruption | Defensive wrapper around parsing errors removed accidentally (M3) | mitigate | Plan instructs executor to classify each try/except as Class A (collapse) or Class B (preserve); SUMMARY must note any Class B wrappers preserved per file |

This plan does not directly mitigate T-87-01/02/03 — those are Plan 02's concern. No new security surface introduced.
</threat_model>

<verification>
After all 3 tasks (Windows-safe):

```
# Verify all 5 files have 0 AST violations
python -c "
import sys, pathlib
sys.path.insert(0, '.')
from tests.test_no_raw_storage_access import _scan_file
files = [
    'web/components/text_editor.py',
    'web/components/translation_report.py',
    'web/pages/home.py',
    'web/pages/settings.py',
    'web/pages/search_results.py',
]
for f in files:
    src = pathlib.Path(f).read_text(encoding='utf-8')
    v = _scan_file(pathlib.Path(f), src)
    print(f, len(v), 'violations')
    assert len(v) == 0, v
print('OK: all 5 files have 0 violations')
"

# Verify all 5 files import safe_storage helpers
python -c "
import re
files = [
    'web/components/text_editor.py',
    'web/components/translation_report.py',
    'web/pages/home.py',
    'web/pages/settings.py',
    'web/pages/search_results.py',
]
for f in files:
    src = open(f).read()
    assert re.search(r'from web\\.safe_storage import', src), f
print('OK: all 5 files import safe_storage')
"

# Verify all 5 files parse and pass ruff
python -c "
import ast
for f in ['web/components/text_editor.py', 'web/components/translation_report.py', 'web/pages/home.py', 'web/pages/settings.py', 'web/pages/search_results.py']:
    ast.parse(open(f).read(), filename=f)
print('All 5 files parse OK')
"
ruff check web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py

# Verify Plan 02 tests still green
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x

# Verify no regression in any existing tests touching these files (no specific test_text_editor.py etc., but run anything matching the file basenames)
python -m pytest tests/ -k "home or search_results or settings or text_editor or translation_report" --tb=short
```
</verification>

<success_criteria>
1. All 5 files have 0 AST-detected raw `app.storage.user` accesses (verified by `_scan_file` from `tests.test_no_raw_storage_access`)
2. Each of the 5 files has at least 1 `from web.safe_storage import` line
3. Total of 16 raw access sites migrated: 3 (text_editor) + 1 (translation_report) + 2 (home) + 7 (settings) + 3 (search_results)
4. `ruff check` clean on all 5 files
5. All 5 files parse as valid Python
6. `python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py -x` exits 0 (Plan 02 invariant preserved)
7. M3: defensive try/except wrappers catching non-storage errors PRESERVED in all 5 files (per-file note in SUMMARY)
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-03-SUMMARY.md` summarizing:
- 5 files migrated (with site counts: text_editor=3, translation_report=1, home=2, settings=7, search_results=3, total=16)
- AST scanner output before/after per file (16 → 0)
- Any `from nicegui import app` imports removed (note which files had no other `app.*` usage)
- **M3 audit results per file**: list each defensive try/except that was PRESERVED and why (e.g., "home.py:_auto_dismiss_ocr — outer wrapper around ocr_banner.delete() preserved (catches non-storage failure)")
- Verification: `pytest tests/test_safe_storage.py tests/test_session_uuid.py` still 16 passed (6 + 10 from Plan 02)
</output>
