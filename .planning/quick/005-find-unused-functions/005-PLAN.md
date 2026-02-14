---
phase: quick
plan: 005
type: execute
wave: 1
depends_on: []
files_modified:
  - .planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md
autonomous: false
must_haves:
  truths:
    - "A categorized report of unused functions exists with evidence"
    - "Each function listed includes file path, line number, and reason it appears unused"
    - "Functions are categorized by confidence level (definitely unused, possibly unused, unclear)"
    - "No code is deleted - report only"
  artifacts:
    - path: ".planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md"
      provides: "Categorized report of unused functions with evidence"
---

<objective>
Find unused functions across the GenizahSearch codebase and produce a categorized report.

Purpose: Identify dead code that can be safely removed to reduce maintenance burden and improve readability.
Output: A markdown report categorizing unused functions by confidence level, with evidence for each finding.
</objective>

<execution_context>
@C:\Users\gersh\.claude/get-shit-done/workflows/execute-plan.md
@C:\Users\gersh\.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@CLAUDE.md
</context>

<tasks>

<task type="auto">
  <name>Task 1: Install vulture and run dead code analysis</name>
  <files>.planning/quick/005-find-unused-functions/vulture-raw-output.txt</files>
  <action>
Install vulture (Python dead code finder): `pip install vulture`

Run vulture against the core source files (NOT dist/, venv/, tests/, __pycache__/):

```bash
vulture genizah_core.py genizah_app.py genizah_translations.py gui_threads.py \
  corrections_client.py corrections_ui.py filter_text_dialog.py column_filter_dialog.py \
  list_filter_dialog.py lists_sync.py sefaria_utils.py server.py shared_export_utils.py \
  supabase_corrections_client.py unified_variants.py build_index.py \
  web/ \
  --min-confidence 60
```

Save the raw output to `.planning/quick/005-find-unused-functions/vulture-raw-output.txt`.

IMPORTANT: vulture reports false positives for:
- PyQt6 slot methods (connected via signals, not direct calls)
- NiceGUI event handlers (registered as callbacks, not direct calls)
- Functions used via string references or dynamic dispatch
- `__init__` methods, `__str__`, etc.
- Functions exported via `__all__`

Note these categories in the raw output for Task 2 to filter.
  </action>
  <verify>vulture-raw-output.txt exists and contains results</verify>
  <done>Raw vulture output saved with all potential unused code identified</done>
</task>

<task type="auto">
  <name>Task 2: Cross-reference and categorize findings into report</name>
  <files>.planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md</files>
  <action>
For each function/method/class reported by vulture, manually verify whether it is truly unused by grepping the ENTIRE codebase (including scripts/, tests/, corpus_mapper/) for references.

For each candidate, run a grep search like:
```bash
grep -rn "function_name" --include="*.py" C:/GenizahSearch/ --exclude-dir=venv --exclude-dir=dist --exclude-dir=__pycache__ --exclude-dir=.git
```

Also check for:
- String-based references: `getattr(obj, 'function_name')`, f-strings, dicts mapping to functions
- PyQt signal connections: `.connect(self.function_name)`
- NiceGUI callback registration: `.on('click', handler)`, `on_click=`, etc.
- Import statements from other files
- Usage in tests

Categorize each finding into one of three confidence levels:

**DEFINITELY UNUSED (High Confidence):**
- Zero references anywhere in codebase outside its own definition
- Not a dunder method, not in __all__
- Not a PyQt slot or NiceGUI callback

**POSSIBLY UNUSED (Medium Confidence):**
- Only referenced in same file but never called from outside
- Only used in commented-out code
- Only used in test files but the tested code path is itself unused
- Appears to be leftover from removed FastAPI backend

**UNCLEAR (Low Confidence):**
- Could be called dynamically (getattr, string dispatch)
- PyQt slot that might be connected in .ui files or dynamically
- Entry point scripts that might be run standalone

Create the report at `.planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md` with this structure:

```markdown
# Unused Functions Report

Generated: [date]
Tool: vulture + manual grep cross-reference

## Summary
- Definitely unused: N functions across M files
- Possibly unused: N functions across M files
- Unclear: N functions across M files
- Total lines of dead code (estimated): N

## Definitely Unused (Safe to Remove)

### [filename.py]
| Function/Method | Line | Confidence | Evidence |
|-----------------|------|------------|----------|
| `function_name` | 123  | 100%       | Zero references in codebase |

### [filename2.py]
...

## Possibly Unused (Review Before Removing)

[Same table format, with notes on why it might still be needed]

## Unclear (Needs Human Decision)

[Same table format, with notes on why it's unclear]

## Entire Files That May Be Unused

[List any .py files that appear to have no importers/callers at all,
especially look for files that were part of the old FastAPI backend]

## Recommendations

[Brief summary of what to remove first, estimated LOC savings]
```

IMPORTANT context for filtering false positives:
- The desktop app (genizah_app.py) uses PyQt6 - many methods are Qt slots connected via signals
- The web app uses NiceGUI - callbacks are registered dynamically
- genizah_core.py is a shared library used by BOTH desktop and web apps
- Some root-level files (corrections_client.py, etc.) may only be used by the desktop app
- scripts/ files are standalone utilities - they import from the codebase but nothing imports them
- The FastAPI backend was removed in Jan 2026 - any leftover references are dead code
- `unified_variants.py` (25K lines) is likely a generated/data file - note but don't enumerate every function
  </action>
  <verify>
Report exists at `.planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md`.
Report has all three confidence sections.
Each entry has file, line number, and evidence column filled in.
  </verify>
  <done>Comprehensive categorized report of unused functions with evidence, ready for human review</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <what-built>
A comprehensive report of unused functions across the GenizahSearch codebase, categorized by confidence level:
- DEFINITELY UNUSED: Safe to remove with zero risk
- POSSIBLY UNUSED: Likely removable but needs a quick review
- UNCLEAR: Needs your judgment (dynamic dispatch, Qt slots, etc.)
  </what-built>
  <how-to-verify>
1. Review the report at `.planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md`
2. Check if the "Definitely Unused" items look correct based on your knowledge of the codebase
3. Review "Possibly Unused" for any functions you know are still needed
4. Decide which items to approve for removal

The report is information-only. No code has been changed.
  </how-to-verify>
  <resume-signal>Indicate which categories/functions to remove, or request a follow-up quick task for the actual cleanup</resume-signal>
</task>

</tasks>

<verification>
- vulture ran successfully against all source files
- Each vulture finding was cross-referenced with grep
- Report categorizes findings into three confidence levels
- No code was modified or deleted
</verification>

<success_criteria>
- Report exists with categorized unused functions
- Each entry includes file path, line number, and evidence
- False positives from Qt/NiceGUI callbacks are filtered out
- User can make informed decisions about what to remove
</success_criteria>

<output>
After completion, create `.planning/quick/005-find-unused-functions/005-SUMMARY.md`
</output>
