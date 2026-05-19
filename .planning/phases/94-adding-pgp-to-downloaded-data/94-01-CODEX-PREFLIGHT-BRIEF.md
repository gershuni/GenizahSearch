# Codex Pre-Flight Brief — Phase 94 Wave 1 (`shared/export_dossier.py`)

You are doing **integration archaeology** on Wave 1 of GenizahSearch Phase 94 *before* it executes. Goal: catch data-flow / test-target / signature mismatches against the real codebase that would otherwise surface as HIGH findings in the next `/gsd-review` round.

Scope: **Wave 1 only** (`shared/export_dossier.py` + `shared_export_utils.build_rich_snippet_cell` + tests). Do not review Waves 2-4 yet.

The plan you are checking is at `.planning/phases/94-adding-pgp-to-downloaded-data/94-01-PLAN.md`. The CONTEXT and prior Codex critique are at `94-CONTEXT.md` and `94-CODEX-CRITIQUE.md` in the same directory. Real implementations live in `shared/document_service.py`, `shared/fjms_service.py`, `shared/nli_crossref_service.py`, and `shared_export_utils.py`.

## What to verify

Read the real code first, then answer each question as **PROBLEM** (concrete plan bug — explain what breaks and what to change), **OK** (verified against real code), or **UNCERTAIN** (need more info — say what).

### Q1 — Monkeypatch target reachability
The plan (Task 1 action block) uses lazy in-function imports inside each helper, e.g.

```python
def pgp_subset_for_sys_id(sys_id):
    try:
        from shared.document_service import get_document_for_fragment
        ...
```

…but the tests (Task 1 action block) use:

```python
monkeypatch.setattr('shared.export_dossier.get_fjms_service', lambda ...: _FakeFjms(...))
monkeypatch.setattr('shared.export_dossier.get_nli_crossref_service', ...)
monkeypatch.setattr('shared.export_dossier.get_document_for_fragment', ...)
```

Will those monkeypatches actually intercept the in-function imports? If not, what is the correct fix — (a) hoist the imports to module scope so `shared.export_dossier.<name>` exists, or (b) patch `shared.fjms_service.get_fjms_service` etc. at the source module? State the recommendation and which tests need to change.

### Q2 — `get_nli_crossref_service` thread_safe default
The plan calls `get_nli_crossref_service(thread_safe=True)`. The real signature at `shared/nli_crossref_service.py:1019` is `def get_nli_crossref_service(thread_safe: bool = False)`. Is passing `thread_safe=True` safe and intended in an export-pipeline context (potentially called from a background task / different threads), or should the plan use the default? Check whether `NliCrossrefService` is documented as thread-safe and what the established consumers pass.

### Q3 — `get_catalog_records` actual field schema
The plan's `catalog_summary_for_sys_id` picks fields `title`, `author_text`, `copy_date`, `copy_place` with Hebrew fallback `title_heb`. Read `shared/fjms_service.py:2435 def get_catalog_records` and the real return shape. Verify:
- Do those 4 field names actually exist on the returned dicts?
- Is `copy_date` already sentinel-normalized to `None` (so `if v and str(v).strip()` is safe), or could it be a sentinel like `'1900'` / `0` / `'9999'`?
- Does the function ever return `full_texts` or any transcription-bearing field in the dict (D-02 boundary)?

### Q4 — `get_bibliography` field reality
The plan projects 6 fields: `running_title`, `title_year`, `mention_page`, `article_name`, `article_author_eng`, `catalog_acronym`. Read `shared/fjms_service.py:2531 def get_bibliography` and verify those keys actually exist on the returned dicts. Are any of them frequently `None` such that the manuscript dossier row would be mostly empty? Are there other bib fields the plan dropped that are commonly populated and would be useful?

### Q5 — `get_document_for_fragment` languages projection
The plan's `_split_pgp_languages` assumes `languages_primary` may be either a list OR a comma-separated string. Read `shared/document_service.py` around the `get_document_for_fragment` projection. What does the *actual* projection return today? If it's already always a list, the comma-split branch is dead code (still defensive but cheap); if it's actually always a string, then the existing list-branch unit test in `Test 1.4` is testing a path that never fires in production. State which.

### Q6 — `library_name` in `meta_resolver`
The plan's `build_manuscript_row` reads `meta.get('library_name')` and trusts the caller to pre-resolve it via `genizah_core.get_library_display(lib_code, short=False, lang='en')`. Test 6.3 asserts the row's library cell equals the English form. Confirm `get_library_display` exists with that signature in `genizah_core.py` and that `lang='en'` reliably returns English (not falling back to library_code itself when the lookup misses).

### Q7 — `build_rich_snippet_cell` sanitize ordering
CONTEXT D-14 + threat T-94-01 require sanitize-first ordering: `safe_text = sanitize_fn(text); if '*' not in safe_text: return safe_text; ...`. Confirm the plan's tests actually verify sanitize-first (e.g. a `=cmd` injection passes through `sanitize_fn` before the `*`-split). If not, what test is missing?

### Q8 — Any other data-flow trap in Wave 1?
Anything else you'd flag HIGH if you reviewed this with full integration archaeology — independent of the 7 questions above. Be specific: cite line numbers in the real code or in the plan.

## Output format

For each question (Q1..Q8): one labeled section, verdict tag (PROBLEM / OK / UNCERTAIN), a 2-5 sentence finding citing real file:line evidence, and (if PROBLEM) a one-sentence concrete fix the planner should apply. No general commentary, no high-level summary — these get folded directly into a plan revision pass.
