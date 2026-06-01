---
status: resolved
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
reviewer: codex (codex-cli 0.130.0)
date: 2026-06-01
findings: { blocker: 0, high: 0, medium: 3, low: 2, nit: 0 }
verified_against_code: true
resolved_in: 8b25d5e4
resolution: all 5 findings fixed (see commit); 273 export tests pass, ruff clean
---

# Codex Cross-AI Code Review — Phase 103 (LOCAL Search-Results Export)

Review scope: `git diff 3f762c91..HEAD` over genizah_app.py + shared/export_dossier.py + shared/docx_export.py + phase tests. Brief: `_tmp/codex-103-review-brief.md`. All findings independently verified against the live code by the orchestrator before recording.

**BLOCKER**
None.

**HIGH**
None.

**MEDIUM**
[genizah_app.py:20182](/C:/Genizahsearch/genizah_app.py:20182) — LOCAL TXT double-prefixes `chunk_locator` as `(page p. 3)`, and CSV uses raw `p_num` without synthesizing `p. N`.
Why it matters: this violates the stated D-02 rule. `chunk_locator` is already human-formatted and should be emitted verbatim; only raw `p_num` fallback should become `p. 3`.
Fix: add one shared `_local_page_label(r)` helper:
```python
return str(r.get("chunk_locator")) if r.get("chunk_locator") else (f"p. {r.get('p_num')}" if r.get("p_num") else "")
```
Use it at [genizah_app.py:19867](/C:/Genizahsearch/genizah_app.py:19867), [genizah_app.py:20112](/C:/Genizahsearch/genizah_app.py:20112), and TXT with `page_str = f"({page})" if page else ""`.

[genizah_app.py:2935](/C:/Genizahsearch/genizah_app.py:2935) — WR-02 confirmed: the extracted CSV/TXT helpers are not used by live `export_results`.
Why it matters: the new tests exercise helpers, while production reimplements the logic inline at [genizah_app.py:19844](/C:/Genizahsearch/genizah_app.py:19844) and [genizah_app.py:20172](/C:/Genizahsearch/genizah_app.py:20172). Fixing a helper can leave the app broken.
Fix: route production through `_build_export_data_row`, `_csv_extra_cols`, `_format_txt_local_block`, and `_format_txt_genizah_block`, or delete the helpers and test the live path with a fake GUI/export sink.

[genizah_app.py:2800](/C:/Genizahsearch/genizah_app.py:2800) — xlsx partitioning uses `_is_local_row()` for the main/local sheets, but Manuscripts/Bibliography rely only on `is_local_sys_id()`.
Why it matters: a row with `display.source == 'LOCAL'` but a non-97 id is treated as LOCAL on the main sheet and Local Documents sheet, but can still leak into Manuscripts/Bibliography in mixed exports.
Fix: build `unique_sys_ids` from non-LOCAL rows only, using the same `_is_local_row(r)` predicate, then iterate that list for Manuscripts/Bibliography.

**LOW**
[genizah_app.py:3027](/C:/Genizahsearch/genizah_app.py:3027) — WR-01 confirmed: `_format_txt_genizah_block` hard-indexes `display.shelfmark/title`; the live TXT branch does the same at [genizah_app.py:20194](/C:/Genizahsearch/genizah_app.py:20194).
Why it matters: a partially populated Genizah result aborts TXT export instead of emitting blanks. Normal rows stay byte-identical if `.get()` is used.
Fix: `d = result_dict.get("display") or {}; d.get("shelfmark", "")`.

[genizah_app.py:2859](/C:/Genizahsearch/genizah_app.py:2859) — WR-03 confirmed: xlsx Local Documents matched text is not whitespace-normalized before rich-cell writing.
Why it matters: CSV/TXT collapse whitespace, but xlsx can concatenate line-broken words because `sanitize_text_for_excel` removes CR/LF rather than replacing them with spaces.
Fix: normalize matched text with `re.sub(r"\s+", " ", text.replace("\r", " ").replace("\n", " ")).strip()` before `build_rich_snippet_cell`.

**NIT**
None.

Confirmed: exactly one `self._prime_local_filepath_cache(results_to_export)` in `export_results`; export loops use `_export_filepath`, not `_lookup_local_filepath`. `tests/test_export_xlsx_cross_parity.py` is unmodified. Genizah-only xlsx shape and active sheet look preserved; Genizah-only CSV keeps 7 columns; Genizah TXT remains byte-identical by design.

Verdict: ship-with-fixes. Formula-injection on every LOCAL xlsx/csv cell: yes, fully mitigated in the live export path; I found no LOCAL cell write bypassing sanitization.