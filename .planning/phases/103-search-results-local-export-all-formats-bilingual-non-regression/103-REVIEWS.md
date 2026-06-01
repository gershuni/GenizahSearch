---
phase: 103
reviewers: [codex]
reviewed_at: 2026-06-01T13:28:24Z
plans_reviewed: [103-01-PLAN.md, 103-02-PLAN.md, 103-03-PLAN.md, 103-04-PLAN.md]
review_mode: plan-vs-live-code-drift
---

# Cross-AI Plan Review — Phase 103

> Codex ran inside the repo with read access and was instructed to verify the plans' cited
> line numbers, signatures, and call-sites against the **live code** (drift detection), not
> just re-grade plan-internal consistency (the internal gsd-plan-checker already passed that).
> All findings below were spot-verified by the orchestrator against the actual plan text and
> confirmed real (no false positives).

## Codex Review

**Summary**
The plans mostly target the right live code and the cited line ranges are accurate. The largest issues are not symbol drift, but implementation gaps: Plan 02 will not produce the locked LOCAL-only workbook shape, Plan 02 partitions LOCAL rows by 97-prefix rather than `display.source` primary, and Plan 03's CSV action snippet does not actually apply the formula-injection mitigation it later declares mandatory.

**Plan↔Code Drift Findings**

- CONFIRMED: `genizah_app.py:2531` has `_build_search_results_xlsx_bytes(...)`. Current signature has no `local_filepath_map`, so the proposed defaulted kwarg is needed for backward compatibility.
- CONFIRMED: the two desktop xlsx `skip_local=False` call sites are exactly at `genizah_app.py:2774` and `genizah_app.py:2796`.
- CONFIRMED: `export_results` (`genizah_app.py:19595`) has the claimed shape — headers/data rows at 19610, CSV at 19856, DOCX table at 19874, TXT at 19925.
- CONFIRMED: `_lookup_local_filepath` (18804) and `_prime_local_filepath_cache` (18829) exist. **Caveat:** `_lookup_local_filepath` can fall back to per-row `indexer.get_filepath()` on cache misses.
- CONFIRMED: `shared/export_dossier.py` has `main_header_row`/`manuscript_header_row`/`bibliography_header_row`/`sheet_titles` (394) and `build_manuscript_row(..., skip_local=False)` (989) / `build_bibliography_rows(..., skip_local=False)` (1110).
- **DRIFT/NUANCE:** `is_local_sys_id` is NOT a module-level symbol in `shared/export_dossier.py`; it is lazily imported from `shared/local_sys_id.py:53`. Any plan wording implying direct export from `shared.export_dossier` should be corrected.
- CONFIRMED: `local_documents_header_row`, `build_local_document_row`, `sheet_titles()['local_documents']` do not exist yet — Plan 01 is correctly a prerequisite.
- CONFIRMED: `sanitize_text_for_excel` exists at `shared_export_utils.py:19` (formula-prefix escaping at 51-55); `build_rich_snippet_cell` at 139 sanitizes before splitting `*` markers (165-172).
- **DRIFT:** Plan 03's CSV edit block writes `clean_row = [str(val).replace('*', '') ...]` and appends raw `_fp/_pg`; it does NOT call the sanitizer in the action snippet. The threat section later requires this, but the actual prescribed code does not wire it.
- CONFIRMED: `tests/test_export_xlsx_cross_parity.py:107` calls `_build_search_results_xlsx_bytes` without any local filepath arg; fixture is Genizah-only (`source='ms'`, 99-prefix), asserts sheetnames/active at 127-137. Conditional Local Documents creation keeps it green unmodified.
- CONFIRMED: `genizah_core.py:7159` `_build_local_result_dict` carries `display['source']=='LOCAL'`, filename via `display['shelfmark']`, `chunk_locator`, `p_num`, `raw_file_hl` (7185-7240). It does NOT populate `display['title']`.
- CONFIRMED: DOCX highlight/RTL logic in Plan 01 mirrors current `_add_docx_highlighted_runs` / `_set_paragraph_rtl` (`genizah_app.py:19550` / 19561).
- **DRIFT:** Plan 02's prescribed sheet creation still creates Search Results, Manuscripts, and Bibliography before Local Documents. With live code's unconditional sheet creation at `genizah_app.py:2652-2667`, LOCAL-only output will NOT be exactly `[Local Documents, Credits and Info]`.
- CONFIRMED WITH RISK: Plans 02/03 touch mostly disjoint regions, but both insert `_prime_local_filepath_cache(results_to_export)`. Plan 03 has a do-not-duplicate note; **Plan 02 does not.**

**Concerns**

- **HIGH:** LOCAL-only xlsx will violate locked D-05 unless Plan 02 explicitly branches/deletes the empty Genizah sheets.
- **HIGH:** CSV formula-injection mitigation is not in the Plan 03 action code. A literal executor would leave LOCAL filename/folder/filepath/matched text unsafe when opened in spreadsheet software.
- **MEDIUM:** Plan 02 uses `is_local_sys_id` as the primary xlsx partition, while D-14 says `display.source == 'LOCAL'` is primary and 97-prefix is only secondary.
- **MEDIUM:** CSV/TXT/DOCX use `_lookup_local_filepath`; due to fallback behavior, cache misses can still trigger per-row SQLite. Conflicts with the "NEVER per-row SQLite" intent.
- **LOW:** Plan 01/03 prefix `page` around `chunk_locator`; live LOCAL PDF locators are already like `p. 3`, causing strings like `page p. 3`.
- **LOW/MEDIUM:** Plan 03 changes Genizah-only TXT content by stripping `*` markers. Structure stays, but current TXT writes markers, so strict content non-regression could fail.

**Suggestions**

- In Plan 02, make LOCAL-only sheet creation explicit: create only `ws_local` and `ws_credits`, OR delete `ws_main/ws_manu/ws_bib` before save when `_local_only`.
- Change xlsx LOCAL detection to: `display.source == 'LOCAL' or is_local_sys_id(sid)`.
- Put the CSV sanitizer directly in the edit snippet, not just the threat section. Apply it to LOCAL-remapped cells and appended filepath/page before `writer.writerow`.
- For CSV/TXT/DOCX filepath reads, use the primed cache dict directly or adjust `_lookup_local_filepath` so an initialized cache is authoritative.
- For page labels, preserve `chunk_locator` verbatim; only format the raw `p_num` fallback.
- Add a do-not-duplicate coordination note to Plan 02 for the `_prime_local_filepath_cache(results_to_export)` insertion (symmetry with Plan 03).

**Risk Assessment**
Overall risk: **MEDIUM-HIGH.** The plans target the correct code, but the LOCAL-only workbook shape and CSV injection gap are real blockers against the locked requirements. The remaining issues are coordination and edge-case risks, not broad architectural drift.

---

## Consensus Summary

Single external reviewer (Codex). Internal gsd-plan-checker passed all 12 dimensions; Codex's
value was reading the *prescribed action code* against the *live source*, surfacing gaps the
internal checker could not see (it validates plan-internal consistency — a plan whose threat
model says "sanitize" reads as consistent even when its action snippet omits the call).

### Agreed Strengths
- Cited line numbers / signatures / call-sites are accurate (no broad symbol drift).
- Plan 01 prerequisite ordering is correct; the new helpers genuinely don't exist yet.
- The cross-parity invariant (`test_export_xlsx_cross_parity.py`) genuinely stays green untouched under D-06's conditional sheet creation.
- DOCX block writer correctly mirrors the live `_add_docx_highlighted_runs` / `_set_paragraph_rtl` logic.

### Agreed Concerns (priority order for the --reviews replan)
1. **[HIGH] D-05 LOCAL-only shape** — Plan 02 must actually omit (or delete) the empty Genizah sheets so a LOCAL-only export is `[Local Documents, Credits and Info]` with Local Documents active. Tighten the acceptance criterion (the current "omits OR they are empty" masks the gap) and the `test_local_only_workbook_usable` test to assert Manuscripts/Bibliography are ABSENT.
2. **[HIGH] CSV formula injection** — wire `sanitize_text_for_excel` (or equivalent) into the CSV action snippet itself, applied to the LOCAL-remapped cells AND the appended filepath/page, before `writer.writerow`. Add a test that a LOCAL filename/filepath beginning with `=`/`+`/`-`/`@` is neutralized in the CSV output.
3. **[MEDIUM] D-14 partition primary** — change the row-level LOCAL test (main-sheet skip + Local Documents inclusion) from `is_local_sys_id(sid)` alone to `display.source == 'LOCAL' or is_local_sys_id(sid)`.
4. **[MEDIUM] per-row SQLite fallback** — for the CSV/TXT/DOCX filepath reads, read the primed cache dict directly (or make an initialized cache authoritative in `_lookup_local_filepath`) so cache misses can't silently re-introduce per-row SQLite.
5. **[LOW] page double-prefix** — preserve `chunk_locator` verbatim (it is already `p. 3`); only synthesize a label for the raw `p_num` fallback.
6. **[LOW] TXT marker stripping** — reconfirm or reverse the deliberate decision to strip `*` markers from Genizah-only TXT; if strict content non-regression is wanted, keep `snippet` (markers) for Genizah rows.
7. **[NIT] doc accuracy** — correct any plan wording implying `is_local_sys_id` is exported from `shared.export_dossier`; it lives in `shared/local_sys_id.py`. Add the missing `_prime_local_filepath_cache` do-not-duplicate note to Plan 02.

### Divergent Views
None — single reviewer.

---

To incorporate this feedback into planning:

```
/gsd-plan-phase 103 --reviews
```
