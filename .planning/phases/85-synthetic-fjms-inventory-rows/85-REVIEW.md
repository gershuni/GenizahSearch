---
phase: 85-synthetic-fjms-inventory-rows
reviewed: 2026-05-08T11:33:22Z
depth: standard
files_reviewed: 27
files_reviewed_list:
  - corrections_client.py
  - desktop/dialogs_scholarly.py
  - desktop/result_dialog.py
  - desktop/viewers.py
  - genizah_app.py
  - genizah_core.py
  - scripts/export_fist_enrichment.py
  - scripts/generate_synthetic_rows.py
  - shared/search_serializer.py
  - shared/synthetic_sys_id.py
  - supabase_corrections_client.py
  - tests/fixtures/synthetic_fixtures.py
  - tests/test_browse_synthetic.py
  - tests/test_export_fist_synthetic.py
  - tests/test_generate_synthetic_rows.py
  - tests/test_search_serializer.py
  - tests/test_synthetic_round_trip.py
  - tests/test_synthetic_sys_id.py
  - web/api.py
  - web/api_hardening.py
  - web/components/bibliography_dialog.py
  - web/pages/browse.py
  - web/pages/browse_enrichment.py
  - web/pages/search_results.py
  - web/search_api.py
  - web/services.py
  - web/static/manuscript_viewer.js
findings:
  critical: 0
  warning: 2
  info: 4
  total: 6
status: issues_found
---

# Phase 85: Code Review Report

**Reviewed:** 2026-05-08T11:33:22Z
**Depth:** standard
**Files Reviewed:** 27
**Status:** issues_found

## Summary

Phase 85 introduces synthetic FJMS inventory rows via a new helper module
(`shared/synthetic_sys_id.py`), a regeneration script
(`scripts/generate_synthetic_rows.py`), a manifest-driven UNION ALL injection
in the FJMS sidecar exporter, browse-page hide-NLI gates across web + desktop,
an additive `is_synthetic` field on the public API serializer, and
corrections-write deferral at both REST + Supabase client entry points.

**Quality is high overall.** The helper module is well-documented, defensively
coded, and enforced by a repo-grep lint test (`TestNoIntCoercion`) that
prevents D-01b drift. The regeneration script is idempotent (marker-block
rewrite + sorted manifest), fail-loud on collision (D-01a) and
CSV-injection leaders, and exercises deterministic ordering on every SQL
query. The export script's UNION ALL pattern preserves backward compatibility
for all 12 AlmaId-keyed tables. The corrections-write gate is correctly
placed at the load-bearing backend entry points, with UI-hide as
defense-in-depth.

The plan's review cycles (Gemini + Codex) caught the dominant cross-plan
divergence risks ahead of execution: (a) manifest-as-authority for
Plan 02 → Plan 03, (b) per-table invariant checks (vs the broken global
`COUNT(DISTINCT)` check), (c) /api/parallels intentionally leaving
`is_synthetic=None`, (d) /api/fl_ids returning `200 + {fl_ids: []}` (not
204) so JSON-expecting clients don't break.

Findings below are minor: two warnings about coverage gaps that may surface
in production, and four informational items about defense-in-depth and code
clarity. None block merge; none affect the green test suite.

## Warnings

### WR-01: Puzzle page _resolve_folios fetches NLI manifest for synthetic sys_ids

**File:** `web/pages/puzzle.py:1989`
**Issue:** `_resolve_folios` falls through to an NLI IIIF manifest fetch
(`https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest`)
when the metadata-manager path doesn't yield images. For synthetic sys_ids
this will hit an NLI 404, polluting NLI access logs and adding ~15 s of
latency per request (timeout=15). The puzzle page is not in the Phase 85
file scope, but a user adding a synthetic-row fragment to a puzzle would
trigger this. D-14 explicitly listed "any `/api/fl_ids` resolution attempts"
under hide gates; the puzzle's parallel resolver is the same pattern at a
different call site.

**Fix:**
```python
# Phase 85 D-14: synthetic sys_ids skip NLI manifest fetch.
from shared.synthetic_sys_id import is_synthetic_sys_id
if is_synthetic_sys_id(sys_id):
    return []  # no NLI manifest exists for synthetic rows
url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
```

Apply parallel guard to `web/components/text_editor.py:233`
(`editorImageFallback` JS) — though that path is unreachable today because
the Edit button is hidden for synthetic rows in `web/pages/browse.py`. If
defense-in-depth matters there too, gate on `window.GENIZAH_IS_SYNTHETIC`
in the JS like `manuscript_viewer.js` already does.

### WR-02: Empty credit_link fallthrough for synthetic rows in browse credit footer

**File:** `web/pages/browse.py:4077-4094`
**Issue:** When `is_synthetic_sys_id(page.sys_id)` is True, `_nli_credit_url`
is set to `''`. The subsequent `credit_link` resolution chain ends with
`else: credit_link = _nli_credit_url` (line 4094) — meaning if the
manuscript is synthetic, NOT Oxford/Manchester/Cambridge/JTS/BL active, and
not matching any other branch (e.g. a synthetic with `library_code='Mosseri'`
and no active source), the resulting `credit_link=''` produces
`ui.link(target='')`. NiceGUI/Quasar typically renders this as an
`<a href="">` that re-loads the current page when clicked — surprising
behavior, but not a crash.

**Fix:** Add an explicit guard so synthetic rows without a routed source
don't render a clickable link at all:
```python
if is_synthetic_sys_id(page.sys_id) and not credit_link:
    # Synthetic row with no resolved external source — render attribution as plain text
    ui.label(credit_text).classes('text-xs').style('color: #aaa; font-style: italic;')
else:
    with ui.link(target=credit_link, new_tab=True).style('text-decoration: none;'):
        ui.label(credit_text).classes('text-xs').style('color: #aaa; font-style: italic;')
```

## Info

### IN-01: web/services.py `is_synthetic_sys_id` import is `noqa: F401` placeholder

**File:** `web/services.py:21`
**Issue:** The import is annotated as
`# noqa: F401  Phase 85 D-06/D-08/D-14: imported as defensive marker for
Phase 86 AUDIT-03...` and is genuinely unused — it serves only to satisfy
a future audit's grep for the helper. The 200+ character justification
comment on a single line is hard to read and signals architectural intent
that isn't expressed in code.

**Fix:** Either (a) remove the import and rely on the audit reading the
comment alone, or (b) use the import in at least one defensive branch
(e.g. add a synthetic-row no-op short-circuit at the top of
`get_metadata_only_browse_page` so future contributors see the gate
pattern in actual code). Option (b) is preferred because it makes the
guarantee load-bearing.

### IN-02: encode_inventory_sys_id docstring example uses inconsistent inventory_id widths

**File:** `shared/synthetic_sys_id.py:93-98`
**Issue:** The docstring examples are correct individually, but the
illustrative `123456 -> '990001234560000000'` (5 zeros padding) reads as
suspicious next to `1 -> '990000000001000000'` (9 zeros). At a glance a
reader might misread `'990001234560000000'` as decoding to `1234560`
(the [2:12] slice would be `'0001234560'` = 1234560) — and indeed Phase
85's `tests/fixtures/synthetic_fixtures.py:17-25` notes that the plan
originally listed this exact pair as a typo and corrected it. Adding a
worked-decode example would prevent future drift.

**Fix:**
```python
>>> encode_inventory_sys_id(123456)
'990000123456000000'
>>> # decode slice [2:12] = '0000123456' = 123456 — round-trip confirmed
>>> decode_inventory_id('990000123456000000')
123456
```

### IN-03: Bibliography UNION query has ORDER BY at outer GROUP BY level only

**File:** `scripts/export_fist_enrichment.py` (export_bibliography)
**Issue:** Per the inline comment "bibliography UNION ALL kept above; ORDER BY
is at the outer GROUP BY level", the determinism is enforced after the
GROUP BY collapses rows. This is correct for byte-stable output, but the
comment language is easy to misread as "the UNION block is unordered" —
which is also true and arguably a code smell next to the other 11 tables
that explicitly `ORDER BY AlmaId, ...` inside each UNION arm. The
GROUP BY makes inner ordering moot, so this is a documentation issue, not
a correctness bug.

**Fix:** Strengthen the comment to:
```sql
-- ORDER BY at the outer GROUP BY level is sufficient for byte-stability.
-- Inner UNION arms are intentionally unordered because GROUP BY collapses
-- duplicate (AlmaId, RunningTitleEng, ...) keys regardless of input order.
```

### IN-04: `_collect_real_alma_ids` accepts >=10-digit first cells as Alma sys_ids

**File:** `scripts/generate_synthetic_rows.py:528-530`
**Issue:** The collision-detection input set is built by extracting digits
from column 0 and accepting any 10+ digit string. libraries.csv real-Alma
IDs are 16-18 digits, but the loose threshold means an unexpected
10-digit numeric in column 0 (e.g. a future malformed row) could be
accepted as a "real Alma ID" silently, masking an actual data quality
issue. D-01a's collision check is the safety net for sys_id discriminator
ambiguity; using a tighter validator would surface CSV corruption faster.

**Fix:**
```python
# Tighter: real Alma IDs are exactly 16 or 18 digits in current corpus.
# 17-digit values are typos worth surfacing.
if sys_id and len(sys_id) in (16, 18):
    out.add(sys_id)
elif sys_id and len(sys_id) >= 10:
    # Surface unexpected widths for inspection rather than silent acceptance.
    print(f"WARNING: unusual sys_id width {len(sys_id)} in libraries.csv: {sys_id!r}",
          file=sys.stderr)
```

---

_Reviewed: 2026-05-08T11:33:22Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
