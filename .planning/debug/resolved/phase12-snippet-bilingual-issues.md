---
status: diagnosed
trigger: "Investigate Issue 1 (tag search snippet shows English metadata) and Issue 2 (Phase 12 bilingual support)"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T00:00:00Z
---

## ROOT CAUSE ANALYSIS

### Issue 1 (Test 14): Tag Search Snippet Shows English Metadata Instead of Transcription Text

**Root Cause:** The `get_fragments_by_tag()` function in `shared/document_service.py` (line 474) only fetches `pgpid, shelfmark_combined, document_type, description` from the `documents` table. It does NOT fetch the `transcription` field. Consequently, `_on_tag_search_results()` in `genizah_app.py` (line 12824) builds the snippet from `doc_type` and `desc` (English metadata):

```python
# genizah_app.py line 12824
snippet = f"{doc_type}: {desc[:120]}..." if desc else doc_type
```

The `description` column contains English metadata (e.g., "Letter from X to Y regarding Z"), not the Hebrew/Arabic transcription text. The `transcription` column (which holds the actual Hebrew text) is never queried or passed through.

**Evidence:**
1. `shared/document_service.py` line 474: `select('pgpid, shelfmark_combined, document_type, description')` -- no `transcription` field
2. `genizah_app.py` line 12824: snippet built from `doc_type` + `desc[:120]` -- English metadata
3. The `transcription` field exists in the `documents` table and is retrieved by `get_transcription_for_document()` (line 117), but this function is never called during tag search

**Files Involved:**
- `C:\GenizahSearch\shared\document_service.py` (line 453-507): `get_fragments_by_tag()` does not fetch `transcription`
- `C:\GenizahSearch\genizah_app.py` (line 12816-12836): `_on_tag_search_results()` builds snippet from English description

**Suggested Fix Direction:**
Two approaches:
1. **Lightweight (recommended):** Add `transcription` to the `select()` in `get_fragments_by_tag()`, then in `_on_tag_search_results()`, use the first ~120 chars of transcription text for the snippet, falling back to description if no transcription exists. Be mindful that fetching full transcriptions for many documents could be heavy -- consider using a Supabase substring or limiting the select to first N chars if supported.
2. **Alternative:** Keep `get_fragments_by_tag()` as-is, and make a second batch call to fetch transcription snippets for the result set using `get_transcription_for_document()`. This is cleaner separation but adds a round-trip.

---

### Issue 2 (Test 15): Phase 12 UI Strings Missing Hebrew Translations

**Root Cause:** Several Phase 12 UI strings use `tr()` for translation but have no corresponding entry in `genizah_translations.py` (the shared TRANSLATIONS dict). When a user switches to Hebrew, these strings fall through and display in English because `tr()` returns the original text when no translation is found.

**Evidence:**

Checked `genizah_translations.py` against all Phase 12 `tr()` calls. Status:

| String | Location | Has Translation? |
|--------|----------|-----------------|
| `"Show Extended Info"` | genizah_app.py:2391, 6743 | YES (line 105) |
| `"Hide Extended Info"` | genizah_app.py:3844, 7460 | YES (line 106) |
| `"Document Type"` | genizah_app.py:7427 | YES (line 1900) |
| `"Tags"` | genizah_app.py:7436 | YES (line 1901) |
| `"Description"` | genizah_app.py:7440 | YES (line 1902) |
| `"Date"` | genizah_app.py:7444 | YES (line 1903) |
| `"View on PGP"` | genizah_app.py:7450 | YES (line 1913) |
| `"No results for tag"` | genizah_app.py:12796 | YES (line 1905) |
| `"Has PGP Transcription"` | web search.py:1229 | YES (line 1909) |
| `"PGP Transcription"` | web search.py:1971 | YES (line 1910) |
| **`"PGP Only"`** | genizah_app.py:6204, web search.py:561 | **MISSING** |
| **`"Show only manuscripts with PGP transcriptions"`** | genizah_app.py:6205, web search.py:563 | **MISSING** |
| **`"Search Tag"`** | genizah_app.py:6219 | **MISSING** |
| **`"PGP Tag:"`** | genizah_app.py:6227 | **MISSING** |
| **`"Search by PGP Tag..."`** | genizah_app.py:6213 | **MISSING** |
| **`"Searching tag: {}..."`** | genizah_app.py:12786 | **MISSING** |
| **`"No local results for tag: {}"`** | genizah_app.py:12813 | **MISSING** |
| **`"Tag: {} - {} results"`** | genizah_app.py:12857 | **MISSING** |
| **`"No results for tag: {}"`** | genizah_app.py:12796 | **MISSING** (note: `"No results for tag"` without `": {}"` exists at line 1905, but the actual tr() key includes `": {}"` so it won't match) |
| `"PGP"` (badge text) | genizah_app.py:12599, 12759 | NOT wrapped in `tr()` (brand name -- debatable) |
| `"PGP"` (column header) | genizah_app.py:6261 | NOT wrapped in `tr()` (brand name -- debatable) |

**Files Involved:**
- `C:\GenizahSearch\genizah_translations.py`: Missing 8+ translation entries for Phase 12 strings
- `C:\GenizahSearch\genizah_app.py`: All Phase 12 strings properly use `tr()` -- the issue is missing dict entries
- `C:\GenizahSearch\web\pages\search.py`: Web uses same `tr()` system via `web/translations.py` which imports from same `genizah_translations.py`

**Additional Finding -- Key Mismatch Bug:**
Line 12796 uses `tr("No results for tag: {}").format(tag)` but the translations dict (line 1905) has `"No results for tag"` (without `": {}"`). The `tr()` lookup key includes the format placeholder, so it will NEVER match the existing translation. This is a key mismatch bug -- either the tr() key or the dict key needs to be aligned.

Similarly, line 1906 has `"Searching by tag..."` but line 12786 uses `"Searching tag: {}..."` -- different keys entirely.

**Suggested Fix Direction:**
1. Add the following entries to `genizah_translations.py` in the `# --- PGP Metadata ---` section:
   - `"PGP Only"`: `"PGP בלבד"`
   - `"Show only manuscripts with PGP transcriptions"`: `"הצג רק כתבי יד עם תעתוקי PGP"`
   - `"Search Tag"`: `"חפש תגית"`
   - `"PGP Tag:"`: `"תגית PGP:"`
   - `"Search by PGP Tag..."`: `"חפש לפי תגית PGP..."`
   - `"Searching tag: {}..."`: `"מחפש תגית: {}..."`
   - `"No results for tag: {}"`: `"אין תוצאות לתגית: {}"`
   - `"No local results for tag: {}"`: `"אין תוצאות מקומיות לתגית: {}"`
   - `"Tag: {} - {} results"`: `"תגית: {} - {} תוצאות"`
2. Fix the key mismatch: either change `tr("No results for tag: {}").format(tag)` to `tr("No results for tag").format(tag)` + adjust the format, or update the dict key to include `": {}"`.
3. The "PGP" badge and column header are brand abbreviations -- likely fine untranslated, but could be wrapped in `tr()` if desired.

---

## Summary

| Issue | Root Cause | Severity |
|-------|-----------|----------|
| Test 14 (snippet) | `get_fragments_by_tag()` never fetches `transcription` field; snippet built from English `description` | Functional bug |
| Test 15 (bilingual) | 8+ Phase 12 `tr()` keys missing from `genizah_translations.py`; 1 key mismatch bug | i18n gap |
