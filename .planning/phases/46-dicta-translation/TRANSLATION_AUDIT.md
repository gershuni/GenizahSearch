# Translation Wiring Audit — 2026-03-07

## Audit Summary

478K pre-computed translations exist across 3 DBs. The infrastructure is **built but severely underutilized**. Most UI surfaces still use on-demand Dicta API calls or show no translations at all, despite pre-computed data being available.

## Available TranslationService Methods (all pre-computed, no API calls)

| Method | DB | Returns |
|--------|----|---------|
| `get_title_translation(sys_id)` | libraries_translations.db | {original_title, english_title, hebrew_title, source} |
| `get_title_translations_batch(sys_ids)` | libraries_translations.db | batch of above (batch_size=400) |
| `get_pgp_description_he(pgpid)` | pgp.db | Hebrew description string |
| `get_pgp_document_type_he(pgpid)` | pgp.db | Hebrew document type string |
| `get_pgp_translations_batch(pgpids)` | pgp.db | {pgpid: {description_he, document_type_he}} |
| `get_pgp_translations_by_sys_ids(sys_ids)` | pgp.db | {sys_id: {description_he, document_type_he}} |
| `get_fjms_translation(alma_id, field_name)` | fjms_enrichment.db | translated_text string |
| `get_fjms_free_desc_en(alma_id, sig_id)` | fjms_enrichment.db | English free description |
| `get_fjms_translations_batch(alma_ids)` | fjms_enrichment.db | {alma_id: {field_name: translated_text}} |
| `get_translated_match_sys_ids(query, sys_ids)` | pgp.db | set of sys_ids with matching translated description |

---

## DESKTOP APP (genizah_app.py)

### A. Search Results List (word search tab)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Title | YES | `_resolve_display_title()` via libraries_translations.db | — |
| Description/snippet | NO | Raw snippet only | Should show pgp description_he from pgp_translations |
| PGP document type | NO | Not shown | Could show document_type_he |
| "Translated match" badge | YES | `get_translated_match_sys_ids()` in SearchThread | — |

### B. Composition/Parallels Tree

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Manuscript titles in tree | **NO** | Raw `meta_mgr.get_meta_for_id()` | Must call `_resolve_display_title()` at lines ~22861, 22909, 23166, 23214, 23278 |
| Page node titles | NO | Empty by design | N/A |

### C. ResultDialog (expanded view)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Title | YES | `_resolve_display_title()` | — |
| PGP description | PARTIAL | On-demand Dicta API only | Should use pre-computed `get_pgp_description_he(pgpid)` first, Dicta fallback |
| PGP document_type | NO | On-demand Dicta only | Should use `get_pgp_document_type_he(pgpid)` |
| FJMS catalog fields | PARTIAL | `get_fjms_translation()` in 2 places | Not systematically applied to all fields |
| Physical description | NO | No translation | Low priority |

### D. Browse Tab (browse by shelfmark)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Title | YES | `_resolve_display_title()` | — |
| PGP description | PARTIAL | On-demand Dicta API | Should use pre-computed first |
| FJMS catalog | PARTIAL | Gap-fill only (title when Hebrew missing) | Not systematic |

### E. Virtual Reading Desk

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Titles | PARTIAL | Inherits from Browse metadata | No direct `_resolve_display_title()` call |

### F. Search History Dropdowns

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Query text | YES | Shows query + count | N/A |
| Result titles | NO | Not shown in dropdown | Low priority |

### G. Catalog Browse (domain/author/work)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Domain names | PARTIAL | Language-aware from FJMS (domain_heb) | Not via TranslationService |
| Author names | NO | Raw FJMS names | Could use fjms_translations for AuthorText |
| Work titles | NO | Raw FJMS names | Could use fjms_translations for GenizahTitleEngTitle |
| Catalog record titles | NO | Raw FJMS | Not translated |

### H. PreSearchFilterDialog

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Domain options | PARTIAL | FJMS domain_heb | Already bilingual from FJMS |
| Author options | NO | Raw names | Low priority |
| Work options | NO | Raw names | Low priority |

---

## WEB APP (web/)

### A. Search Results (web/pages/search.py)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Titles | **NO** | Shows original title only | Should use libraries_translations.db |
| PGP description | YES | `collect_translations()` → `get_pgp_translations_batch()` | — |
| "Translated match" badge | YES | `translation_match_sys_ids` | — |
| PGP document_type | NO | Not shown translated | Has data in pgp_translations |

### B. Composition/Parallels (web/pages/parallels.py)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| ALL metadata | **NO** | Zero translation wiring | Entire page needs translation support |

### C. Expanded/Advanced View (web/pages/search.py)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Title | **NO** | English only | Should show translated title |
| PGP description | YES | TranslationService → toggle-able | — |
| PGP metadata | NO | English only | document_type_he available |
| FJMS catalog title | PARTIAL | Language-aware field (title_heb) | FJMS pre-computed, not TranslationService |
| FJMS author/work | NO | Shows as-is | Could use fjms_translations |

### D. Browse Page (web/pages/browse.py)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| PGP description | YES | TranslationService → toggle-able | — |
| FJMS sections | PARTIAL | Language-aware fields | Not using fjms_translations |

### E. Catalog Browse (web/pages/catalog_browse.py)

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| Title gap-fill | YES | `get_fjms_translations_batch()` | Fills empty fields, no badge |
| Domain/author/work labels | PARTIAL | FJMS pre-computed Hebrew names | Not TranslationService |

### F. Search History

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| All metadata | **NO** | No translations stored/shown | Low priority |

### G. Advanced Filters

| Element | Wired? | How | Gap |
|---------|--------|-----|-----|
| UI labels | YES | `tr()` function | — |
| Domain options | YES | Language-aware + FJMS domain_heb | — |
| Author options | PARTIAL | FJMS heb_desc where available | Not TranslationService |
| Work options | PARTIAL | FJMS org_title | Not TranslationService |

---

## PRIORITY GAPS (ordered by user impact)

### P0 — Critical (pre-computed data exists, not displayed)

1. **Web: titles not translated in search results or expanded view**
   - 184K title translations sit unused on the web side
   - Desktop has `_resolve_display_title()`, web has nothing equivalent

2. **Desktop: composition tree titles not translated**
   - Lines ~22861, 22909, 23166, 23214, 23278 use raw metadata
   - Fix: call `_resolve_display_title()` instead

3. **Web: parallels page has ZERO translation support**
   - No `show_translations` check, no TranslationService calls
   - Composition-heavy user workflow completely untranslated

4. **Desktop: PGP description_he not used in ResultDialog/Browse**
   - 34,954 pre-computed Hebrew descriptions unused
   - Currently falls back to on-demand Dicta API (slow, rate-limited)
   - Fix: check `get_pgp_description_he(pgpid)` before Dicta fallback

### P1 — Important (improves consistency)

5. **Web: PGP document_type_he not displayed**
   - Pre-computed for all documents, never shown

6. **Desktop: FJMS free descriptions (254K) not used**
   - `get_fjms_free_desc_en()` exists, never called in UI

7. **Both: batch methods unused**
   - `get_title_translations_batch()` never called (repeated single lookups)
   - `get_pgp_translations_batch()` called in web search, NOT in desktop

### P2 — Nice to have

8. **Catalog browse author/work translations** — FJMS already has Hebrew variants
9. **Search history translation context** — low visual impact
10. **Filter dialog option translations** — FJMS already bilingual
