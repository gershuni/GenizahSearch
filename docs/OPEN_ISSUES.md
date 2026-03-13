# GenizahSearch - Open Issues Tracker

> **Last Updated:** 2026-03-13 (Fixed P2 PGP page_info filtering — verso-only transcriptions, NLI image nav, desktop PGP refresh)
> **Status:** Active working document

---

## AI Assistant Maintenance Protocol

**IMPORTANT:** This document must be kept current. Follow these rules:

### When to Update This Document

1. **After fixing any issue** - Mark as completed with date
2. **After discovering new issues** - Add to appropriate section
3. **After completing a work session** - Review and update status
4. **When starting work** - Check current status first

### How to Update

```markdown
# When completing an issue:
| Issue | Status | Notes |
| Old text | ג Open | Description |
ג†“
| Old text | ג… Fixed (2026-02-03) | Description |

# When adding new issue:
| **NEW: Issue description** | ג Open | Details |

# When removing (only after verified in production):
Move to "Completed Issues" section at bottom with date
```

### Update Checklist (run after each session)

- [ ] Update "Last Updated" timestamp at top
- [ ] Mark any fixed issues with date
- [ ] Add any newly discovered issues
- [ ] Move verified-complete items to archive section
- [ ] Update summary counts if changed

---

## Quick Summary

| Category | Open | Fixed/Implemented | Total |
|----------|------|-------------------|-------|
| P1 Critical Bugs | 0 | 4 | 4 |
| P2 Medium Bugs | 2 | 8 | 10 |
| P3 Low Priority | 1 | 3 | 4 |
| Documentation Issues | 0 | 8 | 8 |
| Documentation Gaps | 0 | 4 | 4 |
| Code Quality Debt | 0 | 6 | 6 |
| Untested Areas | 7 | 0 | 7 |
| Implemented Plans | 0 | 5 | 5 |
| Archive Candidates | 0 | 4 | 4 |
| **Total** | **10** | **43** | **53** |

---

## 1. Outstanding Bugs

### P1 - Critical

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Desktop Path Traversal** | `filter_text_dialog.py:16-23,58` | ג… Fixed (2026-02-03) | Already fixed - uses `_sanitize_cache_filename()` whitelist approach |

| **Desktop FJMS catalog HTML injection** | `genizah_app.py:6712-6716,6879-6883,6921-6932` | ג… Fixed (2026-03-10) | Added `html.escape()` to all three catalog toggle sections (RunningTitle, FreeDesc, FullText) |
| **Parallels metadata dialog NameError** | `web/pages/parallels.py:3387` | ג… Fixed (2026-03-10) | Changed `_md_show_trans` ג†’ `_par_show_trans` |
| **Search advanced header NameError** | `web/pages/search.py:4656` | ג… Fixed (2026-03-10) | Replaced `_adv_tt` with safe `_adv_tt_resolved` lookup |
### P2 - Medium

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Translation batch script rewires stdio on import and breaks pytest capture** | scripts/translate_pgp_descriptions.py, scripts/translate_libraries_titles.py | ג… Fixed (2026-03-11) | Moved UTF-8 stdio setup from import-time into `_configure_utf8_stdio()` called only from `if __name__ == "__main__":`. Uses `reconfigure()` when available. Fixed in both translation scripts. |
| **FJMS export drops ~38K catalog records (MAX(Version) filter)** | scripts/export_fist_enrichment.py | ✅ Fixed (2026-03-11) | MAX(Version) join on dbo_Signature drops child records when latest version lacks data but earlier versions have it. Affects 6 functions (catalog, running_titles, sizes, fields, textual_frames, mentions). 37,962 catalog recs lost (9.2%), 33,410 AlmaIds affected. 3 other functions (free_desc, full_texts, bibliography) already fixed. Fix: remove MAX(Version) from 6 functions. UnitCatalogRecId never on multiple versions — verified 0 for all 6 child tables (UnitCatalogRec, CatalogMultiRunningTitle, CatalogMultiSize, CatalogMultiField, CatalogMultiMention). See `docs/FJMS_EXPORT_AND_TRANSLATION_BUGS.md`. |
| **FJMS catalog translation toggle shows wrong language by default (desktop + web RT)** | genizah_app.py:6784-7000, web/components/catalog_dialog.py:274-286, shared/translation_service.py:392-420 | ✅ Fixed (2026-03-11) | Translation directions are mixed: RunningTitle/FullText are en2he, FreeDesc is he2en. Service layer drops direction column. Desktop: 3 confirmed wrong-default toggle sections (RunningTitle en2he, FreeDesc he2en, FullText en2he). Web: RunningTitle replacement confirmed broken (replaces EN with HE in EN UI); FreeDesc works by coincidence; FullText has no translation logic. Fix: (1) return direction from translation_service, (2) make desktop renderer + web RT replacement direction-aware. See `docs/FJMS_EXPORT_AND_TRANSLATION_BUGS.md`. |
| **PGP verso-only transcription shown on recto page** | shared/document_service.py, web/pages/browse.py, genizah_app.py | ✅ Fixed (2026-03-13) | 3 bugs: (1) get_section_for_page uses fragment_page_info to suppress unmarked transcriptions on wrong page, (2) desktop NLI image nav used images_ext instead of active_list, (3) desktop _browse_refresh_pgp_for_page bailed on empty sources list. Affects ENA and other NLI-only manuscripts with page-specific PGP links. |
| **Browse title toggle keeps RTL classes** | web/pages/browse.py:2093,2098,2253,2257 | ג… Fixed (2026-03-10) | Changed `.classes()` to use `remove=/add=` for proper class swapping |
| **Debug prints in code** | `genizah_app.py`, `parallels.py` | ג… Fixed (2026-02-03) | Removed all `[DEBUG]` print statements |
| **List Rename** | `web/pages/lists.py:414-423` | ג… Fixed (2026-02-03) | Uses `create_inline_edit_label` for inline editing |
| **Missing CSV/Word exports for Lists** | `lists.py:612-631` | ג­ן¸ Won't Fix | Excel export sufficient for needs |
| **Bare `except:` statements** | Multiple files | ג… Fixed (2026-02-03) | Changed all 16 instances to `except Exception:` |
| **Shelfmark normalization inconsistency** | 5 implementations | ג… Fixed (2026-02-04) | Unified to single `normalize_shelfmark()` in `genizah_core.py` |
| **Star button visual feedback** | `browse.py`, `search.py` | ג… Fixed (2026-02-03) | Shows `star` when in list, `star_border` when not |

### P3 - Low Priority

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Auto-save not working** | `text_editor.py:374` | ג… Fixed (2026-02-03) | Auto-save implemented at lines 443-454 using NiceGUI timer |
| **Race conditions in UI timers** | `parallels.py`, `search.py` | ג… Fixed (2026-02-04) | Added timer tracking and deactivation to prevent duplicates |
| **Cache thread-safety** | `joins_panel.py:17-19` | ג… Fixed (2026-02-04) | Added threading.Lock for cache access |
| **Filter panel overlap with progress bar** | `web/pages/search.py`, `parallels.py` | ג… Fixed (2026-03-03) | Chip bar, progress bar, results overlapped when filter panel open. Auto-collapse panel on search start + scroll to progress + spacing/z-index fix |
| **Pre-search domain filter: bilingual, "Other" ambiguous, missing 3rd level** | `search.py`, `parallels.py`, `genizah_app.py`, `fjms_service.py` | ג… Fixed (2026-03-03) | Dropdown showed bilingual labels (should be current lang only), "Other" had no parent disambiguation, sub-sub-domains missing. Chips also lost qualified names. Fixed all 3 issues + recursive checkbox propagation + qualified-name SQL filtering |
| **CSRF protection missing** | API endpoints | ג Deferred | Low risk - NiceGUI uses WebSocket |
| **Session persistence only saves search tab state** | genizah_app.py | ❌ Open | Session save/restore remembers search state but does not restore position and info in other tabs (Browse, Lists, etc.). User request: persist full tab state across sessions. |

---

## 2. Documentation Issues

### Stale/Outdated Content

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Lists Unification Plan references removed backend** | `LISTS_UNIFICATION_PLAN.md` | ג… Fixed (2026-02-03) | Added deprecation note |
| **Joins Feed Plan references removed backend** | `JOINS_FEED_PLAN.md` | ג… Fixed (2026-02-03) | Added deprecation note |
| **Plans Index stale status** | `PLANS_INDEX.md` | ג… Fixed (2026-02-03) | Updated with current status |
| **Duplicate bug tracking** | `PRE_LAUNCH_CHECKLIST.md` + `FIX_PLAN.md` | ג­ן¸ Deferred | OPEN_ISSUES.md is now canonical |

### Version Number Mismatches

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **README says 5.3** | `README.md` | ג… Fixed (2026-02-03) | Updated to 5.4 |
| **Desktop download reference** | `README.md` | ג… Fixed (2026-02-03) | Updated to V5.4.1 |
| **Pre-launch checklist version** | `PRE_LAUNCH_CHECKLIST.md` | ג… Fixed (2026-02-03) | Updated to 5.4 |
| **Code Quality Audit version** | `CODE_QUALITY_AUDIT_2026-01-30.md` | ג­ן¸ N/A | Already in archive |

---

## 3. Untested Areas

These items from `PRE_LAUNCH_CHECKLIST.md` need verification:

| Area | Status | Notes |
|------|--------|-------|
| **End-to-End Integration** | ❌ Not Tested | Full flows: Search→View→Edit→Submit→Approve |
| **Concurrency** | ❌ Not Tested | Two users editing same correction simultaneously |
| **Browser Compatibility** | ❌ Not Tested | Chrome, Firefox, Safari, Edge, Mobile |
| **Performance** | ❌ Not Tested | 1000+ results, 100+ list items, stress tests |
| **In-App Update (Desktop)** | ❌ Test on Next Release | Build test version with 5.0.0, verify full update flow works (download → install → auto-restart) |
| **Translation QA / hallucination audit** | ✅ Fixed (2026-03-11) | QC module (`shared/translation_qc.py`), audit script, report component, disclaimers added. 12,827 rows fixed: Piyyut (10,256), Bible (979), Mahzor (317), Selihot (347), Kinot (218), stuttering nulled (257), FJMS hallucinations deleted (445), PGP collapsed nulled (8). DBs uploaded to server. |
| **MARC field translations (Date/Subjects/People)** | ❌ Needs Testing | Added translate badges for Date, Subjects, People in ResultDialog and Browse extended info. Hebrew dates use direct gematria converter (`_translate_hebrew_date`) to avoid Dicta errors (e.g. "מאה ט״ו" → "15th century"). Subjects/People use Dicta on-demand. Test: open records with Hebrew dates, subjects, people in EN UI with translations ON. Verify badges appear, translations are correct, toggle works. Test record: sys_id 990001430180205171. |

---

## 4. Pending Plans (Implemented)

| Plan | File | Status | Notes |
|------|------|--------|-------|
| **Mobile Responsive Design** | `MOBILE_RESPONSIVE_PLAN.md` | ג… Implemented | Responsive design completed |
| **Lists/Projects Unification** | `LISTS_UNIFICATION_PLAN.md` | ג… Implemented | Lists and projects unified |
| **Joins in Discovery Feed** | `JOINS_FEED_PLAN.md` | ג… Implemented | Joins appear in discovery feed |
| **Desktop Cloud Sync** | Multiple docs | ג… Implemented | Desktop syncs with Supabase |

> Note: All plans implemented as of 2026-02-04

---

## 5. Code Quality Debt

### Duplication to Address

| Issue | Files | Status | Notes |
|-------|-------|--------|-------|
| **Excel export duplication** | `genizah_app.py` + `export_service.py` | ג… Fixed (2026-02-04) | Unified via `shared_export_utils.py` |
| **Word export duplication** | `genizah_app.py` + `export_service.py` | ג… Fixed (2026-02-04) | Unified via `shared_export_utils.py` |
| **Text sanitization inconsistency** | Desktop vs Web | ג… Fixed (2026-02-04) | Single `sanitize_text_for_excel()` in `shared_export_utils.py` |

### Hardcoded Values

| Value | File | Status | Should Be |
|-------|------|--------|-----------|
| `_CACHE_TTL = 30` | `joins_panel.py:19` | ג… Fixed (2026-02-04) | Now uses `JOINS_CACHE_TTL` env var |
| `CACHE_TTL = 300` | `api.py:46` | ג… Fixed (2026-02-04) | Now uses `NLI_CACHE_TTL` / `IMAGE_CACHE_TTL` env vars |
| Timeouts & retries | `auth_state.py:17-20` | ג Deferred | Low priority - defaults are reasonable |

---

## 6. Documentation Gaps

| Topic | Status | Notes |
|-------|--------|-------|
| **Supabase RLS policies detail** | ג… Fixed (2026-02-03) | Added detailed policy SQL examples to `SUPABASE_GUIDE.md` |
| **OAuth callback handling** | ג… Fixed (2026-02-03) | Documented implicit flow and token extraction in `SUPABASE_GUIDE.md` |
| **Cloudflare rate limiting config** | ג… Fixed (2026-02-03) | Added configuration guide to `DEPLOYMENT_TECHNICAL.md` |
| **Desktop Supabase client** | ג… Fixed (2026-02-03) | Added `supabase_corrections_client.py` to `CODE_INDEX.md` |

---

## 7. Archive Candidates

All completed items have been moved to `docs/archive/`:

| File | Reason | Status |
|------|--------|--------|
| `SUPABASE_MIGRATION_PLAN.md` | Marked COMPLETED | ג… Archived (2026-02-03) |
| `LIBRARY_LOCATION_PLAN.md` | Marked ג… Implemented | ג… Archived (2026-02-03) |
| `LIBRARY_LOCATION_TEST_CHECKLIST.md` | Testing complete | ג… Archived (2026-02-03) |
| `BOUNDARY_SEARCH_SPEC.md` | COMPLETED (Web + Desktop) | ג… Archived (2026-02-03) |

---

## 8. Completed Issues (Archive)

*Move verified-complete items here with completion date*

| Issue | Completed | Notes |
|-------|-----------|-------|
| *None yet* | - | - |

---

## Change Log

| Date | Change | By |
|------|--------|-----|
| 2026-03-11 | Added P2: FJMS export MAX(Version) drops ~38K catalog records (9.2%); P2: Desktop translation toggle shows wrong language by default. Full report in docs/FJMS_EXPORT_AND_TRANSLATION_BUGS.md | Claude |
| 2026-03-11 | MARC field translations: added translate badges for Date, Subjects, People; Hebrew date gematria converter avoids Dicta errors; marked for testing | Claude |
| 2026-03-11 | Added open untested-area item for translation QA / hallucination audit after reviewing the Phase 46 translation rollout | Codex |
| 2026-03-01 | v6.1.1 ג€” async desktop catalog browse (QThread), 100x faster domain queries (35s->0.8s via IN+UNION subquery + dedup CTE), 3-level domain hierarchy, canonical FJMS ordering, thread-safe FjmsService, browse cache v2 | Claude |
| 2026-02-22 | Closed v6.0.0 milestone ג€” local data architecture (pgp.db sidecar, FJMS catalog descriptions, offline browsing), bug fixes (desktop crashes, pagination), performance optimization (parallel NLI, crossref, variant cache), IsNotGenizah badge removed | Claude |
| 2026-02-16 | Closed v5.9.0 milestone ג€” multi-source image & metadata integration (NLI crossref, Cambridge/Manchester/JTS IIIF, bibliography, catalog refs), version bump to 5.9.0 | Claude |
| 2026-02-15 | Closed v5.8.0 milestone ג€” FJMS integration (domains, scientific joins, catalog enrichment), version bump to 5.8.0 | Claude |
| 2026-02-11 | Closed v5.7.2 milestone ג€” version bump to 5.7.2, AI code removed, search normalization, full green test suite, structural sections | Claude |
| 2026-02-09 | Closed v5.6.0 milestone ג€” version bump to 5.6.0, updated CHANGELOG.md and STATE.md | Claude |
| 2026-02-09 | Created `pgp_tag_translations.py` ג€” 251 PGP tags with curated Hebrew translations in 16 categories | Claude |
| 2026-02-09 | Added categorized tag dropdowns with category headers in both web and desktop apps | Claude |
| 2026-02-09 | Language-aware tag display: Hebrew UI shows "׳¢׳‘׳¨׳™׳× (English)", English UI shows English only | Claude |
| 2026-02-09 | Fixed desktop PGP Tags mode layout ג€” hides row1, shows tag combo in row2 after Mode | Claude |
| 2026-02-09 | Fixed web [object Object] in tag dropdown ג€” switched to NiceGUI native dict format | Claude |
| 2026-02-09 | Corrected ~12 tag category misassignments (e.g., Ibn Yiju moved from India Book to People) | Claude |
| 2026-02-09 | PGP Tags search mode added to Mode dropdown in both apps | Claude |
| 2026-02-09 | PGP column sorting, simplified PGP controls, user-friendly labels | Claude |
| 2026-02-09 | Reverted Phase 13 (Transcription Search) ג€” index build too slow for desktop | Claude |
| 2026-02-04 | Improved connection indicator UX - yellow pulsing dot for loading, no alarming text messages | Claude |
| 2026-02-04 | Fixed sidebar opening on mobile - now closes by default on screens < 768px | Claude |
| 2026-02-04 | Improved connection stability - added continuous heartbeat monitoring and reconnect_timeout | Claude |
| 2026-02-04 | Added Hebrew translations for "Reconnecting...", "Connecting..." | Claude |
| 2026-02-04 | Implemented in-app software updates - downloads and runs installer silently via Inno Setup | Claude |
| 2026-02-04 | Added translation button for comments and community messages (Hebrew ג†” English using MyMemory API) | Claude |
| 2026-02-04 | Marked pending plans as implemented: Mobile, Lists Unification, Joins Feed, Desktop Sync | Claude |
| 2026-02-04 | Created `shared_export_utils.py` - unified text sanitization, filename helpers for Desktop & Web | Claude |
| 2026-02-04 | Unified shelfmark normalization - single `normalize_shelfmark()` in genizah_core.py | Claude |
| 2026-02-04 | Made TTL values configurable via environment variables (JOINS_CACHE_TTL, NLI_CACHE_TTL, IMAGE_CACHE_TTL) | Claude |
| 2026-02-04 | Fixed UI timer race conditions in parallels.py and search.py - added timer tracking | Claude |
| 2026-02-04 | Fixed cache thread-safety in joins_panel.py - added threading.Lock | Claude |
| 2026-02-03 | Fixed bare `except:` statements - changed all 16 to `except Exception:` | Claude |
| 2026-02-03 | Verified auto-save in text_editor.py is working (lines 443-454) | Claude |
| 2026-02-03 | Fixed all 4 documentation gaps: RLS policies, OAuth callback, Cloudflare config, Desktop client | Claude |
| 2026-02-03 | Moved 4 completed plans to archive (Supabase, Library Location, Boundary Search) | Claude |
| 2026-02-03 | Updated README.md version to 5.4, fixed download reference to V5.4.1 | Claude |
| 2026-02-03 | Updated PRE_LAUNCH_CHECKLIST.md version to 5.4 | Claude |
| 2026-02-03 | Added deprecation notes to LISTS_UNIFICATION_PLAN.md and JOINS_FEED_PLAN.md | Claude |
| 2026-02-03 | Updated PLANS_INDEX.md with current implementation status | Claude |
| 2026-02-03 | Marked all pending plans as deferred per user request | Claude |
| 2026-02-03 | Fixed star button visual feedback in browse.py, search.py, viewer.py, parallels.py | Claude |
| 2026-02-03 | Removed all `[DEBUG]` print statements from production code | Claude |
| 2026-02-03 | Marked CSV/Word exports as "Won't Fix" per user request | Claude |
| 2026-02-03 | Verified all P2 bugs - list rename already fixed, updated counts | Claude |
| 2026-02-03 | Verified P1 path traversal bug already fixed in `filter_text_dialog.py` | Claude |
| 2026-02-03 | Initial creation from documentation audit | Claude |

---

## Related Documents

- `PRE_LAUNCH_CHECKLIST.md` - Detailed test checklist
- `FIX_PLAN.md` - Bug fix tracking
- `CODE_QUALITY_AUDIT_2026-01-30.md` - Full code audit
- `PLANS_INDEX.md` - Implementation plans overview

