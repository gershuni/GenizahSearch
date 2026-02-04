# GenizahSearch - Open Issues Tracker

> **Last Updated:** 2026-02-04 (cache thread-safety and timer race conditions fixed)
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
| Old text | ❌ Open | Description |
↓
| Old text | ✅ Fixed (2026-02-03) | Description |

# When adding new issue:
| **NEW: Issue description** | ❌ Open | Details |

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

| Category | Open | Fixed/Deferred | Total |
|----------|------|----------------|-------|
| P1 Critical Bugs | 0 | 1 | 1 |
| P2 Medium Bugs | 1 | 5 | 6 |
| P3 Low Priority | 1 | 3 | 4 |
| Documentation Issues | 0 | 8 | 8 |
| Documentation Gaps | 0 | 4 | 4 |
| Untested Areas | 4 | 0 | 4 |
| Pending Plans | 0 | 4 | 4 |
| Archive Candidates | 0 | 4 | 4 |
| **Total** | **6** | **29** | **35** |

---

## 1. Outstanding Bugs

### P1 - Critical

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Desktop Path Traversal** | `filter_text_dialog.py:16-23,58` | ✅ Fixed (2026-02-03) | Already fixed - uses `_sanitize_cache_filename()` whitelist approach |

### P2 - Medium

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Debug prints in code** | `genizah_app.py`, `parallels.py` | ✅ Fixed (2026-02-03) | Removed all `[DEBUG]` print statements |
| **List Rename** | `web/pages/lists.py:414-423` | ✅ Fixed (2026-02-03) | Uses `create_inline_edit_label` for inline editing |
| **Missing CSV/Word exports for Lists** | `lists.py:612-631` | ⏭️ Won't Fix | Excel export sufficient for needs |
| **Bare `except:` statements** | Multiple files | ✅ Fixed (2026-02-03) | Changed all 16 instances to `except Exception:` |
| **Shelfmark normalization inconsistency** | 5 implementations | ❌ Open | `genizah_app.py` (2), `genizah_core.py` (2), `corrections_ui.py` (1) |
| **Star button visual feedback** | `browse.py`, `search.py` | ✅ Fixed (2026-02-03) | Shows `star` when in list, `star_border` when not |

### P3 - Low Priority

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Auto-save not working** | `text_editor.py:374` | ✅ Fixed (2026-02-03) | Auto-save implemented at lines 443-454 using NiceGUI timer |
| **Race conditions in UI timers** | `parallels.py`, `search.py` | ✅ Fixed (2026-02-04) | Added timer tracking and deactivation to prevent duplicates |
| **Cache thread-safety** | `joins_panel.py:17-19` | ✅ Fixed (2026-02-04) | Added threading.Lock for cache access |
| **CSRF protection missing** | API endpoints | ❌ Deferred | Low risk - NiceGUI uses WebSocket |

---

## 2. Documentation Issues

### Stale/Outdated Content

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Lists Unification Plan references removed backend** | `LISTS_UNIFICATION_PLAN.md` | ✅ Fixed (2026-02-03) | Added deprecation note |
| **Joins Feed Plan references removed backend** | `JOINS_FEED_PLAN.md` | ✅ Fixed (2026-02-03) | Added deprecation note |
| **Plans Index stale status** | `PLANS_INDEX.md` | ✅ Fixed (2026-02-03) | Updated with current status |
| **Duplicate bug tracking** | `PRE_LAUNCH_CHECKLIST.md` + `FIX_PLAN.md` | ⏭️ Deferred | OPEN_ISSUES.md is now canonical |

### Version Number Mismatches

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **README says 5.3** | `README.md` | ✅ Fixed (2026-02-03) | Updated to 5.4 |
| **Desktop download reference** | `README.md` | ✅ Fixed (2026-02-03) | Updated to V5.4.1 |
| **Pre-launch checklist version** | `PRE_LAUNCH_CHECKLIST.md` | ✅ Fixed (2026-02-03) | Updated to 5.4 |
| **Code Quality Audit version** | `CODE_QUALITY_AUDIT_2026-01-30.md` | ⏭️ N/A | Already in archive |

---

## 3. Untested Areas

These items from `PRE_LAUNCH_CHECKLIST.md` need verification:

| Area | Status | Notes |
|------|--------|-------|
| **End-to-End Integration** | ❌ Not Tested | Full flows: Search→View→Edit→Submit→Approve |
| **Concurrency** | ❌ Not Tested | Two users editing same correction simultaneously |
| **Browser Compatibility** | ❌ Not Tested | Chrome, Firefox, Safari, Edge, Mobile |
| **Performance** | ❌ Not Tested | 1000+ results, 100+ list items, stress tests |

---

## 4. Pending Plans (Not Yet Implemented)

| Plan | File | Status | Complexity |
|------|------|--------|------------|
| **Mobile Responsive Design** | `MOBILE_RESPONSIVE_PLAN.md` | ⏭️ Deferred | 16 phases, ~4 weeks |
| **Lists/Projects Unification** | `LISTS_UNIFICATION_PLAN.md` | ⏭️ Deferred | 6-8 hours |
| **Joins in Discovery Feed** | `JOINS_FEED_PLAN.md` | ⏭️ Deferred | 4-6 hours |
| **Desktop Cloud Sync** | Multiple docs | ⏭️ Deferred | Desktop still uses local storage |

> Note: Per user request, all pending plans marked as deferred/done for tracking purposes (2026-02-03)

---

## 5. Code Quality Debt

### Duplication to Address

| Issue | Files | Status | Notes |
|-------|-------|--------|-------|
| **Excel export duplication** | `genizah_app.py` + `export_service.py` | ❌ Open | ~80% duplicate code |
| **Word export duplication** | `genizah_app.py` + `export_service.py` | ❌ Open | ~80% duplicate code |
| **Text sanitization inconsistency** | Desktop vs Web | ❌ Open | Different behavior on same input |

### Hardcoded Values

| Value | File | Status | Should Be |
|-------|------|--------|-----------|
| `_CACHE_TTL = 30` | `joins_panel.py:19` | ❌ Open | Environment variable |
| `CACHE_TTL = 300` | `api.py:46` | ❌ Open | Environment variable |
| Timeouts & retries | `auth_state.py:17-20` | ❌ Open | Config file |

---

## 6. Documentation Gaps

| Topic | Status | Notes |
|-------|--------|-------|
| **Supabase RLS policies detail** | ✅ Fixed (2026-02-03) | Added detailed policy SQL examples to `SUPABASE_GUIDE.md` |
| **OAuth callback handling** | ✅ Fixed (2026-02-03) | Documented implicit flow and token extraction in `SUPABASE_GUIDE.md` |
| **Cloudflare rate limiting config** | ✅ Fixed (2026-02-03) | Added configuration guide to `DEPLOYMENT_TECHNICAL.md` |
| **Desktop Supabase client** | ✅ Fixed (2026-02-03) | Added `supabase_corrections_client.py` to `CODE_INDEX.md` |

---

## 7. Archive Candidates

All completed items have been moved to `docs/archive/`:

| File | Reason | Status |
|------|--------|--------|
| `SUPABASE_MIGRATION_PLAN.md` | Marked COMPLETED | ✅ Archived (2026-02-03) |
| `LIBRARY_LOCATION_PLAN.md` | Marked ✅ Implemented | ✅ Archived (2026-02-03) |
| `LIBRARY_LOCATION_TEST_CHECKLIST.md` | Testing complete | ✅ Archived (2026-02-03) |
| `BOUNDARY_SEARCH_SPEC.md` | COMPLETED (Web + Desktop) | ✅ Archived (2026-02-03) |

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
