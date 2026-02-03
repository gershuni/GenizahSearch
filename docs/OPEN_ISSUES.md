# GenizahSearch - Open Issues Tracker

> **Last Updated:** 2026-02-03 (verified P1 bug already fixed)
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

| Category | Open | Fixed | Total |
|----------|------|-------|-------|
| P1 Critical Bugs | 0 | 1 | 1 |
| P2 Medium Bugs | 6 | 0 | 6 |
| P3 Low Priority | 4 | 0 | 4 |
| Documentation Issues | 8 | 0 | 8 |
| Untested Areas | 4 | 0 | 4 |
| Pending Plans | 4 | 0 | 4 |
| **Total** | **26** | **1** | **27** |

---

## 1. Outstanding Bugs

### P1 - Critical

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Desktop Path Traversal** | `filter_text_dialog.py:16-23,58` | ✅ Fixed (2026-02-03) | Already fixed - uses `_sanitize_cache_filename()` whitelist approach |

### P2 - Medium

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Debug prints in code** | `genizah_app.py`, `parallels.py` | ❌ Open | ~60+ `[DEBUG]` lines - info leakage risk |
| **Missing List Rename** | `web/pages/lists.py` | ❌ Open | Can create/delete but not rename |
| **Missing CSV/Word exports for Lists** | `lists.py:419-436` | ❌ Open | Only Excel export implemented |
| **Bare `except:` statements** | Multiple files | ❌ Open | 50+ instances of silent exception handling |
| **Shelfmark normalization inconsistency** | 4 implementations | ❌ Open | Should unify to `shared/shelfmark_utils.py` |
| **Star button visual feedback** | `browse.py`, `search.py` | ❌ Open | Should show filled ★ when item is in a list |

### P3 - Low Priority

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Auto-save not working** | `text_editor.py:374` | ❌ Open | Timer is placeholder, does nothing |
| **Race conditions in UI timers** | `joins_panel.py`, `parallels.py` | ❌ Open | Multiple timers can run in parallel |
| **Cache thread-safety** | `joins_panel.py:17-19` | ❌ Open | Global dict without lock |
| **CSRF protection missing** | API endpoints | ❌ Deferred | Low risk - NiceGUI uses WebSocket |

---

## 2. Documentation Issues

### Stale/Outdated Content

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **Lists Unification Plan references removed backend** | `LISTS_UNIFICATION_PLAN.md` | ❌ Open | References `backend/api/routes/lists.py` |
| **Joins Feed Plan references removed backend** | `JOINS_FEED_PLAN.md` | ❌ Open | References FastAPI backend files |
| **Plans Index stale status** | `PLANS_INDEX.md` | ❌ Open | Shows Supabase as "Immediate" but it's complete |
| **Duplicate bug tracking** | `PRE_LAUNCH_CHECKLIST.md` + `FIX_PLAN.md` | ❌ Open | Same bugs tracked inconsistently |

### Version Number Mismatches

| Issue | File | Status | Notes |
|-------|------|--------|-------|
| **README says 5.3** | `README.md` | ❌ Open | CHANGELOG shows 5.4.0 is latest |
| **Desktop download reference** | `README.md` | ❌ Open | References `V5.3.0_Setup.exe` |
| **Pre-launch checklist version** | `PRE_LAUNCH_CHECKLIST.md` | ❌ Open | Header says "Version: 5.1" |
| **Code Quality Audit version** | `CODE_QUALITY_AUDIT_2026-01-30.md` | ❌ Open | Header says "Version: 5.1" |

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
| **Mobile Responsive Design** | `MOBILE_RESPONSIVE_PLAN.md` | ❌ Not Started | 16 phases, ~4 weeks |
| **Lists/Projects Unification** | `LISTS_UNIFICATION_PLAN.md` | ❌ Not Started | 6-8 hours |
| **Joins in Discovery Feed** | `JOINS_FEED_PLAN.md` | ❌ Not Started | 4-6 hours |
| **Desktop Cloud Sync** | Multiple docs | ❌ Partial | Desktop still uses local storage |

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
| **Supabase RLS policies detail** | ❌ Missing | Not documented in `SUPABASE_GUIDE.md` |
| **OAuth callback handling** | ❌ Missing | Implicit flow token extraction undocumented |
| **Cloudflare rate limiting config** | ❌ Missing | Mentioned but no config documented |
| **Desktop Supabase client** | ❌ Missing | `supabase_corrections_client.py` not in code index |

---

## 7. Archive Candidates

These completed items should be moved to `docs/archive/`:

| File | Reason | Status |
|------|--------|--------|
| `SUPABASE_MIGRATION_PLAN.md` | Marked COMPLETED | ❌ Move to archive |
| `LIBRARY_LOCATION_PLAN.md` | Marked ✅ Implemented | ❌ Move to archive |
| `LIBRARY_LOCATION_TEST_CHECKLIST.md` | Testing complete | ❌ Move to archive |
| `BOUNDARY_SEARCH_SPEC.md` | COMPLETED (Web + Desktop) | ❌ Move to archive |

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
| 2026-02-03 | Verified P1 path traversal bug already fixed in `filter_text_dialog.py` | Claude |
| 2026-02-03 | Initial creation from documentation audit | Claude |

---

## Related Documents

- `PRE_LAUNCH_CHECKLIST.md` - Detailed test checklist
- `FIX_PLAN.md` - Bug fix tracking
- `CODE_QUALITY_AUDIT_2026-01-30.md` - Full code audit
- `PLANS_INDEX.md` - Implementation plans overview
