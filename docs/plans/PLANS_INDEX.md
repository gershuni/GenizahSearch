# GenizahSearch - Plans & Documentation Index

**Last Updated:** 2026-02-02

---

## Active Plans

### 1. Cross-Paragraph Search (COMPLETED)
**File:** `BOUNDARY_SEARCH_SPEC.md`
**Status:** ✅ Implemented (Web + Desktop)
**Goal:** Find parallels that cross paragraph boundaries to filter citation noise

A new parallel search mode that identifies text spanning paragraph boundaries. Useful because citations rarely cross structural breaks, so this effectively filters out Mishnah/Talmud quotes and known phrases.

**Web implementation:** Complete (Feb 2026)
**Desktop implementation:** Complete (Feb 2026)

**Desktop features:**
- Mode selection combo (Full/Cross-paragraph only/Combined)
- Delimiter selection (Line break/Blank line/Period/Colon)
- Real-time boundary stats label
- Advanced settings dialog (boost, min matches, min distance)
- 🔗 indicator on boundary-crossing results with tooltips

### 2. Supabase Migration
**File:** `SUPABASE_MIGRATION_PLAN.md`
**Status:** ✅ Completed
**Goal:** Replace self-hosted backend with Supabase cloud service

This is now the **primary plan** that supersedes the lists unification plan. By moving to Supabase:
- Data is automatically backed up and safe
- No server maintenance needed
- Both web and desktop can share the same cloud database
- Authentication handled by Supabase

### 3. Lists & Projects Unification
**File:** `LISTS_UNIFICATION_PLAN.md`
**Status:** Superseded by Supabase plan
**Goal:** Unified lists with project-based color inheritance

**Note:** The concepts in this plan (projects, color inheritance, hierarchy) are still valid and should be implemented AFTER the Supabase migration. The Supabase plan includes the database schema for projects.

---

## Bug Tracking

### 3. Pre-Launch Checklist
**File:** `PRE_LAUNCH_CHECKLIST.md`
**Status:** Active
**Summary:** 443 test items, 388 passed, tracking remaining issues

### 4. Fix Plan
**File:** `FIX_PLAN.md`
**Status:** Active
**Summary:** Tracking specific bug fixes and their status

### 5. Code Quality Audit
**File:** `CODE_QUALITY_AUDIT_2026-01-30.md`
**Status:** Reference
**Summary:** 85 findings from code review, prioritized by severity

---

## Completed Work (This Session)

### Bug #21: Add-to-List Button ✅ FIXED
**File:** `BUG_21_HANDOFF.md`
**Issues Fixed:**
1. NiceGUI select API issue (set_options arguments)
2. Backend server not running (ConnectError)
3. Color picker visual feedback

**Remaining Enhancement (P3):**
- Star button should show filled when item is already in a list

---

## Implementation Order

### Immediate (Before Launch)
1. ✅ Bug #21 - Add-to-list button (DONE)
2. ⏳ Bug #20 - Lists sync duplicates (partially diagnosed)
3. 🔲 Security issues from audit

### Short-term (Post-Launch)
1. 🔲 Supabase migration (replaces backend)
2. 🔲 Projects UI for web (after Supabase)
3. 🔲 Desktop cloud sync (after Supabase)

### Long-term
1. 🔲 Remove old backend code
2. 🔲 Performance optimizations
3. 🔲 Additional features from audit

---

## Key Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Cloud backend | Supabase | Simple, reliable, free tier sufficient |
| List colors | Inherited from projects | Consistency, simpler UX |
| User data storage | Cloud only | Data safety, cross-device sync |
| Desktop storage | Cloud (not local) | Unify with web |

---

## Files Modified This Session

### Bug Fixes
- `web/components/add_to_list_dialog.py` - Fixed select options, added color picker feedback
- `web/pages/browse.py` - Added debug logging
- `web/user_lists.py` - Added debug logging for migration

### Documentation Created
- `BUG_21_HANDOFF.md` - Bug #21 details
- `LISTS_UNIFICATION_PLAN.md` - Projects/lists architecture
- `SUPABASE_MIGRATION_PLAN.md` - Cloud migration plan
- `PLANS_INDEX.md` - This file

### Documentation Updated
- `PRE_LAUNCH_CHECKLIST.md` - Marked Bug #21 as fixed
- `FIX_PLAN.md` - Updated Bug #21 status

---

## Debug Prints Added (Remove Before Production)

These files have `[DEBUG]` print statements that should be removed:

1. `web/components/add_to_list_dialog.py`
2. `web/pages/browse.py`
3. `web/user_lists.py`
4. `genizah_core.py`

---

## For Next Agent

### If continuing Bug #20 (Lists Sync):
- Issue: Sync banner keeps appearing after sync
- Debug output shows `has_local_lists()` returning True due to 3 items
- After sync, items should be cleared but banner may still show due to page not refreshing

### If starting Supabase migration:
1. Read `SUPABASE_MIGRATION_PLAN.md` thoroughly
2. Create Supabase project first
3. Run the SQL schemas
4. Test manually in Supabase dashboard
5. Then start code changes

### If continuing Lists/Projects UI:
- Wait until Supabase migration is done
- Then implement project tree in web UI
- Desktop already has this UI, just needs cloud sync

---

## Contact & Resources

- **Supabase Docs:** https://supabase.com/docs
- **NiceGUI Docs:** https://nicegui.io/documentation
- **Project Repo:** (local: C:\GenizahSearch)

---

*Documentation is a love letter to your future self.*
