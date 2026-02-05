# GenizahSearch - Plans & Documentation Index

**Last Updated:** 2026-02-05

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

### 4. In-App Software Updates
**File:** `IN_APP_UPDATE_PLAN.md`
**Status:** ✅ Implemented (2026-02-04)
**Goal:** Allow users to update the desktop app without leaving the application

The app now downloads and installs updates directly:
- Download installer with progress bar
- Silent installation via Inno Setup (`/VERYSILENT /RESTARTAPPLICATIONS`)
- Installer handles closing app, updating files, and restarting
- Uses existing GitHub Releases infrastructure (no separate ZIP needed)

### 5. Library Location Feature
**File:** `LIBRARY_LOCATION_PLAN.md`
**Test Checklist:** `LIBRARY_LOCATION_TEST_CHECKLIST.md`
**Status:** ✅ Implemented (Web + Desktop)
**Goal:** Add holding library/institution information to all manuscript records

Added `library_code` column to `libraries.csv` mapping each record to its holding institution. Library information now displayed:
- **Web search results:** Abbreviated code (e.g., "CUL", "JTS") before shelfmark with tooltip
- **Web browse page:** Full library name in metadata panel
- **Desktop search results:** Separate "Library" column
- **All exports:** Library column added

Coverage: 99.65% of ~217,000 records across 29 institutions including Cambridge, JTS, Bodleian, Manchester, British Library, NLR, Mosseri, Gaster, and various special collections.

**Extraction script:** `scripts/extract_library_codes.py`

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

### Completed
1. ✅ Bug #21 - Add-to-list button
2. ✅ Supabase migration (Jan 2026)
3. ✅ Library location feature (Feb 2026)
4. ✅ Cross-paragraph search (Feb 2026)
5. ✅ Debug prints removed (Feb 2026)
6. ✅ Star button visual feedback (Feb 2026)
7. ✅ In-app software updates (Feb 2026)

### Short-term
1. 🔲 Projects UI for web
2. 🔲 Desktop cloud sync improvements
3. 🔲 Mobile responsive design

### Long-term
1. 🔲 Performance optimizations
2. 🔲 Joins in Discovery Feed
3. 🔲 Additional features from audit
4. 🔲 External data integration (PGP, NLI)
5. 🔲 User-added text search (see `USER_TEXT_SEARCH_PLAN.md`)

---

## Exploration Documents

### User-Added Text Search
**File:** `USER_TEXT_SEARCH_PLAN.md`
**Status:** 🔲 Planning
**Goal:** Allow users to add their own texts to search for parallels in the Genizah corpus

**Key Design Question:** How to divide long text into searchable pages?
- Option A: Delimiter-based (paragraph, verse breaks)
- Option B: Fixed word count (every X words)
- Option C: Hybrid (delimiter with word count fallback)

Boundary Search code (`parse_boundaries()`) can be adapted for this feature.

### External Data Integration
**File:** `EXTERNAL_DATA_INTEGRATION_EXPLORATION.md`
**Status:** 🔍 Exploration Phase (awaiting additional data)
**Goal:** Integrate PGP metadata and NLI CrossReference data to enrich GenizahSearch

**Data Sources Analyzed:**
- **Princeton Geniza Project (PGP):** ~41K documents with scholarly descriptions, types, tags, dates, transcriptions
- **NLI CrossReference:** 815K image-level records with library metadata and relationships

**Awaiting:**
- Joins/relationships file from NLI
- Dimensions and number of lines data

**Key Finding:** Join strategy validated - `system_number` links to `NLI_AlmaId` and normalized shelfmarks link to PGP data

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

## Debug Prints

✅ **All debug prints removed** (2026-02-03)

Debug print statements have been removed from all production files.

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
