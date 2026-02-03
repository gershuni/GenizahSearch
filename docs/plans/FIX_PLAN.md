# Pre-Launch Fix Plan
**Generated:** 2026-01-29
**Status:** Ready for execution when user returns

---

## Summary

After investigation, several issues listed in the checklist are **already fixed**. The remaining issues are documented below with exact fix locations and steps.

### Already Fixed (Checklist Outdated)
| Issue | Status | Evidence |
|-------|--------|----------|
| Path traversal in `parallels.py` | ✅ Fixed | Uses `_sanitize_cache_filename()` at line 72 |
| JS injection in `text_editor.py` | ✅ Fixed | Uses `json.dumps()` at lines 215-216 |
| Add-to-list dialog colors | ✅ Fixed | Custom slot with color dots at lines 79-104 |
| #8 Search Word export RTL | ✅ Fixed | User confirmed 2026-01-29 |
| #9 Search Excel formatting | ✅ Fixed | User confirmed 2026-01-29 |
| #16 Parallels export buttons | ✅ Fixed | User confirmed 2026-01-29 |

---

## P1 - Security Issues

### 1. Path Traversal in `filter_text_dialog.py` (Desktop App) ✅ FIXED
**Risk:** Medium (desktop app only, not web)
**File:** `filter_text_dialog.py:16-23,58`
**Status:** ✅ Already fixed - uses `_sanitize_cache_filename()` whitelist approach

The fix was already applied:
- Function defined at lines 16-23
- Used at line 58: `safe_filename = _sanitize_cache_filename(ref)`

---

### 2. Rate Limiting (Deferred)
**Risk:** High but complex to implement
**Recommendation:** Defer to post-launch or implement at infrastructure level (nginx/cloudflare)

**If implementing:**
- Install: `pip install slowapi`
- Add to `web/api.py`:
```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.get('/api/search')
@limiter.limit("30/minute")
def search_api(...):
    ...
```

---

### 3. CSRF Protection (Deferred)
**Risk:** Low (NiceGUI uses WebSocket, not traditional forms)
**Recommendation:** Verify API endpoints are protected by JWT, defer CSRF tokens

---

## P2 - Bug Fixes

### ~~Bug #16: Parallels Export Buttons Not Working~~ ✅ FIXED
User confirmed fixed on 2026-01-29.

---

### ~~Bug #10: List Export "List is empty" Error~~ ✅ FIXED
**Root cause:** `get_items_in_list_sync()` only checked local storage, not server API.
For authenticated users, items are stored on server, not locally.

**Fix:** Added synchronous httpx call in `web/user_lists.py:get_items_in_list_sync()` to fetch items from API when user is authenticated.

Commit: `030d9ea`

---

### ~~Bug #15: Comments Not Shown in Browse~~ ✅ FIXED
**Root cause:** `ui.timer(0.2, check_comments, once=True)` doesn't properly await async functions in NiceGUI. The coroutine was created but never executed.

**Fix:** Changed to use `asyncio.create_task(check_comments())` in `web/components/notes_display.py:299` for proper async execution.

Commit: `ddcf254`

---

### ~~Bugs #8, #9: Export Word/Excel Formatting~~ ✅ FIXED
User confirmed fixed on 2026-01-29.

---

## Execution Order

All critical fixes completed!

1. **Quick wins:**
   - [x] Fix `filter_text_dialog.py` path traversal ✅ (commit `030d9ea`)
   - [x] Update checklist to mark already-fixed items ✅

2. **Bug fixes:**
   - [x] ~~Debug parallels export (#16)~~ ✅ Fixed
   - [x] ~~List export (#10)~~ ✅ Fixed (commit `030d9ea`)
   - [x] ~~Comments display (#15)~~ ✅ Fixed (commit `ddcf254`)

3. **~~Export formatting~~ ✅ DONE:**
   - [x] ~~Word RTL alignment (#8)~~ ✅ Fixed
   - [x] ~~Excel line breaks (#9)~~ ✅ Fixed

4. **Deferred:**
   - [ ] Rate limiting (post-launch or infrastructure)
   - [ ] CSRF (low priority, verify JWT coverage)

---

## NEW Issues Found (2026-01-30)

### Bug #20: Lists Sync Creates Duplicates (P1)
**Symptom:** Clicking "Sync Now" creates duplicate lists; banner keeps appearing
**Status:** Partially fixed - code updated but not fully working

**Root causes:**
1. Backend migration doesn't properly deduplicate (fix attempted but may need review)
2. Local lists not cleared after migration (fix attempted)
3. Existing duplicates in database need manual cleanup

**Files:**
- `backend/services/lists_service.py:456-474` - Migration dedup logic
- `web/user_lists.py:560-565` - Local list clearing
- `genizah_core.py:5460-5464` - clear_all() method

**Immediate fix for user:**
```sql
-- Run in SQLite to remove duplicate lists:
-- First backup: cp corrections.db corrections.db.bak
DELETE FROM user_lists WHERE id NOT IN (
    SELECT MIN(id) FROM user_lists GROUP BY user_id, name
);
```

**Investigation needed:**
1. Check if `clear_all()` is actually being called
2. Verify `has_local_lists()` returns False after clearing
3. Add logging to migration flow

---

### ~~Bug #21: Add-to-List Button Not Working~~ ✅ FIXED
**Symptom:** Clicking star button in Browse/Search does nothing (no dialog, no notification)

**Root Causes Found:**
1. `ui.select.set_options()` was called with invalid arguments (`value_key`, `label_key`)
2. Backend server was not running (ConnectError when creating lists)

**Fixes Applied:**
1. Changed `add_to_list_dialog.py` to use simple dict format for select options
2. Added color picker visual feedback
3. Added comprehensive debug logging

**Files Modified:**
- `web/components/add_to_list_dialog.py` - Fixed select options format
- `web/pages/browse.py` - Added debug logging
- `web/user_lists.py` - Added debug logging

**Remaining Enhancement (P3):**
- Star button should show filled/colored when item is already in a list

Commit: (pending)

---

## Quick Commands

```bash
# Run the web app for testing
python -m web.main

# Check for remaining vulnerable patterns
grep -r "replace.*'/'.*'_'" --include="*.py"

# Run tests
pytest tests/ -v
```

---

**Note:** This plan is based on code analysis. Some issues may resolve differently during actual testing. The debug logging approach allows quick identification of root causes.
