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

### 1. Path Traversal in `filter_text_dialog.py` (Desktop App)
**Risk:** Medium (desktop app only, not web)
**File:** `filter_text_dialog.py:47`

**Current vulnerable code:**
```python
cache_file = os.path.join(cache_dir, f"{ref.replace(' ', '_').replace('/', '_')}_clean.txt")
```

**Fix:** Add sanitization function (copy from parallels.py)
```python
def _sanitize_cache_filename(ref: str) -> str:
    """Sanitize a reference string to create a safe cache filename."""
    return re.sub(r'[^a-zA-Z0-9_\-]', '_', ref)

# Then at line 47:
safe_filename = _sanitize_cache_filename(ref)
cache_file = os.path.join(cache_dir, f"{safe_filename}_clean.txt")
```

**Testing:** Run desktop app, load Sefaria text, verify cache file created with safe name.

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
