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

### Bug #16: Parallels Export Buttons Not Working
**Symptom:** Clicking export buttons does nothing
**Root Cause:** `state.parallels_results` might be empty when user clicks export

**Investigation needed:**
1. Check if results are stored in `state.parallels_results` after search
2. The code at `parallels.py:1019` sets `state.parallels_results = main_results`
3. API reads from `state.parallels_results` at `api.py:593`

**Likely issue:** State is per-user session, but API runs in different context

**Fix approach:**
```python
# In parallels.py, store results in user storage that API can access
# At line 1019-1022, verify this code runs:
state.parallels_results = main_results
state.parallels_filtered = filtered_results
app.storage.user['parallels_results'] = main_results
```

**Testing:**
1. Go to /parallels
2. Run a search with results
3. Click Export Excel button
4. Verify file downloads

---

### Bug #10: List Export "List is empty" Error
**Symptom:** Export fails with "List is empty" message
**Files:** `web/api.py:674-676`, `web/pages/lists.py:547-549`

**Investigation:**
1. `lists.py:547` calls `get_items_in_list_sync(list_id)`
2. If items exist, triggers download via `/api/export/list/{list_id}/excel`
3. API at `api.py:674` also calls `get_items_in_list_sync(list_id)`

**Possible causes:**
- List ID mismatch between page and API
- Items stored differently for logged-in vs anonymous users
- Race condition between check and export

**Fix approach:** Add debug logging to trace the issue
```python
# In api.py around line 674:
print(f"[DEBUG] Exporting list {list_id}, items count: {len(items) if items else 0}")
print(f"[DEBUG] Lists data keys: {list(state.lists_mgr.data.get('lists', {}).keys())}")
```

**Testing:**
1. Create a list and add items
2. Click Export Excel
3. Check console for debug output
4. Verify file downloads

---

### Bug #15: Comments Not Shown in Browse
**Symptom:** Comments added but not displayed
**Files:** `web/components/notes_display.py`, `web/pages/browse.py:2065-2070`

**Code flow:**
1. `browse.py:2067` creates notes panel with `create_notes_panel()`
2. Panel calls `fetch_document_comments()` at `notes_display.py:72-99`
3. API call to `/comments/document/{document_id}`

**Possible causes:**
- API endpoint not returning comments
- Comments stored with different document_id format
- Panel not expanded by default (user must click to see)

**Fix approach:** Verify API returns data
```python
# Add debug in notes_display.py around line 83:
print(f"[DEBUG] Fetching comments for doc {document_id}, page {page_number}")
result = await api_call(...)
print(f"[DEBUG] API result: {result}")
```

**Testing:**
1. Go to /browse with a manuscript
2. Add a comment via the comment button
3. Refresh page
4. Click "Notes & Comments" expansion panel
5. Verify comment appears

---

### Bugs #8, #9: Export Word/Excel Formatting
**Symptom:** Word needs RTL alignment, Excel needs line breaks fixed
**File:** `web/export_service.py`

**For Word RTL (search export_service.py for paragraph creation):**
```python
# Ensure RTL paragraphs have proper alignment
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT

para = doc.add_paragraph(hebrew_text)
para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
para.paragraph_format.right_to_left = True
```

**For Excel line breaks:**
```python
# Replace newlines with spaces or use text wrapping
text = text.replace('\n', ' ').replace('\r', '')
# OR enable text wrap:
cell.alignment = Alignment(wrap_text=True)
```

**Testing:**
1. Run a Hebrew text search
2. Export to Word - verify text is right-aligned
3. Export to Excel - verify no broken lines in cells

---

## Execution Order

When user returns, execute fixes in this order:

1. **Quick wins (5 min each):**
   - [ ] Fix `filter_text_dialog.py` path traversal
   - [ ] Update checklist to mark already-fixed items

2. **Debug & fix (15-30 min each):**
   - [ ] Debug parallels export (#16)
   - [ ] Debug list export (#10)
   - [ ] Debug comments display (#15)

3. **Export formatting (30 min):**
   - [ ] Word RTL alignment (#8)
   - [ ] Excel line breaks (#9)

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
