# Image Loading Investigation Handoff

> Date: 2026-01-23
> Status: **In Progress** - Oxford images not loading in production

---

## Summary

After deploying GenizahSearch to AWS EC2, images from both NLI (National Library of Israel) and Oxford (Bodleian Library) were not loading. We fixed NLI images but Oxford images still have an issue.

---

## Issue 1: NLI Images - FIXED ✅

### Problem
NLI blocks image requests from AWS datacenter IPs. Server-side proxy returned 403/503 errors.

### Solution Implemented
Changed from server proxy to **direct browser loading**:

```python
# OLD (blocked):
img_url = f"/api/nli_image/{fl_digits}?t={page.p_num}"

# NEW (works):
NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
img_url = f"{NLI_IIIF_BASE}/FL{fl_digits}/full/max/0/default.jpg"
```

Added JavaScript fallback that fetches IIIF manifest client-side when FL IDs are stale.

### Files Modified
- `web/pages/browse.py` - Direct NLI URLs, JS fallback
- `web/pages/viewer.py` - Direct NLI URLs
- `web/components/text_editor.py` - Direct NLI URLs, JS fallback

### Commits
- `32a791b` - Fix: Use direct NLI URLs for images (bypass server blocking)
- `068b2fe` - Docs: Add production deployment section for NLI image loading

---

## Issue 2: Oxford Images - NOT FIXED ❌

### Problem
Oxford manuscripts (shelfmarks starting with "MS heb" or "MS. Heb") show "Image not available".

### Investigation Findings

#### 1. Oxford Database IS Loading Correctly
```
INFO: Loaded 3764 codicological parts from Oxford database
```

#### 2. Oxford API Endpoints Work When Called Directly
```bash
# This works - returns 30 images:
curl 'http://localhost:8081/api/oxford_images/990053474490205171'
# Returns: {"part_id":"MS. Heb. f. 43/2","images":[...30 images...]}

# This works - returns 4.4MB image:
curl 'http://localhost:8081/api/oxford_image/990053474490205171?page=0'
# Status: 200, Size: 4475385
```

#### 3. Oxford Server Does NOT Block AWS IPs
```bash
curl -H "User-Agent: Mozilla/5.0" \
     "https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_43_39a.jpg"
# Status: 200, Size: 4475385
```

#### 4. Folio Mappings Are Correct
- 11,762 sys_id → part_id mappings loaded
- Example: `990053474490205171` → `MS. Heb. f. 43/2`
- 16 sys_ids map to part `MS. Heb. f. 43/2`

#### 5. Metadata Returns Correct Shelfmark
```python
m.get_meta_for_id('990053474490205171')
# Returns: ('MS heb. f.43/39', 'פיוט. ; Piyyut...')
```

#### 6. Oxford Detection Pattern Works
```python
shelfmark = 'MS heb. f.43/39'
is_oxford = shelfmark.lower().startswith('ms heb')  # True
```

#### 7. Browse Map Has Oxford Entries
```python
browse_map['990053474490205171']
# Returns: [{'p_num': 1, 'uid': 'IE168047677_P000001_FL168047680', ...}, ...]
```

### The Mystery
All the backend components work correctly when tested individually, but the browse page isn't calling the Oxford API endpoint. Logs show NO requests to `/api/oxford_image/` when viewing Oxford manuscripts.

### Current Code Path (browse.py lines 1596-1604)
```python
if is_oxford and page.sys_id:
    has_image = True
    img_url = f"/api/oxford_image/{page.sys_id}?page={page_idx}{cache_bust}"
    if fl_digits:
        fallback_url = f"{NLI_IIIF_BASE}/FL{fl_digits}/full/max/0/default.jpg"
    else:
        fallback_url = None
```

### Possible Causes (To Investigate)
1. **page.sys_id might be None** - Need to verify page object has sys_id populated
2. **page.shelfmark might be None** - Oxford detection requires shelfmark
3. **JavaScript error** - The `handleImageError` function might be failing
4. **Image tag not rendering** - has_image might be False
5. **FL ID taking precedence** - If fl_digits exists, might be using NLI path instead

### Next Steps to Debug
1. **Check browser Network tab** when viewing Oxford manuscript - what URL is being requested?
2. **Right-click broken image → Inspect** - what is the `src` attribute?
3. **Add console.log** in browse.py to print `is_oxford`, `page.sys_id`, `page.shelfmark`, `fl_digits`
4. **Check if Oxford path is reached** - add `print()` before line 1596

### Test Commands for Server
```bash
# Check if Oxford API works:
curl -s 'http://localhost:8081/api/oxford_images/990053474490205171' | python3 -m json.tool

# Check actual image fetch:
curl -s -o /dev/null -w 'Status: %{http_code}, Size: %{size_download}\n' \
  'http://localhost:8081/api/oxford_image/990053474490205171?page=0'

# Watch logs while browsing:
sudo journalctl -u genizah-web -f | grep -i oxford
```

### Test Oxford Manuscript
- Shelfmark: MS heb. f.43/2
- Known working sys_id: `990053474490205171`
- Browse URL: `https://genizahsearch.com/browse?sys_id=990053474490205171`

---

## Architecture Reference

### NLI Image Loading (FIXED)
```
Browser → NLI IIIF directly (https://iiif.nli.org.il/...)
  ↓ (on error)
Browser fetches IIIF manifest → extracts FL IDs → retries
```

### Oxford Image Loading (BROKEN)
```
Browser → /api/oxford_image/{sys_id}?page={idx}
  ↓
Server → CodicologicalManager.get_part_for_folio(sys_id)
  ↓
Server → CodicologicalManager.get_part_images(part_id)
  ↓
Server → Fetch from hebrew.bodleian.ox.ac.uk
  ↓
Return image to browser
```

### Key Files
| File | Purpose |
|------|---------|
| `web/pages/browse.py` | Main manuscript viewer, image URL generation |
| `web/api.py` | `/api/oxford_image/` and `/api/nli_image/` endpoints |
| `genizah_core.py` | `CodicologicalManager` class for Oxford mappings |
| `oxford_full_db.json` | Oxford parts database (3764 parts) |
| `libraries.csv` | Sys_id to Oxford part mappings |

### Key Functions
| Function | Location | Purpose |
|----------|----------|---------|
| `CodicologicalManager.load()` | genizah_core.py:1895 | Load Oxford database |
| `CodicologicalManager.get_part_for_folio()` | genizah_core.py | Map sys_id → part_id |
| `CodicologicalManager.get_part_images()` | genizah_core.py:2075 | Get image URLs for part |
| `oxford_image()` | web/api.py:208 | API endpoint for Oxford images |
| `handleImageError()` | web/pages/browse.py:31 | JS fallback for failed images |

---

## Server Details
- Host: `ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com`
- App path: `/home/ubuntu/GenizahSearch`
- Services: `genizah-backend`, `genizah-web`
- Logs: `sudo journalctl -u genizah-web -f`

---

## Related Documentation
- `docs/nli_image_loading_fix.md` - Full NLI image loading documentation
- `docs/DEPLOYMENT_TECHNICAL.md` - Server deployment guide
