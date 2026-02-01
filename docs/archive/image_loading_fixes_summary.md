# Image Loading Fixes Summary

**Date:** January 2025
**Issue:** Oxford and NLI images not loading correctly in web app; Oxford mapping issues in both web and desktop

---

## Problems Identified

### 1. NLI Collections (Cambridge, Antonin/Russian) - Web Only
- **Symptom:** Images not loading for some NLI collections (Antonin worked in desktop but not web)
- **Root Cause:** NLI blocks direct browser requests for certain collections (returns 403/500)
- **Solution:** Use server-side proxy (`/api/nli_image_by_sysid/`) for ALL NLI items, which adds proper headers (User-Agent, Referer)

### 2. Oxford Images Showing Wrong Folio - Both Web and Desktop
- **Symptom:** Multiple folios (e.g., f.21/18, f.21/19, f.21/20, f.21/21) all showing the same image (folio 1)
- **Root Cause:**
  - `oxford_full_db.json` only contains images for folios 1-17 in Part 1, but Part 1's `folio_range` is [1, 21]
  - When folio 21 is requested, it's not found in the images list, so code falls back to first available image
- **Solution:** Dynamically generate Oxford URLs for folios within the folio_range but missing from the database

### 3. Oxford Recto/Verso Navigation - Web Only
- **Symptom:** Navigating between page 1 and page 2 showed the same image
- **Root Cause:** Code was always generating the 'a' (recto) URL, ignoring the page parameter
- **Solution:** Use page parameter to select side: page=0 → 'a' (recto), page=1 → 'b' (verso)

### 4. Server Performance / "Connection Lost" - Web Only
- **Symptom:** Brief "Connection lost" messages during image loading
- **Root Cause:** Synchronous HTTP requests to fetch large images (2-3MB) blocking NiceGUI's event loop
- **Mitigation:** Added server-side image caching (10 minute TTL) and browser Cache-Control headers

---

## Files Modified

### `web/api.py`
- Added `_extract_folio_number()` function to extract folio from shelfmark (e.g., "f.21/21" → 21)
- Modified `/api/oxford_image/{sys_id}` endpoint:
  - Extracts folio number from shelfmark
  - Finds matching image in Part's images by `folio_num`
  - If not found but within `folio_range`, generates URL dynamically
  - Uses `page` parameter for recto (a) vs verso (b) selection
  - Added in-memory caching with 10-minute TTL
  - Added `Cache-Control: public, max-age=600` headers
- Added `/api/oxford_image_url/{sys_id}` endpoint (returns URL without proxying)
- Modified `/api/nli_image_by_sysid/{sys_id}` endpoint:
  - Added in-memory caching
  - Added Cache-Control headers
- Removed debug print statements

### `web/pages/browse.py`
- Changed NLI image loading to use unified server proxy for ALL non-Oxford items
- Removed debug print statements (`[IMAGE DEBUG]`)

### `web/services.py`
- Removed debug print statement (`[BROWSE SERVICE]`)

### `genizah_app.py` (Desktop)
- Added `_generate_oxford_dynamic_url()` helper function to generate Oxford URLs for missing folios
- Modified `load_images()` method in ManuscriptViewer:
  - Added `target_folio` parameter
  - Makes copies of image lists to avoid corrupting the cache
  - Adds dynamic Oxford images if target folio is missing but within Part's folio_range
- Updated all calls to `load_images()` to pass `target_folio` parameter:
  - Line ~3448: Main result dialog view
  - Line ~5985: Browse tab view
  - Line ~13540: Part browsing view

### `genizah_core.py`
- Removed debug print statements from `_load_heavy_caches_bg()`

---

## Technical Details

### Oxford URL Pattern
```
https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio}{side}.jpg
```
Example: `MS_HEB_f_21_21a.jpg` for MS. Heb. f. 21, folio 21, recto

### Dynamic URL Generation Logic
1. Extract folio number from shelfmark (e.g., "MS heb. f.21/21" → 21)
2. Check if Part metadata has `folio_range` (e.g., [1, 21])
3. If folio is within range but not in images list:
   - Parse Part ID to get letter and volume (e.g., "MS. Heb. f. 21/1" → f, 21)
   - Generate URL: `MS_HEB_{letter}_{volume}_{folio}{side}.jpg`

### Caching Strategy
- **Server-side:** In-memory dict with (sys_id, page) key, 10-minute TTL
- **Browser-side:** Cache-Control header allows 10-minute caching

---

## Testing Checklist

### Web App Tests

| Test | URL | Expected Result |
|------|-----|-----------------|
| Oxford folio in DB (1-17) | `/browse?sys_id=990053464040205171` | Shows folio 3 correctly |
| Oxford folio NOT in DB (18) | `/browse?sys_id=990053464190205171` | Shows folio 18 (dynamic URL) |
| Oxford folio NOT in DB (21) | `/browse?sys_id=990053464220205171` | Shows folio 21 (dynamic URL) |
| Oxford recto/verso nav | Navigate next on folio 21 | Shows 21a then 21b (different images) |
| Cambridge (NLI proxy) | `/browse?sys_id=990051344900205171` | T-S 20.33 image loads |
| Antonin (NLI proxy) | `/browse?sys_id=990000555810205171` | Ms. Evr. Antonin B 915 loads |
| Page caching | Reload same page | Should load faster from cache |

### Desktop App Tests

| Test | Shelfmark | Expected Result |
|------|-----------|-----------------|
| Oxford folio in DB | MS heb. f.21/3 | Shows folio 3 correctly |
| Oxford folio NOT in DB | MS heb. f.21/18 | Shows folio 18 (dynamic URL) |
| Oxford folio NOT in DB | MS heb. f.21/21 | Shows folio 21 (dynamic URL) |
| Recto/verso navigation | Toggle between sides | Shows different images |
| Other Oxford MS | Any other MS heb. | Verify correct folio mapping |

---

## Bugs Fixed in This Session

### Desktop Cache Corruption Bug
- **Symptom:** After viewing one Oxford manuscript, navigating to another showed images from the previous one, then crashed
- **Root Cause:** Initial fix modified the cached `images_ext` list directly, corrupting the metadata cache
- **Solution:** Make copies of image lists in `load_images()` before adding dynamic images

### Desktop Image Reverting Bug
- **Symptom:** Folios 18-21 briefly show the correct dynamically-generated image, then revert to showing folio 17
- **Root Cause:** Multiple functions were calling `set_page()` directly with an index calculated from the ORIGINAL cached metadata (without dynamic images):
  - `sync_external_view()` - called via `QTimer.singleShot(0, ...)` after enriched data loads
  - Browse tab page navigation - called when navigating within a Part
  - `_update_part_image_for_folio()` - called when navigating between folios in a Part
- **Solution:** Changed these functions to:
  1. Check if dynamic images are needed (folio not in database but within folio_range)
  2. If needed: call `load_images(meta, idx, target_folio=folio_num)` to generate dynamic images
  3. If not needed: use `set_page(idx)` with the viewer's current images list (preserves previously added dynamic images)

### Files Modified for Image Reverting Fix
- `genizah_app.py`:
  - `sync_external_view()` (~line 3598): Now conditionally calls `load_images()` or `set_page()` based on need
  - `browse_render_page()` (~line 13715): Same conditional logic applied
  - `_update_part_image_for_folio()` (~line 14217): Same conditional logic applied

---

## Known Limitations

1. **Incomplete Oxford Database:** `oxford_full_db.json` is missing some folio images. Dynamic URL generation works around this, but ideally the database should be updated.

2. **Brief Connection Lost:** Large images (2-3MB) still cause brief WebSocket delays. Caching helps on subsequent loads.

3. **No Async Image Fetching:** The server-side proxy fetches images synchronously. Making this async would improve responsiveness but requires significant refactoring.

---

## Future Improvements

1. **Update `oxford_full_db.json`** - Re-scrape Oxford to include all folio images
2. **Async Image Proxy** - Use background threads or async requests for image fetching
3. **Image Compression** - Serve smaller images for initial display, full resolution on demand
4. **Persistent Cache** - Use disk-based caching instead of in-memory for image data

---

## Verification Commands

### Test Oxford API directly:
```bash
# Get URL for folio 21 recto
curl "http://localhost:8081/api/oxford_image_url/990053464220205171?page=0"

# Get URL for folio 21 verso
curl "http://localhost:8081/api/oxford_image_url/990053464220205171?page=1"

# Compare image sizes (should be different)
curl -s "http://localhost:8081/api/oxford_image/990053464220205171?page=0" | wc -c
curl -s "http://localhost:8081/api/oxford_image/990053464220205171?page=1" | wc -c
```

### Check modified files:
```bash
git diff --name-only HEAD
git diff genizah_app.py  # Desktop changes
git diff web/api.py      # Web API changes
```
