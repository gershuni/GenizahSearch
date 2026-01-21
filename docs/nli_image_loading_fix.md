# NLI Image Loading - Technical Documentation

## Overview

This document describes the fixes implemented to resolve NLI (National Library of Israel) image loading issues in the GenizahSearch web application. The fixes ensure that page-specific images are displayed correctly when browsing manuscripts.

## Problem Statement

### Original Issues

1. **Stale FL IDs**: The transcription data contains FL (File) IDs that are outdated/invalid. When the web app tried to load images using these IDs, NLI returned 500 errors or placeholder images.

2. **Single FL ID from MARC API**: The original fallback used NLI's MARC API (`/marc/bib/{system_id}`) which only returns **one FL ID per manuscript**, not per page. This meant all pages showed the same image.

3. **No Page-Specific Images**: When navigating between pages, the image didn't update to show the correct page.

4. **Rate Limiting**: Frequent requests to NLI APIs could trigger rate limiting, causing images to stop loading.

## Solution Architecture

### Image Loading Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Page Navigation                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. Try Primary URL: /api/nli_image/{fl_digits}?t={page_num}     │
│    (Uses FL ID from transcription data)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                    Image fails to load (onerror)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Fallback URL: /api/nli_image_by_sysid/{sys_id}?page={idx}    │
│    (Fetches fresh FL IDs from IIIF manifest)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. IIIF Manifest: Contains ALL FL IDs for all pages             │
│    URL: /DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest               │
│    Returns: canvas_map with FL IDs in page order                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Select correct FL ID using page index                         │
│    fl_ids[page_index] → specific page image                      │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Browse Page (`web/pages/browse.py`)

**Image URL Generation:**
```python
# Compute FL ID digits from page data
fl_digits = ""
if fl_id:
    fl_digits = re.sub(r"\D", "", str(fl_id))

# Choose image endpoint
if is_oxford and page.sys_id:
    # Oxford manuscripts use CodicologicalManager
    img_url = f"/api/oxford_image/{page.sys_id}?page={page_idx}"
    fallback_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}"
elif fl_digits:
    # Primary: Use FL ID from transcription (may be stale)
    img_url = f"/api/nli_image/{fl_digits}?t={page.p_num}"
    # Fallback: Use sys_id endpoint with fresh FL IDs
    fallback_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}"
elif page.sys_id:
    # No FL ID in data, use sys_id endpoint directly
    img_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}"
```

**HTML with Fallback:**
```html
<img src="{primary_url}"
     onerror="handleImageError(this, '{fallback_url}')"
/>
```

#### 2. API Endpoints (`web/api.py`)

**`/api/nli_image/{fl_id}`**
- Fetches image directly using FL ID
- Tries IIIF endpoint first, then Rosetta
- Returns 404 if image not found or is placeholder (< 2KB)

**`/api/nli_image_by_sysid/{sys_id}?page={idx}`**
- Fetches FL IDs from IIIF manifest (not MARC)
- Uses `page` parameter to select specific FL ID
- Results are cached for 5 minutes

**FL ID Fetching (IIIF Manifest):**
```python
def fetch_fl_ids_from_nli(system_id: str) -> list:
    # IIIF manifest contains ALL page images
    url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest"

    # Extract FL IDs from canvas sequence
    for canvas in data['sequences'][0]['canvases']:
        service_id = canvas['images'][0]['resource']['service']['@id']
        fl_match = re.search(r'FL(\d+)', service_id)
        if fl_match:
            fl_ids.append(fl_match.group(1))

    return fl_ids  # Ordered list of all page FL IDs
```

#### 3. Caching

To prevent NLI rate limiting, FL IDs are cached:

```python
def fetch_fl_ids_from_nli(system_id: str, _cache={}, _cache_time={}) -> list:
    CACHE_TTL = 300  # 5 minutes

    # Check cache first
    if system_id in _cache:
        cache_age = time.time() - _cache_time.get(system_id, 0)
        if cache_age < CACHE_TTL:
            return _cache[system_id]

    # Fetch and cache
    fl_ids = ... # fetch from NLI
    _cache[system_id] = fl_ids
    _cache_time[system_id] = time.time()
    return fl_ids
```

### MARC API vs IIIF Manifest

| Aspect | MARC API | IIIF Manifest |
|--------|----------|---------------|
| Endpoint | `/marc/bib/{sys_id}` | `/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest` |
| FL IDs returned | 1 (first only) | All pages |
| Data format | XML | JSON |
| Use case | Metadata | Image sequences |

**Example MARC Response:**
```xml
<datafield tag="907">
    <subfield code="d">FL148834961</subfield>
</datafield>
```
Only contains one FL ID.

**Example IIIF Manifest Response:**
```json
{
  "sequences": [{
    "canvases": [
      {"images": [{"resource": {"service": {"@id": ".../FL148834961/..."}}}]},
      {"images": [{"resource": {"service": {"@id": ".../FL148834976/..."}}}]}
    ]
  }]
}
```
Contains FL IDs for all pages in order.

## Edit Dialog Image Loading

The edit transcription dialog (`web/components/text_editor.py`) also displays the manuscript image:

```python
# Build fallback URL using document_id
fallback_url = f"/api/nli_image_by_sysid/{document_id}?page={page_number - 1}"

img_html = f'''
<img src="{primary_url}"
     onerror="if(this.src !== '{fallback_url}') {{ this.src='{fallback_url}'; }}"
/>
'''
```

## Server Management

Added reboot functionality to `start_servers.py`:

- Press `r` to reboot both backend and web servers
- Press `Ctrl+C` to stop and exit
- Uses threaded output reading for non-blocking keyboard input
- Flushes keyboard buffer between reboots

## Files Modified

1. **`web/api.py`**
   - Updated `fetch_fl_ids_from_nli()` to use IIIF manifest
   - Added caching with 5-minute TTL
   - Added `page` parameter to `nli_image_by_sysid` endpoint

2. **`web/pages/browse.py`**
   - Fixed image URL generation with fallback
   - Added cache-buster parameter (`?t={page_num}`)
   - Fixed new search to start at page 1
   - Removed debug labels

3. **`web/components/text_editor.py`**
   - Added HTML img with onerror fallback
   - Fixed height to 65vh

4. **`start_servers.py`**
   - Added 'r' key for reboot
   - Threaded output reading
   - Keyboard buffer flushing

## Testing

1. Navigate to an NLI manuscript (e.g., T-S 8J6.1)
2. Verify first page image loads
3. Navigate to page 2 - image should update
4. Search for a different manuscript - should start at page 1
5. Open edit dialog - image should display

## Known Limitations

1. **Stale FL IDs**: Primary URLs using FL IDs from transcription data will fail and fall back to sys_id endpoint. This adds a small delay.

2. **Cache Duration**: FL IDs are cached for 5 minutes. If NLI updates their data, it won't reflect immediately.

3. **Oxford Manuscripts**: Use a separate system (CodicologicalManager) which maps sys_id to Oxford Part IDs.
