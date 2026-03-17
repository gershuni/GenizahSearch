# Puzzle Image Processing on Production Server

## Problem Statement

The Fragment Puzzle requires **server-side image processing** (background removal via HSV segmentation in Python/NumPy/Pillow). On the production server (AWS EC2), the NLI IIIF image API blocks requests from the server IP, so the server cannot fetch manuscript images to process them.

### Current State (v7.0.0)

- **Desktop app**: Works perfectly. The user's local machine fetches IIIF images, processes them, caches results.
- **Local web dev**: Works perfectly. `localhost` can fetch from NLI IIIF.
- **Production web** (genizahsearch.com on AWS): Images load via `<img>` fallback (no CORS restriction on img tags), but **without background removal**. The puzzle is functional but degraded — fragments show with their original library scanning backgrounds (blue mats, gray/cream borders), making join reconstruction significantly harder.

### Why This Matters

Background removal is the core value of the puzzle feature. Without it:
- Fragments overlap with opaque backgrounds hiding adjacent pieces
- The visual "jigsaw" effect that makes joins discoverable is lost
- The feature ships at ~50% of its intended value on the web

### Technical Constraints

1. **NLI IIIF blocks server IPs**: `https://iiif.nli.org.il/IIIFv21/FL{digits}/full/{size},/0/default.jpg` returns 403/timeout from AWS EC2 IPs. Works from residential/university IPs and browsers.
2. **CORS blocks `fetch()` from browser**: NLI does not send `Access-Control-Allow-Origin` headers, so JavaScript `fetch()` cannot read response bodies. Only `<img>` tags work (CORS-exempt).
3. **Canvas taint**: Loading a cross-origin image into `<img>` without `crossOrigin="anonymous"` taints any canvas it's drawn on — `toBlob()` and `getImageData()` throw SecurityError.
4. **Background removal needs Python**: The HSV segmentation runs in NumPy/Pillow. No JavaScript/WebAssembly port exists.

### Architecture Reference

```
Current (working locally):
  Browser → /api/puzzle_image → Server fetches IIIF → bg removal → cached PNG → Browser

Current (production, degraded):
  Browser → /api/puzzle_image → Server fetch FAILS (NLI blocks) → 404
  Browser → <img src="IIIF URL"> → Image loads (no bg removal) → displayed raw

Desired:
  Browser → [some mechanism to get image bytes to server] → bg removal → cached PNG → Browser
```

### Files Involved

| File | Role |
|------|------|
| `web/api.py` | `/api/puzzle_image` (GET, server-side fetch), `/api/puzzle_process` (POST, accepts client bytes) |
| `web/pages/puzzle.py` | JS canvas code: `addFragment`, `_reloadFragment`, `navigateFolio` |
| `shared/puzzle_image_service.py` | IIIF fetch + cache + bg removal orchestration |
| `shared/background_removal.py` | HSV segmentation engine (NumPy + Pillow) |

---

## Solution Options

### Option A: Canvas-Based Byte Extraction (Recommended)

**Concept**: Load the IIIF image via `<img>` (CORS-exempt), but WITH `crossOrigin="anonymous"` set AFTER testing if NLI sends CORS headers. If NLI doesn't, use a lightweight server-side CORS proxy on our own domain.

**Implementation**:

1. Add a thin CORS proxy endpoint on our server:
   ```python
   @app.get('/api/iiif_proxy')
   def iiif_proxy(fl_id: str, size: int = 800):
       """Proxy IIIF requests, adding CORS headers.
       Fetches from NLI (or Cambridge CUDL) with browser-like headers."""
       # Construct URL
       digits = re.sub(r'\D', '', fl_id)
       url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/{size},/0/default.jpg"
       resp = requests.get(url, headers={
           'User-Agent': 'Mozilla/5.0...',
           'Referer': 'https://www.nli.org.il/'
       }, timeout=30)
       return Response(
           content=resp.content,
           media_type=resp.headers.get('content-type', 'image/jpeg'),
           headers={
               'Access-Control-Allow-Origin': '*',
               'Cache-Control': 'public, max-age=3600'
           }
       )
   ```
   **Problem**: This has the SAME issue — the server fetches from NLI, which blocks server IPs. So this only works if we can make the server's request look like a browser request (see Option E).

**Verdict**: Only works combined with Option E (proxy with browser-like headers that NLI accepts).

---

### Option B: Two-Stage Load (Load Display → Process Background)

**Concept**: Show the raw IIIF image immediately (current fallback), then use a hidden `<img>` + canvas to extract bytes for server processing.

**Implementation**:

1. Load IIIF image via `<img>` WITHOUT `crossOrigin` (current working fallback)
2. Show it on the Fabric.js canvas immediately (no bg removal)
3. In parallel, try loading the SAME URL with `crossOrigin="anonymous"`:
   - If NLI happens to send CORS headers → draw to canvas → `toBlob()` → POST to `/api/puzzle_process`
   - If CORS fails → image stays as-is (no bg removal)

**Pros**: Progressive enhancement. Image always shows. BG removal is best-effort.
**Cons**: NLI almost certainly doesn't send CORS headers, so bg removal would rarely work. This is basically the current degraded state with extra complexity.

**Verdict**: Not a real solution. CORS is the blocker.

---

### Option C: Server-Side Headless Browser (Puppeteer/Playwright)

**Concept**: Run a headless browser on the server that loads NLI IIIF images (as a "real browser"), extracts the bytes, and passes them to the Python bg removal pipeline.

**Implementation**:

1. Install Playwright or Puppeteer on the server
2. New endpoint or background worker:
   ```python
   async def fetch_via_browser(fl_id, size):
       from playwright.async_api import async_playwright
       async with async_playwright() as p:
           browser = await p.chromium.launch()
           page = await browser.new_page()
           url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/{size},/0/default.jpg"
           response = await page.goto(url)
           image_bytes = await response.body()
           await browser.close()
           return image_bytes
   ```
3. Use these bytes for bg removal + caching as normal

**Pros**: Server fetches as a real browser — NLI cannot distinguish from a real user.
**Cons**:
- Heavy dependency (Chromium binary ~200MB)
- Slow cold start (~2-3s per image for browser launch)
- Memory intensive on EC2
- Overkill for fetching a JPEG

**Verdict**: Works but heavy. Last resort.

---

### Option D: WebAssembly Background Removal (Client-Side Processing)

**Concept**: Port the background removal algorithm to run entirely in the browser using WebAssembly or pure JavaScript, eliminating the need for server-side processing.

**Implementation**:

1. Rewrite `shared/background_removal.py` logic in Rust → compile to WASM
   OR rewrite in pure JavaScript using typed arrays
2. Load the WASM/JS module in the browser
3. Process: `<img>` loads IIIF → draw to canvas → `getImageData()` → WASM bg removal → `putImageData()` → create processed image

**Key algorithm steps to port**:
- Corner sampling for background color detection (4 corners × 5×5 pixel blocks)
- Edge midpoint sampling for secondary background (CUL blue mats)
- HSV conversion per pixel
- Hue/saturation/value distance calculation
- Min foreground ratio safety check
- Alpha channel generation (RGBA output)

**Pros**:
- No server dependency for image processing
- Works everywhere (no CORS issues — processing happens client-side)
- Scales infinitely (no server CPU cost)
- Could be even faster than Python for small images

**Cons**:
- Significant development effort (2-4 days for WASM, 1-2 days for JS)
- Canvas taint still requires `crossOrigin="anonymous"` OR same-origin images
- Need to handle the CORS issue anyway to get pixel data from the image

**CORS workaround for this option**: Use an `<img>` tag with `crossOrigin="anonymous"` pointed at a same-origin proxy that simply pipes the bytes through:
```python
@app.get('/api/iiif_pipe/{fl_id}')
def iiif_pipe(fl_id: str, size: int = 800):
    # This still needs the server to fetch from NLI...
    # Same blocking issue as Option A
```

**Verdict**: Elegant long-term but doesn't solve the CORS issue. Still needs server-side fetch OR Option E.

---

### Option E: Server Fetch with Browser-Like Headers + Retry (Recommended First Try)

**Concept**: The server's IIIF fetch might be blocked not by IP but by User-Agent or missing headers. Try fetching with more realistic browser headers, cookies, and Referer.

**Investigation needed**:

1. SSH to server and test manually:
   ```bash
   # Test with curl and browser-like headers
   curl -v -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
        -H "Referer: https://www.nli.org.il/" \
        -H "Accept: image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8" \
        -o /tmp/test.jpg \
        "https://iiif.nli.org.il/IIIFv21/FL990051753360205171/full/800,/0/default.jpg"

   # Check if it worked
   file /tmp/test.jpg
   ```

2. If headers alone don't work, try different IIIF URL patterns:
   ```bash
   # Some IIIF servers respond differently to size syntax
   curl -o /tmp/test2.jpg "https://iiif.nli.org.il/IIIFv21/FL990051753360205171/full/!800,800/0/default.jpg"
   ```

3. If NLI truly blocks by IP range, this won't work. But it's worth 5 minutes of testing before building complex solutions.

**Verdict**: Test this first. If it works, the current architecture needs zero changes.

---

### Option F: External Image Proxy Service

**Concept**: Use a third-party proxy or CDN that can fetch NLI IIIF images on the server's behalf.

**Options**:
- **Cloudflare Workers**: Our domain already uses Cloudflare. A Worker can proxy IIIF requests, adding CORS headers and appearing as a CDN edge node (not an AWS IP).
  ```javascript
  // Cloudflare Worker
  export default {
    async fetch(request) {
      const url = new URL(request.url);
      const fl_id = url.searchParams.get('fl_id');
      const size = url.searchParams.get('size') || '800';
      const digits = fl_id.replace(/\D/g, '');
      const iiifUrl = `https://iiif.nli.org.il/IIIFv21/FL${digits}/full/${size},/0/default.jpg`;

      const resp = await fetch(iiifUrl, {
        headers: { 'Referer': 'https://www.nli.org.il/' }
      });

      const newResp = new Response(resp.body, resp);
      newResp.headers.set('Access-Control-Allow-Origin', '*');
      return newResp;
    }
  }
  ```
- **imgproxy** (self-hosted on a different IP/VPS)
- **Cloudinary** (free tier: 25K transformations/month)

**Pros**: Clean separation. No server code changes (just change the IIIF base URL).
**Cons**: Additional infrastructure. Rate limiting concerns (NLI might block the proxy too).

**Verdict**: Cloudflare Worker is the cleanest if Option E fails. Same Cloudflare account, no new infra.

---

### Option G: Pre-Cache from Desktop / Bulk Download

**Concept**: Use the desktop app (which CAN fetch from NLI) to pre-cache processed images, then sync the cache to the server.

**Implementation**:

1. Desktop user opens a puzzle → images are fetched, processed, cached locally
2. On publish, upload not just the composite PNG but also the individual processed fragment PNGs to Supabase storage
3. Web puzzle loads from Supabase storage (our own domain, no CORS) instead of NLI IIIF

**Or bulk approach**:
1. Script runs on a machine with NLI access
2. Pre-processes the most common Cambridge/NLI fragments
3. Uploads processed PNGs to S3/Supabase storage
4. Web puzzle checks storage first, falls back to raw IIIF

**Pros**: Guaranteed to work. No proxy/CORS issues.
**Cons**: Only works for pre-cached images. New fragments still need processing.

**Verdict**: Good complement to other solutions, not a standalone fix.

---

## Recommended Approach

### Phase 1 (Immediate — 5 minutes)
**Option E**: SSH to server, test curl with browser-like headers. If NLI responds, the fix is adding those headers to `_fetch_iiif_image()`. This is what the code already tries (line 157-159 of `puzzle_image_service.py`) but the headers may need updating.

### Phase 2 (If Option E fails — 1-2 hours)
**Option F (Cloudflare Worker)**: Set up a Cloudflare Worker at `genizahsearch.com/iiif-proxy/` that proxies NLI IIIF requests. The Worker runs on Cloudflare's edge (not AWS), so NLI sees a CDN IP. Update `puzzle_image_service.py` to use this proxy URL when running on the server. Also enables client-side CORS fetch for future Option D.

### Phase 3 (Long-term — 2-4 days)
**Option D (WebAssembly)**: Port bg removal to WASM. Combined with the Cloudflare proxy (for CORS), this eliminates server-side processing entirely. The puzzle becomes fully client-side.

### Phase 4 (Complement)
**Option G**: On puzzle publish, also upload individual processed fragment PNGs. Web puzzle tries these first before falling back to IIIF.

---

## Current Code Flow Reference

### Adding a fragment (web):
```
Python: _add_fragment_by_sys_id() → resolve fl_id → build URL → run JS addFragment(key, url, meta)
JS: addFragment() → new Image().src = /api/puzzle_image?fl_id=X
  → onload: add to Fabric canvas, emit puzzle-add-result
  → onerror: fallback to direct IIIF <img> (no bg removal)
```

### Server-side image resolution:
```
/api/puzzle_image?fl_id=X&threshold=30&size=800&processed=true
  → PuzzleImageService.resolve_fragment_image()
    → check cache (cache/puzzle/{safe_id}_{size}_{threshold}.png)
    → if miss: _fetch_iiif_image(fl_id, size) → requests.get(NLI URL)
    → if fetched: remove_background(raw_bytes, threshold) → cache → return
    → if fetch fails: return None → 404
```

### Cache structure:
```
cache/puzzle/
  FL990051753360205171_800_30.0.png      (processed, threshold 30)
  FL990051753360205171_800_30.0_cul.png  (CUL blue mat removal)
  FL990051753360205171_800_original.jpg  (unprocessed original)
```

---

## Environment Details

| Item | Value |
|------|-------|
| Server | AWS EC2, Ubuntu 24.04, us-west-2 |
| Domain | genizahsearch.com (Cloudflare proxied) |
| Python | 3.12 (venv at /home/ubuntu/GenizahSearch/venv/) |
| NLI IIIF | https://iiif.nli.org.il/IIIFv21/ |
| Cambridge IIIF | Various CUDL endpoints (also may block server) |
| Cache dir | /home/ubuntu/GenizahSearch/cache/puzzle/ (auto-created) |
| Dependencies | numpy 2.4.3, Pillow 12.1.1 (installed in venv) |

---

## Testing Commands (on server)

```bash
# Test if NLI IIIF responds to server
curl -s -o /tmp/nli_test.jpg -w "%{http_code}" \
  -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36" \
  -H "Referer: https://www.nli.org.il/" \
  "https://iiif.nli.org.il/IIIFv21/FL990051753360205171/full/800,/0/default.jpg"

# Check if it's a real image
file /tmp/nli_test.jpg

# Test Cambridge CUDL
curl -s -o /tmp/cudl_test.jpg -w "%{http_code}" \
  "https://images.lib.cam.ac.uk/iiif/MS-TS-00012-00001-00001.jp2/full/800,/0/default.jpg"

# Test our puzzle_image endpoint
curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:8081/api/puzzle_image?fl_id=990051753360205171&threshold=30&size=800"

# Test puzzle_process endpoint with dummy data
curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/octet-stream" \
  --data-binary @/tmp/nli_test.jpg \
  "http://localhost:8081/api/puzzle_process?fl_id=990051753360205171&threshold=30&size=800&processed=true"
```
