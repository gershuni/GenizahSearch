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

### Phase 1 — TESTED AND RULED OUT
**Option E**: Tested on 2026-03-17. Server curl with full browser headers returns HTTP 500 (XML error response). NLI blocks by IP range, not by headers. **This option does not work.**

### Phase 2 — TESTED AND RULED OUT
**Option F (Cloudflare Worker)**: Tested on 2026-03-17. Cloudflare Worker deployed and routed, but NLI returned upstream HTTP 500 from Cloudflare edge IPs as well. NLI blocks datacenter/CDN-class IPs broadly, not just AWS. **This option does not work.** Do not deploy `PUZZLE_IIIF_MODE=proxy`.

### Phase 2a — IMPLEMENTED (Localhost Helper)
**Localhost helper service**: A small HTTP service running on the user's own machine (`http://127.0.0.1:43111`) that reuses the existing Python puzzle image pipeline. The user's residential/university IP can reach NLI. The web page falls back to this helper when server-side fetch fails. Opt-in via `?local_helper=1` URL param or `localStorage.puzzleLocalHelperEnabled = '1'`. See `scripts/puzzle_local_helper.py`.

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

---

## Cloudflare Worker Implementation Plan (Option F)

### Overview

Deploy a Cloudflare Worker at `genizahsearch.com/iiif-proxy/` that proxies NLI IIIF image requests. The Worker runs on Cloudflare's global edge network, so NLI sees a Cloudflare IP (not the blocked AWS EC2 IP). The Worker adds CORS headers so both server-side `requests.get()` and browser-side `fetch()` work.

### Next-Agent Guardrails

1. Make the Worker-backed server fetch the normal production path. Do not keep the current browser-upload flow as the main architecture if the Worker restores server-side fetching.
2. Update all three puzzle image load paths together: `addFragment`, `_reloadFragment`, and `navigateFolio`.
3. If AWS->NLI is known-broken, avoid paying a failing direct fetch on every image forever. Use an environment flag or short circuit-breaker so production can go straight to the proxy.
4. Treat `POST /api/puzzle_process` as temporary unless there is a remaining real use case. If kept, harden it before relying on it.

### Architecture

```
Current (broken):
  EC2 Server → NLI IIIF → 500 (blocked)

With Worker:
  EC2 Server → genizahsearch.com/iiif-proxy/FL{id}/... → Cloudflare Edge → NLI IIIF → 200 ✓
  Browser → genizahsearch.com/iiif-proxy/FL{id}/... → Cloudflare Edge → NLI IIIF → 200 ✓ (with CORS)
```

### Step 1: Create the Worker

In the Cloudflare Dashboard:
1. Go to **Workers & Pages** → **Create application** → **Create Worker**
2. Name it `iiif-proxy`
3. Replace the default code with:

```javascript
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Only handle /iiif-proxy/ paths
    const path = url.pathname;
    if (!path.startsWith('/iiif-proxy/')) {
      return new Response('Not found', { status: 404 });
    }

    // Extract the IIIF path after /iiif-proxy/
    // e.g., /iiif-proxy/FL990051753360205171/full/800,/0/default.jpg
    const iiifPath = path.replace('/iiif-proxy/', '');
    if (!iiifPath || !iiifPath.startsWith('FL')) {
      return new Response('Invalid IIIF path', { status: 400 });
    }

    // Validate: only allow FL{digits} pattern to prevent open proxy abuse
    const flMatch = iiifPath.match(/^FL(\d+)\//);
    if (!flMatch) {
      return new Response('Invalid FL ID format', { status: 400 });
    }

    // Build the upstream NLI IIIF URL
    const upstreamUrl = `https://iiif.nli.org.il/IIIFv21/${iiifPath}`;

    try {
      const upstreamResp = await fetch(upstreamUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Referer': 'https://www.nli.org.il/',
          'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        },
        cf: {
          // Cache on Cloudflare edge for 24 hours
          cacheTtl: 86400,
          cacheEverything: true,
        },
      });

      if (!upstreamResp.ok) {
        return new Response(`Upstream error: ${upstreamResp.status}`, {
          status: upstreamResp.status,
        });
      }

      // Return with CORS headers so both server and browser can use it
      const response = new Response(upstreamResp.body, {
        status: upstreamResp.status,
        headers: {
          'Content-Type': upstreamResp.headers.get('Content-Type') || 'image/jpeg',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=86400',
          'X-Proxy': 'genizah-iiif-proxy',
        },
      });

      return response;
    } catch (err) {
      return new Response(`Proxy error: ${err.message}`, { status: 502 });
    }
  },
};
```

4. Click **Deploy**

### Step 2: Route the Worker to the Domain

1. In the Worker settings, go to **Triggers** → **Routes**
2. Add route: `genizahsearch.com/iiif-proxy/*`
3. Select the `iiif-proxy` Worker
4. Save

### Step 3: Test the Worker

```bash
# From the server (should now return 200 + JPEG)
curl -s -o /tmp/proxy_test.jpg -w "%{http_code}" \
  "https://genizahsearch.com/iiif-proxy/FL990051753360205171/full/800,/0/default.jpg"
echo ""
file /tmp/proxy_test.jpg

# From browser console (should work with CORS)
fetch('https://genizahsearch.com/iiif-proxy/FL990051753360205171/full/800,/0/default.jpg')
  .then(r => r.blob())
  .then(b => console.log('Got', b.size, 'bytes'))
```

### Step 4: Update Server Code

**File: `shared/puzzle_image_service.py`**

Change `_fetch_iiif_image` to use the proxy when the direct fetch fails. Prefer making the order configurable so production can skip the known-broken direct fetch after verification:

```python
NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
NLI_IIIF_PROXY = "https://genizahsearch.com/iiif-proxy"  # Cloudflare Worker
PUZZLE_IIIF_MODE = os.environ.get("PUZZLE_IIIF_MODE", "auto")  # auto | direct | proxy

def _fetch_iiif_image(self, fl_id: str, size: int) -> Optional[bytes]:
    """Fetch image from NLI IIIF, with Cloudflare proxy fallback."""
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return None

    if PUZZLE_IIIF_MODE == "proxy":
        base_urls = [NLI_IIIF_PROXY]
    elif PUZZLE_IIIF_MODE == "direct":
        base_urls = [NLI_IIIF_BASE]
    else:
        base_urls = [NLI_IIIF_BASE, NLI_IIIF_PROXY]

    for base_url in base_urls:
        url = f"{base_url}/FL{digits}/full/{size},/0/default.jpg"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content
        except Exception as e:
            logger.warning(f"IIIF fetch failed for {fl_id} via {base_url}: {e}")
            continue

    logger.error(f"All IIIF sources failed for {fl_id}")
    return None
```

This way:
- Local dev: direct NLI works, proxy is never tried
- Production: direct NLI returns 500, falls back to proxy → works
- Desktop app: uses its own fetch (unaffected)

Recommended production setting after validation:

```bash
export PUZZLE_IIIF_MODE=proxy
```

In production, the goal is to skip the known-broken AWS -> NLI fetch entirely and go straight to Cloudflare. Local dev can still use `auto` or `direct`.

### Step 5: Update All Puzzle Load Paths Together

Once the server can fetch via proxy, the browser should again prefer `/api/puzzle_image` as the normal path. Do not patch only `addFragment` and `_reloadFragment`; `navigateFolio` must be updated in the same pass.

Required touch points:

- `web/pages/puzzle.py` `addFragment`
- `web/pages/puzzle.py` `_reloadFragment`
- `web/pages/puzzle.py` `navigateFolio`

The easy regression to miss is folio navigation: initial fragment load can work while prev/next folio still fails in production.

### Step 6: Simplify or Harden `/api/puzzle_process`

If the Worker restores server-side image fetching reliably, the browser `fetch()` -> `POST` raw bytes -> server process flow should no longer be the normal production path.

Two acceptable outcomes:

1. Remove `/api/puzzle_process` from the primary path and keep it only as a temporary fallback/diagnostic tool.
2. Keep it, but harden it before relying on it:
   - limit request size
   - validate content type
   - avoid caching arbitrary caller-supplied bytes without a provenance check
   - consider binding the upload to a server-issued token/nonced miss instead of trusting query params alone

If a browser-side fallback is still needed, update the JS fallback to use the proxy URL instead of NLI directly:

```javascript
// In addFragment onerror:
var iiifUrl = '/iiif-proxy/FL' + digits + '/full/' + iiifSize + ',/0/default.jpg';
// This is same-origin, so fetch() works with CORS
fetch(iiifUrl).then(r => r.blob()).then(blob => {
    // POST to /api/puzzle_process for bg removal
    ...
});
```

Preferred steady state:

```text
Browser -> /api/puzzle_image -> server fetches via Worker -> bg removal -> cache -> browser
```

### Cost & Limits

- **Cloudflare Workers free tier**: 100,000 requests/day — more than enough for puzzle usage
- **No additional infrastructure** — runs on existing Cloudflare account
- **Edge caching**: `cacheTtl: 86400` means Cloudflare caches images at the edge for 24h, reducing NLI load
- **Latency**: Cloudflare edge is typically faster than direct EC2→NLI due to global distribution

### Security Considerations

- Validate the size segment too (`400`, `800`, `1200`, `2000`) instead of proxying arbitrary IIIF path variants.
- If `/api/puzzle_process` remains enabled, it needs separate hardening; the Worker does not fix that endpoint's trust boundary.

- The Worker only proxies paths starting with `FL` followed by digits — not an open proxy
- Rate limiting can be added via Cloudflare's built-in rate limiting rules
- The Worker doesn't store or process data — pure pass-through with CORS headers

### Rollback

If the Worker has issues:

1. Remove or disable the Cloudflare route
2. Set `PUZZLE_IIIF_MODE=direct` or revert the server fetch change
3. Fall back to the current degraded raw-image behavior if necessary

Note: if the code is changed to depend on the Worker path in production, removing the route alone is not a complete rollback.

### Suggested Implementation Order

1. Deploy and verify the Worker with `curl` from the server and `fetch()` from the browser
2. Update `shared/puzzle_image_service.py` to support proxy mode
3. Set production to `PUZZLE_IIIF_MODE=proxy`
4. Update `web/pages/puzzle.py` so `addFragment`, `_reloadFragment`, and `navigateFolio` all use the same image-loading strategy
5. Verify:
   - first fragment load
   - threshold reload
   - processed/original toggle
   - prev/next folio navigation
   - second load hits cache
6. Remove or harden `/api/puzzle_process`

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
