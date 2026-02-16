from nicegui import app
from fastapi import Response
from fastapi.responses import RedirectResponse
from web.state import state
from web.export_service import get_export_service, encode_filename_for_header
import requests
import re
import os
from genizah_core import Config
from urllib.parse import urlparse

# NOTE: Backend API routes removed - now using Supabase directly
# Auth, users, corrections, comments, discoveries all go through Supabase

# Cache TTL values (configurable via environment variables)
NLI_CACHE_TTL = int(os.environ.get('NLI_CACHE_TTL', '300'))  # 5 minutes default
IMAGE_CACHE_TTL = int(os.environ.get('IMAGE_CACHE_TTL', '600'))  # 10 minutes default

# Allowed domains for image proxy (prevents SSRF attacks)
ALLOWED_IMAGE_DOMAINS = [
    'rosetta.nli.org.il',
    'iiif.nli.org.il',
    'www.nli.org.il',
    'nli.org.il',
    'hebrew.bodleian.ox.ac.uk',
    'luna.manchester.ac.uk',
    'iiif-cloud.princeton.edu',
    'figgy.princeton.edu',
]

def init_api_routes():
    """Register API routes for image proxy and exports."""
    # NOTE: Backend database and routers removed - using Supabase now
    print("API routes initialized (Supabase mode)")

    # Initialize NLI crossref service for local FL ID resolution (Phase 30)
    from shared.nli_crossref_service import get_nli_crossref_service
    nli_svc = get_nli_crossref_service(thread_safe=True)
    if nli_svc.is_available():
        print(f"NLI crossref sidecar loaded (local FL ID resolution enabled)")
    else:
        print("NLI crossref sidecar not available (will use network manifest fetch)")

    def fetch_fl_ids_from_nli(system_id: str, _cache={}, _cache_time={}) -> list:
        """Fetch ALL FL IDs from NLI IIIF manifest (contains all pages). Results are cached.

        Resolution order:
        1. In-memory cache (fastest)
        2. Local SQLite sidecar via NliCrossrefService (no network, ~815K pre-resolved records)
        3. NLI IIIF manifest network fetch (all pages)
        4. NLI MARC API fallback (typically 1 FL ID)
        """
        import time as _time

        # Check cache first
        if system_id in _cache:
            cache_age = _time.time() - _cache_time.get(system_id, 0)
            if cache_age < NLI_CACHE_TTL:
                return _cache[system_id]

        # NOTE: Crossref FGPImageNumberId != IIIF FL number. Cannot use sidecar for image URLs.
        # Always use IIIF manifest for canonical FL IDs.

        # Use IIIF manifest endpoint - this has ALL page images, unlike MARC which only has 1
        url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        try:
            resp = requests.get(url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200:
                data = resp.json()
                fl_ids = []

                # Extract FL IDs from canvas images in order
                if 'sequences' in data and data['sequences']:
                    for canvas in data['sequences'][0].get('canvases', []):
                        images = canvas.get('images', [])
                        if images:
                            resource = images[0].get('resource', {})
                            service = resource.get('service', {})
                            service_id = service.get('@id', '')
                            # Extract FL number (e.g. .../FL7734473/...)
                            fl_match = re.search(r'FL(\d+)', service_id)
                            if fl_match:
                                fl_ids.append(fl_match.group(1))

                if fl_ids:
                    # Cache successful result
                    _cache[system_id] = fl_ids
                    _cache_time[system_id] = _time.time()
                    print(f"Resolved {len(fl_ids)} FL IDs for {system_id} from network IIIF manifest")
                    return fl_ids
        except Exception as e:
            print(f"Failed to fetch FL IDs from IIIF manifest for {system_id}: {e}")

        # Fallback to MARC API (only has 1 FL ID typically)
        try:
            marc_url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{system_id}"
            resp = requests.get(marc_url, headers=headers, timeout=10, verify=True)
            if resp.status_code == 200:
                fl_ids = re.findall(r'FL(\d+)', resp.text)
                seen = set()
                unique_fl_ids = []
                for fl_id in fl_ids:
                    if fl_id not in seen:
                        seen.add(fl_id)
                        unique_fl_ids.append(fl_id)
                if unique_fl_ids:
                    _cache[system_id] = unique_fl_ids
                    _cache_time[system_id] = _time.time()
                    print(f"Cached {len(unique_fl_ids)} FL IDs from MARC for {system_id}")
                return unique_fl_ids
        except Exception as e:
            print(f"MARC fallback also failed for {system_id}: {e}")

        return []

    @app.get('/api/fl_ids/{sys_id}')
    def get_fl_ids(sys_id: str):
        """Return manifest FL IDs for a sys_id (cached). Used by NLI viewer deep links."""
        fl_ids = fetch_fl_ids_from_nli(sys_id)
        return fl_ids

    @app.get('/api/nli_image/{fl_id}')
    def nli_image(fl_id: str):
        """
        Fetch NLI image by FL ID. Tries IIIF first (for valid IDs), then Rosetta.
        """
        # Clean the FL ID
        digits = re.sub(r"\D", "", str(fl_id))
        if not digits:
            return Response(content="Invalid FL ID", status_code=400)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        # Try IIIF first (works for valid FL IDs, returns real images)
        iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/2000,/0/default.jpg"
        try:
            resp = requests.get(iiif_url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                # Verify it's not a tiny placeholder (real images are > 5KB)
                if len(resp.content) > 5000:
                    return Response(
                        content=resp.content,
                        media_type=resp.headers.get('Content-Type', 'image/jpeg')
                    )
        except Exception as e:
            print(f"IIIF failed for FL{digits}: {e}")

        # Fallback to Rosetta - but filter out the "no image" placeholder
        rosetta_url = f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{digits}"
        try:
            resp = requests.get(rosetta_url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                # The "no image" placeholder is ~1615 bytes, real images are larger
                if len(resp.content) > 2000:
                    return Response(
                        content=resp.content,
                        media_type=resp.headers.get('Content-Type', 'image/png')
                    )
        except Exception as e:
            print(f"Rosetta failed for FL{digits}: {e}")

        return Response(content="Image not found", status_code=404)

    # Image cache: (sys_id, page) -> (content, content_type, timestamp)
    _image_cache = {}

    @app.get('/api/nli_image_by_sysid/{sys_id}')
    def nli_image_by_sysid(sys_id: str, page: int = 0):
        """
        Fetch NLI image by System ID. Dynamically gets FL IDs from NLI MARC API.
        """
        import time as _time
        cache_key = (sys_id, page)

        # Check image cache first
        if cache_key in _image_cache:
            content, content_type, cached_at = _image_cache[cache_key]
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                return Response(
                    content=content,
                    media_type=content_type,
                    headers={"Cache-Control": "public, max-age=600"}
                )

        # Fetch FL IDs from NLI (this function has its own cache)
        fl_ids = fetch_fl_ids_from_nli(sys_id)
        if not fl_ids:
            return Response(content="No FL IDs found for this document", status_code=404)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        # If page index specified, try that specific FL ID first
        if 0 <= page < len(fl_ids):
            fl_id = fl_ids[page]
            iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/2000,/0/default.jpg"
            try:
                resp = requests.get(iiif_url, headers=headers, timeout=15, verify=True)
                if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', '') and len(resp.content) > 5000:
                    content_type = resp.headers.get('Content-Type', 'image/jpeg')
                    # Cache the image
                    _image_cache[cache_key] = (resp.content, content_type, _time.time())
                    return Response(
                        content=resp.content,
                        media_type=content_type,
                        headers={"Cache-Control": "public, max-age=600"}
                    )
            except Exception:
                pass

        # Fallback: try each FL ID until one works
        for fl_id in fl_ids:
            iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/2000,/0/default.jpg"
            try:
                resp = requests.get(iiif_url, headers=headers, timeout=15, verify=True)
                if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', '') and len(resp.content) > 5000:
                    content_type = resp.headers.get('Content-Type', 'image/jpeg')
                    _image_cache[cache_key] = (resp.content, content_type, _time.time())
                    return Response(
                        content=resp.content,
                        media_type=content_type,
                        headers={"Cache-Control": "public, max-age=600"}
                    )
            except Exception:
                pass

        return Response(content="Image not found", status_code=404)

    # Oxford image cache: (sys_id, page) -> (content, content_type, timestamp)
    _oxford_image_cache = {}

    # Cambridge image cache: (sys_id, page) -> (content, content_type, timestamp)
    _cambridge_image_cache = {}

    @app.get('/api/cambridge_image/{sys_id}')
    def cambridge_image(sys_id: str, page: int = 0):
        """
        Fetch Cambridge IIIF image by System ID.
        Looks up images_ext from nli_cache and fetches the canvas image via IIIF.
        """
        import time as _time
        cache_key = (sys_id, page)

        # Check image cache first
        if cache_key in _cambridge_image_cache:
            content, content_type, cached_at = _cambridge_image_cache[cache_key]
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                return Response(
                    content=content,
                    media_type=content_type,
                    headers={"Cache-Control": "public, max-age=600"}
                )

        # Look up Cambridge images from nli_cache
        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])

        if not images_ext:
            return Response(content="No Cambridge images available", status_code=404)

        if page < 0 or page >= len(images_ext):
            return Response(content="Page out of range", status_code=404)

        canvas_entry = images_ext[page]
        canvas_url = canvas_entry.get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)

        # Build IIIF Image API URL from canvas base URL
        img_url = f"{canvas_url}/full/2000,/0/default.jpg"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://cudl.lib.cam.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                _cambridge_image_cache[cache_key] = (resp.content, content_type, _time.time())
                return Response(
                    content=resp.content,
                    media_type=content_type,
                    headers={"Cache-Control": "public, max-age=600"}
                )
            else:
                return Response(content=f"Failed to fetch Cambridge image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            return Response(content=f"Error fetching Cambridge image: {e}", status_code=500)

    # Manchester image cache: (sys_id, page) -> (content, content_type, timestamp)
    _manchester_image_cache = {}

    @app.get('/api/manchester_image/{sys_id}')
    def manchester_image(sys_id: str, page: int = 0):
        """
        Fetch Manchester IIIF image by System ID.
        Uses images_ext from nli_cache populated by enrich_metadata via LUNA IIIF manifest.
        """
        import time as _time
        cache_key = (sys_id, page)

        # Check cache
        if cache_key in _manchester_image_cache:
            content, content_type, cached_at = _manchester_image_cache[cache_key]
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                return Response(content=content, media_type=content_type,
                              headers={"Cache-Control": "public, max-age=600"})

        # Look up Manchester images from nli_cache
        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])

        if not images_ext:
            return Response(content="No Manchester images available", status_code=404)

        if page < 0 or page >= len(images_ext):
            return Response(content="Page out of range", status_code=404)

        canvas_entry = images_ext[page]
        canvas_url = canvas_entry.get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)

        # Build IIIF Image API URL from canvas base URL
        img_url = f"{canvas_url}/full/2000,/0/default.jpg"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://luna.manchester.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                _manchester_image_cache[cache_key] = (resp.content, content_type, _time.time())
                return Response(content=resp.content, media_type=content_type,
                              headers={"Cache-Control": "public, max-age=600"})
            else:
                return Response(content=f"Failed to fetch Manchester image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            return Response(content=f"Error fetching Manchester image: {e}", status_code=500)

    # JTS image cache: (sys_id, page) -> (content, content_type, timestamp)
    _jts_image_cache = {}

    @app.get('/api/jts_image/{sys_id}')
    def jts_image(sys_id: str, page: int = 0):
        """
        Fetch JTS/Princeton IIIF image by System ID.
        Uses images_ext from nli_cache populated by enrich_metadata via Figgy IIIF manifest.
        """
        import time as _time
        cache_key = (sys_id, page)

        # Check cache
        if cache_key in _jts_image_cache:
            content, content_type, cached_at = _jts_image_cache[cache_key]
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                return Response(content=content, media_type=content_type,
                              headers={"Cache-Control": "public, max-age=600"})

        # Look up JTS images from nli_cache
        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])

        if not images_ext:
            return Response(content="No JTS images available", status_code=404)

        if page < 0 or page >= len(images_ext):
            return Response(content="Page out of range", status_code=404)

        canvas_entry = images_ext[page]
        canvas_url = canvas_entry.get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)

        # Build IIIF Image API URL from canvas base URL
        img_url = f"{canvas_url}/full/2000,/0/default.jpg"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://dpul.princeton.edu/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                _jts_image_cache[cache_key] = (resp.content, content_type, _time.time())
                return Response(content=resp.content, media_type=content_type,
                              headers={"Cache-Control": "public, max-age=600"})
            else:
                return Response(content=f"Failed to fetch JTS image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            return Response(content=f"Error fetching JTS image: {e}", status_code=500)

    def _extract_folio_number(shelfmark: str) -> int:
        """Extract folio number from Oxford shelfmark like 'MS heb. f.21/21' -> 21"""
        # Pattern: MS heb. X.YY/ZZ where ZZ is the folio number
        match = re.search(r'/(\d+)', shelfmark)
        if match:
            return int(match.group(1))
        return 0

    @app.get('/api/oxford_image/{sys_id}')
    def oxford_image(sys_id: str, page: int = 0):
        """
        Fetch Oxford image by System ID using CodicologicalManager.
        Automatically finds the correct folio image based on shelfmark.
        """
        import time as _time
        cache_key = (sys_id, page)

        # Check image cache first
        if cache_key in _oxford_image_cache:
            content, content_type, cached_at = _oxford_image_cache[cache_key]
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                return Response(
                    content=content,
                    media_type=content_type,
                    headers={"Cache-Control": "public, max-age=600"}
                )

        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return Response(content="Oxford manager not initialized", status_code=503)

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return Response(content="Oxford database still loading", status_code=503)

        # Get the Part ID for this system ID
        part_id = codico.get_part_for_folio(sys_id)

        # Get shelfmark for folio number extraction
        shelfmark = ''
        shelfmark_tuple = state.meta_mgr.get_meta_for_id(sys_id)
        if shelfmark_tuple and shelfmark_tuple[0]:
            shelfmark = shelfmark_tuple[0]

        if not part_id and shelfmark:
            # Try to find part by shelfmark
            part_id, is_part = codico.parse_part_identifier(shelfmark)
            if not is_part:
                part_id = None

        if not part_id:
            return Response(content="No Oxford Part found for this document", status_code=404)

        # Get images for this part
        images = codico.get_part_images(part_id)
        if not images:
            return Response(content="No images available for this Part", status_code=404)

        # Extract folio number from shelfmark and find the matching image
        folio_num = _extract_folio_number(shelfmark)
        img_data = None
        img_url = ''

        if folio_num > 0:
            # Find all images matching this folio number (both 'a' and 'b' sides)
            folio_images = [img for img in images if img.get('folio_num') == folio_num]

            if folio_images:
                # Use page param to select: 0 = first (recto/a), 1 = second (verso/b)
                idx = min(page, len(folio_images) - 1)
                img_data = folio_images[idx]
            else:
                # If image not in database but folio is in range, generate URL dynamically
                metadata = codico.part_metadata.get(part_id, {})
                folio_range = metadata.get('folio_range', [])
                if len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
                    # Generate URL based on part_id pattern
                    # MS. Heb. f. 21/1 -> MS_HEB_f_21_18a.jpg or MS_HEB_f_21_18b.jpg
                    match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)', part_id, re.IGNORECASE)
                    if match:
                        letter, volume = match.groups()
                        # page 0 = recto (a), page 1 = verso (b)
                        side = 'b' if page == 1 else 'a'
                        img_url = f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio_num}{side}.jpg"

        # Fallback to page index if folio not found and no dynamic URL
        if not img_data and not img_url:
            if page < 0 or page >= len(images):
                page = 0
            img_data = images[page]

        if not img_url and img_data:
            img_url = img_data.get('full_url', '')

        if not img_url:
            return Response(content="No image URL available", status_code=404)

        # Fetch the image from Oxford
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://hebrew.bodleian.ox.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                # Cache the image
                _oxford_image_cache[cache_key] = (resp.content, content_type, _time.time())
                return Response(
                    content=resp.content,
                    media_type=content_type,
                    headers={"Cache-Control": "public, max-age=600"}
                )
            else:
                return Response(content=f"Failed to fetch image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            return Response(content=f"Error fetching image: {e}", status_code=500)

    @app.get('/api/oxford_image_url/{sys_id}')
    def oxford_image_url(sys_id: str, page: int = 0):
        """
        Get the direct Oxford URL for an image (no proxy).
        Returns JSON with the URL that the browser can fetch directly.
        """
        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return {"error": "Oxford manager not initialized", "url": None}

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return {"error": "Oxford database still loading", "url": None}

        # Get shelfmark for folio number extraction
        shelfmark = ''
        shelfmark_tuple = state.meta_mgr.get_meta_for_id(sys_id)
        if shelfmark_tuple and shelfmark_tuple[0]:
            shelfmark = shelfmark_tuple[0]

        part_id = codico.get_part_for_folio(sys_id)
        if not part_id and shelfmark:
            part_id, is_part = codico.parse_part_identifier(shelfmark)
            if not is_part:
                part_id = None

        if not part_id:
            return {"error": "No Oxford Part found", "url": None}

        images = codico.get_part_images(part_id)
        folio_num = _extract_folio_number(shelfmark)
        img_url = None

        if folio_num > 0:
            # Find images for this folio
            folio_images = [img for img in images if img.get('folio_num') == folio_num]
            if folio_images:
                idx = min(page, len(folio_images) - 1)
                img_url = folio_images[idx].get('full_url', '')
            else:
                # Generate dynamically if not in database
                metadata = codico.part_metadata.get(part_id, {})
                folio_range = metadata.get('folio_range', [])
                if len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
                    match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)', part_id, re.IGNORECASE)
                    if match:
                        letter, volume = match.groups()
                        side = 'b' if page == 1 else 'a'
                        img_url = f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio_num}{side}.jpg"

        if not img_url and images:
            idx = min(page, len(images) - 1)
            img_url = images[idx].get('full_url', '')

        return {"url": img_url, "folio": folio_num, "page": page, "part_id": part_id}

    @app.get('/api/oxford_images/{sys_id}')
    def oxford_images_list(sys_id: str):
        """
        Get list of available Oxford images for a system ID.
        Returns JSON with image metadata.
        """
        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return {"error": "Oxford manager not initialized", "images": []}

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return {"error": "Oxford database still loading", "images": []}

        # Get the Part ID for this system ID
        part_id = codico.get_part_for_folio(sys_id)
        if not part_id:
            return {"error": "No Oxford Part found", "images": [], "part_id": None}

        # Get images for this part
        images = codico.get_part_images(part_id)

        return {
            "part_id": part_id,
            "images": [
                {
                    "index": i,
                    "label": img.get('label', ''),
                    "folio_num": img.get('folio_num'),
                    "url": f"/api/oxford_image/{sys_id}?page={i}"
                }
                for i, img in enumerate(images)
            ]
        }

    @app.get('/api/oxford_debug')
    def oxford_debug():
        """
        Debug endpoint to check Oxford mapping status.
        """
        if not state.meta_mgr:
            return {"error": "MetadataManager not initialized"}

        codico = state.meta_mgr.codico_mgr
        if not codico:
            return {"error": "CodicologicalManager not initialized"}

        csv_bank_count = len(state.meta_mgr.csv_bank)
        oxford_part_count = sum(1 for v in state.meta_mgr.csv_bank.values() if v.get('oxford_part_id'))

        return {
            "codico_loaded": getattr(codico, '_loaded', False),
            "csv_bank_entries": csv_bank_count,
            "entries_with_oxford_part_id": oxford_part_count,
            "folio_to_part_mappings": len(codico.folio_to_part),
            "part_metadata_count": len(codico.part_metadata),
            "sample_mappings": list(codico.folio_to_part.items())[:5] if codico.folio_to_part else [],
        }

    @app.get('/api/browse_debug/{sys_id}')
    def browse_debug(sys_id: str):
        """
        Debug endpoint to check browse data for a specific sys_id.
        """
        if not state.meta_mgr:
            return {"error": "MetadataManager not initialized"}
        if not state.searcher:
            return {"error": "SearchEngine not initialized"}

        # Get shelfmark from csv_bank
        csv_entry = state.meta_mgr.csv_bank.get(sys_id, {})
        shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)

        # Get browse_map data
        browse_map = state.searcher._load_browse_map() if hasattr(state.searcher, '_load_browse_map') else {}
        browse_entries = browse_map.get(sys_id, [])

        # Extract FL IDs from browse entries
        import re
        fl_ids = []
        for entry in browse_entries:
            full_header = entry.get('full_header', '')
            match = re.search(r'FL(\d+)', full_header)
            if match:
                fl_ids.append(match.group(1))

        # Check Oxford mapping
        part_id = None
        if state.meta_mgr.codico_mgr:
            part_id = state.meta_mgr.codico_mgr.get_part_for_folio(sys_id)

        return {
            "sys_id": sys_id,
            "csv_entry": csv_entry,
            "shelfmark": shelfmark,
            "title": title,
            "browse_entries_count": len(browse_entries),
            "fl_ids_found": fl_ids[:5],  # First 5
            "oxford_part_id": part_id,
            "is_oxford_shelfmark": shelfmark.lower().startswith('ms heb') or shelfmark.lower().startswith('ms. heb') if shelfmark else False,
        }

    @app.get('/api/proxy_image')
    def proxy_image(url: str):
        """
        Proxy image requests to NLI to bypass Referer checks.
        Spoofs the Referer header to look like it's coming from nli.org.il.
        """
        if not url:
            return Response(status_code=400)

        # Validate URL format and domain to prevent SSRF attacks
        try:
            parsed = urlparse(url)
            if parsed.scheme not in ('http', 'https'):
                return Response(content="Invalid URL scheme", status_code=400)
            if not parsed.netloc:
                return Response(content="Invalid URL", status_code=400)
            if parsed.netloc not in ALLOWED_IMAGE_DOMAINS:
                return Response(content="Domain not allowed", status_code=403)
        except Exception:
            return Response(content="Invalid URL format", status_code=400)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.nli.org.il/',
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        }

        try:
            # Fetch the image with timeout
            resp = requests.get(url, headers=headers, timeout=15, verify=True)
            if resp.status_code == 200:
                return Response(
                    content=resp.content,
                    media_type=resp.headers.get('Content-Type', 'image/jpeg')
                )
            else:
                print(f"Proxy got status {resp.status_code} for URL: {url}")
                return Response(status_code=resp.status_code)
        except requests.Timeout:
            print(f"Proxy timeout for URL: {url}")
            return Response(content="Request timeout", status_code=504)
        except Exception as e:
            print(f"Proxy error for {url}: {e}")
            return Response(status_code=500)

    @app.get('/api/export/excel')
    def export_excel():
        """Export search results to Excel format using unified export service."""
        if not state.last_results:
            return Response("No results to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_search_results_excel(
                state.last_results,
                state.current_search_query or ""
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            print(f"Export Excel error: {e}")
            return Response("Export failed", status_code=500)

    @app.get('/api/export/word')
    def export_word():
        """Export search results to Word format using unified export service."""
        if not state.last_results:
            return Response("No results to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_search_results_word(
                state.last_results,
                state.current_search_query or ""
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            print(f"Export Word error: {e}")
            return Response("Export failed", status_code=500)

    @app.get('/api/export/parallels/excel')
    def export_parallels_excel():
        """Export parallels results to Excel using unified export service."""
        from nicegui import app as nicegui_app

        parallels_results = state.parallels_results or []
        filtered_results = state.parallels_filtered or []
        # Get source text from storage for filename
        source_text = nicegui_app.storage.user.get('parallels_source_text', '')

        if not parallels_results and not filtered_results:
            return Response("No parallels results to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_parallels_excel(
                parallels_results, filtered_results, source_text=source_text
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            print(f"Export Parallels Excel error: {e}")
            return Response("Export failed", status_code=500)

    @app.get('/api/export/parallels/word')
    def export_parallels_word():
        """Export parallels results to Word using unified export service."""
        from nicegui import app as nicegui_app

        parallels_results = state.parallels_results or []
        filtered_results = state.parallels_filtered or []
        # Get source text from storage for filename
        source_text = nicegui_app.storage.user.get('parallels_source_text', '')

        if not parallels_results and not filtered_results:
            return Response("No parallels results to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_parallels_word(
                parallels_results, filtered_results, source_text=source_text
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            print(f"Export Parallels Word error: {e}")
            return Response("Export failed", status_code=500)

    @app.get('/api/export/browse/word')
    def export_browse_word():
        """Export current browse page to Word using unified export service."""
        from nicegui import app as nicegui_app

        browse_data = nicegui_app.storage.user.get('browse_export_data')
        if not browse_data:
            return Response("No browse data to export", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_browse_word(browse_data)
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            print(f"Export Browse Word error: {e}")
            return Response("Export failed", status_code=500)

    @app.get('/api/export/list/{list_id}/excel')
    def export_list_excel(list_id: str):
        """Export a specific list to Excel using unified export service."""
        if not state.lists_mgr:
            return Response("Lists manager not available", status_code=400)

        list_data = state.lists_mgr.data.get('lists', {}).get(list_id)
        if not list_data:
            return Response("List not found", status_code=404)

        # Use get_items_in_list() - items are stored in data['items'] with list membership
        items = state.lists_mgr.get_items_in_list_sync(list_id)
        if not items:
            return Response("List is empty", status_code=400)

        try:
            export_svc = get_export_service(state.meta_mgr)
            content, filename = export_svc.export_list_excel(
                list_id, list_data.get('name', 'list'), items
            )
            return Response(
                content=content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": encode_filename_for_header(filename)}
            )
        except ValueError as e:
            return Response(str(e), status_code=400)
        except Exception as e:
            print(f"Export List Excel error: {e}")
            return Response("Export failed", status_code=500)

    @app.post('/api/auth/oauth-callback')
    async def oauth_callback(request):
        """
        Handle OAuth callback - receive tokens from client-side and set session.
        """
        from fastapi import Request
        from fastapi.responses import JSONResponse
        from web.supabase_client import set_session_from_url, get_profile
        from web.auth_state import GlobalAuthState

        try:
            body = await request.json()
            access_token = body.get('access_token')
            refresh_token = body.get('refresh_token')

            if not access_token or not refresh_token:
                return JSONResponse({'error': 'Missing tokens'}, status_code=400)

            # Set session in Supabase client
            result = set_session_from_url(access_token, refresh_token)

            if 'error' in result:
                return JSONResponse({'error': result['error']}, status_code=400)

            user = result.get('user')
            if user:
                # Get or create profile
                profile = get_profile(user['id'])

                # Store in NiceGUI session storage
                from nicegui import app as nicegui_app
                nicegui_app.storage.user[GlobalAuthState.USER_KEY] = user
                if profile:
                    nicegui_app.storage.user[GlobalAuthState.PROFILE_KEY] = profile
                # Store session tokens for per-user Supabase client
                session = result.get('session', {})
                if session:
                    nicegui_app.storage.user['auth_session'] = {
                        'access_token': session.get('access_token'),
                        'refresh_token': session.get('refresh_token'),
                    }

                return JSONResponse({'success': True, 'user': user})

            return JSONResponse({'error': 'No user returned'}, status_code=400)

        except Exception as e:
            print(f"OAuth callback error: {e}")
            return JSONResponse({'error': str(e)}, status_code=500)
