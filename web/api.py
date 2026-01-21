from nicegui import app
from fastapi import Response
from web.state import state
import requests
import re
from genizah_core import Config
import io
import openpyxl
from docx import Document
from urllib.parse import urlparse

# Import corrections API components
from backend.models.database import init_db
from backend.api.routes import auth, users, corrections, comments, documents, versions, discoveries

# Allowed domains for image proxy (prevents SSRF attacks)
ALLOWED_IMAGE_DOMAINS = [
    'rosetta.nli.org.il',
    'iiif.nli.org.il',
    'www.nli.org.il',
    'nli.org.il',
    'hebrew.bodleian.ox.ac.uk',
]

def init_api_routes():
    """Register API routes."""

    # Initialize corrections database
    try:
        init_db()
        print("Corrections database initialized")
    except Exception as e:
        print(f"Warning: Could not initialize corrections database: {e}")

    # Include corrections API routers
    app.include_router(auth.router, prefix="/api/v1", tags=["auth"])
    app.include_router(users.router, prefix="/api/v1", tags=["users"])
    app.include_router(corrections.router, prefix="/api/v1", tags=["corrections"])
    app.include_router(comments.router, prefix="/api/v1", tags=["comments"])
    app.include_router(documents.router, prefix="/api/v1", tags=["documents"])
    app.include_router(versions.router, prefix="/api/v1", tags=["versions"])
    app.include_router(discoveries.router, prefix="/api/v1", tags=["discoveries"])

    def fetch_fl_ids_from_nli(system_id: str, _cache={}, _cache_time={}) -> list:
        """Fetch ALL FL IDs from NLI IIIF manifest (contains all pages). Results are cached for 5 min."""
        import time as _time
        CACHE_TTL = 300  # 5 minutes

        # Check cache first
        if system_id in _cache:
            cache_age = _time.time() - _cache_time.get(system_id, 0)
            if cache_age < CACHE_TTL:
                return _cache[system_id]

        # Use IIIF manifest endpoint - this has ALL page images, unlike MARC which only has 1
        url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        try:
            resp = requests.get(url, headers=headers, timeout=15, verify=False)
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
                    print(f"Cached {len(fl_ids)} FL IDs for {system_id} from IIIF manifest")
                    return fl_ids
        except Exception as e:
            print(f"Failed to fetch FL IDs from IIIF manifest for {system_id}: {e}")

        # Fallback to MARC API (only has 1 FL ID typically)
        try:
            marc_url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{system_id}"
            resp = requests.get(marc_url, headers=headers, timeout=10, verify=False)
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
        iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/max/0/default.jpg"
        try:
            resp = requests.get(iiif_url, headers=headers, timeout=15, verify=False)
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
            resp = requests.get(rosetta_url, headers=headers, timeout=15, verify=False)
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

    @app.get('/api/nli_image_by_sysid/{sys_id}')
    def nli_image_by_sysid(sys_id: str, page: int = 0):
        """
        Fetch NLI image by System ID. Dynamically gets FL IDs from NLI MARC API.
        This is what the desktop does - it fetches the correct FL IDs from NLI.

        Args:
            sys_id: The system ID
            page: Page index (0-based) to select which FL ID to use for multi-page manuscripts
        """
        print(f"[DEBUG] nli_image_by_sysid called: sys_id={sys_id}, page={page}")

        # Fetch FL IDs from NLI
        fl_ids = fetch_fl_ids_from_nli(sys_id)
        print(f"[DEBUG] FL IDs fetched: {fl_ids}")
        if not fl_ids:
            return Response(content="No FL IDs found for this document", status_code=404)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        # If page index specified, try that specific FL ID first
        if 0 <= page < len(fl_ids):
            fl_id = fl_ids[page]
            iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/max/0/default.jpg"
            try:
                resp = requests.get(iiif_url, headers=headers, timeout=15, verify=False)
                if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', '') and len(resp.content) > 5000:
                    return Response(
                        content=resp.content,
                        media_type=resp.headers.get('Content-Type', 'image/jpeg')
                    )
            except Exception:
                pass

        # Fallback: try each FL ID until one works
        for fl_id in fl_ids:
            # Try IIIF
            iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/max/0/default.jpg"
            try:
                resp = requests.get(iiif_url, headers=headers, timeout=15, verify=False)
                if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', '') and len(resp.content) > 5000:
                    return Response(
                        content=resp.content,
                        media_type=resp.headers.get('Content-Type', 'image/jpeg')
                    )
            except Exception:
                pass

        return Response(content="Image not found", status_code=404)

    @app.get('/api/oxford_image/{sys_id}')
    def oxford_image(sys_id: str, page: int = 0):
        """
        Fetch Oxford image by System ID using CodicologicalManager.
        Uses the same logic as the desktop app.

        Args:
            sys_id: The system ID (folio ID)
            page: Optional page index within the part (default 0 = first image)
        """
        if not state.meta_mgr or not state.meta_mgr.codico_mgr:
            return Response(content="Oxford manager not initialized", status_code=503)

        codico = state.meta_mgr.codico_mgr
        if not getattr(codico, '_loaded', False):
            return Response(content="Oxford database still loading", status_code=503)

        # Get the Part ID for this system ID
        part_id = codico.get_part_for_folio(sys_id)
        if not part_id:
            # Try to find by shelfmark
            meta = state.meta_mgr.get_cached_meta(sys_id)
            if meta:
                shelfmark = meta.get('shelfmark', '')
                # Try to resolve the part from shelfmark
                part_id, is_part = codico.parse_part_identifier(shelfmark)
                if not is_part:
                    part_id = None

        if not part_id:
            print(f"No Oxford Part found for sys_id: {sys_id}")
            return Response(content="No Oxford Part found for this document", status_code=404)

        # Get images for this part
        images = codico.get_part_images(part_id)
        if not images:
            print(f"No images for Oxford Part: {part_id}")
            return Response(content="No images available for this Part", status_code=404)

        # Get the requested image (default to first)
        if page < 0 or page >= len(images):
            page = 0

        img_data = images[page]
        img_url = img_data.get('full_url', '')

        if not img_url:
            return Response(content="No image URL available", status_code=404)

        print(f"Fetching Oxford image: {img_url}")

        # Fetch the image from Oxford
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://hebrew.bodleian.ox.ac.uk/',
        }

        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                return Response(
                    content=resp.content,
                    media_type=resp.headers.get('Content-Type', 'image/jpeg')
                )
            else:
                print(f"Oxford image fetch failed: {resp.status_code}")
                return Response(content=f"Failed to fetch image: {resp.status_code}", status_code=resp.status_code)
        except Exception as e:
            print(f"Oxford image fetch error: {e}")
            return Response(content=f"Error fetching image: {e}", status_code=500)

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
            # Fetch the image with timeout (verify=False for NLI SSL issues)
            resp = requests.get(url, headers=headers, timeout=15, verify=False)
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
        if not state.last_results:
            return Response("No results to export", status_code=400)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Genizah Results"

        headers = ["Shelfmark", "Title", "System ID", "Score", "Snippet", "Full Text"]
        ws.append(headers)

        for res in state.last_results:
            display = res.get('display', {})
            row = [
                display.get('shelfmark', ''),
                display.get('title', ''),
                display.get('id', ''),
                str(res.get('sort_score', '')),
                res.get('snippet', '').replace('*', ''),
                res.get('full_text', '')[:32000] # Excel cell limit safety
            ]
            # Sanitize for illegal chars
            clean_row = []
            for cell in row:
                if isinstance(cell, str):
                    # Remove illegal chars (XML 1.0 invalid chars)
                    cell = "".join(ch for ch in cell if (0x20 <= ord(ch) <= 0xD7FF) or (0xE000 <= ord(ch) <= 0xFFFD) or ch in "\n\r\t")
                clean_row.append(cell)
            ws.append(clean_row)

        # Add credits at the bottom
        ws.append([])
        ws.append([])
        ws.append(['Credits'])
        ws.append(['Generated by Genizah Search Pro (Web Version)'])
        ws.append(['Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)'])
        ws.append(['Dataset: https://doi.org/10.5281/zenodo.17734473'])
        ws.append(['Citation: Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473'])

        stream = io.BytesIO()
        wb.save(stream)
        stream.seek(0)

        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=genizah_results.xlsx"}
        )

    @app.get('/api/export/word')
    def export_word():
        if not state.last_results:
            return Response("No results to export", status_code=400)

        doc = Document()
        doc.add_heading('Genizah Search Results', 0)

        for i, res in enumerate(state.last_results):
            display = res.get('display', {})
            shelf = display.get('shelfmark', 'Unknown')
            title = display.get('title', '')

            p = doc.add_paragraph()
            p.add_run(f"{i+1}. {shelf}").bold = True
            if title:
                p.add_run(f" - {title}")

            if res.get('snippet'):
                doc.add_paragraph(res['snippet'].replace('*', ''))

            doc.add_paragraph(f"System ID: {display.get('id', '')}")
            doc.add_paragraph("_" * 40)

        # Add credits
        doc.add_page_break()
        doc.add_heading('Credits', 1)
        doc.add_paragraph('Generated by Genizah Search Pro (Web Version)')
        doc.add_paragraph('Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)')
        doc.add_paragraph('Dataset available at: https://doi.org/10.5281/zenodo.17734473')
        doc.add_paragraph()
        citation = doc.add_paragraph('Full Citation: ')
        citation.add_run('Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473').italic = True

        stream = io.BytesIO()
        doc.save(stream)
        stream.seek(0)

        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": "attachment; filename=genizah_results.docx"}
        )

    @app.get('/api/export/parallels/excel')
    def export_parallels_excel():
        """Export parallels results to Excel."""
        from nicegui import app as nicegui_app

        parallels_results = nicegui_app.storage.user.get('parallels_results', [])
        if not parallels_results:
            return Response("No parallels results to export", status_code=400)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Parallels Results"

        headers = ["#", "Shelfmark", "Title", "Score", "Source Context", "Manuscript Match"]
        ws.append(headers)

        for idx, item in enumerate(parallels_results, 1):
            score = item.get('score', 0)
            raw_header = item.get('raw_header', '')

            # Extract metadata
            sys_id = None
            shelfmark = 'Unknown'
            title = ''

            if raw_header and state.meta_mgr:
                try:
                    import re
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        sys_id = sys_match.group(1)
                        shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                        shelfmark = shelf_temp or shelfmark
                        title = title_temp or ''
                except Exception:
                    pass

            source_ctx = item.get('source_ctx', '').replace('*', '')
            ms_text = item.get('text', '').replace('*', '')

            row = [idx, shelfmark, title, score, source_ctx, ms_text]

            # Sanitize for illegal chars
            clean_row = []
            for cell in row:
                if isinstance(cell, str):
                    cell = "".join(ch for ch in cell if (0x20 <= ord(ch) <= 0xD7FF) or (0xE000 <= ord(ch) <= 0xFFFD) or ch in "\n\r\t")
                clean_row.append(cell)
            ws.append(clean_row)

        # Add credits at the bottom
        ws.append([])
        ws.append([])
        ws.append(['Credits'])
        ws.append(['Generated by Genizah Search Pro (Web Version)'])
        ws.append(['Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)'])
        ws.append(['Dataset: https://doi.org/10.5281/zenodo.17734473'])
        ws.append(['Citation: Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473'])

        stream = io.BytesIO()
        wb.save(stream)
        stream.seek(0)

        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=parallels_results.xlsx"}
        )

    @app.get('/api/export/parallels/word')
    def export_parallels_word():
        """Export parallels results to Word."""
        from nicegui import app as nicegui_app

        parallels_results = nicegui_app.storage.user.get('parallels_results', [])
        if not parallels_results:
            return Response("No parallels results to export", status_code=400)

        doc = Document()
        doc.add_heading('Genizah Parallels Search Results', 0)

        for idx, item in enumerate(parallels_results, 1):
            score = item.get('score', 0)
            raw_header = item.get('raw_header', '')

            # Extract metadata
            shelfmark = 'Unknown'
            title = ''

            if raw_header and state.meta_mgr:
                try:
                    import re
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        sys_id = sys_match.group(1)
                        shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                        shelfmark = shelf_temp or shelfmark
                        title = title_temp or ''
                except Exception:
                    pass

            p = doc.add_paragraph()
            p.add_run(f"{idx}. {shelfmark}").bold = True
            p.add_run(f" - Score: {score}")
            if title:
                doc.add_paragraph(title)

            doc.add_paragraph("Source Context:").bold = True
            doc.add_paragraph(item.get('source_ctx', '').replace('*', ''))

            doc.add_paragraph("Manuscript Match:").bold = True
            doc.add_paragraph(item.get('text', '').replace('*', ''))

            doc.add_paragraph("_" * 60)

        # Add credits
        doc.add_page_break()
        doc.add_heading('Credits', 1)
        doc.add_paragraph('Generated by Genizah Search Pro (Web Version)')
        doc.add_paragraph('Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)')
        doc.add_paragraph('Dataset available at: https://doi.org/10.5281/zenodo.17734473')
        doc.add_paragraph()
        citation = doc.add_paragraph('Full Citation: ')
        citation.add_run('Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473').italic = True

        stream = io.BytesIO()
        doc.save(stream)
        stream.seek(0)

        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": "attachment; filename=parallels_results.docx"}
        )

    @app.get('/api/export/browse/word')
    def export_browse_word():
        """Export current browse page to Word."""
        from nicegui import app as nicegui_app

        browse_data = nicegui_app.storage.user.get('browse_export_data')
        if not browse_data:
            return Response("No browse data to export", status_code=400)

        doc = Document()
        doc.add_heading('Genizah Manuscript', 0)

        # Manuscript info
        if browse_data.get('shelfmark'):
            doc.add_heading(browse_data['shelfmark'], 1)
        if browse_data.get('title'):
            doc.add_paragraph(browse_data['title'])

        if browse_data.get('sys_id'):
            doc.add_paragraph(f"System ID: {browse_data['sys_id']}")

        doc.add_paragraph("_" * 60)

        # Text content
        if browse_data.get('view_all') and browse_data.get('pages'):
            doc.add_heading('Full Manuscript', 2)
            for page_data in browse_data['pages']:
                doc.add_heading(f"Page {page_data.get('p_num', '?')}", 3)
                if page_data.get('text'):
                    doc.add_paragraph(page_data['text'])
        elif browse_data.get('text'):
            doc.add_heading(f"Page {browse_data.get('p_num', '?')}", 2)
            doc.add_paragraph(browse_data['text'])

        # Add credits
        doc.add_page_break()
        doc.add_heading('Credits', 1)
        doc.add_paragraph('Generated by Genizah Search Pro (Web Version)')
        doc.add_paragraph('Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)')
        doc.add_paragraph('Dataset available at: https://doi.org/10.5281/zenodo.17734473')
        doc.add_paragraph()
        citation = doc.add_paragraph('Full Citation: ')
        citation.add_run('Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473').italic = True

        stream = io.BytesIO()
        doc.save(stream)
        stream.seek(0)

        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": "attachment; filename=manuscript.docx"}
        )

    @app.get('/api/export/list/{list_id}/excel')
    def export_list_excel(list_id: str):
        """Export a specific list to Excel."""
        if not state.lists_mgr:
            return Response("Lists manager not available", status_code=400)

        list_data = state.lists_mgr.data.get('lists', {}).get(list_id)
        if not list_data:
            return Response("List not found", status_code=404)

        items = list_data.get('items', [])
        if not items:
            return Response("List is empty", status_code=400)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = list_data.get('name', 'List')[:31]  # Excel sheet name limit

        headers = ["#", "Shelfmark", "Title", "System ID", "FL ID", "Notes", "Added"]
        ws.append(headers)

        for idx, item in enumerate(items, 1):
            sys_id = item.get('sys_id', '')
            shelfmark = 'Unknown'
            title = ''

            if sys_id and state.meta_mgr:
                try:
                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                    shelfmark = shelf_temp or shelfmark
                    title = title_temp or ''
                except Exception:
                    pass

            row = [
                idx,
                shelfmark,
                title,
                sys_id,
                item.get('fl_id', ''),
                item.get('note', ''),
                item.get('added', '')
            ]

            # Sanitize for illegal chars
            clean_row = []
            for cell in row:
                if isinstance(cell, str):
                    cell = "".join(ch for ch in cell if (0x20 <= ord(ch) <= 0xD7FF) or (0xE000 <= ord(ch) <= 0xFFFD) or ch in "\n\r\t")
                clean_row.append(cell)
            ws.append(clean_row)

        # Add credits at the bottom
        ws.append([])
        ws.append([])
        ws.append(['Credits'])
        ws.append(['Generated by Genizah Search Pro (Web Version)'])
        ws.append(['Data Source: MiDRASH Automatic Transcriptions (Stoekl Ben Ezra et al., 2025)'])
        ws.append(['Dataset: https://doi.org/10.5281/zenodo.17734473'])
        ws.append(['Citation: Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473'])

        stream = io.BytesIO()
        wb.save(stream)
        stream.seek(0)

        filename = f"{list_data.get('name', 'list').replace(' ', '_')}.xlsx"
        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
