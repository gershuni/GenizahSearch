from nicegui import app
from fastapi import Response
from web.state import state
import requests
from genizah_core import Config
import io
import openpyxl
from docx import Document
from urllib.parse import urlparse

# Allowed domains for image proxy (prevents SSRF attacks)
ALLOWED_IMAGE_DOMAINS = [
    'rosetta.nli.org.il',
    'iiif.nli.org.il',
    'www.nli.org.il',
    'nli.org.il',
]

def init_api_routes():
    """Register API routes."""

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

        headers = dict(Config.HTTP_HEADERS)
        headers["Referer"] = "https://www.nli.org.il/"

        try:
            # Fetch the image with timeout
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                return Response(
                    content=resp.content,
                    media_type=resp.headers.get('Content-Type', 'image/jpeg')
                )
            else:
                return Response(status_code=resp.status_code)
        except requests.Timeout:
            print(f"Proxy timeout for URL: {url}")
            return Response(content="Request timeout", status_code=504)
        except Exception as e:
            print(f"Proxy error: {e}")
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

        stream = io.BytesIO()
        doc.save(stream)
        stream.seek(0)

        return Response(
            content=stream.read(),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": "attachment; filename=genizah_results.docx"}
        )
