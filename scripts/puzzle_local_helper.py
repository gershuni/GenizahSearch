#!/usr/bin/env python
"""Minimal local helper for puzzle image experiments.

This is an intentionally small proof of concept for the "process on the
user's machine" direction. It exposes a localhost HTTP endpoint that
reuses the existing shared puzzle image pipeline:

    browser -> localhost helper -> resolve_fragment_image() -> image bytes

The helper is not wired into the web UI yet. It exists to validate the
core assumption before broader integration work.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.puzzle_image_service import resolve_fragment_image


LOGGER = logging.getLogger("puzzle_local_helper")
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 43111
VALID_SIZES = {400, 800, 1200, 2000}
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.nli.org.il/",
}


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _pick_size(value: str | None) -> int:
    try:
        size = int(value) if value is not None else 800
    except ValueError:
        size = 800
    if size in VALID_SIZES:
        return size
    return min(VALID_SIZES, key=lambda s: abs(s - size))


def _resolve_fl_ids_for_sys_id(sys_id: str) -> list[str]:
    """Resolve canonical FL IDs in page order.

    This mirrors the current app strategy more closely than a direct image URL
    test: resolve manifest FL IDs first, then use MARC as a weaker fallback.
    """
    digits = re.sub(r"\D", "", sys_id or "")
    if not digits:
        return []

    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{digits}-1/manifest"
    try:
        resp = requests.get(manifest_url, headers=HTTP_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            fl_ids: list[str] = []
            sequences = data.get("sequences") or []
            canvases = sequences[0].get("canvases", []) if sequences else []
            for canvas in canvases:
                images = canvas.get("images") or []
                if not images:
                    continue
                resource = images[0].get("resource", {})
                service = resource.get("service", {})
                service_id = service.get("@id", "") if isinstance(service, dict) else ""
                match = re.search(r"FL(\d+)", service_id)
                if match:
                    fl_ids.append(match.group(1))
            if fl_ids:
                return fl_ids
    except Exception as exc:  # pragma: no cover - exploratory helper
        LOGGER.warning("Manifest FL resolution failed for %s: %s", digits, exc)

    marc_url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{digits}"
    try:
        resp = requests.get(marc_url, headers=HTTP_HEADERS, timeout=10)
        if resp.status_code == 200:
            fl_ids = re.findall(r"FL(\d+)", resp.text)
            unique: list[str] = []
            seen = set()
            for fl_id in fl_ids:
                if fl_id not in seen:
                    seen.add(fl_id)
                    unique.append(fl_id)
            return unique
    except Exception as exc:  # pragma: no cover - exploratory helper
        LOGGER.warning("MARC FL fallback failed for %s: %s", digits, exc)

    return []


class PuzzleLocalHelperHandler(BaseHTTPRequestHandler):
    server_version = "PuzzleLocalHelper/0.1"

    def _send_common_headers(self, *, content_type: str, content_length: int | None = None) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        if content_length is not None:
            self.send_header("Content-Length", str(content_length))

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status.value)
        self._send_common_headers(content_type="application/json; charset=utf-8", content_length=len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT.value)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(HTTPStatus.OK, {"ok": True, "service": "puzzle-local-helper"})
            return

        if parsed.path == "/puzzle/resolve":
            query = parse_qs(parsed.query)
            raw_sys_id = (query.get("sys_id") or [""])[0]
            digits = re.sub(r"\D", "", raw_sys_id)
            if not digits:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid-sys-id"})
                return
            fl_ids = _resolve_fl_ids_for_sys_id(digits)
            self._send_json(HTTPStatus.OK, {"ok": True, "sys_id": digits, "fl_ids": fl_ids})
            return

        if parsed.path == "/puzzle/image_by_sysid":
            query = parse_qs(parsed.query)
            raw_sys_id = (query.get("sys_id") or [""])[0]
            digits = re.sub(r"\D", "", raw_sys_id)
            if not digits:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid-sys-id"})
                return

            try:
                page = int((query.get("page") or ["0"])[0])
            except ValueError:
                page = 0
            if page < 0:
                page = 0

            fl_ids = _resolve_fl_ids_for_sys_id(digits)
            if not fl_ids:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "no-fl-ids", "sys_id": digits})
                return
            fl_id = fl_ids[min(page, len(fl_ids) - 1)]

            query = dict(query)
            query["fl_id"] = [fl_id]
            parsed = parsed._replace(path="/puzzle/image", query="")
            self.path = "/puzzle/image"
            self.path += (
                f"?fl_id={fl_id}"
                f"&threshold={(query.get('threshold') or ['30'])[0]}"
                f"&size={(query.get('size') or ['800'])[0]}"
                f"&processed={(query.get('processed') or ['true'])[0]}"
                f"&is_cul={(query.get('is_cul') or ['false'])[0]}"
            )
            self.do_GET()
            return

        if parsed.path != "/puzzle/image":
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not-found"})
            return

        query = parse_qs(parsed.query)
        raw_fl_id = (query.get("fl_id") or [""])[0]
        digits = re.sub(r"\D", "", raw_fl_id)
        if not digits:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid-fl-id"})
            return

        try:
            threshold = float((query.get("threshold") or ["30"])[0])
        except ValueError:
            threshold = 30.0
        threshold = max(0.0, min(255.0, threshold))

        size = _pick_size((query.get("size") or [None])[0])
        processed = _parse_bool((query.get("processed") or [None])[0], True)
        is_cul = _parse_bool((query.get("is_cul") or [None])[0], False)

        try:
            image_bytes = resolve_fragment_image(
                digits,
                size=size,
                threshold=threshold,
                processed=processed,
                is_cul=is_cul,
            )
        except Exception as exc:  # pragma: no cover - exploratory helper
            LOGGER.exception("Local helper failed for fl_id=%s", digits)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return

        if not image_bytes:
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "image-not-found"})
            return

        content_type = "image/png" if image_bytes[:4] == b"\x89PNG" else "image/jpeg"
        self.send_response(HTTPStatus.OK.value)
        self._send_common_headers(content_type=content_type, content_length=len(image_bytes))
        self.end_headers()
        self.wfile.write(image_bytes)

    def log_message(self, fmt: str, *args) -> None:
        LOGGER.info("%s - %s", self.address_string(), fmt % args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a tiny local puzzle image helper.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Host to bind (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port to bind (default: {DEFAULT_PORT})")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")

    server = ThreadingHTTPServer((args.host, args.port), PuzzleLocalHelperHandler)
    LOGGER.info("Puzzle local helper listening on http://%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("Stopping helper")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
