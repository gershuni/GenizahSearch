## [Unreleased] -- Security updates of the pinned libraries, and stricter image opening (web + desktop, branch `fix/dependency-security-upgrades`)

- `requirements.txt`, `requirements-lock.txt` and `requirements-atlas-bake.txt`: Pillow 13.0.0,
  aiohttp 3.14.5 (with yarl 1.25.1), PyJWT 2.15.1, cryptography 50.0.2, certifi 2026.11.20, requests
  2.34.2, urllib3 2.8.0, lxml 6.1.3 (`requirements.txt` now asks for 6.1.0 or later), Brotli 1.2.1,
  h2 4.4.1, hpack 4.2.0, fsspec 2026.9.0, opentelemetry-api 1.45.2, pytest 9.2.0.
- `shared/background_removal.py::open_jpeg_or_png`: the Fragment Puzzle opens images with the JPEG
  and PNG decoders only. An image sent to `/api/puzzle_process` is also refused above 40 million
  pixels, before it is decoded.
- Pillow 13: `Image.fromarray` is called without its `mode` argument.
- Desktop: the installer needs a build environment refreshed from `requirements-lock.txt`.
