# -*- coding: utf-8 -*-
"""Phase 133 (ATLAS-01) — Visual Atlas Preview flag-gating + data-route contract.

Seven assertion groups (see the plan's Task 3):

1. three-surface single-predicate references (AST scan of web/main.py): the
   /atlas page route, the nav append, and BOTH data routes reference
   ``atlas_preview_available()`` (the teaser is 133-05; the 4-surface behavioral
   integration test is 133-06).
2. flag-OFF clean-hide: with the flag OFF the /atlas route body takes the
   clean-hide branch and does NOT call ``create_atlas_page``.
3. flag-ON / asset-not-loaded: the same clean-hide, AND both data routes 404
   (the asset is unreachable while unavailable).
4. response-level br/identity/* q-value negotiation incl. a reachable 406 and
   the brotli-absent fallback.
5. manifest no-cache + ETag + 304, plus a stale-manifest transition (a rebake to
   a NEW asset_basename 404s the old asset and re-points the manifest).
6. HE translated-value assertions for a sample of the new keys.
7. a flag-INDEPENDENT ``/static/atlas/*`` -> 404 regression guard (HIGH-1: the
   asset can never be reintroduced under the public /static mount).

Groups 1, 6, 7-structural do not need a running app. Groups 2, 3-clean-hide drive
the real (decorated) route body inside a minimal NiceGUI client context. Groups
3-404, 4, 5, 7-behavioral register the real data routes onto a BARE FastAPI so
they exercise the response behavior without the full NiceGUI startup.

Group 3B (Codex round-4 MEDIUM-1/2/3 hardening): binary header/section-table
structural validation, a content-hash-verified basename, and a Brotli-integrity
check on the (optional) ``.bin.br`` sidecar -- each fails closed independently
of the others. Group 4 additionally covers MEDIUM-4 (RFC 9110 weighted
Accept-Encoding preference, not "always prefer br").
"""

import ast
import asyncio
import hashlib
import json
import os
import pathlib
import struct

import brotli
import pytest
from fastapi import FastAPI
from starlette.staticfiles import StaticFiles
from starlette.testclient import TestClient

import web.atlas_assets as aa
import web.main as wm
from web.translations import set_language, tr


MAIN_PY = pathlib.Path(wm.__file__)
MAIN_SRC = MAIN_PY.read_text(encoding="utf-8")

# The committed golden fixture -- a REAL, structurally valid ATLAS001 binary
# (see tests/fixtures/atlas/golden-v1.bin + docs/specs/atlas-asset-schema-v1.md).
# Used as the base for every synthetic ready-asset in this module so the new
# MEDIUM-1 header/section-table validation in web.atlas_assets genuinely
# exercises real bytes, not an arbitrary marker blob.
_FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures" / "atlas"
_GOLDEN_BIN = (_FIXTURES_DIR / "golden-v1.bin").read_bytes()

# Route/nav functions that MUST gate on the single availability predicate.
_PREDICATE_SURFACES = (
    "atlas_page_route",     # the /atlas page route
    "create_layout",        # holds the predicate-gated nav append
    "atlas_manifest_route",  # /atlas-data/manifest.json
    "atlas_asset_route",     # /atlas-data/{asset_name}
)


# ---------------------------------------------------------------------------
# Real-atlas-bytes ready-asset helpers (built from the golden fixture, not an
# arbitrary marker blob -- MEDIUM-1 header/section-table validation requires
# genuinely valid bytes to reach ready=True)
# ---------------------------------------------------------------------------
def _write_asset(dir_path: pathlib.Path, marker: bytes = b"", with_br: bool = True):
    """Write manifest.json + <basename>.bin (+ optional REAL .bin.br) into
    dir_path, built from the committed golden-v1.bin fixture with an optional
    inert trailing ``marker`` appended to vary the content_hash across
    fixtures. The appended bytes sit past every section's
    ``[byte_offset, byte_offset+byte_length)`` range, so they are never
    referenced by the section table and don't affect header/section-table
    validity (MEDIUM-1).

    content_hash matches sha256(plain)[:12] and asset_basename is the
    canonical ``atlas-v1-<content_hash>`` form (MEDIUM-2) so
    ``web.atlas_assets.load_atlas_state()`` accepts it. The ``.bin.br``
    sidecar (when written) is REAL Brotli compression of ``plain`` -- MEDIUM-3
    requires it to actually decompress back to the exact plain bytes.

    Returns (basename, content_hash, plain_bytes, br_bytes_or_None)."""
    plain = _GOLDEN_BIN + marker
    content_hash = hashlib.sha256(plain).hexdigest()[:12]
    basename = f"atlas-v1-{content_hash}"
    (dir_path / f"{basename}.bin").write_bytes(plain)
    br_bytes = None
    if with_br:
        br_bytes = brotli.compress(plain, quality=11)
        (dir_path / f"{basename}.bin.br").write_bytes(br_bytes)
    manifest = {
        "schema_version": 1,
        "content_hash": content_hash,
        "asset_basename": basename,
        "node_count": 1,
    }
    (dir_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return basename, content_hash, plain, br_bytes


@pytest.fixture
def ready_asset(tmp_path, monkeypatch):
    """A loaded, flag-ON atlas state backed by a real (golden-fixture-derived)
    temp asset dir."""
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    basename, chash, plain, br_bytes = _write_asset(tmp_path)
    assert aa.load_atlas_state() is True
    monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", True)
    assert aa.atlas_preview_available() is True
    yield {"dir": tmp_path, "basename": basename, "content_hash": chash, "plain": plain, "br": br_bytes}
    # Restore module state so later tests / the real app are unaffected.
    aa.load_atlas_state()


def _data_endpoints():
    """Register the data routes onto a bare app and return the two endpoint
    callables. Calling them directly returns starlette Response objects whose
    raw ``.body`` / ``.headers`` we inspect WITHOUT httpx auto-decoding (httpx
    would eagerly try — and fail — to Brotli-decode a synthetic .bin.br body,
    and would strip the Content-Encoding header we need to assert)."""
    bare = FastAPI()
    wm._register_atlas_data_routes(bare)
    manifest_ep = asset_ep = None
    for route in bare.routes:
        path = getattr(route, "path", "")
        if path == "/atlas-data/manifest.json":
            manifest_ep = route.endpoint
        elif path == "/atlas-data/{asset_name}":
            asset_ep = route.endpoint
    assert manifest_ep is not None and asset_ep is not None
    return manifest_ep, asset_ep


# ---------------------------------------------------------------------------
# Minimal NiceGUI client-context runner for the /atlas route body
# ---------------------------------------------------------------------------
_SIM_READY = False


def _ensure_sim():
    global _SIM_READY
    if not _SIM_READY:
        from nicegui.testing.general import prepare_simulation
        prepare_simulation()
        _SIM_READY = True


def _collect_texts(client) -> list[str]:
    out = []
    for el in client.elements.values():
        for attr in ("text", "_text", "content"):
            val = getattr(el, attr, None)
            if isinstance(val, str) and val.strip():
                out.append(val)
    return out


def _run_atlas_route(monkeypatch, *, flag_on: bool, ready: bool):
    """Drive the REAL (decorated) wm.atlas_page_route() in a client context.

    Returns (rendered_texts, create_atlas_page_call_count). The shared shell +
    storage are isolated (monkeypatched) so this test targets the gating logic,
    not create_layout()."""
    _ensure_sim()
    from nicegui import ui
    from nicegui.client import Client
    import web.pages.atlas as atlas_mod

    calls = {"n": 0}

    def _recorder():
        calls["n"] += 1
        ui.label("REAL-ATLAS-CHROME-SENTINEL")

    monkeypatch.setattr(wm, "create_layout", lambda: ui.column())
    monkeypatch.setattr(wm, "safe_user_set", lambda *a, **k: None)
    monkeypatch.setattr(wm, "page_meta", lambda *a, **k: "")
    monkeypatch.setattr(wm, "apply_theme_immediately", lambda: "")
    monkeypatch.setattr(atlas_mod, "create_atlas_page", _recorder)
    monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", flag_on)
    monkeypatch.setattr(aa, "_state", aa._AtlasState(ready=ready))
    set_language("en")

    texts: list[str] = []

    async def _run():
        with Client(ui.page("/_atlas_probe")) as client:
            with client:
                wm.atlas_page_route()
        texts.extend(_collect_texts(client))

    asyncio.run(_run())
    return texts, calls["n"]


# ===========================================================================
# GROUP 1 — single-predicate references on the three server surfaces (AST)
# ===========================================================================
def _func_calls_predicate(func_node) -> bool:
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Name) and fn.id == "atlas_preview_available":
                return True
            if isinstance(fn, ast.Attribute) and fn.attr == "atlas_preview_available":
                return True
    return False


def test_all_three_surfaces_gate_on_the_single_predicate():
    tree = ast.parse(MAIN_SRC)
    by_name = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            by_name.setdefault(node.name, node)

    missing = []
    for name in _PREDICATE_SURFACES:
        node = by_name.get(name)
        if node is None:
            missing.append(f"{name} (function not found)")
        elif not _func_calls_predicate(node):
            missing.append(f"{name} (no atlas_preview_available() call)")
    assert not missing, (
        "These atlas surfaces must gate on atlas_preview_available(): "
        + ", ".join(missing)
    )


# ===========================================================================
# GROUP 2 — flag OFF -> clean-hide, create_atlas_page NOT called
# ===========================================================================
def test_flag_off_clean_hides_without_delegating(monkeypatch):
    texts, n_calls = _run_atlas_route(monkeypatch, flag_on=False, ready=True)
    assert n_calls == 0, "create_atlas_page must NOT be called when the flag is OFF"
    assert any("temporarily unavailable" in t for t in texts), (
        "the clean-hide card should render when the flag is OFF"
    )
    assert not any("REAL-ATLAS-CHROME-SENTINEL" in t for t in texts)


def test_flag_on_and_ready_delegates_to_create_atlas_page(monkeypatch):
    # The complementary case: proves the gate is not a constant always-hide.
    texts, n_calls = _run_atlas_route(monkeypatch, flag_on=True, ready=True)
    assert n_calls == 1, "create_atlas_page must be called when available"
    assert not any("temporarily unavailable" in t for t in texts)


def test_real_atlas_chrome_renders_banner_and_cls_reserved_canvas():
    # Render the REAL create_atlas_page (no recorder) to prove the chrome renders
    # bilingually with the honesty banner and a CLS-reserved (fixed-height) canvas.
    _ensure_sim()
    from nicegui import ui
    from nicegui.client import Client
    from web.pages.atlas import create_atlas_page, _ATLAS_CANVAS_HEIGHT_PX

    set_language("en")
    texts: list[str] = []
    with Client(ui.page("/_atlas_chrome_probe")) as client:
        with client:
            create_atlas_page()
        texts.extend(_collect_texts(client))

    blob = "\n".join(texts)
    assert "Connections Atlas" in blob
    assert "algorithmically derived" in blob, "honesty banner (D-15) must render"
    assert "atlas-canvas" in blob, "the <canvas> container must render"
    assert f"height:{_ATLAS_CANVAS_HEIGHT_PX}px" in blob, "canvas must reserve a fixed height (CLS-safe)"


# ===========================================================================
# GROUP 3 — flag ON but asset NOT loaded -> clean-hide + data routes 404
# ===========================================================================
def test_flag_on_asset_not_loaded_clean_hides(monkeypatch):
    texts, n_calls = _run_atlas_route(monkeypatch, flag_on=True, ready=False)
    assert n_calls == 0, "asset-not-loaded must clean-hide (no create_atlas_page)"
    assert any("temporarily unavailable" in t for t in texts)


def test_data_routes_404_when_asset_not_loaded(tmp_path, monkeypatch):
    # Flag ON, but the asset dir is empty -> load fails -> ready False.
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    assert aa.load_atlas_state() is False
    monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", True)
    assert aa.atlas_preview_available() is False
    manifest_ep, asset_ep = _data_endpoints()
    assert manifest_ep(if_none_match="").status_code == 404
    assert asset_ep(asset_name="atlas-v1-anything.bin", accept_encoding="").status_code == 404
    aa.load_atlas_state()  # restore


def test_data_routes_404_when_flag_off(ready_asset, monkeypatch):
    # Asset IS loaded, but the flag is OFF -> still unreachable.
    monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", False)
    assert aa.atlas_preview_available() is False
    manifest_ep, asset_ep = _data_endpoints()
    assert manifest_ep(if_none_match="").status_code == 404
    assert asset_ep(asset_name=f"{ready_asset['basename']}.bin", accept_encoding="").status_code == 404


# ===========================================================================
# GROUP 3B — structural/hash/brotli-integrity hardening (Codex round-4
# MEDIUM-1/2/3): each failure mode fails ready=False (or drops brotli only)
# independently of the others, with no traceback escaping load_atlas_state().
# ===========================================================================
def test_malformed_magic_fails_closed(tmp_path, monkeypatch):
    # MEDIUM-1: bad magic bytes -> ready=False.
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    bad_plain = b"NOTMAGIC" + struct.pack("<II", 1, 0)
    content_hash = hashlib.sha256(bad_plain).hexdigest()[:12]
    basename = f"atlas-v1-{content_hash}"
    (tmp_path / f"{basename}.bin").write_bytes(bad_plain)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "content_hash": content_hash, "asset_basename": basename}),
        encoding="utf-8",
    )
    assert aa.load_atlas_state() is False
    aa.load_atlas_state()  # restore


def test_truncated_section_table_fails_closed(tmp_path, monkeypatch):
    # MEDIUM-1: header claims 5 sections but the buffer holds none of them.
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    bad_plain = struct.pack("<8sII", b"ATLAS001", 1, 5) + b"\x00" * 8
    content_hash = hashlib.sha256(bad_plain).hexdigest()[:12]
    basename = f"atlas-v1-{content_hash}"
    (tmp_path / f"{basename}.bin").write_bytes(bad_plain)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "content_hash": content_hash, "asset_basename": basename}),
        encoding="utf-8",
    )
    assert aa.load_atlas_state() is False
    aa.load_atlas_state()


def test_out_of_bounds_section_fails_closed(tmp_path, monkeypatch):
    # MEDIUM-1: a well-formed header/table entry whose byte range overruns
    # the actual buffer (count*elem_size is internally consistent, but the
    # data simply isn't there).
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    header = struct.pack("<8sII", b"ATLAS001", 1, 1)
    entry = struct.pack("<IIIIQQ", 1, 1, 4, 1000, 48, 4000)  # claims 4000 bytes at offset 48
    bad_plain = header + entry + b"\x00" * 16  # buffer is only 64 bytes total
    content_hash = hashlib.sha256(bad_plain).hexdigest()[:12]
    basename = f"atlas-v1-{content_hash}"
    (tmp_path / f"{basename}.bin").write_bytes(bad_plain)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "content_hash": content_hash, "asset_basename": basename}),
        encoding="utf-8",
    )
    assert aa.load_atlas_state() is False
    aa.load_atlas_state()


def test_missing_content_hash_fails_closed(tmp_path, monkeypatch):
    # MEDIUM-2: manifest omits content_hash entirely -> refuse (no verified
    # content-hashed filename to apply an immutable cache to).
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    plain = _GOLDEN_BIN
    basename = f"atlas-v1-{hashlib.sha256(plain).hexdigest()[:12]}"
    (tmp_path / f"{basename}.bin").write_bytes(plain)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "asset_basename": basename}),  # no content_hash
        encoding="utf-8",
    )
    assert aa.load_atlas_state() is False
    aa.load_atlas_state()


def test_non_content_hashed_basename_fails_closed(tmp_path, monkeypatch):
    # MEDIUM-2: content_hash is correct, but asset_basename does not encode
    # it -> refuse rather than apply an immutable 1-year cache to an
    # unverified filename.
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    plain = _GOLDEN_BIN
    content_hash = hashlib.sha256(plain).hexdigest()[:12]
    basename = "atlas-v1-not-the-real-hash"
    (tmp_path / f"{basename}.bin").write_bytes(plain)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "content_hash": content_hash, "asset_basename": basename}),
        encoding="utf-8",
    )
    assert aa.load_atlas_state() is False
    aa.load_atlas_state()


def test_corrupt_brotli_sidecar_falls_back_to_plain(tmp_path, monkeypatch):
    # MEDIUM-3: a garbage (non-Brotli) .bin.br must never be served -- the
    # brotli representation drops, plain stays servable, readiness unaffected.
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    basename, _chash, plain, _br = _write_asset(tmp_path, with_br=False)
    (tmp_path / f"{basename}.bin.br").write_bytes(b"THIS-IS-NOT-VALID-BROTLI-DATA")
    assert aa.load_atlas_state() is True
    assert aa.atlas_br_bytes() is None
    assert aa.atlas_plain_bytes() == plain
    aa.load_atlas_state()


def test_mismatched_brotli_sidecar_falls_back_to_plain(tmp_path, monkeypatch):
    # MEDIUM-3: a VALID Brotli stream that decompresses to the WRONG content
    # (a stale/mismatched sidecar) must also be dropped, never served.
    monkeypatch.setattr(aa, "ATLAS_DATA_DIR", str(tmp_path))
    basename, _chash, plain, _br = _write_asset(tmp_path, with_br=False)
    wrong_br = brotli.compress(plain + b"tampered-extra-bytes", quality=11)
    (tmp_path / f"{basename}.bin.br").write_bytes(wrong_br)
    assert aa.load_atlas_state() is True
    assert aa.atlas_br_bytes() is None
    assert aa.atlas_plain_bytes() == plain
    aa.load_atlas_state()


# ===========================================================================
# GROUP 4 — response-level br/identity/* q-value negotiation + reachable 406
# ===========================================================================
def _ce(resp):
    return resp.headers.get("content-encoding")


def test_encoding_negotiation_response_level(ready_asset):
    _manifest_ep, asset = _data_endpoints()
    name = f"{ready_asset['basename']}.bin"
    plain = ready_asset["plain"]

    # Accept-Encoding: br  -> Content-Encoding: br + immutable + Vary
    r = asset(asset_name=name, accept_encoding="br")
    assert r.status_code == 200
    assert _ce(r) == "br"
    assert r.headers.get("vary") == "Accept-Encoding"
    assert "immutable" in r.headers.get("cache-control", "")
    assert r.body == ready_asset["br"]  # the REAL (integrity-verified) br bytes, verbatim

    # No Accept-Encoding header -> plain (identity default-acceptable), no CE
    r = asset(asset_name=name, accept_encoding="")
    assert r.status_code == 200 and _ce(r) is None
    assert r.body == plain
    assert r.headers.get("vary") == "Accept-Encoding"

    # br;q=0 -> plain (br refused), no CE
    r = asset(asset_name=name, accept_encoding="br;q=0")
    assert r.status_code == 200 and _ce(r) is None and r.body == plain

    # identity;q=0 -> 406 (reachable: no acceptable representation)
    r = asset(asset_name=name, accept_encoding="identity;q=0")
    assert r.status_code == 406
    assert r.headers.get("vary") == "Accept-Encoding"

    # *;q=0 -> 406 (reachable)
    assert asset(asset_name=name, accept_encoding="*;q=0").status_code == 406

    # bare * -> br (via wildcard)
    r = asset(asset_name=name, accept_encoding="*")
    assert r.status_code == 200 and _ce(r) == "br"

    # wrong asset name -> 404 (whitelist; never a filesystem path)
    assert asset(asset_name="atlas-v1-deadbeef00.bin", accept_encoding="").status_code == 404
    # a traversal-shaped name is also just a non-matching name -> 404
    assert asset(asset_name="../../etc/passwd", accept_encoding="").status_code == 404


def test_weighted_preference_negotiation_response_level(ready_asset):
    # MEDIUM-4 (RFC 9110 S12.5.3): the HIGHEST non-zero q-value wins -- a
    # client strongly preferring identity over br must get identity even
    # though a valid br representation is loaded and would otherwise be
    # merely-acceptable.
    _manifest_ep, asset = _data_endpoints()
    name = f"{ready_asset['basename']}.bin"
    r = asset(asset_name=name, accept_encoding="br;q=0.1, identity;q=1")
    assert r.status_code == 200
    assert _ce(r) is None
    assert r.body == ready_asset["plain"]

    # The reverse weighting still prefers br.
    r = asset(asset_name=name, accept_encoding="br;q=1, identity;q=0.1")
    assert r.status_code == 200
    assert _ce(r) == "br"


def test_negotiate_encoding_unit_weighted_preference():
    # Direct unit coverage of the negotiation function's RFC 9110 tie-break
    # and highest-non-zero-q selection (MEDIUM-4).
    assert wm._negotiate_encoding("br;q=0.1, identity;q=1", have_br=True, have_plain=True) == "identity"
    assert wm._negotiate_encoding("br;q=1, identity;q=0.1", have_br=True, have_plain=True) == "br"
    # Equal non-zero q -> tie-break prefers br.
    assert wm._negotiate_encoding("br;q=0.5, identity;q=0.5", have_br=True, have_plain=True) == "br"
    assert wm._negotiate_encoding("br;q=1", have_br=True, have_plain=True) == "br"
    assert wm._negotiate_encoding("", have_br=True, have_plain=True) == "identity"
    # br unavailable server-side (no .bin.br loaded) -> identity regardless of
    # the client's stated br preference.
    assert wm._negotiate_encoding("br;q=1", have_br=False, have_plain=True) == "identity"


def test_brotli_absent_fallback_and_reachable_406(ready_asset, monkeypatch):
    # Simulate the .bin.br representation being ABSENT.
    monkeypatch.setattr(aa, "_state", aa._AtlasState(
        ready=True,
        manifest_bytes=aa._state.manifest_bytes,
        manifest=aa._state.manifest,
        bin_name=aa._state.bin_name,
        plain_bytes=aa._state.plain_bytes,
        br_bytes=None,  # brotli representation gone
        etag=aa._state.etag,
    ))
    _manifest_ep, asset = _data_endpoints()
    name = f"{ready_asset['basename']}.bin"

    # Accept-Encoding: br, but no br bytes -> fall back to plain
    r = asset(asset_name=name, accept_encoding="br")
    assert r.status_code == 200 and _ce(r) is None
    assert r.body == ready_asset["plain"]

    # identity;q=0 with br unavailable -> the reachable missing-representation 406
    assert asset(asset_name=name, accept_encoding="identity;q=0").status_code == 406


# ===========================================================================
# GROUP 5 — manifest no-cache + ETag + 304, and the stale-manifest transition
# ===========================================================================
def test_manifest_cache_etag_and_304(ready_asset):
    manifest_ep, _asset = _data_endpoints()
    r = manifest_ep(if_none_match="")
    assert r.status_code == 200
    assert r.headers.get("cache-control") == "no-cache, must-revalidate"
    assert r.headers.get("vary") == "Accept-Encoding"
    etag = r.headers.get("etag")
    assert etag and etag.startswith('"')
    # If-None-Match with the current ETag -> 304
    r304 = manifest_ep(if_none_match=etag)
    assert r304.status_code == 304
    assert r304.headers.get("etag") == etag


def test_stale_manifest_transition_after_rebake(ready_asset):
    manifest_ep, asset = _data_endpoints()
    old_name = ready_asset["basename"]
    old_etag = manifest_ep(if_none_match="").headers.get("etag")
    assert asset(asset_name=f"{old_name}.bin", accept_encoding="").status_code == 200

    # Rebake: a NEW payload -> new content_hash -> new asset_basename.
    for f in ready_asset["dir"].iterdir():
        f.unlink()
    new_name, _new_hash, _new_plain, _new_br = _write_asset(ready_asset["dir"], marker=b"ATLAS-TEST-V2")
    assert aa.load_atlas_state() is True
    assert new_name != old_name

    # The OLD content-hashed asset URL now 404s (immutable cache never strands).
    assert asset(asset_name=f"{old_name}.bin", accept_encoding="").status_code == 404
    # The manifest now re-points to the NEW basename with a NEW (revalidated) ETag.
    r = manifest_ep(if_none_match="")
    assert r.status_code == 200
    assert json.loads(bytes(r.body))["asset_basename"] == new_name
    assert r.headers.get("etag") != old_etag
    # The NEW asset serves.
    assert asset(asset_name=f"{new_name}.bin", accept_encoding="").status_code == 200


# ===========================================================================
# GROUP 6 — every new string has a real Hebrew translation value
# ===========================================================================
_SAMPLE_HE_KEYS = [
    "Connections Atlas",
    "Beta",
    "Loading the atlas…",
    "Color by domain",
    "Color by library",
    "Skip intro",
    "Focus constellation",
    "Connections",
    "Search by title or shelfmark…",
    "Continuation (same-work evidence)",
    "Explore the Connections Atlas",
    "The Connections Atlas is temporarily unavailable",
    "The atlas could not be loaded.",
]


def test_new_strings_have_real_hebrew_values():
    set_language("he")
    try:
        offenders = []
        for key in _SAMPLE_HE_KEYS:
            value = tr(key)
            if value == key:
                offenders.append(f"{key!r} (returns English key)")
            elif value.isascii():
                offenders.append(f"{key!r} (value not Hebrew: {value!r})")
        assert not offenders, "Missing/English HE translations: " + "; ".join(offenders)
    finally:
        set_language("he")


# ===========================================================================
# GROUP 7 — flag-independent /static/atlas/* -> 404 regression guard (HIGH-1)
# ===========================================================================
def test_atlas_asset_dir_is_outside_static_mount():
    static_dir = os.path.abspath(wm.STATIC_DIR)
    atlas_dir = os.path.abspath(aa.ATLAS_DATA_DIR)
    # ATLAS_DATA_DIR must NOT be inside STATIC_DIR.
    assert os.path.commonpath([static_dir, atlas_dir]) != static_dir, (
        f"atlas_data ({atlas_dir}) must live OUTSIDE the /static mount ({static_dir})"
    )
    # And no 'atlas' subdirectory may exist under STATIC_DIR.
    assert not os.path.isdir(os.path.join(static_dir, "atlas")), (
        "the atlas asset must never be dropped under web/static/"
    )


@pytest.mark.parametrize("flag_on", [False, True])
def test_static_atlas_urls_404_regardless_of_flag(monkeypatch, flag_on):
    # Mount a real StaticFiles at /static exactly as web.main does; the asset is
    # not under it, so /static/atlas/* is unreachable in EITHER flag state.
    monkeypatch.setattr(aa, "ATLAS_PREVIEW_ENABLED", flag_on)
    bare = FastAPI()
    bare.mount("/static", StaticFiles(directory=wm.STATIC_DIR), name="static")
    client = TestClient(bare, raise_server_exceptions=False)
    for path in ("/static/atlas/atlas-v1-anything.bin",
                 "/static/atlas/manifest.json",
                 "/static/atlas/"):
        assert client.get(path).status_code == 404
