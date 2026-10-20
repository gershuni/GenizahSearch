"""NiceGUI monkey-patches applied at web-app startup.

Each patch function is guarded by a NiceGUI version check and must be
called before ``ui.run()`` / NiceGUI initialises its routes.
"""

from __future__ import annotations

import logging
import mimetypes
import re
import uuid

from packaging.version import Version as _V

import nicegui as _nicegui
from nicegui.storage import RequestTrackingMiddleware as _NiceGUIRequestTrackingMiddleware
from starlette.middleware.sessions import SessionMiddleware as _StarletteSessionMiddleware

from web.session_hardening import is_canonical_session_id

logger = logging.getLogger(__name__)
_NV = _V(_nicegui.__version__)

# Patches below are guarded against NiceGUI versions they were tested against.
# When the running version exceeds this threshold, all patches are skipped and
# this module logs a WARNING asking the dev to re-audit whether the upstream
# bugs (ESM is_file handler, missing <html lang>) were actually fixed upstream
# before removing the patch files. Bump this constant deliberately after audit.
# Skipping the static-asset session patch also skips session id validation, so
# the web app's startup block refuses to start on such a version
# (web.session_hardening.require_session_id_validation).
_PATCH_AUDIT_THRESHOLD = _V('3.8.0')
if _NV > _PATCH_AUDIT_THRESHOLD:
    logger.warning(
        'NiceGUI %s exceeds framework_patches audit threshold %s. '
        'Re-audit upstream fixes for static-asset sessions, ESM is_file handler, and <html lang> '
        'template, then bump _PATCH_AUDIT_THRESHOLD or remove obsolete patches.',
        _NV, _PATCH_AUDIT_THRESHOLD,
    )


_VERSIONED_NICEGUI_ASSET_RE = re.compile(
    # ``/_nicegui/auto/...`` serves files registered at run time (for example
    # single-use downloads), not versioned framework assets.
    r'^/_nicegui/(?!auto/)[^/]+/(?:static|libraries|components|esm)(?:/|$)'
)
_CONTENT_HASHED_ATLAS_RE = re.compile(
    r'^/atlas-data/atlas-v1-[0-9a-f]{12}\.bin$'
)


def _is_public_cacheable_asset_path(path: str) -> bool:
    """Return whether ``path`` is public, session-free, and safe to CDN-cache.

    Deliberately excluded: page HTML, APIs, auth, ``/_nicegui_ws``, NiceGUI
    dynamic resources, and the mutable Atlas manifest. Those must retain normal
    session behavior and/or revalidation semantics.
    """
    return (
        path == '/favicon.ico'
        or path == '/static'
        or path.startswith('/static/')
        or bool(_VERSIONED_NICEGUI_ASSET_RE.match(path))
        or bool(_CONTENT_HASHED_ATLAS_RE.match(path))
    )


# The public MCP endpoint (web/mcp_server.py: MCP_PATH and its slash redirect), and the
# static pages the AI-directory listings link to (web/public_pages.py). MCP clients are
# servers, and crawlers read the legal pages without cookies: a session would be
# discarded, and NiceGUI would create user storage for every request. NOT cacheable
# assets -- tool results must never be cached -- so this is a separate predicate. Kept as
# literals so this module imports neither the MCP SDK nor the pages; tests pin them
# (tests/test_mcp_sessions.py, tests/test_public_pages.py).
_SESSION_FREE_PATHS = frozenset({'/mcp', '/mcp/', '/terms', '/support', '/ai/privacy',
                                 '/.well-known/openai-apps-challenge'})


def _is_session_free_path(path: str) -> bool:
    """Return whether ``path`` must never read, create or reissue a session."""
    return path in _SESSION_FREE_PATHS


# The public library image proxies in web/api.py (``init_api_routes``). Each answers from
# its path and query alone -- no session, user storage, auth or per-visitor limit -- so
# without a cookie Cloudflare and browsers can keep the image bytes (their Cache-Control
# is set by ``_image_cache_headers`` there). One path segment after the route name, so a
# longer route name never matches. NOT the puzzle routes: /api/puzzle_* serve a
# signed-out upload only to its own browser. tests/test_image_route_sessions.py pins it.
_PUBLIC_IMAGE_ROUTE_RE = re.compile(
    r'^/api/(?:nli_image|nli_image_by_sysid|cambridge_image|manchester_image'
    r'|jts_image|oxford_image)/[^/]+'
)


def _is_public_image_route(path: str) -> bool:
    """Return whether ``path`` is one of the session-free library image proxies."""
    return bool(_PUBLIC_IMAGE_ROUTE_RE.fullmatch(path))


def _skips_session(path: str) -> bool:
    return (_is_public_cacheable_asset_path(path) or _is_session_free_path(path)
            or _is_public_image_route(path))


class _CacheSafeSessionMiddleware(_StarletteSessionMiddleware):
    """Bypass cookie parsing/signing for known-public assets and session-free endpoints."""

    async def __call__(self, scope, receive, send) -> None:
        if (
            scope.get('type') == 'http'
            and _skips_session(scope.get('path', ''))
        ):
            await self.app(scope, receive, send)
            return
        await super().__call__(scope, receive, send)


def _validate_session_id(scope) -> None:
    """Session id validation, run before NiceGUI looks up or creates user storage.

    Keeps every session id in the form NiceGUI itself mints, a canonical
    lowercase uuid4 string: any other value is replaced with a fresh one.
    Every other session key is kept, so the validation signs nobody out.
    A session body that is not a JSON object becomes an empty session first.
    """
    session = scope.get('session')
    if not isinstance(session, dict):
        session = {}
        scope['session'] = session
    if not is_canonical_session_id(session.get('id')):
        session['id'] = str(uuid.uuid4())


class _CacheSafeRequestTrackingMiddleware(_NiceGUIRequestTrackingMiddleware):
    """Skip NiceGUI user storage for public assets and session-free endpoints; validate the session id otherwise.

    ``web.session_hardening.require_session_id_validation()`` refuses to start
    the web app unless this class is the one NiceGUI installs.
    """

    async def dispatch(self, request, call_next):
        if _skips_session(request.url.path):
            # _CacheSafeSessionMiddleware bypassed these paths: no session in scope.
            return await call_next(request)
        _validate_session_id(request.scope)
        return await super().dispatch(request, call_next)


def _patch_static_asset_session_middleware() -> None:
    """Install cache-safe NiceGUI middleware classes before ``ui.run``.

    NiceGUI imports both middleware classes into ``nicegui.storage`` and later
    constructs them inside ``set_storage_secret``. Replacing those two module
    references here changes only the instances NiceGUI is about to install; it
    does not monkey-patch Starlette globally.

    The request-tracking class also carries the session id validation. When
    this patch is skipped (NiceGUI newer than the audited version),
    ``web.session_hardening.require_session_id_validation()`` makes the web
    app refuse to start rather than run without it.
    """
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.warning(
            'Static asset session patch skipped: NiceGUI %s exceeds audited %s',
            _NV, _PATCH_AUDIT_THRESHOLD,
        )
        return

    import nicegui.storage as _storage

    _storage.SessionMiddleware = _CacheSafeSessionMiddleware
    _storage.RequestTrackingMiddleware = _CacheSafeRequestTrackingMiddleware
    logger.debug('Static asset session patch applied (NiceGUI %s)', _NV)


def _patch_nicegui_esm_handler() -> None:
    """Add is_file() guard to NiceGUI's ESM route handler.

    NiceGUI's ESM route handler checks filepath.exists() but not filepath.is_file().
    When a browser or bot requests the bare directory URL (e.g. /_nicegui/.../esm/{key}/)
    the path resolves to the dist/ directory itself, passes exists(), and then
    Starlette's FileResponse crashes with:
        RuntimeError: File at path .../aggrid/dist is not a file.

    Fix: override the route to add an is_file() check, returning 404 for directories.
    Still needed as of NiceGUI 3.8.0 -- track upstream for fix.
    See also: .planning/debug/aggrid-dist-not-a-file.md
    """
    # Guard: ESM is_file bug confirmed present in NiceGUI <= _PATCH_AUDIT_THRESHOLD
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.debug('ESM is_file patch skipped (NiceGUI %s >= fix threshold)', _NV)
        return

    from starlette.responses import FileResponse
    from fastapi import HTTPException
    from nicegui import __version__ as _nv
    from nicegui.dependencies import esm_modules
    from nicegui import app

    route_path = f'/_nicegui/{_nv}/esm/{{key}}/{{path:path}}'

    # Remove the existing route so we can replace it
    app.routes[:] = [r for r in app.routes if getattr(r, 'path', None) != route_path]

    @app.get(route_path)
    def _get_esm_patched(key: str, path: str) -> FileResponse:
        if key in esm_modules:
            filepath = esm_modules[key].path / path
            if not filepath.resolve().is_relative_to(esm_modules[key].path.resolve()):
                raise HTTPException(status_code=403, detail='forbidden')
            if filepath.exists() and filepath.is_file():
                media_type, _ = mimetypes.guess_type(filepath)
                return FileResponse(filepath, media_type=media_type)
        raise HTTPException(status_code=404, detail=f'ESM module "{key}" not found')

    logger.debug('ESM is_file patch applied (NiceGUI %s)', _NV)


def _patch_html_lang_attribute() -> None:
    """Ensure <html> tag has lang attribute for Lighthouse a11y compliance.

    NiceGUI's index.html template emits <html> with no lang. Patches the
    template file in-place at startup to add lang="he". Idempotent (no-op
    if already patched) and re-applies on every boot so it survives
    pip install -U nicegui.

    Still needed as of NiceGUI 3.8.0 -- no upstream lang= support yet.
    """
    # Guard: html lang attribute not supported natively in NiceGUI <= _PATCH_AUDIT_THRESHOLD
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.debug('HTML lang patch skipped (NiceGUI %s >= fix threshold)', _NV)
        return

    from pathlib import Path
    tmpl_file = Path(_nicegui.__file__).parent / 'templates' / 'index.html'
    try:
        original = tmpl_file.read_text(encoding='utf-8')
        if '<html>' in original:
            patched = original.replace('<html>', '<html lang="he">', 1)
            tmpl_file.write_text(patched, encoding='utf-8')
            logger.debug('HTML lang patch applied (NiceGUI %s)', _NV)
        else:
            logger.debug('HTML lang patch skipped (already patched or template changed)')
    except Exception as e:
        logger.warning('HTML lang patch failed: %s', e)


# NiceGUI 3.15.0 indexes bindings by object, so removing an element's bindings
# no longer walks the whole process (checked against the 3.14.0/3.15.0 wheels).
_BINDING_REMOVE_FIXED_UPSTREAM = _V('3.15.0')
_nicegui_binding_remove = None  # NiceGUI's own, kept by the patch (tests compare against it)


def _remove_bindings_without_marker_scan(objects) -> None:
    """``nicegui.binding.remove`` without its walk over every property marker.

    NiceGUI 3.8 ends ``binding.remove`` with a loop over ``bindable_properties``,
    which holds a marker for every bindable property of every live element on
    every open page in the process. Each ``element.clear()`` paid for that walk,
    so one page's cleanup got slower with every other tab open -- the
    ``binding.remove`` under ``web/pages/browse.py`` ``clear()`` seen when open
    tabs reloaded in a loop after a restart (tracker: "A website restart can set
    off reload loops from open pages"). The markers are weak references and go
    with their objects; NiceGUI 3.15 stopped deleting them here for that reason.
    The binding links themselves are removed exactly as before.
    """
    from nicegui import binding

    object_ids = set(map(id, objects))
    if not object_ids:
        return
    binding.active_links[:] = [
        (source_obj, source_name, target_obj, target_name, transform)
        for source_obj, source_name, target_obj, target_name, transform in binding.active_links
        if id(source_obj) not in object_ids and id(target_obj) not in object_ids
    ]
    for key, binding_list in list(binding.bindings.items()):
        binding_list[:] = [
            (source_obj, target_obj, target_name, transform)
            for source_obj, target_obj, target_name, transform in binding_list
            if id(source_obj) not in object_ids and id(target_obj) not in object_ids
        ]
        if not binding_list:
            del binding.bindings[key]


def _patch_binding_remove() -> None:
    """Make element cleanup cost independent of how many pages are open."""
    if _NV >= _BINDING_REMOVE_FIXED_UPSTREAM:
        logger.debug('binding.remove patch skipped (NiceGUI %s has the upstream fix)', _NV)
        return
    from nicegui import binding

    global _nicegui_binding_remove
    if binding.remove is not _remove_bindings_without_marker_scan:
        _nicegui_binding_remove = binding.remove
    binding.remove = _remove_bindings_without_marker_scan
    logger.debug('binding.remove patch applied (NiceGUI %s)', _NV)


def apply_all_patches() -> None:
    """Apply all NiceGUI monkey-patches. Call once before ui.run().

    Each patch is independently guarded by NiceGUI version. Failures on
    supported versions are logged at WARNING level so they surface in logs.
    """
    for name, fn in [
        ('Static asset sessions', _patch_static_asset_session_middleware),
        ('ESM is_file', _patch_nicegui_esm_handler),
        ('HTML lang', _patch_html_lang_attribute),
        ('binding.remove', _patch_binding_remove),
    ]:
        try:
            fn()
        except Exception as e:
            logger.warning('Patch "%s" failed unexpectedly on NiceGUI %s: %s', name, _NV, e)
