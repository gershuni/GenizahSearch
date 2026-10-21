"""NiceGUI monkey-patches applied at web-app startup.

Each patch function is guarded by a NiceGUI version check and must be
called before ``ui.run()`` / NiceGUI initialises its routes.
"""

from __future__ import annotations

import collections
import functools
import inspect
import json
import logging
import mimetypes
import os
import re
import ssl
import time
import uuid
from base64 import b64decode

from packaging.version import Version as _V

import nicegui as _nicegui
from nicegui.storage import RequestTrackingMiddleware as _NiceGUIRequestTrackingMiddleware
from starlette.middleware.sessions import SessionMiddleware as _StarletteSessionMiddleware
from starlette.requests import HTTPConnection

try:  # Starlette 1.x: a dict that records whether it was read or changed
    from starlette.middleware.sessions import Session as _StarletteSession
except ImportError:  # Starlette 0.x: a plain dict
    _StarletteSession = dict

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
_PATCH_AUDIT_THRESHOLD = _V('3.18.0')

# NiceGUI 3.18.0's ESM route checks ``filepath.is_file()`` (checked against the
# 3.18.0 wheel; the release notes do not say which release changed it), so a
# bare directory URL is already a 404 there.
_ESM_IS_FILE_FIXED_UPSTREAM = _V('3.18.0')
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
    """Bypass cookie parsing/signing for known-public assets and session-free endpoints.

    Also drops a validly signed session cookie whose body is not a JSON
    object, so the request starts a fresh session: Starlette 1.x builds its
    ``Session`` from the body and raises (HTTP 500) on a list, string or number.
    """

    async def __call__(self, scope, receive, send) -> None:
        if (
            scope.get('type') == 'http'
            and _skips_session(scope.get('path', ''))
        ):
            await self.app(scope, receive, send)
            return
        if scope.get('type') in ('http', 'websocket'):
            scope = self._without_non_object_session_cookie(scope)
        await super().__call__(scope, receive, send)

    def _without_non_object_session_cookie(self, scope):
        cookies = HTTPConnection(scope).cookies
        if self.session_cookie not in cookies:
            return scope
        try:
            data = self.signer.unsign(cookies[self.session_cookie].encode('utf-8'), max_age=self.max_age)
        except Exception:  # a bad or expired signature: Starlette starts a fresh session itself
            return scope
        try:
            if isinstance(json.loads(b64decode(data)), dict):
                return scope
        except Exception:  # signed, but not JSON: treated like a non-object body
            pass
        kept = '; '.join(f'{name}={value}' for name, value in cookies.items() if name != self.session_cookie)
        headers = [(key, value) for key, value in scope.get('headers', []) if key != b'cookie']
        if kept:
            headers.append((b'cookie', kept.encode('latin-1')))
        return {**scope, 'headers': headers}


def _validate_session_id(scope) -> None:
    """Session id validation, run before NiceGUI looks up or creates user storage.

    Keeps every session id in the form NiceGUI itself mints, a canonical
    lowercase uuid4 string: any other value is replaced with a fresh one.
    Every other session key is kept, so the validation signs nobody out.
    A session body that is not a JSON object becomes an empty session first.

    The session is then marked changed, so Starlette 1.x re-issues the cookie
    on every request as Starlette 0.x did. Otherwise a session that is never
    written after its id is minted keeps the expiry of its first response,
    and every visitor would lose their user storage (and sign-in) 14 days
    (Starlette's default max_age) after the first visit, however often
    they came back.
    """
    session = scope.get('session')
    if not isinstance(session, dict):
        session = _StarletteSession()
        scope['session'] = session
    if not is_canonical_session_id(session.get('id')):
        session['id'] = str(uuid.uuid4())
    mark_modified = getattr(session, 'mark_modified', None)
    if mark_modified is not None:
        mark_modified()


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
    Needed on NiceGUI 3.8.0; fixed upstream by 3.18.0 (``_ESM_IS_FILE_FIXED_UPSTREAM``).
    See also: .planning/debug/aggrid-dist-not-a-file.md
    """
    if _NV >= _ESM_IS_FILE_FIXED_UPSTREAM:
        logger.debug('ESM is_file patch skipped (NiceGUI %s has the upstream fix)', _NV)
        return
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.debug('ESM is_file patch skipped (NiceGUI %s exceeds audited %s)', _NV, _PATCH_AUDIT_THRESHOLD)
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


# NiceGUI 3.14 renders the tag from ``ui.run(language=...)`` and, with no
# language configured, emits ``dir="ltr"`` instead of a ``lang`` attribute.
_UPSTREAM_HTML_TAG = '<html{% if html_lang %} lang="{{ html_lang }}"{% else %} dir="ltr"{% endif %}>'
_PATCHED_HTML_TAG = '<html{% if html_lang %} lang="{{ html_lang }}"{% else %} lang="he"{% endif %}>'


def _patch_html_lang_attribute() -> None:
    """Ensure <html> tag has lang attribute for Lighthouse a11y compliance.

    NiceGUI's index.html template emits <html> with no lang (3.8), or, from
    3.14, ``<html dir="ltr">`` unless ``ui.run(language=...)`` is set. We do
    not set ``language``: it would also switch Quasar's language pack for
    every visitor, while the page script in web/main.py sets each visitor's
    own lang/dir/Quasar language. Patches the template file in-place at
    startup to add lang="he" (and, on 3.14+, drop the dir="ltr" the page
    script overrides anyway), as on 3.8. Idempotent (no-op if already
    patched) and re-applies on every boot so it survives pip install -U nicegui.
    """
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.debug('HTML lang patch skipped (NiceGUI %s exceeds audited %s)', _NV, _PATCH_AUDIT_THRESHOLD)
        return

    from pathlib import Path
    tmpl_file = Path(_nicegui.__file__).parent / 'templates' / 'index.html'
    try:
        original = tmpl_file.read_text(encoding='utf-8')
        if '<html>' in original:
            patched = original.replace('<html>', '<html lang="he">', 1)
        elif _UPSTREAM_HTML_TAG in original:
            patched = original.replace(_UPSTREAM_HTML_TAG, _PATCHED_HTML_TAG, 1)
        else:
            logger.debug('HTML lang patch skipped (already patched or template changed)')
            return
        tmpl_file.write_text(patched, encoding='utf-8')
        logger.debug('HTML lang patch applied (NiceGUI %s)', _NV)
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


# NiceGUI's own 404 handler, which ``_patch_sessionless_404`` still delegates to.
_nicegui_404_handler = None


def _patch_sessionless_404() -> None:
    """Answer a 404 that carries no session without building a NiceGUI client.

    ``_CacheSafeSessionMiddleware`` gives the requests of ``_skips_session`` paths
    no session. NiceGUI's 404 handler built its HTML error page in a new
    ``Client``, which lives for at least 60 s, and while it did, every
    ``prune_user_storage`` run raised on its first line (``client.request.session``
    asserts the middleware): no visitor's user storage was pruned. Triggers: any
    stale ``/_nicegui/<old version>/...`` URL after an upgrade, a bare ``esm``
    path, and on 3.8 every missing ``/static`` file. Those now get FastAPI's JSON
    404; pages keep NiceGUI's HTML one. tests/test_sessionless_404.py
    """
    global _nicegui_404_handler
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.warning(
            'Session-less 404 patch skipped: NiceGUI %s exceeds audited %s',
            _NV, _PATCH_AUDIT_THRESHOLD,
        )
        return

    from fastapi.exception_handlers import http_exception_handler
    from nicegui import app

    current = app.exception_handlers.get(404)
    if getattr(current, '_genizah_sessionless_404', False):
        return
    _nicegui_404_handler = current

    async def _sessionless_404(request, exc):
        if 'session' not in request.scope:
            return await http_exception_handler(request, exc)
        return await _nicegui_404_handler(request, exc)

    _sessionless_404._genizah_sessionless_404 = True
    app.add_exception_handler(404, _sessionless_404)
    logger.debug('Session-less 404 patch applied (NiceGUI %s)', _NV)


@functools.lru_cache(maxsize=1)
def _certifi_ca_text() -> str:
    import certifi

    with open(certifi.where(), encoding='ascii') as f:
        return f.read()


def _patch_httpx_ssl_context() -> None:
    """Build httpx's default SSL context from the CA bundle read once.

    ``get_user_client()`` builds a supabase client for each signed-in task, and
    that builds about four httpx clients, each calling
    ``ssl.create_default_context(cafile=certifi.where())``. OpenSSL 3.0 loads a
    CA *file* slowly (about 190 ms per context on the dev box; cpython#95031
    reports 40-60 ms on Ubuntu with OpenSSL 3.0.x), all on the event loop: about
    1.2 s per signed-in call locally. The same certificates from text (cadata)
    give the same context much faster. Each client still gets its OWN context:
    httpcore sets ALPN on a context for each connection, so a shared one would
    race. SSL_CERT_FILE / SSL_CERT_DIR and any non-default ``verify`` keep
    httpx's own code. tests/test_httpx_ssl_context_patch.py
    """
    import httpx

    if not httpx.__version__.startswith('0.28.'):
        logger.warning('httpx SSL context patch skipped: httpx %s is not the audited 0.28.x',
                       httpx.__version__)
        return

    import httpx._transports.default as _transports

    original = _transports.create_ssl_context
    if getattr(original, '_genizah_cadata', False):
        return

    def create_ssl_context(verify=True, cert=None, trust_env=True):
        if (verify is True and not cert
                and not (trust_env and (os.environ.get('SSL_CERT_FILE') or os.environ.get('SSL_CERT_DIR')))):
            return ssl.create_default_context(cadata=_certifi_ca_text())
        return original(verify=verify, cert=cert, trust_env=trust_env)

    create_ssl_context._genizah_cadata = True
    _transports.create_ssl_context = create_ssl_context
    logger.debug('httpx SSL context patch applied (httpx %s)', httpx.__version__)


# NiceGUI asks ``helpers.expects_arguments`` -- an ``inspect.signature`` per call --
# for the change handler of the Classes, Style and Props of every element it builds
# (observables.py) and for every event handler it registers. Those three handlers
# are bound methods of the same three functions (``Classes._update`` and friends),
# so the answer never changes; on 3.18 the question was 31% of a 1,204-element
# build on the dev box. Kept until NiceGUI answers it once itself.
_nicegui_expects_arguments = None  # NiceGUI's own, kept by the patch (tests compare against it)
_expects_arguments_by_function: dict = {}


def _expects_arguments_memo(func) -> bool:
    """``helpers.expects_arguments``, remembered per function for BOUND METHODS only.

    A bound method's signature is its function's without ``self``, the same for
    every instance, so the answer is kept by ``func.__func__``: a function object
    its class keeps alive anyway. Plain functions and lambdas pass straight
    through: keeping them would keep their closures, and the elements those
    capture, alive.
    """
    if inspect.ismethod(func):
        key = func.__func__
        try:
            return _expects_arguments_by_function[key]
        except KeyError:
            pass
        except TypeError:  # an unhashable callable bound as a method: no memo
            return _nicegui_expects_arguments(func)
        result = _nicegui_expects_arguments(func)
        _expects_arguments_by_function[key] = result
        return result
    return _nicegui_expects_arguments(func)


def _patch_expects_arguments_memo() -> None:
    """Answer NiceGUI's per-element signature question once per function.

    Callers resolve the name through the package namespace (``helpers.expects_arguments``
    in observables.py, events.py, event.py, client.py and page.py), so the package
    attribute is what is rebound; ``nicegui.helpers.functions`` keeps its own.
    """
    global _nicegui_expects_arguments
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.warning(
            'expects_arguments memo patch skipped: NiceGUI %s exceeds audited %s',
            _NV, _PATCH_AUDIT_THRESHOLD,
        )
        return
    from nicegui import helpers

    if helpers.expects_arguments is _expects_arguments_memo:
        return
    _nicegui_expects_arguments = helpers.expects_arguments
    helpers.expects_arguments = _expects_arguments_memo
    logger.debug('expects_arguments memo patch applied (NiceGUI %s)', _NV)


# NiceGUI's own ``_on_handshake``, kept by ``_patch_handshake_counter`` (tests compare against it).
_nicegui_on_handshake = None
_HANDSHAKE_CATEGORIES = ('accepted', 'unknown_client', 'refused_other')
_HANDSHAKE_STATS = {name: {'count': 0, 'first': None, 'last': None} for name in _HANDSHAKE_CATEGORIES}
_HANDSHAKE_STATS['burst_warnings'] = 0
_UNKNOWN_CLIENT_RECENT = collections.deque()  # timestamps of unknown_client refusals in the last minute
_HANDSHAKE_BURST_WINDOW_S = 60.0
_HANDSHAKE_BURST_THRESHOLD = 30  # more than this many unknown clients in a minute is a reload burst
_handshake_burst_warned_at = 0.0


def _record_handshake(category: str) -> None:
    global _handshake_burst_warned_at
    now = time.time()
    entry = _HANDSHAKE_STATS[category]
    entry['count'] += 1
    entry['last'] = now
    if entry['first'] is None:
        entry['first'] = now
    if category != 'unknown_client':
        return
    _UNKNOWN_CLIENT_RECENT.append(now)
    while _UNKNOWN_CLIENT_RECENT and _UNKNOWN_CLIENT_RECENT[0] < now - _HANDSHAKE_BURST_WINDOW_S:
        _UNKNOWN_CLIENT_RECENT.popleft()
    if (len(_UNKNOWN_CLIENT_RECENT) > _HANDSHAKE_BURST_THRESHOLD
            and now - _handshake_burst_warned_at >= _HANDSHAKE_BURST_WINDOW_S):
        _handshake_burst_warned_at = now
        _HANDSHAKE_STATS['burst_warnings'] += 1
        logger.warning('reload burst: %d handshakes from unknown clients in the last minute '
                       '(open tabs reloading after a restart; web/static/js/reconnect_backoff.js spreads them)',
                       len(_UNKNOWN_CLIENT_RECENT))


def _handshake_stats() -> dict:
    """A snapshot for ``/_internal/memstat`` (registered as ``handshakes``)."""
    snapshot = {name: dict(_HANDSHAKE_STATS[name]) for name in _HANDSHAKE_CATEGORIES}
    snapshot['burst_warnings'] = _HANDSHAKE_STATS['burst_warnings']
    cutoff = time.time() - _HANDSHAKE_BURST_WINDOW_S
    snapshot['unknown_client_last_minute'] = sum(1 for t in _UNKNOWN_CLIENT_RECENT if t >= cutoff)
    return snapshot


def _patch_handshake_counter() -> None:
    """Count the handshakes NiceGUI accepts and refuses, without changing one.

    After a restart every open tab reconnects with a client id the new process
    never had, and ``_on_handshake`` refuses it (the tab then reloads: see
    web/reconnect_backoff.py). Nothing recorded how many, so a reload storm was
    visible only as load. The wrapper is transparent -- same bool, same
    exceptions, same ``Client`` state -- and counts ``accepted``,
    ``unknown_client`` (``Client.instances`` has no such id) and
    ``refused_other`` (``accept_handshake`` said no), with first/last times;
    more than 30 unknown clients in a minute logs ONE "reload burst" warning.
    Rebinding the module global covers the implicit path (``_on_connect`` looks
    ``_on_handshake`` up at call time); ``sio.on('handshake', ...)`` replaces the
    explicit one. Snapshot: ``web.api.get_runtime_cache_stats()['handshakes']``.
    tests/test_handshake_counter.py
    """
    if _NV > _PATCH_AUDIT_THRESHOLD:
        logger.warning('Handshake counter patch skipped: NiceGUI %s exceeds audited %s',
                       _NV, _PATCH_AUDIT_THRESHOLD)
        return

    import nicegui.nicegui as _server
    from nicegui import core
    from nicegui.client import Client

    global _nicegui_on_handshake
    current = _server._on_handshake
    if getattr(current, '_genizah_handshake_counter', False):
        return
    _nicegui_on_handshake = current

    @functools.wraps(current)
    async def _counted_handshake(sid, data):
        client_id = data.get('client_id') if isinstance(data, dict) else None
        known = Client.instances.get(client_id) is not None
        try:
            accepted = await current(sid, data)
        except BaseException:
            _record_handshake('refused_other')
            raise
        _record_handshake('accepted' if accepted else ('refused_other' if known else 'unknown_client'))
        return accepted

    _counted_handshake._genizah_handshake_counter = True
    _server._on_handshake = _counted_handshake
    core.sio.on('handshake', _counted_handshake)
    try:
        from web.api import register_runtime_cache_stats
        register_runtime_cache_stats('handshakes', _handshake_stats)
    except Exception as e:  # the counter still counts; only /_internal/memstat misses it
        logger.warning('Handshake counter stats not registered: %s', e)
    logger.debug('Handshake counter patch applied (NiceGUI %s)', _NV)


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
        ('Session-less 404', _patch_sessionless_404),
        ('httpx SSL context', _patch_httpx_ssl_context),
        ('expects_arguments memo', _patch_expects_arguments_memo),
        ('Handshake counter', _patch_handshake_counter),
    ]:
        try:
            fn()
        except Exception as e:
            logger.warning('Patch "%s" failed unexpectedly on NiceGUI %s: %s', name, _NV, e)
