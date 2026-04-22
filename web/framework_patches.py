"""NiceGUI monkey-patches applied at web-app startup.

Each patch function is guarded by a NiceGUI version check and must be
called before ``ui.run()`` / NiceGUI initialises its routes.
"""

from __future__ import annotations

import logging
import mimetypes

from packaging.version import Version as _V

import nicegui as _nicegui

logger = logging.getLogger(__name__)
_NV = _V(_nicegui.__version__)

# Patches below are guarded against NiceGUI versions they were tested against.
# When the running version exceeds this threshold, all patches are skipped and
# this module logs a WARNING asking the dev to re-audit whether the upstream
# bugs (ESM is_file handler, missing <html lang>) were actually fixed upstream
# before removing the patch files. Bump this constant deliberately after audit.
_PATCH_AUDIT_THRESHOLD = _V('3.8.0')
if _NV > _PATCH_AUDIT_THRESHOLD:
    logger.warning(
        'NiceGUI %s exceeds framework_patches audit threshold %s. '
        'Re-audit upstream fixes for ESM is_file handler and <html lang> '
        'template, then bump _PATCH_AUDIT_THRESHOLD or remove obsolete patches.',
        _NV, _PATCH_AUDIT_THRESHOLD,
    )


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


def apply_all_patches() -> None:
    """Apply all NiceGUI monkey-patches. Call once before ui.run().

    Each patch is independently guarded by NiceGUI version. Failures on
    supported versions are logged at WARNING level so they surface in logs.
    """
    for name, fn in [
        ('ESM is_file', _patch_nicegui_esm_handler),
        ('HTML lang', _patch_html_lang_attribute),
    ]:
        try:
            fn()
        except Exception as e:
            logger.warning('Patch "%s" failed unexpectedly on NiceGUI %s: %s', name, _NV, e)
