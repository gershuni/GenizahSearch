# -*- coding: utf-8 -*-
"""Are the scholarly editions searched on this website, and for whom (editions plan §3.6)?

The switch (``EDITIONS_SEARCH_ENABLED``: off / on / ``editors``, shared/editions_search.py)
AND the web process's own engine holding a coherent editions generation, never the switch
alone. The generation is opened beside the engine once, at startup (``attach_at_startup``):
rows found in an edition are read again through it (copy, exports, the browse endpoint). A
deploy stops the service before it promotes a new generation (plan §3.7), so the startup
check is the one that counts; each research worker checks it again for its own request.
"""
import logging
import threading

LOGGER = logging.getLogger(__name__)
_resolver = None
_resolver_lock = threading.Lock()


def resolver():
    """The process's EditionResolver (the sidecars' paths, found once)."""
    global _resolver
    with _resolver_lock:
        if _resolver is None:
            from shared.editions_search import EditionResolver
            _resolver = EditionResolver()
    return _resolver


def edition_for_page(sys_id, version):
    """``(entry, resolved)`` for an edition locator (``fgp:<id>`` / ``pgp:<id>``) on manuscript
    *sys_id*: the editions index's entry (its page, its credit; SearchEngine.edition_entry) and
    the edition as the sidecar holds it now (EditionResolver.resolve: its original text). None
    when that edition of that manuscript is not in the open generation or no longer in the
    sidecar."""
    from web.state import state
    find = getattr(state.searcher, 'edition_entry', None)
    entry = find(sys_id, version) if find is not None else None
    if entry is None:
        return None
    resolved = resolver().resolve(version)
    if resolved is None:
        return None
    return entry, resolved


def attach_at_startup(engine) -> bool:
    """Open the editions generation beside the web process's *engine* when the switch is not
    off. Called by web/main.py's engine initialisation, in its worker thread (it opens an
    index). Returns whether the editions are open; a failure is logged and leaves them out."""
    from shared.dictionary_switch import OFF
    from shared.editions_search import editions_switch
    if editions_switch() == OFF:
        return False
    try:
        opened = bool(engine.attach_editions())
    except Exception:
        LOGGER.exception("The editions index could not be opened; searches leave the editions out")
        return False
    if not opened:
        LOGGER.warning("EDITIONS_SEARCH_ENABLED is set but no coherent editions index is installed; "
                       "searches leave the editions out")
    else:
        # The inputs it was built from, against those installed: off the startup path (it
        # reads the sidecars through, to fingerprint them).
        threading.Thread(target=_check_drift, args=(engine,), daemon=True, name='editions-drift').start()
    return opened


_drift = {'checked': False, 'changed': []}

#: The inputs a generation records (shared/editions_indexer.py::input_stamp) and how they read.
_DRIFT_KEYS = (('fgp', 'the FGP sidecar'), ('pgp', 'the PGP sidecar'), ('browse_map', 'the main browse map'),
               ('nli_crossref', 'the NLI crossref'), ('cleaner_version', 'the cleaner'),
               ('builder_version', 'the builder'))


def _check_drift(engine) -> None:
    """Compare the open generation's recorded inputs with the installed ones (editions plan
    section 3.7): a mismatch means the index was not rebuilt after an input changed. Logged,
    and shown on the admin page (``editions_drift``); the stale-hit path covers what changed."""
    import json
    import os
    try:
        from shared.config import Config
        from shared.editions_indexer import META_FILENAME, changed_inputs, default_generation_dir, input_stamp
        child = getattr(engine, '_editions', None)
        gen_dir = getattr(child, '_editions_dir', None) or default_generation_dir()
        with open(os.path.join(gen_dir, META_FILENAME), encoding='utf-8') as f:
            recorded = (json.load(f) or {}).get('inputs') or {}
        r = resolver()
        nli = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'nli_data', 'nli_crossref.db')
        installed = input_stamp(r._fgp_db, r._pgp_db, Config.BROWSE_MAP, nli if os.path.isfile(nli) else None)
    except Exception:
        LOGGER.exception("The editions index's inputs could not be compared with the installed ones")
        return

    differ = set(changed_inputs(recorded, installed))
    changed = [label for key, label in _DRIFT_KEYS if key in differ]
    _drift.update(checked=True, changed=changed)
    if changed:
        LOGGER.warning("The editions index was built from other inputs than those installed (%s): rebuild it "
                       "(scripts/deploy_editions_index.ps1). Hits in changed editions open as changed.",
                       ', '.join(changed))


def editions_drift() -> list:
    """What changed since the open editions index was built (for the admin page): empty when
    nothing did, or before the check has run."""
    return list(_drift['changed'])


def editions_ready() -> bool:
    """The web process's engine has a coherent editions generation open."""
    from web.state import state
    ready = getattr(state.searcher, 'editions_ready', None)
    if ready is None:
        return False
    try:
        return bool(ready())
    except Exception:
        return False


def served_to(editor: bool) -> bool:
    """A website reader gets the editions: the switch serves them (``editors``: *editor* is a
    signed-in editor or admin) and the index is ready."""
    from shared.editions_search import editions_served_to
    return editions_served_to(editor) and editions_ready()


def served_to_api() -> bool:
    """The public API, MCP and the ChatGPT facade, which know no reader: only when the switch is
    on for everyone (``editors`` is a preview on the website)."""
    from shared.dictionary_switch import ON
    from shared.editions_search import editions_switch
    return editions_switch() == ON and editions_ready()
