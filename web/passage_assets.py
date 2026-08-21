# -*- coding: utf-8 -*-
"""Fail-closed asset-state source for passage-matching parallels search
(Phase 145). Modeled on ``web/discovery_assets.py``'s flag-AND-readiness
contract -- mirror it, don't reinvent -- but simplified because
``shared/passage_index.py::open_index`` already IS the fail-closed artifact
validator (exact manifest, layout version, normalizer version, bit budgets,
byte order, CSR sanity, declared-vs-actual file sizes). This module does not
duplicate that; it gates it behind the feature flag and exposes the two
things the rest of the web app asks about the passage index:

  1. "Is passage-matching parallels search available right now?"
        -> ``passage_available()``
  2. "Give me a CompositionSearcher-shaped wrapper over it, or None."
        -> ``get_passage_searcher(text_fetcher)``

The index lives in a repo-root ``passage_index/current/`` directory by
default -- gitignored, multi-GB, machine-local (built by the Phase
141-143 build tooling, never shipped in the repo or the installer; see
``docs/specs/passage-matching-algorithm.md``). ``GENIZAH_PASSAGE_DATA_DIR``
overrides the directory (dev/CI-only), read ONCE at import -- mirrors
``web/discovery_assets.py``'s ``GENIZAH_DISCOVERY_DATA_DIR`` exactly, so a
later ``os.environ`` mutation in-process has no effect; tests instead
monkeypatch this module's ``PASSAGE_DATA_DIR`` attribute directly.

``load_passage_state()`` is called ONCE at startup from ``web/main.py``,
mirroring the ``load_discovery_state()`` / ``load_atlas_state()`` wiring
points. Fail-closed: ANY failure (directory absent, manifest missing or
malformed, a layout/normalizer/schema mismatch, a declared count that
disagrees with an actual file size -- the full list is
``shared/passage_index.py::open_index``'s own docstring) leaves the module
state unready with no traceback ever escaping this function, so
``passage_available()`` reads False and the method selector on
``web/pages/parallels.py`` (and the ``method='passage'`` branch of
``POST /api/parallels``) hide/reject cleanly.
"""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from web.feature_flags import PASSAGE_PARALLELS_ENABLED

if TYPE_CHECKING:
    from shared.passage_parallels import PageTextFetcher

logger = logging.getLogger(__name__)

# Repo-root passage_index/current/ -- deliberately OUTSIDE web/static/ (same
# posture as discovery_data/ and atlas_data/): a multi-GB gitignored asset
# must never become reachable through the public /static mount.
_DEFAULT_PASSAGE_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "passage_index", "current",
)

# DEV/CI-ONLY directory override, read ONCE at import (mirrors
# web/discovery_assets.py::GENIZAH_DISCOVERY_DATA_DIR / DISCOVERY_DATA_DIR
# exactly). Widens no trust boundary -- it selects WHICH directory is read;
# shared.passage_index.open_index still applies its full fail-closed
# contract to whatever it finds there.
_PASSAGE_DATA_DIR_ENV = "GENIZAH_PASSAGE_DATA_DIR"
PASSAGE_DATA_DIR = (
    os.environ.get(_PASSAGE_DATA_DIR_ENV, "").strip() or _DEFAULT_PASSAGE_DATA_DIR
)


@dataclass
class _PassageState:
    ready: bool = False
    index: Optional[object] = None  # shared.passage_index.PassageIndex


_state = _PassageState()
_lock = threading.Lock()


def load_passage_state() -> bool:
    """Open + validate the passage index ONCE at startup -- but ONLY when
    ``PASSAGE_PARALLELS_ENABLED`` is on.

    Codex review finding #14: the flag is checked FIRST, before anything
    else runs. A disabled deployment gets ZERO filesystem/mmap footprint
    from this module: ``shared.passage_index.open_index`` memory-maps
    multi-GB files (``gram_offsets.bin``, ``postings.bin``, ``streams.bin``,
    ``records.bin``) and reads real bytes during its own validation (CSR
    sanity checks touch the first and last CSR entries), which defeats the
    flag's entire operational purpose on a memory-constrained production
    host -- this project has a recorded precedent
    (docs/OPEN_ISSUES.md) of a 7.8 GB index getting evicted on a 15.8 GB
    host by just a 1.4 GB read/write; unconditionally mapping a similarly
    large passage index while the feature is supposed to be OFF is the same
    class of incident. Previously ``open_index`` ran regardless of the flag
    and only ``passage_available()`` (a SEPARATE, later check) ANDed it in
    -- the mapping itself, and its I/O, had already happened by then.

    When the flag is on, ``shared.passage_index.open_index`` IS the
    fail-closed validator and is documented to return ``None`` -- never
    raise -- on anything it does not fully recognise. The ``except
    Exception`` below is a defensive belt matching the discovery loader's
    posture (nothing escapes startup load), not a sign that ``open_index``
    is expected to raise.

    Safe to call more than once (a rebuild + restart, a flag flip, or a test
    re-point of ``PASSAGE_DATA_DIR``) -- it atomically replaces the module
    state under a lock, mirroring
    ``web/discovery_assets.py::load_discovery_state()``.
    """
    global _state
    if not PASSAGE_PARALLELS_ENABLED:
        with _lock:
            _state = _PassageState(ready=False, index=None)
        logger.info(
            "Passage index load skipped: PASSAGE_PARALLELS_ENABLED is off "
            "-- the index directory is never even opened."
        )
        return False

    from shared.passage_index import open_index  # local import: keep this a light web/ import
    try:
        idx = open_index(PASSAGE_DATA_DIR)
    except Exception:
        logger.info(
            "Passage index not loaded (fail-closed): unexpected exception "
            "opening the index directory (path withheld; see "
            "GENIZAH_PASSAGE_DATA_DIR / PASSAGE_DATA_DIR)."
        )
        idx = None

    new_state = _PassageState(ready=idx is not None, index=idx)
    with _lock:
        _state = new_state
    if not new_state.ready:
        logger.info(
            "Passage index not loaded (fail-closed) -- passage-matching "
            "parallels search hides cleanly."
        )
    return new_state.ready


def passage_available() -> bool:
    """The ONE predicate any future passage-matching surface must gate on.

    True only when ``PASSAGE_PARALLELS_ENABLED`` is ON AND the index loaded
    successfully at startup -- mirrors
    ``web/discovery_assets.py::discovery_available()`` exactly: the flag
    alone is necessary but NOT sufficient.
    """
    return bool(PASSAGE_PARALLELS_ENABLED and _state.ready)


def get_passage_searcher(text_fetcher: "PageTextFetcher"):
    """A fresh ``PassageSearcher``, or ``None`` when unavailable.

    Reads the flag and the loaded-index snapshot together, so there is no
    gap between "is it available" and "construct it" for a caller to get
    wrong -- the searcher is simply never constructed when the index is
    unavailable (fail-closed), matching every other passage-matching gate in
    this project.

    Construction itself does no I/O (the index is already open/mmap'd), so a
    fresh instance per request/search is intentional and cheap -- it needs
    no singleton lifecycle, no invalidation-on-rebuild story, nothing to get
    stale.
    """
    if not PASSAGE_PARALLELS_ENABLED:
        return None
    idx = _state.index
    if idx is None:
        return None
    from shared.passage_parallels import PassageSearcher  # local: shared/ stays import-light for web/
    return PassageSearcher(index=idx, text_fetcher=text_fetcher)
