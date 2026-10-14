# -*- coding: utf-8 -*-
"""Letter-level search for My Library: a second passage-index lifecycle, and
the scope router composition search dispatches through.

`desktop/passage_lifecycle.py` keeps its state in module globals -- one
installed index, one lease count, one generation -- and that single-owner
design is what its whole test suite proves. My Library needs a second index
with exactly the same guarantees (leases, drain-before-close, the two-phase
swap, startup recovery), not a re-implementation of them. So this module
loads the lifecycle's OWN code a second time as an independent module object:
same functions, separate globals. Nothing in the original module changes, and
the Genizah index cannot be affected by anything done to this one.

The shared classes -- the exceptions callers catch, and the result
dataclasses -- are rebound to the original module's, so `except
passage_lifecycle.PassageSearchUnavailableError` catches a failure from
either index.

`ScopedPassageSearcher` is what composition search holds: it answers the
`search_composition_logic` call for 'genizah', 'local' or 'all', merging the
two indexes' rows by score for 'all' (the score is matched letters on both
sides, so the two lists are directly comparable).
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
import types
from typing import Optional

from desktop import passage_lifecycle as genizah_lifecycle
from shared.config import Config
from shared.passage_local_corpus import (
    SIGNATURE_NAME, LocalPageTextFetcher, finish_local_rows,
    write_corpus_signature,
)
from shared.sys_id_patterns import LOCAL_SYS_ID_RE

logger = logging.getLogger(__name__)

_IMPL_NAME = 'desktop._passage_lifecycle_local_impl'

# Classes that must be ONE class across both instances.
_SHARED_NAMES = (
    'PassageState', 'PassageSearchError', 'PassageSearchUnavailableError',
    'PassageIndexReplaced', 'BuildCancelled', 'BuildOutcome',
    'SwapValidation', 'SwapResult', 'BuildAndSwapResult', 'RecoveryResult',
)


def _load_second_instance() -> types.ModuleType:
    """Execute passage_lifecycle's code object into a fresh module.

    `get_code` is used rather than a file path because in the frozen app the
    module lives inside the PYZ archive and has no file on disk.
    """
    if _IMPL_NAME in sys.modules:
        return sys.modules[_IMPL_NAME]
    spec = importlib.util.find_spec(genizah_lifecycle.__name__)
    if spec is None or spec.loader is None:
        raise ImportError('cannot locate desktop.passage_lifecycle')
    code = spec.loader.get_code(genizah_lifecycle.__name__)
    mod = types.ModuleType(_IMPL_NAME, genizah_lifecycle.__doc__)
    mod.__package__ = 'desktop'
    mod.__file__ = getattr(genizah_lifecycle, '__file__', None)
    sys.modules[_IMPL_NAME] = mod
    try:
        exec(code, mod.__dict__)
    except BaseException:
        sys.modules.pop(_IMPL_NAME, None)
        raise
    for name in _SHARED_NAMES:
        setattr(mod, name, getattr(genizah_lifecycle, name))
    return mod


#: The My Library lifecycle. Use it exactly like `desktop.passage_lifecycle`
#: (install/close/recover/run_build_and_swap...), always with an explicit
#: `root` of `local_passage_root()`.
lifecycle = _load_second_instance()



def _estimate_local_build_bytes(db_path: str) -> int:
    """The second instance's free-space preflight. The Genizah formula sizes
    a build from its corpus FILE, and the LOCAL sidecar stores its text
    zstd-compressed -- a file-size bound would understate the need several
    times over. `prepare_staging` looks this name up at call time, so
    rebinding it here changes the My Library instance only."""
    from shared.passage_local_corpus import local_text_bytes
    letters_ub = local_text_bytes(db_path) // 2
    return lifecycle.estimate_artifact_bytes(letters_ub) + 8 * letters_ub


lifecycle.estimate_build_bytes = _estimate_local_build_bytes

REASON_LOCAL_NOT_BUILT = 'passage_disabled_local_not_built'
REASON_LOCAL_INDEX_MISSING = 'passage_disabled_local_index_missing'


def local_passage_root() -> str:
    return Config.LOCAL_PASSAGE_INDEX_DIR


def local_db_path() -> str:
    from shared.local_indexer import local_db_path_for
    return local_db_path_for(Config.LOCAL_INDEX_DIR)


def signature_path(root: Optional[str] = None) -> str:
    return os.path.join(root or local_passage_root(), SIGNATURE_NAME)


def local_passage_available() -> bool:
    return lifecycle.passage_available()


def refresh_signature(root: Optional[str] = None, cancel_check=None) -> str:
    """Rewrite the corpus signature file from the LOCAL sidecar as it is now
    and return its path. The build hashes it into its manifest; a freshness
    check rewrites it and compares."""
    return write_corpus_signature(local_db_path(), signature_path(root),
                                  cancel_check=cancel_check)


def local_freshness(cancel_check=None) -> Optional[str]:
    """FRESH / STALE / UNKNOWN for the installed My Library index, or None
    when there is none (or the check was cancelled). Reads the corpus, so it
    belongs off the UI thread."""
    state = lifecycle._state
    if state is None:
        return None
    try:
        import json
        with open(os.path.join(state.live_dir, 'manifest.json'),
                  encoding='utf-8') as fh:
            sources = (json.load(fh).get('corpus') or {}).get('sources') or []
        path = refresh_signature(cancel_check=cancel_check)
    except lifecycle.BuildCancelled:
        return None
    except Exception:                                        # noqa: BLE001
        logger.exception('My Library letter-level freshness check failed')
        return lifecycle.UNKNOWN
    return lifecycle.compute_freshness(sources, [path],
                                       cancel_check=cancel_check)


def local_disabled_reason(local_ready: bool, local_index_ready: bool
                          ) -> Optional[str]:
    """The My Library half of the gate, checked AFTER the Genizah one's
    scope-independent reasons (Lab Mode, a build in flight)."""
    if not local_ready:
        return REASON_LOCAL_NOT_BUILT
    if not local_index_ready:
        return REASON_LOCAL_INDEX_MISSING
    return None


# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------

def _merge(a: dict, b: dict) -> dict:
    """One result from two indexes. Rows keep their own scores (matched
    letters on both sides) and are re-sorted together; counters add,
    truncation flags OR."""
    from shared.passage_parallels import _synthesize_query_report
    out = {
        'main': sorted((a.get('main') or []) + (b.get('main') or []),
                       key=lambda r: r.get('score', 0), reverse=True),
        'filtered': sorted((a.get('filtered') or []) + (b.get('filtered') or []),
                           key=lambda r: r.get('score', 0), reverse=True),
        'truncated_to_200': bool(a.get('truncated_to_200')
                                 or b.get('truncated_to_200')),
        'dropped_text_lookup_failures': int(
            a.get('dropped_text_lookup_failures', 0)
            + b.get('dropped_text_lookup_failures', 0)),
        'duplicate_photography_demoted': int(
            a.get('duplicate_photography_demoted', 0)
            + b.get('duplicate_photography_demoted', 0)),
        'query_report': _synthesize_query_report(
            [r for r in (a.get('query_report'), b.get('query_report')) if r]),
    }
    if a.get('partial') or b.get('partial'):
        out['partial'] = True
    return out


class ScopedPassageSearcher:
    """What composition search holds for a letter-level run. The scope comes
    in on each call (`corpus_scope`, as CompositionThread already passes it),
    and each index is leased only for the duration of its own call, exactly
    as `PassageSearchAdapter` does."""

    __slots__ = ('_engine', '_width', '_length', '_depth', '_render_cap')

    def __init__(self, engine, width: str, length: str, depth: str,
                 render_cap: Optional[int] = None) -> None:
        self._engine = engine
        self._width = width
        self._length = length
        self._depth = depth
        self._render_cap = render_cap

    def _genizah(self):
        return genizah_lifecycle.get_passage_searcher(
            self._engine, self._width, self._length, self._depth,
            render_cap=self._render_cap)

    def _local(self, fetcher):
        return lifecycle.get_passage_searcher(
            fetcher, self._width, self._length, self._depth,
            render_cap=self._render_cap, sys_id_re=LOCAL_SYS_ID_RE)

    def search_composition_logic(self, full_text, chunk_size=5,
                                 max_freq=100.0, mode='exact', *,
                                 corpus_scope='genizah', **kwargs) -> dict:
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'
        results = []
        if corpus_scope in ('genizah', 'all'):
            results.append(self._genizah().search_composition_logic(
                full_text, chunk_size, max_freq, mode,
                corpus_scope='genizah', **kwargs))
        if corpus_scope in ('local', 'all'):
            fetcher = LocalPageTextFetcher(self._engine)
            local = self._local(fetcher).search_composition_logic(
                full_text, chunk_size, max_freq, mode,
                corpus_scope='local', **kwargs)
            results.append(finish_local_rows(local, fetcher))
        if len(results) == 1:
            return results[0]
        return _merge(results[0], results[1])
