# -*- coding: utf-8 -*-
"""Letter-level search for My Library: what is specific to the My Library
index, and the scope router composition search dispatches through.

The My Library index lives in `desktop/passage_lifecycle.py`'s second slot,
`SLOT_LOCAL`. It gets the same leases, drain-before-close, two-phase swap and
startup recovery as the Genizah index, with its own state, lock and
generation, so neither index can block, close or invalidate the other. This
module holds only what differs for My Library: where its index and corpus
live, the build's size estimate, the freshness check against the library's
signature, and the readiness reasons.

`ScopedPassageSearcher` is what composition search holds: it answers the
`search_composition_logic` call for 'genizah', 'local' or 'all', merging the
two indexes' rows by score for 'all' (the score is matched letters on both
sides, so the two lists are directly comparable).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from desktop import passage_lifecycle
from desktop.passage_lifecycle import SLOT_LOCAL
from shared.config import Config
from shared.passage_builder import estimate_artifact_bytes
from shared.passage_local_corpus import (
    SIGNATURE_NAME, LocalPageTextFetcher, finish_local_rows,
    parse_local_header, split_record_id, write_corpus_signature,
)
from shared.sys_id_patterns import ANY_SYS_ID_RE, LOCAL_SYS_ID_RE

logger = logging.getLogger(__name__)

REASON_LOCAL_NOT_BUILT = 'passage_disabled_local_not_built'
REASON_LOCAL_INDEX_MISSING = 'passage_disabled_local_index_missing'

# Where a freshness check writes its fresh signature. NOT the build's own
# signature file: a build hashes that file before it starts and again before
# its swap, so a check rewriting it mid-build would make an hour-long build
# discard itself as "corpus changed". Same file NAME, because the freshness
# comparison includes the basename the manifest recorded.
_FRESHNESS_DIRNAME = '.freshness_check'


def local_passage_root() -> str:
    return passage_lifecycle.slot_root(SLOT_LOCAL)


def local_db_path() -> str:
    from shared.local_indexer import local_db_path_for
    return local_db_path_for(Config.LOCAL_INDEX_DIR)


def signature_path(root: Optional[str] = None) -> str:
    return os.path.join(root or local_passage_root(), SIGNATURE_NAME)


def local_passage_available() -> bool:
    """Is a My Library index installed? Read from the local slot alone. The
    window's gate asks the Genizah question (`passage_lifecycle.
    passage_available()`) and this one separately, and they stay separate
    patch points: replacing the Genizah answer must not change this one."""
    return passage_lifecycle.installed_live_dir(SLOT_LOCAL) is not None


def estimate_local_build_bytes(db_path: str) -> int:
    """The free-space preflight for a My Library build, passed to
    `run_build_and_swap` as `needed_bytes`. The Genizah formula sizes a build
    from its corpus FILE, and the LOCAL sidecar stores its text
    zstd-compressed -- a file-size bound would understate the need several
    times over -- so this sums the uncompressed text lengths instead."""
    from shared.passage_local_corpus import local_text_bytes
    letters_ub = local_text_bytes(db_path) // 2
    return estimate_artifact_bytes(letters_ub) + 8 * letters_ub


def disk_size_label(n_bytes: int) -> str:
    """A byte count as the build dialogs show it: MB below 1 GB, since a
    small library's build needs a few hundred MB, which whole GB rounds to
    "0 GB"."""
    n = max(0, int(n_bytes))
    mb = round(n / 1_000_000)
    if mb < 1000:
        return '{} MB'.format(max(1, mb) if n else 0)
    return '%.1f GB' % (n / 1e9)


def refresh_signature(root: Optional[str] = None, cancel_check=None) -> str:
    """Rewrite the build's corpus signature file from the LOCAL sidecar as it
    is now and return its path. The build hashes it into its manifest."""
    return write_corpus_signature(local_db_path(), signature_path(root),
                                  cancel_check=cancel_check)


def local_freshness(live_dir: Optional[str],
                    cancel_check=None) -> Optional[str]:
    """FRESH / STALE / UNKNOWN for the My Library index installed at
    `live_dir`, or None when there is none (or the check was cancelled).

    `live_dir` is captured on the UI thread
    (`passage_lifecycle.installed_live_dir(SLOT_LOCAL)`) and handed in: the
    slot's state is UI-thread-only, and this runs on a worker because it
    reads the whole library."""
    if not live_dir:
        return None
    try:
        with open(os.path.join(live_dir, 'manifest.json'),
                  encoding='utf-8') as fh:
            sources = (json.load(fh).get('corpus') or {}).get('sources') or []
        check_dir = os.path.join(os.path.dirname(live_dir), _FRESHNESS_DIRNAME)
        path = write_corpus_signature(
            local_db_path(), os.path.join(check_dir, SIGNATURE_NAME),
            cancel_check=cancel_check)
    except passage_lifecycle.BuildCancelled:
        return None
    except Exception:                                        # noqa: BLE001
        logger.exception('My Library letter-level freshness check failed')
        return passage_lifecycle.UNKNOWN
    return passage_lifecycle.compute_freshness(sources, [path],
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
# Rows from either index: ids, witness text, pinned generations.
# ---------------------------------------------------------------------------

def is_local_sys_id(sys_id: Optional[str]) -> bool:
    return bool(sys_id) and LOCAL_SYS_ID_RE.fullmatch(sys_id) is not None


def row_sys_id(row) -> Optional[str]:
    """The sys_id a composition result row belongs to -- a Genizah one or a
    My Library one. On the desktop both kinds of row can sit in one result
    (scope All), so this reads either namespace: `ANY_SYS_ID_RE` is anchored
    on digit boundaries, and every row's header begins with its own id, so
    the first match is the row's id in either case.

    `shared.passage_witness_source.witness_sys_id` stays corpus-only: it is
    the web's too, and the web has no My Library rows."""
    m = ANY_SYS_ID_RE.search((row or {}).get('raw_header') or '')
    return m.group(1) if m else None


def uid_sys_id(uid) -> Optional[str]:
    """The sys_id inside a checked page's uid: `{sys_id}_IE.._P.._FL..` for
    a Genizah page, `LOCAL_{sys_id}_P{n}` for a My Library one."""
    m = ANY_SYS_ID_RE.search(str(uid or ''))
    return m.group(1) if m else None


class WitnessTextFetcher:
    """Page text for promoting a result to a witness (and re-fetching it
    after a restart), from whichever index the page belongs to: a My Library
    header through `LocalPageTextFetcher`, anything else through the main
    engine. A My Library file has no whole-manuscript fallback -- a book is
    far over the witness length limit, and a page that no longer resolves
    is reported as a failure, by name, instead."""

    def __init__(self, engine):
        self._engine = engine
        self._local = LocalPageTextFetcher(engine)

    def get_full_text_by_header(self, header):
        base, _seg = split_record_id(header)
        if parse_local_header(base) is not None:
            return self._local.get_full_text_by_header(header)
        return self._engine.get_full_text_by_header(header)

    def get_full_manuscript(self, sys_id):
        if is_local_sys_id(sys_id):
            return []
        return self._engine.get_full_manuscript(sys_id)


def scope_pins(scope: str) -> dict:
    """The index generations a run on `scope` depends on, pinned on the UI
    thread before the run starts: `{'pinned_generation': <Genizah or None>,
    'pinned_local_generation': <My Library or None>}`. None for an index
    the scope does not search, so swapping that one never voids the run."""
    return {
        # The bare call is the Genizah slot's: the same call the dispatch
        # always made, which tests that move the generation replace.
        'pinned_generation': (
            passage_lifecycle.current_state_generation()
            if scope != 'local' else None),
        'pinned_local_generation': (
            passage_lifecycle.current_state_generation(SLOT_LOCAL)
            if scope in ('local', 'all') else None),
    }


# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------

def _merge(a: dict, b: dict, cap: int = 0) -> dict:
    """One result from two indexes. Rows keep their own scores (matched
    letters on both sides) and are re-sorted together; counters add,
    truncation flags OR.

    `cap` > 0 applies the manuscript-group cap once more, to the merged
    lists, with the same function and rule each index applied to its own
    (`_cap_main_results_by_group`, both buckets capped, truncation read from
    `main`). Each index capped only its own list, so without this two lists
    of up to `cap` manuscripts each came back as up to twice that, with no
    truncation reported."""
    from shared.parallels_service import _cap_main_results_by_group
    from shared.passage_parallels import (_RegexSysIdParser,
                                          _synthesize_query_report)
    main = (a.get('main') or []) + (b.get('main') or [])
    filtered = (a.get('filtered') or []) + (b.get('filtered') or [])
    truncated = bool(a.get('truncated_to_200') or b.get('truncated_to_200'))
    if cap and cap > 0:
        parser = _RegexSysIdParser(ANY_SYS_ID_RE)
        main, main_cut = _cap_main_results_by_group(main, parser, cap=cap)
        filtered, _cut = _cap_main_results_by_group(filtered, parser, cap=cap)
        truncated = truncated or main_cut
    out = {
        'main': sorted(main, key=lambda r: r.get('score', 0), reverse=True),
        'filtered': sorted(filtered, key=lambda r: r.get('score', 0),
                           reverse=True),
        'truncated_to_200': truncated,
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
    as `PassageSearchAdapter` does -- each through its own slot."""

    __slots__ = ('_engine', '_width', '_length', '_depth', '_render_cap')

    def __init__(self, engine, width: str, length: str, depth: str,
                 render_cap: Optional[int] = None) -> None:
        self._engine = engine
        self._width = width
        self._length = length
        self._depth = depth
        self._render_cap = render_cap

    def _genizah(self):
        return passage_lifecycle.get_passage_searcher(
            self._engine, self._width, self._length, self._depth,
            render_cap=self._render_cap)

    def _local(self, fetcher):
        return passage_lifecycle.get_passage_searcher(
            fetcher, self._width, self._length, self._depth,
            render_cap=self._render_cap, slot=SLOT_LOCAL)

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
            # A pre-search restriction (a library filter, search-within,
            # visual-similarity partners) is a set of GENIZAH sys_ids: it
            # applies to Genizah rows only, and My Library rows always show
            # -- exactly as the chunk path's LOCAL loop ignores it
            # (`shared/search_engine.py`; owner, 2026-10-16). Read in the
            # LOCAL namespace it would instead hide every My Library row
            # whenever any Genizah filter was on.
            local_kwargs = {k: v for k, v in kwargs.items()
                            if k != 'restrict_sys_ids'}
            fetcher = LocalPageTextFetcher(self._engine)
            local = self._local(fetcher).search_composition_logic(
                full_text, chunk_size, max_freq, mode,
                corpus_scope='local', **local_kwargs)
            results.append(finish_local_rows(local, fetcher))
        if len(results) == 1:
            return results[0]
        return _merge(results[0], results[1], cap=self._group_cap())

    def _group_cap(self) -> int:
        """The manuscript-group cap each index applied: PassageSearcher's
        default when the caller passed none, else the caller's (0 is
        uncapped -- a multi-witness or conditions run caps later, once)."""
        if self._render_cap is None:
            from shared.parallels_service import PARALLELS_GROUP_CAP
            return PARALLELS_GROUP_CAP
        return self._render_cap

    def search_condition(self, text: str, pages=(), sys_ids=(),
                         should_stop=None, render_limit: int = 0):
        """One version of a Composition Search condition -- on the GENIZAH
        index only. A condition's page set is built from Genizah page keys
        (`shared/passage_pages.py`), which no My Library record has, so the
        window refuses conditions under any other scope before a run starts
        (`_comp_compound_gate`). `CompoundCompositionThread` calls this on
        whatever searcher it was given, so the router must answer it."""
        return self._genizah().search_condition(
            text, pages=pages, sys_ids=sys_ids, should_stop=should_stop,
            render_limit=render_limit)
