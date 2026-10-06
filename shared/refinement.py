# -*- coding: utf-8 -*-
"""
Search Refinement Chain — shared data model and helpers.

Provides RefinementStep dataclass and chain utility functions used by both
the web (NiceGUI) and desktop (PyQt6) apps for search-within-results.

Contract:
- RefinementStep stores full search params for one refinement step
- Chain is a list[RefinementStep] representing successive narrowing
- compute_effective_restrict merges filter and refinement restrict sets
  with explicit None (no restriction) vs empty set (nothing passes) semantics
- replay_chain re-executes a chain against a searcher to rebuild restrict sets
- scope_signature detects when filter context changed under an active chain
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RefinementStep:
    """One step in a search refinement chain.

    Stores the full search parameters so the step can be replayed
    on session restore or when the filter scope changes.
    """
    query: str
    mode: str
    gap: int = 0
    exclude_words: list = field(default_factory=list)
    text_position: Optional[str] = None
    responsa_options: Optional[dict] = None
    result_count: int = 0  # total page-level results (matches display count)
    # The corpus the step searched. 'all' (execute_search's default) for steps saved
    # before 2026-10-04: replay never passed one, so that is what they replayed as.
    corpus_scope: str = 'all'
    # The step's results may leave matches out (D8): its search reached the 50,000-
    # candidate limit or was stopped, or a step before it did (it was restricted to an
    # incomplete set). Its count shows as "N+".
    result_count_capped: bool = False
    # The variant settings the step was searched with (the website records them, so a
    # replay searches exactly as the step first did; None = not recorded).
    variant_settings: Optional[dict] = None

    # Runtime-only fields (not serialized, rebuilt on replay)
    _result_uids: set = field(default_factory=set, repr=False, compare=False)
    # The step's manuscripts: the restriction of the step after it (None until known).
    _result_sys_ids: Optional[set] = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict:
        """Serialize to a plain dict (JSON-safe for session persistence).
        Excludes runtime-only _result_uids."""
        d = dataclasses.asdict(self)
        d.pop('_result_uids', None)
        d.pop('_result_sys_ids', None)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> RefinementStep:
        """Construct from dict, ignoring unknown keys and runtime fields."""
        _skip = {'_result_uids', '_result_sys_ids'}
        known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__ and k not in _skip}
        return cls(**known)

    @property
    def display_label(self) -> str:
        """Label shown in the refinement breadcrumb chip."""
        return self.query


def needs_mode_labels(chain: list[RefinementStep]) -> bool:
    """Return True if chain has steps with different modes (show mode badges)."""
    if len(chain) < 2:
        return False
    return len(set(s.mode for s in chain)) > 1


def compute_effective_restrict(
    filter_restrict: set | None,
    refinement_restrict: set | None,
) -> set | None:
    """Merge filter and refinement restrict sets.

    Contract (explicit None vs empty-set semantics):
    - Both None -> None (no restriction at all)
    - One None, one set -> return the set (could be empty)
    - Both sets -> return intersection (could be empty)

    Key: empty set means "restrict to nothing" (zero results).
    None means "no restriction". These are DIFFERENT.
    """
    if filter_restrict is None and refinement_restrict is None:
        return None
    if filter_restrict is None:
        return refinement_restrict
    if refinement_restrict is None:
        return filter_restrict
    return filter_restrict & refinement_restrict


def truncate_chain(chain: list[RefinementStep], index: int) -> list[RefinementStep]:
    """Remove step at index and all subsequent steps.

    Removing a chip at position N removes it AND everything after it.
    """
    return chain[:index]


def _last_search_cutoff() -> dict:
    """The cut-off signal of the search just run on this thread (none from a searcher
    that is not the real engine)."""
    try:
        from shared.search_engine import consume_last_search_cutoff
    except ImportError:
        return {}
    return consume_last_search_cutoff()


def replay_chain(
    chain: list[RefinementStep],
    searcher,
    filter_restrict: set | None,
    *,
    searcher_for_step=None,
) -> set | None:
    """Replay a refinement chain to rebuild restrict sets.

    Calls searcher.execute_search() for each step sequentially,
    feeding each step's result sys_ids as the restrict for the next.
    Updates each step's result_count.

    Args:
        chain: List of RefinementStep to replay.
        searcher: Object with execute_search(query, mode, gap, **kwargs) method.
        filter_restrict: Pre-search filter restrict set (or None).
        searcher_for_step: Optional ``step -> searcher``; the website binds each
            step's own variant settings with it. Default: *searcher* for every step.

    Returns:
        Final accumulated restrict set, or None if chain is empty.
    """
    if not chain:
        return None

    accumulated_restrict = None  # None = no refinement restriction yet
    incomplete = False  # a step so far left matches out: every later one may too

    for step in chain:
        effective = compute_effective_restrict(filter_restrict, accumulated_restrict)

        step_searcher = searcher_for_step(step) if searcher_for_step else searcher
        results = step_searcher.execute_search(
            step.query,
            step.mode,
            step.gap,
            exclude_words=step.exclude_words or None,
            responsa_options=step.responsa_options,
            restrict_sys_ids=effective,
            text_position=step.text_position,
            corpus_scope=step.corpus_scope,
        )

        result_sys_ids = {
            r.get('display', {}).get('id')
            for r in results
            if r.get('display', {}).get('id')
        }

        # Capture page-level uids for "all terms" filter
        step._result_uids = {
            r.get('uid') or r.get('display', {}).get('id')
            for r in results
            if r.get('uid') or r.get('display', {}).get('id')
        }

        step.result_count = len(results)  # page-level count (matches display)
        step._result_sys_ids = result_sys_ids
        cutoff = _last_search_cutoff()
        incomplete = incomplete or bool(cutoff.get('capped') or cutoff.get('interrupted'))
        step.result_count_capped = incomplete
        accumulated_restrict = result_sys_ids if result_sys_ids else set()

    return accumulated_restrict


def _cannot_complete(step: RefinementStep) -> bool:
    """A Responsa line-break ('|') step: that search has no ids-only path, so running it
    again reads the same cut-off list. It stays marked "+" (tracked for Phase 2)."""
    if not (step.responsa_options or {}).get('responsa_mode'):
        return False
    from shared.responsa import _has_line_break_syntax
    return bool(_has_line_break_syntax(step.query))


def chain_needs_completion(chain: list[RefinementStep], upto: int | None = None) -> bool:
    """Whether a step of *chain* (of its first *upto* steps) left matches out, or its
    manuscripts are unknown, so search-within or the all-terms filter must complete it
    first (D8)."""
    return any(s.result_count_capped or s._result_sys_ids is None for s in chain[:upto])


def complete_chain(
    chain: list[RefinementStep],
    searcher,
    filter_restrict: set | None,
    progress_callback=None,
    upto: int | None = None,
    *,
    searcher_for_step=None,
) -> dict:
    """Complete every step of *chain* that left matches out (D8, 2026-10-04).

    Such a step is run again with ``ids_only=True`` -- every candidate, only page ids
    and manuscripts kept -- under the complete restriction of the steps before it, and
    its ``_result_uids`` / ``_result_sys_ids`` / ``result_count`` become complete. A
    step that was not cut off keeps its sets. A step that still reports a cut-off (the
    line-break search has no ids-only path: such a step is not run again) stays marked,
    and so does every step after it. Stop (the run comes back interrupted) ends the completion: the steps completed
    so far keep their complete sets, the rest stay as they were.

    *upto* limits the work to the first *upto* steps: the all-terms filter only
    filters the shown rows, so the step that produced them needs no completing.
    *searcher_for_step* is as in ``replay_chain``.

    Returns {'restrict': the last completed step's manuscripts (None for no step),
    'interrupted': bool}.
    """
    accumulated = None
    incomplete = False
    for step in chain[:upto]:
        if step.result_count_capped and step._result_sys_ids is not None and _cannot_complete(step):
            incomplete = True
        elif step.result_count_capped or step._result_sys_ids is None:
            effective = compute_effective_restrict(filter_restrict, accumulated)
            step_searcher = searcher_for_step(step) if searcher_for_step else searcher
            rows = step_searcher.execute_search(
                step.query, step.mode, step.gap,
                exclude_words=step.exclude_words or None,
                responsa_options=step.responsa_options,
                restrict_sys_ids=effective,
                text_position=step.text_position,
                corpus_scope=step.corpus_scope,
                progress_callback=progress_callback,
                ids_only=True,
            )
            cutoff = _last_search_cutoff()
            if cutoff.get('interrupted'):
                return {'restrict': None, 'interrupted': True}
            step._result_sys_ids = {r.get('display', {}).get('id') for r in rows
                                    if r.get('display', {}).get('id')}
            step._result_uids = {r.get('uid') or r.get('display', {}).get('id') for r in rows
                                 if r.get('uid') or r.get('display', {}).get('id')}
            step.result_count = len(rows)
            incomplete = incomplete or bool(cutoff.get('capped'))
            step.result_count_capped = incomplete
        accumulated = step._result_sys_ids if step._result_sys_ids else set()
    return {'restrict': accumulated, 'interrupted': False}


def enrich_snippet_with_chain_terms(snippet: str, chain: list[RefinementStep], current_query: str) -> str:
    """Add *highlight* markers for earlier chain queries in a snippet.

    The search engine already marks the CURRENT query's matches with *...*
    markers. This function adds markers for all earlier chain queries so
    the user can see which terms from previous refinement steps also appear.

    Only processes the raw text between existing markers, never double-marks.
    """
    if not snippet or not chain:
        return snippet
    import re

    # Collect queries from earlier chain steps (not the current search query)
    earlier_queries = []
    current_lower = current_query.lower().strip() if current_query else ''
    for step in chain:
        q = step.query.strip()
        if q and q.lower() != current_lower:
            earlier_queries.append(q)
    if not earlier_queries:
        return snippet

    # Build regex: match any earlier query term NOT already inside *...*
    # Split on existing *markers* first, only process non-marked segments
    parts = re.split(r'(\*[^*]+\*)', snippet)
    pattern = '|'.join(re.escape(q) for q in earlier_queries)
    term_re = re.compile(f'({pattern})', re.IGNORECASE)

    result = []
    for part in parts:
        if part.startswith('*') and part.endswith('*'):
            result.append(part)  # Already marked, keep as-is
        else:
            result.append(term_re.sub(r'*\1*', part))
    return ''.join(result)


def compute_all_terms_filter(chain: list[RefinementStep]) -> set | None:
    """Return sys_ids that appear in ALL text-search steps' result sets.

    Used for the "Only results with all terms" checkbox. Intersects
    _result_uids across all text-search steps (skips metadata modes
    like Title/Shelfmark where page-level filtering doesn't apply).

    Returns None if chain has fewer than 2 steps or no valid sets.
    """
    if len(chain) < 2:
        return None
    # Metadata modes operate at manuscript level, not page level
    _metadata_modes = {'Title', 'Shelfmark'}
    sets = [s._result_uids for s in chain
            if s._result_uids and s.mode not in _metadata_modes]
    if len(sets) < 2:
        return None
    return set.intersection(*sets)


def scope_signature(restrict_set: set | None) -> str:
    """Compute a stable signature for the current filter restrict set.

    Used for stale-chain detection (D-16): both UIs store the signature
    at chain creation time and compare when filters change.

    Returns 'none' for None, or a hash string for a set.
    """
    if restrict_set is None:
        return 'none'
    return str(hash(frozenset(restrict_set)))
