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

    # Runtime-only fields (not serialized, rebuilt on replay)
    _result_uids: set = field(default_factory=set, repr=False, compare=False)

    def to_dict(self) -> dict:
        """Serialize to a plain dict (JSON-safe for session persistence).
        Excludes runtime-only _result_uids."""
        d = dataclasses.asdict(self)
        d.pop('_result_uids', None)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> RefinementStep:
        """Construct from dict, ignoring unknown keys and runtime fields."""
        _skip = {'_result_uids'}
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


def replay_chain(
    chain: list[RefinementStep],
    searcher,
    filter_restrict: set | None,
) -> set | None:
    """Replay a refinement chain to rebuild restrict sets.

    Calls searcher.execute_search() for each step sequentially,
    feeding each step's result sys_ids as the restrict for the next.
    Updates each step's result_count.

    Args:
        chain: List of RefinementStep to replay.
        searcher: Object with execute_search(query, mode, gap, **kwargs) method.
        filter_restrict: Pre-search filter restrict set (or None).

    Returns:
        Final accumulated restrict set, or None if chain is empty.
    """
    if not chain:
        return None

    accumulated_restrict = None  # None = no refinement restriction yet

    for step in chain:
        effective = compute_effective_restrict(filter_restrict, accumulated_restrict)

        results = searcher.execute_search(
            step.query,
            step.mode,
            step.gap,
            exclude_words=step.exclude_words or None,
            responsa_options=step.responsa_options,
            restrict_sys_ids=effective,
            text_position=step.text_position,
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
        accumulated_restrict = result_sys_ids if result_sys_ids else set()

    return accumulated_restrict


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
