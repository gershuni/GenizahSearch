# -*- coding: utf-8 -*-
"""The variant settings one desktop search runs with.

Variant search reads two values from the shared settings while it expands a word:
the number of variant pairs (the level; ``VariantManager.set_variant_level``) and
Num Changes (x1-x3, ``LabSettings.variant_max_changes``). The main search bar sets
both before each of its searches. A search started anywhere else used to inherit
whatever the last search left there: Joins Lab ran at x3 after a Maximum x3 search,
and a refinement step searched with Basic x1 ran again at the current x3.

``SettingsBoundSearcher`` runs each ``execute_search`` with the values it is given
and puts the shared ones back when that search ends, so the next search finds them
as they were. The Joins Lab window can search beside the main window, so a value is
put back only while it is still the one this search set: a search started meanwhile
keeps its own.
"""
from __future__ import annotations

from contextlib import contextmanager

from shared.variants import max_changes_by_preset

#: What a refinement step records of the run that produced it
#: (``RefinementStep.variant_settings``), and what running it again applies.
RECORDED_KEYS = ('variant_pairs_count', 'variant_max_changes')


def _checked(values) -> dict:
    """The usable part of *values*: a pair count of 1 or more, x1-x3 (clamped).
    Anything else -- a missing key, a value saved wrong -- is left out, so the
    shared value stays as it is for it."""
    out = {}
    if not isinstance(values, dict):
        return out
    try:
        pairs = int(values['variant_pairs_count'])
        if pairs >= 1:
            out['variant_pairs_count'] = pairs
    except (KeyError, TypeError, ValueError):
        pass
    try:
        out['variant_max_changes'] = max(1, min(3, int(values['variant_max_changes'])))
    except (KeyError, TypeError, ValueError):
        pass
    return out


def variant_settings_now(settings, var_mgr=None):
    """The variant settings a search started now runs with: what its refinement
    step records, so that running the step again searches as it first did.
    None without settings."""
    if settings is None:
        return None
    try:
        pairs = (var_mgr.get_variant_level() if var_mgr is not None
                 else settings.variant_pairs_count)
        return _checked({'variant_pairs_count': pairs,
                         'variant_max_changes': settings.variant_max_changes}) or None
    except AttributeError:
        return None


@contextmanager
def variant_settings_applied(settings, var_mgr, values):
    """Run the block with *values* (``RECORDED_KEYS``; a key that is missing or
    unreadable leaves that shared value as it is), then put the shared values back
    -- each only while it is still the one set here."""
    values = _checked(values)
    if settings is None or not values:
        yield
        return
    changes = values.get('variant_max_changes')
    pairs = values.get('variant_pairs_count') if var_mgr is not None else None
    prev_changes = getattr(settings, 'variant_max_changes', None)
    prev_pairs = getattr(settings, 'variant_pairs_count', None)
    set_changes = changes is not None and changes != prev_changes
    set_pairs = pairs is not None and pairs != prev_pairs
    try:
        if set_changes:
            settings.variant_max_changes = changes
        if set_pairs:
            var_mgr.set_variant_level(pairs)    # rebuilds the pair map, clears the cache
        yield
    finally:
        if set_changes and getattr(settings, 'variant_max_changes', None) == changes:
            settings.variant_max_changes = prev_changes
        if set_pairs and prev_pairs is not None and getattr(settings, 'variant_pairs_count', None) == pairs:
            var_mgr.set_variant_level(prev_pairs)


class SettingsBoundSearcher:
    """*searcher*, except that each ``execute_search`` runs with variant settings of
    its own. *bind()* is called at each search and returns ``(settings, var_mgr,
    values)``; no settings or no values, and the search runs as it is. Every other
    attribute is *searcher*'s."""

    def __init__(self, searcher, bind):
        self._searcher = searcher
        self._bind = bind

    def execute_search(self, *args, **kwargs):
        settings, var_mgr, values = self._bind()
        with variant_settings_applied(settings, var_mgr, values):
            return self._searcher.execute_search(*args, **kwargs)

    def __getattr__(self, name):
        if name in ('_searcher', '_bind'):     # not set yet (a copy being built)
            raise AttributeError(name)
        return getattr(self._searcher, name)


def recorded_settings_searcher(searcher, settings, var_mgr, values):
    """*searcher* running each search with the recorded *values* (a refinement step's
    ``variant_settings``)."""
    return SettingsBoundSearcher(searcher, lambda: (settings, var_mgr, values))


def basic_changes_searcher(searcher, settings_of):
    """*searcher* running each search with Basic's Num Changes, whatever the last
    search left in the shared value (Joins Lab, like the website's: owner ruling
    2026-09-28). *settings_of()* returns the LabSettings at the time of the search;
    without its per-level table the search runs as it is."""
    def bind():
        settings = settings_of()
        table = getattr(settings, 'variant_max_changes_by_preset', None)
        if not isinstance(table, dict):
            return settings, None, None
        return settings, None, {'variant_max_changes': max_changes_by_preset(table)['basic']}
    return SettingsBoundSearcher(searcher, bind)
