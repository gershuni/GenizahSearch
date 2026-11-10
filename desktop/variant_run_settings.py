# -*- coding: utf-8 -*-
"""The variant settings one desktop search runs with.

Variant search reads two values while it expands a word: the number of variant pairs
(the level) and Num Changes (x1-x3, ``LabSettings.variant_max_changes``). The main
search bar sets both in the shared settings -- ``LabSettings`` and the shared
``VariantManager`` (``set_variant_level``) -- before each of its searches. A search
started anywhere else used to inherit whatever the last search left there: Joins Lab
ran at x3 after a Maximum x3 search, and a refinement step searched with Basic x1 ran
again at the current x3.

``SettingsBoundSearcher`` runs each ``execute_search`` on a view of the engine of its
own (``engine_with_variant_settings``): a shallow copy, sharing the read-only index,
searchers and metadata, whose ``var_mgr`` is a new ``VariantManager`` over a copy of
the settings with the bound values. Nothing shared is written, so nothing is put back.
Writing the shared values and putting them back after the search was not safe: the
Joins Lab window searches beside the main window, and a replay at Basic x1 put Maximum
x3 back under a main-window search that had started meanwhile at the same Basic x1
(17 spellings of a word became 8,000).
"""
from __future__ import annotations

import copy

from shared.variants import VariantManager, max_changes_by_preset

#: What a refinement step records of the run that produced it
#: (``RefinementStep.variant_settings``), and what running it again applies.
RECORDED_KEYS = ('variant_pairs_count', 'variant_max_changes')


def _checked(values) -> dict:
    """The usable part of *values*: a pair count of 1 or more, x1-x3 (clamped).
    Anything else -- a missing key, a value saved wrong -- is left out, so the
    current setting is used for it."""
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


def engine_with_variant_settings(engine, settings, values):
    """*engine*, expanding variants with *values* (``RECORDED_KEYS``; a key that is
    missing or unreadable keeps *settings*' current value).

    A shallow copy of *engine* -- the index, searchers and metadata are shared, they
    are only read -- whose ``var_mgr`` is a new VariantManager over a copy of
    *settings* with the values set. Neither *engine*, its VariantManager nor
    *settings* is written. Without settings, usable values or a VariantManager on the
    engine (it expands no variants), *engine* itself."""
    values = _checked(values)
    if settings is None or not values or getattr(engine, 'var_mgr', None) is None:
        return engine
    own = copy.copy(settings)
    for key, value in values.items():
        setattr(own, key, value)
    view = copy.copy(engine)
    view.var_mgr = VariantManager(settings=own)
    return view


class SettingsBoundSearcher:
    """*searcher*, except that each ``execute_search`` runs with variant settings of
    its own, on a view of *searcher* (``engine_with_variant_settings``). *bind()* is
    called at each search and returns ``(settings, values)``; no settings or no
    values, and the search runs on *searcher* as it is. Every other attribute is
    *searcher*'s."""

    def __init__(self, searcher, bind):
        self._searcher = searcher
        self._bind = bind

    def execute_search(self, *args, **kwargs):
        settings, values = self._bind()
        engine = engine_with_variant_settings(self._searcher, settings, values)
        return engine.execute_search(*args, **kwargs)

    def __getattr__(self, name):
        if name in ('_searcher', '_bind'):     # not set yet (a copy being built)
            raise AttributeError(name)
        return getattr(self._searcher, name)


def recorded_settings_searcher(searcher, settings, values):
    """*searcher* running each search with the recorded *values* (a refinement step's
    ``variant_settings``) over the current *settings*."""
    return SettingsBoundSearcher(searcher, lambda: (settings, values))


class PreviewVariants:
    """Spellings for the search bar's variant-count preview, from a VariantManager of
    its own over a copy of the settings: the shared VariantManager and LabSettings
    belong to the search that may be running, and the preview -- recomputed as the
    user types or changes the level -- must not change what that search expands
    with. The manager is rebuilt only when a value it reads changes, so its cache
    serves the preview while the user types."""

    def __init__(self):
        self._key = None
        self._mgr = None

    def manager(self, settings, pairs_count, max_changes):
        key = (pairs_count, max_changes,
               getattr(settings, 'variant_min_word_len', 2),
               bool(getattr(settings, 'variant_aggressive', False)),
               tuple(sorted(str(k) for k in (getattr(settings, 'custom_variants', None) or {}))))
        if self._mgr is None or key != self._key:
            own = copy.copy(settings) if settings is not None else None
            if own is not None:
                own.variant_pairs_count = pairs_count
                own.variant_max_changes = max_changes
            self._mgr = VariantManager(settings=own)
            self._key = key
        return self._mgr


def basic_changes_searcher(searcher, settings_of):
    """*searcher* running each search with Basic's Num Changes, whatever the last
    search left in the shared value (Joins Lab, like the website's: owner ruling
    2026-09-28). *settings_of()* returns the LabSettings at the time of the search;
    without its per-level table the search runs as it is."""
    def bind():
        settings = settings_of()
        table = getattr(settings, 'variant_max_changes_by_preset', None)
        if not isinstance(table, dict):
            return settings, None
        return settings, {'variant_max_changes': max_changes_by_preset(table)['basic']}
    return SettingsBoundSearcher(searcher, bind)
