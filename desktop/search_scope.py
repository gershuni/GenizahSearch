# -*- coding: utf-8 -*-
"""Focus Search: one scope per tab (GS-13).

The Search tab and the Composition tab each narrow their runs with their own
pre-search filters ("Focus Search"). Both tabs used to share one set of
attributes on the main window, so setting Focus Search on one tab restricted
the other, and New on either cleared both.

A ``SearchScope`` holds one tab's filters, the manuscripts they allow and the
state of the lookup that computes them (``FilterCountWorker``, run by the main
window). The window holds one per tab (``scope_of(window, tab)``) and does the
UI work; this module has no Qt.
"""

SEARCH = 'search'
COMPOSITION = 'composition'
TABS = (SEARCH, COMPOSITION)


def tab_for_index(index):
    """The scope of the main window's tab at ``index`` (0 Search, 1 Composition)."""
    return COMPOSITION if index == 1 else SEARCH


class SearchScope:
    """One tab's Focus Search."""

    def __init__(self):
        self.filters = {}              # domains, authors, works, criteria, library, text_*, ...
        self.restrict_sys_ids = None   # the manuscripts the filters allow; None = every one
        self.error = None              # why the last lookup could not run (#17)
        self.lookup_gen = 0            # only the newest lookup may answer
        self.lookup_pending = False    # a lookup is running: a run waits for it (K-14)
        self.rerun = False             # a run is waiting for that lookup
        self.rerun_custom_text = None  # the text a waiting composition was asked with
        self.worker = None             # the lookup running, kept alive until it ends

    def has_filters(self):
        """Filters with nothing but include_mode narrow nothing."""
        return any(k != 'include_mode' for k in self.filters)

    def cancel_rerun(self):
        """Disarm a waiting run; True if one was waiting."""
        was_waiting = self.rerun
        self.rerun = False
        self.rerun_custom_text = None
        return was_waiting

    def supersede(self):
        """The filters were replaced by a known scope: a lookup still running is
        stale, its error no longer applies, and a waiting run is cancelled (it
        was asked for under the old filters). True if a run was waiting."""
        self.lookup_gen += 1
        self.lookup_pending = False
        self.error = None
        return self.cancel_rerun()

    def begin_lookup(self, worker):
        """A new lookup starts; returns its generation."""
        self.lookup_gen += 1
        self.lookup_pending = True
        self.worker = worker
        return self.lookup_gen

    def lookup_failed(self, reason):
        """The lookup could not run: no scope (never a stale or None one taken as
        "every manuscript"), the reason kept, a waiting run cancelled. True if a
        run was waiting."""
        self.lookup_pending = False
        self.error = reason or 'error'
        self.restrict_sys_ids = None
        return self.cancel_rerun()

    def clear(self):
        """New: no filters, no scope, no lookup. True if a run was waiting."""
        was_waiting = self.supersede()
        self.filters = {}
        self.restrict_sys_ids = None
        return was_waiting


def scope_of(window, tab=SEARCH):
    """``tab``'s scope on ``window``, made on first use: tests build the main window
    with GenizahGUI.__new__ (no __init__), or borrow its methods onto other hosts."""
    scopes = window.__dict__.get('_scopes')
    if scopes is None:
        scopes = window.__dict__['_scopes'] = {t: SearchScope() for t in TABS}
    return scopes[tab]


def scope_property(tab, field):
    """A main-window attribute that is a field of ``tab``'s scope (the Search tab's
    keep their old names, ``pre_search_filters`` and ``pre_search_restrict_sys_ids``)."""
    return property(lambda window: getattr(scope_of(window, tab), field),
                    lambda window, value: setattr(scope_of(window, tab), field, value))
