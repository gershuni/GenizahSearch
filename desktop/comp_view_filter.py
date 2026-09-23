# -*- coding: utf-8 -*-
"""Composition results: ONE eligibility rule for the result tree and the Manuscript Viewer.

A heavy user's letter (Tishrei 5787) asked that the expanded view (the Manuscript
Viewer opened from a Composition result) follow what they set in the short view (the
tree). Before this module the tree hid rows widget by widget, reading cell text, and
the viewer ignored visibility altogether -- so filtered-out rows still reached the
viewer, lazy appendix groups vanished whole under any filter (their only child was a
data-less placeholder), and rows instantiated by expanding a group were never
filtered. Both surfaces now ask this module the same question about the same data.

The contract (owner decision, 2026-09-23 -- "split by filter type"):

* MANUSCRIPT-level filters keep or drop a manuscript together with all its pages:
  library, title and printed-column text; the 3-state printed filter; LOCAL
  only/hidden and LOCAL file opt-outs; domain exclusions.
* SHELFMARK matches the visible Shelfmark cell ("T-S 1 (Image 3)"): the
  manuscript's cell keeps all its pages; failing that, the page rows whose own
  cell ("Image 4 [2v]") matches are kept.
* PAGE-level filters keep only the pages whose own text matches: source context and
  manuscript context.
* A manuscript is shown iff it passes the manuscript-level rules AND at least one of
  its pages passes the page-level rules.

A Part (Oxford multi-folio record) is judged by its representative ``sys_id`` -- the
same id the tree has always used for its Printed badge and domain lookup.

Pure logic: the host (``GenizahGUI``) supplies the lookups through ``FilterHost``, so
this module needs no Qt and no metadata services, and is tested directly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, List, Optional

# Column keys. The host maps its comp_col_* indices to these.
MS_LEVEL_COLUMNS = ("library", "shelfmark", "title", "printed")
PAGE_LEVEL_COLUMNS = ("context", "ms_context")

UNCATEGORIZED_DOMAIN = "Uncategorized"


def item_pages(ms_item: dict) -> list:
    """The hit pages of a grouped composition item.

    A manuscript/part carries ``pages``; a single-page manuscript still carries a
    one-element ``pages`` list, and THAT page (not the aggregate) is the hit -- its
    uid, highlights and source come from it. Only an item without ``pages`` (the
    fallback shape) is its own page.
    """
    pages = ms_item.get("pages") if isinstance(ms_item, dict) else None
    if pages:
        return list(pages)
    return [ms_item] if isinstance(ms_item, dict) else []


def page_key(page: dict) -> tuple:
    """Identity of a hit page that survives Qt's copy of a stored dict."""
    return (page.get("uid") or "", page.get("raw_header") or "")


def text_matches(text: str, rule: dict) -> bool:
    """Same semantics as ``GenizahGUI._text_matches_filter``: case-insensitive
    substring, optionally negated; an empty needle matches everything."""
    needle = (rule.get("text") or "").strip()
    if not needle:
        return True
    contains = needle.lower() in (text or "").lower()
    return (not contains) if rule.get("exclude") else contains


@dataclass(frozen=True)
class FilterState:
    """Everything that decides eligibility, frozen at one moment.

    ``local_state`` is the EFFECTIVE state: the host passes 'all' when the LOCAL
    filter is a no-op because no LOCAL hit exists (the D-10 P1 rule).
    """
    column_rules: Dict[str, dict] = field(default_factory=dict)
    printed_state: str = "all"            # 'all' | 'hide_printed' | 'only_printed'
    local_state: str = "all"              # 'all' | 'only_local' | 'no_local'
    optouts_active: bool = False
    domain_exclusions: FrozenSet[str] = frozenset()

    def active(self) -> bool:
        return bool(
            self.column_rules
            or self.printed_state != "all"
            or self.local_state != "all"
            or self.optouts_active
            or self.domain_exclusions
        )


@dataclass
class FilterHost:
    """Lookups the rule needs, supplied by the host application.

    ms_fields(ms_item)  -> {'library','shelfmark','title','printed'} display text,
                           exactly what the tree renders in those cells
    sys_id(ms_item)     -> the manuscript's sys_id ('' if unknown)
    is_printed(sys_id)  -> bool
    is_local(ms_item)   -> bool
    is_opted_out(ms_item) -> bool (LOCAL file the user opted out of)
    domains(sys_id)     -> list of domain names ([] = uncategorized)
    page_texts(page, ms_item) -> {'context', 'ms_context', 'shelfmark'}: the page's
                           preview text, and its own Shelfmark cell (None when the
                           page has no row of its own)
    """
    ms_fields: Callable[[dict], dict]
    sys_id: Callable[[dict], str]
    is_printed: Callable[[str], bool]
    is_local: Callable[[dict], bool]
    is_opted_out: Callable[[dict], bool]
    domains: Callable[[str], list]
    page_texts: Callable[[dict, dict], dict]


def manuscript_passes(ms_item: dict, state: FilterState, host: FilterHost) -> bool:
    """The manuscript-level half of the contract."""
    # Shelfmark is judged in eligible_pages: its cell exists on page rows too.
    ms_rules = {k: r for k, r in state.column_rules.items()
                if k in MS_LEVEL_COLUMNS and k != "shelfmark"}
    if ms_rules:
        fields = host.ms_fields(ms_item) or {}
        for key, rule in ms_rules.items():
            if not text_matches(fields.get(key, ""), rule):
                return False
    sid = None
    if state.printed_state != "all":
        sid = host.sys_id(ms_item)
        printed = bool(sid) and host.is_printed(sid)
        if state.printed_state == "hide_printed" and printed:
            return False
        if state.printed_state == "only_printed" and not printed:
            return False
    if state.local_state != "all":
        local = host.is_local(ms_item)
        if state.local_state == "only_local" and not local:
            return False
        if state.local_state == "no_local" and local:
            return False
    if state.optouts_active and host.is_opted_out(ms_item):
        return False
    if state.domain_exclusions:
        if sid is None:
            sid = host.sys_id(ms_item)
        doms = host.domains(sid) if sid else []
        if not doms:
            if UNCATEGORIZED_DOMAIN in state.domain_exclusions:
                return False
        elif all(d in state.domain_exclusions for d in doms):
            return False
    return True


def eligible_pages(ms_item: dict, state: FilterState, host: FilterHost) -> list:
    """The pages of ``ms_item`` that the current filters keep, in their own order.

    Empty means the manuscript is not shown at all.
    """
    pages = item_pages(ms_item)
    if not state.active():
        return pages
    if not manuscript_passes(ms_item, state, host):
        return []
    shelf_rule = state.column_rules.get("shelfmark")
    if shelf_rule is not None:
        fields = host.ms_fields(ms_item) or {}
        if not text_matches(fields.get("shelfmark", ""), shelf_rule):
            # The Shelfmark cell of the manuscript row does not match; its PAGE rows
            # carry their own visible cells ("Image 4 [2v]"), and the tree has
            # always kept a manuscript whose page row matched. Only pages that have
            # a row of their own (page_texts shelfmark not None) can match this way.
            pages = [
                p for p in pages
                if (host.page_texts(p, ms_item) or {}).get("shelfmark") is not None
                and text_matches((host.page_texts(p, ms_item) or {}).get("shelfmark"), shelf_rule)
            ]
            if not pages:
                return []
    page_rules = {k: r for k, r in state.column_rules.items() if k in PAGE_LEVEL_COLUMNS}
    if not page_rules:
        return pages
    kept = []
    for page in pages:
        texts = host.page_texts(page, ms_item) or {}
        if all(text_matches(texts.get(k, ""), r) for k, r in page_rules.items()):
            kept.append(page)
    return kept


# --- categories -----------------------------------------------------------------------

# Stable ids for the tree's top-level groups. Labels are translated by the host.
CATEGORY_MAIN = "main"
CATEGORY_APPENDIX = "appendix"
CATEGORY_FILTERED = "filtered"
CATEGORY_EXCLUDED = "excluded"
CATEGORY_ALL = "all"            # the "Sort by shelfmark only" flat view


def filter_reason_ids(ms_item: dict) -> tuple:
    """Stable ids for why a result sits in the Filtered group, from its pages and itself.

    Raw ``filter_reason`` values are kept as-is ('source_text', 'high_frequency',
    'duplicate_photography', and any future value); legacy flags map to
    'source_text' / 'filtered'. Sorted, so the same set always gives the same id.
    """
    reasons = set()
    for rec in list(ms_item.get("pages") or []) + [ms_item]:
        fr = rec.get("filter_reason") or ""
        if fr:
            reasons.add(str(fr))
        elif rec.get("is_text_filtered"):
            reasons.add("source_text")
        elif rec is not ms_item and rec.get("is_filtered"):
            reasons.add("filtered")
    return tuple(sorted(reasons))


@dataclass(frozen=True)
class ViewGroup:
    """One run of results in the order the tree shows them."""
    category: str
    items: tuple
    subgroup: Optional[str] = None      # appendix signature, or the reason label


def viewer_pages(groups: List[ViewGroup], state: FilterState, host: FilterHost):
    """Yield ``(group, ms_item, page)`` for every eligible page, in tree order.

    Built from data, not widgets: collapsed groups, unexpanded lazy groups and rows a
    batched load has not drawn yet are all included exactly as the tree will show
    them once drawn.
    """
    for group in groups:
        for ms_item in group.items:
            for page in eligible_pages(ms_item, state, host):
                yield group, ms_item, page
