# -*- coding: utf-8 -*-
"""Where a catalogue search found a manuscript: the panel Quick View and Browse show for a
manuscript opened from a catalogue search's result (shared/catalog_text_rows.py::found_in_catalogue).

A catalogue hit may lie in any field the search reads -- an FJMS note, a colophon, a mention's
description, a record from a general source -- and several of those appear in no catalogue panel
of the site. The panel shows the matching records themselves, whole, each under its source line,
the search's words marked as the transcription marks them.

The lookup runs off the event loop (``place_found_panel``): with near spellings it builds and
looks up each word's candidates, up to a few tenths of a second a word, and a Browse address can
ask for any query (Codex review of PR #51). Its answers are the catalogue's, the same for every
visitor, so the last ones are kept for the whole server (``_CACHE``).
"""
from __future__ import annotations

import html
import logging
import threading
from collections import OrderedDict
from typing import Optional
from urllib.parse import quote

from shared.catalog_text_rows import (FOUND_HEADING, FOUND_MORE, catalog_options, catalogue_matches,
                                      found_records)
from web.translations import tr

logger = logging.getLogger(__name__)

#: The Browse address's parameters for the search a manuscript was opened from (web/main.py).
QUERY_PARAM = "cq"
NEAR_PARAM = "cnear"
TRANSLATIONS_PARAM = "ctr"
#: Lookups whose answers are kept (a manuscript and a search: a few records each).
CACHE_SIZE = 256
_CACHE: "OrderedDict[tuple, tuple[list, int]]" = OrderedDict()
_CACHE_LOCK = threading.Lock()


def found_records_html(found: Optional[dict]) -> str:
    """The panel's records as HTML for ``ui.html(..., sanitize=False)``: every text escaped, the
    matched words in ``mark.highlight-match`` (common.css), each record's direction its own;
    '' when there is none."""
    if not found or not found.get("records"):
        return ""
    blocks = []
    for record in found["records"]:
        body = "".join(
            f'<mark class="highlight-match">{html.escape(text)}</mark>' if matched else html.escape(text)
            for text, matched in record["segments"]).replace("\n", "<br>")
        direction = "rtl" if record.get("rtl") else "ltr"
        blocks.append(
            f'<div class="catalog-found-record">'
            f'<div class="text-xs catalog-found-source" style="color: var(--text-tertiary);">'
            f'{html.escape(record["label"])}</div>'
            f'<div class="catalog-found-text" dir="{direction}">{body}</div></div>')
    shown, count = len(found["records"]), int(found.get("count") or 0)
    if count > shown:
        more = tr(FOUND_MORE).format(shown=shown, count=count)
        blocks.append(f'<div class="text-xs" style="color: var(--text-tertiary);">{html.escape(more)}</div>')
    return "".join(blocks)


def render_found_panel(found: Optional[dict]) -> bool:
    """The panel, at the caller's place in the page; nothing (and False) when there is nothing to
    show."""
    from nicegui import ui

    body = found_records_html(found)
    if not body:
        return False
    with ui.column().classes("w-full gap-1 p-2 mb-3 rounded-lg catalog-found").style(
            "background: var(--bg-tertiary); border-inline-start: 3px solid var(--primary-500);"):
        ui.label(tr(FOUND_HEADING)).classes("text-sm font-bold")
        ui.html(body, sanitize=False).classes("w-full text-sm").style(
            "max-height: 12rem; overflow-y: auto;")
    return True


def _lookup(sys_id, query, options) -> Optional[tuple]:
    """The panel's lookup -- (sys_id, query, near_spellings, include_translations) -- where the
    catalogue search is served and there is a query; None otherwise."""
    from web.catalog_assets import catalog_available

    if not sys_id or not (query or "").strip() or not catalog_available():
        return None
    opts = catalog_options(options)
    return (str(sys_id), query, opts["near_spellings"], opts["include_translations"])


def lookup_for_result(result) -> Optional[tuple]:
    """Quick View's lookup for a catalogue search's result row (the search the row carries)."""
    info = result.get("catalogue") if isinstance(result, dict) else None
    searched = info.get("searched") if isinstance(info, dict) else None
    if not isinstance(searched, dict):
        return None
    return _lookup((result.get("display") or {}).get("id"), searched.get("query"), searched)


def lookup_for_browse(sys_id: str, query: Optional[str], near: bool, translations: bool) -> Optional[tuple]:
    """Browse's lookup when its address carries a catalogue search (``cq``, ``cnear``, ``ctr``)."""
    return _lookup(sys_id, query, {"near_spellings": near, "include_translations": translations})


def cached_matches(lookup: tuple) -> Optional[tuple[list, int]]:
    """A lookup's answer when it is kept, else None."""
    with _CACHE_LOCK:
        hit = _CACHE.get(lookup)
        if hit is not None:
            _CACHE.move_to_end(lookup)
        return hit


def run_lookup(lookup: tuple) -> tuple[list, int]:
    """Answer *lookup* from the catalogue file and keep the answer. Runs in a worker thread: it
    reads nothing of the visitor's session."""
    sys_id, query, near, translations = lookup
    answer = catalogue_matches(sys_id, query, {"near_spellings": near, "include_translations": translations})
    with _CACHE_LOCK:
        _CACHE[lookup] = answer
        _CACHE.move_to_end(lookup)
        while len(_CACHE) > CACHE_SIZE:
            _CACHE.popitem(last=False)
    return answer


#: Lookups running now, by key (the event loop's thread only): a second caller for the same key
#: -- another visitor, or Quick View drawn again -- waits for the first one's answer instead of
#: building the same candidates again (Codex review of PR #51).
_IN_FLIGHT: dict = {}


async def answer_lookup(lookup: tuple) -> tuple[list, int]:
    """*lookup*'s answer: kept, or from the run already going for it, or from a new run off the
    event loop. One run per key at a time; a caller that goes away does not stop it."""
    import asyncio

    from nicegui import run

    answer = cached_matches(lookup)
    if answer is not None:
        return answer
    running = _IN_FLIGHT.get(lookup)
    if running is None:
        running = asyncio.ensure_future(run.io_bound(run_lookup, lookup))
        _IN_FLIGHT[lookup] = running
        running.add_done_callback(lambda _done, key=lookup: _IN_FLIGHT.pop(key, None))
    return await asyncio.shield(running)


def place_found_panel(lookup: Optional[tuple]) -> None:
    """The panel at the caller's place: drawn now when the lookup's answer is kept, else into a
    slot once the lookup, run off the event loop, answers. Nothing for no lookup."""
    if not lookup:
        return
    from nicegui import background_tasks, ui

    answer = cached_matches(lookup)
    if answer is not None:
        render_found_panel(found_records(*answer, translate=tr))
        return
    slot = ui.element("div").classes("w-full")

    async def _fill():
        try:
            found, count = await answer_lookup(lookup)
            if not found or slot.is_deleted:
                return
            with slot:
                render_found_panel(found_records(found, count, translate=tr))
        except Exception:  # noqa: BLE001 -- the view stays without the panel, said in the log
            logger.warning("catalogue panel: the lookup for %s failed", lookup[0], exc_info=True)

    background_tasks.create(_fill(), name="catalogue-found-panel")


def address_params(searched) -> dict:
    """The Browse address's parameters for a catalogue search (``{'query', 'near_spellings',
    'include_translations'}``), in order: ``cq``, and ``cnear=1`` / ``ctr=0`` when the switches
    are not their defaults; empty without a query."""
    if not isinstance(searched, dict) or not (searched.get("query") or "").strip():
        return {}
    params = {QUERY_PARAM: searched["query"]}
    if searched.get("near_spellings"):
        params[NEAR_PARAM] = "1"
    if not searched.get("include_translations", True):
        params[TRANSLATIONS_PARAM] = "0"
    return params


def browse_params(result) -> str:
    """The Browse address's part that carries a catalogue result's search ('&cq=...'), so Browse
    shows where the search found the manuscript; '' for any other row."""
    info = result.get("catalogue") if isinstance(result, dict) else None
    searched = info.get("searched") if isinstance(info, dict) else None
    return "".join(f"&{key}={quote(value)}" for key, value in address_params(searched).items())
