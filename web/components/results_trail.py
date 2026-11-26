# -*- coding: utf-8 -*-
"""Browse opened from a search result: a thin bar to the search's other results.

Without it a reader who opened one result in Browse had to go back to the search page to reach
the next one. The search page keeps the list it shows (``keep_trail``, from
``web/pages/search_results.py::render_results``), and each result's Browse link carries the
list's code and the result's place in it (``trail=``, ``r=``). Browse shows the bar only for
such a link (``place_results_bar``): previous / next result, the count opening a scrolling
list of the results (one line each, the shelfmark and the matched words), the way back to the
search, and a close button that takes the bar and its two parameters away.

The lists are kept in memory, for the browser that made them (its session code), bounded in
number, size and age: a restart, another browser, or a link sent to someone else shows no bar
and Browse opens as it always did. A result opened from the bar replaces the page in the tab's
history (``location.replace``), so the browser's Back from any of them returns to the search.

The search page's address (its ``run=`` code restores that exact search) is not known to the
server, so the click on a result's link writes it in the browser, under the list's code
(``SEARCH_PAGE_JS``); Browse's "Back to the search results" reads it there, else opens /search,
which restores the last search.
"""
from __future__ import annotations

import html
import json
import re
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from web.translations import is_rtl, tr

#: The Browse address's parameters: the list's code and the result's place in it (from 0).
TRAIL_PARAM = "trail"
INDEX_PARAM = "r"
#: Results kept for one list; a result past them opens Browse without the bar.
MAX_ITEMS = 2000
#: Lists kept in all, and how long one is kept after it was last used.
MAX_TRAILS = 200
KEEP_SECONDS = 6 * 60 * 60
#: Characters of a result's matched words shown in the list.
TEXT_CHARS = 90
#: Characters of a result's title shown in the list (its beginning).
TITLE_CHARS = 40

_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{8,40}$")
#: The UI's line-break sentinel in a snippet (SearchEngine.highlight).
_LINE_BREAK = "‖"


@dataclass(frozen=True)
class TrailItem:
    url: str          # the result's Browse address, without this list's parameters
    shelfmark: str
    text: str         # the matched words, short (short_text)
    title: str = ""   # the beginning of the manuscript's title (short_title)


@dataclass
class Trail:
    token: str
    owner: str
    items: tuple
    more: bool        # the search found more than the list holds (a capped count, or cut here)
    touched: float


_trails: "OrderedDict[str, Trail]" = OrderedDict()
_lock = threading.Lock()


def short_text(snippet: Optional[str]) -> str:
    """A result's snippet as one short line: its match markers and line-break sentinels gone,
    its spaces collapsed, cut to TEXT_CHARS with an ellipsis."""
    text = (snippet or "").replace("*", "").replace(_LINE_BREAK, " ")
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > TEXT_CHARS:
        text = text[:TEXT_CHARS].rstrip() + "…"
    return text


def short_title(title: Optional[str]) -> str:
    """The beginning of a result's title, one line: spaces collapsed, cut to TITLE_CHARS with
    an ellipsis."""
    text = re.sub(r"\s+", " ", title or "").strip()
    if len(text) > TITLE_CHARS:
        text = text[:TITLE_CHARS].rstrip() + "…"
    return text


def _prune(now: float) -> None:
    for token in [t for t, trail in _trails.items() if now - trail.touched > KEEP_SECONDS]:
        del _trails[token]
    while len(_trails) > MAX_TRAILS:
        _trails.popitem(last=False)


def keep_trail(items: Iterable[TrailItem], *, owner: Optional[str], more: bool = False) -> Optional[str]:
    """Keep a search's results for its Browse links; their code, or None when there is no owner
    to keep them for (no session) or no result."""
    if not owner:
        return None
    kept = []
    for item in items:
        if len(kept) >= MAX_ITEMS:
            more = True
            break
        kept.append(item)
    if not kept:
        return None
    token = secrets.token_urlsafe(9)
    now = time.monotonic()
    with _lock:
        _trails[token] = Trail(token=token, owner=owner, items=tuple(kept), more=more, touched=now)
        _prune(now)
    return token


def get_trail(token: Optional[str], *, owner: Optional[str]) -> Optional[Trail]:
    """The list kept under ``token`` for this owner, or None (unknown, expired, or another
    browser's)."""
    if not token or not owner or not _TOKEN_RE.match(token):
        return None
    now = time.monotonic()
    with _lock:
        _prune(now)
        trail = _trails.get(token)
        if trail is None or not secrets.compare_digest(trail.owner, owner):
            return None
        trail.touched = now
        _trails.move_to_end(token)
        return trail


def parse_index(value) -> Optional[int]:
    """The ``r=`` parameter as a place in a list, or None."""
    try:
        index = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return index if index >= 0 else None


def trail_params(token: Optional[str], index: int) -> str:
    """The part a result's Browse address gains to show the bar; '' without a list."""
    if not token:
        return ""
    return f"&{TRAIL_PARAM}={token}&{INDEX_PARAM}={int(index)}"


def item_url(trail: Trail, index: int) -> str:
    """The Browse address of the list's result at ``index``, with the list's parameters."""
    return trail.items[index].url + trail_params(trail.token, index)


def count_label(trail: Trail, index: int) -> str:
    """"Result 7 of 143" ("143+" when the search found more than the list holds)."""
    total = f"{len(trail.items):,}" + ("+" if trail.more else "")
    return tr("Result {index} of {total}").format(index=f"{index + 1:,}", total=total)


def clear() -> None:
    """Forget every list (tests)."""
    with _lock:
        _trails.clear()


# --- The search page: the search's own address, for the way back ----------------------------

#: Installed once on the search page: a click (or middle click) on a result's link that carries
#: a list's code writes the search page's address under that code, for Browse's way back. The
#: newest 20 are kept.
SEARCH_PAGE_JS = """
(() => {
  if (window.__gsTrailBack) return;
  window.__gsTrailBack = true;
  const keep = (e) => {
    const a = e.target && e.target.closest && e.target.closest('a[href*="trail="]');
    if (!a) return;
    try {
      const token = new URL(a.href, location.href).searchParams.get('trail');
      if (!token) return;
      const all = JSON.parse(localStorage.getItem('gs-trail-back') || '{}');
      delete all[token];
      all[token] = location.pathname + location.search;
      const keys = Object.keys(all);
      while (keys.length > 20) delete all[keys.shift()];
      localStorage.setItem('gs-trail-back', JSON.stringify(all));
    } catch (err) {}
  };
  document.addEventListener('click', keep, true);
  document.addEventListener('auxclick', keep, true);
})();
"""


def _back_js(token: str) -> str:
    return ("(() => { let u = '/search'; try { const all = JSON.parse(localStorage.getItem('gs-trail-back') || '{}');"
            f" const v = all[{json.dumps(token)}]; if (typeof v === 'string' && v.startsWith('/search')) u = v; }}"
            " catch (err) {} location.href = u; })()")


# --- Browse: the bar ----------------------------------------------------------------------------

def list_html(trail: Trail, index: int) -> str:
    """The scrolling list: one link a result (its number, shelfmark, the beginning of its title
    and its matched words), the current one marked; every text escaped. A click replaces the
    page (the bar's own way), a middle click opens a tab as any link does."""
    rows = []
    for i, item in enumerate(trail.items):
        current = i == index
        attrs = ' aria-current="true" data-current="1"' if current else ""
        title = (f'<span class="results-trail-title" dir="auto">{html.escape(item.title)}</span>'
                 if item.title else '')
        rows.append(
            f'<a class="results-trail-item{" is-current" if current else ""}"{attrs} '
            f'href="{html.escape(item_url(trail, i), quote=True)}">'
            f'<span class="results-trail-num">{i + 1}</span>'
            f'<span class="results-trail-sm" dir="auto">{html.escape(item.shelfmark)}</span>{title}'
            f'<span class="results-trail-text" dir="auto">{html.escape(item.text)}</span></a>')
    return "".join(rows)


#: The list's links replace the page instead of adding a history entry; the current result
#: is scrolled into view when the list opens.
_LIST_JS = """
(() => {
  const box = document.querySelector('.results-trail-list');
  if (!box) return;
  if (!box.dataset.wired) {
    box.dataset.wired = '1';
    box.addEventListener('click', (e) => {
      const a = e.target.closest('a.results-trail-item');
      if (!a || e.ctrlKey || e.metaKey || e.shiftKey || e.button !== 0) return;
      e.preventDefault();
      location.replace(a.href);
    });
  }
  // Only the list scrolls to the current result (scrollIntoView would scroll the menu too, and
  // take the way back above the list out of view).
  const cur = box.querySelector('[data-current]');
  if (cur) box.scrollTop = Math.max(0, cur.offsetTop - box.offsetTop - (box.clientHeight - cur.offsetHeight) / 2);
})();
"""

_STYLE = """
<style>
.results-trail { min-height: 34px; padding: 2px 6px; margin-bottom: 8px; border-radius: 8px;
  background: var(--bg-tertiary); border: 1px solid var(--border-light); color: var(--text-secondary); }
.results-trail .results-trail-count { min-width: 0; }
.results-trail-list { max-height: 60vh; overflow-y: auto; overflow-x: hidden; width: min(42rem, calc(92vw - 40px)); }
.results-trail-item { display: flex; min-width: 0; gap: 8px; align-items: baseline; padding: 6px 12px;
  text-decoration: none; color: var(--text-primary); border-bottom: 1px solid var(--border-light); }
.results-trail-item:hover, .results-trail-item:focus { background: var(--bg-tertiary); }
.results-trail-item.is-current { background: var(--primary-100);
  box-shadow: inset -3px 0 0 var(--border-focus, #10b981); }
[dir="ltr"] .results-trail-item.is-current { box-shadow: inset 3px 0 0 var(--border-focus, #10b981); }
/* The dark theme's own selected-row colour: --primary-100 is a light green there too, under light text. */
[data-theme="dark"] .results-trail-item.is-current { background: var(--bg-active); }
.results-trail-num { color: var(--text-muted); font-size: 0.75rem; min-width: 2.5em; flex-shrink: 0; }
.results-trail-sm { font-weight: 600; white-space: nowrap; flex-shrink: 0; }
.results-trail-title { font-size: 0.85rem; color: var(--text-primary); flex: 0 1 auto; max-width: 15em;
  min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.results-trail-text { color: var(--text-secondary); font-size: 0.85rem; min-width: 0; flex: 1 1 0;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
@media print { .results-trail { display: none !important; } }
</style>
"""


def browse_results_bar(results_trail: Optional[tuple], *, kept: dict, on_close: Callable[[], None]) -> None:
    """Browse's bar, for its ``(trail, r)`` parameters: placed when the list is this browser's
    and holds that place, its parameters then put in ``kept`` (Browse writes them into its
    address, so a reload shows the bar again). Closing the bar empties ``kept`` before
    ``on_close``. An unknown list (a restart, another browser) places nothing, and Browse's next
    address leaves its parameters out."""
    if not results_trail:
        return
    token, raw_index = results_trail
    from web.safe_storage import get_persisted_session_uuid
    trail = get_trail(token, owner=get_persisted_session_uuid())
    index = parse_index(raw_index)
    if trail is None or index is None or index >= len(trail.items):
        return
    kept.update({TRAIL_PARAM: trail.token, INDEX_PARAM: index})

    def _closed() -> None:
        kept.clear()
        on_close()
    place_results_bar(trail, index, on_close=_closed)


def place_results_bar(trail: Trail, index: int, *, on_close: Callable[[], None]):
    """The bar, where the caller is building: close, previous, the count with its list, next.
    Returns the bar's row. ``on_close`` runs after the bar is hidden (Browse drops the list's
    parameters from its address)."""
    from nicegui import ui

    ui.add_head_html(_STYLE)
    rtl = is_rtl()

    def _go(i: int) -> None:
        if 0 <= i < len(trail.items):
            ui.run_javascript(f"location.replace({json.dumps(item_url(trail, i))})")

    def _close() -> None:
        bar.set_visibility(False)
        on_close()

    with ui.row(wrap=False).classes('w-full items-center gap-1 results-trail print-hide').props(
            f'role=navigation aria-label="{tr("Search results")}" data-testid="results-trail"') as bar:
        ui.button(icon='close', on_click=_close).props(
            f'flat round dense size=sm aria-label="{tr("Close")}" data-testid="results-trail-close"'
        ).tooltip(tr('Close'))
        prev_btn = ui.button(icon='chevron_right' if rtl else 'chevron_left', on_click=lambda: _go(index - 1)).props(
            f'flat round dense size=sm aria-label="{tr("Previous result")}" data-testid="results-trail-prev"'
        ).tooltip(tr('Previous result'))
        if index <= 0:
            prev_btn.disable()
        with ui.button(on_click=lambda: None).props(
                'flat dense no-caps size=sm icon-right=expand_more data-testid="results-trail-count"'
        ).classes('results-trail-count'):
            ui.label(tr('Search results') + ' · ' + count_label(trail, index)).classes('ellipsis')
            # The list scrolls, not the menu: room for the list and the way back above it.
            with ui.menu().props('max-height=80vh max-width=92vw') as menu:
                with ui.column().classes('gap-0 no-wrap'):
                    with ui.link(target='/search').classes('results-trail-item').props(
                            'data-testid="results-trail-back"') as back:
                        ui.icon('arrow_forward' if rtl else 'arrow_back').classes('text-sm')
                        ui.label(tr('Back to the search results'))
                    back.on('click.prevent', lambda: ui.run_javascript(_back_js(trail.token)))
                    listing = ui.html('', sanitize=False).classes('results-trail-list')

                def _on_show() -> None:
                    if not listing.content:
                        listing.set_content(list_html(trail, index))
                    ui.run_javascript(_LIST_JS)
                menu.on('show', _on_show)
        next_btn = ui.button(icon='chevron_left' if rtl else 'chevron_right', on_click=lambda: _go(index + 1)).props(
            f'flat round dense size=sm aria-label="{tr("Next result")}" data-testid="results-trail-next"'
        ).tooltip(tr('Next result'))
        if index >= len(trail.items) - 1:
            next_btn.disable()
    return bar
