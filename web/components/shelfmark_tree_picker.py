# -*- coding: utf-8 -*-
"""The web "Pick from the shelf" dialog: library, collection, volume, shelfmark, as FGP's
"find a fragment" screen has them (shelf tree picker plan §3.3, D10; mockup canvas "Main").

Columns on a wide screen -- the last three levels of the path and the shelfmarks of the node
chosen last -- and one level at a time on a phone (the same panels; the CSS shows only the
last). Every column has a filter box and counts; a level with a single child is passed through.
The breadcrumb goes back to any level.

The contract (§10.4), which the desktop widget keeps too: ``on_pick`` receives
``{sys_id, shelfmark, placement_id}``; ``start`` is a ``sys_id``, a placement id, a node id or a
typed prefix (``shared.shelfmark_tree_view.start_at``). Single choice in this PR: a click on a
shelfmark picks it and closes the dialog. Closing the dialog otherwise is "cancel".

Data comes from ``shared.shelfmark_tree_service`` off the event loop; ``service`` is the one
``web.shelfmark_tree_assets.shelf_tree_service()`` answers (the caller checks it is served).
"""
from __future__ import annotations

import html
import inspect
from typing import Any, Callable, Optional

from nicegui import run, ui

from shared.shelfmark_tree_service import MAX_PAGE
from shared.shelfmark_tree_view import (
    PAGE_SIZE,
    direct_index,
    display_name,
    is_top_library,
    start_at,
)
from web.components.typography import h3
from web.translations import get_language, is_rtl, tr

#: Node rows a column shows before "Show more".
NODE_ROWS = 300
#: Node columns a wide screen shows (the earlier ones are in the breadcrumb).
WIDE_COLUMNS = 3

_CSS = """
.stp-card { width: min(1240px, 96vw); max-width: 96vw; height: min(86vh, 900px); display: flex;
  flex-direction: column; padding: 14px 16px; gap: 8px; }
.stp-body { flex: 1 1 auto; min-height: 0; display: flex; gap: 10px; overflow: hidden; }
.stp-col { flex: 0 1 220px; min-width: 160px; display: flex; flex-direction: column; gap: 6px; }
.stp-col.stp-libs { flex: 0 1 270px; min-width: 200px; }
.stp-col > * { flex: none; }
.stp-col > .stp-list { flex: 1 1 auto; }
.stp-leaves { flex: 1 1 auto; min-width: 280px; }
.stp-list { flex: 1 1 auto; min-height: 0; overflow-y: auto; border: 1px solid var(--border-light, #d5d8de);
  border-radius: 8px; background: var(--bg-primary, #fff); color: var(--text-primary, #1d2129); }
.stp-grid { display: grid; align-content: start;
  grid-template-columns: repeat(auto-fill, minmax(min(100%, var(--stp-leaf, 140px)), 1fr)); }
.stp-row { display: flex; justify-content: space-between; gap: 8px; align-items: center; padding: 7px 10px;
  cursor: pointer; border-bottom: 1px solid var(--border-light, #eef0f3); min-height: 40px; }
.stp-row:hover { background: var(--bg-tertiary, #f3f5f8); }
.stp-row.stp-on { background: #1f5fbf; color: #fff; }
.stp-row.stp-on .stp-count { color: #fff; }
.stp-count { color: var(--text-secondary, #5b6270); font-size: 12px; flex: none; }
.stp-sep { padding: 6px 10px; font-size: 12px; font-weight: 600; color: var(--text-secondary, #5b6270);
  background: var(--bg-tertiary, #f3f5f8); }
.stp-sm { direction: ltr; unicode-bidi: isolate; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.stp-name { min-width: 0; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
  line-height: 1.25; overflow-wrap: anywhere; }
/* a library's name is never cut: it wraps to as many lines as it needs (a phone has no tooltip) */
.stp-libs .stp-name { display: block; -webkit-line-clamp: unset; overflow: visible; }
.stp-link { font-style: italic; }
.stp-path { font-size: 11px; color: var(--text-secondary, #5b6270); }
.stp-row.stp-on .stp-path { color: #e5edf9; }
.stp-title { font-weight: 600; font-size: 13px; color: var(--text-primary, #1d2129); }
.stp-crumbs { display: flex; flex-wrap: wrap; align-items: center; gap: 4px; font-size: 13px; }
.stp-crumbs a { color: #1f5fbf; cursor: pointer; }
/* the site's dark theme: the variables above follow it; a link needs a lighter blue to be read */
[data-theme="dark"] .stp-crumbs a { color: #93c5fd; }
.stp-back { display: none; }
/* one level at a time wherever the columns would not fit whole: the library column, two more and the
   shelfmarks (200 + 160 + 160 + 280, three gaps of 10, padding 2 x 16 = 862) in a card of 96vw */
@media (max-width: 900px) {
  .stp-card { width: 100vw; max-width: 100vw; height: 100vh; border-radius: 0; padding: 10px 12px; }
  .stp-body { flex-direction: column; overflow-y: auto; }
  .stp-body > .stp-panel:not(.stp-phone) { display: none; }
  .stp-col { flex: 1 1 auto; min-height: 260px; }
  .stp-grid { grid-template-columns: 1fr; }
  .stp-back { display: inline-flex; }
}
"""


def _skip(node) -> bool:
    """A node with one child and nothing of its own is not a level a reader chooses (a library with a
    single collection: Oxford's "Library Shelmarks")."""
    return node is not None and node.child_nodes == 1 and node.direct_leaves == 0


def _rel_path(service, base: Optional[str], node_id: str) -> tuple:
    """The node ids from just below ``base`` down to ``node_id``."""
    chain = [n.id for n in service.ancestors(node_id)]
    return tuple(chain[chain.index(base) + 1:]) if base in chain else tuple(chain)


def _part(lf, labels: dict) -> str:
    """The Oxford Part with its word before it: alone it reads as another call number."""
    return labels['part'].format(part=lf.part) if lf.part else ''


def _hint(lf, labels: dict) -> str:
    """What a shelfmark's tooltip adds: its own call number where FGP's label differs, its Part, and
    up to three of its other short call numbers (the long "Library ... Ms." forms left out)."""
    others = [a for a in (lf.aliases or '').split(' | ')
              if a and len(a) <= 28 and ',' not in a and a not in (lf.label, lf.shelfmark)][:3]
    return ' · '.join(x for x in (lf.shelfmark if lf.shelfmark != lf.label else '', _part(lf, labels), *others) if x)


def _attr(text: str) -> str:
    return html.escape(text or '', quote=True)


def _name(text: str, more: Optional[str] = None):
    """A name as written: its own direction (a Latin "T-S Ar." in a Hebrew page keeps its dot at the
    end), and the whole of it on hover, with ``more`` (a Part, the other language's name), since a
    long one is cut."""
    lab = ui.label(text).props('dir="auto"')
    lab.tooltip(f"{text} · {more}" if more else text)
    return lab


#: Each list's scroll is kept across a redraw: a click near the end of the libraries does not throw
#: the list back to its top (owner, 2026-11-20).
_KEEP_SCROLL_JS = """
if (!window.__stpScrollOn) {
  window.__stpScrollOn = true;
  document.addEventListener('scroll', (e) => {
    const t = e.target;
    if (t && t.dataset && t.dataset.col) { (window.__stpScroll = window.__stpScroll || {})[t.dataset.col] = t.scrollTop; }
  }, true);
}
"""
_RESTORE_SCROLL_JS = """
setTimeout(() => document.querySelectorAll('.stp-list[data-col]').forEach((el) => {
  const v = (window.__stpScroll || {})[el.dataset.col];
  if (v) { el.scrollTop = v; return; }
  const on = el.querySelector('.stp-on');               // first shown: the chosen row in sight
  if (on) { on.scrollIntoView({block: 'nearest'}); }
}), 30);
"""
#: The shelfmarks are in as many columns as show every one of them whole: a column is as wide as the
#: longest shelfmark shown (three columns, else two, else one; owner, 2026-11-20). Measured in the
#: browser, where the font is known; a phone keeps its one column (the CSS does not read the width).
_FIT_GRID_JS = """
setTimeout(() => document.querySelectorAll('.stp-grid').forEach((grid) => {
  let widest = 0;
  grid.querySelectorAll('.stp-row > .stp-sm').forEach((el) => { widest = Math.max(widest, el.scrollWidth); });
  if (widest) { grid.style.setProperty('--stp-leaf', Math.max(140, widest + 24) + 'px'); }
}), 30);
"""


def _paged(fetch, limit: int) -> list:
    """The first ``limit`` results of ``fetch(offset, n)``, asked for in pages the service serves."""
    out = []
    while len(out) < limit:
        want = min(MAX_PAGE, limit - len(out))
        page = fetch(len(out), want)
        out += page
        if len(page) < want:
            break
    return out


def _library_rows(service, kids, shortcuts, selected, lang, labels, text):
    """The libraries, the largest first, then A-Z; a library shown inside another keeps a shortcut."""
    entries = [(k.manuscripts, display_name(k, lang), k, None) for k in kids]
    for _sid, en, he, short, target in shortcuts:
        name = he if lang == 'he' and he else en
        entries.append((target.manuscripts, name, target, short))
    if text:
        entries = [e for e in entries if text in e[1].lower() or text in (e[2].short or '').lower()
                   or text in (e[3] or '').lower()]
    top = sorted((e for e in entries if e[3] is None and is_top_library(e[2], service)
                  or e[3] is not None and e[0] >= int(service.meta('top_library_min') or 10 ** 9)),
                 key=lambda e: -e[0])
    rest = sorted((e for e in entries if e not in top), key=lambda e: e[1].casefold())
    rows = []
    for group in (top, rest):
        if group is rest and top and rest:
            rows.append({'sep': labels['others']})
        for count, name, node, short in group:
            link = short is not None
            rows.append({'id': node.id, 'label': name, 'count': count, 'link': link,
                         'on': (not link) and node.id == selected,
                         'title': (labels['in'].format(place=display_name(service.ancestors(node.id)[0], lang))
                                   if link else (node.name_en if lang == 'he' else (node.name_he or ''))),
                         'go': tuple(n.id for n in service.ancestors(node.id)) if link else None})
    return rows


def _load_view(service, path: tuple, filters: dict, shown: dict, lang: str, labels: dict) -> dict:
    """Everything the dialog draws for a path (off the event loop). ``labels`` are translated before:
    a worker thread does not carry the visitor's language."""
    columns = []
    parents = (None,) + tuple(path)
    for depth, parent in enumerate(parents):
        if parent is not None:
            node = service.node(parent)
            if node is None or node.child_nodes == 0 or _skip(node):
                continue
        key = f"n:{parent}"
        text = (filters.get(key) or "").strip().lower()
        selected = path[depth] if depth < len(path) else None
        if parent is None:
            rows = _library_rows(service, service.children(None), service.shortcuts(), selected, lang, labels, text)
            columns.append({'depth': depth, 'parent': None, 'title': labels['library'], 'rows': rows,
                            'more': 0, 'filter_key': key, 'libs': True})
            continue
        base = tuple(path[:depth])
        if text:
            # A reader who knows the shelfmark: the volumes and groups anywhere below whose names hold
            # the text, and, with a number, the shelfmarks too.
            # "Show more" raises how many of each (volumes, shelfmarks) are shown.
            limit = shown.get(key, NODE_ROWS)
            rows = []
            for n in _paged(lambda off, n: service.search_nodes(parent, text, limit=n, offset=off), limit):
                rel = _rel_path(service, parent, n.id)
                via = ' › '.join(display_name(service.node(x), lang) for x in rel[:-1])
                rows.append({'id': n.id, 'label': display_name(n, lang), 'count': n.manuscripts, 'on': False,
                             'title': via, 'path': via, 'go': base + rel[:-1] + _passed_through(service, n.id)})
                # base ends at parent; a match of one child opens below it, as a click does
            more = max(0, service.count_nodes(parent, text) - limit)
            if any(ch.isdigit() for ch in text):
                total = service.search_leaves(parent, text, limit=0)[1]
                found = _paged(lambda off, n: service.search_leaves(parent, text, limit=n, offset=off)[0], limit)
                rows += [{'leaf': lf, 'label': lf.label or lf.shelfmark, 'hint': _part(lf, labels)} for lf in found]
                more += max(0, total - len(found))
            columns.append({'depth': depth, 'parent': parent, 'title': display_name(node, lang), 'rows': rows,
                            'more': more, 'filter_key': key})
            continue
        kids = service.children(parent)
        limit = shown.get(key, NODE_ROWS)
        rows = [{'id': k.id, 'label': display_name(k, lang), 'count': k.manuscripts, 'on': k.id == selected,
                 'title': k.name_en if lang == 'he' else (k.name_he or '')} for k in kids[:limit]]
        columns.append({'depth': depth, 'parent': parent, 'title': display_name(node, lang), 'rows': rows,
                        'more': max(0, len(kids) - limit), 'filter_key': key})
    leaves = None
    if path:
        node = service.node(path[-1])
        if node is not None and node.direct_leaves:
            key = f"l:{node.id}"
            text = filters.get(key, '')
            limit = shown.get(key, PAGE_SIZE)
            page, total = service.leaves(node.id, 0, limit, filter=text)
            leaves = {'node': node.id, 'title': display_name(node, lang), 'total': total,
                      'rows': [{'leaf': lf, 'label': lf.label or lf.shelfmark or lf.sys_id,
                                'hint': _hint(lf, labels)} for lf in page],
                      'more': max(0, total - len(page)), 'filter_key': key}
    crumb_nodes = [service.node(p) for p in path]
    crumbs = [(i, n.id, display_name(n, lang)) for i, n in enumerate(crumb_nodes)
              if n is not None and not (i and _skip(crumb_nodes[i - 1]))]
    return {'columns': columns, 'leaves': leaves, 'crumbs': crumbs}


def _settle(service, path: tuple, current: tuple) -> tuple:
    """A path never ends at a level that is no choice (one child, no shelfmarks): its column is not
    drawn. Going back (a prefix of where the reader is) climbs past such levels; going anywhere
    else goes down through them, as a click does."""
    if not path:
        return path
    node = service.node(path[-1])
    if node is None or not _skip(node):
        return path
    if current[:len(path)] == path and len(current) > len(path):
        while path:
            node = service.node(path[-1])
            if node is None or not _skip(node):
                break
            path = path[:-1]
        return path
    return path[:-1] + _passed_through(service, path[-1])


def _passed_through(service, node_id: str) -> tuple:
    """A node and the single children below it: a level with one child is not a choice."""
    out = [node_id]
    while True:
        node = service.node(out[-1])
        if node is None or node.direct_leaves or node.child_nodes != 1:
            return tuple(out)
        out.append(service.children(node.id)[0].id)


def open_shelf_picker(container, service, on_pick: Callable[[dict], Any], start: Optional[str] = None,
                      title: Optional[str] = None):
    """Open the dialog; call ``on_pick({sys_id, shelfmark, placement_id})`` (awaited if async)."""
    lang = get_language()
    labels = {'library': tr('Library'), 'others': tr('All other libraries, A-Z'),
              'in': tr('Shown inside {place}'), 'part': tr('Part: {part}')}
    st = {'path': (), 'filters': {}, 'shown': {}, 'highlight': None, 'parts': {}, 'owed': set()}

    with container:
        ui.add_css(_CSS)
        with ui.dialog() as dialog, ui.card().classes('stp-card').props(
                f'dir={"rtl" if is_rtl() else "ltr"} data-testid="shelf-picker"'):
            with ui.row().classes('w-full items-center justify-between no-wrap'):
                h3(title or tr('Pick from the shelf'), classes='text-lg font-semibold m-0')
                ui.button(icon='close', on_click=dialog.close).props(
                    f'flat round dense aria-label="{tr("Close")}"')
            crumbs_row = ui.element('div').classes('stp-crumbs w-full')
            body = ui.element('div').classes('stp-body w-full')

    async def pick(leaf):
        dialog.close()
        outcome = on_pick(leaf.as_selection())
        if inspect.isawaitable(outcome):
            await outcome

    async def go(path: tuple):
        st['sel'] = sel = st.get('sel', 0) + 1           # supersedes a selection still looked up
        path = await run.io_bound(_settle, service, tuple(path), tuple(st['path']))
        if sel != st['sel']:
            return
        st['path'] = path
        await draw()

    async def choose_node(depth: int, node_id: str):
        # a later click (or any navigation) while this one is looked up wins
        st['sel'] = sel = st.get('sel', 0) + 1
        through = await run.io_bound(_passed_through, service, node_id)
        if sel != st['sel']:
            return
        await go(tuple(st['path'][:depth]) + through)

    async def set_filter(key: str, value: str):
        st['filters'][key] = value or ''
        st['shown'].pop(key, None)
        await draw(only=key)

    async def show_more(key: str, base: int):
        st['shown'][key] = st['shown'].get(key, base) + base
        await draw(only=key)

    def leaf_row(leaf, label, hint, on=False):
        with ui.element('div').classes('stp-row' + (' stp-on' if on else '')).props(
                f'data-leaf="{leaf.id}" data-sys-id="{leaf.sys_id}" role="button" tabindex="0"').on(
                'click', lambda _e=None, lf=leaf: pick(lf)).on(
                'keydown.enter', lambda _e=None, lf=leaf: pick(lf)).on(
                'keydown.space.prevent', lambda _e=None, lf=leaf: pick(lf)):
            _name(label, hint).classes('stp-sm')

    def fill_nodes(col):
        """The rows of a column of nodes (and, under a filter with a number, of shelfmarks)."""
        depth = col['depth']
        for row in col['rows']:
            if 'sep' in row:
                ui.label(row['sep']).classes('stp-sep')
                continue
            if 'leaf' in row:
                leaf_row(row['leaf'], row['label'], row.get('hint'))
                continue
            go_to = row.get('go')
            handler = ((lambda _e=None, g=go_to: go(g)) if go_to is not None
                       else (lambda _e=None, d=depth, n=row['id']: choose_node(d, n)))
            with ui.element('div').classes('stp-row' + (' stp-on' if row['on'] else '')
                                           + (' stp-link' if row.get('link') else '')).props(
                    f'data-node="{row["id"]}" role="button" tabindex="0"'
                    + (' data-shortcut="1"' if row.get('link') else '')).on('click', handler).on(
                    'keydown.enter', handler).on('keydown.space.prevent', handler):
                with ui.element('div').classes('stp-name'):
                    _name(('↪ ' if row.get('link') else '') + row['label'], row['title'])
                    if row.get('path'):
                        ui.label(row['path']).classes('stp-path').props('dir="auto"')
                ui.label(f"{row['count']:,}").classes('stp-count')
        if col['more']:
            ui.button(tr('Show more') + f" ({col['more']:,})",
                      on_click=lambda k=col['filter_key']: show_more(k, NODE_ROWS)).props(
                'flat dense no-caps').classes('w-full')

    def fill_leaves(leaves):
        for row in leaves['rows']:
            leaf_row(row['leaf'], row['label'], row['hint'], on=row['leaf'].id == st['highlight'])
        if leaves['more']:
            ui.button(tr('Show more') + f" ({leaves['more']:,})",
                      on_click=lambda k=leaves['filter_key']: show_more(k, PAGE_SIZE)).props(
                'flat dense no-caps').classes('w-full').style('grid-column: 1 / -1;')

    def leaves_title(leaves) -> str:
        return f"{tr('Shelfmarks')} ({leaves['total']:,}) — {leaves['title']}"

    def filter_box(key: str):
        ui.input(placeholder=tr('Filter'), value=st['filters'].get(key, '')).props(
            'dense outlined clearable debounce=250').classes('w-full').on_value_change(
            lambda e, k=key: set_filter(k, e.value))

    def after_draw():
        body.client.run_javascript(_RESTORE_SCROLL_JS + _FIT_GRID_JS)

    async def draw(only: Optional[str] = None):
        """Draw the dialog for the path. ``only``: one list's filter or length changed, so that list
        alone is drawn again: its filter box stays as it is, with the caret and what is being typed
        (a box drawn again loses both; owner, 2026-11-20)."""
        # a slower, older load (a broad filter) must not replace a newer one
        st['gen'] = gen = st.get('gen', 0) + 1
        if only is None:
            st['whole'] = True                     # the whole dialog is owed until it has been drawn
        else:
            st['owed'].add(only)                   # ... and so is this list, by whichever load is the last
        view = await run.io_bound(_load_view, service, tuple(st['path']), dict(st['filters']),
                                  dict(st['shown']), lang, labels)
        if gen != st['gen']:
            return
        # The last load answers for every list changed meanwhile: a filter typed in one column while
        # another column's load was under way is not left with its old list.
        owed, st['owed'] = st['owed'], set()
        if not st.get('whole') and owed and all(k in st['parts'] for k in owed):
            drawn = {}
            for k in owed:
                shown = next((c for c in view['columns'][-WIDE_COLUMNS:] if c['filter_key'] == k), None)
                leaves = view['leaves'] if view['leaves'] is not None and view['leaves']['filter_key'] == k else None
                if shown is None and leaves is None:
                    drawn = None                   # that list is not in this view any more: draw it all
                    break
                drawn[k] = (shown, leaves)
            if drawn is not None:
                for k, (shown, leaves) in drawn.items():
                    part = st['parts'][k]
                    part['list'].clear()
                    with part['list']:
                        if shown is not None:
                            fill_nodes(shown)
                        else:
                            fill_leaves(leaves)
                    if leaves is not None:
                        part['title'].set_text(leaves_title(leaves))
                after_draw()
                return
        st['whole'] = False
        st['parts'] = parts = {}
        crumbs_row.clear()
        with crumbs_row:
            if st['path']:
                ui.button(icon='arrow_forward' if is_rtl() else 'arrow_back',
                          on_click=lambda: go(tuple(st['path'][:-1]))).props(
                    f'flat round dense aria-label="{tr("Back")}"').classes('stp-back')
            def crumb(text, handler, classes=''):
                # a link without a target is not focusable: the keyboard reaches it as a button
                ui.link(text).props('tabindex="0" role="button" dir="auto"').classes(classes).on('click', handler).on(
                    'keydown.enter', handler).on('keydown.space.prevent', handler)

            crumb(tr('All libraries'), lambda _e=None: go(()), 'stp-crumb')
            for k, (i, nid, name) in enumerate(view['crumbs']):
                ui.label('‹' if is_rtl() else '›').style('color: var(--text-secondary);')
                if k == len(view['crumbs']) - 1:
                    ui.label(name).classes('font-semibold').props('dir="auto"')
                else:
                    crumb(name, lambda _e=None, d=i: go(tuple(st['path'][:d + 1])))
        body.clear()
        columns = view['columns'][-WIDE_COLUMNS:]
        # A phone shows the deepest level only: the last node column (the chosen node's children, which
        # stay reachable when the node also holds shelfmarks of its own) and the shelfmarks, stacked.
        phone_depth = view['columns'][-1]['depth'] if view['columns'] else -1
        if view['leaves'] is not None and phone_depth != len(st['path']):
            phone_depth = -1                       # the chosen node has no child column: shelfmarks only
        with body:
            for col in columns:
                with ui.element('div').classes('stp-col stp-panel'
                                               + (' stp-phone' if col['depth'] == phone_depth else '')
                                               + (' stp-libs' if col.get('libs') else '')):
                    ui.label(col['title']).classes('stp-title stp-sm').props('dir="auto"')
                    filter_box(col['filter_key'])
                    with ui.element('div').classes('stp-list').props(
                            f'data-col="{_attr(col["filter_key"])}"') as holder:
                        fill_nodes(col)
                    parts[col['filter_key']] = {'list': holder}
            leaves = view['leaves']
            if leaves is not None:
                with ui.element('div').classes('stp-col stp-leaves stp-panel stp-phone'):
                    heading = ui.label(leaves_title(leaves)).classes('stp-title stp-sm')
                    filter_box(leaves['filter_key'])
                    with ui.element('div').classes('stp-list stp-grid').props(
                            f'data-col="{_attr(leaves["filter_key"])}"') as holder:
                        fill_leaves(leaves)
                    parts[leaves['filter_key']] = {'list': holder, 'title': heading}
        after_draw()
        if st['highlight']:
            body.client.run_javascript(
                f'setTimeout(() => document.querySelector(\'[data-leaf="{st["highlight"]}"]\')'
                '?.scrollIntoView({block: "center"}), 50)')

    async def begin():
        first = await run.io_bound(start_at, service, start)
        st['path'] = first.path
        if first.leaf is not None:
            st['highlight'] = first.leaf.id
            idx = await run.io_bound(direct_index, service, first.leaf)
            st['shown'][f"l:{first.leaf.node_id}"] = max(PAGE_SIZE, ((idx // PAGE_SIZE) + 1) * PAGE_SIZE)
        await draw()

    dialog.open()
    container.client.run_javascript(_KEEP_SCROLL_JS + 'window.__stpScroll = {};')
    from nicegui import background_tasks
    background_tasks.create(begin(), name='shelf-picker-open')
    return dialog
