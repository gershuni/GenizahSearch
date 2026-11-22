# -*- coding: utf-8 -*-
"""The columns a shelf picker draws, in both apps: which levels are shown, their rows, the
shelfmarks of the node chosen last and the breadcrumb. Pure: it reads a ``ShelfmarkTreeService``
and returns plain values (rows are dicts, shelfmarks are ``ShelfLeaf``); no Qt, no NiceGUI. The
web dialog (``web/components/shelfmark_tree_picker.py``) and the desktop widget
(``desktop/widgets/shelfmark_tree_picker.py``) draw what it returns, so the two cannot drift.

Lifted from the web picker of PR 2 unchanged in behaviour (plan §3.3, D10; handoff "PR 3":
the desktop uses these, it does not copy them).

``labels`` are the interface strings a view needs, translated by the caller before (a worker
thread does not carry the visitor's language): ``library``, ``others``, ``in`` (with
``{place}``), ``part`` (with ``{part}``).
"""
from __future__ import annotations

import re
from typing import Callable, Optional

from shared.shelfmark_tree_service import MAX_PAGE
from shared.shelfmark_tree_view import PAGE_SIZE, display_name, is_top_library

#: Node rows a column shows before "Show more".
NODE_ROWS = 300
#: Node columns a wide picker shows (the earlier ones are in the breadcrumb).
WIDE_COLUMNS = 3


def skip(node) -> bool:
    """A node with one child and nothing of its own is not a level a reader chooses (a library with a
    single collection: Oxford's "Library Shelmarks")."""
    return node is not None and node.child_nodes == 1 and node.direct_leaves == 0


def rel_path(service, base: Optional[str], node_id: str) -> tuple:
    """The node ids from just below ``base`` down to ``node_id``."""
    chain = [n.id for n in service.ancestors(node_id)]
    return tuple(chain[chain.index(base) + 1:]) if base in chain else tuple(chain)


def part_text(lf, labels: dict) -> str:
    """The Oxford Part with its word before it: alone it reads as another call number."""
    return labels['part'].format(part=lf.part) if lf.part else ''


#: "Ms." as a word: what a call number that only restates the shelfmark may follow.
_MS_WORD = re.compile(r"(?:^|(?<=[\s,]))mss?\.?(?=\s)")


def _bare(text: str) -> str:
    return ''.join((text or '').split()).casefold()


def restates(name: str, shelfmark: str) -> bool:
    """``name`` is ``shelfmark`` again (case and spaces aside), or it after "Ms." or a library's name and
    "Ms." ("Cambridge University Library Ms. T-S AS 39.7" of "T-S AS 39.7", "Ms. T-S C 7.119" of
    "T-S C7.119"): nothing the reader does not already see. A different spelling ("Ms. heb. a. 1.5" of
    "MS heb. a.1/5"), or the shelfmark at the end of a longer call number, is not."""
    a, b = ' '.join((name or '').split()).casefold(), _bare(shelfmark)
    if not a or not b:
        return False
    if _bare(a) == b:
        return True
    return any(b in (_bare(a[m.end():]), _bare(a[m.start():])) for m in _MS_WORD.finditer(a))


def call_numbers_once(names) -> list:
    """Each call number once, whichever order its forms come in: one that restates a form listed
    already is left out, and a short form takes the place of the longer forms listed before it
    ("Cambridge University Library, Ms. Or. 1080 J 6 | Or. 1080 J 6" is "Or. 1080 J 6")."""
    out: list = []
    for name in names:
        if any(restates(name, x) for x in out):
            continue
        longer = [j for j, x in enumerate(out) if restates(x, name)]
        if not longer:
            out.append(name)
            continue
        out[longer[0]] = name
        for j in reversed(longer[1:]):
            del out[j]
    return out


def hint(lf, labels: dict) -> str:
    """What a shelfmark's tooltip adds: its own call number where FGP's label differs (more than in its
    spaces), its Part, and up to three of its other short call numbers (the long "Library ... Ms."
    forms, and any that only restate the shelfmark, left out)."""
    others = call_numbers_once([a for a in (lf.aliases or '').split(' | ')
                                if a and len(a) <= 28 and ',' not in a
                                and not any(restates(a, x) for x in (lf.label, lf.shelfmark))])[:3]
    own = lf.shelfmark if lf.shelfmark and not restates(lf.shelfmark, lf.label) else ''
    return ' · '.join(x for x in (own, part_text(lf, labels), *others) if x)


def paged(fetch: Callable[[int, int], list], limit: int) -> list:
    """The first ``limit`` results of ``fetch(offset, n)``, asked for in pages the service serves."""
    out = []
    while len(out) < limit:
        want = min(MAX_PAGE, limit - len(out))
        page = fetch(len(out), want)
        out += page
        if len(page) < want:
            break
    return out


#: The longest title a shelfmark's tooltip shows; a longer one is cut, with an ellipsis.
TITLE_CHARS = 120


def short_title(title: str, limit: int = TITLE_CHARS) -> str:
    """A title on one line, cut at ``limit`` characters (at a space where there is one near)."""
    text = ' '.join((title or '').split())
    if len(text) <= limit:
        return text
    cut = text[:limit]
    space = cut.rfind(' ')
    return (cut[:space] if space > limit * 2 // 3 else cut).rstrip(' ,;:.') + '…'


def leaf_tooltip(row: dict) -> str:
    """A shelfmark row's tooltip, the same in both apps: its other call numbers (``hint``), then the
    manuscript's title on its own line. Empty when there is neither."""
    return '\n'.join(x for x in (row.get('hint') or '', row.get('title') or '') if x)


def _matches_library(text: str, names: tuple) -> bool:
    return any(text in (n or '').lower() for n in names)


def library_rows(service, kids, shortcuts, selected, lang, labels, text):
    """The libraries, the largest first, then A-Z; a library shown inside another keeps a shortcut.
    A filter finds a library by its names in both languages, its short name and its codes ("RNL",
    "CUL": what the chips call it), so a code typed in Hebrew finds it too."""
    entries = [(k.manuscripts, display_name(k, lang), k, None) for k in kids]
    for _sid, en, he, short, target in shortcuts:
        name = he if lang == 'he' and he else en
        entries.append((target.manuscripts, name, target, short))
    if text:
        codes = service.library_codes()
        entries = [e for e in entries
                   if _matches_library(text, (e[1], e[2].name_en, e[2].name_he, e[2].short, e[3],
                                              *codes.get(e[2].id, ())))]
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


def load_view(service, path: tuple, filters: dict, shown: dict, lang: str, labels: dict,
              title_of: Optional[Callable[[str], str]] = None) -> dict:
    """Everything a picker draws for a path: ``columns`` (each with ``depth``, ``parent``, ``title``,
    ``rows``, ``more``, ``filter_key``; the libraries' one has ``libs``), ``leaves`` (the chosen
    node's own shelfmarks, or None) and ``crumbs`` (``(depth, node id, name)``). Blocking: call it
    off the UI thread.

    ``title_of(sys_id)``: a manuscript's title (the app's catalogue); each shelfmark row then has
    its ``title``, cut to ``TITLE_CHARS``, for ``leaf_tooltip``."""
    def titled(lf) -> str:
        if title_of is None:
            return ''
        try:
            return short_title(title_of(lf.sys_id) or '')
        except Exception:   # noqa: BLE001 -- a row is drawn without its title rather than not at all
            return ''

    columns = []
    parents = (None,) + tuple(path)
    for depth, parent in enumerate(parents):
        if parent is not None:
            node = service.node(parent)
            if node is None or node.child_nodes == 0 or skip(node):
                continue
        key = f"n:{parent}"
        text = (filters.get(key) or "").strip().lower()
        selected = path[depth] if depth < len(path) else None
        if parent is None:
            rows = library_rows(service, service.children(None), service.shortcuts(), selected, lang, labels, text)
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
            for n in paged(lambda off, n: service.search_nodes(parent, text, limit=n, offset=off), limit):
                rel = rel_path(service, parent, n.id)
                via = ' › '.join(display_name(service.node(x), lang) for x in rel[:-1])
                rows.append({'id': n.id, 'label': display_name(n, lang), 'count': n.manuscripts, 'on': False,
                             'title': via, 'path': via, 'go': base + rel[:-1] + passed_through(service, n.id)})
                # base ends at parent; a match of one child opens below it, as a click does
            more = max(0, service.count_nodes(parent, text) - limit)
            if any(ch.isdigit() for ch in text):
                total = service.search_leaves(parent, text, limit=0)[1]
                found = paged(lambda off, n: service.search_leaves(parent, text, limit=n, offset=off)[0], limit)
                rows += [{'leaf': lf, 'label': lf.label or lf.shelfmark, 'hint': part_text(lf, labels),
                          'title': titled(lf)} for lf in found]
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
                                'hint': hint(lf, labels), 'title': titled(lf)} for lf in page],
                      'more': max(0, total - len(page)), 'filter_key': key}
    crumb_nodes = [service.node(p) for p in path]
    crumbs = [(i, n.id, display_name(n, lang)) for i, n in enumerate(crumb_nodes)
              if n is not None and not (i and skip(crumb_nodes[i - 1]))]
    return {'columns': columns, 'leaves': leaves, 'crumbs': crumbs}


def leaf_in_view(view: Optional[dict], leaf, columns: Optional[list] = None) -> bool:
    """Whether ``leaf`` is still in the lists ``view`` (``load_view``) draws: its own list of
    shelfmarks, whatever that list's filter hides, or a row a column's filter found. ``columns``: the
    node columns on screen (``shelfmark_tree_rows.shown_columns``: the last three, or the last one in
    the table and the thumbnails); a column the layout leaves out is not in view (Codex #99). All of
    the view's when not given. The details pane and the single choice follow it in both apps: a
    shelfmark left behind is no longer shown or chosen (owner, 2026-11-22)."""
    if not view or leaf is None:
        return False
    leaves = view.get('leaves')
    if leaves is not None and leaves.get('node') == leaf.node_id:
        return True
    cols = view.get('columns') if columns is None else columns
    return any(row.get('leaf') is not None and row['leaf'].id == leaf.id
               for col in cols or () for row in col.get('rows') or ())


def settle(service, path: tuple, current: tuple) -> tuple:
    """A path never ends at a level that is no choice (one child, no shelfmarks): its column is not
    drawn. Going back (a prefix of where the reader is) climbs past such levels; going anywhere
    else goes down through them, as a click does."""
    if not path:
        return path
    node = service.node(path[-1])
    if node is None or not skip(node):
        return path
    if current[:len(path)] == path and len(current) > len(path):
        while path:
            node = service.node(path[-1])
            if node is None or not skip(node):
                break
            path = path[:-1]
        return path
    return path[:-1] + passed_through(service, path[-1])


def passed_through(service, node_id: str) -> tuple:
    """A node and the single children below it: a level with one child is not a choice."""
    out = [node_id]
    while True:
        node = service.node(out[-1])
        if node is None or node.direct_leaves or node.child_nodes != 1:
            return tuple(out)
        out.append(service.children(node.id)[0].id)
