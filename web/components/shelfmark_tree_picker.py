# -*- coding: utf-8 -*-
"""The web "Pick from the shelf" dialog: library, collection, volume, shelfmark, as FGP's
"find a fragment" screen has them (shelf tree picker plan §3.3, D10; mockup canvas "Main").

Columns on a wide screen -- the last three levels of the path and the shelfmarks of the node
chosen last -- and one level at a time on a phone (the same panels; the CSS shows only the
last). Every column has a filter box and counts; a level with a single child is passed through.
The breadcrumb goes back to any level.

The contract (§10.4), which the desktop widget keeps too: ``on_pick`` receives
``{sys_id, shelfmark, placement_id}``; ``start`` is a ``sys_id``, a placement id, a node id or a
typed prefix (``shared.shelfmark_tree_view.start_at``). Single choice (handoff DX21): a click (or
Enter / Space) on a shelfmark selects it, shown chosen, and the foot's button ("Open", or the
host's own words) picks it and closes the dialog; a double click, or Enter on the selected one, is
that button. Closing the dialog otherwise is "cancel".

The pane at the end side of the columns (below them on a phone; DX25 as revised) shows the shelfmark
clicked last, in either mode, and what the manuscript has: ``shared.shelfmark_tree_details`` off the
loop, its thumbnail after the text, then what only the community database knows (its joins, its
public comments). Round 4: labelled fields -- the shelfmark and its other shelfmarks, the title, the
text of every page in a box that scrolls, the field lines (``field_lines``), the joins and the links
(the links right under the shelfmark on a phone). A join is a link that takes the dialog to that
shelfmark, and has a box that chooses it as its box in the list would, turning the multiple choice
on in a picker that has it. A shelfmark's row has its signs (``shared.shelfmark_tree_signs``: PGP, a
scholarly transcription, a join), read once off the loop: rows are drawn without them until then.
A shelfmark's tooltip has its title (DX24, ``leaf_tooltip``) and names its signs.

Multiple choice (PR 4, plan §10.13): a box on every shelfmark, "Select all in <node>" over a
node's shelfmarks (a box on every node below the libraries too only with ``node_boxes``: the
search's exclusions; DX23), a chips bar with "+N more" (D11), a running count, Clear, Add to List
and one confirm. What is chosen is a
``shared.shelfmark_tree_selection.Selection`` kept per reader and purpose until applied or cleared
(D12, ``web/shelf_selection_store.py``); the host gets ``on_confirm(entries, label)``.

Data comes from ``shared.shelfmark_tree_service`` off the event loop; ``service`` is the one
``web.shelfmark_tree_assets.shelf_tree_service()`` answers (the caller checks it is served). What
the columns hold is ``shared.shelfmark_tree_columns.load_view``, which the desktop widget draws too;
this file only draws it.
"""
from __future__ import annotations

import asyncio
import html
import inspect
import logging
from typing import Any, Callable, Optional

from nicegui import background_tasks, run, ui

from shared.shelfmark_tree_columns import (NODE_ROWS, WIDE_COLUMNS, leaf_tooltip, load_view, passed_through,
                                           settle)
from shared.shelfmark_tree_details import add_joins, field_lines, item_details
from shared.shelfmark_tree_limits import chip_name, limit_for, refusal_message
from shared.shelfmark_tree_rows import (
    COLUMNS, HEADER_TIPS, SIZE_LABELS, SIZES, STORAGE_KEY, choice_value, folds_columns,
    normal_size, normal_view, read_choice, row_details, shown_columns, wanted,
)
from shared.shelfmark_tree_selection import ALL, DIRECT, NODE, Chip, Selection, members_function, scope_for
from shared.shelfmark_tree_signs import SIGN_LABELS, shelf_signs, signs_text
from shared.shelfmark_tree_view import CRUMB_SEP, PAGE_SIZE, direct_index, display_name, place_of, start_at
from web import shelf_selection_store as store
from web.auth_state import GlobalAuthState
from web.components.shelf_selection_lists import save_selection_as_list
from web.components.typography import h3
from web.safe_storage import safe_user_get, safe_user_set
from web.translations import get_language, is_rtl, tr

logger = logging.getLogger(__name__)

_CSS = """
.stp-switch { flex: 0 0 auto; white-space: nowrap; }
.stp-switch .q-toggle__label { white-space: nowrap; }
.stp-card { width: min(1500px, 96vw); max-width: 96vw; height: min(86vh, 900px); display: flex;
  flex-direction: column; padding: 14px 16px; gap: 8px; }
/* the columns, then the pane of the shelfmark clicked last at the end side (left in Hebrew; DX25) */
.stp-main { flex: 1 1 auto; min-height: 0; display: flex; gap: 10px; }
.stp-body { flex: 1 1 auto; min-width: 0; min-height: 0; display: flex; gap: 10px; overflow: hidden; }
.stp-details { flex: 0 1 340px; min-width: 260px; min-height: 0; overflow-y: auto; display: flex; flex-direction: column;
  gap: 8px; padding-inline-start: 10px; border-inline-start: 1px solid var(--border-light, #d5d8de);
  color: var(--text-primary, #1d2129); font-size: 13px; }
.stp-d-thumb img { display: block; max-width: 100%; max-height: 240px; object-fit: contain; border-radius: 6px;
  background: var(--bg-tertiary, #f3f5f8); }
/* labelled fields, one under the other, each spotted by its label (round 4) */
.stp-d-text { display: flex; flex-direction: column; gap: 8px; min-width: 0; }
.stp-d-field { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
/* a Latin line keeps its own direction but lines up with the page's (right in Hebrew) */
.stp-card[dir="rtl"] :is(.stp-d-field, .stp-d-lines) > * { text-align: right; }
.stp-card[dir="ltr"] :is(.stp-d-field, .stp-d-lines) > * { text-align: left; }
.stp-d-sm { font-size: 16px; font-weight: 600; overflow-wrap: anywhere; }
.stp-d-cap { font-size: 12px; font-weight: 600; color: var(--text-secondary, #5b6270); }
/* the text as stored, every page, each under its number: read-only, scrolled in its own box */
.stp-d-textbox { max-height: calc(11 * 1.5em + 12px); overflow-y: auto; font-size: 13px; line-height: 1.5;
  padding: 6px 8px; border: 1px solid var(--border-light, #d5d8de); border-radius: 6px;
  background: var(--bg-tertiary, #f3f5f8); }
.stp-d-textbox:focus-visible { outline: 2px solid #1f5fbf; outline-offset: 1px; }
.stp-d-page { font-size: 11px; font-weight: 600; color: var(--text-secondary, #5b6270); margin-top: 6px; }
.stp-d-page:first-child { margin-top: 0; }
/* a page's text lines up with its own direction (Hebrew to the right in an English page) */
.stp-card .stp-d-textbox .stp-d-pagetext { white-space: pre-wrap; overflow-wrap: anywhere; text-align: start; }
.stp-d-lines { display: flex; flex-direction: column; gap: 3px; }
.stp-d-lines b { font-weight: 600; }
.stp-d-join { display: flex; align-items: center; gap: 6px; min-width: 0; }
.stp-d-join > a { color: #1f5fbf; cursor: pointer; overflow-wrap: anywhere; }
[data-theme="dark"] .stp-d-join > a { color: #93c5fd; }
.stp-d-src { font-size: 11px; color: var(--text-secondary, #5b6270); flex: none; }
.stp-d-links { display: flex; flex-wrap: wrap; gap: 4px 12px; }
.stp-d-links a { color: #1f5fbf; }
[data-theme="dark"] .stp-d-links a { color: #93c5fd; }
.stp-tip { white-space: pre-line; }
/* the signs on a shelfmark's row: PGP and a scholarly transcription as in the search's results (their
   green and amber written out: the site's theme names no such colours for the dark theme), a join */
.stp-row > .stp-sm { min-width: 0; }
.stp-signs { flex: none; display: inline-flex; align-items: center; gap: 3px; margin-inline-start: auto; }
.stp-sign-pgp { font-size: 10px; line-height: 16px; padding: 0 5px; border-radius: 4px; font-weight: 600;
  background: #dcfce7; color: #15803d; }
[data-theme="dark"] .stp-sign-pgp { background: rgba(34, 197, 94, .18); color: #86efac; }
.stp-sign-icon { font-size: 16px; }
.stp-sign-edition { color: #b45309; }
[data-theme="dark"] .stp-sign-edition { color: #fbbf24; }
.stp-sign-join { color: var(--text-secondary, #5b6270); }
.stp-row.stp-on .stp-sign-icon { color: #fff; }
/* the pane below the columns where both would not fit side by side */
@media (max-width: 1180px) {
  .stp-main { flex-direction: column; }
  .stp-details { flex: 0 0 auto; max-height: 34%; min-width: 0; flex-direction: row; align-items: flex-start;
    padding-inline-start: 0; border-inline-start: 0; border-top: 1px solid var(--border-light, #d5d8de); padding-top: 8px; }
  .stp-d-thumb { flex: 0 0 120px; }
  .stp-d-thumb img { max-height: 160px; }
  .stp-d-text { flex: 1 1 auto; }
  .stp-d-textbox { max-height: calc(7 * 1.5em + 12px); }
}
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
  .stp-details { max-height: 30%; }
  /* a phone: the links right under the shelfmark, in sight without scrolling the pane */
  .stp-d-text > * { order: 3; }
  .stp-d-text > .stp-d-f-sm { order: 1; }
  .stp-d-text > .stp-d-f-links { order: 2; }
  .stp-d-thumb { flex: 0 0 72px; }
  .stp-d-thumb img { max-height: 110px; }
  .stp-d-textbox { max-height: calc(6 * 1.5em + 12px); }
  .stp-body > .stp-panel:not(.stp-phone) { display: none; }
  .stp-col { flex: 1 1 auto; min-height: 220px; }
  .stp-grid { grid-template-columns: 1fr; }
  .stp-back { display: inline-flex; }
  .stp-foot { flex-wrap: wrap; }
}
/* a short window: the dialog takes nearly all its height (86% left the shelfmarks a few rows) */
@media (max-height: 760px) and (min-width: 901px) { .stp-card { height: 96vh; } }
/* a short window (a laptop's, a phone held sideways): the pane smaller still, its picture too */
@media (max-height: 760px) and (max-width: 1180px) {
  .stp-details { max-height: 28%; }
  .stp-d-thumb img { max-height: 90px; }
}
/* multiple choice (PR 4): boxes, "Select all", the chips bar (D11) and the footer */
.stp-row.stp-pick { justify-content: flex-start; }
.stp-row.stp-ticked { background: var(--bg-tertiary, #eef3fb); }
.stp-box { flex: none; font-size: 20px; color: #1f5fbf; }
[data-theme="dark"] .stp-box { color: #93c5fd; }
.stp-row.stp-on .stp-box { color: #fff; }
.stp-node-box { flex: none; display: inline-flex; align-items: center; border-radius: 4px; }
/* too many to tick whole here (shared/shelfmark_tree_limits.py); a ticked one stays as it is, to untick */
.stp-too-big { opacity: .38; cursor: not-allowed; }
/* a node's box, its name beside it at the start of the row (either direction), its count at the end */
.stp-row:has(> .stp-node-box) { justify-content: flex-start; }
.stp-row:has(> .stp-node-box) > .stp-count { margin-inline-start: auto; }
.stp-node-box:focus-visible, .stp-row:focus-visible { outline: 2px solid #1f5fbf; outline-offset: -2px; }
.stp-whole { justify-content: flex-start; border: 1px dashed var(--border-light, #d5d8de); border-radius: 8px;
  min-height: 34px; padding: 4px 10px; }
.stp-chips { display: flex; flex-wrap: nowrap; gap: 6px; align-items: center; overflow: hidden; min-height: 34px; }
.stp-chip { display: inline-flex; align-items: center; gap: 4px; flex: none; max-width: 260px; padding: 2px 4px 2px 10px;
  border-radius: 16px; background: var(--bg-tertiary, #eef3fb); color: var(--text-primary, #1d2129);
  border: 1px solid var(--border-light, #d5d8de); font-size: 13px; }
.stp-chip-name { min-width: 0; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; unicode-bidi: isolate; }
.stp-chip-count { color: var(--text-secondary, #5b6270); flex: none; font-size: 12px; }
.stp-more { flex: none; display: none; }
.stp-hint { color: var(--text-secondary, #5b6270); font-size: 13px; }
.stp-foot { display: flex; align-items: center; gap: 8px; border-top: 1px solid var(--border-light, #d5d8de);
  padding-top: 8px; }
.stp-total { font-weight: 600; color: var(--text-primary, #1d2129); }
.stp-all-chips { width: min(640px, 96vw); max-height: 80vh; display: flex; flex-direction: column; gap: 8px; }
.stp-all-list { display: flex; flex-wrap: wrap; gap: 6px; overflow-y: auto; }
.stp-all-list .stp-chip { max-width: 100%; }
.stp-save-card { width: min(440px, 96vw); }
/* the shelfmarks pane's views (list, table, thumbnails), its switch in the pane's header */
.stp-lhead { display: flex; align-items: center; gap: 6px; min-width: 0; }
.stp-lhead > .stp-title { flex: 1 1 auto; min-width: 0; }
.stp-views { display: inline-flex; align-items: center; gap: 2px; flex: none; }
.stp-view.stp-view-on { background: var(--bg-tertiary, #eef3fb); color: #1f5fbf; }
[data-theme="dark"] .stp-view.stp-view-on { color: #93c5fd; }
.stp-views .q-btn-group { margin-inline-start: 4px; }
/* the table: a header that stays, rows of cells; scrolled sideways in its own box where narrow */
.stp-table { overflow: auto; --stp-tcols: minmax(110px, 1fr) minmax(140px, 1.5fr) minmax(200px, 2.6fr)
  minmax(90px, 1fr) 62px 66px 48px 58px; }
.stp-table.stp-multi { --stp-tcols-all: 30px var(--stp-tcols); }
.stp-table > * { min-width: 860px; }
.stp-thead, .stp-trow { display: grid; grid-template-columns: var(--stp-tcols-all, var(--stp-tcols)); gap: 0 10px;
  align-items: center; padding: 6px 10px; }
.stp-thead { position: sticky; top: 0; z-index: 2; background: var(--bg-tertiary, #f3f5f8); font-size: 12px;
  font-weight: 600; color: var(--text-secondary, #5b6270); border-bottom: 1px solid var(--border-light, #d5d8de); }
.stp-trow { min-height: 34px; padding-top: 4px; padding-bottom: 4px; cursor: pointer; border-bottom: 1px solid var(--border-light, #eef0f3); font-size: 13px; }
.stp-trow:hover { background: var(--bg-tertiary, #f3f5f8); }
.stp-trow.stp-ticked { background: var(--bg-tertiary, #eef3fb); }
.stp-trow.stp-on { background: #1f5fbf; color: #fff; }
.stp-trow:focus-visible, .stp-tile:focus-visible { outline: 2px solid #1f5fbf; outline-offset: -2px; }
.stp-cells { display: contents; }
/* a heading's grip, at its end edge: drag to resize the column, double click for the first widths */
.stp-thead > .stp-c { position: relative; }
.stp-rsz { position: absolute; top: 0; bottom: 0; inset-inline-end: 0; width: 8px; cursor: col-resize;
  touch-action: none; border-inline-end: 2px solid transparent; }
.stp-rsz:hover { border-inline-end-color: var(--border-light, #b8bec8); }
.stp-c { min-width: 0; overflow: hidden; }
.stp-c-sm { display: flex; align-items: center; gap: 4px; }
.stp-c-sm > .stp-c-name { direction: ltr; unicode-bidi: isolate; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; min-width: 0; }
.stp-c-clamp { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1.3; }
.stp-c-snip { font-size: 12px; color: var(--text-secondary, #5b6270); }
.stp-trow.stp-on .stp-c-snip { color: #e5edf9; }
.stp-c-num { text-align: end; font-variant-numeric: tabular-nums; }
.stp-c-mark { text-align: center; }
.stp-trow.stp-on .stp-sign-icon { color: #fff; }
.stp-dom { display: inline-block; max-width: 100%; vertical-align: middle; font-size: 11px; padding: 1px 6px;
  border-radius: 4px; background: #f3e8ff; color: #7c3aed; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.stp-dom-more { background: #ede9fe; cursor: help; flex: none; }
.stp-c-dom { display: flex; align-items: center; gap: 2px; }
[data-theme="dark"] .stp-dom { background: rgba(167, 139, 250, .18); color: #c4b5fd; }
.stp-printed { font-size: 11px; padding: 1px 6px; border-radius: 4px; background: #fee2e2; color: #dc2626; font-weight: 500;
  white-space: nowrap; }
[data-theme="dark"] .stp-printed { background: rgba(248, 113, 113, .18); color: #fca5a5; }
/* the thumbnails: tiles that wrap, each the first image, the shelfmark and its signs under it */
.stp-gallery { display: grid; align-content: start; gap: 10px; padding: 10px;
  grid-template-columns: repeat(auto-fill, minmax(min(100%, var(--stp-tw, 180px)), 1fr)); }
.stp-size-s { --stp-tw: 150px; --stp-th: 140px; }
.stp-size-m { --stp-tw: 180px; --stp-th: 160px; }
.stp-size-l { --stp-tw: 280px; --stp-th: 240px; }
.stp-tile { position: relative; display: flex; flex-direction: column; gap: 4px; padding: 6px; min-width: 0;
  border: 1px solid var(--border-light, #d5d8de); border-radius: 8px; cursor: pointer; background: var(--bg-primary, #fff); }
.stp-tile:hover { background: var(--bg-tertiary, #f3f5f8); }
.stp-tile.stp-ticked { background: var(--bg-tertiary, #eef3fb); border-color: #1f5fbf; }
.stp-tile.stp-on { outline: 3px solid #1f5fbf; outline-offset: -1px; }
.stp-tile > .stp-box { position: absolute; top: 9px; inset-inline-start: 9px; z-index: 1; border-radius: 4px;
  background: var(--bg-primary, #fff); }
.stp-thumb { height: var(--stp-th, 160px); display: flex; align-items: center; justify-content: center; overflow: hidden;
  border-radius: 6px; background: var(--bg-tertiary, #f3f5f8); }
.stp-thumb img { max-width: 100%; max-height: 100%; object-fit: contain; opacity: 0; transition: opacity .2s; }
.stp-thumb.stp-loaded img { opacity: 1; }
.stp-thumb .stp-ph { display: none; font-size: 32px; color: var(--text-secondary, #5b6270); }
.stp-thumb.stp-noimg img { display: none; }
.stp-thumb.stp-noimg .stp-ph { display: block; }
.stp-tcap { display: flex; align-items: center; gap: 4px; min-width: 0; font-size: 13px; }
/* the start of the title, two lines kept for it in every tile so the rows stay even */
.stp-ttitle { font-size: 12px; line-height: 1.3; min-height: 2.6em; color: var(--text-secondary, #5b6270);
  display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
  overflow-wrap: anywhere; text-align: start; }
.stp-tcap > .stp-c-name { flex: 1 1 auto; direction: ltr; unicode-bidi: isolate; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; min-width: 0; }
"""


def _attr(text: str) -> str:
    return html.escape(text or '', quote=True)


def _name(text: str, more: Optional[str] = None):
    """A name as written: its own direction (a Latin "T-S Ar." in a Hebrew page keeps its dot at the
    end), and the whole of it on hover, with ``more`` (a Part, the other language's name), since a
    long one is cut."""
    lab = ui.label(text).props('dir="auto"')
    lab.tooltip(f"{text} · {more}" if more else text)
    return lab


def _leaf_name(row: dict, marks: tuple = ()):
    """A shelfmark's name, its tooltip the shelfmark (with its Part or other call numbers) and the
    manuscript's title on the next line (DX24, ``leaf_tooltip``), then the signs it has: the line
    breaks kept."""
    text, hint = row['label'], row.get('hint')
    tip = leaf_tooltip({'hint': f"{text} · {hint}" if hint else text, 'title': row.get('title') or ''})
    if marks:
        tip += '\n' + signs_text(marks, tr)
    lab = ui.label(text).props('dir="auto"')
    with lab:
        ui.tooltip(tip).classes('stp-tip')
    return lab


def _signs_of(sys_id: str) -> tuple:
    """The signs of a manuscript's row (``shared.shelfmark_tree_signs``), none until they are loaded:
    a row is drawn at once, without them, and again when they come."""
    try:
        signs = shelf_signs()
        return tuple(signs.of(sys_id)) if signs.ready else ()
    except Exception:   # noqa: BLE001 -- a row without its signs rather than no row
        return ()


def _sign_marks(marks: tuple) -> None:
    """The small marks at the row's end: PGP's badge and a scholarly transcription's icon as the
    search's results draw them (``web/pages/search_results.py``), a link for a join."""
    if not marks:
        return
    with ui.element('span').classes('stp-signs').props('data-signs="' + ' '.join(marks) + '"'):
        for mark in marks:
            name = tr(SIGN_LABELS[mark])
            if mark == 'pgp':
                ui.label('PGP').classes('stp-sign-pgp').props(f'role="img" aria-label="{_attr(name)}"')
            else:
                ui.icon('description' if mark == 'edition' else 'link').classes(
                    f'stp-sign-icon stp-sign-{mark}').props(f'role="img" aria-label="{_attr(name)}"')


def _places_of(service, sys_ids) -> dict:
    """Where each of these manuscripts is on the shelf, ``{sys_id: ShelfLeaf}`` (its first place);
    one with none is left out. The pane's joins: a partner with a place is a link to it and can be
    ticked. Blocking: call it off the loop."""
    out = {}
    for sys_id in sys_ids:
        if sys_id and sys_id not in out:
            try:
                place = place_of(service, sys_id)
            except Exception:   # noqa: BLE001 -- a partner shown as plain text
                place = None
            if place is not None:
                out[sys_id] = place.leaf
    return out


def _pane_info(service, leaf, lang: str, meta, pages_of, texts_of):
    """The pane's contents (``item_details``) and the places of its join partners. Off the loop."""
    info = item_details(service, leaf, lang, meta_mgr=meta, page_count=pages_of, page_texts=texts_of)
    return info, _places_of(service, [j.get('sys_id') for j in (info or {}).get('joins') or ()])


def _community_and_places(service, sys_id: str):
    """``_community`` and the places of the partners it names. Off the loop."""
    partners, comments = _community(sys_id)
    return partners, comments, _places_of(service, [p[0] for p in partners])


def _meta_mgr():
    """The site's catalogue (titles, thumbnails), or None before it is loaded."""
    try:
        from web.state import state as app_state
        return getattr(app_state, 'meta_mgr', None)
    except Exception:   # noqa: BLE001 -- the pane and the tooltips without it
        return None


def _title_of(sys_id: str) -> str:
    """A manuscript's title in ``libraries.csv`` ('' when unknown): the shelfmarks' tooltips (DX24)."""
    meta = _meta_mgr()
    try:
        row = meta.csv_bank.get(sys_id) if meta is not None else None
        return (row.get('title') or '') if row else ''
    except Exception:   # noqa: BLE001 -- a row without its title
        return ''


def _thumbnail_src(sys_id: str, shelfmark: str, width: int = 320) -> str:
    """The manuscript's first image, small, by the site's own image routes (``/api/nli_image_by_sysid``,
    Oxford's, a provider's while NLI is down: ``web/components/candidate_grid.py::build_thumbnail_url``),
    never NLI's own address, as every thumbnail on the site; '' when there is none to ask for. Whether it
    exists is known when it loads: one that fails is hidden."""
    try:
        from web.components.candidate_grid import build_thumbnail_url
        return build_thumbnail_url(sys_id, 1, shelfmark or '', _code_of(sys_id), width=width) or ''
    except Exception:   # noqa: BLE001 -- the pane without its image
        return ''


# -- the table and the thumbnails (shared/shelfmark_tree_rows.py, VX1-VX9) ------------------------

#: Each thumbnail size's image width, as the Joins Lab fetches its cards' (``candidate_grid.CARD_SIZES``).
_THUMB_WIDTH = {'s': 300, 'm': 300, 'l': 600}
#: The switch's buttons: a view, its icon, what the button does.
_VIEW_BUTTONS = (('list', 'view_list', 'Show as a list'), ('table', 'table_rows', 'Show as a table'),
                 ('gallery', 'grid_view', 'Show as thumbnails'))


def _view_choice() -> tuple:
    """The reader's view and thumbnail size (``safe_storage``), the list and medium when none is kept."""
    try:
        return read_choice(safe_user_get(STORAGE_KEY))
    except Exception:   # noqa: BLE001 -- the default
        return read_choice(None)


def _icon_html(name: str, classes: str, label: str = '') -> str:
    aria = f' role="img" aria-label="{_attr(label)}"' if label else ' aria-hidden="true"'
    return f'<i class="q-icon notranslate material-icons {classes}"{aria}>{html.escape(name)}</i>'


def _signs_html(marks: tuple) -> str:
    """The row's signs as HTML, as ``_sign_marks`` draws them (a tile, a table's shelfmark cell)."""
    out = []
    for mark in marks:
        name = tr(SIGN_LABELS[mark])
        if mark == 'pgp':
            out.append(f'<span class="stp-sign-pgp" role="img" aria-label="{_attr(name)}">PGP</span>')
        else:
            out.append(_icon_html('description' if mark == 'edition' else 'link', f'stp-sign-icon stp-sign-{mark}', name))
    return f'<span class="stp-signs" data-signs="{_attr(" ".join(marks))}">{"".join(out)}</span>' if out else ''


def _leaf_tip(row: dict, marks: tuple) -> str:
    """A shelfmark's tooltip, as the list's (``_leaf_name``): its name and other call numbers, its
    title, its signs."""
    text, hint = row['label'], row.get('hint')
    tip = leaf_tooltip({'hint': f"{text} · {hint}" if hint else text, 'title': row.get('title') or ''})
    return tip + ('\n' + signs_text(marks, tr) if marks else '')


def _header_html(multi: bool = False) -> str:
    """The table's header cells (the desktop results table's words, ``COLUMNS``; its tooltips), after a
    blank one over the tick boxes in the multiple choice, so each heading stands over its column."""
    cells = ['<span class="stp-c" data-head="box"></span>'] if multi else []
    for key, label in COLUMNS:
        tip = HEADER_TIPS.get(key)
        title = f' title="{_attr(tr(tip))}"' if tip else ''
        align = ' stp-c-num' if key == 'pages' else (' stp-c-mark' if key in ('printed', 'pgp', 'edition') else '')
        cells.append(f'<span class="stp-c{align}" data-head="{key}"{title}>{html.escape(tr(label))}'
                     '<span class="stp-rsz" aria-hidden="true"></span></span>')
    return ''.join(cells)


def _cells_html(row: dict, det: Optional[dict], marks: tuple, lang: str) -> str:
    """A table row's cells: the shelfmark (its join sign beside it), then ``row_details``' title,
    snippet, domain (the first, "+N" naming the rest), pages, printed, PGP and edition, each blank while
    not known. PGP and the edition are the row's signs until the details come (the same rules)."""
    det = det or {}
    name = html.escape(row['label'])
    join = _icon_html('link', 'stp-sign-icon stp-sign-join', tr(SIGN_LABELS['join'])) if 'join' in marks else ''
    cells = [f'<span class="stp-c stp-c-sm" data-cell="shelfmark" title="{_attr(_leaf_tip(row, marks))}">'
             f'<span class="stp-c-name">{name}</span>{join}</span>']
    title = det.get('title') or ''
    cells.append(f'<span class="stp-c stp-c-clamp" data-cell="title" dir="auto" title="{_attr(title)}">'
                 f'{html.escape(title)}</span>')
    snip = det.get('snippet') or ''
    cells.append(f'<span class="stp-c stp-c-clamp stp-c-snip" data-cell="snippet" dir="auto" title="{_attr(snip)}">'
                 f'{html.escape(snip)}</span>')
    domains = det.get('domains') or []
    dom = ''
    if domains:
        dom = f'<span class="stp-dom" dir="auto">{html.escape(domains[0])}</span>'
        if len(domains) > 1:
            dom += (f'<span class="stp-dom stp-dom-more" title="{_attr(", ".join(domains))}">'
                    f'+{len(domains) - 1}</span>')
    cells.append(f'<span class="stp-c stp-c-dom" data-cell="domain" title="{_attr(", ".join(domains))}">{dom}</span>')
    pages = det.get('pages')
    cells.append(f'<span class="stp-c stp-c-num" data-cell="pages">{"" if pages is None else f"{pages:,}"}</span>')
    printed = ''
    if det.get('printed'):
        from shared.fjms_service import PRINTED_LABEL_EN, PRINTED_LABEL_HE
        printed = f'<span class="stp-printed">{html.escape(PRINTED_LABEL_HE if lang == "he" else PRINTED_LABEL_EN)}</span>'
    cells.append(f'<span class="stp-c stp-c-mark" data-cell="printed">{printed}</span>')
    pgp = det.get('pgp') if det.get('pgp') is not None else ('pgp' in marks)
    cells.append('<span class="stp-c stp-c-mark" data-cell="pgp">'
                 + (f'<span class="stp-sign-pgp" role="img" aria-label="{_attr(tr(SIGN_LABELS["pgp"]))}">PGP</span>'
                    if pgp else '') + '</span>')
    edition = det.get('edition') if det.get('edition') is not None else ('edition' in marks)
    cells.append('<span class="stp-c stp-c-mark" data-cell="edition">'
                 + (_icon_html('description', 'stp-sign-icon stp-sign-edition', tr(SIGN_LABELS['edition']))
                    if edition else '') + '</span>')
    return ''.join(cells)


def _tile_html(row: dict, det: Optional[dict], thumb: Optional[str], marks: tuple) -> str:
    """A thumbnail's tile: the image (its address in ``data-src``, set by ``_THUMBS_JS`` once the tile
    is in sight; a placeholder when there is none or it fails), the shelfmark and its signs under it,
    then the start of the manuscript's title, two lines, readable at a glance (owner, 2026-11-22; the
    row's title from the catalogue, at once, else the details' when they come)."""
    title = row.get('title') or (det or {}).get('title') or ''
    tip = _leaf_tip({**row, 'title': title}, marks)
    if thumb:
        img = f'<img class="stp-lazy" data-src="{_attr(thumb)}" alt="{_attr(row["label"])}">'
        box = 'stp-thumb'
    else:
        img, box = '', 'stp-thumb' + (' stp-noimg' if thumb == '' else '')
    return (f'<div class="{box}" data-thumb="{"1" if thumb else ""}">{img}'
            f'{_icon_html("image_not_supported", "stp-ph")}</div>'
            f'<div class="stp-tcap" title="{_attr(tip)}"><span class="stp-c-name">{html.escape(row["label"])}</span>'
            f'{_signs_html(marks)}</div>'
            f'<div class="stp-ttitle" dir="auto" data-cell="title">{html.escape(title)}</div>')


#: The table's columns resized by their headings' grips (owner, 2026-11-22): the widths, in pixels, become
#: the table's --stp-tcols in a style of their own (a server update of the table never wipes it), kept in
#: this browser for the next time; a double click on a grip gives the columns back their first widths.
_RESIZE_JS = """
(() => {
  const KEY = 'stpTableCols';
  const put = (cols) => {
    let el = document.getElementById('stp-tcols-style');
    if (!el) { el = document.createElement('style'); el.id = 'stp-tcols-style'; document.head.appendChild(el); }
    el.textContent = cols ? `.stp-table { --stp-tcols: ${cols}; } `
      + `.stp-table > * { width: max-content; min-width: max(860px, 100%); }` : '';
  };
  if (window.__stpResize) { return; }
  window.__stpResize = true;
  try { const kept = localStorage.getItem(KEY); if (kept) { put(kept); } } catch (e) { /* no storage */ }
  document.addEventListener('pointerdown', (e) => {
    const grip = e.target.closest && e.target.closest('.stp-rsz');
    if (!grip) { return; }
    e.preventDefault();
    e.stopPropagation();
    const head = grip.closest('.stp-thead');
    const cells = [...head.querySelectorAll('[data-head]:not([data-head="box"])')];
    const widths = cells.map((c) => c.getBoundingClientRect().width);
    const i = cells.indexOf(grip.parentElement);
    const rtl = getComputedStyle(head).direction === 'rtl';
    const x0 = e.clientX, w0 = widths[i];
    const cols = () => widths.map((w) => Math.round(w) + 'px').join(' ');
    const move = (ev) => { widths[i] = Math.max(40, w0 + (ev.clientX - x0) * (rtl ? -1 : 1)); put(cols()); };
    const up = () => {
      document.removeEventListener('pointermove', move);
      document.removeEventListener('pointerup', up);
      try { localStorage.setItem(KEY, cols()); } catch (err) { /* kept while the page is open */ }
    };
    document.addEventListener('pointermove', move);
    document.addEventListener('pointerup', up);
  }, true);
  document.addEventListener('dblclick', (e) => {
    if (!(e.target.closest && e.target.closest('.stp-rsz'))) { return; }
    put('');
    try { localStorage.removeItem(KEY); } catch (err) { /* nothing kept */ }
  }, true);
})();
"""


#: The thumbnails load only once in sight, at most four at a time (each asks the site's image route,
#: which may ask NLI): the tiles in sight are queued, one that leaves the sight before its turn is
#: dropped, a loaded image is shown, a failed one makes way for the placeholder.
_THUMBS_JS = """
(() => {
  if (!window.__stpThumbs) {
    const MAX = 4; let active = 0; const seen = new Set();
    const pump = () => {
      for (const img of [...seen]) {
        if (active >= MAX) { break; }
        seen.delete(img);
        if (!document.body.contains(img) || !img.dataset.src || img.getAttribute('src')) { continue; }
        active += 1;
        const box = img.closest('.stp-thumb');
        const done = (ok) => { active -= 1; if (box) { box.classList.add(ok ? 'stp-loaded' : 'stp-noimg'); } pump(); };
        img.addEventListener('load', () => done(true), {once: true});
        img.addEventListener('error', () => done(false), {once: true});
        img.setAttribute('src', img.dataset.src);
      }
    };
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => { if (e.isIntersecting) { seen.add(e.target); } else { seen.delete(e.target); } });
      pump();
    }, {rootMargin: '150px'});
    window.__stpThumbs = {scan: () => document.querySelectorAll('img.stp-lazy[data-src]:not([data-watched])')
      .forEach((img) => { img.dataset.watched = '1'; io.observe(img); })};
  }
  setTimeout(() => window.__stpThumbs.scan(), 30);
})();
"""


def _page_texts():
    """The text index's pages of a manuscript in order, for the pane's few lines of text; None before
    the index is ready. Read off the loop."""
    try:
        from web.state import state as app_state
        searcher = getattr(app_state, 'searcher', None)
        if not app_state.is_ready() or searcher is None or not hasattr(searcher, 'page_texts'):
            return None
        return searcher.page_texts
    except Exception:   # noqa: BLE001 -- the pane without a preview
        return None


def _community(sys_id: str):
    """What only the community database knows of a manuscript, for the pane: its joins, as partners
    ``(sys_id, shelfmark)``, and how many public comments it has (None: not known). Blocking network
    reads through the anonymous public client, which reads no per-user storage: call it off the loop."""
    partners, comments = [], None
    try:
        from web.supabase_client import get_fragment_joins, get_public_read_client
    except Exception:   # noqa: BLE001 -- no community database here
        return partners, comments
    try:
        for row in get_fragment_joins(fragment_sys_id=sys_id) or ():
            if str(row.get('fragment_a_sys_id') or '') == sys_id:
                partners.append((str(row.get('fragment_b_sys_id') or ''), row.get('fragment_b_shelfmark') or ''))
            else:
                partners.append((str(row.get('fragment_a_sys_id') or ''), row.get('fragment_a_shelfmark') or ''))
    except Exception:   # noqa: BLE001
        logger.debug("shelf picker: no community joins for %s", sys_id, exc_info=True)
    try:
        found = (get_public_read_client().table('comments').select('id', count='exact', head=True)
                 .eq('sys_id', sys_id).eq('is_public', True).execute())
        comments = int(found.count) if found.count is not None else None
    except Exception:   # noqa: BLE001
        logger.debug("shelf picker: no comment count for %s", sys_id, exc_info=True)
    return partners, comments


def _page_count():
    """The text index's pages of a manuscript, for the pane; None (not said) before the index is ready."""
    try:
        from web.services import get_service
        svc = get_service()
        return svc.page_count if getattr(svc, 'is_ready', False) else None
    except Exception:   # noqa: BLE001
        return None


#: The sources of a join, as the pane names them (``shared.shelfmark_tree_details.JOIN_SOURCES``).
_JOIN_SOURCES = {'pgp': 'PGP', 'fjms': 'FJMS', 'community': 'Community'}

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
#: A row brought into sight within its own list only: ``scrollIntoView`` scrolled every box around it too,
#: and on a narrow screen (the columns one under the other, in a box that scrolls) the pane's header --
#: the view switch, the filter -- went out of sight above (owner, 2026-11-22).
_IN_LIST_JS = """
window.__stpInList = window.__stpInList || ((el, center) => {
  const list = el && el.closest('.stp-list');
  if (!list) { return; }
  const r = el.getBoundingClientRect(), l = list.getBoundingClientRect();
  if (center) { list.scrollTop += (r.top - l.top) - (l.height - r.height) / 2; }
  else if (r.top < l.top) { list.scrollTop += r.top - l.top; }
  else if (r.bottom > l.bottom) { list.scrollTop += r.bottom - l.bottom; }
});
"""
_RESTORE_SCROLL_JS = _IN_LIST_JS + """
setTimeout(() => document.querySelectorAll('.stp-list[data-col]').forEach((el) => {
  const v = (window.__stpScroll || {})[el.dataset.col];
  if (v) { el.scrollTop = v; return; }
  const on = el.querySelector('.stp-on');               // first shown: the chosen row in sight
  if (on) { window.__stpInList(on, false); }
}), 30);
"""
#: The shelfmarks are in as many columns as show every one of them whole: a column is as wide as the
#: longest shelfmark shown (three columns, else two, else one; owner, 2026-11-20). Measured in the
#: browser, where the font is known; a phone keeps its one column (the CSS does not read the width).
_FIT_GRID_JS = """
setTimeout(() => document.querySelectorAll('.stp-grid').forEach((grid) => {
  let widest = 0;
  grid.querySelectorAll('.stp-row > .stp-sm').forEach((el) => {
    const box = el.previousElementSibling;           // a tick box (multiple choice) takes its room too
    const signs = el.nextElementSibling;             // ... and so do the row's signs
    widest = Math.max(widest, el.scrollWidth + (box ? box.offsetWidth + 8 : 0) + (signs ? signs.offsetWidth + 8 : 0));
  });
  if (widest) { grid.style.setProperty('--stp-leaf', Math.max(140, widest + 24) + 'px'); }
}), 30);
"""


#: The chips the bar draws; the rest are only behind "+N more" (D11).
CHIPS_DRAWN = 40
_BOX = {'all': 'check_box', 'some': 'indeterminate_check_box', 'none': 'check_box_outline_blank'}
_ARIA = {'all': 'true', 'some': 'mixed', 'none': 'false'}

#: The chips bar shows the chips that fit on its one line and "+N more" for the rest (D11): chips are
#: hidden from the last while the bar overflows, in either direction of writing; measured again when
#: the window is resized.
_FIT_CHIPS_JS = """
(() => {
  const bar = document.querySelector('[data-chips="%s"]');
  if (!bar) { return; }
  const fit = () => {
    if (!document.body.contains(bar)) { window.removeEventListener('resize', bar.__stpFit); return; }
    const chips = [...bar.querySelectorAll('.stp-chip')];
    const more = bar.querySelector('.stp-more');
    const extra = parseInt(bar.dataset.extra || '0', 10);
    chips.forEach((c) => { c.style.display = ''; });
    let hidden = 0;
    const label = () => {
      if (!more) { return; }
      const n = hidden + extra;
      more.style.display = n ? 'inline-flex' : 'none';
      more.textContent = (more.dataset.word || '+{n}').replace('{n}', n);
    };
    label();
    for (let i = chips.length - 1; i >= 0 && bar.scrollWidth > bar.clientWidth + 1; i--) {
      chips[i].style.display = 'none';
      hidden += 1;
      label();
    }
  };
  if (!bar.__stpFit) { bar.__stpFit = fit; window.addEventListener('resize', fit); }
  setTimeout(fit, 30);
})();
"""


def _isolated(name: str) -> str:
    """A name set inside a sentence of the other direction (a Latin "Schoeyen" in "בחירת הכל ב־..."):
    isolated (FSI ... PDI), so the sentence keeps its own direction and the count after it its place
    (owner, 2026-11-21: it was not right to left in Hebrew)."""
    return f"\u2068{name}\u2069" if name else name


def _apply_change(fn, selection: Selection, service, track: bool = False, limit=None) -> tuple:
    """A change to the selection, off the loop (what a node holds is read from the file):
    (changed, the manuscripts it made chosen -- worked out only with ``track``, while a confirm is
    with the host -- the ``Refusal`` of ``limit`` when the change adds past it, the selection then
    left for the caller to drop (a removal always passes), and the chosen sets before and after, for
    the caller to ask the limit again as it commits)."""
    members_of = members_function(service)
    before = selection.chosen_set(members_of) if (track or limit is not None) else None
    changed = bool(fn(selection, members_of))
    if not changed:
        return False, set(), None, None, None
    after = selection.chosen_set(members_of) if (track or limit is not None) else None
    if limit is not None:
        refused = limit.refusal(before, after)
        if refused is not None:
            return False, set(), refused, None, None
    return True, (after - before) if track else set(), None, before, after


def _code_of(sys_id: str) -> str:
    """A manuscript's own library code (``libraries.csv``), which names its chip: a library node may
    hold several codes (Penn's "Katz" and "UPenn"; Codex on #80). '' when the catalogue is not loaded."""
    try:
        from web.state import state as app_state
        meta = getattr(app_state, 'meta_mgr', None)
        return (meta.get_library_for_id(sys_id) or '') if meta is not None else ''
    except Exception:   # noqa: BLE001 -- named by its library node instead
        return ''


def _chip_name(service, lang: str):
    """A chip's name with its library's code first ("CUL T-S 32"; owner, 2026-11-21)."""
    def name_of(chip: Chip) -> str:
        return chip_name(service, chip, lang, code_of=_code_of)
    return name_of


def _selection_state(service, selection: Selection, node_ids: tuple, list_node: Optional[str], lang: str) -> dict:
    """What the multiple-choice parts show, read off the loop from a copy of the selection: the
    manuscripts chosen, the chips with their counts, the state of each drawn node's box and of the
    "Select all" over the shelfmarks shown. A box shows what its click acts on: the node's
    manuscripts, wherever they were chosen (a catalogue's list of a chosen volume is ticked too,
    and its click unticks them) -- never a guess from shelf positions (Codex, PR 4)."""
    members_of = members_function(service)
    name_of = _chip_name(service, lang)
    chosen = selection.chosen_set(members_of)
    chips = [{'key': chip.key, 'label': name_of(chip), 'node': chip.kind == NODE,
              'count': selection.chip_count(chip, members_of)} for chip in selection.chips]

    def state_of(members: frozenset) -> str:
        hit = members & chosen
        return 'none' if not hit else ('all' if hit == members else 'some')

    nodes = {}
    for nid in node_ids:
        # the drawn rows are read once and not kept: the service keeps the chosen nodes' sets
        nodes[nid] = state_of(service.members_under(nid, keep=False)) if chosen else 'none'
    whole = None
    if list_node:
        node = service.node(list_node)
        if node is not None:
            scope = scope_for(node)
            members = service.members_under(node.id, scope == DIRECT)
            whole = {'id': node.id, 'scope': scope, 'total': len(members), 'name': display_name(node, lang),
                     'state': state_of(members) if chosen else 'none'}
    return {'chosen': frozenset(chosen), 'count': len(chosen), 'chips': chips, 'nodes': nodes, 'whole': whole}


def _resolve(service, selection: Selection, lang: str):
    """What a confirm hands the host: the entries, in the order chosen, and a label naming the chips."""
    return selection.resolve_entries(service), selection.summary(_chip_name(service, lang))


#: The list chooser's choice of a new list (Add to List, several at once).
_NEW_LIST = '__new__'


def _read_lists(lists_mgr, context) -> dict:
    """In a worker thread: the reader's lists and projects, one read of the database. ``context``:
    ``(authed, user_id, client)`` resolved on the event loop (``UserListsManager._worker_context``):
    a worker cannot read the visitor's storage, so the manager's own ``data`` would answer for a
    signed-out visitor there. None for a manager without one (a test's), whose ``data`` is read."""
    if context is not None:
        authed, user_id, client = context
        return lists_mgr._get_cached_data(client=client, user_id=user_id, is_authenticated=authed) or {}
    return getattr(lists_mgr, 'data', None) or {}


async def _load_list_options(lists_mgr) -> dict:
    """The reader's own lists for Add to List (``_list_options``), read off the event loop: the
    manager's ``data`` is a fresh database read, and a slow answer on the loop would hold every
    reader on the server (Codex on #88). ``{}`` (a new list only) when the read fails."""
    try:
        worker_context = getattr(lists_mgr, '_worker_context', None)
        context = worker_context() if callable(worker_context) else None
        data = await run.io_bound(_read_lists, lists_mgr, context)
    except Exception:   # noqa: BLE001 -- a new list only
        return {}
    return _list_options(data)


def _list_options(data) -> dict:
    """The reader's own lists a choice can be added to, ``{list_id: name}`` (not the system lists,
    among them the recent list the site fills by itself), as the site's own Add to List offers them,
    from the lists ``data`` read once (``_load_list_options``): asking the manager about each list
    would read them all again from the database, once per list (Codex on #88). On the loop: the
    names are translated for the reader."""
    try:
        lists = (data or {}).get('lists', {}) or {}
    except Exception:   # noqa: BLE001 -- a new list only
        return {}
    try:
        from web.user_lists import localize_list_name
    except Exception:   # noqa: BLE001
        def localize_list_name(lst):
            return lst.get('name', '')
    out = {}
    for lid, lst in lists.items():
        if not isinstance(lst, dict) or lst.get('is_system'):
            continue
        out[str(lid)] = localize_list_name(lst) or str(lid)
    return out


def open_shelf_picker(container, service, on_pick: Optional[Callable[[dict], Any]] = None,
                      start: Optional[str] = None, title: Optional[str] = None, *, mode: str = 'single',
                      purpose: str = '', on_confirm: Optional[Callable[[list, str], Any]] = None,
                      confirm_label: Optional[str] = None, lists_mgr=None,
                      switch_label: Optional[str] = None, limit=None, pick_label: Optional[str] = None,
                      add_to: bool = False, node_boxes: bool = False):
    """Open the dialog.

    Single choice (``mode='single'``; handoff DX21): a click on a shelfmark selects it, and the foot's
    button, ``pick_label`` (by default "Select"; "Open" where the pick opens the manuscript), calls
    ``on_pick({sys_id, shelfmark, placement_id})`` (awaited if async) and closes the dialog. A double
    click, or Enter on the selected row, is that button.

    ``add_to`` (DX22, the hosts whose pick opens a manuscript): "Add to List" and "Add to Puzzle"
    beside that button, in both modes, on the selected shelfmark or the ticked ones. Add to List: the
    site's list chooser over the picker, which stays open (several: the picker's own, which adds to a
    list or makes one); Add to Puzzle: the Puzzle with them added (up to its 30), the picker closed.
    In the multiple choice both leave the ticks as they are: only the confirm takes them.

    Multiple choice (``mode='multi'``, plan §10.13): shelfmarks and whole nodes are ticked into a
    ``shared.shelfmark_tree_selection.Selection`` kept per reader and ``purpose`` (D12,
    ``web/shelf_selection_store.py``) until it is applied or cleared; the chips bar shows it (D11).
    The confirm button calls ``on_confirm(entries, label)`` (awaited if async) with the entries
    ``[{sys_id, shelfmark, placement_id}]`` in the order chosen and a label naming the chips. It
    answers None when it took them all, else the ids it took (an empty list: none); what it did not
    take stays chosen and the dialog stays open. ``lists_mgr``: offer "Save as a list".

    ``switch_label``: a switch in the header between the two modes, in this same dialog and at the
    same place (Browse's "Open more than one", handoff DX16); ``mode`` is the one it opens in, and
    the host gives what both need (``on_pick``, and ``purpose`` with ``on_confirm``). Not flipped
    while a confirm or a save is under way.

    ``limit``: a ``shared.shelfmark_tree_limits.SelectionLimit`` asked before a tick is kept (owner,
    2026-11-21): a tick that adds past it is not kept and the reader is told the limit; a node too big
    for it is drawn faded and its box tells so instead of ticking (a ticked one unticks).

    ``node_boxes``: a box on every node below the libraries in the multiple choice, which ticks the
    whole node (the search's exclusions, where a whole collection is what is wanted). Elsewhere a
    whole node is taken only by "Select all in <node>" over its shelfmarks (DX23)."""
    multi = mode == 'multi'
    both = bool(switch_label)                       # the switch: either mode, flipped in place
    if (multi or both) and (not purpose or on_confirm is None):
        raise ValueError("the multiple-choice mode needs a purpose and on_confirm")
    if both and on_pick is None:
        raise ValueError("the switch needs on_pick for its single choice")
    lang = get_language()
    labels = {'library': tr('Library'), 'others': tr('All other libraries, A-Z'),
              'in': tr('Shown inside {place}'), 'part': tr('Part: {part}')}
    st = {'path': (), 'filters': {}, 'shown': {}, 'highlight': None, 'parts': {}, 'owed': set(), 'list_node': None,
          'ready': False}
    ms = {'sel': Selection(), 'owner': '', 'state': None, 'sgen': 0, 'ticks': {}, 'boxes': {}, 'whole': None,
          'busy': False, 'warned': False, 'since': None, 'big': {}, 'jticks': {}}
    lock = asyncio.Lock()
    if multi or both:
        ms['owner'] = store.current_owner()
        ms['sel'] = store.load_selection(purpose, ms['owner'])
    can_save = bool((multi or both) and lists_mgr is not None and GlobalAuthState.is_logged_in())
    # the single choice's selected shelfmark (DX21), and the pane's shelfmark and its generation (DX25)
    one = {'leaf': None, 'rows': {}}
    # ... the pane's contents as shown, the places of its join partners and the holders filled again
    pane = {'gen': 0, 'leaf': None, 'info': None, 'places': {}, 'lines': None, 'joins': None}
    # the shelfmarks pane's view (VX1, kept per reader), the details of the rows (by sys_id, this dialog's
    # language), the thumbnails' addresses (by width, worked out again at each whole drawing), the HTML
    # each row's cells are drawn in, and the one loader (VX5)
    view0, size0 = _view_choice()
    vw = {'view': view0, 'size': size0, 'rows': {}, 'thumbs': {}, 'cells': {}, 'loading': False, 'again': False}
    words = {'single': pick_label or tr('Select'), 'multi': confirm_label or tr('Done')}
    switch = None

    with container:
        ui.add_css(_CSS)
        with ui.dialog() as dialog, ui.card().classes('stp-card').props(
                f'dir={"rtl" if is_rtl() else "ltr"} data-testid="shelf-picker"'):
            with ui.row().classes('w-full items-center justify-between no-wrap'):
                h3(title or tr('Pick from the shelf'), classes='text-lg font-semibold m-0')
                ui.element('div').style('flex: 1 1 auto;')
                if both:
                    switch = ui.switch(switch_label, value=multi, on_change=lambda e: flip(e)).props(
                        'dense color=primary data-testid="shelf-picker-switch"').classes('stp-switch')
                ui.button(icon='close', on_click=dialog.close).props(
                    f'flat round dense aria-label="{tr("Close")}"')
            crumbs_row = ui.element('div').classes('stp-crumbs w-full')
            chips_row = None
            if multi or both:
                chips_row = ui.element('div').classes('stp-chips w-full').props(f'data-chips="{dialog.id}"')
            with ui.element('div').classes('stp-main w-full'):
                body = ui.element('div').classes('stp-body')
                # not a live region: it holds a manuscript's whole text, read only when the reader goes to it
                details = ui.element('div').classes('stp-details').props('role="region" data-testid="shelf-details"')
                details.props['aria-label'] = tr('Details')
            count_label = clear_btn = list_btn = puzzle_btn = None
            with ui.element('div').classes('stp-foot w-full'):
                if multi or both:
                    count_label = ui.label('').classes('stp-total').props('aria-live="polite"')
                ui.element('div').style('flex: 1 1 auto;')
                if multi or both:
                    clear_btn = ui.button(tr('Clear'), icon='delete_sweep', on_click=lambda: clear_all()).props(
                        'flat dense no-caps').classes('stp-clear')
                if add_to or can_save:
                    list_btn = ui.button(tr('Add to List'), icon='playlist_add', on_click=lambda: add_to_list()).props(
                        'flat dense no-caps').classes('stp-save')
                if add_to:
                    puzzle_btn = ui.button(tr('Add to Puzzle'), icon='extension', on_click=lambda: add_to_puzzle()).props(
                        'flat dense no-caps').classes('stp-puzzle')
                confirm_btn = ui.button(words['multi' if multi else 'single'], icon='check',
                                        on_click=lambda: confirm() if multi else pick_selected()).props(
                    'unelevated no-caps color=primary data-testid="shelf-picker-confirm"').classes('stp-confirm')

    def show_mode():
        """What each mode shows: the chips, the count and Clear in the multiple choice; the confirm's
        word; the list button in the single choice only where it adds (``add_to``)."""
        if chips_row is not None:
            chips_row.set_visibility(multi)
        for el in (count_label, clear_btn):
            if el is not None:
                el.set_visibility(multi)
        if list_btn is not None:
            list_btn.set_visibility(multi or add_to)
        confirm_btn.set_text(words['multi' if multi else 'single'])
        enable_actions()

    def enable_actions():
        """The foot's buttons act on something: the selected shelfmark (single), the ticked ones (multi)."""
        if multi:
            on = bool((ms['state'] or {}).get('count', 0)) and not ms['busy']
        else:
            on = one['leaf'] is not None
        for btn in (confirm_btn, list_btn, puzzle_btn):
            if btn is None:
                continue
            if on:
                btn.enable()
            else:
                btn.disable()

    async def set_mode(want: bool) -> bool:
        """The other mode, in this dialog and at the same place: by the header's switch, or by a join's
        box in the pane, which turns the multiple choice on. Not while a confirm or a save is under
        way (False)."""
        nonlocal multi
        if want == multi:
            return True
        if ms['busy']:
            return False
        multi = want
        if switch is not None and bool(switch.value) != want:
            switch.value = want                     # its handler finds the mode already set
        show_mode()
        if pane['info'] is not None and pane['joins'] is not None and not pane['joins'].is_deleted:
            fill_joins(pane['info'])                # the joins' boxes as this mode has them
        if st['ready']:                             # still opening: its first drawing reads the mode
            await draw()
        return True

    async def flip(event):
        """The header's switch."""
        if not await set_mode(bool(event.value)):
            event.sender.value = multi              # a confirm or a save is under way: it stays as it is

    async def pick(leaf):
        if multi:                                   # a row drawn before the switch, clicked during the redraw
            return
        dialog.close()
        outcome = on_pick(leaf.as_selection())
        if inspect.isawaitable(outcome):
            await outcome

    def mark_selected(leaf_id: Optional[str]):
        """The selected row shown chosen (``stp-on``, ``aria-selected``), the one before not."""
        for lid, rows in list(one['rows'].items()):
            alive = [r for r in rows if not r.is_deleted]
            if not alive:
                del one['rows'][lid]
                continue
            one['rows'][lid] = alive
            for row in alive:
                if lid == leaf_id:
                    row.classes(add='stp-on')
                    row.props('aria-selected="true"')
                else:
                    row.classes(remove='stp-on')
                    row.props('aria-selected="false"')

    async def select(leaf):
        """Single choice: a click (Enter, Space) selects the shelfmark; the foot's button acts on it."""
        if multi:
            return
        if one['leaf'] is not None and one['leaf'].id == leaf.id and pane['leaf'] is not None \
                and pane['leaf'].id == leaf.id:
            return                                  # selected and shown already (a double click's second)
        one['leaf'] = leaf
        st['highlight'] = leaf.id                   # kept by a redraw
        mark_selected(leaf.id)
        enable_actions()
        await show_details(leaf)

    async def enter(leaf):
        """Enter on the selected row is the button; on another row it selects that one."""
        if not multi and one['leaf'] is not None and one['leaf'].id == leaf.id:
            await pick_selected()
        else:
            await select(leaf)

    async def double(leaf):
        """A double click is the button, for the row clicked."""
        if multi:
            return
        one['leaf'] = leaf
        await pick_selected()

    async def pick_selected():
        if multi or one['leaf'] is None:
            return
        await pick(one['leaf'])

    # -- the pane: the shelfmark clicked last (DX25) ------------------------------------------

    def draw_empty_pane():
        details.clear()
        with details:
            ui.label(tr('Choose a shelfmark to see it here')).classes('stp-hint')

    def field(caption: str, key: str):
        """A labelled field of the pane: its label, then what the caller puts in it."""
        el = ui.element('div').classes(f'stp-d-field stp-d-f-{key}').props(f'data-field="{key}"')
        with el:
            ui.label(caption).classes('stp-d-cap')
        return el

    def draw_pane(info: dict):
        """The pane as labelled fields (round 4): the shelfmark (and its other shelfmarks), the title,
        the text, the field lines, the joins and the links, the thumbnail above them. Answers the
        holder of the thumbnail, hidden until the image comes."""
        details.clear()
        with details:
            thumb = ui.element('div').classes('stp-d-thumb')
            thumb.set_visibility(False)
            with ui.element('div').classes('stp-d-text'):
                with field(tr('Shelfmark'), 'sm'):
                    ui.label(info['shelfmark']).classes('stp-d-sm').props(
                        'dir="auto" data-testid="shelf-details-shelfmark"')
                    if info.get('part'):
                        ui.label(labels['part'].format(part=info['part'])).props('dir="auto"')
                if info.get('others'):
                    with field(tr('Other shelfmarks'), 'others').props('data-testid="shelf-details-others"'):
                        for other in info['others']:
                            ui.label(other).props('dir="auto"')
                if info.get('title'):
                    with field(tr('Title'), 'title'):
                        ui.label(info['title']).props('dir="auto" data-testid="shelf-details-title"')
                if info.get('text'):
                    with field(tr('Text'), 'text'):
                        draw_text(info['text'])
                pane['lines'] = ui.element('div').classes('stp-d-lines').props('data-testid="shelf-details-facts"')
                fill_fields(info)
                pane['joins'] = ui.element('div').classes('stp-d-field stp-d-f-joins').props(
                    'data-field="joins" data-testid="shelf-details-joins"')
                fill_joins(info)
                links = [link for link in info.get('links') or () if link.get('url')]
                if links:
                    with field(tr('Links'), 'links').props('data-testid="shelf-details-links"'):
                        with ui.element('div').classes('stp-d-links'):
                            for link in links:
                                ui.link(link.get('label') or link['url'], link['url'], new_tab=True).props(
                                    f'data-link="{_attr(link.get("key") or "")}"')
        return thumb

    def draw_text(pages: list):
        """Every page's text as stored, each under its number: line breaks kept, nothing cut, in a
        read-only box that scrolls (the keyboard too: it takes the focus)."""
        box = ui.element('div').classes('stp-d-textbox').props(
            'tabindex="0" role="region" data-testid="shelf-details-text"')
        box.props['aria-label'] = tr('Text')
        with box:
            for page in pages:
                ui.label(tr('Page {n}').format(n=page.get('page'))).classes('stp-d-page')
                ui.label(page.get('text') or '').classes('stp-d-pagetext').props('dir="auto"')

    def fill_fields(info: dict):
        """The field lines (``field_lines``): each label bold, its value plain, a line's pairs joined
        by " · "; filled again when the community's comments are counted."""
        lines = pane['lines']
        if lines is None or lines.is_deleted:
            return
        lines.clear()
        with lines:
            for key, pairs in field_lines(info, tr):
                text = ' · '.join(f"<b>{html.escape(str(label))}</b>: {html.escape(str(value))}"
                                  for label, value in pairs)
                ui.html(text, sanitize=False).props(f'dir="auto" data-fact="{_attr(key)}"')

    def fill_joins(info: dict):
        """The joins, one row each: a box that chooses the partner as a tick in the list does (in a
        picker with the multiple choice; it turns it on), its shelfmark a link to it on the shelf, and
        its sources. A partner with no id or no place is its shelfmark alone."""
        holder = pane['joins']
        if holder is None or holder.is_deleted:
            return
        holder.clear()
        ms['jticks'] = {}
        joins = info.get('joins') or []
        holder.set_visibility(bool(joins))
        if not joins:
            return
        chosen = ((ms['state'] or {}).get('chosen') or set()) if multi else set()
        with holder:
            ui.label(tr('Joins')).classes('stp-d-cap')
            for join in joins:
                sys_id = join.get('sys_id') or ''
                name = join.get('shelfmark') or sys_id
                placed = pane['places'].get(sys_id) if sys_id else None
                with ui.element('div').classes('stp-d-join').props(f'data-join="{_attr(sys_id or name)}"'):
                    if placed is not None and (multi or both):
                        state = 'all' if sys_id in chosen else 'none'
                        with ui.element('span').classes('stp-node-box stp-d-jbox').props(
                                f'role="checkbox" aria-checked="{_ARIA[state]}" tabindex="0" '
                                f'data-join-box="{_attr(sys_id)}"') as box:
                            icon = ui.icon(_BOX[state]).classes('stp-box')
                        box.props['aria-label'] = tr('Choose {name}').format(name=name)
                        handler = (lambda _e=None, s=sys_id, b=box: tick_partner(s, b))
                        box.on('click', handler).on('keydown.enter', handler).on('keydown.space.prevent', handler)
                        ms['jticks'].setdefault(sys_id, []).append((box, icon))
                    if placed is not None:
                        go_there = (lambda _e=None, s=sys_id: reveal(s))
                        ui.link(name).props('tabindex="0" role="button" dir="auto"').on('click', go_there).on(
                            'keydown.enter', go_there).on('keydown.space.prevent', go_there)
                    else:
                        ui.label(name).props('dir="auto"')
                    sources = [_JOIN_SOURCES.get(s, s) for s in join.get('sources') or ()]
                    if sources:
                        ui.label(', '.join(tr(s) for s in sources)).classes('stp-d-src')

    async def show_details(leaf):
        """The pane shows ``leaf``: its text from local data, then its thumbnail, each off the loop,
        where no NiceGUI or per-user storage call is made. A newer click wins."""
        pane['gen'] = gen = pane['gen'] + 1
        pane['leaf'] = leaf
        meta, pages_of, texts_of = _meta_mgr(), _page_count(), _page_texts()
        info, places = await run.io_bound(_pane_info, service, leaf, lang, meta, pages_of, texts_of)
        if gen != pane['gen'] or details.is_deleted or not info:
            return
        pane['info'], pane['places'] = info, dict(places)
        thumb = draw_pane(info)
        # the tiles' address when they have it (the browser serves it from its cache: no new fetch), else
        # one of the tiles' width, so a gallery drawn later takes it from the cache too (owner, 2026-11-22)
        url = known_thumb_url(leaf.sys_id)
        if url is None:
            url = await run.io_bound(_thumbnail_src, leaf.sys_id, leaf.shelfmark or leaf.label,
                                     _THUMB_WIDTH[vw['size']])
        if gen != pane['gen'] or thumb.is_deleted:
            return
        if url:
            with thumb:
                img = ui.element('img').on('error', lambda _e=None, t=thumb: t.set_visibility(False))  # none: no broken one
            # set as values, not parsed from a props string (which keeps an escaped "&amp;" as it is)
            img.props.update({'src': url, 'alt': info['shelfmark'], 'loading': 'lazy'})
            img.update()
            thumb.set_visibility(True)
        # what only the community database knows, last: a network read
        partners, comments, places = await run.io_bound(_community_and_places, service, leaf.sys_id)
        if gen != pane['gen'] or details.is_deleted:
            return
        add_joins(info, partners, 'community')
        pane['places'].update(places)
        if comments:
            info['comments'] = comments
        fill_fields(info)
        fill_joins(info)

    async def reveal(sys_id: str):
        """A join's link: the dialog goes to that shelfmark, which is shown in its column and
        selected (in the multiple choice: marked), and the pane shows it."""
        st['sel'] = sel = st.get('sel', 0) + 1           # as any navigation: a later one wins
        first = await run.io_bound(start_at, service, sys_id)
        if sel != st['sel'] or dialog.is_deleted or first.leaf is None:
            return
        idx = await run.io_bound(direct_index, service, first.leaf)
        if sel != st['sel'] or dialog.is_deleted:
            return
        # in sight: no filter on the way to it hides it, and its page of the list is drawn
        for parent in (None,) + tuple(first.path):
            st['filters'].pop(f"n:{parent}", None)
        key = f"l:{first.leaf.node_id}"
        st['filters'].pop(key, None)
        st['shown'][key] = max(PAGE_SIZE, ((idx // PAGE_SIZE) + 1) * PAGE_SIZE)
        st['path'] = first.path
        st['highlight'] = first.leaf.id
        one['leaf'] = first.leaf                    # in the multiple choice too: the switch back acts on it
        await draw()
        enable_actions()
        if sel == st['sel'] and not dialog.is_deleted:
            await show_details(first.leaf)

    async def tick_partner(sys_id: str, box):
        """A join's box: the partner chosen, or no longer, as its box in the list would; in the single
        choice it turns the multiple choice on first (and adds)."""
        leaf = pane['places'].get(sys_id)
        if leaf is None:
            return
        on = multi and box.props.get('aria-checked') == 'true'
        if not multi and not await set_mode(True):
            return
        if on:
            await change(lambda s, m, lf=leaf: s.drop_leaf(lf.sys_id, m) if lf.sys_id in s.chosen_set(m) else False)
        else:
            await change(lambda s, m, lf=leaf: s.add_leaf(lf, m) if lf.sys_id not in s.chosen_set(m) else False)

    # -- Add to List, Add to Puzzle (DX22) ----------------------------------------------------

    async def chosen_entries() -> list:
        """What the foot's buttons act on: the selected shelfmark, or the ticked ones in the order
        chosen (read off the loop; the caller checks the mode did not change meanwhile)."""
        if not multi:
            return [one['leaf'].as_selection()] if one['leaf'] is not None else []
        async with lock:
            frozen = ms['sel'].copy()
        if not len(frozen):
            return []
        resolved = await run.io_bound(_resolve, service, frozen, lang)
        return list(resolved[0]) if resolved and resolved[0] else []

    async def add_to_list():
        """Several: the picker's own Add to List (``save_as_list``). One: the site's list chooser,
        over the picker, which stays open."""
        if multi:
            await save_as_list()
            return
        leaf = one['leaf']
        if leaf is None:
            return
        from web.components.add_to_list_dialog import show_add_to_list_dialog
        with container:
            show_add_to_list_dialog(leaf.sys_id, leaf.label or leaf.shelfmark or leaf.sys_id, lists_mgr)

    async def add_to_puzzle():
        """The Puzzle with these added, handed over in this browser
        (``web.joins_lab_storage.stage_fragments_for_puzzle``), up to the Puzzle's limit; the Puzzle
        takes what fits its canvas and says what did not -- one as several, not by ``?add=``, which
        takes it whatever the canvas holds (Codex on #88). The picker closes; the ticks stay."""
        several = multi                             # the mode it was pressed in (Codex: a flip meanwhile)
        if several and (ms['busy'] or not owner_ok()):
            return
        entries = await chosen_entries()
        if not entries or dialog.is_deleted or multi != several:
            return                                  # the switch was flipped while the choice was read
        from web.joins_lab_storage import PUZZLE_STAGING_SOURCE_SHELF, stage_fragments_for_puzzle
        room = limit_for('puzzle')
        ids = [e['sys_id'] for e in entries]
        fits = room.take(ids)
        if len(fits) < len(ids):
            over = room.refusal(fits, list(fits) + [ids[len(fits)]])
            if over is not None:
                ui.notify(refusal_message(over, tr), type='info')
        if not stage_fragments_for_puzzle(fits, source=PUZZLE_STAGING_SOURCE_SHELF):
            ui.notify(tr('The change could not be saved. Check your connection and try again.'), type='negative')
            return
        dialog.close()
        ui.navigate.to('/puzzle')

    async def go(path: tuple):
        st['sel'] = sel = st.get('sel', 0) + 1           # supersedes a selection still looked up
        path = await run.io_bound(settle, service, tuple(path), tuple(st['path']))
        if sel != st['sel']:
            return
        st['path'] = path
        await draw()

    async def choose_node(depth: int, node_id: str):
        # a later click (or any navigation) while this one is looked up wins
        st['sel'] = sel = st.get('sel', 0) + 1
        through = await run.io_bound(passed_through, service, node_id)
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

    # -- multiple choice -----------------------------------------------------------------------

    def owner_ok() -> bool:
        """The dialog writes only under the account it was opened under (a sign-in or sign-out in
        another tab while it is open closes it)."""
        if store.current_owner() == ms['owner']:
            return True
        ui.notify(tr('You signed in or out in another tab. Open the shelf again.'), type='warning')
        dialog.close()
        return False

    # ONE lock, ``lock``, for every change to the selection and for a confirm's start (the copy
    # sent to the host, the tracker begun) and end (the tracker read, what the host took taken
    # out): no change is half made while either reads or writes ``ms['sel']`` or ``ms['since']``
    # (Codex, PR #70 rounds 1-2).

    async def commit(fn) -> bool:
        """One change: ``fn(selection, members_of)`` on a copy, off the loop; the copy replaces
        the selection only when it changed, and is kept at once. The caller holds ``lock``."""
        work = ms['sel'].copy()
        done = await run.io_bound(_apply_change, fn, work, service, ms['since'] is not None, limit=limit)
        refused = done[2] if done and len(done) > 2 else None
        if refused is None and limit is not None and done and done[0] and len(done) > 4:
            # asked again here, with no wait before the commit: the host may have filled meanwhile
            # (a desk's confirm landing while this tick was counted; Codex on #82)
            refused = limit.refusal(done[3], done[4])
        if refused is not None:
            ui.notify(refusal_message(refused, tr), type='warning')   # past the limit: not kept
            return False
        if not done or not done[0] or not owner_ok():
            return False
        ms['sel'] = work
        if ms['since'] is not None:                  # chosen after a confirm was sent: it stays chosen
            ms['since'] |= done[1]
        if not store.save_selection(purpose, ms['owner'], work) and not ms['warned']:
            ms['warned'] = True
            ui.notify(tr('Your choice could not be kept in this browser; it stays while this window is open.'),
                      type='warning')
        return True

    async def change(fn) -> bool:
        """A reader's change, made one at a time, so two quick ticks both count."""
        if not multi:                               # a box drawn before the switch back, clicked meanwhile
            return False
        if not owner_ok():
            return False
        async with lock:
            changed = await commit(fn)
        if changed:
            await refresh()
        return changed

    async def refresh():
        """Count again and repaint the ticks, the chips and the total (the last count wins)."""
        if not multi:
            return
        ms['sgen'] = gen = ms['sgen'] + 1
        state = await run.io_bound(_selection_state, service, ms['sel'].copy(), tuple(ms['boxes']),
                                   st['list_node'], lang)
        if state is None or gen != ms['sgen']:
            return
        ms['state'] = state
        paint()

    def too_big(count) -> bool:
        return limit is not None and count is not None and limit.count_too_big(count)

    def too_many(count) -> str:
        n = limit.max_items if (limit.max_items is not None and count > limit.max_items) else limit.max_pages
        return tr('Too many to choose here (up to {n})').format(n=f"{n:,}")

    def faded(owner, on: bool, count=None):
        """A box too big to tick here, faded and saying so; a ticked one stays as it is, to untick.
        Changed only when its state changes: a repaint does not send every box again."""
        if limit is None or on == ('stp-too-big' in owner.classes):
            return
        if on:
            owner.classes(add='stp-too-big')
            owner.props(f'aria-disabled="true" title="{_attr(too_many(count if count is not None else ms["big"].get(owner_id(owner), 0)))}"')
        else:
            owner.classes(remove='stp-too-big')
            owner.props(remove='aria-disabled title')

    def owner_id(owner) -> str:
        return owner.props.get('data-node-box') or owner.props.get('data-whole') or ''

    def set_box(owner, icon, state: str):
        icon.set_name(_BOX[state])
        owner.props(f'aria-checked="{_ARIA[state]}"')
        if state == 'all':
            owner.classes(add='stp-ticked')
        else:
            owner.classes(remove='stp-ticked')

    def paint():
        state = ms['state']
        if state is None:
            return
        chosen = state['chosen']
        for sid, items in list(ms['ticks'].items()):
            alive = [(r, i) for r, i in items if not r.is_deleted]
            if not alive:
                del ms['ticks'][sid]
                continue
            ms['ticks'][sid] = alive
            for row, icon in alive:
                set_box(row, icon, 'all' if sid in chosen else 'none')
        for sid, items in list(ms['jticks'].items()):      # the pane's joins, ticked as in the list
            alive = [(b, i) for b, i in items if not b.is_deleted]
            if not alive:
                del ms['jticks'][sid]
                continue
            ms['jticks'][sid] = alive
            for box, icon in alive:
                set_box(box, icon, 'all' if sid in chosen else 'none')
        for nid, items in list(ms['boxes'].items()):
            alive = [(b, i) for b, i in items if not b.is_deleted]
            if not alive:
                del ms['boxes'][nid]
                continue
            ms['boxes'][nid] = alive
            if nid in state['nodes']:
                for box, icon in alive:
                    set_box(box, icon, state['nodes'][nid])
                    faded(box, nid in ms['big'] and state['nodes'][nid] != 'all')
        whole, row = state['whole'], ms['whole']
        if row is not None and not row['row'].is_deleted and whole is not None and whole['id'] == row['id']:
            row['text'].set_text(tr('Select all in {name}').format(name=_isolated(whole['name']))
                                 + f" ({whole['total']:,})")
            set_box(row['row'], row['icon'], whole['state'])
            faded(row['row'], too_big(whole['total']) and whole['state'] != 'all', whole['total'])
        draw_chips(state['chips'])
        count_label.set_text(tr('{n} manuscripts chosen').format(n=f"{state['count']:,}"))
        enable_actions()
        if state['count']:
            clear_btn.enable()
        else:
            clear_btn.disable()

    def chip_el(chip, on_remove=None):
        remove = on_remove or remove_chip
        with ui.element('span').classes('stp-chip').props(f'data-chip="{_attr(chip["key"])}"'):
            ui.label(chip['label']).classes('stp-chip-name').props('dir="auto"').tooltip(chip['label'])
            if chip['node']:
                ui.label(f"· {chip['count']:,}").classes('stp-chip-count')
            ui.button(icon='close', on_click=lambda _e=None, k=chip['key']: remove(k)).props(
                f'flat round dense size=xs aria-label="{tr("Remove")}: {_attr(chip["label"])}"')

    def draw_chips(chips):
        chips_row.clear()
        chips_row.props(f'data-extra="{max(0, len(chips) - CHIPS_DRAWN)}"')
        with chips_row:
            if not chips:
                hint = ('Tick shelfmarks, or a whole volume, to choose them.' if node_boxes
                        else 'Tick shelfmarks to choose them.')
                ui.label(tr(hint)).classes('stp-hint')
                return
            for chip in chips[:CHIPS_DRAWN]:
                chip_el(chip)
            # hidden by the CSS until the script shows it: an inline style set here would be set again on
            # every update of the button and hide it under the script (NiceGUI .style() wipes a script's)
            ui.button('', on_click=lambda: show_all_chips()).props(
                f'flat dense no-caps data-word="{_attr(tr("+{n} more"))}"').classes('stp-more')
        chips_row.client.run_javascript(_FIT_CHIPS_JS % dialog.id)

    def show_all_chips():
        """The full list behind "+N more", each chip with its remove button."""
        state = ms['state']
        if state is None:
            return
        with container, ui.dialog() as everything, ui.card().classes('stp-all-chips').props(
                f'dir={"rtl" if is_rtl() else "ltr"}'):
            with ui.row().classes('w-full items-center justify-between no-wrap'):
                total = ui.label('').classes('font-semibold')
                ui.button(icon='close', on_click=everything.close).props(f'flat round dense aria-label="{tr("Close")}"')
            listing = ui.element('div').classes('stp-all-list')

        def fill():
            now = ms['state'] or state
            total.set_text(tr('{n} manuscripts chosen').format(n=f"{now['count']:,}"))
            listing.clear()
            with listing:
                for chip in now['chips']:
                    chip_el(chip, on_remove=remove_here)

        async def remove_here(key: str):
            await remove_chip(key)
            if not everything.is_deleted:
                fill()

        fill()
        everything.on('hide', lambda: everything.delete())
        everything.open()

    async def remove_chip(key: str):
        await change(lambda s, _m, k=key: s.remove_chip(k))

    async def clear_all():
        await change(lambda s, _m: bool(len(s)) and (s.clear() or True))

    async def toggle_leaf(leaf):
        await change(lambda s, m, lf=leaf: s.drop_leaf(lf.sys_id, m) if lf.sys_id in s.chosen_set(m)
                     else s.add_leaf(lf, m))

    async def toggle_node(node_id: str, scope: Optional[str]):
        """A node's box: ticks all of it, or unticks it when all of it is chosen. ``scope`` None:
        the scope "Select all" takes over the node's shelfmarks (``scope_for``). A node too big for the
        limit is not ticked (its manuscripts are not even read): the reader is told the limit."""
        painted = ms['state'] or {}
        if scope == ALL and node_id in ms['big'] and painted.get('nodes', {}).get(node_id) != 'all':
            ui.notify(too_many(ms['big'][node_id]), type='warning')
            return
        whole = painted.get('whole')
        if scope is None and whole and whole['id'] == node_id and too_big(whole['total']) and whole['state'] != 'all':
            ui.notify(too_many(whole['total']), type='warning')
            return

        def flip(s, m):
            node = service.node(node_id)
            if node is None:
                return False
            how = scope or scope_for(node)
            if s.node_state(node, how, m) == 'all':
                return s.drop_node(node, how, m)
            return s.add_node(node, how, m)
        await change(flip)

    async def confirm():
        """Hand the host what is chosen as it stands now; what it takes leaves the selection, what
        it does not stays (and so does anything ticked while it worked). A failure keeps it all."""
        if not multi or ms['busy'] or not owner_ok():   # not multi: clicked as the switch went off
            return
        ms['busy'] = True
        confirm_btn.disable()
        try:
            async with lock:
                frozen = ms['sel'].copy()
                ms['since'] = set()                  # what is chosen from now on is not the host's to take
            if not len(frozen):
                return
            resolved = await run.io_bound(_resolve, service, frozen, lang)
            if not resolved or not resolved[0]:
                return
            entries, label = resolved
            if not owner_ok():                     # signed in or out while the choice was read
                return
            try:
                outcome = on_confirm(entries, label)
                if inspect.isawaitable(outcome):
                    outcome = await outcome
            except Exception:
                logger.exception("shelf picker: the host could not take the choice")
                ui.notify(tr('The choice could not be applied. It is kept; try again.'), type='negative')
                return
            submitted = [e['sys_id'] for e in entries]
            if outcome is None:
                applied = submitted
            else:
                wanted = set(submitted)
                applied = [s for s in outcome if s in wanted]
            # What the reader chose again while the host worked (cleared, then ticked) stays chosen:
            # the host took the choice it was sent, not the later one. Under the lock, so a change
            # still being made has finished and recorded what it chose before the tracker is read.
            async with lock:
                chosen_since, ms['since'] = ms['since'] or set(), None
                applied = [s for s in applied if s not in chosen_since]
                took = bool(applied) and await commit(lambda s, m, a=tuple(applied): (s.consume(a, m) or True))
            if took:
                await refresh()
            left = ms['state']['count'] if ms['state'] is not None else 0
            if applied and not left:
                dialog.close()
            elif left:
                ui.notify(tr('{n} are still chosen').format(n=f"{left:,}"), type='info')
        finally:
            ms['busy'] = False
            if ms['since'] is not None:
                async with lock:
                    ms['since'] = None
            paint()

    def still_open() -> bool:
        """The multiple choice is still on and its picker still open: a read awaited meanwhile may
        have outlived either (the reader closed the picker while the lists were read; Codex on #92)."""
        return bool(multi) and not dialog.is_deleted and bool(dialog.value)

    async def save_as_list():
        """Add to List (D12, DX22): the choice into one of the reader's lists, or a new one named by
        its chips; it stays chosen. Up to a list's limit (``limit_for('lists')``), the rest named."""
        if not multi or not owner_ok():                 # not multi: clicked as the switch went off
            return
        if lists_mgr is None or not GlobalAuthState.is_logged_in():
            from web.components.add_to_list_dialog import require_login_for_lists
            with container:
                require_login_for_lists()
            return
        async with lock:
            frozen = ms['sel'].copy()
        resolved = await run.io_bound(_resolve, service, frozen, lang)
        if not resolved or not resolved[0] or not still_open():   # switched off or closed meanwhile: no dialog
            return
        entries, label = resolved
        ids = [e['sys_id'] for e in entries]
        room = limit_for('lists')
        fits = room.take(ids) if room is not None else ids
        if len(fits) < len(ids):
            over = room.refusal(fits, list(fits) + [ids[len(fits)]])
            if over is not None:
                ui.notify(refusal_message(over, tr), type='info')
            ids = list(fits)
        if not ids:
            return
        # this dialog's own save: the manuscripts as they were when it opened, one write at a time,
        # and a list made by a try whose items were not added (filled by the next try, under the
        # name it was made with). Closing the dialog forgets it.
        saving = {'busy': False, 'list_id': None, 'name': ''}
        options = await _load_list_options(lists_mgr)
        if not still_open() or not owner_ok():   # switched off, closed, or another tab, while it was read
            return
        with container, ui.dialog() as ask, ui.card().classes('stp-save-card').props(
                f'dir={"rtl" if is_rtl() else "ltr"} data-testid="shelf-save-list"'):
            ui.label(tr('Add to List')).classes('font-semibold')
            target = None
            if options:
                target = ui.select({_NEW_LIST: f"+ {tr('Create new list')}", **options}, value=_NEW_LIST,
                                   label=tr('Select List')).props('dense outlined data-testid="shelf-save-target"'
                                                                  ).classes('w-full')
            name = ui.input(tr('List name'), value=label[:120]).props('dense outlined autofocus').classes('w-full')
            if target is not None:
                name.bind_visibility_from(target, 'value', backward=lambda v: v == _NEW_LIST)
            ui.label(tr('{n} manuscripts chosen').format(n=f"{len(ids):,}")).classes('text-sm')

            async def do_save():
                if saving['busy']:
                    return
                if not owner_ok():
                    ask.close()
                    return
                into = target.value if target is not None and target.value != _NEW_LIST else None
                saving['busy'] = True
                save_btn.disable()
                try:
                    if into is not None and saving['list_id'] is None:
                        saved = await save_selection_as_list(lists_mgr, '', ids, list_id=into)
                        if saved is None or not saved.complete:
                            return              # the reader has been told (a failed write)
                        ask.close()
                        ui.notify(f"{tr('Added to list')}: {options.get(into, '')}", type='positive')
                        return
                    called = saving['name'] or name.value.strip()
                    saved = await save_selection_as_list(lists_mgr, called, ids, list_id=saving['list_id'])
                    if saved is None:
                        return
                    if not saved.complete:
                        saving['list_id'], saving['name'] = saved.list_id, called
                        name.disable()                 # the list has its name now
                        if target is not None:
                            target.disable()
                        ui.notify(tr('The list was made, but not everything was added to it. Try again to add the rest.'),
                                  type='warning')
                        return
                    ask.close()
                    ui.notify(tr('Saved as a list: {name}').format(name=called), type='positive')
                finally:
                    saving['busy'] = False
                    if not ask.is_deleted:
                        save_btn.enable()

            with ui.row().classes('w-full justify-end'):
                ui.button(tr('Cancel'), on_click=ask.close).props('flat no-caps')
                save_btn = ui.button(tr('Save'), on_click=do_save).props('unelevated no-caps color=primary')
        ask.on('hide', lambda: ask.delete())
        ask.open()

    # -- drawing ------------------------------------------------------------------------------

    def leaf_el(leaf, base: str):
        """A shelfmark's element in any view (a list row, a table row, a tile), its contents for the
        caller to add inside it. Single choice: a click selects it (DX21), a double click or Enter on
        the selected one acts, shown chosen through ``one['rows']`` (by placement). Multiple choice: a
        click, Enter or Space ticks it, its tick icon first inside it, painted through ``ms['ticks']``
        (by sys_id). Either way the pane shows it (DX25)."""
        on = leaf.id == st['highlight']
        if not multi:
            el = ui.element('div').classes(base + (' stp-on' if on else '')).props(
                f'data-leaf="{leaf.id}" data-sys-id="{leaf.sys_id}" role="option" '
                f'aria-selected="{"true" if on else "false"}" tabindex="0"').on(
                'click', lambda _e=None, lf=leaf: select(lf)).on(
                'dblclick', lambda _e=None, lf=leaf: double(lf)).on(
                'keydown.enter', lambda _e=None, lf=leaf: enter(lf)).on(
                'keydown.space.prevent', lambda _e=None, lf=leaf: select(lf))
            one['rows'].setdefault(leaf.id, []).append(el)
            return el
        ticked = ms['state'] is not None and leaf.sys_id in ms['state']['chosen']
        state = 'all' if ticked else 'none'
        el = ui.element('div').classes(base + ' stp-pick' + (' stp-on' if on else '')
                                       + (' stp-ticked' if ticked else '')).props(
            f'data-leaf="{leaf.id}" data-sys-id="{leaf.sys_id}" role="checkbox" '
            f'aria-checked="{_ARIA[state]}" tabindex="0"').on(
            'click', lambda _e=None, lf=leaf: tick(lf)).on(
            'keydown.enter', lambda _e=None, lf=leaf: tick(lf)).on(
            'keydown.space.prevent', lambda _e=None, lf=leaf: tick(lf))
        with el:
            icon = ui.icon(_BOX[state]).classes('stp-box')
        ms['ticks'].setdefault(leaf.sys_id, []).append((el, icon))
        return el

    def leaf_row(row: dict):
        """A shelfmark's row in the list (and among a filter's rows in a node column)."""
        marks = _signs_of(row['leaf'].sys_id)
        with leaf_el(row['leaf'], 'stp-row'):
            _leaf_name(row, marks).classes('stp-sm')
            _sign_marks(marks)

    def cells_of(row: dict) -> str:
        """A table row's cells or a tile's contents, as the view shows them, with what is known now."""
        sid = row['leaf'].sys_id
        marks = _signs_of(sid)
        if vw['view'] == 'gallery':
            return _tile_html(row, vw['rows'].get(sid),
                              vw['thumbs'].get(_THUMB_WIDTH[vw['size']], {}).get(sid), marks)
        return _cells_html(row, vw['rows'].get(sid), marks, lang)

    def table_row(row: dict, base: str):
        """A table row or a tile (VX6): one element with its events, the tick icon, and one HTML of its
        cells, drawn again when the row's details come (``refresh_cells``)."""
        with leaf_el(row['leaf'], base):
            cells = ui.html(cells_of(row), sanitize=False).classes('stp-cells')   # escaped: _cells_html
        vw['cells'].setdefault(row['leaf'].sys_id, []).append((cells, row))

    def refresh_cells(sys_ids):
        """The rows of these manuscripts drawn again with their details (and new tiles watched)."""
        for sid in sys_ids:
            alive = [(cells, row) for cells, row in vw['cells'].get(sid, ()) if not cells.is_deleted]
            if alive:
                vw['cells'][sid] = alive
            else:
                vw['cells'].pop(sid, None)
            for cells, row in alive:
                cells.set_content(cells_of(row))
        if vw['view'] == 'gallery' and not dialog.is_deleted:
            body.client.run_javascript(_THUMBS_JS)

    def known_thumb_url(sys_id: str) -> Optional[str]:
        """The image address the tiles have for a manuscript (their size's first, then any), or None."""
        widths = [_THUMB_WIDTH[vw['size']]] + [w for w in vw['thumbs'] if w != _THUMB_WIDTH[vw['size']]]
        for width in widths:
            url = vw['thumbs'].get(width, {}).get(sys_id)
            if url is not None:
                return url
        return None

    def open_now() -> bool:
        return not dialog.is_deleted and bool(dialog.value)

    async def load_rows():
        """The details of the rows drawn that are not known yet (``row_details``), a page at a time off
        the loop, each page's rows drawn again when it comes; one loader a dialog, which works out again
        what is missing after each page (a newer drawing, a "Show more", another view). A page that comes
        after the rows have changed is kept all the same: only its drawing waits for the next."""
        if vw['loading']:
            vw['again'] = True
            return
        vw['loading'] = True
        try:
            while True:
                vw['again'] = False
                leaves, view = st.get('leaves'), vw['view']
                if not open_now() or leaves is None or view == 'list':
                    return
                width = _THUMB_WIDTH[vw['size']] if view == 'gallery' else None
                thumbs = vw['thumbs'].setdefault(width, {}) if width else None
                known = vw['rows'] if thumbs is None else {s: 1 for s in vw['rows'] if s in thumbs}
                batches = wanted([r['leaf'].sys_id for r in leaves['rows']], known)
                if not batches:
                    return
                names = {r['leaf'].sys_id: r['leaf'].shelfmark or r['label'] for r in leaves['rows']}
                meta, pages_of, texts_of = _meta_mgr(), _page_count(), _page_texts()
                for batch in batches:
                    thumb_of = ((lambda s, w=width, n=names: _thumbnail_src(s, n.get(s, ''), w)) if width else None)
                    got = await run.io_bound(row_details, batch, lang, meta_mgr=meta, page_count=pages_of,
                                             page_texts=texts_of, thumb_of=thumb_of)
                    for sid, det in (got or {}).items():
                        det = dict(det)
                        thumb = det.pop('thumb', None)
                        vw['rows'][sid] = det
                        if thumbs is not None:
                            thumbs[sid] = thumb or ''
                    if not open_now():
                        return
                    refresh_cells(batch)
                    if vw['again']:
                        break
                if not vw['again']:
                    return
        except Exception:   # noqa: BLE001 -- the cells stay as they are
            logger.warning("shelf picker: the rows' details could not be read", exc_info=True)
        finally:
            vw['loading'] = False

    async def set_view(view: str):
        """The switch (VX1): the pane in another view, kept for the reader; the whole dialog is drawn
        again, as the columns shown change (VX2)."""
        view = normal_view(view)
        if view == vw['view']:
            return
        vw['view'] = view
        safe_user_set(STORAGE_KEY, choice_value(vw['view'], vw['size']))
        await draw()

    async def set_size(size: str):
        size = normal_size(size)
        if size == vw['size']:
            return
        vw['size'] = size
        safe_user_set(STORAGE_KEY, choice_value(vw['view'], vw['size']))
        await draw()

    def view_switch():
        """List / Table / Thumbnails, and the thumbnails' size, in the shelfmarks pane's header."""
        with ui.element('div').classes('stp-views').props('role="group" data-testid="shelf-views"') as group:
            group.props['aria-label'] = tr('View')
            for key, icon, tip in _VIEW_BUTTONS:
                on = vw['view'] == key
                ui.button(icon=icon, on_click=lambda _e=None, k=key: set_view(k)).props(
                    f'flat round dense size=sm data-view="{key}" aria-pressed="{"true" if on else "false"}" '
                    f'aria-label="{_attr(tr(tip))}"').classes('stp-view' + (' stp-view-on' if on else '')).tooltip(tr(tip))
            if vw['view'] == 'gallery':
                ui.toggle({k: tr(SIZE_LABELS[k]) for k in SIZES}, value=vw['size'],
                          on_change=lambda e: set_size(e.value)).props(
                    'dense no-caps data-testid="shelf-thumb-size"').tooltip(tr('Thumbnail size'))

    async def tick(leaf):
        """Multiple choice: the box, and the pane shows the row clicked last (not waited for)."""
        background_tasks.create(show_details(leaf), name='shelf-picker-details')
        await toggle_leaf(leaf)

    def node_box(node_id: str, count: Optional[int] = None):
        """A whole node's box in multi mode: ticks the node, never opens it. ``count``: its manuscripts,
        which say whether it is too big for the limit (from the tree's stored count: nothing read)."""
        state = (ms['state'] or {}).get('nodes', {}).get(node_id, 'none')
        if too_big(count):
            ms['big'][node_id] = count
        with ui.element('span').classes('stp-node-box').props(
                f'role="checkbox" aria-checked="{_ARIA[state]}" tabindex="0" data-node-box="{node_id}" '
                f'aria-label="{_attr(tr("Choose all of it"))}"').on(
                'click.stop', lambda _e=None, n=node_id: toggle_node(n, ALL)).on(
                'keydown.enter.stop', lambda _e=None, n=node_id: toggle_node(n, ALL)).on(
                'keydown.space.stop.prevent', lambda _e=None, n=node_id: toggle_node(n, ALL)) as box:
            icon = ui.icon(_BOX[state]).classes('stp-box')
        ms['boxes'].setdefault(node_id, []).append((box, icon))
        faded(box, node_id in ms['big'] and state != 'all', count)

    def fill_nodes(col):
        """The rows of a column of nodes (and, under a filter with a number, of shelfmarks)."""
        depth = col['depth']
        tickable = multi and node_boxes and not col.get('libs')   # DX23: elsewhere "Select all" only
        for row in col['rows']:
            if 'sep' in row:
                ui.label(row['sep']).classes('stp-sep')
                continue
            if 'leaf' in row:
                leaf_row(row)
                continue
            go_to = row.get('go')
            handler = ((lambda _e=None, g=go_to: go(g)) if go_to is not None
                       else (lambda _e=None, d=depth, n=row['id']: choose_node(d, n)))
            with ui.element('div').classes('stp-row' + (' stp-on' if row['on'] else '')
                                           + (' stp-link' if row.get('link') else '')).props(
                    f'data-node="{row["id"]}" role="button" tabindex="0"'
                    + (' data-shortcut="1"' if row.get('link') else '')).on('click', handler).on(
                    'keydown.enter', handler).on('keydown.space.prevent', handler):
                if tickable:
                    node_box(row['id'], row.get('count'))
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
        """The shelfmarks in the pane's view: the list's rows, the table (its header first) or the tiles;
        then the details the table and the tiles show, read off the loop (``load_rows``)."""
        st['leaves'] = leaves
        view = vw['view']
        if view == 'table':
            ui.html(_header_html(multi), sanitize=False).classes('stp-thead').props('aria-hidden="true"')
        for row in leaves['rows']:
            if view == 'table':
                table_row(row, 'stp-trow')
            elif view == 'gallery':
                table_row(row, 'stp-tile')
            else:
                leaf_row(row)
        if view != 'list':
            background_tasks.create(load_rows(), name='shelf-picker-rows')
        if leaves['more']:
            ui.button(tr('Show more') + f" ({leaves['more']:,})",
                      on_click=lambda k=leaves['filter_key']: show_more(k, PAGE_SIZE)).props(
                'flat dense no-caps').classes('w-full').style('grid-column: 1 / -1;')

    def whole_row(leaves):
        """ "Select all in <node>" over the shelfmarks shown: the whole node, whatever the filter
        shows (its count is the node's manuscripts)."""
        node_id = leaves['node']

        async def flip(_e=None):
            await toggle_node(node_id, None)          # the scope is the node's own (``scope_for``)

        with ui.element('div').classes('stp-row stp-whole').props(
                f'role="checkbox" aria-checked="false" tabindex="0" data-whole="{node_id}"').on(
                'click', flip).on('keydown.enter', flip).on('keydown.space.prevent', flip) as row:
            icon = ui.icon(_BOX['none']).classes('stp-box')
            text = ui.label(tr('Select all in {name}').format(name=_isolated(leaves['title']))).classes('stp-sm')
        ms['whole'] = {'id': node_id, 'row': row, 'icon': icon, 'text': text}

    def leaves_title(leaves) -> str:
        return f"{tr('Shelfmarks')} ({leaves['total']:,}) — {leaves['title']}"

    def filter_box(key: str):
        ui.input(placeholder=tr('Filter'), value=st['filters'].get(key, '')).props(
            'dense outlined clearable debounce=250').classes('w-full').on_value_change(
            lambda e, k=key: set_filter(k, e.value))

    def after_draw():
        body.client.run_javascript(_RESTORE_SCROLL_JS + _FIT_GRID_JS + (_THUMBS_JS if vw['view'] == 'gallery' else '')
                                   + (_RESIZE_JS if vw['view'] == 'table' else ''))

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
        view = await run.io_bound(load_view, service, tuple(st['path']), dict(st['filters']),
                                  dict(st['shown']), lang, labels, _title_of)
        if gen != st['gen'] or view is None:      # None: the app is stopping
            return
        # The last load answers for every list changed meanwhile: a filter typed in one column while
        # another column's load was under way is not left with its old list.
        owed, st['owed'] = st['owed'], set()
        # the columns the layout rule shows (VX2): a partial drawing only while it folds as the one on screen
        folded = folds_columns(vw['view'], view['leaves'] is not None)
        on_screen = shown_columns(view['columns'], vw['view'], view['leaves'] is not None, WIDE_COLUMNS)
        if not st.get('whole') and owed and all(k in st['parts'] for k in owed) and folded == st.get('folded'):
            drawn = {}
            for k in owed:
                shown = next((c for c in on_screen if c['filter_key'] == k), None)
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
                await refresh()
                return
        st['whole'] = False
        st['parts'] = parts = {}
        st['folded'] = folded
        ms['ticks'], ms['boxes'], ms['whole'], ms['big'] = {}, {}, None, {}
        one['rows'] = {}
        vw['cells'], vw['thumbs'] = {}, {}         # the addresses follow the NLI breaker as it is now
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
                # one mirrored character: a right-to-left page draws it pointing left (DX19)
                ui.label(CRUMB_SEP).style('color: var(--text-secondary);').classes('stp-crumb-sep')
                if k == len(view['crumbs']) - 1:
                    ui.label(name).classes('font-semibold').props('dir="auto"')
                else:
                    crumb(name, lambda _e=None, d=i: go(tuple(st['path'][:d + 1])))
        body.clear()
        columns = on_screen                        # the last three; in the table and thumbnails the last one
        # A phone shows the deepest level only: the last node column (the chosen node's children, which
        # stay reachable when the node also holds shelfmarks of its own) and the shelfmarks, stacked.
        phone_depth = view['columns'][-1]['depth'] if view['columns'] else -1
        if view['leaves'] is not None and phone_depth != len(st['path']):
            phone_depth = -1                       # the chosen node has no child column: shelfmarks only
        st['list_node'] = view['leaves']['node'] if view['leaves'] is not None else None
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
                with ui.element('div').classes('stp-col stp-leaves stp-panel stp-phone').props(
                        f'data-view="{vw["view"]}"'):
                    with ui.element('div').classes('stp-lhead'):
                        heading = ui.label(leaves_title(leaves)).classes('stp-title stp-sm')
                        view_switch()
                    filter_box(leaves['filter_key'])
                    if multi:
                        whole_row(leaves)
                    kind = {'table': 'stp-table' + (' stp-multi' if multi else ''),
                            'gallery': f'stp-gallery stp-size-{vw["size"]}'}.get(vw['view'], 'stp-grid')
                    with ui.element('div').classes(f'stp-list {kind}').props(
                            f'data-col="{_attr(leaves["filter_key"])}"') as holder:
                        if not multi:                  # the selected row is an option of this list
                            holder.props.update({'role': 'listbox', 'aria-label': leaves['title']})
                        fill_leaves(leaves)
                    parts[leaves['filter_key']] = {'list': holder, 'title': heading}
        after_draw()
        if st['highlight']:
            body.client.run_javascript(
                _IN_LIST_JS + f'setTimeout(() => window.__stpInList('
                f'document.querySelector(\'[data-leaf="{st["highlight"]}"]\'), true), 50)')
        await refresh()

    async def begin():
        if multi or both:
            # a kept choice is brought up to the tree as it is now (a rebuilt file: chips gone, a
            # shelfmark written anew): adopted whenever the tree was read (prune answers None when it
            # was not), and kept again only when it changed
            before = ms['sel'].to_dict()
            kept = ms['sel'].copy()
            pruned = await run.io_bound(lambda: kept.prune(service))
            if pruned is not None and owner_ok():
                async with lock:
                    if ms['sel'].to_dict() == before and kept.to_dict() != before:
                        ms['sel'] = kept
                        store.save_selection(purpose, ms['owner'], kept)
        first = await run.io_bound(start_at, service, start)
        st['path'] = first.path
        st['ready'] = True                          # from now on a flip draws again (before: this does)
        if first.leaf is not None:
            st['highlight'] = first.leaf.id
            one['leaf'] = first.leaf                # opened at a manuscript: it is the one selected
            idx = await run.io_bound(direct_index, service, first.leaf)
            st['shown'][f"l:{first.leaf.node_id}"] = max(PAGE_SIZE, ((idx // PAGE_SIZE) + 1) * PAGE_SIZE)
        await draw()
        enable_actions()
        if first.leaf is not None and pane['leaf'] is None:
            await show_details(first.leaf)

    async def load_signs():
        """The rows' signs, read once for the whole server, off the loop: the rows drawn without
        them are drawn again with them, each list alone (a filter being typed keeps its box)."""
        signs = shelf_signs()
        if signs.ready:
            return
        try:
            await run.io_bound(signs.load)
        except Exception:   # noqa: BLE001 -- the rows stay without their signs
            logger.warning("shelf picker: the rows' signs could not be read", exc_info=True)
            return
        if dialog.is_deleted or not signs.ready or not st['ready'] or st.get('whole'):
            return                                  # not drawn yet: its first drawing has them
        keys = list(st['parts'])
        if keys:
            st['owed'].update(keys[:-1])
            await draw(only=keys[-1])

    show_mode()
    draw_empty_pane()
    dialog.open()
    container.client.run_javascript(_KEEP_SCROLL_JS + 'window.__stpScroll = {};')
    background_tasks.create(begin(), name='shelf-picker-open')
    background_tasks.create(load_signs(), name='shelf-picker-signs')
    return dialog
