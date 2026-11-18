"""The release's one-time "New" highlight on /search (owner, 2026-11-18).

As letter-level search and the dictionaries switch were announced: the control itself is
highlighted and its label says it is new, no badge beside it.

* The mode list, while the catalogue search is served and this reader has run no Catalog
  search: its frame is highlighted, and in the open list the Catalog item is highlighted and
  reads "New! Catalog (%)".
* The Fuzzy search switch in Title mode, until this reader first uses Title's Fuzzy search:
  highlighted, reading "New! Fuzzy search".

Each is shown one time (owner, 2026-11-18): spent for later visits as soon as it shows, and gone
from the page when the reader chooses a mode, works the switch or uses the feature. Nothing to
dismiss (the desktop does the same, ``GenizahGUI._update_new_feature_marks``). Kept per reader through the
``safe_storage`` chokepoint, as the dictionaries switch keeps its own
(``web/components/blau_panel.py::SEEN_KEY``); ``/reset-hints`` forgets them.
"""
from __future__ import annotations

import html

from web import safe_storage

CATALOG_SEARCH_USED_KEY = 'ui.catalog_search_used'
TITLE_FUZZY_USED_KEY = 'ui.title_fuzzy_used'
KEYS = (CATALOG_SEARCH_USED_KEY, TITLE_FUZZY_USED_KEY)

#: The labels while new (the house form: "New! Letter-level search", "New! Judeo-Arabic dictionaries!").
NEW_CATALOG_LABEL = 'New! Catalog'
NEW_FUZZY_LABEL = 'New! Fuzzy search'
#: What the Catalog item searches (the owner's wording, 2026-11-18): its tooltip, and the mode
#: list's while it is new. The desktop's is GenizahGUI.CATALOG_MODE_TIP.
CATALOG_MODE_TIP = 'Search the catalogue information'

#: The highlight (web/static/common.css), in the dictionaries switch's amber: on the mode list's
#: frame and on the Fuzzy search switch; NEW_OPTION_CLASS on the Catalog item in the open list.
NEW_CLASS = 'gs-new-highlight'
NEW_OPTION_CLASS = 'gs-new-option'


def is_new(key: str) -> bool:
    """Whether this reader has not used the feature yet."""
    return not bool(safe_storage.safe_user_get(key, False))


def mark_used(key: str) -> None:
    """The reader used the feature: its highlight is gone for good. Written once."""
    if is_new(key):
        safe_storage.safe_user_set(key, True)


def mark_used_by_run(run) -> None:
    """A run the page shows again (rows restored, or a run rejoined) is a search this reader ran:
    a Catalog run spends the Catalog's "New", a Title run with Fuzzy search the switch's, as
    running it did. Without this, a reader whose last search was a Catalog one before the marks
    came would see its mode highlighted as new over its own results (Codex review of PR #55)."""
    from web.search_bootstrap import is_catalog_run
    if not isinstance(run, dict):
        return
    if is_catalog_run(run):
        mark_used(CATALOG_SEARCH_USED_KEY)
    elif run.get('page_mode') == 'Title' and (run.get('catalog_options') or {}).get('near_spellings'):
        mark_used(TITLE_FUZZY_USED_KEY)


def _script_string(text: str) -> str:
    """*text* for a single-quoted script string inside a double-quoted template attribute."""
    return text.replace('\\', '\\\\').replace("'", "\\'").replace('"', '&quot;')


def option_slot(new_label: str, plain_label: str, tip: str) -> str:
    """The mode list's option slot: Quasar's own item. The Catalog item, under either label, has
    *tip* as its tooltip (what it searches, the owner's wording); NEW_OPTION_CLASS marks it while
    it reads *new_label*, so once its label is plain again, no item is highlighted."""
    new_q, plain_q = _script_string(new_label), _script_string(plain_label)
    return ('<q-item v-bind="props.itemProps" '
            f":class=\"{{'{NEW_OPTION_CLASS}': props.opt.label === '{new_q}'}}\">"
            '<q-item-section><q-item-label>{{ props.opt.label }}</q-item-label></q-item-section>'
            f"<q-tooltip v-if=\"props.opt.label === '{new_q}' || props.opt.label === '{plain_q}'\">"
            f'{html.escape(tip)}</q-tooltip>'
            '</q-item>')
