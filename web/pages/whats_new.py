# -*- coding: utf-8 -*-
"""``/whats-new``: What's New in every version since 9.0.0 (owner, 2026-11-05).

The entries are ``shared/whats_new_history.py``'s, newest first, in the
visitor's language; each release shows its new features, improvements and
fixes, and an item that is in one app only says which. The header's What's New
panel and Help link here.
"""
from __future__ import annotations

from nicegui import ui

from shared.whats_new_history import CATEGORY_LABELS, app_note, localize, releases
from web.components.typography import h1, h2
from web.translations import get_language, tr

PAGE_CLASS = 'gs-whats-new'
RELEASE_CLASS = 'gs-wn-release'

STYLES = '''
.gs-whats-new {color:var(--text-primary);max-width:820px;margin:0 auto;width:100%;
 display:flex;flex-direction:column;gap:16px;}
.gs-wn-intro {color:var(--text-secondary);}
.gs-wn-release {background:var(--bg-secondary);border:1px solid var(--border-light);
 border-radius:12px;padding:18px 22px;display:flex;flex-direction:column;gap:8px;}
.gs-wn-head {display:flex;flex-wrap:wrap;align-items:baseline;gap:4px 12px;}
.gs-wn-head h2 {margin:0;}
.gs-wn-date {color:var(--text-secondary);font-size:14px;}
.gs-wn-title {font-weight:600;}
.gs-wn-cat {font-size:14px;font-weight:700;color:#17634d;margin-top:6px;}
.gs-wn-items {margin:0;padding-inline-start:20px;display:flex;flex-direction:column;gap:6px;list-style:disc;}
.gs-wn-items li {display:list-item;line-height:1.6;}
.gs-wn-app {display:inline-block;margin-inline-start:6px;padding:0 8px;border-radius:999px;
 font-size:12px;background:var(--bg-tertiary);border:1px solid var(--border-light);
 color:var(--text-secondary);white-space:nowrap;}
@media(max-width:640px) {.gs-wn-release {padding:14px 14px;}}
'''


def _release(release: dict, lang: str) -> None:
    view = localize(release, lang)
    with ui.element('section').classes(RELEASE_CLASS).props(f'id=v{view["version"]}'):
        with ui.element('div').classes('gs-wn-head'):
            h2(tr('Version {version}').format(version=view['version']), classes='text-xl font-bold')
            ui.label(view['date']).classes('gs-wn-date').props('dir=ltr')
        ui.label(view['title']).classes('gs-wn-title')
        for category, items in view['sections']:
            ui.label(tr(CATEGORY_LABELS[category])).classes('gs-wn-cat')
            with ui.element('ul').classes('gs-wn-items'):
                for item in items:
                    with ui.element('li'):
                        ui.label(item['text']).style('display:inline')
                        note = app_note(item['apps'])
                        if note:
                            ui.label(tr(note)).classes('gs-wn-app')


def create_whats_new_page() -> None:
    """The page body (the route in web/main.py adds the head and the layout)."""
    lang = get_language()
    ui.add_css(STYLES)
    with ui.element('div').classes(PAGE_CLASS).props('dir=rtl' if lang == 'he' else 'dir=ltr'):
        h1(tr("What's New in every version"), classes='text-2xl font-bold')
        ui.label(tr('Every version since 9.0.0, newest first.')).classes('gs-wn-intro')
        for release in releases():
            _release(release, lang)
