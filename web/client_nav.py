# -*- coding: utf-8 -*-
"""Navigation the browser does itself, with no round trip to the server first.

``ui.navigate.to`` in a click handler costs a round trip before the next page even
starts loading: the click goes to the server over the websocket, waits its turn on the
one event loop, and only then is the browser told where to go. From Israel to the
server in Oregon that was about half a second on a quiet day (PostHog ``ux_timing``,
2026-11-11), several seconds while the loop was busy, and nothing at all while the
websocket was reconnecting -- the "several seconds from one module to another" the
owner reported that day. A link, or a click handler that runs in the browser, starts
the next page at once.
"""
from __future__ import annotations

import json


def go_to_js(path: str) -> str:
    """A ``js_handler`` that loads ``path`` in this tab (``element.on('click', js_handler=...)``).
    Also right for ``keydown.space``: it keeps the page from scrolling first."""
    return f'(e) => {{ if (e && e.preventDefault) e.preventDefault(); window.location.assign({json.dumps(path)}); }}'


def make_link(element, href: str, *, new_tab: bool = False):
    """Give a ``ui.button`` (Quasar renders it as ``<a>``) or an ``<a>`` element ``href``:
    the browser follows it itself. Returns ``element``."""
    element.props['href'] = href
    if new_tab:
        element.props['target'] = '_blank'
        element.props['rel'] = 'noopener noreferrer'
    return element
