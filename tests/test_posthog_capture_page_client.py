# -*- coding: utf-8 -*-
"""Server-side PostHog captures from a page's background work reach that page.

``posthog_capture`` sent through ``ui.run_javascript``, which needs the current
NiceGUI page. /browse's ``load_page`` runs in a task the page started (no page)
or after ``update_content()`` deleted the clicked Prev/Next button (a dead slot),
and the error was swallowed: ``browse_manuscript`` arrived for about one page
view in ten (140 events against ~1,300 manuscript page views in 30 days,
2026-10-20). The password-recovery events and ``posthog.identify`` in
``web/main.py``'s deferred probe were lost the same way.
"""

from __future__ import annotations

import ast
import asyncio
import itertools
from pathlib import Path

from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

from web.analytics import posthog_capture

ROOT = Path(__file__).resolve().parents[1]
_paths = itertools.count()


def _captures_from_a_task(**capture_kwargs) -> int:
    prepare_simulation()
    count = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page(f'/_posthog_capture_probe_{next(_paths)}')) as client:
            async def background():
                await asyncio.sleep(0.01)
                kwargs = {k: (client if v == 'client' else v) for k, v in capture_kwargs.items()}
                posthog_capture('probe_event', {'n': 1}, **kwargs)
            asyncio.ensure_future(background())
        await asyncio.sleep(0.3)
        count['n'] = sum(1 for m in client.outbox.messages
                         if m[1] == 'run_javascript' and "posthog.capture('probe_event'" in m[2].get('code', ''))

    asyncio.run(_run())
    return count['n']


def test_a_capture_from_a_page_task_needs_the_page_client():
    assert _captures_from_a_task() == 0, 'the trap is gone; the client= rule may be relaxed'
    assert _captures_from_a_task(client='client') == 1


def _call_keywords(source: str, func: str, first_arg: str) -> list[set[str]]:
    return [{k.arg for k in node.keywords}
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == func
            and node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == first_arg]


def test_browse_manuscript_is_captured_through_the_page_client():
    calls = _call_keywords((ROOT / 'web/pages/browse.py').read_text(encoding='utf-8'),
                           'posthog_capture', 'browse_manuscript')
    assert calls and all('client' in kw for kw in calls)


def test_the_recovery_dispatch_runs_inside_the_page():
    tree = ast.parse((ROOT / 'web/main.py').read_text(encoding='utf-8'))
    probe = next(n for n in ast.walk(tree)
                 if isinstance(n, ast.AsyncFunctionDef) and n.name == '_deferred_check')
    dispatch_in_page = [
        w for w in ast.walk(probe) if isinstance(w, ast.With)
        and any(getattr(i.context_expr, 'id', None) == 'page_client' for i in w.items)
        and any(isinstance(c, ast.Call) and getattr(c.func, 'id', None) == '_handle_recovery_payload'
                for c in ast.walk(w))
    ]
    assert dispatch_in_page, ('the recovery dispatch (its PostHog events and posthog.identify) '
                              'runs outside the page: nothing it sends reaches the browser')
