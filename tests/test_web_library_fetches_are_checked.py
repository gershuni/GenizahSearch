# -*- coding: utf-8 -*-
"""Every outbound HTTP call the web process can make is checked or listed.

The web process is every module under ``web/`` plus every repository module
they import (module-level and function-level imports, followed to a fixed
point), plus ``shared/research_worker.py``, which the web starts as a
subprocess (``web/research_jobs.py``).

In those modules a direct outbound HTTP call -- ``requests.<verb>``, a
``requests.Session`` / ``_make_session()`` session's ``<verb>``, ``httpx``,
``aiohttp``, ``urlopen`` -- is allowed only

* inside a checker, the one place a checked fetch makes its plain request
  (``CHECKERS``), or
* at a site listed in ``EXEMPT`` with the reason: services on a fixed host
  that are not library image, manifest or MARC sources.

Everything else must go through ``get_with_checked_redirects`` (or
``MetadataManager._library_get``, or ``nli_image_get`` with ``allowed_url=``),
which follow redirects one hop at a time and only to the library hosts. A new
raw call in a web path fails this test until it uses one of them or is listed
here with its reason.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent

# Started by the web as a subprocess, so no import reaches it.
EXTRA_WEB_MODULES = ('shared/research_worker.py',)

HTTP_VERBS = {'get', 'post', 'head', 'put', 'patch', 'delete', 'request', 'options'}

# (file, function): where a checked fetch makes its plain request.
CHECKERS = {
    ('shared/puzzle_image_service.py', 'get_with_checked_redirects'):
        'the checked-hop helper: each hop is requested with allow_redirects=False',
    ('shared/puzzle_image_service.py', '_image_get'):
        'web rules -> get_with_checked_redirects; the plain branch is the desktop call',
    ('shared/nli_fetch.py', '_get_once'):
        'one hop of nli_image_get; each redirect target goes through allowed_url when given',
    ('shared/metadata_manager.py', 'MetadataManager._library_get'):
        'checked_library_fetches -> get_with_checked_redirects; the plain branch is the desktop call',
}

# (file, function): direct calls to services that are not library image,
# manifest or MARC hosts. Each must still match a call (no stale entries).
EXEMPT = {
    ('shared/sefaria_utils.py', 'get_sefaria_json'):
        'Sefaria API: the desktop branch; web callers pass checked=True and go '
        'through get_with_checked_redirects with the Sefaria host check',
    ('shared/dicta_client.py', '_translate_single'):
        'Dicta translation service: a fixed endpoint, not a library host',
    ('shared/posthog_server.py', '_drain_posthog_queue'):
        'PostHog analytics: a fixed capture URL, the response is not read',
    ('shared/posthog_server.py', '_flush_before_exit'):
        'PostHog analytics: a fixed capture URL, the response is not read',
    ('shared/posthog_server.py', 'send_crash_event_direct'):
        'PostHog analytics: a fixed capture URL, the response is not read',
    ('shared/posthog_server.py', 'send_selftest_event_sync'):
        'PostHog analytics: a fixed capture URL, the response is not read',
    ('web/api_hardening.py', '_drain_posthog_queue'):
        'PostHog analytics: a fixed capture URL, the response is not read',
    ('web/supabase_client.py', 'change_password'):
        "Supabase auth: the app's own backend at the fixed SUPABASE_URL",
}

# Modules the scan must reach (a closure that misses them proves nothing).
MUST_REACH = (
    'shared/metadata_manager.py', 'shared/puzzle_image_service.py', 'shared/nli_fetch.py',
    'shared/sefaria_utils.py', 'web/api.py', 'web/pages/puzzle.py',
    'shared/research_worker.py',
)


# ── the scan ────────────────────────────────────────────────────────────────

def _module_path(name):
    base = REPO.joinpath(*name.split('.'))
    if base.with_suffix('.py').is_file():
        return base.with_suffix('.py')
    if (base / '__init__.py').is_file():
        return base / '__init__.py'
    return None


def _imported_names(path, tree):
    package = list(path.relative_to(REPO).with_suffix('').parts[:-1])
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ''
            if node.level:
                parts = package[:len(package) - (node.level - 1)] if node.level > 1 else package
                base = '.'.join(parts + ([base] if base else []))
            yield base
            for alias in node.names:
                yield f'{base}.{alias.name}'


def web_modules():
    """{repo-relative posix path: parsed tree} for the web process."""
    todo = [p for p in (REPO / 'web').rglob('*.py') if '__pycache__' not in p.parts]
    todo += [REPO / extra for extra in EXTRA_WEB_MODULES]
    seen = {}
    while todo:
        path = todo.pop()
        key = path.relative_to(REPO).as_posix()
        if key in seen:
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'))
        seen[key] = tree
        for name in _imported_names(path, tree):
            found = _module_path(name) if name else None
            if found is not None:
                todo.append(found)
    return seen


def _qualnames(tree):
    """{node: enclosing 'Class.func.inner' name} for every node."""
    names = {}

    def visit(node, stack):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                inner = stack + [child.name]
                names[child] = '.'.join(inner)
                visit(child, inner)
            else:
                names[child] = '.'.join(stack) or '<module>'
                visit(child, stack)

    visit(tree, [])
    return names


def _dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return '.'.join(reversed(parts))
    return None


def outbound_calls(tree):
    """[(lineno, qualname, call text)] for each direct outbound HTTP call in ``tree``."""
    lib_aliases = {}        # local name -> library ('requests', 'httpx', 'aiohttp', 'urllib.request')
    direct_funcs = set()    # names bound by 'from requests import get' etc.
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in ('requests', 'httpx', 'aiohttp', 'urllib.request'):
                    lib_aliases[alias.asname or alias.name] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module in (
                'requests', 'httpx', 'aiohttp', 'urllib.request'):
            for alias in node.names:
                if alias.name in HTTP_VERBS or alias.name in ('urlopen', 'Session', 'Client',
                                                               'AsyncClient', 'ClientSession'):
                    direct_funcs.add(alias.asname or alias.name)

    def _is_session_factory(call):
        if not isinstance(call, ast.Call):
            return False
        name = _dotted(call.func) or ''
        head = name.split('.')[0]
        return (name.endswith('_make_session')
                or (head in lib_aliases and name.rsplit('.', 1)[-1]
                    in ('Session', 'Client', 'AsyncClient', 'ClientSession'))
                or name in direct_funcs and name in ('Session', 'Client', 'AsyncClient',
                                                     'ClientSession'))

    sessions = set()        # identifiers assigned a session
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and _is_session_factory(node.value):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    sessions.add(target.id)
                elif isinstance(target, ast.Attribute):
                    sessions.add(target.attr)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if _is_session_factory(item.context_expr) and isinstance(item.optional_vars, ast.Name):
                    sessions.add(item.optional_vars.id)

    names = _qualnames(tree)
    found = []
    # Attribute and name REFERENCES count, not only calls: ``fn = requests.get``
    # followed by ``fn(url)`` is the same fetch.
    for node in ast.walk(tree):
        hit = False
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            hit = node.id in direct_funcs and node.id not in ('Session', 'Client', 'AsyncClient',
                                                              'ClientSession')
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
            receiver = node.value
            receiver_name = _dotted(receiver) or ''
            last = receiver_name.rsplit('.', 1)[-1]
            if receiver_name in lib_aliases and node.attr in HTTP_VERBS | {'urlopen'}:
                hit = True
            elif receiver_name == 'urllib.request' and node.attr == 'urlopen':
                hit = True
            elif last in sessions and node.attr in HTTP_VERBS:
                hit = True
            elif isinstance(receiver, ast.Call) and _is_session_factory(receiver)                     and node.attr in HTTP_VERBS:
                hit = True  # requests.Session().get(...)
        if hit:
            found.append((node.lineno, names.get(node, '<module>'),
                          _dotted(node) or ast.unparse(node)))
    return found


def _nli_image_get_calls(tree):
    names = _qualnames(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and (_dotted(node.func) or '').endswith('nli_image_get'):
            yield node, names.get(node, '<module>')


@pytest.fixture(scope='module')
def modules():
    return web_modules()


# ── the guard ───────────────────────────────────────────────────────────────

def test_the_scan_reaches_the_modules_that_fetch(modules):
    missing = [path for path in MUST_REACH if path not in modules]
    assert not missing, missing
    assert len(modules) > 100, len(modules)


def test_every_outbound_call_in_the_web_process_is_checked_or_listed(modules):
    unlisted = []
    for path, tree in sorted(modules.items()):
        for lineno, qualname, text in outbound_calls(tree):
            if (path, qualname) in CHECKERS or (path, qualname) in EXEMPT:
                continue
            unlisted.append(f'{path}:{lineno} {qualname}: {text}')
    assert not unlisted, (
        'Outbound HTTP calls in the web process that neither use the checked-hop helper '
        '(shared/puzzle_image_service.py::get_with_checked_redirects) nor are listed in '
        'EXEMPT with a reason:\n  ' + '\n  '.join(unlisted))


def test_every_listed_site_still_makes_a_call(modules):
    seen = {(path, qualname) for path, tree in modules.items()
            for _, qualname, _ in outbound_calls(tree)}
    stale = sorted(set(CHECKERS) - seen) + sorted(set(EXEMPT) - seen)
    assert not stale, f'listed but no longer making a direct call (remove them): {stale}'


def test_every_metadata_manager_the_web_builds_checks_its_fetches(modules):
    """MetadataManager's own fetches are checked only when it is built with the flag."""
    built = []
    for path, tree in modules.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and (_dotted(node.func) or '').endswith('MetadataManager'):
                flag = [kw.value for kw in node.keywords if kw.arg == 'checked_library_fetches']
                ok = bool(flag) and isinstance(flag[0], ast.Constant) and flag[0].value is True
                built.append((f'{path}:{node.lineno}', ok))
    assert {'web/main.py', 'shared/research_worker.py'} <= {b[0].split(':')[0] for b in built}
    unchecked = [where for where, ok in built if not ok]
    assert not unchecked, f'MetadataManager built without checked_library_fetches=True: {unchecked}'


def test_the_web_calls_nli_image_get_with_allowed_url(modules):
    """nli_image_get checks each hop only when given allowed_url; the desktop gives none."""
    calls = [(path, qualname, node) for path, tree in modules.items()
             for node, qualname in _nli_image_get_calls(tree)]
    assert calls, 'no nli_image_get call found in the web process'
    missing = [f'{path}:{node.lineno} {qualname}' for path, qualname, node in calls
               if 'allowed_url' not in {kw.arg for kw in node.keywords}]
    assert not missing, missing


# ── the detector itself finds what it must ─────────────────────────────────

@pytest.mark.parametrize('source', [
    'import requests\ndef f(u):\n    return requests.get(u)\n',
    'import requests as _r\ndef f(u):\n    return _r.post(u, json={})\n',
    'def f(u):\n    import requests as _requests\n    return _requests.head(u)\n',
    'from requests import get\ndef f(u):\n    return get(u)\n',
    'import requests\ndef f(u):\n    s = requests.Session()\n    return s.get(u)\n',
    'import requests\n_s = requests.Session()\ndef f(u):\n    return _s.get(u, timeout=3)\n',
    'class M:\n    def f(self, u):\n        session = self._make_session()\n'
    '        return session.get(u)\n',
    'import requests\ndef f(u):\n    return requests.Session().get(u)\n',
    'import requests\ndef f(u):\n    fetch = requests.get\n    return fetch(u)\n',
    'import httpx\ndef f(u):\n    return httpx.put(u)\n',
    'from urllib.request import urlopen\ndef f(u):\n    return urlopen(u)\n',
    'import urllib.request\ndef f(u):\n    return urllib.request.urlopen(u)\n',
])
def test_the_detector_finds_a_direct_call(source):
    assert len(outbound_calls(ast.parse(source))) == 1, source


@pytest.mark.parametrize('source', [
    'def f(d):\n    return d.get("k")\n',
    'def f(storage):\n    session = storage\n    return session.get("id")\n',
    'from shared.puzzle_image_service import get_with_checked_redirects\n'
    'def f(u):\n    return get_with_checked_redirects(u)\n',
])
def test_the_detector_ignores_what_is_not_a_direct_call(source):
    assert outbound_calls(ast.parse(source)) == [], source


def test_the_listed_sites_have_reasons():
    for table in (CHECKERS, EXEMPT):
        for (path, qualname), reason in table.items():
            assert (REPO / path).is_file(), path
            assert len(reason) > 20, (path, qualname)
