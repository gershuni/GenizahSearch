"""
Regression tests for the dual-protocol progress callback contract of
LabEngine._execute_batched_search (genizah_core.py).

Prod incident 2026-06-12: web/pages/parallels.py defined progress_cb(current, total)
with two REQUIRED positional args, but _execute_batched_search also invokes the
callback with a single string status message ("Scanning items ..."). The TypeError
("missing 1 required positional argument: 'total'") escaped through the
batched-search generator (via the lab_composition_search pass-through lambda) and
aborted lab deep-scan composition searches on the web.

These tests pin:
1. The core emits BOTH protocols: (i: int, total: int) AND (message: str).
2. A callback that cannot handle the single-string protocol degrades gracefully
   (search proceeds, status text lost) instead of aborting the search.
3. InterruptedError raised inside the callback still propagates (cancellation,
   relied on by both desktop gui_threads.py and web parallels.py).
4. AST guard: the web progress_cb stays callable with a single positional arg.
"""

import ast
from pathlib import Path

import pytest

import genizah_core

REPO_ROOT = Path(__file__).resolve().parent.parent


class _FakeResult:
    def __init__(self, hits):
        self.hits = hits


class _FakeSearcher:
    def __init__(self, hits):
        self._hits = hits

    def search(self, query_obj, limit):
        return _FakeResult(self._hits)


def _make_engine(hits):
    """LabEngine without __init__ — _execute_batched_search only needs lab_searcher."""
    engine = object.__new__(genizah_core.LabEngine)
    engine.lab_searcher = _FakeSearcher(hits)
    return engine


_HITS = [(1.0, 'addr-a'), (0.9, 'addr-b'), (0.8, 'addr-c')]


def test_emits_both_numeric_and_string_protocols():
    """Core contract: each batch fires (i, total) AND a single string status."""
    engine = _make_engine(_HITS)
    calls = []

    def cb(*args):
        calls.append(args)

    yielded = list(engine._execute_batched_search('q', progress_callback=cb))

    assert yielded == _HITS
    numeric_calls = [c for c in calls if len(c) == 2]
    string_calls = [c for c in calls if len(c) == 1 and isinstance(c[0], str)]
    assert numeric_calls == [(0, len(_HITS))]
    assert len(string_calls) == 1
    assert string_calls[0][0].startswith('Scanning items')


def test_two_required_arg_callback_does_not_abort_search():
    """Regression (prod 2026-06-12): a (current, total)-only callback raises
    TypeError on the single-string status call; the search must survive."""
    engine = _make_engine(_HITS)
    numeric_calls = []

    def two_arg_cb(current, total):
        numeric_calls.append((current, total))

    yielded = list(engine._execute_batched_search('q', progress_callback=two_arg_cb))

    assert yielded == _HITS, 'string-protocol TypeError must not kill the generator'
    assert numeric_calls == [(0, len(_HITS))]


def test_interrupted_error_in_callback_still_propagates():
    """Cancellation contract: web + desktop raise InterruptedError inside the
    callback to cancel; it must NOT be swallowed by the status-call guard."""
    engine = _make_engine(_HITS)

    def cancelling_cb(*args):
        raise InterruptedError('Search cancelled')

    with pytest.raises(InterruptedError):
        list(engine._execute_batched_search('q', progress_callback=cancelling_cb))


def test_web_progress_cb_accepts_single_positional_arg():
    """AST guard: web/pages/parallels.py progress_cb must remain callable with a
    single positional argument (the core's string-status protocol)."""
    source = (REPO_ROOT / 'web' / 'pages' / 'parallels.py').read_text(encoding='utf-8')
    tree = ast.parse(source)

    defs = [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == 'progress_cb'
    ]
    assert defs, 'progress_cb not found in web/pages/parallels.py'

    for node in defs:
        args = node.args
        if args.vararg is not None:
            continue  # *args — accepts any arity
        required_positional = len(args.posonlyargs) + len(args.args) - len(args.defaults)
        assert required_positional <= 1, (
            f'progress_cb at line {node.lineno} requires {required_positional} '
            'positional args; the core also calls it with a single string '
            '(genizah_core._execute_batched_search) — see prod TypeError 2026-06-12'
        )
