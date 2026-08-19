"""Engine-side cancellation checkpoints.

Cancellation here is cooperative and checkpoint-only: the engines yield control
solely by calling progress_callback, and an InterruptedError raised from it must
travel all the way out. Two things routinely break that, and both are tested:

  * a broad `except Exception` swallowing the raise — InterruptedError is an
    OSError subclass, so every handler ordering matters;
  * a tick placed INSIDE a per-item try, where the raise is caught and logged as
    an item failure and the cancel is simply lost.

Loops too entangled to fake are guarded structurally instead: a progress_callback
call in the loop body AND an enclosing InterruptedError handler. That pair is
what keeps the returned payload's 'partial' flag honest.
"""

import ast
import inspect
import textwrap
import types

import pytest

from shared import lab_engine as lab_mod
from shared import search_engine as se_mod
from shared.search_engine import PHASE_LOCAL_SEARCH, SearchEngine


# --------------------------------------------------------------- AST utilities

def _fn_tree(fn):
    return ast.parse(textwrap.dedent(inspect.getsource(fn)))


def _handler_names(handler):
    t = handler.type
    if isinstance(t, ast.Name):
        return [t.id]
    if isinstance(t, ast.Tuple):
        return [e.id for e in t.elts if isinstance(e, ast.Name)]
    return []


def _calls_progress_callback(node):
    return any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        and n.func.id == 'progress_callback'
        for n in ast.walk(node)
    )


def _find_loop(fn, target_names):
    """The `for` loop whose target binds exactly these names."""
    for node in ast.walk(_fn_tree(fn)):
        if not isinstance(node, ast.For):
            continue
        names = [n.id for n in ast.walk(node.target) if isinstance(n, ast.Name)]
        if set(target_names).issubset(set(names)):
            return node
    return None


def _has_interrupt_handler(fn):
    for node in ast.walk(_fn_tree(fn)):
        if isinstance(node, ast.Try):
            for h in node.handlers:
                if 'InterruptedError' in _handler_names(h):
                    return True
    return False


# ------------------------------------------- lab_search's swallowed cancel (bug 1)

def test_lab_search_batch_cb_reraises_cancellation():
    """batch_cb wrapped progress_callback in a bare `except Exception: pass`,
    which ate the cancel before _execute_batched_search's own re-raise guard
    could see it. Stop in Lab deep scan therefore did nothing."""
    tree = _fn_tree(lab_mod.LabEngine.lab_search)
    batch_cb = next(
        (n for n in ast.walk(tree)
         if isinstance(n, ast.FunctionDef) and n.name == 'batch_cb'), None)
    assert batch_cb is not None, 'batch_cb not found in lab_search'

    reraises = [
        h for t in ast.walk(batch_cb) if isinstance(t, ast.Try)
        for h in t.handlers
        if 'InterruptedError' in _handler_names(h)
        and any(isinstance(s, ast.Raise) for s in h.body)
    ]
    assert reraises, 'batch_cb does not re-raise InterruptedError'

    # ...and the re-raise must come before the broad handler that would eat it.
    for t in ast.walk(batch_cb):
        if not isinstance(t, ast.Try):
            continue
        idx = {}
        for i, h in enumerate(t.handlers):
            for nm in _handler_names(h):
                idx.setdefault(nm, i)
        if 'InterruptedError' in idx and 'Exception' in idx:
            assert idx['InterruptedError'] < idx['Exception']


def test_lab_search_returns_partials_on_cancel():
    """Sections 3+3b are wrapped so a cancel falls through to sort/dedup."""
    assert _has_interrupt_handler(lab_mod.LabEngine.lab_search)


# ------------------------------------- _query_local_index propagates (bug 2)

class _FakeLocalSearcher:
    def __init__(self, n):
        self._n = n
        self.docs_built = 0

    def search(self, q, limit):
        return types.SimpleNamespace(hits=[(1.0, i) for i in range(self._n)])

    def doc(self, addr):
        self.docs_built += 1
        return {'content': ['x'], 'unique_id': ['u%d' % addr]}


def _engine_with_local(n_hits):
    eng = object.__new__(SearchEngine)
    eng._my_library_tab_ref = None
    eng.local_index = types.SimpleNamespace(parse_query=lambda *a, **k: object())
    eng.local_searcher = _FakeLocalSearcher(n_hits)
    eng._build_local_result_dict = lambda doc, score, regex=None, pattern_str=None: {'d': score}
    return eng


def test_query_local_index_ticks_progress():
    """This loop had NO callback at all, so a LOCAL or ALL scope search ignored
    Stop entirely."""
    eng = _engine_with_local(20)
    seen = []
    eng._query_local_index('q', 'literal', 0, progress_callback=lambda i, t: seen.append((i, t)))
    assert seen, 'no progress ticks — the LOCAL pass has no checkpoint'
    assert seen[0][0] == 0


def test_query_local_index_announces_its_phase():
    eng = _engine_with_local(5)
    phases = []
    eng._query_local_index('q', 'literal', 0, phase_callback=phases.append)
    assert phases == [PHASE_LOCAL_SEARCH]


def test_query_local_index_propagates_cancellation_instead_of_returning_empty():
    """The broad `except Exception: return []` at the tail would otherwise turn a
    user cancel into a silent empty LOCAL contribution AND eat the cancel."""
    eng = _engine_with_local(50)

    def cb(i, total):
        if i >= 10:
            raise InterruptedError('cancel')

    with pytest.raises(InterruptedError):
        eng._query_local_index('q', 'literal', 0, progress_callback=cb)

    assert eng.local_searcher.docs_built < 50, 'loop ran to completion after cancel'


def test_query_local_index_survives_a_broken_progress_callback():
    """Progress is advisory: a callback that raises something else must not
    abort the search. Pins the 2026-06-12 production fix's shape."""
    eng = _engine_with_local(10)

    def bad_cb(i, total):
        raise TypeError('takes 3 positional arguments')

    out = eng._query_local_index('q', 'literal', 0, progress_callback=bad_cb)
    assert len(out) == 10


# -------------------------------- _execute_metadata_search keeps partials (bug 3)

def test_metadata_search_returns_partials_on_cancel():
    """This was the ONLY search loop with no try/except, so the raise escaped to
    SearchThread and became results_signal.emit([]) — a stopped Title/Shelfmark
    search discarded every row it had, unlike every other mode."""
    eng = object.__new__(SearchEngine)
    eng.meta_mgr = types.SimpleNamespace(
        search_by_meta=lambda q, f: ['sys%03d' % i for i in range(40)],
        get_display_data=lambda *a, **k: {'shelfmark': 'T-S 1'},
        get_meta_for_id=lambda sid: {'shelfmark': 'T-S %s' % sid, 'title': 't',
                                     'library_code': 'CUL', 'id': sid},
    )
    eng.searcher = None
    eng.index = None

    def cb(i, total):
        if i >= 10:
            raise InterruptedError('cancel')

    out = eng._execute_metadata_search('q', 'Title', progress_callback=cb)
    assert isinstance(out, list), 'cancel escaped instead of yielding partials'
    assert out, 'partial results were discarded'
    assert len(out) < 40


# ------------------------------------- structural guards on the big LOCAL loops

def test_composition_local_post_pass_ticks_and_flags_partial():
    fn = SearchEngine.search_composition_logic
    loop = _find_loop(fn, ['_i_scl', '_plan_scl'])
    assert loop is not None, 'LOCAL post-pass loop not found'
    assert _calls_progress_callback(loop), 'LOCAL post-pass has no checkpoint'
    # The enclosing handler must SET the flag, not merely re-raise: the payload
    # carries 'partial', and reporting False for a cancelled run is a lie.
    src = inspect.getsource(fn)
    assert 'except InterruptedError:' in src
    assert 'was_cancelled = True' in src


def test_lab_composition_local_post_pass_ticks_and_flags_partial():
    fn = lab_mod.LabEngine.lab_composition_search
    loop = _find_loop(fn, ['_i', '_plan'])
    assert loop is not None, 'LOCAL-LAB post-pass loop not found'
    assert _calls_progress_callback(loop), 'LOCAL-LAB post-pass has no checkpoint'
    src = inspect.getsource(fn)
    assert 'except InterruptedError:' in src
    assert 'was_interrupted = True' in src


def test_lab_search_local_lab_loop_ticks():
    loop = _find_loop(lab_mod.LabEngine.lab_search, ['_i_llab'])
    assert loop is not None, 'LOCAL-LAB loop in lab_search not found'
    assert _calls_progress_callback(loop)


@pytest.mark.parametrize('call_site', [
    'progress_callback=progress_callback',
    'phase_callback=phase_callback',
])
def test_execute_search_threads_both_callbacks_to_local(call_site):
    """All four _query_local_index call sites must forward them, or the LOCAL
    pass silently keeps its old un-cancellable behaviour."""
    src = inspect.getsource(SearchEngine.execute_search)
    assert src.count(call_site) >= 3, src.count(call_site)
