"""Structural guards on the worker layer. Imports gui_threads; constructs nothing.

These pin the invariants that are cheap to break and expensive to debug:
exception-handler ORDER (InterruptedError is an OSError subclass, so a handler
placed after `except Exception` is dead), checkpoint-before-emit, and the
two-int acknowledgement signature.

The signature assertion exists because that exact contract already drifted once,
between two sections of the design doc, before any code was written.
"""

import ast
import inspect
import textwrap

import pytest

import gui_threads
from gui_threads import (CompositionThread, GroupingThread, LabCompositionThread,
                         LabSearchThread, MultiWitnessCompositionThread,
                         PausableSearchMixin, SearchThread)

# Every worker that inherits the mixin belongs here. The multi-witness thread
# is the one whose cancel path actually FIRES -- the passage engine calls no
# progress callback, so its checkpoint lives at the witness boundary rather
# than inside one -- which makes these five invariants load-bearing for it in
# a way they are not for a worker nobody can interrupt.
PAUSABLE = (SearchThread, LabSearchThread, CompositionThread,
            LabCompositionThread, MultiWitnessCompositionThread)


def _tree(fn):
    return ast.parse(textwrap.dedent(inspect.getsource(fn)))


def _handler_index(fn, exc_name):
    """Index of the `except <exc_name>` handler within its Try, or None."""
    for node in ast.walk(_tree(fn)):
        if not isinstance(node, ast.Try):
            continue
        for i, handler in enumerate(node.handlers):
            names = []
            t = handler.type
            if isinstance(t, ast.Name):
                names = [t.id]
            elif isinstance(t, ast.Tuple):
                names = [e.id for e in t.elts if isinstance(e, ast.Name)]
            if exc_name in names:
                return i
    return None


@pytest.mark.parametrize('cls', PAUSABLE, ids=lambda c: c.__name__)
def test_worker_is_pausable(cls):
    assert issubclass(cls, PausableSearchMixin)
    # Mixin must come first so its overrides (notably requestInterruption) win.
    assert cls.__mro__.index(PausableSearchMixin) < cls.__mro__.index(gui_threads.QThread)


@pytest.mark.parametrize('cls', PAUSABLE, ids=lambda c: c.__name__)
def test_pause_ack_signal_carries_run_id_and_epoch(cls):
    """Two ints: (run_id, epoch). A presence-only check would not have caught the
    drift this assertion exists for."""
    assert hasattr(cls, 'pause_ack_signal')
    sigs = cls.pause_ack_signal.signatures
    assert any('int,int' in s.replace(' ', '') for s in sigs), sigs


@pytest.mark.parametrize('cls', PAUSABLE, ids=lambda c: c.__name__)
def test_worker_accepts_a_run_id(cls):
    params = inspect.signature(cls.__init__).parameters
    assert 'run_id' in params
    # Defaulted, so existing direct construction (including in older tests) works.
    assert params['run_id'].default == 0


@pytest.mark.parametrize('cls', PAUSABLE, ids=lambda c: c.__name__)
def test_run_uses_the_checkpoint_and_closes_the_gate(cls):
    src = inspect.getsource(cls.run)
    assert '_checkpoint()' in src, 'no cooperative checkpoint'
    assert 'pause_gate.finish' in src, 'gate never finished — a pause pending at completion would strand the UI'
    assert 'total_paused_s' in src, 'perf timing does not discount parked time'
    assert 'if not self.cancel_flag:' in src, 'perf emitted for cancelled runs'


@pytest.mark.parametrize('cls', PAUSABLE, ids=lambda c: c.__name__)
def test_checkpoint_precedes_any_progress_emit(cls):
    """A parked worker must publish no progress, so the bar freezes where it was."""
    src = inspect.getsource(cls.run)
    if 'progress_signal.emit' not in src:
        pytest.skip('no progress emit in this worker')
    assert src.index('_checkpoint()') < src.index('progress_signal.emit')


def test_lab_search_handles_interrupt_before_the_broad_handler():
    """InterruptedError is an OSError subclass. Ordered after `except Exception`
    it would be dead code, and a cancelled Lab search would pop an error dialog
    via error_signal instead of returning results."""
    i_interrupt = _handler_index(LabSearchThread.run, 'InterruptedError')
    i_broad = _handler_index(LabSearchThread.run, 'Exception')
    assert i_interrupt is not None, 'LabSearchThread.run has no InterruptedError handler'
    assert i_broad is not None
    assert i_interrupt < i_broad, 'InterruptedError handler is unreachable'


def test_grouping_thread_is_deliberately_not_pausable():
    """GroupingThread has its own interruption model and emits no terminal signal
    on cancel (it bare-returns), so it is out of pause scope on purpose."""
    assert not issubclass(GroupingThread, PausableSearchMixin)
    assert not hasattr(GroupingThread, 'pause_ack_signal')


def test_request_cancel_sets_the_flag_and_unparks():
    src = inspect.getsource(PausableSearchMixin.request_cancel)
    assert 'cancel_flag = True' in src
    assert 'pause_gate.abort()' in src


def test_request_interruption_is_routed_into_request_cancel():
    """closeEvent's composition branch calls requestInterruption(), which the
    composition threads never polled — a silent no-op until this override."""
    src = inspect.getsource(PausableSearchMixin.requestInterruption)
    assert 'pause_gate.abort()' in src
    assert 'cancel_flag = True' in src


def test_should_abort_covers_both_cancel_channels():
    src = inspect.getsource(PausableSearchMixin._should_abort)
    assert 'cancel_flag' in src
    assert 'isInterruptionRequested' in src


def test_checkpoint_is_usable_without_qt():
    """_should_abort duck-types isInterruptionRequested so the hot path stays
    testable against a plain stub."""
    import types

    from shared.pause_gate import PauseGate

    stub = types.SimpleNamespace(cancel_flag=False, pause_gate=PauseGate())
    stub._should_abort = PausableSearchMixin._should_abort.__get__(stub, types.SimpleNamespace)
    stub._checkpoint = PausableSearchMixin._checkpoint.__get__(stub, types.SimpleNamespace)

    stub._checkpoint()                       # running: returns quietly
    stub.cancel_flag = True
    with pytest.raises(InterruptedError):
        stub._checkpoint()
