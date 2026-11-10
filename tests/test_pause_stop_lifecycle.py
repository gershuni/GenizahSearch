"""Source guards on the stop lifecycle.

Cancellation is checkpoint-only, so a PARKED worker never reaches the code that
reads cancel_flag. Every stop path therefore has to go through request_cancel(),
which sets the flag AND un-parks in one call. A path that only assigns the flag
would burn its whole wait() budget and fall through to QThread.terminate() —
on a thread holding a Tantivy searcher or a SQLite handle.

That invariant is invisible at runtime until it bites, so it is pinned here.
"""

import inspect

import pytest

import genizah_app as app

STOP_PATHS = [
    'stop_search',
    '_reset_search',
    'toggle_composition',
    'cancel_composition',
    '_reset_composition',
    'closeEvent',
]


@pytest.mark.parametrize('method', STOP_PATHS)
def test_stop_path_uses_request_cancel(method):
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert 'request_cancel()' in src, (
        '%s does not call request_cancel(); a paused worker would stay parked '
        'until the wait() budget expired and terminate() fired' % method)


@pytest.mark.parametrize('method', STOP_PATHS)
def test_stop_path_does_not_assign_cancel_flag_directly(method):
    """A bare assignment is the exact bug request_cancel() exists to prevent."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert 'cancel_flag = True' not in src, (
        '%s assigns cancel_flag directly instead of calling request_cancel()' % method)


def test_no_direct_cancel_flag_assignment_remains_anywhere():
    src = inspect.getsource(app)
    assert 'cancel_flag = True' not in src


@pytest.mark.parametrize('method', ['stop_search', '_reset_search'])
def test_search_stop_preserves_the_cancelled_flag_ordering(method):
    """CR-114-02: _search_was_cancelled must be set BEFORE the cancel, so a
    cooperative on_search_finished sees it."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert src.index('_search_was_cancelled = True') < src.index('request_cancel()')


@pytest.mark.parametrize('method', STOP_PATHS[:-1])   # closeEvent has no UI left to paint
def test_stop_path_hides_the_pause_button(method):
    """Otherwise a Pause button sits visible over a run that is cancelling."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert '_apply_pause_state' in src and "'hidden'" in src, method


@pytest.mark.parametrize('method', ['start_search', 'run_composition'])
def test_run_entry_points_refuse_to_rebind_a_live_worker(method):
    """A paused worker lives indefinitely, and rebinding self.search_thread over
    a live QThread drops its last reference — "QThread: Destroyed while thread is
    still running", a hard crash."""
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert '_drain_previous_worker' in src, method


def test_drain_refuses_rather_than_rebinding_on_timeout():
    """If the old worker will not stop, starting anyway would leave two live
    workers emitting into the same window."""
    src = inspect.getsource(app.GenizahGUI._drain_previous_worker)
    assert 'return False' in src
    assert 'isRunning()' in src


@pytest.mark.parametrize('method', ['start_search', 'run_composition'])
def test_run_entry_points_stamp_a_fresh_run_id(method):
    src = inspect.getsource(getattr(app.GenizahGUI, method))
    assert '_run_seq' in src and 'run_id' in src, method


def test_grouping_hides_the_pause_button():
    """GroupingThread is not pausable. Showing a greyed-out Pause beside a live
    Stop would claim a capability that does not exist, at exactly the slow tail
    where the user most wants it."""
    src = inspect.getsource(app.GenizahGUI.start_grouping)
    assert '_apply_pause_state' in src and "'hidden'" in src


def test_close_event_no_longer_relies_on_the_no_op_interruption():
    """closeEvent called comp_thread.requestInterruption(), which the composition
    threads never polled. The mixin now routes that override into request_cancel,
    but closeEvent should be reaching it through the shared entry point."""
    src = inspect.getsource(app.GenizahGUI.closeEvent)
    assert 'request_cancel()' in src
