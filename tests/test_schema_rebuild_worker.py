"""SEED-006 HIGH-5 desktop wiring — SchemaRebuildWorker + MyLibraryTab hookup.

These cover the off-the-UI-thread rebuild worker and guard the startup wiring
that drives it. The full MyLibraryTab startup path is GUI/QThread-heavy and is
validated by a real desktop smoke test (offscreen Qt can't exercise it safely
in CI); here we test the worker in isolation (run() called directly, no thread)
and pin the wiring with source guards.
"""

import inspect

from PyQt6.QtWidgets import QApplication

# Ensure a QApplication exists for QObject/QThread construction (offscreen).
_app = QApplication.instance() or QApplication([])

from desktop.my_library_tab import MyLibraryTab, SchemaRebuildWorker  # noqa: E402


class _StubIndexer:
    def __init__(self, ready=True, boom=False):
        self.calls = 0
        self._ready = ready
        self._boom = boom

    def run_deferred_schema_rebuild(self):
        self.calls += 1
        if self._boom:
            raise RuntimeError("rebuild blew up")
        return self._ready


def test_worker_runs_deferred_rebuild_and_emits_ready():
    si = _StubIndexer(ready=True)
    got = {}
    w = SchemaRebuildWorker(si)
    w.finished_signal.connect(lambda r: got.setdefault("finished", r))
    w.error_signal.connect(lambda m: got.setdefault("error", m))
    w.run()  # direct call (synchronous) — avoids QThread lifecycle in CI
    assert si.calls == 1
    assert got.get("finished") is True
    assert "error" not in got


def test_worker_emits_error_on_failure():
    si = _StubIndexer(boom=True)
    got = {}
    w = SchemaRebuildWorker(si)
    w.finished_signal.connect(lambda r: got.setdefault("finished", r))
    w.error_signal.connect(lambda m: got.setdefault("error", m))
    w.run()
    assert "finished" not in got
    assert "rebuild blew up" in got.get("error", "")


def test_init_indexer_defers_schema_rebuild():
    # The constructor must ask LocalIndexer to defer the rebuild (never rebuild
    # synchronously on the UI thread) and gate is_searchable while pending.
    src = inspect.getsource(MyLibraryTab._init_indexer)
    assert "defer_schema_rebuild=True" in src
    assert "needs_schema_rebuild" in src
    assert "_awaiting_schema_rebuild" in src


def test_finish_deferred_schema_rebuild_uses_worker_and_gates():
    src = inspect.getsource(MyLibraryTab.finish_deferred_schema_rebuild)
    assert "SchemaRebuildWorker(" in src
    # No-op when nothing pending; fail-open when no indexer.
    assert "_awaiting_schema_rebuild" in src

    done = inspect.getsource(MyLibraryTab._on_schema_rebuild_finished)
    assert "self.is_searchable = True" in done            # unblocks LOCAL search
    err = inspect.getsource(MyLibraryTab._on_schema_rebuild_error)
    assert "self.is_searchable = True" in err             # fail-open on failure


def test_startup_finish_triggers_deferred_rebuild():
    # genizah_app.on_startup_finished must drive the deferred rebuild after the
    # background StartupThread (SearchEngine) has run.
    import genizah_app
    src = inspect.getsource(genizah_app.GenizahGUI.on_startup_finished)
    assert "finish_deferred_schema_rebuild()" in src
