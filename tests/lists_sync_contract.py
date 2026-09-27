# -*- coding: utf-8 -*-
"""What the desktop window's list-sync code uses from desktop/lists_sync_runner.py and
from ListsManager, for tests on a branch that may not have them yet.

The window imports the runner module lazily (GenizahGUI._lists_sync_runner and
_sync_dialog_allowed) and calls a few ListsManager reads that come with it. Each stand-in
here is used only while the real one is missing -- once desktop/lists_sync_runner.py and
the ListsManager methods exist, these helpers return or leave the real ones -- so the
same tests then run against them.

- `sync_dialog_allowed` -- the dialog gate: true only when the window is visible, no
  session restore runs, no modal dialog is open, no sign-out is pending and no close is
  pending or under way.
- `runner_module(monkeypatch)` -- the real module, or a stand-in in sys.modules holding
  `sync_dialog_allowed` and `ListsSyncRunner` (= InlineRunner).
- `inline_runner(mgr)` -- ListsSyncRunner(mgr, inline=True): every job runs at once on
  the calling thread and calls on_done before run() returns.
- `add_missing_manager_reads(monkeypatch)` -- ListsManager.saves_failing() (True while
  saves do not reach lists.pkl), pending_web_removals() (nothing pending) and
  resolve_web_removals(), where absent.
"""
import importlib
import sys
import types

RUNNER_MODULE = "desktop.lists_sync_runner"


def sync_dialog_allowed(visible, restoring, modal_open, logout_pending, closing):
    return bool(visible and not restoring and not modal_open and not logout_pending and not closing)


class InlineRunner:
    """The runner's calls as the window makes them, each job run at once."""

    busy = False

    def __init__(self, lists_mgr, parent=None, on_auto_done=None, inline=True):
        self.mgr = lists_mgr
        self.on_auto_done = on_auto_done
        self.auth_epoch = 0
        self.unsent = False
        self.calls = []
        self._closed = False

    def run(self, kind, on_done=None, on_progress=None):
        self.calls.append(("run", kind))
        if self._closed:
            return None
        outcome = {"cancelled": False}
        if kind == "preview":
            outcome["preview"] = self.mgr.get_cloud_lists_preview()
        elif kind in ("download", "merge"):
            outcome["download"] = self.mgr.sync_from_cloud()
            if kind == "merge" and outcome["download"].get("success"):
                outcome["upload"] = self._upload()
        else:
            outcome["upload"] = self._upload()
        job = types.SimpleNamespace(kind=kind, epoch=self.auth_epoch, deadline=None, done=True)
        if on_done is not None:
            on_done(outcome)
        return job

    def _upload(self):
        result = self.mgr.sync_to_cloud()
        self.unsent = not result.get("success")
        return result

    def cancel(self, job):
        self.calls.append(("cancel", job))

    def request_auto(self):
        self.calls.append(("request_auto",))

    def allow_auto(self):
        self.calls.append(("allow_auto",))

    def mark_dirty(self):
        self.calls.append(("mark_dirty",))
        self.unsent = True

    def invalidate_auth(self):
        self.calls.append(("invalidate_auth",))
        self.auth_epoch += 1

    def begin_logout(self, on_done, budget_s):
        self.calls.append(("begin_logout", budget_s))
        return self.run("logout", on_done=on_done)

    def shutdown(self):
        self.calls.append(("shutdown",))
        self._closed = True


def runner_module(monkeypatch):
    """desktop.lists_sync_runner, or a stand-in for it while it does not exist."""
    try:
        return importlib.import_module(RUNNER_MODULE)
    except ImportError:
        pass
    module = types.ModuleType(RUNNER_MODULE)
    module.sync_dialog_allowed = sync_dialog_allowed
    module.ListsSyncRunner = InlineRunner
    monkeypatch.setitem(sys.modules, RUNNER_MODULE, module)
    import desktop
    monkeypatch.setattr(desktop, "lists_sync_runner", module, raising=False)
    return module


def inline_runner(mgr, on_auto_done=None):
    try:
        from desktop.lists_sync_runner import ListsSyncRunner
    except ImportError:
        return InlineRunner(mgr, on_auto_done=on_auto_done)
    return ListsSyncRunner(mgr, inline=True, on_auto_done=on_auto_done)


def add_missing_manager_reads(monkeypatch):
    from shared.lists_manager import ListsManager
    if not hasattr(ListsManager, "saves_failing"):
        monkeypatch.setattr(ListsManager, "saves_failing",
                            lambda self: bool(self._saves_failing), raising=False)
    if not hasattr(ListsManager, "pending_web_removals"):
        monkeypatch.setattr(ListsManager, "pending_web_removals", lambda self: [], raising=False)
    if not hasattr(ListsManager, "resolve_web_removals"):
        monkeypatch.setattr(ListsManager, "resolve_web_removals",
                            lambda self, choices: (0, 0), raising=False)
