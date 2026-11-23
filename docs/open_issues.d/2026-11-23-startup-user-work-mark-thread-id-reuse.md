## P3 desktop: A search that reuses a terminated worker's thread id can lose its user-work mark
Where: `shared/background_work.py::user_work_started`
Opened: 2026-11-23

The desktop's startup builds pause while a search runs (PR #111). Each search worker marks its
thread, keyed by thread id, with an `alive()` callback (the worker's `QThread.isRunning`). A
build waiting at a checkpoint drops the marks whose `alive()` is False, so a worker stopped by
`QThread.terminate()` does not hold the builds back.

`user_work_started` returns early when the thread id is already marked. Suppose a worker is
terminated and a new search starts on a thread that reuses its OS thread id, before the startup
queue's next checkpoint. The new worker then keeps the old worker's `alive()`, and the next
checkpoint drops its mark. The phase-2 builds go on beside that search: it runs slower, as it
did before PR #111, until it ends.

This is reachable where a worker is terminated and another started at once: the Join Lab's
search after `wait(3000)` (`desktop/join_workbench.py`, `self._search_thread.terminate()`). It
needs a terminated search, a reused thread id, and a startup queue still in phase 2. Found by
Codex on PR #111 after the review loop was capped. Read in the code, not reproduced.

Fix sketch: in `user_work_started`, replace an existing mark whose recorded `alive()` is False
(or is not the calling thread's callback), instead of returning early.
