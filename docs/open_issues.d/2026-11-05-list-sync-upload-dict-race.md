## P3 desktop: A list upload racing the user's edits can fail with "dictionary changed size during iteration"
Where: `shared/lists_sync.py` (the upload's `try` block, `logger.error(f"Error syncing to cloud: {e}")` near line 1196); `tests/test_list_sync_identity.py::test_an_auto_upload_racing_the_user_on_another_thread_succeeds`
Opened: 2026-11-05

On CI run 42001452091 (PR #483, which touches nothing in list sync), the Windows shard 1/3 failed
the race test. The second of its four uploads returned
`error = 'dictionary changed size during iteration'`, logged as
`ERROR shared.lists_sync: Error syncing to cloud: dictionary changed size during iteration`. The
test runs an upload while another thread adds entries to the same store (switch interval
1e-5 s), and it passed on the other shards and on earlier runs. So somewhere in the upload path a
dict the user's thread mutates is iterated live. Most loops over `store['items']` already copy
first (`list(...values())`, lines 275, 327, 358, 412, 422, 524, 1968, 1977, 2771), so the site is
another one: a nested dict of an entry or list, or a generator such as `_records(item)`.

**Not identified:** the error is logged without a traceback, so the run does not say which loop
it was. In production this means an upload round fails and is retried later; no data is lost, as
far as the test shows.

Fix sketch: log that exception with `exc_info=True` so the next occurrence names the line; then
copy that iteration (or snapshot the store under the lists manager's lock at the start of the
upload). Run the race test in a loop on Windows (`pytest -k racing_the_user --count 50` with
pytest-repeat, or a shell loop) to confirm it.
