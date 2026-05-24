# Codex Deep Diagnostic — Phase 96 Session Restore + Opt-out Persistence Bug

## Status

Phase 96 has had 8 iterations of fixes attempting to make:
1. **Session restore** work for the Genizah/Local/All corpus scope dropdown
2. **Opt-out persistence** survive close+reopen

Both still fail in the user's real-world testing. Each iteration's tests pass, yet the live behavior is wrong. Something fundamental is being missed across all agents (and previous Codex consult, which only looked at navigation code).

## What's been tried (in order)

**Iter 1** (`e0ee9156`): Added `flush_pending()` to `_UnifiedFileTreeWidget`, claimed wired into closeEvent. Didn't work.

**Iter 2** (`25f43763`): Added `get_file_status_for_folder()` for pre-fill; auto-select via `QTimer.singleShot(300ms)`. Didn't work.

**Iter 7** (`97676224`): Removed 300ms timer from `_refresh_folder_list_ui()`; added `MyLibraryTab.notify_session_restored()` called from `_restore_session()`. Didn't work.

**Iter 8** (`97676224` again — same as iter 7? See latest commits): Discovered iter 7 placed `notify_session_restored()` AFTER try/finally; moved INSIDE finally block with `_notify_done` flag. Also restored corpus scope BEFORE the `has_data` gate. Tests pass; user reports it still doesn't work for either session restore OR opt-outs.

## Investigation requests (do these YOURSELF, don't trust tests)

A. **Trace the session JSON file path** — where is it WRITTEN by `_save_session()` and where is it READ by `_restore_session()`? Are they the same path? Different env-var dependencies? Different working directories at startup vs save time?

B. **Trace ALL call sites of `_save_session()` and `_restore_session()`** — is `_save_session` called on `closeEvent`? If yes, does the flush_pending fire BEFORE the save? If the app force-quits or crashes, is there a save? Is there ANY code path that writes the session AFTER the user toggles checkboxes? Use `grep -rn "_save_session" .` to enumerate every call site.

C. **Verify `_local_file_optouts` is actually written by `_save_session()`** — read the code of `_save_session()` and confirm `local_file_optouts` is in the dict that gets serialized. The user might have been editing on a stale binary or the JSON might be valid but the key name differs.

D. **Verify `_restore_session()` reads `local_file_optouts` and assigns it to `self._local_file_optouts`** — read the code. Check key name match. Check whether `self._local_file_optouts = set(...)` is wrapped in try/except that silently swallows errors.

E. **Verify `notify_session_restored()` actually triggers the tree to repopulate WITH the restored opt-outs** — does the tree iterator that draws checkboxes read from `self._app._local_file_optouts`? Or does it have a local copy that was already snapshotted (with empty set) before restore?

F. **Is there a race** between the worker thread (which writes status to leaves) and the restore (which sets checkboxes)? Could a worker callback fire BEFORE restore and reset the checkbox state?

G. **Are there TWO `_save_session` methods or override-chain situations?** Could a parent class's `_save_session` be the one actually fired (without LOCAL state)?

H. **Diff the session JSON file before close and after restore** — if the user can show us the JSON content, we can see if `local_file_optouts` was actually written. Can you add a `logger.info` in `_save_session` that prints what is being written? And the user just needs to enable logging.

## What to deliver

1. **Definitive root cause** — pinpoint the exact line where the persistence chain breaks
2. **Surgical fix** — one minimal patch that fixes both bugs (or two surgical patches if independent)
3. **Diagnostic logging** — add 5-10 `logger.info` calls at key points so when the user reports "still broken" we can ask for the log output instead of guessing again
4. **Test that mirrors the user's exact scenario** — file open → uncheck → close → reopen → opt-out persisted; corpus dropdown set → close → reopen → corpus preserved

— end brief —
