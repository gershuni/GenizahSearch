# Phase 96-09 Fix-10 Summary: Startup-Window Race Condition Silencing the Ask-Restore Prompt

## Which commit introduced the regression

**Commit `215f7f04` (Phase 96 Iter-9)** introduced the regression by changing
`_on_corpus_scope_changed` from a 500ms debounced save to an **immediate**
`self._save_session()` call:

```python
# Before (v7.14.0 / Iter-8 and earlier):
self._schedule_session_save()   # debounced 500ms

# After (Iter-9, commit 215f7f04):
self._save_session()            # immediate
```

In v7.14.0 the debounce timer fired at T+500ms, well after `_restore_session()`
at T+200ms had already read the valid session file.  After Iter-9 the call was
immediate, colliding with the 200ms startup window before `_restore_session()`
ran.

## The 1-2 line surgical fix

**`genizah_app.py` — two locations:**

1. In `GenizahGUI.__init__` (before `self.init_ui()`):

   ```python
   self._restoring_session = True   # guard: blocks _save_session() until restore completes
   ```

2. At the top of `_save_session()`:

   ```python
   if getattr(self, '_restoring_session', False):
       logger.debug("_save_session: skipped (restore in progress)")
       return
   ```

The existing `finally: self._restoring_session = False` in `_restore_session()`
already handles clearing the flag; no further changes were needed.

## Why the user's testing path (restore_mode='ask') was broken

The 200ms startup window between `GenizahGUI.__init__()` and the
`QTimer.singleShot(200, self._restore_session)` callback contained multiple
code paths that called `_save_session()`:

- `_on_corpus_scope_changed` fired when `init_ui()` programmatically set the
  corpus combo's initial index (signals not blocked during widget construction).
- `_UnifiedFileTreeWidget._commit_changes()` fired via its debounce timer when
  a rescan completed during startup.

Each of these calls wrote `results=[]` to `session.json`, overwriting the valid
on-disk session.  On the very next startup:

1. `load_session_state()` loaded the overwritten file with `results=[]`.
2. `has_data = (reg.get('results') or ...)` evaluated to `False`.
3. The `if not has_data: return` early-exit ran BEFORE the `restore_mode='ask'`
   prompt block.
4. No prompt was shown, and opt-outs were not restored to the folder tree.

Because `_restoring_session` was only set to `True` inside `_restore_session()`
itself (not in `__init__`), the guard that was supposed to prevent
`_save_session()` from overwriting during restore was absent for the entire
startup window.

Setting `self._restoring_session = True` in `__init__` before `self.init_ui()`
closes this window: any `_save_session()` call that fires during widget
construction or early background events is silently skipped, and
`_restore_session()` always reads the untouched on-disk session.

## Tests added

`tests/test_session_restore_ask_fix10.py` -- 5 tests (all passing):

| Test | What it verifies |
|------|-----------------|
| F10-A | `_save_session()` skips write when `_restoring_session=True` |
| F10-B | `_save_session()` writes normally when `_restoring_session=False` |
| F10-C | `restore_mode='ask'` with `has_data=False`: opt-outs and corpus still applied; `notify_session_restored()` fires |
| F10-D | AST: `_restoring_session = True` appears before `self.init_ui()` in `__init__` |
| F10-E | AST: `_restoring_session` guard appears before `save_session_state()` in `_save_session()` |

## Commit

`d461a71b` -- fix(96-09): close startup-window race that silenced the ask-restore prompt
