## [Unreleased] -- The /browse correction tests stop depending on timing (ci, branch `claude/correction-off-loop-test-timing`)

`tests/test_browse_correction_off_loop.py` (from #430) failed in two ways that
said nothing about the website:

- **CI on 0fb81585**: `test_two_quick_save_draft_clicks...` raised
  `IndexError: pop from empty list` at its own `with client:`. A page task an
  earlier test left pending inside `with client:` on its closed loop (the log
  names the joins button's `_runner`) was garbage-collected during this test,
  and its `with` exit popped the slot stack of the task then running -- NiceGUI
  keys slot stacks by task id. Reproduced with a probe; the `editor` fixture now
  runs `gc.collect()` before the test opens any slot.
- **Run alone on Windows with NiceGUI 3.18**, the typed-during-save test failed
  every time: the loop was busy for ~0.35 s at the start of the save, so its
  `sleep(0.1)` typed after a 0.3 s write. Every step that must happen "while the
  write runs" (typing, a page turn, the second click) now waits until the fake
  write is running in its worker, and the write answers only after that step.

Each test still fails when the code it guards is broken: no lock on Save Draft
or Submit, typed text marked Saved, a late draft id on the new page.
