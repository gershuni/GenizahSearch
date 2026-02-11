---
created: 2026-02-11T16:15:29.361Z
title: Fix NiceGUI parent slot deleted error in version selector area
area: web
files:
  - web/components/version_selector.py
  - web/pages/browse.py
---

## Problem

Console error when in the version selector / browse area:

```
RuntimeError: The parent slot of the element has been deleted.
Traceback (most recent call last):
  File "...\nicegui\background_tasks.py", line 93, in _handle_exceptions
    task.result()
  File "...\nicegui\timer.py", line 76, in _run_once
    with self._get_context():
  File "...\nicegui\elements\timer.py", line 12, in _get_context
    return self.parent_slot or nullcontext()
  File "...\nicegui\element.py", line 148, in parent_slot
    raise RuntimeError('The parent slot of the element has been deleted.')
```

The error repeats multiple times in the console. It comes from a NiceGUI `ui.timer` element whose parent slot has been destroyed — likely a timer created inside a dynamically rebuilt UI section (version selector menu, or browse page content) that continues to fire after the parent element is cleared/rebuilt.

## Solution

TBD — investigate which timer(s) in the browse/version selector area outlive their parent elements. Likely fix:
- Deactivate or delete timers before rebuilding the parent UI section
- Use `timer.deactivate()` or check `timer.is_deleted` before running callbacks
- Or restructure to avoid creating timers inside ephemeral UI slots
