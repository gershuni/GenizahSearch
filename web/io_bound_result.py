# -*- coding: utf-8 -*-
"""``run.io_bound`` that says when it got no answer.

NiceGUI 3.8's ``run.io_bound`` returns None, instead of raising, when the task
awaiting it is cancelled (a time limit, one child of a gather), when the app is
stopping, and when its thread pool has shut down. For a callback that can
itself answer None -- "no restriction", "not found", a write that returns
nothing -- that None cannot be told from a call that never answered.

``io_bound`` here runs the callback through NiceGUI's own ``run.io_bound`` with
its answer in a one-element box. No box: the call was interrupted, and it
returns ``INTERRUPTED``. A box: it returns the callback's own value, None
included. ``INTERRUPTED`` is falsy, so ``if not value`` still reads it as "no
answer"; test ``value is INTERRUPTED`` wherever the difference matters.

INTERRUPTED means "no answer arrived", never "nothing happened": cancelling the
await does not stop the thread, so a write may still complete.

Nor does INTERRUPTED pass a cancellation on. NiceGUI swallows it: a task cancelled
while it awaits ``io_bound`` gets INTERRUPTED back and carries on running (only a
task awaiting a gather of such calls still receives the CancelledError). Code
after the call decides what an interrupted call means, and a task that must stop
when cancelled has to stop itself. Two failures of the callback itself also come
back as INTERRUPTED, as NiceGUI suppresses them: raising ``CancelledError``, and a
``RuntimeError`` reading "cannot schedule new futures after shutdown". Every other
exception the callback raises propagates.

The callback keeps its ``__name__`` (``functools.wraps``), so logs and tests
that recognise it by name still do.
"""
from __future__ import annotations

import functools
from typing import Any, Callable

from nicegui import run


class _Interrupted:
    """The one value ``io_bound`` returns when the call gave no answer."""

    __slots__ = ()

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return 'INTERRUPTED'


INTERRUPTED = _Interrupted()


def _boxed(callback: Callable[..., Any]) -> Callable[..., tuple]:
    @functools.wraps(callback)
    def call(*args: Any, **kwargs: Any) -> tuple:
        return (callback(*args, **kwargs),)
    return call


async def io_bound(callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """``run.io_bound(callback, *args, **kwargs)``: the callback's own value,
    or ``INTERRUPTED`` when the call gave no answer."""
    box = await run.io_bound(_boxed(callback), *args, **kwargs)
    return INTERRUPTED if box is None else box[0]


def interrupted(*values: Any) -> bool:
    """True when any of ``values`` is ``INTERRUPTED`` (the results of a gather)."""
    return any(value is INTERRUPTED for value in values)
