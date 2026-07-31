"""Safety helpers for deferred renders.

A deferred loader fetches off the event loop and then mutates NiceGUI elements.
Two things can go wrong, and this module covers both:

* the browser tab closed while the fetch was in flight, so the mutation targets a
  client whose elements are gone -- guard the render with :func:`client_gone`;
* the fetch failed, and the caller's ``except`` swallowed it while leaving the
  loading spinner on screen forever -- replace it with :func:`show_load_error`.

WHY THIS MODULE EXISTS
----------------------
This used to be written inline at nine call sites as::

    if container.client.has_been_deleted:
        return

``Client.has_been_deleted`` **does not exist** in NiceGUI (checked against 3.8.0)
and never has. Reading it raises ``AttributeError`` -- not on teardown, but on
*every* invocation, including the ordinary happy path. Because each caller
wrapped the render in ``except Exception``, the failure surfaced as a spinner
that never resolved: "Loading your edits..." forever, on /corrections (edits,
comments, leaderboard), the homepage recent-items panel, and discovery replies.

The name predates the 2026-07-30 perf work (two sites came in with ``c39838c3``),
but that work copied it onto four main-path loaders and so converted a latent
bug into a total failure of those surfaces in v8.5.2.

Two things let it through review and CI:

* the regression tests asserted the *string* ``'has_been_deleted'`` appeared in
  the loader source, so they pinned the misspelling instead of exercising it;
* no test ever ran a guard against a real ``nicegui.Client``.

``tests/test_client_guard.py`` therefore drives this helper with a genuine
``Client`` and a genuine deleted ``Client``, and a repo-wide guard forbids the
dead attribute name from coming back. Do not reintroduce an inline check.
"""

from __future__ import annotations

from nicegui import Client

__all__ = ['client_gone', 'show_load_error']


def client_gone(element) -> bool:
    """Return ``True`` when ``element``'s client is gone and must not be touched.

    ``Client.delete()`` does ``del Client.instances[self.id]``, so absence from
    ``Client.instances`` is the public, documented-by-behaviour signal that the
    client has been torn down. There is no public ``deleted`` property in
    NiceGUI 3.8.0 -- only a private ``_deleted`` flag -- so membership is the
    supported check.

    Deliberately NOT ``has_socket_connection``: that is ``False`` during initial
    page construction, before the websocket handshake, so using it here would
    skip the very first render of every page -- the same class of bug in the
    opposite direction.

    Fails **open** (returns ``False``, i.e. "go ahead and render"): an element
    with no resolvable client is far more likely to be a test double or an
    unusual-but-live context than a torn-down page, and a spurious ``True``
    silently blanks a working surface. A genuine teardown that slips past this
    check raises ``RuntimeError`` at the render itself, which every caller
    already handles.
    """
    client = getattr(element, 'client', None)
    if client is None:
        return False
    client_id = getattr(client, 'id', None)
    if client_id is None:
        return False
    try:
        return client_id not in Client.instances
    except Exception:  # pragma: no cover - defensive; instances is a plain dict
        return False


def show_load_error(container) -> None:
    """Replace a container's loading spinner with a visible failure message.

    Call this from a deferred loader's ``except`` branch. Without it, a swallowed
    exception leaves the spinner spinning: the user waits forever on "Loading
    your edits..." and nothing on screen says the load failed. That is exactly
    how the ``has_been_deleted`` bug reached production unnoticed -- the surface
    looked merely slow rather than broken.

    Deliberately vague to the user: the underlying error is already logged
    server-side at WARNING, and the reader gains nothing from a Supabase
    traceback. Self-guarding -- safe to call when the client is already gone,
    which is the common case for a genuine teardown.
    """
    if client_gone(container):
        return
    try:
        from nicegui import ui

        from web.translations import tr

        container.clear()
        with container:
            with ui.column().classes('w-full items-center py-8'):
                ui.icon('error_outline').classes('text-4xl').style('color: var(--danger);')
                ui.label(tr('Could not load this section. Please refresh the page.')) \
                    .classes('mt-2').style('color: var(--text-secondary);')
    except RuntimeError:
        # Client torn down between the guard and the render -- nothing to show.
        return
