"""The one way a web page runs a Lists write: signed in, awaited, and honest about the result.

Used by ``web/pages/lists.py`` and ``web/components/project_tree.py`` (the /lists sidebar).
``tests/test_lists_page_write_callbacks.py`` pins that every lists-manager write in those files
goes through ``run_lists_write`` and that its result is checked before anything reports success.
"""

import logging

from nicegui import ui

from web.auth_state import GlobalAuthState
from web.translations import tr

logger = logging.getLogger(__name__)


async def run_lists_write(write, *, falsy_is_failure: bool = True):
    """Run one lists write for the signed-in user; return its result, or None.

    Call it as ``result = await run_lists_write(lambda: lists_mgr.<method>(...))`` followed
    by ``if not result: ... return``.

    * A page's sign-in gate runs once, at render; a session can end while the page stays
      open, so sign-in is re-checked here, before any write.
    * ``write`` returns the manager's coroutine and it is AWAITED here: the
      UserListsManager write methods are async, and a bare call never runs.
    * A write that raises or reports nothing done (None / False) toasts a failure and
      returns None. Pass ``falsy_is_failure=False`` when a falsy result is a legitimate
      answer (Empty Trash can delete 0 lists).
    """
    if not GlobalAuthState.is_logged_in():
        ui.notify(tr('Please log in to access lists'), type='warning')
        return None
    try:
        result = await write()
    except Exception as e:
        logger.warning("lists write failed: %s", e)
        result = None
    if result is None or (falsy_is_failure and not result):
        ui.notify(tr('The change could not be saved. Check your connection and try again.'), type='negative')
        return None
    return result
