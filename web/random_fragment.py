"""Shared corpus-wide random manuscript navigation used by public entry pages."""

import random

from nicegui import ui

from web.state import state


def navigate_random_fragment() -> None:
    """Open a uniformly random corpus manuscript, or the browse fallback."""
    try:
        if state.meta_mgr and state.meta_mgr.csv_bank:
            sys_id = random.choice(list(state.meta_mgr.csv_bank.keys()))
            ui.navigate.to(f'/browse?sys_id={sys_id}')
            return
    except Exception:
        pass
    ui.navigate.to('/browse')


__all__ = ['navigate_random_fragment']
