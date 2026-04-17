"""Helpers for deterministic browse-page bootstrap state."""

from __future__ import annotations

from typing import Any, Dict


def resolve_browse_bootstrap(
    *,
    initial_fl_id: str | None,
    initial_sys_id: str | None,
    initial_page: int,
    pending_shelfmark: str | None,
    saved_reading_desk: dict | None,
    saved_position: dict | None,
) -> Dict[str, Any]:
    """Resolve browse bootstrap action without scheduling async tasks.

    Pure function: no storage reads, no NiceGUI calls, no async scheduling.
    The caller reads storage, passes the values here, then uses the returned
    dict to dispatch the correct load_page() variant (Pitfall 3 - scheduling
    stays with the caller so the Cat-2 deferred-init comment remains local
    to create_browse_page()).

    Returned dict keys:
        action:       'fl_id' | 'sys_id' | 'shelfmark'
                      | 'restore_desk' | 'restore_position' | 'none'
        p_num:        int - page number to load (1-indexed; default from initial_page)
        fl_id:        str | None
        sys_id:       str | None
        shelfmark:    str | None  - for restore_position, the saved shelfmark;
                                    dispatch uses this to set state.shelfmark_query
                                    (Codex HIGH #6 - preserves browse.py:4519 behavior)
        volume_ie:    str | None  - for restore_position, the saved volume_ie;
                                    dispatch validates it against get_volumes_for_sys_id
                                    before assignment (Codex HIGH #6 - preserves
                                    browse.py:4521-4527 behavior)
        restore_desk: bool - true if caller should run its desk-restore flow
        clear_desk:   bool - true if caller should pop stale reading_desk_state

    Precedence (matches browse.py:4446-4512 source of truth):
        1. initial_fl_id -> action='fl_id'
        2. initial_sys_id WITH matching reading desk entry -> action='restore_desk'
        3. initial_sys_id WITHOUT matching desk -> action='sys_id', clear_desk=True if desk exists
        4. pending_shelfmark (no sys_id) -> action='shelfmark'
        5. blank URL + saved reading desk -> action='restore_desk'
        6. blank URL + saved browse_position -> action='restore_position'
        7. blank URL, nothing saved -> action='none'
    """
    # Default result skeleton.
    result: Dict[str, Any] = {
        'action': 'none',
        'p_num': initial_page,
        'fl_id': None,
        'sys_id': None,
        'shelfmark': None,
        'volume_ie': None,
        'restore_desk': False,
        'clear_desk': False,
    }

    # Branch 1: fl_id wins all.
    if initial_fl_id:
        result['action'] = 'fl_id'
        result['fl_id'] = initial_fl_id
        return result

    # Branch 2 / 3: explicit sys_id, with reading-desk collision check.
    if initial_sys_id:
        desk_entries = (saved_reading_desk or {}).get('entries') or []
        persisted_sids = {e.get('sys_id', '') for e in desk_entries}
        if initial_sys_id in persisted_sids:
            # Language-switch: restore the full reading desk.
            result['action'] = 'restore_desk'
            result['sys_id'] = initial_sys_id
            result['restore_desk'] = True
            return result
        # Cross-page navigation: clear stale desk, load requested manuscript.
        result['action'] = 'sys_id'
        result['sys_id'] = initial_sys_id
        result['clear_desk'] = bool(desk_entries)
        return result

    # Branch 4: pending_shelfmark search.
    if pending_shelfmark:
        result['action'] = 'shelfmark'
        result['shelfmark'] = pending_shelfmark
        return result

    # Branch 5: blank URL + saved reading desk wins over position.
    desk_entries = (saved_reading_desk or {}).get('entries') or []
    if desk_entries:
        result['action'] = 'restore_desk'
        result['restore_desk'] = True
        return result

    # Branch 6: blank URL + saved position.
    if saved_position and saved_position.get('sys_id'):
        result['action'] = 'restore_position'
        result['sys_id'] = saved_position['sys_id']
        result['p_num'] = int(saved_position.get('p_num', 1) or 1)
        result['shelfmark'] = saved_position.get('shelfmark')
        result['volume_ie'] = saved_position.get('volume_ie')  # Codex HIGH #6
        return result

    # Branch 7: nothing to do.
    return result
