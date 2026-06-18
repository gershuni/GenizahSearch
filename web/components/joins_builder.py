# -*- coding: utf-8 -*-
"""Joins Lab line-builder widget (web, Phase 118 BLD-03).

Provides:
  _apply_modifiers_to_term(term, mods) → str
      Pure helper: applies per-row modifier flags to a user-typed term before
      constructing a BuilderRow. Desktop parity (RR-13): wildcard_prefix is NOT
      applied to slash-groups (multi-token terms containing '/').

  build_side_query(rows_state, variants, page_position) → Optional[SideQuery]
      Converts a list of row-state dicts into a SideQuery.
      Returns None when all rows are empty (empty-builder guard).

  create_joins_builder(allow_page_position=True) → dict
      NiceGUI factory producing a widget handle dict with:
        build_side_query()    → Optional[SideQuery]
        get_mode()            → str ('exact'|'variants'|'fuzzy')
        get_text_position()   → str (one of 5 option values)
        get_summary()         → str (human-readable for collapsed summary bar)
        is_empty()            → bool
        container             (NiceGUI element — mount point for the page)

No raw app.storage.user access. All per-user state is closure-local (Phase 87 invariant).
"""
from __future__ import annotations

from typing import Optional

from nicegui import ui

from shared.joins_lab import BuilderRow, SideQuery
from web.translations import tr


# ---------------------------------------------------------------------------
# Pure modifier-hoist helper (BLD-03)
# ---------------------------------------------------------------------------

def _apply_modifiers_to_term(term: str, mods: dict) -> str:
    """Apply per-row modifier flags to a user-typed term.

    Desktop parity: desktop/join_workbench.py:1272-1347 (RR-13 rules).

    The web builder accepts a single text field (D-04 Responsa syntax):
      - space = token sequence, a/b = OR-alternatives
      - '/' inside a term makes it a slash-group (wrapped in parens)

    Modifier application order (mirrors genizah_core.py:6014-6027):
      negation → returns '-{wrapped}' immediately (overrides all other mods)
      else:
        plene         → '%' prefix
        prefix        → '#' prefix
        suffix        → '#' suffix (appended)
        wildcard_prefix → '*' prefix  (ONLY when NOT a slash-group — RR-13)
        wildcard_suffix → '*' suffix (appended)

    line_start / line_end are NOT text transforms — they become BuilderRow flags
    and are handled in build_side_query().
    """
    t = term.strip()
    if not t:
        return t

    # Determine if this is a slash-group (multi-token alternative expression)
    # e.g. 'א/ב' is a slash-group; '(א/ב)' is already wrapped (skip)
    is_group = '/' in t and not t.startswith('(')
    wrapped = f'({t})' if is_group else t

    if mods.get('negation'):
        return f'-{wrapped}'

    if mods.get('plene'):
        wrapped = f'%{wrapped}'
    if mods.get('prefix'):
        wrapped = f'#{wrapped}'
    if mods.get('suffix'):
        wrapped = f'{wrapped}#'
    # wildcard_prefix NOT supported on slash-groups (RR-13 parity — parser limitation)
    if mods.get('wildcard_prefix') and not is_group:
        wrapped = f'*{wrapped}'
    if mods.get('wildcard_suffix'):
        wrapped = f'{wrapped}*'

    return wrapped


# ---------------------------------------------------------------------------
# Row-state → SideQuery converter
# ---------------------------------------------------------------------------

def build_side_query(
    rows_state: list,
    variants: bool,
    page_position: Optional[str],
) -> Optional[SideQuery]:
    """Build a SideQuery from a list of row-state dicts.

    Each entry in rows_state: {'term': str, 'mods': dict, 'gap_to_next': int}

    page_position must be None, 'start', or 'end' — the caller is responsible
    for NOT passing 'line_start' or 'line_end' here; those bypass SideQuery
    and go directly to execute_search(text_position=...) in Plan 04.

    Returns None when all rows have empty terms (empty-builder guard).
    """
    builder_rows = []
    for rs in rows_state:
        hoisted = _apply_modifiers_to_term(rs.get('term', ''), rs.get('mods', {}))
        builder_rows.append(BuilderRow(
            term=hoisted,
            line_start=rs.get('mods', {}).get('line_start', False),
            line_end=rs.get('mods', {}).get('line_end', False),
            gap_to_next=int(rs.get('gap_to_next', 0) or 0),
        ))

    # Empty-builder guard: if no row has a non-empty stripped term, return None
    if not any(r.term.strip() for r in builder_rows):
        return None

    return SideQuery(
        rows=tuple(builder_rows),
        variants=variants,
        page_position=page_position,
    )


# ---------------------------------------------------------------------------
# NiceGUI widget factory
# ---------------------------------------------------------------------------

# WR-03: Text Position labels must be resolved at REQUEST time (inside
# create_joins_builder), NOT at module import. tr() reads the process-global
# _current_lang set per-request by web/main.py; a module-level dict freezes the
# labels at the import-time default ('he'), so an English visitor would see
# Hebrew labels that never update. The English KEYS are stable and used for
# state/routing; only the displayed VALUES need deferred tr(). (See _MODIFIER_KEYS
# below, which already defers tr() via lambda for the same reason.)
_TEXT_POSITION_KEYS = ['anywhere', 'start', 'end', 'line_start', 'line_end']
_TEXT_POSITION_LABEL_KEYS = {
    'anywhere': 'Anywhere',
    'start': 'Start of text',
    'end': 'End of text',
    'line_start': 'Line starts',
    'line_end': 'Line ends',
}


def _text_position_options() -> dict:
    """Build the Text Position {value: label} options at request time (WR-03)."""
    return {k: tr(_TEXT_POSITION_LABEL_KEYS[k]) for k in _TEXT_POSITION_KEYS}


_MODIFIER_KEYS = [
    ('line_start',       lambda: tr('Line start (⊢)')),
    ('line_end',         lambda: tr('Line end (⊣)')),
    ('plene',            lambda: tr('Plene / defective')),
    ('prefix',           lambda: tr('Prefix')),
    ('suffix',           lambda: tr('Suffix')),
    ('wildcard_prefix',  lambda: tr('Wildcard prefix')),
    ('wildcard_suffix',  lambda: tr('Wildcard suffix')),
    ('negation',         lambda: tr('Negation')),
]


def create_joins_builder(allow_page_position: bool = True) -> dict:
    """Factory: creates and mounts the Joins Lab line-builder widget.

    Returns a handle dict with:
      container          — the top-level NiceGUI element (mount point)
      build_side_query() — builds a SideQuery from current state
      get_mode()         — 'exact' | 'variants' | 'fuzzy'
      get_text_position() — one of 'anywhere'|'start'|'end'|'line_start'|'line_end'
      get_summary()      — human-readable summary string for the collapsed bar
      is_empty()         — True when all builder rows are blank

    When allow_page_position=False (other side, D-13 parity):
      - Text Position control is hidden
      - build_side_query() always passes page_position=None
    """
    # Mutable in-memory state (closure-local, not app.storage.user — Phase 87 invariant)
    rows_state: list = [{'term': '', 'mods': {}, 'gap_to_next': 0}]
    mode_state: dict = {'mode': 'exact'}
    text_position_state: dict = {'value': 'anywhere'}

    # ---- internal helpers -----------------------------------------------

    def _get_mode() -> str:
        return mode_state['mode']

    def _get_text_position() -> str:
        return text_position_state['value']

    def _is_empty() -> bool:
        return not any(rs.get('term', '').strip() for rs in rows_state)

    def _get_summary() -> str:
        # WR-04: every literal here must go through tr() — the collapsed summary
        # bar is shown after EVERY search (joins_lab._collapse_builder), and the
        # default UI is Hebrew. Mode/Text-Position labels reuse the same English
        # keys translated elsewhere; pluralization uses singular/plural tr() keys.
        _mode_label_keys = {'exact': 'Exact', 'variants': 'Variants', 'fuzzy': 'Fuzzy'}
        mode_raw = _get_mode()
        mode = tr(_mode_label_keys.get(mode_raw, mode_raw.capitalize()))
        n = len(rows_state)
        lines_label = (
            tr('{n} line').format(n=n) if n == 1
            else tr('{n} lines').format(n=n)
        )
        if allow_page_position:
            tp = _get_text_position()
            pos_label = tr(_TEXT_POSITION_LABEL_KEYS.get(tp, tp))
            return f'{mode} · {lines_label} · ' + tr('Text Position: {pos}').format(pos=pos_label)
        return f'{mode} · {lines_label}'

    def _build_sq() -> Optional[SideQuery]:
        """Build SideQuery from current widget state.

        Text position routing:
          'start'/'end'       → page_position in SideQuery
          'anywhere'          → page_position=None
          'line_start'/'line_end' → page_position=None here; Plan 04 passes
                                    these directly to execute_search(text_position=...)
        """
        if not allow_page_position:
            page_pos = None
        else:
            tp = _get_text_position()
            if tp in ('start', 'end'):
                page_pos = tp
            else:
                page_pos = None  # 'anywhere' / 'line_start' / 'line_end' handled by Plan 04

        variants = (_get_mode() == 'variants')
        return build_side_query(rows_state, variants, page_pos)

    # ---- row rendering --------------------------------------------------

    # Container for the rows section (re-rendered on add/remove)
    rows_container: dict = {'el': None}

    def _render_rows(parent_el) -> None:
        """Render all builder rows inside parent_el, clearing it first."""
        parent_el.clear()
        with parent_el:
            for i in range(len(rows_state)):
                _render_row(i)
                if i < len(rows_state) - 1:
                    _render_gap_control(i)
            # Add line button
            ui.button(tr('+ Add line'), icon='add').props('flat small').classes(
                'text-xs mt-2'
            ).style('color: var(--text-secondary);').on(
                'click', lambda: _add_row()
            )

    def _render_row(idx: int) -> None:
        """Render one builder row at index idx."""
        rs = rows_state[idx]
        with ui.row().classes('w-full items-center gap-2'):
            # Row number label (right-aligned, 12px, muted)
            ui.label(str(idx + 1)).classes('text-xs w-5 text-right shrink-0').style(
                'color: var(--text-muted);'
            )

            # Main text input (RTL, Hebrew serif, outlined)
            placeholder = (
                tr('Words on this line (space = sequence, a/b = alternatives)')
                if allow_page_position
                else tr('Words on this side (space = sequence, a/b = alternatives)')
            )
            term_input = ui.input(placeholder=placeholder).props(
                'outlined dense'
            ).classes('flex-grow').style(
                'direction: rtl; text-align: right;'
                ' font-family: "Noto Sans Hebrew", "SBL Hebrew", serif; font-size: 1rem;'
            )
            term_input.value = rs.get('term', '')

            def _on_term_change(v, i=idx):
                rows_state[i]['term'] = v

            term_input.on('update:model-value', lambda e, i=idx: _on_term_change(e.args, i))

            # Tune icon button → per-row modifier popover
            has_active_mod = any(rs.get('mods', {}).get(k, False) for k, _ in _MODIFIER_KEYS)
            tune_style = (
                'color: var(--primary-600);' if has_active_mod else 'color: var(--text-secondary);'
            )
            with ui.button(icon='tune').props('flat dense size=sm').style(
                tune_style
            ).tooltip(tr('Line options')) as _tune_btn:
                with ui.menu():
                    for mod_key, mod_label_fn in _MODIFIER_KEYS:
                        cb = ui.checkbox(
                            mod_label_fn(),
                            value=rs.get('mods', {}).get(mod_key, False),
                        )

                        def _on_mod_change(v, i=idx, k=mod_key):
                            if 'mods' not in rows_state[i]:
                                rows_state[i]['mods'] = {}
                            rows_state[i]['mods'][k] = bool(v)

                        cb.on('update:model-value', lambda e, i=idx, k=mod_key: _on_mod_change(e.args, i, k))

            # Remove button — hidden when only one row remains
            remove_btn = ui.button(icon='close').props('flat dense size=sm color=negative')
            if len(rows_state) <= 1:
                remove_btn.set_visibility(False)
            else:
                remove_btn.on('click', lambda i=idx: _remove_row(i))

    def _render_gap_control(idx: int) -> None:
        """Render the gap control between row idx and idx+1."""
        rs = rows_state[idx]
        gap_val = rs.get('gap_to_next', 0)
        border_color = 'var(--border-focus)' if gap_val > 0 else 'var(--neutral-300)'
        with ui.row().classes('items-center gap-2 py-1'):
            ui.label(tr('↕ gap')).classes('text-xs').style(
                'color: var(--text-tertiary);'
            )
            gap_input = ui.number(
                value=gap_val,
                min=0, max=20, step=1,
            ).props('outlined dense').style(
                f'width: 56px; border-color: {border_color};'
            )

            def _on_gap_change(v, i=idx):
                rows_state[i]['gap_to_next'] = int(v or 0)
                # Re-render to update border color
                _render_rows(rows_container['el'])

            gap_input.on('update:model-value', lambda e, i=idx: _on_gap_change(e.args, i))

    # ---- add / remove row -----------------------------------------------

    def _add_row():
        rows_state.append({'term': '', 'mods': {}, 'gap_to_next': 0})
        _render_rows(rows_container['el'])

    def _remove_row(idx: int):
        if len(rows_state) > 1:
            rows_state.pop(idx)
        _render_rows(rows_container['el'])

    # ---- build the widget -----------------------------------------------

    with ui.column().classes('w-full gap-3') as container:

        # Header row: Text Position (prominent) + mode selector
        with ui.row().classes('w-full items-center gap-4 flex-wrap'):
            if allow_page_position:
                with ui.column().classes('gap-1'):
                    ui.label(tr('Text Position')).classes('text-xs font-bold uppercase').style(
                        'color: var(--text-secondary); letter-spacing: 0.05em;'
                    )
                    text_pos_select = ui.select(
                        options=_text_position_options(),
                        value=text_position_state['value'],
                    ).props('outlined dense').classes('w-40')

                    def _on_tp_change(v):
                        text_position_state['value'] = v

                    text_pos_select.on('update:model-value', lambda e: _on_tp_change(e.args))

            # Mode selector: Exact / Variants / Fuzzy  (flat toggle buttons)
            with ui.row().classes('items-center gap-1'):
                ui.label(tr('Mode')).classes('text-xs font-bold uppercase').style(
                    'color: var(--text-secondary); letter-spacing: 0.05em; margin-right: 4px;'
                )

                mode_btns: dict = {}

                def _set_mode(m: str):
                    mode_state['mode'] = m
                    for mm, btn in mode_btns.items():
                        if mm == m:
                            btn.props('color=primary')
                        else:
                            btn.props(remove='color=primary')
                            btn.props('flat')
                    # Show/hide fuzzy hint
                    fuzzy_hint.set_visibility(m == 'fuzzy')

                for mode_val, mode_label in [
                    ('exact',    tr('Exact')),
                    ('variants', tr('Variants')),
                    ('fuzzy',    tr('Fuzzy')),
                ]:
                    is_active = (mode_val == mode_state['mode'])
                    btn_props = 'color=primary' if is_active else 'flat'
                    b = ui.button(mode_label).props(f'{btn_props} size=sm').on(
                        'click', lambda m=mode_val: _set_mode(m)
                    )
                    mode_btns[mode_val] = b

            # Fuzzy hint (shown only when fuzzy selected)
            fuzzy_hint = ui.label(
                tr('Fuzzy search is slower and uses more server resources.')
            ).classes('text-xs').style(
                'color: var(--text-muted);'
            )
            fuzzy_hint.set_visibility(mode_state['mode'] == 'fuzzy')

        # Rows area (re-rendered as rows are added/removed)
        rows_area = ui.column().classes('w-full gap-0')
        rows_container['el'] = rows_area
        _render_rows(rows_area)

    # ---- return handle dict ---------------------------------------------

    return {
        'container': container,
        'build_side_query': _build_sq,
        'get_mode': _get_mode,
        'get_text_position': _get_text_position,
        'get_summary': _get_summary,
        'is_empty': _is_empty,
    }
