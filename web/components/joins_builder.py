# -*- coding: utf-8 -*-
"""Joins Lab line-builder widget (web, Phase 118 BLD-03).

Provides:
  _apply_modifiers_to_term(term, mods) -> str
      Pure helper: applies per-word modifier flags to a user-typed term before
      constructing a BuilderRow. Desktop parity (RR-13): wildcard_prefix is NOT
      applied to slash-groups (multi-token terms containing '/').

  build_side_query(lines_state, variants, page_position) -> Optional[SideQuery]
      Converts a lines-with-words state list into a SideQuery.
      Each entry in lines_state:
        {
          'words': [{'term': str, 'mods': dict, 'gap_to_next_word': int}, ...],
          'line_start': bool,
          'line_end': bool,
          'gap_to_next_line': int,
        }
      Returns None when no word in any line has a non-empty term.

  create_joins_builder(allow_page_position=True) -> dict
      NiceGUI factory producing a widget handle dict with:
        build_side_query()    -> Optional[SideQuery]
        get_mode()            -> str ('exact'|'variants'|'fuzzy')
        get_text_position()   -> str (one of 5 option values)
        get_summary()         -> str (human-readable for collapsed summary bar)
        is_empty()            -> bool
        container             (NiceGUI element - mount point for the page)

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
    """Apply per-word modifier flags to a user-typed term.

    Desktop parity: desktop/join_workbench.py:1272-1347 (RR-13 rules).

    The web builder accepts a single text field (D-04 Responsa syntax):
      - space = token sequence, a/b = OR-alternatives
      - '/' inside a term makes it a slash-group (wrapped in parens)

    Modifier application order (mirrors genizah_core.py:6014-6027):
      negation -> returns '-{wrapped}' immediately (overrides all other mods)
      else:
        plene         -> '%' prefix
        prefix        -> '#' prefix
        suffix        -> '#' suffix (appended)
        wildcard_prefix -> '*' prefix  (ONLY when NOT a slash-group - RR-13)
        wildcard_suffix -> '*' suffix (appended)

    line_start / line_end are NOT text transforms - they become BuilderRow flags
    and are handled in build_side_query().
    """
    t = term.strip()
    if not t:
        return t

    # Determine if this is a slash-group (multi-token alternative expression).
    # has_slash_group: ANY '/' makes it a slash group — including an ALREADY-wrapped
    # '(א/ב)'. is_group (whether WE need to add parens) excludes the pre-wrapped
    # form. The wildcard-prefix RR-13 guard must key off has_slash_group, not
    # is_group, or '*' wrongly applies to a pre-wrapped '(א/ב)' (CR LOW).
    has_slash_group = '/' in t
    is_group = has_slash_group and not t.startswith('(')
    wrapped = f'({t})' if is_group else t

    if mods.get('negation'):
        return f'-{wrapped}'

    if mods.get('plene'):
        wrapped = f'%{wrapped}'
    if mods.get('prefix'):
        wrapped = f'#{wrapped}'
    if mods.get('suffix'):
        wrapped = f'{wrapped}#'
    # wildcard_prefix NOT supported on slash-groups (RR-13 parity - parser limitation)
    if mods.get('wildcard_prefix') and not has_slash_group:
        wrapped = f'*{wrapped}'
    if mods.get('wildcard_suffix'):
        wrapped = f'{wrapped}*'

    return wrapped


# ---------------------------------------------------------------------------
# Word/line model helpers
# ---------------------------------------------------------------------------

def _default_word() -> dict:
    """Return a new default word-state dict."""
    return {'term': '', 'mods': {}, 'gap_to_next_word': 0}


def _default_line() -> dict:
    """Return a new default line-state dict (one empty word)."""
    return {
        'words': [_default_word()],
        'line_start': False,
        'line_end': False,
        'gap_to_next_line': 0,
    }


def _normalize_word_mods(mods: dict) -> dict:
    """Return a mods dict with all expected keys present (defaulting to False)."""
    keys = ('prefix', 'suffix', 'plene', 'wildcard_prefix', 'wildcard_suffix', 'negation')
    return {k: bool(mods.get(k, False)) for k in keys}


def _toggle_line_anchor(line: dict, which: str) -> None:
    """Toggle a line's 'line_start'/'line_end' anchor in place, mutually exclusive.

    ⊢ and ⊣ are exclusive (a line is anchored to its start OR its end, not both),
    and either can be cleared (clicking the active one turns it off → both off).
    Selecting one clears the other.
    """
    other = 'line_end' if which == 'line_start' else 'line_start'
    new_val = not line.get(which, False)
    line[which] = new_val
    if new_val:
        line[other] = False


# ---------------------------------------------------------------------------
# Lines-with-words -> SideQuery converter (word-level model, Phase 118-06)
# ---------------------------------------------------------------------------

def build_side_query(
    lines_state: list,
    variants: bool,
    page_position: Optional[str],
) -> Optional[SideQuery]:
    """Build a SideQuery from a list of line-state dicts.

    Each entry in lines_state:
      {
        'words': [
          {'term': str, 'mods': dict, 'gap_to_next_word': int},
          ...
        ],
        'line_start': bool,
        'line_end': bool,
        'gap_to_next_line': int,
      }

    For each line: hoist each word via _apply_modifiers_to_term, join them with
    [N] word-gap tokens (gap > 0) or plain space (gap == 0), then build ONE
    BuilderRow(term=line_term, line_start=..., line_end=..., gap_to_next=...).

    Returns None when no word in any line has a non-empty stripped term.
    """
    builder_rows = []
    for line in lines_state:
        words = line.get('words', [])
        hoisted_parts = []
        for w in words:
            h = _apply_modifiers_to_term(w.get('term', ''), w.get('mods', {}))
            hoisted_parts.append((h, int(w.get('gap_to_next_word', 0) or 0)))

        # Build the line term: join words with [N] gap tokens or spaces
        # Each element is (hoisted_term, gap_to_next) where gap applies AFTER this word
        line_tokens = []
        for i, (h, gap) in enumerate(hoisted_parts):
            if h:  # skip empty words
                line_tokens.append(h)
                # If not last word and there is a gap, emit [N] gap marker
                if i < len(hoisted_parts) - 1:
                    next_gap = gap
                    if next_gap > 0:
                        line_tokens.append(f'[{next_gap}]')

        if not line_tokens:
            # Entire line is empty - still create a BuilderRow with empty term
            # (empty-builder guard below will catch it)
            line_term = ''
        else:
            line_term = ' '.join(line_tokens)

        builder_rows.append(BuilderRow(
            term=line_term,
            line_start=bool(line.get('line_start', False)),
            line_end=bool(line.get('line_end', False)),
            gap_to_next=int(line.get('gap_to_next_line', 0) or 0),
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


def _coerce_text_position(v) -> str:
    """Normalize a Text-Position value to a known option KEY string.

    A Quasar dict-options ``q-select`` can deliver its raw ``update:model-value``
    payload as the option OBJECT ``{'label': ..., 'value': ...}`` rather than the
    bare key. If that object reaches ``_TEXT_POSITION_LABEL_KEYS.get(tp, tp)`` it
    raises ``TypeError: unhashable type: 'dict'`` (a dict can't be a dict key),
    500-ing the search handler the moment the user picks any non-default position.
    Coerce defensively so neither the summary bar nor ``_build_sq`` can ever see a
    non-string here; anything unrecognized falls back to ``'anywhere'``.
    """
    if isinstance(v, dict):
        v = v.get('value', 'anywhere')
    return v if v in _TEXT_POSITION_KEYS else 'anywhere'


# Per-WORD modifier symbol table (BLD-03 symbol indicators)
# Each entry: (mod_key, symbol, tr_label_key, tr_tooltip_key)
_WORD_MOD_TABLE = [
    ('prefix',          '#_',  lambda: tr('Prefix'),           lambda: tr('May carry a prefix')),
    ('suffix',          '_#',  lambda: tr('Suffix'),           lambda: tr('May carry a suffix')),
    ('plene',           '%',   lambda: tr('Plene / defective'), lambda: tr('Plene / defective spelling variants')),
    ('wildcard_prefix', '*_',  lambda: tr('Wildcard before'),  lambda: tr('Wildcard before')),
    ('wildcard_suffix', '_*',  lambda: tr('Wildcard after'),   lambda: tr('Wildcard after')),
    ('negation',        '−',   lambda: tr('Negation'),         lambda: tr('Must NOT appear')),
]

# Line-level modifiers (anchor toggles shown per-line)
# (mod_key, symbol, tr_label_key, tr_tooltip_key)
_LINE_MOD_TABLE = [
    ('line_start', '⊢', lambda: tr('Line start (⊢)'), lambda: tr('Line starts here')),
    ('line_end',   '⊣', lambda: tr('Line end (⊣)'),   lambda: tr('Line ends here')),
]


def create_joins_builder(allow_page_position: bool = True, on_submit=None) -> dict:
    """Factory: creates and mounts the Joins Lab word-box line-builder widget.

    Returns a handle dict with:
      container           - the top-level NiceGUI element (mount point)
      build_side_query()  - builds a SideQuery from current state
      get_mode()          - 'exact' | 'variants' | 'fuzzy'
      get_text_position() - one of 'anywhere'|'start'|'end'|'line_start'|'line_end'
      get_summary()       - human-readable summary string for the collapsed bar
      is_empty()          - True when all builder rows are blank

    When allow_page_position=False (other side, D-13 parity):
      - Text Position control is hidden
      - build_side_query() always passes page_position=None

    Word model (Phase 118-06 BLD-03):
      lines_state is a list of LINE dicts; each line has:
        - 'words': list of word dicts {'term': str, 'mods': dict, 'gap_to_next_word': int}
        - 'line_start': bool (line anchor - line start ⊢)
        - 'line_end': bool (line anchor - line end ⊣)
        - 'gap_to_next_line': int
      Each word box carries its OWN modifiers (prefix/suffix/plene/wildcard/negation)
      rendered as responsa symbols beneath the box with hover tooltips.
      Line-level anchors (⊢/⊣) are PER LINE, not per word.
    """
    # The Genizah corpus is Hebrew / Judeo-Arabic, so the word boxes must read
    # right-to-left in EVERY UI language (the search term is Hebrew even when the
    # interface is English). A wrapper-level style(direction:rtl) does not reach the
    # native <input>, and an inline input-style can lose to Quasar's LTR defaults on
    # an LTR page — so force it with a scoped !important rule on the native control.
    ui.add_css(
        '.jl-word-rtl .q-field__native, .jl-word-rtl input, .jl-word-rtl textarea'
        ' { direction: rtl !important; text-align: right !important; }'
    )

    # Mutable in-memory state (closure-local, not app.storage.user - Phase 87 invariant)
    lines_state: list = [_default_line()]
    mode_state: dict = {'mode': 'exact'}
    text_position_state: dict = {'value': 'anywhere'}

    # ---- internal helpers -----------------------------------------------

    def _get_mode() -> str:
        return mode_state['mode']

    def _get_text_position() -> str:
        # Always return a known KEY string - see _coerce_text_position (guards the
        # "unhashable type: 'dict'" crash if a Quasar option object slips in).
        return _coerce_text_position(text_position_state['value'])

    def _is_empty() -> bool:
        for line in lines_state:
            for w in line.get('words', []):
                if w.get('term', '').strip():
                    return False
        return True

    def _get_summary() -> str:
        # WR-04: every literal here must go through tr() - the collapsed summary
        # bar is shown after EVERY search (joins_lab._collapse_builder), and the
        # default UI is Hebrew.
        _mode_label_keys = {'exact': 'Exact', 'variants': 'Variants', 'fuzzy': 'Fuzzy'}
        mode_raw = _get_mode()
        mode = tr(_mode_label_keys.get(mode_raw, mode_raw.capitalize()))
        n_lines = len(lines_state)
        lines_label = (
            tr('{n} line').format(n=n_lines) if n_lines == 1
            else tr('{n} lines').format(n=n_lines)
        )
        if allow_page_position:
            tp = _get_text_position()
            pos_label = tr(_TEXT_POSITION_LABEL_KEYS.get(tp, tp))
            return f'{mode} · {lines_label} · ' + tr('Text Position: {pos}').format(pos=pos_label)
        return f'{mode} · {lines_label}'

    def _build_sq() -> Optional[SideQuery]:
        """Build SideQuery from current widget state.

        Text position routing:
          'start'/'end'       -> page_position in SideQuery
          'anywhere'          -> page_position=None
          'line_start'/'line_end' -> page_position=None here; Plan 04 passes
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
        return build_side_query(lines_state, variants, page_pos)

    # ---- row / word rendering -------------------------------------------

    # Container for the lines section (re-rendered on structural add/remove)
    lines_container: dict = {'el': None}

    def _render_all(parent_el) -> None:
        """Render all lines inside parent_el, clearing it first.

        Only called on STRUCTURAL changes (add/remove word or line).
        Do NOT call this on every keystroke (Guardrail 3 - WR-05).
        """
        parent_el.clear()
        # LOW (CR): drop stale symbol-row references from the prior render so the
        # registry doesn't retain detached elements after each structural rebuild.
        _sym_rows.clear()
        with parent_el:
            for li in range(len(lines_state)):
                _render_line(li)
            # Add-line button (global)
            ui.button(tr('+ Add line'), icon='add').props('flat small').classes(
                'text-xs mt-2'
            ).style('color: var(--text-secondary);').on(
                'click', lambda: _add_line()
            )

    def _render_line(li: int) -> None:
        """Render one line and its words."""
        line = lines_state[li]
        words = line['words']

        with ui.column().classes('w-full gap-1 border-b pb-2 mb-1').style(
            'border-color: var(--neutral-200);'
        ):
            # Line header: line number + ⊢/⊣ toggles + gap-to-next + remove-line
            with ui.row().classes('items-center gap-2 w-full'):
                ui.label(str(li + 1)).classes('text-xs w-5 text-right shrink-0').style(
                    'color: var(--text-muted);'
                )
                # Line-start ⊢ / line-end ⊣ anchors — mutually-exclusive toggles
                # shown with a selected (filled) state. Clicking the active one
                # clears it (both may be off); selecting one clears the other. They
                # map to the | pipe on the line's first/last word in compose().
                ls_btn = ui.button('⊢').props('dense size=sm')
                ls_btn.tooltip(tr('Line starts here'))
                le_btn = ui.button('⊣').props('dense size=sm')
                le_btn.tooltip(tr('Line ends here'))

                def _style_anchor(btn, active):
                    # Filled primary = selected; flat = unselected.
                    if active:
                        btn.props(remove='flat')
                        btn.props('unelevated color=primary')
                    else:
                        btn.props(remove='unelevated color=primary')
                        btn.props('flat')

                def _refresh_anchors(i=li, lb=ls_btn, eb=le_btn):
                    _style_anchor(lb, bool(lines_state[i].get('line_start', False)))
                    _style_anchor(eb, bool(lines_state[i].get('line_end', False)))

                def _toggle_line_start(i=li):
                    _toggle_line_anchor(lines_state[i], 'line_start')
                    _refresh_anchors(i)

                def _toggle_line_end(i=li):
                    _toggle_line_anchor(lines_state[i], 'line_end')
                    _refresh_anchors(i)

                ls_btn.on('click', lambda i=li: _toggle_line_start(i))
                le_btn.on('click', lambda i=li: _toggle_line_end(i))
                _refresh_anchors(li)  # set initial selected visuals

                # Gap to next line (only show if not last line)
                if li < len(lines_state) - 1:
                    gap_val = line.get('gap_to_next_line', 0)
                    border_color = 'var(--border-focus)' if gap_val > 0 else 'var(--neutral-300)'
                    with ui.row().classes('items-center gap-1'):
                        ui.label(tr('↕ gap')).classes('text-xs').style(
                            'color: var(--text-tertiary);'
                        )
                        gap_input = ui.number(
                            value=gap_val,
                            min=0, max=20, step=1,
                        ).props('outlined dense').style(
                            f'width: 56px; border-color: {border_color};'
                        )

                        def _on_line_gap_change(v, i=li, el=gap_input):
                            try:
                                gap = int(v or 0)
                            except (TypeError, ValueError):
                                gap = 0
                            lines_state[i]['gap_to_next_line'] = gap
                            color = 'var(--border-focus)' if gap > 0 else 'var(--neutral-300)'
                            el.style(f'width: 56px; border-color: {color};')

                        gap_input.on(
                            'update:model-value',
                            lambda e, i=li: _on_line_gap_change(e.args, i),
                        )

                # Remove line button
                remove_line_btn = ui.button(icon='remove').props('flat dense size=sm color=negative')
                remove_line_btn.tooltip(tr('Remove line'))
                if len(lines_state) <= 1:
                    remove_line_btn.set_visibility(False)
                else:
                    remove_line_btn.on('click', lambda i=li: _remove_line(i))

            # Words row: each word = text input + modifier menu + symbol indicators.
            # Between words: gap-to-next-word box. The row flows RIGHT-TO-LEFT
            # (direction:rtl, every UI language) so the FIRST word sits rightmost and
            # each '+ Add word' appears to its LEFT — Hebrew reading order. DOM order
            # stays word0, gap0, word1, ... so query/compose order is unaffected.
            with ui.row().classes('items-start gap-2 flex-wrap jl-words-row').style(
                'direction: rtl;'
            ):
                for wi in range(len(words)):
                    _render_word_unit(li, wi)
                    if wi < len(words) - 1:
                        _render_word_gap(li, wi)

                # Add-word button
                ui.button(tr('+ Add word'), icon='add').props('flat dense size=sm').classes(
                    'text-xs self-center'
                ).style('color: var(--text-secondary);').on(
                    'click', lambda i=li: _add_word(i)
                )

    def _render_word_unit(li: int, wi: int) -> None:
        """Render one word box (text input + modifier menu + symbol indicators)."""
        word = lines_state[li]['words'][wi]

        # A lone word on the line gets a WIDE box (type a full sentence); once a
        # second word is added the boxes trim to word-width. The explanatory
        # placeholder shows ONLY on the lone wide box and is dropped once there are
        # multiple word boxes (UAT: box sizing + alt-text).
        single_word = len(lines_state[li]['words']) == 1
        # A lone box takes the FULL row width (flex-basis 100%) so the long
        # explanatory placeholder is never clipped (UAT: "alt-text partially not
        # seen"); the "+ Add word" button wraps below it. Once a second word is
        # added the boxes trim to word-width.
        col_style = (
            'flex: 1 1 100%; min-width: 280px; max-width: 100%;' if single_word
            else 'min-width: 80px; max-width: 180px;'
        )
        with ui.column().classes('items-center gap-0').style(col_style):
            # Word text input (RTL)
            if single_word:
                placeholder = (
                    tr('Words on this line (space = sequence, a/b = alternatives)')
                    if allow_page_position
                    else tr('Words on this side (space = sequence, a/b = alternatives)')
                )
            else:
                placeholder = ''
            # RTL: `input-style` targets the inner <input> so typed Hebrew flows
            # right-to-left (a wrapper-level `style(direction:rtl)` alone does NOT
            # reach the native input). Wrapper keeps direction:rtl for placeholder.
            term_input = ui.input(placeholder=placeholder).props(
                'outlined dense input-style="direction: rtl; text-align: right;"'
            ).classes('w-full jl-word-rtl').style(
                'direction: rtl;'
                ' font-family: "Noto Sans Hebrew", "SBL Hebrew", serif; font-size: 1rem;'
            )
            term_input.value = word.get('term', '')

            def _on_term_change(v, i=li, j=wi):
                # WR-05: update ONLY state; do NOT re-render (Guardrail 3)
                lines_state[i]['words'][j]['term'] = v

            term_input.on('update:model-value', lambda e, i=li, j=wi: _on_term_change(e.args, i, j))
            # Enter in any word box runs the search (parity with the Run Search
            # button). on_submit is the page's (async) search trigger.
            if on_submit is not None:
                term_input.on('keydown.enter', on_submit)

            # Modifier row: tune icon + optional remove-word
            with ui.row().classes('items-center gap-1 justify-center'):
                # Tune button -> per-word modifier menu
                mods = word.get('mods', {})
                has_active = any(mods.get(k, False) for k, _, _, _ in _WORD_MOD_TABLE)
                tune_style = (
                    'color: var(--primary-600);' if has_active else 'color: var(--text-muted);'
                )
                with ui.button(icon='tune').props('flat dense size=xs').style(
                    tune_style
                ).tooltip(tr('Word options')) as _tune_btn:
                    with ui.menu():
                        for mod_key, symbol, mod_label_fn, _ in _WORD_MOD_TABLE:
                            cb = ui.checkbox(
                                f'{symbol} {mod_label_fn()}',
                                value=bool(mods.get(mod_key, False)),
                            )

                            def _on_mod_change(v, i=li, j=wi, k=mod_key, tbtn=_tune_btn):
                                if 'mods' not in lines_state[i]['words'][j]:
                                    lines_state[i]['words'][j]['mods'] = {}
                                lines_state[i]['words'][j]['mods'][k] = bool(v)
                                # Update tune button color in place (Guardrail 3)
                                active = any(
                                    lines_state[i]['words'][j]['mods'].get(mk, False)
                                    for mk, _, _, _ in _WORD_MOD_TABLE
                                )
                                tbtn.style(
                                    'color: var(--primary-600);' if active
                                    else 'color: var(--text-muted);'
                                )
                                # Refresh symbol row in place
                                _refresh_symbol_row(i, j)

                            # Read the checkbox's synced .value (reliable bool) via
                            # on_value_change — robust across menu interactions and
                            # consistent with the other selects/checkboxes. Ensures an
                            # UNCHECK actually clears the symbol (state -> False).
                            cb.on_value_change(
                                lambda e, i=li, j=wi, k=mod_key: _on_mod_change(bool(e.value), i, j, k)
                            )

                # Remove word button (hidden when only one word in the line)
                remove_btn = ui.button(icon='close').props('flat dense size=xs color=negative')
                remove_btn.tooltip(tr('Remove word'))
                if len(lines_state[li]['words']) <= 1:
                    remove_btn.set_visibility(False)
                else:
                    remove_btn.on('click', lambda i=li, j=wi: _remove_word(i, j))

            # Symbol indicators beneath the word box (active modifiers as symbols)
            # Stored reference so we can refresh in place (Guardrail 3)
            sym_row_key = f'sym_{li}_{wi}'
            _sym_rows[sym_row_key] = _build_symbol_row(li, wi)

    def _build_symbol_row(li: int, wi: int):
        """Build (and return) the symbol indicator row for word (li, wi)."""
        word = lines_state[li]['words'][wi]
        mods = word.get('mods', {})
        active_syms = [
            (symbol, tooltip_fn())
            for mod_key, symbol, _, tooltip_fn in _WORD_MOD_TABLE
            if mods.get(mod_key, False)
        ]
        with ui.row().classes('items-center gap-1 justify-center').style(
            'min-height: 18px; flex-wrap: nowrap;'
        ) as sym_row:
            for symbol, tip in active_syms:
                ui.label(symbol).classes('text-xs').style(
                    'color: var(--primary-600); font-weight: bold; cursor: default;'
                ).tooltip(tip)
        return sym_row

    # Registry for symbol rows so we can refresh them in place
    _sym_rows: dict = {}

    def _refresh_symbol_row(li: int, wi: int) -> None:
        """Refresh the symbol indicator row for word (li, wi) in place (Guardrail 3)."""
        key = f'sym_{li}_{wi}'
        existing = _sym_rows.get(key)
        if existing is None:
            return
        # Clear and rebuild inside the existing container
        existing.clear()
        word = lines_state[li]['words'][wi]
        mods = word.get('mods', {})
        active_syms = [
            (symbol, tooltip_fn())
            for mod_key, symbol, _, tooltip_fn in _WORD_MOD_TABLE
            if mods.get(mod_key, False)
        ]
        with existing:
            for symbol, tip in active_syms:
                ui.label(symbol).classes('text-xs').style(
                    'color: var(--primary-600); font-weight: bold; cursor: default;'
                ).tooltip(tip)

    def _render_word_gap(li: int, wi: int) -> None:
        """Render the gap-to-next-word control between word wi and wi+1 in line li."""
        word = lines_state[li]['words'][wi]
        gap_val = int(word.get('gap_to_next_word', 0) or 0)
        border_color = 'var(--border-focus)' if gap_val > 0 else 'var(--neutral-300)'
        with ui.column().classes('items-center gap-0 self-center'):
            ui.label(tr('Gap')).classes('text-xs').style(
                'color: var(--text-tertiary); font-size: 10px;'
            )
            gap_input = ui.number(
                value=gap_val,
                min=0, max=20, step=1,
            ).props('outlined dense').style(
                f'width: 52px; border-color: {border_color};'
            )

            def _on_word_gap_change(v, i=li, j=wi, el=gap_input):
                # WR-05: update state + gap border only (no full re-render)
                try:
                    gap = int(v or 0)
                except (TypeError, ValueError):
                    gap = 0
                lines_state[i]['words'][j]['gap_to_next_word'] = gap
                color = 'var(--border-focus)' if gap > 0 else 'var(--neutral-300)'
                el.style(f'width: 52px; border-color: {color};')

            gap_input.on(
                'update:model-value',
                lambda e, i=li, j=wi: _on_word_gap_change(e.args, i, j),
            )

    # ---- add / remove word + line ----------------------------------------

    def _add_word(li: int):
        lines_state[li]['words'].append(_default_word())
        _render_all(lines_container['el'])

    def _remove_word(li: int, wi: int):
        if len(lines_state[li]['words']) > 1:
            lines_state[li]['words'].pop(wi)
        _render_all(lines_container['el'])

    def _add_line():
        lines_state.append(_default_line())
        _render_all(lines_container['el'])

    def _remove_line(li: int):
        if len(lines_state) > 1:
            lines_state.pop(li)
        _render_all(lines_container['el'])

    # ---- build the widget -----------------------------------------------

    # Initialized to None so _reset() can reference it even when the Text
    # Position control is not rendered (allow_page_position=False, other side).
    text_pos_select = None

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

                    # Read the element's normalized `.value` (the option KEY), NOT the
                    # raw `update:model-value` payload (`e.args`) - for a dict-options
                    # select the latter is the Quasar option object {'label','value'},
                    # a dict, which breaks _TEXT_POSITION_LABEL_KEYS.get(tp). Coerce as
                    # a belt-and-braces guard (WR-03).
                    text_pos_select.on_value_change(
                        lambda: text_position_state.update(
                            value=_coerce_text_position(text_pos_select.value)
                        )
                    )

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

                # Info popup — answers "what syntax is this?". The query ALWAYS
                # runs as Responsa syntax (compose() sets responsa_mode=True); the
                # mode only controls variant expansion, so 'Exact' IS the raw
                # Responsa query.
                with ui.button(icon='help_outline').props('flat dense round size=sm').style(
                    'color: var(--text-muted);'
                ).tooltip(tr('About search modes & syntax')):
                    with ui.menu(), ui.card().classes('p-3 gap-1').style('max-width: 340px;'):
                        ui.label(tr('Search modes & syntax')).classes('text-sm font-bold')
                        ui.label(tr(
                            'Your query always runs as Responsa syntax (shown in the '
                            'collapsed search bar). The mode controls how each word is matched:'
                        )).classes('text-xs')
                        ui.label('• ' + tr(
                            'Exact — match each word as written, no expansion. This is the '
                            'raw Responsa query.'
                        )).classes('text-xs')
                        ui.label('• ' + tr(
                            'Variants — also match known spelling variants of each word.'
                        )).classes('text-xs')
                        ui.label('• ' + tr(
                            'Fuzzy — approximate matching: finds words within 1–2 letter changes '
                            '(typos, missing or swapped letters), independent of the variant '
                            'tables. Slowest.'
                        )).classes('text-xs')
                        ui.label(tr(
                            'You can type Responsa operators directly in a word box '
                            '(space = sequence, a/b = alternatives); the gear menu adds the '
                            '#, %, *, − modifiers.'
                        )).classes('text-xs').style('color: var(--text-muted);')

            # Fuzzy hint (shown only when fuzzy selected)
            fuzzy_hint = ui.label(
                tr('Fuzzy search is slower and uses more server resources.')
            ).classes('text-xs').style(
                'color: var(--text-muted);'
            )
            fuzzy_hint.set_visibility(mode_state['mode'] == 'fuzzy')

        # Lines area (re-rendered as lines/words are added/removed)
        lines_area = ui.column().classes('w-full gap-0')
        lines_container['el'] = lines_area
        _render_all(lines_area)

    # ---- reset (New Search) ---------------------------------------------

    def _reset() -> None:
        """Reset the builder to one empty line / Exact / Anywhere (New Search).

        Clears all typed words, modifiers and line anchors, restores the default
        mode (Exact) and Text Position (Anywhere), and re-renders the lines area.
        Used by the page-level "New Search" button (parity with /search reset).
        """
        lines_state.clear()
        lines_state.append(_default_line())
        mode_state['mode'] = 'exact'
        text_position_state['value'] = 'anywhere'
        _sym_rows.clear()
        if lines_container['el'] is not None:
            _render_all(lines_container['el'])
        _set_mode('exact')  # refresh mode-button visuals + fuzzy hint
        if allow_page_position and text_pos_select is not None:
            text_pos_select.value = 'anywhere'

    # ---- return handle dict ---------------------------------------------

    return {
        'container': container,
        'build_side_query': _build_sq,
        'get_mode': _get_mode,
        'get_text_position': _get_text_position,
        'get_summary': _get_summary,
        'is_empty': _is_empty,
        'reset': _reset,
    }
