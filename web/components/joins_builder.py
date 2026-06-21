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
        get_state()           -> dict  (Phase 120-03 B1: plain-dict snapshot)
        set_state(state)      -> None  (Phase 120-03 B1: restore + visual sync)
        on_change(cb)         -> None  (Phase 120-03 B1: register mutation callback)

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


def _coerce_gap_int(val) -> int:
    """Coerce a persisted gap value to a non-negative int (0 on anything odd)."""
    try:
        n = int(val or 0)
        return n if n >= 0 else 0
    except (TypeError, ValueError):
        return 0


def _normalize_word(raw) -> dict:
    """Coerce a (possibly malformed/legacy) word dict to the current schema."""
    if not isinstance(raw, dict):
        return _default_word()
    term = raw.get('term', '')
    mods = raw.get('mods', {})
    return {
        'term': str(term) if term is not None else '',
        'mods': dict(mods) if isinstance(mods, dict) else {},
        'gap_to_next_word': _coerce_gap_int(raw.get('gap_to_next_word', 0)),
    }


def _normalize_line(raw) -> dict:
    """Coerce a (possibly malformed/legacy) line dict to the current line schema.

    RESTORE TOLERANCE: persisted state written by an older builder schema (e.g.
    before the word-level model) — or any partial blob — may lack ``'words'`` or
    carry malformed entries. A session restore must NEVER crash the page on stale
    state (the observed ``KeyError: 'words'`` in _render_line), so every required
    key is defaulted and at least one word is guaranteed.
    """
    if not isinstance(raw, dict):
        return _default_line()
    raw_words = raw.get('words')
    if isinstance(raw_words, list) and raw_words:
        words = [_normalize_word(w) for w in raw_words]
    else:
        words = [_default_word()]
    return {
        'words': words,
        'line_start': bool(raw.get('line_start', False)),
        'line_end': bool(raw.get('line_end', False)),
        'gap_to_next_line': _coerce_gap_int(raw.get('gap_to_next_line', 0)),
    }


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

# Search-type selector (D-Q1 redesign). 'responsa' shows the structured
# multi-line word builder (Responsa-style, with a Variants toggle); the others
# collapse to a single free-text line that runs the STANDARD search path (so
# Fuzzy is real edit-distance and Regex is a real regex — see CR HIGH-7).
_SEARCH_TYPES = ['responsa', 'exact', 'variants', 'fuzzy', 'regex']
_SEARCH_TYPE_LABEL_KEYS = {
    'responsa': 'Responsa-style',
    'exact': 'Exact',
    'variants': 'Variants',
    'fuzzy': 'Fuzzy',
    'regex': 'Regex',
}
# Single-line type -> engine mode string for execute_search(mode=...).
# 'regex' -> 'Regex' (the engine checks the capitalized literal).
_SIMPLE_TYPE_TO_ENGINE_MODE = {
    'exact': 'exact',
    'variants': 'variants',
    'fuzzy': 'fuzzy',
    'regex': 'Regex',
}

# Responsa operator legend — mirrors the main search legend (web/pages/search.py
# :599-606) so the word-box tooltip shows the SAME operators with the SAME meaning
# keys. Operators are literal (Hebrew מילה = "word"); meanings go through tr().
_RESPONSA_SYNTAX_OPS = [
    ('#מילה', 'prefix'),
    ('מילה#', 'suffix'),
    ('%מילה', 'plene'),
    ('*מילה / מילה*', 'wildcard'),
    ('(א/ב)', 'OR'),
    ('-מילה', 'Exclude'),
    ('|מילה', 'Line starts'),
    ('מילה|', 'Line ends'),
]


def _responsa_syntax_tooltip() -> str:
    """Build the word-box tooltip: the Responsa operator legend exactly as the main
    search lists it (operator = meaning), assembled at request time so tr() honors
    the request language."""
    legend = '  ·  '.join(f'{op} = {tr(mk)}' for op, mk in _RESPONSA_SYNTAX_OPS)
    return (
        tr('Responsa syntax') + ': ' + legend + '. '
        + tr('Space separates words; click the gear icon for the same options per word.')
    )


def create_joins_builder(
    allow_page_position: bool = True,
    on_submit=None,
    show_search_type: bool = True,
    on_type_change=None,
) -> dict:
    """Factory: creates and mounts the Joins Lab word-box line-builder widget.

    show_search_type (D-Q1): when True (the main anchor-side builder) the widget
    shows the segmented search-type selector (Responsa-style | Exact | Variants |
    Fuzzy | Regex). When False (the other-side builder) the type is fixed to
    'responsa' (structured builder + Variants checkbox only — no single-line modes).

    Returns a handle dict with:
      container           - the top-level NiceGUI element (mount point)
      build_side_query()  - builds a SideQuery from current state (None unless
                            the active type is 'responsa')
      build_query()       - unified query descriptor (responsa | simple)
      get_search_type()   - 'responsa'|'exact'|'variants'|'fuzzy'|'regex'
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
        '.jl-rtl-field .q-field__native, .jl-rtl-field input, .jl-rtl-field textarea'
        ' { direction: rtl !important; text-align: right !important; }'
    )

    # Mutable in-memory state (closure-local, not app.storage.user - Phase 87 invariant)
    lines_state: list = [_default_line()]
    # search_type: 'responsa' (structured builder) | single-line standard modes.
    search_type_state: dict = {'type': 'responsa'}
    variants_state: dict = {'on': False}        # Variants toggle for Responsa-style
    single_query_state: dict = {'text': ''}     # the single free-text line query
    text_position_state: dict = {'value': 'anywhere'}

    # Phase 120-03 B1: registered on_change callbacks (persistence / page hooks).
    # Fired at END of each user-mutation handler (not from set_state / reset).
    _on_change_cbs: list = []

    def _fire_on_change() -> None:
        """Fire all registered on_change callbacks (best-effort, never raises)."""
        for _cb in _on_change_cbs:
            try:
                _cb()
            except Exception:  # noqa: BLE001 – fire-and-forget, never crash builder
                pass

    # ---- internal helpers -----------------------------------------------

    def _get_search_type() -> str:
        return search_type_state['type']

    def _get_mode() -> str:
        # Back-compat "mode" accessor. Responsa-style maps to 'variants' when the
        # Variants box is on, else 'exact'; single-line types return themselves.
        t = search_type_state['type']
        if t == 'responsa':
            return 'variants' if variants_state['on'] else 'exact'
        return t

    def _get_text_position() -> str:
        # Always return a known KEY string - see _coerce_text_position (guards the
        # "unhashable type: 'dict'" crash if a Quasar option object slips in).
        return _coerce_text_position(text_position_state['value'])

    def _is_empty() -> bool:
        # Single-line modes are empty iff the free-text box is blank.
        if search_type_state['type'] != 'responsa':
            return not single_query_state['text'].strip()
        for line in lines_state:
            for w in line.get('words', []):
                if w.get('term', '').strip():
                    return False
        return True

    def _get_summary() -> str:
        # WR-04: every literal here must go through tr() - the collapsed summary
        # bar is shown after EVERY search (joins_lab._collapse_builder), and the
        # default UI is Hebrew.
        t = search_type_state['type']
        type_label = tr(_SEARCH_TYPE_LABEL_KEYS.get(t, t.capitalize()))
        tp_seg = ''
        if allow_page_position:
            tp = _get_text_position()
            pos_label = tr(_TEXT_POSITION_LABEL_KEYS.get(tp, tp))
            tp_seg = ' · ' + tr('Text Position: {pos}').format(pos=pos_label)
        if t != 'responsa':
            return f'{type_label}{tp_seg}'
        # Responsa-style: show variants state + line count
        var_label = tr('with variants') if variants_state['on'] else tr('exact')
        n_lines = len(lines_state)
        lines_label = (
            tr('{n} line').format(n=n_lines) if n_lines == 1
            else tr('{n} lines').format(n=n_lines)
        )
        return f'{type_label} ({var_label}) · {lines_label}{tp_seg}'

    def _build_sq() -> Optional[SideQuery]:
        """Build SideQuery from the structured (Responsa-style) state.

        Returns None when the active type is NOT 'responsa' (single-line modes do
        not produce a SideQuery; the page reads build_query() instead).

        Text position routing:
          'start'/'end'       -> page_position in SideQuery
          'anywhere'          -> page_position=None
          'line_start'/'line_end' -> page_position=None here; the page passes these
                                    directly to execute_search(text_position=...)
        """
        if search_type_state['type'] != 'responsa':
            return None
        if not allow_page_position:
            page_pos = None
        else:
            tp = _get_text_position()
            page_pos = tp if tp in ('start', 'end') else None
        return build_side_query(lines_state, variants_state['on'], page_pos)

    def _build_query() -> dict:
        """Unified query descriptor (D-Q1).

        responsa -> {'kind': 'responsa', 'side': SideQuery | None}
        simple   -> {'kind': 'simple', 'mode': <engine mode>, 'query': str}
        """
        t = search_type_state['type']
        if t == 'responsa':
            return {'kind': 'responsa', 'side': _build_sq()}
        return {
            'kind': 'simple',
            'mode': _SIMPLE_TYPE_TO_ENGINE_MODE.get(t, 'exact'),
            'query': single_query_state['text'].strip(),
        }

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
        # Defensive: a malformed/legacy line dict may lack 'words' (or carry an
        # empty list). setdefault repairs it in place so subsequent word-index
        # operations stay consistent. The restore path normalizes upstream
        # (_set_state -> _normalize_line); this is belt-and-braces for any other
        # code path that could produce a wordless line.
        if not line.get('words'):
            line['words'] = [_default_word()]
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
                    _fire_on_change()

                def _toggle_line_end(i=li):
                    _toggle_line_anchor(lines_state[i], 'line_end')
                    _refresh_anchors(i)
                    _fire_on_change()

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
                            _fire_on_change()

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
            # Word text input (RTL). The lone box shows the short "Search in Responsa
            # syntax" placeholder; the detailed syntax help lives in the tooltip
            # below (UAT: shorter alt-text + explanatory tooltip).
            placeholder = tr('Search in Responsa syntax') if single_word else ''
            # RTL: `input-style` targets the inner <input> so typed Hebrew flows
            # right-to-left (a wrapper-level `style(direction:rtl)` alone does NOT
            # reach the native input). Wrapper keeps direction:rtl for placeholder.
            term_input = ui.input(placeholder=placeholder).props(
                'outlined dense input-style="direction: rtl; text-align: right;"'
            ).classes('w-full jl-word-rtl jl-rtl-field').style(
                'direction: rtl;'
                ' font-family: "Noto Sans Hebrew", "SBL Hebrew", serif; font-size: 1rem;'
            )
            term_input.tooltip(_responsa_syntax_tooltip())
            term_input.value = word.get('term', '')

            def _on_term_change(v, i=li, j=wi):
                # WR-05: update ONLY state; do NOT re-render (Guardrail 3)
                lines_state[i]['words'][j]['term'] = v
                _fire_on_change()

            term_input.on('update:model-value', lambda e, i=li, j=wi: _on_term_change(e.args, i, j))
            # Enter in any word box runs the search (parity with the Run Search
            # button). on_submit is the page's (async) search trigger.
            if on_submit is not None:
                term_input.on('keydown.enter', on_submit)

            # Modifier row: gear icon + optional remove-word (matches the
            # desktop ⚙ and the builder tooltip that says "click the gear icon")
            with ui.row().classes('items-center gap-1 justify-center'):
                # Gear button -> per-word modifier menu
                mods = word.get('mods', {})
                has_active = any(mods.get(k, False) for k, _, _, _ in _WORD_MOD_TABLE)
                tune_style = (
                    'color: var(--primary-600);' if has_active else 'color: var(--text-muted);'
                )
                with ui.button(icon='settings').props('flat dense size=xs').style(
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
                                _fire_on_change()

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
                _fire_on_change()

            gap_input.on(
                'update:model-value',
                lambda e, i=li, j=wi: _on_word_gap_change(e.args, i, j),
            )

    # ---- add / remove word + line ----------------------------------------

    def _add_word(li: int):
        lines_state[li]['words'].append(_default_word())
        _render_all(lines_container['el'])
        _fire_on_change()

    def _remove_word(li: int, wi: int):
        if len(lines_state[li]['words']) > 1:
            lines_state[li]['words'].pop(wi)
        _render_all(lines_container['el'])
        _fire_on_change()

    def _add_line():
        lines_state.append(_default_line())
        _render_all(lines_container['el'])
        _fire_on_change()

    def _remove_line(li: int):
        if len(lines_state) > 1:
            lines_state.pop(li)
        _render_all(lines_container['el'])
        _fire_on_change()

    # ---- build the widget -----------------------------------------------

    # Pre-declared so _set_search_type / _apply_type_visibility / _reset can
    # reference them even when a control is not rendered (other-side builder, no
    # Text Position, etc.). Reassigned inside the `with` block below; the closures
    # resolve them at call time.
    text_pos_select = None
    single_input = None
    responsa_container = None
    single_container = None
    type_btns: dict = {}

    def _apply_type_visibility() -> None:
        """Show the structured builder for Responsa-style; the single-line input
        for the simple modes. Notifies the page (on_type_change) so it can hide the
        Responsa-only options (Variants / Flexible spacing / Bidirectional) outside
        Responsa-style."""
        is_resp = (search_type_state['type'] == 'responsa')
        if responsa_container is not None:
            responsa_container.set_visibility(is_resp)
        if single_container is not None:
            single_container.set_visibility(not is_resp)
        if on_type_change is not None:
            on_type_change(search_type_state['type'])

    def _set_search_type(t: str) -> None:
        search_type_state['type'] = t
        for tv, btn in type_btns.items():
            if tv == t:
                btn.props('color=primary')
                btn.props(remove='flat')
            else:
                btn.props(remove='color=primary')
                btn.props('flat')
        _apply_type_visibility()
        _fire_on_change()

    with ui.column().classes('w-full gap-3') as container:

        # Header row: search-type selector + Variants toggle + Text Position
        with ui.row().classes('w-full items-center gap-4 flex-wrap'):

            # Search-type segmented selector (main builder only). The other-side
            # builder is fixed to Responsa-style (show_search_type=False).
            if show_search_type:
                with ui.row().classes('items-center gap-1'):
                    for tv in _SEARCH_TYPES:
                        is_active = (tv == search_type_state['type'])
                        btn_props = 'color=primary' if is_active else 'flat'
                        b = ui.button(tr(_SEARCH_TYPE_LABEL_KEYS[tv])).props(
                            f'{btn_props} size=sm'
                        ).on('click', lambda t=tv: _set_search_type(t))
                        type_btns[tv] = b

                    # Info popup — explains the search types.
                    with ui.button(icon='help_outline').props('flat dense round size=sm').style(
                        'color: var(--text-muted);'
                    ).tooltip(tr('About search types')):
                        with ui.menu(), ui.card().classes('p-3 gap-1').style('max-width: 360px;'):
                            ui.label(tr('Search types')).classes('text-sm font-bold')
                            ui.label('• ' + tr(
                                'Responsa-style — the structured builder: build the line '
                                'word-by-word with gaps, line anchors and per-word modifiers. '
                                'Turn on Variants to also match spelling variants.'
                            )).classes('text-xs')
                            ui.label('• ' + tr(
                                'Exact / Variants / Fuzzy / Regex — a single free-text line that '
                                'searches like the main search bar (Fuzzy = approximate, within '
                                '1–2 letter changes; Regex = a regular expression).'
                            )).classes('text-xs')

            # (Variants moved to the page's options area — set via set_variants;
            #  shown only in Responsa-style mode.)

            # Text Position — applies to ALL types (full join workflow is kept in
            # single-line modes too, D-Q1).
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
                    # raw `update:model-value` payload — for a dict-options select that
                    # is the Quasar option object {'label','value'}, a dict, which breaks
                    # _TEXT_POSITION_LABEL_KEYS.get(tp). Coerce as belt-and-braces (WR-03).
                    def _on_text_pos_change():
                        text_position_state.update(
                            value=_coerce_text_position(text_pos_select.value)
                        )
                        _fire_on_change()

                    text_pos_select.on_value_change(_on_text_pos_change)

        # Responsa-style structured builder (lines area, re-rendered on add/remove)
        responsa_container = ui.column().classes('w-full gap-0')
        with responsa_container:
            lines_area = ui.column().classes('w-full gap-0')
            lines_container['el'] = lines_area
            _render_all(lines_area)

        # Single-line free-text input (Exact / Variants / Fuzzy / Regex). Distinct
        # class jl-single-rtl so the structured-word-box test helpers (which match
        # jl-word-rtl) never pick it up; jl-rtl-field carries the shared RTL CSS.
        single_container = ui.column().classes('w-full')
        with single_container:
            # Same placeholder as the main /search box (web/pages/search.py) so the
            # single-line modes (Exact/Variants/Fuzzy/Regex) read identically to the
            # search bar they mirror.
            single_input = ui.input(placeholder=tr('Enter Hebrew text to search')).props(
                'outlined dense input-style="direction: rtl; text-align: right;"'
            ).classes('w-full jl-single-rtl jl-rtl-field').style(
                'direction: rtl;'
                ' font-family: "Noto Sans Hebrew", "SBL Hebrew", serif; font-size: 1rem;'
            )
            single_input.value = single_query_state['text']
            def _on_single_input_change(e):
                single_query_state.update(text=e.args or '')
                _fire_on_change()

            single_input.on('update:model-value', _on_single_input_change)
            if on_submit is not None:
                single_input.on('keydown.enter', on_submit)

        # Initial visibility (Responsa-style default)
        _apply_type_visibility()

    # ---- reset (New Search) ---------------------------------------------

    def _reset() -> None:
        """Reset the builder to defaults (New Search): Responsa-style, one empty
        line, Variants off, single-line query cleared, Text Position = Anywhere."""
        lines_state.clear()
        lines_state.append(_default_line())
        search_type_state['type'] = 'responsa'
        variants_state['on'] = False
        single_query_state['text'] = ''
        text_position_state['value'] = 'anywhere'
        _sym_rows.clear()
        if lines_container['el'] is not None:
            _render_all(lines_container['el'])
        if single_input is not None:
            single_input.value = ''
        if allow_page_position and text_pos_select is not None:
            text_pos_select.value = 'anywhere'
        # Restore type-button visuals + container visibility
        for tv, btn in type_btns.items():
            if tv == 'responsa':
                btn.props('color=primary')
                btn.props(remove='flat')
            else:
                btn.props(remove='color=primary')
                btn.props('flat')
        _apply_type_visibility()

    def _set_variants(on: bool) -> None:
        """Set the Responsa-style Variants flag (the page owns the checkbox now)."""
        variants_state['on'] = bool(on)
        _fire_on_change()

    # ---- Phase 120-03 B1: state snapshot / restore / on_change API ------

    def _get_state() -> dict:
        """Return a plain-dict snapshot of the builder's current input state.

        All values are JSON-serializable (no widgets, closures, or NiceGUI
        elements). lines_state is deep-copied so the caller can store/compare
        the snapshot without mutating the builder.

        Keys:
          lines_state    - deep copy of the lines-with-words structure
          search_type    - str ('responsa'|'exact'|'variants'|'fuzzy'|'regex')
          variants_on    - bool (Responsa-style Variants toggle)
          single_text    - str (single free-text line query)
          text_position  - str ('anywhere'|'start'|'end'|'line_start'|'line_end')
        """
        import copy
        return {
            'lines_state': copy.deepcopy(lines_state),
            'search_type': search_type_state['type'],
            'variants_on': variants_state['on'],
            'single_text': single_query_state['text'],
            'text_position': text_position_state['value'],
        }

    def _set_state(state: dict | None) -> None:  # noqa: UP007 (py3.9 compat)
        """Restore the builder from a plain-dict snapshot.

        Mirrors the visual-sync sequence from _reset (R2-M1):
          (a) _render_all to rebuild the lines UI
          (b) re-set type-button props
          (c) _apply_type_visibility
          (d) sync single_input.value
          (e) sync text_pos_select.value

        Silently ignores unknown keys. Falls back gracefully on None or partial
        blobs (legacy-blob tolerance: if lines_state is absent/empty, keeps one
        default line).

        NOTE: set_state is a RESTORE operation — it does NOT fire on_change
        callbacks (restoring persisted state should not trigger re-persist).
        """
        if not isinstance(state, dict):
            return  # None or unexpected type — silently ignore

        # --- 1. Restore closure state ---
        raw_lines = state.get('lines_state')
        if raw_lines and isinstance(raw_lines, list):
            # Normalize each restored line to the current schema so a legacy /
            # partial blob (e.g. a pre-word-level-builder session) cannot crash
            # the restore (_render_line -> KeyError 'words'). _normalize_line
            # builds fresh dicts, so no aliasing with the stored snapshot.
            lines_state.clear()
            lines_state.extend(_normalize_line(rl) for rl in raw_lines)
            if not lines_state:
                lines_state.append(_default_line())
        else:
            # Ensure at least one default line (empty or missing lines_state)
            if not lines_state:
                lines_state.append(_default_line())

        new_type = state.get('search_type', search_type_state['type'])
        if new_type in _SEARCH_TYPES or new_type in _SIMPLE_TYPE_TO_ENGINE_MODE:
            search_type_state['type'] = new_type
        # else: keep current — unknown type value ignored

        if 'variants_on' in state:
            variants_state['on'] = bool(state['variants_on'])

        if 'single_text' in state:
            single_query_state['text'] = str(state.get('single_text') or '')

        if 'text_position' in state:
            tp = _coerce_text_position(state['text_position'])
            text_position_state['value'] = tp

        # --- 2. Visual sync (mirrors _reset, R2-M1) ---
        _sym_rows.clear()
        if lines_container['el'] is not None:
            _render_all(lines_container['el'])

        # Re-set type-button props to match restored search_type
        t = search_type_state['type']
        for tv, btn in type_btns.items():
            if tv == t:
                btn.props('color=primary')
                btn.props(remove='flat')
            else:
                btn.props(remove='color=primary')
                btn.props('flat')

        _apply_type_visibility()

        if single_input is not None:
            single_input.value = single_query_state['text']

        if allow_page_position and text_pos_select is not None:
            text_pos_select.value = text_position_state['value']

    def _on_change(cb) -> None:
        """Register a callback to fire on every user-driven builder state mutation.

        The callback is called with no arguments at the END of each existing
        mutation handler (word add/edit/remove, gap change, line anchor toggle,
        mode/type change, Text Position change, variants toggle, single-line edit).

        set_state() and reset() do NOT fire on_change — they are restore/clear
        operations, not user mutations.

        Multiple callbacks may be registered; they fire in registration order.
        """
        _on_change_cbs.append(cb)

    # ---- return handle dict ---------------------------------------------

    return {
        'container': container,
        'build_side_query': _build_sq,
        'build_query': _build_query,
        'get_search_type': _get_search_type,
        'get_mode': _get_mode,
        'get_text_position': _get_text_position,
        'get_summary': _get_summary,
        'is_empty': _is_empty,
        'reset': _reset,
        'set_variants': _set_variants,
        # Phase 120-03 B1 additions
        'get_state': _get_state,
        'set_state': _set_state,
        'on_change': _on_change,
    }
