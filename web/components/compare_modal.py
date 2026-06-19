# -*- coding: utf-8 -*-
"""Compare modal for the Joins Lab — full-screen two-pane side-by-side fragment comparison.

Phase 119 Plan 03 (CMP-01 / CMP-02 / CMP-03 / VSM-02).

Design invariants
-----------------
D-01: Full-screen modal overlay (ui.dialog maximized + persistent), anchor pane |
      candidate pane.  Each pane uses a FRESH AnchorViewer instance (Pitfall 3 —
      separate per-pane page state; navigating one pane must NEVER move the other).
D-02: Compare opens on a FULL candidate (initial_candidate), located in
      filtered_candidates by per-image identity (uid or (sys_id, page)) — NOT
      by sys_id alone (same sys_id can appear on multiple folios, Candidate.key
      == (sys_id, page), joins_lab.py:124-137).
D-03: Y/?/N verdict buttons; recording a verdict calls on_verdict(sys_id, verdict)
      and auto-advances to the next candidate (wrap-around).
D-07/VSM-02: Candidate pane header shows the 👁 badge via badge_and_tooltip() and
      a size-mismatch warning badge when ratio > 1.4.
T-119-09 (SSRF prevention): Images load exclusively via AnchorViewer (reuses the
      per-provider proxy + Phase-98 NLI circuit breaker); no image URLs constructed
      directly in this module.
T-119-09 (propagation): Nested clickables use js_handler='(e) => e.stopPropagation()';
      Python-side propagation methods are not used (AST guard applies).
T-119-10 (XSS): Shelfmarks/tooltips rendered via ui.label/ui.icon (auto-escaped);
      no .html() of raw candidate text.
T-119-11 (state): Compare state is a local mutable dict; zero raw storage access.

IMPORTANT: The viewer asset injection function is NOT called here.  Plan 04 calls it
once at page-build time; the window._msViewerLoaded idempotency guard in AnchorViewer
covers the dynamically-created Compare viewers (anchor_viewer.py:59-60).
"""
from __future__ import annotations

import logging
from typing import Optional, Callable

from nicegui import ui

from shared.joins_lab import Candidate, TRIAGE_ICONS, badge_and_tooltip
from web.components.anchor_viewer import AnchorViewer
from web.components.candidate_grid import is_size_mismatch
from web.translations import tr

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Headless-testable pure helpers (no NiceGUI runtime required)
# ---------------------------------------------------------------------------

def _find_candidate_idx(cand: Candidate, candidates: list) -> int:
    """Return the index of cand in candidates by per-image identity.

    Per-image lookup matches on:
      1. uid (if both sides have a non-empty uid)
      2. (sys_id, page) tuple — Candidate.key (D-02 / Pitfall 6)

    NOT matched by sys_id alone — the same sys_id can appear on multiple folios
    (proven by tests/test_joins_lab.py:425-433: two candidates sharing a sys_id
    on pages 5 and 6 both survive dedup; a sys_id-only lookup would return the
    WRONG folio).

    Returns:
        The 0-based index of the matching candidate, or 0 (fallback) when not found.
    """
    if not candidates:
        return 0

    cand_uid = getattr(cand, "uid", None)
    cand_key = (cand.sys_id, cand.page)

    for i, c in enumerate(candidates):
        # Primary: uid match (stable opaque key)
        c_uid = getattr(c, "uid", None)
        if cand_uid and c_uid and cand_uid == c_uid:
            return i
        # Fallback: (sys_id, page) match — D-02 per-image identity
        if (c.sys_id, c.page) == cand_key:
            return i

    # Not found — start at index 0 (caller will see a different candidate, but
    # the modal still opens; the caller should prefer supplying a matching cand).
    logger.warning(
        "_find_candidate_idx: candidate %s (page=%s) not found in list of %d; defaulting to 0",
        cand.sys_id, cand.page, len(candidates),
    )
    return 0


def create_compare_state(
    anchor_cand: Candidate,
    initial_candidate: Candidate,
    filtered_candidates: list,
) -> dict:
    """Create the headless compare state dict.

    Pure factory — no NiceGUI rendering.  Used directly by tests to exercise
    all flip-through / verdict / folio-independence logic without a live browser.

    State keys:
        anchor_sys_id       — the anchor's sys_id (for triage and source display)
        anchor_page         — the anchor pane's current page number (independent)
        current_candidate   — the currently-displayed Candidate object
        candidate_page      — the candidate pane's current page number (independent)
        idx                 — 0-based index into candidates list
        candidates          — the full filtered list (flip-through source)
    """
    idx = _find_candidate_idx(initial_candidate, filtered_candidates)
    current = filtered_candidates[idx] if filtered_candidates else initial_candidate
    return {
        "anchor_sys_id": anchor_cand.sys_id,
        "anchor_page": anchor_cand.page or 1,
        "current_candidate": current,
        "candidate_page": (current.page or 1) if filtered_candidates else (initial_candidate.page or 1),
        "idx": idx,
        "candidates": list(filtered_candidates),
    }


def step_pane_page(state: dict, pane: str, delta: int) -> None:
    """Advance a pane's current page number by delta.

    Pure helper — no NiceGUI.  Used by tests to assert per-pane folio
    independence (CMP-02 / Pitfall 3).

    Args:
        state:  Compare state dict from create_compare_state().
        pane:   'anchor' or 'candidate'.
        delta:  Number of pages to advance (positive) or retreat (negative).
    """
    if pane == "anchor":
        state["anchor_page"] = max(1, (state.get("anchor_page") or 1) + delta)
    elif pane == "candidate":
        state["candidate_page"] = max(1, (state.get("candidate_page") or 1) + delta)
    else:
        raise ValueError(f"Unknown pane {pane!r}; expected 'anchor' or 'candidate'")


def step_candidate(state: dict, delta: int) -> None:
    """Advance the candidate flip-through by delta (wrap-around, parity desktop step(delta)).

    Pure helper — no NiceGUI.  Used by tests and by the live _step() closure.

    Args:
        state:  Compare state dict from create_compare_state().
        delta:  +1 = next, -1 = previous.  Wrap-around: next after last → first;
                prev before first → last.
    """
    cands = state.get("candidates") or []
    if not cands:
        return
    new_idx = (state["idx"] + delta) % len(cands)
    state["idx"] = new_idx
    state["current_candidate"] = cands[new_idx]
    state["candidate_page"] = cands[new_idx].page or 1


def record_verdict(
    state: dict,
    verdict: str,
    triage: dict,
    on_verdict: Optional[Callable] = None,
) -> None:
    """Record a verdict and auto-advance to the next candidate (CMP-03).

    Pure helper — no NiceGUI.  Used by tests and by the live _record_verdict() closure.

    Verdict is keyed by sys_id (triage is per-sys_id, D-11).
    Navigation/lookup is per-image (state carries the full candidate).

    Args:
        state:      Compare state dict from create_compare_state().
        verdict:    'yes' | 'maybe' | 'no'
        triage:     Shared page-level triage dict keyed by sys_id.
        on_verdict: Optional callback(sys_id, verdict) — the page-level
                    triage update hook.  When None, writes directly to triage.
    """
    cand = state.get("current_candidate")
    if cand is None:
        return
    # Verdict is keyed by sys_id (D-11)
    triage[cand.sys_id] = verdict
    if on_verdict is not None:
        on_verdict(cand.sys_id, verdict)
    # Auto-advance to next candidate (D-03, wrap-around)
    step_candidate(state, delta=1)


# ---------------------------------------------------------------------------
# Live NiceGUI modal factory
# ---------------------------------------------------------------------------

def create_compare_modal(
    anchor_cand: Candidate,
    initial_candidate: Candidate,
    filtered_candidates: list,
    triage: dict,
    on_verdict: Callable,
    enrichment: Optional[dict] = None,
    on_close: Optional[Callable] = None,
) -> ui.dialog:
    """Full-screen two-pane Compare modal (CMP-01 / CMP-02 / CMP-03 / VSM-02).

    Desktop parity: join_workbench.py:3724 (_fill_anchor:4051, _fill_candidate:4086,
    step:3741, _mark:4202).

    Args:
        anchor_cand:         The anchor fragment (left pane, fixed).
        initial_candidate:   The FULL candidate to open on (right pane — lookup is
                             by per-image identity, NOT sys_id alone).
        filtered_candidates: The current sort/filter order for flip-through nav.
        triage:              Shared page-level triage dict keyed by sys_id.
        on_verdict:          Callback(sys_id, verdict) — updates the shared triage.
        enrichment:          Optional dict[sys_id → {width_cm, height_cm, ...}] for
                             the size-mismatch badge (D-15).
        on_close:            Optional callback invoked when the modal closes.

    Returns:
        The ui.dialog() instance (caller opens it with dialog.open()).

    SECURITY:
        - Viewer assets are NOT injected here — Plan 04 injects them at page-build time.
        - Zero raw storage access (Phase-87 invariant).
        - No direct iiif.nli.org.il URL.
        - Nested clickables use js_handler (see T-119-09).

    ASYNC LOADING (G5 fix):
        - create_compare_modal stays SYNC (returns the dialog).
        - Both panes are loaded via an async ``dialog.on("show", _on_show)`` handler
          that awaits BOTH anchor+candidate pane update_content. This runs in the
          active NiceGUI client context when the dialog mounts — NOT via a naked
          background-task coroutine (Codex client-context regression rule, T-119-09).
        - _step / _record_verdict are ASYNC; they await candidate-pane reload only
          (anchor pane is never reloaded — per-pane independence, CMP-02).
        - Modal-level candidate-load generation guard (F-G5): ``_cand_load_gen``
          is incremented each time _fill_candidate creates a NEW candidate viewer
          instance. The candidate-reload coroutine captures its generation, awaits
          update_content, then no-ops if a newer _fill_candidate superseded it.
          This guard works ACROSS instance replacement — AnchorViewer's per-instance
          ``_nav_gen`` only guards the SAME instance; it cannot protect against a
          previous stale instance whose load completes after a new instance is
          created.
    """
    if enrichment is None:
        enrichment = {}

    # Compare state — mutable dict so closures see current values (D-02 Pitfall 6)
    _state = create_compare_state(
        anchor_cand=anchor_cand,
        initial_candidate=initial_candidate,
        filtered_candidates=filtered_candidates,
    )

    dialog = ui.dialog().props("maximized persistent")

    # Container refs to mutate from closures
    _counter_label_ref: list = []      # [label_element]
    _cand_shelfmark_ref: list = []     # [label_element]
    _cand_badge_row_ref: list = []     # [row_element]
    _cand_viewer_container_ref: list = []  # [column_element]
    _cand_pane_ref: list = []          # [column_element] — R2-5 verdict border
    _prev_btn_ref: list = []           # [button_element]
    _next_btn_ref: list = []           # [button_element]

    # Anchor viewer ref — stored here so the show-loader can await update_content.
    _anchor_viewer_ref: list = []      # [AnchorViewer instance]

    # Candidate viewer ref — replaced each _fill_candidate call (Pitfall 3).
    _cand_viewer_ref: list = []        # [AnchorViewer instance]

    # Modal-level candidate-load generation counter (F-G5).
    # Incremented each time _fill_candidate builds a NEW candidate viewer instance.
    # The candidate-reload coroutine captures its generation; if a newer
    # _fill_candidate supersedes it (bumps the counter) the stale coroutine no-ops
    # after its await returns. This guard works ACROSS instance replacement, unlike
    # AnchorViewer's per-instance _nav_gen which only guards the SAME object.
    _cand_load_gen: dict = {"n": 0}

    # Verdict button refs — keyed by verdict ('yes'/'maybe'/'no').
    # Populated during the verdict-buttons loop; refreshed by _refresh_verdict_buttons.
    _verdict_btn_refs: dict = {}

    def _update_counter() -> None:
        """Update the flip-through counter label (e.g. '3 / 47')."""
        cands = _state["candidates"]
        total = len(cands)
        label_text = f"{_state['idx'] + 1} / {total}" if total else "0 / 0"
        if _counter_label_ref:
            _counter_label_ref[0].set_text(label_text)

    def _update_nav_buttons() -> None:
        """Enable/disable Prev/Next based on candidate count (wrap-around rule).

        With wrap-around (D-03 UI-SPEC): both buttons stay enabled when >1 candidate;
        disable both when ≤1 (nothing to navigate).
        """
        cands = _state["candidates"]
        enabled = len(cands) > 1
        if _prev_btn_ref:
            _prev_btn_ref[0].set_enabled(enabled)
        if _next_btn_ref:
            _next_btn_ref[0].set_enabled(enabled)

    def _refresh_verdict_buttons(cand: Candidate) -> None:
        """Refresh verdict-button active/inactive styling for the SHOWN candidate (G3-compare).

        Reads triage.get(cand.sys_id) and sets the matching button to a filled
        (unelevated + solid colour) active state; the other two to outline/flat.

        F-G3c contract: this always keys on the candidate CURRENTLY SHOWN.
        record_verdict writes the verdict AND advances _state atomically, so by
        the time _fill_candidate calls this, _state["current_candidate"] is already
        the POST-ADVANCE candidate. The recorded verdict for the previous candidate
        is persisted in triage and visible when the user navigates back.

        Render-local — _verdict_btn_refs is a factory-scoped dict (no module globals).
        Also updates the candidate pane border (R2-5).
        """
        if not _verdict_btn_refs:
            return
        active_verdict = triage.get(cand.sys_id)
        _triage_colors = {
            "yes": "positive",
            "maybe": "warning",
            "no": "negative",
        }
        for v, btn in _verdict_btn_refs.items():
            if v == active_verdict:
                # Active: filled / unelevated
                btn.props(f"color={_triage_colors[v]} unelevated")
            else:
                # Inactive: outline style
                btn.props(f"outline color={_triage_colors[v]}")

        # R2-5: update candidate pane border to reflect verdict state
        _refresh_pane_border(active_verdict)

    def _refresh_pane_border(active_verdict: Optional[str]) -> None:
        """Update the candidate pane border to reflect the current verdict (R2-5).

        When a verdict exists: 2px solid {TRIAGE_ICONS[verdict]["color"]} (green/amber/red).
        When no verdict: 1px solid var(--border-light) — neutral.
        Mirrors the grid card _make_restyle_fn border formula (candidate_grid.py).
        """
        if not _cand_pane_ref:
            return
        pane = _cand_pane_ref[0]
        if active_verdict and active_verdict in TRIAGE_ICONS:
            color = TRIAGE_ICONS[active_verdict]["color"]
            pane.style(f"border: 2px solid {color};")
        else:
            pane.style("border: 1px solid var(--border-light);")

    def _fill_candidate(cand: Candidate) -> None:
        """Populate the candidate pane with cand (parity _fill_candidate:4086).

        Rebuilds:
          - Candidate shelfmark label
          - Badge row (👁 via badge_and_tooltip + size-mismatch warning)
          - Candidate AnchorViewer (FRESH instance — Pitfall 3)
          - Verdict button refresh (G3-compare)

        Also increments ``_cand_load_gen`` so the candidate-reload coroutine
        (_load_candidate_pane) can detect that a newer instance superseded it (F-G5).
        """
        # Update state tracking
        _state["current_candidate"] = cand
        _state["candidate_page"] = cand.page or 1

        # Shelfmark label
        if _cand_shelfmark_ref:
            _cand_shelfmark_ref[0].set_text(cand.shelfmark or "?")

        # Badge row — rebuild (clear + repopulate)
        if _cand_badge_row_ref:
            badge_row = _cand_badge_row_ref[0]
            badge_row.clear()
            with badge_row:
                # 👁 badge via badge_and_tooltip (D-07/VSM-02)
                icon_name, tooltip_text = badge_and_tooltip(cand)
                if icon_name:
                    ui.icon(icon_name).style("color:#f59e0b;").tooltip(tr(tooltip_text))

                # Size-mismatch warning badge (D-15: ratio > 1.4, anchor's width_cm)
                m = enrichment.get(cand.sys_id, {})
                anchor_w = enrichment.get(anchor_cand.sys_id, {}).get("width_cm")
                if is_size_mismatch(m.get("width_cm"), anchor_w):
                    ui.badge(tr("Size mismatch"), icon="warning").props("color=warning")

        # Fresh AnchorViewer for candidate pane — independent from anchor pane (Pitfall 3).
        # Store it in _cand_viewer_ref so _load_candidate_pane can await update_content.
        # R2-6: suppress_shelfmark_header=True — Compare's green subtitle is the only shelfmark.
        # R2-3: image_max_height="40vh" — caps image so transcription is also visible.
        if _cand_viewer_container_ref:
            container = _cand_viewer_container_ref[0]
            container.clear()
            with container:
                new_viewer = AnchorViewer(
                    sys_id=cand.sys_id,
                    p_num=cand.page,
                    volume_ie=getattr(cand, "volume_ie", None),
                    highlight_pattern=getattr(cand, "highlight_pattern", None),
                    suppress_shelfmark_header=True,
                    image_max_height="40vh",
                )
            # Replace the stored viewer ref
            if _cand_viewer_ref:
                _cand_viewer_ref[0] = new_viewer
            else:
                _cand_viewer_ref.append(new_viewer)

        # F-G5: increment the modal-level generation counter.  Any in-flight
        # _load_candidate_pane coroutine that was awaiting the PREVIOUS viewer's
        # update_content will see its captured generation is stale and no-op.
        _cand_load_gen["n"] += 1

        # Refresh verdict buttons for the newly shown candidate (G3-compare).
        # This also updates the candidate pane border (R2-5).
        _refresh_verdict_buttons(cand)

    async def _load_candidate_pane() -> None:
        """Await the current candidate viewer's update_content (modal-level latest-wins).

        Captures the current generation token from _cand_load_gen before the await;
        after update_content returns, no-ops if a newer _fill_candidate has already
        superseded this load (F-G5 — works across instance replacement, unlike the
        per-instance _nav_gen guard inside AnchorViewer).

        CMP-02: only the CANDIDATE pane is reloaded; the anchor pane is fixed.
        """
        if not _cand_viewer_ref:
            return
        my_gen = _cand_load_gen["n"]
        cand = _state["current_candidate"]
        viewer = _cand_viewer_ref[0]
        await viewer.update_content(p_num=cand.page or 1)
        # Latest-wins guard: if a newer _fill_candidate ran while we were awaiting,
        # the new viewer is already stored; our result is stale — discard it.
        if my_gen != _cand_load_gen["n"]:
            return
        # (If generation still matches, the load succeeded for the shown candidate.)

    async def _step(delta: int) -> None:
        """Advance/retreat through filtered_candidates (parity desktop step(delta):3741).

        ASYNC: after rebuilding the candidate viewer via _fill_candidate, awaits
        _load_candidate_pane so the new candidate's image+transcription actually loads.
        The anchor pane is NEVER reloaded (per-pane independence, CMP-02).
        """
        step_candidate(_state, delta)
        _fill_candidate(_state["current_candidate"])
        _update_counter()
        _update_nav_buttons()
        await _load_candidate_pane()

    async def _record_verdict(verdict: str) -> None:
        """Record verdict and AUTO-ADVANCE (D-03, parity _mark → wb.mark → triage:4202).

        ASYNC: after record_verdict() writes the verdict and advances _state (F-G3c
        constraint — write+advance is atomic), _fill_candidate is called on the
        POST-ADVANCE candidate and its pane is loaded via _load_candidate_pane.

        F-G3c: _refresh_verdict_buttons (inside _fill_candidate) keys on the
        POST-ADVANCE candidate's triage entry. The recorded verdict for the previous
        candidate is persisted in triage and visible on back-navigation.
        """
        record_verdict(_state, verdict, triage, on_verdict=on_verdict)
        # After step_candidate() inside record_verdict(), _state["current_candidate"] is updated
        _fill_candidate(_state["current_candidate"])
        _update_counter()
        _update_nav_buttons()
        await _load_candidate_pane()

    def _handle_close() -> None:
        """Handle modal close — invoke on_close callback if provided."""
        dialog.close()
        if on_close:
            on_close()

    # ── Build the modal UI ────────────────────────────────────────────────────
    with dialog:
        with ui.card().classes("w-full h-full").style(
            "display:flex; flex-direction:column; overflow:hidden;"
        ):
            # ── Header bar ───────────────────────────────────────────────────
            with ui.row().classes(
                "w-full items-center justify-between px-4 py-2"
            ).style(
                "background: var(--bg-header); color: white; flex-shrink:0;"
            ):
                ui.label(tr("Compare")).classes("text-lg font-semibold")
                # R2-2: force LTR + bidi-isolate so "5 / 118" is not flipped to
                # "118 / 5" by the Hebrew RTL UI direction.
                # Marked 'compare-flip-counter' for render-smoke element location.
                counter_label = ui.label("").classes("text-sm").style(
                    "color:rgba(255,255,255,0.8); direction:ltr; unicode-bidi:isolate;"
                ).mark("compare-flip-counter")
                _counter_label_ref.append(counter_label)
                ui.button(icon="close", on_click=_handle_close).props(
                    "flat dense round"
                ).classes("text-white")

            # ── Two-pane body ─────────────────────────────────────────────────
            with ui.row().classes("w-full flex-grow min-h-0").style(
                "overflow:hidden; flex:1;"
            ):
                # ── Anchor pane (left) ────────────────────────────────────────
                with ui.column().classes("flex-1 gap-4 p-4 overflow-y-auto").style(
                    "border-right: 2px solid var(--border-light);"
                ):
                    ui.label(tr("Anchor")).classes(
                        "text-xs font-bold uppercase"
                    ).style("color: var(--text-muted);")

                    ui.label(anchor_cand.shelfmark or "?").classes(
                        "text-sm font-semibold"
                    ).style("color: var(--primary-700);")

                    # Fresh AnchorViewer for anchor pane — NOT the sticky-page viewer (Pitfall 3).
                    # Stored in _anchor_viewer_ref so the show-loader can await update_content.
                    # R2-6: suppress_shelfmark_header=True — green subtitle is the only shelfmark.
                    # R2-3: image_max_height="40vh" — caps image so transcription stays visible.
                    anchor_viewer = AnchorViewer(
                        sys_id=anchor_cand.sys_id,
                        p_num=anchor_cand.page,
                        volume_ie=getattr(anchor_cand, "volume_ie", None),
                        highlight_pattern=getattr(anchor_cand, "highlight_pattern", None),
                        suppress_shelfmark_header=True,
                        image_max_height="40vh",
                    )
                    _anchor_viewer_ref.append(anchor_viewer)

                # ── Candidate pane (right) ────────────────────────────────────
                # R2-5: capture ref for verdict-border updates; mark for render-smoke.
                cand_pane_col = ui.column().classes("flex-1 gap-4 p-4 overflow-y-auto").mark(
                    "compare-candidate-pane"
                ).style("border: 1px solid var(--border-light);")
                _cand_pane_ref.append(cand_pane_col)
                with cand_pane_col:
                    ui.label(tr("Candidate")).classes(
                        "text-xs font-bold uppercase"
                    ).style("color: var(--text-muted);")

                    cand_shelfmark_label = ui.label("").classes(
                        "text-sm font-semibold"
                    ).style("color: var(--primary-700);")
                    _cand_shelfmark_ref.append(cand_shelfmark_label)

                    cand_badge_row = ui.row().classes("gap-2 items-center flex-wrap")
                    _cand_badge_row_ref.append(cand_badge_row)

                    cand_viewer_container = ui.column().classes("w-full gap-4")
                    _cand_viewer_container_ref.append(cand_viewer_container)

            # ── Verdict bar (sticky bottom) ───────────────────────────────────
            with ui.row().classes(
                "w-full items-center justify-between px-4 py-2 flex-wrap gap-2"
            ).style(
                "background: var(--bg-tertiary); position:sticky; bottom:0; flex-shrink:0;"
            ):
                # R2-2: nav buttons — label carries the RTL-correct chevron (from 119-09
                # HE translations: "‹ Prev"→"הקודם ›", "Next ›"→"‹ הבא").  No fixed
                # icon= so there is no contradicting LTR chevron-icon under RTL.
                prev_btn = ui.button(
                    tr("‹ Prev")
                ).props("flat dense").on("click", lambda: _step(-1))
                _prev_btn_ref.append(prev_btn)

                # R2-4: Verdict buttons — ✓/?/✗ glyph buttons (desktop parity).
                # Refs captured into _verdict_btn_refs so _refresh_verdict_buttons can
                # toggle active/inactive state on each candidate change (G3-compare).
                # PRESERVE active(unelevated)/inactive(outline) toggle logic (G3-compare).
                with ui.row().classes("gap-2 items-center"):
                    for verdict, q_color in [
                        ("yes", "positive"),
                        ("maybe", "warning"),
                        ("no", "negative"),
                    ]:
                        _v = verdict

                        def _make_verdict_handler(v=_v):
                            async def _handler():
                                await _record_verdict(v)
                            return _handler

                        _btn = (
                            ui.button(TRIAGE_ICONS[_v]["glyph"])
                            .props(f"outline color={q_color} size=md")
                            .tooltip(tr(TRIAGE_ICONS[_v]["tooltip"]))
                            .on("click", _make_verdict_handler())
                        )
                        _verdict_btn_refs[_v] = _btn

                # R2-2: Next button — no fixed icon= (avoids LTR chevron contradicting HE label).
                next_btn = ui.button(
                    tr("Next ›")
                ).props("flat dense").on("click", lambda: _step(1))
                _next_btn_ref.append(next_btn)

    # Attach async show-loader: fires when dialog mounts in the live client context.
    # Awaits BOTH the anchor pane and the candidate pane's update_content so images
    # and transcriptions actually render (G5 fix). Runs in the active NiceGUI client
    # context via dialog.on("show") — NOT via a naked background coroutine
    # (T-119-09 / Codex client-context regression rule).
    async def _on_show(_: object = None) -> None:
        """Async show handler — awaits both pane update_content calls (G5 fix)."""
        if _anchor_viewer_ref:
            await _anchor_viewer_ref[0].update_content(p_num=anchor_cand.page or 1)
        await _load_candidate_pane()

    dialog.on("show", _on_show)

    # R2-7: Escape-key handler — close the modal when Escape is pressed (keydown only).
    # The dialog is props("persistent") so the built-in Esc-close is DISABLED;
    # we install a ui.keyboard listener scoped to this dialog's open state.
    # Guard: only act when THIS dialog is currently open (dialog.value truthy) so
    # a stale hidden-dialog keyboard from a prior _open_compare call cannot fire
    # globally (Codex P119-R2-7-1).
    def _on_escape(e: object) -> None:
        """Close the Compare modal on Escape keydown (R2-7)."""
        # Guard: only act when this dialog instance is currently open
        if not getattr(dialog, "value", False):
            return
        # Guard: keydown only (avoid double-fire on keyup)
        action = getattr(e, "action", None)
        if action is not None and not getattr(action, "keydown", True):
            return
        key = getattr(e, "key", None)
        key_name = getattr(key, "name", None) if key is not None else str(getattr(e, "key", ""))
        if key_name == "Escape":
            _handle_close()

    with dialog:
        _kb = ui.keyboard(on_key=_on_escape)

    # Expose Esc handler for tests (test seam: invoke directly with a synthetic Escape event).
    dialog._on_escape = _on_escape  # type: ignore[attr-defined]
    dialog._keyboard = _kb  # type: ignore[attr-defined]

    # Expose show-loader for behavioral tests (Task 2 test seam):
    # A test can call _on_show directly via asyncio.run with stub viewers.
    dialog._on_show = _on_show  # type: ignore[attr-defined]
    dialog._load_candidate_pane = _load_candidate_pane  # type: ignore[attr-defined]

    # Populate the candidate pane with the initial candidate
    _fill_candidate(initial_candidate)
    _update_counter()
    _update_nav_buttons()

    return dialog
