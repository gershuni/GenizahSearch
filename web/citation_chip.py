# -*- coding: utf-8 -*-
"""The "How to cite" chip — a small, always-visible floating control.

REPLACES the sticky citation footer (owner, 2026-09-04). That footer was a
full-width strip carrying the MiDRASH citation, an auto-collapse timer, two
dismiss buttons and a pair of storage keys; it cost a band of every page, and
its wording could not follow what a reader was actually looking at.

WHAT THE CHIP SHOWS, AND WHY THERE ARE TWO CITATIONS
----------------------------------------------------
Owner: "most people will want to cite the website usage as a whole (they've
found many things there), and the regular chip will just mention the dicta
genizah search (and site address) and zenodo citation."

So the DEFAULT — every page, including /browse — is the SITE citation. A reader
who worked across forty manuscripts gains nothing from a citation pinned to
whichever folio was on screen last, and would have to edit it back out.

A manuscript page ADDS a page-specific citation above it, naming the library,
the shelfmark, the folio and whoever made that particular transcription. Both
are offered with their own copy button; neither is chosen for the reader.

Both strings come from `shared/transcription_credits`, which is also what the
printed sheet and the Word export use — so a citation copied from the chip, a
citation printed on paper, and a citation in an exported .docx cannot disagree
about who made the text.

WHY A REGISTRY, AND WHY IT IS SMALL
-----------------------------------
The chip is built in `create_layout`, which runs BEFORE the page body: every
route does `content = create_layout()` then `with content: create_<page>()`. But
on /browse the manuscript is known only inside the page. There is no existing
precedent in this codebase for a page updating a layout-owned element — the refs
idiom (`BrowsePageRefs`) runs the other way, a page handing refs OUT — so this
module keeps a per-client registry of one updater function and a page calls
`set_page_citation`.

Per CLIENT, not per user: `app.storage.user` is shared across all of a reader's
tabs, so a manuscript open in one tab would rewrite the chip in another. The
registry is keyed by `ui.context.client.id`, captured SYNCHRONOUSLY during
layout construction — the rule `web/main.py` states for itself, citing its own
prior incident: never re-derive `ui.context.client` inside a deferred callback.

Every mutation goes through `client_gone()` (`web/client_guard.py`) and a
`RuntimeError` guard. That module exists because nine inline call sites once
checked a NiceGUI attribute that does not exist; this follows its documented
shape rather than inventing a tenth.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from nicegui import ui

from shared.transcription_credits import PageCitation, page_citation, site_citation
from web.client_guard import client_gone
from web.translations import tr

logger = logging.getLogger(__name__)

#: client id -> the chip's repaint function. One entry per live connection.
#: Entries are dropped when the client is gone (see `_forget`), so a long-lived
#: process does not accumulate one per page view.
_UPDATERS: Dict[str, Callable[[Optional[PageCitation]], None]] = {}


def _forget(client_id: str) -> None:
    _UPDATERS.pop(client_id, None)


def retrieved_today() -> str:
    """Today's date for an "accessed" stamp, as an ISO string in UTC.

    ISO is the TRANSPORT form, not the display form: this value has to cross
    NiceGUI's JSON-backed session storage to reach the Word export's FastAPI
    route, where a `date` object would not survive.
    `shared.transcription_credits` parses it and writes it out for the reader's
    language ("Sept. 4, 2026" / "4 בספטמבר, 2026").

    UTC, not server-local: an accessed-date in a citation is closer to a durable
    record than to an ephemeral UI label, and this repo already reserves
    tz-aware UTC for anything that becomes one (API and Supabase timestamps),
    keeping naive `datetime.now()` for throwaway display. It also means the
    stamp does not depend on where the box happens to be deployed.

    Nothing in this codebase knows the READER's timezone — there is no
    client-side timezone detection anywhere in web/ — so a citation accessed
    near midnight can name the neighbouring day. For an accessed-date that is
    immaterial, and inventing a guess would be worse than being consistent.
    """
    return datetime.now(timezone.utc).date().strftime('%Y-%m-%d')


def set_page_citation(citation: Optional[PageCitation]) -> None:
    """Give the chip a page-specific citation, or clear it with ``None``.

    Safe to call from a page render or from a later event handler. A call for a
    client whose chip is gone is a no-op, not an error: pages are rebuilt often
    here, and a citation update is never worth taking a surface down for.
    """
    try:
        client_id = ui.context.client.id
    except Exception:                                            # noqa: BLE001
        return  # no client context (a test harness, a background task)
    update = _UPDATERS.get(client_id)
    if update is None:
        return
    try:
        update(citation)
    except RuntimeError:
        # `parent_slot has been deleted` and friends -- the layout this chip
        # belonged to is gone. Drop the entry so we stop trying.
        _forget(client_id)
    except Exception:                                            # noqa: BLE001
        logger.debug('citation chip update failed', exc_info=True)


def render_citation_chip(*, lang: str = 'en') -> None:
    """Build the chip. Called once per page render, from `create_layout`."""
    try:
        client = ui.context.client
    except Exception:                                            # noqa: BLE001
        return
    client_id = client.id

    state: Dict[str, Any] = {'page': None}

    # Fixed, bottom-start, where the footer used to sit. z-index 2000 matches
    # the footer it replaces -- ABOVE page content, and deliberately BELOW
    # Quasar's dialogs (6000), so a dialog is never obscured by a citation chip.
    # `print-hide`: the printed sheet carries its own masthead and citation.
    chip = ui.element('div').classes('citation-chip print-hide').style(
        'position: fixed; bottom: 14px; inset-inline-start: 14px; z-index: 2000;'
    )

    with chip:
        with ui.button(icon='format_quote').props(
            f'no-caps unelevated dense aria-label="{tr("How to cite")}"'
        ).classes('citation-chip-btn'):
            ui.label(tr('How to cite')).classes('citation-chip-label')

            # A MENU, opened by click or keyboard -- not a hover popup. The
            # owner's "hovering chip" meant floating-and-always-visible, which
            # is the chip itself; the panel still has to be reachable by tap and
            # by keyboard. The site's own accessibility statement claims
            # keyboard navigability, and both existing floating-control
            # precedents here (the old footer, the What's New button) are
            # click-triggered.
            with ui.menu().props('auto-close=false').classes('citation-chip-menu'):
                panel = ui.column().classes('citation-chip-panel gap-2 p-3').style(
                    'max-width: min(92vw, 34rem);'
                )

        def _copy_button(text: str) -> None:
            payload = json.dumps(text)
            ui.button(
                tr('Copy citation'), icon='content_copy',
                on_click=lambda: ui.run_javascript(
                    'navigator.clipboard.writeText(%s);' % payload
                ),
            ).props('flat dense no-caps color=primary').classes('self-start')

        def _render_entry(citation: PageCitation) -> None:
            ui.label(citation.heading).classes('text-xs font-bold').style(
                'color: var(--text-secondary);')
            # Base direction follows the UI LANGUAGE, not the script mix.
            #
            # `direction: ltr` was the first cut and it visibly scrambled the
            # Hebrew: the citation is a mixed run (Hebrew site name and "נצפה
            # בתאריך" around Latin URLs, author names and a DOI), so an LTR base
            # reorders the Hebrew clauses and throws the parenthesis around the
            # date to the wrong side. The clauses are ordered for a reader of
            # `lang`, so the base direction has to be that reader's.
            #
            # The COPIED string is unaffected either way -- the clipboard gets
            # the raw text -- so this is purely whether it looks trustworthy.
            ui.label(citation.text).classes('text-xs').style(
                'color: var(--text-primary); line-height: 1.5; '
                'direction: %s; text-align: start; user-select: all;'
                % ('rtl' if str(lang or '').lower().startswith('he') else 'ltr')
            )
            _copy_button(citation.text)

        def _repaint(page: Optional[PageCitation]) -> None:
            state['page'] = page
            if client_gone(panel):
                _forget(client_id)
                return
            panel.clear()
            stamp = retrieved_today()
            with panel:
                # The page-specific citation FIRST when there is one -- it is
                # the more specific claim -- then the site citation, which is
                # the one most readers actually want and is therefore never
                # hidden behind a toggle.
                if page is not None:
                    _render_entry(page)
                    ui.separator()
                _render_entry(site_citation(lang=lang, retrieved_on=stamp))

                # WHY citing matters -- the grant/funding argument, which is the
                # part of the removed citation modal the owner wanted kept. It
                # moved to `/about#citing-midrash`, and until this link existed
                # NOTHING pointed at that anchor: the footer that was supposed to
                # link it was removed by this same chip. A reader asking how to
                # cite is the one reader who might want to know why.
                #
                # Not a new string: reuses the `Learn more` key the telemetry
                # re-invite bar already ships in both languages.
                ui.link(tr('Learn more'), '/about#citing-midrash').classes(
                    'text-xs self-start').style(
                    'color: var(--text-secondary);')
            panel.update()

        _UPDATERS[client_id] = _repaint

        # Drop the entry when THIS client's connection goes, not when someone
        # next tries to use it. `_forget` on a failed update is a backstop for a
        # dead LAYOUT; it never fires for a reader who simply closed the tab,
        # since nothing asks that client for a citation again -- so on a
        # long-running server ordinary traffic accumulated one closure per
        # visit, each holding a whole obsolete UI tree.
        #
        # `client.on_disconnect`, NOT `app.on_disconnect`. The app-level hook
        # appends to ONE global list that fires for EVERY disconnect, so a
        # per-render handler there meant the first reader to close a tab forgot
        # the chips of every other reader still on the site -- their citations
        # then silently stopped following the page. That is worse than the leak
        # it replaced: a leak wastes memory, this served wrong citations. The
        # global list also grew forever, so the leak had merely moved.
        try:
            client.on_disconnect(lambda *_a, _id=client_id: _forget(_id))
        except Exception:                                        # noqa: BLE001
            # An older NiceGUI without the per-client hook. The failed-update
            # backstop still applies; that is a leak, not a wrong citation, and
            # must not take the chip down.
            logger.debug('could not register citation chip cleanup',
                         exc_info=True)

        _repaint(None)


def browse_page_citation(
    version_info: Optional[Dict[str, Any]] = None,
    *,
    lang: str,
    library: Optional[str] = None,
    shelfmark: Optional[str] = None,
    folio: Optional[str] = None,
    page_url: Optional[str] = None,
) -> PageCitation:
    """The manuscript-page citation, with today's date filled in.

    A thin seam so `/browse` does not have to know how the accessed-date is
    produced, and so the chip and the Word export stamp it the same way.
    """
    return page_citation(
        version_info,
        lang=lang,
        library=library,
        shelfmark=shelfmark,
        folio=folio,
        page_url=page_url,
        retrieved_on=retrieved_today(),
    )
