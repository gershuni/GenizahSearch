# -*- coding: utf-8 -*-
"""
Browse Enrichment Functions

Extracted from web/pages/browse.py (Phase 73).
Plan 01 creates this stub with BrowsePageRefs dataclass.
Plan 02 adds the three enrichment function bodies
(load_enrichment, update_enrichment_sections, populate_bib_catalog_buttons).

Each function that was a closure in create_browse_page() now takes explicit
state and refs parameters instead of capturing them via closure.

IMPORTANT: This module must NOT import from web.pages.browse to avoid
circular imports. All shared types come from web.pages.browse_state.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from nicegui import ui, run

from web.translations import tr, get_language
from web.document_service import get_document_for_fragment, get_section_for_page, get_all_sources_for_fragment
from web.feature_flags import web_fgp_enabled
from shared.fgp_service import (
    get_fgp_sources_for_fragment, filter_sources_for_page, folio_label_for_displayed_page,
    fgp_image_number_for_displayed_page, fgp_needs_full_htr,
)
from web.pages.browse_state import BrowseState, _crossref_cache
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)


# ===========================================================================
# The discovery connections panel (Phase 136, plan 136-17, PANEL-01/PANEL-02).
#
# THE OFFLOAD CONTRACT, stated where the code is rather than only in the plan:
#
#   Every discovery read on this path is a DIRECT `await` on a `web.discovery`
#   async wrapper. This path adds NO `run.io_bound`, makes no synchronous
#   service call, and never touches `web.discovery._service`.
#
#   The wrappers already dispatch off the loop internally
#   (`DiscoveryService._enveloped_off_loop` -> `_run_off_loop` ->
#   `run_in_executor` + `asyncio.wait`, never `wait_for`, because executor
#   threads are not cancellable). Handing an async wrapper to `run.io_bound`
#   returns a coroutine object to a sync worker and never executes the query;
#   calling one from INSIDE a `run.io_bound` worker is a NESTED offload that
#   burns two threadpool slots per panel load on a single-worker server.
#
#   Four EAGER reads (three gathered, then the manuscript-works read once the
#   page ids resolve) = FOUR executor crossings on a cold panel load, not one.
#   That is the accepted design: three of the four are served by
#   `_enveloped_off_loop(..., cache_name=...)`, a per-argument
#   sidecar-version-keyed LRU. A single composite callable would be keyed on
#   the whole argument tuple -- `page_id` included -- so it would invalidate
#   the manuscript-scope entry on every folio turn and re-run all four
#   queries each time.
#
#   The related-page ROWS read is LAZY and is NOT part of the page load; it is
#   issued by the panel's own toggle (see web/components/discovery_panel.py).
#   The bundle's `related_rows` field stays None here, which the model reads as
#   NOT REQUESTED -- a state distinct from an `ok` zero and from an outage.
# ===========================================================================


def discovery_panel_enabled() -> bool:
    """Whether the connections panel exists on this page AT ALL.

    Read LAZILY (never captured at import) so a test can flip it, and kept
    SEPARATE from `web.discovery_assets.discovery_available()`, which ANDs the
    same flag with sidecar readiness:

    * flag OFF  -> no placeholder, no entry control, no read. The browse page
      is byte-for-byte what it was before this plan, which is what "deployed
      with the flag off for the public" has to mean.
    * flag ON, sidecar absent/corrupt -> `discovery_available()` is False, every
      wrapper short-circuits to an `unavailable` envelope, and the panel renders
      a visible temporary-unavailable state with a retry. An outage must never
      look like a manuscript with nothing on it (D-13).
    """
    from web.feature_flags import DISCOVERY_ENABLED
    return bool(DISCOVERY_ENABLED)


async def fetch_discovery_panel_bundle(page, lang: str, is_stale=None):
    """The four EAGER discovery reads for one folio, as a `PanelServiceBundle`.

    Module-level (not a closure) for three reasons: the AST offload guard can
    find it by name, the retry handler re-runs exactly the same code the page
    load ran, and it is testable without a browser.

    `is_stale` is called after EVERY await. A fast page navigation otherwise
    paints a stale panel over the wrong folio -- and, worse, issues a second
    query for a folio the reader has already left.

    Returns None when the panel does not apply (flag off, no resolvable
    `page_id`, or the load went stale). Never raises.
    """
    from shared.discovery_panel_model import PanelServiceBundle
    from shared.discovery_surface_projection import STATUS_OK, make_envelope
    from web import discovery as _discovery
    from web.services import discovery_page_id_from_header

    # Per-page values into PLAIN LOCALS before any await (the page object is
    # mutated in place by the enrichment phase).
    _sys_id = getattr(page, 'sys_id', None)
    _volume_ie = getattr(page, 'volume_ie', None)
    _page_id = discovery_page_id_from_header(getattr(page, 'full_header', '') or '')
    if not _sys_id or not _page_id:
        return None

    # The three INDEPENDENT eager reads, in one round.
    page_ids_env, claims_env, related_count_env = await asyncio.gather(
        _discovery.get_manuscript_page_ids(_sys_id, volume_ie=_volume_ie),
        _discovery.get_claims_for_page_enveloped(_page_id, lang=lang),
        _discovery.get_related_page_count_enveloped(_page_id),
    )
    if is_stale is not None and is_stale():
        return None

    # The manuscript-works read is issued ONLY over a RESOLVED page scope.
    # `meta['resolved']` is read explicitly and compared to True: an outage
    # envelope carries no `resolved` key at all, and a branch that treated the
    # missing key as falsy-but-present -- or defaulted it to True -- would query
    # the empty page set and render "nothing elsewhere in this manuscript"
    # during an outage (T-136-17-10).
    scope_resolved = (
        page_ids_env.get('status') == STATUS_OK
        and (page_ids_env.get('meta') or {}).get('resolved') is True
    )
    if scope_resolved:
        works_env = await _discovery.get_manuscript_works_enveloped(
            list(page_ids_env.get('items') or ()), lang=lang)
        if is_stale is not None and is_stale():
            return None
    else:
        # NOT the works query's answer -- the query was never issued. This is
        # the SAME shape the service itself returns when the page scope does not
        # resolve (`page_scope_resolved: False`), and the model's `_scope_state`
        # reads the page-ID envelope FIRST, so the pane reports no fact about
        # the manuscript on this branch.
        works_env = make_envelope(STATUS_OK, [], 0, meta={'page_scope_resolved': False})

    return PanelServiceBundle(
        claims=claims_env,
        page_ids=page_ids_env,
        manuscript_works=works_env,
        related_count=related_count_env,
        related_rows=None,          # NOT REQUESTED -- the toggle owns this read
        lang=lang,
    )


@dataclass
class BrowsePageRefs:
    """UI element references and callbacks needed by extracted browse_enrichment functions.

    Populated in create_browse_page() after all UI elements and callbacks are defined.
    These are ephemeral per-page references, NOT persistent state (per D-06).

    Population timing:
    - enrichment_refs: alias assigned at construction, populated incrementally
      during update_content() calls (lines ~4189, ~4313-4341, ~4617-4618)
    - load_generation: default {'value': 0} at construction, used as-is
    - page_client: set immediately after construction (ui.context.client)
    - content_container: set when main content area is created (~line 4210+)
    - slider_refs: alias assigned at construction, populated in update_content()
    - enter_joined_view: set after enter_joined_view() defined (~line 1450)
    - update_content: set after update_content() defined (~line 2083)

    All callbacks are set BEFORE any asyncio.ensure_future(load_page(...)) call,
    which happens at the end of create_browse_page() (~lines 4982-5041).
    """
    enrichment_refs: Dict[str, Any] = field(default_factory=dict)
    # SAFETY NOTE: load_generation MUST remain a mutable dict container (not an int).
    # Local aliases in browse.py bind to this dict object -- if changed to a primitive,
    # the alias would be a copy and stale-generation checks would silently break.
    load_generation: Dict[str, int] = field(default_factory=lambda: {'value': 0})
    page_client: Any = None
    content_container: Any = None
    slider_refs: Dict[str, Any] = field(default_factory=dict)

    # Callback functions (set after definition in create_browse_page)
    enter_joined_view: Any = None
    update_content: Any = None


async def load_enrichment(state: BrowseState, refs: BrowsePageRefs, page, generation):
    """Phase B: Load PGP + FJMS enrichment data in background."""
    state.enrichment_loading = True

    async def fetch_pgp():
        _page_sys_id = page.sys_id
        _page_p_num = page.p_num

        def _pgp_sync():
            all_sources = get_all_sources_for_fragment(_page_sys_id) or []
            # Merge FGP transcriptions as additional, distinct sources (FGP-05).
            if web_fgp_enabled():
                all_sources = all_sources + (get_fgp_sources_for_fragment(_page_sys_id) or [])
            pgp_doc = get_document_for_fragment(_page_sys_id, _page_p_num)
            # SEED-030: a whole-document FGP edition must be judged against the
            # WHOLE-manuscript HTR (to demote selective excerpts while keeping
            # comprehensive ones). Fetch it here — off the event loop — but ONLY
            # when such a row exists, so the browse hot path is unaffected otherwise.
            full_htr = None
            if all_sources and fgp_needs_full_htr(all_sources):
                try:
                    from web.services import get_service
                    pages = get_service().get_full_manuscript(_page_sys_id) or []
                    full_htr = "\n".join((getattr(p, 'text', '') or '') for p in pages)
                except Exception as _e:
                    logger.debug("full-MS HTR fetch failed for %s: %s", _page_sys_id, _e)
            return all_sources, pgp_doc, full_htr

        try:
            return await run.io_bound(_pgp_sync)
        except Exception as e:
            logger.error(f"Failed to fetch PGP data: {e}")
            return None, None, None

    async def fetch_fjms():
        _page_sys_id = page.sys_id

        def _fjms_sync():
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            if fjms.is_available():
                result = {
                    'catalog_records': fjms.get_catalog_records(_page_sys_id),
                    'domains': fjms.get_domains(_page_sys_id),
                    'bibliography': fjms.get_bibliography(_page_sys_id),
                    'source_names': fjms.get_source_names(_page_sys_id),
                    'catalog_refs': fjms.get_catalog_refs(_page_sys_id),
                    'has_measurements': fjms.has_measurements(_page_sys_id),
                }
                # Phase 57: Visual Similarity availability check
                try:
                    from shared.visual_similarity_service import get_vs_service
                    vs_svc = get_vs_service(thread_safe=True)
                    if vs_svc.is_available():
                        _vs_count = vs_svc.get_suggestion_count(_page_sys_id)
                        result['has_visual_suggestions'] = _vs_count > 0
                        result['visual_suggestion_count'] = _vs_count
                    else:
                        result['has_visual_suggestions'] = False
                        result['visual_suggestion_count'] = 0
                except Exception:
                    result['has_visual_suggestions'] = False  # Feature detection failed; assume not available
                    result['visual_suggestion_count'] = 0
                return result
            return None

        try:
            return await run.io_bound(_fjms_sync)
        except Exception as e:
            logger.error(f"Failed to fetch FJMS data: {e}")
            return None

    async def fetch_crossref():
        _page_sys_id = page.sys_id
        # Check session cache first
        if _page_sys_id in _crossref_cache:
            return _crossref_cache[_page_sys_id]

        def _crossref_sync():
            from shared.nli_crossref_service import get_nli_crossref_service
            svc = get_nli_crossref_service(thread_safe=True)
            if svc.is_available() and _page_sys_id:
                return svc.get_crossref_metadata(_page_sys_id)
            return {}

        try:
            result = await run.io_bound(_crossref_sync)
            _crossref_cache[_page_sys_id] = result  # Cache for session
            return result
        except Exception as e:
            logger.error(f"Failed to fetch crossref data: {e}")
            return {}

    async def fetch_browse_enrichment():
        """Fetch crossref + Oxford + Cambridge + attribution data deferred from Phase A."""
        _page = page
        _sys_id = _page.sys_id
        _shelfmark = _page.shelfmark
        _library_code = _page.library_code
        _is_oxford = _page.is_oxford
        _p_num = _page.p_num
        _total_pages = _page.total_pages

        def _browse_enrich_sync():
            from web.services import ATTRIBUTION_BY_LIBRARY, _get_library_attribution
            from web.state import state as state_mod
            result = {}

            # Attribution cascade
            attribution = ''
            if _sys_id and hasattr(state_mod.meta_mgr, 'nli_cache'):
                cached_meta = state_mod.meta_mgr.nli_cache.get(_sys_id, {})
                attribution = cached_meta.get('attribution', '')
            if _library_code in ATTRIBUTION_BY_LIBRARY:
                lib_attr = _get_library_attribution(_library_code)
                if lib_attr is not None:
                    attribution = lib_attr
            elif _is_oxford:
                attribution = 'Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0'
            if attribution:
                result['attribution'] = attribution

            # Oxford codicological
            if _is_oxford and _sys_id:
                if hasattr(state_mod.meta_mgr, 'get_part_for_folio'):
                    part_id = state_mod.meta_mgr.get_part_for_folio(_sys_id)
                    if part_id:
                        result['oxford_part_id'] = part_id
                        if hasattr(state_mod.meta_mgr.codico_mgr, 'get_part_display_name'):
                            result['oxford_part_display'] = state_mod.meta_mgr.codico_mgr.get_part_display_name(part_id)
                        part_meta = state_mod.meta_mgr.get_part_metadata(part_id)
                        if part_meta:
                            ox_meta = {}
                            for key in ['title', 'contents', 'provenance']:
                                if part_meta.get(key):
                                    ox_meta[key] = part_meta[key]
                            result['oxford_part_metadata'] = ox_meta
                            if part_meta.get('direct_link'):
                                result['external_url'] = part_meta['direct_link']

            # Cambridge MARC
            if 'external_url' not in result:
                marc_data = {}
                if hasattr(state_mod.meta_mgr, 'nli_cache') and _sys_id in state_mod.meta_mgr.nli_cache:
                    marc_data = state_mod.meta_mgr.nli_cache[_sys_id].get('marc', {})
                ext_link = marc_data.get('external_iiif_link')
                if ext_link and "cudl.lib.cam.ac.uk" in ext_link:
                    result['is_cambridge'] = True
                    result['external_url'] = ext_link.replace("/iiif/", "/view/")

            # NLI crossref queries
            try:
                from shared.nli_crossref_service import get_nli_crossref_service
                crossref_svc = get_nli_crossref_service(thread_safe=True)
                if crossref_svc.is_available() and _sys_id:
                    from genizah_core import normalize_shelfmark
                    norm_shelf = normalize_shelfmark(_shelfmark) if _shelfmark else None
                    # Phase 84 follow-up: pass raw shelfmark so the service can
                    # fall through to the bridge cascade when the canonical-form
                    # direct query misses (Mosseri label, T-S slash + leading-zero,
                    # Or.-numeric-collapse). Without this, _has_cambridge stays
                    # False in browse and the Cambridge button + IIIF images are
                    # suppressed despite a valid manifest URL upstream.
                    result['image_source_info'] = crossref_svc.get_image_sources(
                        _sys_id, normalized_shelfmark=norm_shelf, shelfmark=_shelfmark
                    )
                    if not result.get('is_cambridge') and result['image_source_info'].get('cambridge'):
                        result['is_cambridge'] = True
                    folio_images = crossref_svc.get_folio_images(_sys_id)
                    result['folio_images'] = folio_images
                    if (folio_images
                            and len(folio_images) == _total_pages
                            and 0 < _p_num <= len(folio_images)):
                        result['folio_label'] = folio_images[_p_num - 1].get('folio_label', '')
                    # Folio for ALIGNING FGP to the displayed image — multi-IE aware
                    # (total_pages = k * folio_count when a manuscript has several
                    # text editions), so it resolves even when the strict equality
                    # above does not. Kept separate so the display-label logic above
                    # is unchanged.
                    result['fgp_folio_label'] = folio_label_for_displayed_page(
                        folio_images, _p_num, _total_pages)
                    # EXACT per-image FGP key (c_number ↔ fgp_image_number_id):
                    # preferred over the folio label, which is only coincidentally
                    # equal and breaks on bare-sequence / NULL / duplicate
                    # image_side and multi-volume manuscripts. Same positional
                    # (multi-IE-aware) resolution as the label above.
                    result['fgp_image_number'] = fgp_image_number_for_displayed_page(
                        folio_images, _p_num, _total_pages)

                    # Skip get_physical_metadata here -- fetch_crossref already gets it
                    # via get_crossref_metadata. We read it from crossref_data after gather.

                    result['library_viewer_url'] = crossref_svc.get_library_viewer_url(_sys_id)

                    # Derive fl_id for metadata-only records (no Tantivy fl_id)
                    if not _page.fl_id and folio_images:
                        first_fl = folio_images[0].get('fl_id')
                        if first_fl:
                            result['derived_fl_id'] = first_fl
            except Exception as e:
                logger.error("Browse enrichment crossref error: %s", e)

            # External images from nli_cache (populated by enrich_metadata).
            # Phase 117 Plan 03: delegated to the shared resolve_external_images helper
            # (web/components/image_resolution.py) — one source of truth for D-10.
            # This runs inside _browse_enrich_sync which is already under run.io_bound
            # (browse_enrichment.py:269), so the I/O constraint is satisfied.
            if _sys_id:
                from web.components.image_resolution import resolve_external_images
                _ext = resolve_external_images(_sys_id, meta_mgr=state_mod.meta_mgr)
                result['cambridge_images'] = _ext['cambridge_images']
                result['external_provider'] = _ext['external_provider']
                result['cambridge_alignment'] = _ext['cambridge_alignment']
                # Phase 84 follow-up: propagate external_url back from nli_cache when
                # the MARC branch (line 192-199 above) didn't set one. enrich_metadata
                # may have resolved it via the bridge supplement (Cambridge for
                # CUDL-only rows like Mosseri). Without this, pg.external_url stays
                # empty for Mosseri/CUL-CUDL rows and the browse "View on CUDL" link
                # falls through to a lossy shelfmark-slug fallback that 404s.
                # (Read back the post-enrich nli_cache entry for the external_url field.)
                if not result.get('external_url') and hasattr(state_mod.meta_mgr, 'nli_cache'):
                    _cached_after = state_mod.meta_mgr.nli_cache.get(_sys_id, {})
                    if _cached_after.get('external_url'):
                        cached_url = _cached_after.get('external_url') or ''
                        if 'cudl.lib.cam.ac.uk' in cached_url.lower():
                            result['is_cambridge'] = True
                        result['external_url'] = cached_url

            return result

        try:
            return await run.io_bound(_browse_enrich_sync)
        except Exception as e:
            logger.error(f"Failed to fetch browse enrichment: {e}")
            return {}

    async def fetch_discovery_panel():
        """The FIFTH enrichment read (plan 136-17) -- the connections panel.

        Shaped exactly like the four `fetch_*` closures above: it wraps its work
        in try/except and returns a safe default, so a discovery failure can
        never take the rest of the enrichment phase down with it.

        Unlike them it adds NO `run.io_bound` -- see the offload contract at the
        top of this module.
        """
        if not discovery_panel_enabled():
            return None
        # Per-user state into a plain local BEFORE any await: `run.io_bound`
        # silently degrades `safe_user_*` to `{}` and `ensure_future` empties the
        # slot stack, so the UI context raises. This project has been bitten by
        # both (memory `reference_io_bound_safe_storage_trap`).
        _lang = get_language()

        def _stale() -> bool:
            return generation != refs.load_generation['value']

        try:
            return await fetch_discovery_panel_bundle(page, _lang, is_stale=_stale)
        except Exception as e:
            logger.error("Failed to fetch discovery panel data: %s", e)
            return None

    try:
        (all_sources, pgp_doc, full_htr_text), fjms_data, crossref_data, browse_enrich, discovery_bundle = await asyncio.gather(
            fetch_pgp(), fetch_fjms(), fetch_crossref(), fetch_browse_enrichment(),
            fetch_discovery_panel()
        )
    except Exception as e:
        logger.error(f"Enrichment fetch failed: {e}")
        state.enrichment_loaded = True
        state.enrichment_loading = False
        return

    # Stale check
    if generation != refs.load_generation['value']:
        return

    # The panel's bundle rides in enrichment_refs, beside the four existing
    # placeholder handles, so no per-user state is introduced anywhere else.
    refs.enrichment_refs['discovery_bundle'] = discovery_bundle

    # Process PGP + FGP sources — centralized per-page filter (FGP-04.4). Preserves
    # the prior PGP behavior exactly; FGP rows are aligned to the displayed image
    # by folio (1r↔1r) using the multi-IE-aware folio label resolved above.
    if all_sources:
        _folio = (browse_enrich or {}).get('fgp_folio_label')
        _img_num = (browse_enrich or {}).get('fgp_image_number')
        # Pass the V0.8 page text so FGP editions align to it by textual
        # similarity (robust where folio/positional matching can't).
        state.all_sources = filter_sources_for_page(
            all_sources, page.p_num, _folio, _img_num,
            page_text=getattr(page, 'text', '') or '') or None
    else:
        state.all_sources = None
    # SEED-030: whole-MS HTR for the FGP coverage check (None unless a whole-doc
    # FGP edition is present; see fetch_pgp/_pgp_sync).
    state.fgp_full_htr_text = full_htr_text

    # Process PGP document metadata
    if pgp_doc:
        state.pgp_metadata = {
            'document_type': pgp_doc.get('document_type'),
            'tags': pgp_doc.get('tags', []),
            'description': pgp_doc.get('description'),
            'languages_primary': pgp_doc.get('languages_primary'),
            'languages_secondary': pgp_doc.get('languages_secondary'),
            'doc_date_original': pgp_doc.get('doc_date_original'),
            'doc_date_standard': pgp_doc.get('doc_date_standard'),
            'inferred_date_display': pgp_doc.get('inferred_date_display'),
            'inferred_date_standard': pgp_doc.get('inferred_date_standard'),
            'inferred_date_rationale': pgp_doc.get('inferred_date_rationale'),
            'pgp_url': pgp_doc.get('pgp_url'),
            'pgpid': pgp_doc.get('pgpid'),
        }

        pgpid = pgp_doc.get('pgpid')
        doc_relation = pgp_doc.get('doc_relation', '')
        is_edition = 'Edition' in doc_relation or not doc_relation
        page_content = get_section_for_page(pgp_doc['transcription'], page.p_num, fragment_page_info=pgp_doc.get('_fragment_page_info')) if pgp_doc.get('transcription') else None

        if is_edition and page_content:
            state.pgp_transcription = {
                'full_content': pgp_doc['transcription'],
                'content': page_content,
                'attribution': pgp_doc.get('transcription_source', 'PGP'),
                'pgp_url': pgp_doc.get('pgp_url'),
                'pgpid': pgpid
            }
        else:
            state.pgp_transcription = None
    else:
        state.pgp_transcription = None
        state.pgp_metadata = None

    state.fjms_data = fjms_data
    state.crossref_data = crossref_data

    # Apply browse enrichment (crossref + Oxford + Cambridge + attribution) to page
    if state.current_page:
        pg = state.current_page
        if browse_enrich:
            if browse_enrich.get('attribution'):
                pg.attribution = browse_enrich['attribution']
            if browse_enrich.get('oxford_part_id'):
                pg.oxford_part_id = browse_enrich['oxford_part_id']
                pg.oxford_part_display = browse_enrich.get('oxford_part_display', '')
                pg.oxford_part_metadata = browse_enrich.get('oxford_part_metadata', {})
            if browse_enrich.get('external_url'):
                pg.external_url = browse_enrich['external_url']
            if browse_enrich.get('is_cambridge'):
                pg.is_cambridge = True
            if browse_enrich.get('image_source_info'):
                pg.image_source_info = browse_enrich['image_source_info']
            if browse_enrich.get('folio_images'):
                pg.folio_images = browse_enrich['folio_images']
            if browse_enrich.get('folio_label'):
                pg.folio_label = browse_enrich['folio_label']
            if browse_enrich.get('library_viewer_url'):
                pg.library_viewer_url = browse_enrich['library_viewer_url']
            if browse_enrich.get('cambridge_images'):
                pg.cambridge_images = browse_enrich['cambridge_images']
            if browse_enrich.get('external_provider'):
                pg.external_provider = browse_enrich['external_provider']
            if browse_enrich.get('cambridge_alignment') is not None:
                pg.cambridge_alignment = browse_enrich['cambridge_alignment']
            # Derived fl_id for metadata-only records
            if browse_enrich.get('derived_fl_id') and not pg.fl_id:
                from web.services import get_thumbnail_url, get_full_image_url
                pg.fl_id = browse_enrich['derived_fl_id']
                pg.thumb_url = get_thumbnail_url(pg.fl_id)
                pg.image_url = get_full_image_url(pg.fl_id)
                pg.p_num = 1
                state.page_input_value = 1
                if browse_enrich.get('folio_images'):
                    pg.folio_label = browse_enrich['folio_images'][0].get('folio_label', '')
                    pg.total_pages = len(browse_enrich['folio_images'])
                    pg.current_idx = 1
            # Phase 86: synthetic rows (no NLI crossref fl_id) have empty
            # folio_images but cambridge_images populated via the bridge.
            # Derive total_pages from cambridge_images so pagination works,
            # and clamp p_num/current_idx into [1, total_pages].
            if not pg.total_pages and pg.cambridge_images:
                pg.total_pages = len(pg.cambridge_images)
                clamped = pg.p_num if pg.p_num and 1 <= pg.p_num <= pg.total_pages else 1
                pg.p_num = clamped
                pg.current_idx = clamped
                state.page_input_value = clamped

        # Physical metadata from crossref_data (independent of browse_enrich success)
        if crossref_data and crossref_data.get('physical_metadata'):
            pg.physical_metadata = crossref_data['physical_metadata']

        # Oxford translations (deferred from Phase A since oxford_part_metadata was empty)
        if pg.oxford_part_metadata:
            _ox_meta = pg.oxford_part_metadata
            def _fetch_oxford_translations():
                try:
                    from shared.translation_service import TranslationService
                    svc = TranslationService(thread_safe=True)
                    result = {}
                    if svc.oxford_available():
                        texts = [_ox_meta.get(f, '').strip() for f in ('title', 'contents', 'provenance') if _ox_meta.get(f, '').strip()]
                        if texts:
                            result = svc.get_oxford_translations_batch(texts)
                    svc.close()
                    return result
                except Exception:
                    return {}  # Lookup failed; return empty dict
            try:
                ox_result = await run.io_bound(_fetch_oxford_translations)
                # Re-check generation after await to avoid stale state on rapid navigation
                if generation == refs.load_generation['value']:
                    state.oxford_translations = ox_result
            except Exception:
                pass  # Translation lookup failed; continue without translation

    # Re-check generation before committing final state (guards Oxford await above)
    if generation != refs.load_generation['value']:
        return

    # Client-liveness guard: the generation check only catches intra-Browse
    # navigation. If the visitor left the page entirely (e.g. opened the Joins
    # Lab) while this enrichment awaited, the client is deleted but the
    # generation is unchanged — rebuilding UI on it spams "Client has been
    # deleted but is still being used". Bail quietly when the page is DEFINITELY
    # gone. Fail OPEN: if liveness is unknown (no container ref yet), proceed —
    # update_content() carries its own hardened deleted-client guard, so a false
    # negative here only risks the benign warning, never a missing joins button.
    _cc = refs.content_container
    _page_gone = False
    if _cc is not None:
        try:
            _page_gone = _cc.is_deleted or getattr(_cc.client, '_deleted', False)
        except (RuntimeError, AttributeError):
            _page_gone = True
    if _page_gone:
        return

    state.enrichment_loaded = True
    state.enrichment_loading = False

    # Re-render with full enrichment data (attribution, source badges, folio labels,
    # external links, Oxford part, physical metadata) then update PGP/FJMS containers
    refs.update_content()
    update_enrichment_sections(state, refs)


def update_enrichment_sections(state: BrowseState, refs: BrowsePageRefs):
    """Update enrichment placeholder containers after Phase B completes."""
    # PGP link button in header
    pgp_container = refs.enrichment_refs.get('pgp_link_container')
    if pgp_container:
        pgp_container.clear()
        if state.pgp_metadata and state.pgp_metadata.get('pgp_url'):
            with pgp_container:
                with ui.link(target=state.pgp_metadata['pgp_url'], new_tab=True).classes(
                    'flex items-center gap-1 px-2 py-1 rounded'
                ).style(
                    'text-decoration: none; '
                    'color: #ffffff !important; '
                    'background: rgba(255, 255, 255, 0.2);'
                ):
                    ui.icon('open_in_new', size='sm').style('color: #ffffff !important;')
                    ui.label('PGP').classes('text-sm font-semibold').style('color: #ffffff !important;')

    # Version selector
    version_container = refs.enrichment_refs.get('version_container')
    if version_container and (state.pgp_transcription or state.all_sources):
        version_container.clear()
        handler = refs.enrichment_refs.get('version_change_handler')
        page = state.current_page
        if handler and page and page.text:
            with version_container:
                from web.components import create_version_selector
                create_version_selector(
                    document_id=page.sys_id,
                    page_number=page.p_num,
                    original_text=page.text,
                    on_version_change=handler,
                    pgp_transcription=state.pgp_transcription,
                    all_sources=state.all_sources,
                    full_original_text=getattr(state, 'fgp_full_htr_text', None),
                )

    # Joins button
    joins_container = refs.enrichment_refs.get('joins_container')
    if joins_container:
        joins_container.clear()
        page = state.current_page
        if page and page.shelfmark:
            pgpid_for_joins = state.pgp_metadata.get('pgpid') if state.pgp_metadata else None
            navigate_fn = refs.enrichment_refs.get('navigate_to_shelfmark')
            # FND-05 (D-19): build the Lab deep link so the joins dialog always
            # offers a "Find Joins in the Joins Lab" entry point, even with zero
            # known joins. (The inline create_joins_button at browse.py builds the
            # same URL; this deferred Phase-B path is the one that runs in normal
            # use, so it MUST pass find_joins_url too.)
            _joins_lab_url = None
            if page.sys_id:
                _joins_lab_url = f'/joins-lab?sys_id={page.sys_id}'
                if getattr(page, 'volume_ie', None):
                    _joins_lab_url += f'&volume_ie={page.volume_ie}'
            with joins_container:
                from web.components import create_joins_button
                create_joins_button(
                    shelfmark=page.shelfmark,
                    document_id=page.sys_id,
                    pgpid=pgpid_for_joins,
                    on_navigate=navigate_fn,
                    on_view_all=refs.enter_joined_view,
                    find_joins_url=_joins_lab_url,
                )

    # Bibliography & Catalog buttons
    bib_catalog_container = refs.enrichment_refs.get('bib_catalog_container')
    if bib_catalog_container:
        populate_bib_catalog_buttons(bib_catalog_container, state, state.current_page)

    # The discovery connections panel (plan 136-17) -- the FIFTH placeholder,
    # filled on the same staleness-guarded path as the four above.
    update_discovery_panel_section(state, refs)


def update_discovery_panel_section(state: BrowseState, refs: BrowsePageRefs):
    """Fill the entry-control and panel-body placeholders from the bundle.

    Separate from `update_enrichment_sections` so the retry handler can re-run
    exactly this, and so the AST offload guard has one named panel-path
    function to walk.
    """
    entry_container = refs.enrichment_refs.get('discovery_entry_container')
    panel_container = refs.enrichment_refs.get('discovery_panel_container')
    if entry_container is None and panel_container is None:
        return

    bundle = refs.enrichment_refs.get('discovery_bundle')
    if entry_container is not None:
        entry_container.clear()
    if panel_container is not None:
        panel_container.clear()
    if bundle is None:
        # Flag off, no resolvable page_id, or a stale load. Nothing is claimed
        # about the manuscript -- the control is simply absent.
        return

    from shared.discovery_panel_model import build_panel_rows
    from web.components.discovery_panel import (
        render_discovery_entry_control, render_discovery_panel_body,
    )

    try:
        model = build_panel_rows(bundle)
    except Exception as e:
        logger.error("Discovery panel model refused the bundle: %s", type(e).__name__)
        return

    open_state = refs.enrichment_refs.setdefault('discovery_panel_open', {'value': False})

    async def _retry():
        """Re-issue the four eager reads and re-render. The retry offered on an
        outage has to actually re-query; a retry that only re-renders the same
        envelope is a button that cannot work."""
        page = state.current_page
        if page is None:
            return
        generation = refs.load_generation['value']

        def _stale() -> bool:
            return generation != refs.load_generation['value']

        try:
            fresh = await fetch_discovery_panel_bundle(page, model.lang, is_stale=_stale)
        except Exception as e:
            logger.error("Discovery panel retry failed: %s", e)
            return
        if fresh is None or _stale():
            return
        _cc = refs.content_container
        if _cc is not None:
            try:
                if _cc.is_deleted or getattr(_cc.client, '_deleted', False):
                    return
            except (RuntimeError, AttributeError):
                return
        refs.enrichment_refs['discovery_bundle'] = fresh
        update_discovery_panel_section(state, refs)

    def _toggle_panel():
        open_state['value'] = not open_state['value']
        if panel_container is not None:
            panel_container.style(
                'display: block;' if open_state['value'] else 'display: none;')

    # THE CATALOGUE TITLE for the expansion rows -- "what does the library call
    # this manuscript", beside its shelfmark, exactly as the corpus-wide findings
    # page shows it (`web/pages/findings.py`, coordinator-authorized 2026-08-05;
    # missing here until the owner reported it on 2026-08-07).
    #
    # MEMOISED PER RENDER, and per-render is the right scope: the dict lives as
    # long as the closure the rows capture, so a work carried by twelve
    # manuscripts costs twelve lookups no matter how often the reader pages
    # through the expansion, and nothing survives to go stale.
    #
    # `csv_bank` is a plain in-memory dict populated once at process startup
    # (`MetadataManager.__init__`), so this is a dict lookup with NO I/O -- it
    # needs no offload wrapper and adds none, which matters on a path the panel's
    # own AST guard watches for exactly that (`_PANEL_PATH_FUNCTIONS`).
    #
    # ~14% of manuscripts have no title in `libraries.csv`; those resolve to
    # None, and the renderer draws nothing at all for them rather than a
    # placeholder.
    _catalogue_titles: Dict[str, Optional[str]] = {}

    def _catalogue_title(item) -> Optional[str]:
        key = str(item.get('representative_sys_id') or '')
        if not key:
            return None
        if key not in _catalogue_titles:
            title = None
            try:
                from web.state import state as app_state
                if app_state.meta_mgr is not None:
                    csv_row = app_state.meta_mgr.csv_bank.get(key)
                    title = (csv_row or {}).get('title') or None
            except Exception:
                title = None      # a title is an ENRICHMENT; never fail the row
            _catalogue_titles[key] = title
        return _catalogue_titles[key]

    # The claims envelope's own `meta['page_id']` -- the panel never re-derives
    # an id the service already told it.
    _page_id = (bundle.claims.get('meta') or {}).get('page_id')

    if panel_container is not None:
        panel_container.style(
            'display: block;' if open_state['value'] else 'display: none;')
        with panel_container:
            render_discovery_panel_body(model, on_retry=_retry, page_id=_page_id,
                                        catalogue_title=_catalogue_title)

    if entry_container is not None:
        with entry_container:
            render_discovery_entry_control(model, on_toggle=_toggle_panel)


def populate_bib_catalog_buttons(container, state: BrowseState, page):
    """Populate bibliography and catalog buttons in the page navigation pane.

    Called both inline (if enrichment already loaded) and from
    update_enrichment_sections (deferred enrichment Phase B).
    """
    if not page:
        return
    container.clear()

    fjms_data = state.fjms_data or {}
    fjms_bib = fjms_data.get('bibliography', [])

    # NLI/Ktiv bibliography from crossref cache
    marc_bib = []
    # Phase 85 D-06: synthetic sys_ids have no NLI MARC record — skip the cache
    # read entirely so the bibliography panel doesn't render a "Bibliography Ktiv"
    # chip with empty contents.
    if not is_synthetic_sys_id(page.sys_id):
        try:
            from web.state import state as app_state
            if app_state.meta_mgr and hasattr(app_state.meta_mgr, 'nli_cache'):
                cached = app_state.meta_mgr.nli_cache.get(page.sys_id, {})
                marc_bib = cached.get('marc', {}).get('bibliography', [])
        except Exception:
            pass  # Cache operation failed; continue without cached data

    catalog_source_count = len(fjms_data.get('source_names', []))

    has_meas = fjms_data.get('has_measurements', False)
    has_vs = fjms_data.get('has_visual_suggestions', False)
    if not fjms_bib and not marc_bib and catalog_source_count == 0 and not has_meas and not has_vs:
        return

    from web.components.bibliography_dialog import create_fjms_bibliography_dialog, create_nli_bibliography_dialog
    from web.components.catalog_dialog import show_catalog_dialog

    bib_chip_style = 'border: 1.5px solid #7e57c2; border-radius: 12px; min-height: 22px; color: #7e57c2; font-weight: 600;'
    catalog_chip_style = 'border: 1.5px solid #5c6bc0; border-radius: 12px; min-height: 22px; color: #5c6bc0; font-weight: 600;'

    with container:
        if fjms_bib:
            fjms_dlg = create_fjms_bibliography_dialog(
                fjms_bib, page.sys_id,
                shelfmark=page.shelfmark or '',
            )
            ui.button(
                f'{tr("Bib. FJMS")} ({len(fjms_bib)})',
                on_click=fjms_dlg.open,
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(bib_chip_style)
        if marc_bib:
            nli_dlg = create_nli_bibliography_dialog(
                marc_bib, page.sys_id,
                shelfmark=page.shelfmark or '',
            )
            ui.button(
                f'{tr("Bib. Ktiv")} ({len(marc_bib)})',
                on_click=nli_dlg.open,
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(bib_chip_style)
        if catalog_source_count > 0:
            ui.button(
                f'{tr("Catalog rec.")} ({catalog_source_count})',
                on_click=lambda s=page.sys_id, sm=page.shelfmark or '': show_catalog_dialog(s, sm),
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(catalog_chip_style)

        # Measurements button (teal chip, distinct from catalog indigo and bib purple)
        if has_meas:
            from web.components.measurements_dialog import show_measurements_dialog
            measurements_chip_style = 'border: 1.5px solid #00897b; border-radius: 12px; min-height: 22px; color: #00897b; font-weight: 600;'
            _meas_side = 'recto' if page.p_num == 1 else 'verso'
            ui.button(
                f'{tr("Measurements")}',
                on_click=lambda s=page.sys_id, sm=page.shelfmark or '', side=_meas_side: show_measurements_dialog(s, sm, image_side=side),
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(measurements_chip_style)

        # Phase 57: Visual Similarity chip (orange, distinct from other enrichment colors)
        has_vs = fjms_data.get('has_visual_suggestions', False)
        if has_vs:
            from web.components.visual_similarity_dialog import show_visual_similarity_dialog
            vs_chip_style = 'border: 1.5px solid #ef6c00; border-radius: 12px; min-height: 22px; color: #ef6c00;'
            vs_count = fjms_data.get('visual_suggestion_count', 0)
            ui.button(
                f'{tr("Visual Similarity")} ({vs_count})',
                on_click=lambda s=page.sys_id, sm=page.shelfmark or '': show_visual_similarity_dialog(s, sm),
            ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(vs_chip_style)
