# -*- coding: utf-8 -*-
"""
FGP (Friedberg Genizah Project) transcription service.

Mirrors ``shared/document_service.py::PgpService`` for a SEPARATE, gitignored
sidecar ``fgp_data/fgp_transcriptions.db`` (~387 MB, ~45K rows). The goal is to
surface FGP transcriptions as a DISTINCT, selectable source ALONGSIDE PGP in the
version chooser, in both apps (web NiceGUI + desktop PyQt6).

The FGP schema MIRRORS PGP ``document_sources`` but is flatter/denormalized:
``sys_id``, ``page_info`` and the FGP C-number live directly on each source row
(there is no ``document_fragments`` join table — ``sys_id`` was resolved at build
time, 99.94% against ``libraries.csv`` ``system_number``).

Read-only. Degrades gracefully:
  * flag off  -> ``get_fgp_sources_for_fragment()`` returns ``[]``
  * DB absent -> ``is_available()`` returns ``False``; queries return ``[]``

Thread-safe: uses per-thread SQLite connections via ``ThreadLocalConnection`` so
concurrent NiceGUI ``run.io_bound()`` calls each get their own connection.

------------------------------------------------------------------------------
SCHEMA — confirmed from fgp_data/README.md (the data-store reference, 2026-06-21)
------------------------------------------------------------------------------
The real DB is gitignored/absent in this repo, but its schema is documented in
``fgp_data/README.md``. Source table ``fgp_transcriptions`` (45,034 rows) — the
columns this service reads:
  * ``id``            surrogate PK
  * ``sys_id``        GenizahSearch join key (== NLI Alma id); 99.9% resolved
  * ``c_number``      FGP fragment id ``C#####`` (present on 24,184 rows)
  * ``source_scholar``'FGP' (no per-transcriber field in the source)
  * ``doc_relation``  'Digital Edition' (41,692) / 'Digital Translation' (He 2,725 / En 617)
  * ``language``      'Hebrew'/'English' for translations; NULL for editions
  * ``content``       full plain-text transcription (PyMuPDF get_text())
  * ``sections``      JSON ``[{"page_num":1,"text":"…"}]`` — one per PDF page
  * ``page_info``     'recto'/'verso' (18,222 rows, via the C-number); else NULL
  * (also present, unused here: collection, image_id, content_length, n_pages,
    heb_ratio, rel_path/filename, author_*, title_*, domain, image_side, folio_num)
There is NO ``sequence_order`` column, so rows are ordered by ``id``. Other tables
in the DB (``fgp_meta``, ``fgp_shelfmark_meta``, ``fgp_cnumber_info``) are ignored.

The service still DISCOVERS the source table + columns at connect time (see
``_discover_source_table``) and reads every column defensively, so it tolerates a
refreshed/renamed schema; Phase C should still smoke-test against the live DB.
"""

import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from shared.text_normalize import strip_search_diacritics
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

# Default sidecar location (mirrors PgpService).
_SIDECAR_FILENAME = "fgp_transcriptions.db"
_SIDECAR_DIR = "fgp_data"

# ── Source-kind discriminator (FGP-03) ─────────────────────────────
# Shared, normalized provider tags + an attribution string. Used at EVERY
# classifier surface (web version_selector, desktop _populate_pgp_combo) so FGP
# rows never silently fold into the green "PGP" group just because they share
# PGP's 'Digital Edition' / 'Digital Translation' doc_relation values.
SOURCE_FGP = "fgp"
SOURCE_PGP = "pgp"

# Default attribution. NOTE (FGP-10): the EXACT credit/licensing text is a
# release-gated sign-off item; this is the working default per the milestone doc.
FGP_ATTRIBUTION = "FGP (Friedberg Genizah Project)"

# Candidate source-table names, confirmed name first (fgp_data/README.md). The
# rest are fallbacks in case the DB is rebuilt under a different name.
_CANDIDATE_TABLES = (
    "fgp_transcriptions",
    "document_sources",
    "fgp_sources",
    "sources",
    "transcriptions",
)
# Column names that may hold the transcription text, in preference order.
_CONTENT_COLUMNS = ("content", "transcription", "text")


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Up to 5 levels
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier (table name), doubling embedded quotes.

    The source table name is DISCOVERED from the sidecar DB, not user input, but
    a malformed/hostile identifier could otherwise break or alter the SQL (Codex
    MEDIUM). Defense-in-depth for the f-string-interpolated table name.
    """
    return '"' + str(name).replace('"', '""') + '"'


def _fgp_enabled() -> bool:
    """Return whether the shared FGP flag is enabled.

    Read from the environment on every call so the flag can be flipped without a
    restart (consistent with the project's other request-time-read flags). Lives
    in ``shared/`` so both apps share one gate; ``shared/`` must NOT import
    ``web/`` (the web app layers an optional ``WEB_FGP_ENABLED`` override on top
    via ``web/feature_flags.py``).

    Default: ON (2026-06-22, go-live). FGP transcriptions surface wherever the
    gitignored sidecar DB is present; absent the DB this is a graceful no-op
    (``get_fgp_sources_for_fragment`` returns ``[]`` via ``is_available()``), so
    enabling by default is safe even before the DB is deployed. Kill-switch:
    set ``FGP_TRANSCRIPTIONS_ENABLED=0`` (or ``false``/``no``/``off``).
    """
    value = os.environ.get("FGP_TRANSCRIPTIONS_ENABLED")
    if value is None:
        return True
    return value.strip().lower() in {"1", "true", "yes", "on"}


# ── Source-kind helpers (pure; FGP-03) ─────────────────────────────


def source_provider(source: Dict[str, Any]) -> str:
    """Return the normalized provider tag for a chooser source dict.

    ``'fgp'`` for FGP sources (carry ``source='fgp'`` / ``is_fgp=True``),
    ``'pgp'`` otherwise. Use this at every classifier so FGP and PGP editions
    render in distinct groups even though they share ``doc_relation`` values.
    """
    if source.get("source") == SOURCE_FGP or source.get("is_fgp"):
        return SOURCE_FGP
    return SOURCE_PGP


def source_relation_kind(source: Dict[str, Any]) -> str:
    """Normalize ``doc_relation`` into ``'edition'`` / ``'translation'`` / ``'other'``.

    Mirrors the existing substring logic (``'Edition'`` / ``'Translation'`` in
    ``doc_relation``) used by the web ``version_selector`` and the desktop
    ``_populate_pgp_combo`` classifiers, centralized so both apps agree.
    """
    rel = source.get("doc_relation") or ""
    # Check Edition FIRST: a compound "Edition ; Translation" is an edition (this
    # matches the desktop classifier; checking Translation first mis-routed
    # compounds and drifted from desktop — Codex LOW).
    if "Edition" in rel:
        return "edition"
    if "Translation" in rel:
        return "translation"
    return "other"


def namespaced_source_id(source: Dict[str, Any]) -> Optional[str]:
    """Return a collision-free id like ``'pgp:123'`` / ``'fgp:123'`` (FGP-03).

    Both PGP and FGP carry an integer ``id`` that would collide when merged into a
    single ``all_sources`` list. Namespacing by provider keeps selection state and
    cache keys distinct. Returns ``None`` when the source has no id.
    """
    raw = source.get("id")
    if raw is None:
        return None
    return f"{source_provider(source)}:{raw}"


def group_transcription_sources(sources: Optional[List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """Split a merged PGP+FGP source list into chooser groups (FGP-03/07).

    Centralizes the Edition/Translation + provider classification so the web
    ``version_selector`` and the desktop ``_populate_pgp_combo`` render identical
    groups. FGP editions get their OWN group (``fgp_editions``) and are NOT folded
    into the PGP group even though they share PGP's ``'Digital Edition'``
    ``doc_relation`` — the whole point of the ``source='fgp'`` discriminator.

    Only sources carrying ``content`` are included. Input order (already
    sequence-ordered by the services) is preserved within each group.

    Returns a dict with keys ``pgp_editions`` / ``fgp_editions`` /
    ``pgp_translations`` / ``fgp_translations`` (``'other'`` relations dropped).
    """
    groups: Dict[str, List[Dict[str, Any]]] = {
        "pgp_editions": [],
        "fgp_editions": [],
        "pgp_translations": [],
        "fgp_translations": [],
    }
    for s in sources or []:
        if not s.get("content"):
            continue
        provider = source_provider(s)
        kind = source_relation_kind(s)
        if kind == "edition":
            groups[f"{provider}_editions"].append(s)
        elif kind == "translation":
            groups[f"{provider}_translations"].append(s)
    return groups


def _normalize_fgp_sections(sections: Any) -> Optional[List[Dict[str, Any]]]:
    """Normalize FGP ``sections`` so canvas-based page lookup works (FGP-02).

    FGP ``sections`` are keyed ``page_num``; the shared canvas-matching code
    keys on ``canvas_num``. Copy ``page_num`` -> ``canvas_num`` where missing so
    a section can be matched by page number. Returns ``None`` for empty/invalid.
    """
    if not isinstance(sections, list):
        return None
    out: List[Dict[str, Any]] = []
    for sec in sections:
        if isinstance(sec, dict):
            sec = dict(sec)
            if "canvas_num" not in sec and "page_num" in sec:
                try:
                    sec["canvas_num"] = int(sec["page_num"])
                except (TypeError, ValueError):
                    pass
            out.append(sec)
        else:
            out.append(sec)
    return out or None


def get_fgp_section_for_page(source: Dict[str, Any], page_num: int) -> Optional[str]:
    """Return the FGP source's text for a page (1=recto, 2=verso), or ``None``.

    FGP-specific page split, with the precedence the integration plan (§4b)
    prescribes and the data model in fgp_data/README.md confirms:

      1. ``page_info`` ('recto'/'verso') is AUTHORITATIVE for the fragment side
         (set for 18,222 rows via the FGP C-number). Each FGP row is ONE
         image/side, so the WHOLE row content belongs to that side — full content
         on the matching page, ``None`` on the other.
      2. No ``page_info`` -> fall back to per-page ``sections`` (``page_num``,
         normalized to ``canvas_num``): the section for this page, else ``None``.
      3. Neither -> default to recto only (page 1).

    Never returns the full text on BOTH recto and verso (FGP-02). Deliberately
    does NOT reuse ``document_service.get_section_for_page`` (whose "return full
    transcription" fallbacks would duplicate marker-less FGP text on both sides),
    and never touches the shared PGP path (FGP-12).
    """
    content = (source.get("content") or "").strip()
    if not content:
        return None

    # 1. page_info is authoritative for the fragment side.
    page_info = (source.get("page_info") or "").lower()
    if "recto" in page_info or "verso" in page_info:
        side = "recto" if "recto" in page_info else "verso"
        target = "recto" if page_num == 1 else "verso"
        return content if side == target else None

    # 2. No page_info -> per-page sections (page_num -> fragment page).
    sections = source.get("sections") or []
    if sections:
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            cnum = sec.get("canvas_num")
            if cnum is None:
                cnum = sec.get("page_num")
            if cnum == page_num:
                text = sec.get("text") or sec.get("content")
                return text if text else None
        return None  # page not covered -> no content (never full-on-both)

    # 3. No page_info, no sections -> recto only (FGP-02).
    return content if page_num == 1 else None


def _fgp_match_folio(source: Dict[str, Any]) -> str:
    """The REAL folio identity of an FGP row for image alignment.

    Returns a normalized folio label ('1r', '2v', …) when the row is one
    image of a foliated manuscript, or ``''`` when it is a whole-document
    transcription with no per-image folio. Prefers the raw ``image_side``
    ('1r'/'1v'); else composes ``folio_num`` + recto/verso from ``page_info``.

    DELIBERATELY does NOT fall back to ``c_number`` the way the *display*
    label (``_fgp_folio_label``) does: a c_number is not a folio, so using it
    as a match key would hide that row on every displayed image.
    """
    side = (source.get("image_side") or "").strip()
    if side:
        return side.lower()
    fn = source.get("folio_num")
    pi = (source.get("page_info") or "").lower()
    if fn is not None and ("recto" in pi or "verso" in pi):
        return f"{fn}{'r' if 'recto' in pi else 'v'}".lower()
    return ""


def _fgp_match_image_number(source: Dict[str, Any]) -> str:
    """The FGP image number of a source row — the EXACT per-image join key.

    Returns the FGP image number (``c_number`` with its leading ``C`` stripped,
    e.g. ``'62553'``) or ``''`` when the row has no c_number (whole-document
    rows). This equals ``nli_images.FGPImageNumberId`` (the gallery image's
    ``fgp_image_number_id``) 100% of the time, so it is the robust per-image
    alignment key — unlike the folio LABEL, which is independently derived and
    only coincidentally agrees:

      * a bare-sequence ``image_side`` (``'1'``,``'2'``…) never equals a gallery
        ``'1r'`` label (Geneva → FGP hidden entirely);
      * two volumes both parse to label ``'1r'`` (Manchester → both volumes'
        text shown on one image);
      * ``image_side`` can be NULL on a foliated row (NLI Heb 577 → row dropped).

    Each physical image (including each volume's) has a UNIQUE FGPImageNumberId,
    so this key is volume-aware and order-independent.
    """
    cn = source.get("fgp_c_number") or source.get("c_number")
    if cn is None:
        return ""
    s = str(cn).strip()
    if s[:1] in ("C", "c"):
        s = s[1:]
    return s.strip()


def fgp_source_for_folio(
    source: Dict[str, Any],
    folio_label: Optional[str],
    image_number: Optional[str] = None,
) -> bool:
    """Decide whether an FGP source belongs on the currently displayed image.

    PREFERRED key — FGP image number: when the displayed image's
    ``image_number`` (its ``fgp_image_number_id``) is known AND the source row
    carries a ``c_number``, the row belongs on this image iff the two are equal.
    This is the exact ``c_number ↔ FGPImageNumberId`` join (100% on real data),
    which — unlike the folio label — is volume-aware and immune to bare-sequence
    / NULL / duplicate ``image_side`` values (the Geneva / Manchester / NLI-Heb
    bugs). A row with a c_number that does NOT equal the displayed image number
    is correctly hidden.

    FALLBACK key — folio label (legacy behavior, preserved): used when the
    displayed image number is unknown, or the source has no c_number. The
    mapping is by folio (1r↔1r, …); both labels derive from the same NLI
    crossref ``ImageName`` so they usually agree.

      * whole-doc row (no per-image folio AND no c_number): applies to the
        whole manuscript → show on every page;
      * displayed image/folio unknown (non-NLI manuscript, no ``folio_images``,
        page out of range): keep the row rather than hide it.
    """
    # Preferred: exact per-image number match.
    mk_num = _fgp_match_image_number(source)
    disp_num = str(image_number).strip() if image_number not in (None, "") else ""
    if mk_num and disp_num:
        return mk_num == disp_num

    # Fallback: folio-label match (legacy).
    mk = _fgp_match_folio(source)
    if not mk:
        return True
    fl = (folio_label or "").strip().lower()
    if not fl:
        return True
    return mk == fl


def folio_label_for_displayed_page(
    folio_images: Optional[List[Dict[str, Any]]],
    page_num: int,
    total_pages: int = 0,
) -> str:
    """Folio label ('1r','2v',…) of the image shown at 1-based ``page_num``.

    Robust to manuscripts digitized as MULTIPLE text editions (IEs): such a
    manuscript exposes ``total_pages = k * len(folio_images)`` pages — the same
    ``n`` folios repeating once per IE — so the displayed page index can exceed
    ``n``. We map it back onto the folio sequence with ``(page_num-1) % n``.
    Single-IE manuscripts (``total_pages == n``) are the ``k == 1`` case, so the
    modulo is a no-op. (Without this, positional ``folio_images[page-1]`` ran off
    the end on the 2nd IE and the FGP transcription defaulted to the wrong folio.)

    Only trusts the modulo when ``total_pages`` is a clean multiple of ``n``
    (the well-formed IE case); otherwise falls back to a best-effort in-range
    positional lookup, and returns ``''`` when even that is out of range — in
    which case callers keep all FGP rows rather than show the wrong one.
    """
    img = _image_at_displayed_page(folio_images, page_num, total_pages)
    return (img.get("folio_label") or "").strip() if img else ""


def _image_at_displayed_page(
    folio_images: Optional[List[Dict[str, Any]]],
    page_num: int,
    total_pages: int = 0,
) -> Optional[Dict[str, Any]]:
    """Resolve the gallery image dict shown at 1-based ``page_num`` (or ``None``).

    The single source of the page→image positional/modulo logic (so the folio
    LABEL and the FGP image NUMBER resolvers can never drift). Multi-IE aware:
    a manuscript with ``total_pages = k * len(folio_images)`` repeats the same
    ``n`` folios once per text edition, so map the page index back with
    ``(page_num-1) % n`` when ``total_pages`` is a clean multiple of ``n``.
    """
    imgs = folio_images or []
    n = len(imgs)
    if not n or not page_num or page_num < 1:
        return None
    # A page index past the known total is stale/bad — never fabricate an image
    # from it via the modulo below (that would silently show the wrong image's
    # transcription and hide the rest). Out of range -> caller keeps all FGP.
    if total_pages and page_num > total_pages:
        return None
    if total_pages:
        # Alignable ONLY when the page count is a clean multiple of the image
        # count (k editions × n folios each). Otherwise the manuscript is
        # structurally unalignable — uneven editions, fewer pages than images,
        # or page-count not a multiple — and a positional guess would put FGP on
        # the WRONG folio. Bail to keep-all (None): the chooser then shows every
        # FGP transcription on every page rather than a confidently-wrong one.
        # See docs/OPEN_ISSUES.md "FGP per-folio alignment". (Multi-volume MSS
        # whose counts happen to match are NOT caught here and remain in that
        # open issue — counts alone can't distinguish them from a clean 1:1 MS.)
        if total_pages % n == 0:
            return imgs[(page_num - 1) % n]
        return None
    # total unknown -> best-effort positional (legacy behavior).
    if page_num <= n:
        return imgs[page_num - 1]
    return None


def fgp_image_number_for_displayed_page(
    folio_images: Optional[List[Dict[str, Any]]],
    page_num: int,
    total_pages: int = 0,
) -> str:
    """FGP image number (``fgp_image_number_id``) of the image at ``page_num``.

    The EXACT per-image alignment key (see :func:`_fgp_match_image_number`).
    Same positional/modulo resolution as :func:`folio_label_for_displayed_page`
    (shared via :func:`_image_at_displayed_page`), but reads the gallery image's
    ``fgp_image_number_id`` instead of its label. Returns ``''`` when the image
    is unavailable or lacks the id — callers then fall back to the folio label.
    """
    img = _image_at_displayed_page(folio_images, page_num, total_pages)
    if not img:
        return ""
    v = img.get("fgp_image_number_id")
    return str(v).strip() if v not in (None, "") else ""


def displayed_folio_label(sys_id: str, page_num: int, total_pages: int = 0) -> str:
    """Folio label ('1r','2v',…) of the displayed image at 1-based ``page_num``.

    Reads the LOCAL NLI crossref ``folio_images`` list (no network) and resolves
    the folio via :func:`folio_label_for_displayed_page` (multi-IE aware when
    ``total_pages`` is given). Returns ``''`` when unavailable — non-NLI
    manuscript, page out of range, or the service is down — in which case callers
    keep all FGP rows rather than hide them. Centralizes the ``(sys_id, page) ->
    folio`` resolution so the web and desktop chooser paths cannot drift apart.
    """
    if not sys_id or not page_num:
        return ""
    try:
        from shared.nli_crossref_service import get_nli_crossref_service
        svc = get_nli_crossref_service(thread_safe=True)
        if not svc or not svc.is_available():
            return ""
        imgs = svc.get_folio_images(sys_id) or []
        return folio_label_for_displayed_page(imgs, page_num, total_pages)
    except Exception:
        return ""


def displayed_fgp_image_number(sys_id: str, page_num: int, total_pages: int = 0) -> str:
    """FGP image number (``fgp_image_number_id``) of the displayed image at ``page_num``.

    The exact per-image alignment key, resolved from the LOCAL NLI crossref
    ``folio_images`` (no network) via :func:`fgp_image_number_for_displayed_page`
    (multi-IE aware when ``total_pages`` is given). Returns ``''`` when
    unavailable — non-NLI manuscript, page out of range, missing id, or the
    service is down — in which case callers fall back to the folio label and
    ultimately keep all FGP rows rather than hide them. Mirrors
    :func:`displayed_folio_label` so web and desktop cannot drift.
    """
    if not sys_id or not page_num:
        return ""
    try:
        from shared.nli_crossref_service import get_nli_crossref_service
        svc = get_nli_crossref_service(thread_safe=True)
        if not svc or not svc.is_available():
            return ""
        imgs = svc.get_folio_images(sys_id) or []
        return fgp_image_number_for_displayed_page(imgs, page_num, total_pages)
    except Exception:
        return ""


# ── Textual-similarity alignment (FGP edition ↔ V0.8 page) ─────────
# FGP editions and the V0.8 (HTR) text transcribe the SAME folio, so they share
# most words; a different folio shares few. Picking the FGP edition whose text is
# most similar to the displayed V0.8 page aligns FGP to V0.8 by CONTENT — immune
# to the volume/edition/gap ORDERING issues that defeat positional/label/fl_id
# matching (the structurally-unalignable manuscripts). Calibrated on real data:
# same folio ≈ 0.4–0.6 word-overlap, different folio ≈ 0.1–0.2.
_SIM_FLOOR = 0.18          # min overlap to call a match real (well below ~0.4 same-folio)
_SIM_MIN_TOKENS = 6        # too-short pages give unreliable overlap -> don't override folio
_NIKUD_RE = re.compile(r"[֑-ׇ]")        # Hebrew points/accents (combining)
_HEBWORD_RE = re.compile(r"[א-ת]+")     # base Hebrew letters only


def _heb_token_set(text: Optional[str]) -> Set[str]:
    """Set of base-Hebrew-letter words in ``text`` (nikud/punctuation stripped).

    A diacritic- and punctuation-insensitive content fingerprint so an FGP
    edition (often pointed) and the V0.8 HTR compare on consonantal words.
    """
    if not text:
        return set()
    return set(_HEBWORD_RE.findall(_NIKUD_RE.sub("", text)))


# ── Search-scoped "must contain" override (SEED-033 Option A) ──────────────
# Unlike ``_heb_token_set`` (an unordered word-overlap fingerprint used for
# FGP-vs-HTR alignment), a "does this source contain the searched phrase"
# check needs ORDERED text, so a set is unusable — this is a plain substring
# test on normalized text. Normalizes with the SAME rule SEED-030 uses for
# symmetric comparison (``strip_search_diacritics``: geresh/gershayim/quote
# variants), plus nikud (a pointed FGP/PGP edition vs an unpointed search
# phrase must still match) and whitespace (a phrase spanning a line break in
# one source must still match a single-line rendering in another).
_WS_RE = re.compile(r"\s+")


def _normalize_for_contains(text: Optional[str]) -> str:
    """Diacritic/whitespace-normalized text for a ``must_contain`` substring
    check — strips nikud, folds geresh/gershayim/quote variants via
    :func:`shared.text_normalize.strip_search_diacritics`, and collapses
    whitespace, so a search phrase and a transcription that differ only in
    vocalization, quote style, or line breaks still match. Order-preserving
    (unlike :func:`_heb_token_set`) — this is a substring test, not overlap."""
    if not text:
        return ""
    t = _NIKUD_RE.sub("", text)
    t = strip_search_diacritics(t)
    return _WS_RE.sub(" ", t).strip()


# ── FGP-vs-HTR default-coverage policy (SEED-030) ──────────────────────────────
# The reading-view default cascade (web ``version_selector.load_and_apply_latest``
# + desktop ``_auto_select_pgp_edition``) auto-selects an FGP source over the
# V0.8/HTR ("MiDRASH") transcription whenever one exists. For most collections FGP
# is as full as — or fuller than — the HTR, but some FGP editions are *partial /
# selected* excerpts of the folio (notably the Firkovich collections: median ~9%
# of the folio's text). ``choose_default_source`` demotes such partial FGP
# editions below the HTR so the reader sees the fuller transcription by default;
# the FGP edition stays selectable in the version menu. Coverage is measured in
# base Hebrew LETTERS (nikud/te'amim/punctuation and the HTR's ``][`` lacuna
# markers stripped, letters inside editorial brackets kept) so neither HTR
# artefacts nor an FGP edition's vocalization skew the ratio.

# Minimum coverage (FGP letters / HTR letters) for an FGP edition to remain the
# default; below it the edition is demoted to the HTR. Overridable per-request via
# ``FGP_DEFAULT_MIN_COVERAGE`` so ops can retune without a redeploy.
_DEFAULT_MIN_COVERAGE = 0.33
# The HTR is a fullness BASELINE, not ground truth: when a folio's HTR has too few
# letters to be a reliable denominator (blank / heavily-garbled / wrong-page) the
# coverage ratio is UNKNOWN and we fail toward KEEPING FGP — never demote on a bad
# baseline.
_COVERAGE_MIN_HTR_LETTERS = 40


def _min_coverage() -> float:
    """Coverage threshold, overridable via ``FGP_DEFAULT_MIN_COVERAGE`` (re-read
    per call). Falls back to the module default on a missing / unparseable /
    out-of-[0,1] value."""
    raw = os.environ.get("FGP_DEFAULT_MIN_COVERAGE")
    if raw:
        try:
            v = float(raw)
            if 0.0 <= v <= 1.0:
                return v
        except ValueError:
            pass
    return _DEFAULT_MIN_COVERAGE


def _heb_letter_count(text: Optional[str]) -> int:
    """Count base Hebrew letters in ``text`` — a normalized length immune to
    whitespace, punctuation, nikud/te'amim and the HTR's ``][`` lacuna markers,
    while KEEPING letters inside editorial brackets (they are still text)."""
    if not text:
        return 0
    return sum(len(w) for w in _HEBWORD_RE.findall(_NIKUD_RE.sub("", text)))


def _fgp_is_whole_doc(source: Dict[str, Any]) -> bool:
    """A whole-document FGP row: NO per-image alignment key at all — neither a
    folio label (``_fgp_match_folio``) nor a c-number image id
    (``_fgp_match_image_number``). Such a row shows its full ``content`` on every
    folio, so its coverage is judged against the WHOLE manuscript. A foliated OR
    c-numbered row is per-image (its content is one folio's text) and is judged
    against the displayed folio's HTR — so c-numbered rows with a null
    ``image_side`` must NOT be treated as whole-doc (they're 5.8k real per-image
    editions that would otherwise be wrongly demoted)."""
    return not _fgp_match_folio(source) and not _fgp_match_image_number(source)


def fgp_needs_full_htr(sources: Optional[List[Dict[str, Any]]]) -> bool:
    """True if any FGP *edition* in ``sources`` is a whole-document row (see
    ``_fgp_is_whole_doc``). Those must be measured against the WHOLE-manuscript
    HTR, so callers gate the (relatively costly) full-manuscript fetch on this —
    foliated-/c-numbered-only pages skip it entirely."""
    return any(
        _fgp_is_whole_doc(ed)
        for ed in group_transcription_sources(sources)["fgp_editions"]
    )


def choose_default_source(
    sources: Optional[List[Dict[str, Any]]],
    htr_text: Optional[str],
    full_htr_getter: Optional[Callable[[], Optional[str]]] = None,
    must_contain: Optional[str] = None,
) -> Dict[str, Any]:
    """Decide the reading-view default source for a folio: a PGP edition, an
    FGP edition, or a fall-through to the V0.8/HTR ("MiDRASH") transcription.

    PURE and side-effect-free so the web ``version_selector`` and the desktop
    ``_auto_select_pgp_edition``/``_populate_pgp_combo`` share ONE policy (and
    it is unit-testable without a GUI or the sidecar DB) — callers RENDER the
    returned decision only, they do not re-implement precedence.

    ``must_contain`` (SEED-033 Option A) — when the caller supplies the phrase
    the user actually searched for, prefer the first source (PGP edition, then
    FGP edition, then V0.8) whose text CONTAINS it (substring, normalized via
    :func:`_normalize_for_contains` on both sides — nikud/diacritics/whitespace
    insensitive), so the reading view never silently shows a transcription that
    does not contain what was searched. A miss (the phrase is in none of them,
    e.g. it matched a translation or a different source entirely) is NOT a
    demotion signal — falls through to today's exact default order below,
    unaffected. Omitted/empty → today's order, unconditionally.

    Default order (``must_contain`` absent or matched nothing) — unconditional
    PGP-first, preserved exactly as both apps enforced it before this ever
    lived in one place: any PGP edition wins outright (``reason='pgp_edition'``);
    only when NONE exists does FGP-vs-HTR coverage arbitration below run.

    FGP-vs-HTR coverage (SEED-030) — coverage = FGP edition's displayed
    base-Hebrew-letter count ÷ the HTR letter count of the RIGHT baseline. The
    baseline depends on the edition's scope:
      * **Foliated** row (``_fgp_match_folio`` → a folio): the DISPLAYED folio's
        HTR (``htr_text``) — the row IS that folio's transcription.
      * **Whole-document** row (no per-image folio): the WHOLE-manuscript HTR,
        obtained lazily via ``full_htr_getter`` — a whole-doc row shows its full
        ``content`` on every folio, so a *comprehensive* one (≈ the whole MS)
        stays default while a *selective excerpt* (e.g. Firkovich, ~a few % of the
        MS) is demoted. Comparing it against a single folio would spuriously keep
        it. When no getter is supplied, whole-doc rows fall back to the folio
        baseline (degraded — callers SHOULD pass a getter; see fgp_needs_full_htr).

    Text-match demotion (D, MS heb. g.2 case) — a whole-doc row clearing the
    coverage bar (comprehensive relative to the whole MS, or an unmeasurable
    "unknown" baseline) still is not necessarily ABOUT the displayed folio: a
    codex-level catalogue excerpt can be long/comprehensive yet describe a
    *different* folio entirely. So before defaulting to a whole-doc candidate,
    at least one whole-doc edition must clear the SAME word-overlap floor
    ``_select_fgp_editions_by_similarity`` uses (``_SIM_FLOOR``, via
    ``_content_similarity``) against the displayed folio's ``htr_text`` — else
    demote (``reason='demote_no_text_match'``). Skipped when ``htr_text`` is too
    short to trust (fewer than ``_SIM_MIN_TOKENS`` tokens — fail toward FGP, same
    philosophy as ``htr_too_short``) or when the candidate is a **foliated /
    c-numbered** row (a confident per-image alignment already means "about this
    folio" — zero regression on the ~5,400 foliated FGP editions).

    Only FGP EDITIONS are weighed for coverage/text-match — translations are a
    different language than the Hebrew HTR, so a length ratio is meaningless and
    they are never demoted. The displayed text is the whole-row ``content`` (no
    display path narrows FGP ``content`` to a sub-section), NOT
    ``get_fgp_section_for_page`` (whose page_num is a recto/verso 1/2 flag, not
    the global displayed page).

    Returns ``{source, reason, ratio, eligible, provider, must_contain_matched}``:
      * ``source``   — the source dict to default to, or ``None`` to fall through
                       to V0.8.
      * ``eligible`` — ``True`` → default to ``source``; ``False`` → fall through
                       to the HTR default (PGP/FGP stay selectable in the menu).
      * ``reason``   — ``'must_contain_match'`` / ``'must_contain_v08'`` /
                       ``'fgp_text_match'`` (coverage pick replaced by the whole-doc
                       edition that actually overlaps this folio) /
                       ``'pgp_edition'`` / ``'no_fgp_edition'`` / ``'htr_too_short'``
                       / ``'fgp_sufficient'`` / ``'demote_low_coverage'`` /
                       ``'demote_no_text_match'``.
      * ``ratio``    — FGP/HTR letter ratio, or ``None`` when not computed.
      * ``provider`` — ``'pgp'`` / ``'fgp'`` / ``'v08'`` (V0.8, ``source`` is
                       ``None``) / ``None`` (no source anywhere — no PGP/FGP
                       edition exists at all).
      * ``must_contain_matched`` — ``True`` only when ``must_contain`` was
                       supplied AND the returned decision was chosen because of
                       it (lets callers show a "showing the version containing
                       your search" note without re-deriving the reason).
    """
    groups = group_transcription_sources(sources)
    eds = groups["fgp_editions"]
    pgp_eds = groups["pgp_editions"]

    # ── SEED-033 Option A: search-scoped override ──────────────────────────
    needle = _normalize_for_contains(must_contain) if must_contain else ""
    if needle:
        for ed in pgp_eds:
            if needle in _normalize_for_contains(ed.get("content")):
                return {"source": ed, "reason": "must_contain_match", "ratio": None,
                        "eligible": True, "provider": "pgp", "must_contain_matched": True}
        for ed in eds:
            if needle in _normalize_for_contains(ed.get("content")):
                return {"source": ed, "reason": "must_contain_match", "ratio": None,
                        "eligible": True, "provider": "fgp", "must_contain_matched": True}
        if needle in _normalize_for_contains(htr_text):
            return {"source": None, "reason": "must_contain_v08", "ratio": None,
                    "eligible": False, "provider": "v08", "must_contain_matched": True}
        # Matched nothing -> NOT a demotion signal; fall through unchanged below.

    # ── PGP-first (unconditional; the rule both apps always enforced) ──────
    if pgp_eds:
        return {"source": pgp_eds[0], "reason": "pgp_edition", "ratio": None,
                "eligible": True, "provider": "pgp", "must_contain_matched": False}

    if not eds:
        return {"source": None, "reason": "no_fgp_edition", "ratio": None,
                "eligible": False, "provider": None, "must_contain_matched": False}

    folio_htr_len = _heb_letter_count(htr_text)
    page_tokens = _heb_token_set(htr_text)
    _full_len_cache: Dict[str, int] = {}

    def _full_htr_len() -> int:
        if "v" not in _full_len_cache:
            txt = full_htr_getter() if full_htr_getter else None
            _full_len_cache["v"] = _heb_letter_count(txt)
        return _full_len_cache["v"]

    # Score each edition against the baseline appropriate to its scope. An edition
    # whose baseline is too short to trust (blank/garbled folio, or a whole-doc row
    # with no full-HTR available) is "unknown" → keep-eligible (fail toward FGP).
    best_known, best_ratio = None, -1.0
    unknown_ed = None
    for ed in eds:
        if _fgp_is_whole_doc(ed):           # whole-doc → whole-MS HTR (else folio)
            baseline_len = _full_htr_len() or folio_htr_len
        else:                               # foliated / c-numbered → this folio's HTR
            baseline_len = folio_htr_len
        if baseline_len < _COVERAGE_MIN_HTR_LETTERS:
            if unknown_ed is None:
                unknown_ed = ed
            continue
        ratio = _heb_letter_count(ed.get("content")) / baseline_len
        if ratio > best_ratio:
            best_known, best_ratio = ed, ratio

    threshold = _min_coverage()
    if best_known is not None and best_ratio >= threshold:
        candidate, reason, ratio_out = best_known, "fgp_sufficient", best_ratio
    elif unknown_ed is not None:
        # No measurable edition cleared the bar, but at least one has no reliable
        # baseline → keep it rather than demote on an unknown.
        candidate, reason, ratio_out = unknown_ed, "htr_too_short", None
    else:
        logger.debug("FGP default coverage: ratio=%.3f threshold=%.2f -> demote", best_ratio, threshold)
        return {"source": None, "reason": "demote_low_coverage", "ratio": best_ratio,
                "eligible": False, "provider": None, "must_contain_matched": False}

    # ── (D) text-match demotion for whole-document FGP rows ────────────────
    # A whole-doc row shows the SAME content on every folio of the manuscript,
    # so clearing the coverage bar (comprehensive vs. the whole MS, or an
    # unmeasurable baseline) does not mean it is ABOUT this folio. Foliated /
    # c-numbered rows (a confident per-image alignment) are never subject to
    # this — their coverage baseline is already this folio's own HTR.
    # Codex P1 (2026-09-02): validate the edition that will actually be DISPLAYED.
    # An `any(...)` over all whole-doc editions accepted `candidate` whenever some
    # OTHER edition matched the folio -- and `candidate` was picked purely by
    # coverage ratio, so the long unrelated row could still be shown while a short
    # related one satisfied the gate. Score the candidate itself; when it fails,
    # promote the best-matching whole-doc edition if one clears the floor, and only
    # demote when none does.
    if _fgp_is_whole_doc(candidate) and len(page_tokens) >= _SIM_MIN_TOKENS:
        if _content_similarity(page_tokens, candidate.get("content")) < _SIM_FLOOR:
            # Codex P1 (2026-09-02): the replacement must clear the COVERAGE bar
            # as well as the similarity floor. `_content_similarity` divides by the
            # smaller token set, so a one-word excerpt sharing one word with the
            # folio scores 1.0 -- promoting it here would walk straight past the
            # low-coverage demotion (SEED-030) this function exists to enforce.
            # Editions with no reliable baseline stay eligible, same as above.
            best_match, best_sim, best_match_ratio = None, _SIM_FLOOR, None
            for ed in eds:
                if not _fgp_is_whole_doc(ed):
                    continue
                sim = _content_similarity(page_tokens, ed.get("content"))
                if sim < best_sim:
                    continue
                ed_baseline = _full_htr_len() or folio_htr_len
                if ed_baseline < _COVERAGE_MIN_HTR_LETTERS:
                    ed_ratio = None                      # unknown -> keep-eligible
                else:
                    ed_ratio = _heb_letter_count(ed.get("content")) / ed_baseline
                    if ed_ratio < threshold:
                        continue                          # too partial to default to
                best_match, best_sim, best_match_ratio = ed, sim, ed_ratio
            if best_match is None:
                return {"source": None, "reason": "demote_no_text_match", "ratio": ratio_out,
                        "eligible": False, "provider": None, "must_contain_matched": False}
            # A different whole-doc row IS about this folio -- show that one.
            logger.debug(
                "FGP default: coverage pick has no folio overlap; switching to the "
                "best text match (similarity=%.3f)", best_sim)
            candidate, reason, ratio_out = best_match, "fgp_text_match", best_match_ratio

    logger.debug("FGP default coverage: ratio=%s threshold=%.2f -> keep (%s)", ratio_out, threshold, reason)
    return {"source": candidate, "reason": reason, "ratio": ratio_out,
            "eligible": True, "provider": "fgp", "must_contain_matched": False}


def _content_similarity(page_tokens: Set[str], content: Optional[str]) -> float:
    """Word-overlap of a page's tokens vs an FGP source's content (0..1).

    Overlap / min(|a|,|b|) — robust to one side being a longer transcription of
    the same folio. 0.0 when either side has no Hebrew tokens.
    """
    ct = _heb_token_set(content)
    if not page_tokens or not ct:
        return 0.0
    return len(page_tokens & ct) / min(len(page_tokens), len(ct))


def _select_fgp_editions_by_similarity(
    eds_all: List[Dict[str, Any]],
    folio_eds: List[Dict[str, Any]],
    page_text: Optional[str],
) -> List[Dict[str, Any]]:
    """Choose the FGP edition(s) for a page, preferring V0.8 textual similarity.

    SAFE-BY-DESIGN against regressing the manuscripts that already work:
      * with no/short page text or <2 editions → return the folio match unchanged;
      * if the folio filter already pinned a SINGLE edition that matches the page
        well (sim ≥ floor) → TRUST it (no similarity override → zero regression);
      * otherwise (folio gave many = keep-all/unalignable, or a single weak/wrong
        pick — e.g. a multi-volume manuscript whose positional guess landed on the
        wrong volume) → pick the SINGLE best-matching edition (argmax) when it
        clears the floor, so each page shows one transcription (per user
        direction). No runner-up margin: on a continuous work, adjacent folios
        share vocabulary, so a margin would leave many pages showing every
        transcription; argmax is the best available guess. Below the floor (no
        real signal — e.g. blank/heavily-garbled V0.8) → keep the folio match,
        else keep-all.
    """
    page_tokens = _heb_token_set(page_text)
    if len(page_tokens) < _SIM_MIN_TOKENS or len(eds_all) < 2:
        return folio_eds
    if len(folio_eds) == 1 and _content_similarity(page_tokens, folio_eds[0].get("content")) >= _SIM_FLOOR:
        return folio_eds
    scored = sorted(
        ((_content_similarity(page_tokens, s.get("content")), s) for s in eds_all),
        key=lambda x: -x[0],
    )
    if scored[0][0] >= _SIM_FLOOR:
        return [scored[0][1]]
    return folio_eds or [s for _, s in scored]


def _select_fgp_sources_for_page(
    fgp_sources: List[Dict[str, Any]],
    folio_label: Optional[str],
    image_number: Optional[str],
    page_text: Optional[str],
) -> List[Dict[str, Any]]:
    """The FGP sources to show on a page: editions aligned to V0.8 by similarity
    (with the folio match as the safe default), translations kept by folio match.

    Translations are a different language than the Hebrew V0.8, so they can't be
    similarity-aligned — they stay on the folio/positional path (keep-all when
    the manuscript is structurally unalignable). Output preserves input order.
    """
    if not fgp_sources:
        return []
    folio_kept = [s for s in fgp_sources if fgp_source_for_folio(s, folio_label, image_number)]
    if not page_text:
        return folio_kept
    eds_all = [s for s in fgp_sources if source_relation_kind(s) == "edition"]
    folio_eds = [s for s in folio_kept if source_relation_kind(s) == "edition"]
    folio_trans = [s for s in folio_kept if source_relation_kind(s) != "edition"]
    sel_eds = _select_fgp_editions_by_similarity(eds_all, folio_eds, page_text)
    keep_ids = {id(s) for s in sel_eds} | {id(s) for s in folio_trans}
    return [s for s in fgp_sources if id(s) in keep_ids]


def filter_sources_for_page(
    sources: Optional[List[Dict[str, Any]]],
    page_num: int,
    folio_label: Optional[str] = None,
    image_number: Optional[str] = None,
    page_text: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Filter a merged PGP+FGP source list to one displayed page.

    Centralizes the per-page filtering that was duplicated across the web
    enrichment path (``browse_enrichment``), the web Advanced-view path
    (``search_results``) and the desktop ``PGPSourceWorker`` (FGP-04.4).

    PGP behavior is preserved EXACTLY (FGP-12):
      * keep a source whose ``page_info`` matches the current side, or that has
        no ``page_info``;
      * for a kept PGP NON-translation source with no ``page_info``, narrow its
        ``content`` to this page via ``get_section_for_page`` (mutated in place,
        as the original code did).

    FGP is one row per manuscript image, aligned to the displayed image by
    folio (see ``fgp_source_for_folio``): a foliated row shows only on the
    image whose folio it matches; a whole-doc row shows on every page. When
    ``folio_label`` is omitted (caller could not resolve the displayed folio)
    every FGP row is kept, so the chooser never silently hides a transcription.

    Returns a (possibly empty) list; callers may apply ``or None``.
    """
    # Imported here to avoid any import-order coupling; document_service does not
    # import this module, so there is no cycle.
    from shared.document_service import get_section_for_page

    # FGP is chosen COLLECTIVELY (so similarity can pick the best edition for this
    # page vs the V0.8 text); PGP is filtered individually (unchanged). When
    # page_text is given, FGP editions align to V0.8 by textual similarity with
    # the folio match as the safe default; without it, pure folio matching.
    fgp_all = [s for s in (sources or []) if source_provider(s) == SOURCE_FGP]
    fgp_keep_ids = {
        id(s) for s in _select_fgp_sources_for_page(
            fgp_all, folio_label, image_number, page_text)
    }

    current_page_info = "recto" if page_num == 1 else "verso"
    page_sources: List[Dict[str, Any]] = []
    for source in sources or []:
        if source_provider(source) == SOURCE_FGP:
            if id(source) in fgp_keep_ids:
                page_sources.append(source)
            continue
        # PGP path — preserved verbatim from the original sites.
        source_page = source.get("page_info")
        if source_page == current_page_info or not source_page:
            is_translation = "Translation" in (source.get("doc_relation") or "")
            if source.get("content"):
                if not is_translation and not source_page:
                    source["content"] = get_section_for_page(
                        source["content"], page_num, source.get("sections")
                    )
            page_sources.append(source)
    return page_sources


def _fgp_folio_label(d: Dict[str, Any]) -> str:
    """Short folio/side label for the chooser (e.g. '1r', '2v', or 'recto').

    Lets multi-folio manuscripts (one FGP row per image) render distinguishable
    entries instead of N identical 'FGP Transcription' rows. Prefers the raw
    ``image_side`` ('1r'/'1v'); else composes ``folio_num`` + recto/verso suffix
    from ``page_info``; else the bare side word; else ''.
    """
    side = (d.get("image_side") or "").strip()
    if side:
        return side
    page_info = (d.get("page_info") or "").lower()
    sfx = "r" if "recto" in page_info else ("v" if "verso" in page_info else "")
    folio = d.get("folio_num")
    if folio is not None and str(folio).strip() not in ("", "None"):
        return f"{folio}{sfx}"
    if sfx:
        return "recto" if sfx == "r" else "verso"
    # No side/folio at all (Codex MEDIUM: 5,822 such rows render identically).
    # Fall back to the FGP image id (c_number) so entries stay distinguishable.
    cn = d.get("c_number") or d.get("fgp_c_number")
    return str(cn) if cn else ""


def _fgp_sort_key(s: Dict[str, Any]):
    """Sort FGP chooser sources into FGP file order: 1r, 1v, 2r, 2v, … (FGP-B).

    Editions before translations, then folio number ascending, then recto before
    verso. Rows WITHOUT a folio number (single-leaf fragments / unsided rows) are
    NOT reordered among themselves — they keep their insertion order via Python's
    stable sort (so the PGP-parity behavior for plain fragments is unchanged).
    """
    is_trans = 1 if "Translation" in (s.get("doc_relation") or "") else 0
    try:
        fn = int(s.get("folio_num"))
        has_folio = True
    except (TypeError, ValueError):
        fn, has_folio = 10**9, False
    if has_folio:
        side = (s.get("image_side") or "").lower()
        pi = (s.get("page_info") or "").lower()
        if side.endswith("r") or pi == "recto":
            side_rank = 0
        elif side.endswith("v") or pi == "verso":
            side_rank = 1
        else:
            side_rank = 2
    else:
        side_rank = 0  # don't reorder unsided rows; rely on stable sort
    return (is_trans, fn, side_rank)


def dedupe_fgp_sources(sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse the row-per-image FGP data into chooser-ready sources (FGP-B).

    Two corpus artifacts make the raw rows unusable in the chooser:

    * **Whole-document rows** (no ``c_number``, ~46% of rows) hold the ENTIRE
      manuscript's text and duplicate the per-page rows. Dropped for a given
      ``doc_relation`` whenever c-numbered per-page rows of that relation exist;
      kept only when they are the sole source (e.g. CUL direct-``Trans/`` layout).
    * **Duplicate scans** (~3.2K groups): the same folio side photographed twice
      yields two rows with the same ``c_number``/side. Deduped to one per
      ``(c_number, doc_relation, language)``, keeping the longest content.

    Order-preserving. A no-``c_number`` row that survives is kept verbatim
    (each is a distinct whole-doc transcription).
    """
    # Key on (doc_relation, language): a c-numbered Hebrew translation must not
    # suppress a whole-doc English translation, since both are 'Digital
    # Translation' (Codex MEDIUM).
    relations_with_cnum = {
        (s.get("doc_relation"), s.get("language"))
        for s in sources if s.get("fgp_c_number")
    }
    out: List[Dict[str, Any]] = []
    seen: Dict[tuple, int] = {}
    for s in sources:
        cn = s.get("fgp_c_number")
        if not cn:
            if (s.get("doc_relation"), s.get("language")) in relations_with_cnum:
                continue  # redundant whole-doc row; per-page rows cover it
            out.append(s)
            continue
        key = (cn, s.get("doc_relation"), s.get("language"))
        if key in seen:
            idx = seen[key]
            if len(s.get("content") or "") > len(out[idx].get("content") or ""):
                out[idx] = s  # keep the longest of the duplicate scans
            continue
        seen[key] = len(out)
        out.append(s)
    return out


def _row_to_fgp_source(row: sqlite3.Row) -> Dict[str, Any]:
    """Map a raw FGP DB row to a chooser-shaped source dict (FGP-01/02/03).

    Output keys mirror what ``version_selector`` / ``_populate_pgp_combo`` read
    from PGP sources (``doc_relation``, ``content``, ``source_scholar``,
    ``language``, ``id``), plus the FGP discriminator + extras (``source_credit``
    = the FGP team credit, ``folio_label`` for multi-folio disambiguation).
    """
    d = dict(row)

    content = ""
    for col in _CONTENT_COLUMNS:
        if d.get(col):
            content = d[col]
            break

    sections = d.get("sections")
    if isinstance(sections, str):
        try:
            sections = json.loads(sections)
        except (json.JSONDecodeError, TypeError):
            sections = None
    sections = _normalize_fgp_sections(sections)

    raw_id = d.get("id")
    out: Dict[str, Any] = {
        "source": SOURCE_FGP,
        "is_fgp": True,
        "id": raw_id,
        "uid": f"{SOURCE_FGP}:{raw_id}" if raw_id is not None else None,
        # FGP transcription text are editions; default if the column is absent.
        "doc_relation": d.get("doc_relation") or "Digital Edition",
        "language": d.get("language"),
        "content": content,
        "sections": sections,
        "page_info": d.get("page_info"),
        "source_scholar": d.get("source_scholar") or "FGP",
        "attribution": FGP_ATTRIBUTION,
        # Per-source FGP credit (team leader, e.g. "יעקב זוסמן, ראש צוות FGP…").
        # None on an old DB lacking the column (graceful — display falls back to
        # the generic attribution). ``source_credit`` is the legacy single-language
        # value; ``_he``/``_en`` are the bilingual split (2026-06-24) — the UI picks
        # by language via :func:`pick_fgp_credit`, falling back across all three.
        "source_credit": d.get("source_credit"),
        "source_credit_he": d.get("source_credit_he"),
        "source_credit_en": d.get("source_credit_en"),
        "folio_num": d.get("folio_num"),
        "image_side": d.get("image_side"),
        "folio_label": _fgp_folio_label(d),
        # Real column is ``c_number``; keep ``fgp_c_number`` as a defensive alias.
        "fgp_c_number": d.get("c_number") or d.get("fgp_c_number"),
        "sequence_order": d.get("sequence_order") or 0,
    }
    return out


def pick_fgp_credit(src: Dict[str, Any], lang: str = "en") -> Optional[str]:
    """Pick the language-appropriate FGP credit from a chooser source dict.

    Hebrew UI prefers ``source_credit_he``, English UI prefers
    ``source_credit_en``; either falls back to the other, then to the legacy
    single-language ``source_credit`` (so an old sidecar still shows something).
    Returns ``None`` only when no credit exists at all.
    """
    he = src.get("source_credit_he")
    en = src.get("source_credit_en")
    legacy = src.get("source_credit")
    if str(lang or "").lower().startswith("he"):
        return he or en or legacy
    return en or he or legacy


def fgp_incipit(content: Optional[str], max_chars: int = 40) -> str:
    """First ~``max_chars`` characters of ``content`` (nikud stripped,
    whitespace collapsed to one line) — a distinguishing snippet for an FGP
    combo/menu label (D2). Multiple FGP rows on one manuscript otherwise all
    render as the bare "FGP Transcription" and are untellable apart (see
    MS heb. g.2, ~10 identical-looking entries). Empty when ``content`` has no
    text. An ellipsis marks truncation; a shorter-than-``max_chars`` content is
    returned in full with no ellipsis."""
    if not content:
        return ""
    text = _NIKUD_RE.sub("", content)
    text = _WS_RE.sub(" ", text).strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "…"


class FgpService:
    """Service for accessing FGP transcription data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        """
        Initialize FgpService.

        Args:
            db_path: Path to fgp_transcriptions.db. If None, auto-detect from the
                LOCALAPPDATA sidecar location first, then the project root.
            thread_safe: If True, use per-thread connections (NiceGUI web app).
                Desktop app may leave this False (single-threaded).
        """
        self._conn = None  # ThreadLocalConnection or sqlite3.Connection
        self._db_path: Optional[str] = None
        self._table: Optional[str] = None
        self._columns: Set[str] = set()

        # Resolve db_path
        if db_path is None:
            # Check user-updated sidecar location first (LOCALAPPDATA), matching
            # PgpService + the path the desktop sidecar updater writes to.
            user_path = os.path.join(
                os.environ.get("LOCALAPPDATA", ""),
                "GenizahSearchPro", "data", _SIDECAR_DIR, _SIDECAR_FILENAME,
            )
            if os.path.isfile(user_path):
                db_path = user_path
            else:
                root = _find_project_root()
                if root:
                    db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)

        if db_path is None:
            logger.warning("FgpService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            # Expected when the (gitignored, downloaded-on-demand) DB isn't present.
            logger.info("FgpService: Sidecar database not found at %s", db_path)
            return

        try:
            uri = f"file:{db_path}?mode=ro"
            if thread_safe:
                self._conn = ThreadLocalConnection(
                    uri, row_factory=sqlite3.Row, timeout=10.0
                )
            else:
                self._conn = sqlite3.connect(
                    uri, uri=True, check_same_thread=True, timeout=10.0
                )
                self._conn.row_factory = sqlite3.Row
            self._table, self._columns = _discover_source_table(self._conn)
            if self._table is None:
                logger.warning(
                    "FgpService: connected to %s but found no source table with a "
                    "'sys_id' column; FGP sources will be empty", db_path
                )
            else:
                logger.info(
                    "FgpService: Connected to %s (table=%s)", db_path, self._table
                )
        except Exception as e:
            logger.error("FgpService: Failed to connect to %s: %s", db_path, e)
            self._conn = None

    def is_available(self) -> bool:
        """True if the sidecar DB connection is active AND a source table was found."""
        return self._conn is not None and self._table is not None

    def get_fgp_sources_for_fragment(self, sys_id: str) -> List[Dict[str, Any]]:
        """
        Get all FGP transcription sources for a fragment, chooser-shaped.

        Args:
            sys_id: The GenizahSearch system ID (== libraries.csv system_number).

        Returns:
            List of chooser-shaped FGP source dicts (see ``_row_to_fgp_source``),
            ordered by ``sequence_order`` when available. Returns ``[]`` when the
            flag is off, the DB is absent, ``sys_id`` is falsy, or on error.
        """
        if not _fgp_enabled():
            return []
        if not sys_id or not self.is_available() or "sys_id" not in self._columns:
            return []

        try:
            # Editions before translations (doc_relation alpha), then folio order,
            # stable by id (integration plan §4a). Built from columns that exist —
            # there is no sequence_order column in fgp_transcriptions.
            order_cols = [c for c in ("doc_relation", "folio_num", "id") if c in self._columns]
            order = f" ORDER BY {', '.join(order_cols)}" if order_cols else ""
            cursor = self._conn.execute(
                f'SELECT * FROM {_quote_ident(self._table)} WHERE sys_id = ?{order}', (sys_id,)
            )
            # Collapse whole-doc + duplicate-scan rows into chooser-ready sources
            # (FGP-B); raw rows are one-per-image with heavy redundancy.
            sources = dedupe_fgp_sources(
                [_row_to_fgp_source(row) for row in cursor.fetchall()]
            )
            # Present per-image transcriptions in FGP file order (1r, 1v, 2r, …)
            # so multi-folio manuscripts read as a navigable sequence (FGP-B).
            sources.sort(key=_fgp_sort_key)
            return sources
        except Exception as e:
            logger.error("Error getting FGP sources for fragment %s: %s", sys_id, e)
            return []

    def get_sys_ids_with_fgp_sources(self, sys_ids: List[str]) -> Set[str]:
        """
        Batch-check which sys_ids have FGP transcription sources.

        Chooser-availability helper ONLY (e.g. to show an "FGP" affordance). This
        is NOT a search/discovery signal — FGP-12 forbids touching Tantivy,
        ``get_sys_ids_with_transcriptions``, ``has_pgp`` or PGP search filters.

        Args:
            sys_ids: List of system IDs to check.

        Returns:
            Set of sys_ids that have at least one FGP source. ``set()`` when the
            flag is off, the DB is absent, or on error.
        """
        if not _fgp_enabled():
            return set()
        if not sys_ids or not self.is_available() or "sys_id" not in self._columns:
            return set()

        try:
            result_set: Set[str] = set()
            batch_size = 500  # stay under SQLite's 999 variable limit
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                cursor = self._conn.execute(
                    f'SELECT DISTINCT sys_id FROM {_quote_ident(self._table)} '
                    f"WHERE sys_id IN ({placeholders})",
                    batch,
                )
                result_set.update(row["sys_id"] for row in cursor)
            return result_set
        except Exception as e:
            logger.error("Error batch checking FGP sources: %s", e)
            return set()

    def get_sys_ids_with_fgp_editions(self, sys_ids: Optional[List[str]] = None) -> Set[str]:
        """Batch-check which sys_ids have an FGP scholarly EDITION (SEED-023).

        EDITIONS-ONLY: ``doc_relation = 'Digital Edition'`` -- deliberately NOT
        :meth:`get_sys_ids_with_fgp_sources` (which also counts 'Digital
        Translation'). Pairs with
        :meth:`shared.document_service.PGPDocumentService.get_sys_ids_with_editions`
        (PGP ``%Edition%``) to form the full scholarly-editions union used by the
        homepage stat and the catalog editions filter. Honors the FGP kill switch.

        Args:
            sys_ids: List of system IDs to check, or ``None`` for the FULL corpus
                set. An empty list returns ``set()``.

        Returns:
            Set of sys_ids with an FGP Digital Edition. ``set()`` when the flag is
            off, the DB is absent, or on error.
        """
        if not _fgp_enabled():
            return set()
        if not self.is_available() or "sys_id" not in self._columns:
            return set()
        if sys_ids is not None and not sys_ids:
            return set()

        base = (
            f'SELECT DISTINCT sys_id FROM {_quote_ident(self._table)} '
            "WHERE doc_relation = 'Digital Edition' AND sys_id IS NOT NULL"
        )
        try:
            result_set: Set[str] = set()
            if sys_ids is None:
                cursor = self._conn.execute(base)
                result_set.update(row["sys_id"] for row in cursor if row["sys_id"])
                return result_set
            batch_size = 500  # stay under SQLite's 999 variable limit
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                cursor = self._conn.execute(
                    f"{base} AND sys_id IN ({placeholders})",
                    batch,
                )
                result_set.update(row["sys_id"] for row in cursor if row["sys_id"])
            return result_set
        except Exception as e:
            logger.error("Error batch checking FGP editions: %s", e)
            return set()

    def close(self):
        """Close the database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("FgpService: Connection closed")
            except Exception as e:
                logger.error("FgpService.close error: %s", e)
            finally:
                self._conn = None
                self._table = None
                self._columns = set()

    def get_version(self) -> Optional[str]:
        """Get the sidecar DB version from a build-metadata table, or ``None``.

        Build metadata lives in ``fgp_meta`` (per the README); older/other DBs may
        use ``meta``. Best-effort and assumes a key/value shape — returns ``None``
        rather than raising if neither table/shape exists.
        """
        if self._conn is None:
            return None
        for table in ("fgp_meta", "meta"):
            try:
                cursor = self._conn.execute(
                    f"SELECT value FROM {_quote_ident(table)} WHERE key = 'version'"
                )
                row = cursor.fetchone()
                if row:
                    return row["value"]
            except Exception:
                continue  # table missing or different shape; try the next
        return None


def _discover_source_table(conn) -> tuple:
    """Discover the FGP source table name + its columns.

    Returns ``(table_name, columns_set)`` for the table that holds FGP sources,
    or ``(None, set())`` if none is found. Prefers the documented candidate names
    (those carrying a ``sys_id`` column); otherwise falls back to any table that
    has both ``sys_id`` and a content-ish column. Makes the service resilient to
    minor differences between the assumed and real schema.
    """
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cursor.fetchall()]
    except Exception as e:
        logger.error("FgpService: could not list tables: %s", e)
        return None, set()

    def cols(table: str) -> Set[str]:
        try:
            return {row["name"] for row in conn.execute(f'PRAGMA table_info({_quote_ident(table)})')}
        except Exception:
            return set()

    # First pass: documented candidate names that carry sys_id.
    for table in _CANDIDATE_TABLES:
        if table in tables:
            columns = cols(table)
            if "sys_id" in columns:
                return table, columns

    # Second pass: any table with sys_id + a content-ish column.
    content_cols = set(_CONTENT_COLUMNS)
    for table in tables:
        columns = cols(table)
        if "sys_id" in columns and (content_cols & columns):
            return table, columns

    return None, set()


# ── Module-level Singleton ─────────────────────────────────────────

_default_service: Optional[FgpService] = None


def get_fgp_service(thread_safe: bool = True) -> FgpService:
    """Get or create the default FgpService singleton.

    Args:
        thread_safe: If True (default), per-thread read-only connections — safe
            for both web and desktop.

    Returns:
        FgpService instance (may have ``is_available() == False`` if the DB is
        missing or the flag is off).
    """
    global _default_service
    if _default_service is None:
        _default_service = FgpService(thread_safe=thread_safe)
    return _default_service


def reset_fgp_service():
    """Reset the singleton FgpService.

    Call after the ``fgp_transcriptions.db`` sidecar is downloaded/replaced (the
    desktop sidecar updater's post-download reset) to force re-initialization on
    next access.
    """
    global _default_service
    if _default_service is not None:
        _default_service.close()
        _default_service = None


# ── Module-level Wrapper Functions ─────────────────────────────────


def get_fgp_sources_for_fragment(sys_id: str) -> List[Dict[str, Any]]:
    """Get all FGP transcription sources for a fragment (chooser-shaped)."""
    return get_fgp_service().get_fgp_sources_for_fragment(sys_id)


def get_sys_ids_with_fgp_sources(sys_ids: List[str]) -> Set[str]:
    """Batch-check which sys_ids have FGP sources (chooser availability only)."""
    return get_fgp_service().get_sys_ids_with_fgp_sources(sys_ids)


def get_sys_ids_with_fgp_editions(sys_ids: Optional[List[str]] = None) -> Set[str]:
    """sys_ids with an FGP scholarly edition (SEED-023, 'Digital Edition' only).
    ``None`` => full corpus. See
    :meth:`FgpService.get_sys_ids_with_fgp_editions`."""
    return get_fgp_service().get_sys_ids_with_fgp_editions(sys_ids)


def get_version() -> Optional[str]:
    """Get the FGP sidecar database version."""
    svc = get_fgp_service()
    return svc.get_version() if svc.is_available() else None
