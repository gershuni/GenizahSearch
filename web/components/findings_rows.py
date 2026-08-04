# -*- coding: utf-8 -*-
"""The findings page's launch headline and its result rows (Phase 136, plan
136-18, NOVEL-01 / NOVEL-02 / PANEL-02).

`web/pages/findings.py` (plan 136-16) owns the page SHELL -- header, reserved
headline slot, caveat, mode strip, filter bar, result bar, pager and the four
service states. This module owns what goes INSIDE two of those places: the
launch headline ruling U reserved, and the anatomy of one result row in each of
the three shipped units.

WHAT THIS MODULE MAY NOT DO, stated as prohibitions because each one has a test:

* **No launch figure as a literal, in any form** (ruling U constraint 2). Every
  number in the headline is read from `web.discovery.get_launch_stats_enveloped`
  at request time and formatted through a placeholder. Plan 136-22 ships a
  repo-level guard that globs this module and fails on a figure appearing as a
  string, a numeric constant, a formatted expression or constant-folded
  arithmetic; plan 136-18's own render-smoke suite additionally drives the
  headline from a SENTINEL envelope no artifact contains, which is what covers
  the shapes a static scan cannot see (a figure assembled across statements,
  imported, or read from a file).
* **No second bucket rule.** `main_pool` arrives materialized on the row, and
  the bucket NAME comes from `shared.discovery_main_pool.bucket_label` through
  `shared.discovery_display_strings.bucket_name`. Nothing here re-derives it.
* **No second coverage vocabulary.** The qualified coverage clause is DERIVED
  from `shared.discovery_display_strings.row_headline` (see `coverage_clause`
  below) rather than retyped, so the one permitted percentage on a discovery
  surface keeps the exact qualifier the shared honesty gate recognises, and the
  direct-family gating stays the shared module's decision rather than a copy of
  it.
* **No letter count.** The identification grain materializes `max_coverage_ppm`
  and nothing else about how much text matched; per-evidence letter counts are
  not on a findings row and all shipped propagated rows have none at all
  (`tests/test_discovery_findings_query.py` pins the reason). This module never
  reads such a field and never writes that phrase.
* **No raw stored vocabulary in markup.** `relation_kind`, `novelty_status`,
  `main_pool_reason`, `unit` and `shade` are branched on and mapped; none of
  them is ever rendered.
* **No row treatment keyed on novelty, and no demotion of a second-bucket row.**
  The findings-page reference records that the sketch README claimed a
  row-level accent rule the CSS does not have, and that adding one needs a D-24
  check first because a row treatment keyed on novelty is close to the styling
  the requirement prohibits. A second-bucket row therefore renders with exactly
  the anatomy and exactly the classes of a main-pool row: the bucket means
  there was not enough evidence for the main-pool rule, never that the
  identification is probably wrong.
* **No CSS.** Every visual class here (`row`, `side`, `rel`, `nov`,
  `nov unknown`, `chip`, `dnote`) is one plan 136-10 landed in
  `web/static/common.css` under `.gs-discovery`; the page root carries that
  class or none of it applies. Every directional property in that block is
  logical, and this module adds no style of its own.

WHERE THE STRINGS COME FROM
---------------------------
The claim vocabulary is `shared/discovery_display_strings.py`. Ruling U's
headline wording, the three contribution-shade labels and the row's count
phrases exist in neither that module nor `tr()`, and both files are outside
this plan's `files_modified` -- so they live in `_COPY` below, bilingual, and
this plan's suite sweeps every one of them through the SAME shared honesty gate
in both languages. `tr()` is deliberately not used for them: it falls back to
the English key on a missing entry, which would silently render English on a
Hebrew reader's most honesty-sensitive element.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

from nicegui import ui

import shared.discovery_display_strings as ds
from shared.discovery_novelty import CANDIDATE_STATUS, DEFAULT_STATUS, NOVELTY_STATUSES
from shared.discovery_service import (
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNIT_MANUSCRIPT,
    FINDINGS_UNIT_WORK,
    LAUNCH_CONTRIBUTION_SHADES,
)
from shared.discovery_surface_projection import is_outage

# ---------------------------------------------------------------------------
# Marker classes. Render-smoke assertions scope to these; an assertion that
# searches the whole rendered page can pass for the wrong reason (the
# findings-page reference records exactly that failure). Renaming one is a
# breaking change, not a refactor.
# ---------------------------------------------------------------------------

LAUNCH_CLASS = "gs-findings-launch"
LAUNCH_TOTAL_CLASS = "gs-findings-launch-total"
LAUNCH_BASIS_CLASS = "gs-findings-launch-basis"
LAUNCH_SHADE_CLASS = "gs-findings-launch-shade"
LAUNCH_CONTEXT_CLASS = "gs-findings-launch-context"
LAUNCH_STATE_CLASS = "gs-findings-launch-state"

ROW_CLASS = "gs-findings-row"
ROW_TITLE_CLASS = "gs-findings-row-title"
ROW_SUB_CLASS = "gs-findings-row-sub"
ROW_META_CLASS = "gs-findings-row-meta"
ROW_RELATION_CLASS = "gs-findings-row-relation"
ROW_NOVELTY_CLASS = "gs-findings-row-novelty"
ROW_PAGES_CLASS = "gs-findings-row-pages"
ROW_COVERAGE_CLASS = "gs-findings-row-coverage"
ROW_BUCKET_CLASS = "gs-findings-row-bucket"
ROW_SHELFMARK_CLASS = "gs-findings-row-shelfmark"
ROW_ANNOTATION_CLASS = "gs-findings-row-annotation"

NOVELTY_HELP_CLASS = "gs-findings-novelty-help"

#: U+05BE HEBREW PUNCTUATION MAQAF -- D-21 fixes the Hebrew compound hyphen at
#: this codepoint. Composed in as an escape so a copy/paste through a tool that
#: normalizes punctuation cannot substitute an ASCII hyphen.
_MAQAF = "־"

#: U+00B7 MIDDLE DOT -- D-21's row separator, and the separator
#: `row_headline` appends the coverage clause behind.
_DOT = "·"


# ---------------------------------------------------------------------------
# The bilingual strings this surface needs and neither `tr()` nor the shared
# claim vocabulary defines. Every entry is swept through the shared honesty
# gate, in both languages, by this plan's suite.
# ---------------------------------------------------------------------------

_COPY: Dict[str, Dict[str, str]] = {
    # -- ruling U's headline. NO NUMBER is written here; `{count}` is the only
    # -- substitution, and it is filled from the artifact-backed envelope.
    #
    # "The finding aids did not already have it" is a claim about the AIDS, not
    # about the match: it says where the identification is absent, never how
    # often the identification is right. The wording carries no rate word and no
    # quantity word, so it cannot be read as an accuracy statement and cannot
    # trip the honesty gate's rate detector beside its own four numbers.
    "launch_total": {
        "en": "{count} identifications the finding aids did not already have",
        "he": "{count} זיהויים שכלי העזר לא כללו",
    },
    # Ruling U constraint 1: ONE basis, STATED IN WORDS. `{bucket}` is
    # `bucket_name(True, lang)` -- the shared bucket vocabulary, never a second
    # name for the same pool.
    "launch_basis": {
        "en": "Counted in the {bucket} only.",
        "he": "נספר ב{bucket} בלבד.",
    },
    "launch_manuscripts": {
        "en": "Across {count} fragments in the {bucket}.",
        "he": "על פני {count} קטעים ב{bucket}.",
    },
    # The context figures. They count EVERY bucket and every shade, so the line
    # says so in words -- a context number beside a main-pool headline that did
    # not name its own basis is exactly the mixed-basis defect ruling U was
    # issued over.
    "launch_context": {
        "en": "Out of {fragments} fragments across {pages} pages in the whole corpus.",
        "he": "מתוך {fragments} קטעים ב" + _MAQAF + "{pages} דפים בכלל האוסף.",
    },
    # -- the three contribution shades, match-framed (ruling U constraint 4).
    #    `container_predicts` says what the aid DID name; it never says the aid
    #    was wrong.
    "shade_fills_gap": {
        "en": "no prior identification",
        "he": "אין זיהוי קודם",
    },
    # NOT "more accurate than the aid": the Hebrew word for accurate is a rate
    # word in the shared honesty gate's lexicon, and beside a shade count it
    # would read -- to the gate and to a reader -- as an accuracy claim about
    # the match rather than a statement about granularity.
    "shade_refines_granularity": {
        "en": "finer than the aid",
        "he": "פירוט עדין יותר מכלי העזר",
    },
    "shade_container_predicts": {
        "en": "the aid named only a container",
        "he": "כלי העזר ציין מכלול בלבד",
    },
    "shade_manuscripts": {
        "en": "across {count} fragments",
        "he": "על פני {count} קטעים",
    },
    # -- row counts, with singular agreement in both languages (Hebrew
    #    number-noun agreement is not optional).
    "pages_one": {"en": "1 page", "he": "דף אחד"},
    "pages_many": {"en": "{count} pages", "he": "{count} דפים"},
    "manuscripts_one": {"en": "1 fragment", "he": "קטע אחד"},
    "manuscripts_many": {"en": "{count} fragments", "he": "{count} קטעים"},
    "works_one": {"en": "1 work", "he": "חיבור אחד"},
    "works_many": {"en": "{count} works", "he": "{count} חיבורים"},
    # -- the per-manuscript unit's inline annotation. A manuscript holding more
    #    than one work has no single candidacy verdict, and the row says so
    #    rather than showing a verdict that would be ambiguous.
    "multi_work": {
        "en": "Holds more than one work — a single candidacy verdict would be ambiguous.",
        "he": "כולל יותר מחיבור אחד — הכרעת מועמדות אחת תהיה דו" + _MAQAF + "משמעית.",
    },
    # -- the domain facet's header. It NAMES ITS AXIS: the domain of the
    #    IDENTIFIED WORK, never the manuscript's catalogue domain. Filtering on
    #    the catalogue axis would hide exactly the findings that disagree with
    #    the catalogue, which are the valuable ones.
    "facet_domain_header": {
        "en": "Domain of the identified work",
        "he": "תחום החיבור המזוהה",
    },
    # -- the novelty help affordance's as-of line. The DATE is read from the
    #    artifact's own `data_as_of` meta key; when the artifact does not carry
    #    one the line is omitted entirely rather than dated by guess.
    "novelty_as_of": {
        "en": "Sources checked as of {date}.",
        "he": "המקורות נבדקו נכון ל" + _MAQAF + "{date}.",
    },
}


def _lang_key(lang: str) -> str:
    return "he" if lang == "he" else "en"


def copy_text(key: str, lang: str = "en") -> str:
    """One of this module's bilingual strings. Raises on an unknown key rather
    than rendering an empty element for a string nobody designed."""
    entry = _COPY.get(key)
    if entry is None:
        raise ValueError(
            "findings_rows.copy_text: unknown key {!r} (expected one of {})".format(
                key, sorted(_COPY)
            )
        )
    return entry[_lang_key(lang)]


def copy_keys() -> Tuple[str, ...]:
    """Every key, for the suite's honesty sweep and its bilingual-completeness
    check."""
    return tuple(sorted(_COPY))


# ---------------------------------------------------------------------------
# The contribution-shade labels, keyed by the STORED shade value. The mapping
# is validated at IMPORT time against the frozen ruling-U tuple: a shade the
# reader would otherwise meet as a raw stored key, or a label left behind by a
# retired shade, fails at import rather than at render time.
# ---------------------------------------------------------------------------

_SHADE_COPY_KEY: Mapping[str, str] = {
    shade: "shade_" + shade for shade in LAUNCH_CONTRIBUTION_SHADES
}

_missing = sorted(k for k in _SHADE_COPY_KEY.values() if k not in _COPY)
if _missing:
    raise RuntimeError(
        "web/components/findings_rows.py: no reader-facing label for launch "
        f"contribution shade(s) {_missing} -- a shade with no label would reach "
        "the headline as a raw stored vocabulary key (ruling U constraint 4)"
    )
del _missing


def launch_shade_label(shade: str, lang: str = "en") -> str:
    """The match-framed reader label for one stored contribution shade.

    Raises on an unknown shade: a blank label beside a real number is a worse
    failure than a loud one, and echoing the stored key would put raw
    vocabulary on the release's headline.

    **The message NAMES the authority instead of enumerating it.** An exception
    message is an egress class of its own -- it reaches a log and, uncaught, a
    reader -- without passing through either the markup scan or the envelope
    scan, so listing the valid shades here would put three stored vocabulary
    values on exactly the egress the honesty gate's error-path scan exists to
    cover. The received value is safe to echo: by construction it is NOT a
    member of the vocabulary.
    """
    key = _SHADE_COPY_KEY.get(shade)
    if key is None:
        raise ValueError(
            "launch_shade_label: unknown contribution shade {!r} -- the valid "
            "set is shared.discovery_service.LAUNCH_CONTRIBUTION_SHADES".format(shade)
        )
    return copy_text(key, lang)


def _count(value: Any) -> str:
    """A figure, grouped. Never a launch literal -- the value always arrives
    from the envelope."""
    try:
        return "{:,}".format(int(value))
    except (TypeError, ValueError):
        return ""


def _plural(base: str, count: Any, lang: str) -> str:
    try:
        n = int(count)
    except (TypeError, ValueError):
        return ""
    if n == 1:
        return copy_text(base + "_one", lang)
    return copy_text(base + "_many", lang).format(count=_count(n))


# ---------------------------------------------------------------------------
# THE LAUNCH HEADLINE (ruling U).
# ---------------------------------------------------------------------------


def render_launch_headline(
    envelope: Mapping[str, Any], lang: str = "en", *, on_retry=None
) -> None:
    """Ruling U's headline: the main-pool contribution total, its three shades
    and the context figures -- every number read from `envelope`.

    An OUTAGE renders a named temporary condition with a retry and NO number.
    A headline reading "0" during a sidecar failure would announce that the
    release contributes nothing, which is the exact false-zero the envelope
    exists to prevent; that is why this branches on the status rather than on
    whether `items` happens to be empty.
    """
    lang = _lang_key(lang)
    with ui.column().classes(f"{LAUNCH_CLASS} w-full gap-1"):
        if not isinstance(envelope, Mapping) or is_outage(envelope):
            _render_launch_outage(envelope, lang, on_retry)
            return

        items = list(envelope.get("items") or ())
        meta = dict(envelope.get("meta") or {})
        total = envelope.get("total")

        ui.label(
            copy_text("launch_total", lang).format(count=_count(total))
        ).classes(f"{LAUNCH_TOTAL_CLASS} text-lg font-bold")

        # The basis, in words (ruling U constraint 1). `bucket_name` delegates
        # to `shared.discovery_main_pool.bucket_label`, so the headline and the
        # result bar can never name the same pool two ways.
        main_pool_name = ds.bucket_name(True, lang)
        ui.label(
            copy_text("launch_basis", lang).format(bucket=main_pool_name)
        ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

        manuscripts = meta.get("main_pool_manuscript_count")
        if manuscripts is not None:
            ui.label(
                copy_text("launch_manuscripts", lang).format(
                    count=_count(manuscripts), bucket=main_pool_name)
            ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

        for item in items:
            _render_launch_shade(item, lang)

        fragments = meta.get("corpus_manuscript_count")
        pages = meta.get("corpus_page_count")
        if fragments is not None and pages is not None:
            ui.label(
                copy_text("launch_context", lang).format(
                    fragments=_count(fragments), pages=_count(pages))
            ).classes(f"{LAUNCH_CONTEXT_CLASS} dnote text-xs")


def _render_launch_shade(item: Mapping[str, Any], lang: str) -> None:
    """One contribution shade: its count, its match-framed label, and the
    fragments it spans. The stored shade value never reaches markup."""
    label = launch_shade_label(item.get("shade"), lang)
    with ui.row().classes(f"{LAUNCH_SHADE_CLASS} items-baseline gap-2 flex-wrap"):
        ui.label(_count(item.get("identification_count"))).classes("font-semibold")
        ui.label(label)
        manuscripts = item.get("manuscript_count")
        if manuscripts is not None:
            ui.label(
                copy_text("shade_manuscripts", lang).format(count=_count(manuscripts))
            ).classes("dnote text-xs")


def _render_launch_outage(envelope: Any, lang: str, on_retry) -> None:
    """A VISIBLE temporary condition plus a retry -- never a zero.

    `service_state_message` raises on a status nobody designed, so an envelope
    with no status at all is reported as `unavailable` rather than rendering
    silence.
    """
    status = (envelope or {}).get("status") if isinstance(envelope, Mapping) else None
    if status not in ("unavailable", "timeout", "busy"):
        status = "unavailable"
    with ui.row().classes(f"{LAUNCH_STATE_CLASS} items-center gap-2 dnote"):
        ui.label(ds.service_state_message(status, lang))
        if on_retry is not None:
            ui.button(ds.retry_label(lang), on_click=on_retry).props(
                "flat dense size=sm no-caps")


# ---------------------------------------------------------------------------
# THE NOVELTY AXIS.
# ---------------------------------------------------------------------------


def render_novelty_help(lang: str = "en", *, as_of: Optional[str] = None) -> None:
    """The help affordance beside the candidacy switch.

    Carries, in one place: the checked-source list, the sentence that the
    identification is an unreviewed algorithmic match so this is a candidate
    rather than a confirmed find, and -- when the artifact records one -- the
    date the sources were checked as of. All three come from data or from the
    shared vocabulary; none is composed here.
    """
    lang = _lang_key(lang)
    with ui.column().classes(f"{NOVELTY_HELP_CLASS} dnote text-xs gap-1"):
        ui.label(ds.novelty_strings(lang)["help"])
        if as_of:
            ui.label(copy_text("novelty_as_of", lang).format(date=as_of))


def novelty_badge(item: Mapping[str, Any], lang: str = "en") -> Optional[Tuple[str, str]]:
    """`(text, css classes)` for a row's novelty badge, or `None` for no badge.

    Three cases, and the third is the one that matters:

    * the candidacy shade -> the solid `.nov` badge, worded as CANDIDACY only;
    * the fail-closed `not_checked` shade -> the muted-italic `.nov.unknown`
      badge, which tells the reader the check did not run rather than showing a
      verdict that was never reached;
    * every OTHER shade -> **no badge at all**. A badge asserts something; its
      absence asserts nothing. There is no ratified reader wording for the
      remaining shades, and inventing one on the page that carries the novelty
      axis is how an unearned claim gets made.

    Returns `None` when the unit does not offer novelty (`novelty_offered` is
    the service's own flag -- a work spanning many manuscripts has no single
    verdict) or when the status is out of vocabulary.
    """
    if not item.get("novelty_offered"):
        return None
    status = item.get("novelty_status")
    if status not in NOVELTY_STATUSES:
        return None
    if status == CANDIDATE_STATUS:
        return ds.novelty_strings(_lang_key(lang))["badge"], "nov"
    if status == DEFAULT_STATUS:
        return ds.novelty_unknown_badge(_lang_key(lang)), "nov unknown"
    return None


# ---------------------------------------------------------------------------
# COVERAGE -- the one permitted percentage on a discovery surface (D-08/D-21).
# ---------------------------------------------------------------------------


def coverage_clause(item: Mapping[str, Any], lang: str = "en") -> Optional[str]:
    """The qualified coverage clause for one findings row, or `None`.

    DERIVED from `shared.discovery_display_strings.row_headline` rather than
    retyped, for two reasons that are both load-bearing:

    1. the honesty gate permits exactly one percentage on a discovery surface,
       and only when its own qualifier sits within a short window after it. A
       clause composed here would be a second coverage vocabulary, and the
       first time the shared one moved, this surface would start failing the
       gate -- or worse, stop being covered by the exception;
    2. the GATING (direct family only; a propagated row carries none; a null
       measurement omits the clause and its separator) is the shared module's
       decision. Re-deriving it here is how a surface starts showing a figure
       the data does not support.

    `SURFACE_FINDING_FIELDS` carries no `evidence_source`, so the family gate
    runs on `relation_kind` alone -- the conservative reading `row_headline`
    documents. That is sound on this grain: `max_coverage_ppm` is the MAX over
    the identification's evidence and every shipped propagated row measures
    NULL, so a purely propagated identification arrives here with nothing to
    show and is omitted by the null rule.
    """
    ppm = item.get("max_coverage_ppm")
    relation = item.get("relation_kind")
    if ppm is None or not relation:
        return None
    try:
        with_coverage = ds.row_headline("", ppm, relation, lang)
        without_coverage = ds.row_headline("", None, relation, lang)
    except ValueError:
        # An out-of-vocabulary relation kind. The shared module raises rather
        # than rendering a blank; on THIS element the honest treatment is to
        # omit the figure, because the row is still worth showing.
        return None
    if with_coverage == without_coverage:
        return None
    return with_coverage[len(without_coverage):].strip().lstrip(_DOT).strip()


# ---------------------------------------------------------------------------
# ONE RESULT ROW, in each of the three shipped units.
# ---------------------------------------------------------------------------


def _work_title(item: Mapping[str, Any], lang: str) -> str:
    """Ruling R: every work title a reader sees routes through
    `display_work_title`. A surface formatting `neutral_title` directly
    silently opts out of the curation and prints a halakhic work's name over
    pages the owner ruled are mostly liturgy."""
    raw = item.get("neutral_title")
    if not raw:
        return ds.missing_title(lang)
    work_id = item.get("display_work_id") or item.get("canonical_work_id") or ""
    return ds.display_work_title(work_id, raw, lang) or ds.missing_title(lang)


def _render_shelfmark(item: Mapping[str, Any]) -> None:
    """The manuscript link -- LIVE, unlike the work title.

    `/work/{id}` does not exist until Phase 136.1, so work titles render as
    plain text; the manuscript page does exist and a reader needs to reach it
    from the row.
    """
    sys_id = item.get("sys_id")
    shelfmark = item.get("shelfmark_display")
    library = item.get("library_code")
    if library:
        ui.label(str(library)).classes("chip")
    if not shelfmark:
        return
    if sys_id:
        ui.link(str(shelfmark), f"/browse?sys_id={sys_id}").classes(ROW_SHELFMARK_CLASS)
    else:
        ui.label(str(shelfmark)).classes(ROW_SHELFMARK_CLASS)


def _render_row_meta(item: Mapping[str, Any], lang: str, unit: str) -> None:
    """The meta line: relation chip, novelty chip, page count, coverage, bucket.

    NO band tooltip. The identification grain exposes `best_band_rank` and no
    band label, evidence source or confidence band, so there is nothing here to
    put on the chip's `title` -- and deriving a label from a rank would be a
    second band vocabulary, which is precisely what the panel renderer refuses
    to do for the same reason. Recorded as a deviation in this plan's summary
    rather than papered over with an invented tooltip.
    """
    with ui.row().classes(f"{ROW_META_CLASS} side items-center gap-2 flex-wrap"):
        relation = item.get("relation_kind")
        if relation:
            try:
                chip = ds.relation_chip(relation, lang)
            except ValueError:
                chip = None
            if chip:
                ui.label(chip).classes(f"rel {ROW_RELATION_CLASS}")

        badge = novelty_badge(item, lang)
        if badge is not None:
            text, classes = badge
            ui.label(text).classes(f"{classes} {ROW_NOVELTY_CLASS}")

        pages = _plural("pages", item.get("page_count"), lang)
        if pages:
            ui.label(pages).classes(f"{ROW_PAGES_CLASS} dnote text-xs")

        coverage = coverage_clause(item, lang)
        if coverage:
            ui.label(coverage).classes(f"{ROW_COVERAGE_CLASS} dnote text-xs")

        # The bucket NAME, from the shared rule -- identical treatment in both
        # buckets. `main_pool` is materialized by the bake from
        # `shared.discovery_main_pool.main_pool_decision`; this renders its
        # name and never re-decides it.
        ui.label(ds.bucket_name(bool(item.get("main_pool")), lang)).classes(
            f"{ROW_BUCKET_CLASS} dnote text-xs")

        if unit == FINDINGS_UNIT_MANUSCRIPT and item.get("multi_work_annotation"):
            ui.label(copy_text("multi_work", lang)).classes(
                f"{ROW_ANNOTATION_CLASS} dnote text-xs")


def render_finding_row(item: Mapping[str, Any], lang: str = "en") -> None:
    """One result row, in whichever unit the service produced it.

    The unit arrives ON the row (`unit`, part of the projection); it is
    branched on and never rendered. The three units differ only in what
    identifies the row and what the sub-line counts -- the meta line, the
    bucket treatment and the title routing are identical, which is what makes
    "a second-bucket row looks like a main-pool row" a property of the code
    rather than of a reviewer's care.
    """
    lang = _lang_key(lang)
    unit = item.get("unit") or FINDINGS_UNIT_IDENTIFICATION

    with ui.column().classes(f"row {ROW_CLASS} w-full gap-1 p-2"):
        if unit == FINDINGS_UNIT_MANUSCRIPT:
            with ui.row().classes(f"{ROW_TITLE_CLASS} items-center gap-2 font-bold"):
                _render_shelfmark(item)
            works = _plural("works", item.get("work_count"), lang)
            if works:
                ui.label(works).classes(f"{ROW_SUB_CLASS} r-sub text-xs")
        elif unit == FINDINGS_UNIT_WORK:
            ui.label(_work_title(item, lang)).classes(f"{ROW_TITLE_CLASS} font-bold")
            with ui.row().classes(f"{ROW_SUB_CLASS} r-sub items-center gap-2 text-xs"):
                author = item.get("author")
                if author:
                    ui.label(str(author))
                manuscripts = _plural("manuscripts", item.get("manuscript_count"), lang)
                if manuscripts:
                    ui.label(manuscripts)
        else:
            ui.label(_work_title(item, lang)).classes(f"{ROW_TITLE_CLASS} font-bold")
            with ui.row().classes(f"{ROW_SUB_CLASS} r-sub items-center gap-2 text-xs"):
                _render_shelfmark(item)
                author = item.get("author")
                if author:
                    ui.label(str(author))

        _render_row_meta(item, lang, unit)


__all__ = [
    "LAUNCH_BASIS_CLASS",
    "LAUNCH_CLASS",
    "LAUNCH_CONTEXT_CLASS",
    "LAUNCH_SHADE_CLASS",
    "LAUNCH_STATE_CLASS",
    "LAUNCH_TOTAL_CLASS",
    "NOVELTY_HELP_CLASS",
    "ROW_ANNOTATION_CLASS",
    "ROW_BUCKET_CLASS",
    "ROW_CLASS",
    "ROW_COVERAGE_CLASS",
    "ROW_META_CLASS",
    "ROW_NOVELTY_CLASS",
    "ROW_PAGES_CLASS",
    "ROW_RELATION_CLASS",
    "ROW_SHELFMARK_CLASS",
    "ROW_SUB_CLASS",
    "ROW_TITLE_CLASS",
    "copy_keys",
    "copy_text",
    "coverage_clause",
    "launch_shade_label",
    "novelty_badge",
    "render_finding_row",
    "render_launch_headline",
    "render_novelty_help",
]
