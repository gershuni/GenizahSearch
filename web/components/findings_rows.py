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
  class or none of it applies. `web/static/common.css` is not modified by this
  module and two tests assert so.

  ONE exception, added 2026-08-04 with the owner's layout verdict: the launch
  headline's own container carries an inline rule + tint so it reads as one
  designed block rather than as loose prose lines. It is INLINE (no stylesheet
  rule, no new class to keep in sync) and every directional property in it is
  LOGICAL (`border-inline-start`), because this surface renders in both
  directions and a physical `border-left` would put the rule on the wrong side
  in Hebrew. `test_no_row_level_accent_rule_is_keyed_on_novelty` fails on any
  physical directional property in this file.

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

import html as _html
import inspect as _inspect
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import quote

from nicegui import ui

import shared.discovery_display_strings as ds
from shared.discovery_novelty import CANDIDATE_STATUS, DEFAULT_STATUS, NOVELTY_STATUSES
from shared.discovery_service import (
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNIT_MANUSCRIPT,
    FINDINGS_UNIT_WORK,
    LAUNCH_CONTRIBUTION_SHADES,
    _build_findings_filter,
)
from shared.discovery_surface_projection import is_outage
from web.components import discovery_links as links

# ---------------------------------------------------------------------------
# Marker classes. Render-smoke assertions scope to these; an assertion that
# searches the whole rendered page can pass for the wrong reason (the
# findings-page reference records exactly that failure). Renaming one is a
# breaking change, not a refactor.
# ---------------------------------------------------------------------------

LAUNCH_CLASS = "gs-findings-launch"
#: The LEDE -- the unconditional main-pool figure and the label beside it. Two
#: elements in one `items-baseline` row and NEVER one concatenated string: a
#: leading Latin-digit run followed by a Hebrew phrase can reorder
#: unpredictably at the boundary, and the figure is the one thing on this page
#: that must not move.
LAUNCH_LEDE_CLASS = "gs-findings-launch-lede"
LAUNCH_POOL_TOTAL_CLASS = "gs-findings-launch-pool-total"
LAUNCH_POOL_LABEL_CLASS = "gs-findings-launch-pool-label"
#: The APPROVED lede (owner ruling, 2026-08-05): fragments and works, then the
#: all-in-all match count, then the pool split. Every figure is its own element
#: for the same reason the main-pool lede's is -- a Latin-digit run and a
#: Hebrew phrase concatenated into one string can reorder at the boundary --
#: and each half of the split carries its own bucket name, because a number
#: with no bucket beside it is a number a reader has to guess the basis of.
#: NO `LAUNCH_LEDE_ROW_CLASS`. It existed for a few hours on 2026-08-06 as the
#: wrapper that put the fragment lede and the work count on ONE LINE, and the
#: STAT-CARD BAND that replaced it the same day does that job better: each figure
#: is a bounded card in `LAUNCH_STATS_BAND_CLASS`, so the pair share a row by
#: construction. The constant went with the element rather than lingering as a
#: marker a test could still assert against -- a selector for something nothing
#: renders is a guard that can only ever pass.
#: THE STAT-CARD BAND (owner ruling, 2026-08-06: "do the top stats in neat
#: cards"). The band and one card inside it.
#:
#: The card treatment is `web/pages/home.py`'s corpus-stats band, VERBATIM -- the
#: same border, radius, tint, min-width and flex basis -- because this site
#: already has a stat card and a second one would be a second visual language for
#: the same kind of fact. Copied inline rather than promoted to a shared class:
#: this module ships no stylesheet rule, and the two bands are near enough that
#: the duplication is honest while a shared abstraction over two call sites is
#: premature.
LAUNCH_STATS_BAND_CLASS = "gs-findings-launch-stats"
LAUNCH_STAT_CARD_CLASS = "gs-findings-launch-stat"
#: A card's decorative glyph (owner, 2026-08-06: "what about icons"). Its own
#: class so a test can assert it is DECORATIVE -- `aria-hidden`, outside the
#: figure/label pair -- rather than a fact a reader depends on. An icon that
#: carried meaning here would be information available only to sighted readers.
LAUNCH_STAT_ICON_CLASS = "gs-findings-launch-stat-icon"
#: The CONTRIBUTION band -- the shade-filtered figures, in their OWN band below
#: the dotted rule. Deliberately NOT `LAUNCH_STATS_BAND_CLASS`: those figures
#: count every bucket and every shade, these count the main pool only after shade
#: filtering, and a card grid implies its cells are comparable. Two bands, one
#: basis each, is what keeps ruling U's "one stated basis" true in a layout that
#: cards everything.
LAUNCH_CONTRIB_BAND_CLASS = "gs-findings-launch-contrib"
LAUNCH_FRAGMENTS_CLASS = "gs-findings-launch-fragments"
LAUNCH_FRAGMENTS_LABEL_CLASS = "gs-findings-launch-fragments-label"
LAUNCH_MATCHED_CLASS = "gs-findings-launch-matched"
LAUNCH_WORK_TOTAL_CLASS = "gs-findings-launch-work-total"
LAUNCH_ALL_TOTAL_CLASS = "gs-findings-launch-all-total"
LAUNCH_SPLIT_CLASS = "gs-findings-launch-split"
LAUNCH_SPLIT_ITEM_CLASS = "gs-findings-launch-split-item"
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
ROW_DIVERGENCE_CLASS = "gs-findings-row-divergence"
ROW_PAGES_CLASS = "gs-findings-row-pages"
ROW_COVERAGE_CLASS = "gs-findings-row-coverage"
#: NO `ROW_BUCKET_CLASS`. The per-row bucket chip was removed from BOTH pools on
#: 2026-08-06 (see `_render_row_meta`), and its class constant went with it
#: rather than lingering as a marker a test could still assert against -- a
#: selector for an element nothing renders is a guard that can only ever pass.
ROW_SHELFMARK_CLASS = "gs-findings-row-shelfmark"
ROW_ANNOTATION_CLASS = "gs-findings-row-annotation"
ROW_REPORT_CLASS = "gs-findings-row-report"
#: THE ADMIN HIDE CONTROL (owner ruling, 2026-08-06). Its own class so a test can
#: assert it is ABSENT for a non-admin -- the property that matters, and one a
#: scan of rendered text cannot see, because an absent button renders no text.
ROW_SUPPRESS_CLASS = "gs-findings-row-suppress"
#: The catalogue's OWN title for the manuscript (libraries.csv, injected --
#: see `_render_shelfmark`). Lives beside the shelfmark, not the meta line: it
#: names WHICH manuscript this is, the same job the shelfmark does, rather
#: than describing the finding.
ROW_CATALOGUE_TITLE_CLASS = "gs-findings-row-catalogue-title"
#: THE AUTHOR OF THE COMPUTED IDENTIFICATION'S WORK -- ours, from the sidecar
#: (`display_author`/`author`), NEVER the catalogue's.
#:
#: ITS OWN LINE, ABOVE the shelfmark line, and that placement is a CORRECTNESS fix
#: rather than a layout preference (owner report, 2026-08-07: *"the name in the id
#: looks like it comes from the cat[alogue]"*). It previously rendered as the last
#: element of the same flex row that carries "Catalogued as: <title>", so a reader
#: scanning left-to-right met `Catalogued as: ספר השרשים (קטע). נתן בן יחיאל מרומי`
#: and read the name as part of the quotation -- i.e. as the LIBRARY's attribution.
#: That is precisely the misreading this page cannot afford: the whole purpose of
#: showing the catalogue's own words is to let a reader weigh them AGAINST the
#: computed identification, and an attribution on the wrong side of that line
#: destroys the comparison.
#:
#: Its own class so the placement is assertable -- "which line is it on" is not
#: something a scan of rendered text can see.
ROW_AUTHOR_CLASS = "gs-findings-row-author"

#: THE EXPANSION (owner-approved, 2026-08-05): a manuscript or work row opens
#: IN PLACE onto the identifications underneath it. Its own classes, because the
#: children are `ROW_CLASS` rows and a scan scoped to the parent's class would
#: otherwise be unable to tell a parent from its child.
ROW_EXPANDER_CLASS = "gs-findings-row-expander"
ROW_CHILDREN_CLASS = "gs-findings-row-children"
ROW_CHILD_CLASS = "gs-findings-row-child"
ROW_CHILDREN_STATE_CLASS = "gs-findings-row-children-state"
#: The preview, which lives on the identification LEAF only. A work row spans
#: manuscripts, so "preview" there would have to pick one, and picking is
#: adjudication -- which is the one thing no surface here does.
ROW_PREVIEW_CLASS = "gs-findings-row-preview"
#: The text-vs-text disclosure (excerpt-v1), leaf-only for the same reason as
#: the preview: a grouped row spans identifications, and choosing whose texts
#: to show is adjudication.
ROW_EXCERPT_CLASS = "gs-findings-row-excerpt"


def preview_targets_a_folio(item: Mapping[str, Any]) -> bool:
    """Whether this row's preview opens ON a matched folio rather than on the
    manuscript's first page.

    ONE PREDICATE, USED BY BOTH THE NOTE AND THE LINK (Codex review,
    2026-08-08). The note lives here and the URL is built in `web/pages/
    findings.py`, so before this existed each asked its own question -- the note
    "is there a folio?" and the URL "is there a folio AND a volume?" -- and a row
    carrying one without the other made the note promise what the link did not
    deliver. The service now emits the pair atomically, which closes it at the
    source; this closes it at the two consumers as well, so neither a hand-built
    row nor a future second producer can reopen it.

    A THIN ADAPTER over `web/components/discovery_links.py`, which owns the rule
    for all three discovery link sites. This function's whole content is knowing
    which two KEYS a findings row carries the address under; the rule itself is
    not restated here, because a second statement of it is how the panel's rows
    and this page's rows came to disagree in the first place.
    """
    return links.browse_target_is_a_folio(
        item.get("first_match_page"), item.get("first_match_volume_ie"))

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
    # H2 -- THE REPORT AFFORDANCE (owner ruling, 2026-08-05, recorded in
    # `136-FLAG-ON-READINESS.md`). Wrong attributions are SYSTEMATIC rather than
    # incidental -- CERT-01's per-stratum precision runs 1.000 down to 0.471 --
    # so the correction mechanism at scale is the next data bake, NOT a
    # per-item takedown list. Reports feed the bake. The owner's ruling on the
    # mechanism was "email us is enough", so this is a `mailto:` and nothing
    # else: no table, no write, no moderation queue, no retraction path.
    #
    # THE WORDING PROMISES NOTHING. It invites a report; it does not commit to a
    # response, a correction or a timeline, because none of those is what the
    # ruling created. "Report a problem" is an offer; "we will fix it" would be
    # a commitment nobody made.
    "report_link": {
        "en": "Report a problem",
        "he": "דיווח על בעיה",
    },
    # THE ADMIN HIDE CONTROL's accessible name (owner ruling, 2026-08-06: "can we
    # add X to identification so admin can click and hide it").
    #
    # It says HIDE, not "delete" or "reject": the row is removed from the surface
    # and nothing about the underlying identification changes -- the artifact is
    # content-hash verified and cannot be edited at all. Naming it "delete" would
    # promise a permanence this mechanism does not have, and "reject" would claim
    # an adjudication, which is the one thing no surface in this phase does.
    #
    # Bilingual even though only the owner sees it: an admin reading the Hebrew
    # interface should not meet an English control, and the cost of the second
    # string is nil.
    "suppress_row": {
        "en": "Hide this finding",
        "he": "הסתרת ממצא זה",
    },
    # THE LINK TEXT IS BILINGUAL; THE PREFILLED MESSAGE IS ASCII. The reader
    # writes their own text into the body in whichever language they please;
    # what the link prefills is only the part they could not supply themselves,
    # so keeping those two field labels in English costs a reader nothing.
    #
    # It is NOT a honesty-gate workaround, and an earlier revision of this
    # comment claimed it was -- wrongly, on two counts that were then measured:
    # the markup gate extracts TEXT and never reads an `href` attribute, and an
    # ASCII URL trips the percentage detector anyway whenever the digest ends in
    # a digit (`...9%0A`), which is most of them. Percent-encoding is transport;
    # what a human reads is the DECODED message, and that is what the suite
    # scans through the six-detector entry point.
    "report_subject": {
        "en": "Computed identification report",
        "he": "Computed identification report",
    },
    # The body carries ONLY an identifier and a version -- the two things that
    # make a report reproducible against the exact artifact that produced the
    # row. D-25 binds here as everywhere: nothing drawn from a masked source
    # goes into an email body, and neither of these is.
    "report_body": {
        "en": (
            "Finding: {identification}\n"
            "Data version: {version}\n\n"
        ),
        "he": (
            "Finding: {identification}\n"
            "Data version: {version}\n\n"
        ),
    },
    # OWNER WORDING, 2026-08-06: "not found in available catalogs".
    #
    # Applied with ONE substantive change to the owner's phrasing, flagged rather
    # than made silently: "the catalogues we checked", not "available catalogs".
    #
    # The reason is that "available" is a claim we cannot support and the honest
    # version is narrower. What the novelty check actually consults is a FIXED,
    # ENUMERABLE, DATED set -- FJMS + NLI catalogues and bibliography, titles,
    # PGP, FGP, shelfmark attributions (`novelty_strings()['help']`, ratified) --
    # and the ratified help text says in as many words that "absence from the
    # finding aids checked ... only means the checked sources do not already
    # record it". "Available catalogues" reads as ALL of them, i.e. as a claim
    # that nothing in the scholarly literature records this identification. That
    # is a much stronger statement than the data supports, on the most prominent
    # line of the page, and it is the direction the whole surface leans away
    # from. "We checked" keeps the owner's plainer word ("catalogues" for
    # "finding aids") while keeping the scope the check really has.
    #
    # The claim SHAPE is unchanged and that is what matters for the honesty gate:
    # this is a statement about where the identification is ABSENT, never about
    # how often it is right. No rate word, no quantity word, so it cannot read as
    # an accuracy claim beside its own four figures.
    #
    # Hebrew follows the same reading: `שלא נמצאו בקטלוגים שבדקנו`.
    "launch_total": {
        "en": "{count} identifications not found in the catalogues we checked",
        "he": "{count} זיהויים שלא נמצאו בקטלוגים שבדקנו",
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
    # -- THE LEDE (2026-08-05). The headline led with the SUBSET: `total` is the
    # -- shade-filtered contribution figure, and the release's own size -- every
    # -- main-pool identification, whatever its novelty shade -- was not in the
    # -- envelope at all until `meta.main_pool_total` was added beside it.
    #
    # Two keys, one basis, and the basis is NAMED in the lede itself
    # (`{bucket}`, from the single bucket definition) rather than inferred.
    # "computed identifications" is the page's own title, not a new claim: it
    # says what the rows ARE -- matches found by software -- and asserts nothing
    # about how many are right.
    "launch_pool_total": {
        "en": "computed identifications in the {bucket}",
        "he": "זיהויים מחושבים ב{bucket}",
    },
    # DELIBERATELY NOT `launch_manuscripts` above, which repeats the bucket
    # name. The lede has just named the pool one line up, and repeating it there
    # is a large part of what made the old block read as a wall of figures.
    "launch_pool_manuscripts": {
        "en": "Across {count} fragments.",
        "he": "על פני {count} קטעים.",
    },
    # -- THE APPROVED LEDE (owner ruling, 2026-08-05). The headline led with the
    # -- MAIN POOL, a subset: the number a reader met first was smaller than the
    # -- release, and the second pool appeared nowhere in the headline at all.
    #
    # The first draft of the fix led with the all-in-all identification count
    # alone, and the owner chose against it from a rendered comparison, for a
    # measured reason: 81% of fragments (31,158 of 38,431) carry exactly one
    # identification, so a reader's one-fragment-one-identification model is
    # right most of the time -- which makes "53,581 identifications on 38,431
    # fragments" look like an error rather than like a distribution. The
    # approved lede therefore states TWO DIFFERENT KINDS of thing, fragments and
    # works, so no ratio is invited at all, and puts the identification count
    # under them as a quiet third line.
    #
    # All three figures share ONE basis (every bucket, every shade), which is
    # what lets them sit in one block. `total` and `all_bucket_total` are shade
    # filtered and must never join them.
    "launch_fragments_lede": {
        "en": "fragments",
        "he": "קטעים",
    },
    # OWNER WORDING, 2026-08-06: "555 known works matched", not "matched to 555
    # known works".
    #
    # The card put the figure ABOVE its words, and the old phrasing was written for
    # a figure sitting INSIDE them -- so the card read "555 / matched to known
    # works", where "matched to" dangles with nothing before it. Leading with the
    # noun and closing with the participle is what the card shape wants: the
    # number, then "known works matched".
    #
    # `{count}` STAYS IN THE TEMPLATE even though `_sentence_label` strips it for
    # the card. The template is the single definition of the sentence and the v1/v2
    # fallbacks may yet render it inline; a template with no placeholder would
    # silently drop the figure there.
    "launch_matched_works": {
        "en": "{count} known works matched",
        "he": "{count} חיבורים מוכרים הותאמו",
    },
    # WORDING NOTE, raised rather than resolved silently: this line says
    # "matches" while the line below it says "identifications" and the page is
    # titled "Computed Identifications". The owner approved this wording; the
    # alternative would have meant editing a ratified string, which is not a
    # change to make in passing. Flagged for a vocabulary ruling.
    "launch_matches_in_all": {
        "en": "{count} matches in all.",
        "he": "{count} התאמות בסך הכל.",
    },
    # THE POOL SPLIT, directly under the lede that sums it. It is what makes the
    # second pool visible as a real, comparable body of work rather than a word
    # on a chip -- the owner's stated reason for leading with the sum.
    #
    # RULING T IS UNTOUCHED: it forbids a count on the bucket CONTROL, and this
    # is the headline. The figures must never be mirrored onto the chips.
    #
    # The Hebrew does NOT open with the figure: a Latin-digit run at the start
    # of an RTL label reorders unpredictably at the boundary with the word after
    # it, so the Hebrew leads with the bucket name and the English with the
    # count. `{bucket}` is `bucket_name`, the single definition.
    "launch_pool_share": {
        "en": "{count} under '{bucket}'",
        "he": "תחת '{bucket}': {count}",
    },
    # The scope figures, which count what THIS RELEASE TOUCHED. They used to be
    # rendered as "Out of {fragments} fragments across {pages} pages in the
    # whole corpus", and that was a COVERAGE OVERCLAIM on the most prominent
    # line of a scholarly surface: `corpus_manuscript_count` counts fragments
    # that already carry a computed identification and `corpus_page_count`
    # counts pages carrying at least one claim, while the project's corpus is
    # ~255,615 manuscript records. Presenting the denominator of what we matched
    # as the denominator of everything made the release read ~6.6x better
    # covered than it is. Both strings now say what they count; neither claims a
    # corpus denominator, and none is invented from another database.
    "launch_context": {
        "en": (
            "Across {fragments} fragments carrying a computed identification, "
            "on {pages} pages with at least one match."
        ),
        "he": (
            "על פני {fragments} קטעים שיש בהם זיהוי מחושב, ב" + _MAQAF +
            "{pages} דפים עם התאמה אחת לפחות."
        ),
    },
    # The same scope statement with the FRAGMENT half dropped, for the block
    # whose lede already gave the fragment figure. Repeating it two lines lower
    # is the reading hazard the level structure exists to remove.
    "launch_claim_pages": {
        "en": "On {pages} pages with at least one match.",
        "he": "ב" + _MAQAF + "{pages} דפים עם התאמה אחת לפחות.",
    },
    # -- the three contribution shades, match-framed (ruling U constraint 4).
    #    Each says what the CATALOGUES DID record; none says a catalogue was
    #    wrong.
    #
    # "THE CATALOGUES WE CHECKED", NOT "the aid" (owner ruling, 2026-08-06: "'the
    # aid' is not good. should be 'available catalogs'"). Two changes in one:
    #
    # 1. "aid" is jargon. "Finding aid" is a library-science term of art, and on a
    #    card with three words of room it reads as a typo for "the aid" in the
    #    humanitarian sense. The owner is right that no reader outside the field
    #    parses it.
    # 2. "we checked", not "available". Applied with the same correction as
    #    `launch_total` above and for the same reason -- the check consults a
    #    FIXED, ENUMERABLE, DATED set (FJMS + NLI catalogues and bibliography,
    #    titles, PGP, FGP, shelfmark attributions), and the ratified help text says
    #    in as many words that absence from it "only means the checked sources do
    #    not already record it". "Available catalogues" reads as ALL of them, which
    #    would turn a statement about our own source list into a claim about the
    #    scholarly literature. Flagged rather than applied silently.
    #
    # The CLAIM SHAPE is unchanged, which is what the honesty gate turns on: each
    # label still describes what the checked catalogues recorded, never how likely
    # our match is to be right.
    "shade_fills_gap": {
        "en": "no prior identification",
        "he": "אין זיהוי קודם",
    },
    # NOT "more accurate than the catalogues": the Hebrew word for accurate is a
    # rate word in the shared honesty gate's lexicon, and beside a shade count it
    # would read -- to the gate and to a reader -- as an accuracy claim about
    # the match rather than a statement about granularity.
    #
    # "פירוט רב יותר", not "פירוט עדין יותר" (owner, 2026-08-07: the latter "does
    # not sound good"). `עדין` is *delicate/subtle* -- an aesthetic word about
    # texture, which is not what a granularity comparison means; `רב יותר` is
    # plainly *more* detail, the same comparative the English "finer than" makes.
    #
    # "שנבדקו", not "שבדקנו" (owner, 2026-08-07). The passive drops the
    # first-person: the sentence is about WHICH catalogues were consulted, not
    # about who did the consulting, and "we checked" put a speaker into a row
    # that otherwise has none. The English "we checked" is unchanged -- it reads
    # naturally there, and the owner corrected only the Hebrew.
    "shade_refines_granularity": {
        "en": "finer than the catalogues we checked",
        "he": "פירוט רב יותר מהקטלוגים שנבדקו",
    },
    "shade_container_predicts": {
        "en": "the catalogues named only a container",
        "he": "הקטלוגים ציינו מכלול בלבד",
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
    # -- THE EXPANSION. A grouped row states a count; these open it onto the
    #    rows the count counted. The label says WHAT opens, not "more" or
    #    "details": a reader deciding whether to spend a click is deciding
    #    whether they want the individual matches.
    "expand_open": {
        "en": "Show the individual matches",
        "he": "הצגת ההתאמות הבודדות",
    },
    "expand_close": {
        "en": "Hide the individual matches",
        "he": "הסתרת ההתאמות הבודדות",
    },
    # A FAILED expansion says so. An empty body after a failed read is
    # indistinguishable from "this row has no matches underneath it", and one of
    # those is a fact while the other is an outage -- the same false-zero class
    # the envelope's named statuses exist to prevent.
    "expand_failed": {
        "en": "Could not load the individual matches. Try again.",
        "he": "לא ניתן לטעון את ההתאמות הבודדות. נסו שוב.",
    },
    # An expansion showing SOME of what it counted says how many, so a reader
    # never mistakes a bounded page for the whole group.
    "expand_partial": {
        "en": "Showing {shown} of {count}",
        "he": "מוצגות {shown} מתוך {count}",
    },
    # The route to the rest of the group. Named as an ACTION on the same
    # population the line beside it just counted, so the pair reads as one
    # statement: this much of that many, and here is how to see more.
    #
    # IT NAMES WHAT IT LOADS, and a bare "Show more" is exactly what it must not
    # be (external review, 2026-08-06). The pool invitation on the same page is
    # labelled "Show more possible matches" (`SHOW_MORE_TOGGLE`, a ratified D-11
    # constant pinned byte-for-byte by `tests/test_discovery_band_labels.py`, so
    # THIS is the string that moves). In English "Show more" was a strict PREFIX
    # of it, so anything matching a control by text matched both -- and the two
    # do very different things: one loads the next 25 children of the row you
    # opened, the other switches the whole page to the second pool. A reader on
    # a work with 2,981 children could aim for one and be given the other.
    #
    # This is the SAME collision class as the "Show" / "Show as" pair fixed
    # earlier the same day, one instance further on: the fix there was to name
    # the axis, and the fix here is to name the population.
    "expand_more": {
        "en": "Load more of this group",
        "he": "טעינת עוד מקבוצה זו",
    },
    # -- THE PREVIEW, on the identification leaf only. "Preview" rather than
    #    "open": it does not leave the page, and a reader who expects to leave
    #    and does not is a reader who lost their filters.
    #    "כתב היד", not "הכתב" (owner, 2026-08-07). `הכתב` on its own is *the
    #    handwriting* or *the script* -- a palaeographic word, and exactly the
    #    wrong one on a surface where readers do compare hands. The manuscript is
    #    `כתב היד`; only the full construct says the object rather than its
    #    letterforms.
    "preview_open": {
        "en": "Preview the manuscript",
        "he": "תצוגה מקדימה של כתב היד",
    },
    "preview_close": {
        "en": "Close",
        "he": "סגירה",
    },
    # The preview is a VIEWER, not a verdict. It shows the manuscript; it says
    # nothing about whether the match is right, because nothing here does.
    #
    # TWO NOTES, BECAUSE THE AFFORDANCE NOW HAS TWO OUTCOMES (owner report,
    # 2026-08-08). It USED to have one: it opened the manuscript at folio 1
    # always, and the note named that limit on the stated basis that "a findings
    # row carries `page_count` and NO folio identifier, so there is nothing here
    # to target a page with", with the folio deferred to a future bake. The
    # second half of that was wrong -- every contributing page id was already in
    # the served asset on `discovery_evidence.a_page_id`, aggregated away only at
    # the identification grain -- so the service now resolves it at read time and
    # the preview opens ON a matched folio.
    #
    # The old wording is KEPT, verbatim, as `preview_note_manuscript`, because
    # the fallback it describes is still reachable: a row whose folio did not
    # resolve opens the manuscript exactly as before, and telling that reader
    # they are looking at the matched folio is the single most misleading thing
    # this affordance could say. A reader who lands on folio 1r of a 40-folio
    # manuscript and finds nothing resembling the identification reasonably
    # concludes the identification is wrong.
    #
    # "A folio the match was found on" -- never "THE matched folio", and never
    # "the FIRST" either. Both stronger readings are claims this sentence is not
    # entitled to make:
    #
    #   * "the" -- 46% of identifications match on more than one folio, so there
    #     is usually no single matched folio to be definite about.
    #   * "the first" -- the folio is chosen by ordering page ids, which orders
    #     by inventory-entry id before folio number. THAT IS NOT AN AUTHORITATIVE
    #     VOLUME ORDER (Codex review, 2026-08-08): inventory-entry ids are 7, 8
    #     and 9 digits long in the served artifact, so their order is neither
    #     numeric nor the shelf order of the volumes, and on 163 of the 988
    #     multi-volume identifications a numeric reading would name a different
    #     volume. Within ONE volume the choice genuinely is the earliest matched
    #     folio, which is the 98.2% case -- but a sentence that says "first" on
    #     every row is wrong on the rest, and the row cannot tell a reader which
    #     kind it is.
    #
    # The weaker sentence is true of every row, which is the only kind of
    # sentence this surface is allowed to print.
    "preview_note": {
        "en": "Opens at a folio the match was found on. "
              "Browse to read it for yourself.",
        "he": "נפתח בדף שבו נמצאה ההתאמה. "
              "אפשר לדפדף ולקרוא.",
    },
    "preview_note_manuscript": {
        "en": "Opens the manuscript at its first page — not the matched folio. "
              "Browse to read it for yourself.",
        "he": "נפתח בעמוד הראשון של כתב היד — לא בדף שהותאם. "
              "אפשר לדפדף ולקרוא.",
    },
    # -- the domain facet's header. It NAMES ITS AXIS: the domain of the
    #    IDENTIFIED WORK, never the manuscript's catalogue domain. Filtering on
    #    the catalogue axis would hide exactly the findings that disagree with
    #    the catalogue, which are the valuable ones.
    "facet_domain_header": {
        "en": "Domain of the identified work",
        "he": "תחום החיבור המזוהה",
    },
    # NO `novelty_as_of` STRING, AND NO DATE ANYWHERE IN THIS AFFORDANCE (owner
    # ruling, 2026-08-06). Deleted rather than reworded, twice over, and the
    # second attempt is why it is gone rather than fixed.
    #
    # The line rendered `data_as_of` from the artifact's meta. It first read
    # "Sources checked as of {date}", which the owner rejected because it implied
    # the LIVE catalogues were consulted; it was reworded to "Checked against a
    # snapshot of the sources taken {date}", and the owner rejected that too, for
    # a harder reason: `data_as_of` is the BAKE's date, not the snapshot's. The
    # novelty check ran against the sidecars this website already had -- FJMS,
    # NLI, PGP, FGP copies refreshed on their own schedules, each already older
    # than the bake by an interval nobody recorded. So a single date cannot name
    # what was checked, whatever words surround it: there is no one snapshot and
    # no one date, and any date printed here is read as the freshness of all of
    # them.
    #
    # The honest statement is therefore the CATEGORICAL one below, with no date at
    # all. A reader loses nothing they could have relied on -- the date they were
    # being shown did not mean what it appeared to mean.
    #
    # `data_as_of` remains in the artifact's meta and in the loader's required
    # keys; it is provenance for us, not a reader-facing freshness claim. If a
    # future bake records a real per-source snapshot date, that is a NEW string
    # with its own wording ruling, not this one restored.
    #
    # THE STALENESS WARNING, now the whole of what this affordance says about
    # freshness. It states the direction that matters: the live catalogues may
    # have moved on, so an identification the page calls a candidate may already
    # be recorded upstream.
    #
    # THIS IS THE HONEST DIRECTION OF THE ERROR. Every other caveat on this
    # surface guards against overclaiming a match's correctness; this one guards
    # against overclaiming its NOVELTY, which is the other way a reader can be
    # misled here and the only one the rest of the page does not already cover.
    #
    # NO DATE, NO FIGURE, and no promise of a refresh schedule: we do not know
    # what the live services now hold (that is the whole point), and naming a
    # cadence would commit to one nobody has ruled on.
    "novelty_live_may_differ": {
        "en": (
            "Live catalogues may hold newer information we did not check — a "
            "candidate may already be recorded there."
        ),
        "he": (
            "בקטלוגים המקוונים עשוי להיות מידע עדכני יותר שלא נבדק — ייתכן "
            "שמועמד כבר מתועד שם."
        ),
    },
    # -- the catalogue-title attribution (2026-08-05, coordinator ruling: ship
    #    beside the shelfmark, verbatim, one language -- see `_render_shelfmark`
    #    for the reasoning). This is OUR text, introducing THEIR words, so it
    #    alone is bilingual; the title it introduces is not translated.
    "catalogue_title_label": {
        "en": "Catalogued as:",
        "he": "מקוטלג בשם:",
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

    **The message NAMES the authority instead of enumerating it, and WITHHOLDS
    the value it received.** An exception message is an egress class of its own
    -- it reaches a log and, uncaught, a reader -- without passing through
    either the markup scan or the envelope scan.

    Both halves are needed and only the first was there. Listing the valid
    shades would put three stored vocabulary values on that egress. Echoing the
    RECEIVED one puts an ARTIFACT-DERIVED value there, which is the same egress
    and the same rule (D-25): `shade` arrives from the launch envelope's own
    items, and the earlier reasoning for echoing it -- that a value reaching
    this branch is by construction not a member of the vocabulary -- argues
    only that it is not one of OUR strings. It says nothing about what it is,
    and "not a value we recognise" is precisely the description of a value
    nobody has checked. The service's own launch reader already applies this
    rule to its logging, recording the exception TYPE and never an
    artifact-derived value; this is the same boundary one layer out.
    """
    key = _SHADE_COPY_KEY.get(shade)
    if key is None:
        raise ValueError(
            "launch_shade_label: unknown contribution shade (value withheld) -- "
            "the valid set is shared.discovery_service.LAUNCH_CONTRIBUTION_SHADES"
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

    LAYOUT (2026-08-04, owner verdict): the same figures, the same wording and
    the same elements -- read as four stacked prose lines followed by three more
    stacked lines. They now sit in one bounded block: the total leads at display
    size, the two basis lines and the context line share a wrapping row of small
    notes beneath it, and the three contribution shades wrap as inline units
    instead of stacking. Nothing was dropped to achieve that, and no number was
    moved out of the envelope: every figure below is still read from `envelope`
    at request time, which is what the sentinel fixture in this plan's suite
    proves.

    LEAD (2026-08-05, owner ruling): "the headline should say all in all how
    many computed identifications there are, that's the main number to be
    highlighted" -- the lede was the MAIN POOL, a subset, and the second pool
    was not in the headline at all. The approved block ledes with the FRAGMENT
    count and the WORK count (two different kinds of thing, so no ratio is
    invited), states the all-in-all match count quietly under them, and shows
    the pool split so the second pool is visible as a comparable body of work
    rather than as a word on a chip. The contribution figure and its shades
    keep their wording, their classes and their basis line; only their rank
    moved. THREE blocks now, and each is the previous one's fallback: no
    `main_pool_total` renders the 2026-08-04 block, no `identification_total`
    renders the earlier 2026-08-05 lede block, and a missing key never becomes
    a rendered zero.

    RANK (2026-08-05, earlier the same day): the block LED WITH THE SUBSET.
    `total` is the
    shade-filtered contribution figure -- what the release adds to the finding
    aids -- and the release's own size, every main-pool identification whatever
    its shade, was not in the envelope at all. Seven figures then sat at two
    weights in one box in an order whose logic was invisible. With
    `meta.main_pool_total` available the block takes four LEVELS: the pool total
    ledes, the contribution follows at its old weight and its old wording, the
    three shades decompose it, and the corpus context closes quietly. The lede
    and the contribution are separated by a dotted rule, which is what stops
    seven numbers reading as one list: it says everything below is a part of, or
    context for, what is above.

    IT DEGRADES. When `meta.main_pool_total` is absent -- an older sidecar, or
    any caller with an envelope built before that key existed -- the block below
    is EXACTLY the one shipped on 2026-08-04, element for element. A missing key
    must never become a rendered zero: a headline reading "0" is the same class
    of falsehood as a hardcoded one.
    """
    lang = _lang_key(lang)
    with ui.column().classes(f"{LAUNCH_CLASS} w-full gap-1 p-3").style(
        "border-inline-start: 3px solid var(--primary-600); "
        "background: var(--bg-secondary); border-radius: 6px;"
    ):
        if not isinstance(envelope, Mapping) or is_outage(envelope):
            _render_launch_outage(envelope, lang, on_retry)
            return

        items = list(envelope.get("items") or ())
        meta = dict(envelope.get("meta") or {})
        total = envelope.get("total")
        # `bucket_name` delegates to `shared.discovery_main_pool.bucket_label`,
        # so the headline and the result bar can never name the same pool two
        # ways.
        main_pool_name = ds.bucket_name(True, lang)

        if meta.get("main_pool_total") is None:
            _render_launch_v1(items, meta, total, main_pool_name, lang)
            return
        if meta.get("identification_total") is None:
            _render_launch_v2(items, meta, total, main_pool_name, lang)
            return
        _render_launch_v3(items, meta, total, main_pool_name, lang)


#: The stat card's own geometry and chrome, from `web/pages/home.py`'s band.
#:
#: MOBILE-FRIENDLY BY FLEX, not by a media query (owner, 2026-08-06), and the
#: numbers here are the whole mechanism. `flex: 1 1 140px` with `min-width:
#: 140px` means the cards fill the line they are on and wrap when they cannot:
#: five across on a desktop, two across on a phone, with nothing to maintain and
#: no breakpoint to get wrong. `max-width` is deliberately ABSENT -- it was
#: capping the cards at 260px and leaving a ragged gap at the end of a wide row.
#:
#: 140px rather than the homepage band's 150px: this band holds FIVE cards where
#: that one holds five *shorter* labels, and 140 is what fits two per line at
#: 360px (the narrowest phone worth targeting) once the page padding and the
#: block's own inline rule are subtracted.
_STAT_CARD_STYLE = (
    "min-width: 140px; flex: 1 1 140px; "
    "border: 1px solid var(--border-light); border-radius: 10px; "
    "background: var(--bg-tertiary);"
)

#: One stat card's classes. Written once because there are now five call sites
#: and a card that drifted from the others would be the "second visual language"
#: this band exists to avoid.
_STAT_CARD_CLASSES = (
    f"{LAUNCH_STAT_CARD_CLASS} items-center justify-center text-center "
    "px-3 py-3 gap-0"
)

#: EVERY FIGURE IS BIG (owner ruling, 2026-08-06: "make all numbers big"). One
#: constant, so "big" cannot come to mean three different sizes across five
#: cards.
#:
#: `text-3xl` for the supporting figures and `text-4xl` for the lede, which is
#: the ONE hierarchy kept: the owner chose the fragment count as the lede on
#: 2026-08-05 from a rendered comparison, and a test enforces its size. Making
#: every figure identical would have quietly overruled that ratified decision
#: while implementing a layout request. A hero stat beside big supporting stats
#: is still "all numbers big".
_STAT_FIGURE_CLASSES = "text-3xl font-bold"
_STAT_FIGURE_STYLE = "color: var(--primary-700); line-height: 1.15;"
#: The noun under a figure. Small and quiet ON PURPOSE -- it is the label, and
#: the figure is what the owner asked to make big.
_STAT_LABEL_CLASSES = "text-xs text-center"
_STAT_LABEL_STYLE = "color: var(--text-secondary);"


def _render_launch_v1(items, meta: Mapping[str, Any], total: Any,
                      main_pool_name: str, lang: str) -> None:
    """The 2026-08-04 block, unchanged, for an envelope with no lede figure.

    Kept as its own function rather than as branches inside the new one: the
    fallback's whole job is to be BYTE-IDENTICAL to what shipped, and a shared
    body with conditionals is how "identical" quietly stops being true.
    """
    ui.label(
        copy_text("launch_total", lang).format(count=_count(total))
    ).classes(f"{LAUNCH_TOTAL_CLASS} text-xl font-bold")

    with ui.row().classes("items-baseline gap-x-3 gap-y-1 flex-wrap"):
        ui.label(
            copy_text("launch_basis", lang).format(bucket=main_pool_name)
        ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

        manuscripts = meta.get("main_pool_manuscript_count")
        if manuscripts is not None:
            ui.label(
                copy_text("launch_manuscripts", lang).format(
                    count=_count(manuscripts), bucket=main_pool_name)
            ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

        fragments = meta.get("corpus_manuscript_count")
        pages = meta.get("corpus_page_count")
        if fragments is not None and pages is not None:
            ui.label(
                copy_text("launch_context", lang).format(
                    fragments=_count(fragments), pages=_count(pages))
            ).classes(f"{LAUNCH_CONTEXT_CLASS} dnote text-xs")

    # The three shades on ONE wrapping line. Each keeps its own count, its
    # match-framed label and its fragment span; only the stacking is gone.
    with ui.row().classes("items-baseline gap-x-4 gap-y-1 flex-wrap"):
        for item in items:
            _render_launch_shade(item, lang)


def _render_launch_v2(items, meta: Mapping[str, Any], total: Any,
                      main_pool_name: str, lang: str) -> None:
    """FOUR LEVELS, from a lede figure down to the corpus context.

    Every number is still read from the envelope through a placeholder, and the
    two new ones come from `meta.main_pool_total` /
    `meta.main_pool_total_manuscript_count` -- an UNCONDITIONAL main-pool
    population, deliberately NOT `total` (shade filtered) and deliberately NOT
    `main_pool_manuscript_count` (also shade filtered). Pairing one basis's
    figure with the other's is the mixed-basis defect ruling U was issued over,
    which is why the two lede figures are read from the pair that belongs
    together and the contribution keeps its own basis line unchanged.

    `main_pool_manuscript_count` is not rendered here: the lede has just said
    how many fragments the pool spans, and a second, smaller fragment figure
    two lines below it -- on a different basis, in the same block -- is exactly
    the reading hazard the level structure exists to remove. It stays in the
    envelope under its own named key for any caller that wants it.
    """
    # LEVEL 1 -- the lede. The figure and its label are TWO elements in one
    # baseline row, never one concatenated string: a leading Latin-digit run
    # followed by a Hebrew phrase can reorder unpredictably at the boundary.
    with ui.row().classes(
        f"{LAUNCH_LEDE_CLASS} items-baseline gap-x-3 gap-y-1 flex-wrap"
    ):
        ui.label(_count(meta.get("main_pool_total"))).classes(
            f"{LAUNCH_POOL_TOTAL_CLASS} text-4xl font-bold"
        ).style("color: var(--primary-700);")
        ui.label(
            copy_text("launch_pool_total", lang).format(bucket=main_pool_name)
        ).classes(f"{LAUNCH_POOL_LABEL_CLASS} text-sm")

    # LEVEL 1b -- the basis under the lede, on the SAME (unconditional) pair.
    pool_manuscripts = meta.get("main_pool_total_manuscript_count")
    if pool_manuscripts is not None:
        ui.label(
            copy_text("launch_pool_manuscripts", lang).format(
                count=_count(pool_manuscripts))
        ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

    # The separator. LOGICAL and inline -- no stylesheet rule, no new class to
    # keep in sync, and nothing that needs to flip for RTL.
    ui.element("div").classes("w-full").style(
        "border-block-start: 1px dotted var(--border-light); "
        "margin-block-start: 6px; padding-block-start: 6px;"
    )

    # LEVEL 2 -- THE CONTRIBUTION, and it is carded too (owner, 2026-08-06:
    # "what about the rest of the text").
    #
    # IN ITS OWN BAND, and that separation is the one thing this layout may not
    # collapse. The five figures above count EVERY bucket and EVERY shade; these
    # count the main pool only, after shade filtering. A card grid says "these
    # cells are comparable", so putting the two sets in one band would assert a
    # comparison between figures on two different bases -- the mixed-basis defect
    # ruling U was issued over, made this time by a layout rather than by a
    # sentence. Two bands, one basis each, with the dotted rule and the basis
    # line between them. A test asserts the contribution never enters the band
    # above.
    #
    # THE HEADLINE SENTENCE STAYS A SENTENCE. It is the page's claim -- "N
    # identifications not found in the catalogues we checked" -- and a claim is
    # not a stat; shrinking it into a 140px cell would bury the one line that
    # says what this release IS. Its figure is already the largest thing in the
    # block. So the CARDS here are the three shades that decompose it.
    ui.label(
        copy_text("launch_total", lang).format(count=_count(total))
    ).classes(f"{LAUNCH_TOTAL_CLASS} text-xl font-bold")
    ui.label(
        copy_text("launch_basis", lang).format(bucket=main_pool_name)
    ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

    # LEVEL 3 -- the three shades, now one card each: big figure, its
    # match-framed label, and the fragment span as the card's quiet third line.
    # Same strings, same classes, same figures -- `_render_launch_shade` renders
    # them into a card instead of onto a wrapping text line.
    if items:
        with ui.row().classes(
            f"{LAUNCH_CONTRIB_BAND_CLASS} w-full gap-3 flex-wrap items-stretch"
        ):
            for item in items:
                _render_launch_shade(item, lang)

    # LEVEL 4 -- the corpus context: unchanged string and class, last and
    # quietest. It counts EVERY bucket and every shade, and says so in words.
    #
    # DELIBERATELY NOT A CARD. It carries TWO figures in one sentence whose whole
    # job is to state the release's scope in words ("across N fragments carrying a
    # computed identification, on M pages with at least one match"); split into
    # cells, each number would lose the qualifier that keeps it from reading as a
    # corpus denominator -- the coverage overclaim this string was rewritten to
    # fix. It stays prose, quietly, under the cards.
    fragments = meta.get("corpus_manuscript_count")
    pages = meta.get("corpus_page_count")
    if fragments is not None and pages is not None:
        ui.label(
            copy_text("launch_context", lang).format(
                fragments=_count(fragments), pages=_count(pages))
        ).classes(f"{LAUNCH_CONTEXT_CLASS} dnote text-xs")


def _stat_card(figure: Any, label: str, *, figure_marker: str = "",
               label_marker: str = "", pair_marker: str = "",
               figure_classes: str = "", icon: Optional[str] = None,
               note: Optional[str] = None, note_marker: str = "") -> None:
    """ONE stat card in `web/pages/home.py`'s shape: icon, BIG FIGURE, small noun
    under it (owner, 2026-08-06 -- "take the style from there", "what about
    icons").

    That shape is the whole reason every figure here can be big. The ratified
    strings are SENTENCES with the figure inside them ("matched to {count} known
    works", "{count} matches in all", "{count} under '{bucket}'"), and a sentence
    cannot carry a 3xl number in its middle without wrecking its own line
    breaking. Split at the placeholder, the same string becomes exactly the
    homepage's figure-plus-label card -- and no ratified wording is edited,
    because the words are unchanged and only their layout differs.

    THE FIGURE AND THE LABEL ARE SEPARATE ELEMENTS, which is also what the RTL
    property needs: a Latin-digit run and a Hebrew phrase in ONE string can
    reorder unpredictably at the boundary. Here they cannot touch.

    THE ICON IS DECORATION AND IS MARKED AS SUCH. It sits OUTSIDE `pair_marker`
    (so the tests that read the figure/label pair are unaffected) and carries
    `aria-hidden=true`: it adds no information the figure and its noun do not
    already carry, so a screen reader announcing a glyph name would be noise
    between the number and what it counts. Material names only -- the same set
    `web/pages/home.py` draws from, so the two bands cannot diverge in style.

    `note` is the card's optional third line, for the fragment span the three
    contribution shades carry ("across N fragments"). It is quieter than the
    label because it is a scope qualifier on the figure, not the figure's noun.

    `pair_marker` goes on the container the existing tests resolve.
    """
    with ui.column().classes(_STAT_CARD_CLASSES).style(_STAT_CARD_STYLE):
        if icon:
            ui.icon(icon).classes(f"{LAUNCH_STAT_ICON_CLASS} text-2xl").props(
                "aria-hidden=true").style("color: var(--primary-600);")
        with ui.column().classes(
            " ".join(part for part in (pair_marker, "items-center gap-0") if part)
        ):
            ui.label(_count(figure)).classes(
                " ".join(part for part in (
                    figure_marker, figure_classes or _STAT_FIGURE_CLASSES) if part)
            ).style(_STAT_FIGURE_STYLE)
            ui.label(label).classes(
                " ".join(part for part in (label_marker, _STAT_LABEL_CLASSES)
                         if part)
            ).style(_STAT_LABEL_STYLE)
        if note:
            ui.label(note).classes(
                " ".join(part for part in (note_marker, "text-xs text-center")
                         if part)
            ).style("color: var(--text-muted);")


def _sentence_label(template: str, lang: str, **fields: Any) -> str:
    """A ratified template with its `{count}` REMOVED, for use as a card label.

    The figure moves to the card's big slot, so the words that framed it become
    the noun underneath. The words themselves are untouched: this only drops the
    placeholder and tidies the seam it leaves, so a reader sees the ratified
    phrasing minus the digits that are now above it.

    THE HEBREW SEAM NEEDS ITS OWN STEP, and skipping it produced a visible
    defect. `launch_matched_works` is `הותאמו ל־{count} חיבורים מוכרים`, where the
    maqaf ATTACHES to the number -- so removing the placeholder alone left
    `הותאמו ל־ חיבורים מוכרים`: a dangling connector with a space after it, which
    reads as a typo to any Hebrew reader. A prefix ending on a connector is
    therefore closed up against what follows, giving `הותאמו לחיבורים מוכרים`
    ("matched to known works") -- the same statement the sentence made, with its
    figure lifted out.

    Only the SEAM is touched. No word is added, removed or reordered, so the
    ratified phrasing is intact and this stays a layout change.

    `lang` is accepted for symmetry with every other string helper here and is
    deliberately unused: the operation is purely structural, and taking the
    argument keeps the call sites reading like their neighbours.
    """
    prefix, _placeholder, suffix = template.partition("{count}")
    if fields:
        prefix, suffix = prefix.format(**fields), suffix.format(**fields)
    prefix = " ".join(prefix.split())
    suffix = " ".join(suffix.split())
    if prefix.endswith((_MAQAF, "-")):
        text = prefix.rstrip(_MAQAF + "-") + suffix
    elif prefix and suffix:
        text = prefix + " " + suffix
    else:
        text = prefix or suffix
    return " ".join(text.split()).strip(" ,.;:").strip()


def _render_launch_v3(items, meta: Mapping[str, Any], total: Any,
                      main_pool_name: str, lang: str) -> None:
    """THE APPROVED BLOCK (owner ruling, 2026-08-05), as TWO CARD BANDS.

    LAYOUT (owner, 2026-08-06): "do the top stats in neat cards", "make all
    numbers big", "take the style from there" (the homepage corpus-stats band),
    "what about icons", "what about the rest of the text", "have it
    mobile-friendly". Every figure below is now a card with a decorative glyph, a
    big number and its noun beneath -- the homepage's own shape, so the two
    surfaces read as one product.

    NOTHING ABOUT THE CLAIMS MOVED. Every figure still comes from its own
    envelope key through a placeholder (the sentinel fixture proves it), every
    level is skipped when its key is absent rather than rendered as a zero, and
    every ratified string keeps its words -- the cards only lift each figure out
    of the middle of its own sentence, because a 3xl number inside running text
    wrecks that text's line breaking.

    TWO BANDS, ONE BASIS EACH, and this is the constraint the layout may never
    collapse. `LAUNCH_STATS_BAND_CLASS` holds figures counting EVERY bucket and
    EVERY shade; `LAUNCH_CONTRIB_BAND_CLASS` holds the shade-filtered main-pool
    contribution. A card grid asserts that its cells are comparable, so one band
    holding both would claim a comparison between two different bases -- the
    mixed-basis defect ruling U was issued over, expressed by a layout instead of
    by a sentence. The dotted rule and the basis line sit between them, and a
    test asserts the contribution never enters the first band.

    WHAT IS DELIBERATELY NOT A CARD:

    * the CONTRIBUTION HEADLINE ("N identifications not found in the catalogues
      we checked"). It is the page's claim, not a stat, and its figure is already
      the largest thing in the block. Shrinking it into a 140px cell would bury
      the one line that says what this release IS.
    * the SCOPE LINE. It carries two figures in a sentence whose job is to state
      scope in words; split into cells, each number loses the qualifier that stops
      it reading as a corpus denominator -- the coverage overclaim that string was
      rewritten to fix.

    MOBILE IS FLEX, NOT A BREAKPOINT. Every card is `flex: 1 1 140px` inside a
    `flex-wrap` band, so the cards fill whatever line they are on and wrap when
    they cannot: five across on a desktop, two across on a phone, with nothing to
    maintain and no breakpoint to get wrong.

    THE THREE FALLBACKS ARE UNTOUCHED. `render_launch_headline` still routes an
    envelope with no `main_pool_total` to `_render_launch_v1` and one with no
    `identification_total` to `_render_launch_v2`, both byte-identical to what
    shipped. A missing key never becomes a rendered zero.
    """
    fragments = meta.get("corpus_manuscript_count")
    work_total = meta.get("work_total")
    identification_total = meta.get("identification_total")
    split = [
        (value, bucket)
        for value, bucket in ((meta.get("main_pool_total"), main_pool_name),
                              (meta.get("more_pool_total"), ds.bucket_name(False, lang)))
        if value is not None
    ]

    # BAND 1 -- every figure on the unfiltered basis: fragments, works, all-in-all
    # matches, and one card per pool.
    with ui.row().classes(
        f"{LAUNCH_STATS_BAND_CLASS} w-full gap-3 flex-wrap items-stretch"
    ):
        # THE LEDE, at `text-4xl` where the others are `text-3xl`, and a test
        # enforces it: the owner chose the fragment count as the lede on
        # 2026-08-05 from a rendered comparison. "Make all numbers big" is a
        # request about SIZE, not licence to flatten a ratified reading order --
        # so every figure got big and the lede stayed biggest.
        if fragments is not None:
            _stat_card(
                fragments, copy_text("launch_fragments_lede", lang),
                pair_marker=LAUNCH_LEDE_CLASS,
                figure_marker=LAUNCH_FRAGMENTS_CLASS,
                label_marker=LAUNCH_FRAGMENTS_LABEL_CLASS,
                figure_classes="text-4xl font-bold",
                icon="auto_stories",
            )

        # What those fragments were matched TO.
        if work_total is not None:
            _stat_card(
                work_total,
                _sentence_label(copy_text("launch_matched_works", lang), lang),
                pair_marker=LAUNCH_MATCHED_CLASS,
                figure_marker=LAUNCH_WORK_TOTAL_CLASS,
                icon="menu_book",
            )

        # The all-in-all count. The words that framed it become the noun beneath,
        # because the wording is what states its basis ("matches in all") and a
        # card cannot say that for it.
        if identification_total is not None:
            _stat_card(
                identification_total,
                _sentence_label(copy_text("launch_matches_in_all", lang), lang),
                pair_marker=LAUNCH_ALL_TOTAL_CLASS,
                icon="link",
            )

        # ONE CARD PER POOL. This is what the band buys that the stack could not:
        # the two pools sit side by side as equally-weighted cells, which is
        # exactly the "comparable body of work" the split exists to convey. Each
        # names its own bucket from the single definition, and a half whose figure
        # is absent is omitted rather than zeroed.
        #
        # NO NESTED ROW around the pair. It used to carry `flex: 2 1 320px`, which
        # made the two pool cards ONE flex item -- so they wrapped as a block and
        # could not share a line with the other three. On a phone that was a 320px
        # item in a 140px grid. Flat siblings wrap freely.
        for index, (value, bucket) in enumerate(split):
            _stat_card(
                value,
                _sentence_label(copy_text("launch_pool_share", lang), lang,
                                bucket=bucket),
                pair_marker=f"{LAUNCH_SPLIT_CLASS} {LAUNCH_SPLIT_ITEM_CLASS}",
                # `split` is built main-pool-first, so index 0 is the main pool.
                # NEITHER glyph ranks its pool: both are neutral "a body of
                # things" icons. A check/warning pair here would say the second
                # pool is worse, which is the framing the pool vocabulary refuses.
                icon="inventory_2" if index == 0 else "layers",
            )

    # The separator between the two bases. LOGICAL and inline -- no stylesheet
    # rule, no new class to keep in sync, nothing that flips for RTL.
    ui.element("div").classes("w-full").style(
        "border-block-start: 1px dotted var(--border-light); "
        "margin-block-start: 6px; padding-block-start: 6px;"
    )

    # THE CONTRIBUTION CLAIM and its basis: same strings, same classes, same
    # figures. A claim rather than a stat, so it stays a sentence.
    ui.label(
        copy_text("launch_total", lang).format(count=_count(total))
    ).classes(f"{LAUNCH_TOTAL_CLASS} text-xl font-bold")
    ui.label(
        copy_text("launch_basis", lang).format(bucket=main_pool_name)
    ).classes(f"{LAUNCH_BASIS_CLASS} dnote text-xs")

    # BAND 2 -- the three shades that decompose the claim above, one card each:
    # big figure, match-framed label, fragment span as the quiet third line.
    if items:
        with ui.row().classes(
            f"{LAUNCH_CONTRIB_BAND_CLASS} w-full gap-3 flex-wrap items-stretch"
        ):
            for item in items:
                _render_launch_shade(item, lang)

    # The scope line: last, quietest, and prose for the reason given above.
    pages = meta.get("corpus_page_count")
    if pages is not None:
        ui.label(
            copy_text("launch_claim_pages", lang).format(pages=_count(pages))
        ).classes(f"{LAUNCH_CONTEXT_CLASS} dnote text-xs")


#: SHADE -> its decorative glyph. Keyed on the stored shade so a shade added to
#: the frozen ruling-U tuple renders with no icon rather than a wrong one -- the
#: figure and its label are what carry the meaning, and `_stat_card` marks the
#: glyph `aria-hidden`, so a missing one costs a reader nothing.
#:
#: NONE OF THE THREE RANKS ITS SHADE. `new_releases` (nothing recorded),
#: `zoom_in` (finer than the aid) and `inventory_2` (the aid named a container)
#: all describe WHAT the aid said, never how likely our match is to be right --
#: the same discipline the shade WORDING already follows. A star, a tick or a
#: warning triangle here would be a confidence signal in a glyph, which is what
#: D-24 forbids in a colour.
#: `folder_open` and not `inventory_2` for `container_predicts`: the pool card in
#: the band above already uses `inventory_2`, and one glyph appearing on two cards
#: that mean different things is worse than no glyph at all -- a reader who has
#: learned to skim by icon would read the two as related.
_SHADE_ICON: Mapping[str, str] = {
    "fills_gap": "new_releases",
    "refines_granularity": "zoom_in",
    "container_predicts": "folder_open",
}


def _render_launch_shade(item: Mapping[str, Any], lang: str) -> None:
    """One contribution shade, AS A CARD: big count, its match-framed label, and
    the fragments it spans as the card's quiet third line.

    Same string, same figure, same `LAUNCH_SHADE_CLASS`, same
    `launch_shade_label` -- so the stored shade value still never reaches markup
    and the raw-vocabulary guard is unaffected. What changed is the container
    (owner, 2026-08-06: card the rest of the text, make all numbers big).

    The count is `text-3xl` like every other figure in the block. It was
    `font-semibold` body text, which is what made these three read as a wrapping
    sentence rather than as the decomposition of the claim above them.
    """
    label = launch_shade_label(item.get("shade"), lang)
    manuscripts = item.get("manuscript_count")
    _stat_card(
        item.get("identification_count"), label,
        pair_marker=LAUNCH_SHADE_CLASS,
        icon=_SHADE_ICON.get(item.get("shade") or ""),
        note=(copy_text("shade_manuscripts", lang).format(
            count=_count(manuscripts)) if manuscripts is not None else None),
    )


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
    rather than a confirmed find, and the STALENESS WARNING. Both come from data
    or from the shared vocabulary; neither is composed here.

    NO DATE IS RENDERED (owner ruling, 2026-08-06). `as_of` is still ACCEPTED and
    deliberately IGNORED -- see `_COPY` for the full reasoning: the value callers
    pass is `data_as_of`, the BAKE's date, while the novelty check ran against
    this website's own FJMS/NLI/PGP/FGP sidecars, each already older than the bake
    by an interval nobody recorded. There is no single snapshot and no single
    date, so any date here reads as a freshness claim about all of them.

    THE ARGUMENT IS KEPT rather than removed, and that is a judgement worth
    stating. `web/pages/findings.py` passes `discovery_meta("data_as_of")` and the
    masking sweep drives this function with an explicit `as_of`; dropping the
    keyword would break both call sites for no gain, and keeping it means a future
    bake that records a REAL per-source snapshot date has a wired parameter to
    render -- under a new string with its own wording ruling, never the deleted
    one restored.

    THE STALENESS WARNING IS UNCONDITIONAL, which was true before the date went
    and matters more now that it is the whole of what this says about freshness:
    the live catalogues can always hold something newer, whatever the artifact
    recorded about itself.
    """
    lang = _lang_key(lang)
    with ui.column().classes(f"{NOVELTY_HELP_CLASS} dnote text-xs gap-1"):
        ui.label(ds.novelty_strings(lang)["help"])
        ui.label(copy_text("novelty_live_may_differ", lang)).classes(
            f"{NOVELTY_HELP_CLASS}-live")


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
    # C-track: gated on the RENDERED relation, not the stored claim_type. This
    # is the whole reason the surface carries one field rather than two: a row
    # the matrix demoted to `shared_text` (or fail-closed to `uncertain`) must
    # not still advertise "68% of page", which is what gating on the stored
    # value would have let it do.
    relation = item.get("rendered_relation")
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


def _render_shelfmark(item: Mapping[str, Any], lang: str = "en",
                      catalogue_title=None) -> None:
    """The manuscript link -- LIVE, unlike the work title, and pointed at the
    MATCHED FOLIO where the row knows one -- and, beside it, the catalogue's OWN
    title for the same manuscript.

    `/work/{id}` does not exist until Phase 136.1, so work titles render as
    plain text; the manuscript page does exist and a reader needs to reach it
    from the row.

    `catalogue_title`, like `render_finding_row`'s `load_children` and
    `preview_url`, is INJECTED as a callable and not defaulted to a working
    implementation: this module renders and does not read, and a manuscript's
    catalogue title is not on the row projection (`libraries.csv`, not the
    discovery sidecar). `None` renders nothing at all -- neither the label nor
    an empty line -- which matters because ~14% of rows have no CSV title and a
    blank line there would read as "the catalogue has no title", a claim this
    module is not in a position to make.

    THE TITLE ITSELF IS RENDERED VERBATIM, IN ONE LANGUAGE, NEVER TRANSLATED --
    a deliberate departure from this page's usual bilingual discipline, ruled
    on 2026-08-05. This element is a QUOTATION of what the library said about
    the manuscript, and the whole point of showing it here is to let a reader
    weigh that claim against the computed identification beside it -- on
    ~23.6% of rows (`divergence_marker`) the two disagree. A machine
    translation would put words in the cataloguer's mouth at exactly the
    moment the reader is judging those words. Rendering it as written is
    therefore the correct behaviour, not a degradation this module tolerates --
    only the LABEL that introduces it ("Catalogued as:") is this module's own
    text, so only the label is bilingual. `dir="auto"` on the title text
    itself, because a Hebrew title must read correctly on an English render of
    this page and a Latin one on a Hebrew render, and this is the one place on
    the row that deliberately mixes directions.

    Two separate elements, never one concatenated string -- the same reason
    `LAUNCH_LEDE_CLASS` is two elements: a label in the page's language
    followed by a title in the catalogue's own language can reorder
    unpredictably at the boundary if joined into one run of text.
    """
    shelfmark = item.get("shelfmark_display")
    library = item.get("library_code")
    if library:
        ui.label(str(library)).classes("chip")
    if not shelfmark:
        return
    # THE FOLIO, WHEN THE ROW KNOWS ONE (owner report, 2026-08-09). This link
    # opened the manuscript's first page while the preview two lines below it
    # opened the matched folio -- so the SAME row offered two destinations for
    # the same claim and the more prominent one was the less useful.
    #
    # THE UNIT DECIDES, without this function branching on it. `first_match_*`
    # is resolved by the service on the identification LEAF only: a manuscript
    # row spans works and has no single folio to answer for, so the pair is None
    # there and `browse_url` degrades to the manuscript on its own. Branching on
    # `item['unit']` here would be a second place that knows which unit has a
    # folio, and the two would drift.
    #
    # NO `embed`: this is a navigation the reader asked for, so it gets the full
    # page. The preview's iframe is the one that needs the bare, snapshot-safe
    # viewer.
    target = links.browse_url(
        item.get("sys_id"), page=item.get("first_match_page"),
        volume_ie=item.get("first_match_volume_ie"))
    if target:
        ui.link(str(shelfmark), target).classes(ROW_SHELFMARK_CLASS)
    else:
        ui.label(str(shelfmark)).classes(ROW_SHELFMARK_CLASS)

    if catalogue_title is not None:
        title_text = catalogue_title(item)
        if title_text:
            # `font-weight: normal` overrides the `font-bold` the manuscript
            # unit's title row carries; a NON-directional property, so this is
            # not a `margin-left`-class violation of the logical-CSS rule.
            with ui.row().classes(
                f"{ROW_CATALOGUE_TITLE_CLASS} items-center gap-1"
            ).style("font-weight: normal;"):
                ui.label(copy_text("catalogue_title_label", lang)).classes(
                    "dnote text-xs")
                ui.label(str(title_text)).classes("dnote text-xs").props(
                    'dir="auto"').style("unicode-bidi: isolate;")


def divergence_marker(item: Mapping[str, Any], lang: str = "en") -> Optional[Tuple[str, str]]:
    """`(text, tooltip)` for a catalogue-divergent row, or `None`.

    Reads the SERVICE's own `divergent` flag rather than re-deriving the shade
    membership here. That matters on the two grouped units, where
    `novelty_status` is NULL whenever the group mixes shades: a renderer
    deriving the marker from the shade would leave a manuscript row carrying
    one divergent identification looking exactly like an ordinary finding, and
    a work row never marked at all.

    NEUTRAL, and the neutrality is structural: one string, one tooltip, no
    branch on WHICH divergence shade produced it. There is nothing here to
    colour-code by kind and no tier to key a row treatment on (D-24), which is
    the same discipline `render_finding_row` already applies to the bucket.
    """
    if not item.get("divergent"):
        return None
    return ds.divergence_chip(lang), ds.divergence_warning(lang)


#: Where a report goes. The site's own contact address, as `/about` and the
#: homepage already publish it -- not a new channel, because the ruling created
#: no new channel.
REPORT_ADDRESS = "gershuni@gmail.com"


def report_mailto(item: Mapping[str, Any], lang: str = "en",
                  sidecar_version: Any = None) -> Optional[str]:
    """The `mailto:` a reader reports THIS row through, or `None`.

    `None` on any row with no `identification_id`: the per-manuscript and
    per-work units group many identifications into one line, so a report from
    one of those rows could not name what it was about, and a report that
    cannot be traced back to a row is a report the next bake cannot act on.
    Those units are not reportable rather than reportable-and-useless.

    THE VERSION COMES FROM THE ENVELOPE the page already read, never from a
    second source: a report has to be reproducible against the exact artifact
    that produced the row, and an artifact swap between the read and the render
    would make a separately-fetched version name the wrong one. Omitted (and
    the affordance withheld) when the envelope did not supply one -- a report
    naming no version is not reproducible, which is the whole point of
    prefilling it.

    Every value is URL-QUOTED. An identifier is a sha256 digest today, but a
    field that reaches a URL unquoted is one data change away from breaking the
    link or smuggling a header into it.
    """
    identification = item.get("identification_id")
    if not identification or not sidecar_version:
        return None
    subject = quote(copy_text("report_subject", lang))
    body = quote(copy_text("report_body", lang).format(
        identification=identification, version=sidecar_version))
    return f"mailto:{REPORT_ADDRESS}?subject={subject}&body={body}"


def _render_row_meta(item: Mapping[str, Any], lang: str, unit: str,
                     sidecar_version: Any = None, on_suppress=None) -> None:
    """The meta line: relation chip, novelty chip, page count, coverage, bucket.

    NO band tooltip. The identification grain exposes `best_band_rank` and no
    band label, evidence source or confidence band, so there is nothing here to
    put on the chip's `title` -- and deriving a label from a rank would be a
    second band vocabulary, which is precisely what the panel renderer refuses
    to do for the same reason. Recorded as a deviation in this plan's summary
    rather than papered over with an invented tooltip.
    """
    with ui.row().classes(f"{ROW_META_CLASS} side items-center gap-2 flex-wrap"):
        # C-track: Contract 1's matrix output, not the stored claim_type. The
        # two grouped units carry NULL here and therefore render no chip, which
        # is the rule matrix-spec 3.1 pins: a row aggregating DIFFERENT
        # identifications asserts no single relation.
        relation = item.get("rendered_relation")
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

        # Ruling F's row marker. A PLAIN `.chip` -- the same neutral treatment
        # the relation vocabulary and the manuscript pane already use -- rather
        # than a new visual language: these rows are reached only by a reader
        # who deliberately opened the axis, and a louder styling would be the
        # page taking the side ruling F says nobody has taken. The full
        # two-sentence statement rides on the chip's own tooltip.
        divergence = divergence_marker(item, lang)
        if divergence is not None:
            text, tooltip = divergence
            ui.label(text).classes(f"chip {ROW_DIVERGENCE_CLASS}").tooltip(tooltip)

        pages = _plural("pages", item.get("page_count"), lang)
        if pages:
            ui.label(pages).classes(f"{ROW_PAGES_CLASS} dnote text-xs")

        coverage = coverage_clause(item, lang)
        if coverage:
            ui.label(coverage).classes(f"{ROW_COVERAGE_CLASS} dnote text-xs")

        # NO BUCKET NAME ON THE ROW (owner ruling, 2026-08-06), and the reason is
        # that it could not vary. The page offers exactly two buckets and no
        # union between them (`_OFFERED_BUCKETS` deliberately excludes the
        # all-bucket sentinel, ruling U constraint 1), and an expansion's
        # children inherit their parent's bucket -- so this label was measured
        # to be CONSTANT across every row of all six unit x bucket combinations.
        # It repeated, up to fifty times a page, exactly what the result bar
        # states once directly above the rows.
        #
        # It was also the only item on a meta line of ~5.3 that told the reader
        # nothing about the row it sat on.
        #
        # D-24 IS SATISFIED MORE CLEANLY, not bypassed: removing it from BOTH
        # pools leaves the two anatomies identical, which is what D-24 asks for.
        # Removing it from one would be the demotion D-24 forbids -- so if this
        # ever comes back, it comes back for both or not at all. The bucket name
        # itself keeps its single definition
        # (`shared.discovery_main_pool.bucket_label`, via `ds.bucket_name`),
        # which the result bar and the launch statistics still read.
        if unit == FINDINGS_UNIT_MANUSCRIPT and item.get("multi_work_annotation"):
            ui.label(copy_text("multi_work", lang)).classes(
                f"{ROW_ANNOTATION_CLASS} dnote text-xs")

        # ON THE ROW, at the end of the meta line. A reader reporting a problem
        # is looking at the row that has it, and a page-level affordance could
        # not prefill WHICH row -- which is the one thing that makes a report
        # usable by the next bake. It is the smallest element on the line and
        # the last, so it reads as an aside rather than as an action the page
        # is asking for.
        target = report_mailto(item, lang, sidecar_version)
        if target:
            ui.link(copy_text("report_link", lang), target).classes(
                f"{ROW_REPORT_CLASS} dnote text-xs")

        # THE ADMIN ✕. INJECTED, like every other affordance on this row: this
        # module renders and does not read, so it cannot reach Supabase and does
        # not know what an admin is. `None` -- the case for every ordinary reader
        # -- renders NOTHING, not a disabled button.
        #
        # ON THE IDENTIFICATION LEAF ONLY, gated on `identification_id` exactly as
        # the report link is: the grouped units aggregate many identifications into
        # one line, so there is no single id to hide and a ✕ there would either do
        # nothing or hide an arbitrary one of them.
        identification = item.get("identification_id")
        if on_suppress is not None and identification:
            async def _hide(_event=None, _id=str(identification)) -> None:
                await on_suppress(_id)

            hide = ui.button(icon="close", on_click=_hide).props(
                "flat round dense size=xs")
            hide.classes(f"{ROW_SUPPRESS_CLASS} dnote")
            # An icon-only control needs a name a screen reader can read, and
            # `tooltip` alone is not one.
            hide.props(f'aria-label="{copy_text("suppress_row", lang)}"')
            hide.tooltip(copy_text("suppress_row", lang))


#: UNIT -> the row field that identifies the group, and the filter axis the
#: expansion pins it to. `None` on the leaf unit: an identification IS the leaf,
#: so it has nothing underneath it to open.
#:
#: These pairs are the SAME pairs `shared/discovery_service.py::
#: _FINDINGS_UNIT_GROUP_BY` groups by (`di.sys_id`, `di.display_work_id`), and
#: that is the whole reason an expansion can be honest: the children come from
#: the SHIPPED findings read at the leaf grain with this key pinned, so the
#: reader's every active filter still applies and a parent's count cannot
#: contradict the rows underneath it. Fetching them from a differently-filtered
#: read -- `get_manuscript_works_enveloped` takes no bucket, novelty or
#: divergence filter at all -- would show a reader children their own parent
#: says do not exist.
#: MANUSCRIPT WAS DELIBERATELY ABSENT UNTIL 2026-08-07, and the reason it is now
#: present is that the missing half was supplied rather than that the caution was
#: wrong. `_build_findings_filter` had NO `sys_id` axis, so a manuscript expansion
#: would have passed a keyword the read silently IGNORES -- opening the row onto
#: every identification matching the reader's filters instead of the ones in THAT
#: manuscript. Measured, not feared: the argument reached the service and was
#: dropped without an error. That is worse than a crash, because the reader sees a
#: plausible list and cannot tell it is the wrong one.
#:
#: What that safety cost was the whole unit: a reader grouping by manuscript saw a
#: shelfmark and a work count with no way through to the identifications underneath
#: (owner report, 2026-08-07: "In One Row Per Manuscript I don't see the computed
#: identifications at all"). So `di.sys_id = ?` now exists in the shared predicate,
#: and the pair below is admitted BY THE SAME DERIVED CHECK that previously refused
#: it -- `EXPANSION_SUPPORTED_AXES` reads the builder's signature, so this table
#: cannot claim an axis the query lacks.
EXPANSION_KEY_BY_UNIT: Dict[str, Optional[Tuple[str, str]]] = {
    FINDINGS_UNIT_IDENTIFICATION: None,
    FINDINGS_UNIT_MANUSCRIPT: ("sys_id", "sys_id"),
    FINDINGS_UNIT_WORK: ("display_work_id", "work_id"),
}


#: The filter axes an expansion may legitimately pin, READ FROM the shipped
#: predicate builder's own signature at import.
#:
#: Derived rather than listed because the failure mode is silence, not an error.
#: `get_findings_enveloped` takes its filters as keywords, so an axis
#: `_build_findings_filter` does not implement is accepted and IGNORED -- the
#: expansion then returns every row matching the reader's filters instead of the
#: ones under the parent that was clicked. Measured on a manuscript row before
#: this existed: `sys_id` reached the service and vanished. A hand-written list
#: here would be one edit away from claiming an axis the predicate lacks; a
#: signature read cannot be.
EXPANSION_SUPPORTED_AXES: frozenset = frozenset(
    _inspect.signature(_build_findings_filter).parameters) - {"unit", "bucket"}


def expansion_target(item: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    """`(filter_axis, value)` the children of `item` are fetched with, or `None`.

    `None` for the leaf unit (nothing to open) and for a grouped row whose own
    key is missing -- an expansion that cannot name what it is expanding would
    fetch the whole unfiltered leaf grain, which is not this row's children but
    every row on the page.
    """
    pair = EXPANSION_KEY_BY_UNIT.get(item.get("unit") or FINDINGS_UNIT_IDENTIFICATION)
    if pair is None:
        return None
    field, axis = pair
    value = item.get(field)
    return (axis, str(value)) if value else None


def _render_author(item: Mapping[str, Any]) -> None:
    """The work's author, on a line of its OWN, or nothing at all.

    OURS, from the sidecar projection -- not the catalogue's attribution. The
    separate line is the whole point; see `ROW_AUTHOR_CLASS` for why sharing a row
    with the catalogue quotation was a correctness defect rather than a cramped
    layout.

    NOT bilingual, and not translated: an author name is a name. `dir="auto"` for
    the same reason `_render_shelfmark` puts it on the catalogue title -- a Hebrew
    name must read correctly on an English render of this page and a Latin one on a
    Hebrew render.

    Absent author renders NOTHING -- no label, no empty line. A blank line where an
    attribution belongs reads as "this work has no known author", which is a claim
    about the record that a missing projection field does not support.

    NO EARLY `return` for that absent case. The masking sweep's line-granular gate
    reports a bare `return` as a line no capture paints -- correctly, since every
    fixture carries an author -- and that gate has already found three dead branches
    in this phase's work. Written as a positive `if`, every line here is both
    reachable and painted.
    """
    author = item.get("author")
    if author:
        ui.label(str(author)).classes(
            f"{ROW_AUTHOR_CLASS} {ROW_SUB_CLASS} r-sub text-xs"
        ).props('dir="auto"').style("unicode-bidi: isolate;")


def render_finding_row(item: Mapping[str, Any], lang: str = "en",
                       sidecar_version: Any = None,
                       load_children=None, preview_url=None,
                       catalogue_title=None, on_suppress=None,
                       load_excerpt=None) -> None:
    """One result row, in whichever unit the service produced it.

    The unit arrives ON the row (`unit`, part of the projection); it is
    branched on and never rendered. The three units differ only in what
    identifies the row and what the sub-line counts -- the meta line, the
    bucket treatment and the title routing are identical, which is what makes
    "a second-bucket row looks like a main-pool row" a property of the code
    rather than of a reviewer's care.

    `load_children` (grouped units) and `preview_url` (the leaf) are INJECTED,
    and neither is defaulted to a working implementation. This module renders;
    it does not read, and it does not know what a URL to a manuscript looks
    like. A component that reached for the service itself could not be swept by
    a masking capture that drives it directly, which is how this suite scans it.

    `catalogue_title` is the same shape of injection, for the same reason: see
    `_render_shelfmark`. Passed to both manuscript-identity call sites (the
    per-manuscript unit and the identification leaf); the work unit never calls
    `_render_shelfmark` at all, because a work spans manuscripts and has no
    single one to title.
    """
    lang = _lang_key(lang)
    unit = item.get("unit") or FINDINGS_UNIT_IDENTIFICATION

    # A HAIRLINE BETWEEN ROWS (owner report, 2026-08-06: "list items blend
    # together"). Applied to EVERY row identically, in both pools and at both
    # levels -- which is what keeps it clear of D-24: a separator is not a row
    # TREATMENT, because it carries no information about the row it sits under
    # and cannot be read as a verdict on it. The moment it varied by pool, novelty
    # or band it would become exactly the styling D-24 prohibits.
    #
    # Inline and side-neutral: `border-block-end` is the block axis, so it needs
    # no RTL mirror, and this module adds no stylesheet rule (a test asserts
    # that). `--border-light` is the same token the chips and cards already use,
    # so this is the existing hairline weight rather than a new one.
    #
    # `p-2` keeps its padding and `gap-1` its internal spacing; only the bottom
    # edge is new, so no row grows or moves.
    with ui.column().classes(f"row {ROW_CLASS} w-full gap-1 p-2").style(
        "border-block-end: 1px solid var(--border-light);"
    ):
        if unit == FINDINGS_UNIT_MANUSCRIPT:
            with ui.row().classes(f"{ROW_TITLE_CLASS} items-center gap-2 font-bold"):
                _render_shelfmark(item, lang, catalogue_title=catalogue_title)
            works = _plural("works", item.get("work_count"), lang)
            if works:
                ui.label(works).classes(f"{ROW_SUB_CLASS} r-sub text-xs")
        elif unit == FINDINGS_UNIT_WORK:
            ui.label(_work_title(item, lang)).classes(f"{ROW_TITLE_CLASS} font-bold")
            _render_author(item)
            manuscripts = _plural("manuscripts", item.get("manuscript_count"), lang)
            if manuscripts:
                ui.label(manuscripts).classes(f"{ROW_SUB_CLASS} r-sub text-xs")
        else:
            ui.label(_work_title(item, lang)).classes(f"{ROW_TITLE_CLASS} font-bold")
            # THE AUTHOR FIRST, ON ITS OWN LINE, then the shelfmark line that
            # carries the catalogue quotation. Order is the fix: see
            # `ROW_AUTHOR_CLASS`. Sharing the row with "Catalogued as: <title>"
            # made our attribution read as the library's.
            _render_author(item)
            with ui.row().classes(f"{ROW_SUB_CLASS} r-sub items-center gap-2 text-xs"):
                _render_shelfmark(item, lang, catalogue_title=catalogue_title)

        _render_row_meta(item, lang, unit, sidecar_version=sidecar_version,
                         on_suppress=on_suppress)

        # THE AFFORDANCES, each on the unit where it has a meaning. A
        # grouped row opens onto its children; the leaf previews its manuscript
        # and compares its texts. None is on both: a work row spanning
        # manuscripts has no single page to preview and no single pair of
        # texts to compare, and an identification has nothing underneath it
        # to open.
        if load_children is not None and expansion_target(item) is not None:
            _render_expansion(item, lang, load_children,
                              sidecar_version=sidecar_version,
                              preview_url=preview_url,
                              catalogue_title=catalogue_title,
                              on_suppress=on_suppress,
                              load_excerpt=load_excerpt)
        elif unit == FINDINGS_UNIT_IDENTIFICATION:
            if load_excerpt is not None:
                _render_excerpt(item, lang, load_excerpt)
            if preview_url is not None:
                _render_preview(item, lang, preview_url)


def _render_expansion(item: Mapping[str, Any], lang: str, load_children,
                      sidecar_version: Any = None, preview_url=None,
                      catalogue_title=None, on_suppress=None,
                      load_excerpt=None) -> None:
    """The grouped row's children, IN PLACE, fetched when the reader asks.

    Lazy for the reason the panel's expansion is: the heaviest work carries
    hundreds of identifications while most rows carry a handful, so opening
    every row's children with the page pays the worst case to serve the common
    one -- on a single-uvicorn-worker server, once per row on the page.

    A FAILED read renders a NAMED failure with a retry, never an empty body.
    The panel's own expansion returns silently on an exception, which leaves a
    reader looking at an opened, empty expander -- indistinguishable from "this
    row has no matches underneath it", and one of those is an outage.
    """
    #: THE TOGGLE IS CREATED FIRST, AND THAT ORDER IS THE WHOLE FIX (owner
    #: report, 2026-08-06: "expanding requires excessive downward scrolling to
    #: collapse the list").
    #:
    #: The button already retitles itself to "Hide the individual matches" the
    #: moment it opens -- that was never the defect. The defect was DOM ORDER:
    #: `body` was created before `button`, so the only control that could close
    #: the group rendered BELOW every child it had just revealed. On the heaviest
    #: work in the served artifact that is 25 rows away on first open and 2,981
    #: if the reader keeps loading, so the affordance to undo the click was
    #: reliably off-screen at the moment it was wanted.
    #:
    #: Nothing else about the expansion changed. The two elements are siblings in
    #: one column and neither carries a position rule, so swapping their creation
    #: order is the entire change -- no CSS, no wrapper, no second control.
    button = ui.button(copy_text("expand_open", lang)).props(
        "flat dense size=sm no-caps").classes(f"{ROW_EXPANDER_CLASS} dnote")
    #: STICKY while the group is open, so the close control stays reachable from
    #: anywhere inside a 2,981-child expansion instead of only from its top.
    #:
    #: `top: 64px` AND NOT `top: 0`, and the offset is load-bearing rather than a
    #: taste choice. `web/main.py` renders the site chrome as
    #: `ui.header(...).props('reveal').style('height: 64px')`, and a Quasar header
    #: is `position: fixed` -- so it is OUTSIDE this element's scroll flow and a
    #: sticky pinned at `top: 0` parks itself UNDERNEATH it. The control would
    #: then be "always on screen" and invisible, which is worse than the
    #: scroll-back-up it replaces, because a reader would have no reason to look
    #: for it. 64px is the header's own declared height, so the button lands
    #: exactly below the chrome when the `reveal` header is showing and 64px down
    #: from the top when it has slid away -- visible in both states, which is the
    #: property that matters. (The number is a LAYOUT constant, not a discovery
    #: figure; ruling U's no-literals rule is about artifact-derived counts.)
    #:
    #: Applied INLINE and only here: this module adds no stylesheet rule (a test
    #: asserts it injects no CSS), and every property is side-neutral --
    #: `position`, `top` and `z-index` are block-axis or non-directional, so
    #: nothing needs an RTL mirror. `top` rather than `inset-block-start` is
    #: deliberate: this page is `horizontal-tb` in Hebrew too, so the two resolve
    #: identically, and the guard that forbids physical properties in this file
    #: targets the INLINE-axis ones (`margin/padding/border-left|right`,
    #: `text-align`) because those are the ones that break mirroring.
    #:
    #: `background` is required, not decoration: a transparent sticky element
    #: lets the scrolling children show through it and the label becomes
    #: unreadable over its own list.
    button.style(
        "position: sticky; top: 64px; z-index: 2; "
        "background: var(--bg-primary); align-self: flex-start;"
    )
    body = ui.column().classes(f"{ROW_CHILDREN_CLASS} w-full gap-1")
    body.style("display: none;")
    #: `page` is the child list's OWN page, independent of the reader's page
    #: through the parent list. `shown` accumulates across pages so the extent
    #: line can say how much of the group is on screen after "show more".
    state: Dict[str, Any] = {"open": False, "loaded": False, "page": 1,
                             "shown": 0}
    #: Holds the CURRENT extent line so a later page can replace it rather than
    #: add a second one -- two extent lines would be two different claims about
    #: the same group. Declared before the closures that rebind it.
    extent: Dict[str, Any] = {"holder": None}

    async def _load(_event=None, *, append: bool = False) -> None:
        """One page of children. `append` keeps what is already on screen.

        `_event` is accepted and ignored so this can be bound directly to the
        retry button: NiceGUI's `on_click=` passes the click arguments
        POSITIONALLY, and a keyword-only signature would raise `TypeError` the
        first time a reader pressed Retry -- a retry affordance that cannot
        retry, on the failure path, which is where it is the only way forward.

        The heaviest work in the served artifact carries 2,981 identifications
        against a 25-row child page, so WITHOUT a route to the rest, "Showing 25
        of 2,981" is a dead end that names its own incompleteness -- which is
        worse than a bounded list, because the reader can see what they are
        being denied and cannot act on it.
        """
        if not append:
            # `body.clear()` DESTROYS the extent element, so the handle kept for
            # replacing it must be FORGOTTEN here -- `.delete()` on an
            # already-removed element raises `ValueError` out of NiceGUI's own
            # child list. Found by a control that made show-more reload with
            # `append=False`; I could not then construct a shipped sequence that
            # reaches it, so this is invariant maintenance (clearing a container
            # invalidates handles into it) rather than a fix for a reachable bug.
            # Recorded that way because the two are different claims.
            body.clear()
            extent["holder"] = None
            state["shown"] = 0
        try:
            envelope = await load_children(item, state["page"])
        except Exception:
            # The value is never echoed -- an artifact-derived id in a reader's
            # error line is the D-25 egress class error paths are scanned for.
            envelope = None
        with body:
            if not envelope or (envelope or {}).get("status") != "ok":
                # NAMED, and retryable. `state['loaded']` stays False so the
                # next open tries again rather than re-rendering the failure.
                with ui.row().classes(
                        f"{ROW_CHILDREN_STATE_CLASS} items-center gap-2"):
                    ui.label(copy_text("expand_failed", lang)).classes(
                        "dnote text-xs")
                    # The SHARED retry label, not a fourth copy of the word --
                    # `shared/discovery_display_strings.py` owns it and the
                    # launch outage and the panel both take it from there.
                    ui.button(ds.retry_label(lang), on_click=_load).props(
                        "flat dense size=sm no-caps")
                return
            state["loaded"] = True
            children = list((envelope or {}).get("items") or ())
            # `make_envelope` guarantees an int here (it coerces and raises), so
            # the default is for a hand-built mapping in a probe, never for a
            # real read.
            total = int((envelope or {}).get("total") or 0)
            state["shown"] = state["shown"] + len(children)
            if extent["holder"] is not None:
                # The previous page's extent line and its "show more" are
                # replaced rather than accumulated -- two of them would be two
                # different claims about the same group.
                extent["holder"].delete()
                extent["holder"] = None
            for child in children:
                with ui.column().classes(f"{ROW_CHILD_CLASS} w-full"):
                    # The child is a LEAF row, rendered by this same renderer --
                    # so it carries its own report affordance and its own
                    # preview, and it cannot drift from a top-level row's
                    # anatomy. It is given NO `load_children`: a leaf has
                    # nothing under it, and passing one would build a tree.
                    # The child is a LEAF, so it gets the ✕ too: a wrong row
                    # inside an expanded work is exactly as visible as one at the
                    # top level, and the owner should not have to change the row
                    # unit to hide it.
                    # `catalogue_title` IS PASSED DOWN (owner report, 2026-08-07:
                    # the "Catalogued as:" line was missing from a work row's
                    # children). It was the one injected affordance the child did
                    # not receive, so every child rendered with `None` -- which
                    # means "render nothing", so the absence was silent. That is
                    # the WORST place to lose it: a work row's children are its
                    # witnesses, and comparing our identification against what
                    # each library called the same manuscript is precisely the
                    # reading the expansion exists to support.
                    render_finding_row(child, lang,
                                       sidecar_version=sidecar_version,
                                       preview_url=preview_url,
                                       catalogue_title=catalogue_title,
                                       on_suppress=on_suppress,
                                       load_excerpt=load_excerpt)
            extent["holder"] = _render_expansion_extent(
                state["shown"], total, lang, on_more=_more)

    async def _more(_event=None) -> None:
        """The next page of children, appended in place.

        Appended rather than paged-in-place because a reader who opened a work to
        read its witnesses is building up a view of the group; replacing the list
        under them would lose the one they were looking at.

        `_event` is accepted and ignored: NiceGUI's `on_click=` passes the click
        arguments positionally, so a zero-argument handler raises `TypeError` on
        the FIRST press -- i.e. the button would have looked right and done
        nothing. `_toggle` below is bound with `.on("click", ...)`, which does
        not, and that asymmetry is exactly the kind a reviewer's eye slides over.
        """
        state["page"] = int(state["page"]) + 1
        await _load(append=True)

    async def _toggle() -> None:
        state["open"] = not state["open"]
        body.style("display: flex;" if state["open"] else "display: none;")
        button.text = copy_text(
            "expand_close" if state["open"] else "expand_open", lang)
        if state["open"] and not state["loaded"]:
            await _load()

    button.on("click", _toggle)


def _render_expansion_extent(shown: int, total: int, lang: str,
                             on_more=None) -> Any:
    """"Showing N of M", with a route to the rest -- and ONLY when the two differ.

    Returns the element holding the line (so a later page can replace it), or
    `None` when nothing was rendered.

    A bounded page rendered with no extent line reads as the whole group, which
    is a number the reader will believe. Written from the envelope's own `total`
    (the count query's exact result) and never from `len(items)`, which cannot
    know what it was a page OF.

    `total` IS AN INT, guaranteed by the producer rather than re-checked here.
    An earlier revision wrapped this in `try: int(total) except: return`, and the
    masking sweep's line-granular gate showed that handler was never executed --
    correctly, because `shared/discovery_surface_projection.py::make_envelope`
    coerces `total` with `int(total)` and RAISES on anything it cannot, so a
    non-numeric total cannot reach a renderer through an envelope at all. A
    defensive branch against an impossible input is not free: it is a line no
    scan can look at and no test can reach, and it invites the reader of this
    function to believe the guarantee is weaker than it is.
    """
    if total <= shown:
        return None
    holder = ui.row().classes(
        f"{ROW_CHILDREN_STATE_CLASS} items-center gap-2 flex-wrap")
    with holder:
        ui.label(copy_text("expand_partial", lang).format(
            shown=_count(shown), count=_count(total))).classes("dnote text-xs")
        # THE ROUTE TO THE REST. Without it the line names its own
        # incompleteness and offers nothing -- and on the heaviest work in the
        # served artifact that is 25 rows shown out of 2,981.
        if on_more is not None:
            ui.button(copy_text("expand_more", lang), on_click=on_more).props(
                "flat dense size=sm no-caps").classes(
                f"{ROW_CHILDREN_STATE_CLASS}-more")
    return holder


def _render_preview(item: Mapping[str, Any], lang: str, preview_url) -> None:
    """The identification leaf's preview: the manuscript page, in place.

    ON THE LEAF ONLY. A work row spans manuscripts and a manuscript row spans
    works, so a preview on either would have to CHOOSE which page to show, and
    choosing between a row's candidates is adjudication -- the one thing no
    surface in this phase does.

    Rendered into an `iframe` pointed at the EXISTING bare browse viewer
    (`/browse?…&embed=1`), which already disables snapshot restore and persist,
    so previewing a manuscript here cannot overwrite the reader's own browse
    position. That property is why the bare viewer is reused rather than a new
    read being written against the same data.

    THE NOTE IS CHOSEN FROM THE ROW, NOT FROM THE URL. `first_match_page` is the
    row's own record of whether the service resolved a matched folio, and it is
    the SAME value `preview_url` requires before it targets one -- so the note
    and the link cannot disagree. Parsing the URL to find out would be a second
    derivation of the same fact, and a second derivation is how the note starts
    promising a folio the link is not opening.
    """
    try:
        url = preview_url(item)
    except Exception:
        url = None
    if not url:
        return
    body = ui.column().classes(f"{ROW_PREVIEW_CLASS} w-full gap-1")
    body.style("display: none;")
    state = {"open": False, "loaded": False}
    button = ui.button(copy_text("preview_open", lang)).props(
        "flat dense size=sm no-caps").classes(f"{ROW_PREVIEW_CLASS}-toggle dnote")

    def _toggle() -> None:
        state["open"] = not state["open"]
        body.style("display: flex;" if state["open"] else "display: none;")
        button.text = copy_text(
            "preview_close" if state["open"] else "preview_open", lang)
        if state["open"] and not state["loaded"]:
            state["loaded"] = True
            with body:
                note_key = ("preview_note" if preview_targets_a_folio(item)
                            else "preview_note_manuscript")
                ui.label(copy_text(note_key, lang)).classes(
                    "dnote text-xs")
                # The iframe is created on FIRST OPEN, not with the row: a page
                # of 50 rows would otherwise issue 50 manuscript loads nobody
                # asked for, against the image services the browse page fetches
                # from.
                ui.element("iframe").props(
                    f'src="{url}" loading="lazy" '
                    'referrerpolicy="no-referrer"').classes(
                    f"{ROW_PREVIEW_CLASS}-frame w-full").style(
                    "border: 0; min-height: 60vh;")

    button.on("click", _toggle)


def _compose_excerpt_piece(text: str, intervals, ja_braces: bool,
                           *, whole_span: bool = False) -> str:
    """Escaped markup for one excerpt piece, by per-character flags.

    Two independent flags -- `hl` (a matched word) and `heb` (inside a JA
    {...} Hebrew-word mark) -- are painted onto a character array and then
    chunked into runs, the way the research decks render mixed states: runs
    can overlap freely and the output can never produce mis-nested tags.
    Brace characters themselves are DROPPED (the owner's ruling: color the
    content, remove the marks); an orphan brace -- a pair split across the
    piece boundary by the offset slice -- is dropped without coloring, so a
    truncation artifact never paints a claim. Newlines pass through untouched
    (the pane is `white-space: pre-wrap`), which is what keeps the manuscript
    lineation visible.
    """
    n = len(text)
    hl = [whole_span] * n
    for pair in intervals or ():
        try:
            start, end = int(pair[0]), int(pair[1])
        except (TypeError, ValueError, IndexError):
            continue
        for k in range(max(0, start), min(n, end)):
            hl[k] = True
    heb = [False] * n
    drop = [False] * n
    if ja_braces:
        open_at: Optional[int] = None
        for i, ch in enumerate(text):
            if ch == "{":
                drop[i] = True
                open_at = i
            elif ch == "}":
                drop[i] = True
                if open_at is not None:
                    for k in range(open_at + 1, i):
                        heb[k] = True
                    open_at = None
    parts = []
    i = 0
    while i < n:
        if drop[i]:
            i += 1
            continue
        j = i
        while j < n and not drop[j] and hl[j] == hl[i] and heb[j] == heb[i]:
            j += 1
        segment = _html.escape(text[i:j])
        classes = []
        if hl[i]:
            classes.append(f"{ROW_EXCERPT_CLASS}-hl")
        if heb[i]:
            classes.append(f"{ROW_EXCERPT_CLASS}-heb")
        if classes:
            parts.append(f'<span class="{" ".join(classes)}">{segment}</span>')
        else:
            parts.append(segment)
        i = j
    return "".join(parts)


def _render_excerpt(item: Mapping[str, Any], lang: str, load_excerpt) -> None:
    """The identification leaf's text-vs-text disclosure (excerpt-v1).

    ON THE LEAF ONLY -- same reasoning as the preview: a grouped row spans
    identifications, and choosing which one's texts to show is adjudication.

    The excerpt is fetched on FIRST OPEN (a page of 50 rows must not issue 50
    sidecar reads nobody asked for), through the INJECTED `load_excerpt` --
    this module renders; it does not read. Failure is NAMED and retryable
    (the preview's silent no-render is a pattern the 2026-08-13 pre-flight
    flagged, not one to copy). An ok-empty envelope is the honest "no
    excerpts for this identification", which is a different sentence from an
    outage and gets different copy.

    The six pieces arrive as PLAIN TEXT from the sidecar and are escaped here
    before entering markup; the highlight is ONE class, identical for every
    row, relation kind, band and novelty status -- D-24 leaves no room for a
    per-anything variant of it.
    """
    strings = ds.excerpt_strings(lang)
    body = ui.column().classes(f"{ROW_EXCERPT_CLASS} w-full gap-1")
    body.style("display: none;")
    state = {"open": False, "loaded": False}
    button = ui.button(strings["toggle"]).props(
        "flat dense size=sm no-caps").classes(
        f"{ROW_EXCERPT_CLASS}-toggle dnote")

    def _piece_markup(row: Mapping[str, Any], side: str) -> str:
        """One side's {before, span, after}, composed as markup.

        The span piece highlights the MATCHED WORDS when the bake carried
        word-level alignment intervals (`frag_hl`/`work_hl`, char offsets
        into the span piece) -- fuzzy on the bake side, so an HTR miscopy
        still pairs with its edition word. `None` intervals (no work side to
        be parallel to, or a pre-round-2 asset) fall back to highlighting the
        whole span, which is the claim the offsets themselves make.

        `work_markup == 'ja_braces'` turns the J-corpus {...} Hebrew-word
        convention into a colored span WITH THE BRACES REMOVED, on the work
        side only -- keyed on the baked flag, never sniffed from the text, so
        a literal brace in any other corpus stays a literal brace.
        """
        ja = side == "work" and row.get("work_markup") == "ja_braces"
        hl = row.get(f"{side}_hl")
        before = _compose_excerpt_piece(row.get(f"{side}_before") or "",
                                        None, ja)
        after = _compose_excerpt_piece(row.get(f"{side}_after") or "",
                                       None, ja)
        span = _compose_excerpt_piece(row.get(f"{side}_span") or "",
                                      hl, ja, whole_span=hl is None)
        return (f'<span class="{ROW_EXCERPT_CLASS}-ctx">{before}</span>'
                f'{span}'
                f'<span class="{ROW_EXCERPT_CLASS}-ctx">{after}</span>')

    def _render_panes(row: Mapping[str, Any]) -> None:
        with ui.element("div").classes(f"{ROW_EXCERPT_CLASS}-panes w-full"):
            with ui.element("div").classes(f"{ROW_EXCERPT_CLASS}-pane"):
                label = strings["frag_label"]
                # 'htr' is the automated layer; FGP/PGP transcriptions are
                # human work and carry no qualifier.
                if (row.get("text_layer") or "") == "htr":
                    label = f"{label} ({strings['frag_htr_note']})"
                ui.label(label).classes("dnote text-xs")
                ui.html(f'<p class="{ROW_EXCERPT_CLASS}-text" dir="rtl">'
                        f'{_piece_markup(row, "frag")}</p>')
            with ui.element("div").classes(f"{ROW_EXCERPT_CLASS}-pane"):
                ui.label(strings["work_label"]).classes("dnote text-xs")
                if row.get("work_span"):
                    ui.html(f'<p class="{ROW_EXCERPT_CLASS}-text" dir="rtl">'
                            f'{_piece_markup(row, "work")}</p>')
                    if row.get("work_source") == "reprojected":
                        ui.label(strings["reprojected_note"]).classes(
                            "dnote text-xs")
                    attribution = row.get("attribution")
                    if attribution:
                        ui.label(str(attribution)).classes(
                            f"{ROW_EXCERPT_CLASS}-attr dnote text-xs")
                else:
                    # An availability fact, not an outage -- and it never
                    # says WHY the edition is undisplayable.
                    ui.label(strings["work_unavailable"]).classes(
                        f"{ROW_EXCERPT_CLASS}-state dnote text-xs")
        n_spans = row.get("n_spans")
        if isinstance(n_spans, int) and n_spans > 1:
            ui.label(strings["multi_span"].format(
                count=_count(n_spans))).classes("dnote text-xs")

    async def _load(_event=None) -> None:
        """`_event` accepted and ignored so the retry button can bind this
        directly (NiceGUI passes click args positionally)."""
        body.clear()
        try:
            envelope = await load_excerpt(item)
        except Exception:
            # Never echo identifiers into a reader-visible error line (D-25).
            envelope = None
        with body:
            if not envelope or (envelope or {}).get("status") != "ok":
                # NAMED, and retryable. `state['loaded']` stays False so the
                # next open tries again instead of re-rendering the failure.
                with ui.row().classes(
                        f"{ROW_EXCERPT_CLASS}-state items-center gap-2"):
                    ui.label(strings["failed"]).classes("dnote text-xs")
                    ui.button(ds.retry_label(lang), on_click=_load).props(
                        "flat dense size=sm no-caps")
                return
            state["loaded"] = True
            rows_ = list((envelope or {}).get("items") or ())
            if not rows_:
                ui.label(strings["none"]).classes(
                    f"{ROW_EXCERPT_CLASS}-state dnote text-xs")
                return
            _render_panes(rows_[0])

    async def _toggle(_event=None) -> None:
        state["open"] = not state["open"]
        body.style("display: flex;" if state["open"] else "display: none;")
        if state["open"] and not state["loaded"]:
            await _load()

    button.on("click", _toggle)


__all__ = [
    "LAUNCH_ALL_TOTAL_CLASS",
    "LAUNCH_BASIS_CLASS",
    "LAUNCH_CLASS",
    "LAUNCH_CONTEXT_CLASS",
    "LAUNCH_FRAGMENTS_CLASS",
    "LAUNCH_FRAGMENTS_LABEL_CLASS",
    "LAUNCH_LEDE_CLASS",
    "LAUNCH_MATCHED_CLASS",
    "LAUNCH_POOL_LABEL_CLASS",
    "LAUNCH_POOL_TOTAL_CLASS",
    "LAUNCH_SHADE_CLASS",
    "LAUNCH_SPLIT_CLASS",
    "LAUNCH_SPLIT_ITEM_CLASS",
    "LAUNCH_WORK_TOTAL_CLASS",
    "LAUNCH_STATE_CLASS",
    "LAUNCH_TOTAL_CLASS",
    "NOVELTY_HELP_CLASS",
    "ROW_ANNOTATION_CLASS",
    "ROW_CATALOGUE_TITLE_CLASS",
    "ROW_CLASS",
    "ROW_COVERAGE_CLASS",
    "ROW_DIVERGENCE_CLASS",
    "ROW_META_CLASS",
    "ROW_NOVELTY_CLASS",
    "ROW_PAGES_CLASS",
    "ROW_RELATION_CLASS",
    "ROW_REPORT_CLASS",
    "ROW_CHILDREN_CLASS",
    "ROW_CHILDREN_STATE_CLASS",
    "ROW_CHILD_CLASS",
    "ROW_EXPANDER_CLASS",
    "ROW_EXCERPT_CLASS",
    "ROW_PREVIEW_CLASS",
    "EXPANSION_KEY_BY_UNIT",
    "REPORT_ADDRESS",
    "ROW_SHELFMARK_CLASS",
    "ROW_SUB_CLASS",
    "ROW_SUPPRESS_CLASS",
    "ROW_TITLE_CLASS",
    "copy_keys",
    "copy_text",
    "coverage_clause",
    "divergence_marker",
    "expansion_target",
    "launch_shade_label",
    "novelty_badge",
    "report_mailto",
    "render_finding_row",
    "render_launch_headline",
    "render_novelty_help",
]
