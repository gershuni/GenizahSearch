# -*- coding: utf-8 -*-
"""
Help Center - Dicta Genizah Search

Comprehensive bilingual (English/Hebrew) documentation and tutorials for researchers.
Based on the desktop Help.html but adapted for the web application.
"""

from nicegui import ui
from web.translations import get_language
from web.feature_flags import WEB_PUZZLE_ENABLED
from web.discovery_assets import discovery_available
from web.atlas_assets import atlas_preview_available
from web.passage_assets import (
    passage_available,
    passage_multi_witness_available,
)
from web.components.typography import h1, h2, h3
from shared.discovery_band_labels import (
    BAND_LABELS,
    band_label,
    band_measurement_status,
    NUMERATOR_LABEL,
    DENOMINATOR_LABEL,
    DRAW_SIZE_LABEL,
)


# ---------------------------------------------------------------------------
# ⟨RETIRED AND RESTORED 2026-08-13⟩ A merge removed both language call sites of
# `_render_confidence_section` and its TOC entry; the owner restored them the
# same day, because REL-01 gates the public launch on this report being
# published and CERT-02 names it as where each tier's unit and status are
# recorded -- so retiring it was never a free choice. It now renders BELOW the
# practical computed-identifications card, on the same `discovery_available()`
# gate, and was SHORTENED in the same pass (owner: "concise and
# understandable"): the estimand prose is stated once for the section instead of
# once per tier, an ungraded tier stops after its count and status rather than
# printing four more placeholder lines, and the two-bucket rule and limitations
# paragraph moved out entirely because the practical card already carries them.
#
# BAND-05 "Confidence Bands & Methods" methods section (Phase 135, plan 135-02,
# D-10; rewritten QUALITATIVELY in Phase 136, plan 136-02, D-06a). A flag-gated,
# bilingual SECTION inside this Help page (never a new route), with per-band
# anchors for tooltip deep-links. Band labels are rendered THROUGH
# shared.discovery_band_labels (never re-worded here); only the section's own
# field labels + placeholders are local UI copy. The body card + its TOC entry
# are each independently gated on discovery_available() (Codex #11). noindex
# until REL-01 is driven separately in web/main.py.
#
# D-06a / docs/specs/discovery-band-labels-v1.md Amendment 2026-08-02 Note 3:
# NO precision percentage, confidence interval, weighted estimate or strata
# table is reachable from this section, or from any Phase-136 surface,
# tooltip included. The band-labels module's per-band precision-copy
# formatter is DELIBERATELY NOT imported here -- it has no surface caller
# after this plan and is retained ONLY for offline/report use (e.g. the
# CERT-01 measurement record). Each band's status is instead rendered
# qualitatively via `band_measurement_status()`. See
# tests/render_smoke/discovery_honesty_gate.py for the shared no-numbers
# gate every later discovery surface suite imports.
# ---------------------------------------------------------------------------

# Unique marker class so a render-smoke test can scope its assertions (esp. the
# no-"certified"/no-מאושר word gate) to THIS section — the word מאושר legitimately
# appears elsewhere in the Help page (the Joins-Lab "confirmed join" copy).
_CONFIDENCE_SECTION_CLASS = 'discovery-methods-section'

# The limitations PARAGRAPH's own marker class (Phase 136, plan 136-17).
# A MARKUP HOOK ONLY: the wording of _LIMITATIONS_TEXT and its heading are
# untouched.
#
# ⟨AMENDED 2026-08-13⟩ The paragraph no longer sits in the BAND-05 methods card
# (retired, below): it renders inside the practical computed-identifications
# card, under `_COMPUTED_ID_SECTION_CLASS`. The BINDING is unchanged, because it
# was always to THIS paragraph class and never to whatever card encloses it --
# which is exactly why the move cost nothing. Read "the enclosing card" below as
# the computed-identifications card.
#
# What it is for: the shared honesty gate's accuracy/rate detector fires on
# qualitative error-rate language ("a small minority ... a low single-digit
# share ... is misattributed"), and that wording is the D-06a rewrite the owner
# approved and plan 136-02 shipped. The exception is therefore bound to ONE
# registered ELEMENT rather than carved out of the lexicon -- so the same words
# on a findings row, on an error path, inside an envelope, or in a NEIGHBOURING
# subsection of this same card remain a violation.
#
# _CONFIDENCE_SECTION_CLASS above is NOT a limitations selector: it is applied
# to the whole methods CARD and therefore covers the bucket-rule and
# novelty-check subsections too. That is why this class exists.
# `tests/render_smoke/discovery_honesty_gate.py::D06A_QUALITATIVE_SCOPES` holds
# exactly this string and is pinned to it by a lazy-import equality assertion.
_LIMITATIONS_PARAGRAPH_CLASS = 'discovery-methods-limitations'

# The practical computed-identifications card's scope class (2026-08-13).
#
# The honesty gate scans a CSS CLASS, not a NiceGUI `.mark()` marker, and
# retiring the BAND-05 card took the only scope class on this page off the page
# with it -- so `assert_discovery_honesty(scope_selector=...)` had nothing to
# bind to and NO gate could run over the real /help render at all. That is the
# state the restored honesty tests found; this class is what they scope to.
#
# NOT registered in D06A_QUALITATIVE_SCOPES, and it must never be: the card
# encloses the limitations paragraph, so registering the CARD would exempt every
# neighbouring subsection inside it -- the precise leak the paragraph-level
# binding exists to prevent (round 10; see control 12b).
_COMPUTED_ID_SECTION_CLASS = 'discovery-computed-id-section'

_CONFIDENCE_TOC_TITLE = {
    'en': 'Confidence Bands and Methods',
    'he': 'דרגות ודאות ושיטות',
}

# ⟨SHORTENED 2026-08-13, owner: "keep it concise and understandable"⟩ Three
# sentences instead of five, and the dangling cross-reference is gone: this
# section no longer carries the "Known limitations" subsection (it moved to the
# practical card), so pointing at it "below" would have sent the reader nowhere.
_CONFIDENCE_INTRO = {
    'en': (
        'Each tier below says how many identifications it holds and whether anyone has '
        'graded it — in words, never as a percentage or an interval. Grading is done on a '
        'sample and describes the tier as a whole, so it is never a probability for a single '
        'identification, and every identification remains an unreviewed algorithmic match. '
        'Where a tier has not been graded, this says so.'
    ),
    'he': (
        'כל דרגה למטה מציינת כמה זיהויים היא כוללת ואם דורגה — במילים, לעולם לא כאחוז או '
        'כטווח. הדירוג נעשה על מדגם ומתאר את הדרגה כולה, ולכן אינו הסתברות לזיהוי בודד, וכל '
        'זיהוי נותר התאמה אלגוריתמית שלא נבדקה. דרגה שלא דורגה — כך נאמר עליה.'
    ),
}

_CONFIDENCE_FIELD_LABELS = {
    # ⟨SHORTENED 2026-08-13⟩ was "Population (shipped claims in this band)" --
    # the parenthetical restated the field for an audience that already knows
    # what a population is, and puzzled the one that does not.
    'population': {
        'en': 'Identifications in this tier',
        'he': 'זיהויים בדרגה זו',
    },
    'unit': {'en': 'Unit of measurement', 'he': 'יחידת מדידה'},
    'sample': {'en': 'Sample size', 'he': 'גודל המדגם'},
    'status': {'en': 'Measurement status', 'he': 'סטטוס מדידה'},
    'measurement_date': {'en': 'Measurement date', 'he': 'תאריך מדידה'},
    'grader': {'en': 'Grader', 'he': 'מדרג'},
    'audit_status': {'en': 'Audit status', 'he': 'סטטוס ביקורת'},
    'report_id': {'en': 'Report identifier', 'he': 'מזהה דוח'},
}

_CONFIDENCE_PLACEHOLDERS = {
    'not_measured': {'en': 'not yet measured', 'he': 'טרם נמדד'},
    'audit_pending': {'en': 'independent audit pending', 'he': 'ביקורת בלתי-תלויה בהמתנה'},
    'count_unavailable': {'en': 'count unavailable', 'he': 'מספר לא זמין'},
}

# D-05: the estimand unit; CI clustered by physical MS.
_CONFIDENCE_UNIT_PROSE = {
    'en': 'per (page, work) claim; confidence interval clustered by physical manuscript',
    'he': 'לכל טענת (עמוד, חיבור); רווח סמך מקובץ לפי כתב-יד פיזי',
}

# D-06a / Amendment 2026-08-02 Note 3: the qualitative replacement for the
# STRUCK weighted-estimate + confidence-interval line. Keyed by the
# band_measurement_status() return value (shared/discovery_band_labels.py) so
# a status change needs no code edit here — only a sidecar data change, same
# discipline as the label table it replaces. Any status this table does not
# (yet) name falls back to the "not yet measured" placeholder.
_MEASUREMENT_STATUS_COPY = {
    'not_measured': {'en': 'not yet measured', 'he': 'טרם נמדד'},
    'measured_pass': {
        'en': 'graded to completion and passed its pre-registered floor',
        'he': 'סווג עד תום ועבר את הסף שנקבע מראש',
    },
    'measured_fail': {
        'en': 'graded to completion; did not pass its pre-registered floor',
        'he': 'סווג עד תום; לא עבר את הסף שנקבע מראש',
    },
    'insufficient_evidence': {
        'en': 'graded; not enough evidence to reach a pass/fail floor',
        'he': 'סווג; אין די ראיות כדי להכריע מעבר או כישלון בסף',
    },
    'measured_audit_pending': {
        'en': 'graded; independent audit pending',
        'he': 'סווג; ביקורת בלתי-תלויה בהמתנה',
    },
}


def _measurement_status_copy(status, lk):
    """The qualitative sentence for a band_measurement_status() value, never a
    bare percentage or interval. Falls back to "not yet measured" for any
    status this table does not name."""
    entry = _MEASUREMENT_STATUS_COPY.get(status) or _MEASUREMENT_STATUS_COPY['not_measured']
    return entry[lk]


# ---------------------------------------------------------------------------
# Phase 136, plan 136-02 (Task 2): the two-bucket rule, its known limitations,
# and the novelty check — each rendered qualitatively, in words, exactly ONCE
# per language at section scope (not per band). Source of the wording:
# .claude/skills/sketch-findings-genizahsearch/references/main-pool-rule.md
# ("The rule" + "Known limitations, stated plainly" + "Wording and internal
# state") and 136-CONTEXT.md D-23/D-23b.
# ---------------------------------------------------------------------------

_BUCKET_RULE_HEADING = {'en': 'The two-bucket rule', 'he': 'כלל שתי הקבוצות'}

# MAIN_POOL_SENTENCE is the SINGLE source of the bucket-rule wording on this
# page. Plan 136-07 asserts that shared.discovery_main_pool.main_pool_sentence()
# returns this text byte-identical, so the rule can never be described two
# different ways on two surfaces.
MAIN_POOL_SENTENCE = {
    'en': (
        'A fragment is treated as a probable identification when it matches the work across '
        'more than one leaf, or covers almost a whole page on its own. Everything else appears '
        'under ‘more matches’.'
    ),
    'he': (
        'קטע נחשב לזיהוי סביר כאשר הוא תואם את החיבור ביותר מדף אחד, או מכסה כמעט עמוד שלם '
        'בפני עצמו. כל השאר מופיע תחת ‘התאמות נוספות’.'
    ),
}

# The second bucket means insufficient evidence, never "probably wrong" — a
# distinction the sketch's own honesty gate exists to protect.
_SECOND_BUCKET_MEANING = {
    'en': (
        'The second group does not mean the match is wrong — it means there was not enough '
        'evidence for the rule above. It holds probable quotations, shared wording, unresolved '
        'ties, missing signals, and genuinely indeterminate cases alike.'
    ),
    'he': (
        'הקבוצה השנייה אינה אומרת שההתאמה שגויה — היא אומרת שלא הייתה מספיק ראיה לכלל '
        'שלמעלה. היא כוללת ציטוטים אפשריים, ניסוח משותף, תיקו שלא הוכרע, סימנים חסרים, '
        'ומקרים שלא ניתן להכריע בהם באופן מובהק, לצד אלה.'
    ),
}

_LIMITATIONS_HEADING = {'en': 'Known limitations, stated plainly', 'he': 'מגבלות ידועות, כפי שהן'}

#: ⟨REVISED 2026-08-13, owner-approved⟩ The two SUBSTANTIVE disclosures were
#: restored after a revision dropped them: the qualitative misattribution rate
#: (the containment residue -- the only place a reader is told how often the
#: main pool is wrong for this reason) and the two-sides-of-one-leaf caveat (the
#: honesty note attached to the multi-folio limb of the main-pool rule itself).
#: Restored SHORTER and in the plainer register of the revision that dropped
#: them -- its opening list and its actionable closing sentence are kept
#: verbatim. The composition-date caveat was NOT restored: it is a statement
#: about method rather than something a reader must act on, and the owner asked
#: for shorter.
#:
#: The qualitative rate wording is what makes D-06a's ONE named exception
#: load-bearing rather than vacuous: "minority" / "single-digit" are the
#: quantity words and "share" / "misattributed" the rate words the shared
#: gate's accuracy detector fires on (`_QUANTITY_WORDS_*` / `_RATE_WORDS_*` in
#: tests/render_smoke/discovery_honesty_gate.py), and it is compliant ONLY
#: inside `_LIMITATIONS_PARAGRAPH_CLASS` below. The Hebrew quantity token is
#: `חד-ספרתי` with a PLAIN HYPHEN, never a maqaf -- the detector matches the
#: literal, so a typographic "improvement" there silently unbinds the control.
#: `LIMITATIONS_TEXT_SHA256` in tests/render_smoke/test_panel_render_smoke.py
#: pins this wording and was re-pinned in the same commit as this revision.
_LIMITATIONS_TEXT = {
    'en': (
        'Automatic matching can be misled by quotations, formulas, shared wording, short '
        'passages, and errors in the automatic transcription. A larger work that embeds '
        'another text absorbs matches belonging to the embedded passage — a prayer inside a '
        'legal code — so a low single-digit share of the main pool is misattributed this '
        'way. Two pages matching the same work are often two sides of one leaf, not two '
        'independent leaves. Check the image, the text match, and the context before '
        'accepting a suggestion.'
    ),
    'he': (
        'התאמה אוטומטית עלולה לטעות בגלל ציטוטים, נוסחאות, לשון משותפת, קטעים קצרים ושגיאות '
        'בתעתוק האוטומטי. חיבור גדול המשבץ בתוכו טקסט אחר סופג התאמות ששייכות לקטע המשובץ — '
        'תפילה בתוך קובץ הלכתי — ולכן נתח נמוך וחד-ספרתי מהמאגר העיקרי משויך בטעות מסיבה זו. '
        'שני עמודים המתאימים לאותו חיבור הם לעיתים שני צדי אותו דף, ולא שני דפים עצמאיים. '
        'לפני שמקבלים הצעה כדאי לבדוק את התמונה, את התאמת הטקסט ואת ההקשר.'
    ),
}

_NOVELTY_CHECK_HEADING = {'en': 'The novelty check', 'he': 'בדיקת החידוש'}

# The checked-source list, copied VERBATIM from the shipped public wording in
# shared/discovery_display_strings.py::_NOVELTY_HELP (the parenthesis inside
# it). That string has no separate constant to import, so keep the two in step
# by hand, and never add a source name that is not already in it: the list
# names no restricted corpus (D-25), only "shelfmark attributions".
_NOVELTY_CHECKED_SOURCES_PLACEHOLDER = {
    'en': (
        'the finding aids we check today — FJMS + NLI catalogues and bibliography, '
        'titles, PGP, FGP, shelfmark attributions'
    ),
    'he': (
        'אמצעי העזר שאנו בודקים כיום — קטלוגים וביבליוגרפיה של FJMS ושל הספרייה הלאומית, '
        'כותרות, PGP, FGP, ייחוסים לפי סימני מדף'
    ),
}

_NOVELTY_CHECK_TEXT = {
    'en': (
        'Every identification is checked against a fixed, enumerable set of finding aids — '
        '{sources}. Even when nothing turns up, the identification behind a candidate is still '
        'an unreviewed algorithmic match, so a candidate is not a confirmed find. Absence from '
        'the finding aids checked is not evidence that a match is correct — it only means the '
        'checked sources do not already record it.'
    ),
    'he': (
        'כל זיהוי נבדק מול קבוצה קבועה וניתנת למניה של אמצעי עזר — {sources}. גם כאשר לא נמצא '
        'דבר, הזיהוי שביסוד המועמד נותר התאמה אלגוריתמית שלא נבדקה, כך שמועמד אינו ממצא '
        'סופי. היעדרות מאמצעי העזר שנבדקו אינה ראיה לכך שההתאמה נכונה — היא אומרת רק '
        'שהמקורות שנבדקו אינם מתעדים אותה כבר.'
    ),
}


#: ⟨DELETED 2026-08-13⟩ `_render_bucket_rule_subsection` and
#: `_render_known_limitations_subsection` are gone. Both rendered copy the
#: practical computed-identifications card now carries, so a reader met the
#: two-bucket rule and the limitations paragraph TWICE on one page -- and a
#: second element carrying `_LIMITATIONS_PARAGRAPH_CLASS` also breaks the
#: wording pin, which digests every element with that class.
#:
#: Their heading/copy CONSTANTS are deliberately kept: `_BUCKET_RULE_HEADING`
#: and `_LIMITATIONS_HEADING` are imported by the render-smoke pins in
#: tests/render_smoke/test_panel_render_smoke.py (the D-06a card-boundary
#: control and the "scope is the paragraph, not the heading" assertion), and
#: `_SECOND_BUCKET_MEANING`'s point -- the second group is not "probably wrong"
#: -- is made in the practical card's pane guide instead.


def _render_novelty_check_subsection(lk, text_style):
    """Task 2.3: the enumerable checked-source set + the candidate-is-not-a-
    confirmed-find framing (NOVEL-01/D-23b)."""
    h3(
        _NOVELTY_CHECK_HEADING[lk],
        classes='text-lg font-semibold mt-3 mb-1',
        style='color: var(--text-primary);',
    )
    ui.label(
        _NOVELTY_CHECK_TEXT[lk].format(sources=_NOVELTY_CHECKED_SOURCES_PLACEHOLDER[lk])
    ).style(text_style)

# D-10 per-band deep-link anchor registry (help-confidence-<band>): one anchor
# per band, keyed by the canonical (evidence_source, band) pair. This is the
# SINGLE greppable source of truth for the tooltip deep-link targets Phase 136
# will link to — never re-derive a competing anchor string elsewhere.
_CONFIDENCE_BAND_ANCHORS = {
    ('track1_direct', 'high_confidence_algorithmic'): 'help-confidence-high_confidence_algorithmic',
    ('track1_direct', 'tier_a'): 'help-confidence-tier_a',
    ('track1_direct', 'screening_rb'): 'help-confidence-screening_rb',
    ('track1_direct', 'screening_canon'): 'help-confidence-screening_canon',
    ('propagated', 'corroborated'): 'help-confidence-corroborated',
    ('propagated', 'weak'): 'help-confidence-weak',
    ('propagated', 'not_evaluated'): 'help-confidence-not_evaluated',
}


def _confidence_lang_key(lang: str) -> str:
    return 'he' if lang == 'he' else 'en'


def _confidence_bands():
    """The 7 (evidence_source, canonical_band_key) pairs in display order —
    sourced from the values module's label table so this never re-lists a
    competing band vocabulary (drift-guarded there)."""
    return list(BAND_LABELS.keys())


def _band_population(band_counts, evidence_source, canonical_band):
    """The RUNTIME display-deduplicated shipped-claim count for a band, trying
    the v1 stored key as a fallback (§5 v1-read-compat). None when absent."""
    if not band_counts:
        return None
    n = band_counts.get((evidence_source, canonical_band))
    if n is None and canonical_band == 'high_confidence_algorithmic':
        n = band_counts.get((evidence_source, 'expert_verified'))
    return n


def _band_precision_row(precision, evidence_source, canonical_band):
    """The band_precision row for a band, trying the v1 stored key as a
    fallback. Returns {} (never None) so downstream .get()/format calls are
    placeholder-safe."""
    if not precision:
        return {}
    row = precision.get((evidence_source, canonical_band))
    if row is None and canonical_band == 'high_confidence_algorithmic':
        row = precision.get((evidence_source, 'expert_verified'))
    return row or {}


def _render_confidence_section(lang, precision, band_counts):
    """Render the flag-gated BAND-05 methods section body card.

    Codex #11: the CALLER must guard this on discovery_available() — the body
    is its OWN conditional card, NOT part of the TOC render loop (that loop
    emits only ui.link TOC entries), so the loop-continue does not gate it.

    D-06a (Phase 136, plan 136-02): every number below is either a runtime
    COUNT (population, sample size) or absent — no precision percentage, no
    confidence interval, no weighted estimate, no strata table anywhere in
    this section, in either language. Each band's measurement is instead
    explained in words via band_measurement_status(). Three qualitative
    subsections (the two-bucket rule, known limitations, the novelty check)
    render once per language, ahead of the per-band detail.
    """
    lk = _confidence_lang_key(lang)
    rtl = lk == 'he'
    text_style = 'color: var(--text-secondary);' + (
        ' direction: rtl; text-align: right;' if rtl else ''
    )
    col_style = 'direction: rtl; text-align: right;' if rtl else 'direction: ltr;'

    with ui.card().classes(f'w-full p-6 {_CONFIDENCE_SECTION_CLASS}'):
        ui.element('a').props('name="help-confidence"')
        with ui.column().classes('w-full gap-2').style(col_style):
            with ui.row().classes('items-center gap-3 mb-2'):
                ui.icon('analytics').classes('text-2xl text-primary')
                h2(
                    _CONFIDENCE_TOC_TITLE[lk],
                    classes='text-xl font-bold',
                    style='color: var(--text-primary);',
                )

            ui.label(_CONFIDENCE_INTRO[lk]).style(text_style).classes('mb-2')

            # WHAT IS MEASURED, ONCE. It is the same estimand prose for all
            # seven tiers, so rendering it per tier was six repetitions of one
            # sentence -- most of this section's length was repetition, not
            # information.
            ui.label(
                f"{_CONFIDENCE_FIELD_LABELS['unit'][lk]}: {_CONFIDENCE_UNIT_PROSE[lk]}"
            ).style(text_style).classes('mb-2')

            # The bucket rule and the limitations paragraph are NOT rendered
            # here any more -- both now live in the practical
            # computed-identifications card above, so a reader met each of them
            # twice on one page.
            #
            # For the limitations paragraph that was also a CORRECTNESS
            # problem, not only repetition: `LIMITATIONS_TEXT_SHA256` digests
            # the text of EVERY element carrying `_LIMITATIONS_PARAGRAPH_CLASS`,
            # so a second copy silently doubles the digested string and the pin
            # fails on wording nobody touched. Do not re-add it here.
            #
            # The novelty check STAYS: nothing else on the page carries it, and
            # it is NOVEL-01/D-23b's candidate-is-not-a-confirmed-find framing.
            _render_novelty_check_subsection(lk, text_style)

            for evidence_source, canonical_band in _confidence_bands():
                _render_one_band(
                    evidence_source, canonical_band, lang, precision, band_counts, text_style
                )


def _render_one_band(evidence_source, canonical_band, lang, precision, band_counts, text_style):
    """One band's documentation block: a deep-link anchor (D-10) + its label +
    the BAND-05 field set, each field STRICTLY per the plan's field-sourcing
    table (population from the runtime display-deduplicated shipped count;
    sample split into draw/determinate/successes; registry fields placeholder-
    safe, never fabricated)."""
    lk = _confidence_lang_key(lang)
    fl = _CONFIDENCE_FIELD_LABELS
    ph = _CONFIDENCE_PLACEHOLDERS
    nm = ph['not_measured'][lk]
    row = _band_precision_row(precision, evidence_source, canonical_band)

    # D-10 per-band anchor (tooltip deep-link target) from the anchor registry;
    # fall back to the derived name if a future band is added to BAND_LABELS but
    # not yet to the registry (fail-safe — the anchor still renders).
    anchor_name = _CONFIDENCE_BAND_ANCHORS.get(
        (evidence_source, canonical_band), f'help-confidence-{canonical_band}'
    )
    ui.element('a').props(f'name="{anchor_name}"')
    h3(
        band_label(evidence_source, canonical_band, lang),
        classes='text-lg font-semibold mt-3 mb-1',
        style='color: var(--text-primary);',
    )

    # population — RUNTIME display-deduplicated shipped-claim count (never the
    # denominator, never raw evidence rows, never the Wave-4 frame doc).
    n = _band_population(band_counts, evidence_source, canonical_band)
    pop_val = f"{n:,}" if isinstance(n, int) else ph['count_unavailable'][lk]
    ui.label(f"{fl['population'][lk]}: {pop_val}").style(text_style)

    # measurement status — QUALITATIVE ONLY (D-06a): no percentage, no CI, no
    # strata table. band_measurement_status() is the SAME status-derivation
    # function is_default_eligible() reads — a status change here needs no
    # code edit, only a sidecar data change.
    status = band_measurement_status(row)
    ui.label(f"{fl['status'][lk]}: {_measurement_status_copy(status, lk)}").style(text_style)

    # ⟨2026-08-13, owner: "concise and understandable"⟩ AN UNGRADED TIER STOPS
    # HERE. Its sample size, date, grader, audit state and report id are all
    # absent, so what rendered was four more lines of "not yet measured" under a
    # status line that had just said exactly that -- and with most tiers
    # ungraded, that repetition WAS this section, which is how a methods page
    # ends up unreadable. Nothing publishable is withheld: the status line above
    # is the honest statement, and it is unconditional.
    if status == 'not_measured':
        return

    # sample size — THREE distinct numbers, never conflated with population.
    def _num(v):
        return f"{v:,}" if isinstance(v, int) else nm

    sample_parts = [
        f"{DRAW_SIZE_LABEL[lk]} {_num(row.get('draw_size'))}",
        f"{DENOMINATOR_LABEL[lk]} {_num(row.get('denominator'))}",
        f"{NUMERATOR_LABEL[lk]} {_num(row.get('numerator'))}",
    ]
    ui.label(f"{fl['sample'][lk]}: " + "; ".join(sample_parts)).style(text_style)

    # the CERT-01 registry fields — read via .get(), NEVER fabricated (135-05
    # columns; filled by 135-09). Each is skipped when absent rather than
    # printed as a placeholder; the audit state is unconditional because its
    # own placeholder ("independent audit pending") is a real statement about a
    # graded tier rather than a stand-in for a missing one.
    for field in ('measurement_date', 'grader', 'report_id'):
        value = row.get(field)
        if value:
            ui.label(f"{fl[field][lk]}: {value}").style(text_style)
    ui.label(
        f"{fl['audit_status'][lk]}: {row.get('audit_status') or ph['audit_pending'][lk]}"
    ).style(text_style)


def _help_tool_link(label: str, target: str, marker: str, *, rtl: bool = False) -> None:
    """Render a compact native link at the end of a practical help section."""
    arrow = 'arrow_back' if rtl else 'arrow_forward'
    with ui.link(target=target).classes(
        'inline-flex items-center gap-2 min-h-[44px] px-4 py-2 rounded-lg '
        'no-underline font-semibold'
    ).style(
        'color: var(--primary-700); border: 1px solid var(--border-medium); '
        'background: var(--bg-secondary);'
    ).mark(marker):
        ui.label(label)
        ui.icon(arrow).props('aria-hidden=true')


def _render_computed_identifications_help(lang: str) -> None:
    """Practical guide to the current two-level computed-matches interface."""
    is_hebrew = lang == 'he'
    text_style = (
        'color: var(--text-secondary); line-height: 1.75;'
        + (' direction: rtl; text-align: right;' if is_hebrew else '')
    )

    with ui.card().classes(
            f'w-full p-6 {_COMPUTED_ID_SECTION_CLASS}'
    ).mark('help-computed-identifications'):
        ui.element('a').props('name="help-computed-identifications"')
        with ui.row().classes('items-center gap-3 mb-3 flex-wrap'):
            ui.icon('fact_check').classes('text-2xl text-primary').props('aria-hidden=true')
            h2(
                'זיהויים מחושבים (בטא)' if is_hebrew else 'Computed Identifications (Beta)',
                classes='text-xl font-bold',
                style='color: var(--text-primary);',
            )

        ui.label(
            (
                'האתר משווה את התעתיקים האוטומטיים של כתבי־היד לספרייה גדולה של חיבורים '
                'מוכרים, ומציע התאמות טקסטואליות לבדיקה. אלו הצעות אלגוריתמיות, לא זיהויים '
                'שנבדקו בידי חוקר.'
            )
            if is_hebrew
            else (
                'The site compares automatic manuscript transcriptions with a large library of '
                'known works and offers promising textual matches for examination. These are '
                'algorithmic suggestions, not identifications reviewed by a scholar.'
            )
        ).classes('mb-3').style(text_style)

        h3(
            'כך קוראים את הפאנל' if is_hebrew else 'Reading the matches pane',
            classes='text-lg font-semibold mt-2 mb-1',
            style='color: var(--text-primary);',
        )
        ui.markdown(
            '''
- **בדף זה** מציג את קבוצת המועמדים הראשית לדף הפתוח.
- **הצג התאמות אפשריות נוספות** פותח מועמדים שלא נכנסו לקבוצה הראשית; אין פירוש הדבר שהם שגויים.
- **בכתב־יד זה** מרכז חיבורים שהוצעו באחד מדפי כתב־היד; התווית **כולל דף זה** מסמנת שהחיבור מופיע גם בדף הפתוח.
- **הצגת התאמת הטקסט** מאפשרת להשוות את קטעי הטקסט שעליהם מבוססת ההצעה.
- **אינו מתאים לקטלוג — לא הוכרע** מציין חוסר התאמה לזיהוי הקטלוגי הקיים, בלי לקבוע אם הקטלוג או ההצעה נכונים.
            '''
            if is_hebrew
            else '''
- **On this page** contains the main group of candidates for the open folio.
- **Show more possible matches** opens candidates outside that first group; it does not mean they are wrong.
- **In this manuscript** gathers works suggested on any page of the manuscript; **includes this page** marks a work also found on the open folio.
- **View text match** lets you compare the passages on which the suggestion is based.
- **Does not correspond to the catalogue — not adjudicated** records a difference from existing catalogue metadata without declaring either side correct.
            ''',
            extras=['rtl'] if is_hebrew else [],
        ).style(text_style)

        # The PAGE, as distinct from the pane above (owner, 2026-08-13). The card
        # documented the connections pane and then linked to
        # /computed-identifications without saying what is there -- so the
        # corpus-wide surface, which is the one with all the controls, was
        # undocumented.
        #
        # Bucket NAMES are deliberately not written here. They have exactly one
        # definition (`shared.discovery_main_pool.bucket_label`), the page itself
        # never retypes them, and a Help page holding its own copy of them is how
        # a fourth vocabulary for the two pools gets created.
        h3(
            'דף הזיהויים המחושבים' if is_hebrew else 'The Computed Identifications page',
            classes='text-lg font-semibold mt-3 mb-1',
            style='color: var(--text-primary);',
        )
        ui.label(
            (
                'הפאנל מראה את ההתאמות של כתב־היד הפתוח; הדף מציג את כולן יחד, בכל האוסף, '
                'ומאפשר להגיע אליהן לפי חיבור, מחבר או יחס לקטלוג ולא רק לפי סימן מדף. '
                'בדף עצמו, "איך לקרוא את הדף הזה" פורש את ההנחיות שלהלן במלואן.'
            )
            if is_hebrew
            else (
                'The pane shows the matches for the manuscript you are reading; the page '
                'gathers them across the whole corpus, so you can arrive by work, author, or '
                'relationship to the catalogue rather than only by shelfmark. On the page '
                'itself, "How to read this page" expands the guidance below.'
            )
        ).classes('mb-2').style(text_style)
        ui.markdown(
            '''
- **אילו ממצאים** מצמצם לפי היחס לאמצעי העזר: כל הממצאים, מועמדים לממצאים חדשים, ממצאים שאינם מתאימים לקטלוג, או אחד מן השניים.
- **באיזה מאגר** מחליף בין קבוצת המועמדים הראשית לבין הרחבה ממנה, לפי אותו כלל שבפאנל.
- **הצג כ** קובע מה נמנה בכל שורה — זיהוי, כתב יד או חיבור — ולצידו מיון: **החזקים תחילה**, **דפים שהותאמו** או **טקסט שהותאם**.
- **תחום החיבור המזוהה**, **מחבר** ו**חיבור** מצמצמים לפי החיבור שזוהה, ולא לפי נושא הקטלוג של כתב־היד.
- סינון המסומן **עדיין לא זמין** חסר את הנתונים הדרושים לו; אין זה כמו סינון שאינו מחזיר דבר.
- המספר בראש הדף מתאר את המהדורה כולה ואינו משתנה בעקבות סינון; השורה שלצד התוצאות מציינת תמיד מה נמנה ברשימה הנוכחית, ולכן שני המספרים יכולים להיות שונים בלי שדבר שגוי.
- **הצגת התאמת הטקסט** פועלת גם כאן, ובשורות הבודדות יש **דיווח על בעיה** — הדרך המהירה להודיע לנו שהצעה שגויה.
            '''
            if is_hebrew
            else '''
- **Which findings** narrows by relationship to the finding aids: all findings, candidates for new finds, those that do not correspond to the catalogue, or either of the two.
- **Which pool** switches between the main group of candidates and the wider one, on the same rule the pane uses.
- **Show as** sets what one row counts — an identification, a manuscript, or a work — and the sort beside it offers **Strongest first**, **Pages matched** or **Matched text**.
- **Domain of the identified work**, **Author** and **Work** narrow by the work that was identified, not by the manuscript's catalogue subject.
- A filter tagged **not available yet** is missing the data it would need; that is not the same as a filter returning nothing.
- The figure at the top of the page describes the whole release and deliberately does not move when you filter; the line beside the results always states what the current list counts, so the two numbers can differ without either being wrong.
- **View text match** works here as well, and individual rows carry **Report a problem** — the quickest way to tell us a suggestion is wrong.
            ''',
            extras=['rtl'] if is_hebrew else [],
        ).style(text_style)

        h3(
            'כיצד בודקים הצעה' if is_hebrew else 'How to evaluate a suggestion',
            classes='text-lg font-semibold mt-3 mb-1',
            style='color: var(--text-primary);',
        )
        ui.label(MAIN_POOL_SENTENCE[lang]).style(text_style)
        ui.label(_LIMITATIONS_TEXT[lang]).classes(_LIMITATIONS_PARAGRAPH_CLASS).style(text_style)
        ui.label(
            (
                'התחילו מן ההתאמה, אבל חזרו תמיד לתמונה, לתעתוק ולהקשר. עד לבדיקת חוקר, '
                'יש לצטט את כתב־היד ואת מקורות הקטלוג — לא את ההצעה כזיהוי ודאי.'
            )
            if is_hebrew
            else (
                'Use the match as a lead, then return to the image, transcription, and context. '
                'Until a scholar has reviewed it, cite the manuscript and catalogue sources — '
                'not the suggestion as an established identification.'
            )
        ).classes('mt-2').style(text_style)

        _help_tool_link(
            'עיון בכל ההתאמות המחושבות' if is_hebrew else 'Explore all computed matches',
            '/computed-identifications',
            'help-computed-identifications-link',
            rtl=is_hebrew,
        )


def _render_visual_atlas_help(lang: str) -> None:
    """Short orientation to the atlas and the claims it does not make."""
    is_hebrew = lang == 'he'
    text_style = (
        'color: var(--text-secondary); line-height: 1.75;'
        + (' direction: rtl; text-align: right;' if is_hebrew else '')
    )

    with ui.card().classes('w-full p-6').mark('help-visual-atlas'):
        ui.element('a').props('name="help-visual-atlas"')
        with ui.row().classes('items-center gap-3 mb-3 flex-wrap'):
            ui.icon('hub').classes('text-2xl text-primary').props('aria-hidden=true')
            h2(
                'אטלס הגניזה החזותי' if is_hebrew else 'The Visual Genizah Atlas',
                classes='text-xl font-bold',
                style='color: var(--text-primary);',
            )

        ui.label(
            (
                'האטלס מציג מבט רחב על קשרים טקסטואליים בין כתבי־יד בכל קורפוס הגניזה. '
                'כתבי־יד בעלי טקסט דומה מתקבצים יחד, וקווים מסמנים קשרים שחושבו ביניהם.'
            )
            if is_hebrew
            else (
                'The atlas gives a corpus-wide view of textual connections between Genizah '
                'manuscripts. Manuscripts with similar text gather into clusters, and lines mark '
                'connections calculated between them.'
            )
        ).classes('mb-2').style(text_style)
        ui.markdown(
            '''
- חפשו לפי כותרת או מספר מדף, או התקרבו לאזור שמסקרן אתכם.
- החליפו צביעה לפי תחום או ספרייה, הסתירו ספרייה, או התמקדו במקבץ אחד.
- לחצו על כתב־יד לתצוגה מקדימה, ומשם פתחו את עמוד העיון המלא.
- בטלפון מומלץ לעבור למסך מלא כדי לגרור ולהגדיל בחופשיות.
            '''
            if is_hebrew
            else '''
- Search by title or shelfmark, or zoom into a region that catches your attention.
- Color by domain or library, hide a library, or focus on one constellation.
- Select a manuscript for a preview, then open its full Browse page.
- On a phone, Full screen gives the easiest pan-and-zoom experience.
            ''',
            extras=['rtl'] if is_hebrew else [],
        ).style(text_style)
        ui.label(
            (
                'חשוב: הקרבה במפה משקפת דמיון טקסטואלי שחושב אלגוריתמית — לא מוצא פיזי '
                'משותף ולא צירוף בין קטעים. האטלס נועד לגלות שכנים וקשרים שכדאי לבדוק בכתב־היד.'
            )
            if is_hebrew
            else (
                'Important: proximity reflects algorithmically calculated textual similarity — '
                'not shared physical provenance and not a proposed physical join. Use the atlas '
                'to discover neighbours and connections worth checking in the manuscript.'
            )
        ).classes('mt-2').style(text_style)

        _help_tool_link(
            'פתיחת אטלס הגניזה' if is_hebrew else 'Open the Visual Genizah Atlas',
            '/atlas',
            'help-visual-atlas-link',
            rtl=is_hebrew,
        )


def create_help_page(precision=None, band_counts=None):
    """Create the comprehensive Help Center page with bilingual content.

    ``precision`` / ``band_counts`` remain accepted for route compatibility;
    the former confidence-band report has been replaced by a practical guide
    to the current reader-facing tools."""

    is_hebrew = get_language() == 'he'

    with ui.column().classes('w-full max-w-4xl mx-auto gap-6 fade-in p-4'):

        h1(
            'מרכז עזרה' if is_hebrew else 'Help Center',
            classes='text-3xl font-bold mb-4',
            style='color: var(--text-primary);'
        )

        with ui.column().classes('w-full gap-6'):
            if is_hebrew:
                _create_hebrew_content(precision=precision, band_counts=band_counts)
            else:
                _create_english_content(precision=precision, band_counts=band_counts)


def _create_english_content(precision=None, band_counts=None):
    """Create the English help content."""

    # === Table of Contents ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('list').classes('text-2xl text-primary')
            h2('Table of Contents', classes='text-xl font-bold', style='color: var(--text-primary);')

        with ui.column().classes('gap-2'):
            toc_items = [
                ('intro', 'Introduction: How it Works'),
                ('search', 'Search'),
                ('responsa', 'Responsa-Style Search'),
                ('filters', 'Focused Search ("Search only in...")'),
                ('search-within', 'Search Within Results'),
                ('exclude-manuscripts', 'Exclude Known Manuscripts'),
                ('translations', 'Catalog & Metadata Translations'),
                ('parallels', 'Find Parallels'),
                ('joins-lab', 'Joins Lab'),
                ('pgp', 'Princeton Geniza Project (PGP) Data'),
                ('reading-desk', 'Reading Desk'),
                ('browse', 'Browse by Shelfmark'),
                ('catalog-browse', 'Browse by Identification'),
            ]
            if discovery_available():
                toc_items.append(('computed-identifications', 'Computed Identifications (Beta)'))
                # BAND-05 methods section (D-10), restored 2026-08-13 under the
                # SAME gate as the card above and its body card below. The
                # pre-merge code additionally carried an `anchor == 'confidence'
                # and not discovery_available()` continue inside the render loop;
                # it is not restored, because appending under this gate already
                # decides it and a second unreachable guard reads as a real one.
                toc_items.append(('confidence', _CONFIDENCE_TOC_TITLE['en']))
            if atlas_preview_available():
                toc_items.append(('visual-atlas', 'The Visual Genizah Atlas'))
            toc_items.extend([
                ('lists', 'My Lists'),
                ('export', 'Exporting Data'),
                ('api', 'Public API & AI Tools'),
                ('my-library', 'My Library — Local Documents'),
            ])
            if WEB_PUZZLE_ENABLED:
                # Their sections follow Reading Desk, so their entries do too.
                _after_desk = [k for k, _ in toc_items].index('reading-desk') + 1
                toc_items.insert(_after_desk, ('puzzle', 'Fragment Puzzle'))
                toc_items.insert(_after_desk + 1, ('community-publish', 'Community Publishing'))
            for anchor, title in toc_items:
                if not WEB_PUZZLE_ENABLED and anchor in {'puzzle', 'community-publish'}:
                    continue
                ui.link(f'• {title}', f'#help-{anchor}').classes('text-primary hover:underline')

    # === Introduction ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-intro"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('Introduction: How it Works', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**Dicta Genizah Search** provides fast and advanced access to the transcription corpus of the "MiDRASH" project.
The platform is based on a high-speed search engine (Tantivy) and integrates unique algorithms to handle
some of the reading errors from the MiDRASH project's decoding algorithm.

**Citation Requirement:** MiDRASH transcriptions are released under CC-BY-4.0 license, meaning they can be used with proper attribution. If you use the transcriptions, please credit:

> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments [Data set]. Zenodo.
> https://doi.org/10.5281/zenodo.17734473

The application fetches metadata and images from:
- **National Library of Israel (NLI)**
- **Bodleian Library** at Oxford
- **Cambridge University Library**

*Note:* Some library servers may block access from non-home networks (e.g., mobile hotspots).

**New to the site?** **[Start Here](/start)** in the menu offers guided starting points: manuscripts to browse, prepared searches to try, and a way into the research tools.
        ''').style('color: var(--text-secondary);')

    # === Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-search"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('search').classes('text-2xl text-primary')
            h2('Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('This is the entry point for free text or metadata searches within the corpus. You can use the shortcuts shown in parentheses below to go directly to your desired search type.').style('color: var(--text-secondary);').classes('mb-4')

        h3('Search Modes', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        modes_data = [
            ('Exact (=)', 'Matches only the exact word or sequence of words as typed. To search with gaps between words, fill in the "Gap" field with the desired number (up to 10). In Responsa-style search you can write a larger gap in the query itself, e.g. `[25]`; in Regex mode the Gap field is not used, and the pattern itself sets any distance.'),
            ('Variants (?)', 'Accounts for common letter substitutions in these texts (e.g., Dalet/Resh, He/Het, Vav/Yod). Choose a level: **Basic**, **Extended** (adds pairs like Qof/Kaf and Tet/Tav) or **Maximum** (the broadest flexibility) \u2014 a higher level gives broader recall but slower, noisier results. **Num Changes** sets how many letters in each word may be swapped (\u00d71, \u00d72 or \u00d73); each level keeps its own (by default \u00d71 for Basic, \u00d72 for Extended and Maximum). In the general settings you can switch the level buttons to a slider.'),
            ('\U0001F195 Responsa (R)', 'Search syntax inspired by the Bar-Ilan Responsa Project, with prefix/suffix expansion, wildcards, spelling variants, and proximity gaps. Also includes a convenient and flexible tabular query builder. Familiar to Responsa Project users; easy to learn for newcomers. See [Responsa-Style Search](#help-responsa) below.'),
            ('Fuzzy (~)', 'Finds near spellings, useful for decoding errors: whole words that differ from the query by one letter (two in words of 5 letters or more) — a letter added, dropped or changed, or two neighbouring letters swapped. Prefixed forms are included (ושלום for שלום); a word found only inside a longer word is not.'),
            ('Regex (/)', 'Advanced search for experienced users. Example: \\b\u05d0[\u05d0-\u05ea]{3}\\b finds 4-letter words starting with Aleph. You can use your preferred AI assistant to help build a regex that fits your needs.'),
            ('Title ($)', 'Searches within the catalog titles of compositions.'),
            ('Shelfmark (#)', 'Fast search for shelfmarks (e.g., "T-S NS 13.15").'),
            ('\U0001F195 PGP Tags', 'Browse manuscripts by topic tags from the Princeton Geniza Project (PGP). See [PGP Data](#help-pgp) below.'),
        ]

        with ui.column().classes('gap-3 mb-4'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2'):
                    ui.label(f'\u2022 {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

        ui.markdown('''
The **Search History** button (clock icon) near the **Search** button lists your recent searches. Clicking one restores its query, its search mode (with its variant level), **Gap** and **Text Position**, and its domain, author, work, date range, **Exclude Printed** and **Text Filter** choices under **Search only in...**, then runs the search again (an older entry that saved its results shows them instead). Other options — for example **Exclude Words** and the Responsa-style checkboxes — stay as they are.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Text Position (for join detection)', classes='text-lg font-semibold mb-2 mt-2', style='color: var(--text-primary);')
        ui.markdown('''
The **Advanced Options** include a **Text Position** selector \u2014 *Anywhere*, *Start of text*, *End of text*, *Line starts*, or *Line ends* \u2014 that works in **every search mode**. It is especially useful for **finding joins** between fragments. For example, if you have part of a phrase at the start of a torn fragment, you can search for the rest of the phrase restricted to the *start* of fragments \u2014 or the *end* of fragments if you are looking for the preceding page of the manuscript. Responsa mode adds finer per-word and per-line control (see [Responsa-Style Search](#help-responsa) below).

Also under **Advanced Options**, **Exclude Words** hides results containing any of the words you enter (separated by spaces). This is separate from excluding whole manuscripts (see [Exclude Known Manuscripts](#help-exclude-manuscripts) below).
        ''').style('color: var(--text-secondary);')

    # === Responsa-Style Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-responsa"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('tune').classes('text-2xl text-primary')
            h2('Responsa-Style Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This mode offers two search methods inspired by the Bar-Ilan Responsa Project: a textual syntax with operators for prefixes, suffixes, plene/defective spelling and more; and an intuitive tabular search interface that builds the query for you.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        ui.markdown('''
**Activation:** Select **Responsa (R)** from the search Mode dropdown, or type `R ` (R followed by a space) at the beginning of your query.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Syntax', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        syntax_data = [
            ('#word', 'Prefixes (24 forms: \u05d5, \u05d4, \u05d1, \u05db, \u05dc, \u05de, \u05e9 + compounds)', '#\u05e9\u05dc\u05d5\u05dd \u2192 \u05d5\u05e9\u05dc\u05d5\u05dd, \u05d4\u05e9\u05dc\u05d5\u05dd, \u05d1\u05e9\u05dc\u05d5\u05dd...'),
            ('word#', 'Suffixes (25 forms: \u05d9, \u05d5, \u05dd, \u05df, \u05d4, \u05da, \u05db\u05dd, \u05db\u05df...)', '\u05e9\u05dc\u05d5\u05dd# \u2192 \u05e9\u05dc\u05d5\u05de\u05dd, \u05e9\u05dc\u05d5\u05de\u05d5, \u05e9\u05dc\u05d5\u05de\u05da...'),
            ('#word#', 'Both prefixes + suffixes (all 600 combinations)', '#\u05e9\u05dc\u05d5\u05dd# \u2192 all combinations'),
            ('word*', 'Any letters after the word, none included', '\u05e9\u05dc\u05d5\u05dd* \u2192 \u05e9\u05dc\u05d5\u05dd, \u05e9\u05dc\u05d5\u05de\u05d5\u05ea, \u05e9\u05dc\u05d5\u05de\u05e0\u05d5...'),
            ('wo*rd', 'Any letters inside the word, none included, as many * as you like', '\u05db*\u05e0\u05d9\u05dd \u2192 \u05db\u05d4\u05e0\u05d9\u05dd, \u05db\u05d5\u05d4\u05e0\u05d9\u05dd, \u05db\u05e0\u05d9\u05dd...'),
            ('*word', 'Any letters before the word, none included', '*\u05e0\u05d9\u05dd \u2192 \u05db\u05d4\u05e0\u05d9\u05dd, \u05d1\u05e0\u05d9\u05dd, \u05e0\u05d9\u05dd...'),
            ('%word', 'Plene/defective spelling (insert/remove \u05d5/\u05d9)', '%\u05e9\u05dc\u05d5\u05dd \u2192 \u05e9\u05dc\u05d5\u05dd, \u05e9\u05dc\u05dd'),
            ('(a/b)', 'OR alternatives', '(\u05e9\u05dc\u05d5\u05dd/\u05e9\u05dc\u05d5\u05de\u05d5\u05ea)'),
            ('[N]', 'Gap of N words', '\u05e8\u05d0\u05d5\u05d1\u05df [3] \u05e9\u05de\u05e2\u05d5\u05df'),
            ('[|N]', 'Gap of N lines (between line groups)', '\u05e8\u05d0\u05d5\u05d1\u05df [|2] \u05e9\u05de\u05e2\u05d5\u05df'),
        ]

        with ui.element('table').classes('w-full mb-4').style('border-collapse: collapse;'):
            with ui.element('thead'):
                with ui.element('tr'):
                    for header in ['Syntax', 'Meaning', 'Example']:
                        with ui.element('th').style('padding: 6px 10px; border-bottom: 2px solid var(--primary-300); color: var(--text-primary); text-align: left;'):
                            ui.label(header).classes('font-bold text-sm')
            with ui.element('tbody'):
                for syntax, meaning, example in syntax_data:
                    with ui.element('tr').style('border-bottom: 1px solid var(--border-color, #e0e0e0);'):
                        with ui.element('td').style('padding: 4px 10px; white-space: nowrap;'):
                            ui.label(syntax).classes('font-mono font-bold text-sm').style('color: var(--primary-700);')
                        with ui.element('td').style('padding: 4px 10px; color: var(--text-secondary);'):
                            ui.label(meaning).classes('text-sm')
                        with ui.element('td').style('padding: 4px 10px; color: var(--text-tertiary, #888);'):
                            ui.label(example).classes('text-sm font-mono')

        ui.markdown('Modifiers can be combined, e.g. `#%word*` = prefix expansion + plene variants + wildcard suffix.').style('color: var(--text-secondary);').classes('mb-2')

        ui.markdown('*Whole words:* every word of the query is matched as a whole word, the first and the last too: `\u05db\u05d4\u05e0\u05d9\u05dd \u05d2\u05d3\u05d5\u05dc` does not find `\u05db\u05d4\u05e0\u05d9\u05dd \u05d2\u05d3\u05d5\u05dc\u05d9\u05dd`. Brackets inside a word of the text do not get in the way: `\u05e9\u05dc\u05d5\u05dd` finds `\u05e9\u05dc[\u05d5]\u05dd`.').style('color: var(--text-secondary);').classes('mb-2')

        ui.markdown('*Stars:* a `*` stands for any letters inside one word, none included -- never a space, a digit or punctuation: `\u05de\u05d0\u05de* \u05e7\u05d5\u05e8*` finds `\u05de\u05d0\u05de\u05ea\u05d9 \u05e7\u05d5\u05e8\u05d9\u05df`, and spelling variants and Judeo-Arabic apply to the letters around it. A `*` can stand anywhere in a word, as often as you like: `*\u05e0\u05d9\u05dd *\u05dc\u05d9\u05dd` finds `\u05db\u05d4\u05e0\u05d9\u05dd \u05d2\u05d3\u05d5\u05dc\u05d9\u05dd`, and `*\u05db\u05d4\u05e0*` finds every word that holds `\u05db\u05d4\u05e0`. A `*` alone matches every word: for any one word, use a gap such as `[1]`. A word with only one or two letters besides its `*` (`\u05db\u05d4*`) matches very many words, so its list may end with "+".').style('color: var(--text-secondary);').classes('mb-2')

        ui.markdown('*Across pages:* matches that run from one page into the next are found too, read from the pages that follow each other in the manuscript. When a search with `*` cannot read all of them in good time, the count shows "+" and the results line says which were not searched.').style('color: var(--text-secondary);').classes('mb-4')

        h3('Sub-Options', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
- **Variants**: Enable letter-variant matching on all expanded terms
- **Judeo-Arabic (JA)**: Expand words with the Arabic definite article \u05d0\u05dc- (8 forms per word)
- **Flex Spacing**: Ignore erroneous spaces within words \u2014 very useful given the many spacing errors in automatic transcription, but adds load to the query
- **Bidirectional Gap**: Search for terms in both forward and reverse order
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Tabular Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
1. Click the **Tabular Search** button (visible when Responsa mode is active)
2. Add 2\u20134 **components**, each representing a search term or group
3. Enter one or more **words** per component (multiple words = OR alternatives)
4. Toggle **per-word modifiers**: prefix (#), suffix (#), wildcard (*), plene (%), negation
5. Set the **distance** between components using the spinners, or choose **Within Document** to require every component anywhere in the transcription, in any order (`word1 AND word2`)
6. Watch the **live preview** update in real time
7. Click **Search** to execute the query
        ''').style('color: var(--text-secondary);').classes('mb-4')

        ui.markdown('*Note:* When a query expands beyond 500 terms, the system automatically downgrades options (variants, Judeo-Arabic, plene, etc.) to maintain speed, and displays a notification accordingly.').style('color: var(--text-secondary);')

        h3('Line & Text Position Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
In addition to the **Text Position** selector available in every mode (see [Search](#help-search) above), **Responsa mode** supports fine-grained line positioning **per word** and **per line**:
- **|word** — the word must appear at the **start** of a line
- **word|** — the word must appear at the **end** of a line
- **|** (between words) — a **line break**: the following words must start on a new line
- **[|N]** — a **gap of N lines**: skip N lines between line groups. For example `ראובן [|2] שמעון` finds *ראובן*, then *שמעון* **two lines later**.

These positional operators are ideal for **join detection** — build a multi-line query matching how one fragment ends and how the joining fragment begins. The **Tabular Query Builder** provides a visual interface for constructing them.
        ''').style('color: var(--text-secondary);')

    # === Focused Search ("Search only in...") ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-filters"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('filter_list').classes('text-2xl text-primary')
            h2('Focused Search ("Search only in...")', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Use the **Search only in...** panel (available on both Search and Parallels pages) to narrow results by manuscript properties before searching. This focuses your search on specific subsets of the corpus.

**Available Filters:**
- **Domain:** Filter by scholarly classification (e.g., Bible, Talmud, Poetry)
- **Author:** Filter by attributed author
- **Work:** Filter by specific work title
- **Date Range:** Filter by manuscript dating
- **Material:** Filter by material type (manuscript vs. printed)

**How it works:**
- Open the collapsible **Search only in...** panel above the search results
- Select one or more filters — the manuscript count updates in real time
- Active filters appear as removable **chips** above the results
- Filters apply to all search modes (Exact, Variants, Responsa, etc.)
- On the Browse page, domain and author labels link directly to a filtered search

**Filter by library:** after searching, the **Filter by library** button in the results bar opens a window where you can **Show only selected** libraries or **Hide selected** libraries (e.g. CUL, JTS). Your choice is kept for later searches.
        ''').style('color: var(--text-secondary);')

    # === Search Within Results ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-search-within"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('filter_list').classes('text-2xl text-primary')
            h2('Search Within Results', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
After running a search, click **"Search within N manuscripts"** in the results header to restrict your next query to only the manuscripts found in the current result set. This lets you progressively narrow results through multiple refinement steps.

**How it works:**
- Search for a term (e.g., "חנוכה") — results show all matching manuscripts
- Click "Search within N manuscripts" — a badge appears on the search bar
- Type a second term (e.g., "נרות") and search — results are restricted to manuscripts from the first search
- A tag strip shows your refinement chain: [חנוכה] › [נרות]
- Click × on any tag to remove that step and all subsequent steps
- Click "Clear all" to return to unrestricted search

**"Only results with all terms" checkbox:** When your chain has two or more steps, a checkbox appears on the tag strip. Checking it filters the display to show only pages from manuscripts that appeared in every step's results.

**Cross-mode refinement:** You can mix search modes freely — for example, search by Shelfmark, then refine with a text search, then narrow further with Responsa syntax. The restriction always operates at the manuscript level regardless of mode.

**Note:** The restriction works at the manuscript level — if a manuscript contains "חנוכה" on page 1 and "נרות" on page 5, both pages may appear in refined results. Use the "Only results with all terms" checkbox to see only pages from manuscripts that matched all queries.
        ''').style('color: var(--text-secondary);')

    # === Exclude Known Manuscripts ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-exclude-manuscripts"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('person_remove').classes('text-2xl text-primary')
            h2('Exclude Known Manuscripts', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Click **Exclude known manuscripts** in the "Search only in..." panel (before searching) or in the **Filter Results** panel (after searching) to hide specific manuscripts from search results. Three methods are available:

**Paste shelfmarks:** The default tab — paste a list of shelfmarks (one per line). Lines starting with # are ignored. Click Apply to resolve and exclude.

**From List:** Expand any saved list to see individual manuscripts with checkboxes. Check entire lists or specific items. Multiple lists can be selected simultaneously.

**From File:** Upload a TXT file (one shelfmark per line) or CSV file (shelfmark column auto-detected). A resolution report shows per-row status — found, not found, or duplicate — before you apply.

**Managing exclusions:**
- Active exclusions appear as red chips in the results bar, one per source with the number of manuscripts it hides; click a chip to clear that source
- A collapsible "Excluded manuscripts" section at the bottom of results shows what was hidden and why
- Exclusions persist across searches and page navigation within your session
- Exports (Excel/Word) only include visible manuscripts — excluded items are never exported
- Exclusions are independent of "Search within results" refinement — they don't affect each other
        ''').style('color: var(--text-secondary);')

    # === Translations ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-translations"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('translate').classes('text-2xl text-primary')
            h2('Catalog & Metadata Translations', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Catalog data, titles, and scholarly descriptions are available in both Hebrew and English, powered by machine translation via [Dicta Translation](https://translate.dicta.org.il/).

**Toggle:** Use the switch in the sidebar footer (it reads **Translations OFF** or **Translations ON**) to enable translated descriptions in search results, browse views, and catalog dialogs.

When enabled, translated text appears with a clickable **Translated/Original** badge — click to toggle between the translated and original text.
        ''').style('color: var(--text-secondary);')

        ui.markdown('''
> **Important:** Translations are machine-generated scholarly aids and may contain errors, including incorrect terminology, hallucinated content, or inconsistent transliterations. Always verify against the original text for research purposes. If you encounter a problematic translation, click the small flag icon next to the translated text to help us improve quality.
        ''').style('color: var(--text-secondary); background: var(--bg-tertiary); border-left: 3px solid var(--warning, #f59e0b); padding: 8px 12px; border-radius: 4px; margin-top: 4px;')

    # === Find Parallels ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('Find Parallels', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This tool is designed for researchers wishing to find **parallel texts** for a complete literary composition
(such as a Piyyut, medieval commentary, or other rare work) within the Genizah, thereby locating additional
textual witnesses\u2014both direct and indirect.

(The desktop app calls this tool Composition Search.)
        ''').style('color: var(--text-secondary);').classes('mb-4')

        if passage_available():
            h3('Choosing a Search Method', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
            ui.markdown('''
The page offers two engines, chosen with the radio buttons above the text box.

**Letter-level search is the default, and it is the one to use unless you need
something specific from the other.** It compares your text to the corpus as a
stream of letters rather than as words, so differences in spelling, word
division, punctuation and transcription do not break a match. It answers in
about a second where the older method takes minutes, and in a graded comparison
of live runs it returned far fewer irrelevant results.

**Chunk search** is the older method, kept because it can do things
letter-level search cannot: word-level Exact / Variants / Fuzzy modes,
cross-paragraph filtering, and a tunable chunk size. It is much slower.

Two differences people expect but will not find: **both methods ignore
nikkud**, and **both treat a line break as an ordinary space**. Neither is a
reason to pick one over the other.
            ''').style('color: var(--text-secondary);').classes('mb-4')

            h3('How Letter-level Search Works', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
            ui.markdown('''
Your text and the whole corpus are reduced to a stream of Hebrew base letters:
final forms are folded to their normal form, and everything else \u2014 nikkud,
cantillation, punctuation, digits, Latin characters and all whitespace \u2014 is
dropped. The engine then looks for the longest passages the two streams share.

Because it never sees a word boundary, it is unaffected by the things that most
often break a word-based search of manuscript text: a scribe who splits or joins
words differently, a transcription that reads one letter wrong, a line break in
the middle of a word.

Three controls shape the result:

- **Match width** \u2014 how close a passage must be to count. Narrow finds
  near-identical text; wider settings admit more variation and more noise.
- **Passage length** \u2014 whether a short shared passage also counts.
  **Normal passages (default)** keeps only substantial overlap;
  **Also short passages** adds formulaic phrases, quotations and badly damaged
  copies, at roughly double the results for a third fewer correct ones.
- **Search depth** \u2014 how hard the engine looks. Deeper settings find more and
  cost proportionally more time (roughly one second, six seconds and twenty
  seconds per search).
            ''').style('color: var(--text-secondary);').classes('mb-4')

        if passage_multi_witness_available():
            h3('Searching With Several Witnesses', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
            ui.markdown('''
One copy of a work is rarely enough to find all the others. If you have several
manuscript copies of the same composition, the **Witnesses** panel lets you
search with all of them at once and merges the results into one list.

Each witness is searched **separately** and the lists are then combined by rank.
They are never joined into one long query \u2014 that measurably finds *less* than
the best single witness would, because each search has a fixed budget to spend
and a long query spreads it too thin.

**Adding witnesses** \u2014 paste them with **Add witness text** (tick the box if
your paste holds several copies separated by blank lines and they will be split
apart), or tick results you already have and press **Search with these too**.
**Auto-expand** repeats that promotion for you over several rounds.

**What it costs.** Reach goes up and the top of the list gets noisier: more
manuscripts are found, but more of the first page is irrelevant. The sort
control reorders by combined score, by best single match, or by how many
witnesses point at each manuscript \u2014 the last is often the quickest way to see
which results are well attested.

**Witness quality varies enormously.** In one measured set of ten manuscript
witnesses, six found nothing at all and added thousands of junk rows, while the
best found nineteen genuine parallels. Each witness shows how many matches it
contributed, and removing it takes its rows with it.
            ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('How Chunk Search Works', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
        ui.markdown('''
Unlike a regular search, the engine does **not** search for the entire text as a single unit. The process works as follows:

1. **Chunking:** The software splits your source text into small segments ("chunks") of N words each.
2. **Individual Search:** Each chunk is searched separately in the Genizah database.
3. **Scoring:** If a specific chunk is found in a manuscript, it receives a "score" based on match quality.
4. **Aggregation:** At the end of the process, the software **aggregates** the results\u2014if a manuscript contains many matching chunks, it receives a high score and appears at the top of the list.

You can also search in Lab mode, using an algorithm based on the **Shmidman-Koppel-Porat fingerprinting method**, which encodes Hebrew words into normalized "fingerprints" that allow matching despite spelling variations common in medieval manuscripts.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Important Parameters', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
        ui.markdown('''
These belong to **chunk search** and Lab mode. Letter-level search has its own
three controls, described above.

- **Chunk Size:** The number of words in each search unit. A low value (2\u20133) will result in slower search and many irrelevant results; a high value (10+) may miss true matches.
- **Search Mode:** Like regular search\u2014Exact, Variants, or Fuzzy.
- **Variant Level / Num Changes:** Controls flexibility of letter substitutions (see Search Modes above).
- **Deep Scan:** Relevant for Lab mode. A much deeper and more thorough scan, significantly slower but recommended for finding rare phrases.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Filtering Known Sources', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
A powerful and recommended feature for reducing "noise" in your results. If your source text quotes Bible verses, Mishnah,
Talmud, or other known texts, you can **load these sources** so matches found in them are filtered separately.

**How to use:**
1. Expand the **"Filter text (exclude known sources)"** panel
2. Click **Tanakh**, **Mishnah**, or **Talmud** to load standard sources from Sefaria
3. Or click **More Sources...** to browse the full Sefaria library
4. Or click **Search Sefaria** to load any text by reference (e.g., "Rashi on Genesis 1")
5. Or click **Add Custom Text** to paste your own reference text

Matches found in your filter texts appear in a separate **"Excluded Results"** section, so you can focus on new parallels. The texts will automatically load in your next search as well, until you remove them.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Cross-Paragraph Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
When searching for parallels to a text that contains paragraph breaks (e.g., a piyyut with stanzas, or a text with section divisions),
you can enable **cross-paragraph search** to specifically find manuscripts that preserve text spanning across these boundaries.

**Why is this useful?**
- Text **within** paragraphs often contains citations from other sources or sources that quote the composition you're searching for
- Text that **crosses** paragraph boundaries is much less likely to be a citation, since citations rarely span across structural breaks
- This effectively filters out most of the "noise" and helps you find genuine textual witnesses

**How to use:**
1. Enter your text with paragraph breaks (or set a custom delimiter like period or colon)
2. Select a search mode: **Full search** (all results), **Cross-paragraph only** (only matches that cross boundaries),
   or **Full + Cross-paragraph boost** (all results, with boundary-crossing matches ranked higher)
3. Results that cross paragraph boundaries are marked with a special indicator
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Understanding Results', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
Results are **grouped by manuscript** and sorted by score:
- **Max Score:** The highest-scoring match found in that manuscript
- **Avg Score:** Average score across all matches in the manuscript

What the score counts depends on the method: in **letter-level search** it is
the number of letters the two texts share in that match, so a longer shared
passage scores higher. In **chunk search** it reflects the quality of a matching
chunk. The two are not comparable to each other, and a result list only ever
contains one method's scores.

Click on a result to expand and see your source text alongside the matching
manuscript text, with the matching part **highlighted** for easy comparison.

Once results arrive, **Filter by library** lets you either **Show only selected**
libraries or **Hide selected** ones. Your choice is kept for later searches.
        ''').style('color: var(--text-secondary);')

    # === Joins Lab ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-joins-lab"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('join_inner').classes('text-2xl text-primary')
            h2('Joins Lab', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
The **Joins Lab** is a dedicated workspace for hunting **physical joins** — fragments once part of the same leaf or codex, now scattered across collections. You pin one fragment as the **anchor**, then search for and triage candidate fragments against it, side by side. It works **without logging in** (saving a join or adding to a list does require login).

**Opening the Joins Lab** — the **Joins Lab** link in the sidebar (or go directly to `/joins-lab`); the **Find joins** button on **Browse**; or from any search result (opens the Lab anchored on that page).

**The anchor pane** — the pinned fragment: its shelfmark, brief metadata, and page image. Use the folio arrows to page through its images and zoom in to inspect the script. **Known joins** (yours or the community's) appear in a collapsible section.

**Building a query** — the default search mode is **Responsa-style line search**, best suited for joins since you describe the joining fragment line by line. Other modes (exact, variants, fuzzy) are available from the search-type selector. Build a line-by-line query for the text you expect on the joining fragment:

- **+ Add Line** — add another manuscript line, and set how many lines to skip before it (0 = the very next line).
- **+ or** — add an alternative word that may appear instead on the same line.
- **Word options (⚙)** — on each word's own gear icon: negation (−), plene/defective spelling (%), prefixes (`#_`), suffixes (`_#`), wildcards (`*_` / `_*`). Separately, the **⊢** / **⊣** buttons on each line anchor that line to the start or end of a manuscript line.
- **Variants**, **Flexible spacing**, **Bidirectional** — checkboxes under the builder that apply to the whole query (Responsa-style mode only).
- **Text Position** — restrict a line to the start or end of the page text.
- **Other side of the leaf (p ±1)** — also require or include text on the adjacent page; **Narrow** keeps only candidates whose adjacent page also matches, **Widen** adds adjacent pages as extra candidates.
- **Visual Similarity** — show only fragments that look alike, optionally combined with a text query.

> ⚠ **About visual similarity:** the visual-similarity index covers only about **half of the Genizah**, and the algorithm is not conclusive — so it can miss real matches. **Do not assume that a fragment absent from the visual-similarity results is not a join.** Use it to surface candidates, never to rule a fragment out.

Click **Find Candidates** to run the query.

> 💡 **Tip — torn phrases & verses:** A tear often splits a known phrase or verse between the two joining fragments. Search for the *missing* half and pin it to the torn edge:
> - If a line on the anchor **starts** with the second half of a phrase (the first half is torn away), search for that **first half** with the line's **⊣** button ("Line ends here") — on the join it will sit at a line's end.
> - If a line **ends** with the first half of a phrase, search for the **second half** with the **⊢** button ("Line starts here").
> - You can also set **Text Position** to **Start of text** / **End of text** so only candidates with that text at the page's torn edge are returned.

**Triaging candidates** — candidates appear as cards with their image and metadata. The buttons on each card (named here by their tooltips):

- **✓** / **?** / **✗** — **Mark yes** / **Mark maybe** / **Mark no**.
- **View in Browse** — open the fragment on the Browse page.
- **Compare fragment** — view it side by side with the anchor (each pane zooms and pages independently).
- The pin icon, **Pivot the workbench: make this fragment the new anchor**.
- **Add as Join** (login) — record a confirmed join.
- **Add anchor + this candidate to the Fragment Puzzle**.

To act on several candidates at once, tick their checkboxes (**Select for bulk actions**): **Add to List** (login) and **Add to Puzzle** then appear next to **Export**. The filter button (**Filter candidates**) opens **Filters**: **Material**, **Has dimensions data**, **Exclude size mismatch** (hides candidates whose width differs from the anchor's by a factor of more than 1.4), **Triage state**, and a box that matches text in the shelfmark or title. **Table** switches to a compact table for many results. Your anchor, query, candidates and triage **survive a page refresh**; **Reset** clears them.
        ''').style('color: var(--text-secondary);')

    # === PGP Information ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-pgp"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('school').classes('text-2xl text-primary')
            h2('Princeton Geniza Project (PGP) Data', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
The system integrates data from the Princeton Geniza Project (PGP)\u2014a scholarly database containing approximately 36,000 cataloged documents with transcriptions, translations, descriptions, and detailed subject tagging.

**PGP Badge in Search Results**

Manuscripts with available PGP data are marked with a green "PGP" badge in search results, so you can quickly identify manuscripts with scholarly transcriptions and additional research information.

**PGP Information in Browse by Shelfmark**

When a manuscript has PGP data, an information panel is displayed showing:
- **Document type** and languages (e.g., Letter, Judeo-Arabic)
- **Subject tags** \u2014 clicking a tag searches for all manuscripts with that topic
- **Description** in English (with translation option)
- **Dating** (including rationale if available)
- **Link to PGP** to view the original document on the Princeton website

**Transcriptions and Translations**

When scholarly transcriptions or translations from the Princeton project are available, they appear in the version selector alongside the automatic transcription. The system automatically prefers a PGP edition (if available) over the automatic reading.

**Search by Tags**

Select **PGP Tags** from the search Mode dropdown to browse manuscripts by topic. Tags are organized into categories (Document Types, Law & Society, Medicine, Trade & Travel, and more) and displayed in a dropdown with Hebrew translations.
        ''').style('color: var(--text-secondary);')

    # === Reading Desk ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-reading-desk"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('Reading Desk', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
A side-by-side view that allows you to examine multiple manuscripts together, with synchronized images and a version selector for each fragment.

The Reading Desk is useful for any researcher who wants to view multiple shelfmarks together\u2014whether they are fragments that join into a single document (according to PGP or other joins) or any collection of manuscripts you wish to examine side by side.

**How to use:**
- Click the green **Add to Reading Desk** icon in the toolbar of the Browse by Shelfmark page
- Add more manuscripts from search results or from browsing other manuscripts
- Each fragment is displayed with its source image, version selector (including PGP editions if available), and extended information
- Click **Back to Page View** when done
        ''').style('color: var(--text-secondary);')

    # === Fragment Puzzle ===
    _puzzle_card = ui.card().classes('w-full p-6')
    _puzzle_card.set_visibility(WEB_PUZZLE_ENABLED)
    with _puzzle_card:
        ui.element('a').props(f'name="help-puzzle"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('extension').classes('text-2xl text-primary')
            h2('Fragment Puzzle', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
A visual canvas for arranging manuscript fragment images side by side to reconstruct physical joins between Genizah fragments.

**Adding Fragments:**
- Type a shelfmark in the input box and press Enter or click the add button
- From a personal list: open the **Lists** dropdown and select fragments to add
- From known joins: when PGP or FJMS joins are available for a fragment, click the joins button to load all related fragments at once

**Canvas Controls:**
- **Zoom:** Scroll to zoom in/out on the canvas; double-click to reset view
- **Rotate:** Select a fragment and use the rotate controls (or keyboard arrows) to fine-tune the angle
- **Background removal:** Adjust the threshold slider to remove parchment/paper background from fragment images, isolating the text
- **Background modes:** Cycle through canvas backgrounds (dark, black, white, checkerboard, parchment, grid) to find the best contrast for your fragments
- **Crop:** Trim away empty margins from a fragment image
- **Flip:** Mirror a fragment horizontally or vertically
- **Folio navigation:** Use Previous/Next page controls to switch between recto and verso, or navigate to other folios of the same manuscript

**Layer Controls:**
- **Bring Forward / Send Backward:** When fragments overlap, use these controls to change the stacking order so you can position fragments on top of or behind each other

**Fragment Selector:**
- Use the combobox above the canvas to select a loaded fragment; the **Browse** button opens that fragment in the Browse by Shelfmark page

**Saving & Loading:**
- Click the **Save** button (💾) to save the current arrangement as a join document with a title and optional notes
- After the first save, changes are auto-saved as you move, rotate, or resize fragments
- Click the **Open** button (📂) to browse your saved joins; each entry shows a thumbnail preview
- Select a saved join to load it back onto the canvas

**Export:**
- Click the **Export PNG** button (🖼️) to download a composite PNG of all fragments as arranged on the canvas
- The exported image includes a metadata banner listing all fragment shelfmarks

**Recto/Verso:**
- The Flip Puzzle button mirrors the entire canvas arrangement and navigates each fragment to its verso (or recto), so you can examine the reverse side of a reconstructed join
        ''').style('color: var(--text-secondary);')

    # === Community Publishing ===
    _community_card = ui.card().classes('w-full p-6')
    _community_card.set_visibility(WEB_PUZZLE_ENABLED)
    with _community_card:
        ui.element('a').props(f'name="help-community-publish"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('publish').classes('text-2xl text-primary')
            h2('Community Publishing', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Share your puzzle join reconstructions with the research community and browse joins published by other scholars.

**Publishing a Join:**
- After saving a puzzle join, click the **Publish** button (📤) in the toolbar
- When published, the button turns green and a share link dialog appears with a **Copy** button so you can send the link to colleagues
- Your arrangement is uploaded as a composite image along with metadata (title, notes, fragment list)

**Unpublishing:**
- Click the green Publish button again to remove your join from the community feed
- Deleting a local join document also automatically removes it from the community if it was published

**Browsing Published Joins:**
- The **Discoveries Center** (**Community** in the menu) shows published puzzle joins from all users, displayed with thumbnail previews
- Use the type filter to show only **Puzzle Joins** in the feed
- Click **View Details** to see full-resolution image, notes, and fragment list

**Opening a Published Join:**
- Click **Open in Puzzle** on any published join to fork a copy into your local workspace
- This creates an independent copy you can modify without affecting the original publication

**Community Puzzle Joins Panel:**
- When browsing a manuscript, a panel shows any published joins that contain that fragment
- This helps you discover existing reconstructions relevant to the manuscript you are studying
        ''').style('color: var(--text-secondary);')

    # === Browse by Shelfmark ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('Browse by Shelfmark', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This page enables convenient continuous reading of a full manuscript, synchronized with source images. The page itself is headed **Browse Manuscripts**.

**Loading a Manuscript:** Enter a **Shelfmark** in the search box. The search is flexible and ignores spaces/punctuation (e.g., `TS NS 13 15` finds `T-S NS 13.15`).

**Features:**
- **Images:** An image viewer displays the manuscript page. You can zoom, rotate, and view in full screen
- **Page Navigation:** Use the arrows or page dropdown to navigate between pages
- **View All:** Click to display all manuscript pages in one long scrollable view
- **Search for Parallels:** Send the current page text to **Find Parallels**
- **Ktiv:** Opens the manuscript in the National Library of Israel's online catalog
- **Edit & Comment:** Submit corrections or add scholarly comments for the benefit of the entire research community, or for yourself (requires login)
- **PGP Info:** If Princeton Geniza Project data is available, an information panel is displayed with transcriptions, description, tags, and dating (see [PGP Data](#help-pgp) above)
- **Volume Selector:** Manuscripts with multiple microfilm scans (~1.5% of the collection) now show a volume dropdown. Previously, images could be mismatched with the text — the volume selector fixes this by displaying each volume's own images and transcription, with page navigation staying within the active volume
        ''').style('color: var(--text-secondary);')

    # === Browse by Identification ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-catalog-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('category').classes('text-2xl text-primary')
            h2('Browse by Identification', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Browse the scholarly catalog by domain classification, author, or work title.

**Domain Hierarchy:** The left panel shows the domain tree (e.g., Bible > Torah > Genesis). Click a domain to see all manuscripts classified under it. Counts show how many manuscripts belong to each category.

**Author & Work Filters:** Use the search dropdowns to filter by author name or work title. Filters combine — selecting a domain and an author shows only manuscripts matching both.

**Text Filter:** Type keywords to search across catalog titles, descriptions, and domain names. Choose ALL (all terms must match), ANY (any term matches), or NOT (exclude matching terms). Add multiple terms as color-coded chips.

**Features:**
- **Filter chips:** Active filters appear as removable chips above the results
- **Pagination:** Results display 50 per page with navigation controls
- **Deep linking:** The URL updates with your filter selections for bookmarking and sharing
- **Cross-links:** Domain and author labels on the Browse by Shelfmark page link directly here with the appropriate filter pre-set
        ''').style('color: var(--text-secondary);')

    if discovery_available():
        _render_computed_identifications_help('en')
        # BAND-05 methods section (Phase 135, D-10; qualitative per D-06a),
        # RESTORED 2026-08-13 and placed BELOW the practical card deliberately:
        # REL-01 gates the public launch on this report being published and
        # CERT-02 names it as where each tier's unit and status are recorded, so
        # it has to exist -- but its audience is someone auditing the method,
        # and a first-time reader needs the practical guidance first.
        #
        # Codex #11: this body is its OWN conditional card, never part of the
        # TOC render loop, so it carries this gate rather than inheriting one.
        _render_confidence_section('en', precision, band_counts)

    if atlas_preview_available():
        _render_visual_atlas_help('en')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('My Lists', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Save important manuscripts to personal lists for later reference.

**Creating Lists:** Click the \u2b50 star icon on any search result, parallel match, or browse page to add it to a list. Create new lists to organize your research by topic, project, or any other criteria.

**Managing Lists:**
- View all your lists on the **My Lists** page (its own heading reads **Personal Lists**)
- Add notes to individual items
- Export lists to Excel format
- Lists sync across devices when logged in

**Projects:** Group related lists into **Projects** for better organization. Each project can have its own color coding.
        ''').style('color: var(--text-secondary);')

    # === Export ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-export"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('download').classes('text-2xl text-primary')
            h2('Exporting Data', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
At any stage, you can export results for external use:

- **📊 Excel (XLSX) — Research workbook:** a 4-sheet citation-ready
  workbook:
  - **Search Results** — your hits with snippet, full text, PGP and
    Printed flags, Domains, and a clickable image link per row
  - **Manuscripts** — one row per unique manuscript with clickable PGP,
    Library Viewer, and GenizahSearch URLs
  - **Bibliography** — one row per bibliography entry, linked to
    Manuscripts by System ID
  - **Credits and Info** — query, search mode, gap setting, timestamp,
    and result count
- **📄 Word (DOCX):** formatted report suitable for academic prose
- **JSON:** programmatic format. The Search Results JSON carries the same
  enrichment fields as the Excel workbook (`has_pgp`, `is_printed`,
  `domains` per item); the Parallels JSON has no `has_pgp`/`is_printed`.
  The public API (see **Public API & AI Tools** below) returns `domains`
  but not `has_pgp`/`is_printed`.

Headers and sheet titles appear in Hebrew or English, matching your UI
language.

**Export locations:**
- **Search Results:** Export buttons above the results table
- **Parallels Results:** Export buttons in the results header
- **Lists:** Export individual lists from the **My Lists** page
        ''').style('color: var(--text-secondary);')

    # === Public API & AI Tools ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-api"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('api').classes('text-2xl text-primary')
            h2('Public API & AI Tools', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
GenizahSearch exposes a public HTTP/JSON API for researchers and AI
tools that need programmatic access to the corpus.

**Endpoints:**
- `POST /api/search` — text search across the corpus
- `GET /api/browse` — fetch transcription, metadata, and image for a
  manuscript
- `POST /api/parallels` — find Genizah manuscripts containing chunks
  matching a long source text

**OpenAPI schema:** [`/api/openapi.json`](/api/openapi.json) describes
the full API.

**Cairo Genizah Research skill for Claude:** a ready-to-use
[Claude skill](https://github.com/gershuni/GenizahSearch/tree/master-main/skills/cairo-genizah-research)
drives this API to find candidate witnesses for a phrase, piyyut,
responsum, letter, or composition, returning a tiered ranked list
with browse text, library attribution, and image URLs.

**AI tools page:** the **[AI tools](/ai)** page in the menu lists the ways
currently available to use GenizahSearch from your own AI assistant (such as
ChatGPT, Claude or an MCP client), with setup instructions.
        ''').style('color: var(--text-secondary);')

    # === My Library — Local Documents (Phase 95 D-31 + D-32 + D-33) ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props('name="help-my-library"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('folder_open').classes('text-2xl text-primary')
            h2('My Library — Local Documents', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
In the **[downloadable desktop app](/download)** you can run all the search modes over your own local files too (`.docx`, `.pdf`, `.txt`, `.html`, `.xlsx`, and `.csv`).

*My Library feature inspired by Yehuda Seewald's GenizahLocal prototype.*
        ''').style('color: var(--text-secondary);')

    # === Contact ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('mail').classes('text-2xl text-primary')
            h2('Feedback & Contact', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('For questions, bug reports, or feature requests:').style('color: var(--text-secondary);')
        ui.label('gershuni@gmail.com').classes('text-lg font-mono mt-2')


def _create_hebrew_content(precision=None, band_counts=None):
    """Create the Hebrew help content."""

    # === Table of Contents ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('list').classes('text-2xl text-primary')
            h2('תוכן העניינים', classes='text-xl font-bold', style='color: var(--text-primary);')

        with ui.column().classes('gap-2'):
            toc_items = [
                ('intro', 'הקדמה: איך זה עובד?'),
                ('search', 'חיפוש'),
                ('responsa', 'חיפוש בסגנון פרויקט השו"ת'),
                ('filters', 'חיפוש ממוקד ("חפש רק ב...")'),
                ('search-within', 'חיפוש בתוך תוצאות'),
                ('exclude-manuscripts', 'החרגת כתבי יד מוכרים'),
                ('translations', 'תרגומי קטלוג ומטא-נתונים'),
                ('parallels', 'מציאת מקבילות'),
                ('joins-lab', 'מעבדת צירופים'),
                ('pgp', 'מידע מפרויקט הגניזה של פרינסטון (PGP)'),
                ('reading-desk', 'שולחן עיון (Reading Desk)'),
                ('browse', 'עיון לפי מספר מדף'),
                ('catalog-browse', 'עיון לפי זיהוי'),
            ]
            if discovery_available():
                toc_items.append(('computed-identifications', 'זיהויים מחושבים (בטא)'))
                # BAND-05 methods section (D-10), restored 2026-08-13 -- see the
                # English half for why the loop-level guard is not restored.
                toc_items.append(('confidence', _CONFIDENCE_TOC_TITLE['he']))
            if atlas_preview_available():
                toc_items.append(('visual-atlas', 'אטלס הגניזה החזותי'))
            toc_items.extend([
                ('lists', 'הרשימות שלי'),
                ('export', 'ייצוא נתונים'),
                ('api', 'ממשק API ציבורי וכלי AI'),
                ('my-library', 'הספרייה שלי — מסמכים מקומיים'),
            ])
            if WEB_PUZZLE_ENABLED:
                # Their sections follow Reading Desk, so their entries do too.
                _after_desk = [k for k, _ in toc_items].index('reading-desk') + 1
                toc_items.insert(_after_desk, ('puzzle', 'פאזל קטעים'))
                toc_items.insert(_after_desk + 1, ('community-publish', 'פרסום לקהילה'))
            for anchor, title in toc_items:
                if not WEB_PUZZLE_ENABLED and anchor in {'puzzle', 'community-publish'}:
                    continue
                ui.link(f'• {title}', f'#help-{anchor}').classes('text-primary hover:underline').style('direction: rtl;')

    # === Introduction ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-intro"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('info').classes('text-2xl text-primary')
            h2('הקדמה: איך זה עובד?', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**חיפוש גניזת קהיר של דיקטה** מאפשר גישה מהירה ומתקדמת לקורפוס התעתוקים של פרויקט "מדרש" (MiDRASH).
הפלטפורמה מבוססת על מנוע חיפוש מהיר (Tantivy) ומשלבת אלגוריתמים ייחודיים לטיפול בחלק משיבושי הקריאה של אלגוריתם הפענוח של פרויקט מדרש.

**דרישת ייחוס:** תעתיקי מדרש משוחררים ברישיון CC-BY-4.0, ופירוש הדבר שניתן להשתמש בהם תוך ייחוס מתאים. לכן אם אתם משתמשים בתעתיקים, אנא תנו קרדיט ל:

> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments [Data set]. Zenodo.
> https://doi.org/10.5281/zenodo.17734473

האפליקציה מושכת מידע ותמונות מ:
- **הספרייה הלאומית של ישראל (NLI)**
- **ספריית הבודליאנה** באוקספורד
- **ספריית אוניברסיטת קיימברידג'**

*הערה:* חלק משרתי הספריות עשויים לחסום גישה מרשתות שאינן ביתיות (למשל, נקודה חמה מטלפון נייד).

**חדשים באתר?** בעמוד **[התחילו כאן](/start)** שבתפריט תמצאו נקודות פתיחה מודרכות: כתבי יד לעיון, חיפושים מוכנים לניסיון וכניסה לכלי המחקר.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-search"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('search').classes('text-2xl text-primary')
            h2('חיפוש', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('זוהי נקודת הכניסה לחיפוש טקסט חופשי או מטא-דאטה בקורפוס. ניתן להשתמש בקיצורי הדרך המופיעים בסוגריים להלן כדי להגיע ישירות לסוג החיפוש המעניין אתכם.').style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4 w-full')

        h3('מצבי חיפוש', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')

        modes_data = [
            ('מדויק (=)', 'מוצא רק את המילה או את רצף המילים בדיוק כפי שנכתבו. לחיפוש עם פערים בין המילים יש למלא את התיבה "מרווח" במספר הרצוי (עד 10). בחיפוש בסגנון פרויקט השו"ת ניתן לכתוב מרווח גדול יותר בתוך השאילתא עצמה, למשל `[25]`; במצב ביטוי רגולרי התיבה "מרווח" אינה בשימוש, והתבנית עצמה קובעת כל מרחק.'),
            ('וריאנטים (?)', 'מתחשב בחילופי אותיות נפוצים בטקסטים אלו (למשל: ד/ר, ה/ח, ו/י). בחרו רמה: **בסיסי**, **מורחב** (מוסיף זוגות כמו ק/כ ו-ט/ת) או **מרבי** (הגמישות הרחבה ביותר) — רמה גבוהה יותר מרחיבה את התוצאות אך איטית ורועשת יותר. **מס\' שינויים** קובע כמה אותיות בכל מילה אפשר להחליף (×1, ×2 או ×3); לכל רמה ערך משלה (ברירת מחדל: ×1 בבסיסי, ×2 במורחב ובמרבי). בהגדרות הכלליות ניתן להחליף את כפתורי הרמה במחוון.'),
            ('\U0001F195 פרויקט השו"ת (R)', 'חיפוש בתחביר בסגנון החיפוש המתקדם של פרויקט השו"ת של אוניברסיטת בר-אילן, עם הרחבת תחיליות/סיומות, תווים כלליים, חלופות כתיב ומרווחים. כולל גם בונה שאילתות טבלאי נוח וגמיש. מוכר למשתמשי פרויקט השו"ת; קל ללמוד גם למי שלא מכיר. ראו [חיפוש בסגנון פרויקט השו"ת](#help-responsa) להלן.'),
            ('מקורב (~)', 'מוצא כתיב קרוב, שימושי לשגיאות פענוח: מילים שלמות השונות מהמילה המבוקשת באות אחת (בשתיים במילים של 5 אותיות ומעלה) — אות שנוספה, נשמטה או הוחלפה, או שתי אותיות סמוכות שהתחלפו. צורות עם תחילית נכללות (ושלום עבור שלום); מילה שנמצאת רק בתוך מילה ארוכה יותר — לא.'),
            ('ביטוי רגולרי (/)', 'חיפוש מתקדם למשתמשים מנוסים. דוגמה: \\bא[א-ת]{3}\\b מוצא מילים בנות 4 אותיות המתחילות באל"ף. תוכלו להיעזר במנוע הבינה המלאכותית המועדף עליכם כדי לבנות ביטוי רגולרי המתאים לצרכיכם.'),
            ('כותרת ($)', 'חיפוש בתוך כותרות הקטלוג של חיבורים.'),
            ('מספר מדף (#)', 'חיפוש מהיר של מספרי מדף (למשל: "T-S NS 13.15").'),
            ('\U0001F195 תגיות PGP', 'עיון לפי נושאים בכתבי יד שקוטלגו על ידי פרויקט הגניזה של פרינסטון (Princeton Geniza Project). ראו [מידע PGP](#help-pgp) להלן.'),
        ]

        with ui.column().classes('gap-3 mb-4 w-full'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2 w-full').style('direction: rtl;'):
                    ui.label(f'• {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

        ui.markdown('''
כפתור **היסטוריית חיפוש** (סמל השעון) שליד כפתור **חפש** מציג את החיפושים האחרונים שלכם. לחיצה על אחד מהם משחזרת את השאילתא, את מצב החיפוש (עם רמת הווריאנטים שלו), את **מרווח** ואת **מיקום בטקסט**, ואת הבחירות של תחום, מחבר, חיבור, טווח תאריכים, **הסתר מודפסים** ו**סינון טקסט** שתחת **חפש רק ב...**, ואז מריצה את החיפוש מחדש (רשומה ישנה ששמרה את תוצאותיה מציגה אותן במקום זאת). אפשרויות אחרות — למשל **מילים להחרגה** ותיבות הסימון של חיפוש בסגנון פרויקט השו"ת — נשארות כפי שהן.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('מיקום בטקסט (לאיתור צירופים)', classes='text-lg font-semibold mb-2 mt-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
תחת **אפשרויות מתקדמות** קיים בורר **מיקום בטקסט** — *בכל מקום*, *תחילת הטקסט*, *סוף הטקסט*, *תחילת שורות* או *סוף שורות* — הפועל ב**כל מצבי החיפוש**. הוא שימושי במיוחד ל**איתור צירופים** בין קטעים. לדוגמה, אם יש ברשותכם חלק של ביטוי בתחילת קטע קרוע, תוכלו לחפש את החלק השני של הביטוי רק בתחילתם של קטעים, או בסופם של קטעים אם אתם מחפשים את העמוד הקודם בכתב היד. מצב פרויקט השו"ת מוסיף שליטה עדינה יותר ברמת המילה והשורה (ראו [חיפוש בסגנון פרויקט השו"ת](#help-responsa) להלן).

תחת **אפשרויות מתקדמות** נמצאת גם התיבה **מילים להחרגה**: תוצאות המכילות אחת מהמילים שתזינו (מופרדות ברווח) יסוננו. זה נפרד מהחרגת כתבי יד שלמים (ראו [החרגת כתבי יד מוכרים](#help-exclude-manuscripts) להלן).
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Responsa-Style Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-responsa"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('tune').classes('text-2xl text-primary')
            h2('חיפוש בסגנון פרויקט השו"ת', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
מצב זה מציע שתי דרכי חיפוש ברוח פרויקט השו"ת של אוניברסיטת בר-אילן: האחת בתחביר טקסטואלי עם אופרטורים לתחיליות, סיומות, כתיב מלא/חסר ועוד; והשנייה בצורת חיפוש טבלאי אינטואיטיבי, שבפועל בונה את השאילתא עבורכם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        ui.markdown('''
**הפעלה:** בחרו **פרויקט השו"ת (R)** מתפריט מצב החיפוש, או הקלידו `R ` (R ואחריו רווח) בתחילת השאילתא.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('תחביר', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')

        syntax_data = [
            ('#מילה', 'תחיליות (24 צורות: ו, ה, ב, כ, ל, מ, ש + צירופים)', '#שלום \u2190 ושלום, השלום, בשלום...'),
            ('מילה#', 'סיומות (25 צורות: י, ו, ם, ן, ה, ך, כם, כן...)', 'שלום# \u2190 שלומם, שלומו, שלומך...'),
            ('#מילה#', 'תחיליות + סיומות יחד (כל 600 הצירופים)', '#שלום# \u2190 כל הצירופים'),
            ('מילה*', 'אותיות כלשהן אחרי המילה, גם אף אחת', 'שלום* \u2190 שלום, שלומות, שלומנו...'),
            ('מי*לה', 'אותיות כלשהן בתוך המילה, גם אף אחת, בכמה כוכביות שתרצו', 'כ*נים \u2190 כהנים, כוהנים, כנים...'),
            ('*מילה', 'אותיות כלשהן לפני המילה, גם אף אחת', '*נים \u2190 כהנים, בנים, נים...'),
            ('%מילה', 'כתיב מלא/חסר (הוספת/הסרת ו/י)', '%שלום \u2190 שלום, שלם'),
            ('(א/ב)', 'חלופות OR', '(שלום/שלומות)'),
            ('[N]', 'מרווח של N מילים', 'ראובן [3] שמעון'),
            ('[|N]', 'מרווח של N שורות (בין קבוצות שורה)', 'ראובן [|2] שמעון'),
        ]

        # Render as a compact table
        with ui.element('table').classes('w-full mb-4').style('border-collapse: collapse; direction: rtl; text-align: right;'):
            with ui.element('thead'):
                with ui.element('tr'):
                    for header in ['סימן', 'משמעות', 'דוגמה']:
                        with ui.element('th').style('padding: 6px 10px; border-bottom: 2px solid var(--primary-300); color: var(--text-primary); text-align: right;'):
                            ui.label(header).classes('font-bold text-sm')
            with ui.element('tbody'):
                for syntax, meaning, example in syntax_data:
                    with ui.element('tr').style('border-bottom: 1px solid var(--border-color, #e0e0e0);'):
                        with ui.element('td').style('padding: 4px 10px; direction: ltr; text-align: left; white-space: nowrap;'):
                            ui.label(syntax).classes('font-mono font-bold text-sm').style('color: var(--primary-700);')
                        with ui.element('td').style('padding: 4px 10px; color: var(--text-secondary);'):
                            ui.label(meaning).classes('text-sm')
                        with ui.element('td').style('padding: 4px 10px; direction: ltr; text-align: left; color: var(--text-tertiary, #888);'):
                            ui.label(example).classes('text-sm font-mono')

        ui.markdown('ניתן לשלב מגדירים, למשל `#%מילה*` = תחיליות + כתיב מלא/חסר + תו כללי בסוף.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-2')

        ui.markdown('*מילים שלמות:* כל מילה בשאילתא נמצאת כמילה שלמה, גם הראשונה והאחרונה: `כהנים גדול` אינו מוצא `כהנים גדולים`. סוגריים בתוך מילה בטקסט אינם מפריעים: `שלום` מוצא `של[ו]ם`.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-2')

        ui.markdown('*כוכביות:* `*` מייצגת כל רצף אותיות בתוך מילה אחת, גם רצף ריק -- לעולם לא רווח, ספרה או סימן פיסוק: `מאמ* קור*` מוצא `מאמתי קורין`, ווריאנטים כתיביים ויהודית-ערבית חלים על האותיות שסביבה. `*` יכולה לעמוד בכל מקום במילה, ככל שתרצו: `*נים *לים` מוצא `כהנים גדולים`, ו-`*כהנ*` מוצא כל מילה שיש בה `כהנ`. `*` לבדה מתאימה לכל מילה: למילה אחת כלשהי השתמשו במרווח, למשל `[1]`. מילה שיש בה רק אות אחת או שתיים מלבד הכוכבית (`כה*`) מתאימה למילים רבות מאוד, ולכן הרשימה עשויה להסתיים ב-"+".', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-2')

        ui.markdown('*בין עמודים:* גם התאמות שעוברות מעמוד לעמוד שאחריו נמצאות, מן העמודים הסמוכים זה לזה בכתב היד. כשחיפוש עם `*` אינו מספיק לקרוא את כולן בזמן סביר, הספירה מסתיימת ב-"+" ושורת התוצאות אומרת מה לא נחפש.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('אפשרויות משנה', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
- **וריאנטים**: התאמת חלופי אותיות על כל המונחים המורחבים
- **ערבית-יהודית (JA)**: הרחבת מילים עם ה"א הידיעה הערבית אל- (8 צורות למילה)
- **ריווח גמיש**: התעלמות מרווחים שגויים בתוך מילים — שימושי מאוד בגלל ריבוי הרווחים השגויים בקריאה האוטומטית, אך מכביד על השאילתא
- **פער דו-כיווני**: חיפוש מונחים גם בסדר קדימה וגם בסדר הפוך
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('חיפוש טבלאי', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
1. לחצו על כפתור **חיפוש טבלאי** (גלוי כאשר מצב פרויקט השו"ת פעיל)
2. הוסיפו 2–4 **רכיבים**, כל אחד מייצג מונח חיפוש או קבוצה
3. הזינו **מילה** אחת או יותר לכל רכיב (מספר מילים = חלופות OR)
4. הפעילו **מגדירים למילה**: תחילית (#), סיומת (#), תו כללי (*), כתיב מלא/חסר (%), שלילה
5. הגדירו את ה**מרחק** בין רכיבים באמצעות הספינרים
6. צפו ב**תצוגה מקדימה חיה** המתעדכנת בזמן אמת
7. לחצו על **חפש** כדי להפעיל את החיפוש
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        ui.markdown('*הערה:* כאשר שאילתא מתרחבת מעבר ל-500 מונחים, המערכת מורידה אוטומטית אפשרויות (וריאנטים, ערבית-יהודית, כתיב וכו\') כדי לשמור על מהירות, ומציגה התראה בהתאם.', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        h3('חיפוש לפי מיקום בשורה/טקסט', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
בנוסף לבורר **מיקום בטקסט** הזמין בכל מצב (ראו [חיפוש](#help-search) למעלה), מצב **פרויקט השו"ת** תומך במיקום מדויק **לכל מילה** ו**לכל שורה**:
- **|מילה** — המילה חייבת להופיע ב**תחילת** שורה
- **מילה|** — המילה חייבת להופיע ב**סוף** שורה
- **|** (בין מילים) — **מעבר שורה**: המילים הבאות חייבות להתחיל בשורה חדשה
- **[|N]** — **מרווח של N שורות**: דילוג על N שורות בין קבוצות שורה. למשל `ראובן [|2] שמעון` מאתר את *ראובן*, ואת *שמעון* **שתי שורות לאחר מכן**.

אופרטורים אלה אידיאליים ל**איתור צירופים** — ניתן לבנות שאילתה רב-שורתית התואמת לאופן שבו קטע אחד מסתיים ולאופן שבו הקטע המצטרף מתחיל. **בונה השאילתות הטבלאי** מספק ממשק חזותי לבנייתם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Focused Search ("Search only in...") ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-filters"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('filter_list').classes('text-2xl text-primary')
            h2('חיפוש ממוקד ("חפש רק ב...")', classes='text-xl font-bold', style='color: var(--text-primary); direction: rtl; text-align: right;')

        ui.markdown('''
השתמשו בפאנל **"חפש רק ב..."** (זמין בדפי חיפוש ומקבילות) כדי לצמצם תוצאות לפי מאפייני כתב יד לפני החיפוש. כך תוכלו למקד את החיפוש בתת-קבוצה ספציפית של הקורפוס.

**מסננים זמינים:**
- **תחום:** סינון לפי סיווג מדעי (כגון תנ"ך, תלמוד, שירה)
- **מחבר:** סינון לפי מחבר מיוחס
- **יצירה:** סינון לפי שם יצירה
- **טווח תאריכים:** סינון לפי תיארוך כתב היד
- **חומר:** סינון לפי סוג חומר (כתב יד מול דפוס)

**אופן השימוש:**
- פתחו את פאנל **"חפש רק ב..."** המתקפל מעל תוצאות החיפוש
- בחרו מסנן אחד או יותר — מספר כתבי היד מתעדכן בזמן אמת
- מסננים פעילים מופיעים כ**צ'יפים** ניתנים להסרה מעל התוצאות
- המסננים חלים על כל מצבי החיפוש (מדויק, וריאנטים, פרויקט השו"ת וכו')
- בדף העיון, תוויות תחום ומחבר מקשרות ישירות לחיפוש מסונן

**סינון לפי ספרייה:** אחרי החיפוש, הכפתור **סינון לפי ספרייה** שבשורת התוצאות פותח חלון שבו אפשר לבחור **הצג רק נבחרות** או **הסתר נבחרות**, כלומר להציג רק ספריות מסוימות או להסתיר אותן (למשל CUL, JTS). הבחירה נשמרת גם לחיפושים הבאים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Search Within Results ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-search-within"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('filter_list').classes('text-2xl text-primary')
            h2('חיפוש בתוך תוצאות', classes='text-xl font-bold', style='color: var(--text-primary); direction: rtl; text-align: right;')

        ui.markdown('''
לאחר חיפוש, לחצו על **"חפש בתוך N כתבי יד"** בכותרת התוצאות כדי להגביל את השאילתה הבאה לכתבי היד שנמצאו בתוצאות הנוכחיות. כך ניתן לצמצם תוצאות בהדרגה דרך מספר שלבי חיפוש.

**איך זה עובד:**
- חפשו מונח (למשל "חנוכה") — התוצאות מציגות את כל כתבי היד התואמים
- לחצו "חפש בתוך N כתבי יד" — תג מופיע על שורת החיפוש
- הקלידו מונח שני (למשל "נרות") וחפשו — התוצאות מוגבלות לכתבי היד מהחיפוש הראשון
- פס תגיות מציג את שרשרת הצמצום: [חנוכה] › [נרות]
- לחצו × על תגית כדי להסיר אותה ואת כל השלבים שאחריה
- לחצו "נקה הכל" לחזרה לחיפוש ללא הגבלות

**תיבת סימון "רק תוצאות עם כל המונחים":** כאשר יש שני שלבים או יותר בשרשרת, מופיעה תיבת סימון בפס התגיות. סימון שלה מסנן את התצוגה ומציג רק דפים מכתבי יד שהופיעו בתוצאות של כל שלב.

**צמצום חוצה מצבים:** ניתן לשלב מצבי חיפוש בחופשיות — למשל חיפוש לפי מספר מדף, ואז צמצום בחיפוש טקסט, ואז צמצום נוסף בתחביר פרויקט השו"ת. ההגבלה פועלת תמיד ברמת כתב היד ללא קשר למצב.

**הערה:** ההגבלה פועלת ברמת כתב היד — אם כתב יד א׳ מכיל "חנוכה" בדף 1 ו"נרות" בדף 5, שני הדפים עשויים להופיע בתוצאות המצומצמות. השתמשו בתיבת הסימון "רק תוצאות עם כל המונחים" כדי לראות רק דפים מכתבי יד שתאמו לכל השאילתות.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Exclude Known Manuscripts (Hebrew) ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-exclude-manuscripts"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('person_remove').classes('text-2xl text-primary')
            h2('החרגת כתבי יד מוכרים', classes='text-xl font-bold', style='color: var(--text-primary); direction: rtl; text-align: right;')

        ui.markdown('''
לחצו על **"החרג כתבי יד מוכרים"** בפאנל "חפש רק ב..." (לפני החיפוש) או בפאנל **סנן תוצאות** (אחרי החיפוש) כדי להסתיר כתבי יד ספציפיים מתוצאות החיפוש. שלוש שיטות זמינות:

**הדבקת מספרי מדף:** הלשונית הראשונית — הדביקו רשימת מספרי מדף (אחד בכל שורה). התוכנה תתעלם משורות המתחילות ב-#. לחצו "החל" לאיתור והחרגה.

**מרשימה:** הרחיבו כל רשימה שמורה כדי לראות כתבי יד בודדים עם תיבות סימון. סמנו רשימות שלמות או פריטים ספציפיים. ניתן לבחור ממספר רשימות בו-זמנית.

**מקובץ:** העלו קובץ TXT (מספר מדף אחד בכל שורה) או CSV (עמודת מספרי מדף מזוהה אוטומטית). דו"ח איתור מציג סטטוס לכל שורה — נמצא, לא נמצא, כפול — לפני ההחלה.

**ניהול החרגות:**
- החרגות פעילות מוצגות כצ'יפים אדומים בשורת התוצאות, צ'יפ לכל מקור עם מספר כתבי היד שהוא מסתיר; לחיצה על צ'יפ מנקה את המקור
- קטע מתקפל "כתבי יד שהוחרגו" בתחתית התוצאות מראה מה הוסתר ולמה
- ההחרגות נשמרות בין חיפושים וניווט בדפים במהלך הסשן
- ייצוא (אקסל/וורד) כולל רק כתבי יד גלויים — פריטים מוחרגים לא מיוצאים
- ההחרגות בלתי תלויות ב"חיפוש בתוך תוצאות" — הן לא משפיעות זו על זו
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Translations ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-translations"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('translate').classes('text-2xl text-primary')
            h2('תרגומי קטלוג ומטא-נתונים', classes='text-xl font-bold', style='color: var(--text-primary); direction: rtl; text-align: right;')

        ui.markdown('''
נתוני קטלוג, כותרות ותיאורים מדעיים זמינים גם בעברית וגם באנגלית, באמצעות [תרגום דיקטה](https://translate.dicta.org.il/).

**הפעלה:** השתמשו במתג שבתחתית סרגל הצד (הוא מציג **תרגומים כבויים** או **תרגומים מופעלים**) כדי להציג תיאורים מתורגמים בתוצאות חיפוש, תצוגות עיון ודיאלוגי קטלוג.

כאשר מופעל, טקסט מתורגם מופיע עם תג **מתורגם/מקור** לחיץ — לחצו כדי לעבור בין הטקסט המתורגם למקורי.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        ui.markdown('''
> **חשוב:** התרגומים הם כלי עזר ממוחשבים ועלולים להכיל שגיאות, לרבות מונחים שגויים, תוכן שאינו מופיע במקור, או תעתיקים לא עקביים. יש לאמת תמיד מול הטקסט המקורי לצורכי מחקר. אם נתקלתם בתרגום בעייתי, לחצו על סמל הדגל הקטן ליד הטקסט המתורגם כדי לסייע לנו לשפר את האיכות.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right; background: var(--bg-tertiary); border-right: 3px solid var(--warning, #f59e0b); padding: 8px 12px; border-radius: 4px; margin-top: 4px;')

    # === Find Parallels ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('מציאת מקבילות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
כלי זה מיועד לחוקרים המעוניינים למצוא **טקסטים מקבילים** לחיבור ספרותי שלם (כגון פיוט, פירוש מימי הביניים או יצירה נדירה אחרת) בתוך הגניזה, ובכך לאתר עדי נוסח נוספים – ישירים ועקיפים.

(בתוכנה למחשב נקרא כלי זה "חיפוש חיבורים".)
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        if passage_available():
            h3('בחירת שיטת חיפוש', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
            ui.markdown('''
בדף קיימות שתי שיטות חיפוש, הנבחרות בכפתורי הבחירה שמעל תיבת הטקסט.

**חיפוש ברמת האות הוא ברירת המחדל, והוא המומלץ אלא אם כן דרושה לכם יכולת מסוימת של השנייה.** הוא משווה את הטקסט שלכם למאגר כרצף של אותיות ולא כרצף של מילים, ולכן הבדלי כתיב, חלוקת מילים, פיסוק ושיבושי פענוח אינם שוברים התאמה. הוא מחזיר תשובה תוך כשנייה, במקום דקות בשיטה הוותיקה, ובהשוואה מדורגת של חיפושים אמיתיים הוא החזיר הרבה פחות תוצאות לא רלוונטיות.

**חיפוש מקטעים** הוא השיטה הוותיקה, שנשמרה משום שהיא מאפשרת דברים שחיפוש ברמת האות אינו מאפשר: מצבי מדויק / וריאנטים / מקורב ברמת המילה, סינון חוצה-פסקאות, וגודל מקטע ניתן לכיול. היא איטית בהרבה.

שני הבדלים שמצפים להם ואינם קיימים: **שתי השיטות מתעלמות מניקוד**, ו**שתיהן מתייחסות לשבירת שורה כאל רווח רגיל**. אין באלו סיבה להעדיף אחת על פני האחרת.
            ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

            h3('איך עובד חיפוש ברמת האות?', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
            ui.markdown('''
הטקסט שלכם והמאגר כולו מצומצמים לרצף של אותיות עבריות בלבד: אותיות סופיות מוחלפות לצורתן הרגילה, וכל השאר – ניקוד, טעמים, פיסוק, ספרות, אותיות לועזיות וכל רווח – נשמט. המנוע מחפש אז את הקטעים הארוכים ביותר המשותפים לשני הרצפים.

משום שהוא אינו רואה גבולות מילים, הוא חסין בפני הדברים ששוברים לרוב חיפוש מבוסס-מילים בכתבי יד: סופר שחילק או חיבר מילים אחרת, פענוח שקרא אות אחת בטעות, או שבירת שורה באמצע מילה.

שלושה בקרים מעצבים את התוצאה:

- **רוחב התאמה** – כמה קרוב חייב קטע להיות כדי להיספר. צר מוצא טקסט כמעט זהה; הגדרות רחבות יותר מקבלות יותר שונות וגם יותר רעש.
- **אורך הקטע** – האם גם קטע משותף קצר נחשב התאמה. האפשרות **קטעים רגילים (ברירת מחדל)** משאירה רק חפיפה משמעותית; האפשרות **גם קטעים קצרים** מוסיפה צירופים נוסחאיים, ציטוטים ועותקים פגומים מאוד – בערך פי שניים תוצאות, תמורת שליש פחות תוצאות נכונות.
- **עומק החיפוש** – כמה קשה המנוע מחפש. הגדרות עמוקות יותר מוצאות יותר, ועולות בזמן בהתאם (בערך שנייה, שש שניות ועשרים שניות לחיפוש).
            ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        if passage_multi_witness_available():
            h3('חיפוש עם כמה עדי נוסח', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
            ui.markdown('''
עד נוסח אחד של חיבור אינו מספיק בדרך כלל כדי למצוא את כל האחרים. אם בידכם כמה עדי נוסח של אותו חיבור, פאנל **עדים** מאפשר לחפש בכולם בבת אחת וממזג את התוצאות לרשימה אחת.

כל עד נוסח נשלח לחיפוש **בנפרד**, והרשימות ממוזגות לפי דירוג. הם לעולם אינם מחוברים לשאילתה ארוכה אחת – מדידות מראות שחיבור כזה מוצא *פחות* מעד הנוסח הטוב ביותר לבדו, משום שלכל חיפוש תקציב קבוע, ושאילתה ארוכה מפזרת אותו דק מדי.

**הוספת עדי נוסח** – הדביקו אותם בלחצן **הוספת נוסח עד** (סמנו את תיבת הסימון אם ההדבקה מכילה כמה עדים המופרדים בשורות ריקות, והם יפוצלו), או סמנו תוצאות שכבר בידכם ולחצו **חפש גם עם אלו**. **הרחבה אוטומטית** חוזרת על פעולה זו עבורכם על פני כמה סבבים.

**המחיר.** ההיקף גדל וראש הרשימה נעשה רועש יותר: נמצאים יותר כתבי יד, אך חלק גדול יותר מהעמוד הראשון אינו רלוונטי. בקרת המיון מאפשרת לסדר מחדש לפי ציון משולב, לפי ההתאמה הטובה ביותר, או לפי מספר עדי הנוסח המצביעים על כל כתב יד – האחרון הוא לעיתים הדרך המהירה ביותר לראות אילו תוצאות מבוססות היטב.

**איכות עדי הנוסח משתנה מאוד.** בקבוצה שנמדדה של עשרה עדי נוסח, שישה לא מצאו דבר והוסיפו אלפי שורות רעש, בעוד שהטוב ביותר מצא תשע עשרה מקבילות אמיתיות. ליד כל עד נוסח מוצג כמה תוצאות תרם, והסרתו מסירה את שורותיו איתו.
            ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('איך עובד חיפוש מקטעים?', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
בניגוד לחיפוש רגיל, המנוע **לא** מחפש את הטקסט כולו כמקשה אחת. התהליך מתבצע כך:

1. **חלוקה למקטעים:** התוכנה מחלקת את טקסט המקור שלכם למקטעים קטנים בני N מילים כל אחד.
2. **חיפוש פרטני:** כל מקטע נשלח לחיפוש בנפרד במאגר הגניזה.
3. **ניקוד:** אם מקטע מסוים נמצא בכתב יד, הוא מקבל "ניקוד" על פי איכות ההתאמה.
4. **צבירה:** בסוף התהליך, התוכנה **מקבצת** את התוצאות – אם כתב יד מכיל מקטעים רבים שנמצאו, הוא יקבל ציון גבוה ויופיע בראש הרשימה.

ניתן לחפש גם במצב מעבדה, על פי אלגוריתם מבוסס על **שיטת הטביעות של שמידמן-קופל-פורת**, אשר מקודדת מילים עבריות ל"טביעות" מנורמלות המאפשרות התאמה למרות שינויי כתיב הנפוצים בכתבי יד מימי הביניים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('פרמטרים חשובים', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
אלו שייכים ל**חיפוש מקטעים** ולמצב מעבדה. לחיפוש ברמת האות שלושה בקרים משלו, המתוארים למעלה.

- **גודל מקטע:** מספר המילים בכל יחידת חיפוש. ערך נמוך (2–3) יגרור חיפוש איטי ותוצאות לא רלוונטיות רבות; ערך גבוה (10+) עלול להחמיץ התאמות אמיתיות.
- **מצב חיפוש:** כמו בחיפוש רגיל — מדויק, וריאנטים, או מקורב.
- **רמת וריאנטים / מס' שינויים:** שליטה בגמישות חילופי האותיות (ראו מצבי חיפוש לעיל).
- **סריקה עמוקה:** רלוונטי למצב מעבדה. סריקה מעמיקה ויסודית יותר, איטית משמעותית אך מומלצת למציאת ביטויים נדירים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('סינון מקורות ידועים', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
תכונה חזקה ומומלצת להפחתת "רעש" בתוצאות. אם טקסט המקור שלכם מצטט פסוקי תנ"ך, משנה, תלמוד או טקסטים ידועים אחרים, תוכלו **לטעון מקורות אלה** כך שהתאמות שנמצאו בהם יסוננו בנפרד.

**כיצד להשתמש:**
1. הרחיבו את הפאנל **"סינון טקסט (החרג מקורות ידועים)"**
2. לחצו על **תנ"ך**, **משנה** או **תלמוד** לטעינת מקורות סטנדרטיים מספריא
3. או לחצו על **מקורות נוספים...** לעיון בספריית ספריא המלאה
4. או לחצו על **חיפוש בספריא** לטעינת כל טקסט לפי הפניה (למשל: "רש"י על בראשית א")
5. או לחצו על **הוסף טקסט מותאם** להדבקת טקסט ייחוס משלכם

התאמות שנמצאו בטקסטי הסינון מופיעות בקטע **"תוצאות שהוחרגו"** נפרד, כך שתוכלו להתמקד במקבילות חדשות. הטקסטים ייטענו אוטומטית גם בחיפוש הבא, עד שתסירו אותם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('חיפוש חוצה-פסקאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
כאשר מחפשים מקבילות לטקסט המכיל מעברי פסקה (למשל: פיוט עם בתים, או טקסט עם חלוקה לסעיפים), ניתן להפעיל **חיפוש חוצה-פסקאות** כדי למצוא באופן ספציפי כתבי יד ששומרים על טקסט החוצה גבולות אלה.

**למה זה שימושי?**
- טקסט **בתוך** פסקאות מכיל לעיתים קרובות ציטוטים ממקורות אחרים או מקורות שמצטטים את החיבור שאתם מחפשים
- טקסט ש**חוצה** גבולות פסקה הוא הרבה פחות סביר להיות ציטוט, מכיוון שציטוטים לעיתים רחוקות חוצים שברים מבניים
- זה מסנן ביעילות את רוב ה"רעש" ועוזר למצוא עדי נוסח אמיתיים

**כיצד להשתמש:**
1. הזינו את הטקסט עם מעברי פסקה (או הגדירו מפריד מותאם כמו נקודה או נקודתיים)
2. בחרו מצב חיפוש: **חיפוש מלא** (כל התוצאות), **חוצה-פסקאות בלבד** (רק התאמות שחוצות גבולות), או **מלא + העדפת חוצה-פסקאות** (כל התוצאות, עם העדפת התאמות חוצות-פסקאות)
3. תוצאות שחוצות גבולות פסקה מסומנות בסימון מיוחד
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('הבנת התוצאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
התוצאות **מקובצות לפי כתב יד** וממוינות לפי ציון:
- **ציון מקסימלי:** ההתאמה בעלת הציון הגבוה ביותר שנמצאה בכתב היד
- **ציון ממוצע:** ממוצע הציונים של כל ההתאמות בכתב היד

מה שהציון סופר תלוי בשיטה: ב**חיפוש ברמת האות** זהו מספר האותיות המשותפות לשני הטקסטים באותה התאמה, ולכן קטע משותף ארוך יותר מקבל ציון גבוה יותר. ב**חיפוש מקטעים** הוא משקף את איכות המקטע שהותאם. השניים אינם ברי-השוואה זה לזה, ורשימת תוצאות מכילה תמיד ציונים של שיטה אחת בלבד.

לחצו על תוצאה כדי להרחיב ולראות את המקטע מהמקור שלכם לצד הטקסט המקביל מכתב יד הגניזה, עם מילים תואמות **מודגשות** להשוואה קלה.

לאחר קבלת התוצאות, הכפתור **סינון לפי ספרייה** מאפשר לבחור **הצג רק נבחרות** או **הסתר נבחרות** מן הספריות. הבחירה נשמרת גם לחיפושים הבאים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Joins Lab (מעבדת צירופים) ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-joins-lab"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('join_inner').classes('text-2xl text-primary')
            h2('מעבדת צירופים', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
**מעבדת הצירופים** היא סביבת עבודה ייעודית לאיתור **צירופים פיזיים** — קטעים שהיו בעבר חלק מאותו דף או כרך, וכיום מפוזרים בין אוספים שונים. אתם מקבעים קטע אחד כ**עוגן**, ואז מחפשים קטעים מועמדים וממיינים אותם מולו, זה לצד זה. המעבדה פועלת **בלי צורך בהתחברות** (שמירת צירוף או הוספה לרשימה כן דורשות התחברות).

**כיצד לפתוח את מעבדת הצירופים** — קישור **מעבדת צירופים** בתפריט הצד (או מעבר ישיר אל `/joins-lab`); כפתור **מצא צירופים** בעמוד **עיון**; או מכל תוצאת חיפוש (נפתחת מעוגנת על אותו עמוד).

**פאנל העוגן** — הקטע המקובע: סימן המדף שלו, מטא-דאטה תמציתית, ותמונת העמוד. השתמשו בחצי הדפדוף למעבר בין תמונות העוגן, והגדילו כדי לבחון את הכתב. **צירופים ידועים** (שלכם או של הקהילה) מופיעים בקטע ניתן-לקיפול.

**בניית שאילתה** — מצב החיפוש שמוגדר כברירת מחדל הוא **חיפוש שורות בסגנון פרויקט השו״ת**, המתאים ביותר לאיתור צירופים, שכן הוא מאפשר לתאר את הקטע המצורף שורה אחר שורה. מצבי חיפוש נוספים (מדויק, וריאנטים, מקורב) זמינים בבורר סוג החיפוש. בנו שאילתה שורה-אחר-שורה לטקסט הצפוי בקטע המצורף:

- **+ הוסף שורה** — הוסיפו שורת כתב יד נוספת, וקבעו כמה שורות לדלג לפניה (0 = השורה הבאה מיד).
- **+ או** — הוסיפו מילה חלופית שעשויה להופיע במקומה באותה שורה.
- **אפשרויות מילה (⚙)** — בגלגל השיניים של כל מילה בנפרד: שלילה (−), כתיב מלא/חסר (%), תחיליות (`#_`), סופיות (`_#`), תווים כלליים (`*_` / `_*`). בנפרד, כפתורי **⊢** / **⊣** שבכל שורה מעגנים אותה לתחילת שורה או לסוף שורה בכתב היד.
- **וריאנטים**, **ריווח גמיש**, **דו-כיווני** — תיבות סימון מתחת לבונה השאילתה, החלות על השאילתה כולה (במצב חיפוש בסגנון פרויקט השו״ת בלבד).
- **מיקום בטקסט** — הגבילו שורה לתחילת טקסט העמוד או לסופו.
- **הצד השני של הדף (p ±1)** — דרשו או כללו גם טקסט בעמוד שמנגד; **צמצם** משאיר רק מועמדים שגם העמוד הסמוך שלהם תואם, **הרחב** מוסיף עמודים סמוכים כמועמדים נוספים.
- **דמיון חזותי** — הציגו רק קטעים הדומים חזותית, אפשר בשילוב עם שאילתת טקסט.

> ⚠ **על אודות הדמיון החזותי:** מאגר הדמיון החזותי מכסה כיום רק כמחצית מן הגניזה, והאלגוריתם אינו חד-משמעי — ולכן עלול להחמיץ התאמות אמיתיות. **אין להסיק שקטע שאינו מופיע בתוצאות הדמיון החזותי אינו צירוף.** השתמשו בו כדי להעלות מועמדים, ולעולם לא כדי לפסול קטע.

לחצו **מצא מועמדים** כדי להריץ את השאילתה.

> 💡 **טיפ — ביטויים ופסוקים קרועים:** קרע מפצל לעיתים ביטוי או פסוק ידוע בין שני הקטעים המצורפים. חפשו את המחצית ה*חסרה* וקבעו אותה אל קצה הקרע:
> - אם שורה בעוגן **מתחילה** במחצית השנייה של ביטוי (המחצית הראשונה נקרעה), חפשו את **המחצית הראשונה** עם כפתור **⊣** של השורה ("השורה מסתיימת כאן") — בקטע המצורף היא תופיע בסוף שורה.
> - אם שורה **מסתיימת** במחצית הראשונה של ביטוי, חפשו את **המחצית השנייה** עם כפתור **⊢** ("השורה מתחילה כאן").
> - אפשר גם להגדיר את **מיקום בטקסט** ל**תחילת טקסט** / **סוף טקסט**, כדי לקבל רק מועמדים שבהם הטקסט הזה נמצא בקצה הקרוע של העמוד.

**מיון מועמדים** — המועמדים מופיעים ככרטיסים עם תמונה ומטא-דאטה. הכפתורים בכל כרטיס (בשמות שמופיעים כשמרחפים מעליהם):

- **✓** / **?** / **✗** — **סמן כן** / **סמן אולי** / **סמן לא**.
- **עיין בכתב יד** — פתיחת הקטע בעמוד העיון.
- **השווה קטע** — הצגתו זה לצד זה עם העוגן (כל פאנל מתקרב ומדפדף באופן עצמאי).
- סמל הנעץ, **הפוך קטע זה לעוגן החדש**.
- **הוסף כצירוף** (דורש התחברות) — תיעוד צירוף מאושר.
- **הוסף את העוגן ואת המועמד הזה לפאזל הקטעים**.

כדי לפעול על כמה מועמדים בבת אחת, סמנו את תיבות הסימון שלהם (**בחר לפעולות קבוצתיות**): ליד **ייצוא** יופיעו אז **הוסף לרשימה** (דורש התחברות) ו**הוסף לפאזל**. כפתור הסינון (**סנן מועמדים**) פותח את **סינון**: **חומר**, **קיימים נתוני מידות**, **הסתר אי-התאמת גודל** (מסתיר מועמדים שרוחבם שונה מרוחב העוגן ביותר מפי 1.4), **מצב מיון**, ותיבה שמחפשת טקסט במספר המדף או בכותרת. הכפתור **טבלה** עובר לתצוגת טבלה קומפקטית כשיש הרבה תוצאות. העוגן, השאילתה, המועמדים ומצב המיון **נשמרים גם לאחר רענון הדף**; **איפוס** מאפס אותם.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === PGP Information ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-pgp"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('school').classes('text-2xl text-primary')
            h2('מידע מפרויקט הגניזה של פרינסטון (PGP)', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
המערכת משלבת נתונים מפרויקט הגניזה של פרינסטון (Princeton Geniza Project) — מאגר מחקרי הכולל כ-36,000 מסמכים מקוטלגים עם תעתוקים, תרגומים, תיאורים ותיוג נושאי מפורט.

**תג PGP בתוצאות חיפוש**

כתבי יד שקיים עליהם מידע מפרויקט הגניזה של פרינסטון מסומנים בתג ירוק "PGP" בתוצאות החיפוש, כך שתוכלו לזהות במהירות כתבי יד עם תעתוקים ומידע מחקרי נוסף.

**מידע PGP בעיון לפי מספר מדף**

כאשר לכתב יד קיים מידע מפרויקט הגניזה של פרינסטון, מוצג פאנל מידע הכולל:
- **סוג מסמך** ושפות (למשל: מכתב, ערבית-יהודית)
- **תגיות נושא** — לחיצה על תגית מעבירה לחיפוש כל כתבי היד באותו נושא
- **תיאור** מחקרי באנגלית (עם אפשרות תרגום)
- **תיארוך** (כולל נימוק אם קיים)
- **קישור ל-PGP** לצפייה במסמך המקורי באתר פרינסטון

**תעתוקים ותרגומים**

כשקיימים תעתוקים או תרגומים של חוקרים מפרויקט הגניזה של פרינסטון, הם זמינים בבורר הגרסאות לצד התעתוק האוטומטי. המערכת מעדיפה אוטומטית תעתוק PGP (אם קיים) על פני הקריאה האוטומטית.

**חיפוש לפי תגיות**

בחרו מצב **תגיות PGP** מתפריט מצבי החיפוש כדי לעיין בכתבי יד לפי נושא. התגיות מאורגנות בקטגוריות (סוגי מסמכים, משפט וחברה, רפואה, סחר ומסעות ועוד) ומוצגות בתפריט נפתח עם תרגום לעברית.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Reading Desk ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-reading-desk"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('auto_stories').classes('text-2xl text-primary')
            h2('שולחן עיון (Reading Desk)', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
תצוגה מקבילה המאפשרת עיון במספר כתבי יד זה לצד זה, עם תמונות מסונכרנות ובורר גרסאות לכל קטע בנפרד.

שולחן העיון שימושי לכל חוקר המעוניין לצפות בכמה מספרי מדף יחד — בין אם מדובר בקטעים המצטרפים למסמך אחד (על פי PGP או צירופים אחרים) ובין אם בכל אוסף כתבי יד שברצונכם לעיין בהם זה לצד זה.

**כיצד להשתמש:**
- לחצו על הסמל הירוק **הוסף לשולחן העיון** בסרגל הכלים של עמוד עיון לפי מספר מדף
- הוסיפו כתבי יד נוספים מתוצאות חיפוש או מעיון בכתבי יד אחרים
- כל קטע מוצג עם תמונת המקור, בורר גרסאות (כולל תעתוקי PGP אם קיימים), ומידע מורחב
- לסיום לחצו **חזרה לתצוגת עמוד**
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Fragment Puzzle ===
    _puzzle_card = ui.card().classes('w-full p-6')
    _puzzle_card.set_visibility(WEB_PUZZLE_ENABLED)
    with _puzzle_card:
        ui.element('a').props(f'name="help-puzzle"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('extension').classes('text-2xl text-primary')
            h2('פאזל קטעים', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
קנבס חזותי לסידור תמונות קטעי כתבי יד זה לצד זה, לשחזור צירופים פיזיים בין קטעי גניזה.

**הוספת קטעים:**
- הקלידו מספר מדף בתיבת הקלט ולחצו Enter או על כפתור ההוספה
- מרשימה אישית: פתחו את תפריט **הרשימות** ובחרו קטעים להוספה
- מצירופים ידועים: כשקיימים צירופי PGP או פרידברג לקטע מסוים, לחצו על כפתור הצירופים כדי לטעון את כל הקטעים הקשורים בבת אחת

**פקדי הקנבס:**
- **זום:** גלילה להגדלה/הקטנה; לחיצה כפולה לאיפוס התצוגה
- **סיבוב:** בחרו קטע והשתמשו בפקדי הסיבוב (או בחצי המקלדת) לכוונון עדין של הזווית
- **הסרת רקע:** כוונו את מחוון הסף להסרת רקע הקלף/נייר מתמונות הקטעים, לבידוד הטקסט
- **מצבי רקע:** מעבר בין רקעי קנבס (כהה, שחור, לבן, משובץ, קלף, רשת) למציאת הניגודיות האופטימלית
- **חיתוך:** חיתוך שוליים ריקים מתמונת קטע
- **היפוך:** שיקוף קטע אופקית או אנכית
- **ניווט בדפים:** השתמשו בפקדי עמוד קודם/הבא למעבר בין רקטו וורסו, או לניווט לדפים אחרים של אותו כתב יד

**שכבות:**
- **הבא קדימה / שלח אחורה:** כשקטעים חופפים, השתמשו בפקדים אלה לשינוי סדר השכבות כדי למקם קטעים מעל או מתחת לאחרים

**בורר קטעים:**
- השתמשו בתיבת הבחירה מעל הקנבס לבחירת קטע טעון; כפתור **עיון** פותח את הקטע בעמוד עיון לפי מספר מדף

**שמירה וטעינה:**
- לחצו על כפתור **שמירה** (💾) לשמירת הסידור הנוכחי כמסמך צירוף עם כותרת והערות
- לאחר השמירה הראשונה, שינויים נשמרים אוטומטית בכל הזזה, סיבוב או שינוי גודל
- לחצו על כפתור **פתיחה** (📂) לעיון בצירופים שמורים; כל רשומה מציגה תמונה ממוזערת
- בחרו צירוף שמור כדי לטעון אותו חזרה לקנבס

**ייצוא:**
- לחצו על כפתור **ייצוא PNG** (🖼️) להורדת תמונת PNG מורכבת של כל הקטעים כפי שסודרו על הקנבס
- התמונה המיוצאת כוללת באנר מטא-נתונים עם רשימת מספרי המדף של כל הקטעים

**רקטו/ורסו:**
- כפתור הפיכת הפאזל משקף את כל סידור הקנבס ומנווט כל קטע לצד ורסו (או רקטו), כדי שתוכלו לבחון את הצד ההפוך של צירוף משוחזר
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Community Publishing ===
    _community_card = ui.card().classes('w-full p-6')
    _community_card.set_visibility(WEB_PUZZLE_ENABLED)
    with _community_card:
        ui.element('a').props(f'name="help-community-publish"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('publish').classes('text-2xl text-primary')
            h2('פרסום לקהילה', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שתפו את שחזורי הצירופים שלכם עם קהילת החוקרים ועיינו בצירופים שפורסמו על ידי חוקרים אחרים.

**פרסום צירוף:**
- לאחר שמירת צירוף בפאזל, לחצו על כפתור **פרסום** (📤) בסרגל הכלים
- לאחר הפרסום, הכפתור הופך לירוק ומופיע דיאלוג שיתוף קישור עם כפתור **העתקה** לשליחה לעמיתים
- הסידור שלכם מועלה כתמונה מורכבת יחד עם מטא-נתונים (כותרת, הערות, רשימת קטעים)

**ביטול פרסום:**
- לחצו שוב על כפתור הפרסום הירוק כדי להסיר את הצירוף מהקהילה
- מחיקת מסמך צירוף מקומי מסירה אותו אוטומטית גם מהקהילה אם היה מפורסם

**עיון בצירופים שהתפרסמו:**
- **מרכז התגליות** (**קהילה** בתפריט) מציג צירופי פאזל שפורסמו על ידי כל המשתמשים, עם תמונות ממוזערות
- השתמשו בסינון הסוג כדי להציג רק **צירופי פאזל** בפיד
- לחצו על **הצג פרטים** לצפייה בתמונה ברזולוציה מלאה, הערות ורשימת קטעים

**פתיחת צירוף מפורסם:**
- לחצו על **פתח בפאזל** בכל צירוף מפורסם כדי ליצור עותק מקומי בסביבת העבודה שלכם
- נוצר עותק עצמאי שתוכלו לערוך מבלי להשפיע על הפרסום המקורי

**פאנל צירופי קהילה:**
- בעת עיון בכתב יד, מוצג פאנל עם צירופים שהתפרסמו הכוללים את הקטע הנוכחי
- זה מסייע לגלות שחזורים קיימים הרלוונטיים לכתב היד שאתם חוקרים
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Browse by Shelfmark ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('עיון לפי מספר מדף', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
עמוד זה מאפשר קריאה רציפה ונוחה של כתב יד שלם, בסנכרון עם תמונות המקור. כותרת העמוד עצמו: **דפדוף בכתבי יד**.

**טעינת כתב יד:** הזינו **מספר מדף** בתיבת החיפוש. החיפוש גמיש ומתעלם מרווחים/סימני פיסוק (למשל: `TS NS 13 15` מוצא `T-S NS 13.15`).

**תכונות:**
- **תמונות:** צפיין תמונות מציג את עמוד כתב היד. ניתן לעשות זום, לסובב, ולצפות במסך מלא
- **ניווט בעמודים:** השתמשו בחצים או בתפריט העמודים לניווט בין עמודים
- **הצג הכל:** לחצו להצגת כל עמודי כתב היד ברצף אחד לגלילה
- **חפש מקבילות:** שליחת טקסט העמוד הנוכחי ל**מציאת מקבילות**
- **כתיב:** פתיחת כתב היד בקטלוג המקוון של הספרייה הלאומית
- **עריכה והערות:** הגישו תיקונים או הוסיפו הערות מחקריות לטובת כלל קהילת החוקרים, או לעצמכם (דורש התחברות)
- **מידע PGP:** אם קיים מידע מפרויקט הגניזה של פרינסטון, יוצג פאנל מידע עם תעתוקים, תיאור, תגיות ותיארוך (ראו [מידע PGP](#help-pgp) לעיל)
- **בורר כרכים:** כתבי יד עם מספר סריקות מיקרופילם (כ-1.5% מהאוסף) מציגים כעת תפריט בחירת כרך. בעבר, התמונות לא תמיד התאימו לתעתוק — בורר הכרכים מתקן זאת ומציג לכל כרך את התמונות והתעתוק שלו, וניווט העמודים נשאר בתוך הכרך הפעיל
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Browse by Identification ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-catalog-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('category').classes('text-2xl text-primary')
            h2('עיון לפי זיהוי', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
עיון בקטלוג המדעי לפי סיווג תחום, מחבר או כותרת יצירה.

**היררכיית תחומים:** הפאנל השמאלי מציג את עץ התחומים (למשל: מקרא > תורה > בראשית). לחצו על תחום כדי לראות את כל כתבי היד המסווגים תחתיו. המספרים מראים כמה כתבי יד שייכים לכל קטגוריה.

**סינון לפי מחבר ויצירה:** השתמשו בתפריטי החיפוש לסינון לפי שם מחבר או כותרת יצירה. המסננים משתלבים — בחירת תחום ומחבר מציגה רק כתבי יד התואמים לשניהם.

**סינון טקסט:** הקלידו מילות מפתח לחיפוש בכותרות, תיאורים ושמות תחומים. בחרו הכל (כל המונחים חייבים להתאים), אחד (כל מונח מתאים), או ללא (הוצאת תוצאות תואמות). הוסיפו מספר מונחים כצ׳יפים צבעוניים.

**תכונות:**
- **צ׳יפים של מסננים:** מסננים פעילים מופיעים כצ׳יפים ניתנים להסרה מעל התוצאות
- **דפדוף:** התוצאות מוצגות 50 בעמוד עם פקדי ניווט
- **קישור עמוק:** הכתובת מתעדכנת עם בחירות המסנן שלכם לסימנייה ושיתוף
- **קישורים צולבים:** תוויות תחום ומחבר בעמוד עיון לפי מספר מדף מקשרות ישירות לכאן עם המסנן המתאים
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    if discovery_available():
        _render_computed_identifications_help('he')
        # BAND-05 methods section, restored 2026-08-13 -- see the English half.
        _render_confidence_section('he', precision, band_counts)

    if atlas_preview_available():
        _render_visual_atlas_help('he')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('הרשימות שלי', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שמרו כתבי יד חשובים ברשימות אישיות להתייחסות עתידית.

**יצירת רשימות:** לחצו על סמל הכוכב ⭐ בכל תוצאת חיפוש, התאמת מקבילות או עמוד עיון כדי להוסיף לרשימה. צרו רשימות חדשות לארגון המחקר לפי נושא, פרויקט או כל קריטריון אחר.

**ניהול רשימות:**
- צפו בכל הרשימות שלכם בעמוד **הרשימות שלי** (הכותרת בעמוד עצמו: **רשימות אישיות**)
- הוסיפו הערות לפריטים בודדים
- ייצאו רשימות לפורמט Excel
- רשימות מסתנכרנות בין מכשירים כאשר מחוברים

**פרויקטים:** קבצו רשימות קשורות ל**פרויקטים** לארגון טוב יותר. לכל פרויקט יכול להיות קידוד צבע משלו.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Export ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-export"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('download').classes('text-2xl text-primary')
            h2('ייצוא נתונים', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
בכל שלב, ניתן לייצא תוצאות לשימוש חיצוני:

- **📊 Excel (XLSX) — חוברת מחקרית:** חוברת בת 4 גיליונות מוכנה לציטוט:
  - **תוצאות חיפוש** — התוצאות עם קטעי טקסט, טקסט מלא, סימוני PGP ומהדורה מודפסת, תחומים, וקישור לחיץ לתמונה בכל שורה
  - **כתבי יד** — שורה לכל כתב יד עם קישורים לחיצים ל-PGP, לצפייה בספרייה ול-GenizahSearch
  - **ביבליוגרפיה** — שורה לכל פריט ביבליוגרפי, מקושר לגיליון כתבי היד לפי מזהה מערכת
  - **קרדיט ומידע** — שאילתה, מצב חיפוש, הגדרת מרווח, תאריך, מספר תוצאות
- **📄 Word (DOCX):** דוח מעוצב המתאים לעבודה אקדמית
- **JSON:** פורמט תכנותי. קובץ ה-JSON של תוצאות החיפוש כולל את אותם שדות העשרה כמו חוברת ה-Excel (‎`has_pgp`, `is_printed`, `domains` לכל פריט); ב-JSON של המקבילות אין `has_pgp`/`is_printed`. ה-API הציבורי (ראו **ממשק API ציבורי וכלי AI** למטה) מחזיר `domains` אך לא `has_pgp`/`is_printed`.

כותרות העמודות ושמות הגיליונות מוצגים בעברית או באנגלית, לפי שפת הממשק.

**מיקומי ייצוא:**
- **תוצאות חיפוש:** כפתורי הייצוא מעל טבלת התוצאות
- **תוצאות מקבילות:** כפתורי הייצוא בכותרת התוצאות
- **רשימות:** ייצוא רשימות בודדות מעמוד **הרשימות שלי**
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === ממשק API ציבורי וכלי AI ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-api"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('api').classes('text-2xl text-primary')
            h2('ממשק API ציבורי וכלי AI', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
מערכת GenizahSearch מציעה ממשק HTTP/JSON ציבורי לחוקרים ולכלי AI הזקוקים לגישה תכנותית לקורפוס.

**נקודות קצה:**
- `POST /api/search` — חיפוש טקסטואלי בקורפוס
- `GET /api/browse` — טעינת תעתיק, מטא-נתונים ותמונה של כתב יד
- `POST /api/parallels` — מציאת כתבי יד מהגניזה הכוללים מקטעים תואמים לטקסט מקור ארוך

**סכמת OpenAPI:** [`/api/openapi.json`](/api/openapi.json) מתעדת את המבנה המלא של ה-API.

**סקיל Cairo Genizah Research ל-Claude:** [סקיל מוכן לשימוש](https://github.com/gershuni/GenizahSearch/tree/master-main/skills/cairo-genizah-research) המפעיל את ה-API למציאת עדי נוסח לביטוי, פיוט, תשובה, מכתב או חיבור, ומחזיר רשימה מדורגת עם טקסט מתוך עמוד העיון, ייחוס לספרייה וקישורי תמונה.

**עמוד כלי AI:** בעמוד **[כלי AI](/ai)** שבתפריט מפורטות הדרכים הזמינות כעת להשתמש ב-GenizahSearch מתוך עוזר ה-AI שלכם (כגון ChatGPT ו-Claude, או לקוח MCP), עם הוראות חיבור.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === הספרייה שלי — מסמכים מקומיים (Phase 95 D-31 + D-32 + D-33) ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props('name="help-my-library"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('folder_open').classes('text-2xl text-primary')
            h2('הספרייה שלי — מסמכים מקומיים', classes='text-xl font-bold', style='color: var(--text-primary); direction: rtl; text-align: right;')

        ui.markdown('''
[בגרסת התוכנה להורדה](/download) תוכלו להשתמש בסוגי החיפוש השונים גם בתוך קבצים מקומיים (.docx, .pdf, .txt, .html, .xlsx ו-.csv).

*תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זייבלד.*
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Contact ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('mail').classes('text-2xl text-primary')
            h2('משוב ויצירת קשר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('לשאלות, דיווח על באגים או בקשות תכונות:').style('color: var(--text-secondary); direction: rtl; text-align: right;')
        ui.label('gershuni@gmail.com').classes('text-lg font-mono mt-2')
