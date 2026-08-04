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
# A MARKUP HOOK ONLY: the wording of _LIMITATIONS_TEXT, its heading and the
# enclosing methods card are untouched.
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

_CONFIDENCE_TOC_TITLE = {
    'en': 'Confidence Bands and Methods',
    'he': 'דרגות ודאות ושיטות',
}

_CONFIDENCE_INTRO = {
    'en': (
        'Confidence here is a group-level status, not a per-item probability. Each band below '
        'names its population, its unit of measurement and sample size, and whether it has '
        'been graded to completion — in words, never as a percentage or an interval. A '
        'sampled band status is never applied to an individual identification. Every '
        'identification remains an unreviewed algorithmic estimate; see “Known '
        'limitations” below for what that does and does not mean.'
    ),
    'he': (
        'מידת הביטחון כאן היא סטטוס ברמת הקבוצה, ולא הסתברות פרטנית. כל דרגה למטה מציינת את '
        'האוכלוסייה שלה, את יחידת המדידה וגודל המדגם, ואם היא נבדקה עד תום — במילים, לעולם '
        'לא כאחוז או כטווח. סטטוס דגימה ברמת דרגה לעולם אינו מיוחס לזיהוי בודד. כל זיהוי נותר '
        'הערכה אלגוריתמית שלא נבדקה; ראו "מגבלות ידועות" למטה למה זה אומר ומה לא.'
    ),
}

_CONFIDENCE_FIELD_LABELS = {
    'population': {
        'en': 'Population (shipped claims in this band)',
        'he': 'אוכלוסייה (טענות שפורסמו בדרגה זו)',
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

_LIMITATIONS_TEXT = {
    'en': (
        "A work that contains another work's text can absorb matches that really belong to the "
        'contained work — a blessing or prayer embedded inside a larger legal code is the live '
        'example — so a small minority of the main pool, a low single-digit share, is '
        'misattributed for this reason. A two-page agreement is often the two sides of one '
        'physical leaf rather than two independent leaves. A composition date can rule out an '
        'implausible direction of borrowing, but it cannot settle identity by itself.'
    ),
    'he': (
        'חיבור המכיל בתוכו טקסט של חיבור אחר עלול לספוג התאמות ששייכות למעשה לחיבור המוכל '
        'בתוכו — ברכה או תפילה המשובצת בתוך קובץ הלכתי גדול היא הדוגמה החיה — כך שמיעוט קטן '
        'מהמאגר העיקרי, נתח נמוך וחד-ספרתי, משויך בטעות מסיבה זו. הסכמה בין שני עמודים היא '
        'לעיתים קרובות שני צדי אותו דף פיזי, ולא שני דפים עצמאיים. תאריך חיבור יכול לשלול '
        'כיוון היווצרות בלתי סביר, אך אינו יכול לקבוע זהות בפני עצמו.'
    ),
}

_NOVELTY_CHECK_HEADING = {'en': 'The novelty check', 'he': 'בדיקת החידוש'}

# TODO(136-04): the enumerable checked-source list (FJMS + NLI catalogue and
# bibliography, titles, PGP, FGP, M-source shelfmark attributions) plus each
# source's as-of date lands with plan 136-04's novelty ingestion. Until then
# this names only the CATEGORY of sources checked and does not invent a date.
# 136-CONTEXT.md D-23 is the frozen source of truth for the eventual list.
_NOVELTY_CHECKED_SOURCES_PLACEHOLDER = {
    'en': (
        'the finding aids we check today — catalogue descriptions, titles, bibliography, and '
        'scholarly reference attributions (the full dated list publishes once plan 136-04 lands)'
    ),
    'he': (
        'אמצעי העזר שאנו בודקים כיום — תיאורי קטלוג, כותרות, ביבליוגרפיה וייחוסים '
        'ביבליוגרפיים-מדעיים (הרשימה המלאה עם תאריכים תתפרסם עם תוכנית 136-04)'
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


def _render_bucket_rule_subsection(lk, text_style):
    """Task 2.1: the two-bucket rule, verbatim from MAIN_POOL_SENTENCE, plus
    what the second bucket does NOT mean (insufficient evidence, never
    "probably wrong")."""
    h3(
        _BUCKET_RULE_HEADING[lk],
        classes='text-lg font-semibold mt-3 mb-1',
        style='color: var(--text-primary);',
    )
    ui.label(MAIN_POOL_SENTENCE[lk]).style(text_style)
    ui.label(_SECOND_BUCKET_MEANING[lk]).style(text_style)


def _render_known_limitations_subsection(lk, text_style):
    """Task 2.2: containment, the two-sides-of-one-leaf caveat, and the dating
    caveat — every share stated in words, never as a measured percentage."""
    h3(
        _LIMITATIONS_HEADING[lk],
        classes='text-lg font-semibold mt-3 mb-1',
        style='color: var(--text-primary);',
    )
    # The marker class goes on THIS label and nothing else -- not the heading,
    # not the subsection, not the card. The D-06a exception must cover the
    # approved SENTENCE and nothing adjacent to it.
    ui.label(_LIMITATIONS_TEXT[lk]).classes(_LIMITATIONS_PARAGRAPH_CLASS).style(text_style)


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

            _render_bucket_rule_subsection(lk, text_style)
            _render_known_limitations_subsection(lk, text_style)
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

    # unit of measurement (static estimand prose).
    ui.label(f"{fl['unit'][lk]}: {_CONFIDENCE_UNIT_PROSE[lk]}").style(text_style)

    # sample size — THREE distinct numbers, never conflated with population.
    def _num(v):
        return f"{v:,}" if isinstance(v, int) else nm

    sample_parts = [
        f"{DRAW_SIZE_LABEL[lk]} {_num(row.get('draw_size'))}",
        f"{DENOMINATOR_LABEL[lk]} {_num(row.get('denominator'))}",
        f"{NUMERATOR_LABEL[lk]} {_num(row.get('numerator'))}",
    ]
    ui.label(f"{fl['sample'][lk]}: " + "; ".join(sample_parts)).style(text_style)

    # measurement status — QUALITATIVE ONLY (D-06a): no percentage, no CI, no
    # strata table. band_measurement_status() is the SAME status-derivation
    # function is_default_eligible() reads — a status change here needs no
    # code edit, only a sidecar data change.
    status = band_measurement_status(row)
    ui.label(f"{fl['status'][lk]}: {_measurement_status_copy(status, lk)}").style(text_style)

    # the four CERT-01 registry fields — read via .get(), placeholder when
    # absent/NULL, NEVER fabricated (135-05 columns; filled by 135-09).
    ui.label(f"{fl['measurement_date'][lk]}: {row.get('measurement_date') or nm}").style(text_style)
    ui.label(f"{fl['grader'][lk]}: {row.get('grader') or nm}").style(text_style)
    ui.label(
        f"{fl['audit_status'][lk]}: {row.get('audit_status') or ph['audit_pending'][lk]}"
    ).style(text_style)
    ui.label(f"{fl['report_id'][lk]}: {row.get('report_id') or nm}").style(text_style)


def create_help_page(precision=None, band_counts=None):
    """Create the comprehensive Help Center page with bilingual content.

    ``precision`` / ``band_counts`` (optional) carry the band_precision rows +
    the runtime display-deduplicated per-band claim counts fetched by the async
    /help route (web/main.py); when None the flag-gated methods section falls
    back to placeholders."""

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
                ('filters', 'Focused Search (Advanced Filters)'),
                ('search-within', 'Search Within Results'),
                ('exclude-manuscripts', 'Exclude Known Manuscripts'),
                ('translations', 'Catalog & Metadata Translations'),
                ('parallels', 'Parallels Search'),
                ('joins-lab', 'Joins Lab'),
                ('pgp', 'Princeton Geniza Project (PGP) Data'),
                ('reading-desk', 'Reading Desk'),
                ('browse', 'Browse Manuscript'),
                ('catalog-browse', 'Browse by Identification'),
                ('lists', 'Lists'),
                ('export', 'Exporting Data'),
                ('api', 'Public API & AI Tools'),
                ('my-library', 'My Library — Local Documents'),
            ]
            if WEB_PUZZLE_ENABLED:
                toc_items.insert(9, ('puzzle', 'Fragment Puzzle'))
                toc_items.insert(10, ('community-publish', 'Community Publishing'))
            # BAND-05 methods section TOC entry — gated on discovery_available()
            # (mirrors the WEB_PUZZLE_ENABLED conditional-insert + loop-continue).
            if discovery_available():
                toc_items.append(('confidence', _CONFIDENCE_TOC_TITLE['en']))
            for anchor, title in toc_items:
                if not WEB_PUZZLE_ENABLED and anchor in {'puzzle', 'community-publish'}:
                    continue
                if anchor == 'confidence' and not discovery_available():
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
            ('Exact (=)', 'Matches only the exact word or sequence of words as typed. To search with gaps between words, fill in the "Gap" field with the desired number.'),
            ('Variants (?)', 'Accounts for common letter substitutions in these texts (e.g., Dalet/Resh, He/Het, Vav/Yod). By default a **slider** controls how much flexibility is allowed \u2014 raising it adds more letter-pairs (from limited substitutions, through pairs like Qof/Kaf and Tet/Tav, up to maximum flexibility), giving broader recall but slower, noisier results. A separate **Num Changes** control sets the number of changes per word (\u00d71 strict, \u00d72 balanced \u2014 the default, \u00d73 lenient). In the general settings you can switch the level selector from the slider to preset buttons (Basic, Extended, Maximum).'),
            ('\U0001F195 Responsa Project (R)', 'Search syntax inspired by the Bar-Ilan Responsa Project, with prefix/suffix expansion, wildcards, spelling variants, and proximity gaps. Also includes a convenient and flexible tabular query builder. Familiar to Responsa Project users; easy to learn for newcomers. See [Responsa-Style Search](#help-responsa) below.'),
            ('Fuzzy (~)', 'Uses [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) to find similar words even with decoding errors.'),
            ('Regex (/)', 'Advanced search for experienced users. Example: \\b\u05d0[\u05d0-\u05ea]{3}\\b finds 4-letter words starting with Aleph.'),
            ('Title ($)', 'Searches within the catalog titles of compositions.'),
            ('Shelfmark (#)', 'Fast search for shelfmarks (e.g., "T-S NS 13.15").'),
            ('\U0001F195 PGP Tags', 'Browse manuscripts by topic tags from the Princeton Geniza Project (PGP). See [PGP Data](#help-pgp) below.'),
        ]

        with ui.column().classes('gap-3 mb-4'):
            for mode, desc in modes_data:
                with ui.row().classes('gap-2'):
                    ui.label(f'\u2022 {mode}:').classes('font-bold min-w-40').style('color: var(--primary-700);')
                    ui.markdown(desc).style('color: var(--text-secondary);')

        h3('Text Position (for join detection)', classes='text-lg font-semibold mb-2 mt-2', style='color: var(--text-primary);')
        ui.markdown('''
The **Advanced** search options include a **Text Position** selector \u2014 *Anywhere*, *Start of text*, *End of text*, *Line starts*, or *Line ends* \u2014 that works in **every search mode**. It is especially useful for **finding joins** between fragments. For example, if you have part of a phrase at the start of a torn fragment, you can search for the rest of the phrase restricted to the *start* of fragments \u2014 or the *end* of fragments if you are looking for the preceding page of the manuscript. Responsa mode adds finer per-word and per-line control (see [Responsa-Style Search](#help-responsa) below).
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
**Activation:** Select **Responsa Project (R)** from the search Mode dropdown, or type `R ` (R followed by a space) at the beginning of your query.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Syntax', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')

        syntax_data = [
            ('#word', 'Prefixes (24 forms: \u05d5, \u05d4, \u05d1, \u05db, \u05dc, \u05de, \u05e9 + compounds)', '#\u05e9\u05dc\u05d5\u05dd \u2192 \u05d5\u05e9\u05dc\u05d5\u05dd, \u05d4\u05e9\u05dc\u05d5\u05dd, \u05d1\u05e9\u05dc\u05d5\u05dd...'),
            ('word#', 'Suffixes (25 forms: \u05d9, \u05d5, \u05dd, \u05df, \u05d4, \u05da, \u05db\u05dd, \u05db\u05df...)', '\u05e9\u05dc\u05d5\u05dd# \u2192 \u05e9\u05dc\u05d5\u05de\u05dd, \u05e9\u05dc\u05d5\u05de\u05d5, \u05e9\u05dc\u05d5\u05de\u05da...'),
            ('#word#', 'Both prefixes + suffixes', '#\u05e9\u05dc\u05d5\u05dd# \u2192 all combinations'),
            ('*word', 'Wildcard before', '*\u05e9\u05dc\u05d5\u05dd \u2192 \u05db\u05d1\u05e9\u05dc\u05d5\u05dd...'),
            ('word*', 'Wildcard after', '\u05e9\u05dc\u05d5\u05dd* \u2192 \u05e9\u05dc\u05d5\u05de\u05d5\u05ea...'),
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

        ui.markdown('*Note:* You cannot search with wildcards on both sides (`*word*`) due to search engine limitations; such a query is automatically converted to `#word#` (grammatical prefixes and suffixes).').style('color: var(--text-secondary);').classes('mb-4')

        h3('Sub-Options', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
- **Variants**: Enable letter-variant matching on all expanded terms
- **Judeo-Arabic (JA)**: Expand words with the Arabic definite article \u05d0\u05dc- (8 forms per word)
- **Flexible Spacing**: Ignore erroneous spaces within words \u2014 very useful given the many spacing errors in automatic transcription, but adds load to the query
- **Bidirectional Gap**: Search for terms in both forward and reverse order
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Tabular Search', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
1. Click the **Tabular Search** button (visible when Responsa mode is active)
2. Add 2\u20134 **components**, each representing a search term or group
3. Enter one or more **words** per component (multiple words = OR alternatives)
4. Toggle **per-word modifiers**: prefix (#), suffix (#), wildcard (*), plene (%), negation
5. Set the **distance** between components using the spinners
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

    # === Advanced Filters ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-filters"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('filter_list').classes('text-2xl text-primary')
            h2('Focused Search (Advanced Filters)', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Use the **Advanced Filters** panel (available on both Search and Parallels pages) to narrow results by manuscript properties before searching. This focuses your search on specific subsets of the corpus.

**Available Filters:**
- **Domain:** Filter by scholarly classification (e.g., Bible, Talmud, Poetry)
- **Author:** Filter by attributed author
- **Work:** Filter by specific work title
- **Date Range:** Filter by manuscript dating
- **Material:** Filter by material type (manuscript vs. printed)

**How it works:**
- Open the collapsible **Advanced Filters** panel above the search results
- Select one or more filters — the manuscript count updates in real time
- Active filters appear as removable **chips** above the results
- Filters apply to all search modes (Exact, Variants, Responsa, etc.)
- On the Browse page, domain and author labels link directly to a filtered search
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
Click **"Exclude manuscripts"** in the results header, filter panel, or "Search only in..." panel to hide specific manuscripts from search results. Three methods are available:

**Paste shelfmarks:** The default tab — paste a list of shelfmarks (one per line). Lines starting with # are ignored. Click Apply to resolve and exclude.

**From List:** Expand any saved list to see individual manuscripts with checkboxes. Check entire lists or specific items. Multiple lists can be selected simultaneously.

**From File:** Upload a TXT file (one shelfmark per line) or CSV file (shelfmark column auto-detected). A resolution report shows per-row status — found, not found, or duplicate — before you apply.

**Managing exclusions:**
- Active exclusions show as a red count on the "Exclude manuscripts" button
- When multiple sources are active, per-source chips let you clear individual sources
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

**Toggle:** Use the **Show Translations** toggle in the sidebar to enable translated descriptions in search results, browse views, and catalog dialogs.

When enabled, translated text appears with a clickable **Translated/Original** badge — click to toggle between the translated and original text.
        ''').style('color: var(--text-secondary);')

        ui.markdown('''
> **Important:** Translations are machine-generated scholarly aids and may contain errors, including incorrect terminology, hallucinated content, or inconsistent transliterations. Always verify against the original text for research purposes. If you encounter a problematic translation, click the **Report** button next to the translated text to help us improve quality.
        ''').style('color: var(--text-secondary); background: var(--bg-tertiary); border-left: 3px solid var(--warning, #f59e0b); padding: 8px 12px; border-radius: 4px; margin-top: 4px;')

    # === Parallels Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('Parallels Search', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This tool is designed for researchers wishing to find **parallel texts** for a complete literary composition
(such as a Piyyut, medieval commentary, or other rare work) within the Genizah, thereby locating additional
textual witnesses\u2014both direct and indirect.
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('How it Works', classes='text-lg font-semibold mb-2', style='color: var(--text-primary);')
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

Matches found in your filter texts appear in a separate **"Filtered Results"** section, so you can focus on new parallels. The texts will automatically load in your next search as well, until you remove them.
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
   or **Combined** (all results, with boundary-crossing matches boosted)
3. Results that cross paragraph boundaries are marked with a special indicator
        ''').style('color: var(--text-secondary);').classes('mb-4')

        h3('Understanding Results', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary);')
        ui.markdown('''
Results are **grouped by manuscript** and sorted by score:
- **Max Score:** The highest-scoring match found in that manuscript
- **Avg Score:** Average score across all matches in the manuscript

Click on a result to expand and see your source chunk alongside the matching manuscript text, with matching words **highlighted** for easy comparison.
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
- **Line options (⚙)** — per-line modifiers: negation (−), plene/defective spelling (%), prefixes (`#_`), suffixes (`_#`), wildcards (`*`), and "starts line" / "ends line" position anchors.
- **Search options** — global toggles: spelling variants, Judeo-Arabic, flexible spacing, and bidirectional matching.
- **Position** — restrict a line to the start or end of the page text.
- **Other side of the leaf (p ±1)** — also require or include text on the adjacent page; **Narrow** keeps only candidates whose adjacent page also matches, **Widen** adds adjacent pages as extra candidates.
- **Visual Similarity** — show only fragments that look alike, optionally combined with a text query.

> ⚠ **About visual similarity:** the visual-similarity index covers only about **half of the Genizah**, and the algorithm is not conclusive — so it can miss real matches. **Do not assume that a fragment absent from the visual-similarity results is not a join.** Use it to surface candidates, never to rule a fragment out.

Click **Find Candidates** to run the query.

> 💡 **Tip — torn phrases & verses:** A tear often splits a known phrase or verse between the two joining fragments. Search for the *missing* half and pin it to the torn edge:
> - If a line on the anchor **starts** with the second half of a phrase (the first half is torn away), search for that **first half** with the **"ends line"** modifier — on the join it will sit at a line's end.
> - If a line **ends** with the first half of a phrase, search for the **second half** with **"starts line"**.
> - You can also set **Position** to **start of text** / **end of text** so only candidates with that text at the page's torn edge are returned.

**Triaging candidates** — candidates appear as cards with their image and metadata. For each one you can mark **Yes** / **Maybe** / **No** (then filter by triage state); **Browse** it; **Compare** it side by side with the anchor (each pane zooms and pages independently); **Re-anchor** on it; **Add as Join** (login) to record a confirmed join; **Add to Puzzle** to send it with the anchor to the Fragment Puzzle; or **Add to list** (login). A **⚠ size mismatch** note flags candidates whose physical dimensions differ from the anchor's. Use the filter bar to narrow by shelfmark, text, title, material, dimensions or triage state, and switch to a compact **Table view** for many results. Your anchor, query, candidates and triage **survive a page refresh**; **Clear** resets them.
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

**PGP Information in Browse Manuscript**

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
- Click the **Add to Reading Desk** button on the Browse Manuscript page
- Add more manuscripts from search results or from browsing other manuscripts
- Each fragment is displayed with its source image, version selector (including PGP editions if available), and extended information
- Click **Exit Reading Desk** when done
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
- Use the combobox above the canvas to select a loaded fragment; the **Browse** button opens that fragment in the Browse Manuscript page

**Saving & Loading:**
- Click the **Save** button (💾) to save the current arrangement as a join document with a title and optional notes
- After the first save, changes are auto-saved as you move, rotate, or resize fragments
- Click the **Open** button (📂) to browse your saved joins; each entry shows a thumbnail preview
- Select a saved join to load it back onto the canvas

**Export:**
- Click the **Export** button (🖼️) to generate a composite PNG image of all fragments as arranged on the canvas
- Choose from multiple resolution levels (draft, standard, or full resolution)
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
- The **Discoveries Center** shows published puzzle joins from all users, displayed with thumbnail previews
- Use the type filter to show only **Puzzle Joins** in the feed
- Click **View Details** to see full-resolution image, notes, and fragment list

**Opening a Published Join:**
- Click **Open in Puzzle** on any published join to fork a copy into your local workspace
- This creates an independent copy you can modify without affecting the original publication

**Community Puzzle Joins Panel:**
- When browsing a manuscript, a panel shows any published joins that contain that fragment
- This helps you discover existing reconstructions relevant to the manuscript you are studying
        ''').style('color: var(--text-secondary);')

    # === Browse Manuscript ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('Browse Manuscript', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
This page enables convenient continuous reading of a full manuscript, synchronized with source images.

**Loading a Manuscript:** Enter a **Shelfmark** in the search box. The search is flexible and ignores spaces/punctuation (e.g., `TS NS 13 15` finds `T-S NS 13.15`).

**Features:**
- **Images:** An image viewer displays the manuscript page. You can zoom, rotate, and view in full screen
- **Page Navigation:** Use the arrows or page dropdown to navigate between pages
- **View All:** Click to display all manuscript pages in one long scrollable view
- **Find Parallels:** Send the current page text to Parallels Search
- **View on Ktiv:** Opens the manuscript in the National Library of Israel's online catalog
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
- **Cross-links:** Domain and author labels on the manuscript Browse page link directly here with the appropriate filter pre-set
        ''').style('color: var(--text-secondary);')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('Lists', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
Save important manuscripts to personal lists for later reference.

**Creating Lists:** Click the \u2b50 star icon on any search result, parallel match, or browse page to add it to a list. Create new lists to organize your research by topic, project, or any other criteria.

**Managing Lists:**
- View all your lists in the **Lists** page
- Add notes to individual items
- Export lists to Excel or Word format
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
- **JSON:** programmatic format with the same enrichment fields as the
  Excel workbook (`has_pgp`, `is_printed`, `domains` per item) — see
  the **Public API & AI Tools** section below

Headers and sheet titles appear in Hebrew or English, matching your UI
language.

**Export locations:**
- **Search Results:** Export buttons above the results table
- **Parallels Results:** Export buttons in the results header
- **Lists:** Export individual lists from the Lists page
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

    # === Confidence Bands and Methods (Phase 135, BAND-05, D-10) ===
    # Codex #11: this body card is NOT part of the TOC render loop above (which
    # emits only ui.link TOC entries), so it carries its OWN discovery_available()
    # gate — the loop-continue only gates the TOC LINK, not this card.
    if discovery_available():
        _render_confidence_section('en', precision, band_counts)

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
                ('filters', 'חיפוש ממוקד (סינון מתקדם)'),
                ('search-within', 'חיפוש בתוך תוצאות'),
                ('translations', 'תרגומי קטלוג ומטא-נתונים'),
                ('parallels', 'חיפוש מקבילות'),
                ('joins-lab', 'מעבדת צירופים'),
                ('pgp', 'מידע מפרויקט הגניזה של פרינסטון (PGP)'),
                ('reading-desk', 'שולחן קריאה (Reading Desk)'),
                ('browse', 'עיון בכתב יד'),
                ('catalog-browse', 'עיון לפי זיהוי'),
                ('lists', 'רשימות'),
                ('export', 'ייצוא נתונים'),
                ('api', 'ממשק API ציבורי וכלי AI'),
                ('my-library', 'הספרייה שלי — מסמכים מקומיים'),
            ]
            if WEB_PUZZLE_ENABLED:
                toc_items.insert(8, ('puzzle', 'פאזל קטעים'))
                toc_items.insert(9, ('community-publish', 'פרסום לקהילה'))
            # BAND-05 methods section TOC entry — gated on discovery_available()
            # (mirrors the WEB_PUZZLE_ENABLED conditional-insert + loop-continue).
            if discovery_available():
                toc_items.append(('confidence', _CONFIDENCE_TOC_TITLE['he']))
            for anchor, title in toc_items:
                if not WEB_PUZZLE_ENABLED and anchor in {'puzzle', 'community-publish'}:
                    continue
                if anchor == 'confidence' and not discovery_available():
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
            ('מדויק (=)', 'מוצא רק את המילה או את רצף המילים בדיוק כפי שנכתבו. לחיפוש עם פערים בין המילים יש למלא את התיבה "מרווח" במספר הרצוי.'),
            ('וריאנטים (?)', 'מתחשב בחילופי אותיות נפוצים בטקסטים אלו (למשל: ד/ר, ה/ח, ו/י). כברירת מחדל **מחוון** (סליידר) קובע את מידת הגמישות — הגדלת הערך מוסיפה עוד זוגות אותיות (מחילופים מצומצמים, דרך זוגות כמו ק/כ ו-ט/ת, ועד גמישות מרבית), ומרחיבה את כמות התוצאות אך איטית ורועשת יותר. פקד **מספר שינויים** נפרד קובע את מספר השינויים למילה (×1 מחמיר, ×2 מאוזן — ברירת המחדל, ×3 מקל). בהגדרות הכלליות ניתן להחליף את בחירת הרמה מהמחוון לכפתורי קביעה מראש (בסיסי, מורחב, מרבי).'),
            ('\U0001F195 פרויקט השו"ת (R)', 'חיפוש בתחביר בסגנון החיפוש המתקדם של פרויקט השו"ת של אוניברסיטת בר-אילן, עם הרחבת תחיליות/סיומות, תווים כלליים, חלופות כתיב ומרווחים. כולל גם בונה שאילתות טבלאי נוח וגמיש. מוכר למשתמשי פרויקט השו"ת; קל ללמוד גם למי שלא מכיר. ראו [חיפוש בסגנון פרויקט השו"ת](#help-responsa) להלן.'),
            ('מקורב (~)', 'משתמש ב[מרחק לווינשטיין](https://he.wikipedia.org/wiki/%D7%9E%D7%A8%D7%97%D7%A7_%D7%9C%D7%95%D7%99%D7%A0%D7%A9%D7%98%D7%99%D7%99%D7%9F) למציאת מילים דומות גם עם שגיאות פענוח.'),
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

        h3('מיקום בטקסט (לאיתור צירופים)', classes='text-lg font-semibold mb-2 mt-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
תחת **אפשרויות מתקדמות** קיים בורר **מיקום בטקסט** — *בכל מקום*, *תחילת הטקסט*, *סוף הטקסט*, *תחילת שורות* או *סוף שורות* — הפועל ב**כל מצבי החיפוש**. הוא שימושי במיוחד ל**איתור צירופים** בין קטעים. לדוגמה, אם יש ברשותכם חלק של ביטוי בתחילת קטע קרוע, תוכלו לחפש את החלק השני של הביטוי רק בתחילתם של קטעים, או בסופם של קטעים אם אתם מחפשים את העמוד הקודם בכתב היד. מצב פרויקט השו"ת מוסיף שליטה עדינה יותר ברמת המילה והשורה (ראו [חיפוש בסגנון פרויקט השו"ת](#help-responsa) להלן).
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
            ('#מילה#', 'תחיליות + סיומות יחד', '#שלום# \u2190 כל הצירופים'),
            ('*מילה', 'תו כללי לפני', '*שלום \u2190 כבשלום...'),
            ('מילה*', 'תו כללי אחרי', 'שלום* \u2190 שלומות...'),
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

        ui.markdown('*הערה:* לא ניתן לחפש מילה עם כוכביות משני צידיה (`*מילה*`) בגלל מגבלות מנוע החיפוש; שאילתא כזו תומר אוטומטית ל-`#מילה#` (תוספות דקדוקיות לפני ואחרי).', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('אפשרויות משנה', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
- **וריאנטים**: התאמת חלופי אותיות על כל המונחים המורחבים
- **ערבית-יהודית (JA)**: הרחבת מילים עם ה"א הידיעה הערבית אל- (8 צורות למילה)
- **ריווח גמיש**: התעלמות מרווחים שגויים בתוך מילים — שימושי מאוד בגלל ריבוי הרווחים השגויים בקריאה האוטומטית, אך מכביד על השאילתא
- **מרווח דו-כיווני**: חיפוש מונחים גם בסדר קדימה וגם בסדר הפוך
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

    # === Advanced Filters ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-filters"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('filter_list').classes('text-2xl text-primary')
            h2('חיפוש ממוקד (סינון מתקדם)', classes='text-xl font-bold', style='color: var(--text-primary); direction: rtl; text-align: right;')

        ui.markdown('''
השתמשו בפאנל **סינון מתקדם** (זמין בדפי חיפוש ומקבילות) כדי לצמצם תוצאות לפי מאפייני כתב יד לפני החיפוש. כך תוכלו למקד את החיפוש בתת-קבוצה ספציפית של הקורפוס.

**מסננים זמינים:**
- **תחום:** סינון לפי סיווג מדעי (כגון תנ"ך, תלמוד, שירה)
- **מחבר:** סינון לפי מחבר מיוחס
- **יצירה:** סינון לפי שם יצירה
- **טווח תאריכים:** סינון לפי תיארוך כתב היד
- **חומר:** סינון לפי סוג חומר (כתב יד מול דפוס)

**אופן השימוש:**
- פתחו את פאנל **סינון מתקדם** המתקפל מעל תוצאות החיפוש
- בחרו מסנן אחד או יותר — מספר כתבי היד מתעדכן בזמן אמת
- מסננים פעילים מופיעים כ**צ'יפים** ניתנים להסרה מעל התוצאות
- המסננים חלים על כל מצבי החיפוש (מדויק, וריאנטים, רספונסה וכו')
- בדף העיון, תוויות תחום ומחבר מקשרות ישירות לחיפוש מסונן
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
- פס תגיות מציג את שרשרת הצמצום: [חנוכה] ‹ [נרות]
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
לחצו על **"החרג כתבי יד"** בכותרת התוצאות, בפאנל הסינון או בפאנל "חפש רק ב..." כדי להסתיר כתבי יד ספציפיים מתוצאות החיפוש. שלוש שיטות זמינות:

**הדבקת מספרי מדף:** הלשונית הראשונית — הדביקו רשימת מספרי מדף (אחד בכל שורה). התוכנה תתעלם משורות המתחילות ב-#. לחצו "החל" לאיתור והחרגה.

**מרשימה:** הרחיבו כל רשימה שמורה כדי לראות כתבי יד בודדים עם תיבות סימון. סמנו רשימות שלמות או פריטים ספציפיים. ניתן לבחור ממספר רשימות בו-זמנית.

**מקובץ:** העלו קובץ TXT (מספר מדף אחד בכל שורה) או CSV (עמודת מספרי מדף מזוהה אוטומטית). דו"ח איתור מציג סטטוס לכל שורה — נמצא, לא נמצא, כפול — לפני ההחלה.

**ניהול החרגות:**
- החרגות פעילות מוצגות כמספר אדום על כפתור "החרג כתבי יד"
- כאשר מספר מקורות פעילים, צ'יפים נפרדים מאפשרים ניקוי מקור בודד
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

**הפעלה:** השתמשו במתג **הצג תרגומים** בסרגל הצד כדי להציג תיאורים מתורגמים בתוצאות חיפוש, תצוגות עיון ודיאלוגי קטלוג.

כאשר מופעל, טקסט מתורגם מופיע עם תג **מתורגם/מקור** לחיץ — לחצו כדי לעבור בין הטקסט המתורגם למקורי.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

        ui.markdown('''
> **חשוב:** התרגומים הם כלי עזר ממוחשבים ועלולים להכיל שגיאות, לרבות מונחים שגויים, תוכן שאינו מופיע במקור, או תעתיקים לא עקביים. יש לאמת תמיד מול הטקסט המקורי לצורכי מחקר. אם נתקלתם בתרגום בעייתי, לחצו על כפתור **דיווח** ליד הטקסט המתורגם כדי לסייע לנו לשפר את האיכות.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right; background: var(--bg-tertiary); border-right: 3px solid var(--warning, #f59e0b); padding: 8px 12px; border-radius: 4px; margin-top: 4px;')

    # === Parallels Search ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-parallels"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('compare_arrows').classes('text-2xl text-primary')
            h2('חיפוש מקבילות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
כלי זה מיועד לחוקרים המעוניינים למצוא **טקסטים מקבילים** לחיבור ספרותי שלם (כגון פיוט, פירוש מימי הביניים או יצירה נדירה אחרת) בתוך הגניזה, ובכך לאתר עדי נוסח נוספים – ישירים ועקיפים.
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('איך זה עובד?', classes='text-lg font-semibold mb-2', style='color: var(--text-primary); direction: rtl; text-align: right;')
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
- **גודל מקטע:** מספר המילים בכל יחידת חיפוש. ערך נמוך (2–3) יגרור חיפוש איטי ותוצאות לא רלוונטיות רבות; ערך גבוה (10+) עלול להחמיץ התאמות אמיתיות.
- **מצב חיפוש:** כמו בחיפוש רגיל — מדויק, וריאנטים, או מקורב.
- **רמת וריאנטים / מספר שינויים:** שליטה בגמישות חילופי האותיות (ראו מצבי חיפוש לעיל).
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

התאמות שנמצאו בטקסטי הסינון מופיעות בקטע **"תוצאות מסוננות"** נפרד, כך שתוכלו להתמקד במקבילות חדשות. הטקסטים ייטענו אוטומטית גם בחיפוש הבא, עד שתסירו אותם.
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
2. בחרו מצב חיפוש: **חיפוש מלא** (כל התוצאות), **חוצה-פסקאות בלבד** (רק התאמות שחוצות גבולות), או **משולב** (כל התוצאות, עם הגברת התאמות חוצות-פסקאות)
3. תוצאות שחוצות גבולות פסקה מסומנות בסימון מיוחד
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;').classes('mb-4')

        h3('הבנת התוצאות', classes='text-lg font-semibold mb-2 mt-4', style='color: var(--text-primary); direction: rtl; text-align: right;')
        ui.markdown('''
התוצאות **מקובצות לפי כתב יד** וממוינות לפי ציון:
- **ציון מקסימלי:** ההתאמה בעלת הציון הגבוה ביותר שנמצאה בכתב היד
- **ציון ממוצע:** ממוצע הציונים של כל ההתאמות בכתב היד

לחצו על תוצאה כדי להרחיב ולראות את המקטע מהמקור שלכם לצד הטקסט המקביל מכתב יד הגניזה, עם מילים תואמות **מודגשות** להשוואה קלה.
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
- **אפשרויות שורה (⚙)** — שינויים לכל שורה: שלילה (−), כתיב מלא/חסר (%), תחיליות (`#_`), סופיות (`_#`), תווים כלליים (`*`), ועוגני מיקום "תחילת שורה" / "סוף שורה".
- **אפשרויות חיפוש** — מתגים גלובליים: שינויי כתיב, יהודית-ערבית, ריווח גמיש, והתאמה דו-כיוונית.
- **מיקום** — הגבילו שורה לתחילת טקסט העמוד או לסופו.
- **הצד השני של הדף (p ±1)** — דרשו או כללו גם טקסט בעמוד שמנגד; **צמצם** משאיר רק מועמדים שגם העמוד הסמוך שלהם תואם, **הרחב** מוסיף עמודים סמוכים כמועמדים נוספים.
- **דמיון חזותי** — הציגו רק קטעים הדומים חזותית, אפשר בשילוב עם שאילתת טקסט.

> ⚠ **על אודות הדמיון החזותי:** מאגר הדמיון החזותי מכסה כיום רק כמחצית מן הגניזה, והאלגוריתם אינו חד-משמעי — ולכן עלול להחמיץ התאמות אמיתיות. **אין להסיק שקטע שאינו מופיע בתוצאות הדמיון החזותי אינו צירוף.** השתמשו בו כדי להעלות מועמדים, ולעולם לא כדי לפסול קטע.

לחצו **מצא מועמדים** כדי להריץ את השאילתה.

> 💡 **טיפ — ביטויים ופסוקים קרועים:** קרע מפצל לעיתים ביטוי או פסוק ידוע בין שני הקטעים המצורפים. חפשו את המחצית ה*חסרה* וקבעו אותה אל קצה הקרע:
> - אם שורה בעוגן **מתחילה** במחצית השנייה של ביטוי (המחצית הראשונה נקרעה), חפשו את **המחצית הראשונה** עם המתג **"סוף שורה"** — בקטע המצורף היא תופיע בסוף שורה.
> - אם שורה **מסתיימת** במחצית הראשונה של ביטוי, חפשו את **המחצית השנייה** עם **"תחילת שורה"**.
> - אפשר גם להגדיר את **המיקום** ל**תחילת טקסט** / **סוף טקסט**, כדי לקבל רק מועמדים שבהם הטקסט הזה נמצא בקצה הקרוע של העמוד.

**מיון מועמדים** — המועמדים מופיעים ככרטיסים עם תמונה ומטא-דאטה. עבור כל אחד תוכלו לסמן **כן** / **אולי** / **לא** (ואז לסנן לפי מצב המיון); **עיון** בו; **השווה** אותו זה לצד זה עם העוגן (כל פאנל מתקרב ומדפדף באופן עצמאי); **עגן מחדש** עליו; **הוסף כצירוף** (דורש התחברות) לתיעוד צירוף מאושר; **הוסף לפאזל** לשליחתו עם העוגן אל פאזל הקטעים; או **הוסף לרשימה** (דורש התחברות). הערת **⚠ אי-התאמת גודל** מסמנת מועמדים שמידותיהם הפיזיות שונות מן העוגן. השתמשו בסרגל הסינון לצמצום לפי סימן מדף, טקסט, כותרת, חומר, מידות או מצב מיון, ועברו ל**תצוגת טבלה** קומפקטית כשיש הרבה תוצאות. העוגן, השאילתה, המועמדים ומצב המיון **נשמרים גם לאחר רענון הדף**; **נקה** מאפס אותם.
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

**מידע PGP בעיון בכתב יד**

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
            h2('שולחן קריאה (Reading Desk)', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
תצוגה מקבילה המאפשרת עיון במספר כתבי יד זה לצד זה, עם תמונות מסונכרנות ובורר גרסאות לכל קטע בנפרד.

שולחן הקריאה שימושי לכל חוקר המעוניין לצפות בכמה מספרי מדף יחד — בין אם מדובר בקטעים המצטרפים למסמך אחד (על פי PGP או צירופים אחרים) ובין אם בכל אוסף כתבי יד שברצונכם לעיין בהם ברצף.

**כיצד להשתמש:**
- לחצו על כפתור **הוספה לשולחן הקריאה** בעמוד עיון בכתב יד
- הוסיפו כתבי יד נוספים מתוצאות חיפוש או מעיון בכתבי יד אחרים
- כל קטע מוצג עם תמונת המקור, בורר גרסאות (כולל תעתוקי PGP אם קיימים), ומידע מורחב
- לסיום לחצו **יציאה משולחן הקריאה**
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Fragment Puzzle ===
    with ui.card().classes('w-full p-6'):
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
- השתמשו בתיבת הבחירה מעל הקנבס לבחירת קטע טעון; כפתור **עיון** פותח את הקטע בעמוד עיון בכתב יד

**שמירה וטעינה:**
- לחצו על כפתור **שמירה** (💾) לשמירת הסידור הנוכחי כמסמך צירוף עם כותרת והערות
- לאחר השמירה הראשונה, שינויים נשמרים אוטומטית בכל הזזה, סיבוב או שינוי גודל
- לחצו על כפתור **פתיחה** (📂) לעיון בצירופים שמורים; כל רשומה מציגה תמונה ממוזערת
- בחרו צירוף שמור כדי לטעון אותו חזרה לקנבס

**ייצוא:**
- לחצו על כפתור **ייצוא** (🖼️) ליצירת תמונת PNG מורכבת של כל הקטעים כפי שסודרו על הקנבס
- בחרו מבין מספר רמות רזולוציה (טיוטה, רגילה או רזולוציה מלאה)
- התמונה המיוצאת כוללת באנר מטא-נתונים עם רשימת מספרי המדף של כל הקטעים

**רקטו/ורסו:**
- כפתור הפיכת הפאזל משקף את כל סידור הקנבס ומנווט כל קטע לצד ורסו (או רקטו), כדי שתוכלו לבחון את הצד ההפוך של צירוף משוחזר
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Community Publishing ===
    with ui.card().classes('w-full p-6'):
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
- **מרכז התגליות** מציג צירופי פאזל שפורסמו על ידי כל המשתמשים, עם תמונות ממוזערות
- השתמשו בסינון הסוג כדי להציג רק **צירופי פאזל** בפיד
- לחצו על **הצג פרטים** לצפייה בתמונה ברזולוציה מלאה, הערות ורשימת קטעים

**פתיחת צירוף מפורסם:**
- לחצו על **פתח בפאזל** בכל צירוף מפורסם כדי ליצור עותק מקומי בסביבת העבודה שלכם
- נוצר עותק עצמאי שתוכלו לערוך מבלי להשפיע על הפרסום המקורי

**פאנל צירופי קהילה:**
- בעת עיון בכתב יד, מוצג פאנל עם צירופים שהתפרסמו הכוללים את הקטע הנוכחי
- זה מסייע לגלות שחזורים קיימים הרלוונטיים לכתב היד שאתם חוקרים
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Browse Manuscript ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-browse"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('menu_book').classes('text-2xl text-primary')
            h2('עיון בכתב יד', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
עמוד זה מאפשר קריאה רציפה ונוחה של כתב יד שלם, בסנכרון עם תמונות המקור.

**טעינת כתב יד:** הזינו **מספר מדף** בתיבת החיפוש. החיפוש גמיש ומתעלם מרווחים/סימני פיסוק (למשל: `TS NS 13 15` מוצא `T-S NS 13.15`).

**תכונות:**
- **תמונות:** צפיין תמונות מציג את עמוד כתב היד. ניתן לעשות זום, לסובב, ולצפות במסך מלא
- **ניווט בעמודים:** השתמשו בחצים או בתפריט העמודים לניווט בין עמודים
- **הצג הכל:** לחצו להצגת כל עמודי כתב היד ברצף אחד לגלילה
- **מצא מקבילות:** שליחת טקסט העמוד הנוכחי לחיפוש מקבילות
- **צפה בכתיב:** פתיחת כתב היד בקטלוג המקוון של הספרייה הלאומית
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
- **קישורים צולבים:** תוויות תחום ומחבר בעמוד עיון בכתב יד מקשרות ישירות לכאן עם המסנן המתאים
        ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')

    # === Lists ===
    with ui.card().classes('w-full p-6'):
        ui.element('a').props(f'name="help-lists"')
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('star').classes('text-2xl text-primary')
            h2('רשימות', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.markdown('''
שמרו כתבי יד חשובים ברשימות אישיות להתייחסות עתידית.

**יצירת רשימות:** לחצו על סמל הכוכב ⭐ בכל תוצאת חיפוש, התאמת מקבילות או עמוד עיון כדי להוסיף לרשימה. צרו רשימות חדשות לארגון המחקר לפי נושא, פרויקט או כל קריטריון אחר.

**ניהול רשימות:**
- צפו בכל הרשימות שלכם בעמוד **רשימות**
- הוסיפו הערות לפריטים בודדים
- ייצאו רשימות לפורמט Excel או Word
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
  - **קרדיטים ומידע** — שאילתה, מצב חיפוש, הגדרת מרווח, תאריך, מספר תוצאות
- **📄 Word (DOCX):** דוח מעוצב המתאים לעבודה אקדמית
- **JSON:** פורמט תכנותי עם אותם שדות העשרה כמו חוברת ה-Excel (‎`has_pgp`, `is_printed`, `domains` לכל פריט) — ראו את הסעיף **ממשק API ציבורי וכלי AI** למטה

כותרות העמודות ושמות הגיליונות מוצגים בעברית או באנגלית, לפי שפת הממשק.

**מיקומי ייצוא:**
- **תוצאות חיפוש:** כפתורי הייצוא מעל טבלת התוצאות
- **תוצאות מקבילות:** כפתורי הייצוא בכותרת התוצאות
- **רשימות:** ייצוא רשימות בודדות מעמוד הרשימות
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

    # === דרגות ודאות ושיטות (Phase 135, BAND-05, D-10) ===
    # Codex #11: this body card is NOT part of the TOC render loop above (which
    # emits only ui.link TOC entries), so it carries its OWN discovery_available()
    # gate — the loop-continue only gates the TOC LINK, not this card.
    if discovery_available():
        _render_confidence_section('he', precision, band_counts)

    # === Contact ===
    with ui.card().classes('w-full p-6'):
        with ui.row().classes('items-center gap-3 mb-4'):
            ui.icon('mail').classes('text-2xl text-primary')
            h2('משוב ויצירת קשר', classes='text-xl font-bold', style='color: var(--text-primary);')

        ui.label('לשאלות, דיווח על באגים או בקשות תכונות:').style('color: var(--text-secondary); direction: rtl; text-align: right;')
        ui.label('gershuni@gmail.com').classes('text-lg font-mono mt-2')
