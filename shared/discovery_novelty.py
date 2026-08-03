# -*- coding: utf-8 -*-
"""Discovery novelty axis -- contract module (Phase 136, plan 136-04, NOVEL-01/02).

THE contract this module implements is ``docs/specs/discovery-novelty-v1.md``
-- tunable ONLY by versioning that file (same discipline as
``docs/specs/discovery-band-labels-v1.md`` / ``discovery-budgets.md``). Every
novelty-computing code path (the build-time funnel,
``scripts/discovery_novelty_funnel.py``; the future ingestion wiring, plan
136-12; any later verifier or UI consumer) reads its vocabulary, its LLM
contract, its masking rule and its verdict-to-column mapping THROUGH this
module -- never a second, hand-copied literal list.

No network access. No dependency on any gitignored research tree (such
material is read-only reference for the humans who wrote this module,
never an import target).

## Why a TEN-VALUE SHADE ENUM, not a boolean or a tri-state

The novelty axis started as a boolean (``is_new``), was amended to a
tri-state (``not_in_finding_aids`` / ``already_recorded`` / ``not_checked``,
D-23a), and was then widened five more times by direct owner ruling --
``136-GATE1-DECISIONS.md`` sections E, E-prime, F, G, H (2026-08-02) --
because each narrower vocabulary was measured to COLLAPSE qualitatively
different findings into the same bucket. Concretely: the live discovery-v1
asset already contains 665 claims whose OWN evidence rows disagree with each
other on the legacy ``is_new`` boolean (of 29,054 multi-evidence claims) -- a
single boolean cannot even represent internal disagreement, let alone the
finer distinctions the owner asked for:

- A catalogue CONTRADICTION ("an aid ties this fragment to a DIFFERENT
  work") and a genuine "previously unknown" case both scored the SAME
  tri-state value (``not_in_finding_aids``) -- but a contradiction is a claim
  that the catalogue, or possibly our identification, is wrong; a different
  and far more reputationally loaded assertion than "no aid mentions this"
  (ruling E).
- A granularity refinement (an aid names the parent work; we name the
  specific chapter) scored ``already_recorded`` and became invisible, though
  it is genuinely informative (ruling E); the OPPOSITE direction (an aid
  already names something FINER than our claim) is not informative at all
  and must not score the same as the informative direction (ruling E').
- Divergence itself splits by SCOPE (a different work vs. a different part
  of the SAME work, ruling F) and needs an ORTHOGONAL correctness call the
  shade token itself cannot carry, because the owner's own review of real
  cases found BOTH directions (catalogue right / claim right) occur under
  the identical shade (ruling F).
- A catalogue naming a broader standard-rite CONTAINER (a machzor, a siddur)
  whose predictable content includes the claimed unit, without ever naming
  the unit itself, is not a "previously unknown" case (it would misfile as
  ``fills_gap`` by elimination under every narrower vocabulary) -- ruling H
  names this ``container_predicts``, deliberately NOT ``witness`` (that word
  already carries five OTHER distinct meanings in this project; see
  ``136-NOVELTY-PRIOR-ART.md`` section 8).

Full rationale, worked cases and the enumerated downstream-contract list for
every widening step: ``136-GATE1-DECISIONS.md`` sections E, E-prime, F, G, H,
I, J. The CANONICAL, single-cited restatement of the full ten-value list
going forward is ``docs/specs/discovery-novelty-v1.md`` (this module's own
sibling contract doc) -- cite THAT file, or ``136-GATE1-DECISIONS.md``
directly, rather than re-deriving the list a third time. The one place a
second literal restatement is structurally unavoidable is the SQL CHECK
constraint in ``docs/specs/discovery-sidecar-schema-v1.md`` (SQLite CHECK
constraints cannot import a shared constant) --
``tests/test_discovery_novelty_contract.py`` is the drift guard if the two
ever disagree.

## Funnel-first architecture (ruling J)

The novelty check runs in TWO STAGES, never a single uniform pass:

1. A MECHANICAL, zero-model-call heuristic pass (``scripts/discovery_novelty_funnel.py``,
   built on this module's ``normalize_free_text``/``build_cache_key``/vocabulary)
   runs over EVERY identification. It can only ever produce two outcomes: a
   genuine, textual name-match (resolves to ``confirms``) or "unresolved"
   (the row becomes part of the RESIDUAL). It never itself decides
   ``diverges_work`` / ``diverges_part`` / ``refines_granularity`` /
   ``aid_more_specific`` / ``alias_merge`` / ``container_predicts`` /
   ``extends`` -- those are judgments the model makes over the residual,
   never a mechanical string-match's job.
2. The pinned LLM gate (``LLM_MODEL`` / ``LLM_MODEL_VERSION`` /
   ``LLM_REASONING_EFFORT``, ruling B) runs ONLY over the residual -- rows
   the mechanical pass could not resolve. A row the mechanical pass
   resolves, in EITHER direction, NEVER reaches the model.

The real, measured cost of this choice (ruling J): the funnel only ever
DEMOTES (discovery -> known, never the reverse), so a MECHANICAL
false-known is now PERMANENT and UNRECOVERABLE -- nothing downstream ever
re-examines a row the heuristic pass has already resolved to ``confirms``.
This is why the mechanical pass is deliberately CONSERVATIVE about what
counts as a match (ruling G; Codex findings 1/6,
``136-NOVELTY-PRIOR-ART.md`` section 6) -- mere SOURCE PRESENCE (a
bibliography row exists; a PGP description exists) is never sufficient; an
ACTUAL textual name-match, checked against the source's own FREE TEXT under
a looser reading (spelling variant, author-name match, language/edition
qualifier -- ruling G), is required before the mechanical pass demotes
anything.

## divergence_correctness is HUMAN-ONLY (ruling L, 2026-08-03)

``136-GATE1-DECISIONS.md`` section L drops ``divergence_correctness`` from
the model's job entirely, after the ruling-I re-measurement measured it at
8/28 (28.6%) -- at or below chance for a three-way vocabulary, on cases
where the owner's own review scored 31/32 on the identical questions.
Ruling F's rationale for hiding ``diverges_work``/``diverges_part`` rows by
default (``HIDDEN_BY_DEFAULT_SHADES``) applies REGARDLESS of which side is
right, so no shipped surface ever needed the model's correctness call in
the first place -- only the shade token itself gates visibility.

Concretely, as of ruling L:

- ``NOVELTY_PROMPT_TEMPLATE`` no longer asks the model for a
  ``divergence_correctness`` value at all -- the model's ONLY output is the
  ten-value ``novelty_status`` shade (unchanged in vocabulary; only the
  correctness sub-question is removed). ``PROMPT_SHA256`` changed again on
  this account (see the module's own computed value; the literal old hash,
  ``441058ae3bab6e5ee17beb0fc5ea39426d7c250feb6c2bd288f0bc1605c98be5``, is
  retired and must never be cited as current going forward -- see
  ``136-GATE1-DECISIONS.md`` section L for the new value on record).
- ``resolve_model_output`` now ALWAYS returns ``divergence_correctness:
  None`` -- structurally incapable of surfacing a model-supplied value for
  that field, exactly like ``masked_provenance_label`` is structurally
  incapable of leaking a restricted corpus name. This holds regardless of
  what a raw response happens to contain (a stale pre-ruling-L cached
  response, a hallucinated key, anything).
- The STORED column, the CHECK constraint, ``DIVERGENCE_CORRECTNESS_VALUES``,
  ``divergence_correctness_applicable``, and ``novelty_columns_for``'s own
  ``divergence_correctness`` parameter are ALL UNCHANGED -- ruling L keeps
  the column and every owner-supplied value already collected; nothing is
  deleted. A human/owner annotation pass (not this module, not the model
  arm) remains the SOLE path that may ever populate this column going
  forward -- ``novelty_columns_for`` is the mapping such an annotation pass
  would call, exactly as it always was.

## Masking (D-25 / NOVEL-02)

``masked_provenance_label`` implements an ALLOWLIST, not a denylist: it can
only ever return one of a small, fixed set of pre-approved bilingual
strings (naming a source only when that source is itself public), or the
fixed non-identifying fallback. A restricted-corpus codename is
STRUCTURALLY incapable of reaching a return value, because the function
never echoes its input -- it only ever looks its input UP in a small table
and returns a pre-written string.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from typing import Any, Dict, FrozenSet, Mapping, Optional, Tuple

from shared.text_normalize import strip_nikud, strip_search_diacritics

# ---------------------------------------------------------------------------
# The ten-value shade enum (owner rulings E / E' / F / G / H,
# 136-GATE1-DECISIONS.md sections E, E', F, G, H). Order matches the
# canonical ordering the owner's own shade table and the schema doc's SQL
# CHECK constraint both use: decision E's original shades, E''s
# aid_more_specific inserted right after its sibling refines_granularity,
# F's diverges_work/diverges_part replacing the single diverges token, H's
# container_predicts inserted right before its sibling fills_gap -- the
# shade it would otherwise be misfiled into by elimination.
# ---------------------------------------------------------------------------

NOVELTY_STATUS_ORDER: Tuple[str, ...] = (
    "confirms",
    "refines_granularity",
    "aid_more_specific",
    "diverges_work",
    "diverges_part",
    "container_predicts",
    "fills_gap",
    "extends",
    "alias_merge",
    "not_checked",
)

NOVELTY_STATUSES: FrozenSet[str] = frozenset(NOVELTY_STATUS_ORDER)

# The fail-closed default -- unrun, failed, source unavailable, identifier
# does not normalize, snapshot incomplete, cache stale, or model abstained.
# Unchanged in meaning across every widening step (D-23a through ruling J).
DEFAULT_STATUS: str = "not_checked"
assert DEFAULT_STATUS in NOVELTY_STATUSES  # noqa: S101 -- module-load self-check, not a test

# The SOLE shade the public "Candidates for new finds" predicate selects
# (decision E, unchanged by every later widening -- E'/F/H all explicitly
# confirm this stays a one-value predicate).
CANDIDATE_STATUS: str = "fills_gap"

# Ruling F: diverges_work/diverges_part are HIDDEN BY DEFAULT (not merely
# excluded from the candidate toggle) behind an explicit, separately-labelled
# warned toggle -- because the owner's own review of real cases found these
# rows disproportionately OUR false positives. Ruling H is explicit that this
# rationale does NOT generalize to container_predicts (no disagreement to
# warn about there) -- a future implementer must not add it here.
HIDDEN_BY_DEFAULT_SHADES: FrozenSet[str] = frozenset({"diverges_work", "diverges_part"})

# Ruling F: the two divergence shades, and ONLY these two, carry a separate
# `divergence_correctness` sibling column.
DIVERGENCE_SHADES: FrozenSet[str] = frozenset({"diverges_work", "diverges_part"})

# Ruling F's separate correctness vocabulary -- orthogonal to the shade
# itself because the owner's own review found BOTH directions occur under
# the identical shade token.
DIVERGENCE_CORRECTNESS_VALUES: FrozenSet[str] = frozenset(
    {"catalogue_correct", "claim_correct", "unclear"}
)

# `novelty_source_label` populates on every shade where SOME finding aid
# says something nameable about this fragment-work pair (decision E's own
# rule, extended identically by every later ruling that added a shade):
# confirms/refines_granularity/aid_more_specific/alias_merge/extends/
# diverges_work/diverges_part/container_predicts. It stays NULL on
# `fills_gap` (nothing to name) and `not_checked` (nothing was checked).
SOURCE_LABEL_ELIGIBLE_SHADES: FrozenSet[str] = frozenset(
    {
        "confirms",
        "refines_granularity",
        "aid_more_specific",
        "alias_merge",
        "extends",
        "diverges_work",
        "diverges_part",
        "container_predicts",
    }
)


def is_candidate_for_new_finds(novelty_status: str) -> bool:
    """The public "Candidates for new finds" predicate (decision E). Raises
    on an out-of-vocabulary status rather than silently treating an unknown
    value as non-candidate -- an unrecognized status is a bug to surface,
    never a value to quietly default through."""
    if novelty_status not in NOVELTY_STATUSES:
        raise ValueError(f"is_candidate_for_new_finds: unknown novelty_status {novelty_status!r}")
    return novelty_status == CANDIDATE_STATUS


def is_hidden_by_default(novelty_status: str) -> bool:
    """Ruling F's default-hidden/explicit-warned-toggle posture. Only
    diverges_work/diverges_part are hidden by default -- every other shade,
    including container_predicts (ruling H's explicit carve-out), renders
    normally."""
    if novelty_status not in NOVELTY_STATUSES:
        raise ValueError(f"is_hidden_by_default: unknown novelty_status {novelty_status!r}")
    return novelty_status in HIDDEN_BY_DEFAULT_SHADES


def divergence_correctness_applicable(novelty_status: str) -> bool:
    """True only for diverges_work/diverges_part -- callers (the build
    script, the verifier, the panel) must use this rather than re-deriving
    the membership test independently."""
    if novelty_status not in NOVELTY_STATUSES:
        raise ValueError(f"divergence_correctness_applicable: unknown novelty_status {novelty_status!r}")
    return novelty_status in DIVERGENCE_SHADES


def novelty_columns_for(
    verdict: str,
    *,
    divergence_correctness: Optional[str] = None,
    source_label: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """The pure verdict -> column mapping plan 136-12's build script calls,
    so the mapping is testable without a build.

    ``source_label`` is the ALREADY-MASKED label (see
    ``masked_provenance_label``) the caller computed for whichever source
    justifies this verdict, if any -- this function's own job is only to
    GATE whether that label is eligible to populate at all, per
    ``SOURCE_LABEL_ELIGIBLE_SHADES``: passed through unchanged when the
    shade is eligible, forced to ``None`` when it is not (even if a caller
    mistakenly supplied one for e.g. ``fills_gap`` -- nothing to name, so
    nothing is stored).

    Raises ``ValueError`` (never silently drops) when ``divergence_correctness``
    is supplied alongside a non-divergence shade, or when either value is
    itself out of vocabulary -- per ``divergence_correctness_applicable``.
    """
    if verdict not in NOVELTY_STATUSES:
        raise ValueError(f"novelty_columns_for: unknown novelty_status {verdict!r}")

    if divergence_correctness is not None:
        if verdict not in DIVERGENCE_SHADES:
            raise ValueError(
                f"novelty_columns_for: divergence_correctness={divergence_correctness!r} supplied "
                f"alongside non-divergence shade {verdict!r} -- divergence_correctness is valid ONLY "
                "on diverges_work/diverges_part (136-GATE1-DECISIONS.md section F)"
            )
        if divergence_correctness not in DIVERGENCE_CORRECTNESS_VALUES:
            raise ValueError(
                f"novelty_columns_for: divergence_correctness {divergence_correctness!r} outside "
                f"{sorted(DIVERGENCE_CORRECTNESS_VALUES)}"
            )

    novelty_source_label = source_label if verdict in SOURCE_LABEL_ELIGIBLE_SHADES else None

    return {
        "novelty_status": verdict,
        "novelty_source_label": novelty_source_label,
        "divergence_correctness": divergence_correctness,
    }


# ---------------------------------------------------------------------------
# D-25 / NOVEL-02 masking -- an ALLOWLIST, never a denylist. A restricted
# corpus codename (M-source, R-source) must NEVER appear in a return value
# on any code path, including error paths and unknown/None/malformed input
# -- this function structurally cannot leak one, because it never echoes
# its own input back; it only ever looks the input up in a small,
# pre-written table and returns a fixed string.
# ---------------------------------------------------------------------------

_NAMEABLE_SOURCE_LABELS: Dict[str, Dict[str, str]] = {
    "fjms_catalogue": {"en": "recorded in the catalogue", "he": "מתועד בקטלוג"},
    "fjms_bibliography": {"en": "recorded in the bibliography", "he": "מתועד בביבליוגרפיה"},
    "pgp": {"en": "recorded in the Princeton Geniza Project", "he": "מתועד בפרויקט הגניזה של פרינסטון"},
    "fgp": {"en": "recorded in a scholarly transcription", "he": "מתועד בתעתיק מדעי"},
    "nli_catalogue": {"en": "recorded in the NLI catalogue", "he": "מתועד בקטלוג הספרייה הלאומית"},
    # Deliberately NO entry for any restricted-corpus source (e.g. an
    # "m_source_shelfmark" code) -- such a source falls through to
    # _FALLBACK_LABEL below, exactly like any other unrecognized input. Do
    # NOT add one: naming it, even under its project codename, would be
    # exactly the leak D-25/NOVEL-02 and the M-source codename rule forbid.
}

_FALLBACK_LABEL: Dict[str, str] = {
    "en": "recorded in another reference source",
    "he": "מתועד במקור עזר אחר",
}


def masked_provenance_label(source_code: Any, lang: str = "en") -> str:
    """D-25: name the source where it is nameable ("recorded in the
    catalogue"); otherwise the fixed non-identifying label ("recorded in
    another reference source"). Never raises: an unhashable, malformed,
    ``None``, or unknown ``source_code`` all fall through to the same
    fixed fallback string as a genuinely-unknown-but-well-formed code."""
    lang_key = "he" if lang == "he" else "en"
    try:
        entry = _NAMEABLE_SOURCE_LABELS.get(source_code)
    except TypeError:
        entry = None
    if entry is not None:
        return entry[lang_key]
    return _FALLBACK_LABEL[lang_key]


# ---------------------------------------------------------------------------
# D-23d / ruling G's reviewed, alias-aware identity key. Distinct from both
# the raw source work id and `canonical_work_id` (documented as
# over-collapsing -- one collapsed id covers 39 Bible books). Loads a
# curated alias-groups artifact rather than hard-coding aliases; returns
# `None` (never a fabricated key) when a work has no reviewable identity at
# all.
# ---------------------------------------------------------------------------

DEFAULT_ALIAS_GROUPS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "discovery_data",
    "novelty_alias_groups-v1.json",
)


def load_alias_groups(path: Optional[str] = None) -> Dict[str, FrozenSet[str]]:
    """Loads the curated work-id alias-group artifact (a JSON object shaped
    ``{"groups": [["id1", "id2", ...], ...]}``) from ``path`` (default
    ``discovery_data/novelty_alias_groups-v1.json``, gitignored like every
    sibling ``discovery_data/`` artifact). Returns ``{}`` when the artifact
    is absent -- FAIL CLOSED: with no curated groups loaded, every work id
    is its own singleton identity (see ``novelty_work_key``) rather than
    guessed at.
    """
    if path is None:
        path = DEFAULT_ALIAS_GROUPS_PATH
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    groups: Dict[str, FrozenSet[str]] = {}
    for group in raw.get("groups", []):
        frozen = frozenset(group)
        for member in group:
            groups[member] = frozen
    return groups


def novelty_work_key(
    work_row: Mapping[str, Any],
    alias_groups: Optional[Mapping[str, FrozenSet[str]]] = None,
) -> Optional[str]:
    """The reviewed, alias-aware identity a novelty check keys on.

    ``work_row`` must carry the work's own RAW source work id under
    ``"work_id"`` (never ``canonical_work_id`` -- Codex finding 3,
    ``136-NOVELTY-PRIOR-ART.md`` section 6: ``M:Ytext1000``-style collapsed
    canonical ids merge up to 39 distinct Bible-book titles into one
    representative). NEVER FABRICATES a key: a work with no raw id (falsy
    ``work_id``) returns ``None`` -- the caller maps that to
    ``not_checked``, never guesses.

    Implements D-23d's rule, restated under the current shade vocabulary:
    "known via ANY alias implies ``confirms``" -- every member of a curated
    alias group shares the SAME reviewed key (the group's own deterministic,
    order-independent representative -- the lexicographically smallest raw
    id in the group), so a caller doing a membership test against a
    "known" set need only test ONE key regardless of which alias spelling
    the catalogue happens to use. A work whose raw id is not a member of
    any curated group is its own singleton identity (the raw id itself) --
    a real, reviewable identity, not a fabricated one.

    ``alias_groups`` defaults to ``load_alias_groups()`` (fail-closed to
    ``{}`` when the curated artifact is absent).
    """
    raw_id = work_row.get("work_id")
    if not raw_id:
        return None
    if alias_groups is None:
        alias_groups = load_alias_groups()
    group = alias_groups.get(raw_id)
    if not group:
        return raw_id
    return min(group)


# ---------------------------------------------------------------------------
# Input normalization (ruling G's free-text contract) -- shared by the
# mechanical funnel pass and the model's cache key alike, so "the same
# normalized text" means the same thing on both sides of the funnel-first
# boundary.
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")


def normalize_free_text(text: Optional[str]) -> str:
    """NFC-normalize, strip nikud (``shared.text_normalize.strip_nikud``)
    and combining diacritics/quote variants
    (``shared.text_normalize.strip_search_diacritics``), then collapse
    whitespace. The SAME normalization the mechanical funnel pass' looser
    reading test (ruling G) and the LLM cache key both apply -- never a
    second, divergent normalization routine."""
    if not text:
        return ""
    t = unicodedata.normalize("NFC", text)
    t = strip_nikud(t)
    t = strip_search_diacritics(t)
    t = _WS_RE.sub(" ", t).strip()
    return t


# ---------------------------------------------------------------------------
# The pinned LLM contract (ruling B: gemini-3.6-flash, reasoning effort
# low -- "do NOT downgrade the model"). Every constant below is a
# module-level string literal a CI grep can see. PROMPT_SHA256 and
# INPUT_NORMALIZATION_SHA256 are DERIVED from the literal prompt/spec text
# in this module at import time -- they can never silently drift out of
# sync with what they claim to hash, because nothing hand-copies a hex
# digest here.
# ---------------------------------------------------------------------------

LLM_MODEL: str = "gemini-3.6-flash"
# Kept as its own literal (not merely an alias of LLM_MODEL) so a run
# record independently confirms BOTH the provider's `model` field AND its
# `model_version`-shaped field (whatever the provider calls it) against
# this pin -- catching a silent provider-side default-snapshot upgrade
# that LLM_MODEL alone would not detect.
LLM_MODEL_VERSION: str = "gemini-3.6-flash"
LLM_REASONING_EFFORT: str = "low"

ABSTENTION_TOKEN: str = "abstain"

NOVELTY_PROMPT_TEMPLATE: str = """You are assessing whether a computed manuscript identification is already recorded somewhere among a fixed set of finding aids checked for THIS specific fragment.

INPUT: for one (sys_id, claimed work) pair, you receive the claimed work's own title and author, AND the FULL free-text identification from every checked source that has ANY text for this fragment, each explicitly tagged by its own provenance:
  - catalogue: the FJMS/NLI catalogue's own identification text for this manuscript (never merely the structured work-id it is filed under -- READ THE PROSE)
  - bibliography: Friedberg bibliography entries for this manuscript (may include TranscriptionType values such as "published_full" -- PRESENCE of a bibliography entry is NOT by itself evidence the entry names this specific work; read what it actually SAYS)
  - pgp: Princeton Geniza Project description and/or transcription text for this fragment (a bare PGP description existing is NOT by itself evidence it names this specific work)
  - fgp: FGP scholarly transcription text for this fragment
  - m_source_shelfmark: an internal reference-corpus shelfmark attribution (source name withheld; treat as an ordinary finding aid, never surface or repeat any corpus name in your output)

DECISIVE RULE (owner ruling G): if the aid's own prose already names this identification under any spelling/phrasing, the answer is `confirms`, even if the aid's structured field points elsewhere. A structured field pointing elsewhere is NEVER sufficient by itself to conclude divergence -- always check the free text for the claimed work's own identity first, under a looser reading (an alias, a spelling variant, an author-name match, a language/edition qualifier), before concluding an aid says something different.

Choose exactly ONE of these ten values for `novelty_status`:
  - confirms: an aid already ties this fragment to this work (including via ruling G's free-text rule above, and including a known alias of the same work)
  - refines_granularity: OUR claim is MORE SPECIFIC than what any aid says (an aid names the whole work, we name a specific chapter/book of it) -- only when NO aid's free text already states our specific identification (else: confirms)
  - aid_more_specific: an AID's identification is MORE SPECIFIC than ours (an aid names a chapter/book, we name only the whole work) -- we add nothing here; the aid already knew more
  - diverges_work: an aid names a genuinely DIFFERENT work (not a granularity variant of ours)
  - diverges_part: an aid names a different or finer PART of the SAME work
  - container_predicts: an aid names a BROADER rite/cycle/ceremony/container (a full standard-rite prayer book such as a siddur or machzor, or a named ceremony/occasion) whose STANDARD, PREDICTABLE content includes this specific unit, WITHOUT the aid ever naming the unit itself (e.g. the catalogue names a standard-rite machzor for a specific festival and the claim is a Yotzer for that festival) -- distinct from `confirms` (no aid names this specific unit) and from `fills_gap` (the content IS predictable, so it is not unknown)
  - fills_gap: none of the checked sources identify this fragment as anything at all -- the genuine "previously unknown" case
  - extends: aids tie OTHER folios of the SAME manuscript to this work, but not this specific folio
  - alias_merge: the two work-ids under discussion are the SAME underlying work, not yet canonically merged
  - (structured abstention: see below -- never a guess)

STRUCTURED ABSTENTION: if you cannot make this judgment from the information given -- ambiguous, insufficient, or contradictory evidence -- respond with {"abstain": true, "reason": "<short reason>"} instead of guessing. An abstention is a real and useful answer; it is NEVER penalized and is stored as `not_checked`. NEVER emit a `novelty_status` value outside the ten listed above.

Respond ONLY with a single JSON object: {"novelty_status": "<one of the ten values>"} OR the abstention object above. No prose outside the JSON object, and no field beyond the one shown.
"""

PROMPT_SHA256: str = hashlib.sha256(NOVELTY_PROMPT_TEMPLATE.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# Batched variant of the pinned gate (owner ruling O, 2026-08-03).
#
# The single-case contract above re-sends a ~4,070-char system prompt for ONE
# judgment: 88% of every call's input is the instruction block. Judging N cases
# per call amortizes it. MEASURED over the same 300 residual cases the
# single-case arm had already classified: batch 10 costs $34 projected over the
# 55,184-row residual vs $186 at batch 1 (5.5x), holds 3-way behavioural
# agreement at 89.0%, and -- the safety-critical number -- injects only +2 new
# `fills_gap` verdicts per 300 vs +10 at batch 20 and +11 at batch 40. The knee
# is sharp between 20 and 10, which is why the default is 10 and NOT larger.
#
# The single-case template above is DELIBERATELY retained unchanged: it stays
# the validated fallback that a repeatedly-malformed batch degrades to.
# ---------------------------------------------------------------------------

_SINGLE_RESPONSE_TAIL: str = (
    'Respond ONLY with a single JSON object: {"novelty_status": "<one of the ten values>"} '
    "OR the abstention object above. No prose outside the JSON object, and no field beyond the one shown."
)

# Fail loudly if the single-case template is ever edited without revisiting the
# batch variant derived from it, rather than silently shipping a batch prompt
# built from a stale tail.
assert NOVELTY_PROMPT_TEMPLATE.rstrip().endswith(_SINGLE_RESPONSE_TAIL), (
    "NOVELTY_PROMPT_TEMPLATE no longer ends with the response tail the batch prompt is derived from -- "
    "update _SINGLE_RESPONSE_TAIL and re-pin BATCH_PROMPT_SHA256."
)

_BATCH_RESPONSE_TAIL: str = (
    "You will be given SEVERAL numbered cases at once. Judge EACH ONE INDEPENDENTLY, exactly as if it "
    "were the only case given; a case must never influence another. Respond ONLY with a single JSON object "
    'mapping every case number to its own verdict: {"results": {"1": {"novelty_status": "<one of the ten values>"}, '
    '"2": {"abstain": true, "reason": "<short reason>"}}}. Include EVERY case number exactly once. '
    "No prose outside the JSON object."
)

NOVELTY_BATCH_PROMPT_TEMPLATE: str = (
    NOVELTY_PROMPT_TEMPLATE.rstrip()[: -len(_SINGLE_RESPONSE_TAIL)].rstrip()
    + "\n\n"
    + _BATCH_RESPONSE_TAIL
    + "\n"
)

BATCH_PROMPT_SHA256: str = hashlib.sha256(NOVELTY_BATCH_PROMPT_TEMPLATE.encode("utf-8")).hexdigest()

# Measured knee (see the block comment above). Raising this past 10 measurably
# increases false `fills_gap` promotions -- do not raise it without re-running
# the batch-size measurement against a known-good reference arm.
DEFAULT_BATCH_SIZE: int = 10


class BatchResponseInvalid(Exception):
    """A batched response could not be safely aligned to the cases sent.

    Raised INSTEAD of returning partial results. A batch reply that is missing
    a case, carries an unexpected case number, or repeats one cannot be trusted
    positionally -- accepting the well-formed subset risks silently attributing
    one fragment's verdict to a different fragment, which is precisely the
    error class that would be invisible downstream. Fail closed; the caller
    retries and ultimately degrades to the single-case contract.
    """


def resolve_batch_model_output(
    raw: Optional[Mapping[str, Any]], batch_size: int
) -> Dict[int, Dict[str, Optional[str]]]:
    """Maps a raw BATCHED response to per-case stored columns, keyed by the
    1-based case number sent.

    All-or-nothing by design: every case number 1..batch_size must be present
    exactly once, or ``BatchResponseInvalid`` is raised and NOTHING is returned.
    Individual case payloads still fail closed to ``not_checked`` through
    ``resolve_model_output``, so a malformed or out-of-vocabulary verdict for
    one case degrades that case only -- it is the ALIGNMENT that is all-or-
    nothing, not the vocabulary check.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    if not isinstance(raw, Mapping):
        raise BatchResponseInvalid("response is not a JSON object")
    results = raw.get("results")
    if not isinstance(results, Mapping):
        raise BatchResponseInvalid("response has no 'results' object")

    expected = {str(i) for i in range(1, batch_size + 1)}
    got = {str(k) for k in results.keys()}
    if got != expected:
        missing = sorted(expected - got, key=int)
        unexpected = sorted(got - expected)
        raise BatchResponseInvalid(
            f"case-number mismatch: missing={missing} unexpected={unexpected} "
            f"(expected exactly 1..{batch_size})"
        )

    return {
        i: resolve_model_output(results.get(str(i)) if isinstance(results.get(str(i)), Mapping) else None)
        for i in range(1, batch_size + 1)
    }

INPUT_NORMALIZATION_SPEC: str = """Input normalization for the pinned novelty LLM gate (ruling G's free-text contract): each free-text field entering the prompt is NFC-normalized, then run through shared.text_normalize.strip_nikud and shared.text_normalize.strip_search_diacritics, then whitespace-collapsed (consecutive whitespace becomes a single space, leading/trailing stripped) -- see normalize_free_text in shared/discovery_novelty.py. The cache key is built over the fields in CACHE_KEY_FIELDS, IN THAT ORDER: the pinned model/version/effort/prompt-hash/input-normalization-hash identifiers FIRST (so a change to any of them invalidates every existing cache entry), then sys_id, ref_work_id, and the NORMALIZED claimed title/author and per-source free text, in that fixed order. A cache hit therefore provably means the identical question (same pinned contract, same candidate, same evidence) was already asked and answered."""

INPUT_NORMALIZATION_SHA256: str = hashlib.sha256(INPUT_NORMALIZATION_SPEC.encode("utf-8")).hexdigest()

# The exact fields, and their exact order, that enter the cache key --
# specified explicitly (not left implicit in code) per this plan's own
# instruction. Per-source free text fields are the NORMALIZED text (see
# normalize_free_text), never the raw text (so two byte-different but
# semantically-identical inputs share one cache entry).
CACHE_KEY_FIELDS: Tuple[str, ...] = (
    "llm_model",
    "llm_model_version",
    "llm_reasoning_effort",
    "prompt_sha256",
    "input_normalization_sha256",
    "sys_id",
    "ref_work_id",
    "claimed_title_normalized",
    "claimed_author_normalized",
    "catalogue_text_normalized",
    "bibliography_text_normalized",
    "pgp_text_normalized",
    "fgp_text_normalized",
    "m_source_shelfmark_text_normalized",
)


def build_cache_key(fields: Mapping[str, Any]) -> str:
    """Builds the novelty LLM gate's cache key deterministically over
    ``CACHE_KEY_FIELDS``, in that fixed order. Raises ``ValueError`` (fails
    closed) if any required field is missing -- a cache key built from a
    partial input is worse than no cache key, since a partial-input hit
    would silently reuse an answer to a DIFFERENT question."""
    missing = [f for f in CACHE_KEY_FIELDS if f not in fields]
    if missing:
        raise ValueError(f"build_cache_key: missing required field(s) {missing}")
    payload = [fields[f] for f in CACHE_KEY_FIELDS]
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def resolve_model_output(raw: Optional[Mapping[str, Any]]) -> Dict[str, Optional[str]]:
    """Maps a raw structured model response to the stored
    ``novelty_status`` column, FAILING CLOSED to ``not_checked`` on anything
    that is not a well-formed, in-vocabulary response: ``raw`` is not a
    mapping, the model explicitly abstains (``raw["abstain"] is True`` or
    ``raw["novelty_status"] == ABSTENTION_TOKEN``), or ``raw["novelty_status"]``
    is missing or outside ``NOVELTY_STATUSES``.

    ``divergence_correctness`` is ALWAYS returned as ``None`` -- owner ruling
    L (``136-GATE1-DECISIONS.md`` section L) removes this field from the
    model's output contract entirely (measured at 8/28, at or below chance
    for a three-way vocabulary); this function is structurally incapable of
    surfacing a model-supplied correctness value, exactly like
    ``masked_provenance_label`` is structurally incapable of leaking a
    restricted corpus name -- even a malformed or legacy ``raw`` payload
    that happens to still carry a ``divergence_correctness`` key (a stale
    pre-ruling-L cached response, a hallucinated key) can never reach the
    return value. ``divergence_correctness`` remains a HUMAN/owner
    annotation ONLY, attached through a different path
    (``novelty_columns_for``'s own ``divergence_correctness`` parameter),
    never through this function.
    """
    if not isinstance(raw, Mapping):
        return {"novelty_status": DEFAULT_STATUS, "divergence_correctness": None}
    if raw.get("abstain") is True:
        return {"novelty_status": DEFAULT_STATUS, "divergence_correctness": None}

    status = raw.get("novelty_status")
    if status == ABSTENTION_TOKEN or status not in NOVELTY_STATUSES:
        return {"novelty_status": DEFAULT_STATUS, "divergence_correctness": None}

    return {"novelty_status": status, "divergence_correctness": None}
