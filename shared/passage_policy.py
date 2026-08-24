# -*- coding: utf-8 -*-
"""Query policy for the passage matcher: one frozen object, one stable id.

Contract: docs/specs/passage-matching-algorithm.md sections 7.2, 8 and 10.2.

Why this exists as its own module. Every knob here changes retrieval behaviour
without changing the artifact, so recall/precision can be measured PER POLICY
against one index. That is only honest if results carry the exact policy that
produced them -- a number without its settings is unfalsifiable -- and if a
sweep cannot quietly fit itself on the deciding data. Hence:

  * PassagePolicy is FROZEN. A variation is a new object with a new id.
  * policy_id is a content hash of every field. Two policies with the same id
    ARE the same policy; a changed default changes the id.
  * Named presets are the only things a UI should offer. `standard-40` is the
    single default-comparable policy; everything else is exploratory until it
    has its own held-out measurement.

MIN_SPAN and the boundary regime are query policy, not artifact inputs (spec
section 8): changing them must never trigger an index rebuild, and this module
deliberately imports nothing from the builder.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace

POLICY_SCHEMA_VERSION = 1

# The two acceptance-boundary regimes (spec section 7.1). The regime is
# USER-DECLARED, never detected: only the researcher knows whether the pasted
# text is a clean edition or a noisy transcription.
REGIME_ONE_SIDED = 'one_sided'      # clean query vs noisy corpus
REGIME_TWO_SIDED = 'two_sided'      # noisy query vs noisy corpus
_REGIMES = (REGIME_ONE_SIDED, REGIME_TWO_SIDED)

# Budget allocation policies (spec section 10.2). 'band' is the specified
# default shape; 'rarest_first' and 'no_cap' exist so the three can be
# compared under identical budgets, as the spec requires -- not because they
# are recommended.
BUDGET_BAND = 'band'
BUDGET_RAREST_FIRST = 'rarest_first'
BUDGET_NO_CAP = 'no_cap'
_BUDGET_POLICIES = (BUDGET_BAND, BUDGET_RAREST_FIRST, BUDGET_NO_CAP)


def _boundary_one_sided(aligned_len: int) -> float:
    # Implementation constants from the asymmetric matcher. NOT a calibrated
    # fit -- the spec records this asymmetry explicitly (section 7.1).
    return 0.28 if aligned_len < 100 else 0.35


def _boundary_two_sided(aligned_len: int) -> float:
    # q95 fit of true-pair densities per length band (liturgy_q95 profile).
    if aligned_len < 100:
        return 0.30
    if aligned_len < 200:
        return 0.386
    return 0.418


@dataclass(frozen=True)
class PassagePolicy:
    """Everything about a query that can change its results.

    Fields deliberately exclude anything baked into the artifact (stride,
    DF cap, normalizer version): those live in the index manifest, and a
    measurement is identified by (index manifest, policy_id) together.
    """
    name: str
    min_span: int = 40                 # normalized letters (spec section 8)
    regime: str = REGIME_ONE_SIDED
    density_scale: float = 1.0         # multiplies the boundary; 1.0 = spec
    budget_policy: str = BUDGET_BAND
    # Defaults MEASURED on the full 702,466-record index (2026-08-20), warm,
    # with strength-ordered verification. Sweeping verify_cap 50K->1K and
    # posting_budget 2M->500K changed self-retrieval NOT AT ALL -- verbatim
    # (7/10, 8/10, 9/10 per length band) and 20%-corrupted two-sided
    # (7/10, 5/10, 10/10) identical in every row -- while p50 fell from
    # 1.0-4.8 s to 0.09-0.7 s. True matches carry tens of distinct anchors,
    # so strength ordering keeps them inside even a small cap; the old
    # generous caps bought nothing but Levenshtein calls on junk. 3K keeps
    # 3x headroom over the smallest cap tested.
    posting_budget: int = 500_000      # postings admitted per query
    candidate_cap: int = 200_000       # diagonal clusters kept at most
    verify_cap: int = 3_000            # Levenshtein calls at most
    min_anchors: int = 2               # distinct gram codes per cluster
    # Verification half-window, letters (spec section 7). 30 comes from the
    # research scripts, where every span was long: at MIN_SPAN 40 it is a 75%
    # overhead and harmless. Below that it DECIDES the result. Measured
    # 2026-08-24: a true 9-letter shared name (a transliterated proper noun,
    # which is identical Hebrew letters in an Aramaic original and a
    # Judeo-Arabic translation) is scored over ~70 letters of unrelated
    # flanking text and rejected at ~0.85 density. Lowering min_span alone
    # therefore changes NOTHING -- rejected_short falls to zero and
    # rejected_density absorbs every one. The pair (min_span, verify_margin)
    # has to move together, which is why this is policy and not a constant.
    verify_margin: int = 30
    schema_version: int = POLICY_SCHEMA_VERSION

    def __post_init__(self):
        if self.regime not in _REGIMES:
            raise ValueError(f'unknown regime {self.regime!r}')
        if self.budget_policy not in _BUDGET_POLICIES:
            raise ValueError(f'unknown budget policy {self.budget_policy!r}')
        if self.min_span < 5:
            raise ValueError('min_span below gram width K=5 can never match')
        if self.min_anchors < 1:
            raise ValueError('min_anchors must be >= 1')
        if not (0.1 <= self.density_scale <= 2.0):
            raise ValueError('density_scale outside [0.1, 2.0]')
        if self.verify_margin < 0:
            raise ValueError('verify_margin must be >= 0')
        for f_name in ('posting_budget', 'candidate_cap', 'verify_cap'):
            if getattr(self, f_name) <= 0:
                raise ValueError(f'{f_name} must be positive')

    # -- identity ----------------------------------------------------------

    @property
    def policy_id(self) -> str:
        """Content hash over every field. Stable across processes and runs.

        Identity extension rule: a field added AFTER measurements were
        recorded (shared/retrieval_eval.py's ledger keys on policy_id) enters
        the hash only once it is moved off the value that reproduces the
        historical behaviour. That way a preset measured before the field
        existed keeps the id its results were filed under, and any policy
        that actually behaves differently necessarily gets a new id.
        """
        d = asdict(self)
        # verify_margin joined the schema on 2026-08-24; at its historical
        # value (30)
        # the query behaves exactly as before, so it enters the
        # hash only when moved off that value.
        if self.verify_margin == 30:
            del d['verify_margin']
        blob = json.dumps(d, sort_keys=True, ensure_ascii=True,
                          separators=(',', ':'))
        return 'pp1-' + hashlib.sha256(blob.encode('ascii')).hexdigest()[:16]

    def as_dict(self) -> dict:
        d = asdict(self)
        d['policy_id'] = self.policy_id
        return d

    # -- the acceptance rule -----------------------------------------------

    def max_density(self, aligned_len: int) -> float:
        base = (_boundary_one_sided(aligned_len)
                if self.regime == REGIME_ONE_SIDED
                else _boundary_two_sided(aligned_len))
        return base * self.density_scale

    def accepts(self, shorter_span_len: int, aligned_len: int,
                density: float) -> bool:
        if shorter_span_len < self.min_span:
            return False
        return density <= self.max_density(aligned_len)


# ---------------------------------------------------------------------------
# Named presets. A UI offers THESE, never a raw slider: a free-floating floor
# makes every setting a separate estimand, and the pre-registered comparison
# only licenses the policies it actually measured.
# ---------------------------------------------------------------------------

STANDARD_40 = PassagePolicy(name='standard-40')
STANDARD_40_NOISY = PassagePolicy(name='standard-40-noisy',
                                  regime=REGIME_TWO_SIDED)
# Exploratory: the flat-25 comparator the span-floor question calls for.
# NOT default-comparable until it has its own held-out measurement.
FLAT_25 = PassagePolicy(name='flat-25', min_span=25)
FLAT_25_NOISY = PassagePolicy(name='flat-25-noisy', min_span=25,
                              regime=REGIME_TWO_SIDED)

# wide-40: the recall-leaning operating point from the 2026-08-21 tradeoff
# sweep (full tune sample, n=300 per instrument): density_scale 1.3 buys
# +9.0/+9.3 recall@50 points over standard-40 on the FGP/witness instruments
# with strict precision on the graded labeled pairs unchanged; median result
# size 4 (p90 139) against standard's 2 (p90 47); latency flat. 1.45 climbs
# further on FGP but doubles the burden again and shows the first precision
# dent -- past the knee. What the wide point's NEW returns are (discovery or
# noise) is measured by the delta grading deck, not assumed.
WIDE_40 = PassagePolicy(name='wide-40', density_scale=1.3)

# wider-40: density_scale 1.6, the operating point the owner's 2026-08-22
# ruling opened up ("a researcher has no problem receiving 50 or even three
# hundred fragments if in the end there is something that fits"). 1.3 had been
# chosen at a knee defined by BURDEN doubling -- a cost that ruling removes.
# Re-measured on 300 FGP tune queries: recall@200 0.820 -> 0.893, median
# manuscripts 3 -> 8, latency still sub-second. Not pushed further because
# recall@50 PEAKS at 1.8 and falls at 2.0 -- past that the target is buried by
# its own noise, so an unbounded display does not make ordering free.
# Its added results are NOT yet graded; that is what deck_delta_wider_v1 asks.
WIDER_40 = PassagePolicy(name='wider-40', density_scale=1.6)

# widest-40: density_scale 1.8, the web GUI's operating point (owner ruling,
# 2026-08-23). Chosen on two live GUI case studies scored against the owner's
# OWN row-by-row grading, both at PRODUCTION caps (verify_cap 3,000):
#
#   Yom Shabbaton (28 owner-verified manuscripts, union of two incumbent
#   modes):  1.0 -> 13/28,  1.3 -> 18/28,  1.6 -> 24/28,  1.8 -> 26/28
#   (one behind chunk-4's 27/28, at ~74% worst-case precision vs its 49%,
#   in 0.6s vs minutes), plus 9 same-series candidates the incumbent never
#   surfaced.  2.0 -> 265 manuscripts returned, STILL 26/28: the cliff.
#   Dror Yikra: 2 -> 5 records, including the one noisy witness (density
#   0.405) that only the incumbent had found.
#
# 1.8 is also where the corpus-wide tune sweep put the recall@50 PEAK
# (0.857, falling to 0.853 at 2.0) -- the last point before the target is
# buried by its own noise. Not the library default: DEFAULT_POLICY stays
# standard-40 so eval tooling keeps choosing explicitly; the web surface
# opts in at web/passage_assets.py::get_passage_searcher. A user-facing
# control for this knob is planned (Phase 146A).
WIDEST_40 = PassagePolicy(name='widest-40', density_scale=1.8)

# max-40: density_scale 2.0, the validation ceiling -- offered in the GUI as
# "Maximal" at the owner's request (2026-08-23, the Birkat Hamazon session),
# with the measured caveats attached rather than hidden: 2.0 is PAST the
# recall@50 peak (ordering starts burying the target in its own noise:
# 0.857 -> 0.853 on the corpus sweep, 265 manuscripts returned for 26
# useful on the Yom Shabbaton case), and on the very query that prompted
# the request it changes nothing VISIBLE -- BH at 1.8 already found 497
# manuscripts of which the 200-group display cap showed 198; 2.0 finds 586
# and still shows 200. The real unlock for many-witness texts is paging
# (Phase 146A). This step exists for narrow-ish queries where the tail
# fits, and becomes fully useful the day paging lands.
MAX_40 = PassagePolicy(name='max-40', density_scale=2.0)

# ---------------------------------------------------------------------------
# The SECOND axis: how short a shared passage may be and still count.
#
# min_span and verify_margin are ONE decision, not two (section 8.1): the span
# floor is checked against the margin-extended window, so at margin 30 a floor
# of 40 is cleared automatically and moving it alone does nothing. They are
# therefore offered as a joint profile, never as independent controls.
#
# 'short' was measured 2026-08-24 on the Antiochus query against the
# 83-positive adjudicated deck, composed onto widest-40:
#
#   widest-40 (span 40, margin 30):  56 manuscripts, 100% precision, 67% recall
#   + short   (span 28, margin 12): 104 manuscripts,  61% precision, 72% recall
#
# It adds five graded positives (a piyyut versifying the scroll, a colophon
# rubric, a Hanukkah piyyut quoting it, a Hebrew version in a siddur, a
# damaged Aramaic copy) AND one witness no method had returned before,
# including word-chunk matching: MS heb. e.45/36, catalogued מגילת אנטיוכוס.
#
# What it does NOT do is reach cross-language witnesses: 1 of the 20
# Judeo-Arabic/rhymed targets. Those share too little contiguous Hebrew-letter
# material with an Aramaic query for any span threshold, which two failed
# experiments (docs/specs section 10.4) established independently.
#
# Score gives no cutoff inside the added rows: the five positives scored
# 35/35/39/58/255 against noise spanning 31-55. The added tail needs eyes.
LENGTH_PROFILES = {
    'normal': (40, 30),   # the historical pair; every width preset's default
    'short': (28, 12),    # measured 2026-08-24, see above
}
DEFAULT_LENGTH = 'normal'


def compose(width: str, length: str = DEFAULT_LENGTH) -> PassagePolicy:
    """A width preset plus a passage-length profile, as one named policy.

    The surface offers two small selects rather than raw sliders: the space
    is a handful of nameable, hashed points, and the two parameters inside
    `length` are coupled in a way a slider would misrepresent (a user
    lowering a span floor alone would see NO change and conclude the control
    was broken). Composed policies carry a derived name and, because
    policy_id is a content hash, their own id -- so a result is always
    traceable to exactly the settings that produced it, measured or not.
    """
    base = get_preset(width)
    if length == DEFAULT_LENGTH:
        return base
    try:
        min_span, verify_margin = LENGTH_PROFILES[length]
    except KeyError:
        raise ValueError(f'unknown passage-length profile {length!r}; '
                         f'known: {sorted(LENGTH_PROFILES)}')
    return replace(base, name=f'{width}+{length}', min_span=min_span,
                   verify_margin=verify_margin)


# The one measured point on the short axis, registered so evaluation tooling
# can name it directly.
SHORT_28 = PassagePolicy(name='short-28', min_span=28, verify_margin=12,
                         density_scale=1.8)

PRESETS = {p.name: p for p in
           (STANDARD_40, STANDARD_40_NOISY, FLAT_25, FLAT_25_NOISY,
            WIDE_40, WIDER_40, WIDEST_40, MAX_40, SHORT_28)}
DEFAULT_POLICY = STANDARD_40


def get_preset(name: str) -> PassagePolicy:
    try:
        return PRESETS[name]
    except KeyError:
        raise ValueError(
            f'unknown policy preset {name!r}; known: {sorted(PRESETS)}')
