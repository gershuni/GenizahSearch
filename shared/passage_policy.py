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
from dataclasses import asdict, dataclass

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
        for f_name in ('posting_budget', 'candidate_cap', 'verify_cap'):
            if getattr(self, f_name) <= 0:
                raise ValueError(f'{f_name} must be positive')

    # -- identity ----------------------------------------------------------

    @property
    def policy_id(self) -> str:
        """Content hash over every field. Stable across processes and runs."""
        blob = json.dumps(asdict(self), sort_keys=True, ensure_ascii=True,
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

PRESETS = {p.name: p for p in
           (STANDARD_40, STANDARD_40_NOISY, FLAT_25, FLAT_25_NOISY)}
DEFAULT_POLICY = STANDARD_40


def get_preset(name: str) -> PassagePolicy:
    try:
        return PRESETS[name]
    except KeyError:
        raise ValueError(
            f'unknown policy preset {name!r}; known: {sorted(PRESETS)}')
