# -*- coding: utf-8 -*-
"""fam-v1 — the versioned five-way family assignment (schema Amendment
2026-08-12 (S); CD schema batch).

ONE canonical recipe, deliberately shared by the lock emitter
(``scripts/emit_population_lock.py``) and the release verifier's retention
gate — the same never-duplicated posture as ``compute_frame_content_hash``.
The population lock's per-family counts are meaningless unless every future
recomputation assigns families identically, so the rule lives here, versioned,
and nowhere else.

Design constraints, in force:

- **Computable from the asset alone.** The rule reads ``works.genre`` and
  ``works.source_corpus`` plus a pinned override list carried IN the asset's
  own meta (``population_lock_daf_overrides``) — never the locus tables, which
  are legitimately EMPTY between the schema batch and the D-track import. A
  family label that shifted with locus availability would turn the 22-work
  skip fix into a phantom retention breach (Codex pre-flight finding 8).
- **The base rule is the A0c-frozen derivation** (``a0c_sample.py::family``),
  under which the sample was drawn and graded. Changing it re-labels graded
  rows; that is a fam-v2, a new lock, and an A0b conversation — never an edit
  here.
- **The daf stratum is measured, not derivable from genre.** The preflight's
  "daf (other)" family (1,026 main-pool rows) is exactly the works whose base
  family is ``other_staged`` and whose locus grain is ``daf_rif`` — the Rif-
  style daf-addressed works. Genre alone cannot see them, so their canonical
  ids are pinned at emit time (measured against the locus artifact) and
  carried as the override list. Reproduction check (2026-08-12, public asset
  ``discovery-v3-PUBLIC``): bible 19,885 / canonical 1,823 / daf 1,026 /
  ja 3,842 / other_staged 1,888 = 28,464 — the preflight's §2 table, exactly.
- **Precedence: language first, content second** (the Contract-3 frame rule):
  Arabic tafsir sits in ``ja``… by corpus code, which subsumes the tafsir
  case — a JA-corpus work is ``ja`` regardless of genre.
"""
from __future__ import annotations

from typing import Collection, Optional

FAMILY_VERSION = "fam-v1"

FAMILY_BIBLE = "bible"
FAMILY_CANONICAL = "canonical"
FAMILY_DAF = "daf"
FAMILY_JA = "ja"
FAMILY_OTHER_STAGED = "other_staged"
FAMILIES = (
    FAMILY_BIBLE,
    FAMILY_CANONICAL,
    FAMILY_DAF,
    FAMILY_JA,
    FAMILY_OTHER_STAGED,
)

# Genre substrings, lowercase, FROZEN with the A0c sample's own derivation.
_BIBLE_GENRE_MARKERS = ("bible: texts", "targumim", "tafsir")
_CANONICAL_GENRE_MARKERS = ("talmud bavli", "talmud yerushalmi", "mishnah", "tosefta")


def base_family(genre: Optional[str], source_corpus: Optional[str]) -> str:
    """The A0c-frozen four-way: ja / bible / canonical / other_staged."""
    g = (genre or "").lower()
    if source_corpus == "ja":
        return FAMILY_JA
    if any(marker in g for marker in _BIBLE_GENRE_MARKERS):
        return FAMILY_BIBLE
    if any(marker in g for marker in _CANONICAL_GENRE_MARKERS):
        return FAMILY_CANONICAL
    return FAMILY_OTHER_STAGED


def assign_family(
    genre: Optional[str],
    source_corpus: Optional[str],
    canonical_work_id: Optional[str],
    daf_override_canonical_ids: Collection[str],
) -> str:
    """The fam-v1 five-way: the base rule, then the pinned daf overrides.

    An override applies ONLY to a base ``other_staged`` row — a canonical id
    that later drifted into another base family keeps that family, so a stale
    override can narrow the daf stratum but never corrupt another one."""
    fam = base_family(genre, source_corpus)
    if fam == FAMILY_OTHER_STAGED and canonical_work_id in set(daf_override_canonical_ids):
        return FAMILY_DAF
    return fam
