"""Source-agnostic "has manual transcription" presence (SEED-022).

A reader wants to know, on a result row, whether there is a *manual / scholarly*
transcription OR translation to READ for a manuscript -- regardless of which
project produced it. This module computes that union across sources:

    PGP readable text  ∪  FGP sources   (today)
    ∪  user-contributed transcriptions  (FUTURE -- no store exists yet)

It is deliberately ADDITIVE and independent of the existing PGP badge:

* The PGP badge keeps its own *link-presence* helper
  (``document_service.get_sys_ids_with_transcriptions`` -- ~34K sys_ids, "is this
  in PGP at all"). It is NOT touched here.
* This union uses the PGP *text-presence* predicate
  (``document_service.get_sys_ids_with_pgp_text`` -- has_transcription/has_translation,
  ~7.3K sys_ids) so the new tag means "there is genuine manual text to read".

Graceful degradation: FGP returns an empty set when its sidecar/flag is absent, so
the union then simply equals the PGP-text set (still a correct superset by
construction).
"""

from __future__ import annotations

from typing import List, Set


def get_sys_ids_with_manual_transcriptions(
    sys_ids: List[str], *, include_user: bool = True
) -> Set[str]:
    """Union of manual-transcription presence across sources (translations included).

    PGP readable text ∪ FGP sources today; a user-source slot is reserved for the
    future (no ``transcriptions`` store exists yet -- ``corrections`` are edits and
    ``discoveries`` are discussion, so ``include_user`` is a no-op for now and kept
    only so a future store is a one-line add).

    Args:
        sys_ids: System IDs to check (cast to list once; matches the underlying
            helpers which require ``List[str]`` for ``len()``/slicing).
        include_user: Reserved. When a user-transcription store ships, this gates a
            third union term.

    Returns:
        Set of sys_ids that have at least one manual transcription OR translation.
    """
    sys_ids = list(sys_ids or [])
    if not sys_ids:
        return set()

    # Imported lazily so importing this module never forces the FGP/PGP service
    # singletons (and their SQLite connections) to construct.
    from shared.document_service import get_sys_ids_with_pgp_text
    from shared.fgp_service import get_sys_ids_with_fgp_sources

    out: Set[str] = set()
    out |= get_sys_ids_with_pgp_text(sys_ids)        # PGP readable text
    out |= get_sys_ids_with_fgp_sources(sys_ids)     # FGP (editions + translations); set() when absent
    # if include_user:
    #     out |= get_sys_ids_with_user_transcriptions(sys_ids)  # FUTURE -- no store yet
    return out


def union_manual_transcriptions(pgp_text_ids: Set[str], fgp_ids: Set[str]) -> Set[str]:
    """Combine already-fetched per-source sets into the manual-transcription union.

    Use this at call sites that ALREADY fetch the PGP-text and FGP sets (e.g. the
    web enrichment passes) to avoid re-querying PGP. Pure set algebra, no I/O.
    """
    return set(pgp_text_ids) | set(fgp_ids)
