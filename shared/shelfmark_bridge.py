# -*- coding: utf-8 -*-
"""Bridge module for CUDL shelfmark normalization (Phase 84).

Layered on top of genizah_core.normalize_shelfmark() — does NOT replace it.
Used only at the four cross-system lookup sites listed in Phase 84 D-08:

  1. Shelfmark search fallback (genizah_core.py shelfmark-mode search)
  2. Browse CUDL external-link builder (web/pages/browse.py)
  3. cambridge_manifests reverse lookup (shared/nli_crossref_service.py)
  4. Orphan-scanner unification (scripts/scan_cudl_orphans.py)

Wiring is NOT done in this module — see Plan 04 of Phase 84.

Functions:
  cudl_normalize(s)                  -- full normalization (runtime)
  _normalize_without_zero_collapse(s)-- audit-only sibling (no leading-zero strip)
  lookup_cudl(classmark)             -- alias index lookup (stub until Plan 02/03)
  build_alias_index(csv_bank)        -- populate alias index (stub until Plan 02)
  load_collision_keys(report_path)   -- load D-06 gate file (stub until Plan 03)
  shelfmark_to_cudl_label(shelfmark) -- reverse lookup (stub until Plan 03)
  _is_collision_key(key)             -- check collision safety net
"""
from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Dict, Optional, Set

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled regexes
# ---------------------------------------------------------------------------

#: Exported for scripts/scan_cudl_orphans.py re-import (one source of truth).
NUM_RE = re.compile(r"^(.+?)(\d+)$")

# ---------------------------------------------------------------------------
# Normalizer (NORM-03) — ported verbatim from scripts/scan_cudl_orphans.py:37-58
# ---------------------------------------------------------------------------


def cudl_normalize(s: str) -> str:
    """Normalize a shelfmark for CUDL-vs-libraries.csv matching.

    CUDL collapses dots between letter and digit groups (e.g. ``T-S Ar. 48.211``
    → ``tsar48.211``) but keeps dots between numeric groups. Mirror that:
    drop the dot when it sits at a letter↔digit boundary, keep it between
    digits.

    Implements all four NORM-03 rules:
      - Slash and comma → dot (``T-S F 8/002`` → ``tsf8.2``)
      - Dot adjacent to a letter → stripped (``T-S Ar. 48.211`` → ``tsar48.211``)
      - Leading zeros stripped from numeric segments (``329.0014`` → ``329.14``)
      - General lowercase + whitespace/hyphen/quote removal

    Per Phase 84 D-07 these rules apply uniformly across all CUL/Cambridge
    collections. Do NOT modify this function without running the Phase 86
    regression suite.
    """
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    s = s.replace("ms.", "").replace("-", "").replace('"', "").replace("'", "")
    # Slashes and commas separate numeric groups in libraries.csv but CUDL
    # uses dots (e.g. "T-S F 8/002" → "tsf8.2", "Add. 863, 2" → "add863.2").
    s = s.replace("/", ".").replace(",", ".")
    # Drop dots adjacent to a letter on either side (so "ar.48" -> "ar48",
    # "i.3" -> "i3", but "48.211" stays "48.211").
    s = re.sub(r"(?<=[a-z])\.|\.(?=[a-z])", "", s)
    # Strip leading zeros from numeric segments ("329.0014" → "329.14",
    # "8.002" → "8.2") so libraries.csv slash-zero forms match CUDL.
    s = re.sub(r"(?<=\.)0+(\d)", r"\1", s)
    s = re.sub(r"^0+(\d)", r"\1", s)
    return s


def _normalize_without_zero_collapse(s: str) -> str:
    """cudl_normalize() WITHOUT the two leading-zero stripping regex steps.

    Audit-only sibling of cudl_normalize. Do NOT use at runtime — runtime
    always uses the full cudl_normalize.

    Comparing cudl_normalize(v) vs _normalize_without_zero_collapse(v)
    isolates collisions caused ONLY by the zero-collapse step from collisions
    caused by other rules (slash, comma, dot-after-letter). This is the
    delta isolation required by Phase 84 D-06 / Codex MEDIUM review item #4.
    """
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    s = s.replace("ms.", "").replace("-", "").replace('"', "").replace("'", "")
    s = s.replace("/", ".").replace(",", ".")
    s = re.sub(r"(?<=[a-z])\.|\.(?=[a-z])", "", s)
    # NOTE: the two leading-zero re.sub lines are intentionally omitted here.
    return s


# ---------------------------------------------------------------------------
# Collision safety net (Phase 84 D-06, Gemini LOW review item)
# ---------------------------------------------------------------------------

# Hardcoded collision safety net — applies even when reports/leading_zero_collisions.csv
# is absent or has not yet been generated (Gemini LOW review item).
# Keep this set small and well-documented. Add entries only when the dynamic audit
# has confirmed them as zero-collapse collisions in production.
_BUILTIN_COLLISION_KEYS: Set[str] = set()  # initially empty; populate via audit findings

#: Alias index populated by build_alias_index(). None means not yet built.
_CUDL_ALIAS_INDEX: Optional[Dict[str, tuple]] = None  # {cudl_normalized -> (sys_id, shelfmark)}

#: Leading-zero collision keys loaded from reports/leading_zero_collisions.csv at build time.
_COLLISION_KEYS: Set[str] = set()


def _is_collision_key(key: str) -> bool:
    """Return True if key is a known leading-zero collision (must not be indexed).

    Combines the dynamic audit set (_COLLISION_KEYS, loaded from reports/ CSV) with
    the hardcoded safety net (_BUILTIN_COLLISION_KEYS) so safety holds even if the
    audit report has not been generated yet.
    """
    return key in _COLLISION_KEYS or key in _BUILTIN_COLLISION_KEYS


# ---------------------------------------------------------------------------
# Stub functions (implemented in Plans 02 / 03)
# ---------------------------------------------------------------------------


def lookup_cudl(classmark: str) -> Optional[Dict[str, str]]:
    """Look up a CUDL classmark in the alias index.

    Returns ``{"sys_id": ..., "shelfmark": ...}`` if found, ``None`` otherwise.
    Never raises — graceful-None per shared-service convention.

    The alias index must first be populated by build_alias_index(). Returns None
    if the index has not been built yet or is empty.

    Plan 02/03 implement the full build + lookup logic.
    """
    if not _CUDL_ALIAS_INDEX:
        return None
    key = cudl_normalize(classmark)
    if not key:
        return None
    entry = _CUDL_ALIAS_INDEX.get(key)
    if entry is None:
        return None
    sys_id, shelfmark = entry
    return {"sys_id": sys_id, "shelfmark": shelfmark}


def build_alias_index(csv_bank) -> None:
    """Populate the CUDL alias index from the libraries.csv data bank.

    Stub — Plan 02 implements the full walk-and-index logic for Mosseri,
    CUL, and Or. rows. Does nothing for now.
    """
    pass  # Plan 02 implements this


def load_collision_keys(report_path: Optional[Path] = None) -> int:
    """Load leading-zero collision keys from the audit gate file.

    Reads ``reports/leading_zero_collisions.csv`` (or *report_path* if provided)
    and populates ``_COLLISION_KEYS`` from the ``normalized_key`` column. These
    keys are excluded from the runtime alias index at build time (Phase 84 D-06).

    Returns the number of collision keys loaded. If the file is absent, logs an
    INFO message and returns 0 — the hardcoded ``_BUILTIN_COLLISION_KEYS`` safety
    net still applies.

    Plan 03 calls this from ``build_alias_index`` to enforce D-06 exclusion.
    """
    global _COLLISION_KEYS
    if report_path is None:
        report_path = Path(__file__).resolve().parent.parent / "reports" / "leading_zero_collisions.csv"
    if not report_path.exists():
        logger.info(
            "No leading-zero collisions report at %s; relying on _BUILTIN_COLLISION_KEYS only (%d keys)",
            report_path,
            len(_BUILTIN_COLLISION_KEYS),
        )
        return 0
    keys: Set[str] = set()
    with report_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = row.get("normalized_key", "").strip()
            if k:
                keys.add(k)
    _COLLISION_KEYS = keys
    logger.info("Loaded %d leading-zero collision keys from %s", len(keys), report_path)
    return len(keys)


def shelfmark_to_cudl_label(shelfmark: str) -> Optional[str]:
    """Convert a libraries.csv shelfmark to its CUDL viewer URL classmark form.

    Stub — Plan 03 implements the reverse-map logic (Mosseri, CUL) so the
    browse page "Cambridge" button links to the correct CUDL viewer page.
    Returns None for now.
    """
    return None  # Plan 03 implements this
