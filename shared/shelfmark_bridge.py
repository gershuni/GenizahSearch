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
  _index_key_for_label(label)        -- module-level helper: forward CUDL label -> index key
  lookup_cudl(classmark)             -- alias index lookup (implemented in Plan 02)
  build_alias_index(csv_bank, ...)   -- populate alias index with strict ambiguity exclusion
  load_collision_keys(report_path)   -- load D-06 gate file
  shelfmark_to_cudl_label(shelfmark) -- reverse lookup (stub until Plan 03)
  _is_collision_key(key)             -- check collision safety net
  _write_alias_collision_report(...) -- write ambiguous-key audit CSV

build_alias_index() uses a STRICT ambiguity-exclusion policy (Codex HIGH #2):
any normalized key that maps to >1 distinct sys_id is EXCLUDED from the
runtime index and written to reports/cudl_alias_collisions.csv (overridable
via the report_path parameter — Round 3 Codex MEDIUM).

_index_key_for_label is module-level (not nested) so Plan 03's lookup_cudl
extension and shelfmark_to_cudl_label can reuse the SAME implementation
(Round 3 Codex HIGH #1 — single source of truth).
"""
from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
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
# Module-level _index_key_for_label (Round 3 Codex HIGH #1 — single source of truth)
# ---------------------------------------------------------------------------


def _index_key_for_label(label: str) -> str:
    """Normalize a forward CUDL label (e.g. 'MS-MOSSERI-III-00027-O') into the
    index key form ('mosseriiii27o').

    Strips the leading 'MS' segment that construct_mosseri_cudl_label() emits
    (CUDL viewer slugs drop it), then collapses zfill zeros from numeric segments
    before passing through cudl_normalize.

    MUST be module-level (not nested) — Plan 03's lookup_cudl extension and
    shelfmark_to_cudl_label both call this. ONE implementation, three callers.
    """
    if not label:
        return ""
    segs = label.split('-')
    if segs and segs[0].upper() == 'MS':
        segs = segs[1:]
    parts = []
    for seg in segs:
        if seg.isdigit():
            parts.append(seg.lstrip('0') or '0')
        else:
            parts.append(seg)
    return cudl_normalize('-'.join(parts))


# ---------------------------------------------------------------------------
# Alias index — build + lookup
# ---------------------------------------------------------------------------


def _write_alias_collision_report(
    ambiguous: Dict[str, set],
    report_path: Optional[Path] = None,
) -> None:
    """Write ambiguity-audit CSV with columns key,sys_ids,shelfmarks.

    Round 3 Codex MEDIUM: report_path is now a parameter so unit tests can pass
    tmp_path / 'collisions.csv' and avoid mutating the real diagnostic artifact.
    Default (None) resolves to <project_root>/reports/cudl_alias_collisions.csv.

    Codex LOW (Round 2): swallow OSError quietly in packaged/read-only contexts —
    app startup must not fail or emit WARNING noise on read-only filesystems.
    """
    if report_path is None:
        reports_dir = Path(__file__).resolve().parent.parent / "reports"
        try:
            reports_dir.mkdir(exist_ok=True)
        except OSError as e:
            logger.debug("alias-collisions report skipped (cannot create reports dir): %s", e)
            return
        report_path = reports_dir / "cudl_alias_collisions.csv"
    else:
        # Caller-supplied path (tests). Ensure parent exists.
        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.debug("alias-collisions report skipped (cannot create parent dir): %s", e)
            return
    try:
        with report_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["key", "sys_ids", "shelfmarks"])
            for key in sorted(ambiguous.keys()):
                claims = ambiguous[key]
                sys_ids = sorted({sid for (sid, _s) in claims})
                shelfmarks = sorted({s for (_sid, s) in claims if s})
                w.writerow([key, "|".join(sys_ids), "|".join(shelfmarks)])
    except OSError as e:
        logger.debug("alias-collisions report skipped (write failed): %s: %s", report_path, e)


def build_alias_index(csv_bank, report_path: Optional[Path] = None) -> None:
    """Build the CUDL alias index with strict ambiguity-exclusion (Codex HIGH #2).

    For each CUL/Mosseri row, walk its variants and produce normalized index keys
    via the Mosseri forward path (construct_mosseri_cudl_label) and/or the generic
    CUDL-form path (cudl_normalize). Collect all (sys_id, shelfmark) claims per key
    into a builder set. After the walk, include ONLY keys with exactly one distinct
    sys_id. Write excluded ambiguous keys to report_path (default
    reports/cudl_alias_collisions.csv).

    Args:
        csv_bank: dict-of-dicts from MetadataManager.csv_bank.
        report_path: optional path for the ambiguous-key audit file. Defaults to
            <project_root>/reports/cudl_alias_collisions.csv. Tests should pass
            tmp_path / 'collisions.csv' (Round 3 Codex MEDIUM — keep tests from
            dirtying the real diagnostic artifact in the working tree).
    """
    from genizah_core import construct_mosseri_cudl_label  # late import — break cycle
    global _CUDL_ALIAS_INDEX

    try:
        load_collision_keys()
    except Exception as e:
        logger.debug("load_collision_keys failed (continuing): %s", e)

    index_builder: defaultdict = defaultdict(set)

    for sys_id, data in csv_bank.items():
        variants = data.get('call_numbers_raw') or []
        primary = data.get('shelfmark')
        if primary and primary not in variants:
            variants = list(variants) + [primary]

        canonical = data.get('shelfmark') or (variants[0] if variants else '')
        library_code = data.get('library_code') or ''

        for variant in variants:
            # --- Mosseri forward path (D-03) ---
            if library_code == 'Mosseri':
                label = construct_mosseri_cudl_label(variant)
                if label:
                    key = _index_key_for_label(label)
                    if key and not _is_collision_key(key):
                        index_builder[key].add((sys_id, canonical))

            # --- Generic CUDL-form path (CUL Or./T-S/Add. — applies in Plan 03 too) ---
            if library_code in ('CUL', 'Mosseri'):
                key = cudl_normalize(variant)
                if key and not _is_collision_key(key):
                    index_builder[key].add((sys_id, canonical))

    # Materialize: keep only keys with exactly ONE distinct sys_id.
    final_index: Dict[str, tuple] = {}
    ambiguous: Dict[str, set] = {}
    for key, claim_set in index_builder.items():
        sys_ids = {sid for (sid, _shelf) in claim_set}
        if len(sys_ids) == 1:
            final_index[key] = next(iter(claim_set))
        else:
            ambiguous[key] = claim_set

    _CUDL_ALIAS_INDEX = final_index

    # Round 3 Codex MEDIUM: route the report path through the parameter so tests
    # can inject tmp_path. None → canonical reports/cudl_alias_collisions.csv.
    _write_alias_collision_report(ambiguous, report_path=report_path)

    logger.warning(
        "alias index built: %d keys, %d ambiguous keys excluded",
        len(final_index), len(ambiguous),
    )


def lookup_cudl(classmark: str) -> Optional[Dict[str, str]]:
    """Map a CUDL classmark to a libraries.csv row. Returns None if no match
    OR if the key was excluded as ambiguous.

    Tries the plain cudl_normalize form first (covers CUDL slug inputs like
    'mosseriiii27o'); on miss, also tries _index_key_for_label (covers
    forward-label inputs like 'MS-MOSSERI-III-00027-O').

    The alias index must first be populated by build_alias_index(). Returns None
    if the index has not been built yet or is empty.

    Plan 03 will EXTEND this with the Or.-collapse retry — Plan 03 must NOT
    replace this body.
    """
    if _CUDL_ALIAS_INDEX is None or not _CUDL_ALIAS_INDEX:
        return None
    if not classmark:
        return None
    k1 = cudl_normalize(classmark)
    hit = _CUDL_ALIAS_INDEX.get(k1) if k1 else None
    if hit is None:
        # Forward-label form (e.g. "MS-MOSSERI-III-00027-O") — strip MS- and zfill
        # via the module-level _index_key_for_label helper. Plan 03 extends this
        # cascade with an Or.-only numeric-collapse third tier.
        k2 = _index_key_for_label(classmark)
        if k2 and k2 != k1:
            hit = _CUDL_ALIAS_INDEX.get(k2)
    if hit is None:
        return None
    sys_id, shelfmark = hit
    return {"sys_id": sys_id, "shelfmark": shelfmark}


# ---------------------------------------------------------------------------
# Collision key loader (Phase 84 D-06)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Stub: shelfmark_to_cudl_label (Plan 03)
# ---------------------------------------------------------------------------


def shelfmark_to_cudl_label(shelfmark: str) -> Optional[str]:
    """Convert a libraries.csv shelfmark to its CUDL viewer URL classmark form.

    Stub — Plan 03 implements the reverse-map logic (Mosseri, CUL) so the
    browse page "Cambridge" button links to the correct CUDL viewer page.
    Returns None for now.
    """
    return None  # Plan 03 implements this
