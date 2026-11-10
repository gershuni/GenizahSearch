# -*- coding: utf-8 -*-
"""Bidirectional FIST<->CUDL shelfmark bridge (Phase 86).

Reverse-direction sibling to shared/shelfmark_bridge.py (Phase 84):
- Phase 84: libraries.csv <-> CUDL  (cudl_normalize, lookup_cudl)
- Phase 86: FIST.dbo_Inventory.Shelfmark <-> CUDL (this module)

Used ONLY by scripts/generate_synthetic_rows.py at generation time --
NOT a runtime hot path. NORM-04 keeps shelfmark_bridge.py byte-clean;
this module imports cudl_normalize from it but does not mutate it.

Public API:
  fist_to_cudl_keys(fist_shelfmark) -> set[str]
  build_fist_alias_index(fist_conn) -> None
  lookup_fist_by_cudl(classmark) -> Optional[InventoryRecord]
  explain_fist_by_cudl(classmark) -> tuple[str, list[InventoryRecord]]

The explain_ variant exposes the disambiguation status ('not_found',
'single', 'multi_inventory_ambiguous') so generation can classify
residue rows with the correct ambiguity_kind (Codex review HIGH #6).

SQL contract: title-metadata join MUST go through dbo_Signature (Pass 2
HIGH-2 fix). Reference: scripts/export_fist_enrichment.py uses the same
3-table path; the previous shortcut
``dbo_UnitCatalogRec.SignatureId = dbo_InventorySignature.SetSignatureId``
is incorrect against the real database schema.

D-02a normalization patterns:
  1. Mosseri Roman expansion (BOTH dotted and concat forms per Pitfall 1)
  2. FIST data-noise prefix-strip after LAST colon
  3. (N) series-suffix strip -- prefix-gated to T-S F / T-S Ar (Codex MEDIUM)
  4. Or. multi-segment dot-fix (or1080/or1081 letter-digit boundary)
"""
from __future__ import annotations

import logging
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from shared.shelfmark_bridge import cudl_normalize

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

# Roman series -- must match _MOSSERI_CUDL_SERIES in genizah_core.py
_MOSSERI_ROMAN = r"I{1,4}A?|I{0,3}V|VI{0,3}A?|VII{0,3}|VIII|IX|X"
_MOSSERI_FIST_RE = re.compile(
    rf"^Moss\.\s+({_MOSSERI_ROMAN})\s*[,.]\s*(.+)$",
    re.IGNORECASE,
)

# (N) series-suffix strip -- Codex MEDIUM: prefix-gated to T-S F and T-S Ar
# so we don't create spurious aliases on Add./Or./other families that
# happen to carry parentheticals.
#
# MAINTENANCE NOTE (Pass 2 LOW -- Gemini): If CUDL adds parenthetical series
# digits to non-TS-F/TS-Ar families in the future (e.g., Add. (1) starts
# appearing in cambridge_manifests.normalized_shelfmark), update this tuple
# to include the new family prefix. Until then, family-gating prevents
# spurious aliases on Add. that carry legitimate parentheticals.
_SERIES_N_RE = re.compile(r"^(.*?)\((\d+)\)(.*)$")
_SERIES_N_FAMILY_PREFIXES = ("t-s f", "t-s ar")  # case-insensitive match

# Module-level alias-index state (populated by build_fist_alias_index).
_FIST_ALIAS_INDEX: Optional[Dict[str, List["InventoryRecord"]]] = None
# key -> [InventoryRecord, ...]  (list because multi_inventory must surface)


# ---------------------------------------------------------------------------
# InventoryRecord dataclass (Gemini HIGH #8 -- title metadata)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InventoryRecord:
    """A single FIST inventory row resolved by the bridge.

    Fields:
      inventory_id    -- FIST dbo_Inventory.InventoryId (int, opaque)
      fist_shelfmark  -- FIST dbo_Inventory.Shelfmark verbatim
      has_alma        -- True iff dbo_InventoryAlma row exists (Phase 85 SYNTH-04 gate)
      title_heb       -- dbo_UnitCatalogRec.Title (deterministic MIN(UnitCatalogRecId)
                          row per inventory) or None when no UCR row exists
      genizah_title   -- dbo_UnitCatalogRec.GenizahTitleText (same row) or None
    """
    inventory_id: int
    fist_shelfmark: str
    has_alma: bool
    title_heb: Optional[str] = None
    genizah_title: Optional[str] = None


# ---------------------------------------------------------------------------
# fist_to_cudl_keys -- D-02a candidate-key generator
# ---------------------------------------------------------------------------


def fist_to_cudl_keys(fist_shelfmark: str) -> Set[str]:
    """Generate candidate CUDL keys from a FIST shelfmark (D-02a patterns).

    Returns a set of normalized keys that may match dbo_Inventory.Shelfmark
    to a CUDL classmark. Empty / None input returns empty set.

    Patterns applied:
      1. Mosseri Roman expansion with BOTH dotted and concat forms
         (HIGH #1 -- CUDL stores BOTH 'mosseriiii27.1' AND 'mosseriiii271')
      2. Prefix-strip after LAST colon (FIST data-noise: 'AIU: CUL: ...')
      3. (N) series-suffix strip -- FAMILY-GATED to T-S F / T-S Ar
         (Codex MEDIUM -- prevent spurious aliases on Add./Or.)
      4. Or. multi-segment dot-fix ('Or.1080 1.5' -> 'or1080.1.5'/'or1080.15')
    """
    keys: Set[str] = set()
    sm = (fist_shelfmark or "").strip()
    if not sm:
        return keys

    # Pattern 2: FIST data-noise prefix-strip after LAST colon.
    candidates = [sm]
    if ":" in sm:
        after_colon = sm.rsplit(":", 1)[1].strip()
        if after_colon:
            candidates.append(after_colon)

    for c in candidates:
        # Base normalize via Phase 84 normalizer.
        base = cudl_normalize(c)
        if base:
            keys.add(base)

        # Pattern 1: Mosseri Roman expansion (BOTH dotted and concat forms
        # per Pitfall 1 / RESEARCH.md / reviewers' HIGH concern #1).
        m = _MOSSERI_FIST_RE.match(c)
        if m:
            roman = m.group(1).lower()
            rest_norm = cudl_normalize(m.group(2))
            if rest_norm:
                keys.add(f"mosseri{roman}{rest_norm}")
                keys.add(f"mosseri{roman}{rest_norm.replace('.', '')}")  # concat form

        # Pattern 3: (N) series-suffix strip -- FAMILY-GATED to T-S F / T-S Ar
        # (Codex MEDIUM -- prevent spurious aliases on Add. and other families).
        c_lower = c.lower().strip()
        if any(c_lower.startswith(p) for p in _SERIES_N_FAMILY_PREFIXES):
            m2 = _SERIES_N_RE.match(c)
            if m2:
                stripped = (m2.group(1) + m2.group(3)).strip()
                kn = cudl_normalize(stripped)
                if kn:
                    keys.add(kn)

        # Pattern 4: Or. multi-segment dot-fix.
        for prefix in ("or1080", "or1081"):
            if base and base.startswith(prefix) and len(base) > len(prefix) and base[len(prefix)].isdigit():
                keys.add(prefix + "." + base[len(prefix):])

    return keys


# ---------------------------------------------------------------------------
# build_fist_alias_index -- one-shot generation-time index builder
# ---------------------------------------------------------------------------


def build_fist_alias_index(fist_conn: sqlite3.Connection) -> None:
    """Build FIST inventory CUDL-key alias index. Called once at generation time.

    LEFT JOIN dbo_InventoryAlma to detect has_alma.
    LEFT JOIN dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec
    (3-table production-correct path per Pass 2 HIGH-2 -- matches
    scripts/export_fist_enrichment.py join shape) to capture title_heb /
    genizah_title metadata for synthetic-row title fallback (Gemini HIGH #8).
    Tie-break: MIN(UnitCatalogRecId) for determinism.
    """
    global _FIST_ALIAS_INDEX
    builder: Dict[str, List[InventoryRecord]] = defaultdict(list)
    # SQL: one row per Inventory; pick the lowest-UnitCatalogRecId per
    # inventory to keep the result deterministic and avoid fanout.
    #
    # 3-table join path (Pass 2 HIGH-2 fix -- was 2-table shortcut):
    #   dbo_InventorySignature.SetSignatureId
    #     -> dbo_Signature.SetSignatureId
    #     -> dbo_Signature.SignatureId
    #     -> dbo_UnitCatalogRec.SignatureId
    #
    # MED-86-01 (Pass 3 Codex): the previous shape used
    # ``SELECT cat.Title, MIN(cat.UnitCatalogRecId) ... GROUP BY isig.InventoryId``
    # which is non-deterministic -- SQLite permits it, but the non-aggregated
    # title fields are NOT guaranteed to come from the MIN(UnitCatalogRecId)
    # row. The CTE form below pins the title to the deterministic
    # min-rowid row by first computing the per-inventory minimum rec id
    # and then joining back to dbo_UnitCatalogRec on that exact rowid.
    # CTE form is chosen (over ROW_NUMBER() OVER (PARTITION BY ...)) for
    # maximum SQLite version compatibility (window functions need SQLite >= 3.25).
    sql = """
        WITH min_ucr_per_inv AS (
          SELECT
            isig.InventoryId      AS InventoryId,
            MIN(cat.UnitCatalogRecId) AS min_ucr_id
          FROM dbo_InventorySignature isig
          JOIN dbo_Signature        sig ON sig.SetSignatureId = isig.SetSignatureId
          JOIN dbo_UnitCatalogRec   cat ON cat.SignatureId    = sig.SignatureId
          GROUP BY isig.InventoryId
        )
        SELECT
          inv.InventoryId,
          inv.Shelfmark,
          alma.AlmaId,
          ucr_pick.Title            AS title_heb,
          ucr_pick.GenizahTitleText AS genizah_title
        FROM dbo_Inventory inv
        LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId
        LEFT JOIN min_ucr_per_inv m       ON m.InventoryId    = inv.InventoryId
        LEFT JOIN dbo_UnitCatalogRec ucr_pick
               ON ucr_pick.UnitCatalogRecId = m.min_ucr_id
        WHERE inv.Shelfmark IS NOT NULL AND inv.Shelfmark != ''
        ORDER BY inv.InventoryId
    """
    for inv_id, shelfmark, alma_id, title_heb, genizah_title in fist_conn.execute(sql):
        has_alma = alma_id is not None
        rec = InventoryRecord(
            inventory_id=inv_id,
            fist_shelfmark=shelfmark,
            has_alma=has_alma,
            title_heb=title_heb,
            genizah_title=genizah_title,
        )
        for k in fist_to_cudl_keys(shelfmark):
            builder[k].append(rec)
    _FIST_ALIAS_INDEX = dict(builder)
    logger.info(
        "build_fist_alias_index: %d keys from FIST.dbo_Inventory (title metadata via 3-table join)",
        len(_FIST_ALIAS_INDEX),
    )


# ---------------------------------------------------------------------------
# explain_fist_by_cudl -- status-aware lookup (Codex HIGH #6)
# ---------------------------------------------------------------------------


def explain_fist_by_cudl(classmark: str) -> Tuple[str, List[InventoryRecord]]:
    """Resolve a CUDL classmark to FIST inventories WITH explicit status.

    Status values:
      'not_found'                  -- key absent from alias index
      'single'                     -- one distinct InventoryId resolved (D-04 relax)
      'multi_inventory_ambiguous'  -- 2+ distinct InventoryIds (D-04a exclude)

    The entries list contains all matching InventoryRecord objects (in
    InventoryId order). Generation/Plan 02 uses this to differentiate
    residue ambiguity_kind values (no_fist_match vs multi_inventory).
    """
    if not _FIST_ALIAS_INDEX or not classmark:
        return ("not_found", [])
    for k in (classmark, cudl_normalize(classmark)):
        if k and k in _FIST_ALIAS_INDEX:
            entries = list(_FIST_ALIAS_INDEX[k])
            distinct_inv = {e.inventory_id for e in entries}
            if len(distinct_inv) == 0:
                return ("not_found", [])
            if len(distinct_inv) > 1:
                # Sort entries by InventoryId for stable presentation.
                entries_sorted = sorted(entries, key=lambda e: e.inventory_id)
                return ("multi_inventory_ambiguous", entries_sorted)
            # 'single': one distinct InventoryId; sort entries deterministically.
            entries_sorted = sorted(entries, key=lambda e: e.inventory_id)
            return ("single", entries_sorted[:1])
    return ("not_found", [])


# ---------------------------------------------------------------------------
# lookup_fist_by_cudl -- convenience wrapper
# ---------------------------------------------------------------------------


def lookup_fist_by_cudl(classmark: str) -> Optional[InventoryRecord]:
    """Convenience wrapper: return single InventoryRecord or None.

    Returns the record when explain_fist_by_cudl status is 'single';
    None for 'not_found' (D-02b residue) or 'multi_inventory_ambiguous'
    (D-04a exclude). Use explain_fist_by_cudl when you need to disambiguate.
    """
    status, entries = explain_fist_by_cudl(classmark)
    if status == "single" and entries:
        return entries[0]
    return None
