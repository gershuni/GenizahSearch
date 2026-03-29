# -*- coding: utf-8 -*-
"""
Exclusion Service for filtering out known manuscripts from search results.

Provides shared data models and logic for both web and desktop apps:
- ExclusionSource / ResolvedEntry dataclasses for multi-source tracking
- File parsing (txt one-per-line, CSV with column auto-detect)
- Shelfmark resolution via normalize_shelfmark + csv_bank lookup
- Serialization for session persistence
"""

import csv
import io
import logging
from dataclasses import dataclass, field

from genizah_core import normalize_shelfmark

logger = logging.getLogger(__name__)

# Keywords for auto-detecting the shelfmark column in CSV headers
_SHELFMARK_KEYWORDS = {
    'shelfmark', 'shelf_mark', 'call_number', 'signature',
    'classmark', 'shelf mark', 'call_numbers',
}


@dataclass
class ResolvedEntry:
    """Per-row resolution detail for the D-04 resolution report table."""
    original: str
    normalized: str
    sys_id: str | None
    status: str  # 'found', 'not_found', 'duplicate'


@dataclass
class ExclusionSource:
    """A single source of excluded manuscripts (file import or list selection)."""
    label: str
    source_type: str  # 'list' or 'file'
    source_id: str
    sys_ids: set = field(default_factory=set)
    unresolved: list = field(default_factory=list)
    resolved_entries: list = field(default_factory=list)  # transient, not serialized


def parse_shelfmark_file(content: str) -> list[str]:
    """Parse a plain-text file with one shelfmark per line.

    Strips whitespace, ignores blank lines and comment lines (starting with #).
    """
    results = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            results.append(stripped)
    return results


def parse_csv_shelfmarks(content: str) -> list[str]:
    """Parse a CSV file, auto-detecting the shelfmark column.

    Checks headers against known keywords (case-insensitive). Falls back to
    column 0 if no keyword match. Handles UTF-8 BOM encoding.
    """
    reader = csv.reader(io.StringIO(content))
    try:
        headers = next(reader)
    except StopIteration:
        return []

    # Strip BOM from first header cell
    if headers and headers[0].startswith('\ufeff'):
        headers[0] = headers[0].lstrip('\ufeff')

    # Auto-detect shelfmark column
    col_idx = 0
    for i, h in enumerate(headers):
        if h.strip().lower() in _SHELFMARK_KEYWORDS:
            col_idx = i
            break

    results = []
    for row in reader:
        if col_idx < len(row):
            val = row[col_idx].strip()
            if val:
                results.append(val)
    return results


def build_shelf_map(csv_bank: dict) -> dict[str, str]:
    """Build {normalized_shelfmark: sys_id} from csv_bank.

    Indexes both the primary shelfmark and all call_numbers_raw variants.
    Uses first-write-wins so the first sys_id mapped to a normalized form wins.
    """
    shelf_map = {}
    for sys_id, entry in csv_bank.items():
        # Primary shelfmark
        primary = entry.get('shelfmark', '')
        if primary:
            norm = normalize_shelfmark(primary)
            if norm and norm not in shelf_map:
                shelf_map[norm] = sys_id
        # Variant shelfmarks
        for variant in (entry.get('call_numbers_raw') or []):
            if variant:
                norm = normalize_shelfmark(variant)
                if norm and norm not in shelf_map:
                    shelf_map[norm] = sys_id
    return shelf_map


def resolve_shelfmarks(
    raw_lines: list[str],
    shelf_map: dict[str, str],
) -> tuple[set[str], list[str], list[ResolvedEntry]]:
    """Resolve raw shelfmark strings to sys_ids via the shelf_map.

    Returns:
        (resolved_sys_ids, unresolved_lines, resolved_entries)
    """
    resolved_ids: set[str] = set()
    unresolved: list[str] = []
    entries: list[ResolvedEntry] = []
    seen_ids: set[str] = set()

    for raw in raw_lines:
        norm = normalize_shelfmark(raw)
        sys_id = shelf_map.get(norm)

        if sys_id is None:
            unresolved.append(raw)
            entries.append(ResolvedEntry(raw, norm, None, 'not_found'))
        elif sys_id in seen_ids:
            entries.append(ResolvedEntry(raw, norm, sys_id, 'duplicate'))
        else:
            resolved_ids.add(sys_id)
            seen_ids.add(sys_id)
            entries.append(ResolvedEntry(raw, norm, sys_id, 'found'))

    return resolved_ids, unresolved, entries


def compute_excluded_ids(sources: list[ExclusionSource]) -> set[str]:
    """Return the union of all sys_ids across sources."""
    if not sources:
        return set()
    return set().union(*(s.sys_ids for s in sources))


def serialize_sources(sources: list[ExclusionSource]) -> list[dict]:
    """Serialize sources for session persistence.

    Does NOT include resolved_entries (transient, too large for storage).
    """
    return [
        {
            'label': s.label,
            'source_type': s.source_type,
            'source_id': s.source_id,
            'sys_ids': sorted(s.sys_ids),
            'unresolved': s.unresolved,
        }
        for s in sources
    ]


def deserialize_sources(data: list[dict]) -> list[ExclusionSource]:
    """Reconstruct ExclusionSource list from serialized dicts.

    Handles missing/malformed entries gracefully — skips bad items with a warning.
    """
    sources = []
    if not isinstance(data, list):
        logger.warning("deserialize_sources: expected list, got %s", type(data).__name__)
        return sources

    for i, item in enumerate(data):
        try:
            sources.append(ExclusionSource(
                label=item['label'],
                source_type=item['source_type'],
                source_id=item['source_id'],
                sys_ids=set(item.get('sys_ids', [])),
                unresolved=item.get('unresolved', []),
                resolved_entries=[],
            ))
        except (KeyError, TypeError) as e:
            logger.warning("deserialize_sources: skipping malformed entry %d: %s", i, e)

    return sources
