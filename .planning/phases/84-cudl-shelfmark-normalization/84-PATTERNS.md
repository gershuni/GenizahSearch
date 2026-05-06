# Phase 84: CUDL Shelfmark Normalization - Pattern Map

**Mapped:** 2026-05-06
**Files analyzed:** 8 (4 new, 4 modified)
**Analogs found:** 8 / 8

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| **NEW** `shared/shelfmark_bridge.py` | shared service (pure-fn module + module-level alias index) | transform + dict-lookup | `shared/translation_qc.py` (pure-fn module) + `genizah_core.py:194-298` (`normalize_shelfmark`, `construct_mosseri_cudl_label`) | exact |
| **NEW** `tests/fixtures/cudl_must_resolve.csv` | test fixture | static data | `reports/cudl_orphans_with_neighbor.csv` (DictWriter CSV with header row) | role-match (no prior `tests/fixtures/` dir exists — create it) |
| **NEW** `scripts/audit_leading_zero_collisions.py` | one-shot audit script | CSV walk + dict collision detect | `scripts/scan_cudl_orphans.py` (same shape: load libraries.csv → build norm dict → emit CSV report) | exact |
| **NEW** `tests/test_shelfmark_bridge.py` | unit + golden-fixture test | pytest assert | `tests/test_mosseri_cudl.py` (unit tests for normalizer in same domain) | exact |
| **MODIFIED** `genizah_core.py` (~line 4487) | search method (within `MetadataManager.search_by_meta`) | request-response | self — already-existing fallthrough pattern at line 4498 (`if field == 'shelfmark'`) | self-modification |
| **MODIFIED** `web/pages/browse.py` (lines 3605-3624) | web page link-builder | request-response | self — current naive `.replace(' ', '-')` on line 3607 | self-modification |
| **MODIFIED** `shared/nli_crossref_service.py` (lines 313, 337) | sidecar DB lookup | dict-lookup over SQLite | self — `get_cambridge_manifest()` and `get_cambridge_manifest_by_label()` adjacent | self-modification |
| **MODIFIED** `scripts/scan_cudl_orphans.py` (line 37) | audit script | CSV walk | self — replace local `normalize()` with bridge import | self-modification |

## Pattern Assignments

### NEW `shared/shelfmark_bridge.py` (shared service, pure functions + alias index)

**Primary analogs:**
- `shared/translation_qc.py` — module-level pure-function organization + module-level regex constants
- `scripts/scan_cudl_orphans.py:37-58` — `normalize()` function to PORT VERBATIM (per `<code_context>` "port these into the bridge module rather than re-deriving them")
- `genizah_core.py:259-298` — `construct_mosseri_cudl_label()` to CALL (not duplicate) for the alias index per D-03

**Module header pattern** (copy from `shared/translation_qc.py:1-15`):
```python
# -*- coding: utf-8 -*-
"""
Shelfmark Bridge for CUDL classmark <-> libraries.csv reverse mapping.

Layered on top of genizah_core.normalize_shelfmark() — does NOT replace it.
Used only at the four cross-system lookup sites listed in Phase 84 D-08.

[... rationale ...]
"""

import csv
import logging
import re
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)
```

**Pure-function `cudl_normalize()` — port from `scripts/scan_cudl_orphans.py:37-58`:**
```python
NUM_RE = re.compile(r"^(.+?)(\d+)$")

def cudl_normalize(s: str) -> str:
    """Normalize a shelfmark for CUDL-vs-libraries.csv matching.

    CUDL collapses dots between letter and digit groups (e.g. ``T-S Ar. 48.211``
    → ``tsar48.211``) but keeps dots between numeric groups. ...
    """
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    s = s.replace("ms.", "").replace("-", "").replace('"', "").replace("'", "")
    s = s.replace("/", ".").replace(",", ".")
    s = re.sub(r"(?<=[a-z])\.|\.(?=[a-z])", "", s)
    s = re.sub(r"(?<=\.)0+(\d)", r"\1", s)
    s = re.sub(r"^0+(\d)", r"\1", s)
    return s
```

**Module-level alias index pattern** (mirrors `genizah_core.py:3343-3400` `_load_csv_bank` style — load once, store in module dict; reset to None for lazy/forced rebuild):
```python
# Module-level state populated once at startup. None = not yet built.
_CUDL_ALIAS_INDEX: Optional[Dict[str, str]] = None  # {cudl_normalized -> sys_id}
_COLLISION_KEYS: set = set()  # leading-zero collisions excluded per D-06

def build_alias_index(csv_bank: Dict[str, dict]) -> None:
    """Populate the CUDL alias index from an already-loaded csv_bank.

    Call once after MetadataManager._load_csv_bank() completes (genizah_core.py:3400).
    Reuses construct_mosseri_cudl_label() (genizah_core.py:259) — no reverse parser.
    """
    global _CUDL_ALIAS_INDEX
    from genizah_core import construct_mosseri_cudl_label  # late import to avoid cycle

    index: Dict[str, str] = {}
    for sys_id, data in csv_bank.items():
        for variant in data.get('call_numbers_raw') or []:
            # Mosseri forward path
            label = construct_mosseri_cudl_label(variant)
            if label:
                key = cudl_normalize(label)
                if key and key not in _COLLISION_KEYS:
                    index.setdefault(key, sys_id)
            # Generic CUDL-form path (Or., T-S, Add.)
            key = cudl_normalize(variant)
            if key and key not in _COLLISION_KEYS:
                index.setdefault(key, sys_id)
    _CUDL_ALIAS_INDEX = index
    logger.info("Built CUDL alias index: %d entries", len(index))
```

**Lookup function shape** (D-04 — return both sys_id AND canonical shelfmark; let callers pick):
```python
def lookup_cudl(classmark: str) -> Optional[Dict[str, str]]:
    """Map a CUDL classmark to a libraries.csv row.

    Returns {'sys_id': ..., 'shelfmark': ...} or None.
    """
```

**Cross-system call-site precedent** — `genizah_core.py:194-242` (`normalize_shelfmark`) shows the project's preferred shape for a normalizer: pure function, lowercase, regex-driven, multiple progressive `re.sub` rules, prefix-stripping `if cleaned.startswith(...)` blocks. Mirror that style for any helper functions in the bridge.

---

### NEW `tests/fixtures/cudl_must_resolve.csv` (test fixture)

**No existing `tests/fixtures/` directory** — must be created. Closest analog for shape: `reports/cudl_orphans_with_neighbor.csv` (DictWriter, header row, comma-separated).

**Suggested columns** (drawn from D-09 fixture spec):
```csv
cudl_classmark,expected_sys_id,expected_shelfmark,category,notes
mosseriiii27o,990001234560205171,Moss. III 27O,mosseri,letter-suffix
tsar48.211,...,T-S Ar. 48.211,ts-ar,dot-after-letter
or1080j15,...,Or. 1080 J 15,or-letter-suffix,
or1080.11,...,Or. 1080.1.1,or-numeric-collapse,
tsf8.2,...,T-S F 8/002,ts-f,leading-zero
add863.2,...,Add. 863, 2,add,comma-separator
tsns329.14,...,T-S NS 329/0014,ts-ns,slash-leading-zero
```

Aim for ~50 rows spanning the 7 categories listed in D-09.1.

---

### NEW `scripts/audit_leading_zero_collisions.py` (one-shot audit)

**Analog:** `scripts/scan_cudl_orphans.py` — same shape (load libraries.csv → walk variants → emit CSV report under `reports/`).

**Header + path setup pattern** (copy from `scripts/scan_cudl_orphans.py:21-32`):
```python
from __future__ import annotations

import csv
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LIBRARIES_CSV = ROOT / "libraries.csv"
REPORTS_DIR = ROOT / "reports"
```

**CSV reader idiom** (copy from `scripts/scan_cudl_orphans.py:66-78`):
```python
norm_to_sys: dict[str, list[str]] = defaultdict(list)
with LIBRARIES_CSV.open("r", encoding="utf-8", newline="") as f:
    for row in csv.reader(f):
        if len(row) < 4 or row[3] != "CUL":
            continue
        sys_id = row[0]
        for variant in (row[2] or "").split("|"):
            v = variant.strip()
            if not v:
                continue
            n = normalize_with_leading_zero_collapse(v)
            if n:
                norm_to_sys[n].append(sys_id)

# D-06: emit collisions where 1 normalized key maps to >1 distinct sys_id
collisions = {k: sids for k, sids in norm_to_sys.items() if len(set(sids)) > 1}
```

**Output writer pattern** (copy from `scripts/scan_cudl_orphans.py:130-143`):
```python
out_path = REPORTS_DIR / "leading_zero_collisions.csv"
with out_path.open("w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["normalized_key", "sys_ids", "variants"])
    for k, sids in collisions.items():
        w.writerow([k, "|".join(sorted(set(sids))), ...])

if __name__ == "__main__":
    raise SystemExit(main())
```

The set of collision keys produced here gets fed into `_COLLISION_KEYS` in `shared/shelfmark_bridge.py` (either via a generated module or a checked-in constant).

---

### NEW `tests/test_shelfmark_bridge.py` (unit + golden-fixture tests)

**Analog:** `tests/test_mosseri_cudl.py` — same domain, established class-based test layout.

**Test class skeleton** (copy from `tests/test_mosseri_cudl.py:1-13`):
```python
"""Tests for shared.shelfmark_bridge — CUDL classmark <-> libraries.csv mapping."""

from shared.shelfmark_bridge import cudl_normalize, lookup_cudl, build_alias_index


class TestCudlNormalize:
    """Unit tests for the cudl_normalize() pure function."""

    def test_dot_after_letter_dropped(self):
        assert cudl_normalize("T-S Ar. 48.211") == "tsar48.211"

    def test_slash_to_dot(self):
        assert cudl_normalize("T-S F 8/002") == "tsf8.2"

    def test_comma_to_dot(self):
        assert cudl_normalize("Add. 863, 2") == "add863.2"
```

**Golden-fixture loader** (D-09.1 — load `tests/fixtures/cudl_must_resolve.csv` and parametrize):
```python
import csv
from pathlib import Path
import pytest

_FIXTURE = Path(__file__).parent / "fixtures" / "cudl_must_resolve.csv"

def _load_fixture():
    with _FIXTURE.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

@pytest.mark.parametrize("row", _load_fixture(), ids=lambda r: r["cudl_classmark"])
def test_cudl_must_resolve(row):
    result = lookup_cudl(row["cudl_classmark"])
    assert result is not None, f"{row['cudl_classmark']} ({row['category']}) failed to resolve"
    assert result["sys_id"] == row["expected_sys_id"]
```

**Canonical-untouched assertion (D-09.3)** — direct shape:
```python
from genizah_core import normalize_shelfmark

@pytest.mark.parametrize("shelfmark,expected", [
    ("MS. Heb. a.1", "heba1"),         # Oxford
    ("Yevr. III B 1093", "evriiib1093"), # RNL
    ("ENA-MS 2956", "ena2956"),         # JTS
    # ... ~10 representative non-CUL cases
])
def test_canonical_normalizer_unchanged(shelfmark, expected):
    assert normalize_shelfmark(shelfmark) == expected
```

---

### MODIFIED `genizah_core.py` (~line 4487 — wiring site #1)

**Current code at 4474-4509** shows the project's "fallthrough" search style: try canonical match first, then progressively looser matches (`val_normalized == q_normalized` → dot-agnostic → prefix). The bridge call slots in as a final fallback.

**Pattern to insert (after canonical fails):**
```python
# Existing canonical lookup is unchanged (D-02).
# Fallback: try CUDL classmark form (Phase 84 NORM-01/02)
if field == 'shelfmark' and not results:
    from shared.shelfmark_bridge import lookup_cudl
    hit = lookup_cudl(query)
    if hit:
        results.add(hit["sys_id"])
```

**Late import idiom** for cross-package import to avoid cycles — same pattern as `_load_csv_bank` and `build_alias_index` callsite. The bridge build hook should be added immediately after `genizah_core.py:3400` (`LOGGER.info("Loaded %d records into csv_bank ...")`).

---

### MODIFIED `web/pages/browse.py:3607` (wiring site #2)

**Current code (line 3605-3607):**
```python
cudl_url = page.external_url or ''
if not cudl_url and page.shelfmark:
    cudl_url = f"https://cudl.lib.cam.ac.uk/view/{page.shelfmark.replace(' ', '-')}"
```

**Replacement pattern** — call bridge to translate libraries.csv shelfmark → CUDL form, then build URL:
```python
if not cudl_url and page.shelfmark:
    from shared.shelfmark_bridge import shelfmark_to_cudl_label
    cudl_label = shelfmark_to_cudl_label(page.shelfmark) or page.shelfmark.replace(' ', '-')
    cudl_url = f"https://cudl.lib.cam.ac.uk/view/{cudl_label}"
```

Note the `or` fallback preserves current behavior on miss — no regression risk for already-working CUL rows.

---

### MODIFIED `shared/nli_crossref_service.py:313, 337` (wiring site #3)

**Current code (lines 309-321 + 333-345):** two SQL lookups — `WHERE normalized_shelfmark = ?` and `WHERE label = ?`.

**Wiring pattern** — at each call site to these methods (NOT in the methods themselves; the service stays pure SQLite), the caller should fall back to the bridge when the canonical normalized lookup fails. Or alternately add a wrapper:

```python
def get_cambridge_manifest_with_bridge(self, shelfmark: str) -> Optional[str]:
    """Try canonical normalized lookup first; on miss, try CUDL bridge form."""
    from genizah_core import normalize_shelfmark
    from shared.shelfmark_bridge import cudl_normalize

    # Canonical path (unchanged D-02)
    url = self.get_cambridge_manifest(normalize_shelfmark(shelfmark))
    if url:
        return url
    # Bridge fallback — CUDL stores normalized_shelfmark in CUDL form
    return self.get_cambridge_manifest(cudl_normalize(shelfmark))
```

**Key insight**: `cambridge_manifests.normalized_shelfmark` is itself stored in CUDL form (per `nli_crossref.db` schema), so the bridge `cudl_normalize()` is the right normalizer for queries against this table — different from the rest of the codebase.

---

### MODIFIED `scripts/scan_cudl_orphans.py:37` (wiring site #4)

**Current `normalize()` definition (lines 37-58)** — port verbatim into bridge as `cudl_normalize()`. Replace at line 37 with import:

```python
# Before:
def normalize(s: str) -> str:
    """..."""
    ...

# After:
from shared.shelfmark_bridge import cudl_normalize as normalize
```

All call sites within the script (`normalize(v)` at line 76, `normalize(ns)` at line 94) keep working unchanged. This satisfies D-08.4 — one source of truth — and is the lowest-risk change in the wiring set.

---

## Shared Patterns

### Late Import to Break Cycles
**Source:** `shared/document_service.py` and `shared/nli_crossref_service.py` already use `from shared.thread_local_db import ThreadLocalConnection` at module top. For `genizah_core` ↔ `shared.shelfmark_bridge` (the bridge calls `construct_mosseri_cudl_label`, and `genizah_core.search_by_meta` calls the bridge), use late imports inside functions:
```python
def build_alias_index(csv_bank):
    from genizah_core import construct_mosseri_cudl_label
    ...
```
**Apply to:** `shared/shelfmark_bridge.py` (calls into `genizah_core`) and the genizah_core `search_by_meta` modification (calls into the bridge).

### Module-Level Compiled Regex
**Source:** `shared/translation_qc.py:22-36`, `genizah_core.py:253-256`, `scripts/scan_cudl_orphans.py:34`
```python
NUM_RE = re.compile(r"^(.+?)(\d+)$")
_LETTER_DOT_RE = re.compile(r"(?<=[a-z])\.|\.(?=[a-z])")
```
**Apply to:** `shared/shelfmark_bridge.py` regex constants.

### Logger Per-Module
**Source:** `shared/nli_crossref_service.py:26`, `shared/document_service.py:41`, `shared/translation_qc.py` (no logger needed — pure utility)
```python
logger = logging.getLogger(__name__)
```
**Apply to:** `shared/shelfmark_bridge.py` — needed for the "log and exclude" collision behavior in D-06.

### Graceful Degradation on Missing Data
**Source:** `shared/document_service.py:22-24`, `shared/nli_crossref_service.py:9-11` ("All methods handle errors gracefully, returning None or empty lists rather than raising exceptions").
**Apply to:** `lookup_cudl()` returns `None` when alias index not yet built or no match. Never raise.

### Pytest Class-per-Function Layout
**Source:** `tests/test_mosseri_cudl.py:6` — `class TestConstructMosseriCudlLabel:` groups all tests for one function.
**Apply to:** `tests/test_shelfmark_bridge.py` — separate classes for `TestCudlNormalize`, `TestLookupCudl`, `TestAliasIndex`, plus a top-level parametrize for the golden fixture and the canonical-untouched check.

## No Analog Found

None — every new file has a strong precedent in the codebase.

## Metadata

**Analog search scope:** `shared/`, `scripts/`, `tests/`, `genizah_core.py`, `web/pages/browse.py`
**Files scanned:** ~15
**Pattern extraction date:** 2026-05-06
