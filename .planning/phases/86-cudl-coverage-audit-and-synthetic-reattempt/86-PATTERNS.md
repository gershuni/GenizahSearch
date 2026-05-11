# Phase 86: CUDL Coverage Audit + Synthetic Re-attempt - Pattern Map

**Mapped:** 2026-05-10
**Files analyzed:** 17 (4 NEW code, 1 REWRITE, 6 NEW data/report, 6 NEW research/test)
**Analogs found:** 17 / 17

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/fist_cudl_bridge.py` | service / shared module | transform (one-shot at generation time) | `shared/shelfmark_bridge.py` | exact (sibling, mirrors API surface) |
| `scripts/generate_synthetic_rows.py` (REWRITE `_build_qualifying_inventories`) | script / batch | batch transform (CUDL walk → resolve via FIST → emit) | self (existing implementation) | self-rewrite — preserve outer contract |
| `scripts/audit_nli_attribution.py` | script / one-shot scan | batch read + assert | `scripts/fix_nli_oxford_mislabel.py` | exact (regex + CSV row scan, but read-only) |
| `tests/test_fist_cudl_bridge.py` | test (unit) | request-response (function-level) | `tests/test_shelfmark_bridge_unit_index.py` | exact |
| `tests/test_synthetic_generation_phase86.py` | test (integration) | request-response (function-level + script-level) | `tests/test_generate_synthetic_rows.py` | exact (extend with CUDL-walk fixtures) |
| `tests/test_nli_oxford_attribution.py` | test (regression) | batch read + assert | `tests/test_synthetic_sys_id.py::TestNoIntCoercion` (repo-walking lint pattern) + `tests/test_shelfmark_bridge.py` (golden-fixture parametrize) | hybrid |
| `tests/fixtures/nli_oxford_flipped_sysids.txt` | test fixture | static data | `tests/fixtures/cudl_must_resolve.csv` (golden CSV referenced by parametrize) | role-match |
| `reports/cudl_coverage.md` | report (durable artifact) | batch write (markdown) | `reports/synthetic_coverage.md` (Phase 85) — produced by `_write_coverage` | exact |
| `reports/scan_cudl_orphans_post_phase86.txt` | report (text dump) | batch write | (existing console output of `scan_cudl_orphans.py`; no current text artifact — just stdout) | partial (re-run with stdout redirect) |
| `reports/cudl_orphans_all_post_phase86.csv` | report (CSV) | batch write | `reports/cudl_orphans_all_post_phase84.csv` | exact (same script, new --out-suffix) |
| `reports/cudl_orphans_with_neighbor_post_phase86.csv` | report (CSV) | batch write | `reports/cudl_orphans_with_neighbor_post_phase84.csv` | exact (same script, new --out-suffix) |
| `.planning/phases/86-.../86-RESIDUE-PATTERNS.md` | research artifact | static document | (no direct analog — this is novel D-02c human-in-loop iteration venue) | none |
| `libraries.csv` (synthetic block) | data | batch rewrite (marker-fenced) | self — Phase 85 marker-block contract preserved | exact |
| `fist_data/synthetic_manifest.json` | data | batch write (JSON) | self — Phase 85 `_write_manifest` contract preserved | exact |
| `fist_data/fjms_enrichment.db` | data (regenerated) | batch write | regenerate via existing `scripts/export_fist_enrichment.py` (UNCHANGED) | exact |
| `CHANGELOG.md` | docs | append | self (v7.x release entries) | exact |
| `CLAUDE.md` (Recently Changed) | docs | append | self (Recently Changed list) | exact |
| `docs/OPEN_ISSUES.md` | docs | update (mark fixed) | self | exact |

---

## Pattern Assignments

### `shared/fist_cudl_bridge.py` (service / shared module, transform)

**Analog:** `shared/shelfmark_bridge.py` (Phase 84, 465 lines)

**Module docstring contract** (analog lines 1-39):
```python
"""Bidirectional FIST↔CUDL shelfmark bridge (Phase 86).

Reverse-direction sibling to shared/shelfmark_bridge.py (Phase 84):
- Phase 84: libraries.csv ↔ CUDL  (cudl_normalize, lookup_cudl)
- Phase 86: FIST.dbo_Inventory.Shelfmark ↔ CUDL (this module)

Used ONLY by scripts/generate_synthetic_rows.py at generation time —
NOT a runtime hot path. NORM-04 keeps shelfmark_bridge.py byte-clean;
this module imports cudl_normalize from it but does not mutate it.

Functions:
  fist_to_cudl_keys(fist_shelfmark) -> set[str]
  build_fist_alias_index(fist_conn) -> None
  lookup_fist_by_cudl(classmark) -> Optional[InventoryRecord]
"""
```

**Imports pattern** (analog lines 40-49 → mirror):
```python
from __future__ import annotations
import csv
import logging
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from shared.shelfmark_bridge import cudl_normalize  # reuse base normalizer
```

**Module-level state** (analog lines 156-160):
```python
# Mirror Phase 84 alias-index pattern: module-level dict, populated by builder.
_FIST_ALIAS_INDEX: Optional[Dict[str, List[Tuple[int, str, bool]]]] = None
# key -> [(inventory_id, fist_shelfmark, has_alma), ...]
```

**Normalizer-fan-out pattern** (NEW — research §"Common Operation 1", D-02a):
```python
# Mirror Phase 84 _index_key_for_label single-source-of-truth pattern.
# Keys generated MUST include both base form AND each pattern variant.
_MOSSERI_FIST_RE = re.compile(
    rf"^Moss\.\s+({_MOSSERI_ROMAN})\s*[,.]\s*(.+)$",
    re.IGNORECASE,
)
_SERIES_N_RE = re.compile(r"^(.*?)\((\d+)\)(.*)$")

def fist_to_cudl_keys(fist_shelfmark: str) -> Set[str]:
    """Generate candidate CUDL keys from a FIST shelfmark (D-02a confirmed patterns)."""
    keys: Set[str] = set()
    sm = (fist_shelfmark or "").strip()
    if not sm:
        return keys
    candidates = [sm]
    if ":" in sm:                                # D-02a prefix-strip
        candidates.append(sm.rsplit(":", 1)[1].strip())
    for c in candidates:
        base = cudl_normalize(c)                 # D-02a base normalize
        if base:
            keys.add(base)
        # Mosseri Roman, (N) series-strip, Or. dot-fix per D-02a ...
    return keys
```

**Alias-index builder pattern** (analog lines 249-326 → mirror with FIST schema):
```python
# Mirror Phase 84 build_alias_index pattern:
# (1) walk source rows, (2) accumulate (key -> claims) via fist_to_cudl_keys,
# (3) materialize final dict, (4) write collision report for ambiguous keys.

def build_fist_alias_index(fist_conn: sqlite3.Connection) -> None:
    """Build FIST.dbo_Inventory CUDL-key alias index. Called once at generation time."""
    global _FIST_ALIAS_INDEX
    builder: Dict[str, List[Tuple[int, str, bool]]] = defaultdict(list)
    for inv_id, shelfmark, alma_id in fist_conn.execute("""
        SELECT inv.InventoryId, inv.Shelfmark, alma.AlmaId
        FROM dbo_Inventory inv
        LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId
        WHERE inv.Shelfmark IS NOT NULL AND inv.Shelfmark != ''
        ORDER BY inv.InventoryId
    """):
        has_alma = alma_id is not None
        for k in fist_to_cudl_keys(shelfmark):
            builder[k].append((inv_id, shelfmark, has_alma))
    _FIST_ALIAS_INDEX = dict(builder)
```

**Lookup pattern with multi_inventory exclusion** (analog lines 329-364 → mirror; D-04a):
```python
@dataclass(frozen=True)
class InventoryRecord:
    inventory_id: int
    fist_shelfmark: str
    has_alma: bool

def lookup_fist_by_cudl(classmark: str) -> Optional[InventoryRecord]:
    """Resolve a CUDL classmark to a FIST inventory.

    Returns None for: not found, multi-inventory ambiguous, empty index.
    Returns the lowest-InventoryId for unambiguous + multi-signature
    (D-04 relax: multiple Signatures within one Inventory is OK).
    """
    if not _FIST_ALIAS_INDEX or not classmark:
        return None
    candidates = [classmark, cudl_normalize(classmark)]
    for k in candidates:
        if k and k in _FIST_ALIAS_INDEX:
            entries = _FIST_ALIAS_INDEX[k]
            distinct_inv = {e[0] for e in entries}
            if len(distinct_inv) > 1:
                return None  # D-04a multi_inventory exclude
            inv_id, shelfmark, has_alma = sorted(entries)[0]
            return InventoryRecord(inv_id, shelfmark, has_alma)
    return None
```

**Forbidden patterns** (carry-forward from analog NORM-04 contract):
- DO NOT mutate `shared/shelfmark_bridge.py`
- DO NOT reimplement `cudl_normalize` — import it
- DO NOT call `int(sys_id)` anywhere (TestNoIntCoercion guard at `tests/test_synthetic_sys_id.py:213`)

---

### `scripts/generate_synthetic_rows.py` (script / batch — REWRITE `_build_qualifying_inventories`)

**Analog:** Self — `scripts/generate_synthetic_rows.py:105-379` (existing 274-line function). Outer script contract preserved per Pitfall §"Pattern 2: Single-function rewrite preserving outer contract".

**Imports pattern** (analog lines 48-69 → preserved + extend):
```python
# Existing imports stay byte-stable. Add the Phase 86 bridge imports:
from shared.synthetic_sys_id import (  # noqa: E402
    encode_inventory_sys_id, is_synthetic_sys_id,
)
from shared.shelfmark_bridge import cudl_normalize, lookup_cudl, build_alias_index  # noqa: E402
from shared.fist_cudl_bridge import (  # noqa: E402  -- NEW Phase 86
    build_fist_alias_index, lookup_fist_by_cudl, fist_to_cudl_keys,
)
```

**Outer contract — MUST stay unchanged** (analog lines 436-444, 447-461, 464-508, 539-571):
- `_read_libraries_csv` CRLF-detection guard (line 436-444; v7.9.4 lesson — see Pitfall 5)
- `_strip_existing_synthetic_block` marker-block stripper (line 447-461)
- `_build_synthetic_rows` D-01a collision check + 8-column shape (line 464-508)
- `_collect_real_alma_ids` (line 511-531)
- `_write_manifest` AUTHORITATIVE contract (line 539-571)
- `_write_residue` 8-column CSV header (line 574-613) — Phase 86 ADDS new column `pattern_guess`

**Core pattern — REWRITE shape** (research §"Pattern 2"):
```python
# REWRITE TARGET — replace existing FIST-walk with CUDL-walk:
def _build_qualifying_inventories(
    fist_conn: sqlite3.Connection,
    nli_conn: Optional[sqlite3.Connection] = None,
) -> tuple[dict[int, dict], list[dict]]:
    """CUDL-WALKED rewrite (Phase 86 D-01).

    Replaces the Phase 85 FIST-walk + multi_signature STRICT predicate.
    Walks nli_crossref.db.cambridge_manifests (~141K rows); for each:
      a. lookup_cudl(classmark) → if hit, SKIP (already in libraries.csv)
      b. lookup_fist_by_cudl(classmark) → if hit:
          - check parent-shadow filter (D-06)
          - check has_alma (D-01a — only emit when no Alma exists)
          - emit synthetic row with cudl_label + manifest_url
      c. else log to residue with pattern_guess column (D-02c)
    """
    cudl_classmarks = list(nli_conn.execute(
        "SELECT label, manifest_url, normalized_shelfmark FROM cambridge_manifests "
        "ORDER BY normalized_shelfmark"
    ))
    build_fist_alias_index(fist_conn)            # Phase 86 NEW
    qualifying: dict[int, dict] = {}
    residue: list[dict] = []
    parent_shelfmarks = _load_parent_shelfmark_set()  # D-06 filter
    for label, manifest_url, classmark in cudl_classmarks:
        if lookup_cudl(classmark) is not None:
            continue                              # Phase 84 covers it
        rec = lookup_fist_by_cudl(classmark)
        if rec is None:
            residue.append({
                "cudl_label": label, "classmark": classmark,
                "ambiguity_kind": "no_fist_match",
                "pattern_guess": _guess_pattern(classmark),  # NEW column
                "inventory_id": "", "signature_id": "",
                "fist_signature_ids": "", "fist_inventory_ids": "",
                "leading_char": "",
            })
            continue
        # D-04 multi_signature relax: lookup_fist_by_cudl returns lowest InventoryId
        # already; multi_inventory excluded by D-04a inside the bridge.
        # D-06 parent-shadow filter:
        if rec.fist_shelfmark in parent_shelfmarks:
            continue
        # D-01a: only emit if no Alma (otherwise libraries.csv row already exists)
        if rec.has_alma:
            continue
        qualifying[rec.inventory_id] = {
            "canonical_shelfmark": rec.fist_shelfmark,
            "title_heb": None, "title_eng": None, "genizah_title": None,
            "library_code": _classify_library_code(rec.fist_shelfmark),
            "has_cudl_manifest": True,            # D-01a invariant
            "has_fjms_metadata": False,           # filled by separate FJMS query
            "cudl_label": label,
        }
    return qualifying, residue
```

**Pattern-guess helper** (NEW — research §"Common Operation 2", D-02c):
```python
def _guess_pattern(cudl_classmark: str) -> str:
    """Categorize residue classmark into one of the 5 known D-02b families."""
    if cudl_classmark.startswith("tsf"):     return "tsf_flattened_series"
    if cudl_classmark.startswith("tsar"):    return "tsar_flattened_series"
    if cudl_classmark.startswith("tsns"):    return "tsns_minute_or_letter"
    if cudl_classmark.startswith("or"):      return "or_single_segment"
    if cudl_classmark.startswith("mosseri"): return "mosseri_exotic_letter"
    if cudl_classmark.startswith("tsmisc"):  return "tsmisc_multi_segment"
    return "other"
```

**Parent-shadow loader** (NEW — D-06):
```python
def _load_parent_shelfmark_set() -> set[str]:
    """Read reports/synthetic_parent_shelfmarks.csv into a set of parent_shelfmark values.

    File schema (existing 175-row Phase 85 audit):
      parent_shelfmark,synthetic_sys_id,inventory_id,real_child_count,sample_real_children
    """
    path = ROOT / "reports" / "synthetic_parent_shelfmarks.csv"
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8", newline="") as f:
        return {row["parent_shelfmark"] for row in csv.DictReader(f) if row.get("parent_shelfmark")}
```

**Residue CSV column extension** (analog lines 583-613 → ADD `pattern_guess`):
```python
# _write_residue: existing 8 columns + 1 new column at end of header list:
w.writerow([
    "inventory_id", "signature_id", "ambiguity_kind", "classmark",
    "cudl_label", "fist_signature_ids", "fist_inventory_ids", "leading_char",
    "pattern_guess",                                 # NEW Phase 86 column
])
```

---

### `scripts/audit_nli_attribution.py` (script / one-shot scan, batch read + assert)

**Analog:** `scripts/fix_nli_oxford_mislabel.py` (65 lines, v7.9.4)

**Imports pattern** (analog lines 8-16 → mirror, READ-ONLY):
```python
from __future__ import annotations
import csv
import re
import sys
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parent.parent / "libraries.csv"
NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)
```

**Core scan pattern** (analog lines 31-37 → READ-ONLY variant):
```python
# Mirror analog scan loop, but assert (no rewrite).
def main() -> int:
    regressions = []
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or row[0].startswith("#"):
                continue                     # skip marker lines per Phase 85 D-04a
            if row[3] == "Oxford" and NLI_RE.search(row[2] or ""):
                regressions.append((row[0], row[2][:80]))
    if regressions:
        print(f"REGRESSION: {len(regressions)} Oxford rows match NLI regex")
        for sys_id, calls in regressions[:5]:
            print(f"  {sys_id}  {calls}")
        return 1
    print(f"OK: no Oxford rows match NLI regex (v7.9.4 fix intact)")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

**Forbidden patterns:**
- DO NOT write to libraries.csv (this is read-only audit; only the v7.9.4 analog does the actual rewrite)
- DO NOT use `--apply`/`--dry-run` argparse — single mode (scan), exit code is the contract
- DO NOT replicate the CRLF line-terminator logic (we never write)

---

### `tests/test_fist_cudl_bridge.py` (test / unit, request-response)

**Analog:** `tests/test_shelfmark_bridge_unit_index.py` (107 lines)

**Imports pattern** (analog lines 1-10 → mirror):
```python
"""Phase 86 Plan 01: deterministic unit tests for fist_cudl_bridge.

Codex MEDIUM #7: avoid integration-only coverage. These tests build small
synthetic FIST.db schemas in memory and exercise the bridge logic directly.
"""
import sqlite3
import pytest
from shared.fist_cudl_bridge import (
    fist_to_cudl_keys, build_fist_alias_index, lookup_fist_by_cudl,
    InventoryRecord,
)
```

**D-02a normalizer test pattern** (analog lines 13-28 → 4 patterns × few cases each):
```python
class TestFistToCudlKeys:
    def test_mosseri_roman_expansion(self):
        # D-02a Pattern 1: Moss. III,27.1 → mosseriii27.1
        keys = fist_to_cudl_keys("Moss. III,27.1")
        assert "mosseriii27.1" in keys

    def test_prefix_strip_after_last_colon(self):
        # D-02a Pattern 2: "Mosseri: Moss. IV,27.1" → also tries after ":"
        keys = fist_to_cudl_keys("Mosseri: Moss. IV,27.1")
        assert "mosseriv27.1" in keys

    def test_series_n_strip(self):
        # D-02a Pattern 3: "T-S F1(1).11" → tsf1.11
        keys = fist_to_cudl_keys("T-S F1(1).11")
        assert "tsf1.11" in keys

    def test_or_dot_fix(self):
        # D-02a Pattern 4: "Or.1080 X.Y" (FIST norm) ↔ "or1080.X.Y" (CUDL)
        keys = fist_to_cudl_keys("Or.1080 1.5")
        assert "or1080.1.5" in keys or "or1080.15" in keys
```

**Multi-inventory exclude test pattern** (analog lines 26-33 → mirror with FIST schema; D-04a):
```python
class TestLookupFistByCudl:
    def _seed(self, rows: list[tuple]) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.executescript("""
            CREATE TABLE dbo_Inventory (InventoryId INTEGER PRIMARY KEY, Shelfmark TEXT);
            CREATE TABLE dbo_InventoryAlma (ID INTEGER PRIMARY KEY, InventoryId INTEGER, AlmaId INTEGER);
        """)
        for inv_id, shelfmark, alma_id in rows:
            conn.execute("INSERT INTO dbo_Inventory VALUES (?, ?)", (inv_id, shelfmark))
            if alma_id is not None:
                conn.execute("INSERT INTO dbo_InventoryAlma VALUES (NULL, ?, ?)", (inv_id, alma_id))
        conn.commit()
        return conn

    def test_multi_inventory_returns_none(self):
        # D-04a: same CUDL key → 2 distinct InventoryIds → None.
        conn = self._seed([(10, "T-S 12.345", None), (11, "T-S 12.345", None)])
        build_fist_alias_index(conn)
        assert lookup_fist_by_cudl("ts12.345") is None
```

**Forbidden patterns** (analog lesson — Phase 84 Round 3 Codex MEDIUM):
- DO NOT touch real `fist_data/FIST.db` from tests — use `:memory:` schema seeds
- DO NOT mutate `_FIST_ALIAS_INDEX` directly — go through `build_fist_alias_index`

---

### `tests/test_synthetic_generation_phase86.py` (test / integration, request-response)

**Analog:** `tests/test_generate_synthetic_rows.py` (existing, 800+ lines)

**Helper-seed pattern** (analog lines 123-189 → reuse `_make_fist_seed` + `_make_nli_seed` shape):
```python
# Mirror analog _make_fist_seed schema EXACTLY (it's verified against worktree
# main checkout per analog docstring). Phase 86 tests can either:
#   (a) import _make_fist_seed/_make_nli_seed from test_generate_synthetic_rows, or
#   (b) re-define identical helpers.
# Recommendation: (a) — single source of truth across both test files.
from tests.test_generate_synthetic_rows import _make_fist_seed, _make_nli_seed
```

**T-S NS 329.96 closure fixture** (analog lines 197-229 pattern → CUDL-walk inversion):
```python
class TestCudlWalkedGeneration:
    def test_tsns_329_96_synthetic_emitted(self):
        """D-04 relax + CUDL-walk: T-S NS 329.96 must close (the originating user case)."""
        from scripts.generate_synthetic_rows import _build_qualifying_inventories
        # Real InventoryId per research §"Empirical confirmation": 65549106
        fist = _make_fist_seed("""
            INSERT INTO dbo_Inventory VALUES (65549106, 'T-S NS 329.96');
            INSERT INTO dbo_InventorySignature VALUES
                (65549106, 100), (65549106, 101), (65549106, 102);
            INSERT INTO dbo_Signature VALUES
                (100, 1000), (101, 1001), (102, 1002);
            -- 13 SignatureIds in real data; 3 here is enough to test multi_signature relax.
            -- No InventoryAlma row → has_alma=False → emits synthetic.
        """)
        nli = _make_nli_seed(["tsns329.96"])
        qualifying, _residue = _build_qualifying_inventories(fist, nli)
        assert 65549106 in qualifying, "T-S NS 329.96 (D-04 multi_signature relax) failed to emit"
```

**Image-bearing-only invariant pattern** (NEW — D-01a):
```python
def test_all_emitted_have_cudl_manifest(self):
    """D-01a: every synthetic row HAS a CUDL manifest by construction."""
    from scripts.generate_synthetic_rows import _build_qualifying_inventories
    fist = _make_fist_seed("""
        INSERT INTO dbo_Inventory VALUES (200, 'T-S NS 999.1');
        INSERT INTO dbo_InventorySignature VALUES (200, 2000);
        INSERT INTO dbo_Signature VALUES (2000, 20000);
        -- No catalog title, no bib/freedesc/fulltext: bib-only stance is REJECTED in Phase 86.
    """)
    nli = _make_nli_seed([])  # no CUDL manifest at all
    qualifying, _residue = _build_qualifying_inventories(fist, nli)
    assert 200 not in qualifying, "Phase 86 D-01a: bib-only inclusion is FORBIDDEN"
```

**Parent-shadow filter test pattern** (analog `tests/test_generate_synthetic_rows.py` extend; D-06):
```python
def test_parent_shadow_filter_applied(self, tmp_path, monkeypatch):
    """D-06: shelfmark in parent-shadow CSV is excluded from synthetic emission."""
    from scripts import generate_synthetic_rows as gen
    parent_csv = tmp_path / "synthetic_parent_shelfmarks.csv"
    parent_csv.write_text(
        "parent_shelfmark,synthetic_sys_id,inventory_id,real_child_count,sample_real_children\n"
        "T-S NS 161,990001000000000000,1000000,1009,T-S NS 161.1\n",
        encoding="utf-8",
    )
    # Monkeypatch ROOT/reports path lookup ...
```

**Forbidden patterns** (analog lesson lines 10-12):
- NEVER mutate real `libraries.csv`, real `reports/`, or real `fist_data/` from tests — use `tmp_path`
- Connections injected (no `monkeypatch.setattr(sqlite3, "connect", ...)` outside the `_run_apply` helper)

---

### `tests/test_nli_oxford_attribution.py` (test / regression, batch read + assert)

**Analogs:**
- `tests/test_synthetic_sys_id.py::TestNoIntCoercion` (lines 149-236) — for the **scan-sweep** style
- `tests/test_shelfmark_bridge.py` (lines 46-72) — for the **golden parametrize** style
- `scripts/fix_nli_oxford_mislabel.py:18` — for the regex source

**Imports pattern** (hybrid):
```python
"""v7.9.4 regression test (AUDIT-03 D-10): 461 NLI-flipped rows must stay library_code='NLI'."""
import csv
import re
from pathlib import Path

import pytest

CSV_PATH = Path(__file__).resolve().parent.parent / "libraries.csv"
NLI_RE = re.compile(r"The National Library of Israel|JER NLI Heb", re.IGNORECASE)
GOLDEN_SAMPLE_PATH = Path(__file__).parent / "fixtures" / "nli_oxford_flipped_sysids.txt"
```

**Module-scoped fixture pattern** (analog `test_shelfmark_bridge.py:20-43` shape):
```python
@pytest.fixture(scope="module")
def libraries_csv_data():
    """Load libraries.csv once per module, key by sys_id."""
    rows_by_sysid = {}
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if len(row) < 4 or row[0].startswith("#"):
                continue
            rows_by_sysid[row[0]] = row
    return rows_by_sysid

@pytest.fixture(scope="module")
def golden_sysids():
    if not GOLDEN_SAMPLE_PATH.exists():
        pytest.skip(f"Fixture file missing: {GOLDEN_SAMPLE_PATH}")
    return GOLDEN_SAMPLE_PATH.read_text().strip().splitlines()
```

**Golden-fixture parametrize pattern** (analog `test_shelfmark_bridge.py:53-71` style — but loop, not parametrize, because fixture file is loaded lazily):
```python
def test_nli_flipped_rows_unchanged(libraries_csv_data, golden_sysids):
    """Each of the 20 golden sys_ids (sample of v7.9.4's 461) must remain library_code='NLI'."""
    for sys_id in golden_sysids:
        row = libraries_csv_data.get(sys_id)
        assert row is not None, f"sys_id {sys_id} missing from libraries.csv"
        assert row[3] == "NLI", (
            f"v7.9.4 regression: sys_id {sys_id} library_code={row[3]!r}, expected 'NLI'"
        )
```

**Scan-sweep pattern** (analog `TestNoIntCoercion::test_no_int_sys_id_coercion` lines 213-236 shape — repo-walk → assert no violations):
```python
def test_no_new_oxford_with_nli_text(libraries_csv_data):
    """Catch-all: no Oxford-coded row should match the v7.9.4 NLI regex."""
    regressions = [
        sys_id
        for sys_id, row in libraries_csv_data.items()
        if row[3] == "Oxford" and NLI_RE.search(row[2] or "")
    ]
    assert not regressions, (
        f"v7.9.4 regression: {len(regressions)} rows: {regressions[:5]}"
    )
```

**Test-scope decision** (Claude's discretion per CONTEXT.md): **20-golden + scan-sweep hybrid** per research §"Open Questions Q2". Golden fixture file at `tests/fixtures/nli_oxford_flipped_sysids.txt` (one sys_id per line, generated from `git show v7.9.4:libraries.csv` diff vs v7.9.3).

---

### `reports/cudl_coverage.md` (report / durable artifact)

**Analog:** `reports/synthetic_coverage.md` (existing; written by `scripts/generate_synthetic_rows.py::_write_coverage` lines 616-678)

**Function signature pattern** (analog lines 616-623 → mirror with new sections):
```python
def _write_cudl_coverage_md(
    path: Path,
    p84_resolved: int,
    p86_resolved: int,
    multi_inv: int,
    truly_orphan: int,
    by_collection: dict[str, dict],
    pattern_adjudication: list[dict],
) -> None:
    """Write reports/cudl_coverage.md — Phase 86 AUDIT-02 deliverable."""
```

**Markdown structure pattern** (analog lines 640-678 → mirror with new sections; research §"Pattern 3"):
```python
# Mirror Phase 85's f-string-driven write pattern:
path.write_text(
    f"# CUDL Coverage Report (Phase 86)\n\n"
    f"**Generated:** {date_iso}\n"
    f"**Source data:** nli_crossref.db.cambridge_manifests (~141K), "
    f"FIST.db.dbo_Inventory (~279K), libraries.csv ({n_real} real + {n_synth} synthetic).\n\n"
    f"## Summary\n\n"
    f"| Status | Count | % of CUDL total |\n"
    f"| ------ | ----- | --------------- |\n"
    f"| Resolved via Phase 84 bridge | {p84_resolved} | ... |\n"
    f"| Resolved via Phase 86 FIST↔CUDL bridge | {p86_resolved} | ... |\n"
    f"| Multi-inventory ambiguous (excluded) | {multi_inv} | ... |\n"
    f"| Truly orphan (residue) | {truly_orphan} | ... |\n\n"
    f"## Per-Collection Breakdown\n\n"
    # ... table rows from by_collection ...
    f"## Residue Pattern Adjudication (D-02c outcomes)\n\n"
    # ... pattern decisions accepted/rejected/deferred from pattern_adjudication ...
    f"## Re-run Instructions\n\n"
    f"```bash\n"
    f"python scripts/generate_synthetic_rows.py --apply\n"
    f"python scripts/export_fist_enrichment.py\n"
    f"python scripts/scan_cudl_orphans.py --out-suffix _post_phase86\n"
    f"```\n\n"
    f"## See Also\n\n"
    f"- `reports/synthetic_coverage.md` — Phase 85 tier breakdown (cross-link, do not rewrite)\n"
    f"- `.planning/phases/86-.../86-RESIDUE-PATTERNS.md` — D-02c artifact\n",
    encoding="utf-8",
)
```

**Required sections** (per research §"Layer-by-layer validation" → `test_cudl_coverage_artifact.py::test_required_sections_present`):
1. `## Summary`
2. `## Per-Collection Breakdown`
3. `## Residue Pattern Adjudication (D-02c outcomes)`
4. `## Re-run Instructions`
5. `## See Also`

---

### `reports/scan_cudl_orphans_post_phase86.txt` + `reports/cudl_orphans_*_post_phase86.csv`

**Analog:** `reports/cudl_orphans_all_post_phase84.csv` + `reports/cudl_orphans_with_neighbor_post_phase84.csv` (already produced by existing script)

**Re-run pattern** (no code change — just CLI invocation per `scripts/scan_cudl_orphans.py:42-46`):
```bash
# Phase 86 re-run produces _post_phase86 suffixed artifacts:
python scripts/scan_cudl_orphans.py --out-suffix _post_phase86 \
    > reports/scan_cudl_orphans_post_phase86.txt
```

**Argparse contract preserved** (analog `scan_cudl_orphans.py:42-46`):
```python
parser.add_argument('--out-suffix', default='', help="Suffix appended to output CSV filenames")
suffix = args.out_suffix
all_path = REPORTS_DIR / f"cudl_orphans_all{suffix}.csv"
nb_path = REPORTS_DIR / f"cudl_orphans_with_neighbor{suffix}.csv"
```

The `.txt` artifact is the captured stdout of the script run (which already prints CUL-row count, distinct-norm count, manifest count, both written-CSV counts).

---

### `.planning/phases/86-.../86-RESIDUE-PATTERNS.md` (research artifact / static document)

**Analog:** None directly — novel D-02c human-in-loop iteration venue. **Closest reference:** `.planning/phases/85-.../85-04-AUDIT.md` for the "audit artifact lives in phase dir" pattern.

**Structure** (CONTEXT.md D-02b/D-02c specifies):
```markdown
# Phase 86 Residue Pattern Adjudication

**Generated:** [date]
**Source residue:** reports/synthetic_ambiguity_residue.csv (Phase 86 rebuild)
**Adjudication target:** 5 pattern families × accept/reject/spot-check

## Pattern Family 1: T-S F "flattened-series" hypothesis (~392 entries)

**Hypothesis:** CUDL `tsf1.1100` corresponds to FIST `T-S F1(1).100` or `T-S F1(2).100`
(the leading `1` in `1100` may encode the FIST `(N)` series digit).

**Sample fixtures** (CUDL classmark + 3 nearest FIST candidates each):
| CUDL classmark | FIST candidate 1 | FIST candidate 2 | FIST candidate 3 |
| -------------- | ---------------- | ---------------- | ---------------- |
| tsf1.1100      | T-S F1(1).100    | T-S F1(2).100    | T-S F1(3).100    |
| ...

**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more

## Pattern Family 2: T-S NS "minute fragments" + letter suffixes (~179 entries)
[... same structure ...]

## Pattern Family 3: Or. single-segment ambiguity (~571 entries)
## Pattern Family 4: Mosseri exotic letter suffixes (~48 entries)
## Pattern Family 5: T-S Misc multi-segment patterns (~98 entries)
```

**Format choice** (Claude's discretion): **markdown tables** (per Claude's discretion in CONTEXT.md). Easier for user to adjudicate inline than CSV. Sample fixtures generated by a small wave-0 prototype script per research §"Open Questions Q1".

---

### `libraries.csv` synthetic block (data, batch rewrite)

**Analog:** Self — Phase 85 marker-block contract preserved verbatim.

**Marker-block pattern** (`scripts/generate_synthetic_rows.py:79-80, 750-758`):
```python
MARKER_BEGIN = "# BEGIN SYNTHETIC"
MARKER_END = "# END SYNTHETIC"

# In main():
final_rows.append([MARKER_BEGIN, "", "", "", "", "", "", ""])
final_rows.extend(synthetic_rows)
final_rows.append([MARKER_END, "", "", "", "", "", "", ""])
with csv_path.open("w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, lineterminator=line_terminator)
    writer.writerows(final_rows)
```

**8-column shape** (`scripts/generate_synthetic_rows.py:505-507`):
```python
# system_number, oxford_part_id, call_numbers, library_code, (3 reserved/empty), titles_non_placeholder
out.append([sys_id, "", call_numbers, library_code, "", "", "", title])
```

**CRLF preservation** (`scripts/generate_synthetic_rows.py:436-444` → MUST stay; v7.9.4 Pitfall 5):
```python
def _read_libraries_csv(path: Path) -> tuple[list[list[str]], str]:
    with path.open("rb") as f:
        sample = f.read(8192)
    line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"
    ...
```

---

### `fist_data/synthetic_manifest.json` (data, batch write)

**Analog:** Self — Phase 85 manifest contract preserved (`scripts/generate_synthetic_rows.py:539-571`).

**JSON shape** (analog lines 549-565 → keep all 5 keys per record):
```python
{
    "inventory_id": int,
    "synthetic_sys_id": str,         # encode_inventory_sys_id result
    "source": str,                    # "both" | "cudl_match" | "fjms_metadata"
    "canonical_shelfmark": str,
    "library_code": str,              # "CUL" | "Mosseri"
}
```

**Byte-stability contract** (analog line 569):
```python
path.write_text(
    json.dumps(items, ensure_ascii=False, indent=2, sort_keys=True),
    encoding="utf-8",
)
```

---

### `fist_data/fjms_enrichment.db` (data, regenerated)

**Analog:** Existing pipeline — `scripts/export_fist_enrichment.py` UNCHANGED.

**Regeneration pattern** (per research §"FJMS Enrichment" D-07):
```bash
# After scripts/generate_synthetic_rows.py --apply has written:
#   - libraries.csv (synthetic block)
#   - fist_data/synthetic_manifest.json (AUTHORITATIVE input for export)
# Run the unchanged export script:
python scripts/export_fist_enrichment.py
```

The script's existing UNION-ALL pattern reads `synthetic_manifest.json` and injects synthetic AlmaIds into 12 enrichment tables (catalog, bibliography, measurements, etc.) without code change.

---

## Shared Patterns

### Authentication / Access Control
**Source:** N/A (Phase 86 has no auth surface — all access is local SQLite read-only or filesystem write at generation time).

**Apply to:** N/A.

---

### SQLite Read-Only URI
**Source:** `scripts/generate_synthetic_rows.py:718-723`
**Apply to:** `shared/fist_cudl_bridge.py::build_fist_alias_index`, all generation script DB opens.
```python
fist_conn = sqlite3.connect(f"file:{fist_db}?mode=ro", uri=True)
nli_conn = sqlite3.connect(f"file:{nli_db}?mode=ro", uri=True) if nli_db.exists() else None
```

---

### Deterministic Ordering
**Source:** `scripts/generate_synthetic_rows.py:213, 276-279, 477, 549, 567, 597-601`
**Apply to:** Every SELECT, every dict-iteration, every JSON write in Phase 86.
```python
# Every SELECT has explicit ORDER BY:
"... ORDER BY inv.InventoryId, sig.SignatureId, cat.UnitCatalogRecId"
# Every dict-iteration uses sorted():
for key in sorted(by_key.keys()):
    ...
# Every JSON write uses sort_keys=True:
json.dumps(items, ensure_ascii=False, indent=2, sort_keys=True)
```

---

### CSV-Injection Fail-Loud
**Source:** `scripts/generate_synthetic_rows.py:82-96, 330-352, 496-503`
**Apply to:** Synthetic-row emission path (preserved verbatim — no Phase 86 changes).
```python
_CSV_INJECTION_LEADERS = ("=", "+", "-", "@")

def _has_csv_injection_leader(value: object) -> bool:
    if not value:
        return False
    s = str(value)
    return bool(s) and s[0] in _CSV_INJECTION_LEADERS
# Rows with a leading injection char are LOGGED to residue, NOT sanitized.
```

---

### CRLF Line-Terminator Detection
**Source:** `scripts/fix_nli_oxford_mislabel.py:53-55` (v7.9.4 origin) + `scripts/generate_synthetic_rows.py:436-444` (Phase 85 carry-over)
**Apply to:** Any script that REWRITES `libraries.csv`. Phase 86's `audit_nli_attribution.py` is read-only so does NOT need this.
```python
with path.open("rb") as f:
    sample = f.read(8192)
line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"
```

**Warning (Pitfall 5):** Failing to preserve CRLF produces a 255K-line full-file diff against v7.9.4-origin libraries.csv.

---

### Marker-Block Idempotent Rewrite
**Source:** `scripts/generate_synthetic_rows.py:79-80, 447-461, 750-758`
**Apply to:** Any script that mutates `libraries.csv` synthetic block.
```python
MARKER_BEGIN = "# BEGIN SYNTHETIC"
MARKER_END = "# END SYNTHETIC"

def _strip_existing_synthetic_block(rows: list[list[str]]) -> list[list[str]]:
    out = []
    in_block = False
    for row in rows:
        first = row[0] if row else ""
        if first == MARKER_BEGIN:
            in_block = True
            continue
        if first == MARKER_END:
            in_block = False
            continue
        if not in_block:
            out.append(row)
    return out
```

---

### sys_id Helper Discipline (D-01b)
**Source:** `shared/synthetic_sys_id.py:79-112`
**Apply to:** All Phase 86 code that touches synthetic sys_ids.
```python
# REQUIRED:
from shared.synthetic_sys_id import (
    encode_inventory_sys_id, decode_inventory_id, is_synthetic_sys_id,
)
sys_id = encode_inventory_sys_id(inv_id)        # Never: '99' + str(inv).zfill(10) + '000000'
inv = decode_inventory_id(sys_id)               # Never: int(sys_id[2:12])
```

**Lint enforcement:** `tests/test_synthetic_sys_id.py::TestNoIntCoercion::test_no_int_sys_id_coercion` (lines 213-236) walks every first-party `.py` file and fails CI on `int(sys_id)` matches outside the helper module's allowlist.

---

### Test Path Hygiene
**Source:** `tests/test_generate_synthetic_rows.py:10-13` (Phase 84 Round 3 Codex MEDIUM lesson)
**Apply to:** Every Phase 86 test that touches files.
> "NEVER mutate real libraries.csv, real reports/, or real fist_data/ from tests. Use tmp_path fixture for all writes."

```python
# Pattern: monkeypatch module-level path constants to tmp_path/...
monkeypatch.setattr(gen, "CSV_PATH", tmp_path / "libraries.csv")
monkeypatch.setattr(gen, "MANIFEST_PATH", tmp_path / "synthetic_manifest.json")
# ... etc.
```

---

### Connection Injection (Testability)
**Source:** `scripts/generate_synthetic_rows.py:105-108` + `tests/test_generate_synthetic_rows.py:407-417`
**Apply to:** Any new generation-time function (Phase 86 `build_fist_alias_index` already takes `fist_conn`).
```python
def _build_qualifying_inventories(
    fist_conn: sqlite3.Connection,
    nli_conn: Optional[sqlite3.Connection] = None,
) -> tuple[dict[int, dict], list[dict]]:
    """Connections injectable for testability (Gemini LOW accepted)."""
```

Tests use `_make_fist_seed`/`_make_nli_seed` `:memory:` helpers + monkeypatch `sqlite3.connect` only at the script-CLI boundary.

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `.planning/phases/86-.../86-RESIDUE-PATTERNS.md` | research artifact | static doc | Novel D-02c human-in-loop iteration venue. No prior phase has this artifact shape (closest reference is `85-04-AUDIT.md`'s "audit-artifact-lives-in-phase-dir" idiom, but 85-04-AUDIT enumerates code sites; 86-RESIDUE-PATTERNS proposes pattern rules for user adjudication). Format choice (markdown tables) per Claude's discretion in CONTEXT.md. |
| `reports/scan_cudl_orphans_post_phase86.txt` | report (text dump) | batch write | No prior phase has a `.txt` capture of script stdout. Created by `python scripts/scan_cudl_orphans.py --out-suffix _post_phase86 > reports/scan_cudl_orphans_post_phase86.txt` — pure stdout redirect. The CSVs (`cudl_orphans_all_post_phase86.csv`, `cudl_orphans_with_neighbor_post_phase86.csv`) ARE produced by the script directly per analog `_post_phase84` files. |

---

## Cross-Phase Audit Trail

**Phase 84 (frozen):** `shared/shelfmark_bridge.py` — Phase 86 imports `cudl_normalize` and `lookup_cudl` but does not modify. NORM-04 contract preserved.

**Phase 85 (frozen at infrastructure level):** `shared/synthetic_sys_id.py`, `scripts/export_fist_enrichment.py`, browse hide-NLI gates, `/api/fl_ids` empty + `/api/nli_image_by_sysid` 204, `is_synthetic` field in `shared/search_serializer.py`, corrections-write rejection, PostHog `is_synthetic` property — all stay byte-stable. Phase 86 ONLY rewrites `_build_qualifying_inventories` in `scripts/generate_synthetic_rows.py` and ADDS the new bridge module + audit script + tests.

**Forbidden under D-11:**
- DO NOT touch `shared/synthetic_sys_id.py`
- DO NOT touch the 12 source files with browse hide-NLI gates (enumerated in `85-04-AUDIT.md`)
- DO NOT change `web/api.py` synthetic-route shapes
- DO NOT change `shared/search_serializer.py` `is_synthetic` field
- DO NOT change `scripts/export_fist_enrichment.py` UNION-ALL pattern

---

## Metadata

**Analog search scope:** `shared/`, `scripts/`, `tests/`, `reports/`, `tests/fixtures/`, `genizah_core.py`, `.planning/phases/84-*/`, `.planning/phases/85-*/`
**Files scanned:** ~30 (4 fully read: shelfmark_bridge.py, synthetic_sys_id.py, generate_synthetic_rows.py header+core+tail, fix_nli_oxford_mislabel.py, scan_cudl_orphans.py, conftest.py, synthetic_fixtures.py; 6 sampled: test_synthetic_sys_id.py, test_generate_synthetic_rows.py first 500 lines, test_shelfmark_bridge*.py, synthetic_coverage.md, synthetic_parent_shelfmarks.csv, cudl_orphans_all_post_phase84.csv; 1 grep: genizah_core.py for construct_mosseri_cudl_label)
**Pattern extraction date:** 2026-05-10

---

*Phase: 86-cudl-coverage-audit-and-synthetic-reattempt*
*Patterns mapped: 2026-05-10*
