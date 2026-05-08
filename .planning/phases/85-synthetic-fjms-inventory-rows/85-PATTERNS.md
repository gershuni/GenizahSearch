# Phase 85: Synthetic FJMS Inventory Rows - Pattern Map

**Mapped:** 2026-05-08
**Files analyzed:** 22 (10 new + 12 modified)
**Analogs found:** 22 / 22 (100%)

This phase is overwhelmingly *additive* — Phase 84's bridge module is the architectural template for the new helper, Phase 53's metadata-only search path absorbs synthetic rows with zero new branches, and the FJMS sidecar exporter is the single mutation point for D-01 ("write synthetic IDs into AlmaId column"). Most "modified" files get a one-line `if not is_synthetic_sys_id(...): ` guard; no architectural change.

## File Classification

### New Files

| New File | Role | Data Flow | Closest Analog | Match Quality |
|----------|------|-----------|----------------|---------------|
| `shared/synthetic_sys_id.py` | helper module | read-only / pure-functions | `shared/shelfmark_bridge.py` | exact (same shape: pure functions, no I/O, public contract for cross-system lookup) |
| `scripts/generate_synthetic_rows.py` | build script | reads-FIST.db + nli_crossref.db, writes-libraries.csv + writes-manifest.json | `scripts/fix_nli_oxford_mislabel.py` (idempotent CSV rewrite + line-ending preservation) PLUS `scripts/export_fist_enrichment.py` (FIST.db harvest) | role-match (composite of two analogs) |
| `tests/test_synthetic_sys_id.py` | unit test | read-only / pure | `tests/test_shelfmark_bridge_unit_index.py` | exact (Phase 84 unit-test template) |
| `tests/test_generate_synthetic_rows.py` | integration test | reads-CSV + tmp_path | `tests/test_shelfmark_bridge.py` (golden fixture + parametrize) | role-match |
| `tests/test_export_fist_synthetic.py` | integration test | reads-DB | `tests/test_shelfmark_bridge_unit_index.py::TestAliasIndexInMemory` (tmp_path injection) | role-match |
| `tests/test_browse_synthetic.py` | integration test | UI render smoke | `tests/test_shelfmark_bridge.py` (parametrize over fixture rows) | partial-match (new browse-render territory) |
| `tests/test_synthetic_round_trip.py` | integration test | Supabase round-trip | (no direct analog — closest: `tests/test_shelfmark_bridge.py` for fixture pattern) | partial-match |
| `tests/test_search_serializer.py` | unit test | serialization | (existing `tests/test_search_api*.py` — closest by role) | role-match |
| `tests/fixtures/synthetic_fixtures.py` | test fixture | golden-input | `tests/fixtures/cudl_must_resolve.csv` | exact (same fixture-CSV pattern; Phase 84 v7.10 has 44 rows) |
| `reports/synthetic_ambiguity_residue.csv` | audit artifact | written-by-script | `reports/cudl_alias_collisions.csv` (Phase 84 ambiguity exclusion) | exact |
| `fist_data/synthetic_manifest.json` | audit artifact | written-by-script | (no analog — first JSON manifest in fist_data; recommendation A2) | new pattern |

### Modified Files

| Modified File | Role | Data Flow | Closest Analog (Pattern Source) | Match Quality |
|---------------|------|-----------|--------------------------------|---------------|
| `scripts/export_fist_enrichment.py` | build script | DB query + UNION ALL | self (existing 11-table pattern) | exact (extend in place) |
| `libraries.csv` | data file | written-by-script | (data — no code analog) | n/a |
| `genizah_core.py` `_load_csv_bank` (~3357-3411) | loader | CSV read | self (existing loader; one-line addition) | exact |
| `web/pages/browse.py` (~12-14 sites) | UI renderer | runtime-render | self (`is_oxford` / library_code branches at lines 3457-3556 — same kind of conditional source-switching) | exact |
| `web/pages/browse_enrichment.py` (~line 503) | enrichment | data fan-out | self (existing `marc_bib` conditional at line 503) | exact (one-line gate) |
| `web/api.py` (lines 467, 587) | HTTP endpoint | request/response | self (existing 404 path at line 599) | exact (early-return guard) |
| `desktop/viewers.py` (~702-861) | desktop UI | runtime-render | self (existing `_detect_external_provider` at ~816) | exact |
| `genizah_app.py` (~12792, 21717) | desktop UI | runtime-render | `web/pages/browse.py` KTIV sites (line 1708) | partial (web has same pattern) |
| `shared/search_serializer.py` `_serialize_item` (~292-310) | serializer | serialize-out | self (return-dict pattern at line 292-310) | exact (additive field) |
| `web/api_hardening.py::wrap_endpoint` (~340-422) | telemetry | event-emit | self (existing `captured_state` plumbing at lines 368-415) | exact (one-key addition) |
| `shared/corrections_service.py` | service | DB read | self (existing read-side: returns `[]` for empty) | exact (no code change needed — confirmed safe) |
| `web/pages/browse.py` corrections-button + `genizah_app.py` desktop button | UI gate | runtime-render | `web/pages/browse.py:1708` KTIV hide pattern | exact (one-line conditional) |

---

## Pattern Assignments

### `shared/synthetic_sys_id.py` (helper module, read-only / pure-functions)

**Analog:** `shared/shelfmark_bridge.py` (Phase 84)

**Module docstring discipline** (lines 1-39):
```python
# Source: shared/shelfmark_bridge.py:1-39
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
  ...
"""
from __future__ import annotations
```

**Public-function shape** (lines 66-97):
```python
# Source: shared/shelfmark_bridge.py:66-97
def cudl_normalize(s: str) -> str:
    """Normalize a shelfmark for CUDL-vs-libraries.csv matching.
    ...
    Per Phase 84 D-07 these rules apply uniformly across all CUL/Cambridge
    collections. Do NOT modify this function without running the Phase 86
    regression suite.
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

**Adapt by:**
- Keep the `from __future__ import annotations` + module-level docstring with explicit decision references (D-01, D-01a, D-01b, D-13).
- Three pure functions: `is_synthetic_sys_id(s) -> bool`, `encode_inventory_sys_id(int) -> str`, `decode_inventory_id(str) -> Optional[int]`.
- Module-level constants for `_SYNTHETIC_PREFIX = "99"`, `_SYNTHETIC_SUFFIX = "000000"`, `_INVENTORY_PAD = 10`, `_TOTAL_LENGTH = 18` (mirrors `_BUILTIN_COLLISION_KEYS`/`_NUMERIC_RUN_RE` pattern at bridge:154-160).
- No I/O, no logger calls in the helper functions themselves (bridge logger calls live in `build_alias_index`/`load_collision_keys`, which are I/O-touching APIs; pure helpers stay quiet).
- Docstring contract per D-13: explicit "this helper accepts the canonical all-digit form; non-digit input returns False".
- `decode_inventory_id` returns `None` on non-synthetic (no exception) — mirrors `lookup_cudl` returning `None` on miss (bridge:329-364).

---

### `scripts/generate_synthetic_rows.py` (build script, reads-FIST.db + nli_crossref.db, writes-libraries.csv + writes-manifest.json)

**Analog A:** `scripts/fix_nli_oxford_mislabel.py` (idempotent CSV rewrite with line-ending preservation)

**CLI argparse + dry-run pattern** (lines 21-26):
```python
# Source: scripts/fix_nli_oxford_mislabel.py:21-26
def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    args = ap.parse_args()
```

**Atomic rewrite + line-ending preservation** (lines 47-60):
```python
# Source: scripts/fix_nli_oxford_mislabel.py:47-60
    backup = CSV_PATH.with_suffix(".csv.bak")
    shutil.copy2(CSV_PATH, backup)
    print(f"Backup: {backup}")

    # Detect dominant line ending in original file to preserve it.
    with CSV_PATH.open("rb") as f:
        sample = f.read(8192)
    line_terminator = "\r\n" if sample.count(b"\r\n") > sample.count(b"\n") // 2 else "\n"

    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator=line_terminator)
        writer.writerows(out_rows)
    print(f"Wrote {CSV_PATH} ({len(out_rows)} rows, {len(flipped)} flipped)")
    return 0
```

**Analog B:** `scripts/export_fist_enrichment.py` (FIST.db harvest pattern)

**FIST.db query template** (lines 68-83):
```python
# Source: scripts/export_fist_enrichment.py:68-83
    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            d.EngDesc as Domain,
            d.HebDesc as DomainHeb,
            pd.EngDesc as ParentDomain,
            pd.HebDesc as ParentDomainHeb
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        ...
    """)
```

**Adapt by:**
- Keep `argparse` with mutually exclusive `--dry-run` / `--apply` (analog A line 23-25).
- Backup-before-write pattern: `shutil.copy2(CSV_PATH, CSV_PATH.with_suffix(".csv.bak"))` (analog A line 48-49).
- **Critical: line-ending detection pattern** at analog A:53-55 (avoids the LF/CRLF bug user fixed in v7.9.4). Re-use verbatim.
- For idempotency (D-04a): NEW logic — read existing libraries.csv, detect `# BEGIN SYNTHETIC` / `# END SYNTHETIC` marker lines, drop everything between them, append regenerated block at end. Mirror analog A's "filter rows + write all" pattern but with marker-based extraction instead of column-based filter.
- For FIST.db harvest: parameterized SQL via `sqlite3.Cursor.execute(query, params)` (security domain note in §research). Pull canonical shelfmark + InventoryId from `dbo_Signature` join chain (analog B:75-83 template).
- For ambiguity-exclusion (D-05a): mirror Phase 84 `build_alias_index` pattern at `shared/shelfmark_bridge.py:249-326` — collect (key → set of (sys_id, shelfmark)) claims, then keep only keys with exactly ONE distinct identity, write excluded keys to `reports/synthetic_ambiguity_residue.csv` (column shape: `cudl_label, fist_signature_ids, fist_inventory_ids`).
- For collision check (D-01a): assert no synthetic sys_id collides with any real Alma row in libraries.csv. Use `is_synthetic_sys_id()` from the helper, not hand-rolled detection.
- Write `fist_data/synthetic_manifest.json` alongside CSV block as audit artifact (recommendation A2).

---

### `genizah_core.py` `_load_csv_bank` (~lines 3357-3411) — MODIFIED

**Analog:** self — existing loader at the same line range.

**Pattern to extend** (lines 3368-3375):
```python
# Source: genizah_core.py:3368-3375
                for row in reader:

                    if not row or len(row) < 3:
                        continue
                    # Format: system_number, oxford_part_id, call_numbers, library_code, ..., titles_non_placeholder
                    raw_sys_id = row[0]
                    sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())
```

**Adapt by:** Add ONE line after `raw_sys_id = row[0]`:
```python
if raw_sys_id.startswith('#'):
    continue   # Phase 85: tolerate `# BEGIN SYNTHETIC` / `# END SYNTHETIC` marker block
```
Per A8 in research assumptions log — currently the loader's digit-normalization makes marker rows harmless garbage (they produce sys_id `''` which gets overwritten); explicit skip is hygiene. Keep change minimal — loader is hot-path code touched by every startup.

---

### `scripts/export_fist_enrichment.py` — MODIFIED (UNION ALL synthetic AlmaId rows in 11 tables)

**Analog:** self — existing 11-table query pattern.

**Existing query pattern** (lines 68-83 — domains table):
```python
# Source: scripts/export_fist_enrichment.py:68-83
    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            d.EngDesc as Domain,
            ...
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        ...
    """)
```

**Adapt by (per RESEARCH.md Pattern 2):** UNION ALL with synthetic AlmaId for inventories qualifying per D-02:
```sql
-- Existing real-Alma rows (kept verbatim)
SELECT DISTINCT
    TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
    ...
FROM dbo_InventoryAlma alma
JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
...
UNION ALL
-- Synthetic rows: inventories qualifying per D-02 with no Alma link
SELECT DISTINCT
    ('99' || printf('%010d', inv.InventoryId) || '000000') as AlmaId,
    ...
FROM dbo_Inventory inv
JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
LEFT JOIN dbo_InventoryAlma alma ON alma.InventoryId = inv.InventoryId
WHERE alma.AlmaId IS NULL
  AND inv.InventoryId IN (SELECT InventoryId FROM <qualifying_set>)
```
- Repeat for all 11 enrichment-table queries (domains, joins, catalog, catalog_running_titles, catalog_sizes, catalog_fields, catalog_free_desc, catalog_full_texts, catalog_textual_frames, catalog_mentions, bibliography). Header comment at file top (analog file lines 1-30) lists them.
- `<qualifying_set>` is a CTE built once at the top of the export — UNION of (a) inventories matched to cambridge_manifests, (b) inventories with non-empty FJMS metadata.
- Use `printf('%010d', inv.InventoryId)` (sqlite-native) NOT Python f-string — keeps the synthetic-AlmaId computation inside the SQL engine.

---

### `web/pages/browse.py` (~12-14 hide-NLI sites) — MODIFIED

**Analog:** self — existing source-switching block at lines 3457-3556 (Cambridge/Manchester/JTS/Oxford auto-default pattern).

**Pattern at line 1708 (KTIV link)** — unmodified:
```python
# Source: web/pages/browse.py:1708-1717
                        # Ktiv link
                        ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{page.sys_id}"
                        with ui.link(target=ktiv_url, new_tab=True).classes(...).style(...):
                            ui.icon('open_in_new', size='sm').style('color: #ffffff !important;')
                            ui.label(tr('Ktiv')).classes('text-sm font-semibold').style('color: #ffffff !important;')
```

**Auto-default source-switching pattern** (lines 3445-3462):
```python
# Source: web/pages/browse.py:3445-3462
                # External source override: if user switched to Cambridge/Manchester/JTS/Oxford and images are available
                _has_ext_images = bool(page.cambridge_images)
                _has_cambridge_images = _has_ext_images and page.external_provider not in ('manchester', 'jts')
                _has_manchester_images = _has_ext_images and page.external_provider == 'manchester'
                _has_jts_images = _has_ext_images and page.external_provider == 'jts'
                _has_oxford_images = bool(is_oxford and page.sys_id)

                # Auto-default to external sources when available (before image URL construction)
                if _has_jts_images and state.active_source == 'nli' and not state.source_user_override:
                    state.active_source = 'jts'
                if _has_manchester_images and state.active_source == 'nli' and not state.source_user_override:
                    state.active_source = 'manchester'
                if _has_oxford_images and state.active_source == 'nli' and not state.source_user_override:
                    state.active_source = 'oxford'
```

**Adapt by:**
- KTIV / NLI image / fl_ids / NLI credit sites (lines 1708, 1973, 2430, 2898, 3442, 3568-3576, 3994-4032): wrap in `if not is_synthetic_sys_id(page.sys_id):` — element doesn't render in synthetic case.
- Auto-default block (lines 3457-3462): add a synthetic-aware branch BEFORE existing `_has_jts_images`/etc. checks:
  ```python
  _is_synth = is_synthetic_sys_id(page.sys_id)
  if _is_synth and _has_cambridge_images:
      _cam_safe_default = True   # synthetic+CUDL: Cambridge is THE source
      if state.active_source == 'nli' and not state.source_user_override:
          state.active_source = 'cambridge'
  ```
- One import at top of file: `from shared.synthetic_sys_id import is_synthetic_sys_id`.

---

### `web/api.py` (lines 467, 587) — MODIFIED

**Analog:** self — existing `Response(content="Image not found", status_code=404)` at line 599.

**Existing endpoint shape** (lines 587-599):
```python
# Source: web/api.py:587-599
    @target_app.get('/api/nli_image_by_sysid/{sys_id}')
    def nli_image_by_sysid(sys_id: str, page: int = 0, width: int = 2000, suffix: int = 1):
        """
        Fetch NLI image by System ID. Dynamically gets FL IDs from NLI IIIF manifest.
        ...
        """
        got = _fetch_nli_image_bytes(sys_id, page, width=width, suffix=suffix)
        if got is None:
            return Response(content="Image not found", status_code=404)
```

**Adapt by:** add early-return guard at the very top of each handler (lines 468 and 588):
```python
from shared.synthetic_sys_id import is_synthetic_sys_id
if is_synthetic_sys_id(sys_id):
    return Response(content="", status_code=204)   # No Content — synthetic rows have no NLI source
```
Per A5: 204 not 404 (gentler on `<img>` error handlers). Same pattern applies to `/api/fl_ids/{sys_id}` at line 467.

---

### `shared/search_serializer.py` `_serialize_item` (~292-310) — MODIFIED

**Analog:** self — existing return-dict.

**Existing return structure** (lines 292-310):
```python
# Source: shared/search_serializer.py:292-310
    return {
        'uid': result.get('uid', '') or '',
        'locator': {
            'sys_id': final_sys_id or None,
            'volume_ie': parsed.get('ie_id'),
            'p_num': parsed.get('p_num'),
        },
        'score': score,
        'shelfmark': display.get('shelfmark', '') or '',
        'title': display.get('title', '') or '',
        'library': {'code': library_code, 'name': library_name},
        'domains': domains,
        'dating': dating,
        'snippet': snippet_clean,
        'excerpt': excerpt,
        'match_terms': match_terms,
        # HIGH-07: pass library_code so non-NLI providers get null
        'image_url': _build_image_url(final_sys_id, parsed.get('p_num'), library_code),
    }
```

**Adapt by:** add ONE additive field at top level (per A3):
```python
from shared.synthetic_sys_id import is_synthetic_sys_id
return {
    'uid': result.get('uid', '') or '',
    'locator': {...},
    'is_synthetic': is_synthetic_sys_id(final_sys_id),   # Phase 85 D-14
    'score': score,
    ...
}
```
Schema additive — keep `SCHEMA_VERSION = 1` (per Phase 83 stability commitment, additive changes don't bump version). Apply the same field in `serialize_browse_payload` envelope. Document in `docs/SEARCH_API.md` + CHANGELOG entry.

---

### `web/api_hardening.py::wrap_endpoint` (~340-422) — MODIFIED

**Analog:** self — existing `captured_state` plumbing.

**Existing captured_state init pattern** (lines 368-377):
```python
# Source: web/api_hardening.py:368-377
            captured_state: dict = {
                'mode': None,
                'result_count': None,
                # 81A D-08 — uniform PostHog event shape across endpoints.
                # browse + parallels handlers may overwrite if they ever start
                # accepting a search_mode field; today both leave them at
                # None/0 (set explicitly in the handler bodies for clarity).
                'search_mode_value': None,
                'responsa_options_count': 0,
            }
```

**Existing capture_api_event call** (lines 403-415):
```python
# Source: web/api_hardening.py:403-415
                    capture_api_event(
                        endpoint=endpoint_name,
                        mode=captured_state.get('mode'),
                        latency_seconds=elapsed,
                        result_count=captured_state.get('result_count'),
                        status_code=status_code,
                        error_code=error_code,
                        client_ip=client_ip,
                        # 81A D-08 — plumb the two new properties from the
                        # captured_state contract so wrap_endpoint-decorated
                        # endpoints (browse, parallels) emit a uniform shape.
                        search_mode_value=captured_state.get('search_mode_value'),
                        responsa_options_count=captured_state.get('responsa_options_count', 0),
                    )
```

**Adapt by:**
- Add `'is_synthetic': None` to the `captured_state` dict init at line 368-377 (default None — handlers populate when they have a sys_id).
- Add `is_synthetic=captured_state.get('is_synthetic')` keyword arg to the `capture_api_event` call at lines 403-415.
- Update `capture_api_event` signature to accept the new property (additive, default None) and pass through to `posthog.capture(properties=...)`.
- Endpoint handlers (`web/search_api.py` `_search_body` / `_browse_body` / `_parallels_body`) populate `captured_state['is_synthetic'] = is_synthetic_sys_id(sys_id)` after locator validation.

---

### `desktop/viewers.py` (~702-861) + `genizah_app.py` (~12792, 21717) — MODIFIED

**Analog:** `web/pages/browse.py:1708` KTIV pattern (cross-app parity).

**Web KTIV link pattern (template)** is at `web/pages/browse.py:1708` (shown above).

**Adapt by:**
- `desktop/viewers.py:702-710` (`btn_ktiv = QPushButton(...)`): in `_detect_external_provider` or the call site, set `btn_ktiv.setVisible(False)` when `is_synthetic_sys_id(self._ktiv_sys_id)`. The button is already initialized invisible — just ensure the visibility-set codepath gates on synthetic.
- `desktop/viewers.py:856-861` (`btn_ktiv.setVisible(False)` reset + `_detect_external_provider`): when synthetic, never flip to visible; pre-set `external_provider = 'cambridge'` if manifest exists.
- `genizah_app.py:12792` (KTIV link string): wrap construction + display in `if not is_synthetic_sys_id(sys_id):`.
- `genizah_app.py:21717` (`QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/...{self.current_browse_sid}..."))`): wrap the open call in the same guard.
- Cross-app parity is critical (CLAUDE.md "Both apps must be maintained"). Each web change has a desktop twin.

---

### `tests/test_synthetic_sys_id.py` (helper unit tests)

**Analog:** `tests/test_shelfmark_bridge_unit_index.py`

**Class structure + parametrize pattern** (lines 13-29):
```python
# Source: tests/test_shelfmark_bridge_unit_index.py:13-29
class TestCudlNormalize:
    def test_dot_after_letter_dropped(self):
        assert cudl_normalize("T-S Ar. 48.211") == "tsar48.211"

    def test_slash_to_dot(self):
        assert cudl_normalize("T-S F 8/002") == "tsf8.2"

    def test_comma_to_dot(self):
        assert cudl_normalize("Add. 863, 2") == "add863.2"

    def test_leading_zero_strip(self):
        assert cudl_normalize("T-S NS 329/0014") == "tsns329.14"

    def test_empty_input(self):
        assert cudl_normalize("") == ""
        assert cudl_normalize(None) == ""
```

**Adapt by:**
- One test class per public function: `TestIsSyntheticSysId`, `TestEncodeInventorySysId`, `TestDecodeInventoryId`, plus `TestRoundTrip` for encode↔decode symmetry.
- Per RESEARCH.md test map: explicit asserts for `is_synthetic_sys_id("990025143260205171") == False` (real Alma at NLI institution suffix) and `is_synthetic_sys_id("990001234560000000") == True` (synthetic).
- D-13 contract: `test_d13_normalization_contract` — pass `99-0001234560-000000` (with dashes) and assert False (helper only accepts canonical all-digit form).
- D-01a collision-check: `test_real_alma_not_synthetic` — load 100 random rows from real `libraries.csv`, assert none satisfy `is_synthetic_sys_id`.
- D-01b string discipline: `test_decode_returns_int` — decode result is int, but never call `int(sys_id)` directly anywhere in helper.
- Encode validation: `test_encode_validation` — assert ValueError on negative/zero, on >10-digit overflow, on non-int.
- Use plain `assert` (no fixtures needed for pure functions) — analog file lines 13-29 demonstrate.

---

### `tests/test_generate_synthetic_rows.py` (regeneration idempotency + collision tests)

**Analog:** `tests/test_shelfmark_bridge_unit_index.py::TestAliasIndexInMemory` (tmp_path injection)

**tmp_path fixture pattern** (lines 70-83):
```python
# Source: tests/test_shelfmark_bridge_unit_index.py:70-83
class TestAliasIndexInMemory:
    def test_mosseri_lookup(self, tmp_path):
        build_alias_index({
            "X1": {"shelfmark": "Moss. III,27O", "library_code": "Mosseri", "call_numbers_raw": ["Moss. III,27O"]},
        }, report_path=tmp_path / "col.csv")
        r = lookup_cudl("mosseriiii27o")
        assert r and r["sys_id"] == "X1"
```

**Adapt by:**
- Use `tmp_path` fixture for libraries.csv copies — never mutate the real artifact in tests (Round 3 Codex MEDIUM lesson from Phase 84).
- `test_idempotent_regeneration`: copy libraries.csv to tmp, run generate_synthetic_rows with `--apply`, save bytes; run again, assert byte-identical.
- `test_no_collision_with_real_alma`: run script in --dry-run mode against real libraries.csv; iterate emitted synthetic IDs and assert `is_synthetic_sys_id(real_id) == False` for every existing row in csv_bank.
- `test_marker_block_round_trip`: write a CSV with existing `# BEGIN SYNTHETIC` block, regenerate, assert old block dropped + new block added (verify the marker-based extraction).
- `test_ambiguity_residue_logged`: feed a synthetic CUDL classmark that maps to 2 FIST signatures, assert `reports/synthetic_ambiguity_residue.csv` (passed via tmp_path) gains a row and NO synthetic row is emitted.

---

### `tests/test_export_fist_synthetic.py` (FJMS sidecar UNION tests)

**Analog:** existing fjms_service tests + `tests/test_shelfmark_bridge_unit_index.py` tmp_path pattern.

**Adapt by:**
- Use a small in-memory sqlite3 connection seeded with FIST.db's minimal schema (dbo_Inventory, dbo_InventoryAlma, dbo_Signature, dbo_UnitCatalogRec) plus 3-5 fixture rows (one with Alma, two without).
- Run the modified `export_fist_enrichment.py` UNION query via `cursor.execute()` against the in-memory DB.
- Assert: real-Alma rows present unchanged; synthetic rows have `AlmaId LIKE '99%000000'`; synthetic rows are 18 chars; `is_synthetic_sys_id(synthetic_alma_id) == True`.
- One test per of the 11 enrichment tables — parametrize across them.

---

### `tests/test_browse_synthetic.py` (browse hide-NLI smoke tests)

**Analog:** `tests/test_shelfmark_bridge.py` (golden fixture parametrize)

**Parametrize pattern** (lines 53-71):
```python
# Source: tests/test_shelfmark_bridge.py:53-71
@pytest.mark.parametrize("row", _load_fixture(),
                         ids=lambda r: r["cudl_classmark"] if r else "no-fixture")
def test_cudl_must_resolve(row, alias_index_built):
    result = lookup_cudl(row["cudl_classmark"])
    assert result is not None, (
        f"{row['cudl_classmark']} ({row['category']}) failed to resolve. "
        f"Notes: {row.get('notes')}"
    )
```

**Adapt by:**
- Smoke-test approach (no NiceGUI runtime required): import `browse.py` module, mock `page` BrowsePage dataclass with synthetic sys_id, call the hide-NLI conditionals in isolation.
- Test per hide-list site from RESEARCH.md §"Browse Hide-NLI Audit": assert `is_synthetic_sys_id(page.sys_id)` blocks the conditional render.
- For the auto-default block: assert `state.active_source == 'cambridge'` after synthetic+manifest path; assert metadata-only fallback (`total_pages=0`) for synthetic+no-manifest.
- KTIV link generation: assert no KTIV URL string is returned for synthetic.

---

### `tests/test_synthetic_round_trip.py` (lists/exclusions/parallels/comments round-trip)

**Analog:** existing supabase tests (closest by role); fixture pattern from `tests/test_shelfmark_bridge.py`.

**Adapt by:**
- Mock supabase client; assert `list_items.insert({sys_id: '990001234560000000'})` succeeds (string passthrough).
- Assert read-side: `corrections_service.get_pending_corrections_for_page(client, synthetic_id, 1, user_id)` returns `[]` without exception (verified safe per RESEARCH.md §Corrections Subsystem Audit).
- Assert parallels: feed source text through `search_composition_logic`, iterate results, assert no synthetic sys_ids appear (no Tantivy chunks → naturally absent).
- Assert exclusions: add synthetic id to `excluded_sys_ids: set`, run search filter, assert filter excludes by string match.

---

### `tests/test_search_serializer.py` (public-API is_synthetic field)

**Analog:** existing `tests/test_search_api*.py` tests by role (closest match).

**Adapt by:**
- `_serialize_item` test: pass a result dict with synthetic sys_id, assert returned envelope has `'is_synthetic': True` at top level.
- `_serialize_item` test with real sys_id: assert `'is_synthetic': False`.
- `serialize_browse_payload` test: assert envelope has the field at the same level as `'shelfmark'`.
- `/api/parallels` test (per Q4): seed source text, assert envelope items also carry `is_synthetic` field (via shared `_serialize_item` — single source of truth per Phase 77 D-14).

---

### `tests/fixtures/synthetic_fixtures.py` (or `tests/fixtures/synthetic_must_resolve.csv`)

**Analog:** `tests/fixtures/cudl_must_resolve.csv` (44 rows, Phase 84)

**Existing fixture shape:**
```csv
# Source: tests/fixtures/cudl_must_resolve.csv:1-9
cudl_classmark,expected_sys_id,expected_shelfmark_substring,category,notes
mosseriiii27o,990053835020205171,Ms. III 27O,mosseri,
mosseriiv281a,990053835750205171,Ms. IV 281A,mosseri,
or1080j15,990052439490205171,Or.1080 J15,or-letter-suffix,
or1080j58,990052439940205171,Or.1080 J58,or-letter-suffix,
MS-MOSSERI-III-00027-O,990053835020205171,Ms. III 27O,mosseri-zfill,critical: forward-label MS-prefix + zfill
```

**Adapt by:**
- Create `tests/fixtures/synthetic_must_resolve.csv` with column shape: `inventory_id, expected_synthetic_sys_id, expected_shelfmark_substring, category, notes`.
- Origin case row: `T-S NS 329.96` (the user case from CONTEXT.md L17 / RESEARCH.md SYNTH-02 test).
- Coverage tiers per D-03: rows from each tier (1) CUDL+FJMS, (2) CUDL-only no-FJMS, (3) FJMS-only no-CUDL.
- Critical-path entries marked `notes='critical: ...'` mirror Phase 84 fixture (line 8: `critical: Mosseri CUDL slug`).
- A Python-module variant `tests/fixtures/synthetic_fixtures.py` is acceptable IF the planner needs runtime fixture construction (mock FIST.db rows). The CSV form is the canonical golden-input.

---

### `reports/synthetic_ambiguity_residue.csv` (audit artifact)

**Analog:** `reports/cudl_alias_collisions.csv` written by `_write_alias_collision_report` at `shared/shelfmark_bridge.py:208-247`.

**Existing residue-writer pattern** (lines 236-247):
```python
# Source: shared/shelfmark_bridge.py:236-247
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
```

**Adapt by:**
- Same shape: header `cudl_label, fist_signature_ids, fist_inventory_ids`, sorted output, pipe-separated multi-values.
- OSError swallow pattern (line 245-246) — packaged/read-only context tolerance.
- Written by `scripts/generate_synthetic_rows.py` at end of run, after qualifying-set CTE produces ambiguity-detected rows.
- Phase 86 audit picks it up (parallels Phase 84's `reports/cudl_alias_collisions.csv` consumed by `scan_cudl_orphans.py`).

---

### `fist_data/synthetic_manifest.json` (audit artifact paired with marker block)

**Analog:** none direct (first JSON manifest under `fist_data/`); recommendation A2 in research.

**Adapt by:**
- JSON array of `{inventory_id: int, synthetic_sys_id: str, source: "cudl_match" | "fjms_metadata" | "both", canonical_shelfmark: str, library_code: "CUL" | "Mosseri"}`.
- Generated alongside libraries.csv block in same script invocation. Diff visibility for ~150-2K rows.
- Loader (genizah_core / runtime) does NOT read this file — purely diagnostic.

---

## Shared Patterns

### Pattern: Helper-as-Public-Contract (D-01)
**Source:** `shared/shelfmark_bridge.py:1-465` (entire file)
**Apply to:** `shared/synthetic_sys_id.py`
- Module-level docstring with explicit decision references.
- Pure functions; no I/O; no logger calls in the helper functions themselves.
- Module-level constants for format anchors.
- Single source of truth — every call site imports the helper, never hand-rolls string slicing.

### Pattern: Layered Hide (D-06 quiet degradation)
**Source:** `web/pages/browse.py:3457-3556` (existing source-switching auto-default)
**Apply to:** All ~22-26 hide-NLI sites (web + desktop) — RESEARCH.md §"Browse Hide-NLI Audit" enumerates them.
- Wrap NLI-only UI elements in `if not is_synthetic_sys_id(sys_id):`.
- Mirror existing library_code-aware source-switching pattern at lines 3457-3462.
- Each web change has a desktop twin in `desktop/viewers.py` or `genizah_app.py` (cross-app parity).

### Pattern: Idempotent Regeneration with Backup
**Source:** `scripts/fix_nli_oxford_mislabel.py:47-60`
**Apply to:** `scripts/generate_synthetic_rows.py`
- Backup-before-write (`shutil.copy2(CSV_PATH, CSV_PATH.with_suffix(".csv.bak"))`).
- Detect-then-preserve line endings via 8KB sample read.
- `csv.writer(f, lineterminator=line_terminator)` — never `csv.writer(f)` default.
- argparse `--dry-run` / `--apply` mutually exclusive group.

### Pattern: Ambiguity-Exclusion + Residue Audit Log
**Source:** `shared/shelfmark_bridge.py:208-326` (`_write_alias_collision_report` + `build_alias_index`)
**Apply to:** `scripts/generate_synthetic_rows.py` (D-05a CUDL↔FIST ambiguity) + `reports/synthetic_ambiguity_residue.csv`.
- Collect (key → set of (id, label)) claims during walk.
- Materialize only keys with exactly ONE distinct id.
- Write excluded keys to a residue CSV for downstream audit.
- Tests inject `report_path=tmp_path / "..."` to avoid mutating real artifact.

### Pattern: tmp_path-Injectable Test Fixtures
**Source:** `tests/test_shelfmark_bridge_unit_index.py:70-107`
**Apply to:** All Phase 85 test files.
- Use `tmp_path` fixture for any file written by the script under test.
- Pass injectable paths to script functions (`report_path=` param).
- Never mutate real `reports/`, `fist_data/`, or `libraries.csv` from tests.

### Pattern: Additive Public-API Field
**Source:** `shared/search_serializer.py:292-310` + Phase 83 stability commitment
**Apply to:** `shared/search_serializer.py` `_serialize_item` and `serialize_browse_payload` envelope.
- Top-level field (NOT nested under `locator`) per A3.
- Schema version stays at 1 (additive only).
- Document in `docs/SEARCH_API.md` + CHANGELOG.

### Pattern: PostHog Property via captured_state
**Source:** `web/api_hardening.py:368-415`
**Apply to:** `web/api_hardening.py` `wrap_endpoint` + `web/search_api.py` handlers.
- Add key to `captured_state` dict default (None until handler populates).
- Endpoint handler sets after locator validation.
- `capture_api_event` reads with `.get(...)` and passes to `posthog.capture(properties=...)`.

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `fist_data/synthetic_manifest.json` | audit JSON | written-by-script | First JSON manifest under `fist_data/` — no precedent. Recommendation A2; planner picks shape (JSON array of rows with provenance fields). |

All other 21 files have either exact or role-match analogs in the existing codebase.

---

## Metadata

**Analog search scope:**
- `shared/` — bridge module, search_serializer, fjms_service, nli_crossref_service, corrections_service
- `scripts/` — fix_nli_oxford_mislabel, export_fist_enrichment
- `tests/` — test_shelfmark_bridge*.py and tests/fixtures/
- `web/` — api.py, api_hardening.py, pages/browse.py, pages/browse_enrichment.py
- `genizah_core.py` — `_load_csv_bank`, `_execute_metadata_search`

**Files scanned:** 12 source files + 3 test files + 1 fixture CSV.

**Pattern extraction date:** 2026-05-08

**Confidence:** HIGH for helper module + idempotent regen + hide-NLI sites (Phase 84 precedent + verified existing patterns). MEDIUM for the new ambiguity-residue CSV and audit-manifest JSON (planner picks final column/key shape).
