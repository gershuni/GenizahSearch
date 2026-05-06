---
phase: 84-cudl-shelfmark-normalization
plan: "04"
subsystem: shared
tags: [normalization, cudl, shelfmark, bridge-wiring, runtime-migration, nli-crossref]
dependency_graph:
  requires:
    - shared/shelfmark_bridge.py (cudl_normalize, build_alias_index, lookup_cudl, shelfmark_to_cudl_label from Plans 01-03)
    - shared/nli_crossref_service.py (get_cambridge_manifest, get_cambridge_manifest_by_label — existing)
    - genizah_core.MetadataManager (csv_bank, search_by_meta, _load_csv_bank — existing)
    - web/pages/browse.py (CUDL external-link builder — existing)
    - scripts/scan_cudl_orphans.py (normalize + NUM_RE — replaced by bridge import)
  provides:
    - genizah_core.py: _BRIDGE_IMPORT_WARNED + _warn_bridge_import_failed (module-level WARNING-once)
    - genizah_core.py: build_alias_index call in _load_csv_bank after csv_bank populated
    - genizah_core.py: lookup_cudl fallback in search_by_meta for field=='shelfmark'
    - genizah_core.py: 2a-supplement migrated to get_cambridge_manifest_with_bridge(shelfmark)
    - genizah_core.py: 2a-mosseri variant loop preserved, inner call migrated to get_cambridge_manifest_with_bridge(variant)
    - web/pages/browse.py: _BRIDGE_IMPORT_WARNED + _warn_bridge_import_failed + shelfmark_to_cudl_label wiring
    - shared/nli_crossref_service.py: _BRIDGE_IMPORT_WARNED + get_cambridge_manifest_with_bridge() 4-tier wrapper
    - scripts/scan_cudl_orphans.py: normalize + NUM_RE imported from bridge (one source of truth)
  affects:
    - Plan 84-05 (integration test suite validates all four D-08 wiring sites)
tech_stack:
  added: []
  patterns:
    - WARNING-once module-level flag (_BRIDGE_IMPORT_WARNED) in every bridge-import site
    - ImportError degraded path normalizes internally (Round 3 Codex MEDIUM)
    - Variant loop preserved in 2a-mosseri block (Round 3 Codex HIGH #4 option-b)
    - 4-tier bridge cascade in get_cambridge_manifest_with_bridge (canonical -> cudl_normalize -> mosseri label -> slug)
key_files:
  created: []
  modified:
    - genizah_core.py
    - web/pages/browse.py
    - shared/nli_crossref_service.py
    - scripts/scan_cudl_orphans.py
decisions:
  - "Variant loop at genizah_core.py:~3997 PRESERVED (Round 3 Codex HIGH #4 option-b): wrapper takes one shelfmark; only the per-variant loop knows which alternates to try — deleting it would silently lose CUDL manifests for Mosseri rows whose primary shelfmark is not the constructible form"
  - "2a-supplement passes raw shelfmark to wrapper (not norm_sm): wrapper owns normalization internally, so passing already-normalized input would skip cudl_normalize / shelfmark_to_cudl_label fallback branches"
  - "ImportError fallback in get_cambridge_manifest_with_bridge calls normalize_shelfmark internally (Round 3 Codex MEDIUM): pre-phase callers passed normalized input; after option-(b) migration callers pass raw, so degraded path must still normalize"
  - "_warn_bridge_import_failed uses module-level flag (not attribute on the imported symbol): inside ImportError the symbol is undefined, so attaching state to it would NameError (Codex HIGH #4 Round 2)"
  - "scan_cudl_orphans.py import re left in place (harmless): only NUM_RE and normalize were replaced; re module still present as implicit import"
metrics:
  duration: "~15 minutes"
  completed: "2026-05-06"
  tasks_completed: 2
  tasks_total: 2
  files_created: 0
  files_modified: 4
---

# Phase 84 Plan 04: Bridge Wiring — All Four D-08 Call Sites Summary

**One-liner:** Wired shelfmark_bridge into all four runtime call sites (genizah_core search fallback + _load_csv_bank hook, browse.py CUDL link builder, nli_crossref_service get_cambridge_manifest_with_bridge wrapper, scan_cudl_orphans one-source-of-truth import) with variant loop preserved and WARNING-once logging at every ImportError boundary.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Wire build_alias_index + shelfmark search fallback into genizah_core.py | 6ee7359c | genizah_core.py |
| 2 | Wire CUDL link builder + NLI bridge wrapper + orphan-scanner unification + migrate NLI runtime callers | c32c64c1 | web/pages/browse.py, shared/nli_crossref_service.py, scripts/scan_cudl_orphans.py, genizah_core.py |

## What Was Built

### Task 1 — genizah_core.py (Task 1 + partial Task 2)

**Module-level WARNING-once flag** added after the `LOGGER` definition:
```python
_BRIDGE_IMPORT_WARNED = False

def _warn_bridge_import_failed(exc):
    ...
```

**`_load_csv_bank` hook** — immediately after `LOGGER.info("Loaded %d records into csv_bank ...")`:
```python
try:
    from shared.shelfmark_bridge import build_alias_index as _build_cudl_alias_index
    _build_cudl_alias_index(self.csv_bank)
except ImportError as e:
    _warn_bridge_import_failed(e)
except Exception as e:
    LOGGER.warning("CUDL alias index build failed (continuing without bridge): %s", e)
```

**`search_by_meta` CUDL fallback** — after canonical matching returns empty results for `field == 'shelfmark'`:
```python
if field == 'shelfmark' and not results:
    try:
        from shared.shelfmark_bridge import lookup_cudl
        hit = lookup_cudl(query)
        if hit and hit.get('sys_id'):
            results.add(hit['sys_id'])
    except ImportError as e:
        _warn_bridge_import_failed(e)
    except Exception as e:
        LOGGER.debug("Bridge fallback failed for query %r: %s", query, e)
```

**2a-supplement migration** (genizah_core.py:~3984, before → after):
- Before: `norm_sm = normalize_shelfmark(shelfmark)` + `crossref_svc.get_cambridge_manifest(norm_sm)`
- After: `crossref_svc.get_cambridge_manifest_with_bridge(shelfmark)` (raw; wrapper normalizes internally)

**2a-mosseri variant loop preserved** (genizah_core.py:~3997, Round 3 Codex HIGH #4 option-b):
- Before (inner loop body): `label = construct_mosseri_cudl_label(variant)` + `if label:` + `crossref_svc.get_cambridge_manifest_by_label(label)`
- After (inner loop body): `cam_url = crossref_svc.get_cambridge_manifest_with_bridge(variant)`
- The `for variant in variants:` loop and `if cam_url: ... break` are PRESERVED

### Task 2 — web/pages/browse.py

Module-level WARNING-once flag added after `logger = logging.getLogger(__name__)`.

CUDL link builder (line ~3621, after shift) — before:
```python
cudl_url = f"https://cudl.lib.cam.ac.uk/view/{page.shelfmark.replace(' ', '-')}"
```
After:
```python
_cudl_slug = None
try:
    from shared.shelfmark_bridge import shelfmark_to_cudl_label
    _cudl_slug = shelfmark_to_cudl_label(page.shelfmark)
except ImportError as _e:
    _warn_bridge_import_failed(_e)
except Exception:
    _cudl_slug = None
if not _cudl_slug:
    _cudl_slug = page.shelfmark.replace(' ', '-')  # Pre-Phase-84 fallback
cudl_url = f"https://cudl.lib.cam.ac.uk/view/{_cudl_slug}"
```

### Task 2 — shared/nli_crossref_service.py

Module-level `_BRIDGE_IMPORT_WARNED = False` added after `logger`.

New method `get_cambridge_manifest_with_bridge(self, shelfmark)` added after `get_cambridge_manifest_by_label`. 4-tier cascade:
1. `normalize_shelfmark(shelfmark)` → `get_cambridge_manifest()` (canonical, pre-Phase-84 behavior)
2. `cudl_normalize(shelfmark)` → `get_cambridge_manifest()` (CUDL classmark form)
3. `construct_mosseri_cudl_label(shelfmark)` → `get_cambridge_manifest_by_label(mosseri_label)` (Mosseri label)
4. `shelfmark_to_cudl_label(shelfmark)` → `get_cambridge_manifest(slug)` (T-S / Or. / Add. forward slug)

ImportError fallback: calls `normalize_shelfmark(shelfmark)` then `get_cambridge_manifest()` — normalizes internally (Round 3 Codex MEDIUM).

### Task 2 — scripts/scan_cudl_orphans.py

Replaced local `normalize()` function + inline `NUM_RE` definition with:
```python
import sys as _sys
if str(ROOT) not in _sys.path:
    _sys.path.insert(0, str(ROOT))
from shared.shelfmark_bridge import cudl_normalize as normalize, NUM_RE  # noqa: F401
```
All existing call sites (`normalize(v)`, `normalize(ns)`, `NUM_RE.match(n)`) work unchanged with the imported alias.

## Migrated NLI Call Sites (Codex MEDIUM #6)

| File | Line (approx) | Before | After |
|------|---------------|--------|-------|
| genizah_core.py | ~3984 | `crossref_svc.get_cambridge_manifest(norm_sm)` | `crossref_svc.get_cambridge_manifest_with_bridge(shelfmark)` |
| genizah_core.py | ~4001 (loop body) | `crossref_svc.get_cambridge_manifest_by_label(label)` | `crossref_svc.get_cambridge_manifest_with_bridge(variant)` |

Zero surviving `crossref_svc.get_cambridge_manifest(` or `crossref_svc.get_cambridge_manifest_by_label(` calls remain in `genizah_core.py` (Round 3 Codex MEDIUM verified by grep).

## Variant Loop Preservation (Round 3 Codex HIGH #4)

The `for variant in variants:` loop at `genizah_core.py:~3997` is **PRESERVED**. Only the inner call was migrated from the two-line `label = ... if label: cam_url = get_cambridge_manifest_by_label(label)` to the single wrapper call `cam_url = get_cambridge_manifest_with_bridge(variant)`.

Why: the wrapper accepts one shelfmark and cannot enumerate a row's `call_numbers_raw` variants internally. Any Mosseri sys_id whose primary shelfmark is not the constructible form but whose alternate variant is — that row would silently lose its CUDL manifest if the loop were removed.

## Deviations from Plan

None — plan executed exactly as written. All Round 3 Codex and Gemini findings addressed:
- Round 3 Codex HIGH #4: variant loop preserved (option-b only)
- Round 3 Codex MEDIUM: ImportError branch normalizes internally
- Round 3 Codex MEDIUM: zero surviving pre-bridge calls in genizah_core.py
- Codex HIGH #4 Round 2: module-level warned flag (not attached to failed-import symbol)
- Codex MEDIUM #6: migration unconditional (both sites migrated)
- Gemini LOW: WARNING-once logging at all bridge-import try/except sites

## Known Stubs

None. All four D-08 wiring sites are fully implemented and runtime-connected.

## Threat Flags

None. This plan modifies normalization routing only — no new network endpoints, no auth paths, no Supabase writes, no schema changes. Bridge failures degrade to v7.10 behavior (pre-Phase-84).

## Self-Check: PASSED

- `genizah_core.py` modified and importable: CONFIRMED
- `web/pages/browse.py` has `shelfmark_to_cudl_label` + fallback preserved: CONFIRMED
- `shared/nli_crossref_service.py` has `get_cambridge_manifest_with_bridge` method: CONFIRMED
- `scripts/scan_cudl_orphans.py` imports from bridge; local `normalize()` removed: CONFIRMED
- `_BRIDGE_IMPORT_WARNED` present in all three modified modules: CONFIRMED
- variant loop at genizah_core.py:~3997 PRESERVED with wrapper(variant) inside: CONFIRMED
- zero `crossref_svc.get_cambridge_manifest(` and `crossref_svc.get_cambridge_manifest_by_label(` in genizah_core.py: CONFIRMED
- ImportError branch of `get_cambridge_manifest_with_bridge` calls `normalize_shelfmark` internally: CONFIRMED
- Commits `6ee7359c` and `c32c64c1` exist: CONFIRMED
- No unexpected file deletions: CONFIRMED
