---
id: SEED-011
status: shipped
planted: 2026-06-21
planted_during: SEED-010 debug session (received an out-of-band Codex performance audit, unrelated to images)
trigger_when: A performance-focused standalone phase or a set of /gsd-quick passes. Independent of SEED-010 (Joins Lab images). Finding 1 (composition chunk-plan dedup) is the highest-leverage, self-contained win and can be done first on its own. Composition-core findings are dual-app (web + desktop share genizah_core.py); validate both. Real-index perf tests need Genizah_Index present (absent in the audit env).
scope: medium (one composition-core hot-path dedup + two web-UI items + one conditional storage tweak)
---

# SEED-011: Composition search + Joins-Lab-UI performance audit (Codex)

> Captured as a seed (NOT fixed inline). Source: an out-of-band Codex "Performance Audit" automation,
> delivered during the SEED-010 image-resolution debug session. NOT related to the image bug.
> Codex automation memory: `C:\Users\gersh\.codex\automations\performance-audit\memory.md`.

## Findings (Codex, verbatim intent + file:line)

### 1. HIGHEST LEVERAGE — `corpus_scope='all'` duplicates per-chunk prep in composition search
Composition search with `corpus_scope='all'` builds the per-chunk queries/regexes **twice** — once per
index — instead of reusing a shared plan. Measured: **111 chunks → 222 queries/regexes** in `all` vs
**111** in single scope.
- `genizah_core.py:9216`
- `genizah_core.py:9362`

### 2. SAME PATTERN — LAB composition double-prep
LAB composition repeats the same per-chunk duplication: **23 chunks → 46 weak/fingerprint calls**.
- `genizah_core.py:1604`
- `genizah_core.py:1772`

**Fix direction (findings 1 + 2):** precompute a **shared per-chunk plan once**, then
parse/search **per index separately**. The chunk plan (query string + compiled regex + weak/fingerprint
derivations) is index-independent; only the Tantivy search + regex filter pass needs to run per index.
This is the durable shape; both `all` and LAB are instances of the same redundancy.

### 3. WEB UI — candidate grid table view sends ALL rows (grid paginates, table doesn't)
The grid surface paginates, but the **table** view serializes **all rows**. ~**5k rows → ~1.66 MB**
before NiceGUI/websocket overhead.
- `web/components/candidate_grid.py:1101`

**Fix direction:** paginate / virtualize the table path too, or cap + lazy-load rows, mirroring the grid's
pagination. (NOTE: this file is also touched by SEED-010, but this is a DISTINCT concern — table-row
serialization, not per-provider image-URL resolution. Don't conflate the two fixes.)

### 4. STORAGE — snapshots bounded but large-ish (CONDITIONAL)
Per-user storage snapshots are bounded but not tiny: worst simulated snapshot ~**557 KB / 14.9 ms**.
**Only act if UI jank appears** — then debounce writes or skip unchanged writes.

### 5. TEST GAP — real-index perf tests skipped
`Genizah_Index` was absent in the audit environment, so real-index perf tests were skipped. Any perf
work here should be validated against a present `Genizah_Index` to get real numbers, not just simulated.

## Priority order for a future phase
1. Composition chunk-plan reuse (findings 1 + 2) — highest leverage, self-contained, dual-app core.
2. Candidate-grid table-view pagination/virtualization (finding 3) — web-only, user-visible payload win.
3. Storage snapshot debounce (finding 4) — only if jank observed; lowest priority.
4. Stand up `Genizah_Index` for real perf validation (finding 5) — prerequisite for trustworthy numbers.

## Notes
- Findings 1 + 2 live in `genizah_core.py` (shared by web + desktop) — a fix benefits BOTH apps but must
  be validated in both. See memory key constraint: all search logic in genizah_core.py.
- Finding 3 is web-only (NiceGUI Joins Lab).
- Validate with `Genizah_Index` present; the audit ran without it (simulated numbers only).
