---
type: session-handoff
date: 2026-04-21
next-action: triage OPEN_ISSUES before release bump
---

# Session handoff — 2026-04-21

## Context

Closed out two investigations today. Unreleased on `master-main`; plan is to triage
more `docs/OPEN_ISSUES.md` entries before the next release bump (last release was
`v7.9.0` at commit `17663e1b`).

## What landed today (16 commits since v7.9.0)

### 260419-cfx — CUL CUDL image-text alignment (follow-up to 260419-nwv)

User-visible change: CUL manuscripts where CUDL canvas count ≠ NLI/transcription
count now default to NLI images instead of mis-aligned CUDL positional serving.
CUDL still available via source toggle. Verified on T-S NS 158.112 (12 CUDL
canvases vs 14 NLI canvases).

Also retracted H3 from the predecessor 260419-nwv summary: the NLI IIIF manifest
was NOT resolving to the wrong IE — Transcriptions.txt FL ids are text-layer FLs
(FL167150424–437) that 500 on image GET, while the manifest correctly returns
image-layer FLs (FL167150439–452) from the same IE167150422. Fix scope therefore
only covers H1 (CUDL positional mismatch).

Key commits: `7d0fbb29`, `7ba37277`, `e2709838`, `549ef6af`, `a854a5ee`, `08596e1c`,
`4f5c8461`, `ebf84e1b`, `8bab6263`.

Open partial-fix gap (documented in `docs/OPEN_ISSUES.md:83`): when CUDL and NLI
have the **same count but different canvas order** (e.g. CUDL prepends a binding
canvas and drops the last folio), the count-match heuristic misses it. NLI canvas
order is authoritative. Proper fix would align by (folio_num, side) regardless
of count. Not urgent, tracked.

### FJMS `Instatution` migration

User-visible change: ~30,625 manuscripts that previously rendered empty catalog
dialogs and `0` scholarly-source button counts now show correct named team
columns (GRU – Cambridge, The Fleischer Piyut Project, Uri Ehrlich, Yad Harav
Herzog, Schocken – Zulay, etc.). Verified on T-S NS 325.82.

- `scripts/fix_instatution_sources.py` created (idempotent, dry-run by default)
- Migration applied: 267,104 catalog rows + 47,800 catalog_free_desc rows
  rewritten via local `FIST.db::CODE_Institution` join (NOT the stale Panel 7
  API checkpoint)
- Backup at `fist_data/fjms_enrichment_pre_instatution_20260421_140512.db`
- 4 service-layer regression tests added in `tests/test_fjms_service.py`
- `desktop/dialogs_scholarly.py` dedupe within-source repeats so identical
  per-record values (e.g. 5 Uri Ehrlich records all carrying NumFolio=6) show
  once instead of `6, 6, 6, 6, 6`

Key commits: `74c2e1f2`, `cbbfe9ff`, `1138c7e2`, `9c089a7a`.

Residual (accepted as-is): 993 catalog AlmaIds + 3,398 free_desc AlmaIds remain
with only generic `Inventory`/`Nuscha` sources. `Inventory` suppression is
semantically correct (library accession records). `Nuscha` migration was
deferred — `CODE_Author` join is known-stale for SourceId=300 (prior audit
2026-03-12), and spot samples of Yevr. III manuscripts all resolve to SubId=2
= "Abitbol, Michel" uniformly (implausible). Validate per-sample on FJMS before
any Nuscha migration.

## Repo state

- **Branch:** `master-main`
- **Last release:** `v7.9.0` at commit `17663e1b` (2026-04-20 era)
- **APP_VERSION:** still 7.7.2 per CLAUDE.md (release bump deferred)
- **Tests:** full suite 1,122 passed / 8 skipped
- **Docs:** `scripts/check_docs.py` green
- **Uncommitted:** nothing material — just untracked debug/scratch files

## Suggested triage before release

### OPEN_ISSUES entries worth considering

| Line | Issue | Priority hint |
|------|-------|---------------|
| L80 | Web search Export ignores row checkboxes | P2 — pre-existing, user-visible |
| L81 | JTS browse missing DPUL source switch (ENA 1052.1) | P2 — pre-existing, visible regression per user |
| L86 | `FjmsService` batch queries `"bad parameter"` / `"tuple index out of range"` | P2 — intermittent, fails gracefully |
| L87 | Font-display middleware rebuilds 304s as 200s | P2 — cache-correctness bug, low user impact |
| L91 | `parent_slot deleted` RuntimeError in logs | P3 — cosmetic log noise |
| L108–109 | Responsa wildcard recall / explosion guard | P2 — search correctness |
| L182–184 | Reading Desk UX (Add to view, green bar, session restore) | P3 — polish |
| L267 | `ilike` injection via unescaped user input | **P1 — security, verify urgency** |
| L273–274 | Minor code-quality from prior review (WR-01, WR-02) | P3 — cleanup |
| L276–277 | Minor code-quality (IN-01, IN-02) | P3 — cleanup |

Full list: 23 `❌ Open` entries in `docs/OPEN_ISSUES.md`. Anything tagged
`Fixed` or `Partially Fixed` is already resolved.

### Release-mechanical steps (when ready)

1. Pick target version (likely `v7.9.1` for bugfix-only since 7.9.0 was the
   Structural Foundation + Decomposition release).
2. `python scripts/bump_version.py 7.9.1` — updates `version.py`, Windows
   `version_info.txt`, Inno Setup `CompileScriptGenizah.iss`, and `README.md`
   header line automatically.
3. Manual: update `CHANGELOG.md` with a new `## [7.9.1]` section (CUL image
   fix, Instatution migration, dedupe polish).
4. Manual: add a "Recently Changed" entry to `CLAUDE.md`.
5. Manual: update `README.md` "What's New" section.
6. `/release` skill handles build + deploy + GitHub release after that.

### Migration artifact note

`fist_data/fjms_enrichment.db` was modified in-place today. If the repo ships
this DB (it's ~900MB per MEMORY.md), the updated version should be deployed.
Backup file `fist_data/fjms_enrichment_pre_instatution_20260421_140512.db` is
untracked and can be kept locally or removed once the migration is confirmed
good in prod.

## Open questions / loose ends

- None blocking. The CUL-partial-fix gap (same-count-different-order case) is
  tracked but shouldn't block a bugfix release.
- If Nuscha-attribution comes up: validate `CODE_Author` mapping against FJMS
  for a few samples BEFORE writing a migration. The stale-mapping risk is
  documented in `docs/OPEN_ISSUES.md` under the FJMS Inventory/Nuscha
  "Accepted" entry.

## To resume

```bash
# At session start, verify state
git log --oneline 17663e1b..HEAD   # 16 commits since v7.9.0
git status                          # should be clean

# Triage open issues
grep -n "❌ Open" docs/OPEN_ISSUES.md
```

Then pick from the suggested-triage table above, or ask the user which issues
to prioritize before bumping.
