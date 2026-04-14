# Phase 65: Repo Hygiene - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Clean up structural debt identified by dual code review (Claude Opus + Codex): audit silent exception handlers, isolate NiceGUI monkey-patches into a dedicated module, gitignore accumulated root debris, and update .gitignore patterns to prevent future accumulation. Zero user-visible behavior changes.

</domain>

<decisions>
## Implementation Decisions

### Silent Exception Handlers (HYGN-01)
- **D-01:** Add logging (log.warning or log.debug) to every silent `except Exception: pass` in first-party code. Don't just comment — make errors visible in logs.
- **D-02:** Scope: first-party source files only (genizah_core.py, genizah_app.py, web/, shared/). Exclude .venv/, venv/, dist/, third-party code.
- **D-03:** Known locations: genizah_core.py lines 540, 585, 703. Full audit may find more in genizah_app.py and web/.

### Monkey-Patch Isolation (HYGN-02)
- **D-04:** Move the 2 existing NiceGUI patches from web/main.py (lines 28-89) to a new `web/framework_patches.py` module.
- **D-05:** Add NiceGUI version guards (using `packaging.version.Version()` or equivalent) so patches are skipped when the upstream fix ships.
- **D-06:** Add justification comment for each patch explaining why it still exists.
- **D-07:** web/main.py imports and calls the patches from framework_patches.py at startup.

### Root Debris Cleanup (HYGN-03)
- **D-08:** Gitignore all temp/debug/backup files — do NOT delete from disk or relocate. Keep local files untouched.
- **D-09:** Claude's Discretion on which root files are "intentional assets" vs debris: check which files are actually imported/referenced in code. Referenced files stay tracked; unreferenced temp/debug/backup files get gitignored.
- **D-10:** Known debris categories (gitignore these patterns):
  - Debug/temp: `_*.txt`, `_*.log`, `_*.js`, `_*.md` (underscore-prefixed scratch files)
  - Backup files: `*.bak`, `*.tmp.*`
  - Translation checkpoints: `translate_*_checkpoint.json`, `translate_*_log.txt`
  - PageSpeed/reports: `psi_*.json`, `battery-report.html`
  - Crash logs: `crash_log*.txt`
  - Data backups: `libraries_backup_*.csv`, `libraries_fixed.csv`, `libraries_translations_backup*.db`, `libraries_translations_clean.db`, `libraries_translations_new.db`
  - Build/import artifacts: `fist_gap_*.txt`, `fist_gap_*.csv`, `fist_domains.db`, `char_merges_*.xlsx`
  - Misc: `sync.ffs_db`, `genizah-extension-*.zip`, `genizah_app.py.tmp.*`

### Gitignore Update (HYGN-04)
- **D-11:** Add wildcard patterns to .gitignore that prevent future accumulation of the debris categories above.
- **D-12:** Intentional root assets that MUST stay tracked: `libraries.csv`, `ie_volume_map.json`, `primary_ie_map.json`, `Help.html`, `icon.ico`, `image.png`, `genizah_core.py`, `genizah_app.py`, `genizah_translations.py`, `unified_variants.py`, and all other source `.py` files. Claude verifies by checking code references.

### Claude's Discretion
- Exact log levels per exception handler (warning vs debug vs info)
- Whether to narrow bare `except:` to specific exception types where appropriate
- Classification of borderline root files (e.g., sample_*.txt, pgp_sample_*.txt) — check if referenced in tests/code
- Exact gitignore pattern syntax and ordering
- Whether `bodleian_master_index.csv`, `oxford_full_db.json`, `cambridge_genizah.json`, `CrossReference_Final.csv`, `nli_crossreference.csv` are still used (check code references; gitignore if unreferenced)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements
- `.planning/REQUIREMENTS.md` — HYGN-01 through HYGN-04 acceptance criteria
- `.planning/ROADMAP.md` — Phase 65 success criteria (5 items)

### Monkey-patch targets
- `web/main.py` lines 28-89 — Current location of 2 NiceGUI patches to be moved
- `.planning/debug/aggrid-dist-not-a-file.md` — Origin story for the ESM handler patch

### Exception audit targets
- `genizah_core.py` lines 540, 585, 703 — Known silent `except Exception: pass`
- `.planning/debug/explosion-guard-cascade.md` — Documents silent error swallowing in search.py

### CI safety net
- `.github/workflows/ci.yml` — Phase 63 CI workflow (must stay green)

### Current gitignore
- `.gitignore` — Current patterns (50 lines, has gaps)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `web/main.py` lines 28-89 — Existing patch code to extract (well-structured, already has comments)
- `.gitignore` — Existing patterns to extend (not replace)
- `ruff.toml` — Phase 63 linting config (will catch any new syntax issues)

### Established Patterns
- Project uses `logging.getLogger(__name__)` throughout — follow same pattern for new log lines
- NiceGUI version available via `nicegui.__version__` (already imported in web/main.py)
- `.venv/` is the active virtual environment; `venv/` is legacy (both already gitignored)

### Integration Points
- `web/main.py` startup sequence — framework_patches.py must be imported and called before NiceGUI starts
- `genizah_core.py` exception handlers — in search/metadata paths, must not change behavior (add logging only)
- `.gitignore` — changes affect `git status` for all developers

</code_context>

<specifics>
## Specific Ideas

- User wants plain-English communication; technical decisions deferred to AI review
- Keep local files untouched — gitignore only, no deletion or relocation
- Conservative approach: this is a structural debt cleanup, not a feature

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 65-repo-hygiene*
*Context gathered: 2026-04-14*
