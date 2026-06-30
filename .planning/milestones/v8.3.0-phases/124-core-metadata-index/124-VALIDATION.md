---
phase: 124
slug: core-metadata-index
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-26
---

# Phase 124 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Phase 124 is a pure mechanical extraction (GUARD-02 = zero behavior change), so
> validation is dominated by **identity/facade** (CORE-08/09, GUARD-04),
> **back-edge** (GUARD-01), and **source-scan retarget** (GUARD-03) checks layered on
> top of the existing behavioral integration suite.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing) |
| **Config file** | none — pytest.ini / pyproject.toml auto-discovered |
| **Quick run command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_no_back_edges_core.py tests/test_nli_cache_bounded_lru.py tests/test_browse_synthetic.py -x -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` (NO `-n auto` — OOMs loading Tantivy per worker) |
| **Estimated runtime** | quick ~30–60s; full suite several minutes |

---

## Sampling Rate

- **After every task commit:** Run the per-commit gate (quick + the touched modules' integration tests):
  `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_no_back_edges_core.py tests/test_nli_cache_bounded_lru.py tests/test_browse_synthetic.py tests/test_audit_followup_2026_05_29.py tests/test_desktop_folio_navigation.py tests/test_api_nli_breaker_integration.py -x -q`
- **After every plan wave:** Run the full suite command.
- **Before `/gsd:verify-work`:** Full suite must be green; per-file ruff clean on all touched files.
- **Max feedback latency:** ~60 seconds (per-commit gate).

---

## Per-Task Verification Map

> Task IDs are indicative (planner finalizes). Phase 124 is one plan, two commits:
> Commit 1 = metadata_manager (+ `_BoundedLRUCache`) + GUARD-03 retarget; Commit 2 = indexer.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 124-01-01 | 01 | 1 | CORE-08 | — | N/A (mechanical move) | unit (identity) | `pytest tests/test_no_back_edges_core.py -k metadata_manager -x -q` | ❌ W0 | ⬜ pending |
| 124-01-02 | 01 | 1 | CORE-08 | — | `_BoundedLRUCache` importable from both genizah_core + shared | unit (identity) | `pytest tests/test_nli_cache_bounded_lru.py -q` | ✅ | ⬜ pending |
| 124-01-03 | 01 | 1 | GUARD-01 | — | `shared.metadata_manager` imports with no module-level genizah_core back-edge | unit (AST) | `pytest tests/test_no_back_edges_core.py -x -q` | ✅ (registry grows) | ⬜ pending |
| 124-01-04 | 01 | 1 | GUARD-03 | — | enrich_metadata source-scan reads `shared/metadata_manager.py` | unit (source/AST) | `pytest tests/test_desktop_folio_navigation.py -q` | ✅ (retargeted) | ⬜ pending |
| 124-01-05 | 01 | 1 | GUARD-02 | — | NLI breaker + IIIF/MARC + synthetic browse behavior unchanged | integration | `pytest tests/test_api_nli_breaker_integration.py tests/test_browse_synthetic.py tests/test_audit_followup_2026_05_29.py -q` | ✅ | ⬜ pending |
| 124-01-06 | 01 | 1 | CORE-09 | — | `shared.indexer.Indexer is genizah_core.Indexer` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k indexer -x -q` | ❌ W0 | ⬜ pending |
| 124-01-07 | 01 | 1 | GUARD-01 | — | `shared.indexer` imports with no module-level genizah_core back-edge (inline `_tr` / `_strip_brackets`) | unit (AST) | `pytest tests/test_no_back_edges_core.py -x -q` | ✅ (registry grows) | ⬜ pending |
| 124-01-08 | 01 | 1 | GUARD-04 | — | genizah_core re-export shims not stripped; `build_index.py` resolves `Indexer.create_index` | lint + import | `python -m ruff check genizah_core.py shared/metadata_manager.py shared/indexer.py` | N/A (gate) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_no_back_edges_core.py` — add `"shared/metadata_manager.py"` and `"shared/indexer.py"` to `EXTRACTED_MODULES`; add `test_metadata_manager_identity`, `test_metadata_manager_standalone_import`, `test_indexer_identity`, `test_indexer_standalone_import` (patterns in RESEARCH §Code Examples).
- [ ] `tests/test_desktop_folio_navigation.py` — add a `metadata_manager_source()` fixture reading `shared/metadata_manager.py`; retarget the 3 tests that scan `enrich_metadata` from `genizah_core_source` to `metadata_manager_source`. **Must land in the same commit as the MetadataManager extraction** (GUARD-03 / Pitfall 3).

*All other infrastructure (`test_nli_cache_bounded_lru.py`, `test_browse_synthetic.py`, `test_audit_followup_2026_05_29.py`, `test_api_nli_breaker_integration.py`) already exists.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Post-extraction Codex 3-round review convergence | GUARD-02/04 | Cross-AI adversarial review is out-of-band of pytest | `codex exec -s read-only "$(cat brief)" < /dev/null` over the `<base>..HEAD` range; converge to APPROVE |
| Base-vs-HEAD facade-name diff | GUARD-04 | Detects dropped re-exports that no single test asserts | Run `facade_diff.py` (AST module-level name diff, BOM-stripped) base vs HEAD; confirm no previously-public name dropped |

---

## Validation Sign-Off

- [ ] All tasks have automated verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (identity tests + GUARD-03 retarget)
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s (per-commit gate)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
