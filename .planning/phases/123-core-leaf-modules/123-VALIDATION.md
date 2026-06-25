---
phase: 123
slug: core-leaf-modules
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-25
---

# Phase 123 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Pure mechanical refactor (zero behavior change, GUARD-02). Validation = same-object
> identity per module + the existing regression suites green at every cluster commit boundary.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing) |
| **Config file** | `pytest.ini` (existing) |
| **Quick run command** | `pytest tests/test_no_back_edges_core.py tests/test_responsa_core.py tests/test_shelfmark_bridge.py -x -q` |
| **Full suite command** | `pytest tests/ -q` (CI uses the marker-based gui split; locally set `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` to avoid the PyQt6 headless segfault — do NOT use `-n auto`, it OOMs loading Tantivy per worker) |
| **Estimated runtime** | quick ~30s; full suite several minutes |

---

## Sampling Rate

- **After every task commit (per cluster):** Run `pytest tests/test_no_back_edges_core.py tests/test_responsa_core.py tests/test_shelfmark_bridge.py -x -q` (< 30s)
- **After every plan wave / cluster commit boundary:** Run the full suite (SC#5 — full pytest suite green at EVERY cluster commit)
- **Before `/gsd:verify-work`:** Full suite must be green + per-file `python -m ruff check` on all 7 new modules + `genizah_core.py` (never repo-wide `ruff --fix` — strips `# noqa: F401` shims)
- **Max feedback latency:** ~30 seconds (quick path)

---

## Per-Task Verification Map

> Task IDs provisional — aligned to the D-02 leaf-first cluster ordering (one atomic commit per cluster).
> The planner refines IDs; the requirement→test mapping below is authoritative.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 123-01-01 | 01 | 1 | CORE-06 | — | N/A (pure refactor) | unit (identity) + AST | `pytest tests/test_no_back_edges_core.py -k "browse_map" -x -q` | ✅ | ⬜ pending |
| 123-01-01 | 01 | 1 | CORE-06 / GUARD-03 | — | N/A | regression | `pytest tests/test_shelfmark_bridge.py -q` (snapshot regenerated) | ✅ | ⬜ pending |
| 123-01-02 | 01 | 2 | CORE-07 | — | N/A | unit (identity) + AST | `pytest tests/test_no_back_edges_core.py -k "text_normalize" -x -q` | ✅ | ⬜ pending |
| 123-01-02 | 01 | 2 | CORE-07 | — | N/A | regression (local_indexer retargets) | `pytest tests/test_local_pdf_nikud_strip.py -q` | ✅ | ⬜ pending |
| 123-01-03 | 01 | 3 | CORE-02 | — | N/A | unit (identity) + AST | `pytest tests/test_no_back_edges_core.py -k "variant" -x -q` | ✅ W0 | ⬜ pending |
| 123-01-04 | 01 | 4 | CORE-01 / SC#2 | — | N/A | unit (identity) + regression | `pytest tests/test_no_back_edges_core.py -k "responsa" -x -q && pytest tests/test_responsa_*.py -q` | ✅ W0 | ⬜ pending |
| 123-01-05 | 01 | 5 | CORE-03 | — | N/A | unit (identity) + AST | `pytest tests/test_no_back_edges_core.py -k "codicological" -x -q` | ✅ W0 | ⬜ pending |
| 123-01-06 | 01 | 6 | CORE-04 | — | N/A | unit (identity) + regression | `pytest tests/test_no_back_edges_core.py -k "joins_manager" -x -q && pytest tests/test_*joins*.py -q` | ✅ W0 | ⬜ pending |
| 123-01-07 | 01 | 7 | CORE-05 | — | N/A | unit (identity) + regression | `pytest tests/test_no_back_edges_core.py -k "lists_manager" -x -q && pytest tests/test_user_lists_cache_isolation.py tests/test_recently_viewed_bugs.py -q` | ✅ W0 | ⬜ pending |
| 123-01-* | 01 | all | GUARD-01 | — | N/A | static (AST) | `pytest tests/test_no_back_edges_core.py -q` (EXTRACTED_MODULES grows 1→8) | ✅ | ⬜ pending |
| 123-01-* | 01 | all | GUARD-02 | — | N/A | regression | full suite green at every cluster commit | ✅ | ⬜ pending |
| 123-01-* | 01 | all | GUARD-04 | — | N/A | static (per-file ruff) | `python -m ruff check genizah_core.py` (shims intact, no `# noqa: F401` stripping) | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_no_back_edges_core.py` — extend (same file as Phase 122) with 7 same-object identity tests + 7 standalone-import smoke tests (one per new module, D-03), mirroring the Phase 122 `test_config_identity` pattern; grow `EXTRACTED_MODULES` 1→8 (one entry per cluster, added in the SAME commit as each module's creation — Pitfall 5).
- [ ] `tests/fixtures/normalize_shelfmark_snapshot.json` — regenerate the SHA256 snapshot from `shared.browse_map_utils.normalize_shelfmark` source (GUARD-03 / Pitfall 1: `inspect.getsource` follows the object to its new defining file).

*Existing regression infrastructure (responsa, joins, lists, shelfmark-bridge, local-pdf) otherwise covers all phase requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| No repo-wide `ruff --fix` run during the phase | GUARD-04 | Process discipline, not a runtime assertion — `--fix` strips `# noqa: F401` shims (Pitfall 4) | Reviewer confirms each commit ran only per-file `python -m ruff check <file>`, never `ruff check . --fix` |

*All extraction behaviors otherwise have automated verification via identity tests + existing regression suites.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (identity/smoke tests + snapshot regeneration)
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s (quick path)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
