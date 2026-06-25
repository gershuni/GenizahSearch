---
phase: 122
slug: config-enabler
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-25
---

# Phase 122 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 122-RESEARCH.md "Validation Architecture". Pure refactor — zero behavior
> change (GUARD-02). The only NEW test infrastructure is `tests/test_no_back_edges_core.py`.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, no version bump) |
| **Config file** | `pyproject.toml` (`[tool.pytest.ini_options]`) |
| **Quick run command** | `PYTHONUTF8=1 pytest tests/test_no_back_edges_core.py tests/test_history_no_result_snapshots.py tests/test_local_filter_persistence.py -x -q` |
| **Full suite command** | `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q` |
| **Estimated runtime** | quick ~15s · full suite a few min |

**Windows / local executor notes (from RQ-5):**
- `PYTHONUTF8=1` required (cp1255 console chokes on emoji/Hebrew in output).
- Do **NOT** set `GITHUB_ACTIONS=true` locally — it skips `test_my_library_tab_*.py` which exercise the `Config` monkeypatch (D-14) identity guarantee.
- Do **NOT** use `-n auto` (Tantivy loads its index per worker → OOM); bare `pytest` or `-n 2` max.
- `QT_QPA_PLATFORM=offscreen` only if running GUI-marked tests locally without a display (not needed for this phase's quick smoke — no Qt).
- `test_no_back_edges_core.py` is **not** a GUI test — do NOT add it to `_GUI_TEST_FILES`.

---

## Sampling Rate

- **After every task commit:** `PYTHONUTF8=1 pytest tests/test_no_back_edges_core.py tests/test_history_no_result_snapshots.py tests/test_local_filter_persistence.py -x -q`
- **After every plan wave / before phase close:** `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q` — must be green
- **Max feedback latency:** ~15s for the quick smoke

---

## Per-Task Verification Map

| Req ID | Behavior | Threat Ref | Test Type | Automated Command | File Exists | Status |
|--------|----------|------------|-----------|-------------------|-------------|--------|
| CONFIG-01 | `shared.config.Config is genizah_core.Config` (same object, identity) | — | unit | `pytest tests/test_no_back_edges_core.py::test_config_identity -x` | ❌ W0 | ⬜ pending |
| GUARD-01 | No extracted-this-milestone `shared/` module has a module-level `import genizah_core` (registry = {`shared/config.py`}); AST via `ast.iter_child_nodes` (module-level only) | — | AST static | `pytest tests/test_no_back_edges_core.py -x` | ❌ W0 | ⬜ pending |
| GUARD-02 | Full existing suite green (zero behavior change) | — | integration | `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q` | ✅ | ⬜ pending |
| GUARD-03 | No source-scanning/AST test broken (none deleted this phase) | — | structural | covered by GUARD-02 full run | ✅ | ⬜ pending |
| GUARD-04 | `from genizah_core import Config` still resolves (permanent facade) | — | import smoke | `python -c "from genizah_core import Config; print(Config)"` | ✅ | ⬜ pending |
| D-02 | `shared/session_persistence.py` imports `Config` from `shared.config`, not `genizah_core` | — | unit | `pytest tests/test_history_no_result_snapshots.py tests/test_local_filter_persistence.py -x` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_no_back_edges_core.py` — NEW; covers GUARD-01 (parametrized extracted-module registry, AST `iter_child_nodes`) **and** CONFIG-01 (`test_config_identity`).

*All other anchors use existing tests — no other new infrastructure needed.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| — | — | — | — |

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers the one MISSING reference (`test_no_back_edges_core.py`)
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s (quick smoke)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
