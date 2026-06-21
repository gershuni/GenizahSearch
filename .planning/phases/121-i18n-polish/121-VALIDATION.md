---
phase: 121
slug: i18n-polish
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-21
---

# Phase 121 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing) |
| **Config file** | `pyproject.toml` (`[tool.pytest.ini_options]`) |
| **Quick run command** | `python -m pytest tests/test_joins_lab_i18n.py -x -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -q` |
| **Estimated runtime** | ~5s for the i18n guard; render-smoke ~30s |

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/test_joins_lab_i18n.py -x -q`
- **After every plan wave:** Run `python -m pytest tests/test_joins_lab_i18n.py tests/render_smoke/ -q`
- **Before `/gsd:verify-work`:** i18n guard + render-smoke green AND HE-mode HUMAN-UAT checklist signed off
- **Max feedback latency:** ~5 seconds (guard); ~30 seconds (render-smoke)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| (filled during planning) | — | 0 | FND-07 (SC#3) | — | N/A | AST static guard | `pytest tests/test_joins_lab_i18n.py::test_no_raw_hebrew_literals -x` | ❌ W0 | ⬜ pending |
| (filled during planning) | — | 0 | FND-07 (SC#1) | — | N/A | AST coverage guard | `pytest tests/test_joins_lab_i18n.py::test_all_tr_keys_covered -x` | ❌ W0 | ⬜ pending |
| (filled during planning) | — | 0 | FND-07 (SC#1) | — | N/A | explicit-list check | `pytest tests/test_joins_lab_i18n.py::test_badge_strings_covered -x` | ❌ W0 | ⬜ pending |
| (filled during planning) | — | 0 | FND-07 (SC#1) | — | N/A | scoped host-key check | `pytest tests/test_joins_lab_i18n.py::test_entry_point_keys -x` | ❌ W0 | ⬜ pending |
| (filled during planning) | — | 1 | FND-07 (SC#2) | — | N/A | render-smoke RTL assertion | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -x -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_joins_lab_i18n.py` — new permanent guard (AST check a+b + explicit badge-string list + scoped entry-point key-check), adapted from `tests/test_join_workbench_i18n.py`
- [ ] HE keys for the 17 missing translations added to `genizah_translations.py::TRANSLATIONS` (14 literal keys + 3 badge strings) — must land BEFORE the coverage guard turns green
- [ ] TRANSLATIONS drift fix: `'Open in Joins Lab'` → `'פתח במעבדת הצירופים'`
- [ ] render-smoke RTL test requires `set_language('he')` before `user.open('/joins-lab')` (existing `tests/render_smoke/` harness)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Visual RTL correctness across every Joins Lab surface (computed-height collapse, clipping/overlap, mirroring, transcription right-alignment, LTR prev/next counter) | FND-07 (SC#2) | Headless render-smoke cannot see computed-height collapse or visual mirroring (memory `feedback_nicegui_render_smoke_gap`); Phases 119/120 shipped "green" then accumulated RTL fixes only in live HE-mode UAT | Planner authors a concrete per-surface HE-mode UAT checklist (D-01b); Hillel runs `python -m web.main`, switches to HE, walks each surface |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (the new guard + the 17 HE keys)
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
