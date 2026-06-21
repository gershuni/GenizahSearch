# Phase 121: i18n Polish - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-21
**Phase:** 121-i18n-polish
**Areas discussed:** RTL verification method, i18n guard form, Audit & coverage scope, HE glossary consistency

---

## RTL verification method (SC#2)

| Option | Description | Selected |
|--------|-------------|----------|
| Both: render-smoke + HE-UAT | Automated dir/text-align/flex-row-reverse assertions in CI + a structured HE-mode live-UAT checklist across all surfaces | ✓ |
| Live HE-UAT checklist only | Skip automated assertions; rely on a thorough HE-mode UAT pass | |
| Automated render-smoke only | CI-enforced dir/text-align checks, no formal UAT pass | |

**User's choice:** Both: render-smoke + HE-UAT
**Notes:** Grounded in memory `feedback_nicegui_render_smoke_gap` + `reference_nicegui_flex_height_css` and the 119/120 history — those phases shipped "green" then needed RTL fixes only found in live HE-mode UAT. Render-smoke is a regression guard for structural attributes; the HE-UAT checklist is the real acceptance artifact for computed-height/mirroring bugs.

---

## i18n guard form (SC#3)

| Option | Description | Selected |
|--------|-------------|----------|
| Permanent CI guard, both checks | Adapt desktop's test_join_workbench_i18n.py: (a) no raw Hebrew literal outside tr() AND (b) every tr() key resolves in TRANSLATIONS; allowlist for bilingual-safe tokens | ✓ |
| Permanent guard, leak-only | Permanent guard asserts only no-raw-Hebrew-leak; coverage via a one-time audit | |
| One-time audit script | Run an audit now, no permanent CI guard | |

**User's choice:** Permanent CI guard, both checks
**Notes:** Dual-direction failure coverage — no Hebrew→EN reverse-leak (raw-literal check) and no English→HE gap (every key resolves). Nuance captured in CONTEXT D-03: lang-gated inline-bilingual English-else branches get HE keys ADDED (harmless) rather than allowlist exceptions, keeping check (b) total.

---

## Audit & coverage scope (SC#3)

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated files + entry-point keys | 6 components + joins_lab.py + storage (full scan) PLUS scoped key-checks on Joins-Lab strings in /search, /browse, /lists (D-19), /puzzle (ACT-02), export (D-06) | ✓ |
| Dedicated Joins-Lab files only | Page + 6 components + storage only; entry-point touchpoints out of scope | |

**User's choice:** Dedicated files + entry-point keys
**Notes:** Mirrors the desktop guard's two-part structure (full scan of new files + scoped host-key check on huge shared files). joins_executor.py excluded (0 tr(), no user-facing strings); shared/joins_lab.py to be confirmed for any untranslated label text.

---

## HE glossary consistency (SC#1)

| Option | Description | Selected |
|--------|-------------|----------|
| Reconcile against desktop | Align web HE terms with the UAT-approved desktop Joins Lab glossary via the shared TRANSLATIONS dict; fix drift | ✓ |
| Accept existing keys | Trust existing HE keys; only fill genuine gaps, no cross-app pass | |

**User's choice:** Reconcile against desktop
**Notes:** Source of truth = desktop/join_workbench.py + its existing TRANSLATIONS keys. Anchor terms: מעבדת צירופים (Joins Lab), anchor/candidate/Compare/look-alike/triage Yes·Maybe·No + builder modifier meanings; reuse the glossary anchors from `reference_i18n_audit_method`.

---

## Claude's Discretion

- Exact render-smoke RTL assertions per surface and the precise HE-UAT checklist line items.
- AST scanner structure + the final no-raw-Hebrew allowlist entries (reuse desktop scanner).
- The final enumerated list of scoped entry-point keys.
- Filling any newly-discovered missing strings with HE keys (default; harmless per D-03).
- Research flag: confirm how the web app switches language (reload vs live re-render) for SC#1's
  "without page reload" clause — treat any reload requirement as pre-existing app-wide behavior.

## Deferred Ideas

None — discussion stayed within phase scope. The one genuine web todo match
(search-results-survive-navigation) was already folded into Phase 120; the other
`todo.match-phase 121` hits are spurious keyword coincidences reviewed-and-rejected in Phases 117–120.
