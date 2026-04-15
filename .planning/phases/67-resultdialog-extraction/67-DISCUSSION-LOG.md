# Phase 67: ResultDialog Extraction - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-15
**Phase:** 67-resultdialog-extraction
**Areas discussed:** Parent Coupling Strategy, Shared Helpers Placement, Helper Class Co-location, Verification Approach

---

## Parent Coupling Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| A. Leave `self.parent()` as-is | Minimal change. Qt parent-link already points to GenizahGUI. No code edits beyond moving the class. Coupling stays hidden. | |
| B. Name the connection (`self._app = parent`) | Same behavior, mechanical rename of all `self.parent()` → `self._app` inside ResultDialog. Coupling is explicit and greppable. | ✓ |
| C. Build a Protocol/ABC contract | Define a formal `ResultDialogHost` interface. Cleanest long-term but significantly bigger job. | |
| D. Hybrid (A now, Protocol later) | Keep `self.parent()` now; plan ADR for Phase 71. | |

**User's choice:** B (via external Codex review)
**Notes:** Codex rationale — keeps Phase 67 low-risk, makes coupling explicit and searchable, avoids hidden fragility of repeated `self.parent()` lookups. Protocol/ABC explicitly deferred to Phase 71.

---

## Shared Helpers Placement

| Option | Description | Selected |
|--------|-------------|----------|
| A. Leave in `genizah_app.py` | New dialog file imports from `genizah_app`. Risk: circular import tangles. | |
| B. Move to neutral module (`desktop/widgets.py` or `desktop/helpers.py`) | Both big file and new dialog file import from this neutral spot. Cleanest pattern. | ✓ |
| C. Split — function moves, widget stays | Pragmatic but inconsistent. | |

**User's choice:** B (neutral module, name `desktop/widgets.py` or `desktop/helpers.py` — exact name is Claude's discretion)
**Notes:** Codex rationale — `ActionsHoverWidget` (20+ callers) and `_format_add_to_list_label` (7 callers) have many non-ResultDialog callers. Leaving them in `genizah_app.py` would create the exact import-direction problem we want to avoid. Setting the pattern now benefits Phases 68-71.

---

## Helper Class Co-location

| Option | Description | Selected |
|--------|-------------|----------|
| A. Only `ResultDialog` moves | Everything else handled per Gray Area #2. | |
| B. `ResultDialog` + any ResultDialog-exclusive helpers | Mechanical check during research; shared helpers follow Gray Area 2. | ✓ |
| C. Claude discretion | | |

**User's choice:** B
**Notes:** Codex rationale — keeps the extracted module coherent. Shared helpers go to the neutral module per D-03/D-04, not duplicated into `result_dialog.py`.

---

## Verification Approach

| Option | Description | Selected |
|--------|-------------|----------|
| A. pytest only | 1,067 tests, fast, mostly good enough. | |
| B. pytest + import smoke | Catches import breakage pytest may not exercise. | |
| C. pytest + manual desktop smoke checklist | User/executor eyeballs the desktop app. | |
| D. All of the above | Thorough — pytest + import smoke + minimal manual smoke. | ✓ |

**User's choice:** D
**Notes:** Codex specifics —
- Import smoke should cover THREE lines: `from desktop.result_dialog import ResultDialog`, `from desktop.widgets import ActionsHoverWidget, _format_add_to_list_label`, `from genizah_app import GenizahGUI`.
- Manual smoke kept minimal: app start, run a search, open one result dialog, navigate once, close it. Not a formal checklist — two minutes of eyeballing.
- Extra verification worth it because this is the first extraction and establishes the pattern for Phases 68-71.

---

## Claude's Discretion

- Exact name of the neutral shared module (`desktop/widgets.py` vs `desktop/helpers.py`) — whichever reads better after research.
- Commit granularity — one big commit vs several (create package, move helpers, move ResultDialog, rename parent, re-import). Whatever keeps each commit pytest-green.
- Exact docstring wording for new modules.

## Deferred Ideas

- **Protocol/ABC for the ResultDialog ↔ GenizahGUI interface** → Phase 71 (GenizahGUI Consolidation).
- **Doc path updates** (CODE_INDEX.md etc. pointing to `genizah_app.py:6045`) → Phase 76 (Documentation Close).
- **`desktop/widgets.py` growing as shared UI helpers are surfaced** → Phases 68-70.

## Process Notes

- User preference: ask questions in plain English; defer technical specifics to external AI (Codex / Gemini CLIs).
- Codex (external AI) provided the specific answers; user relayed them verbatim.
- No `AskUserQuestion` tool was available in this session — discussion was conducted as a plain-text numbered-list turn (text mode).
