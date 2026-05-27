# Phase 96: Completing My Library feature - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-24
**Phase:** 96-completing-my-library-feature-add-features-and-fix-bugs
**Areas discussed:** Scope selection, D-F5 highlighting, D-F4 PDF extraction, D-F1 folder drill-down, NEW-1 button cleanup, NEW-2 LOCAL next/prev + View All

---

## Gray Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Scope selection | Which of D-F1..D-F5 ship in Phase 96, plus any new items | ✓ |
| D-F5 highlighting (P1) | Search-term highlighting broken for LOCAL hits | ✓ |
| D-F4 PDF extraction | One-word-per-line bug fix | ✓ |
| Feature additions (D-F1/F2/F3) | Folder drill-down, OCR, side-by-side rendering | ✓ |

**User's choice:** All four areas selected.

---

## Scope Selection

### Which deferred items ship in Phase 96?
| Option | Selected |
|--------|----------|
| D-F5 highlighting (P1) | ✓ |
| D-F4 PDF extraction | ✓ |
| D-F1 folder drill-down | ✓ |
| D-F2 + D-F3 (OCR + side-by-side) | |

**User's choice:** D-F5 + D-F4 + D-F1. D-F2 and D-F3 stay deferred to v7.15+.

### Are there bugs/features NOT in D-F1..D-F5?
**User's choice:** Yes — three new items described in free text:
1. **NEW-1:** ResultDialog has a redundant `צפה בדפדוף` button (wrong translation); the `עיין` (Browse) button already covers it. Remove.
2. **NEW-2:** Allow next/previous "image" navigation in LOCAL for both ResultDialog and Browse, plus "View All" (הכל) support in Browse for LOCAL.
3. **NEW-3:** Freestyle / on-the-fly fix bucket during phase smoke testing.

---

## NEW-2 Clarification: What does "next/previous image" mean for LOCAL?

### Navigation unit
| Option | Selected |
|--------|----------|
| Next/prev chunk within same file | |
| Next/prev page (PDF only) | |
| Next/prev FILE in same folder | |
| Skip — explain inline | ✓ |

**User's choice:** "page in PDF, chunk in txt/docx" — format-aware. PDFs use page boundaries; .docx/.txt use chunk boundaries.

### "View All" (הכל) for LOCAL in Browse
| Option | Selected |
|--------|----------|
| Show full file text (all chunks concatenated) | ✓ |
| Show all files in the same folder | |
| Skip — explain inline | |

**User's choice:** Full file text, all chunks concatenated in one continuous view.

---

## D-F5 Highlighting (P1)

### Fix approach
| Option | Selected |
|--------|----------|
| Normalize LOCAL hit dict shape | |
| Per-source branch in highlight pipeline | |
| Investigate first, defer the choice | ✓ |

**User's choice:** Investigate first. Pick normalize-vs-branch after scouting the highlight pipeline during planning.

### Regex-aware highlighting?
| Option | Selected |
|--------|----------|
| Yes — same two-phase highlight as Genizah | ✓ |
| No — simple substring highlight for LOCAL | |

**User's choice:** Same two-phase regex highlight as Genizah corpus. Consistency over ease.

---

## D-F4 PDF Extraction

### Fix strategy
| Option | Selected |
|--------|----------|
| Switch to get_text("text") globally | |
| Detect-then-fallback | ✓ |
| Audit-first (try all 4 modes on sample corpus) | |
| Investigate first, defer the choice | |

**User's choice:** Detect-then-fallback, with the caveat: "We'll try 2 but test it on different pdfs." Implementation = detect-then-fallback, validation = small sample of representative PDFs (not full audit-first sweep).

---

## D-F1 Folder Drill-down + NEW-1 + NEW-2

### Drill-down UX
| Option | Selected |
|--------|----------|
| Expandable tree (inline) | |
| Split panel (folder list left, files right) | |
| Modal dialog on folder click | |

**User's choice (free text):** Reuse the existing vertical split panel in MyLibraryTab. The bottom panel becomes a horizontal split: tree (with subfolders and files + checkboxes) on the left; the file-status display from scanning on the right.

### Per-file opt-out persistence
| Option | Selected |
|--------|----------|
| SQLite cache (sidecar or extend existing LOCAL cache) | |
| QSettings (in-app user state) | ✓ |
| Session-only (no persistence) | |

**User's choice:** QSettings. Note: "User may want each search to select another file. So perhaps 2?" — selections persist but are easy to toggle per-search.

### NEW-1: Remove redundant `צפה בדפדוף` button
| Option | Selected |
|--------|----------|
| Remove from ResultDialog for LOCAL hits only | ✓ |
| Remove for ALL hits | |
| Rename instead of remove | |

**User's choice:** "The button is only in LOCAL, should be removed." Remove from LOCAL only (Genizah hits don't have it).

### NEW-2: Where does next/prev navigation live?
| Option | Selected |
|--------|----------|
| ResultDialog | ✓ |
| Browse panel | ✓ |
| Search results table (compact row arrows) | |

**User's choice:** ResultDialog + Browse panel. Not the search results table.

---

## Continue or Wrap?

| Option | Selected |
|--------|----------|
| Ready for CONTEXT | ✓ |
| More questions | |

**User's choice:** Ready for CONTEXT.md.

---

## Claude's Discretion

- D-F5 normalize-vs-branch (locked AFTER planner/researcher scouts the highlight pipeline)
- D-F4 exact PyMuPDF fallback mode (`"text"` is the first attempt; planner can confirm)
- Tree widget exact PyQt6 class (`QTreeWidget` vs `QTreeView + model`)
- Tri-state checkbox styling and label conventions
- Page/chunk separator visual style in "View All"

## Deferred Ideas

- **D-F2 — PDF OCR** for scanned image-only PDFs. Defer to v7.15+ (own phase, OCR-engine choice as primary discussion).
- **D-F3 — Side-by-side PDF page rendering** (PDF image next to extracted text in Browse + ResultDialog). Defer to v7.15+.
