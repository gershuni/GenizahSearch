---
id: SEED-003
status: dormant
planted: 2026-05-29
planted_during: Spike 001 (PDF extraction reorder) — Phase 102 scoping
trigger_when: user demand for searching scanned/image-only PDFs in My Library, OR an OCR/accessibility milestone, OR corrupt-text-layer (F-G) files become a recurring complaint
scope: Large
related: D-F2, F-G (Spike 001), Phase 102
---

# SEED-003: Optional opt-in OCR extension for image-only / corrupt-text-layer PDFs in My Library

## Why This Matters

Spike 001's corpus profiler (one PDF per folder across Hillel's ~18K-file `ספרים` library)
found that **a large share of the real library is IMAGE-ONLY scans with no text layer** —
תפילה (siddurim), מדרש (Albeck), ערוך, פילון, מילון בן יהודה, אנציקלופדיות, בית שני,
תרגומים, ספריה, and more. The current My Library indexer indexes **nothing** for these
(`extraction_status='no_text_layer'`). Additionally, some PDFs have a **corrupt text layer**
(F-G — e.g. `Israeli_Vilna_shabbat_part_2.pdf`, bad/missing ToUnicode cmap) that no
reorder/de-spacing can rescue; OCR is the only real fix for those too.

So OCR would unlock search over a big, currently-invisible chunk of user content.

## Why It's a SEED (not in Phase 102)

Hillel's explicit constraint (2026-05-29): OCR is **heavy**, and **most users won't need it**.
Design tenets if/when built:
- **Opt-in, on-demand** — never on the core indexing hot path; common users unaffected
  (no startup cost, no mandatory dependency).
- **Optional/separate install** — Tesseract + Hebrew/Judeo-Arabic traineddata should NOT
  become a hard dependency of the desktop app. Consider an optional extension/download or
  a "detected Tesseract on PATH → offer OCR" capability.
- **User accepts the tradeoff** — time + outcome quality are the user's call per file/folder.
- **Escape hatch already exists** — power users can pre-OCR PDFs into a searchable text
  layer with off-the-shelf tools; the rewritten Phase 102 extractor then indexes them
  normally. Document this in Help as the no-build workaround.

## When to Surface

Present during `/gsd-new-milestone` when scope matches:
- User demand to search scanned books already in their My Library
- An OCR / accessibility / "index everything" milestone
- F-G corrupt-text-layer files become a recurring complaint
- A desktop dependency/packaging milestone (decide optional-extension delivery)

## Scope Estimate

**Large** — likely its own multi-phase effort: (1) Tesseract integration + optional-install
UX + language data (Hebrew, Judeo-Arabic in Hebrew script), (2) page render → OCR → text
pipeline wired into `LocalIndexerWorker` as an opt-in per-file/folder action, (3) quality
handling (deskew, confidence thresholds, mixed Heb/Latin), (4) progress/cancel UX for the
slow path. Reuses Phase 99's PDF page-render plumbing.

## Investigation Needed

1. **Engine choice** — Tesseract (offline, free, optional install) vs cloud OCR APIs
   (better Hebrew accuracy, but network + privacy + cost). Hillel leans local/optional.
2. **Quality on old scans** — accuracy on the actual library (faded photostats, נספח tables,
   vocalized text). Set expectations; OCR'd text is noisy and should be flagged as such.
3. **Delivery mechanism** — how to ship Tesseract+traineddata as an *optional* component
   without bloating the base installer (separate download? detect-on-PATH? plugin dir?).
4. **F-G overlap** — auto-route corrupt-text-layer PDFs (detected in Phase 102) into the
   OCR path when the extension is present.
5. **Performance** — time per page; must be background + cancellable (Phase 97.3 patterns).

## Foundations already in place

- Phase 99 PDF page renderer (page → image) — reuse for the OCR render step.
- Spike 001 profiler (`profile_corpus.py`) can classify which library files are image-only.
- Phase 102 will emit F-G corrupt-encoding detection — the trigger signal for per-file OCR.
