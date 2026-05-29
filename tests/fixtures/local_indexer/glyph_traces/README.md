# Glyph-trace fixtures — Phase 102-01 (LOCAL PDF RTL reconstruction)

Hand-authored, geometry-correct glyph traces that pin the pure helpers in
`shared/local_indexer_rtl.py`. A text-only `.expected.txt` cannot pin a
bbox-dependent bug (Codex LOW-11), so these fixtures carry full per-glyph
geometry. bboxes are **hand-authored PDF points** (x increases left→right);
they are not extracted from a real PDF, so they are stable and self-documenting.

## Schema

Each file is a JSON object:

```jsonc
{
  "description": "what this fixture exercises + provenance",
  "expected_despaced": "string after despace_line_to_word_units join",
  "expected_reordered": "string after reorder_word_units_rtl",
  "lines": [ <line dict>, ... ]
}
```

A **line dict** mirrors PyMuPDF `rawdict`:

```jsonc
{ "bbox": [x0, y0, x1, y1],
  "spans": [ { "font": "...", "size": 11.0, "chars": [ <glyph>, ... ] } ] }
```

A **glyph record** carries the RICHER contract (REVIEWS HIGH-4/HIGH-5) — a bare
`{c, bbox}` glyph is insufficient because the de-space hysteresis needs
span/font boundaries and the reorder needs original rawdict reading order:

```jsonc
{ "c": "מ",
  "bbox": [x0, y0, x1, y1],     // PDF points
  "font": "David",
  "size": 11.0,
  "span_id": 0,                 // id of the originating rawdict span
  "original_order": 0 }         // index in ORIGINAL rawdict reading order
```

**The `chars` array is NOT pre-sorted to logical order.** `original_order`
records the rawdict emission order; the helpers must reconstruct logical reading
order from bbox + original_order, never assume the array is already logical.

## Center-x convention and the M3 visual-LTR case

`center-x = (bbox[0] + bbox[2]) / 2`. For a properly typeset RTL word the
**first** consonant read (right-to-left) sits at the **highest** center-x.

`intra_word_visual_ltr.json` is the critical M3 case: the word **שלום** is
emitted in visual-LTR order, so:

- the glyph with the **smallest center-x is the logically-LAST consonant ם**,
- the glyph with the **largest center-x is the logically-FIRST consonant ש**.

Applying the **descending-center-x** intra-unit sort recovers the correct R→L
order **"שלום"** (not the ascending-x **"םולש"**). The line-2 nikud variant
places a holam (U+05B9) over ו; it must stay attached to ו after the letter
ordering.

## Fixture inventory

| file | exercises |
|------|-----------|
| `letter_spaced_line.json` | F-D de-space; emission == reading order; reorder is a no-op |
| `letter_spaced_reversed_line.json` | F-E de-space BEFORE reorder; emission reversed, reorder fixes by x |
| `rtl_running_header.json` | F-F running-header word reversal |
| `ltr_latin_line.json` | LTR no-regression pin (every helper passes through) |
| `undersplit_line.json` | Codex MED-6: mid-gap split corroborated by space + span boundary |
| `overmerge_line.json` | Codex MED-6: split driven by span/font boundary, not the 1.8× threshold |
| `intra_word_visual_ltr.json` | Codex M3: visual-LTR word → descending-x letter order; nikud stays on base |
