---
sketch: 002
name: panel-embedded-in-browse
question: "How does the discovery panel sit inside the real /browse page, and how does offset highlighting actually work on its text pane?"
winner: null
tags: [discovery, panel, phase-136, browse, integration, highlighting, d-12, panel-01, panel-03]
---

# Sketch 002: The panel embedded in the real `/browse` page

## Design Question

Sketch 001 asked what the panel should look like. This one asks the two questions that only make
sense *in situ*: where does it attach on the real page, and how does the evidence highlighting work
on the text pane that already exists?

## How to View

```
start .planning\sketches\002-panel-embedded-in-browse\index.html
```

Opens on **Moss. V,374**, which has the longest span and shows the highlighting problem most clearly.

| Control | What it does |
|---|---|
| **Manuscript** | the same 13 real manuscripts as sketch 001 (shared `data.js`) |
| **Service state** | ready · loading · outage · sidecar absent · **genuine zero** — watch the entry control appear and disappear |
| **Highlight in the text pane** | **correct** vs the two broken approaches vs off |
| **search terms active** | simulates arriving from a search, so both highlight kinds compete |
| version dropdown *(in the page)* | switch source and watch the highlight get dropped |
| **Design notes** | the evidence behind each finding, with file:line |
| 375 / 820 / Full · theme | mobile + the three production themes |

## What is reproduced, and from where

The frame is not decorative — every measurement is taken from the real page:

| Element | Source |
|---|---|
| app header, 64px, `--bg-header` gradient | `web/main.py` |
| compact metadata header, gradient `#15803d → #166534` | `browse.py:1682` |
| main content card, `min-height: 60vh` | `browse.py:2568` |
| two toolbar rows (NLI/Bibliography/Catalog · page nav · Edit/Comments/Notes/Joins/Reading Desk) | `browse.py:3760-3960` |
| side-by-side flex, `gap: 16px` | `browse.py:3978` |
| left image pane, `flex: 0 0 50%` | `browse.py:3984` |
| image box `#1a1a1a`, `height: calc(60vh - 100px)` | `browse.py:4046` |
| version selector row, `items-center p-2 border-b` | `browse.py:4232` |
| text scroll area, `calc(60vh - 80px)`, `padding: 20px` | `browse.py:4184` |
| the line-numbered RTL grid the highlight must survive | `typography.py:66-215` |

## Answer 1 — how it embeds

The entry control goes in **toolbar row 2, beside Joins** — the row that already holds Edit
Transcription, Comments, Notes and Add to Reading Desk. Mechanically the panel is a **fifth
`enrichment_refs` placeholder**: an empty container created during the synchronous render and filled
by `update_enrichment_sections()` (`browse_enrichment.py:488`) once Phase B finishes. Four sections
already work exactly this way (`pgp_link_container`, `version_container`, `joins_container`,
`bib_catalog_container`), so this is reuse, not new plumbing.

The panel **body** renders full-width beneath the two panes, because at `flex: 0 0 50%` and 60vh
neither pane has room for it — and that is also the only place sketch 001's even-pane layout fits.

Three obligations the seam imposes:

1. **Re-check the generation token after every await** (`browse_enrichment.py:319, 452, 458`) or fast
   page navigation paints a stale panel over the wrong folio.
2. **Bind `page_client` at render time** — `run.io_bound` silently degrades `safe_user_*` to `{}` and
   `ensure_future` makes `ui.context.*` raise.
3. **The D-13 envelope.** Today's wrappers collapse timeout, overload, absent-sidecar and genuine-zero
   all to `[]`. Only ~17% of manuscripts carry claims, so hiding the control on a *true* zero is
   right — hiding it during an outage is not. Cycle the state selector: `zero` removes the control
   entirely, `outage` shows a visible retry.

## Answer 2 — how highlighting works, and two defects in D-12

### Defect 1: the offsets are not raw-text indices

**D-12 says: "slice the RAW page text at the stored offsets, escape each part, wrap the middle."**
That highlights the wrong characters. Verified against the asset:

- `span_end − span_start == matched_letters` in **11 of 14** sampled rows; the 3 exceptions are
  exactly the multi-span rows, where only the largest span is stored but `matched_letters` sums all.
- On the `clean` manuscript the span is `0–5259`, the normalized letter count is **5259**, and the raw
  HTR text is **6670** characters — the span covers the *entire* normalized stream.

So offsets index **`norm_stream`**: NFC-normalize → fold final letters → keep **only** U+05D0–U+05EA,
dropping spaces, newlines, nikud, cantillation, punctuation, digits and Latin
(`build_discovery_sidecar.py:532-552`). Rendering therefore needs a **normalized-index → raw-index
map** built at render time, then the span translated through it. Measured translations:

| Manuscript | stored span (letters) | true raw range | letters, verified |
|---|---|---|---|
| clean | 0–5259 | 0–6668 | 5259 exact |
| reviewed (Moss. V,374) | 638–2374 | **809–3026** | 1736 exact |
| variety-c | 1114–2071 | 1405–2660 | 957 exact |

D-12 taken literally ends Moss. V,374's highlight at character 2374 when it should end at **3026** —
**off by 652 characters**.

### Defect 2: the highlight must be clipped per line

`render_line_numbered_html` splits the highlight HTML on `\n` and drops each line into its own grid
`<div>` (`typography.py:139, 170`), because of the line-number gutter. A span crossing a newline
leaves an unclosed `<span>` in one row, **no highlight in the middle rows**, and an orphan `</span>`
at the end; the browser silently auto-closes and discards. **Every span in the sample crosses
newlines** (152, 27, 17, 55, 6), so this is the normal case.

Measured on Moss. V,374 (148 grid rows):

| Approach | rows highlighted | unbalanced |
|---|---|---|
| **correct** — mapped + clipped per line | **72** | 0 |
| mapped, not clipped | 1 | 2 |
| D-12 literal (raw slice) | 1 | 2 |

The fix is small: clip the translated span against each line, closing and reopening the `<span>` at
every newline. But it has to be *in the plan* — done naively it silently shows one line of a
1,736-letter match and raises no error, which defeats PANEL-03's entire purpose.

### Two further collisions in the same slot

- **Search terms compete with discovery spans.** `render_text_content` computes
  `highlight_html = highlight_text(text) if state.highlight_terms else None` (`browse.py:4186`).
  Arrive from a search and both want that one parameter. Toggle "search terms active": here the amber
  term marks nest *inside* the green span, which means one renderer must emit both — two renderers
  cannot share the slot.
- **The version selector invalidates the offsets.** `handle_version_change` (`browse.py:4207`) swaps
  the text and re-renders. Offsets were computed against one HTR snapshot; switch to an FGP
  transcription or a translation and they address different characters. D-12 covers snapshot-hash
  drift *over time* but not the user actively switching source. Change the dropdown in the sketch —
  the highlight is dropped, not re-validated.

### One honesty problem

Only the largest span is stored (9,549 shipped direct rows have several). On Moss. V,374 the row
states **2,809** matched letters but only **1,736** are highlightable. Either the label needs
qualifying or the evidence view must say it is showing one span of several.

## Automated checks

`node` smoke over 13 manuscripts × 5 service states × 2 languages × 4 highlight modes —
**540 assertions, all pass**:

- the normalized→raw mapping is **exact** for all 13 manuscripts (the letters inside the mapped raw
  range equal the expected normalized slice, character for character)
- correct mode: 0 unbalanced rows; both broken modes: unbalanced, as claimed
- the D-12 raw slice does **not** coincidentally equal the mapped range
- no `precision`, no raw precision figure, no confidence interval, no review badge (EN or HE),
  no "copy of" / "quotes" / "witness of", no stored `claim_type` key in rendered HTML
- a **true zero removes the entry control**; an **outage shows a visible retry** (D-13)

## What this means for planning

Gate 6 (PANEL-03) is bigger than D-12 describes. It needs: a normalized-index → raw-index mapper, a
per-line span clipper, a precedence rule for search-term vs discovery highlighting, invalidation on
version change, and a decision about the matched-letters label on multi-span rows. All cheap
individually; none currently written down.
