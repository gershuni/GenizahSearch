# Browse Integration & Evidence Highlighting

Validated in sketch 002 by reproducing the real `/browse` page frame from source measurements and
running the highlighting against real HTR page text and real stored offsets.

## Design Decisions

**Where the panel attaches.** The entry control goes in `browse.py`'s **second toolbar row, beside
Joins** — the row already holding Edit Transcription, Comments, Notes, Add to Reading Desk. The panel
**body** renders full-width **beneath** the two panes, because at `flex: 0 0 50%` × 60vh neither pane
has room, and that is also the only place the even-pane layout fits.

**Mechanism: a fifth `enrichment_refs` placeholder.** An empty container created during the
synchronous render, filled by `update_enrichment_sections()` (`browse_enrichment.py:488`) once Phase B
completes. Four sections already work this way — `pgp_link_container`, `version_container`,
`joins_container`, `bib_catalog_container`. This is reuse, not new plumbing.

**Three obligations of that seam:**
1. Re-check the generation token after **every** await (`browse_enrichment.py:319, 452, 458`) or fast
   page navigation paints a stale panel over the wrong folio.
2. Bind `page_client` at render time — `run.io_bound` silently degrades `safe_user_*` to `{}`, and
   `ensure_future` makes `ui.context.*` raise.
3. Implement the D-13 envelope. Today's wrappers collapse timeout, overload, absent-sidecar and
   genuine-zero all to `[]`. Only ~17% of manuscripts carry claims, so hiding the entry control on a
   **true** zero is correct; hiding it during an **outage** is not — that state needs a visible retry.

## The real page's measurements (reproduce, don't invent)

| Element | Source |
|---|---|
| app header, 64px, `--bg-header` gradient | `web/main.py` |
| compact metadata header, `linear-gradient(135deg,#15803d,#166534)` | `browse.py:1682` |
| main content card, `min-height: 60vh` | `browse.py:2568` |
| two toolbar rows | `browse.py:3760-3960` |
| side-by-side flex, `gap: 16px` | `browse.py:3978` |
| left image pane, `flex: 0 0 50%` | `browse.py:3984` |
| image box `#1a1a1a`, `height: calc(60vh - 100px)` | `browse.py:4046` |
| version selector row, `items-center p-2 border-b` | `browse.py:4232` |
| text scroll area, `calc(60vh - 80px)`, `padding: 20px` | `browse.py:4184` |
| line-numbered RTL grid | `web/components/typography.py:66-215` |

## Evidence highlighting — the correct algorithm

**Two defects in D-12 as written, both verified against the asset.**

### 1. Stored offsets are NOT raw-text indices

They index **`norm_stream`**: NFC-normalize → fold final letters → keep only U+05D0–U+05EA, dropping
spaces, newlines, nikud, cantillation, punctuation, digits and Latin
(`scripts/build_discovery_sidecar.py:532-552`).

Evidence: `span_end − span_start == matched_letters` in 11 of 14 sampled rows (the 3 exceptions are
exactly the multi-span rows, where only the largest span is stored but `matched_letters` sums all). On
the `clean` manuscript the span `0–5259` equals the normalized letter count while the raw text is
6,670 characters.

So rendering requires a **normalized-index → raw-index map** built at render time:

```js
const HEB_MIN = 0x05D0, HEB_MAX = 0x05EA;
const FINAL_FOLD = {0x05DA:0x05DB, 0x05DD:0x05DE, 0x05DF:0x05E0, 0x05E3:0x05E4, 0x05E5:0x05E6};

function normMap(raw) {                       // map[i] = raw index of the i-th normalized letter
  const nf = raw.normalize('NFC'), map = [];
  for (let i = 0; i < nf.length; i++) {
    let code = nf.codePointAt(i);
    if (FINAL_FOLD[code] !== undefined) code = FINAL_FOLD[code];
    if (code >= HEB_MIN && code <= HEB_MAX) map.push(i);
  }
  return { nf, map };
}
function spanToRaw(raw, a, b) {               // normalized [a,b) -> raw [rs,re)
  const { nf, map } = normMap(raw);
  if (!map.length || a >= map.length) return null;
  return { nf, rs: map[a], re: (b - 1 < map.length ? map[b-1] + 1 : map[map.length-1] + 1) };
}
```

Verified exact on all 13 manuscripts. Example translations: `clean` 0–5259 → raw 0–6668;
`reviewed` 638–2374 → raw **809–3026**; `variety-c` 1114–2071 → raw 1405–2660.

D-12 taken literally ends Moss. V,374's highlight at character 2374 instead of 3026 — **652
characters early**.

### 2. The highlight must be clipped per line

`render_line_numbered_html` splits highlight HTML on `\n` and drops each line into its own grid
`<div>` (`typography.py:139, 170`). A span crossing a newline leaves an unclosed `<span>` in one row,
**no highlight in the middle rows**, and an orphan `</span>` at the end; browsers silently auto-close
and discard. Every sampled span crosses newlines (152, 27, 17, 55, 6) — this is the normal case.

Measured on Moss. V,374 (148 grid rows): mapped **and** per-line clipped → **72 rows highlighted, 0
unbalanced**. Either broken approach → **1 row**, 2 unbalanced.

```js
function highlightSpan(raw, a, b) {           // the correct combination
  const m = spanToRaw(raw, a, b); if (!m) return escape(raw);
  const { nf, rs, re } = m;
  const out = []; let pos = 0;
  for (const line of nf.split('\n')) {        // clip to each line, close+reopen at every newline
    const ls = pos, le = pos + line.length;
    const s = Math.max(rs, ls), e = Math.min(re, le);
    out.push(s < e
      ? escape(line.slice(0, s-ls)) + '<span class="discovery-match">' +
        escape(line.slice(s-ls, e-ls)) + '</span>' + escape(line.slice(e-ls))
      : escape(line));
    pos = le + 1;                             // +1 for the '\n'
  }
  return out.join('\n');
}
```

`highlight_text` (`browse.py:1577-1601`) is **not reusable** — it escapes the whole string first, then
regex-substitutes search *terms*, so escaping shifts every index.

### Two collisions in the same render slot

- **Search terms vs discovery spans.** `render_text_content` computes
  `highlight_html = highlight_text(text) if state.highlight_terms else None` (`browse.py:4186`).
  Arriving from a search, both want that one parameter. **One renderer must emit both** — term marks
  nested inside the span mark. Two renderers cannot share the slot.
- **The version selector invalidates offsets.** `handle_version_change` (`browse.py:4207`) swaps the
  text and re-renders. Offsets belong to one HTR snapshot; switching to an FGP transcription or a
  translation makes them address different characters. The highlight must be **dropped**, not
  re-validated. D-12 covers snapshot drift over time but not the user switching source.

### One honesty problem

Only the largest span is stored (9,549 shipped direct rows have several). Moss. V,374 states **2,809**
matched letters while only **1,736** are highlightable. Either qualify the label or have the evidence
view say it is showing one span of several.

## What to Avoid

- Slicing raw page text at stored offsets (wrong units).
- Emitting a single `<span>` across newlines into the line-numbered grid.
- Reusing `highlight_text` for offsets.
- Letting an outage render as an empty panel or a hidden entry control.

## Origin

Sketch 002, accepted. Source in `sources/002-panel-embedded-in-browse/` (shares
`../001-discovery-panel-architecture/data.js`).
