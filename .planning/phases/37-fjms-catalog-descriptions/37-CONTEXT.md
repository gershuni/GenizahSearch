# Phase 37: FJMS Catalog Descriptions - Context

**Gathered:** 2026-02-17
**Status:** Ready for planning

<domain>
## Phase Boundary

Surface FJMS scholarly catalog descriptions (TextualFrame data from the existing catalog table in fjms_enrichment.db) via a dedicated button and dialog in both web and desktop apps. Data already exists — no new export needed. The button appears in browse metadata, search results, and the desktop Result Dialog.

</domain>

<decisions>
## Implementation Decisions

### Data Source
- Query directly from existing `catalog` table's `TextualFrameEng`/`TextualFrameHeb` columns — no new `full_texts` table needed
- ~96K records with TextualFrame data, ~500K total catalog rows
- Also display `Title`/`TitleHeb`, `AuthorText`, `CopyDate`, `CopyPlace` when available

### Description Presentation
- **Dialog/modal popup** — matches existing Bibliography FJMS pattern
- **Follow app language** — show TextualFrameHeb when app is Hebrew, TextualFrameEng when English
- **Language fallback** — if preferred language version is empty, fall back to the other language
- **Markup rendering** — preserve `[$...$]` and `@` markup data but render it nicely (styled/emphasized text), not raw
- **Title as heading** — show Title/TitleHeb as a heading above the description when present
- **Author field** — show AuthorText when available (after title, before frame text)
- **Extra metadata** — show CopyDate and CopyPlace in the dialog when available
- **Desktop parity** — QDialog popup in desktop app, same content layout as web modal

### Button Design & Placement
- **Label:** "Catalog Records (N)" in English / "מידע קטלוגי (N)" in Hebrew, with entry count
- **Icon:** `description` (Material doc icon) — distinct from bibliography's `menu_book`
- **Style:** `outline dense` — matches existing Bibliography FJMS/Ktiv buttons
- **Web browse:** In the bibliography buttons row (near Bibliography FJMS / Bibliography Ktiv)
- **Web search results:** Button in the metadata section of search result cards
- **Desktop browse:** Same row as bibliography buttons (ext_info_row)
- **Desktop Result Dialog:** Button follows bibliography button pattern (visible when data exists)
- **Empty state:** Button always visible but disabled with (0) count when no records — consistent across all locations

### Attribution Display
- **Source header per group** — group entries by SourceName, show source name once as a section header
- **Source language** — follow app language (SourceNameHeb in Hebrew mode, SourceName in English)
- **Author placement** — after title, before frame text: Title → Author → Description

### Multiple Descriptions
- **Show all, scrollable** — dialog scrolls, no truncation or cap regardless of entry count
- **No deduplication** — show all entries from all sources as-is, even if overlapping
- **Dialog title** — "Catalog Records — {shelfmark}" (count only on the button, not in dialog title)

### Claude's Discretion
- Truncation strategy for very long individual descriptions (up to 2,688 chars)
- Handling of identical TextualFrameEng/TextualFrameHeb content (dedup display or just show chosen language)
- Exact spacing, typography, and scroll behavior in the dialog

</decisions>

<specifics>
## Specific Ideas

- The `[$...$]` markup "probably used to smart search" — preserve it in the data, render it nicely but don't strip it
- Desktop and web should have feature parity for this feature across all surfaces (browse, search results, Result Dialog)
- Disabled-when-empty pattern everywhere for consistency — researchers see the capability exists even when no data

</specifics>

<deferred>
## Deferred Ideas

- **Clickable [$reference$] links** — clicking a `[$Sifra$]` or `[$Talmud Yerushalmi$]` reference to trigger a search — future phase
- **FJMS FTS5 search of descriptions** — already tracked as FJMS-04 in requirements (future)

</deferred>

---

*Phase: 37-fjms-catalog-descriptions*
*Context gathered: 2026-02-17*
