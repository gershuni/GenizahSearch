# Quick Task 260319-mc4: Search UX Improvements - Context

**Gathered:** 2026-03-19
**Status:** Ready for planning (revised after GPT Codex review + UX discussion)

<domain>
## Task Boundary

Major search UX overhaul based on PostHog data (most users search) and UX expert feedback:
1. Hero search bar on the home page
2. Replace splitter+viewer with inline accordion on search results
3. Simplify result cards (move actions to expansion)
4. Compact search chrome (merge status into results header)
5. Citation bar collapse to compact line

</domain>

<decisions>
## Implementation Decisions

### Hero Search Bar
- Large, prominent search input inside the hero section of home.py
- On Enter, navigate to `/search?q=...` with URL-encoded query (use encodeURIComponent or quote())
- Must also fix the existing header quick search to URL-encode (main.py:239)

### Replace Splitter with Inline Accordion
- **Remove the splitter entirely** — no more split-pane viewer
- Results list gets 100% width (full column, no splitter)
- Clicking a result **expands an inline accordion below that result card**
- Clicking the same result again → collapses it
- Clicking a different result → collapses previous, expands new one
- Only one expansion open at a time
- Advanced View dialog stays for deep-dive (full text, image viewer with zoom, folios, etc.)

### Expansion Content (Side-by-Side Layout)
- **Left**: Manuscript image thumbnail (~200px), fetched from NLI crossref IIIF
- **Right**: Larger formatted snippet with highlighting + action buttons
- Action buttons in expansion: Browse, Advanced View, Find Parallels, Add to List, Exclude
- If no image available, snippet gets full width

### Simplify Result Cards
- Cards show: badges (index, library, PGP, domain, printed) + shelfmark + title + snippet
- **Remove from cards**: action buttons (Browse, Advanced View, Star, Exclude), catalog records button
- These actions move to the inline expansion
- Net effect: cards become scannable, actions appear on demand

### Compact Search Chrome
- Remove the separate "Search completed in X — N Results" status bar
- Merge into results header as a compact badge: "15 Results · 0.00s"
- The results header row (with checkboxes, filter buttons, view toggles) stays

### Citation Bar
- After ~10 seconds, collapse the full citation footer to a compact single line
- Compact line shows: "Cite: MiDRASH (Stoekl Ben Ezra et al., 2025) — [Copy]"
- Copy button copies the FULL citation text (not the short version)
- Manual dismiss (X button) behavior unchanged (localStorage permanent)

### Claude's Discretion
- Image thumbnail sizing and fallback behavior
- Accordion expand/collapse animation
- Exact compact citation line wording

</decisions>

<specifics>
## Specific Ideas

### GPT Codex Review Findings (incorporated)
- P1: Splitter direction was backwards (value=0 hides results, not viewer) — MOOT now, splitter removed entirely
- P2: URL-encode queries in hero search AND fix existing header search
- P2: One-shot guard not needed with accordion (no splitter to reset)
- P3: Citation should remain visible (compact line), not fully hidden

### Architecture Notes
- Current viewer code: `load_in_viewer()` at search.py:5414 — will be refactored into inline expansion
- Current result cards: `create_result_card()` at search.py:3879 — will be simplified
- Splitter: search.py:1490 — will be removed, replaced with simple scroll area
- Mobile already has inline expansion (search.py:4118 `result-mobile-expand`) — can use similar pattern
- NLI image URLs can come from nli_crossref_service (already used in browse page)
- Citation footer: main.py:489-504 — site-wide, not home-page-only

### What NOT to change
- Advanced View dialog (open_advanced_dialog) stays as-is
- Search header (collapsible search panel) stays as-is
- Filter/sort functionality stays as-is
- Mobile expansion behavior stays (already inline)
- Pagination stays

</specifics>

<canonical_refs>
## Canonical References

- PostHog session replay showing search UX friction
- UX expert assessment: "Prominent, Persistent Search Bar"
- GPT Codex code review with P1-P3 findings

</canonical_refs>
