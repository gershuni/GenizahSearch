# Quick Task 260320-dvg: Add icons and reorganize Browse by Shelfmark tab - Context

**Gathered:** 2026-03-20
**Status:** Ready for planning

<domain>
## Task Boundary

Add icons and reorganize the Browse by Shelfmark tab in desktop to match ResultDialog patterns. Also fix cross-shelfmark page navigation and add external library links.

</domain>

<decisions>
## Implementation Decisions

### Button Icons
- Match ResultDialog emoji-icon style for all browse tab buttons
- Browse by List = folder icon, Go = keep plain, Add to View = eye icon, Find parallels = magnifier, View on Ktiv = globe, View Corrections = notepad
- All action buttons get emoji prefixes like ResultDialog (e.g. `f"icon {tr('Label')}"`)

### Row Reorganization (ext_info_row becomes action row)
- **Row1 (top bar)**: Keep as-is — navigation inputs + Go + Add to View (stays after Go for discoverability)
- **ext_info_row reorganized**: Move action buttons here from row1:
  - Left group: Puzzle, Parallels, List (moved from row1)
  - Then: Extended Info button (moved from left side to middle, near bib/cat)
  - Then: Bib FJMS, Bib NLI, Catalog Records
  - Then: External links group — View on Ktiv + library-specific link (Cambridge/Oxford/Manchester/Princeton, like ResultDialog's btn_external_link)
  - Right end: Translations toggle as compact icon (colored when ON, uncolored when OFF, with tooltip — no longer a wide text button)

### Community Bar Icons
- View Corrections gets notepad emoji to match ResultDialog pattern

### Cross-Shelfmark Page Navigation
- Remove the disable logic at page boundaries (lines 27964-27965)
- Keep btn_b_prev/btn_b_next always enabled when a manuscript is loaded
- browse_navigate already uses allow_cross=True, so wrapping to next/prev shelfmark will work automatically

### External Library Links
- Add a btn_b_external_link button (like ResultDialog's btn_external_link)
- Shows Cambridge/Oxford/Manchester/Princeton based on external_provider from enrichment metadata
- Visible only when external_url is available
- Placed in the ext_info_row near View on Ktiv

### ResultDialog Image Toggle State
- Remember hide/show image state when navigating between results in ResultDialog
- Currently btn_toggle_image.isChecked() is not preserved across result navigation

</decisions>

<specifics>
## Specific Ideas

- ResultDialog action_row order for reference: Browse, Parallels, List, Puzzle, Info, Bib FJMS, Bib NLI, Catalog, Image Toggle, Translations
- ResultDialog external links: btn_img (Go to Ktiv) + btn_external_link (Cambridge/Oxford/Manchester/Princeton)
- Translation toggle: small icon button with colored/uncolored state + tooltip, not a wide text button

</specifics>
