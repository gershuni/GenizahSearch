## [Unreleased] -- A phone layout for search, browse, the header and catalog browse (web, branch `ccr-204fb02d-6h6khg`)

The site below 640px was the desktop layout stacked, not a phone layout
(owner, 2026-10-22, with phone screenshots). Measured at 390x844 before this
change: on `/browse` the manuscript image began 958px down the page (1,042px
in English); on `/search` the open form and the toolbar left 300px of the
screen to results and one result card was 400px; the English header wrapped
its login group out of its 64px bar; `/catalog-browse` kept a 280px sidebar
beside a results column about 60px wide.

### Header
- The header row no longer wraps. Login and Register are icon buttons on a
  phone (`phone-icon-only`, the name kept in `aria-label` and the tooltip);
  wider screens see the same text buttons as before.

### Search
- On a phone the page scrolls instead of a results pane inside a fixed-height
  shell, so the header and the browser's address bar hide on scroll.
- The form folds into its one-line strip when a search starts (the width check
  runs in the browser; the strip's own button brings the form back).
- The results toolbar is one line: count and filter buttons scroll sideways,
  the export icons are a menu, a filter that is not yet available takes no
  space (it still reserves its space on wide screens), the pagination
  placeholder goes, and cards lose their desktop padding and spacing.

### Browse
- Hidden on a phone: the page title, the lookup card's decorative icon, the
  image panel's title, the zoom figure, the rotation slider and the
  separators. Search for Parallels and Show Metadata are icon-only there
  (`browse-icon-collapse`, text kept in the tree).
- The image sets the height of its box (no dark bands above and below it);
  the brightness/contrast/gamma sliders sit behind a `tune` button; the
  transcription is part of the page instead of a pane that scrolls inside it.

### Catalog browse
- Under 768px the filter sidebar is hidden behind a Filters button and stacks
  above the results; the table keeps its columns and scrolls sideways.

### Every width (the owner's mockups, 2026-10-22)
- Search: the form is one row on a laptop (two on a tablet) with Advanced
  Options and "Search only in..." as buttons that open their panels; the
  results toolbar carries a density toggle and one menu for the post-filters
  and the exports. Rows are compact by default (one header line, a two-line
  snippet, actions on hover), remembered per visitor; "comfortable" restores
  the older card.
- An expanded result shows the full text in the snippet's place beside a
  150 px thumbnail, with one action line; on a phone the thumbnail drops
  under the text.
- Quick view: one info line on a laptop, the image sliders behind a `tune`
  button, catalog and PGP details as collapsed bars at the foot; on a phone
  the view is three tabs, Text, Image and Info, with prev and next in the
  header.
- Browse: the page title is for screen readers, the lookup is a slim row,
  the green card is one line on a laptop with the title cut to the width,
  the image sliders sit behind `tune` at every width; a tablet gets the
  overlay drawer (breakpoint 1023). Transcription type scales by width and
  content caps at 1600 px on wide screens.
- The manuscript viewer takes touch: two fingers pinch-zoom, one finger pans
  a zoomed image, and an unzoomed image leaves the page free to scroll.
