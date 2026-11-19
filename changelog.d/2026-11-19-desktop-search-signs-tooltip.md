## [Unreleased] -- The desktop search box's tooltip lists every mode sign (desktop, branch `claude/desktop-signs-tooltip`)

### Fixed
- **The desktop search box's shortcuts tooltip is up to date.** Its list of signs was written
  out by hand and had fallen behind: it had no `%` (Catalog), no `??` or `???` (the Variants
  levels, which it told the reader to pick with the buttons), and gave `R` as a sign with no
  space after it, which since v10.1.0 searches a word (`R שלום` is a Responsa search, `Rashi`
  is not). Since v10.1.0 the tooltip also opens with the mode's lead ("Type a term to search"),
  so the old list showed on every hover. It now reads the one table of signs
  (`shared/search_mode_signs.py::MODE_SIGNS`) through
  `desktop/catalog_search.py::signs_tooltip`, so a sign added there is listed too: each mode by
  the name the mode lists use ("Variants Extended: ??"), `R` with "and a space", `%` only where
  the catalogue is installed, and an example of each kind (`?שלום`, `R שלום`), in English and
  Hebrew.
