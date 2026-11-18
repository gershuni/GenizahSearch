## [Unreleased] -- The search box says what to type in its mode, with an example (web + desktop, branch `claude/mode-placeholders`)

### Changed
- **The search box now says what to type in the mode chosen, and shows an example.** It read
  "Enter Hebrew text to search" on the website and "Search terms, title or shelfmark..." on the
  desktop in every mode. Now (owner's design and wording, 2026-11-18) a short lead names what
  the mode takes -- "Type a term to search" (Exact, Variants, Fuzzy), "Type words with operators
  for an advanced search" (Responsa, Regex), "...the titles", "...the catalog data", "Type a
  string to search the shelfmarks" -- and the box's grey text is one example, drawn at random
  from the mode's own each time the page opens or the mode changes (in Responsa and Regex mode
  each example shows a different operator). Every example finds results. On the website the lead
  is the box's label, on its top border as the Mode field's is; on the desktop it heads the
  box's tooltip, above the shortcuts. The box follows the mode however it is chosen: in the list,
  by a sign typed in the box (`$ `, `# `...), or from history. One table for both apps
  (`shared/search_mode_examples.py`; `web/pages/search.py`,
  `desktop/catalog_search.py::show_mode_examples`). The Joins Lab box is unchanged.
