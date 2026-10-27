## [Unreleased] -- Joins Lab and Visual Similarity: Library and Domain shown only or hidden (web + desktop, branch `claude/wonderful-meitner-oorblp`)

### Improvements
- **The Library and Domain filters can hide what you choose, and take several values everywhere.** Each list now
  opens with a **Show only | Hide** switch: Show only keeps the candidates in the chosen libraries or domains, as
  before; Hide leaves them out and keeps the rest, a candidate with no library or domain included. Nothing chosen
  still shows every candidate.
  - Website Joins Lab: the switch sits at the top of the toolbar's Library and Domain lists and applies with the
    picks when the list closes; while values are hidden the select's label reads "Library · Hiding" or
    "Domain · Hiding" above their names. The choices (`libraries_mode` / `domains_mode` in the filter state) are
    kept in the saved session and in a copied link; a link from before them shows only, as it always did.
  - Website Visual Similarity window: the same switch on its Library and Domain lists, applied at once, like
    its picks.
  - Desktop Joins Lab: the Filter dialog's Library and Domain rows are checklists of the results' values, each
    with its count (each was a single choice), under a Show only / Hide choice. Reset and Clear empty them and go
    back to Show only.
  - One rule for all three: `shared.joins_lab.filter_passes` / `normalize_filter_mode`.
