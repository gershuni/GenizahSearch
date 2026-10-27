## [Unreleased] -- Joins Lab and Visual Similarity: Library and Domain shown only or hidden, the domain tree, counts that follow each other (web + desktop, branch `claude/wonderful-meitner-oorblp`)

### Improvements
- **The Library and Domain filters can hide what you choose, and take several values everywhere.** Each list now
  opens with a **Show only | Hide** switch: Show only keeps the candidates in the chosen libraries or domains, as
  before; Hide leaves them out and keeps the rest, a candidate with no library or domain included. Nothing chosen
  still shows every candidate.
- **Domains are a tree** (owner, after trying the desktop): the FJMS hierarchy, parent → domain → sub-domain, in
  its canonical order (`shared/domain_hierarchy.py`, as the search page's filter); choosing a parent takes in its
  whole branch. A fragment is still placed by its first domain, as its card shows it.
- **Each filter counts what the others keep**: choosing or hiding domains updates the library counts, and the
  other way round; every value keeps its row, at (0) when none is left.
  - Website Joins Lab: the switch sits at the top of the toolbar's Library and Domain lists and applies with the
    picks when the list closes; while values are hidden the label reads "Library · Hiding" / "Domain · Hiding"
    above their names. The Domain list is indented by level. The choices (`libraries_mode` / `domains_mode` in the
    filter state) are kept in the saved session and in a copied link; a link from before them shows only.
  - Website Visual Similarity window: the same switch and nested Domain list, applied at once, like its picks;
    the other list's counts follow at once. The Hebrew UI now lists domains in the tree's order, as the English.
  - Desktop Joins Lab: the Filter dialog's Library and Domain are checklists (Domain the tree; a parent checked,
    partly checked or not by its branch), each with **Select All / Select None** and the Show only / Hide choice,
    and their counts change as you check, before Apply. Under the results toolbar a row of **chips** names every
    filter that is on (text, material, library, domain, dimensions, triage, width); each chip removes its filter,
    "Clear all" removes them all, and the Filter button shows how many are on ("Filter (2) ▾"). The chip is the
    search filter dialog's, now shared: `desktop/widgets/filter_chip.py`.
  - One rule for all three: `shared.joins_lab` (`filter_passes`, `path_passes`, `domain_path`,
    `domain_tree_options`, `facet_rows`).
