## [Unreleased] -- Joins Lab: several domains, shown only or hidden (web + desktop, branch `claude/wonderful-meitner-oorblp`)

### Improvements
- **The Joins Lab's Domain filter can hide domains, and takes several on the desktop.** Its list now opens with
  a **Show only | Hide** switch: Show only keeps the candidates in the chosen domains, as before; Hide leaves
  them out and keeps the rest, a candidate with no domain included. Nothing chosen still shows every candidate.
  - Website: the switch sits at the top of the toolbar's Domain list and applies with the picks when the list
    closes; while domains are hidden the select's label reads "Domain · Hiding" above their names. The choice
    (`domains_mode` in the filter state) is kept in the saved session and in a copied link; a link from before
    it shows only, as it always did.
  - Desktop: the Filter dialog's Domain row is a checklist of the results' domains, each with its count (it was a
    single choice), under the same Show only / Hide choice. Reset and Clear empty it and go back to Show only.
  - One rule for both apps: `shared.joins_lab.domain_passes` / `normalize_domain_mode`.
