## [Unreleased] -- Catalogue criteria, core: Catalog Browse lists the whole corpus (web + desktop, branch `script-filter`)

PR 1a of `docs/plans/CATALOGUE_CRITERIA_PLAN.md`: the shared base of the "Manuscript features" panel
(PRs 1b web, 1c desktop, 2 API). No panel yet; what a
reader already sees changes in three places:

- **Catalog Browse lists all 255,723 manuscripts** (web and desktop), the catalogued ones first, then the
  29,169 with no FJMS catalogue record, marked "No catalogue record" (rulings D16, D22). Filters that pick
  something in (domain, author, work, date, catalogue text) list catalogued manuscripts only; PGP, edition
  and library filters apply to every manuscript.
- **A list that cannot be read is an error**, not "No manuscripts match the current filters" (rule 9).
- **"Search only in..." with only exclusions** (Exclude domains/authors/works, catalogue text "not") now
  searches the manuscripts with no catalogue record too (D17); before, they were silently left out.

Underneath:

- `shared/catalog_criteria.py` (registry, address form, matching) and `shared/catalog_numbers.py` (line,
  column and copy-date parsing); `FjmsService.get_filter_sys_ids` / `get_browse_results` /
  `get_browse_library_facets` take `criteria` and the corpus (`MetadataManager.corpus_sys_ids`, which never
  hands over part of libraries.csv); Catalog Browse fails closed.
- FJMS sidecar **5.1.0**: `scripts/migrate_fjms_criteria.py` adds `criteria_numbers` (lines, columns,
  copy-date spans), `criteria_values` (vocalization extent, attributes), an index on `catalog_fields` and
  `avg_line_height_mm` (which revives the Line Height filter, dead in production). The build sequence, with
  the 'Instatution' fix after every export, is in `docs/guides/DEPLOYMENT_TECHNICAL.md`. The desktop opens a
  downloaded data file only when it is at least as new as the bundled one.
