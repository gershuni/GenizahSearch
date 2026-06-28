# Phase 129: Library Filter — Search + Browse-by-Identification (SEED-026) - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-28
**Phase:** 129-Library Filter — Search + Browse-by-Identification (SEED-026)
**Areas discussed:** Library labels, Facet behavior, Control UI & placement, Desktop parity scope

---

## Library labels

| Option | Description | Selected |
|--------|-------------|----------|
| Human names EN+HE | 'Cambridge UL' / Hebrew via get_library_display(code) + LIBRARY_CODES_HE; readable, RTL-correct, no English leak under Hebrew | ✓ |
| Name + code | 'Cambridge UL (CUL)' — readable plus the code researchers recognize; longer chips | |
| Raw codes | CUL / JTS / RNL — compact, language-neutral, but opaque and matches nothing else in the UI | |

**User's choice:** Human names EN+HE
**Notes:** → D-01. Use the existing `get_library_display` helper; enforce the standing no-English-leak-under-Hebrew i18n invariant.

---

## Facet behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Facet on web, list on catalog | Web search: counts + hide-empty (result set already in hand, cheap). Catalog: plain full list (avoids an extra GROUP BY over the paginated query) | ✓ |
| Facet everywhere | Counts + hide-empty on both; nicer/consistent but costs a GROUP BY on the catalog browse query | |
| Plain list everywhere | Static ~11-library list, no counts, on both; simplest but shows 0-match libraries | |

**User's choice:** Facet on web, list on catalog
**Notes:** → D-02. Pragmatic split — accurate facet where free (web search), cheap plain list where a facet would cost an extra query (catalog). Catalog facet counts noted as a possible additive follow-up.

---

## Control UI & placement

| Option | Description | Selected |
|--------|-------------|----------|
| Dropdown next to filters | Compact 'Filter by library' menu-button with a checklist inside, beside the existing PGP/Printed buttons; active picks render as removable chips; minimal vertical space | ✓ |
| Always-visible checklist | Inline checkbox list / chip cloud always shown; more discoverable but eats vertical space | |

**User's choice:** Dropdown next to filters
**Notes:** → D-03. Removable chips for active selections, consistent with existing filter chips. Empty selection = all.

---

## Desktop parity scope

| Option | Description | Selected |
|--------|-------------|----------|
| Build now (full parity) | Add the library filter to the desktop catalog tab this phase; _CatalogRefreshWorker + _get_catalog_filter_sets already thread filter sets into get_browse_results; earns the desktop 8.3.0 version bump (LIBFILTER-03) | ✓ |
| Defer desktop catalog | Ship web-only for 8.3.0; park desktop catalog filter as a follow-up | |

**User's choice:** Build now (full parity)
**Notes:** → D-04. Confirms the LIBFILTER-03 / roadmap requirement over the seed's "likely deferred" hedge — desktop earns the public version bump with a visible feature.

---

## Claude's Discretion

- Exact widget per platform (NiceGUI `ui.menu`/`ui.select multiple` vs custom checklist; Qt `QMenu` checkable actions vs multi-select combo) — planner/implementer's call, provided D-01..D-03 hold.
- Cheapest catalog push-down query shape (library_codes → sys-id set via meta_mgr / reverse map, or temp-table intersection per SEED-023's no-giant-`IN(...)` rule) — researcher/Codex to pin down. The mapping primitive `meta_mgr.get_library_for_id(sid)` is confirmed; the bulk/reverse direction is the open detail (the Codex-review-before-code gate target).

## Deferred Ideas

- API library-filter param (`/api/search`, `/api/browse`) — natural follow-up, out of scope here.
- Catalog facet counts — out of scope (D-02 keeps catalog a plain list); possible additive follow-up.
- Library filter on other pages (reading desk, Joins Lab, puzzle) — not requested, out of scope.
- Reviewed-but-not-folded todos: all 6 `todo.match-phase` hits were keyword-fuzzy and off-topic (desktop corrections migration, Reading-Desk UX, server-side search w/ email, NLI MARC crawl, unified metadata search, one-click citations) — none touch `library_code` filtering.
