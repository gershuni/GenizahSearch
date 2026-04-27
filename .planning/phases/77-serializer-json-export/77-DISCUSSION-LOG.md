# Phase 77: Serializer & JSON Export - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-27
**Phase:** 77-serializer-json-export
**Areas discussed:** Per-item fields, Top-level envelope, Parallels shape

---

## Gray Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Module placement | shared/ vs web/serializers/ vs extending export_service.py | |
| Per-item fields | Field set, full_text policy, highlight handling, locator emission | ✓ |
| Top-level envelope | Wrapping shape, query echo, warnings slot, pagination | ✓ |
| Parallels shape | Filtered hits, chunk metadata, grouping, module API | ✓ |

**Notes:** Module placement deferred to Claude's discretion — picked `shared/search_serializer.py` to avoid web→core circular dep when Phase 78 imports.

---

## Per-item Fields

### Q1: What field set should each result item carry?

| Option | Description | Selected |
|--------|-------------|----------|
| Minimum (Recommended) | locator + score + shelfmark + title + snippet + library + domain + dating | |
| Minimum + image URL | Above + primary image URL from existing display['img'] | ✓ |
| Rich (everything available) | Above + raw_header, source, FJMS catalog refs, bibliography | |

**User's choice:** Minimum + image URL.

### Q2: Include the full transcription text in each item?

| Option | Description | Selected |
|--------|-------------|----------|
| Excluded — snippet only (Recommended) | Skill drills in via /api/browse for full text | |
| Included with cap (~32K like Excel) | Bulk text upfront, capped | (Other) |
| Add an `excerpt` field (~500 chars) | Always-present clean preview | (Other ✓ via follow-up) |

**User's choice:** Initially "option 2 or 3, the skill should have more than just snippet" (free text). Follow-up clarification: **Option 3 (excerpt ~500 chars) plus locator-driven /api/browse drill-in** for full text when needed.

**Notes:** User's "option to fetch the full text if needed" maps directly to the Phase 79 `/api/browse` path. Excerpt covers the gap when snippet is empty (metadata-only / Title / Shelfmark hits).

### Q3: How should the snippet field handle the existing `*term*` highlight markers?

| Option | Description | Selected |
|--------|-------------|----------|
| Stripped clean (Recommended) | `text foo bar` — ready to ingest | |
| Kept as-is | Preserves which terms matched | |
| Stripped + separate `match_terms: [...]` | Clean snippet + explicit term list | ✓ |

**User's choice:** Stripped + separate `match_terms: [...]`.

### Q4: How should the drill-down locator be emitted on each item?

| Option | Description | Selected |
|--------|-------------|----------|
| Both fields always populated (Recommended) | uid + locator both present every item | ✓ |
| uid when present, locator object only when uid missing | "preferred / fallback" literal | |
| Only the locator object, drop uid entirely | Most stable, loses canonical ID | |

**User's choice:** Both fields always populated.

---

## Top-level Envelope

### Q1: How should the top-level envelope structure metadata?

| Option | Description | Selected |
|--------|-------------|----------|
| Flat keys (Recommended) | `{results, query, mode, count, generated_at, warnings}` | ✓ |
| Namespaced under `meta` | `{results, meta: {...}}` | |
| Bare results only | `{results}` only | |

**User's choice:** Flat keys.

### Q2: Should the response echo back the query that produced the results?

| Option | Description | Selected |
|--------|-------------|----------|
| Full echo (Recommended) | query, mode, gap?, filters? exactly as used | ✓ |
| Just query and mode | Minimal | |
| No echo | Skill tracks own request context | |

**User's choice:** Full echo.

### Q3: How should the `warnings` slot behave in Phase 77?

| Option | Description | Selected |
|--------|-------------|----------|
| Always present, often empty (Recommended) | `warnings: []` even on clean queries | ✓ |
| Only present when non-empty | Lean clean-query downloads | |
| Skip in Phase 77, add in Phase 78 | Defer | |

**User's choice:** Always present, often empty.

### Q4: What pagination/count metadata should the envelope carry?

| Option | Description | Selected |
|--------|-------------|----------|
| `count` + `total` (Recommended) | Items in payload + full result count | ✓ |
| `count` only | No total signal | |
| Full pagination: count, offset, limit, total | Closer to Phase 78 API | |

**User's choice:** count + total.

### Q5 (follow-up): Should the envelope include a `source` field identifying which page/endpoint produced it?

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, `source: 'search' \| 'parallels'` (Recommended) | Self-identifying when reopened | ✓ |
| Skip — mode + filename are enough | Mild duplication argument | |

**User's choice:** Yes, source field.

### Q6 (follow-up): Add a schema/version field to the envelope now?

| Option | Description | Selected |
|--------|-------------|----------|
| `schema_version: 1` (Recommended) | Cheap insurance against drift | ✓ |
| Skip — "no stability promise" | No versioning implication | |
| `generated_by: 'genizah-search-7.10'` | App-version coupling | |

**User's choice:** schema_version: 1.

---

## Parallels Shape

### Q1: How should filtered / high-frequency hits appear in the parallels payload?

| Option | Description | Selected |
|--------|-------------|----------|
| Separate `filtered: [...]` array (Recommended) | Mirrors UI separation | ✓ |
| Flag on each item, one results array | Flatter contract | |
| Omit filtered hits entirely | Lean, loses evidence | |

**User's choice:** Separate filtered array.

### Q2: What source-chunk metadata should each parallels item carry?

| Option | Description | Selected |
|--------|-------------|----------|
| Full: `source_chunk_text` + `chunk_index` (Recommended) | Self-explanatory matches | ✓ |
| `chunk_index` only | Skill reconstructs text | |
| Skip chunk metadata | Loses parallels signal | |

**User's choice:** Full chunk metadata.

### Q3: When one manuscript matches multiple source chunks, how should it appear?

| Option | Description | Selected |
|--------|-------------|----------|
| One result per (manuscript, chunk) pair | Chunk-level granularity | |
| One result per manuscript with `matches: [...]` array (Recommended) | Mirrors UI | ✓ |
| Match the UI's current behavior | Trust existing shape | |

**User's choice:** One result per manuscript with matches[] array.

### Q4: How should the single serializer module expose search vs parallels?

| Option | Description | Selected |
|--------|-------------|----------|
| Two named functions sharing helpers (Recommended) | serialize_search_payload + serialize_parallels_payload | ✓ |
| Polymorphic serialize_payload(source, ...) | Branchy single entry | |
| Class-based with subclasses | Most ceremony | |

**User's choice:** Two named functions sharing private `_serialize_item()` helper.

---

## Claude's Discretion

The following decisions were not asked of the user and are captured under "Claude's Discretion" in CONTEXT.md:

- Module placement — `shared/search_serializer.py`
- Toolbar button placement, icon (`data_object`), tooltip styling
- Disabled-state behavior (mirror existing Word/Excel disable pattern)
- Filename format — `genizah-{search,parallels}-{ISO timestamp}.json`
- HTTP route names — `/api/export/json` and `/api/export/parallels/json`
- Score normalization — round to 4 decimals, rename `sort_score` → `score`
- `match_terms` extraction — parse `*term*` markers once at serialize time
- Image URL field — use existing `display['img']`, null when not resolved
- Test surface — unit tests for both serializer functions, spot-check downloads per phase gate
- Manuscript-level score aggregation rule (D-13) — planner picks from existing UI logic (max or sum)

## Deferred Ideas

None.
