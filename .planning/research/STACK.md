# Technology Stack: Search Refinement & Scholarly Joins

**Project:** v7.3 Search Refinement & Scholarly Joins
**Researched:** 2026-03-26

## Recommended Stack

No new technology required. All features build on existing stack.

### Core (EXISTING, unchanged)
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Python 3.10+ | 3.10+ | Core language | Existing |
| Tantivy (tantivy-py) | latest | Full-text search engine | Existing, restrict_sys_ids pipeline |
| SQLite | 3.x | FJMS sidecar (fjms_enrichment.db) | Existing, catalog_sizes + joins tables |
| Supabase | latest | User lists, auth | Existing, list items for exclude feature |
| NiceGUI | latest | Web UI framework | Existing |
| PyQt6 | latest | Desktop UI framework | Existing |

### Supporting Libraries (EXISTING, unchanged)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| supabase-py | latest | Supabase client | Fetching user lists for exclude feature |
| genizah_core.SearchEngine | - | Search with restrict_sys_ids | All search features |
| shared/fjms_service.FjmsService | - | FIST data access | Dimensions, joins queries |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Dimension filter storage | Extend get_filter_sys_ids() | New SQLite table | Unnecessary; catalog_sizes already indexed |
| Join search | restrict_sys_ids with cached join set | New Tantivy field "has_joins" | Would require index rebuild; restrict_sys_ids sufficient |
| Exclude persistence (web) | Per-session state + Supabase lists | Local storage / cookies | Lists already in Supabase; session state is ephemeral by design |
| File import parsing | Python csv/text parsing | Pandas | Overkill for line-by-line shelfmark file |

## Installation

No new packages needed.

```bash
# No changes to requirements
```

## Database Index Addition

```sql
-- Add to fjms_enrichment.db for dimension range queries
CREATE INDEX IF NOT EXISTS idx_catsz_size ON catalog_sizes(SizeX, SizeY);
```

## Sources

- Existing codebase analysis (all technologies already in use)
