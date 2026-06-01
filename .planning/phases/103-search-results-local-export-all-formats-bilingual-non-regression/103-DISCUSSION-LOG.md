# Phase 103: Search-Results LOCAL Export - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-01
**Phase:** 103-search-results-local-export-all-formats-bilingual-non-regression
**Areas discussed:** Local Documents sheet design, CSV/TXT/DOCX single-table merge (per-format), LOCAL-only workbook shape, Exclusion + Hebrew labels

---

## Local Documents sheet design

### Extra columns beyond the 5 required
| Option | Description | Selected |
|--------|-------------|----------|
| Full Text only | Add full indexed page text, skip System ID | |
| Full Text + System ID | Include both | |
| Just the 5 required | Filename/folder/filepath/page/matched text only | ✓ |

**User's choice:** Just the 5 required.

### Page column value
| Option | Description | Selected |
|--------|-------------|----------|
| Locator, page-# fallback | chunk_locator ("p. 3"/"§ Intro"), fall back to raw p_num | ✓ |
| Raw page number only | 1-based p_num int | |
| Both columns | Page (int) + Location (locator) | |

**User's choice:** Locator, page-# fallback.

### Matched-text rendering
| Option | Description | Selected |
|--------|-------------|----------|
| Rich highlight (bold terms) | Same as Genizah Snippet col (build_rich_snippet_cell) | ✓ |
| Plain text | Asterisks stripped, no bolding | |

**User's choice:** Rich highlight (bold terms).

---

## CSV / TXT / DOCX (per-format treatment)

The user rejected a single merge rule across all three ("each of them needs separate treatment"), so each format was taken on its own terms.

### CSV (single flat table)
| Option | Description | Selected |
|--------|-------------|----------|
| Repurpose + add Filepath/Page | Shelfmark=filename, Library=folder, Source=LOCAL, Snippet=matched text; add Filepath+Page cols only when LOCAL present | ✓ |
| Append named local columns | Keep 7 Genizah cols + append Filename/Folder/Filepath/Page (LOCAL rows show blank Genizah cells) | |

**User's choice:** Repurpose + add Filepath/Page.

### DOCX
| Option | Description | Selected |
|--------|-------------|----------|
| Separate 'Local Documents' table | Second labeled table below the Genizah table | |
| One table, repurpose + add cols | Single table, filename→Shelfmark etc. | |
| One table, append named cols | Single table + appended local cols | |
| **(User free-text)** | **"It's not a table. But we can enrich the docx more (for Genizah too) while we're at it"** | ✓ |

**User's choice (free-text):** DOCX should not be a table at all. Redesign into a per-result rich-document block layout, applied to **both Genizah and LOCAL** rows, and "add URL". Confirmed in a follow-up exchange: (1) go with the per-result block layout replacing the table; (2) apply it to Genizah DOCX too (consciously relaxing LEXP-08's DOCX-non-regression clause); (3) extra field = URL (GenizahSearch URL for Genizah / filepath for LOCAL).
**Notes:** Recorded as an intentional deviation (CONTEXT D-10/D-12). LEXP-08 and ROADMAP success-criterion #5 amended to carve out DOCX; the xlsx cross-parity invariant is unaffected.

### TXT (labeled blocks)
| Option | Description | Selected |
|--------|-------------|----------|
| Header + Path/page line + snippet | `=== filename \| folder ===`, `Path: {filepath} (page N)`, snippet | ✓ |
| Everything in the header | `=== filename \| filepath \| page N ===`, snippet | |
| Minimal (no full path) | `=== filename (page N) ===`, snippet | |

**User's choice:** Header + Path/page line + snippet.

---

## LOCAL-only workbook shape

### Genizah sheets in a LOCAL-only export
| Option | Description | Selected |
|--------|-------------|----------|
| Omit the empty Genizah sheets | Workbook = [Local Documents, Credits and Info] | ✓ |
| Keep all sheets, Genizah ones empty | All 5 sheets present, Genizah header-only | |

**User's choice:** Omit the empty Genizah sheets.

### Mixed-workbook sheet order + active sheet
| Option | Description | Selected |
|--------|-------------|----------|
| Pos 4, Credits last; Search Results active | [Search Results, Manuscripts, Bibliography, Local Documents, Credits and Info] | ✓ |
| Right after Search Results | [Search Results, Local Documents, Manuscripts, Bibliography, Credits and Info] | |
| Very last, after Credits | [..., Credits and Info, Local Documents] | |

**User's choice:** Pos 4, Credits last; Search Results active (LOCAL-only opens on Local Documents).

---

## Exclusion + Hebrew labels

### Hebrew label set
| Option | Description | Selected |
|--------|-------------|----------|
| מסמכים מקומיים set | שם קובץ / תיקייה / נתיב מלא / עמוד / טקסט תואם | ✓ |
| קבצים מקומיים set | קובץ / תיקיית אב / נתיב / עמוד / קטע תואם | |
| Let me derive to match existing style | Claude chooses | |

**User's choice:** מסמכים מקומיים set.
**Notes:** LOCAL exclusion from Manuscripts/Bibliography (LEXP-04) was treated as locked by the requirement (flip `skip_local=True`); not a discussion fork.

---

## Claude's Discretion

- openpyxl column widths for the Local Documents sheet; DOCX block typography + separator style.
- Optional extra Genizah metadata in DOCX blocks beyond the URL (kept lean by default).
- Helper placement (export_dossier.py for sheet/row helpers; shared DOCX block writer reusable by Phase 104).
- Graceful handling of unresolved LOCAL filepaths (blank cell + log, never error).

## Deferred Ideas

- Composition-report DOCX/LOCAL parity (Phase 104, LEXP-02) — design the DOCX block writer to be reusable.
- Full Text column on the Local Documents sheet (declined; revisitable).
- Extra Genizah DOCX metadata (PGP/domains/date) beyond URL.
- JSON export + Parallels export LOCAL adaptation (out of v7.17 entirely).
