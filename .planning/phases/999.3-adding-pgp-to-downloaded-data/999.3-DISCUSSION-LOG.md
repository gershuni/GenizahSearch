# Phase 999.3: Adding PGP to downloaded data - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-15
**Phase:** 999.3-adding-pgp-to-downloaded-data
**Areas discussed:** Formats, Fields, Missing, Columns, Lists, Language, Repeat

---

## Which exports gain PGP fields

Multi-select. Investigated existing exports first: JSON already includes PGP via `_build_pgp_subset` at `shared/search_serializer.py:473`. Excel and Word currently have no PGP info. List and parallels exports also lack it.

| Option | Description | Selected |
|--------|-------------|----------|
| Excel search results | export_service.py:286 — 7 columns, no PGP | ✓ |
| Word search results | export_service.py:357 — shelfmark + title + snippet only |  |
| List exports | export_service.py:413 — user-saved lists |  |
| Parallels exports (Excel + Word) | export_service.py:477 / :563 |  |

**User's choice:** Excel search results only.

---

## Which PGP fields to include

Multi-select.

| Option | Description | Selected |
|--------|-------------|----------|
| PGP link (URL to princeton.edu) | pgp_url field | ✓ |
| PGP description (HE or EN per user language) | Free-text scholarly description | ✓ (English only — see Language area) |
| PGP transcription text (page-scoped) | Full transcription, can be long |  |
| PGP metadata (type, dates, languages, tags) | Structured fields | ✓ |

**User's choice:** Link + Description + Metadata. Transcription text excluded.
**Notes:** Final column set locked: PGP URL, PGP Description, PGP Type, PGP Date, PGP Languages, PGP Tags.

---

## Missing PGP data

| Option | Description | Selected |
|--------|-------------|----------|
| Empty cell / null | Matches existing convention. (Recommended) | ✓ |
| '—' or 'N/A' literal | Explicit marker; breaks sort |  |
| Skip rows that have no PGP | Filters export down to PGP-only |  |

**User's choice:** Empty cell / null.

---

## Column placement

| Option | Description | Selected |
|--------|-------------|----------|
| Appended at end after 'Full Text' | Preserves existing column order. (Recommended) | ✓ |
| Inserted after System ID (before Score) | Groups PGP with identification columns |  |
| Inserted after Title (before System ID) | Most prominent placement |  |

**User's choice:** Appended at end.

---

## List-valued field rendering

| Option | Description | Selected |
|--------|-------------|----------|
| Comma + space separated | Most human-readable. (Recommended) |  |
| Pipe separated ('Bible\|Letter\|Legal') | Round-trip safe | ✓ |
| Newline separated | Vertical readability in cell |  |

**User's choice:** Pipe separated.
**Notes:** Explicitly overrode the "Recommended" default. User wants research-script-friendly delimiter.

---

## Language fallback

| Option | Description | Selected |
|--------|-------------|----------|
| Fall back to other language with '(EN)' marker | Preserves data, signals gap. (Recommended) |  |
| Fall back silently | Cleaner cells, loses signal |  |
| Leave cell empty if requested language missing | Strict |  |
| (User override) | "PGP is always in English, so this is how the info will be presented" | ✓ |

**User's choice:** Override the entire question — PGP is canonical English, export always in English.
**Notes:** This is a strong directive. No `get_language()` call in the export path, no translation lookups, no fallback logic. The export is a citation artifact in the source language.

---

## Per-row repetition for multi-folio hits

| Option | Description | Selected |
|--------|-------------|----------|
| Repeat PGP fields on every row | Each row self-contained for sort/filter. (Recommended) | ✓ |
| Only fill PGP on first row for that manuscript | Cleaner scan but breaks sort |  |

**User's choice:** Repeat on every row.

---

## Claude's Discretion

- Exact column widths for the new columns.
- Whether to extract a shared `pgp_fields_for_export(sys_id, meta_mgr)` helper or inline the lookup.
- Whether to escape pipe characters that legitimately appear inside a tag value (extremely rare).
- Whether to render PGP URL as an Excel hyperlink (clickable) or plain text — default plain.

## Deferred Ideas

- Word search-results export PGP columns.
- List export PGP columns.
- Parallels exports PGP columns.
- PGP transcription text in exports (Excel cell-size limit concern).
- PGP source / scholar attribution columns (Goitein, V0.8, etc.).
- Hebrew-translated PGP descriptions as an opt-in export mode.
- Excel hyperlink rendering for PGP URL.
