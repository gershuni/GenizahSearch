# Code Review: FIST Bibliography Enhancement + Catalog Dedup

## Summary of Changes

Two fixes for FIST-sourced data in GenizahSearch (NiceGUI web + PyQt6 desktop):

1. **Catalog free description deduplication** (already committed as `77e562e2`)
2. **Bibliography data enhancement** — 8 new fields from FIST source, volume fix, Hebrew title support (uncommitted, pending review)

## Prior Review Actions

GPT Codex reviewed the initial plan and found 3 issues. All have been addressed:

| Finding | Resolution |
|---------|------------|
| P1: `n_cols = 23` but only 22 columns/values | Fixed to `n_cols = 22` |
| P2: `TitleAcronymHeb` exported but never wired | Now wired through service + both UIs (Hebrew fallback) |
| P2: `ui.html(sanitize=False)` with raw sidecar text | Added `html.escape()` wrapper (`_esc()`) for all detail panel values in web dialog |

Codex recommendations adopted:
- Vol. column: keep COALESCE'd value, `e_volume` in detail panel only
- Comment: detail panel only (too noisy for table)
- Backward compat: Option B — detect columns once on init (`_has_bib_extended`)
- Hebrew title: prefer Hebrew when UI language is Hebrew, single title string
- JournalDate: detail panel only

## Files Changed (4 files, ~130 lines added/changed)

### 1. `scripts/export_fist_enrichment.py` — Export Script

**What changed:** Bibliography table gains 8 new columns. Volume source fixed.

- CREATE TABLE: 15 → 22 columns (added RunningTitleHeb, TitleAcronymHeb, EVolume, JournalDate, Comment, NoteForDisplay, CatalogEntry, and moved Volume to use COALESCE)
- SELECT: Added `t.RunningTitleHeb`, `t.AcronymHeb`, `bib.EVolume`, `bib.JournalDate`, `bib.Comment`, `bib.NoteForDisplay`, `bib.CatalogEntry`
- Volume: `bib.Volume` → `COALESCE(NULLIF(bib.JournalVolumeTxt, ''), bib.Volume)` — JournalVolumeTxt has 71K populated rows vs Volume's 11K (mutually exclusive)
- INSERT: hardcoded 15 `?` → parameterized `n_cols = 22`

**Review focus:** Column count alignment (was the P1 bug). Count the CREATE TABLE columns, SELECT columns, and `n_cols` — they must all be 22.

### 2. `shared/fjms_service.py` — Service Layer

**What changed:** Backward-compatible extended field support.

- `__init__`: Added `_has_bib_extended` flag, detected via `SELECT RunningTitleHeb FROM bibliography LIMIT 0` (follows existing `_has_persons_titles` pattern)
- `get_bibliography()`: Returns 7 new keys when `_has_bib_extended` is True, else None for each. Changed from list comprehension to explicit loop.
- `get_catalog_detail()`: Added deduplication for free descriptions by `(source_name, text)` tuple (the already-committed fix)

**Review focus:** Does the backward-compat detection work correctly? Is `LIMIT 0` a safe probe? Are all 8 new export columns represented in the service return (7 new keys + the Volume COALESCE which replaces the old Volume)?

### 3. `web/components/bibliography_dialog.py` — Web UI

**What changed:** Hebrew title support, extended detail panel, HTML escaping.

- Added `import html as html_mod` and `get_language`
- `_build_rows()`: When `get_language() == 'he'`, title fallback chain is: `running_title_heb` → `running_title` → `title_acronym_heb` → `title_acronym`
- `_esc()` helper: wraps `html.escape()` for safe rendering in `ui.html(sanitize=False)` detail panel
- `on_row_click()`: Shows e_volume, journal_date, catalog_entry, comment, note_for_display when populated
- `apply_filters()`: Added `running_title_heb` and `title_acronym_heb` to searchable text

**Review focus:** Is the `_esc()` function sufficient for XSS prevention in the `ui.html(sanitize=False)` context? Should `sanitize=True` be used instead? Is the Hebrew fallback chain correct?

### 4. `genizah_app.py` — Desktop UI

**What changed:** Mirror of web changes for PyQt6.

- Table population: Same Hebrew title fallback chain using `CURRENT_LANG == 'he'`
- `_safe()` static method: strips/validates values (no HTML escaping needed — PyQt uses `setPlainText`)
- `_on_row_selected()`: Shows same extended fields as web
- `_filter_rows()`: Added Hebrew fields to searchable text

**Review focus:** `_safe()` doesn't HTML-escape because desktop uses `setPlainText()` — is this correct? Is the `CURRENT_LANG` access safe at dialog construction time (vs. dynamic language toggle)?

## Data Impact

| Field | FIST Source | Populated Rows | Total Rows |
|-------|-----------|---------------|------------|
| JournalVolumeTxt → Volume | `bib.JournalVolumeTxt` | 71,132 | 733,209 |
| EVolume | `bib.EVolume` | 20,478 | 733,209 |
| JournalDate | `bib.JournalDate` | 441,112 | 733,209 |
| RunningTitleHeb | `CODE_Title.RunningTitleHeb` | 2,138 | 4,309 |
| TitleAcronymHeb | `CODE_Title.AcronymHeb` | ~2,000 | 4,309 |
| Comment | `bib.Comment` | 447,191 | 733,209 |
| NoteForDisplay | `bib.NoteForDisplay` | 1,253 | 733,209 |
| CatalogEntry | `bib.CatalogEntry` | 7,181 | 733,209 |

## Next Steps (after review passes)

1. Backup current `fjms_enrichment.db`
2. Re-export bibliography from FIST backup (`C:\GenizahSearch\FIST_DB_BACKUP\FIST.db`)
3. Verify MS heb. d. 32/6 shows volume `תרצ"ד, ספר חמישי`
4. Commit all changes
5. Update STATE.md

## Questions for Reviewer

1. The web dialog detail panel uses `ui.html(sanitize=False)` with `_esc()` wrapping each value. Is `html.escape()` sufficient here, or should we switch to `sanitize=True` (which would strip our `<br>` tags)?

2. The `_has_bib_extended` detection runs a `SELECT ... LIMIT 0` against the bibliography table. If the table doesn't exist at all (very old sidecar), this would fail inside the outer try/except that also catches connection failures. Is this safe?

3. `CURRENT_LANG` is read once at dialog construction time. If the user toggles language while the dialog is open, it won't update. Acceptable?
