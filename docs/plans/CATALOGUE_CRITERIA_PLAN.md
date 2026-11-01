# Catalogue criteria: find manuscripts by what the catalogue records about them

**Status:** plan **rev. 4** (2026-11-01). Every decision is ruled (D1-D22, R1-R3, §9) and the first
PR split is agreed (§8). Two review rounds by Codex and an Opus subagent (§10): round 1 on rev. 2
REWORK / SHIP WITH FIXES, round 2 on rev. 3 **SHIP WITH FIXES / SHIP WITH FIXES**; rev. 4 folds in
round 2 and the three rulings it led to (D20-D22). D19 was settled by the owner's server check (the
server copy equals the local one) and its production defect has a hotfix ready (§6.5).
**Settled by the owner, 2026-11-01 ("Settled, start 1a"). PR 1a is in progress (§8).** Changes by
revision: §11.
**Origin:** a user asked whether the site can find manuscripts by their script, "a bit like
Friedberg". The owner then asked to list *all* manuscripts with a trait (script, size), judged the
first UI "not so nice" (screenshots, 2026-11-01), and pointed at the FJMS Advanced Search as the
model to beat.
**Scope:** web and desktop, Catalog Browse and "Search only in..." (pre-search filters), and the
public API / MCP.
**Branch:** `script-filter` (not pushed). See §2 for what is committed and what is not.

---

## 1. The problem in one paragraph

The FJMS catalogue in our sidecar (`fist_data/fjms_enrichment.db`) records far more about each
manuscript than our filters expose: script style and region, language and alphabet, vocalization,
columns, lines, physical status, material, attributes (illustration, palimpsest, colophon,
cantillation, massorah), the source of each record, bibliography, and per-image computed
measurements. Until today none of it beyond domain/author/work/date/printed/measurements was
searchable. Adding filters one at a time (as §2 did for script and size) costs about ten files
per filter and crowds the sidebars: the owner's screenshots show the desktop domain tree squeezed
to two rows, and the web sidebar overflowing with a horizontal scroll bar and a cut-off "מקס".

**Proposal:** one list of catalogue criteria, defined once in `shared/`, read by every surface;
one FJMS-style panel (sections on the side, checkbox lists with counts) opened from Catalog Browse
and from "Search only in...", on the web and the desktop.

---

## 2. Where things stand (branch `script-filter`)

### 2.1 Committed: `4a820fb3` — script filters in "Search only in..."

- `shared/fjms_service.py`: `get_filter_sys_ids(script_styles=, script_places=)`;
  `FjmsService.get_script_values`, `script_filter_options`, `script_value_label`,
  `SCRIPT_FILTER_CATEGORIES`; `validate_filter_values` checks both keys.
- Web `/search`, `/parallels`: two multi-selects in the panel (`web/components/filter_panel.py::
  build_script_filter_row`), chips, history (+ summary), Back, Clear All, New Search.
- Desktop "Search only in..." dialog: two checkable lists (`desktop/dialogs_filter.py::
  build_script_lists`); main-window chips; history summary.
- API/MCP: `filters.script_styles`, `filters.script_places`.
- Three Codex rounds: SHIP WITH FIXES -> SHIP WITH FIXES -> SHIP.
- **D11:** never released on its own; PR 1 replaces its controls. Its keys never shipped, so they
  need no legacy adapter.

### 2.2 Not committed: Catalog Browse "Script and size"

Working tree, tested (25 new tests, 811 related tests passed; live-checked):
- `shared/fjms_service.py`: `traits` on `get_browse_results` / `get_browse_library_facets`;
  `_script_conditions` / `_measurement_conditions` now shared by browse and `get_filter_sys_ids`
  (the measurement SQL moved there unchanged); `clean_traits`, `BROWSE_TRAIT_KEYS`.
- Web `/catalog-browse`: a "כתב ומידות" card (script style/place, width, height, lines, measured
  material), chips, URL params, session restore, handoff to `/search` (all) and `/parallels`
  (script only).
- Desktop Catalog tab: "Script and size..." button -> `CatalogTraitsDialog`; chips, workers, session.
- **A real fix that stands on its own:** removing any chip on web Catalog Browse left the removed
  filter in the address and the saved session (the refresh's `ui.run_javascript` ran in the
  deleted chip's context and raised). Now routed through the page's client; pinned by
  `tests/render_smoke/test_catalog_browse_traits_render_smoke.py`.
- **D10:** not committed separately; it becomes the first commit of PR 1a's branch, staged by path
  (the tree also holds unrelated untracked files: `pgp_data/*.csv`, `antiochus.txt`,
  `names10.csv`, `m12s28.csv`, a stray file named `--help`). Until then nobody stashes, resets or
  checks out over this working tree. Its URL params (`material` = measured material, `lines` =
  measured average) never shipped and are NOT read as legacy by the new address code.

Known defects of 2.2 (the owner's review): web card too wide (horizontal scroll, "מקס" cut);
desktop left column over-full (the domain tree has no minimum height inside its scroll area, so it
collapses when the column overflows — that predates 2.2, which added one more row); "מקום כתב" is
an awkward label (FJMS: "כתב" | "אזור"); its Material uses the *measured* material (17,804
manuscripts) where FJMS uses the catalogue's (136,546).

---

## 3. Today's filters, by surface

| Surface | Filters |
|---|---|
| Web `/search` "Search only in..." | Include/Exclude toggle (domains/authors/works), domain, author, work, date range, exclude printed, catalogue text (all/any/not), script style/place, measurements (width, height, lines, line height, text density, measured material) |
| Web `/parallels` "Search only in..." | same, without measurements |
| Web post-search (`/search`) | domain filter, library (show/hide), printed (all/hide/only), PGP (all/only/hide), measurement post-filters |
| Web Catalog Browse | domain tree, availability (PGP, scholarly transcription), library, author, work, date (+centuries, include undated), catalogue text; + script/size card (uncommitted) |
| Desktop "Search only in..." dialog | domain tree, author, work, date (+undated), type (printed 3-way), catalogue text, measurements, script lists |
| Desktop Catalog tab | domain tree, author, work, date (+centuries, undated), text, PGP/editions, library; + "Script and size..." (uncommitted) |
| API / MCP `filters` | domains, authors, works, library (+mode), materials (catalogue `FragmentMaterial`, include), script_styles, script_places, date_from, date_to. No measurement keys (`FiltersModel` forbids extra fields, `web/search_api.py:129`) |

Today's measurement meanings (`shared/fjms_service.py:149-206`): width and height = the catalogue
figure (MAX over cataloguers) where there is one, else the measured MAX; lines, line height and text
density = measured per-manuscript averages; measured material = the most common `extra_info` value.
All of them start `FROM catalog`, so uncatalogued manuscripts never match.

Each filter today is threaded by hand through: the shared lookup, web state + storage keys +
history + run snapshots + chips + Clear All + handoff, the desktop dialog + chips + summary +
session, and the API model. That is the cost §6 removes.

---

## 4. The FJMS Advanced Search (from the owner's screenshots)

Left: a list of sections; right: the section's controls; a ">>" button records the step, "Search"
runs all steps. Sections, with the Hebrew names visible in the screenshots:

| # | English | Hebrew | Controls |
|---|---|---|---|
| 1 | Collection | אוסף | (not shown) |
| 2 | Titles (Alphabetical) | חיבורים (לפי סדר הא"ב) | (not shown) |
| 3 | Domain and Title | תחום וחיבור | (not shown) |
| 4 | Author | מחבר | (not shown) |
| 5 | Period of Copy | תקופת העתקה | Between __ and __; "Or: period of copy unknown" |
| 6 | Language and Script | שפה ואות | two checkbox lists: Language (scrolling), Type of Script |
| 7 | Script Style | כתיבה | two lists: Region of Script (אזור) and Style (כתב), incl. Unknown |
| 8 | Vocalization | ניקוד | Exists / None radio + types (Arabic, Babylonian (upper), Babylonian, Diacritic, Lower punctuation, Mixed, Palestinian, Tiberian, unspecified, Unknown) |
| 9 | No. of Columns | מספר טורים | Between __ and __ columns; "Or: number of columns unknown" |
| 10 | Range of Lines | טווח שורות | Between __ and __ lines; "Also by computer generated data"; "Or: number of lines unknown" |
| 11 | Physical Status | מצב פיזי | long checkbox list (Rip on left … Worn out, Unknown) |
| 12 | Material | חומר | Cloth, Leather / Skin, Paper, Papyrus, Printed, Vellum, Unknown |
| 13 | Attributes | תכונות | "Only if the fragment is/has": Palimpsest, Illustration, Colophon, Cantillation |
| 14 | Information Source | מקור המידע | (not shown) |
| 15 | Additional Conditions | תנאים נוספים | "Only if the chosen shelfmarks also have (in this site)": Image(s); Identifications [at least 1]; Bibliographies [more than] [0]; Transcription(s); English Translation(s); Hebrew Translation(s); Catalog Scans |
| 16 | Computer generated data | ? | From–To: Resolution (dpi), Dimension width/height (cm), No. of lines, Margin right/left/top/bottom (cm), Average Line Height-Text / -Total (mm), Average Text Density (per 10 cm) |
| 17 | Bibliographical References | ? | From year / To year, Publication Type, Publication Name, Author Name, Type of mention: Discussed / Has Transcription / Has Translation / Has Image / Just Mentioned |

The Hebrew names of 16 and 17 were not in the screenshots (D9: ours until the owner sends FJMS's).

What FJMS does less well, and we can do better: no counts next to values; no live total; the
recording step (">>" then "Search") is easy to miss; long lists without a search box; no way to
see how much of the corpus a section covers; criteria cannot be combined with a text search or
shared as a link.

---

## 5. What our data holds for each FJMS section

Distinct manuscripts in the local sidecar (`fist_data/fjms_enrichment.db`, meta version 5.0.0,
226,555 catalogued manuscripts), read-only queries 2026-11-01, corrected after the review (§10).
"Any record" = a manuscript matches when any of its catalogue records gives the value (it has 1-8+
records).

| FJMS section | Our source | Coverage | Notes |
|---|---|---|---|
| Collection | `csv_bank` library codes (existing library filters) | all | exists on every page, but as different controls (pre-search on Catalog Browse, post-search on `/search` and `/parallels`); to unify |
| Titles / Domain and Title / Author | `domains`, `genizah_titles`, `genizah_persons`, catalog Author/GenizahTitleId | — | done (domain tree, author, work) |
| Period of copy | `catalog.CopyDate`, `CopyToDate` | **13,121** with a year > 0; 10,565 of them with a span to a later year | 17,285 have any text, incl. the sentinels `0` (3,756) and `-99` (464); 421 have only a `CopyToDate` (14 of them above 100); 662 records (325 manuscripts) end before they start (`1027-8`); 5,927 have a dated and an undated record |
| Language | `catalog_fields` `GenizahLanguages` | 137,485 | 25 values (§A.1) |
| Type of script (alphabet) | `catalog_fields` `TypeOfScript` | 100,866 | 10 values |
| Script style / region | `TypeOfScriptStyle` / `TypeOfScriptPlace` | 45,125 / 36,937 | done (§2) |
| Vocalization types | `catalog_fields` `TypeOfVocalization` | 38,981 | 12 values |
| Vocalization extent | `catalog.PartVocalCode` (code table 36) | **58,365** | per value: Exists 20,282 · Full 18,944 · None 16,703 · Partial 13,208 (+ 23 Unknown / Cannot be determined); 10,262 manuscripts carry two or more extents, 1,410 "None" in one record and Full/Partial/Exists in another |
| No. of columns | `catalog.NumColumn` (text) | 19,173 with a positive number (21,014 with any text) | `1`, `2`, `2-2`, `1-1`, `1,2`; `0` on 1,915 manuscripts (Lieberman, Danzig), of which 74 also have a positive value in another record |
| Range of lines | `catalog.NumRow` (text) | 109,874 with a positive number (110,785 with any text) | 18,232 dash forms, 11,811 comma forms; 136 records with an en dash, ~24 with a soft hyphen (`19\xad-24`), a few irregular (`5-`, `7,3,`, `15 (-16)`, `8-9-12`, `0-25`, `26-0`); `0` only on 911 manuscripts; 59 records with a number over 80 |
| Physical status | `catalog_fields` `FragmentStatus` | 106,370 | 36 values (§A.4) |
| Material | `catalog_fields` `FragmentMaterial` | 136,546 | Paper 96,761 · Vellum 31,400 · Printed 12,451 · Leather 586 · Papyrus 26 · Cloth 8 |
| Attribute: illustration | `catalog.IllustrationCode` (table 38 yes/no) | **3,311 yes** (11,337 no; Unknown 4, Cannot be determined 7) | |
| Attribute: palimpsest | `catalog.PalimpsestCode` (table 38) | **86 yes** (2,132 no; 14 / 16) | |
| Attribute: colophon | `catalog.Colophon` (text) | 710 | has text = yes |
| Attribute: cantillation | `catalog.CantillationCode` (table 36) | Full 16,261 · None 8,761 · Exists 1,904 · Partial 849 | |
| Attribute: massorah | `catalog.Massorah` (**bare code**, table 40) | **6,045** | Parva 3,079 · Magna+Parva 2,414 · Magna 506 · Massora 48 (per-value counts overlap) |
| Attribute: autograph | `catalog.Autograph` (0/1) | 31 | |
| Information source | `catalog.SourceName` / `SourceNameHeb` | all; 260 names | top: "Instatution" (sic; see D19) 140,328 · NLI Aleph Catalog 97,449 · Lieberman 26,187 · personal handlist 23,954 · Inventory 19,902 · Nuscha 14,715 … group by kind (team / catalogue / institution / handlist) with `_TEAM_NAMES`, `ref_catalogs` |
| ~~Additional conditions~~ | — | — | **dropped (D12)**, backlog GS-283 |
| Computed data | `computed_measurements`: 434,369 rows, 311,083 distinct images (`FGP`), **159,032 manuscripts**; page width/height, lines, margins ×4, written width/height, avg line height (text), text density, DPI | 159,032 | per image, indexed by AlmaId (`idx_cm_alma`); FJMS "line height total" has no column here. `manuscript_measurements` holds per-manuscript aggregates (no margins, **no `avg_line_height_mm`** here or in production) |
| Catalogue sizes | `catalog_sizes` (per cataloguer record; flags `Flag_WH_Swap`, `Flag_Unit_Error`) | 103,338 (aggregate) | 11,501 manuscripts have cataloguer widths more than 1 cm apart |
| Bibliographical references | `bibliography`: TitleYear 1844-2026 (103,232 ms), RunningTitle (2,280 publications), ArticleAuthorEng (1,480; 1,546 with Hebrew fallback), MentionType (Mentioned 126,324 · Discussion 42,103 · Index 5,558), TranscriptionType, TranslationType | 133,088 | no publication-type column (code table 37 exists, no column carries it) |
| (beyond FJMS) joins | `joins` (48,780 rows, 20,136 ms): Physical 5,545 ms · Codex 4,728 ms … | 20,136 | "has a join" / join type |
| (beyond FJMS) mentions | `catalog_mentions`: Personalities 10,721 · Places 925 · Creations 654 · Dates 458 | 11k | later |
| (beyond FJMS) creation type | `catalog.CreationTypeCode` (table 22): Original 54,055 · Commentary 1,304 · Translation 767 · Extracts 463 | 55,416 | later |

**Coded columns.** Most coded catalog columns store `sign(code) × (|code| × 1000 + FCDTableId)`:
`PartVocalCode = 4036` is table 36 code 4 ("Exists / קיים"), `-1036` is code -1 ("Unknown").
Labels in both languages come from `code_values (FCDTableId, Code, EngDesc, HebDesc)`. Checked for
tables 22, 36, 38, 315. **Exceptions:** `Massorah` stores the bare table-40 code (1-5);
`Autograph` is 0/1.

**Hebrew value labels** come from the catalogue itself (`catalog_fields.FieldValueHeb`,
`code_values.HebDesc`) — the same FIST database FJMS shows, so our Hebrew matches theirs.

### 5.1 Facts the rulings rest on (read-only queries, 2026-11-01)

| Fact | Figure | Bears on |
|---|---|---|
| Corpus (`libraries.csv`, all `99…`) | 255,723 sys_ids; 226,554 catalogued; **29,169 with no catalogue record**; 1 catalogued id not in the corpus (990002047800205171) | D14, D16 |
| Uncatalogued corpus manuscripts with measurements | 4,089 with an unflagged photograph (4,202 with any); 13 with an unflagged catalogue size; 2,697 with a measured material | D6 × D14 |
| Records that disagree | two or more materials 4,687 (4,681 without Unknown/Cannot be determined): Paper+Vellum 2,965, Paper+Printed 1,634; two or more regions 1,315 (1,314); 1,258 "no vocalization" in one record but a vocalization type in another | D4 |
| Size sources (width) | catalogue 103,338 · measured 157,347 · either 179,639 · both 81,046; width 10-15: today 66,221 · catalogue any record 41,074 vs MAX 37,994 | D6, D4 |
| Line sources | catalogue 109,874 · measured 157,347 | D6 |
| Material sources | catalogue 136,546 · measured 17,804 (11,121 of them with no catalogue material) | D6 |
| Measured lines 20-25 | any unflagged photograph 26,337 · manuscript average 16,394 (today, from `catalog`: 15,796); 934 match by average with no page in range, 10,878 have a page in range but an average outside it | R1, R2 |
| Width ≤ 10 and height ≥ 30, measured | one measured page (row) 469 · one photograph (`FGP`, may hold several fragments) 510 · any pages 569 | R1 |
| Lines 20-25 and width 10-15, measured | one measured page 11,676 · any pages 12,964 | D21 |
| "Region: all except Oriental" | 225,375, of which 218,786 have no region recorded (189,617 catalogued + 29,169 uncatalogued) | D15 |
| Dates 1000-1200 | start year 4,561 · span overlap 4,702 (reversed spans read as the start year; with the recorded end 4,697); "+ include undated" today 220,832 · D18 per manuscript 218,135 + 29,169 = 247,304 (2,732 leave, 29,205 join) | D18 |
| Line/column parsing (D7) | 244,895 intervals; build 0.96 s; query 0.04 s (vs 0.45 s parsing per query) | D13 |
| Existing indexes | `catalog_fields(AlmaId)`, `catalog_fields(FieldCategory)`; `computed_measurements(AlmaId)`, `(FGP)` | §6.5 |
| Lookup times today | one list value 0.06-0.27 s; a 36-value facet 0.27 s; per-image range scan 0.11 s | §6.5 |
| 'Instatution' rows | 267,481 + 47,809 `catalog_free_desc`, in the local file AND on the server (owner's check: identical size, meta and counts). The April fix rewrote 267,104 + 47,800; the 2026-05 re-export lost it. Re-applied to a copy: 100% resolved, 30,628 manuscripts regain a named source | D19 |

---

## 6. Design (rev. 3)

### 6.1 Rules

1. **One definition per criterion** (`shared/catalog_criteria.py`): key, section, labels he/en,
   control kind, sources, SQL, value list. Every surface and the API read it. Adding a criterion
   = one entry + its test.
2. **Lists:** the ticked values are alternatives (OR). Every list has an "only these / all except
   these" toggle (D15). "All except S" = the universe minus what "only S" gives, so it includes
   manuscripts with nothing recorded unless "Not recorded" is itself ticked; the live total then
   says "of which N not recorded" (e.g. "all except Oriental": 225,375, of which 218,786 not
   recorded). Ranges have no toggle. Sections, and the lists within a section, combine with AND,
   and with the older filters (rule 7).
3. **Any record (D4)** for catalogue values, including catalogue sizes: each unflagged
   `catalog_sizes` record (`Flag_WH_Swap` and `Flag_Unit_Error` empty, as today's aggregate) is
   tested on its own, not the MAX. Width and height set together must hold on one record.
   Consequence, stated in the panel's help: "all except Vellum" also drops the 2,965
   paper-and-vellum manuscripts.
4. **Measured values per page (R1, D21):** the unit is one **measured page** — one unflagged row of
   `computed_measurements` (all four flags 0), i.e. one fragment/side of one photograph (a
   photograph can hold several). Every measured condition the user sets, in any section (the
   measured half of Lines, Material excluded), must hold on **one** measured page.
   - Measured-only ranges (written area, margins, line height, density, DPI) are always tested on
     that page, also when width/height are matched from the catalogue.
   - Width and height in "catalogue" mode hold together on one unflagged `catalog_sizes` record;
     catalogue lines on one catalogue record.
   - "Either" for a criterion: a manuscript matches if, for each "either" criterion, its catalogue
     half or its measured half can be chosen so that all measured choices plus the measured-only
     ranges hold on one measured page and the catalogue choices hold on their records. Example:
     width 10-12 (either) + left margin 1-2 cm: `990001261770205171` has catalogue width 11.5 but no
     page with that margin, so it fails.
5. **The universe is the corpus (D14, D16):** the 255,723 corpus sys_ids. Catalog Browse always
   lists all of them, **catalogued first, then the uncatalogued, each in sys_id order (D22)**: the
   29,169 uncatalogued appear with shelfmark and library from `libraries.csv` and "no catalogue
   record".
6. **Not recorded** follows the section's chosen source: no catalogue record (catalogue), no
   photograph (measured), neither (either) gives a usable value. It is decided per manuscript: a
   record of `0` is not a usable value (D7), but a manuscript with `0` in one record and `1` in
   another has a value (74 such for columns). Every list ends with the recorded "Unknown" /
   "Cannot be determined", then "Not recorded (N)" (D5) — **except Attributes, which list only
   what is present** (owner, D5 follow-up). Ranges get "or: not recorded".
7. **Older filters and uncatalogued manuscripts (D17):** filters that pick something in (domain /
   author / work include, catalogue text all/any) cannot match an uncatalogued manuscript; filters
   that leave something out (domain / author / work Exclude, catalogue text "not", hide printed) let
   it through; the desktop's "only printed" is an inclusion; library, PGP and transcription
   availability apply as to any manuscript. The domain/author/work Include/Exclude toggle does not
   touch criteria. Consequence: an old saved Exclude-mode search widens by up to 29,169 when
   reopened. The printed control stays beside Material's "Printed" value (same catalogue field).
8. **Counts everywhere:** each value shows its manuscript count (corpus counts, cached per
   process); the panel shows the live total; each section its coverage ("recorded for 36,937 of
   255,723"). Library facets stay live and honour every other active filter, as today
   (`shared/fjms_service.py:2856`): JTS shows 43,722 with no filter, 25,477 with catalogue Paper.
9. **Fail closed, on every path.** A criterion the open sidecar cannot answer raises
   `FilterUnavailable`, never "no restriction" — including on Catalog Browse and its facets, which
   today catch every error and show 0 (`shared/fjms_service.py:2834`, `:2928`), as do their callers
   (`web/pages/catalog_browse.py:549`, `genizah_app.py:709`); PR 1a changes all four to show an
   error.
   A criterion that needs `criteria_numbers` (catalogue lines or columns, in any source mode, and
   their "Not recorded") is unavailable **as a whole** on a 5.0.0 file — never evaluated by its
   measured half alone — whether it comes from the panel, a restored session, an address or the
   API; the panel disables those controls with a note (K-16 three states). No corpus list -> "Not
   recorded", "all except" and Catalog Browse raise. The corpus list loads in a background thread
   after a restart (`web/main.py:3483`; `_csv_bank_loaded`, `shared/metadata_manager.py:309`): the
   provider waits for it and never hands over a partial list.

### 6.2 Sections

Phase 1 (D3, ten sections; FJMS's Hebrew names, D9):

| Section (he / en) | Controls | Source |
|---|---|---|
| תקופת העתקה / Period of copy | from–to (+ centuries), "or: not recorded". **Replaces today's date range and "include undated" on every surface (D18).** A record's span `CopyDate..CopyToDate` matches when it overlaps the range (1000-1200: 4,702; start year only: 4,561). An end before the start (662 records, e.g. `1027-8`) is read as the start year alone; an end with no start (421 manuscripts, 14 above 100) counts as no date. Results show the span ("944–1100"), not `MAX(CopyDate)` (`shared/fjms_service.py:2801`) | `CopyDate` > 0; `0` / `-99` = no date |
| שפה ואות / Language and script | two lists: language (25), alphabet (10) | `catalog_fields` |
| כתיבה / Script | two lists: כתב / Style (15), אזור / Region (14) — replaces "מקום כתב" | `catalog_fields` |
| ניקוד / Vocalization | two lists: extent (Full, Exists, Partial, None) and type (12) — replaces FJMS's Exists/None radio | `PartVocalCode`, `TypeOfVocalization` |
| מספר טורים / Columns | from–to, "or: not recorded" | catalogue only (`criteria_numbers`, D7) |
| טווח שורות / Lines | from–to; source **either (default) / catalogue / measured** (D6); "or: not recorded" | `criteria_numbers` ∪ per-page `Num_Lines` (rule 4) |
| מצב פיזי / Physical status | list (36) with a filter box | `catalog_fields` |
| חומר / Material | list; source either / catalogue / measured (D6). Measured Paper/Vellum/Papyrus merge into the catalogue values by name; measured-only Mix (5) and Wood (2) are values of their own | `catalog_fields` per record ∪ `manuscript_measurements.material` (the most common `extra_info` value; only 1 manuscript has two, so the aggregate is enough) |
| תכונות / Attributes | list, **any of (OR, R3)**, with the toggle: illustration (3,311), palimpsest (86), colophon (710), cantillation (Full / Exists / Partial), massorah (6,045), autograph (31); yes-only, no Unknown / Not recorded | coded columns |
| נתונים ממוחשבים / Measured data | width, height: source either / catalogue / measured (D6); measured only: written width/height, margins ×4, line height (text), text density, DPI; one photograph for all (rule 4) | `catalog_sizes` ∪ `computed_measurements` |

Every list has the toggle (rule 2; for Attributes "all except illustration, colophon" = has
neither). Every list except Attributes ends with Unknown, Cannot be determined (where recorded) and
Not recorded (rule 6).

Phase 2: אוסף / Collection (the existing library control, moved in); מקור המידע / Information
source (grouped). Phase 3: הפניות ביבליוגרפיות / Bibliography. **Dropped (D12):** תנאים נוספים /
Additional conditions — backlog GS-283.

Domain / author / work stay where they are (tree and pickers); the panel shows them read-only in
its summary.

### 6.3 UI

- **One button** where the script row / measurements group / traits card / date range are today:
  **"מאפייני כתב היד" / "Manuscript features"** (D9) with a badge of active criteria. Chips under
  it, one per list or range ("כתב: מרובע, בינוני", "אזור: הכול חוץ ממזרחי", "שורות 20-25"),
  each removable.
- **The panel.** Web (D2): a window over the page, full screen on a phone, section list beside the
  controls (RTL-aware), live total at the bottom with Clear section / Clear all / Apply. Desktop: a
  window with the same layout (section list + stacked pages). Long lists get a filter box; values
  sorted by count, then Unknown, Cannot be determined, Not recorded. One line under the total:
  "These criteria cover all 255,723 manuscripts. 29,169 have no FJMS catalogue record: they match
  'Not recorded', 'all except' and measured data (13 also have a catalogue size)."
- **Same panel** from Catalog Browse (lists manuscripts) and from "Search only in..." (restricts
  a text search), on web and desktop (D1). A browse list hands its criteria to a search unchanged.
- **Catalog Browse lists all 255,723 (D16):** with no criteria too; catalogued first, then the
  uncatalogued (D22), with one paging over both; uncatalogued rows show shelfmark, library and "no
  catalogue record" in place of title/date/domain; library facets count them; the domain tree gets
  a "No catalogue record (29,169)" entry beside "Unclassified" so it adds up to the list total
  (`web/pages/catalog_browse.py:1986`).
- **Post-search measurement filters (D20)** — web `/search` (`web/pages/search.py:4314`,
  `get_measurements_batch` at `shared/fjms_service.py:3740`) and the desktop's
  `_post_measurement_filters` — switch to the panel's rules (either source, per page, rule 4)
  through the same shared code, in 1b and 1c.
- **Desktop left column:** the traits button and the date controls fold into the panel, so the
  column loses rows; in addition the domain tree gets a minimum height (~240 px) so it never
  collapses, and the lower sections scroll.

### 6.4 State, address, API

- One value per surface: `criteria = {key: value}`. Lists: `{"values": [...], "mode":
  "only"|"except", "source": "either"|"catalogue"|"measured"}` (source only where the section has
  one), with the reserved value `"_none"` (Not recorded). Ranges: `{"min", "max", "not_recorded":
  bool, "source": ...}`, either bound open. Web state field `filter_criteria`, storage key
  `{pfx}_filter_criteria`, history/run snapshots carry it; desktop `pre_search_filters['criteria']`
  and the Catalog tab's `_catalog_criteria`.
- **Address (D8), one param per criterion, its whole state in the value:**
  - list: `style=Square,Semi-Cursive`; all except: `region=!Oriental`; Not recorded: `_none`;
    source prefix `cat:` / `meas:` (absent = either), always before `!`:
    `material=cat:Vellum,_none`, `material=meas:!Paper`.
  - range: `lines=20..25`, open `lines=..25` / `lines=20..`, years `period=-1200..-1000`; source
    prefix as above (`lines=meas:20..25`); "or not recorded" as a trailing `,_none`
    (`columns=2..2,_none`); "only not recorded" as `_none` alone (`columns=_none`).
  - `!` on a range, an unknown param or a malformed value is dropped and the page says so.
  - Values are the catalogue's English values (none contains a comma or starts with `!`).
  - No legacy params: 2.2's never shipped.
- **Saved state from before this release (R2, owner: "the least work"):** old measurement filters
  (`width_min` … `text_density_max`, `measurement_material`) are **not restored** — no adapter, no
  note; a reopened search shows no measurement chip. So that what is shown is what runs, the
  stored result rows of such a search are dropped too and it re-runs: the desktop session keeps up
  to 5,000 rows (`genizah_app.py:35878`, `:36177`), history snapshots (`:35467`), the web's Back
  fallback 250 (`web/search_runs.py:11`). Saved date ranges and "include undated" are read into
  Period of copy (D18). 2.1's script keys never shipped. This applies to **saved** state only:
  today's controls keep working, with today's meanings, until 1b/1c replace them (§8).
- **API/MCP (PR 2):** `filters.criteria` (same shape); old keys still accepted: `materials` ->
  material, catalogue source; `date_from` / `date_to` -> period; `script_styles` /
  `script_places` were never released and are replaced. An old key and `criteria` for the same
  criterion in one request -> 400 naming both (no silent precedence). `date_from`/`date_to` change
  from start year to span overlap (1000-1200: 4,561 -> 4,702); `SEARCH_API.md` says so. A
  vocabulary endpoint `GET /api/criteria`
  (sections, keys, values, labels, counts) so callers — and the MCP's "documented values only"
  rule — can discover values instead of guessing. Fix the `materials` examples (`'paper'`,
  `'parchment'` give a 400 today, `web/search_api.py:169`).

### 6.5 Data layer

- **Hotfix first (D19, owner 2026-11-01: "now").** The April 'Instatution' fix was re-applied to a
  copy, `fist_data/fjms_enrichment.hotfix.db` (still 5.0.0; 1,618,018,304 bytes; sha256
  `031c7bb6…851079e`; checks in the tracker fragment). Once the owner uploads it, it is the base of
  5.1.0 everywhere.
- **Data file 5.1.0 (D13).** One build sequence, in this order:
  1. the base: the fixed 5.0.0 file above;
  2. on a fresh export only: `scripts/export_fist_enrichment.py`, then
     `scripts/import_measurements.py` (the measurement tables come from the second);
  3. **after** any export: the 'Instatution' fix (`scripts/fix_instatution_sources.py`, needs
     `fist_data/FIST.db`; it opens writable connections even on a dry run, so it runs on a copy).
     The export rebuilds `catalog` and `catalog_free_desc` and undoes the fix — that is how it was
     lost in May (`scripts/export_fist_enrichment.py:1756`);
  4. the criteria step (`scripts/migrate_fjms_criteria.py`, new): `criteria_numbers(AlmaId, key,
     lo, hi)` from `NumRow` / `NumColumn` by the D7 rules, index `(key, lo, hi)`; index
     `catalog_fields(FieldCategory, FieldValue, AlmaId)`; `manuscript_measurements.
     avg_line_height_mm` through `migrate_add_line_height` (revives today's dead Line Height
     post-search filter in production); `meta.version` -> 5.1.0;
  5. validation: 0 'Instatution' rows, row counts of every other table unchanged, `PRAGMA
     quick_check`, `criteria_numbers` count.
  The criteria step is a no-op on a valid 5.1.0 (it checks the table and indexes) and refuses any
  other version than 5.0.x / 5.1.0. The sequence goes into `docs/guides/DEPLOYMENT_TECHNICAL.md`.
  The server copy goes up before the code (owner); the desktop installer bundles the file.
- **D7 parse rules, complete:** strip soft hyphens; any dash (`-`, en, em) marks a range; a
  parenthesised part is read as a number (`15 (-16)` -> 15-16); a chain `a-b-c` is min..max; trailing
  separators are ignored (`5-` -> 5, `7,3,` -> 7 or 3); zero endpoints are dropped (`0-25` -> 25,
  `26-0` -> 26); `0` alone is no value; numbers over 80 are kept. A golden-table test covers every
  distinct non-plain value in the file (about 2,850 for lines, 24 for columns; the table is
  generated from the file, not pinned by count).
- **Desktop data-file choice:** `FjmsService.__init__` opens the `%LOCALAPPDATA%` copy before the
  bundled one without comparing versions (`shared/fjms_service.py:942`). PR 1a: open it only when its
  `meta.version` is at least the bundled one. (The `data-latest` updater that fills it never went
  live; if it ever does, 5.1.0 is published there too.)
- **Universe:** the corpus sys_ids come from the metadata both apps already load from
  `libraries.csv` (`CORPUS_SYS_ID_RE`), passed into the criteria layer; the FJMS service does not
  read `libraries.csv` itself. A shared `corpus_sys_ids(meta_mgr)` returns the set only once
  `_csv_bank_loaded` is true (rule 9). Catalog Browse pages over a per-connection temp table of
  corpus ids left-joined to `catalog`, catalogued first (D22).
- **Facet counts** (count per value under the other active criteria) are the expensive part;
  phase 1 shows corpus counts (cached), phase 2 live facets if they stay under ~0.5 s. Per-photograph
  ranges scan `computed_measurements` (0.11 s today); add value indexes only if a measured search
  passes 0.5 s.

---

## 7. Code to change

| File | Change | PR |
|---|---|---|
| `shared/catalog_criteria.py` (new) | registry: `Criterion` (key, section, labels, kind, sources), sections order, `clean_criteria`, `criteria_conditions(criteria, universe)` / set algebra for all except and Not recorded, value lists (+ code-table decoding), chip/summary text, address encode/decode | 1a |
| `scripts/migrate_fjms_criteria.py` (new), `scripts/export_fist_enrichment.py`, `docs/guides/DEPLOYMENT_TECHNICAL.md` | the 5.1.0 step and the build sequence (§6.5) | 1a |
| `shared/fjms_service.py` | `get_filter_sys_ids(criteria=, universe=)`; the older filters' universe rule (D17); `_build_browse_conditions(criteria=)`; `get_browse_results` / `get_browse_library_facets` over the corpus (D16, D22) and failing closed; span display for dates; `get_criterion_values(key)`; capability check for `criteria_numbers`; version-aware data-file choice; `validate_filter_values` validates `criteria`; the per-page measured matching shared with the post-search filters (D20); today's date and measurement arguments keep their meaning until 1b/1c; `SCRIPT_FILTER_CATEGORIES` / `traits` / `clean_traits` removed after the surfaces move | 1a |
| `shared/metadata_manager.py` (or a small new helper) | `corpus_sys_ids(meta_mgr)`, waiting for `_csv_bank_loaded` | 1a |
| Browse and filter call sites: `web/pages/catalog_browse.py:383`, `:438`, `:549`; `genizah_app.py:692`, `:709`, `:756`, `:11996`, `:12273`; `web/components/filter_panel.py:585`; `web/pages/search.py:5558`; `web/pages/parallels.py:4893`; `desktop/gui_threads.py:2352`; `genizah_app.py:12900`; `web/search_api.py:483` | pass the corpus; show an error, not 0, when the service raises; a minimal "no catalogue record" label on uncatalogued rows; nothing else of the new UI | 1a |
| `web/components/criteria_panel.py` (new) | the window, the button + badge, chips | 1b |
| `web/pages/catalog_browse.py` | the button replaces the traits card (2.2) and the date controls; `current_criteria`; URL, session, handoff; uncatalogued rows | 1b |
| `web/components/filter_panel.py` | `has_active_filters`, `load_filter_state`, `recompute_filter_count`, `consume_incoming_filters`, `build_filter_summary` on `criteria`; drop `build_script_filter_row` | 1b |
| `web/pages/search.py`, `web/pages/parallels.py` | the button replaces the script row, the measurements expansion and the date range; chips, Clear All, history save/restore, `filter_kwargs`, `_last_filters_applied`, badge count; post-search measurement filters on the panel's rules (D20) | 1b |
| `web/pages/search_state.py` | `filter_criteria`, storage key, `RUN_FILTER_FIELDS`, clear helpers; old dates read in, old measurement keys ignored | 1b |
| `web/main.py` | `/catalog-browse` reads criteria params from the registry | 1b |
| `desktop/dialogs_filter.py` | `CriteriaDialog` (section list + stacked pages); "Search only in..." loses its script, measurement and date groups for the button; `CatalogTraitsDialog` removed | 1c |
| `genizah_app.py` | Catalog tab: criteria button, chips, workers, session, uncatalogued rows, "No catalogue record" tree entry; domain tree minimum height; main-window chips and history summary; `_apply_catalog_handoff_filters`; `_post_measurement_filters` on the panel's rules (D20); stored rows dropped with old measurement keys (R2) | 1c |
| `desktop/gui_threads.py` | `FilterCountWorker` passes `criteria` | 1c |
| `shared/genizah_translations.py` | section names, control labels | 1b/1c |
| `web/search_api.py`, `web/mcp_server.py`, `docs/SEARCH_API.md` | `filters.criteria`, `GET /api/criteria`, old keys, docs | 2 |
| tests | **1a:** registry ↔ SQL for every criterion on a fixture sidecar; browse ≡ search for every criterion; all except = universe − only; Not recorded per source and per manuscript, incl. uncatalogued; one measured page across sections, measured-only ranges mandatory under "either", one catalogue size record (rule 4, with the `990001261770205171` and `990001003810205171` shapes); date normalization (reversed, end-only, spans); D7 golden table; code decoding; address encode/decode round-trip; migration idempotence + refusal; 5.0.0 file -> whole criterion unavailable at the service; browse and its callers fail closed; corpus not yet loaded -> raises, never partial; paging catalogued-then-uncatalogued; version-aware data-file choice; today's controls still give today's results. **1b/1c:** 5.0.0 unavailability through address and session; old dates migrated, old measurement keys and their stored rows dropped; post-search filters ≡ panel; panel render-smoke (web) and Qt dialog tests. **2:** API precedence 400, vocabulary. Mutation checks per call site, as in 2.1/2.2 | each PR |

---

## 8. Phasing (agreed 2026-11-01)

0. **Hotfix (now, owner):** upload the fixed 5.0.0 data file (§6.5). Independent of the PRs.
1. **PR 1a — data and shared core.** Its first commit is today's working tree (2.1 committed +
   2.2 uncommitted, D10), staged by path. Then: the 5.1.0 step and build sequence; the registry;
   the matching rules (any record, per measured page, all except, Not recorded, corpus universe,
   older filters, period spans); Catalog Browse over the corpus, catalogued first, failing closed;
   the version-aware data-file choice; and the small caller changes this needs (§7: pass the
   corpus, show errors, a "no catalogue record" label). Today's date and measurement controls and
   2.2's traits keep working, with today's meanings, until 1b/1c replace them; the new period rule
   arrives with the panel.
2. **PR 1b — web:** the panel, Catalog Browse with the D8 address, `/search` and `/parallels`,
   post-search filters (D20), state migration, render-smoke tests.
3. **PR 1c — desktop:** `CriteriaDialog`, the Catalog tab, "Search only in...", post-search
   filters (D20), session migration, the domain-tree minimum height.
4. **PR 2 — API/MCP:** `filters.criteria`, `/api/criteria`, docs.
5. **Release (D11)** — one release, web and desktop, after 1a-1c and 2; nothing in between is
   deployed. Before it: the 5.1.0 file built from the fixed base (§6.5); the owner copies it to the
   server before the code; the installer bundles it.
6. **PR 3** — phase 2: Collection in the panel, Information source; live facet counts if fast
   enough.
7. **PR 4** — Bibliography section.

Backlog: Additional conditions (GS-283, D12). Tracker: the lost 'Instatution' fix (fragment
`docs/open_issues.d/2026-11-01-fjms-instatution-fix-lost.md`, D19).

---

## 9. Decisions for the owner (my recommendation first; rulings in bold)

| # | Decision | Recommendation |
|---|---|---|
| D1 | One panel for Catalog Browse *and* "Search only in..." on web and desktop? | Yes — **RULED 2026-11-01: yes, one panel in all four places** |
| D2 | Web container: dialog (full-screen on phone) or side drawer? | Dialog with a live total — **RULED 2026-11-01: a window over the page (full screen on a phone), sections beside controls, live total + Apply** |
| D3 | Phase-1 sections | as §6.2 — **RULED 2026-11-01: as proposed (ten sections)** |
| D4 | A manuscript with several records: match if *any* record has the value, or only if all agree? | Any (as today); show disagreement later if wanted — **RULED 2026-11-01: any record** (applies to catalogue sizes per record too, rule 3) |
| D5 | Show recorded "Unknown" / "Cannot be determined" as values? "Or not recorded" for ranges? | Yes to both, Unknown last — **RULED 2026-11-01: yes to both, AND every list also ends with "Not recorded (N)". Follow-up (after review): Attributes are yes-only, without Unknown / Not recorded** |
| D6 | Lines and material: catalogue values by default, computed as an option (FJMS's "also by computer generated data")? | Yes — **RULED 2026-11-01 (revised question): lines, width/height and material match when the catalogue OR the measurement is in range (default), with a per-section "catalogue only / measured only" choice** |
| D7 | Multi-number fields (`19-21`, `22,25`): match if any number is in range? | Yes — **RULED 2026-11-01 (revised): `a-b` is the interval a..b, `a,b` is a or b; a record matches when any part overlaps the asked range; `0` = not recorded; numbers over 80 kept as written** (irregular forms: §6.5) |
| D8 | Address format: one compact `c=` param or one param per criterion? | One param per criterion (readable, as 2.2) — **RULED 2026-11-01: one param per criterion** (grammar: §6.4) |
| D9 | Hebrew section names: FJMS's (owner to supply 16, 17) or ours? Button label? | FJMS's where they exist — **RULED 2026-11-01: FJMS's names; 16 = נתונים ממוחשבים, 17 = הפניות ביבליוגרפיות until the owner sends FJMS's own. Button: "מאפייני כתב היד" / "Manuscript features"** (not "Advanced…": the web already has "Advanced Options") |
| D10 | The uncommitted 2.2 work: commit as an interim step, or fold into PR 1? | Commit now — **RULED 2026-11-01: fold into PR 1, no interim commit.** It becomes PR 1a's first commit; never stash, reset or check out over it |
| D11 | Release: web and desktop together? | Yes — **RULED 2026-11-01: one release, web and desktop, after PR 1 and PR 2; the committed 2.1 script row never ships on its own** |
| D12 | Meanings to pin in Additional conditions | Discuss with FJMS examples; phase 2 — **RULED 2026-11-01: the whole Additional conditions section is dropped from this plan; backlog GS-283** |
| D13 | Derived numbers table at service start, or in the sidecar export (needs a deploy)? | Service start first — **RULED 2026-11-01: in the data file now** (parse per query 0.45 s; table 0.96 s once, then 0.04 s). Build sequence §6.5 |
| D14 | Do the 29,169 corpus manuscripts with no FJMS catalogue record count as "not recorded"? | No, catalogued only — **RULED 2026-11-01: yes, include them** (rule 5) |
| D15 | Can a list exclude values ("every style except Square")? | Not in the first release — **RULED 2026-11-01: a per-list "only these / all except these" toggle now** (rule 2) |
| D16 | (review) Catalog Browse: show uncatalogued manuscripts always, or only when a criterion brings them? | Only when a criterion brings them — **RULED 2026-11-01: always, all 255,723** (JTS gains 12,857 "no catalogue record" rows) |
| D17 | (review) Older filters on uncatalogued manuscripts | Pass exclusions — **RULED 2026-11-01: inclusions cannot match them, exclusions let them through** (rule 7) |
| D18 | (review) Period of copy: one date filter or two; how to match | Move into the panel, spans overlap — **RULED 2026-11-01: the date range moves into the panel on every surface; `CopyDate..CopyToDate` overlap; "or not recorded" per manuscript; saved dates read in** |
| D19 | (review) Which copy of the data file is the 5.1.0 base: the local file (lacks the April 'Instatution' fix) or the server's? | Local + re-apply the fix, after comparing counts on the server — **DEFERRED 2026-11-01, then SETTLED BY EVIDENCE the same day:** the owner's read-only check of the server copy matches the local file exactly (1,589,698,560 bytes, meta 5.0.0 created 2026-03-11, 731,354 catalog rows, 267,481 + 47,809 'Instatution' rows; server file saved 2026-05-12). So the base is the local file, and the build re-applies the April fix (§6.5 step 2). Production lacks the fix too (tracker fragment). **Owner: hotfix now** — fixed copy built and checked, upload = owner (§6.5) |
| D20 | (round 2) Post-search measurement filters keep today's meanings beside the panel's | Same meaning as the panel — **RULED 2026-11-01: same meaning as the panel**, in 1b/1c (§6.3) |
| D21 | (round 2) Measured lines and measured size: on the same page? | Same page — **RULED 2026-11-01: every measured value, in any section, on one measured page** (rule 4; lines 20-25 + width 10-15: 11,676 vs 12,964) |
| D22 | (round 2) Catalog Browse order with uncatalogued rows | Catalogued first — **RULED 2026-11-01: catalogued first, then uncatalogued, each in sys_id order** (rule 5) |
| R1 | Measured ranges: any photograph or the manuscript's average? | Any page, one page for combined ranges — **RULED 2026-11-01: any unflagged page; one page for combined ranges** (rule 4: a page = one measured fragment/side, D21 extends it across sections) |
| R2 | Old saved measurement filters when reopened | Convert to the new meaning — **RULED 2026-11-01: "the least work"** — not restored, no adapter, no note (§6.4) |
| R3 | Attributes: any of, or all of? | Any of (OR) — **RULED 2026-11-01: any of (OR)**, help text says "any of" |
| PR | First-PR scope | 1a / 1b / 1c / 2 — **AGREED 2026-11-01** (§8) |

Nothing is open. What remains for the owner: upload the hotfix (§6.5), and say the plan is settled.

---

## 10. Review log

**Round 1 (rev. 2), 2026-11-01** — owner: "Run all decisions through sub agent and Codex." Same
brief to both (`_tmp/criteria_review_brief.md`; Codex log `_tmp/criteria_codex_round1.log`).
Codex: **REWORK**; Opus subagent: **SHIP WITH FIXES**. Both agreed with all fifteen rulings.

Disposition:
- **Adopted into rev. 3:** fact corrections (§5, §5.1); Not recorded per chosen source and per
  manuscript (rule 6); `source` on lists (§6.4); complete address grammar (§6.4); D7 irregular forms
  and zero endpoints (§6.5); a 5.0.0 file makes a whole criterion unavailable on every entry path
  (rule 9); Catalog Browse fails closed (rule 9, verified at `shared/fjms_service.py:2834`, `:2928`);
  version-aware desktop data-file choice (verified at `:942`); catalogue sizes per record (rule 3);
  one photograph for combined ranges (rule 4); "of which N not recorded" for all except (rule 2);
  the build sequence and idempotence (§6.5); no adapters for 2.1/2.2 keys; R2's code description
  corrected (§3); no legacy API measurement keys (§3); the `materials` examples (§6.4); the PR split
  (§8).
- **Put to the owner and ruled:** D16 (Browse universe), D17 (older filters), D18 (dates), R1, R2,
  R3 (the reviewers split: Codex AND, Opus OR; owner: OR), the Attributes exception to D5, the PR
  split. D19 deferred.
- **Not adopted:** Codex HIGH "existing `material=Vellum` links change meaning" — 2.2 never shipped,
  so no such links exist (verified: the params are in no commit). Codex HIGH "a downloaded 5.0.0
  masks the bundled 5.1.0" kept as a fix but downgraded: the `data-latest` updater never went live
  and no such copy exists on the owner's machine.

**Round 2 (rev. 3), 2026-11-01** — brief `_tmp/criteria_review_brief_r2.md`, Codex log
`_tmp/criteria_codex_round2.log`. Codex: **SHIP WITH FIXES**; Opus: **SHIP WITH FIXES**. Both agreed
with D16-D18, R1-R3, the Attributes exception and the PR split.

Disposition:
- **Adopted into rev. 4 (both reviewers unless marked):** the 'Instatution' fix runs after any
  export (§6.5; HIGH, both); measured-only ranges stay mandatory when width/height come from the
  catalogue (rule 4; HIGH Opus, MED Codex); the unit is one measured page, not a photograph (rule 4,
  §5.1: 469 vs 510; Codex); date normalization for reversed and end-only spans, span display (§6.2);
  stored result rows dropped with old measurement keys (§6.4; Opus, verified the cited lines);
  live library facets kept (rule 8; Codex); API old-and-new-key conflict -> 400 (§6.4; Codex);
  `_none`-only ranges and the `cat:!` order in the address (§6.4); the corpus provider waits for
  `_csv_bank_loaded` (rule 9; Opus, verified `web/main.py:3483`, `shared/metadata_manager.py:309`);
  1a carries the corpus wiring, caller error handling (verified `catalog_browse.py:549`,
  `genizah_app.py:709`), a minimal uncatalogued label, and today's controls with today's meanings
  (§7, §8; Codex HIGH); the domain tree's "No catalogue record" entry (Opus); old Exclude-mode
  searches widen (rule 7; Opus); the printed control and Material (rule 7; Opus); the Material
  open check settled (verified: 1 manuscript with two `extra_info` materials); figures 247,304,
  510, 11,676 / 12,964 (§5.1).
- **Put to the owner and ruled:** D20 (post-search filters), D21 (lines and size on one page), D22
  (catalogued first). D19 settled by the owner's server check; hotfix chosen.
- **Not adopted:** Codex's D19 method (compare schemas and distributions) — the owner's check found
  the two copies identical in size, meta and counts, which settles the base.

The reviews have converged (REWORK -> SHIP WITH FIXES on both); a third round is optional.

---

## 11. Changes by revision

**rev. 1 -> rev. 2** (owner rulings D1-D15): §5 corrections and §5.1; rules for all except,
corpus universe and Not recorded; sources per section; Additional conditions dropped; button label;
uncatalogued rows; list modes and `_none`; data file 5.1.0 now; R1-R3 opened; PR 1 from the
working tree; release after PR 1+2.

**rev. 2 -> rev. 3** (review round 1 + D16-D19, R1-R3): see §10. Rules rewritten as nine (§6.1);
Period of copy replaces the existing date controls; Attributes OR and yes-only; measured values per
photograph; Catalog Browse over the corpus and fail-closed; full address grammar; old measurement
filters not restored; the 5.1.0 build sequence with D19 open; PR split 1a/1b/1c/2.

**rev. 3 -> rev. 4** (review round 2 + D19 settled + D20-D22): see §10, round 2. Rule 4 rewritten
(measured page, across sections, measured-only mandatory); rule 5 order; rules 7-9 extended; date
normalization; address details; API precedence; hotfix and the corrected build order; 1a carries its
caller wiring; tests split by PR.

---

## Appendix A. Value lists (local sidecar, 2026-11-01): value | Hebrew | manuscripts

**A.1 Language (`GenizahLanguages`, 137,485):** Hebrew עברית 95,350 · Arabic ערבית 70,660 ·
Aramaic ארמית 11,780 · Persian פרסית 386 · Ladino לדינו 275 · Spanish ספרדית 174 · Coptic קופטית 128 ·
Syriac סורית 72 · Turkish טורקית 69 · Yiddish אידיש 57 · Greek יוונית 43 · Italian איטלקית 41 ·
Cryptic script כתב סתרים 20 · Tartaric טאטרית 19 · Nomina Barbara ברברית 19 · Latin לטינית 15 ·
Unknown לא ידוע 10 · French צרפתית 8 · English אנגלית 8 · Portuguese פורטוגזית 3 · Other אחר 3 ·
Catalan קטלנית 3 · German גרמנית 2 · Slavonic סלבונית 1 · Armenian ארמנית 1

**A.2 Type of script (`TypeOfScript`, 100,866):** Hebrew עברית 88,816 · Arabic ערבית 19,112 · Other
אחר 484 · Coptic קופטית 74 · Greek יוונית 65 · Latin לטינית 49 · Syriac סורית 34 · French צרפתית 8 ·
Unknown לא ידוע 1 · Cyrillic קירילית 1

**A.3 Script style (`TypeOfScriptStyle`, 45,125):** Semi-Cursive בינוני 23,418 · Square מרובע 21,270 ·
Cursive רהוט 3,275 · Rabbinical רבני 2,609 · Naskhi נסכ'י 2,387 · Karaitic קראי 964 · Kufic כופי 35 ·
Nasta'lik נסתעלק 31 · Riqa רקעה 26 · Square Karaitic קראי מרובע 6 · Other אחר 3 · Mirrored Script
כתב ראי 3 · Diwani דיוואני 2 · Unknown לא ידוע 1 · Thuluth ת'לות' 1

**A.3b Script region (`TypeOfScriptPlace`, 36,937):** Oriental מזרחי 30,348 · Spanish ספרדי 4,011 ·
Syrian סורי 1,160 · Yemenite תימני 1,103 · Ashkenazi אשכנזי 593 · North African צפון אפריקאי 361 ·
Italian איטלקי 316 · Byzantine ביזנטי 202 · Persian פרסי 167 · Maghrebi מגרבי 38 · Unknown לא ידוע 10 ·
Other אחר 7 · European אירופאי 4 · Cannot be determined לא ניתן לדעת 2 (FJMS also lists German
גרמני, Western מערבי: no manuscripts here)

**A.4 Physical status (`FragmentStatus`, 106,370):** Torn קרוע 35,032 · Rubbed משופשף 32,855 ·
Mutilated מושחת 28,693 · Stained מוכתם 24,269 · Holes מחורר 20,026 · Missing חסר 19,742 · Defective
לקוי 16,045 · Faded דהוי 13,221 · Injured פגום 9,246 · Fragmented מקוטע 7,644 · Small קטן 7,603 ·
Illegible בלתי קריא 7,313 · Remnant שריד 6,997 · Damaged ניזוק 6,836 · Blurred מטושטש 3,222 · Whole
שלם 2,136 · Erased מחוק 1,928 · Preserved שמור 1,857 · [Almost] Empty [כמעט] ריק 1,229 · Cut נחתך
1,112 · Creased מקופל 524 · Worn out בלוי 442 · good condition מצב טוב 279 · Stuck together מודבק 243 ·
Frayed שחוק 235 · Wormed מתולע 195 · Crumb פירור 170 · Other אחר 41 · Rip on top 31 · Rip on bottom
24 · Tear in upper right 4 · Tear in lower left 4 · Rip on right 4 · Tear in upper left 3 · Unknown 1 ·
Rip on left 1

**A.5 Material (`FragmentMaterial`, 136,546):** Paper נייר 96,761 · Vellum קלף 31,400 · Printed
מודפס 12,451 · Leather / Skin עור 586 · Papyrus פפירוס 26 · Cloth אריג 8 · Unknown לא ידוע 5 ·
Cannot be determined לא ניתן לדעת 3 · Other Material אחר 1

**A.6 Vocalization type (`TypeOfVocalization`, 38,981):** Tiberian טברני 32,847 · Lower punctuation
ניקוד תחתון 3,642 · Babylonian בבלי 2,080 · Arabic ערבי 1,693 · Diacritic דיאקריטי 865 · Palestinian
ארץ ישראלי 617 · Unknown לא ידוע 128 · Babilonian (upper) punctuation ניקוד עליון 71 · unspecified
לא ספציפי 70 · Mixed מעורב 57 · Cannot be determined לא ניתן לדעת 6 · Other אחר 1

**A.7 Coded catalog columns (`code_values` tables):** vocalization / cantillation extent — table 36
(Full מלא, None אין, Partial חלקי, Exists קיים); illustration / palimpsest — table 38 (Yes כן, No
לא); massorah — table 40, stored as the bare code (Massora, Magna, Parva, Magna and Parva,
Cumulative); creation type — table 22 (Original מקורי, Commentary on פרוש ל, Tafsir of תפסיר ל,
Translation of תרגום ל, Extracts ליקוט/תקציר/עיבוד); "is Genizah" — table 315 (Yes 7,184 · No 61 ·
Doubtful 923).
