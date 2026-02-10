# Help Content Update Plan

**Purpose:** Expand and update help content in both web (`web/pages/help.py`) and desktop (`Help.html` + `genizah_app.py:HelpDialog`) apps to cover all features from phases 1-17.

**Created:** 2026-02-10
**Status:** Planning

---

## Current Help Coverage

### Web Help (`web/pages/help.py`)
Bilingual (EN/HE) NiceGUI page at `/help`. Sections:
1. Introduction: How it Works
2. Search (modes: Exact, Variants, Fuzzy, Regex, Title, Shelfmark)
3. Parallels Search (chunking, parameters, filter text, cross-paragraph)
4. Browse Manuscript
5. Lists
6. Exporting Data
7. Contact

### Desktop Help (`Help.html` loaded by `HelpDialog`)
Bilingual HTML file with `<!-- START_LANG_EN/HE -->` markers. Sections:
1. Introduction
2. Search Tab (modes, options, settings)
3. Composition Search (mechanism, parameters, cross-paragraph)
4. Browse Manuscript
5. Lab Mode (parameters, deep scan)
6. Settings & About
7. Exporting Data

### Desktop Inline Help (`genizah_app.py`)
- `get_search_help_text()` -- brief search modes tooltip (line ~12789)
- `get_comp_help_text()` -- composition search tooltip (line ~12793)
- `get_browse_help_text()` -- browse tooltip (line ~12797)

---

## Missing Content (by feature area)

### 1. Responsa Search (Phase 14-17) -- NEW SECTION NEEDED

Neither help system mentions Responsa search. This is the largest gap.

**Content to add (both apps):**

#### Responsa Mode
- How to activate: select "Responsa (R)" from the Mode dropdown, or type `R ` (R+Space)
- What it does: enables Responsa-style syntax for advanced Hebrew/Judeo-Arabic search
- When Responsa is ON, prefix shortcuts (=, ?, ~, /, $, #) are disabled -- the query uses Responsa syntax instead

#### Responsa Syntax Reference
| Syntax | Meaning | Example |
|--------|---------|---------|
| `#word` | Prefix expansion (24 Hebrew prefix forms) | `#שלום` finds ושלום, השלום, בשלום, etc. |
| `word#` | Suffix expansion (25 Hebrew suffix forms) | `שלום#` finds שלומם, שלומו, שלומך, etc. |
| `#word#` | Both prefix and suffix expansion | `#שלום#` finds all combinations |
| `*word` | Wildcard prefix (any characters before) | `*שלום` finds כבשלום, etc. |
| `word*` | Wildcard suffix (any characters after) | `שלום*` finds שלומות, etc. |
| `%word` | Plene/defective variants (ו/י insertion/removal) | `%שלום` finds שלום, שלם |
| `(a/b)` | OR alternatives | `(שלום/שלומות)` matches either |
| `[N]` | Gap of N words between terms | `שלום [3] עולם` = up to 3 words between |

#### Sub-Options Checkboxes
- **Variants**: Enable letter-variant matching (same as Variants mode) on expanded terms
- **Judeo-Arabic (JA)**: Expand words with Arabic definite article אל- (8 forms per word)
- **Flexible Spacing**: Tolerate spaces within words (handles OCR errors where spaces are inserted mid-word)
- **Bidirectional Gap**: Search for terms in both forward and reverse order

#### Combinatorial Explosion Guard
- When a query expands beyond 500 terms, the system automatically downgrades options to keep the search fast
- Downgrade order: variants basic -> off -> JA off -> plene off -> suffixes off -> prefixes off
- A warning notification appears explaining what was turned off
- Tip: use more specific queries or fewer modifiers to avoid hitting the guard

#### Query Builder (Tabular Interface)
- Access: click "Query Builder" button (visible when Responsa mode is active)
- **Components**: 2-4 columns, each representing a search term or group
- **Words**: Enter one or more words per component (multiple = OR alternatives)
- **Per-word modifiers**: Checkboxes for prefix (#), suffix (#), wildcard (*), plene (%), negation
- **Distance**: Set max words between components using spinners
- **Preview**: Live syntax preview updates as you modify
- **Apply**: Generates Responsa syntax and inserts into search field, triggers search
- One-way: changes in the builder update the text field, not vice versa

---

### 2. PGP Integration (Phases 1-12) -- MISSING FROM WEB HELP

The web help mentions nothing about PGP. The desktop Help.html also lacks PGP content.

**Content to add:**

#### PGP Transcriptions & Sources
- When browsing a manuscript linked to a PGP document, scholarly transcriptions appear as version options
- Use the **version selector** to switch between:
  - MiDRASH auto-transcriptions (V0.8, V0.7)
  - PGP scholarly editions (by named scholars)
  - PGP English translations
  - User corrections
- Per-source directionality: Hebrew/Arabic editions RTL, English translations LTR

#### PGP Metadata
- Documents linked to PGP show additional metadata:
  - Document type, date range, description
  - Tags (thematic classification)
  - Related fragments from PGP joins
  - Footnotes and bibliography
- Click any tag to search for related documents

#### PGP Tag Search
- Select "PGP Tags" from the Mode dropdown
- Browse 251 tags organized in 16 categories
- Categories: Document Types, Law & Society, Medicine, Trade, India Book, People, etc.
- Tags display bilingually (Hebrew + English)

#### Virtual Reading Desk
- Access: click "Reading Desk" from a browse page or result with multiple fragments
- Displays all fragments side by side: images in viewer pane, transcriptions in text pane
- Fragment-level sync scrolling
- Per-fragment version selector
- Independent zoom/rotate controls per image

#### PGP Badges in Search Results
- Search results show a PGP badge when the manuscript is linked to a PGP document
- Badge tooltip shows PGP document ID (pgpid)

---

### 3. Library/Holding Institution (Phase ~5.4) -- MISSING

**Content to add:**
- Search results show a library code badge (e.g., "CUL", "JTS", "Oxford")
- Tooltip shows full institution name
- 70+ institutions covered, 99.99% of records
- Library column available in exports

---

### 4. Community Features (Phase ~5.0) -- PARTIALLY COVERED

Web help mentions corrections briefly in Browse. Desktop has no community feature help.

**Content to add:**
- **Discoveries**: Share and explore research findings
- **Comments**: Add scholarly notes to manuscripts (page-specific)
- **Corrections**: Submit transcription corrections with review workflow
- **My Edits & Comments**: Track your contributions

---

### 5. Desktop-Specific Gaps

Features in the desktop app not covered in Help.html:

- **PGP columns and sorting** in search results
- **JoinsDialog** for viewing fragment relationships
- **Responsa mode** in combo dropdown
- **Query Builder QDialog**
- **In-app updates** (v5.5)
- **Remember Me** login (v5.4.1)
- **Library column** in search results

---

### 6. Web-Specific Gaps

Features in the web app not covered in help.py:

- **PGP Tags mode** in search
- **Virtual Reading Desk**
- **Advanced View dialog** (IIIF viewer, inline editing)
- **Responsa mode** and sub-options
- **Query Builder dialog**
- **Mobile responsive** features
- **URL sharing** (search state in URL)

---

## Implementation Plan

### Phase A: Add Responsa Section to Both Apps

**Priority: HIGH** (new feature, users need guidance)

#### Web (`web/pages/help.py`)

1. Add "Responsa Search" to table of contents (both EN and HE)
2. Add new `help-responsa` section after Search section with:
   - How to activate Responsa mode
   - Syntax reference table
   - Sub-options explanation
   - Query Builder walkthrough
   - Explosion guard explanation
   - Tips and examples
3. Update Search Modes section to include "Responsa (R)" in the modes list

**Estimated work:** ~200 lines of Python (100 EN + 100 HE)

#### Desktop (`Help.html`)

1. Add "Responsa Search" to table of contents (both EN and HE sections)
2. Add new `<h2 id="responsa">` section after Search Tab with equivalent content
3. Update Search Modes list to include "Responsa (R)"

**Estimated work:** ~300 lines of HTML (150 EN + 150 HE)

#### Desktop Inline Help (`genizah_app.py`)

1. Update `get_search_help_text()` to include Responsa mode in the modes list
2. Possibly add `get_responsa_help_text()` for the Responsa sub-row

**Estimated work:** ~20 lines

### Phase B: Add PGP Integration Section

**Priority: MEDIUM** (shipped in v5.6, users may already be familiar)

#### Both Apps

1. Add "PGP Integration" section covering:
   - Transcriptions and version selector
   - Metadata display
   - Tag search mode
   - Virtual Reading Desk
   - PGP badges
2. Update Browse section to mention PGP features

**Estimated work:** ~150 lines per app per language

### Phase C: Update Existing Sections

**Priority: LOW** (incremental improvements)

1. Add Library/Holding Institution to Search and Browse sections
2. Add Community Features section (web help only, desktop has limited community features)
3. Update search modes list to include PGP Tags
4. Add URL sharing documentation (web help)
5. Add keyboard shortcuts reference

### Phase D: Desktop Help.html Modernization (Optional)

**Priority: LOW** (functional as-is)

- Consider whether Help.html should be migrated to a different format
- Current QTextBrowser has limited CSS support (no `display: none`, no flexbox)
- Alternative: generate HTML from a shared markdown source

---

## File Locations

| File | Purpose | Languages |
|------|---------|-----------|
| `web/pages/help.py` | Web help center | EN + HE (code-generated) |
| `Help.html` | Desktop help (bundled) | EN + HE (HTML with markers) |
| `genizah_app.py:1912` | `HelpDialog` class | N/A (loads Help.html) |
| `genizah_app.py:12789` | `get_search_help_text()` | EN + HE |
| `genizah_app.py:12793` | `get_comp_help_text()` | EN + HE |
| `genizah_app.py:12797` | `get_browse_help_text()` | EN + HE |
| `genizah_translations.py` | Hebrew translations for inline help | HE |

## Content Sync Strategy

Both apps should have consistent help content. Recommended approach:
1. Write content once in English
2. Translate to Hebrew
3. Implement in both `help.py` (NiceGUI components) and `Help.html` (raw HTML)
4. Inline help texts (`get_*_help_text()`) should be brief summaries pointing to the full help

---

## Responsa Search Quick Reference Card

For users who want a printable reference (could be added as a collapsible section):

```
RESPONSA SYNTAX QUICK REFERENCE

Prefixes:    #word     -> expands with ו,ה,ב,כ,ל,מ,ש + compounds (24 forms)
Suffixes:    word#     -> expands with י,ו,ם,ן,ה,ך,כם,כן,הם,הן... (25 forms)
Both:        #word#    -> prefix + suffix expansion
Wildcard:    *word     -> any characters before the word
             word*     -> any characters after the word
Plene:       %word     -> insert/remove ו/י for spelling variants
OR group:    (a/b/c)   -> matches any alternative
Gap:         [N]       -> max N words between terms
Combined:    #%word*   -> modifiers can be combined

CHECKBOXES
Variants:      Letter substitution matching (ד/ר, ה/ח, etc.)
Judeo-Arabic:  Expand with אל- article (8 forms)
Flex Spacing:  Tolerate spaces within words (OCR errors)
Bidirectional: Match words in either order

QUERY BUILDER
Click "Query Builder" when Responsa mode is active.
Build visually with 2-4 components, then "Apply" to search.

EXPLOSION GUARD
Queries that expand beyond 500 terms are automatically simplified.
A warning explains which options were turned off.
```

---

*Created: 2026-02-10*
