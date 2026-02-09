# Responsa Search — AI Agent Implementation Brief

> **Purpose**: This document prepares an AI coding agent to implement the Responsa-style search feature in GenizahSearch. Read this FIRST, then consult the numbered documents for details.

---

## Quick Context

**GenizahSearch** is a search platform for the Cairo Genizah — a corpus of ~400,000 manuscript fragments (mostly Hebrew and Judeo-Arabic, 9th–19th century). It has a **web app** (NiceGUI/Python) and a **desktop app** (PyQt6/Python), both sharing the same search engine (`genizah_core.py`).

**The Responsa Project** (Bar-Ilan University's פרויקט השו"ת) is a well-known Hebrew text search tool. Many GenizahSearch users are familiar with its syntax. We want to add Responsa-style search capabilities to GenizahSearch.

---

## What You're Building

A **"Responsa Search" mode** activated by a checkbox, supporting:

| Feature | Syntax | Example | What it does |
|---------|--------|---------|-------------|
| Suffix wildcard | `word*` | `שלום*` (shalom*) | Matches שלומי, שלומו, שלומות... |
| Prefix wildcard | `*word` | `*נדר` (*neder) | Matches הנדר, בנדר... |
| Character pattern | `*a*b*c*` | `*פ*ט*ר*פ*` | Matches אפוטרופוס and variants |
| Grammatical prefixes | `#word` | `#שלום` (#shalom) | Adds ו,ה,ב,כ,ל,מ,ש prefixes |
| OR alternatives | `(a/b/c)` | `(עץ/אילן)` (tree/ilan) | Matches either word |
| Inline alternatives | `a(x/y)b` | `אירו(ס/ש)ין` | Matches אירוסין or אירושין |
| Judeo-Arabic (אל-) | Checkbox | `#כלמה` with JA on | Adds אלכלמה, ואלכלמה, באלכלמה... |
| Flexible spaces | Checkbox | Any query | Makes word boundaries optional (OCR fix) |
| Variants | Checkbox | Any query | Adds Hebrew spelling variants |
| Bidirectional gap | Checkbox | With gap>0 | Searches both word orders |

---

## Architecture Decision: Option II → IIb

The recommended implementation path is:

### Phase 1: Option II (Hybrid Integration) — ~300 LOC
- Parse Responsa syntax into structured components
- Feed components to **both** Tantivy (OR groups with boosting) and Regex (wildcards, patterns)
- Add checkboxes to web and desktop UI
- Add real-time query preview
- **This is the MVP**

### Phase 2: Option IIb (Tabular Dialog) — ~150 LOC additional
- Add a "Query Builder" button that opens a dialog
- Dialog has columns (components) with word inputs and per-component modifiers
- Dialog generates Responsa syntax string → inserts into search field
- One-directional sync (dialog → text field)

---

## Files to Modify

### Core Engine (shared between web and desktop)

| File | What to change | Scope |
|------|---------------|-------|
| `genizah_core.py` | New: `parse_responsa_query()` — parse syntax into components | ~100 lines |
| `genizah_core.py` | New: `expand_grammatical_prefixes(word)` — Hebrew prefix expansion | ~30 lines |
| `genizah_core.py` | New: `expand_judeo_arabic(word)` — Arabic definite article expansion | ~40 lines |
| `genizah_core.py` | Modify: `build_tantivy_query()` — support OR groups from components | ~40 lines |
| `genizah_core.py` | Modify: `build_regex_pattern()` — support wildcards, alternations | ~50 lines |
| `genizah_core.py` | Modify: `execute_search()` — add `responsa_mode` parameter path | ~20 lines |

### Web App UI

| File | What to change | Scope |
|------|---------------|-------|
| `web/pages/search.py` | Add checkboxes (שו"ת, variants, JA, flex spaces) | ~30 lines |
| `web/pages/search.py` | Add query preview label | ~20 lines |
| `web/pages/search.py` | Toggle mode dropdown visibility | ~10 lines |
| `web/pages/search.py` | Wire checkboxes to `execute_search()` | ~20 lines |

### Desktop App UI

| File | What to change | Scope |
|------|---------------|-------|
| `genizah_app.py` | Add Row 3 with checkboxes in `create_search_tab()` (~line 5866) | ~40 lines |
| `genizah_app.py` | Add preview QLabel | ~20 lines |
| `genizah_app.py` | Modify `start_search()` (~line 10936) for Responsa mode | ~20 lines |
| `gui_threads.py` | Add `responsa_options` param to `SearchThread` (~line 25) | ~15 lines |

---

## Key Technical Details

### Search Engine: Two-Phase Architecture

GenizahSearch uses a **two-phase** search:
1. **Phase 1 (Tantivy)**: Fast full-text index lookup — retrieves candidate documents
2. **Phase 2 (Regex)**: Precise pattern matching on candidates — filters and highlights

For Responsa mode, **both phases must be aware**:
- **Tantivy** gets OR groups: `("שלום"^5 OR "ושלום" OR "השלום" OR ...)` — boosted exact terms
- **Regex** gets patterns: `(שלום|ושלום|השלום|...)` with wildcards: `שלום\S*`

### Critical Functions (genizah_core.py)

```
parse_query_syntax()     — line ~4424  — Current prefix detection (?, =, ~, /, #, $)
build_tantivy_query()    — line ~4187  — Builds Tantivy OR clauses with boosting
build_regex_pattern()    — line ~4236  — Builds regex with gap separators
execute_search()         — line ~4461  — Orchestrates two-phase search
```

### Existing Patterns to Follow

The variant system already creates OR groups:
```python
# In build_tantivy_query() — EXISTING pattern:
parts.append(f'({" OR ".join(clean_vars)})')
```

Responsa components should follow the same pattern, just with different expansion logic.

### Combinatorial Explosion Guard

When combining prefixes + JA + variants, term count can explode:
- `#word` = ~10 prefix forms
- `+JA` = ×5 (אל-, ואל-, באל-, פאל-, כאל-)
- `+variants` = ×30

**Hard cap**: `MAX_EXPANDED_TERMS = 500`. If exceeded, auto-downgrade (disable variants first, then JA).

### Regex Separator Patterns

```python
# Gap=0 (adjacent words):
separator = r'[^\w\u0590-\u05FF\']+'

# Gap=N (up to N intermediate words):
separator = rf'(?:[^\w\u0590-\u05FF\']+[\w\u0590-\u05FF\']+){{0,{N}}}[^\w\u0590-\u05FF\']+'

# Flexible spaces (OCR fix): change + to *
separator = r'[^\w\u0590-\u05FF\']*'  # Zero or more non-word chars
```

### Bidirectional Gap

Simple regex alternation:
```python
if bidirectional:
    forward = separator.join(regex_parts)
    backward = separator.join(reversed(regex_parts))
    final = f'({forward})|({backward})'
```

---

## Genizah-Specific Features (Not in Responsa Project)

### Judeo-Arabic Definite Article (אל-)

The Genizah corpus contains extensive Judeo-Arabic text. The Arabic definite article "al-" (written אל in Hebrew characters) attaches to nouns:

```python
SUN_LETTERS = set('תדרזסשצטלנ')

def expand_judeo_arabic(word):
    forms = [word, 'אל' + word]
    for prep in ['ו', 'ב', 'פ', 'כ', 'ל']:
        forms.append(prep + 'אל' + word)
    # Sun letter assimilation: אל + שמש → אשׁשׁמש
    if word and word[0] in SUN_LETTERS:
        assimilated = 'א' + word[0] + word
        forms.append(assimilated)
        for prep in ['ו', 'ב', 'פ', 'כ', 'ל']:
            forms.append(prep + assimilated)
    return forms
```

### Flexible Spaces (OCR/HTR Fix)

OCR and manual transcription often produce wrong word boundaries. The flexible spaces option makes the regex separator accept zero-width boundaries:
- Normal: `word1[separator]+word2` (requires at least one separator char)
- Flexible: `word1[separator]*word2` (allows zero separator — words can be joined)

---

## UI State Persistence

### Web (NiceGUI)
```python
# URL params (for sharing):
/search?q=...&responsa=1&variants=1&ja=1&flex_spaces=1&gap=3&bidirectional=1

# Storage (for remembering):
app.storage.user['search_responsa_mode']      # bool
app.storage.user['search_responsa_variants']   # bool
app.storage.user['search_responsa_ja']         # bool
app.storage.user['search_responsa_flex']       # bool
app.storage.user['search_bidirectional']       # bool
```

### Desktop (PyQt6)
```python
# QSettings or lab_engine.settings for persistence
```

---

## Testing Strategy

### Unit Tests (genizah_core.py)
1. `parse_responsa_query()` — test each syntax element independently
2. `expand_grammatical_prefixes()` — verify all prefix combinations
3. `expand_judeo_arabic()` — verify sun letter assimilation
4. Combinatorial cap — verify MAX_EXPANDED_TERMS enforcement
5. Integration: full query → Tantivy string + regex pattern

### Search Tests (with real corpus)
1. `#שלום` should find forms with prefixes (ושלום, השלום, בשלום...)
2. `שלום*` should find שלומי, שלומו, שלומות...
3. `(עץ/אילן)*` with gap=3 should find "עצי השדה" and "אילנות"
4. JA mode: `#כלמה` should find אלכלמה, ואלכלמה...
5. Flexible spaces: joined/split word forms should both match

### UI Tests
1. Checkbox toggle shows/hides secondary options
2. Mode dropdown disabled when Responsa active
3. Preview updates in real-time
4. URL params round-trip correctly
5. Desktop and web produce identical search results

---

## Document Index

Read in this order:

| # | File | What it covers | Priority |
|---|------|---------------|----------|
| 0 | **AGENT_BRIEF.md** (this file) | Quick overview, what to build, key decisions | **READ FIRST** |
| 1 | `01_feature_analysis.md` | Responsa Project features, gap analysis, checkbox approach | Background |
| 2 | `02_options_report.md` | Three implementation options (I/II/III) compared | Architecture |
| 3 | `03_review_insights.md` | Critical review of the options | Risk analysis |
| 4 | `04_implementation_response.md` | Response to review, implementation paths | Planning |
| 5 | `05_judeo_arabic_spacing.md` | Judeo-Arabic and OCR spacing (Genizah-specific) | **CRITICAL** |
| 6 | `06_ui_integration_sketch.md` | Visual UI mockups for web + desktop, Option IIb | **CRITICAL** |

**For implementation**: Read 0, then 6, then 5, then 2. Skip 1/3/4 unless you need historical context.

---

## Known Consistency Notes

These documents were created progressively over a planning session. Later documents supersede earlier ones:

1. **Documents 1-4** do not mention Judeo-Arabic or flexible spacing (added in Doc 5)
2. **Documents 1-5** do not cover the desktop app (added in Doc 6)
3. **Option IIb** (Dialog) appears only in Doc 6 — it supersedes the inline tabular approach in Doc 2's Option III
4. The `##` syntax for Judeo-Arabic (in Doc 5) is an optional enhancement; the checkbox approach (Doc 6) is the MVP
5. **Final recommendation**: Phase 1 = Option II, Phase 2 = Option IIb (Dialog)

---

## Do NOT Change

- Existing search modes (Exact, Variants, Fuzzy, Regex, Shelfmark, Title) must continue to work unchanged when Responsa checkbox is OFF
- The two-phase search architecture (Tantivy → Regex) is not changing
- URL routes (`/search?q=...`) must remain backward-compatible
- The variant system (`VariantManager`, `unified_variants.py`) is not being modified — it's used as-is via the Variants checkbox
