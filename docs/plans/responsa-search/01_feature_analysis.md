# Planning: Responsa Project-Style Search for GenizahSearch

> Document 1 of 6 in the Responsa Search planning series.

## Date: 2026-02-09

---

## 1. Summary of Responsa Project Features

### Three Search Levels

| Level | Name | Description |
|-------|------|-------------|
| 1 | Basic Search | Adjacent words, wildcards (`*`, `?`), root (`%`), lexical entry (`$`), prefixes (`#`) |
| 2 | Advanced Search | Single query line with special syntax for proximity, alternatives, and modifiers |
| 3 | Tabular Search | Table/grid: each column = search component + alternatives, with proximity and modifiers |

### Key Features (from supplied documentation)

#### A. Proximity (Distance Between Words)
- **Adjacent**: Words in sequence (default)
- **Numeric distance**: `נר [4:1] שבת` (Shabbat candle) -- the second word appears 1-4 positions after the first
- **Bidirectional distance**: `חכמים [1:2-] תקנו` (the sages enacted) -- searches both before and after
- **Uniform distance**: `10: עגונה גוי עדות` (agunah, gentile, testimony) -- all words within a range of 10 words
- **Scope ranges**: `משפט:` (sentence), `פסקה:` (paragraph), `מסמך:` (document) -- search within a structural scope

#### B. Alternative Words (OR)
- Parentheses with slashes: `(עץ/אילן/נטיעה)` (tree/tree/planting)
- Combined with proximity: `(קוצץ/עוקר/משחית) [3:1] (עץ/אילן/נטיעה)` (cuts/uproots/destroys [3:1] tree/tree/planting)

#### C. Modifiers
| Symbol | Modifier | Description |
|--------|----------|-------------|
| `*` before | Prefixes | All possible prefixes |
| `*` after | Suffixes | All possible suffixes |
| `#` before | Grammatical prefixes | Service letters: vav, he, bet, kaf, lamed, mem, shin |
| `!` before | Plene/defective spelling | Word in both spelling forms |
| `%` | Root | All inflections of the root |
| `$` | Lexical entry | All forms of the dictionary entry |
| `+` | Quotation marks | Find with/without abbreviation marks |
| `@` | Empty | Optional letter: `טר(י/@)פה` = טריפה (treifah) or טרפה (trefah) |

#### D. Character Pattern Wildcards
- `*פ*ט*ר*פ*` -- finds all forms of "אפוטרופוס" (apotropos/guardian)
- `*ו*נ*צ*י*` -- finds all spellings of "ונציה" (Venice)

#### E. Letter Alternations Within a Word
- `אירו(ס/ש)ין` -- finds both אירוסין (erusin) and אירושין (erushin, i.e., betrothal)

#### F. Negation (NOT)
- Negation component: filters out results that contain a specific word

#### G. Tabular Search -- Grid Interface
- 3 columns, each with 4 rows (main word + 3 alternatives)
- Between columns: distance field + direction ("in order")
- Per word: modifiers (grammatical prefixes, suffixes, spelling, root, lexical entry, negation)
- Scope options: words, sentence, paragraph, document

---

## 2. Current State of GenizahSearch

### Existing Search Modes
1. **Exact** (`=`) -- exact match
2. **Variants** (`?`/`??`/`???`) -- character substitutions (30/70/150 pairs)
3. **Fuzzy** (`~`) -- fuzzy search with edit distance
4. **Regex** (`/`) -- regular expression
5. **Shelfmark** (`#`) -- search by catalog number
6. **Title** (`$`) -- search by title

### Existing Advanced Features
- **Gap** (0-10): distance between words (unidirectional)
- **Exclude Words**: filter results by unwanted words
- **Lab Mode**: fingerprint-based search
- **Deep Scan**: extended scanning
- **Boundary Crossing**: search across paragraph boundaries

---

## 3. What **Already Exists** in GenizahSearch (Mapped to Responsa Project)

| Responsa Project Feature | GenizahSearch Equivalent | Notes |
|--------------------------|--------------------------|-------|
| Phrase (adjacent words) | Exact search (gap=0) | Exists |
| Word proximity | Gap (0-10) | Exists, but unidirectional only |
| Plene/defective spelling | Variants | Exists (broader -- also includes similar substitutions) |
| Prefix/suffix wildcards | Regex | Exists via regex |
| Component negation | Exclude Words | Exists |
| Scope: document | Boundary Crossing | Partially exists |

## 4. What Is **Missing** (The Gap)

| Responsa Project Feature | Missing in GenizahSearch | Priority |
|--------------------------|--------------------------|----------|
| **Alternative words (OR)** | No `(word1/word2)` syntax | High |
| **Bidirectional distance** | Gap is unidirectional | Medium |
| **Tabular interface** | None -- only a single search bar | High (UI) |
| **Grammatical prefixes** | None -- variants do not handle service letters | Medium |
| **Root / lexical entry** | No morphological engine | Low (requires dictionary) |
| **Letter alternations within a word** | `(ס/ש)` (samekh/shin) -- no syntax | Medium |
| **Character pattern wildcards** | `*פ*ט*ר*פ*` -- possible via regex | Low |
| **Scope: sentence/paragraph** | No structural scope definitions | Low |

---

## 5. Approach: "Responsa Project Mode" Checkbox

### Guiding Principle
**Instead of** prefix shortcuts like `?`, `=`, `~`, `/`, `#`, `$` -- a **single checkbox** that activates Responsa Project syntax directly in the main search field.

### Why This Is Preferable:
1. **Simple** -- the user does not need to remember prefixes
2. **Familiar** -- Responsa Project users will recognize the syntax immediately
3. **No conflicts** -- when the checkbox is active, `*`, `#`, `(/)` take on their Responsa Project meaning
4. **Room for both** -- other modes (Regex, Shelfmark, Title) remain available via the dropdown when the checkbox is off

### What Happens When the Checkbox Is Active:

| Syntax | Meaning | Example | Internal Translation |
|--------|---------|---------|----------------------|
| `word` | Exact search | `שלום` (shalom/peace) | Same as exact |
| `word*` | Everything starting with... | `שלום*` (shalom*) | regex: `שלום\S*` |
| `*word` | Everything ending with... | `*נדר` (*neder/vow) | regex: `\S*נדר` |
| `*a*b*c*` | Character pattern wildcards | `*פ*ט*ר*פ*` | regex: `\S*פ\S*ט\S*ר\S*פ\S*` |
| `#word` | Grammatical prefixes | `#שלום` (#shalom) | OR: `(שלום\|ושלום\|השלום\|בשלום\|...)` |
| `(a/b/c)` | Alternative words | `(עץ/אילן/נטיעה)` (tree/tree/planting) | OR group |
| `word(a/b)word` | Letter alternations | `אירו(ס/ש)ין` (erusin/erushin) | regex: `אירו[סש]ין` |
| space | Adjacent words | `נר שבת` (Shabbat candle) | Same as gap=0 |

### Integration with Existing Features:
- **Gap** -- remains as-is (numeric field)
- **Variants** -- separate checkbox (spelling/orthography) that works **on top of** the rest of the search
- **Exclude Words** -- remains as-is
- **Mode dropdown** -- remains, but without prefix shortcuts. When Responsa mode is active, the dropdown is hidden/irrelevant

---

## 6. Interface Design -- Updated Version

### Main Search with Responsa Checkbox

```
┌──────────────────────────────────────────────────────────────────────┐
│  Search Query                                                        │
│  ┌────────────────────────────────────────────────────────────┐      │
│  │  #(קוצץ/עוקר/משחית) (עץ/אילן/נטיעה)*                    │ [🔍] │
│  └────────────────────────────────────────────────────────────┘      │
│                                                                      │
│  ☑ Responsa Mode    ☐ Variants    Gap: [3]                          │
│                                                                      │
│  ┌─ Responsa Syntax (help) ────────────────────────────────────┐    │
│  │  *   = wildcards (prefixes/suffixes)                         │    │
│  │  #   = grammatical prefixes (vav,he,bet,kaf,lamed,mem,shin) │    │
│  │  (/) = alternative words                                     │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

### Tabular Search (separate expansion panel)

```
┌─ Tabular Search (Responsa-style) ──────────────────────────────────────┐
│                                                                        │
│  ┌──────────────┐  Distance  ┌──────────────┐  Distance  ┌──────────────┐│
│  │  Component 1  │  ┌──┐    │  Component 2  │  ┌──┐    │  Component 3  ││
│  ├──────────────┤  │ 5│    ├──────────────┤  │ 3│    ├──────────────┤│
│  │ [Word 1     ] │  └──┘    │ [Word 1     ] │  └──┘    │ [Word 1     ] ││
│  │ [Alt 2      ] │          │ [Alt 2      ] │          │ [Alt 2      ] ││
│  │ [Alt 3      ] │          │ [Alt 3      ] │          │ [Alt 3      ] ││
│  ├──────────────┤          ├──────────────┤          ├──────────────┤│
│  │ ☐ Variants   │          │ ☐ Variants   │          │ ☐ Variants   ││
│  │ ☐ Prefixes#  │          │ ☐ Prefixes#  │          │ ☐ Prefixes#  ││
│  └──────────────┘          └──────────────┘          └──────────────┘│
│                                                                        │
│  ☐ In Order    ☐ Variants (global)    Exclude: [________]             │
│                                                                        │
│  [       🔍 Search      ]     [  Clear  ]                              │
│                                                                        │
│  Translated query: #(קוצץ/עוקר) [5] (עץ/אילן)                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Technical Details -- Query Processing in Responsa Mode

### Step 1: Parse

A new function `parse_responsa_query(query_str)` that breaks the query into components:

```python
# Input: "#(קוצץ/עוקר/משחית) (עץ/אילן/נטיעה)*"
#         (cuts/uproots/destroys) (tree/tree/planting)*
# Output:
[
    {
        'type': 'or_group',
        'words': ['קוצץ', 'עוקר', 'משחית'],
        'prefix': True,    # # = grammatical prefixes
        'wildcard': None
    },
    {
        'type': 'or_group',
        'words': ['עץ', 'אילן', 'נטיעה'],
        'prefix': False,
        'wildcard': 'suffix'  # * after = suffixes
    }
]
```

### Step 2: Expand

Each component is expanded into terms and regex patterns:

#### Asterisks `*`
```python
def expand_wildcard(word, wildcard_type):
    if wildcard_type == 'suffix':     # word*
        return rf'{re.escape(word)}\S*'
    elif wildcard_type == 'prefix':   # *word
        return rf'\S*{re.escape(word)}'
    elif wildcard_type == 'pattern':  # *a*b*c*
        chars = [c for c in word if c != '*']
        return r'\S*' + r'\S*'.join(re.escape(c) for c in chars) + r'\S*'
```

#### Hash `#` (Grammatical Prefixes)
```python
GRAMMATICAL_PREFIXES = [
    '',                          # no prefix
    'ו', 'ה', 'ב', 'כ', 'ל', 'מ', 'ש',  # single-letter
    'וה', 'וב', 'וכ', 'ול', 'ומ', 'וש',  # common combinations
    'שה', 'שב', 'שכ', 'של', 'שמ',
    'בש', 'לכ', 'מה',
    'כש', 'כשה', 'לכש',
    'משה', 'ושה', 'ובש',
]

def expand_grammatical_prefixes(word):
    """#word -> list of words with grammatical prefixes"""
    return [prefix + word for prefix in GRAMMATICAL_PREFIXES]
```

#### Slashes `(/)` (Alternatives)
```python
def parse_alternatives(token):
    """(עץ/אילן/נטיעה) (tree/tree/planting) -> ['עץ', 'אילן', 'נטיעה']"""
    if token.startswith('(') and token.endswith(')'):
        inner = token[1:-1]
        return inner.split('/')
    return [token]

def parse_inline_alternatives(word):
    """אירו(ס/ש)ין (erusin/erushin) -> regex: אירו[סש]ין"""
    # Finds (X/Y) inside a word
    pattern = r'\(([^)]+)\)'
    def replace_alt(m):
        chars = m.group(1).split('/')
        if all(len(c) == 1 for c in chars):
            return f'[{"".join(chars)}]'  # character class
        return f'({"".join(chars)})'  # alternation
    return re.sub(pattern, replace_alt, word)
```

### Step 3: Build Query

The expanded components are passed to the existing `build_tantivy_query()` and `build_regex_pattern()` functions, where each OR group becomes `(" " OR " " OR " ")` in Tantivy.

---

## 8. Technical Mapping -- Required Changes

### `genizah_core.py` -- Search Engine
1. **New function: `parse_responsa_query(query_str)`** -- parses Responsa syntax into components
2. **New function: `expand_grammatical_prefixes(word)`** -- expands service letters
3. **New function: `expand_wildcard(word)`** -- translates `*` to regex
4. **New function: `parse_alternatives(token)`** -- parses `(a/b/c)`
5. **Upgrade: `build_tantivy_query()`** -- support for OR groups (already has internal OR structure, needs expansion)
6. **Upgrade: `build_regex_pattern()`** -- support for wildcards and alternations
7. **Upgrade: `execute_search()`** -- parameter `responsa_mode=False` that activates the new parser

### `web/pages/search.py` -- User Interface
1. **"Responsa Project Mode" checkbox** -- next to the search field
2. **"Variants" checkbox** -- separate, works on top of Responsa mode
3. **Hide dropdown** -- when Responsa mode is active, the mode dropdown is hidden
4. **Help**: tooltip/expansion panel with Responsa syntax reference
5. **Query preview**: (optional) shows the expanded query

### `web/pages/search.py` -- Tabular Interface (Phase 2)
1. **New expansion panel**: "Tabular Search"
2. **3 columns x 4 rows** -- structured input
3. **Distance fields** between columns
4. **Per-component checkboxes** -- variants, prefixes
5. **Table-to-query translation** -- converts to Responsa query, then runs through the same engine

---

## 9. Comparison: GenizahSearch vs. Responsa Project

| Criterion | Responsa Project | GenizahSearch (Current) | GenizahSearch (Proposed) |
|-----------|-----------------|-------------------------|--------------------------|
| Adjacent words | Yes | Yes | Yes |
| Numeric distance | Yes (up to N) | Yes (Gap 0-10) | Yes (Gap 0-20, bidirectional) |
| Alternative words | Yes `(a/b/c)` | No | Yes (table + syntax) |
| Grammatical prefixes | Yes | No | Yes (basic) |
| Plene/defective spelling | Yes | Yes (Variants) | Yes (Variants per-component) |
| Root | Yes | No | No (requires dictionary) |
| Lexical entry | Yes | No | No (requires dictionary) |
| Negation | Yes (per-component) | Yes (global) | Yes (per-component) |
| Tabular interface | Yes | No | Yes |
| Fuzzy Search | No | Yes | Yes |
| Regex | No | Yes | Yes |
| Lab Mode (fingerprint) | No | Yes | Yes |

---

## 10. Phases

### Phase 1: Responsa Checkbox + Syntax in Main Search
1. **"Responsa Project Mode" checkbox** in the interface
2. **Asterisks `*`** -- wildcards (easy to implement, direct translation to regex)
3. **Slashes `(/)`** -- alternative words (parsing + OR groups)
4. **Hash `#`** -- grammatical prefixes (expansion to list)
5. **Variants as a separate checkbox** -- works on top of the rest of the search
6. **Letter alternations** -- `אירו(ס/ש)ין` (erusin/erushin) (translation to character class)

### Phase 2: Tabular Interface
7. **Expansion panel** for structured tabular search
8. **3 columns x 4 rows** with distance fields
9. **Per-component modifiers** -- variants and prefixes for each component

### Phase 3: Enhancements
10. **Bidirectional distance** -- upgrade the Gap feature
11. **Structural scope** -- sentence / paragraph / document
12. **Per-component negation**

### What **Not** to Include:
- Root / lexical entry (requires a full morphological dictionary)
- Word families (requires dedicated infrastructure)
- Saved searches / previous queries (browser history already covers this)
- Quotation marks / abbreviation marks (less relevant for Genizah texts)

---

## 11. Usage Examples

### Example 1: Word Search with Grammatical Prefixes
```
Input:    #שלום  (#shalom / #peace)
Expanded: (שלום|ושלום|השלום|בשלום|כשלום|לשלום|משלום|ששלום|והשלום|...)
          (shalom|ve-shalom|ha-shalom|be-shalom|ke-shalom|le-shalom|mi-shalom|she-shalom|ve-ha-shalom|...)
```

### Example 2: Alternatives with Wildcards
```
Input:    (קוצץ/עוקר/משחית) (עץ/אילן)*
          (cuts/uproots/destroys) (tree/tree)*
Expanded: (קוצץ|עוקר|משחית) (עץ\S*|אילן\S*)
Finds:    "קוצץ אילנות" (cuts trees), "עוקר עצים" (uproots trees), "משחית אילני" (destroys trees of)
```

### Example 3: Character Pattern Wildcards
```
Input:    *פ*ט*ר*פ*
Regex:    \S*פ\S*ט\S*ר\S*פ\S*
Finds:    "אפוטרופוס" (apotropos), "אפטרופס" (apotrops), "אפוטרפוס" (apotrpos), "פוטרופא" (potropa)
```

### Example 4: Letter Alternations
```
Input:    אירו(ס/ש)ין  (erusin/erushin, i.e., betrothal)
Regex:    אירו[סש]ין
Finds:    "אירוסין" (erusin), "אירושין" (erushin)
```

### Example 5: Combining Prefixes + Alternatives + Gap
```
Input:    #(קוצץ/עוקר) (עץ/אילן)     gap=3
          #(cuts/uproots) (tree/tree)   gap=3
Expanded: (קוצץ|וקוצץ|הקוצץ|...|עוקר|ועוקר|...) ... (עץ|אילן)
Finds:    "הקוצץ את האילן" (he who cuts the tree), "ועוקר שלושה עצים" (and uproots three trees)
```

### Example 6: Combining with Variants (Checkbox)
```
Input:    #שלום  (#shalom)       + ☑ Variants
Step 1:   Prefixes  -> (שלום|ושלום|השלום|בשלום|...)
Step 2:   Variants  -> (שלום|סלום|שלו|ושלום|וסלום|...)
Finds:    "שלו" (shalo), "השלום" (ha-shalom), "בסלום" (be-salom) (combination of prefixes + spelling variants)
```
