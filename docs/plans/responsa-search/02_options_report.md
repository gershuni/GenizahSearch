# Options Report: Integrating Responsa Project-Style Search

Document 2 of 6 in the Responsa Search planning series.

## Date: 2026-02-09

---

## Background

GenizahSearch currently includes a Tantivy + regex-based search engine with modes: Exact, Variants, Fuzzy, Regex.
The goal: integrate syntax and features familiar from the Responsa Project (asterisks, hash marks, slashes, tabular search) so that users accustomed to the Responsa Project feel at home.

This document presents **three options** that differ in depth, implementation complexity, and flexibility of results.

---

# Option I: Translation Layer

> **Approach**: A pre-processing function that translates Responsa syntax to a regex query — no changes to the engine

## UI

```
┌──────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  #(קוצץ/עוקר) (עץ/אילן)*                                     │  │
│  │  (#(cuts/uproots) (tree/tree)*)                                │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Mode: [Exact ▼]   ☑ Responsa Mode   Gap: [3]                       │
│                                                                      │
│  ── Advanced Options ──                                              │
│  ☐ Lab Mode   Exclude: [________]                                    │
└──────────────────────────────────────────────────────────────────────┘
```

### Interface Changes
- **"Responsa mode" checkbox** next to the existing dropdown
- When the checkbox is active: the dropdown is **hidden** (mode is determined automatically)
- When the checkbox is off: everything works as today (including prefix shortcuts)
- **Nothing else changes** — Gap, Exclude, Lab Mode remain as they are
- Small tooltip: "Syntax: `*` wildcards, `#` prefixes, `(/)` alternatives"

### Algorithm

```
User Input ──→ responsa_preprocess() ──→ Regex string ──→ execute_search(mode='Regex')
```

**A single function** `responsa_preprocess(query_str)` that translates directly to regex:

```python
def responsa_preprocess(query_str):
    """
    Translates Responsa syntax to a regex string.
    '#שלום' → '(שלום|ושלום|השלום|בשלום|כשלום|לשלום|משלום|ששלום)'
    '(עץ/אילן)*' → '(עץ|אילן)\S*'
    '*פ*ט*ר*פ*' → '\S*פ\S*ט\S*ר\S*פ\S*'
    'אירו(ס/ש)ין' → 'אירו[סש]ין'
    """
    tokens = tokenize_responsa(query_str)  # Split into tokens
    regex_parts = []
    for token in tokens:
        regex_parts.append(token_to_regex(token))
    return " ".join(regex_parts)  # Passed to execute_search as Regex mode
```

**The translation itself** — each token becomes a regex pattern:

| Input | Intermediate step | Final regex |
|-------|-------------------|-------------|
| `שלום` (shalom) | Plain word | `שלום` |
| `שלום*` (shalom*) | Suffix wildcard | `שלום\S*` |
| `*שלום` (*shalom) | Prefix wildcard | `\S*שלום` |
| `*פ*ט*ר*פ*` (letter pattern *p*t*r*p*) | Character pattern | `\S*פ\S*ט\S*ר\S*פ\S*` |
| `#שלום` (#shalom — with grammatical prefixes) | Grammatical prefixes | `(שלום\|ושלום\|השלום\|בשלום\|...)` |
| `(עץ/אילן/נטיעה)` (tree/tree/sapling — OR group) | OR group | `(עץ\|אילן\|נטיעה)` |
| `אירו(ס/ש)ין` (betrothal with samekh/shin alternatives) | Inline alternatives | `אירו[סש]ין` |

**Gap** — handled by the existing regex separator (no change).

### Required Changes

| File | Change | Scope |
|------|--------|-------|
| `genizah_core.py` | New function `responsa_preprocess()` (~80 lines) | Small |
| `web/pages/search.py` | Checkbox + activation logic (~20 lines) | Small |
| `genizah_core.py` | **No change** to `build_tantivy_query`, `build_regex_pattern`, `execute_search` | Zero |

### Advantages

| # | Advantage |
|---|-----------|
| 1 | **Simplicity** — single function, no engine changes |
| 2 | **Low risk** — cannot break existing search |
| 3 | **Fast to implement** — ~100 new lines of code |
| 4 | **Variants** — can be combined by adding variants to the regex (complex but possible) |

### Disadvantages

| # | Disadvantage |
|---|--------------|
| 1 | **Tantivy unaware** — all search goes through Regex mode, meaning the first stage (Tantivy) is less focused. Search retrieves candidates by extracting Hebrew words from the regex, but without boosting |
| 2 | **Poor performance on broad queries** — `*פ*ט*ר*פ*` (letter pattern *p*t*r*p*) will produce poor Tantivy candidates (only individual letters), since Tantivy cannot search patterns |
| 3 | **Limited variants** — combining variants + wildcards requires aggressive regex expansion (may cause size blowup) |
| 4 | **No tabular interface** — text field syntax only |
| 5 | **Unidirectional gap** — cannot upgrade to bidirectional gap without touching the engine |

### Flow Example

```
Input:    #(קוצץ/עוקר) (עץ/אילן)*     gap=3
          #(cuts/uproots) (tree/tree)*

Step 1 — responsa_preprocess():
  → regex: "(קוצץ|וקוצץ|הקוצץ|...|עוקר|ועוקר|...)" "(עץ\S*|אילן\S*)"

Step 2 — build_tantivy_query() (Regex mode):
  → Word extraction: "קוצץ AND עוקר AND עץ AND אילן" (candidates)

Step 3 — build_regex_pattern() (Regex mode):
  → Regex compiled with gap separator

Step 4 — Result scanning:
  → Tantivy returns ~50K candidates
  → Regex filters down to ~50 results
```

---

# Option II: Hybrid Integration

> **Approach**: Parse Responsa syntax into components, then feed them into the existing engine as "expanded terms" (OR groups, regex terms)

## UI

```
┌──────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  #(קוצץ/עוקר) (עץ/אילן)*                                     │  │
│  │  (#(cuts/uproots) (tree/tree)*)                                │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ☑ Responsa Mode    ☐ Variants    Gap: [3]                           │
│                                                                      │
│  Query: (קוצץ|וקוצץ|הקוצץ|עוקר|ועוקר|...) GAP≤3 (עץ*|אילן*)      │
│                                                                      │
│  ── Advanced Options ──                                              │
│  ☐ Lab Mode   Exclude: [________]                                    │
│  ☐ Bidirectional Gap (also search in reverse order)                  │
└──────────────────────────────────────────────────────────────────────┘
```

### Interface Changes
- **"Responsa mode" checkbox** — replaces the dropdown (not hides — replaces)
- **"Variants" checkbox** — separate, works on top of Responsa mode
- **Query preview line** — shows the query after parsing (debugging + transparency)
- **"Bidirectional Gap" checkbox** — in Advanced Options
- When the Responsa checkbox is off: returns to the regular dropdown (exact/variants/fuzzy/regex)

### Algorithm

```
User Input ──→ parse_responsa_query() ──→ Component List ──→ build_tantivy_query_v2()
                                                          ──→ build_regex_pattern_v2()
                                                          ──→ execute_search()
```

**Step 1: Parsing into a component list**

```python
def parse_responsa_query(query_str):
    """
    Breaks a Responsa query into a list of structured components.
    Each component is a dict with: words (list), modifiers (dict)
    """
    # Input: "#(קוצץ/עוקר) (עץ/אילן)*"
    #         #(cuts/uproots) (tree/tree)*
    # Output:
    return [
        ResponsaComponent(
            words=['קוצץ', 'עוקר'],
            grammatical_prefixes=True,
            wildcard=None,
            negate=False
        ),
        ResponsaComponent(
            words=['עץ', 'אילן'],
            grammatical_prefixes=False,
            wildcard='suffix',
            negate=False
        )
    ]
```

**Step 2: Expansion — each component is expanded into tokens**

| Component | words | modifiers | Expansion for Tantivy | Expansion for Regex |
|-----------|-------|-----------|----------------------|---------------------|
| `#(קוצץ/עוקר)` (#(cuts/uproots)) | [קוצץ, עוקר] | prefix=True | `("קוצץ" OR "וקוצץ" OR "הקוצץ" OR ... OR "עוקר" OR "ועוקר" OR ...)` | `(קוצץ\|וקוצץ\|הקוצץ\|...\|עוקר\|ועוקר\|...)` |
| `(עץ/אילן)*` ((tree/tree)*) | [עץ, אילן] | wildcard=suffix | `("עץ" OR "עצי" OR "עצים" OR "אילן" OR "אילנות" OR ...)` (from index) | `(עץ\S*\|אילן\S*)` |

**The fundamental difference from Option I**: Tantivy receives a detailed **OR list**, not regex. This enables boosting and focused candidate retrieval.

**Step 3: Building the Tantivy query — OR groups**

The function `build_tantivy_query()` already creates OR groups for variants:
```python
# Existing:
parts.append(f'({" OR ".join(clean_vars)})')

# New — OR group from Responsa component:
all_expanded = []
for word in component.words:
    if component.grammatical_prefixes:
        all_expanded.extend(expand_prefixes(word))
    else:
        all_expanded.append(word)
# + variants if the checkbox is active
if variants_enabled:
    all_with_variants = []
    for w in all_expanded:
        all_with_variants.extend(var_mgr.get_variants(w, 'variants'))
    all_expanded = list(set(all_expanded + all_with_variants))
tantivy_or = " OR ".join(f'"{w}"' for w in all_expanded)
parts.append(f'({tantivy_or})')
```

**Step 4: Building the regex pattern — wildcards**

```python
# Per-component regex:
if component.wildcard == 'suffix':
    word_patterns = [rf'{re.escape(w)}\S*' for w in component.words]
elif component.wildcard == 'prefix':
    word_patterns = [rf'\S*{re.escape(w)}' for w in component.words]
elif component.wildcard == 'pattern':
    # *פ*ט*ר*פ* → \S*פ\S*ט\S*ר\S*פ\S*
    word_patterns = [expand_char_pattern(w) for w in component.words]
else:
    word_patterns = [re.escape(w) for w in all_expanded]  # includes prefixes

regex_group = f'({"|".join(word_patterns)})'
```

**Step 5: Bidirectional gap** (optional)

```python
if bidirectional_gap:
    # A gap B  →  (A sep B) | (B sep A)
    forward = sep.join(regex_parts)
    backward = sep.join(reversed(regex_parts))
    final = f'({forward})|({backward})'
else:
    final = sep.join(regex_parts)  # Same as today
```

### Wildcard Challenges in Tantivy

The problem: `שלום*` (shalom* — suffix wildcard) — Tantivy cannot search patterns, only exact terms.

**Solution**: We do not search `שלום*` in Tantivy. Instead:
1. In Tantivy: search for `"שלום"` (shalom — exact) — this retrieves documents containing the word
2. In Regex: the filter is `שלום\S*` — this finds שלומו (his peace), שלומי (my peace), שלומות (peaces), etc.

**Drawback**: Documents containing only "שלומות" (peaces — not "שלום"/shalom) **will not be retrieved** by Tantivy.

**Advanced solution**: Expanded retrieval — Tantivy searches for the stem (word root), or increase the SEARCH_LIMIT.

### Required Changes

| File | Change | Scope |
|------|--------|-------|
| `genizah_core.py` | `parse_responsa_query()` — parsing into components (~100 lines) | Medium |
| `genizah_core.py` | `expand_grammatical_prefixes()` — prefix expansion (~30 lines) | Small |
| `genizah_core.py` | Upgrade `build_tantivy_query()` — ResponsaComponent support (~40 lines) | Medium |
| `genizah_core.py` | Upgrade `build_regex_pattern()` — wildcards + alternations (~50 lines) | Medium |
| `genizah_core.py` | Upgrade `execute_search()` — `responsa_mode` parameter + variants (~20 lines) | Small |
| `web/pages/search.py` | Checkboxes + query preview + bidirectional gap (~60 lines) | Medium |
| **Total** | **~300 lines of changes** | |

### Advantages

| # | Advantage |
|---|-----------|
| 1 | **Tantivy aware** — focused OR groups with boosting, leading to better candidates and faster search |
| 2 | **Variants + Responsa** — natural combination: variants expanded for each word in the OR group |
| 3 | **Query preview** — the user sees what will actually be searched (transparency) |
| 4 | **Bidirectional gap** — easy upgrade via regex alternation |
| 5 | **Foundation for tabular UI** — the structure (Components) maps exactly to a future tabular interface |
| 6 | **Shortcuts preserved** — prefix shortcuts (`?`, `/`, `#`) can be kept when the checkbox is off |

### Disadvantages

| # | Disadvantage |
|---|--------------|
| 1 | **Wildcard + Tantivy gap** — `שלום*` (shalom*) in Tantivy is limited to exact stem, may miss matches |
| 2 | **Moderate complexity** — changes to 3 core functions |
| 3 | **No tabular interface** — still text field only (but the structure is ready for it) |
| 4 | **Grammatical prefixes** — expansion to ~30 forms per word can create large OR groups |

### Flow Example

```
Input:    #(קוצץ/עוקר) (עץ/אילן)*     gap=3, variants=on
          #(cuts/uproots) (tree/tree)*

Step 1 — parse_responsa_query():
  → Component 1: words=[קוצץ, עוקר], prefix=True
                  words=[cuts, uproots]
  → Component 2: words=[עץ, אילן], wildcard=suffix
                  words=[tree, tree]

Step 2 — expand:
  → Component 1 expanded: [קוצץ, וקוצץ, הקוצץ, בקוצץ, ..., עוקר, ועוקר, ...]
     + variants: [כוצץ, קוזץ, ...]  (30 pairs)
  → Component 2: [עץ, אילן] (wildcard handled in regex only)
     + tantivy: ["עץ", "אילן"] (stem only)

Step 3 — build_tantivy_query():
  → ("קוצץ"^5 OR "וקוצץ" OR "הקוצץ" OR ... OR "כוצץ" OR ...)
    AND ("עץ"^5 OR "אילן"^5)

Step 4 — build_regex_pattern():
  → (קוצץ|וקוצץ|הקוצץ|...|כוצץ|...) + gap_sep + (עץ\S*|אילן\S*)

Step 5 — execute_search():
  → Tantivy: ~5K candidates (focused!)
  → Regex: ~30 matches
```

---

# Option III: Extended Engine

> **Approach**: New architecture — each search component is an independent entity with its own search mode + tabular interface

## UI — Main Search

```
┌──────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  #(קוצץ/עוקר) (עץ/אילן)*                                     │  │
│  │  (#(cuts/uproots) (tree/tree)*)                                │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ☑ Responsa Mode    ☐ Variants    Gap: [3]  ☐ Bidirectional         │
│                                                                      │
│  Expanded query:                                                     │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ [# קוצץ|עוקר +vars] ──3── [עץ*|אילן* +vars]               │    │
│  │ [# cuts|uproots +vars] ──3── [tree*|tree* +vars]            │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ── Advanced Options ──                                              │
│  ☐ Lab Mode   Exclude: [________]                                    │
└──────────────────────────────────────────────────────────────────────┘
```

## UI — Tabular Interface

```
┌─ Tabular Search ─────────────────────────────────────────────────────┐
│                                                                       │
│  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐    │
│  │  Component 1  │ Dist. │  Component 2  │ Dist. │  Component 3  │    │
│  ├──────────────┤ ┌───┐ ├──────────────┤ ┌───┐ ├──────────────┤    │
│  │ [קוצץ cuts ] │ │   │ │ [עץ tree   ] │ │   │ │ [           ] │    │
│  │ [עוקר uprts] │ │ 3 │ │ [אילן tree ] │ │   │ │ [           ] │    │
│  │ [משחית dstr] │ │   │ │ [נטיעה splg] │ │   │ │ [           ] │    │
│  ├──────────────┤ └───┘ ├──────────────┤ └───┘ ├──────────────┤    │
│  │ Mode:        │       │ Mode:        │       │ Mode:        │    │
│  │ [Exact    ▼] │       │ [Exact    ▼] │       │ [Exact    ▼] │    │
│  │ ☑ Prefixes # │       │ ☐ Prefixes # │       │ ☐ Prefixes # │    │
│  │ ☑ Variants   │       │ ☐ Variants   │       │ ☐ Variants   │    │
│  │ ☐ Wildcard * │       │ ☑ Wildcard * │       │ ☐ Wildcard * │    │
│  │ ☐ Negate ✕   │       │ ☐ Negate ✕   │       │ ☐ Negate ✕   │    │
│  └──────────────┘       └──────────────┘       └──────────────┘    │
│                                                                       │
│  ☐ Ordered    ☐ Bidirectional Gap    Scope: [Words ▼]                │
│                                                                       │
│  Query: #(קוצץ/עוקר/משחית) [3] (עץ/אילן/נטיעה)*                    │
│          #(cuts/uproots/destroys) [3] (tree/tree/sapling)*           │
│                                                                       │
│  [Search]  [Clear]                                                    │
└───────────────────────────────────────────────────────────────────────┘
```

### Interface Changes
- **Tabular interface** built as an expansion panel
- **Mode per-component** — each column can be exact/variants/wildcard
- **Checkboxes per-component** — prefixes, variants, wildcard, negation
- **Translated query line** — the table generates Responsa syntax (can also be edited directly)
- **Scope** — range: words / sentence / paragraph / document
- **Bidirectional** — the interface offers the option
- **The table and main text field are synchronized** — editing the text field updates the table and vice versa

### Algorithm

```
                        ┌──→ Tabular UI
User Input ──→ parse ──→│
                        └──→ Text field (synced)
                              ↓
                    SearchPlan (list of SearchComponents)
                              ↓
                ┌─────────────┼─────────────┐
                ↓             ↓             ↓
           Component 1   Component 2   Component 3
           (own mode)    (own mode)    (own mode)
                ↓             ↓             ↓
           Expand terms  Expand terms  Expand terms
                ↓             ↓             ↓
                └─────────────┼─────────────┘
                              ↓
                   build_tantivy_query()
                   build_regex_pattern()
                              ↓
                       execute_search()
```

**New data structures:**

```python
@dataclass
class SearchComponent:
    """A single search component — one column in the table"""
    words: List[str]              # Words (primary + alternatives)
    mode: str = 'exact'           # exact / variants / fuzzy
    grammatical_prefixes: bool = False  # # prefixes
    wildcard: str = None          # None / 'suffix' / 'prefix' / 'pattern'
    negate: bool = False          # Component negation
    variant_level: int = 30       # Variant level (if mode=variants)

@dataclass
class SearchPlan:
    """A complete search plan"""
    components: List[SearchComponent]
    distances: List[int]          # Distances between components [gap1, gap2]
    ordered: bool = False         # Ordered?
    bidirectional: bool = True    # Bidirectional gap?
    scope: str = 'words'          # words / sentence / paragraph / document
    exclude_words: List[str] = field(default_factory=list)
```

**Tantivy query building — per-component:**

```python
def build_tantivy_from_plan(plan: SearchPlan):
    parts = []
    for comp in plan.components:
        terms = set()
        for word in comp.words:
            if comp.grammatical_prefixes:
                terms.update(expand_prefixes(word))
            else:
                terms.add(word)

            if comp.mode in ('variants', 'variants_extended', 'variants_maximum'):
                for t in list(terms):
                    terms.update(var_mgr.get_variants(t, comp.mode))

        if comp.negate:
            # Negation: NOT clause
            parts.append(f'NOT ({" OR ".join(f"{t}" for t in terms)})')
        elif comp.wildcard:
            # Wildcard: send only stems to Tantivy, regex will handle filtering
            stems = [w for w in comp.words]
            parts.append(f'({" OR ".join(f"{s}" for s in stems)})')
        else:
            boosted = []
            for t in terms:
                if t in comp.words:
                    boosted.append(f'"{t}"^5')
                else:
                    boosted.append(f'"{t}"')
            parts.append(f'({" OR ".join(boosted)})')

    return " AND ".join(parts)
```

**Regex building — per-component with wildcard support:**

```python
def build_regex_from_plan(plan: SearchPlan):
    regex_parts = []
    for comp in plan.components:
        all_terms = expand_component(comp)  # includes prefixes + variants
        if comp.wildcard == 'suffix':
            patterns = [rf'{re.escape(t)}\S*' for t in comp.words]  # words only (not expanded)
            if comp.grammatical_prefixes:
                for w in comp.words:
                    for pfx in PREFIXES:
                        patterns.append(rf'{re.escape(pfx + w)}\S*')
        elif comp.wildcard == 'prefix':
            patterns = [rf'\S*{re.escape(t)}' for t in all_terms]
        elif comp.wildcard == 'pattern':
            patterns = [expand_char_pattern(w) for w in comp.words]
        else:
            patterns = sorted([re.escape(t) for t in all_terms], key=len, reverse=True)

        regex_parts.append(f'({"|".join(patterns)})')

    # Gap handling with direction
    if plan.scope == 'sentence':
        sep = r'[^.!?;:]*'  # within sentence
    elif plan.scope == 'paragraph':
        sep = r'[^\n]*'  # within paragraph
    elif plan.scope == 'document':
        sep = r'[\s\S]*?'  # anywhere in document
    else:
        # Word-based gap
        if max(plan.distances + [0]) == 0:
            sep = r'[^\w\u0590-\u05FF\']+'
        else:
            max_gap = max(plan.distances)
            sep = rf'(?:[^\w\u0590-\u05FF\']+[\w\u0590-\u05FF\']+){{0,{max_gap}}}[^\w\u0590-\u05FF\']+'

    joined = sep.join(regex_parts)

    if plan.bidirectional and len(regex_parts) > 1:
        reversed_join = sep.join(reversed(regex_parts))
        final = f'({joined})|({reversed_join})'
    else:
        final = joined

    return re.compile(final, re.IGNORECASE)
```

### Required Changes

| File | Change | Scope |
|------|--------|-------|
| `genizah_core.py` | `SearchComponent`, `SearchPlan` dataclasses (~40 lines) | Small |
| `genizah_core.py` | `parse_responsa_query()` — returns `SearchPlan` (~120 lines) | Medium |
| `genizah_core.py` | `expand_grammatical_prefixes()` (~30 lines) | Small |
| `genizah_core.py` | `build_tantivy_from_plan()` — replaces/extends `build_tantivy_query()` (~80 lines) | Large |
| `genizah_core.py` | `build_regex_from_plan()` — replaces/extends `build_regex_pattern()` (~100 lines) | Large |
| `genizah_core.py` | Upgrade `execute_search()` — `SearchPlan` support (~30 lines) | Medium |
| `web/pages/search.py` | Checkboxes + query preview (~60 lines) | Medium |
| `web/pages/search.py` | **Tabular interface** — expansion panel (~200 lines) | Large |
| `web/pages/search.py` | Table-to-text-field synchronization (~50 lines) | Medium |
| **Total** | **~700 lines of changes** | |

### Advantages

| # | Advantage |
|---|-----------|
| 1 | **Closest to the Responsa Project** — tabular interface, per-component modifiers, scope, negation |
| 2 | **Mode per-component** — component 1 with variants, component 2 exact, component 3 wildcard |
| 3 | **Bidirectional gap** — built-in |
| 4 | **Scope** — sentence / paragraph / document |
| 5 | **Per-component negation** — filters out results containing a specific component |
| 6 | **Table-to-text synchronization** — the user can choose between interfaces |
| 7 | **SearchPlan** — structure enables serialization (saving queries, sharing) |
| 8 | **Tantivy aware** — same as Option II, with boosting and OR groups |

### Disadvantages

| # | Disadvantage |
|---|--------------|
| 1 | **High complexity** — ~700 lines, changes to 5 core functions |
| 2 | **Risk** — changes to `build_tantivy_query` and `build_regex_pattern` may break existing search |
| 3 | **Heavy interface** — the table takes up space; need to ensure it does not overwhelm casual users |
| 4 | **Synchronization** — maintaining bidirectional sync between table and text field is complex |
| 5 | **Wildcard + Tantivy** — same limitation as Option II (stems only) |
| 6 | **Development time** — 3-4x that of Option I |

---

# Comprehensive Comparison Table

## Algorithm

| Criterion | I: Translation Layer | II: Hybrid | III: Extended Engine |
|-----------|---------------------|------------|---------------------|
| **Asterisks** `*` | Direct regex | Regex + Tantivy stems | Regex + Tantivy stems |
| **Hash marks** `#` | Regex alternation | OR group in Tantivy | OR group in Tantivy |
| **Slashes** `(/)` | Regex alternation | OR group in Tantivy | OR group in Tantivy |
| **Letter alternation** `(ס/ש)` (samekh/shin) | Character class | Character class | Character class |
| **Variants + Responsa** | Possible but bloated regex | Tantivy OR + regex | Per-component |
| **Tantivy awareness** | None (generic Regex mode) | Focused OR groups | OR groups + boosting |
| **Bidirectional gap** | No | Regex alternation | Built-in |
| **Scope (sentence/paragraph)** | No | No | Yes |
| **Per-component negation** | No | No | Tantivy NOT |
| **Per-component mode** | No | No | Yes |
| **Performance (wildcard)** | Tantivy word extraction only | Stems only | Stems only |
| **Performance (OR groups)** | Regex scanning only | Tantivy boosted | Tantivy boosted |
| **Performance (prefixes)** | Large regex | Tantivy OR | Tantivy OR |

## User Interface

| Criterion | I: Translation Layer | II: Hybrid | III: Extended Engine |
|-----------|---------------------|------------|---------------------|
| **Responsa checkbox** | Yes | Yes | Yes |
| **Variants checkbox** | No (mode dropdown) | Separate | Per-component |
| **Query preview** | No | Yes | Yes |
| **Tabular interface** | No | No (but structure is ready) | Yes |
| **Bidirectional gap UI** | No | Checkbox | Checkbox |
| **Scope UI** | No | No | Dropdown |
| **Per-component negation** | No | No | Checkbox |
| **Help / tooltip** | Basic | Yes | Yes |
| **UI complexity** | Low | Medium | High |

## Implementation

| Criterion | I: Translation Layer | II: Hybrid | III: Extended Engine |
|-----------|---------------------|------------|---------------------|
| **New lines of code** | ~100 | ~300 | ~700 |
| **Files changed** | 2 | 2 | 2 |
| **Core functions changed** | 0 | 3 | 5 |
| **Regression risk** | **Zero** | Low | Medium |
| **Future extensibility** | Limited | Good | Excellent |
| **Dependency on existing structure** | Full (Regex mode) | Partial | Low |

---

# Recommendation

## Recommended approach: **II (Hybrid)**, optionally with a path to III

Note: A fourth option (IIb: Hybrid + Dialog) was later introduced in Document 6 (06_ui_integration_sketch.md) and is now the recommended path for the tabular UI component.

### Rationale:

1. **Option I** is simple but **one-and-done** — it will not scale. Tantivy is unaware, variants do not integrate well, there is no foundation for a tabular interface.

2. **Option II** is the **sweet spot**: deep enough for Tantivy to be aware (OR groups, boosting), simple enough not to break things. The structure (`SearchComponent`) forms a **natural foundation** for a future tabular interface.

3. **Option III** is the richest but **the risk and complexity are high**. It can be reached incrementally from Option II.

### Proposed path:

```
Phase 1 (II):     Checkbox + syntax + OR groups + variants + bidirectional gap
                   ↓
Phase 2 (III partial): Add tabular interface (UI only) that generates Responsa syntax
                   ↓
Phase 3 (III full): Per-component mode + scope + negation (if there is demand)
```

This way we start with ~300 lines, get immediate value, and expand incrementally.
