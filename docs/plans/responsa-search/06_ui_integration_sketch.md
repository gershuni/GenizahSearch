# Integration Sketch: Advanced Search in Responsa Style + Tabular Search

> Document 6 of 6 in the Responsa Search planning series. This is the most comprehensive document, introducing Option IIb and desktop parallel implementation.

## Date: 2026-02-09

---

## 0. Current State — For Reference

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│  שאילתת חיפוש                                                                   │
│  ┌──────────────────────────────────────────────┐                                │
│  │  שלום עליכם                          [   ✕ ] │   Mode: [Exact (=)      ▼]    │
│  └──────────────────────────────────────────────┘                                │
│                                                       Gap: [ 0 ]    [🔍 Search] │
│                                                                                 │
│  ┌─ Advanced Options ─────────────────────────────────────────────────────────┐ │
│  │  ☐ Lab Mode    ☐ Deep Scan        Exclude: [________________]              │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  ═══════════════════════════════════════════════════════════════════════════════ │
│  │                        │                                                     │
│  │    Results List        │         Document Viewer                              │
│  │    (Splitter 35/65)    │         (Transcription Text)                         │
│  │                        │                                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Existing elements that must be preserved:**
- RTL input field with clearable
- Mode dropdown (Exact/Variants x3/Fuzzy/Regex/Shelfmark/Title)
- Gap (0-10)
- Search/Stop buttons
- Advanced Options expansion (Lab Mode, Deep Scan, Exclude Words)
- Collapsed/Expanded panel toggle
- Progress bar + status
- Splitter: Results | Viewer

---

# Option I: "Lightweight Checkbox"

> **Approach**: Minimal — add a Responsa checkbox + a Variants checkbox, without touching the layout

## Sketch

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│  שאילתת חיפוש                                                                    │
│  ┌──────────────────────────────────────────────┐                                 │
│  │  #(קוצץ/עוקר) (עץ/אילן)*            [   ✕ ] │    Gap: [ 3 ]   [🔍 Search]   │
│  └──────────────────────────────────────────────┘                                 │
│                                                                                  │
│  ☑ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים                                   │
│                                                                                  │
│  ┌─ Advanced Options ──────────────────────────────────────────────────────────┐  │
│  │  ☐ Lab Mode   ☐ Deep Scan   ☐ Gap דו-כיווני   Exclude: [_____________]    │  │
│  │  ☐ ערבית יהודית (הרחבת אל-)                                                │  │
│  └─────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### What changes in the UI:
| Element | Change |
|--------|-------|
| **Mode dropdown** | **Hidden** when Responsa checkbox is active. Returns when inactive |
| **"Responsa Search" checkbox** | New, on a row below the input field |
| **"Variants" checkbox** | New, next to Responsa |
| **"Flexible Spaces" checkbox** | New, next to Variants |
| **"Bidirectional Gap" checkbox** | New, in Advanced Options |
| **"Judeo-Arabic" checkbox** | New, in Advanced Options |
| **Gap field** | Remains — works with Responsa |
| **Exclude Words** | Remains — works with Responsa |

### When Responsa checkbox is **off** — everything returns to current state:
```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  ┌──────────────────────────────────────────────┐                                 │
│  │  שלום עליכם                          [   ✕ ] │    Mode: [Exact (=)     ▼]    │
│  └──────────────────────────────────────────────┘                                 │
│                                                       Gap: [ 0 ]   [🔍 Search]  │
│  ☐ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים                                   │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Algorithmic Flow:

```
                                     ☑ שו"ת (Responsa)
                                       │
         query_input.value ────────────┤
                                       ▼
                              responsa_preprocess()
                              ┌─────────────────┐
                              │ # → prefixes    │
                              │ * → wildcards   │
                              │ (/) → OR groups │
                              └────────┬────────┘
                                       │
                    ┌──────────────────┤ regex string
                    │                  │
              ☐ וריאנטים?          ☐ JA?
              (Variants?)          (Judeo-Arabic?)
              │ Yes → expand       │ Yes → expand אל- (al-)
              │      variant pairs │      for each word
              │                    │
              └──────┬─────────────┘
                     ▼
              execute_search(
                  query=expanded_regex,
                  mode='Regex',
                  gap=gap_input.value,
                  exclude_words=not_words
              )
```

### Pros / Cons:

| Pros | Cons |
|------|------|
| Zero layout changes | Tantivy is "blind" — mode='Regex' |
| 3 checkboxes + dropdown hiding | No query preview |
| ~100 lines of code | Poor performance with wildcards |
| No regression risk | No foundation for tabular interface |

---

# Option II: "Hybrid with Preview"

> **Approach**: Checkbox + query preview + Tantivy-aware OR groups + foundation for tabular

## Sketch — Responsa Mode Active

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│  שאילתת חיפוש                                                                    │
│  ┌──────────────────────────────────────────────┐                                 │
│  │  #(קוצץ/עוקר) (עץ/אילן)*            [   ✕ ] │    Gap: [ 3 ]   [🔍 Search]   │
│  └──────────────────────────────────────────────┘                                 │
│                                                                                  │
│  ☑ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים          ⓘ תחביר שו"ת            │
│                                                                                  │
│  ┌ שאילתה מורחבת ──────────────────────────────────────────────────────────────┐ │
│  │ (קוצץ|וקוצץ|הקוצץ|בקוצץ|...|עוקר|ועוקר|...) ──3── (עץ*|אילן*)           │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│  ┌─ Advanced Options ──────────────────────────────────────────────────────────┐  │
│  │  ☐ Lab Mode    ☐ Deep Scan    ☐ Gap דו-כיווני    Exclude: [____________]   │  │
│  │  ☐ ערבית יהודית (אל-)         ☐ Boundary Crossing                          │  │
│  └─────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Responsa mode **off** — returns to normal:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  ┌──────────────────────────────────────────────┐                                 │
│  │  ?שלום                               [   ✕ ] │   Mode: [Variants (?) ▼]      │
│  └──────────────────────────────────────────────┘          Num: [×1]              │
│                                                       Gap: [ 0 ]   [🔍 Search]  │
│  ☐ חיפוש שו"ת                                                                   │
│                                  ← Other checkboxes hidden when not in Responsa mode
└──────────────────────────────────────────────────────────────────────────────────┘
```

### New Component: **Query Preview Row**

```
  ┌ שאילתה מורחבת (Expanded Query) ─────────────────────────────┐
  │                                                               │
  │  ╭─────────────────────╮  ←3→  ╭──────────────────╮          │
  │  │ #קוצץ | #עוקר      │       │ עץ* | אילן*      │          │
  │  │  18 צורות ⓘ        │       │  2 stems          │          │
  │  ╰─────────────────────╯       ╰──────────────────╯          │
  │                                                               │
  │  Tantivy: ("קוצץ"^5 OR "וקוצץ" OR ... ) AND ("עץ" OR "אילן")│
  │  Regex:   (קוצץ|וקוצץ|...) .{gap} (עץ\S*|אילן\S*)          │
  │                                                               │
  └───────────────────────────────────────────────────────────────┘
```

The preview **updates in real time** (debounced) as the user types. Clicking on the info icon next to "18 forms" shows a tooltip with the full list.

### Interaction Between Checkboxes:

```
  ☑ חיפוש שו"ת (Responsa ON)            ☐ חיפוש שו"ת (Responsa OFF)
  ├── ☐ וריאנטים    (visible)            └── [Mode dropdown visible]
  ├── ☐ רווחים גמישים (visible)
  ├── שאילתה מורחבת  (visible)
  └── Mode dropdown  (hidden)

  ☑ Responsa + ☑ Variants:
  └── Preview shows: "(קוצץ|כוצץ|קוזץ|וקוצץ|...) ──3── (עץ*|אילן*)"
      + "52 forms" (prefixes x variants)

  ☑ Responsa + ☑ JA:
  └── Preview shows: "# expanded with al-: (כלמה|אלכלמה|ואלכלמה|באלכלמה|...)"
                      (kalima|alkalima|wa-alkalima|ba-alkalima|...)

  ☑ Responsa + ☑ Spaces:
  └── Preview shows: regex with \s* between letters: "א\s*ל\s*כ\s*ל\s*מ\s*ה"
```

### Algorithmic Flow:

```
         query_input.value
                │
                ▼
     parse_responsa_query()
     ┌──────────────────────┐
     │ → Component[]        │
     │   [{words, prefix,   │
     │     wildcard}]       │
     └──────────┬───────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
   build_tantivy()   build_regex()
   ┌──────────────┐  ┌──────────────┐
   │ OR groups    │  │ wildcards    │
   │ w/ boosting  │  │ alternations │
   │ per component│  │ gap sep      │
   └──────┬───────┘  └──────┬───────┘
          │                  │
          └────────┬─────────┘
                   ▼
          execute_search()
          ┌──────────────────┐
          │ Phase 1: Tantivy │  ← focused OR groups
          │ Phase 2: Regex   │  ← wildcard patterns
          └──────────────────┘
```

### Pros / Cons:

| Pros | Cons |
|------|------|
| Tantivy-aware -> focused candidates | ~300 lines of changes |
| Transparent preview | Changes in 3 core functions |
| Variants x prefixes integrate well | Still no tabular interface |
| Ready foundation for tabular (Components) | Wildcard + Tantivy: stems only |
| JA + spacing as checkboxes | |

---

# Option III: "Full — Including Tabular Interface"

> **Approach**: Like II + tabular interface as an expansion panel below the main field

## Sketch — Main Search (Collapsed)

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│  שאילתת חיפוש                                                                    │
│  ┌──────────────────────────────────────────────┐                                 │
│  │  #(קוצץ/עוקר) (עץ/אילן)*            [   ✕ ] │    Gap: [ 3 ]   [🔍 Search]   │
│  └──────────────────────────────────────────────┘                                 │
│                                                                                  │
│  ☑ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים          ⓘ תחביר שו"ת            │
│                                                                                  │
│  ┌ שאילתה מורחבת ──────────────────────────────────────────────────────────────┐ │
│  │ (קוצץ|וקוצץ|...|עוקר|ועוקר|...) ──3── (עץ*|אילן*)                        │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│  ▸ חיפוש טבלאי (click to open)                                                  │
│                                                                                  │
│  ▸ Advanced Options                                                              │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Sketch — Tabular Interface (Open)

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│  ┌──────────────────────────────────────────────┐                                 │
│  │  (auto-generated from table)                  │    Gap: [ 3 ]   [🔍 Search]   │
│  └──────────────────────────────────────────────┘                                 │
│                                                                                  │
│  ☑ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים                                   │
│                                                                                  │
│  ▾ חיפוש טבלאי (Tabular Search)                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                            │   │
│  │   ┌─ מרכיב 1 (Comp 1) ──┐         ┌─ מרכיב 2 (Comp 2) ──┐               │   │
│  │   │                    │  מרחק   │                    │                    │   │
│  │   │ [קוצץ            ]│ (dist)  │ [עץ              ]│                    │   │
│  │   │ [עוקר            ]│  ┌───┐  │ [אילן            ]│                    │   │
│  │   │ [משחית           ]│  │ 3 │  │ [נטיעה           ]│                    │   │
│  │   │ [               ]│  └───┘  │ [               ]│                    │   │
│  │   │                    │         │                    │                    │   │
│  │   │ ☑ קידומות #       │         │ ☐ קידומות #       │   [+ מרכיב]       │   │
│  │   │  (prefixes)       │         │                    │  (+ component)    │   │
│  │   │ ☐ וריאנטים       │         │ ☑ סיומות *        │                    │   │
│  │   │  (variants)       │         │  (suffixes)        │                    │   │
│  │   │ ☐ JA אל-         │         │ ☐ וריאנטים       │                    │   │
│  │   │ ☐ שלילה ✕        │         │ ☐ שלילה ✕        │                    │   │
│  │   │  (negation)       │         │  (negation)        │                    │   │
│  │   └────────────────────┘         └────────────────────┘                    │   │
│  │                                                                            │   │
│  │   ☐ לפי הסדר (ordered)    ☐ Gap דו-כיווני (bidirectional)                │   │
│  │                                                                            │   │
│  │   Query: #(קוצץ/עוקר/משחית) [3] (עץ/אילן/נטיעה)*                        │   │
│  │                                                                            │   │
│  │   [  ניקוי (Clear)  ]                                                     │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  ▸ Advanced Options                                                              │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### "+ Component" button — adding a third column:

```
   ┌─ מרכיב 1 ──┐       ┌─ מרכיב 2 ──┐       ┌─ מרכיב 3 ──┐
   │ [קוצץ     ]│ ┌──┐ │ [עץ       ]│ ┌──┐ │ [גן       ]│
   │ [עוקר     ]│ │ 3│ │ [אילן     ]│ │ 2│ │ [חצר      ]│
   │ [         ]│ └──┘ │ [         ]│ └──┘ │ [         ]│
   │ ☑ #  ☐ *  │       │ ☐ #  ☑ *  │       │ ☐ #  ☐ *  │
   │ ☐ var ☐ ✕ │       │ ☐ var ☐ ✕ │       │ ☑ var ☐ ✕ │  [+ מרכיב]
   └────────────┘       └────────────┘       └────────────┘  (+ component)
                                                            [× הסר (remove)]
```

### Table <-> Text Field Synchronization:

```
  ╔═══════════════════╗         ╔══════════════════════════════╗
  ║   TABULAR UI      ║  sync   ║     TEXT FIELD               ║
  ║                   ║ ◄────► ║                              ║
  ║  Col 1:           ║         ║  #(קוצץ/עוקר) [3] (עץ/אילן)*  ║
  ║   קוצץ, עוקר     ║         ║                              ║
  ║   ☑ #             ║         ╚══════════════════════════════╝
  ║  Gap: 3           ║
  ║  Col 2:           ║         Sync direction:
  ║   עץ, אילן       ║         • Edit in table → update text field
  ║   ☑ *             ║         • Edit in field → update table (parse)
  ╚═══════════════════╝         • Parse error → red field + tooltip
```

### Algorithmic Flow:

```
     ╭───────────────╮      ╭───────────────╮
     │ Tabular UI    │      │ Text Field    │
     │ (Components)  │◄────►│ (Syntax)      │
     ╰───────┬───────╯      ╰───────┬───────╯
             │                       │
             ▼                       ▼
        tabular_to_syntax()    parse_responsa_query()
             │                       │
             └───────────┬───────────┘
                         ▼
                  SearchPlan
                  ┌─────────────────────┐
                  │ components: [...]   │
                  │ distances: [3]      │
                  │ bidirectional: bool │
                  │ options: {...}      │
                  └─────────┬───────────┘
                            │
               ┌────────────┤────────────┐
               ▼            ▼            ▼
          build_tantivy  build_regex   query_preview
          (OR groups)    (wildcards)   (UI display)
               │            │
               └─────┬──────┘
                     ▼
              execute_search()
```

### Pros / Cons:

| Pros | Cons |
|------|------|
| Full tabular interface — familiar from Responsa Project | ~700 lines of changes |
| Per-component modifiers | Bidirectional sync complexity |
| Dynamic component addition | Heavy UI — takes up space |
| Tantivy-aware like II | Changes in 5 core functions |
| Serializable query (SearchPlan) | Medium regression risk |

---

# Option IIb: "Hybrid + Table as Dialog"

> **Variant of II**: The table is not inline on the page but opens as a **dialog** (floating window)

## Sketch — "Query Builder" button in the search row:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│  ┌──────────────────────────────────────────────┐                                 │
│  │  #(קוצץ/עוקר) (עץ/אילן)*            [   ✕ ] │    Gap: [ 3 ]   [🔍 Search]   │
│  └──────────────────────────────────────────────┘                                 │
│                                                                                  │
│  ☑ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים       [📋 בונה שאילתות]           │
│                                                        (Query Builder)            │
│  ┌ שאילתה מורחבת (Expanded Query) ─────────────────────────────────┐            │
│  │ (קוצץ|וקוצץ|...) ──3── (עץ*|אילן*)                             │            │
│  └──────────────────────────────────────────────────────────────────┘            │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Sketch — The Dialog That Opens:

```
  ╔═══════════════════════════════════════════════════════════════════════╗
  ║  בונה שאילתות (Query Builder)                                  [✕]  ║
  ╠═════════════════════════════════════════════════════════════════════╣
  ║                                                                     ║
  ║   ┌─ מרכיב 1 (Comp 1) ──┐         ┌─ מרכיב 2 (Comp 2) ──┐        ║
  ║   │                    │  מרחק   │                    │            ║
  ║   │ [קוצץ            ]│ (dist)  │ [עץ              ]│            ║
  ║   │ [עוקר            ]│  ┌───┐  │ [אילן            ]│            ║
  ║   │ [משחית           ]│  │ 3 │  │ [נטיעה           ]│            ║
  ║   │ [               ]│  └───┘  │ [               ]│  [+ מרכיב] ║
  ║   │                    │         │                    │ (+ component)║
  ║   │ ☑ קידומות #       │         │ ☐ קידומות #       │            ║
  ║   │  (prefixes)        │         │                    │            ║
  ║   │ ☐ וריאנטים       │         │ ☑ סיומות *        │            ║
  ║   │  (variants)        │         │  (suffixes)        │            ║
  ║   │ ☐ JA אל-         │         │ ☐ וריאנטים       │            ║
  ║   │ ☐ שלילה ✕        │         │ ☐ שלילה ✕        │            ║
  ║   │  (negation)        │         │  (negation)        │            ║
  ║   └────────────────────┘         └────────────────────┘            ║
  ║                                                                     ║
  ║   ☐ לפי הסדר (ordered)  ☐ Gap דו-כיווני (bidir.)  ☐ רווחים גמישים ║
  ║                                            (flexible spaces)        ║
  ║   ┌ שאילתה (Query) ────────────────────────────────────────────┐   ║
  ║   │ #(קוצץ/עוקר/משחית) [3] (עץ/אילן/נטיעה)*                  │   ║
  ║   └──────────────────────────────────────────────────────────────┘   ║
  ║                                                                     ║
  ║           [ העבר לחיפוש ]          [ ניקוי ]                        ║
  ║           (Apply to Search)        (Clear)                          ║
  ║                                                                     ║
  ╚═════════════════════════════════════════════════════════════════════╝
```

**"Apply to Search"** closes the dialog and updates the main input field.

### Why a dialog?

| Expansion panel (III) | Dialog (IIb) |
|-----------------------|-------------|
| Takes up page space | Does not push layout |
| Always visible (if open) | Opens only when needed |
| Continuous sync | One-time sync ("Apply") |
| Complicated on mobile | Dialog responsive built-in |
| ~200 lines of UI | ~150 lines of UI (NiceGUI dialog) |

---

# Summary Comparison — Four Options

## Feature Table

| Feature | I: Lightweight | II: Hybrid | IIb: Hybrid+Dialog | III: Full |
|---------|---------------|------------|---------------------|----------|
| Responsa checkbox | Yes | Yes | Yes | Yes |
| `*` wildcards | Yes | Yes | Yes | Yes |
| `#` prefixes | Yes | Yes | Yes | Yes |
| `(/)` alternates | Yes | Yes | Yes | Yes |
| Variants checkbox | Yes | Yes | Yes | Yes (per-component) |
| JA al- checkbox | Yes | Yes | Yes | Yes (per-component) |
| Spaces checkbox | Yes | Yes | Yes | Yes |
| Query Preview | No | Yes | Yes | Yes |
| Tantivy-aware | No | Yes | Yes | Yes |
| Bidirectional Gap | Yes (regex) | Yes | Yes | Yes |
| Tabular interface | No | No | Yes (dialog) | Yes (inline) |
| Per-comp modifiers | No | No | Yes | Yes |
| Per-comp negation | No | No | Yes | Yes |
| Scope (sentence/paragraph) | No | No | No | Yes |

## Implementation Table

| Metric | I: Lightweight | II: Hybrid | IIb: Hybrid+Dialog | III: Full |
|--------|---------------|------------|---------------------|----------|
| New lines | ~100 | ~300 | ~450 | ~700 |
| Core functions | 1 (preprocess) | 3 (parse, tantivy, regex) | 3+dialog | 5 |
| Regression risk | **Zero** | Low | Low | Medium |
| UI complexity | Low | Medium | Medium+ | High |
| Future upgradability | Limited | Any direction | Any direction | Already there |

---

# Mapping to Current Layout

## Visual Structure — What is Added per Option:

```
                 ┌──────────────────────────────────────────┐
                 │  SEARCH INPUT + MODE + GAP + SEARCH BTN  │  ← existing
                 │                                          │
 Option I ──►   │  [new row] ☑שו"ת ☐וריאנטים ☐רווחים      │  ← new
                 │           (Responsa)(Variants)(Spaces)    │
                 │                                          │
 Option II ──►  │  [new row] expanded query (preview)       │  ← new
                 │                                          │
 Option IIb ─►  │  [button] 📋 Query Builder → dialog       │  ← new
                 │                                          │
 Option III ─►  │  [expansion] ▸ Tabular Search             │  ← new
                 │  [open] 3 cols × 4 rows + modifiers      │
                 │                                          │
                 │  ▸ Advanced Options                       │  ← existing
                 │  Progress Bar                             │  ← existing
                 │  Splitter: Results │ Viewer               │  ← existing
                 └──────────────────────────────────────────┘
```

## Responsa Checkbox — Toggle Behavior:

```
  ☐ חיפוש שו"ת (off):                    ☑ חיפוש שו"ת (on):
  ┌─────────────────────────┐               ┌─────────────────────────┐
  │ [input field         ]  │               │ [input field         ]  │
  │ Mode: [Exact    ▼]     │               │ Mode: [HIDDEN]         │
  │       [Variants  ]     │               │ ☐ וריאנטים (variants)  │
  │       [Fuzzy     ]     │               │ ☐ רווחים גמישים (flex) │
  │       [Regex     ]     │               │ ☐ JA (in Advanced)     │
  │       [Shelfmark ]     │               │                         │
  │       [Title     ]     │               │ preview: (query...)     │
  │ Gap: [ 0 ]             │               │ Gap: [ 3 ]             │
  └─────────────────────────┘               └─────────────────────────┘
```

---

# Interaction: Judeo-Arabic + Spaces

## How the Checkboxes Integrate Visually:

```
  ☑ חיפוש שו"ת   ☐ וריאנטים   ☐ רווחים גמישים
  (Responsa)      (Variants)    (Flexible Spaces)
                                                   ← main row, always visible

  ┌─ Advanced Options ─────────────────────────────────────────┐
  │                                                             │
  │  ☐ ערבית יהודית (הרחבת אל-) (Judeo-Arabic, al- expansion) │
  │     └ Adds: אלX, ואלX, באלX, פאלX... + solar letter       │
  │       assimilation                                          │
  │                                                             │
  │  ☐ Gap דו-כיווני (Bidirectional Gap)                       │
  │     └ Also searches in reverse order: "B ... A"             │
  │       in addition to "A ... B"                              │
  │                                                             │
  │  ☐ Lab Mode    ☐ Deep Scan    Exclude: [____________]      │
  └─────────────────────────────────────────────────────────────┘
```

## Example: All Checkboxes Active

```
  Input:      #כלמה (kalima — "word")
  ☑ Responsa ☑ Variants ☑ JA ☑ Spaces

  Pipeline:
  ┌────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌───────────────┐
  │ # prefixes │ → │ JA al- expansion │ → │ Variants         │ → │ Flex spaces   │
  │ כלמה       │   │ כלמה             │   │ כלמה → כלמא     │   │ כ\s*ל\s*מ\s*ה│
  │ וכלמה      │   │ אלכלמה          │   │ אלכלמה→ אלכלמא │   │ (per variant) │
  │ הכלמה      │   │ ואלכלמה         │   │ ...              │   │               │
  │ בכלמה      │   │ באלכלמה         │   │                  │   │               │
  │ ...        │   │ פאלכלמה         │   │                  │   │               │
  │ (10 forms) │   │ (× 5 = 50)      │   │ (× 3 = 150)     │   │ (150 patterns)│
  └────────────┘   └──────────────────┘   └──────────────────┘   └───────────────┘

  Tantivy:  ("כלמה"^5 OR "וכלמה" OR "אלכלמה" OR "ואלכלמה" OR "כלמא" OR ...)
  Regex:    (כ\s*ל\s*מ\s*ה|ו\s*כ\s*ל\s*מ\s*ה|א\s*ל\s*כ\s*ל\s*מ\s*ה|כ\s*ל\s*מ\s*א|...)

  Cap: MAX_EXPANDED_TERMS = 500 → if exceeded, drop variants
```

---

# Checkbox State — URL State

All state should be saved in the URL (for sharing) and in storage (for persistence):

```
/search?q=%23(קוצץ/עוקר)+(עץ/אילן)*
        &responsa=1           ← Responsa search
        &variants=1           ← Variants
        &ja=1                 ← Judeo-Arabic
        &flex_spaces=1        ← Flexible spaces
        &gap=3
        &bidirectional=1      ← Bidirectional gap
        &exclude=מים (water)
```

```python
# Storage keys (app.storage.user):
'search_responsa_mode': bool       # Responsa checkbox
'search_responsa_variants': bool   # Variants checkbox
'search_responsa_ja': bool         # JA checkbox
'search_responsa_flex': bool       # Flexible spaces checkbox
'search_bidirectional': bool       # Bidirectional gap checkbox
```

---

# Recommendation

## Phased Path: **II -> IIb**

```
Phase 1: Option II (Hybrid)
         ☑ Responsa search + ☐ Variants + ☐ JA + ☐ Spaces
         + Query Preview
         + Tantivy-aware OR groups
         + Bidirectional Gap
         ≈ 300 lines, low risk
              │
              ▼
Phase 2: Add Option IIb (Dialog)
         + "Query Builder" button → tabular dialog
         + Per-component modifiers
         + One-way sync (dialog → field)
         ≈ +150 lines
              │
              ▼
Phase 3: (if there is demand)
         + Scope (sentence/paragraph)
         + Per-component negation
         + Saved queries
```

### Why II -> IIb and not III directly:

1. **Phase 1 (II)** delivers **80% of the value** with 40% of the work — wildcards, prefixes, OR groups, preview, JA, spaces
2. **Dialog (IIb)** is preferable to an expansion panel (III) because:
   - Does not push Results down
   - Works well on mobile
   - One-way sync is simpler than bidirectional
   - NiceGUI `ui.dialog()` is ready to use
3. **III** is reserved for when there is real demand for an inline tabular interface

---

# Appendix: Tooltip Help for Responsa Syntax

The info button next to the checkbox opens a short help panel:

```
╔════════════════════════════════════════════════════╗
║  תחביר חיפוש שו"ת (Responsa Search Syntax)        ║
╠════════════════════════════════════════════════════╣
║                                                    ║
║  *word    = ends with...       → *נדר   = הנדר    ║
║           (suffix match)        (*ndr = ha-neder)  ║
║  word*    = starts with...     → שלום*  = שלומי   ║
║           (prefix match)        (shalom* = shlomi) ║
║  *a*b*c*  = characteristic     → *פ*ט*ר*= אפוטרופ║
║           letters               (*p*t*r* = apotrop)║
║  #word    = with grammatical   → #שלום  = והשלום  ║
║           prefixes              (#shalom = ve-ha-  ║
║                                  shalom)           ║
║  (a/b/c)  = alternative words  → (עץ/אילן)        ║
║                                  (tree/tree)       ║
║  a(s/sh)b = letter alternation → אירו(ס/ש)ין      ║
║                                  (erusin/erushin)  ║
║                                                    ║
║  Combined: #(קוצץ/עוקר) (עץ/אילן)*  gap=3        ║
║  (all forms of "cutter"/"uproots" + "tree..."      ║
║   or "tree..." within 3 words)                     ║
║                                                    ║
╚════════════════════════════════════════════════════╝
```

---
---

# Part II: Integration in the Desktop Application (PyQt6)

## Current State — `genizah_app.py`

The desktop application is built with PyQt6 with a similar (but not identical) structure to the web application.

### Current Visual Structure — Search Tab

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Row 1: Query Input                                                             │
│  ┌──────────┬──────────────────────────────────────────┬──────────┬────────────┐│
│  │ Query:   │ [Search terms, title or shelfmark... ] │ [Search] │ [🤖 AI]   ││
│  └──────────┴──────────────────────────────────────────┴──────────┴────────────┘│
│                                                                                 │
│  Row 2: Search Parameters                                                       │
│  ┌──────┬──────────────┬─────────────────────────────┬────┬────┬──────┬──────┐ │
│  │Mode: │[Exact (=)  ▼]│ [variant controls - hidden] │Gap:│[  ]│Excl:│[    ]│ │
│  │      │              │                              │    │    │     │      │ │
│  └──────┴──────────────┴─────────────────────────────┴────┴────┴──────┴──────┘ │
│  ┌──────┬────────────┬───────────┬───┐                                          │
│  │ [⚙] │ [Lab Mode] │ [☐ Deep] │[?]│                                          │
│  └──────┴────────────┴───────────┴───┘                                          │
│                                                                                 │
│  [LabPanel - hidden until Lab Mode enabled]                                     │
│  [Progress Bar - hidden until search starts]                                    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │  Results Table                                                              ││
│  │  ☐ │ ⚡ │ System ID │ Library │ Shelfmark │ Img │ Title │ Snippet │ Src    ││
│  │  ──┼────┼───────────┼─────────┼───────────┼─────┼───────┼─────────┼────    ││
│  │  ☐ │ 📖☆│ 12345     │ CUL     │ T-S 12.34 │ 🖼  │ ...   │ *שלום*  │ V0.8  ││
│  │  ...                                                                        ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  Ready. │ [Add to List] │ Export: [XLSX] [CSV] [TXT] [DOCX]                    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Existing Component Map (Row 2):

| Component | Widget | Variable | File:Line |
|-----------|--------|----------|-----------|
| Mode dropdown | `QComboBox` | `self.mode_combo` | `genizah_app.py:5719` |
| Variant presets | `QPushButton` x3 | `self.btn_variant_{basic,extended,maximum}` | `:5742-5758` |
| Variant slider | `QSlider` | `self.variant_slider` | `:5774` |
| Variant count | `QLabel` | `self.variant_count_label` | `:5795` |
| Max changes | `QSpinBox` | `self.spin_max_changes` | `:5801` |
| Gap | `QLineEdit` | `self.gap_input` | `:5821` |
| Exclude | `QLineEdit` | `self.exclude_input` | `:5825` |
| Settings gear | `QPushButton` | `self.btn_search_settings` | `:5830` |
| Lab Mode | `QPushButton` (checkable) | `self.btn_lab_mode_toggle` | `:5836` |
| Deep Scan | `QCheckBox` | `self.chk_lab_deep` | `:5842` |
| Help | `QPushButton` | `btn_help` | `:5848` |

### Current Search Flow:

```
query_input.returnPressed  OR  btn_search.clicked
        │
        ▼
    toggle_search()                          (genizah_app.py:10931)
        │
        ├── stop_search()  [if running]
        │
        └── start_search()                   (genizah_app.py:10936)
              │
              ├── parse_query_syntax(query)   (genizah_core.py:4424)
              │   → mode_override, clean_query
              │
              ├── Map mode to combo index
              │   modes = ['literal','variants','fuzzy','Regex','Title','Shelfmark']
              │
              ├── Get variant level from UI   (_get_current_variant_pairs_count)
              ├── Get gap, exclude_words
              │
              ├── Lab Mode?
              │   ├── Yes → LabSearchThread(lab_engine, query, mode, gap, deep, limit)
              │   └── No  → SearchThread(searcher, query, mode, gap, exclude_words)
              │
              └── thread.start()
                    │
                    ├── results_signal → on_search_finished()
                    ├── progress_signal → update progress bar
                    └── error_signal → on_error()
```

---

## Integrating Responsa Search in Desktop — Three Options

### Option I: "Lightweight Checkbox" (Desktop)

**New row (Row 3)** below Row 2:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Row 1: [Query:] [_______________________] [Search] [🤖 AI]                    │
│                                                                                 │
│  Row 2: [Mode: ▼] [variant controls]  [Gap:][__] [Excl:][____] [⚙][Lab][Deep]│
│                                                                                 │
│  Row 3 (new):                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ ☑ חיפוש שו"ת  │  ☐ וריאנטים  │  ☐ רווחים גמישים  │  ☐ JA (אל-)  │ [ⓘ]  ││
│  │ (Responsa)     │  (Variants)  │  (Flex Spaces)     │  (JA al-)    │       ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  [Results Table]                                                                │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Toggle Behavior:**

```
  ☑ חיפוש שו"ת (ON):                      ☐ חיפוש שו"ת (OFF):
  ┌────────────────────────┐                ┌────────────────────────┐
  │ Mode dropdown: DISABLED │                │ Mode dropdown: ENABLED  │
  │ (locked to internal     │                │ (Exact/Variants/Fuzzy/  │
  │  Regex processing)      │                │  Regex/Title/Shelfmark) │
  │                         │                │                         │
  │ Row 3: VISIBLE          │                │ Row 3: HIDDEN           │
  │ ☐ וריאנטים  VISIBLE    │                │ (or just the checkbox   │
  │ ☐ רווחים    VISIBLE    │                │  visible but unchecked) │
  │ ☐ JA        VISIBLE    │                │                         │
  └────────────────────────┘                └────────────────────────┘
```

**Code Changes — `genizah_app.py`:**

```python
# In create_search_tab(), after row2:

# Row 3: Responsa Mode Controls
row3 = QHBoxLayout()

self.chk_responsa_mode = QCheckBox(tr("Responsa Search"))
self.chk_responsa_mode.setToolTip(tr("Enable Bar-Ilan Responsa-style syntax: * # (/)"))
self.chk_responsa_mode.toggled.connect(self._on_responsa_mode_toggled)

self.chk_responsa_variants = QCheckBox(tr("Variants"))
self.chk_responsa_variants.setToolTip(tr("Add spelling variants to each search term"))

self.chk_responsa_flex_spaces = QCheckBox(tr("Flexible Spaces"))
self.chk_responsa_flex_spaces.setToolTip(tr("Ignore word boundaries (for OCR errors)"))

self.chk_responsa_ja = QCheckBox(tr("Judeo-Arabic (al-)"))
self.chk_responsa_ja.setToolTip(tr("Expand # to include Arabic definite article forms"))

self.btn_responsa_help = QPushButton("ⓘ")
self.btn_responsa_help.setFixedWidth(24)
self.btn_responsa_help.clicked.connect(self._show_responsa_help)

row3.addWidget(self.chk_responsa_mode)
row3.addWidget(self.chk_responsa_variants)
row3.addWidget(self.chk_responsa_flex_spaces)
row3.addWidget(self.chk_responsa_ja)
row3.addStretch()
row3.addWidget(self.btn_responsa_help)

self.responsa_row = QWidget()
self.responsa_row.setLayout(row3)
# Variant/flex/JA checkboxes hidden until Responsa mode checked
self.chk_responsa_variants.setVisible(False)
self.chk_responsa_flex_spaces.setVisible(False)
self.chk_responsa_ja.setVisible(False)
self.btn_responsa_help.setVisible(False)

top_layout.addLayout(row1)
top_layout.addLayout(row2)
top_layout.addWidget(self.responsa_row)  # ← new
```

**Handler:**

```python
def _on_responsa_mode_toggled(self, checked):
    """Toggle Responsa search mode UI."""
    # Show/hide sub-checkboxes
    self.chk_responsa_variants.setVisible(checked)
    self.chk_responsa_flex_spaces.setVisible(checked)
    self.chk_responsa_ja.setVisible(checked)
    self.btn_responsa_help.setVisible(checked)

    # Disable mode dropdown when Responsa is active
    self.mode_combo.setEnabled(not checked)
    if checked:
        self.mode_combo.setToolTip(tr("Disabled in Responsa mode"))
    else:
        self.mode_combo.setToolTip("")
```

**Changes in `start_search()`:**

```python
def start_search(self):
    query = self.query_input.text().strip()
    if not query: return

    # === NEW: Responsa Mode ===
    if hasattr(self, 'chk_responsa_mode') and self.chk_responsa_mode.isChecked():
        # Pre-process with Responsa syntax
        processed = self.searcher.responsa_preprocess(
            query,
            variants=self.chk_responsa_variants.isChecked(),
            ja_mode=self.chk_responsa_ja.isChecked(),
            flex_spaces=self.chk_responsa_flex_spaces.isChecked()
        )
        mode = 'Regex'
        query = processed
        # Skip prefix detection
    else:
        # Original logic
        mode_override, clean_query = self.searcher.parse_query_syntax(query)
        ...

    # Rest of method unchanged
    self.search_thread = SearchThread(self.searcher, query, mode, gap, exclude_words)
    ...
```

---

### Option II: "Hybrid with Preview" (Desktop)

**Row 3 + Preview label:**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Row 1: [Query:] [#(קוצץ/עוקר) (עץ/אילן)*_______] [Search] [🤖 AI]          │
│                                                                                 │
│  Row 2: [Mode:▼ DISABLED]               [Gap:][3 ] [Excl:][____] [⚙][Lab][D] │
│                                                                                 │
│  Row 3:                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ ☑ חיפוש שו"ת  │  ☐ וריאנטים  │  ☐ רווחים  │  ☐ JA (אל-)  │ [ⓘ]         ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  Preview:                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ (קוצץ|וקוצץ|הקוצץ|...|עוקר|ועוקר|...) ──3── (עץ\S*|אילן\S*)            ││
│  │ Tantivy: 18 terms ← OR groups    Regex: 2 patterns ← wildcards            ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                                                                 │
│  [Results Table]                                                                │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Preview Widget — `QLabel` with word wrap:**

```python
# Preview label (shows expanded query)
self.responsa_preview = QLabel("")
self.responsa_preview.setWordWrap(True)
self.responsa_preview.setStyleSheet(
    "background-color: #f8f9fa; border: 1px solid #dee2e6; "
    "border-radius: 4px; padding: 6px; font-size: 11px; "
    "color: #495057; direction: rtl;"
)
self.responsa_preview.setVisible(False)
self.responsa_preview.setMaximumHeight(60)

# Real-time update on typing
self.query_input.textChanged.connect(self._update_responsa_preview)
```

```python
def _update_responsa_preview(self):
    """Update the Responsa query preview in real-time."""
    if not (hasattr(self, 'chk_responsa_mode') and self.chk_responsa_mode.isChecked()):
        return
    query = self.query_input.text().strip()
    if not query:
        self.responsa_preview.setText("")
        return

    try:
        components = self.searcher.parse_responsa_query(query)
        gap = int(self.gap_input.text()) if self.gap_input.text().isdigit() else 0

        # Format preview
        parts = []
        for comp in components:
            desc = "|".join(comp.words[:3])
            if len(comp.words) > 3:
                desc += f"|... ({len(comp.words)})"
            if comp.grammatical_prefixes:
                desc = "#(" + desc + ")"
            if comp.wildcard == 'suffix':
                desc += "*"
            parts.append(desc)

        preview = f" ──{gap}── ".join(parts)
        self.responsa_preview.setText(preview)
    except Exception:
        self.responsa_preview.setText("⚠ Syntax error")
```

**Changes in `start_search()`** — Tantivy-aware:

```python
if hasattr(self, 'chk_responsa_mode') and self.chk_responsa_mode.isChecked():
    components = self.searcher.parse_responsa_query(query)
    # Build Tantivy OR groups + Regex patterns from components
    # Pass options: variants, ja, flex_spaces, bidirectional
    self.search_thread = SearchThread(
        self.searcher, query, 'responsa',  # ← new mode
        gap, exclude_words=exclude_words,
        responsa_options={
            'variants': self.chk_responsa_variants.isChecked(),
            'ja': self.chk_responsa_ja.isChecked(),
            'flex_spaces': self.chk_responsa_flex_spaces.isChecked(),
        }
    )
```

**Changes in `SearchThread`** (`gui_threads.py`):

```python
class SearchThread(QThread):
    def __init__(self, searcher, query, mode, gap, exclude_words=None,
                 responsa_options=None):  # ← NEW
        super().__init__()
        self.searcher = searcher
        self.query = query
        self.mode = mode
        self.gap = gap
        self.exclude_words = exclude_words
        self.responsa_options = responsa_options  # ← NEW

    def run(self):
        try:
            def cb(curr, total): self.progress_signal.emit(curr, total)

            if self.mode == 'responsa' and self.responsa_options:
                # New Responsa-mode search path
                results = self.searcher.execute_responsa_search(
                    self.query, self.gap,
                    progress_callback=cb,
                    exclude_words=self.exclude_words,
                    **self.responsa_options
                )
            else:
                # Original search path
                results = self.searcher.execute_search(
                    self.query, self.mode, self.gap,
                    progress_callback=cb,
                    exclude_words=self.exclude_words
                )
            self.results_signal.emit(results)
        except Exception as e:
            self.error_signal.emit(str(e))
```

---

### Option III: "Full with Tabular Dialog" (Desktop)

**"Query Builder" button opens a QDialog:**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Row 1: [Query:] [#(קוצץ/עוקר) (עץ/אילן)*_______] [Search] [🤖 AI]          │
│                                                                                 │
│  Row 2: [Mode:▼]                  [Gap:][3 ] [Excl:][____] [⚙][Lab][D]       │
│                                                                                 │
│  Row 3:                                                                         │
│  │ ☑ חיפוש שו"ת │ ☐ וריאנטים │ ☐ רווחים │ ☐ JA │ [📋 בונה שאילתות] │ [ⓘ]  │
│  │ (Responsa)    │ (Variants) │ (Spaces) │      │ (Query Builder)    │       │
│                                                                                 │
│  Preview:                                                                       │
│  │ (קוצץ|וקוצץ|...) ──3── (עץ*|אילן*)    [18 forms]                           │
│                                                                                 │
│  [Results Table]                                                                │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**QDialog — Query Builder:**

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  בונה שאילתות — חיפוש טבלאי בסגנון פרויקט השו"ת                          [✕]  ║
║  (Query Builder — Tabular Search in Responsa Project Style)                      ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                  ║
║  ┌─── מרכיב 1 (Comp 1) ────┐             ┌─── מרכיב 2 (Comp 2) ────┐           ║
║  │                        │   מרחק     │                        │               ║
║  │  [קוצץ              ] │  (dist)    │  [עץ                ] │               ║
║  │  [עוקר              ] │  ┌─────┐   │  [אילן              ] │               ║
║  │  [משחית             ] │  │  3  │   │  [נטיעה             ] │   [+ מרכיב]   ║
║  │  [                  ] │  └─────┘   │  [                  ] │  (+ component) ║
║  │                        │             │                        │               ║
║  │  ☑ # קידומות דקדוקיות │             │  ☐ # קידומות דקדוקיות │               ║
║  │   (gram. prefixes)     │             │                        │               ║
║  │  ☐ * סיומות (wildcard)│             │  ☑ * סיומות (wildcard)│               ║
║  │   (suffix wildcard)    │             │   (suffix wildcard)    │               ║
║  │  ☐ וריאנטים (variants)│             │  ☐ וריאנטים (variants)│               ║
║  │  ☐ JA אל-            │             │  ☐ שלילה ✕ (negation) │               ║
║  │  ☐ שלילה ✕ (negation) │             │                        │               ║
║  └────────────────────────┘             └────────────────────────┘               ║
║                                                                                  ║
║  ───────────────────────────────────────────────────────────────────             ║
║                                                                                  ║
║  ☐ לפי הסדר (ordered)    ☐ Gap דו-כיווני (bidir.)    ☐ רווחים גמישים (flex)    ║
║                                                                                  ║
║  ┌─ שאילתה מתורגמת (Translated Query) ───────────────────────────────────┐      ║
║  │  #(קוצץ/עוקר/משחית) [3] (עץ/אילן/נטיעה)*                            │      ║
║  └──────────────────────────────────────────────────────────────────────────┘      ║
║                                                                                  ║
║         [  העבר לחיפוש  ]              [  ניקוי  ]              [  ביטול  ]      ║
║         (Apply to Search)              (Clear)                  (Cancel)          ║
║                                                                                  ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

**PyQt6 Implementation Sketch:**

```python
class ResponsaQueryBuilderDialog(QDialog):
    """Tabular query builder dialog — Responsa Project style."""

    def __init__(self, parent=None, initial_query=""):
        super().__init__(parent)
        self.setWindowTitle(tr("Query Builder — Responsa Style"))
        self.setMinimumSize(700, 500)
        self.components = []  # List of ComponentWidget

        main_layout = QVBoxLayout(self)

        # Components area (horizontal scroll)
        self.components_area = QHBoxLayout()
        self._add_component()  # First component
        self._add_component()  # Second component
        main_layout.addLayout(self.components_area)

        # Add component button
        btn_add = QPushButton(tr("+ Add Component"))
        btn_add.clicked.connect(self._add_component)
        main_layout.addWidget(btn_add)

        # Options row
        opts = QHBoxLayout()
        self.chk_ordered = QCheckBox(tr("Ordered"))
        self.chk_bidirectional = QCheckBox(tr("Bidirectional Gap"))
        self.chk_flex = QCheckBox(tr("Flexible Spaces"))
        opts.addWidget(self.chk_ordered)
        opts.addWidget(self.chk_bidirectional)
        opts.addWidget(self.chk_flex)
        main_layout.addLayout(opts)

        # Translated query preview
        self.preview_label = QLabel("")
        self.preview_label.setStyleSheet("background: #f8f9fa; padding: 8px; border: 1px solid #ccc;")
        self.preview_label.setWordWrap(True)
        main_layout.addWidget(self.preview_label)

        # Buttons
        btns = QHBoxLayout()
        btn_apply = QPushButton(tr("Apply to Search"))
        btn_apply.clicked.connect(self.accept)
        btn_clear = QPushButton(tr("Clear"))
        btn_clear.clicked.connect(self._clear_all)
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btns.addWidget(btn_apply)
        btns.addWidget(btn_clear)
        btns.addWidget(btn_cancel)
        main_layout.addLayout(btns)

    def get_query_string(self) -> str:
        """Convert tabular input to Responsa syntax string."""
        parts = []
        for comp in self.components:
            words = comp.get_words()  # non-empty words
            if not words: continue
            # Build syntax
            word_str = "/".join(words) if len(words) > 1 else words[0]
            if len(words) > 1:
                word_str = f"({word_str})"
            if comp.chk_prefix.isChecked():
                word_str = "#" + word_str
            if comp.chk_wildcard.isChecked():
                word_str = word_str + "*"
            parts.append(word_str)
        return " ".join(parts)


class ComponentWidget(QWidget):
    """Single column in the tabular query builder."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        self.setStyleSheet("border: 1px solid #ccc; border-radius: 4px; padding: 4px;")

        # 4 word inputs
        self.word_inputs = []
        for i in range(4):
            inp = QLineEdit()
            inp.setPlaceholderText(tr("Word") if i == 0 else tr("Alternative"))
            inp.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            layout.addWidget(inp)
            self.word_inputs.append(inp)

        # Modifiers
        self.chk_prefix = QCheckBox(tr("# Grammatical prefixes"))
        self.chk_wildcard = QCheckBox(tr("* Suffix wildcard"))
        self.chk_variants = QCheckBox(tr("Variants"))
        self.chk_ja = QCheckBox(tr("JA (al-)"))
        self.chk_negate = QCheckBox(tr("✕ Negate"))
        layout.addWidget(self.chk_prefix)
        layout.addWidget(self.chk_wildcard)
        layout.addWidget(self.chk_variants)
        layout.addWidget(self.chk_ja)
        layout.addWidget(self.chk_negate)

    def get_words(self) -> list:
        return [inp.text().strip() for inp in self.word_inputs if inp.text().strip()]
```

**Using the Dialog from `genizah_app.py`:**

```python
def _open_query_builder(self):
    """Open the Responsa tabular query builder dialog."""
    current = self.query_input.text()
    dlg = ResponsaQueryBuilderDialog(self, initial_query=current)
    if dlg.exec() == QDialog.DialogCode.Accepted:
        syntax = dlg.get_query_string()
        self.query_input.setText(syntax)
        # Ensure Responsa mode is on
        self.chk_responsa_mode.setChecked(True)
```

---

## Comparison: Desktop vs. Web

| Aspect | Web (NiceGUI) | Desktop (PyQt6) |
|--------|---------------|-----------------|
| **Checkbox placement** | Row below input field | Row 3 (new row) |
| **Mode toggle** | `ui.select` -> `.set_visibility(False)` | `QComboBox` -> `.setEnabled(False)` |
| **Preview** | `ui.label` with debounce | `QLabel` with `textChanged` signal |
| **Tabular UI** | `ui.dialog()` or expansion | `QDialog` with `QHBoxLayout` |
| **Thread model** | `run.io_bound()` (async) | `QThread` (SearchThread) |
| **URL state** | URL params (`?responsa=1`) | `QSettings` persistence |
| **Storage** | `app.storage.user[...]` | `QSettings` / lab_engine.settings |

### Shared Code (in `genizah_core.py`):

```
genizah_core.py
├── parse_responsa_query(query_str)        ← new, shared
├── expand_grammatical_prefixes(word)      ← new, shared
├── expand_judeo_arabic(word)              ← new, shared
├── build_tantivy_query(terms, mode, ...)  ← modified, shared
├── build_regex_pattern(terms, mode, ...)  ← modified, shared
├── execute_search(query, mode, ...)       ← modified, mode='responsa' support
└── execute_responsa_search(query, ...)    ← new (or integrated into execute_search)
```

**The shared code** between Desktop and Web **is identical** — all logic resides in `genizah_core.py`. The difference is **only in the UI layer**:
- **Web**: `web/pages/search.py` — NiceGUI widgets
- **Desktop**: `genizah_app.py` — PyQt6 widgets

---

## Parallel Implementation Plan

```
Phase 1: Core Engine (genizah_core.py)          ← SHARED
├── parse_responsa_query()
├── expand_grammatical_prefixes()
├── expand_judeo_arabic()
├── Update build_tantivy_query() for OR groups
├── Update build_regex_pattern() for wildcards
└── Add responsa_mode support to execute_search()

Phase 2a: Web UI (web/pages/search.py)
├── Add checkboxes (Responsa, variants, JA, flex)
├── Add query preview
├── Toggle mode dropdown
└── Wire to execute_search(mode='responsa')

Phase 2b: Desktop UI (genizah_app.py)           ← IN PARALLEL
├── Add Row 3 with checkboxes
├── Add preview QLabel
├── Toggle mode_combo enabled state
├── Wire to SearchThread with responsa_options
└── Update gui_threads.py (SearchThread)

Phase 3a: Web Tabular Dialog
Phase 3b: Desktop Tabular QDialog               ← IN PARALLEL
```

**Important note**: Phase 2a and Phase 2b can run **in parallel** because both depend only on Phase 1 (Core). Once the core is ready, the two UIs are independent of each other.
