# דו"ח אפשרויות: שילוב חיפוש בסגנון פרויקט השו"ת

## תאריך: 2026-02-09

---

## רקע

GenizahSearch כולל כיום מנוע חיפוש מבוסס Tantivy + regex עם מצבים: Exact, Variants, Fuzzy, Regex.
המטרה: לשלב תחביר ותכונות מוכרות מפרויקט השו"ת (כוכביות, סולמיות, לוכסנים, חיפוש טבלאי) כדי שמשתמשים שמכירים את פרויקט השו"ת ירגישו בבית.

מסמך זה מציג **שלוש אפשרויות** שנבדלות ברמת העומק, מורכבות המימוש, וגמישות התוצאה.

---

# אפשרות א': שכבת תרגום (Translation Layer)

> **גישה**: פונקציית pre-processing שמתרגמת תחביר שו"ת לשאילתת regex — ללא שינוי במנוע

## UI

```
┌──────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐          │
│  │  #(קוצץ/עוקר) (עץ/אילן)*                              │  [🔍]  │
│  └────────────────────────────────────────────────────────┘          │
│                                                                      │
│  Mode: [Exact ▼]   ☑ מצב שו"ת   Gap: [3]                           │
│                                                                      │
│  ── Advanced Options ──                                              │
│  ☐ Lab Mode   Exclude: [________]                                    │
└──────────────────────────────────────────────────────────────────────┘
```

### שינויי ממשק
- **צ'קבוקס "מצב שו"ת"** ליד ה-dropdown הקיים
- כשהצ'קבוקס פעיל: ה-dropdown **מוסתר** (מצב נקבע אוטומטית)
- כשהצ'קבוקס כבוי: הכל עובד כמו היום (כולל prefix shortcuts)
- **שום דבר אחר לא משתנה** — Gap, Exclude, Lab Mode נשארים כמו שהם
- tooltip קטן: "תחביר: `*` wildcards, `#` קידומות, `(/)` חלופות"

### אלגוריתם

```
User Input ──→ responsa_preprocess() ──→ Regex string ──→ execute_search(mode='Regex')
```

**פונקציה יחידה** `responsa_preprocess(query_str)` שמתרגמת ישירות ל-regex:

```python
def responsa_preprocess(query_str):
    """
    מתרגם תחביר שו"ת למחרוזת regex.
    '#שלום' → '(שלום|ושלום|השלום|בשלום|כשלום|לשלום|משלום|ששלום)'
    '(עץ/אילן)*' → '(עץ|אילן)\S*'
    '*פ*ט*ר*פ*' → '\S*פ\S*ט\S*ר\S*פ\S*'
    'אירו(ס/ש)ין' → 'אירו[סש]ין'
    """
    tokens = tokenize_responsa(query_str)  # מפרק לטוקנים
    regex_parts = []
    for token in tokens:
        regex_parts.append(token_to_regex(token))
    return " ".join(regex_parts)  # מועבר ל-execute_search כ-Regex mode
```

**התרגום עצמו** — כל טוקן הופך ל-regex pattern:

| קלט | שלב ביניים | regex סופי |
|-----|-----------|-----------|
| `שלום` | מילה רגילה | `שלום` |
| `שלום*` | suffix wildcard | `שלום\S*` |
| `*שלום` | prefix wildcard | `\S*שלום` |
| `*פ*ט*ר*פ*` | character pattern | `\S*פ\S*ט\S*ר\S*פ\S*` |
| `#שלום` | grammatical prefixes | `(שלום\|ושלום\|השלום\|בשלום\|...)` |
| `(עץ/אילן/נטיעה)` | OR group | `(עץ\|אילן\|נטיעה)` |
| `אירו(ס/ש)ין` | inline alternatives | `אירו[סש]ין` |

**Gap** — מטופל ע"י ה-regex separator הקיים (לא משתנה).

### שינויים נדרשים

| קובץ | שינוי | היקף |
|-------|-------|------|
| `genizah_core.py` | פונקציה חדשה `responsa_preprocess()` (~80 שורות) | קטן |
| `web/pages/search.py` | צ'קבוקס + לוגיקת הפעלה (~20 שורות) | קטן |
| `genizah_core.py` | **שום שינוי** ב-`build_tantivy_query`, `build_regex_pattern`, `execute_search` | אפס |

### יתרונות

| # | יתרון |
|---|-------|
| 1 | **פשטות** — פונקציה יחידה, אין שינוי במנוע |
| 2 | **סיכון נמוך** — אי אפשר לשבור חיפוש קיים |
| 3 | **מהיר למימוש** — ~100 שורות קוד חדשות |
| 4 | **וריאנטים** — אפשר לשלב ע"י הוספת variants ל-regex (מורכב אך אפשרי) |

### חסרונות

| # | חיסרון |
|---|--------|
| 1 | **Tantivy לא מודע** — כל החיפוש עובר ב-Regex mode, מה שאומר שהשלב הראשון (Tantivy) פחות ממוקד. החיפוש שולף candidates ע"י חילוץ מילים עבריות מה-regex, אבל בלי boosting |
| 2 | **ביצועים ירודים בשאילתות רחבות** — `*פ*ט*ר*פ*` ישלוף candidates גרועים ב-Tantivy (רק אותיות בודדות), כי Tantivy לא יודע לחפש patterns |
| 3 | **וריאנטים מוגבלים** — שילוב וריאנטים + wildcards דורש הרחבה אגרסיבית של ה-regex (עלול לפוצץ את הגודל) |
| 4 | **אין ממשק טבלאי** — רק תחביר בשדה טקסט |
| 5 | **Gap חד-כיווני** — לא ניתן לשדרג ל-gap דו-כיווני בלי לגעת במנוע |

### דוגמת זרימה

```
קלט:     #(קוצץ/עוקר) (עץ/אילן)*     gap=3

שלב 1 — responsa_preprocess():
  → regex: "(קוצץ|וקוצץ|הקוצץ|...|עוקר|ועוקר|...)" "(עץ\S*|אילן\S*)"

שלב 2 — build_tantivy_query() (Regex mode):
  → חילוץ מילים: "קוצץ AND עוקר AND עץ AND אילן" (candidates)

שלב 3 — build_regex_pattern() (Regex mode):
  → regex compiled עם gap separator

שלב 4 — סריקת results:
  → Tantivy מחזיר ~50K candidates
  → Regex מסנן ל-~50 תוצאות
```

---

# אפשרות ב': שילוב היברידי (Hybrid Integration)

> **גישה**: פרסור תחביר שו"ת למרכיבים → הזנה למנוע הקיים כ-"terms מורחבים" (OR groups, regex terms)

## UI

```
┌──────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐          │
│  │  #(קוצץ/עוקר) (עץ/אילן)*                              │  [🔍]  │
│  └────────────────────────────────────────────────────────┘          │
│                                                                      │
│  ☑ מצב שו"ת    ☐ וריאנטים    Gap: [3]                               │
│                                                                      │
│  שאילתה: (קוצץ|וקוצץ|הקוצץ|עוקר|ועוקר|הע...) GAP≤3 (עץ*|אילן*)   │
│                                                                      │
│  ── Advanced Options ──                                              │
│  ☐ Lab Mode   Exclude: [________]                                    │
│  ☐ Gap דו-כיווני (חפש גם בסדר הפוך)                                │
└──────────────────────────────────────────────────────────────────────┘
```

### שינויי ממשק
- **צ'קבוקס "מצב שו"ת"** — מחליף את ה-dropdown (לא מסתיר — מחליף)
- **צ'קבוקס "וריאנטים"** — נפרד, פועל על גבי מצב שו"ת
- **שורת תצוגה מקדימה** — מראה את השאילתה אחרי פרסור (debugging + שקיפות)
- **צ'קבוקס "Gap דו-כיווני"** — ב-Advanced Options
- כשצ'קבוקס שו"ת כבוי: חוזרים ל-dropdown הרגיל (exact/variants/fuzzy/regex)

### אלגוריתם

```
User Input ──→ parse_responsa_query() ──→ Component List ──→ build_tantivy_query_v2()
                                                          ──→ build_regex_pattern_v2()
                                                          ──→ execute_search()
```

**שלב 1: פרסור → רשימת מרכיבים**

```python
def parse_responsa_query(query_str):
    """
    מפרק שאילתת שו"ת לרשימת מרכיבים מובנים.
    כל מרכיב הוא dict עם: words (list), modifiers (dict)
    """
    # קלט: "#(קוצץ/עוקר) (עץ/אילן)*"
    # פלט:
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

**שלב 2: הרחבה — כל מרכיב מורחב לטוקנים**

| מרכיב | words | modifiers | הרחבה → Tantivy | הרחבה → Regex |
|--------|-------|-----------|----------------|---------------|
| `#(קוצץ/עוקר)` | [קוצץ, עוקר] | prefix=True | `("קוצץ" OR "וקוצץ" OR "הקוצץ" OR ... OR "עוקר" OR "ועוקר" OR ...)` | `(קוצץ\|וקוצץ\|הקוצץ\|...\|עוקר\|ועוקר\|...)` |
| `(עץ/אילן)*` | [עץ, אילן] | wildcard=suffix | `("עץ" OR "עצי" OR "עצים" OR "אילן" OR "אילנות" OR ...)` ← from index | `(עץ\S*\|אילן\S*)` |

**ההבדל המהותי מאפשרות א'**: Tantivy מקבל OR list **מפורטת**, לא regex. זה מאפשר boosting ושליפת candidates ממוקדת.

**שלב 3: בניית Tantivy query — OR groups**

הפונקציה `build_tantivy_query()` כבר יוצרת OR groups לוריאנטים:
```python
# קיים:
parts.append(f'({" OR ".join(clean_vars)})')

# חדש — OR group ממרכיב שו"ת:
all_expanded = []
for word in component.words:
    if component.grammatical_prefixes:
        all_expanded.extend(expand_prefixes(word))
    else:
        all_expanded.append(word)
# + וריאנטים אם הצ'קבוקס פעיל
if variants_enabled:
    all_with_variants = []
    for w in all_expanded:
        all_with_variants.extend(var_mgr.get_variants(w, 'variants'))
    all_expanded = list(set(all_expanded + all_with_variants))
tantivy_or = " OR ".join(f'"{w}"' for w in all_expanded)
parts.append(f'({tantivy_or})')
```

**שלב 4: בניית regex pattern — wildcards**

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

**שלב 5: Gap דו-כיווני** (אופציונלי)

```python
if bidirectional_gap:
    # A gap B  →  (A sep B) | (B sep A)
    forward = sep.join(regex_parts)
    backward = sep.join(reversed(regex_parts))
    final = f'({forward})|({backward})'
else:
    final = sep.join(regex_parts)  # כמו היום
```

### Wildcard challenges ב-Tantivy

הבעיה: `שלום*` (suffix wildcard) — Tantivy לא יכול לחפש pattern, רק exact terms.

**פתרון**: לא מחפשים `שלום*` ב-Tantivy. במקום זה:
1. ב-Tantivy: מחפשים `"שלום"` (exact) — זה שולף documents שמכילים את המילה
2. ב-Regex: הסינון הוא `שלום\S*` — זה מוצא שלומו, שלומי, שלומות

**חסרון**: documents שמכילים רק "שלומות" (לא "שלום") **לא יישלפו** ע"י Tantivy.

**פתרון מתקדם**: שליפה מורחבת — Tantivy מחפש את ה-stem (שורש המילה), או שמגדילים את SEARCH_LIMIT.

### שינויים נדרשים

| קובץ | שינוי | היקף |
|-------|-------|------|
| `genizah_core.py` | `parse_responsa_query()` — פרסור למרכיבים (~100 שורות) | בינוני |
| `genizah_core.py` | `expand_grammatical_prefixes()` — הרחבת קידומות (~30 שורות) | קטן |
| `genizah_core.py` | שדרוג `build_tantivy_query()` — תמיכה ב-ResponsaComponent (~40 שורות) | בינוני |
| `genizah_core.py` | שדרוג `build_regex_pattern()` — wildcards + alternations (~50 שורות) | בינוני |
| `genizah_core.py` | שדרוג `execute_search()` — פרמטר `responsa_mode` + וריאנטים (~20 שורות) | קטן |
| `web/pages/search.py` | צ'קבוקסים + תצוגה מקדימה + gap דו-כיווני (~60 שורות) | בינוני |
| **סה"כ** | **~300 שורות שינויים** | |

### יתרונות

| # | יתרון |
|---|-------|
| 1 | **Tantivy מודע** — OR groups ממוקדים עם boosting → candidates טובים יותר → חיפוש מהיר |
| 2 | **וריאנטים + שו"ת** — שילוב טבעי: וריאנטים מורחבים לכל מילה ב-OR group |
| 3 | **תצוגה מקדימה** — המשתמש רואה מה באמת יחופש (שקיפות) |
| 4 | **Gap דו-כיווני** — שדרוג קל דרך regex alternation |
| 5 | **בסיס לטבלאי** — המבנה (Components) מתאים בדיוק לממשק טבלאי עתידי |
| 6 | **קיצורי דרך** — אפשר לשמור את prefix shortcuts (`?`, `/`, `#`) כשצ'קבוקס כבוי |

### חסרונות

| # | חיסרון |
|---|--------|
| 1 | **Wildcard + Tantivy gap** — `שלום*` ב-Tantivy מוגבל ל-exact stem, עלול לפספס matches |
| 2 | **מורכבות בינונית** — שינוי ב-3 פונקציות core |
| 3 | **אין ממשק טבלאי** — עדיין רק שורת טקסט (אבל המבנה מוכן לזה) |
| 4 | **קידומות דקדוקיות** — הרחבה ל-~30 צורות × מספר מילים יכולה ליצור OR groups גדולים |

### דוגמת זרימה

```
קלט:     #(קוצץ/עוקר) (עץ/אילן)*     gap=3, variants=on

שלב 1 — parse_responsa_query():
  → Component 1: words=[קוצץ, עוקר], prefix=True
  → Component 2: words=[עץ, אילן], wildcard=suffix

שלב 2 — expand:
  → Component 1 expanded: [קוצץ, וקוצץ, הקוצץ, בקוצץ, ..., עוקר, ועוקר, ...]
     + variants: [כוצץ, קוזץ, ...]  (30 pairs)
  → Component 2: [עץ, אילן] (wildcard handled in regex only)
     + tantivy: ["עץ", "אילן"] (stem only)

שלב 3 — build_tantivy_query():
  → ("קוצץ"^5 OR "וקוצץ" OR "הקוצץ" OR ... OR "כוצץ" OR ...)
    AND ("עץ"^5 OR "אילן"^5)

שלב 4 — build_regex_pattern():
  → (קוצץ|וקוצץ|הקוצץ|...|כוצץ|...) + gap_sep + (עץ\S*|אילן\S*)

שלב 5 — execute_search():
  → Tantivy: ~5K candidates (ממוקד!)
  → Regex: ~30 matches
```

---

# אפשרות ג': מנוע חיפוש מורחב (Extended Engine)

> **גישה**: ארכיטקטורה חדשה — כל מרכיב חיפוש הוא ישות עצמאית עם מצב חיפוש משלו + ממשק טבלאי

## UI — חיפוש ראשי

```
┌──────────────────────────────────────────────────────────────────────┐
│  ┌────────────────────────────────────────────────────────┐          │
│  │  #(קוצץ/עוקר) (עץ/אילן)*                              │  [🔍]  │
│  └────────────────────────────────────────────────────────┘          │
│                                                                      │
│  ☑ מצב שו"ת    ☐ וריאנטים    Gap: [3]  ☐ דו-כיווני                 │
│                                                                      │
│  שאילתה מורחבת:                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ [# קוצץ|עוקר +vars] ──3── [עץ*|אילן* +vars]               │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ── Advanced Options ──                                              │
│  ☐ Lab Mode   Exclude: [________]                                    │
└──────────────────────────────────────────────────────────────────────┘
```

## UI — ממשק טבלאי

```
┌─ חיפוש טבלאי ────────────────────────────────────────────────────────┐
│                                                                       │
│  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐    │
│  │  מרכיב 1     │ מרחק  │  מרכיב 2     │ מרחק  │  מרכיב 3     │    │
│  ├──────────────┤ ┌───┐ ├──────────────┤ ┌───┐ ├──────────────┤    │
│  │ [קוצץ      ] │ │   │ │ [עץ        ] │ │   │ │ [           ] │    │
│  │ [עוקר      ] │ │ 3 │ │ [אילן      ] │ │   │ │ [           ] │    │
│  │ [משחית     ] │ │   │ │ [נטיעה     ] │ │   │ │ [           ] │    │
│  ├──────────────┤ └───┘ ├──────────────┤ └───┘ ├──────────────┤    │
│  │ Mode:        │       │ Mode:        │       │ Mode:        │    │
│  │ [Exact    ▼] │       │ [Exact    ▼] │       │ [Exact    ▼] │    │
│  │ ☑ קידומות # │       │ ☐ קידומות # │       │ ☐ קידומות # │    │
│  │ ☑ וריאנטים  │       │ ☐ וריאנטים  │       │ ☐ וריאנטים  │    │
│  │ ☐ Wildcard * │       │ ☑ Wildcard * │       │ ☐ Wildcard * │    │
│  │ ☐ שלילה ✕   │       │ ☐ שלילה ✕   │       │ ☐ שלילה ✕   │    │
│  └──────────────┘       └──────────────┘       └──────────────┘    │
│                                                                       │
│  ☐ לפי הסדר    ☐ Gap דו-כיווני    Scope: [מילים ▼]                  │
│                                                                       │
│  שאילתה: #(קוצץ/עוקר/משחית) [3] (עץ/אילן/נטיעה)*                   │
│                                                                       │
│  [🔍 חפש]  [ניקוי]                                                   │
└───────────────────────────────────────────────────────────────────────┘
```

### שינויי ממשק
- **ממשק טבלאי** מובנה כ-expansion panel
- **Mode per-component** — כל טור יכול להיות exact/variants/wildcard
- **צ'קבוקסים per-component** — קידומות, וריאנטים, wildcard, שלילה
- **שורת שאילתה מתורגמת** — הטבלה מייצרת תחביר שו"ת (ניתן גם לערוך ישירות)
- **Scope** — טווח: מילים / משפט / פסקה / מסמך
- **דו-כיווני** — הממשק מציע את האפשרות
- **הטבלה והשורה הראשית מסונכרנות** — עריכה בשורה הטקסטית מעדכנת את הטבלה ולהפך

### אלגוריתם

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

**מבנה נתונים חדש:**

```python
@dataclass
class SearchComponent:
    """מרכיב חיפוש בודד — טור אחד בטבלה"""
    words: List[str]              # מילים (ראשית + חלופות)
    mode: str = 'exact'           # exact / variants / fuzzy
    grammatical_prefixes: bool = False  # # קידומות
    wildcard: str = None          # None / 'suffix' / 'prefix' / 'pattern'
    negate: bool = False          # שלילת מרכיב
    variant_level: int = 30       # רמת וריאנטים (אם mode=variants)

@dataclass
class SearchPlan:
    """תוכנית חיפוש מלאה"""
    components: List[SearchComponent]
    distances: List[int]          # מרחקים בין מרכיבים [gap1, gap2]
    ordered: bool = False         # לפי הסדר?
    bidirectional: bool = True    # gap דו-כיווני?
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
            # שלילה: NOT clause
            parts.append(f'NOT ({" OR ".join(f"{t}" for t in terms)})')
        elif comp.wildcard:
            # Wildcard: שולחים רק stems ל-Tantivy, regex יעשה את הסינון
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

### שינויים נדרשים

| קובץ | שינוי | היקף |
|-------|-------|------|
| `genizah_core.py` | `SearchComponent`, `SearchPlan` dataclasses (~40 שורות) | קטן |
| `genizah_core.py` | `parse_responsa_query()` → returns `SearchPlan` (~120 שורות) | בינוני |
| `genizah_core.py` | `expand_grammatical_prefixes()` (~30 שורות) | קטן |
| `genizah_core.py` | `build_tantivy_from_plan()` — מחליף/מרחיב `build_tantivy_query()` (~80 שורות) | גדול |
| `genizah_core.py` | `build_regex_from_plan()` — מחליף/מרחיב `build_regex_pattern()` (~100 שורות) | גדול |
| `genizah_core.py` | שדרוג `execute_search()` — תמיכה ב-`SearchPlan` (~30 שורות) | בינוני |
| `web/pages/search.py` | צ'קבוקסים + תצוגה מקדימה (~60 שורות) | בינוני |
| `web/pages/search.py` | **ממשק טבלאי** — expansion panel (~200 שורות) | גדול |
| `web/pages/search.py` | סנכרון טבלה ↔ שורת טקסט (~50 שורות) | בינוני |
| **סה"כ** | **~700 שורות שינויים** | |

### יתרונות

| # | יתרון |
|---|-------|
| 1 | **הכי קרוב לשו"ת** — ממשק טבלאי, per-component modifiers, scope, שלילה |
| 2 | **Mode per-component** — מרכיב 1 עם וריאנטים, מרכיב 2 exact, מרכיב 3 wildcard |
| 3 | **Gap דו-כיווני** — built-in |
| 4 | **Scope** — משפט / פסקה / מסמך |
| 5 | **שלילה per-component** — מסנן תוצאות שמכילות מרכיב ספציפי |
| 6 | **סנכרון טבלה ↔ טקסט** — המשתמש יכול לבחור בין הממשקים |
| 7 | **SearchPlan** — מבנה מאפשר serialization (שמירת שאילתות, sharing) |
| 8 | **Tantivy מודע** — כמו אפשרות ב', עם boosting ו-OR groups |

### חסרונות

| # | חיסרון |
|---|--------|
| 1 | **מורכבות גבוהה** — ~700 שורות, שינוי ב-5 פונקציות core |
| 2 | **סיכון** — שינוי ב-build_tantivy_query ו-build_regex_pattern עלול לשבור חיפוש קיים |
| 3 | **ממשק כבד** — הטבלה תופסת מקום, צריך לוודא שלא מכבידה על משתמשים פשוטים |
| 4 | **סנכרון** — שמירה על סנכרון דו-כיווני בין טבלה לשורת טקסט זה מורכב |
| 5 | **Wildcard + Tantivy** — אותה מגבלה כמו באפשרות ב' (stems בלבד) |
| 6 | **זמן פיתוח** — פי 3-4 מאפשרות א' |

---

# טבלת השוואה מקיפה

## אלגוריתם

| קריטריון | א' שכבת תרגום | ב' היברידי | ג' מנוע מורחב |
|-----------|--------------|------------|--------------|
| **כוכביות** `*` | ✅ regex ישיר | ✅ regex + Tantivy stems | ✅ regex + Tantivy stems |
| **סולמיות** `#` | ✅ regex alternation | ✅ OR group ב-Tantivy | ✅ OR group ב-Tantivy |
| **לוכסנים** `(/)` | ✅ regex alternation | ✅ OR group ב-Tantivy | ✅ OR group ב-Tantivy |
| **חילופי אותיות** `(ס/ש)` | ✅ character class | ✅ character class | ✅ character class |
| **וריאנטים + שו"ת** | ⚠️ אפשרי אך regex מנופח | ✅ Tantivy OR + regex | ✅ per-component |
| **Tantivy awareness** | ❌ Regex mode (גנרי) | ✅ OR groups ממוקדים | ✅ OR groups + boosting |
| **Gap דו-כיווני** | ❌ | ✅ regex alternation | ✅ built-in |
| **Scope (משפט/פסקה)** | ❌ | ❌ | ✅ |
| **שלילה per-component** | ❌ | ❌ | ✅ Tantivy NOT |
| **Mode per-component** | ❌ | ❌ | ✅ |
| **ביצועים (wildcard)** | ⚠️ Tantivy חילוץ מילים בלבד | ⚠️ stems בלבד | ⚠️ stems בלבד |
| **ביצועים (OR groups)** | ⚠️ regex scanning בלבד | ✅ Tantivy boosted | ✅ Tantivy boosted |
| **ביצועים (קידומות)** | ⚠️ regex גדול | ✅ Tantivy OR | ✅ Tantivy OR |

## ממשק משתמש

| קריטריון | א' שכבת תרגום | ב' היברידי | ג' מנוע מורחב |
|-----------|--------------|------------|--------------|
| **צ'קבוקס שו"ת** | ✅ | ✅ | ✅ |
| **צ'קבוקס וריאנטים** | ❌ (mode dropdown) | ✅ נפרד | ✅ per-component |
| **תצוגה מקדימה** | ❌ | ✅ | ✅ |
| **ממשק טבלאי** | ❌ | ❌ (אבל המבנה מוכן) | ✅ |
| **Gap דו-כיווני UI** | ❌ | ✅ צ'קבוקס | ✅ צ'קבוקס |
| **Scope UI** | ❌ | ❌ | ✅ dropdown |
| **שלילה per-component** | ❌ | ❌ | ✅ צ'קבוקס |
| **עזרה / tooltip** | ✅ בסיסי | ✅ | ✅ |
| **מורכבות UI** | נמוכה | בינונית | גבוהה |

## מימוש

| קריטריון | א' שכבת תרגום | ב' היברידי | ג' מנוע מורחב |
|-----------|--------------|------------|--------------|
| **שורות קוד חדשות** | ~100 | ~300 | ~700 |
| **קבצים שמשתנים** | 2 | 2 | 2 |
| **פונקציות core שמשתנות** | 0 | 3 | 5 |
| **סיכון regression** | **אפס** | נמוך | בינוני |
| **יכולת הרחבה עתידית** | מוגבלת | טובה | מצוינת |
| **תלות במבנה קיים** | מלאה (Regex mode) | חלקית | נמוכה |

---

# המלצה

## גישה מומלצת: **ב' (היברידי)**, אופציונלית עם נתיב ל-ג'

### נימוק:

1. **אפשרות א'** פשוטה אבל **חד-פעמית** — היא לא תגדל. Tantivy לא מודע, וריאנטים לא משתלבים טוב, אין בסיס לטבלאי.

2. **אפשרות ב'** היא **sweet spot**: מספיק עמוקה כדי ש-Tantivy יהיה מודע (OR groups, boosting), מספיק פשוטה כדי לא לשבור דברים. המבנה (`SearchComponent`) מהווה **בסיס טבעי** לממשק טבלאי עתידי.

3. **אפשרות ג'** עשירה ביותר אך **הסיכון והמורכבות גבוהים**. אפשר להגיע אליה בהדרגה מאפשרות ב'.

### נתיב מוצע:

```
שלב 1 (ב'): צ'קבוקס + תחביר + OR groups + וריאנטים + gap דו-כיווני
             ↓
שלב 2 (ג' חלקי): הוספת ממשק טבלאי (UI בלבד) שמייצר תחביר שו"ת
             ↓
שלב 3 (ג' מלא): per-component mode + scope + שלילה (אם יש ביקוש)
```

כך מתחילים עם ~300 שורות, מקבלים ערך מיידי, ומרחיבים בהדרגה.
