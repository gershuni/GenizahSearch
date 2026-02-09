# Adaptations for the Genizah Corpus: Judeo-Arabic and Spacing Issues

> Document 5 of 6 in the Responsa Search planning series. This document covers Genizah-specific features NOT found in the Bar-Ilan Responsa Project.

## Date: 2026-02-09

---

## Background: Why This Matters

The Cairo Genizah corpus is not a purely Hebrew corpus. A significant portion of the manuscripts — letters, scientific treatises, commentaries, legal documents — are written in **Judeo-Arabic**: the Arabic language written in Hebrew script. This was the everyday language of Jews in Islamic lands from the 9th to the 19th century.

Text search in such a corpus requires understanding two phenomena:
1. **The definite article "al-"** — The Arabic definite article that attaches to the beginning of a word (and its combinations with prepositions)
2. **Incorrect spacing** — An OCR/transcription problem where word boundaries do not match the source

Both of these issues **do not exist** in the Bar-Ilan Responsa Project (which handles printed Hebrew texts) and therefore require adaptation specific to GenizahSearch.

---

## 1. Judeo-Arabic — The Definite Article (al-)

### What Is the Definite Article?

The "al" (ال in Arabic) is the definite article in Arabic — the equivalent of the Hebrew definite article "ha-" (ה). In Judeo-Arabic it is written in Hebrew letters: **אל**. It attaches to the beginning of the noun as a single word.

| Hebrew | Judeo-Arabic | Transliteration | Explanation |
|--------|-------------|-----------------|-------------|
| **ה**לשון (the tongue) | **אל**לסאן | al-lisān | "ha-" → "al-" |
| **ה**שיניים (the teeth) | **אל**אסנאן | al-asnān | |
| **ה**מילה (the word) | **אל**כלמה | al-kalima | |
| **ה**אותיות (the letters) | **אל**חרוף | al-ḥurūf | |

### What Is Actually Found in the Corpus

From the transcriptions of actual manuscripts (`Transcriptions_part.txt`) — **al- appears in nearly every line** of Judeo-Arabic texts:

```
אללסאן    (al-lisān    = the tongue)
אלהלקום   (al-ḥalqūm   = the throat)
אלאסנאן   (al-asnān    = the teeth)
אלמרפיין  (al-mirfaʿain = the elbows)
אלשפתין   (al-shafatain = the lips)
אלחרוף    (al-ḥurūf    = the letters)
אלכלמה    (al-kalima   = the word)
אלשוא     (al-shwā     = the shva [vowel])
אלמכתוב   (al-maktūb   = the written)
אלואו     (al-wāw      = the vav [letter])
אלפאתחה   (al-fātḥa    = the patach [vowel mark])
אלנקטה    (al-nuqṭa    = the dot)
אלסאכן    (al-sākin    = the quiescent [i.e., without a vowel])
אלמואצע   (al-mawāḍiʿ  = the places)
```

**A typical line from the corpus:**
```
פצל פי אלכלאם עלי אלשוא אלשוא ינקסם
(= A chapter on the discussion of the shva. The shva is divided...)
```

16 occurrences of al- in 10 lines — this is **the dominant pattern** in Judeo-Arabic text.

### Preposition + al- Combinations

In Judeo-Arabic (as in literary Arabic) short prepositions **attach** to the definite article as a single word. This creates fixed combinations:

| Combination | Components | Meaning | Corpus Example | Translation |
|-------------|-----------|---------|----------------|-------------|
| **באל** | ב + אל | "in" + "the" | `באלאסנאן` | "in the teeth" |
| **ואל** | ו + אל | "and" + "the" | `ואלנקטה` | "and the dot" |
| **ואל** | ו + אל | "and" + "the" | `ואלנקטתין` | "and the two dots" |
| **פאל** | פי + אל | "in/within" + "the" | `פאלמלא` | "in the filling" |
| **פי אל** | פי + אל (separate) | "in/within" + "the" | `פי אלמלא` | same meaning, written separately |
| **לאל** | ל + אל | "to" + "the" | (less common in corpus) | |
| **כאל** | כ + אל | "like" + "the" | (less common in corpus) | |
| **מן אל** | מן + אל | "from" + "the" | `מן אלוגוה` | "from the aspects" (two words) |
| **עלי אל** | עלי + אל | "on" + "the" | `עלי אלף` | "on [the letter] alef" (two words) |

**Important point**: Short prepositions (ב, ו, פ, כ, ל) attach to al- as a single word. Long prepositions (מן, עלי, אלי, פי) are usually written as **separate words** — but not always consistently.

### Sun Letters (حروف شمسية) — Assimilation of the Lamed

This is an important phonological rule in Arabic. There are 14 consonants called "sun letters" (حروف شمسية) — when they follow the definite article, the lamed **is assimilated** (not pronounced) and the following letter **is doubled** in pronunciation. In Genizah manuscripts this can be reflected in spelling:

| Full Spelling (with lamed) | Assimilated Spelling (without lamed) | Transliteration | Meaning |
|---------------------------|--------------------------------------|-----------------|---------|
| אל + שמס | **אשׁשׁמס** | ash-shams | the sun |
| אל + דאר | **אדדאר** | ad-dār | the house |
| אל + נאס | **אננאס** | an-nās | the people |
| אל + תורה | **אתתורה** | at-tawrā | the Torah |
| אל + לסאן | **אללסאן** | al-lisān | the tongue |
| אל + סאכן | **אססאכן** | as-sākin | the quiescent (= vowelless) |
| אל + רגש | **ארגש** / **אררגש** | ar-rajsh | the emphasis mark |

**The 14 sun letters in Hebrew script**: **ת, ד, ר, ז, ס, ש, צ, ט, ל, נ, ת׳, ד׳, ט׳, ץ**

The remaining letters are called "moon letters" (حروف قمرية) — the lamed **is preserved** in pronunciation and spelling:
- אל + כתאב = **אלכתאב** (al-kitāb = the book) — the lamed remains
- אל + מלך = **אלמלך** (al-malik = the king) — the lamed remains
- אל + חרוף = **אלחרוף** (al-ḥurūf = the letters) — the lamed remains

### The Special Difficulty in the Genizah: Inconsistent Spelling

**However** — in Genizah manuscripts the spelling is **not consistent**. The scribes did not always distinguish between sun and moon letters, and sometimes mixed between the forms:

**Example from the corpus** — the word "הלשון" (al-lisān, "the tongue"): lamed is a sun letter, so the "correct" spelling is אללסאן (with doubled lamed). But:
```
Occurrence 1 (line 10): אללסאן  ← full spelling (al- + doubled lamed) ✓
Occurrence 2 (line 87): אללסאן  ← same
Occurrence 3 (line 75): אללסאן  ← same
```
Here the spelling is consistent. But for other words:
```
אלשוא (line 32) — shin is a sun letter, but written as אל (with lamed, not assimilated)
אלשפתין (line 13) — shin is a sun letter, written as אל (not אששפתין)
אלסאכן (line 33) — samekh is a sun letter, written as אל (not אססאכן)
```

**Conclusion**: In the Genizah, most scribes **preserve al- even before sun letters** ("conservative" spelling). But we need to search for **both forms** — both אלשוא and אששוא — because some scribes do assimilate.

### How Much Judeo-Arabic Text Is in the Corpus?

Based on data from `corpus_mapper/parsers/ja_parser.py` (a dedicated parser for Friedberg's Judeo-Arabic), and the fact that there are 629 variant pairs involving "al-" in `unified_variants.py`, the estimate is that a substantial portion of the corpus is Judeo-Arabic. This includes:
- Grammatical treatises (like the examples above)
- Philosophical and theological commentaries
- Commercial and personal letters
- Legal deeds and documents
- Medical and scientific treatises
- Biblical commentaries (such as those by Saadia Gaon)

**Implications for search**: A user searching for a term in Judeo-Arabic **must** handle the definite article, otherwise they will miss a large portion of occurrences.

---

### Impact on Search — Concrete Examples

**What happens today without al- handling:**

Suppose a researcher searches for the word `שוא` (the grammatical shva):

| Query | Finds | Misses |
|-------|-------|--------|
| `שוא` (shva) | "שוא סאכנא" (line 38) | `אלשוא` (al-shva, line 32) — 6+ occurrences! |
| `אלשוא` (al-shva) | "אלשוא אלאנאינרסם" (line 32) | `שוא` alone, `ואלשוא` (and-the-shva), `באלשוא` (in-the-shva) |

**The researcher needs to search 3 times** to find all occurrences: once for `שוא`, once for `אלשוא`, once for `ואלשוא`. And that is without sun letters.

**With the proposed feature** (`##שוא`): a single search finds **everything**.

---

### Recommendation: Extending `#` for Judeo-Arabic

#### Three Approaches — Pros and Cons

**Approach A — Automatic expansion (always add al-):**

Every use of `#` automatically adds Judeo-Arabic prefixes as well.

```python
# #שלום → שלום, ושלום, השלום, בשלום, ..., אלשלום, ואלשלום, ...
```

| Advantage | Disadvantage |
|-----------|-------------|
| Simple — one symbol | Inflates every query (even pure Hebrew) |
| No need to remember ## | Adds ~12 extra forms not relevant to Hebrew text |

**Approach B — Double symbol `##` (recommended for syntax):**

| Symbol | Meaning | Example |
|--------|---------|---------|
| `#` | Hebrew prefixes only | `#שלום` (shalom) → שלום, ושלום, השלום... |
| `##` | Hebrew prefixes **+** Judeo-Arabic | `##כלמה` (kalima) → כלמה, אלכלמה, ואלכלמה... |

| Advantage | Disadvantage |
|-----------|-------------|
| Does not inflate Hebrew queries | Requires remembering two symbols |
| User has control | |
| Transparent — visible in Preview what was searched | |

**Approach C — "Judeo-Arabic" checkbox (recommended for UI):**

```
☑ Responsa Mode    ☐ Variants    ☑ Judeo-Arabic    Gap: [3]
```

When the checkbox is checked, **every** `#` behaves like `##`.

| Advantage | Disadvantage |
|-----------|-------------|
| Most user-friendly — toggle with one click | Global state — not per-component |
| Suitable for researchers working with Judeo-Arabic all the time | |
| Can be combined with ## syntax for mixed cases | |

**Recommendation: Combine B + C** — A checkbox that sets the default, plus `##` syntax for specific cases. This way:
- A researcher working on Judeo-Arabic text checks the box → every `#` also expands with al-
- A researcher working on Hebrew text with one Arabic word → uses `##` only on that word

### Implementation of al- Expansion — Details

```python
# Sun letters — the lamed of al- is assimilated before them
SUN_LETTERS = set('תדרזסשצטלנ')

# Moon letters — the lamed is preserved
# (all others: א, ב, ג, ה, ו, ח, י, כ, ע, פ, ק, מ)

def expand_judeo_arabic_prefixes(word):
    """
    ##כלמה → all forms with the definite article and its combinations.

    Covers:
    1. Regular definite article (אלכלמה)
    2. Preposition + al (ואלכלמה, באלכלמה, ...)
    3. Sun letter assimilation (אלשוא → אששוא)
    4. Preposition + assimilated form (ואששוא, באששוא)
    """
    forms = [word]  # The word itself, without any prefix

    # --- Regular definite article ---
    forms.append('אל' + word)

    # --- Preposition + al ---
    # ו (and), ב (in), פ (in/by, short for פי), כ (like), ל (to/for)
    for prep in ['ו', 'ב', 'פ', 'כ', 'ל']:
        forms.append(prep + 'אל' + word)

    # --- ל + אל → לל (common combination: "to" + "the" = "ll") ---
    forms.append('לל' + word)

    # --- Sun letter assimilation ---
    if word and word[0] in SUN_LETTERS:
        first = word[0]
        # אל + שמש → א + ש + שמש (doubling of the first letter)
        assimilated = 'א' + first + word
        forms.append(assimilated)  # אששמש

        # Preposition + assimilated form
        for prep in ['ו', 'ב', 'פ', 'כ', 'ל']:
            forms.append(prep + assimilated)  # ואששמש

    return forms
```

### Expansion Table — Full Examples

**Word with a moon letter (כ):**
```
##כלמה  →
  כלמה        (the word itself)
  אלכלמה      (al-kalima = the word)
  ואלכלמה     (wa-al-kalima = and the word)
  באלכלמה     (bi-al-kalima = in the word)
  פאלכלמה     (fi-al-kalima = in the word)
  כאלכלמה     (ka-al-kalima = like the word)
  לאלכלמה     (li-al-kalima = to the word)
  ללכלמה      (li-al-kalima = to the word, contracted form)
  ← no assimilation (כ is a moon letter) — total 8 forms
```

**Word with a sun letter (ש):**
```
##שוא  →
  שוא         (the word itself)
  אלשוא       (al-shwā = the shva — conservative spelling)
  ואלשוא      (wa-al-shwā = and the shva)
  באלשוא      (bi-al-shwā = in the shva)
  פאלשוא, כאלשוא, לאלשוא, ללשוא
  אששוא       (ash-shwā = the shva — assimilated spelling)
  ואששוא      (wa-sh-shwā = and the shva)
  באששוא, פאששוא, כאששוא, לאששוא
  ← total 14 forms (two doubled forms: with and without lamed)
```

**Word with a sun letter (ל — special case):**
```
##לסאן  →
  לסאן        (the word itself)
  אללסאן      (al-lisān = the tongue — lamed doubled because it is both a sun letter and the lamed of al-)
  ואללסאן, באללסאן, ...
  אללסאן      (the assimilated form is also identical — because ל+ל=לל and אל+ל=אלל)
  ← total 8 forms (assimilation = regular spelling, no difference)
```

---

## 2. Incorrect Spacing — The Word Boundary Problem

### The Problem

Genizah manuscripts are transcribed (manually or automatically) into digital text. Transcription includes decisions about **where to place spaces** between words. Common problems:

1. **The manuscript itself** — Medieval scribes did not always place clear spaces between words
2. **OCR/HTR** — Automatic recognition systems make errors in identifying word boundaries
3. **Manual reading** — Different transcribers interpret the same manuscript differently
4. **Judeo-Arabic** — Prepositions that attach/detach inconsistently

### Evidence from the Corpus — Comparing Two Readings of the Same Page

The file `Transcriptions_part.txt` contains **two independent readings** of the same manuscript. Comparing them reveals the problem:

**Reading A (IE104549337) vs. Reading B (IE19213988) — the very same page:**

| Line | Reading A | Reading B | Difference |
|------|-----------|-----------|------------|
| 1 | `אלהלקום` | `אלזולקום` | Different letters (ה→ז, missing ול) |
| 1 | `דט'ר נגג` | `דטר נמ` | Different spacing + letters |
| 5 | `]אל מהא תנבדל` | `קאל אנהא תנבדל` | One word (]אל) → two words (קאל אנהא) |
| 10 | `אלמכתוב גפי` | `אלמכתוב בפי` | Different letter (ג→ב) |

**Additional examples of problematic spacing from the corpus:**

```
Line 104: קדיגי אליוד     ← should be: "קד יגי" (two words merged)
Line 46:  המצפונפיס        ← words completely fused
Line 154: הואלאם           ← "הוא לאם"? "הואל אם"? unclear
Line 197: בית הסכרים       ← "בית הספרים" (the library) (ס was read as כ)
Line 200: עזוכזסאוחלו      ← string of letters with no spaces — completely unclear
```

**And lines 161-170 vs. 209-224 — the same Haggadah text:**

```
Reading A (161): גורשו ממצרים ולא יכלו
                 (they were expelled from Egypt and could not)
Reading B (209): יר שו ממצרים ולא יכלו
                 ^^^^
                 "גורשו" → "יר שו" (one word became two)

Reading A (172): חייב אדם לראות את עצמו
                 (a person is obligated to see themselves)
Reading B (220): חייב אדם לראויב את י
                          ^^^^^^^     ^
                 "לראות" → "לראויב" + "עצמו" → "י" (space disappeared + different letter)

Reading A (175): לפיכך אני חייבין לוה
                 (therefore we are obligated to give thanks)
Reading B (223): לכיכך אני חייבין
                 ^^^^^^
                 "לפיכך" → "לכיכך" (different letter)
```

### Classification of Problem Types

| Type | What Happens | Real Example | Frequency |
|------|-------------|--------------|-----------|
| **Merged words** | Two words written as one | `קדיגי` ← `קד יגי` | High |
| **Split word** | One word split into two | `יר שו` ← `גורשו` | High |
| **Shifted boundary** | The space is in the wrong place | `ביתד ין` ← `בית דין` | Medium |
| **al- separated/attached** | The definite article separated from the word | `אל כלמה` ← `אלכלמה` | High (JA) |
| **Letter chain** | Multiple words without spaces | `עזוכזסאוחלו` | Low |

### Impact on Search — Why This Hurts

**Current state**: The engine splits queries using `.split()` (space = word boundary). Searching `בית דין` (court) searches for "בית" (house) then "דין" (law) as two separate words. If the text says `ביתדין` (without a space) — **not found**.

```
Query:   בית דין (court)    (gap=0)
regex:   בית[^\w]+דין   ← requires at least one non-word character between "בית" and "דין"
Text:   "ביתדין"       ← no space → ❌ not found!
Text:   "ביתד ין"      ← space in wrong place → ❌ not found!
Text:   "בית דין"      ← space in right place → ✅ found
```

### Three Approaches to a Solution

#### Approach A: Flexible Spacing (Space-Flexible Regex) — **Recommended for MVP**

**Idea**: A "flexible spacing" checkbox that makes spaces in the query **optional** in the regex.

**The change** — a single character:
```python
# Regular (gap=0) — current state:
sep = r'[^\w\u0590-\u05FF\']+'     # required: one or more non-word characters
#                              ^  plus = one or more

# Flexible spacing — proposed:
sep = r'[^\w\u0590-\u05FF\']*'     # optional: zero or more
#                              ^  asterisk = zero or more
```

**What this provides — all cases:**
```
Query:   בית דין (court)      + ☑ Flexible Spacing
regex:    בית[^\w]*דין

Finds:
  "בית דין"    ✅  (space → [^\w]* matches the space)
  "ביתדין"     ✅  (no space → [^\w]* matches zero characters)
  "ביתד ין"    ❌  (space in different place — does not match!)
```

**Note**: Approach A **does not solve** the "shifted boundary" case (`ביתד ין`). It only solves merged words.

**More precise implementation** — in addition to the separator, also allow an optional space **within** each term:

```python
# Improved approach: add \s* between every letter in the query
def make_space_flexible(term):
    """בית → ב\s*י\s*ת"""
    chars = list(term)
    return r'\s*'.join(re.escape(c) for c in chars)

# Then:
# "בית דין" → ב\s*י\s*ת\s*ד\s*י\s*ן
# Finds: "בית דין" ✓  "ביתדין" ✓  "ביתד ין" ✓  "בי תדין" ✓
```

**Advantages**:
- Minimal change (can start with just `+`→`*`, and upgrade later)
- Works with all existing search mechanisms (variants, wildcards, OR groups)
- Solves most cases

**Disadvantages**:
- The `\s*`-per-char version may **produce overly long matches** (a sequence `ב...י...ת` in a long text)
- No Tantivy awareness — Tantivy searches for exact terms, does not know about flexible spacing
- **Solution for Tantivy**: search for both the joined and separated forms: `"בית" AND "דין"` + `"ביתדין"`

#### Approach B: Dual-Track Search

**Idea**: Run two searches in parallel — one regular and one on text **with spaces removed**.

```python
def search_with_flexible_spacing(query, text):
    # Track 1: Regular search (as today)
    results_normal = regex_search(query, text)

    # Track 2: Spaceless search
    stripped_text = text.replace(' ', '').replace('\n', '')
    stripped_query = query.replace(' ', '')
    results_stripped = regex_search(stripped_query, stripped_text)

    # Merge results (with deduplication by document ID)
    return merge(results_normal, results_stripped)
```

**What this solves beyond Approach A**:
```
Query: אלכלמה (al-kalima, the word)
Track 1 (regular): finds "אלכלמה" ✅
Track 2 (spaceless): also finds "אל כלמה" ✅ (because without spaces it becomes "אלכלמה")
```

**Advantages**: Finds **everything** — both merged words, split words, and shifted boundaries

**Disadvantages**:
- Requires an additional Tantivy index (a `content_nospaces` field) **or** double regex scanning — doubles the time
- Highlighting is complex: we found a match in stripped text, need to map back to the original text to show the user
- Adds false positives: "גמל" (camel) would be found inside "דוגמלאומית" (national) (without spaces)

**Worth considering**: An additional Tantivy field `content_nospaces` that is indexed at index-build time. This adds ~30% to the index size but solves the problem at the recall level.

#### Approach C: N-gram Index (Character-Level)

**Idea**: Index the text as character n-grams (trigrams/4-grams) in Tantivy. N-grams cross word boundaries (after removing spaces).

```
"בית דין" → (remove spaces) → "ביתדין"
          → trigrams: "בית", "יתד", "תדי", "דין"
          → 4-grams:  "ביתד", "יתדי", "תדין"
```

Searching `בית דין` (court) → trigrams: `"בית" AND "יתד" AND "תדי" AND "דין"` — will find text containing the sequence ביתדין, regardless of where the space is.

**Advantages**: Solves the spacing problem in a **fundamental** way, space-agnostic, also works for fuzzy matching

**Disadvantages**:
- Significantly larger index (3-4x more terms)
- Low precision: short trigrams produce many false positives (every document containing "בית" and "דין" separately)
- Requires a structural change to the index (rebuild)
- High implementation complexity

**Suitable for Phase 3 if there is demand** — not for MVP.

---

## 3. Combining Both Features — Interaction Analysis

### Judeo-Arabic + Flexible Spacing = A Compounded Problem

The two problems **reinforce** each other in Judeo-Arabic:

1. The definite article can be written **attached** (`אלכלמה`) or **separate** (`אל כלמה`) — a spacing problem
2. Preposition + al- can be **attached** (`באלכלמה`) or **separate** (`ב אל כלמה` / `באל כלמה`) — combination of both problems
3. An assimilated sun letter can be written in different forms: `אלשוא`, `אששוא`, `אל שוא`, `אש שוא`

**Example from the corpus** — the word "אלחרוף" (al-ḥurūf, "the letters"):
```
Occurrence 1: אלחרוף     (attached, correct)
Occurrence 2: אל חרוף    (separated — transcriber placed a space)
Occurrence 3: אלארוף     (spelling error: ח→א, or misreading)
Occurrence 4: אלחרופ     (spelling error: ף→פ)
```

**Without the new features** — 4 separate searches are needed. **With the features** — a single search:
```
##(חרוף/ארוף)  + ☑ Variants + ☑ Flexible Spacing
→ finds all 4 occurrences
```

### Interaction: Variants + Judeo-Arabic

When variants are active, they apply **to every expanded form**. This means:
- `##כלמה` + variants → variants for "כלמה", for "אלכלמה", for "ואלכלמה" as well
- `כלמה`→`כלמא` (ה→א), but also `אלכלמה`→`אלכלמא`

**Combinatorial explosion**: 14 forms (al- + combinations + sun letters) x 30 variants = 420 terms per single word.

**Solution**: As noted in `responsa_implementation_response.md` — a hard cap of `MAX_EXPANDED_TERMS = 500`. When exceeded: auto-downgrade variants to basic (30 pairs instead of 70/150) + warning to the user.

---

## 4. Combined Recommendation — What to Adopt

### Interface: Four Checkboxes

```
☑ Responsa Mode    ☐ Variants    ☐ Judeo-Arabic    ☐ Flexible Spacing    Gap: [3]
```

| Checkbox | What It Does | When Useful |
|----------|-------------|-------------|
| **Responsa Mode** | Enables `*`, `#`, `(/)` syntax | Always |
| **Variants** | Letter substitutions (ד↔ר, ב↔כ, ...) | Unclear handwriting, inconsistent spelling |
| **Judeo-Arabic** | `#` also expands with al-, `##` always expands | Judeo-Arabic texts |
| **Flexible Spacing** | Spaces become optional | Bad OCR, incorrect spacing |

### Implementation Table

| Feature | Approach | Scope | Priority | Phase |
|---------|----------|-------|----------|-------|
| **Definite article** | `expand_judeo_arabic_prefixes()` + checkbox | ~40 lines | High | 1 |
| **Sun letters** | Included in expand function | ~15 lines | High | 1 |
| **Flexible spacing (basic)** | `+`→`*` in separator | ~5 lines | High | 1 |
| **Flexible spacing (advanced)** | `\s*` between letters | ~20 lines | Medium | 1 |
| **Tantivy: send joined form** | `"ביתדין"` in addition to `"בית" AND "דין"` | ~10 lines | Medium | 1 |
| Dual-track search (stripped) | Tantivy `content_nospaces` field | Index change | Low | 2 |
| N-gram index | Character trigram index | Structural change | Low | 3 |

### Pipeline — How Everything Works Together

```
Input:   ##(חרוף/אותיות) ##כלמה    + ☑ Variants + ☑ JA + ☑ Flexible Spacing + gap=3
         (letters/letters) (word)

Step 1 — Responsa parsing:
  Component 1: OR group [חרוף, אותיות], prefix=##
  Component 2: [כלמה], prefix=##

Step 2 — Judeo-Arabic expansion (##):
  חרוף → חרוף, אלחרוף, ואלחרוף, באלחרוף, ...
  אותיות → אותיות, אלאותיות, ואלאותיות, ...
  כלמה → כלמה, אלכלמה, ואלכלמה, באלכלמה, ...

Step 3 — Variant expansion (per term):
  חרוף → חרוף, חרופ, ארוף, הרוף, ...
  אלחרוף → אלחרוף, אלחרופ, אלארוף, ...
  כלמה → כלמה, כלמא, כלמת, ...
  אלכלמה → אלכלמה, אלכלמא, ...
  (cap: MAX_EXPANDED_TERMS = 500)

Step 4 — Tantivy query:
  ("חרוף"^5 OR "אלחרוף" OR "ארוף" OR "אותיות" OR "אלאותיות" OR ...)
  AND
  ("כלמה"^5 OR "אלכלמה" OR "כלמא" OR ...)

Step 5 — Regex (flexible spacing):
  (ח\s*ר\s*ו\s*ף|א\s*ל\s*ח\s*ר\s*ו\s*ף|...)
  [gap separator]
  (כ\s*ל\s*מ\s*ה|א\s*ל\s*כ\s*ל\s*מ\s*ה|...)

Step 6 — Execute:
  Tantivy → ~3K candidates
  Regex → ~25 matches
  Highlight → highlighted results
```

---

## 5. Realistic Usage Examples

All examples are based on **real** text from `Transcriptions_part.txt`.

### Example 1: Searching for a Grammatical Term in Judeo-Arabic
```
Query:   ##שוא (shva)
Expanded: שוא | אלשוא | ואלשוא | באלשוא | פאלשוא | אששוא | ואששוא | באששוא ...

Finds in corpus:
  Line 32:  "פצל פי אלכלאם עלי אלשוא"        ← אלשוא ✅
  Line 33:  "קסמין סאכן ומתחרך"               ← (does not contain shva) ❌
  Line 38:  "אלשוא סאכנא ואנפצל"              ← אלשוא ✅
  Line 108: "פצר פי אלכלאם עלי אלשוא"         ← אלשוא (second reading) ✅
```
**Without ##**: Searching `שוא` finds only occurrences without al-. Searching `אלשוא` misses `ואלשוא`.

### Example 2: Flexible Spacing — Haggadah Text
```
Query:   גורשו ממצרים (expelled from Egypt)       + ☑ Flexible Spacing
regex:    ג\s*ו\s*ר\s*ש\s*ו\s*מ\s*מ\s*צ\s*ר\s*י\s*ם

Finds:
  Line 161: "גורשו ממצרים"     ← regular ✅
  Line 209: "יר שו ממצרים"     ← erroneous reading, but "שו ממצרים" ✅ (partial)
```
**Note**: In the case of line 209, "יר שו" is an erroneous reading of "גורשו". Variants could help with "גורשו"↔"יורשו" but not with "יר שו". Only spaceless search (Approach B) would help here.

### Example 3: Full Combination — Searching for a Grammatical Concept
```
Query:   ##(חרוף/אותיות) ##(כלמה/מלה)
         (letters)        (word)
Options: ☑ Variants + ☑ JA + ☑ Flexible Spacing + gap=3

Finds in corpus:
  Line 14:  "אלחרוף ]אל מהא תנבדל"            ← אלחרוף ... (gap)
  Line 57:  "פצל פי מחאל אלחרוף"               ← אלחרוף ✅
  Line 71:  "פצל פי מחאל אלחרוף"               ← אלחרוף (Reading B) ✅
  Line 130: "פצל פי מחל אלארוף א אלחרוף אכ"   ← אלארוף (variant: ח→א) ✅
```

### Example 4: Sun Letters — Two Forms
```
Query:   ##לסאן (lisān, tongue)
Expanded: לסאן | אללסאן | ואללסאן | באללסאן | ...
          (lamed is a sun letter → assimilation: אל+ל=אלל, which is identical to regular spelling)

Finds:
  Line 10: "אללסאן ממא ילי"     ← אללסאן ✅
  Line 75: "ואצל אללסאן"        ← אללסאן ✅
  Line 87: "תלת אללסאן ממא"     ← אללסאן (Reading B) ✅
```

### Example 5: Judeo-Arabic + Hebrew Prefixes
```
Query:   ##תורה (Torah)
Expanded:
  Hebrew:  תורה, ותורה, התורה, בתורה, לתורה, ...
  JA:      אלתורה, ואלתורה, באלתורה, ...
  Sun:     אתתורה, ואתתורה, ...

Finds both Hebrew texts and Judeo-Arabic translations in the same search.
```
**This is the power of the approach**: A researcher working on a Judeo-Arabic biblical commentary finds both Hebrew quotations ("התורה", "the Torah" in Hebrew) and Judeo-Arabic discussion ("אלתורה", "the Torah" in JA) — **in a single search**.

---

## 6. Edge Cases and Limitations

### Limitation 1: Flexible Spacing + Short Queries
`\s*` between every letter makes 2-letter queries dangerous:
```
Query: אל (al)    + ☑ Flexible Spacing
regex:  א\s*ל
Finds: nearly every line in the corpus (א and ל appear everywhere)
```
**Solution**: Require a minimum of 3 letters when flexible spacing is active.

### Limitation 2: al- That Is Not the Definite Article
The word "אל" in Hebrew is also a negation particle ("do not") and part of divine names ("אלהים", God). Expanding with ## will also find non-Arabic occurrences.
**Solution**: There is no automatic solution. This is disambiguation that requires context. The user will filter manually.

### Limitation 3: Combinatorial Explosion with Full Feature Combination
```
##word + variants + JA + flexible spacing
= 14 forms × 30 variants × regex-per-char
= very long regex
```
**Solution**: MAX_EXPANDED_TERMS = 500 + auto-downgrade. Flexible spacing applies only at the regex level (does not multiply terms in Tantivy).

### Limitation 4: Tantivy Does Not Find Joined Forms
`בית דין` (court) → Tantivy searches for `"בית" AND "דין"`. If the text says `ביתדין` (one word), Tantivy **will not retrieve** the document.
**Phase 1 solution**: Add the joined form to the Tantivy query as well: `("בית" AND "דין") OR "ביתדין"`.
**Phase 2 solution**: A `content_nospaces` field in the index.
