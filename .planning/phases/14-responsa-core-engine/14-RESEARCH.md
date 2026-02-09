# Phase 14: Responsa Core Engine - Research

**Researched:** 2026-02-09
**Domain:** Hebrew/Judeo-Arabic search query parsing, term expansion, regex pattern building, Tantivy full-text search integration
**Confidence:** HIGH

## Summary

Phase 14 implements the core engine logic for Responsa-style search in `genizah_core.py`. The existing codebase already has a two-phase search architecture (Tantivy for candidate retrieval with OR groups and boosting, Regex for precise filtering and highlighting) that maps directly onto the Responsa feature requirements. The key new capabilities are: (1) a Responsa query parser that understands `*`, `#`, and `(/)` syntax, (2) Hebrew grammatical prefix expansion, (3) Judeo-Arabic definite article expansion with sun letter assimilation, (4) wildcard-to-regex translation, (5) bidirectional gap via regex alternation, (6) flexible spacing for OCR issues, and (7) a combinatorial explosion guard.

The implementation follows Option II (Hybrid Integration) as decided in the planning documents. All new logic resides in `genizah_core.py` (shared core) per XAPP-02. The existing `build_tantivy_query()` and `build_regex_pattern()` methods already build OR groups with boosting and regex alternation groups respectively -- the Responsa engine extends these patterns with additional expansion logic. No new external dependencies are required.

**Primary recommendation:** Implement as a `ResponsaComponent` dataclass and a set of pure functions (`parse_responsa_query`, `expand_grammatical_prefixes`, `expand_judeo_arabic`) that produce expanded term lists, then feed them into the existing `build_tantivy_query()` and `build_regex_pattern()` methods with minimal modifications.

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python `re` | stdlib | Regex pattern building and matching | Already used extensively in `genizah_core.py` for regex phase |
| Python `dataclasses` | stdlib | `ResponsaComponent` structured data | Already used in `corrections_client.py`, `web/services.py` |
| tantivy-py | existing | Full-text search index (OR groups, boosting) | Already the search backend -- no version change |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `unified_variants.py` | existing | Character substitution pairs for variants expansion | When Variants checkbox is ON in Responsa mode |
| `VariantManager` | existing class | Generate spelling variants for expanded terms | Called per-term when variants enabled |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Dataclass | Dict/NamedTuple | Dataclass preferred -- mutable, clear field names, matches codebase patterns |
| Separate file for Responsa logic | Keep in genizah_core.py | Planning docs suggest class inside genizah_core.py now, extract later if needed. File is already 7K+ lines so keeping it together avoids import complexity |

**Installation:**
```bash
# No new dependencies -- all stdlib + existing packages
```

## Architecture Patterns

### How the New Code Fits Into Existing Structure

```
genizah_core.py (existing ~7000 lines)
|
|-- class Config (line 1591)
|     +-- MAX_EXPANDED_TERMS = 500          # NEW constant
|
|-- class VariantManager (line 2043)        # UNCHANGED -- called by expansion
|
|-- class SearchEngine (line 4139)
|     |-- build_tantivy_query() (line 4196) # MODIFIED: accept ResponsaComponent[]
|     |-- build_regex_pattern() (line 4245) # MODIFIED: wildcards, flex spacing, bidir
|     |-- parse_query_syntax() (line 4433)  # MODIFIED: bypass when responsa_mode=True
|     |-- execute_search() (line 4470)      # MODIFIED: new responsa_mode path
|     |
|     +-- NEW functions/class:
|         |-- @dataclass ResponsaComponent
|         |-- parse_responsa_query()
|         |-- expand_grammatical_prefixes()
|         |-- expand_judeo_arabic()
|         +-- _apply_explosion_guard()
```

### Pattern 1: ResponsaComponent Dataclass
**What:** Structured representation of a parsed Responsa query component
**When to use:** Output of `parse_responsa_query()`, input to `build_tantivy_query()` and `build_regex_pattern()`
**Example:**
```python
# Based on planning docs and existing codebase patterns
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ResponsaComponent:
    """A single component of a Responsa query."""
    words: List[str]                          # Base words (from OR groups or single word)
    grammatical_prefixes: bool = False        # # prefix -- expand Hebrew grammatical prefixes
    wildcard: Optional[str] = None            # None, 'suffix', 'prefix', 'pattern'
    inline_pattern: Optional[str] = None      # For inline alternations like אירו(ס/ש)ין
```

### Pattern 2: Term Expansion Pipeline
**What:** Sequential expansion: parse -> prefix expand -> JA expand -> variant expand -> guard
**When to use:** Every Responsa search goes through this pipeline

```python
# Expansion pipeline (conceptual flow)
def expand_responsa_components(components, variants_on, ja_on, var_mgr, mode):
    """
    For each component:
    1. Start with base words
    2. If grammatical_prefixes: expand each word with Hebrew prefixes (~10 forms)
    3. If ja_on: expand each word with JA article forms (8-14 per word)
    4. If variants_on: expand each term with VariantManager
    5. Check explosion guard (MAX_EXPANDED_TERMS=500)
    """
    all_tantivy_terms = []
    all_regex_parts = []
    total_terms = 0

    for comp in components:
        expanded = set(comp.words)

        if comp.grammatical_prefixes:
            for word in comp.words:
                expanded.update(expand_grammatical_prefixes(word))
                if ja_on:
                    expanded.update(expand_judeo_arabic(word))

        if variants_on:
            with_variants = set()
            for term in expanded:
                with_variants.update(var_mgr.get_variants(term, mode, limit=200))
            expanded.update(with_variants)

        total_terms += len(expanded)
        # ... build tantivy and regex parts

    return all_tantivy_terms, all_regex_parts, total_terms
```

### Pattern 3: Existing OR Group Pattern (to follow)
**What:** The current `build_tantivy_query()` already builds OR groups with boosting
**Source:** `genizah_core.py:4220-4241`
```python
# EXISTING pattern in build_tantivy_query():
clean_vars.append(f'"{term}"^5')      # exact boosted
clean_vars.append(f'"{v_clean}"')      # variant
parts.append(f'({" OR ".join(clean_vars)})')
# Result: ("שלום"^5 OR "סלום" OR "שלו") AND ("עולם"^5 OR "עולס")
```

Responsa components should produce identical structure -- just with different expansion logic feeding the term lists.

### Pattern 4: Regex Group Pattern (to follow)
**What:** The current `build_regex_pattern()` builds alternation groups sorted by length
**Source:** `genizah_core.py:4250-4271`
```python
# EXISTING pattern in build_regex_pattern():
unique_vars = sorted(list(set(vars_list)), key=len, reverse=True)
escaped = [re.escape(v) for v in unique_vars]
parts.append(f"({'|'.join(escaped)})")
# Result: (שלומות|ושלום|השלום|שלום) [sep] (עולמות|עולם)
```

### Anti-Patterns to Avoid
- **Separate search path:** Do NOT create a completely separate `execute_responsa_search()` function. Instead, extend `execute_search()` with a `responsa_mode` parameter that feeds ResponsaComponents into the existing Tantivy+Regex pipeline. This prevents code duplication and ensures all existing features (exclude words, deduplication, highlighting, boundary crossing) work automatically.
- **Modifying VariantManager:** Do NOT change the VariantManager class. Use it as-is -- call `get_variants()` on expanded terms. Variants are orthogonal to Responsa expansion.
- **Regex-only approach (Option I):** Do NOT bypass Tantivy. The whole point of Option II is that Tantivy receives focused OR groups for better candidate retrieval.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hebrew variant generation | Custom character substitution | `VariantManager.get_variants()` | Already handles 25,802 frequency-sorted pairs, caching, tier limits |
| Tantivy OR groups | New query builder | Existing `build_tantivy_query()` pattern | Already handles boosting, quoting, OR joining |
| Regex alternation groups | New regex builder | Existing `build_regex_pattern()` pattern | Already handles sorting by length, escaping, gap separators |
| Gap separators | Custom gap regex | Existing separator logic in `build_regex_pattern()` lines 4273-4278 | Already handles word-boundary-aware gap counting |
| Result deduplication | Custom dedup | Existing `_deduplicate()` method | Already handles V0.7/V0.8 priority |
| Exclude words filtering | Custom filter | Existing post-search filter in `execute_search()` lines 4604-4621 | Already integrated into result pipeline |

**Key insight:** Phase 14 is an **extension** of the existing search pipeline, not a replacement. The core innovation is in parsing Responsa syntax and expanding terms -- the actual search execution reuses existing infrastructure.

## Common Pitfalls

### Pitfall 1: Combinatorial Explosion
**What goes wrong:** `#word` with JA + variants can produce 10 * 14 * 30 = 4,200 terms per word. A 3-word query could generate 12,600 terms.
**Why it happens:** Each expansion layer multiplies the previous count.
**How to avoid:** Implement the downgrade cascade EARLY (before building queries). The cascade is: (1) downgrade variants to basic (30 pairs), (2) disable variants entirely, (3) disable JA, (4) error with message.
**Warning signs:** Tantivy query string exceeds ~100KB, regex compilation takes >1 second, or `total_terms > MAX_EXPANDED_TERMS`.

### Pitfall 2: Regex Catastrophic Backtracking
**What goes wrong:** Flexible spacing with `\s*` between every character creates patterns like `א\s*ל\s*כ\s*ל\s*מ\s*ה` that can cause exponential backtracking on non-matching text.
**Why it happens:** `\s*` is greedy and the regex engine tries many combinations before failing.
**How to avoid:** Apply flexible spacing (`\s*` per char) ONLY to original terms (before expansion), not to every expanded variant. Use possessive quantifiers or atomic groups if available. Set a regex timeout or limit match attempts. The planning docs explicitly state "flex spacing on original terms only."
**Warning signs:** Regex search phase takes >10 seconds on a single document.

### Pitfall 3: Wildcard + Tantivy Mismatch
**What goes wrong:** `שלום*` should match `שלומות` but Tantivy only searches exact terms, not wildcards. If only `"שלום"` is sent to Tantivy, documents containing only `שלומות` (without `שלום`) are missed.
**Why it happens:** Tantivy is a term-index, not a regex engine.
**How to avoid:** For suffix wildcards, send the stem to Tantivy (best effort recall) and rely on regex for precision. Accept this as a known limitation documented in the planning docs. For character patterns (`*פ*ט*ר*פ*`), extract longest consecutive letter sequences as Tantivy terms, or use bigrams.
**Warning signs:** Users report "I know this text exists but search doesn't find it" when using wildcards.

### Pitfall 4: `#` Symbol Conflict
**What goes wrong:** `#` currently means "Shelfmark search" in `parse_query_syntax()`. In Responsa mode, `#` means "grammatical prefixes."
**Why it happens:** Symbol reuse between modes.
**How to avoid:** When `responsa_mode=True`, bypass `parse_query_syntax()` entirely (CORE-08). The query goes directly to `parse_responsa_query()` which interprets `#` as prefix expansion. The existing prefix shortcuts (`?`, `=`, `~`, `/`, `$`, `#`) are all disabled in Responsa mode.
**Warning signs:** User gets Shelfmark results when they expected prefix expansion.

### Pitfall 5: Sun Letter Assimilation Edge Cases
**What goes wrong:** The letter lamed (ל) is both a sun letter AND the lamed in "al-" (אל). So `אל + לסאן = אללסאן`, which looks like a doubled lamed but is actually the regular form.
**Why it happens:** The assimilated form is identical to the regular form for lamed-initial words.
**How to avoid:** The `expand_judeo_arabic()` function should handle this correctly by generating the assimilated form `א + first_letter + word`, which for lamed gives `אללסאן` -- identical to `אל + לסאן`. The deduplication in `set()` handles this naturally.
**Warning signs:** Duplicate terms in expansion lists.

### Pitfall 6: Breaking Existing Search Modes
**What goes wrong:** Modifying `build_tantivy_query()` and `build_regex_pattern()` breaks existing Exact/Variants/Fuzzy/Regex searches.
**Why it happens:** These functions are called for ALL search modes, not just Responsa.
**How to avoid:** Add the Responsa path as a separate branch (check for `mode == 'responsa'` or check if input is `ResponsaComponent[]` vs `str[]`). Keep existing paths untouched. Write regression tests for existing modes.
**Warning signs:** Existing search tests fail after changes.

## Code Examples

### Example 1: Grammatical Prefix Expansion
```python
# From planning docs (05_judeo_arabic_spacing.md), verified against requirements
GRAMMATICAL_PREFIXES = [
    '',                                     # base word (no prefix)
    'ו', 'ה', 'ב', 'כ', 'ל', 'מ', 'ש',   # single-letter
    'וה', 'וב', 'וכ', 'ול', 'ומ', 'וש',   # vav + single
    'שה', 'שב', 'שכ', 'של', 'שמ',         # shin + single
    'כש', 'כשה',                           # kaf-shin combinations
    'מה', 'בש', 'לכ',                      # other common combos
]
# Total: ~25 prefixes, producing ~25 forms per word
# The AGENT_BRIEF says "~10 forms" -- this is the full set from 01_feature_analysis.md
# Recommendation: start with the 8 basic (empty + 7 single-letter), add combos if needed

def expand_grammatical_prefixes(word):
    """Expand word with Hebrew grammatical prefixes. Returns list of forms."""
    return [prefix + word for prefix in GRAMMATICAL_PREFIXES if prefix + word]
```

### Example 2: Judeo-Arabic Expansion with Sun Letters
```python
# From 05_judeo_arabic_spacing.md
SUN_LETTERS = set('תדרזסשצטלנ')

def expand_judeo_arabic(word):
    """Expand word with Judeo-Arabic definite article forms."""
    forms = [word]

    # Regular al-
    forms.append('אל' + word)

    # Preposition + al-
    for prep in ['ו', 'ב', 'פ', 'כ', 'ל']:
        forms.append(prep + 'אל' + word)

    # Contracted lamed: ל + אל -> לל
    forms.append('לל' + word)

    # Sun letter assimilation
    if word and word[0] in SUN_LETTERS:
        assimilated = 'א' + word[0] + word
        forms.append(assimilated)
        for prep in ['ו', 'ב', 'פ', 'כ', 'ל']:
            forms.append(prep + assimilated)

    return forms
# Moon letter word: 8 forms (word + al + 5 preps + ll)
# Sun letter word: 14 forms (8 + assimilated + 5 prep-assimilated)
```

### Example 3: Explosion Guard Cascade
```python
MAX_EXPANDED_TERMS = 500

def apply_explosion_guard(components, variants_on, ja_on, var_mgr, mode):
    """
    Try to expand terms. If over limit, cascade downgrade.
    Returns (expanded_components, warning_message, actual_options).
    """
    options = {'variants': variants_on, 'ja': ja_on, 'mode': mode}

    # Try full expansion
    total = count_expanded_terms(components, options, var_mgr)

    if total <= MAX_EXPANDED_TERMS:
        return expand_all(components, options, var_mgr), None, options

    # Cascade 1: Downgrade variants to basic (30 pairs)
    options['mode'] = 'variants'
    total = count_expanded_terms(components, options, var_mgr)
    if total <= MAX_EXPANDED_TERMS:
        return expand_all(components, options, var_mgr), \
            "Variants downgraded to basic (30 pairs) to stay within term limit.", options

    # Cascade 2: Disable variants
    options['variants'] = False
    total = count_expanded_terms(components, options, var_mgr)
    if total <= MAX_EXPANDED_TERMS:
        return expand_all(components, options, var_mgr), \
            "Variants disabled to stay within term limit.", options

    # Cascade 3: Disable JA
    options['ja'] = False
    total = count_expanded_terms(components, options, var_mgr)
    if total <= MAX_EXPANDED_TERMS:
        return expand_all(components, options, var_mgr), \
            "Variants and Judeo-Arabic disabled to stay within term limit.", options

    # Cascade 4: Error
    raise ValueError(
        f"Query too complex: {total} expanded terms exceeds maximum of {MAX_EXPANDED_TERMS}. "
        "Simplify the query or reduce the number of OR alternatives."
    )
```

### Example 4: Bidirectional Gap Regex
```python
# From AGENT_BRIEF.md -- simple regex alternation
def build_bidirectional_regex(regex_parts, separator):
    """Build regex that matches terms in either order."""
    forward = separator.join(regex_parts)
    backward = separator.join(reversed(regex_parts))
    return f'({forward})|({backward})'

# Example:
# parts = ['(שלום|ושלום|השלום)', '(עולם|העולם)']
# sep = gap separator
# Result: ((שלום|ושלום|השלום)[sep](עולם|העולם))|((עולם|העולם)[sep](שלום|ושלום|השלום))
```

### Example 5: Flexible Spacing
```python
def make_flex_spacing_pattern(term):
    """Add \\s* between each character for OCR-flexible matching."""
    chars = list(term)
    return r'\s*'.join(re.escape(c) for c in chars)

# "בית" -> "ב\\s*י\\s*ת"
# Matches: "בית" (normal), "בי ת" (split), "ב ית" (shifted)

# IMPORTANT: Apply only to ORIGINAL terms, not expanded variants
# This prevents pattern explosion
```

### Example 6: Parse Responsa Query
```python
def parse_responsa_query(query_str):
    """
    Parse Responsa syntax into components.

    Syntax:
    - word       -> plain word
    - word*      -> suffix wildcard
    - *word      -> prefix wildcard
    - *a*b*c*    -> character pattern
    - #word      -> grammatical prefixes
    - (a/b/c)    -> OR alternatives
    - #(a/b/c)   -> prefixed OR group
    - a(x/y)b    -> inline alternation

    Returns: List[ResponsaComponent]
    """
    # Tokenize: split by whitespace, keeping compound tokens together
    # Handle: #(word1/word2)* as a single token
    # The parser must handle nested parens for OR groups
    tokens = tokenize_responsa(query_str)
    components = []

    for token in tokens:
        comp = ResponsaComponent(words=[])

        # Check for # prefix
        if token.startswith('#'):
            comp.grammatical_prefixes = True
            token = token[1:]

        # Check for wildcards
        if token.endswith('*') and not token.startswith('*'):
            comp.wildcard = 'suffix'
            token = token[:-1]
        elif token.startswith('*') and not token.endswith('*'):
            comp.wildcard = 'prefix'
            token = token[1:]
        elif token.startswith('*') and token.endswith('*') and '*' in token[1:-1]:
            comp.wildcard = 'pattern'
            # Keep full pattern for later processing

        # Check for OR group: (word1/word2/word3)
        if token.startswith('(') and token.endswith(')'):
            inner = token[1:-1]
            comp.words = [w.strip() for w in inner.split('/') if w.strip()]
        # Check for inline alternation: word(a/b)word
        elif '(' in token and ')' in token:
            comp.inline_pattern = token
            comp.words = [token]  # Will be handled at regex level
        else:
            comp.words = [token] if token else []

        if comp.words:
            components.append(comp)

    return components
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single regex preprocessor (Option I) | Hybrid integration (Option II) -- parse to components, feed both Tantivy and Regex | Decided in planning session, 2026-02-09 | Better Tantivy recall via focused OR groups |
| `#` = Shelfmark only | `#` = Shelfmark (normal mode) or Grammatical Prefixes (Responsa mode) | Phase 14 | Mode-dependent symbol interpretation |
| Fixed gap (unidirectional) | Bidirectional gap via regex alternation | Phase 14 | Users can search both word orders |
| No term expansion | Hebrew prefix + JA article expansion | Phase 14 | Dramatically better recall for prefixed forms |

**Not changing:**
- Two-phase search architecture (Tantivy candidates -> Regex filter)
- VariantManager and unified_variants.py
- Existing search modes (Exact, Variants, Fuzzy, Regex, Shelfmark, Title)
- URL routing, result format, highlighting mechanism

## Existing Code Integration Points

### Functions to MODIFY (minimal changes)

| Function | Line | Change | Risk |
|----------|------|--------|------|
| `SearchEngine.build_tantivy_query()` | 4196 | Add branch for `mode == 'responsa'` that accepts pre-expanded term lists per component | LOW -- new branch, existing path unchanged |
| `SearchEngine.build_regex_pattern()` | 4245 | Add branch for `mode == 'responsa'` with wildcard patterns, flex spacing, bidirectional gap | LOW -- new branch, existing path unchanged |
| `SearchEngine.execute_search()` | 4470 | Add `responsa_mode` parameter; when True, parse query -> expand -> build queries differently | LOW -- parameter addition, early branch |
| `Config` class | 1704 | Add `MAX_EXPANDED_TERMS = 500` constant | ZERO risk |

### Functions to ADD (new code)

| Function | Purpose | Estimated Lines |
|----------|---------|-----------------|
| `ResponsaComponent` dataclass | Structured query component | 10 |
| `parse_responsa_query()` | Parse Responsa syntax string into components | 60-80 |
| `expand_grammatical_prefixes()` | Hebrew prefix expansion | 15-20 |
| `expand_judeo_arabic()` | JA article expansion with sun letters | 25-30 |
| `_apply_explosion_guard()` | Cascade downgrade when terms exceed limit | 30-40 |
| `_expand_responsa_for_tantivy()` | Build Tantivy OR groups from components | 30-40 |
| `_expand_responsa_for_regex()` | Build regex patterns from components | 40-50 |

**Total new code: ~220-270 lines**

### Key Constants

| Constant | Value | Source |
|----------|-------|--------|
| `SEARCH_LIMIT` | 50,000 | Config (line 1704) -- max Tantivy candidates |
| `REGEX_VARIANTS_LIMIT` | 8,000 | Config (line 1706) -- max variants per term in regex |
| `WORD_TOKEN_PATTERN` | `[\w\u0590-\u05FF']+` | Config (line 1707) -- used in gap separator |
| `MAX_EXPANDED_TERMS` | 500 | NEW -- total terms across all components |
| Exact term boost | `^5` | Existing pattern (line 4222) |
| Length-diff variant boost | `^3` | Existing pattern (line 4237) |

## Testing Strategy

### Unit Tests (no index required)

Tests can be written as pure Python unit tests with no Tantivy index dependency:

1. **`test_parse_responsa_query()`** -- parse syntax into components
   - `"#(שלום/שלומות) עולם*"` -> 2 components, first with prefixes, second with suffix wildcard
   - `"*פ*ט*ר*פ*"` -> 1 component with pattern wildcard
   - `"אירו(ס/ש)ין"` -> 1 component with inline alternation
   - Empty string -> empty list
   - Single word -> 1 component, no modifiers

2. **`test_expand_grammatical_prefixes()`**
   - `"שלום"` -> includes שלום, ושלום, השלום, בשלום, כשלום, לשלום, משלום, ששלום, plus combos
   - Count: ~25 forms (or ~10 if using basic set)

3. **`test_expand_judeo_arabic()`**
   - Moon letter: `"כלמה"` -> 8 forms (no assimilation)
   - Sun letter: `"שוא"` -> 14 forms (includes assimilated)
   - Lamed: `"לסאן"` -> 8 forms (assimilated = regular for lamed)

4. **`test_explosion_guard()`**
   - Under limit: all options preserved
   - Over limit with variants+JA: variants downgraded first
   - Way over limit: JA disabled, warning returned
   - Extreme: error raised

5. **`test_bidirectional_regex()`**
   - 2 parts -> `(A[sep]B)|(B[sep]A)`
   - 3 parts -> `(A[sep]B[sep]C)|(C[sep]B[sep]A)`

6. **`test_flex_spacing()`**
   - `"בית"` -> `ב\s*י\s*ת`
   - Applied to original terms only, not variants

7. **`test_wildcard_regex()`**
   - `"שלום"` + suffix -> `שלום\S*`
   - `"נדר"` + prefix -> `\S*נדר`
   - Character pattern -> `\S*פ\S*ט\S*ר\S*פ\S*`

8. **`test_mode_bypass()`** (CORE-08)
   - When responsa_mode=True, `parse_query_syntax()` shortcuts are NOT applied
   - `#שלום` in Responsa mode -> prefix expansion (not Shelfmark search)

## Open Questions

1. **Prefix list size**
   - What we know: Planning docs show ~25 prefixes (basic 7 + combinations). AGENT_BRIEF says "~10 forms."
   - What's unclear: Should we use the full 25-prefix set or the basic 8 (empty + 7 singles)?
   - Recommendation: Start with the full set from `01_feature_analysis.md` (~25 prefixes). The explosion guard protects against overcounting. More recall is better for researchers.

2. **Flex spacing scope**
   - What we know: "Apply `\s*` per char on ORIGINAL terms only" per requirements.
   - What's unclear: Does "original terms" mean the base words before ANY expansion, or the words after prefix/JA expansion but before variant expansion?
   - Recommendation: Apply flex spacing to base words only (before prefix and variant expansion). This is the safest interpretation and prevents pattern explosion.

3. **Wildcard + Tantivy recall gap**
   - What we know: `שלום*` sends `"שלום"` to Tantivy but documents containing only `שלומות` may not match Tantivy's exact term lookup.
   - What's unclear: How significant is this gap in practice?
   - Recommendation: Accept the gap for Phase 14. Document it. Consider `SEARCH_LIMIT` increase for wildcard queries in a future enhancement.

4. **Inline alternation handling**
   - What we know: `אירו(ס/ש)ין` should become regex `אירו[סש]ין` (character class).
   - What's unclear: Should multi-char alternatives like `אירו(סין/שין)` use alternation `(אירוסין|אירושין)` instead?
   - Recommendation: Single-char alternatives -> character class `[ab]`. Multi-char alternatives -> alternation `(a|b)`. The parser should detect this automatically.

## Sources

### Primary (HIGH confidence)
- `genizah_core.py` lines 4196-4283 -- existing `build_tantivy_query()` and `build_regex_pattern()` implementation
- `genizah_core.py` lines 4433-4468 -- existing `parse_query_syntax()` prefix handling
- `genizah_core.py` lines 4470-4624 -- existing `execute_search()` full pipeline
- `genizah_core.py` lines 2043-2068 -- VariantManager configuration and tier limits
- `docs/plans/responsa-search/AGENT_BRIEF.md` -- master implementation overview
- `docs/plans/responsa-search/05_judeo_arabic_spacing.md` -- JA expansion details, sun letters, spacing
- `docs/plans/responsa-search/06_ui_integration_sketch.md` -- Option IIb decision, flow diagrams

### Secondary (HIGH confidence)
- `docs/plans/responsa-search/02_options_report.md` -- Option II hybrid algorithm details
- `docs/plans/responsa-search/04_implementation_response.md` -- code validation, explosion guard design
- `.planning/ROADMAP.md` -- Phase 14 requirements and success criteria
- MEMORY.md -- v5.7.0 key decisions (explosion guard, flex spacing, `#` conflict resolution)

### Tertiary (MEDIUM confidence)
- `docs/plans/responsa-search/01_feature_analysis.md` -- Responsa Project feature comparison (historical)
- `docs/plans/responsa-search/03_review_insights.md` -- Review insights (historical, mostly addressed)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all existing patterns
- Architecture: HIGH -- extensive planning docs + verified against actual code
- Pitfalls: HIGH -- identified from planning docs and code review
- Code examples: HIGH -- derived from existing codebase patterns + planning docs
- Testing: HIGH -- pure unit testable, no external dependencies needed

**Research date:** 2026-02-09
**Valid until:** 2026-03-09 (stable domain, all decisions locked)
