# -*- coding: utf-8 -*-
"""Responsa query parsing and expansion for Hebrew manuscript search.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import ResponsaComponent, parse_responsa_query, ...``
callers continue working unchanged.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional

from genizah_translations import TRANSLATIONS

from shared.config import Config


def _tr(text: str) -> str:
    """Translate text if current language is Hebrew.

    Mirrors genizah_core.tr() — lazy import of CURRENT_LANG inside the
    function body so we always see the live value (Pitfall 2 of Phase 123).
    GUARD-01-safe: the import is function-body-only, not module-level.
    """
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text


GRAMMATICAL_PREFIXES = [
    '',     # bare word
    'ו', 'ה', 'ב', 'כ', 'ל', 'מ', 'ש',                     # single
    'וה', 'וב', 'וכ', 'ול', 'ומ', 'וש',                     # vav + prefix
    'שה', 'שב', 'שכ', 'של', 'שמ',                           # shin + prefix
    'כש', 'כשה', 'מה', 'בש', 'לכ',                          # misc combos
]

# Hebrew grammatical suffixes for # suffix expansion (~25 entries)
GRAMMATICAL_SUFFIXES = [
    '',      # bare word (no suffix)
    'ה',     # feminine singular
    'ת',     # feminine construct / verbal
    'ים',    # masculine plural
    'ות',    # feminine plural
    'י',     # 1st person singular possessive / construct
    'ך',     # 2nd person masculine singular possessive
    'ו',     # 3rd person masculine singular possessive
    'נו',    # 1st person plural possessive
    'כם',    # 2nd person masculine plural possessive
    'כן',    # 2nd person feminine plural possessive
    'ם',     # 3rd person masculine plural possessive (short)
    'ן',     # 3rd person feminine plural possessive (short)
    'הם',    # 3rd person masculine plural possessive (long)
    'הן',    # 3rd person feminine plural possessive (long)
    'ני',    # 1st person singular emphatic
    'הו',    # 3rd person masculine singular (variant)
    'יו',    # 3rd person masculine singular (on plurals)
    'יה',    # 3rd person feminine singular (on plurals)
    'ינו',   # 1st person plural (on plurals)
    'יך',    # 2nd person masculine singular (on plurals)
    'יכם',   # 2nd person masculine plural (on plurals)
    'יכן',   # 2nd person feminine plural (on plurals)
    'יהם',   # 3rd person masculine plural (on plurals)
    'יהן',   # 3rd person feminine plural (on plurals)
]



@dataclass
class ResponsaComponent:
    """Structured representation of a single token in a parsed Responsa query.

    Each component represents one logical search element:
    - A plain word, an OR group of words, a wildcard pattern, or an inline alternation.
    - The grammatical_prefixes flag indicates that Hebrew prefix expansion (leading #) is requested.
    - The grammatical_suffixes flag indicates that Hebrew suffix expansion (trailing #) is requested.
    - The plene_defective flag indicates that plene/defective spelling variants (%) are requested.
    """
    words: List[str]
    grammatical_prefixes: bool = False
    grammatical_suffixes: bool = False
    plene_defective: bool = False
    wildcard: Optional[str] = None          # None, 'suffix', 'prefix', 'pattern'
    wildcard_pattern: Optional[str] = None  # raw pattern for character patterns like *a*b*c*
    inline_pattern: Optional[str] = None    # for inline alternations like word(a/b)word
    negated: bool = False                   # True if prefixed with - (exclude from results)


def parse_responsa_query(query_str: str) -> List[ResponsaComponent]:
    """Parse a Responsa-Project style query string into a list of ResponsaComponent objects.

    Supported syntax:
    - Plain words: "שלום" -> single component
    - Suffix wildcard: "שלום*" -> wildcard='suffix'
    - Prefix wildcard: "*נדר" -> wildcard='prefix'
    - Character pattern: "*פ*ט*ר*פ*" -> wildcard='pattern'
    - Grammatical prefixes: "#שלום" -> grammatical_prefixes=True
    - Grammatical suffixes: "שלום#" -> grammatical_suffixes=True
    - Both prefix+suffix: "#שלום#" -> grammatical_prefixes=True, grammatical_suffixes=True
    - Plene/defective: "%שלום" -> plene_defective=True
    - Combined: "%#שלום#" -> plene_defective + prefixes + suffixes
    - OR groups: "(עץ/אילן)" -> words=['עץ', 'אילן']
    - Hash + OR: "#(שלום/שלומות)" -> OR group with grammatical_prefixes=True
    - Inline alternation: "אירו(ס/ש)ין" -> inline_pattern set
    - Multiple components separated by whitespace

    Args:
        query_str: Raw query string from user input

    Returns:
        List of ResponsaComponent objects (empty list for empty/whitespace input)
    """
    if not query_str or not query_str.strip():
        return []

    # Tokenize: split by whitespace, but keep parenthesized groups with adjacent text
    # together. We use a regex that captures tokens respecting parentheses.
    # A token is either:
    #   - A sequence that may start with # and contain a parenthesized group
    #   - A plain word (possibly with * wildcards)
    tokens = _tokenize_responsa_query(query_str.strip())

    components = []
    for token in tokens:
        if not token:
            continue
        # Skip [N] gap tokens — they are handled by extract_per_pair_gaps()
        if _GAP_TOKEN_RE.match(token):
            continue
        components.append(_parse_single_token(token))

    return components


# Regex for matching [N] gap tokens like [3], [0], [15]
_GAP_TOKEN_RE = re.compile(r'^\[(\d+)\]$')

# Regex for matching [|N] line-gap tokens like [|2], [|0]
_LINE_GAP_TOKEN_RE = re.compile(r'^\[\|(\d+)\]$')


def _has_line_break_syntax(query_str: str) -> bool:
    """Check if a query string contains line-break syntax (| characters).

    Returns True if any token starts/ends with | or is a standalone |,
    or contains [|N] line-gap notation.
    """
    if not query_str:
        return False
    tokens = _tokenize_responsa_query(query_str.strip())
    for token in tokens:
        if not token:
            continue
        if _LINE_GAP_TOKEN_RE.match(token):
            return True
        if token == '|':
            return True
        # Check for |word or word| (but not inside parentheses)
        stripped = token
        # Strip leading Responsa modifiers to find the |
        while stripped and stripped[0] in ('%', '#', '-'):
            stripped = stripped[1:]
        if stripped.startswith('|'):
            return True
        # Strip trailing modifiers to find |
        stripped = token
        while stripped and stripped[-1] in ('#',):
            stripped = stripped[:-1]
        if stripped.endswith('|'):
            return True
    return False


@dataclass
class LineGroup:
    """A constraint on one line of text in a line-break search.

    Each group represents words that must appear on the same line,
    with optional positional constraints (start/end of line).
    """
    components: List['ResponsaComponent']
    line_start: bool = False   # First word must be at start of line
    line_end: bool = False     # Last word must be at end of line
    # Per-pair word gaps WITHIN the line: word_gaps[i] is the gap (from a [N]
    # token) between component i and component i+1; None/0 = adjacent. len ==
    # len(components) - 1 when populated (CR HIGH-6).
    word_gaps: List[Optional[int]] = field(default_factory=list)


def _parse_line_break_query(query_str: str):
    """Parse a Responsa query with line-break syntax into line groups.

    Returns (line_groups, line_gaps) where:
    - line_groups: List[LineGroup] — each group = one line constraint
    - line_gaps: List[Optional[int]] — gaps between consecutive groups
      (None = consecutive, int = skip N lines)

    Returns (None, None) if no line-break syntax detected.
    """
    if not query_str or not _has_line_break_syntax(query_str):
        return None, None

    tokens = _tokenize_responsa_query(query_str.strip())
    if not tokens:
        return None, None

    groups = []
    line_gaps = []
    current_tokens = []       # raw tokens for current group
    current_line_start = False
    current_line_end = False
    pending_line_gap = None   # gap value from [|N] token

    def _flush_group():
        """Finalize current group and append to groups list."""
        nonlocal current_tokens, current_line_start, current_line_end, pending_line_gap
        if not current_tokens:
            return
        # Parse tokens through Responsa pipeline. [N] word-gap tokens are captured
        # as the gap between the previous and next word (CR HIGH-6) — previously
        # they were dropped, so word gaps silently had no effect in line mode.
        comps = []
        word_gaps = []          # gap BEFORE each component after the first
        pending_word_gap = None
        for t in current_tokens:
            gm = _GAP_TOKEN_RE.match(t)
            if gm:
                pending_word_gap = int(gm.group(1))
                continue
            comp = _parse_single_token(t)
            if comps:
                word_gaps.append(pending_word_gap)
            comps.append(comp)
            pending_word_gap = None
        if comps:
            if groups:
                line_gaps.append(pending_line_gap)
            groups.append(LineGroup(
                components=comps,
                line_start=current_line_start,
                line_end=current_line_end,
                word_gaps=word_gaps,
            ))
        current_tokens = []
        current_line_start = False
        current_line_end = False
        pending_line_gap = None

    for token in tokens:
        if not token:
            continue

        # [|N] line gap
        lg_match = _LINE_GAP_TOKEN_RE.match(token)
        if lg_match:
            _flush_group()
            pending_line_gap = int(lg_match.group(1))
            continue

        # [N] word gap — pass through (ignored in line mode, but keep for component parsing)
        if _GAP_TOKEN_RE.match(token):
            current_tokens.append(token)
            continue

        # Standalone | — line break without position constraint
        if token == '|':
            _flush_group()
            continue

        # Check for leading | (after stripping Responsa modifiers)
        raw = token
        prefix_mods = ''
        while raw and raw[0] in ('%', '#', '-'):
            prefix_mods += raw[0]
            raw = raw[1:]

        has_leading_pipe = raw.startswith('|')
        if has_leading_pipe:
            raw = raw[1:]  # strip the |

        # Check for trailing | (after stripping trailing modifiers)
        suffix_mods = ''
        temp = raw
        while temp and temp[-1] in ('#',):
            suffix_mods = temp[-1] + suffix_mods
            temp = temp[:-1]
        has_trailing_pipe = temp.endswith('|')
        if has_trailing_pipe:
            temp = temp[:-1]  # strip the |
            raw = temp + suffix_mods

        # If leading |, this starts a new group
        if has_leading_pipe:
            _flush_group()
            current_line_start = True

        # Reconstruct the clean token (without |) for Responsa parsing
        clean_token = prefix_mods + raw
        if clean_token:
            current_tokens.append(clean_token)

        # If trailing |, mark end and flush (next token starts new group)
        if has_trailing_pipe:
            current_line_end = True
            _flush_group()

    # Flush remaining
    _flush_group()

    if not groups:
        return None, None

    # Fill remaining gaps
    while len(line_gaps) < len(groups) - 1:
        line_gaps.append(None)

    return groups, line_gaps


def extract_per_pair_gaps(query_str: str) -> List[Optional[int]]:
    """Extract per-pair gap values from [N] tokens in a Responsa query.

    Returns list of gap values (one per adjacent component pair).
    None means "use global gap value" for that pair.

    Examples:
        "word1 [3] word2"       -> [3]
        "word1 [3] word2 [5] word3" -> [3, 5]
        "word1 word2"           -> [None]
        "word1 [2] word2 word3" -> [2, None]
    """
    if not query_str or not query_str.strip():
        return []

    tokens = _tokenize_responsa_query(query_str.strip())
    gaps = []
    last_was_component = False
    component_count = 0

    for token in tokens:
        if not token:
            continue
        gap_match = _GAP_TOKEN_RE.match(token)
        if gap_match:
            gap_value = int(gap_match.group(1))
            if last_was_component and len(gaps) == component_count - 1:
                gaps.append(gap_value)
            last_was_component = False
        else:
            if last_was_component and len(gaps) < component_count:
                gaps.append(None)
            component_count += 1
            last_was_component = True

    # Fill remaining gaps with None
    while len(gaps) < component_count - 1:
        gaps.append(None)

    return gaps


def generate_tabular_syntax(components, distances, scope='word_range'):
    """Generate Responsa syntax string from tabular builder state.

    Args:
        components: List of dicts with 'words' key, where words is a list of
                   {'text': str, 'mods': dict} dicts. Mods keys: prefix, suffix,
                   wildcard_prefix, wildcard_suffix, plene, negation,
                   line_start, line_end (for lines scope)
        distances: List of ints (len = len(components) - 1), gap between adjacent pairs
        scope: 'word_range', 'within_document', or 'lines'

    Returns:
        Tuple of (syntax_str, negated_words) where negated_words is a list of
        words marked for exclusion. Negated words are ALSO embedded in the syntax
        as -word for display, but extracted separately for backward compatibility.
    """
    parts = []
    negated_words = []
    valid_component_index = 0

    for i, comp in enumerate(components):
        words_with_mods = []
        has_line_start = False
        has_line_end = False
        for word_info in comp.get('words', []):
            text = word_info.get('text', '').strip()
            if not text:
                continue
            mods = word_info.get('mods', {})

            # Track line position modifiers (lines scope)
            if mods.get('line_start'):
                has_line_start = True
            if mods.get('line_end'):
                has_line_end = True

            # Check negation -- embed as -word in syntax AND extract
            if mods.get('negation'):
                negated_words.append(text)
                words_with_mods.append(f'-{text}')
                continue

            decorated = text
            # Apply modifiers in order: plene, then prefix/suffix, then wildcards
            if mods.get('plene'):
                decorated = '%' + decorated
            if mods.get('prefix'):
                decorated = '#' + decorated
            if mods.get('suffix'):
                decorated = decorated + '#'
            if mods.get('wildcard_prefix'):
                decorated = '*' + decorated
            if mods.get('wildcard_suffix'):
                decorated = decorated + '*'

            words_with_mods.append(decorated)

        if not words_with_mods:
            continue  # Skip empty components

        if len(words_with_mods) > 1:
            part = f"({'/'.join(words_with_mods)})"
        else:
            part = words_with_mods[0]

        # Lines scope: wrap with | for position, add [|N] for gaps
        if scope == 'lines':
            if has_line_start:
                part = '|' + part
            if has_line_end:
                part = part + '|'
            # If neither start nor end, just a bare component (any position on line)

            if valid_component_index > 0:
                dist_idx = valid_component_index - 1
                dist = distances[dist_idx] if dist_idx < len(distances) else 0
                if dist > 0:
                    parts.append(f'[|{dist}]')
                else:
                    # Consecutive lines: insert | separator if previous part
                    # doesn't already end with | and current doesn't start with |
                    prev = parts[-1] if parts else ''
                    if not prev.endswith('|') and not part.startswith('|'):
                        parts.append('|')
        elif scope == 'word_range':
            # Add distance notation between components (only for word_range scope)
            if valid_component_index > 0:
                dist_idx = valid_component_index - 1
                dist = distances[dist_idx] if dist_idx < len(distances) else 0
                if dist > 0:
                    parts.append(f'[{dist}]')

        parts.append(part)
        valid_component_index += 1

    return ' '.join(parts), negated_words


def _tokenize_responsa_query(query: str) -> List[str]:
    """Split a Responsa query into tokens, respecting parentheses as grouping.

    Splits on whitespace but treats anything inside parentheses (and adjacent
    characters) as part of the same token.
    """
    tokens = []
    current = []
    paren_depth = 0

    for ch in query:
        if ch == '(':
            paren_depth += 1
            current.append(ch)
        elif ch == ')':
            paren_depth = max(0, paren_depth - 1)
            current.append(ch)
        elif ch in (' ', '\t', '\n', '\r') and paren_depth == 0:
            if current:
                tokens.append(''.join(current))
                current = []
        else:
            current.append(ch)

    if current:
        tokens.append(''.join(current))

    return tokens


def _parse_single_token(token: str) -> ResponsaComponent:
    """Parse a single Responsa query token into a ResponsaComponent.

    Handles: # (prefix/suffix), %, *, (a/b) OR groups, inline (a/b) alternations.

    Operator positions:
    - Leading %: plene/defective spelling variants
    - Leading #: grammatical prefix expansion
    - Trailing #: grammatical suffix expansion
    - Leading *: prefix wildcard (any chars before)
    - Trailing *: suffix wildcard (any chars after)
    - Combinable: %#word#, #word*, etc.
    """
    original = token

    # Check for leading - (negation)
    has_negation = False
    if token.startswith('-') and len(token) > 1:
        has_negation = True
        token = token[1:]

    # Strip leading operators (% and #) in any order
    # Supports: %#word, #%word, %word, #word
    has_percent = False
    has_leading_hash = False
    while token and token[0] in ('%', '#'):
        if token[0] == '%':
            has_percent = True
        elif token[0] == '#':
            has_leading_hash = True
        token = token[1:]

    # Check for trailing # (suffix expansion)
    # But only if token doesn't end with * (which takes precedence for trailing position)
    has_trailing_hash = False
    if token.endswith('#') and not token.endswith('*'):
        has_trailing_hash = True
        token = token[:-1]

    # Check for OR group: token is entirely "(word1/word2/...)" possibly with trailing *
    if token.startswith('(') and ')' in token:
        close_idx = token.rindex(')')
        inner = token[1:close_idx]
        after = token[close_idx + 1:]

        if '/' in inner:
            words = [w.strip() for w in inner.split('/') if w.strip()]

            wildcard = None
            if after == '*':
                wildcard = 'suffix'
            # Check for trailing # after OR group close
            trailing_hash_or = has_trailing_hash
            if after == '#':
                trailing_hash_or = True

            return ResponsaComponent(
                words=words,
                grammatical_prefixes=has_leading_hash,
                grammatical_suffixes=trailing_hash_or,
                plene_defective=has_percent,
                wildcard=wildcard,
                negated=has_negation,
            )

    # Check for inline alternation: text(a/b)text  (parentheses not at start)
    if '(' in token and ')' in token and not token.startswith('('):
        paren_open = token.index('(')
        paren_close = token.index(')')
        inner = token[paren_open + 1:paren_close]
        if '/' in inner:
            return ResponsaComponent(
                words=[token],
                grammatical_prefixes=has_leading_hash,
                grammatical_suffixes=has_trailing_hash,
                plene_defective=has_percent,
                inline_pattern=token,
                negated=has_negation,
            )

    # Wildcard detection
    wildcard = None
    wildcard_pattern = None
    stripped = token

    if '*' in token:
        asterisk_count = token.count('*')

        if asterisk_count >= 3:
            wildcard = 'pattern'
            wildcard_pattern = token
            return ResponsaComponent(
                words=[token],
                grammatical_prefixes=has_leading_hash,
                grammatical_suffixes=has_trailing_hash,
                plene_defective=has_percent,
                wildcard=wildcard,
                wildcard_pattern=wildcard_pattern,
                negated=has_negation,
            )
        elif token.endswith('*') and not token.startswith('*'):
            wildcard = 'suffix'
            stripped = token.rstrip('*')
        elif token.startswith('*') and not token.endswith('*'):
            wildcard = 'prefix'
            stripped = token.lstrip('*')
        elif token.startswith('*') and token.endswith('*') and asterisk_count == 2:
            wildcard = 'pattern'
            wildcard_pattern = token
            return ResponsaComponent(
                words=[token],
                grammatical_prefixes=has_leading_hash,
                grammatical_suffixes=has_trailing_hash,
                plene_defective=has_percent,
                wildcard=wildcard,
                wildcard_pattern=wildcard_pattern,
                negated=has_negation,
            )

    return ResponsaComponent(
        words=[stripped],
        grammatical_prefixes=has_leading_hash,
        grammatical_suffixes=has_trailing_hash,
        plene_defective=has_percent,
        wildcard=wildcard,
        negated=has_negation,
    )


def expand_grammatical_prefixes(word: str) -> List[str]:
    """Expand a Hebrew word with all grammatical prefix combinations.

    Uses the GRAMMATICAL_PREFIXES constant to generate ~25 forms by
    prepending each prefix to the given word.

    Args:
        word: Base Hebrew word (without any prefix)

    Returns:
        List of unique prefixed forms (including the bare word)
    """
    seen = set()
    result = []
    for prefix in GRAMMATICAL_PREFIXES:
        form = prefix + word
        if form not in seen:
            seen.add(form)
            result.append(form)
    return result


def expand_judeo_arabic(word: str) -> List[str]:
    """Expand a Judeo-Arabic word with definite article and preposition forms.

    Simplified model: the definite article is ALWAYS 'אל' regardless of the
    first letter (no sun letter assimilation). Every word gets exactly 8 forms:
      1. base word
      2. אל + word (definite article)
      3. ואל + word (wa + definite article)
      4. באל + word (bi + definite article)
      5. פאל + word (fa + definite article)
      6. כאל + word (ka + definite article)
      7. לאל + word (la + definite article)
      8. לל + word (lil- contraction: li + al-)

    Args:
        word: Base Judeo-Arabic word in Hebrew script

    Returns:
        List of 8 unique forms
    """
    forms = [
        word,                 # bare word
        'אל' + word,         # al- (definite article)
        'ואל' + word,        # wa-al-
        'באל' + word,        # bi-al-
        'פאל' + word,        # fa-al-
        'כאל' + word,        # ka-al-
        'לאל' + word,        # la-al-
        'לל' + word,         # lil- (li + al- contraction)
    ]

    # Deduplicate while preserving order (e.g., if word starts with ל,
    # 'אל' + 'לword' = 'אללword' which is unique, but edge cases may occur)
    seen = set()
    result = []
    for form in forms:
        if form not in seen:
            seen.add(form)
            result.append(form)
    return result


# --------------------------------------------------------------------------
# Search normalization: diacritics stripping and mark-tolerant patterns
# --------------------------------------------------------------------------
# COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics moved to shared/text_normalize.py
# (Phase 123). Re-exported above via shim.


# Hebrew final-letter to regular-letter mapping (sofit → normal)
_SOFIT_TO_NORMAL = {
    'ם': 'מ',
    'ן': 'נ',
    'ץ': 'צ',
    'ף': 'פ',
    'ך': 'כ',
}


def expand_grammatical_suffixes(word: str) -> List[str]:
    """Expand a Hebrew word with all grammatical suffix combinations.

    Uses the GRAMMATICAL_SUFFIXES constant to generate ~25 forms by
    appending each suffix to the given word. When a suffix is added,
    final letters (sofit) at the end of the base word are converted
    to their regular forms (e.g., שלום# → שלומה, שלומות, not שלוםה).

    Args:
        word: Base Hebrew word (without any suffix)

    Returns:
        List of unique suffixed forms (including the bare word)
    """
    seen = set()
    result = []
    for suffix in GRAMMATICAL_SUFFIXES:
        if suffix and word and word[-1] in _SOFIT_TO_NORMAL:
            # Convert final letter to regular form before appending suffix
            form = word[:-1] + _SOFIT_TO_NORMAL[word[-1]] + suffix
        else:
            form = word + suffix
        if form not in seen:
            seen.add(form)
            result.append(form)
    return result


def expand_plene_defective(word: str) -> List[str]:
    """Generate plene/defective spelling variants of a Hebrew word.

    Both directions:
    - Removal (plene→defective): remove each interior ו or י one at a time
    - Addition (defective→plene): insert ו or י after each consonant position

    Interior = not the first or last character of the word.
    Only single-letter changes are made per variant (not combinatorial).

    Args:
        word: Hebrew word to generate spelling variants for

    Returns:
        List of unique spelling variants (always includes the original word)
    """
    if len(word) < 2:
        return [word]

    seen = {word}
    result = [word]

    matres = {'ו', 'י'}

    # Removal: remove each interior ו/י one at a time
    for i in range(1, len(word) - 1):
        if word[i] in matres:
            variant = word[:i] + word[i + 1:]
            if variant not in seen:
                seen.add(variant)
                result.append(variant)

    # Addition: insert ו or י after each consonant in interior positions
    # We insert between positions 1..len-1 (after first char, before last char)
    for i in range(1, len(word)):
        # Only insert after a consonant (not after an existing mater lectionis)
        # and not immediately before an existing mater lectionis (avoids וו, יי, וי, יו)
        if word[i - 1] not in matres and word[i] not in matres:
            for m in matres:
                variant = word[:i] + m + word[i:]
                if variant not in seen:
                    seen.add(variant)
                    result.append(variant)

    return result


def _count_expanded_terms(components: List[ResponsaComponent],
                          variants_on: bool, ja_on: bool,
                          var_mgr, variant_mode: str) -> int:
    """Estimate the total number of expanded terms for a set of Responsa components.

    This counts without fully materializing the expansion, used by the explosion
    guard to check limits before committing to a particular expansion level.

    Args:
        components: List of ResponsaComponent objects
        variants_on: Whether spelling variants are enabled
        ja_on: Whether Judeo-Arabic expansion is enabled
        var_mgr: VariantManager instance (or mock)
        variant_mode: Variant mode string ('variants', 'variants_extended', 'variants_maximum')

    Returns:
        Estimated total number of search terms that would be generated
    """
    total = 0

    for comp in components:
        # Start with the base words count
        base_words = comp.words

        # If grammatical prefixes are on, each word becomes ~25 forms
        if comp.grammatical_prefixes:
            word_count = len(base_words) * len(GRAMMATICAL_PREFIXES)
        else:
            word_count = len(base_words)

        # If grammatical suffixes are on, each form gets ~25 suffix forms
        if comp.grammatical_suffixes:
            word_count *= len(GRAMMATICAL_SUFFIXES)

        # If plene/defective is on, estimate ~5 variants per form
        if comp.plene_defective:
            word_count *= 5

        # If JA expansion is on, each form becomes 8 JA forms
        if ja_on:
            word_count *= 8

        # If variants are on, each form gets variant expansions
        if variants_on and var_mgr is not None:
            # Estimate variant count from the first word as representative
            sample_word = base_words[0] if base_words else "sample"
            try:
                sample_variants = var_mgr.get_variants(sample_word, variant_mode)
                variant_multiplier = max(1, len(sample_variants))
            except Exception:
                variant_multiplier = 1  # Variant expansion failed; treat as single variant
            word_count *= variant_multiplier

        total += word_count

    return total


def _apply_explosion_guard(
    components: List[ResponsaComponent],
    variants_on: bool,
    ja_on: bool,
    var_mgr,
    variant_mode: str,
) -> tuple:
    """Apply the explosion guard to prevent combinatorial blowup in Responsa queries.

    Checks the estimated total expanded terms against MAX_EXPANDED_TERMS (500)
    and progressively downgrades options until the query fits:

    Cascade order:
      1. Downgrade variant mode to 'variants' (basic, 30 pairs)
      2. Disable variants entirely
      3. Disable Judeo-Arabic expansion
      4. Disable plene/defective on all components
      5. Disable grammatical suffixes on all components
      6. Disable grammatical prefixes on all components
      7. If still over limit, raise ValueError

    Args:
        components: List of ResponsaComponent objects from parse_responsa_query
        variants_on: Whether spelling variants are enabled
        ja_on: Whether Judeo-Arabic expansion is enabled
        var_mgr: VariantManager instance (or mock) for counting variant expansions
        variant_mode: Current variant mode string

    Returns:
        Tuple of (components, warning_message, actual_options_dict)
        - components: The components (may be modified in-place if cascade steps 4-6
          disabled plene_defective, grammatical_suffixes, or grammatical_prefixes)
        - warning_message: str describing what was downgraded, or None if no changes
        - actual_options: dict with keys 'variants_on', 'ja_on', 'variant_mode'
          reflecting the actual options after any downgrades

    Raises:
        ValueError: If the query exceeds MAX_EXPANDED_TERMS even after all downgrades
    """
    limit = Config.MAX_EXPANDED_TERMS
    warnings = []

    current_variants_on = variants_on
    current_ja_on = ja_on
    current_variant_mode = variant_mode

    # Check initial count
    count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)

    if count <= limit:
        return components, None, {
            'variants_on': current_variants_on,
            'ja_on': current_ja_on,
            'variant_mode': current_variant_mode,
        }

    # Cascade 1: Downgrade variant mode to basic ('variants')
    if current_variants_on and current_variant_mode != 'variants':
        current_variant_mode = 'variants'
        count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)
        warnings.append(_tr("Variant mode downgraded to basic (30 pairs)"))
        if count <= limit:
            return components, '; '.join(warnings), {
                'variants_on': current_variants_on,
                'ja_on': current_ja_on,
                'variant_mode': current_variant_mode,
            }

    # Cascade 2: Disable variants entirely
    if current_variants_on:
        current_variants_on = False
        count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)
        warnings.append(_tr("Spelling variants disabled"))
        if count <= limit:
            return components, '; '.join(warnings), {
                'variants_on': current_variants_on,
                'ja_on': current_ja_on,
                'variant_mode': current_variant_mode,
            }

    # Cascade 3: Disable Judeo-Arabic expansion
    if current_ja_on:
        current_ja_on = False
        count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)
        warnings.append(_tr("Judeo-Arabic expansion disabled"))
        if count <= limit:
            return components, '; '.join(warnings), {
                'variants_on': current_variants_on,
                'ja_on': current_ja_on,
                'variant_mode': current_variant_mode,
            }

    # Cascade 4: Disable plene/defective on all components
    any_plene = any(c.plene_defective for c in components)
    if any_plene:
        for c in components:
            c.plene_defective = False
        count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)
        warnings.append(_tr("Plene/defective expansion disabled"))
        if count <= limit:
            return components, '; '.join(warnings), {
                'variants_on': current_variants_on,
                'ja_on': current_ja_on,
                'variant_mode': current_variant_mode,
            }

    # Cascade 5: Disable grammatical suffixes on all components
    any_suffixes = any(c.grammatical_suffixes for c in components)
    if any_suffixes:
        for c in components:
            c.grammatical_suffixes = False
        count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)
        warnings.append(_tr("Grammatical suffix expansion disabled"))
        if count <= limit:
            return components, '; '.join(warnings), {
                'variants_on': current_variants_on,
                'ja_on': current_ja_on,
                'variant_mode': current_variant_mode,
            }

    # Cascade 6: Disable grammatical prefixes on all components
    any_prefixes = any(c.grammatical_prefixes for c in components)
    if any_prefixes:
        for c in components:
            c.grammatical_prefixes = False
        count = _count_expanded_terms(components, current_variants_on, current_ja_on, var_mgr, current_variant_mode)
        warnings.append(_tr("Grammatical prefix expansion disabled"))
        if count <= limit:
            return components, '; '.join(warnings), {
                'variants_on': current_variants_on,
                'ja_on': current_ja_on,
                'variant_mode': current_variant_mode,
            }

    # Cascade 7: Still over limit -- nothing left to downgrade
    raise ValueError(
        _tr("Query exceeds the limit of {limit} expanded terms (estimated {count} terms). "
           "Please simplify your query by using fewer OR-group alternatives or "
           "removing the # (grammatical prefixes) modifier from some terms.").format(limit=limit, count=count)
    )


def _expand_inline_alternation(pattern_str: str) -> str:
    """Expand an inline alternation pattern into a regex.

    For inline alternation like "word(a/b)end":
    - If all alternatives are single chars, use character class: word[ab]end
    - If any alternative is multi-char, use alternation: word(a|bc)end

    Returns: regex pattern string
    """
    if not pattern_str or '(' not in pattern_str:
        return re.escape(pattern_str) if pattern_str else ''

    result = []
    i = 0
    while i < len(pattern_str):
        if pattern_str[i] == '(':
            # Find the matching closing paren
            close = pattern_str.find(')', i)
            if close == -1:
                result.append(re.escape(pattern_str[i]))
                i += 1
                continue
            inner = pattern_str[i + 1:close]
            alternatives = inner.split('/')
            if all(len(alt) <= 1 for alt in alternatives):
                # Single char alternatives -> character class
                escaped_alts = [re.escape(a) for a in alternatives if a]
                result.append(f"[{''.join(escaped_alts)}]")
            else:
                # Multi-char -> alternation group
                escaped_alts = [re.escape(a) for a in alternatives]
                result.append(f"({'|'.join(escaped_alts)})")
            i = close + 1
        else:
            result.append(re.escape(pattern_str[i]))
            i += 1

    return ''.join(result)

