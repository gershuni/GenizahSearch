# -*- coding: utf-8 -*-
"""
Translation Quality Control heuristics for GenizahSearch.

Provides lightweight automatic checks to flag suspicious machine translations
before or after insertion into sidecar databases. Each translation receives
a numeric qc_score (0.0 = worst, 1.0 = best) and a list of qc_flags
describing detected issues.

These heuristics are intentionally conservative — they flag for human review,
never auto-reject. False positives are acceptable; false negatives are costly.
"""

import re
import unicodedata
from typing import Dict, List, Optional, Tuple

# =============================================================================
# Constants
# =============================================================================

# Hebrew Unicode block range
_HE_RANGE = re.compile(r'[\u0590-\u05FF\uFB1D-\uFB4F]')
# Latin letter range
_LATIN_RANGE = re.compile(r'[A-Za-z\u00C0-\u024F]')
# Digits
_DIGIT_RE = re.compile(r'\d+')
# Parenthesized groups
_PAREN_RE = re.compile(r'[()]')
# Bracket groups
_BRACKET_RE = re.compile(r'[\[\]]')
# Question marks (sometimes added by hallucinating models)
_QUESTION_RE = re.compile(r'\?')
# Ellipsis or truncation markers
_TRUNCATION_RE = re.compile(r'\.{3,}|…')
# Repeated punctuation
_REPEATED_PUNCT_RE = re.compile(r'([!?,;:])\1{2,}')

# Length ratio thresholds (source/target)
# Hebrew is typically more compact than English
LENGTH_RATIO_MIN_HE2EN = 0.3   # Hebrew source much shorter than English target is OK
LENGTH_RATIO_MAX_HE2EN = 5.0   # But not 5x longer
LENGTH_RATIO_MIN_EN2HE = 0.2   # English → Hebrew can compress
LENGTH_RATIO_MAX_EN2HE = 4.0

# Minimum source length to apply ratio checks (very short texts are noisy)
MIN_LENGTH_FOR_RATIO = 15


# =============================================================================
# Helper Functions
# =============================================================================

def _count_script(text: str, pattern: re.Pattern) -> int:
    """Count characters matching a script pattern."""
    return len(pattern.findall(text))


def _hebrew_fraction(text: str) -> float:
    """Fraction of alphabetic characters that are Hebrew."""
    he = _count_script(text, _HE_RANGE)
    la = _count_script(text, _LATIN_RANGE)
    total = he + la
    if total == 0:
        return 0.0
    return he / total


def _count_numbers(text: str) -> List[str]:
    """Extract all digit sequences."""
    return _DIGIT_RE.findall(text)


def _count_char(text: str, pattern: re.Pattern) -> int:
    """Count occurrences of a character pattern."""
    return len(pattern.findall(text))


# =============================================================================
# Individual Check Functions
# =============================================================================

def check_length_ratio(
    source: str, target: str, direction: str
) -> Optional[str]:
    """
    Flag if target length is suspiciously different from source length.
    Returns a flag string or None.
    """
    if len(source) < MIN_LENGTH_FOR_RATIO or len(target) < 3:
        return None

    ratio = len(target) / len(source)

    if direction in ('he2en', 'HE2EN'):
        if ratio > LENGTH_RATIO_MAX_HE2EN:
            return f'length_ratio_high:{ratio:.1f}'
        if ratio < LENGTH_RATIO_MIN_HE2EN:
            return f'length_ratio_low:{ratio:.1f}'
    else:  # en2he
        if ratio > LENGTH_RATIO_MAX_EN2HE:
            return f'length_ratio_high:{ratio:.1f}'
        if ratio < LENGTH_RATIO_MIN_EN2HE:
            return f'length_ratio_low:{ratio:.1f}'
    return None


def check_copied_source(source: str, target: str) -> Optional[str]:
    """
    Flag if the target is essentially a copy of the source (not translated).
    """
    if len(source) < 5:
        return None
    # Normalize whitespace for comparison
    s_norm = ' '.join(source.split()).strip()
    t_norm = ' '.join(target.split()).strip()
    if s_norm == t_norm:
        return 'copied_source'
    # Check high overlap (>90% character match)
    if len(s_norm) > 20:
        common = sum(1 for a, b in zip(s_norm, t_norm) if a == b)
        overlap = common / max(len(s_norm), len(t_norm))
        if overlap > 0.90:
            return f'near_copy:{overlap:.0%}'
    return None


def check_script_mismatch(
    source: str, target: str, direction: str
) -> Optional[str]:
    """
    Flag if the target text has the wrong script balance.
    HE→EN target should be mostly Latin; EN→HE target should be mostly Hebrew.
    """
    if len(target) < 5:
        return None

    target_he_frac = _hebrew_fraction(target)

    if direction in ('he2en', 'HE2EN'):
        # Target should be mostly English/Latin
        if target_he_frac > 0.6:
            return f'script_mismatch:target_mostly_hebrew:{target_he_frac:.0%}'
    else:  # en2he
        # Target should be mostly Hebrew
        if target_he_frac < 0.3:
            return f'script_mismatch:target_mostly_latin:{1-target_he_frac:.0%}'
    return None


def check_number_drift(source: str, target: str) -> Optional[str]:
    """
    Flag if numbers in source and target differ significantly.
    Dates, page numbers, and quantities should be preserved.
    """
    src_nums = set(_count_numbers(source))
    tgt_nums = set(_count_numbers(target))

    if not src_nums and not tgt_nums:
        return None

    # Numbers added in target that weren't in source
    added = tgt_nums - src_nums
    # Numbers removed from source
    removed = src_nums - tgt_nums

    # Filter out trivially short numbers (1-2 digits) which may be ordinals etc.
    significant_added = {n for n in added if len(n) >= 3}
    significant_removed = {n for n in removed if len(n) >= 3}

    if significant_added:
        return f'numbers_added:{",".join(sorted(significant_added)[:3])}'
    if significant_removed and len(significant_removed) > len(src_nums) * 0.5:
        return f'numbers_removed:{",".join(sorted(significant_removed)[:3])}'
    return None


def check_bracket_mismatch(source: str, target: str) -> Optional[str]:
    """
    Flag if brackets/parentheses balance differs between source and target.
    Scholarly text uses brackets carefully — they shouldn't be dropped or added.
    """
    src_parens = _count_char(source, _PAREN_RE)
    tgt_parens = _count_char(target, _PAREN_RE)
    src_brackets = _count_char(source, _BRACKET_RE)
    tgt_brackets = _count_char(target, _BRACKET_RE)

    issues = []
    if src_parens > 0 and tgt_parens == 0:
        issues.append('parens_dropped')
    if src_parens == 0 and tgt_parens > 2:
        issues.append('parens_added')
    if src_brackets > 0 and tgt_brackets == 0 and src_brackets >= 2:
        issues.append('brackets_dropped')
    if src_brackets == 0 and tgt_brackets > 2:
        issues.append('brackets_added')

    return '|'.join(issues) if issues else None


def check_added_questions(source: str, target: str) -> Optional[str]:
    """
    Flag if the target adds question marks not present in the source.
    This often indicates the model is expressing uncertainty by hallucinating.
    """
    src_q = _count_char(source, _QUESTION_RE)
    tgt_q = _count_char(target, _QUESTION_RE)
    if tgt_q > src_q + 1:
        return f'questions_added:{tgt_q - src_q}'
    return None


def check_truncation(target: str) -> Optional[str]:
    """Flag if the target appears to be truncated."""
    if _TRUNCATION_RE.search(target[-10:]) if len(target) > 10 else _TRUNCATION_RE.search(target):
        return 'possible_truncation'
    return None


def check_repeated_punctuation(target: str) -> Optional[str]:
    """Flag repeated punctuation in target (model stuttering)."""
    if _REPEATED_PUNCT_RE.search(target):
        return 'repeated_punctuation'
    return None


def check_empty_translation(target: str) -> Optional[str]:
    """Flag if target is empty or only whitespace/punctuation."""
    stripped = target.strip()
    if not stripped:
        return 'empty_translation'
    # Only punctuation/digits, no actual words
    alpha = re.sub(r'[\s\d\W]', '', stripped)
    if len(stripped) > 3 and len(alpha) < 2:
        return 'no_words_in_translation'
    return None


def check_too_short(source: str, target: str) -> Optional[str]:
    """
    Flag if source is substantial but target is suspiciously short.
    Different from length_ratio — this catches cases where a paragraph
    becomes a single word.
    """
    if len(source) > 50 and len(target) < 10:
        return f'too_short:{len(target)}chars_from_{len(source)}'
    return None


# =============================================================================
# Main QC Function
# =============================================================================

# All checks in order of severity
_ALL_CHECKS = [
    check_empty_translation,     # target only
    check_copied_source,         # source + target
    check_script_mismatch,       # source + target + direction
    check_length_ratio,          # source + target + direction
    check_too_short,             # source + target
    check_number_drift,          # source + target
    check_bracket_mismatch,      # source + target
    check_added_questions,       # source + target
    check_truncation,            # target only
    check_repeated_punctuation,  # target only
]


def run_qc(
    source_text: str,
    translated_text: str,
    direction: str = 'he2en'
) -> Dict:
    """
    Run all QC heuristics on a single translation pair.

    Args:
        source_text: Original text
        translated_text: Machine-translated text
        direction: 'he2en' or 'en2he'

    Returns:
        Dict with:
            - qc_score: float 0.0 (worst) to 1.0 (best)
            - qc_flags: list of string flags
            - flag_count: int number of flags raised
    """
    if not source_text or not translated_text:
        return {
            'qc_score': 0.0,
            'qc_flags': ['missing_text'],
            'flag_count': 1,
        }

    flags = []

    for check_fn in _ALL_CHECKS:
        try:
            # Determine which arguments the check needs
            fn_name = check_fn.__name__
            if fn_name in ('check_truncation', 'check_repeated_punctuation'):
                result = check_fn(translated_text)
            elif fn_name == 'check_empty_translation':
                result = check_fn(translated_text)
            elif fn_name in ('check_script_mismatch', 'check_length_ratio'):
                result = check_fn(source_text, translated_text, direction)
            else:
                result = check_fn(source_text, translated_text)

            if result:
                flags.append(result)
        except Exception:
            pass  # Individual check failure should never block others

    # Calculate score: start at 1.0, deduct per flag
    # Severe flags deduct more
    score = 1.0
    for flag in flags:
        if flag in ('empty_translation', 'copied_source', 'missing_text'):
            score -= 0.5
        elif flag.startswith(('script_mismatch', 'no_words')):
            score -= 0.4
        elif flag.startswith(('length_ratio', 'near_copy', 'too_short')):
            score -= 0.3
        elif flag.startswith('numbers_added'):
            score -= 0.2
        else:
            score -= 0.1

    score = max(0.0, min(1.0, score))

    return {
        'qc_score': round(score, 2),
        'qc_flags': flags,
        'flag_count': len(flags),
    }


def run_qc_batch(
    pairs: List[Tuple[str, str, str]],
) -> List[Dict]:
    """
    Run QC on a batch of (source, target, direction) tuples.

    Returns:
        List of QC result dicts in the same order as input.
    """
    return [run_qc(src, tgt, d) for src, tgt, d in pairs]


def summarize_qc_results(results: List[Dict]) -> Dict:
    """
    Produce a summary report from a batch of QC results.

    Returns:
        Dict with:
            - total: int
            - flagged: int (any flags)
            - clean: int (no flags)
            - mean_score: float
            - flag_distribution: dict of flag_name -> count
            - worst_scores: list of indices with score < 0.5
    """
    total = len(results)
    flagged = sum(1 for r in results if r['flag_count'] > 0)
    clean = total - flagged
    mean_score = sum(r['qc_score'] for r in results) / total if total else 0.0

    flag_dist = {}
    worst = []
    for i, r in enumerate(results):
        if r['qc_score'] < 0.5:
            worst.append(i)
        for flag in r['qc_flags']:
            # Normalize flag name (strip parameters after colon)
            base_flag = flag.split(':')[0]
            flag_dist[base_flag] = flag_dist.get(base_flag, 0) + 1

    return {
        'total': total,
        'flagged': flagged,
        'clean': clean,
        'flagged_pct': round(flagged / total * 100, 1) if total else 0,
        'mean_score': round(mean_score, 3),
        'flag_distribution': dict(sorted(flag_dist.items(), key=lambda x: -x[1])),
        'worst_count': len(worst),
    }
