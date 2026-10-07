"""Variant and Lab preferences of one website visitor.

The web process holds one LabSettings object (and one VariantManager) for every
visitor. The website therefore never writes a visitor's choices to it or to its
file: each visitor's choices are kept in that visitor's own storage and sent with
that visitor's searches (``web.research_jobs.with_request_settings``), and every
web search job starts from ``WEBSITE_DEFAULTS``, never from the server's file.

``WEBSITE_DEFAULTS`` are what a visitor who has chosen nothing gets, and what the
public API always uses, so an API search and a website search with default
settings give the same results.

No NiceGUI import at module level: ``web.research_jobs`` (which must not import
NiceGUI) reads the defaults and validators from here.
"""
from __future__ import annotations

_DIAERESIS, _DOT_ABOVE = chr(0x0308), chr(0x0307)

# Letters commonly confused in the transcriptions, and dotted letters read as plain.
WEBSITE_CUSTOM_VARIANTS = {
    **{pair: True for pair in ('ב=כ', 'ה=ח', 'ד=ר', 'ס=ם')},
    f'ה{_DIAERESIS}=ה': True,
    **{f'{letter}{_DOT_ABOVE}={letter}': True for letter in 'תדטכצץ'},
}

# Pairs each variant level uses (the preset buttons and the ?, ?? and ??? prefixes).
PRESET_PAIRS = {'variants': 30, 'variants_extended': 70, 'variants_maximum': 150}
BASIC_PAIRS = PRESET_PAIRS['variants']

WEBSITE_DEFAULTS = {
    'variant_pairs_count': BASIC_PAIRS,
    'variant_max_changes': 1,          # Basic's x1 (each level keeps its own: max_changes)
    'variant_min_word_len': 2,
    'variant_aggressive': False,
    'custom_variants': WEBSITE_CUSTOM_VARIANTS,
    'comp_min_score': 70,
}


def _bounded_int(low, high):
    def check(value):
        return max(low, min(high, int(value)))
    return check


def _custom_pairs(value):
    """At most 50 'a=b' pairs of one to three characters a side."""
    if not isinstance(value, dict):
        raise ValueError('custom pairs must be a dict')
    kept = {}
    for key in list(value)[:50]:
        left, sep, right = str(key).partition('=')
        left, right = left.strip(), right.strip()
        if sep and 0 < len(left) <= 3 and 0 < len(right) <= 3:
            kept[f'{left}={right}'] = True
    return kept


# The settings one web search may choose for itself, each with its check.
REQUEST_SETTINGS = {
    'variant_pairs_count': _bounded_int(10, 300),
    'variant_max_changes': _bounded_int(1, 3),
    'variant_min_word_len': _bounded_int(1, 5),
    'variant_aggressive': bool,
    'custom_variants': _custom_pairs,
    'comp_min_score': _bounded_int(10, 100),
}


def request_settings(**values) -> dict:
    """Checked copies of per-search settings. An unknown name is a programming
    error; a value that cannot be read falls back to the website default."""
    checked = {}
    for name, value in values.items():
        if name not in REQUEST_SETTINGS:
            raise ValueError(f'{name} cannot be set per request')
        if value is None:
            continue
        try:
            checked[name] = REQUEST_SETTINGS[name](value)
        except (TypeError, ValueError):
            checked[name] = REQUEST_SETTINGS[name](WEBSITE_DEFAULTS[name])
    return checked


def website_defaults() -> dict:
    """A fresh copy of the website defaults (safe to change)."""
    return request_settings(**WEBSITE_DEFAULTS)


# What a visitor sets on the Settings page, kept per visitor. The variant level
# and Num Changes are kept by the search bar ('search_preset', 'search_max_changes').
_PREFERENCES = {
    'variant_min_word_len': WEBSITE_DEFAULTS['variant_min_word_len'],
    'variant_aggressive': WEBSITE_DEFAULTS['variant_aggressive'],
    'custom_variants': WEBSITE_CUSTOM_VARIANTS,
    'comp_min_score': WEBSITE_DEFAULTS['comp_min_score'],
    'variant_use_slider': False,   # display only; not sent with searches
}
_SENT = ('variant_min_word_len', 'variant_aggressive', 'custom_variants', 'comp_min_score')


def _key(name):
    return f'variant_pref_{name}'


def get(name):
    """This visitor's value (the website default if never set or unreadable)."""
    from web.safe_storage import safe_user_get
    default = _PREFERENCES[name]
    value = safe_user_get(_key(name), None)
    if value is None:
        return dict(default) if isinstance(default, dict) else default
    if name == 'variant_use_slider':
        return bool(value)
    return request_settings(**{name: value})[name]


def set(name, value):  # noqa: A001 - module-level accessor pair
    """Keep this visitor's value (checked first). Returns True when stored."""
    from web.safe_storage import safe_user_set
    if name not in _PREFERENCES:
        raise KeyError(name)
    if name != 'variant_use_slider':
        value = request_settings(**{name: value})[name]
    return safe_user_set(_key(name), bool(value) if name == 'variant_use_slider' else value)


def current() -> dict:
    """This visitor's Settings-page preferences, ready to send with a search."""
    return {name: get(name) for name in _SENT}


def level_of(mode_or_pairs) -> str:
    """'basic', 'extended' or 'maximum' for a variant mode or a pair count."""
    from shared.variants import variant_preset_of
    return variant_preset_of(PRESET_PAIRS.get(mode_or_pairs, mode_or_pairs))


def max_changes_table() -> dict:
    """This visitor's x1-x3 per level (Num Changes). The single value saved before
    each level had its own seeds Extended and Maximum only; Basic starts at x1."""
    from shared.variants import max_changes_by_preset
    from web.safe_storage import safe_user_get
    stored = safe_user_get('search_max_changes_by_level', None)
    if isinstance(stored, dict):
        return max_changes_by_preset(stored)
    return max_changes_by_preset(legacy=safe_user_get('search_max_changes', None))


def max_changes(level: str = 'basic') -> int:
    """This visitor's x1-x3 for one level."""
    return max_changes_table()[level]


def set_max_changes(level: str, value) -> bool:
    """Keep this visitor's x1-x3 for one level."""
    from web.safe_storage import safe_user_set
    table = max_changes_table()
    table[level] = request_settings(variant_max_changes=value)['variant_max_changes']
    return safe_user_set('search_max_changes_by_level', table)


def for_search(mode: str, pairs_count: int | None = None, changes: int | None = None) -> dict:
    """The complete settings one of this visitor's searches runs with.

    Variant modes use *pairs_count* (the slider or the level's preset) and the
    level's x1-x3 (or *changes*). Fuzzy uses x2. Every other mode -- Responsa,
    composition, Joins -- uses the Basic level and Basic's x1-x3, with the visitor's
    Settings-page preferences (owner ruling 2026-09-28).
    """
    settings = {**website_defaults(), **current()}
    if mode in PRESET_PAIRS or mode == 'fuzzy':
        pairs = pairs_count or PRESET_PAIRS.get(mode, BASIC_PAIRS)
        settings['variant_pairs_count'] = pairs
        if mode == 'fuzzy':
            settings['variant_max_changes'] = changes or 2
        else:
            settings['variant_max_changes'] = changes or max_changes(level_of(pairs))
    else:
        settings['variant_max_changes'] = max_changes('basic')
    return request_settings(**settings)
