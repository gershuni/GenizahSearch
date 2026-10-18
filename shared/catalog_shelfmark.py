"""Catalogue-form shelfmarks: the fallback behind ``MetadataManager.resolve_system_by_shelfmark``.

Plan: ``docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md`` (rev. 5). Users paste shelfmarks
as the NLI catalogue writes them -- ``Cambridge University Library, Ms. T-S K 08.026``,
``Mosseri, Jacques, Ms. VIII.402`` -- which today's resolver ("T1") cannot match: the
numbers are zero-padded, the institution is written out, and Mosseri units are stored
only as their parts. This module is the second pass ("T2") that runs **only when T1
found no exact match**, so no input that resolves exactly today changes.

T2 takes the union of these candidate sets (rev. 3; first-tier-wins hid same-library
records):

* ``D0`` -- T1's exact answer for the input with its padding zeros removed, restricted
  to the named institution;
* ``Da`` -- the rebuilt long form ``<head> Ms. <rest>`` (records that store that form);
* ``Db`` -- Mosseri heads only: our short form ``Moss. <rest>``;
* ``Dr`` -- the rest within the named institution (same-library records stored
  without the long form);
* ``Dc`` -- the whole input, any library, only when no institution is named;
* ``Dd`` -- Mosseri parent (``VIII.402`` -> ``VIII,402.1``/``.2``), only if all of the
  above are empty.

Every candidate passes a numeric check (its stored variant's integers equal, in order,
the query's; a parent's start with them), so nothing matches only because
``normalize_shelfmark`` glued two numbers together. Leading zeros are stripped on the
raw text, before normalizing (normalizing first turned ``577.10.06=04`` into ``…604``).

Library codes come only from ``LIBRARY_CODES`` (a name or first-word match), never from
how often a head appears: three Sassoon variants stored under ``CUL`` must not admit
every Cambridge record.

T1 itself keeps a library its prefix names (``docs/plans/SHELFMARK_PREFIX_SCOPE_PLAN.md``):
``prefix_codes`` says which codes a removed prefix names and ``named_library_exact`` turns
T1's exact candidates into that library's records plus the records that store the whole
input. ``MetadataManager.resolve_system_by_shelfmark`` and the restated T1 below both call
them, so ``resolve_catalog_form(s)`` and the resolver cannot disagree.

Pure module: no Qt, no NiceGUI, no I/O. ``MetadataManager`` owns the index and the
tables' lifetime.
"""
import functools
import re
from collections import Counter, defaultdict

from shared.browse_map_utils import (
    LIBRARY_CODES,
    _strip_library_prefix,
    natural_sort_key,
    normalize_shelfmark,
)

HEAD_RE = re.compile(r'^(?P<head>.+?)[,\s]+(?:Ms\.?|MS\.?)\s+(?P<rest>\S.*)$')
# What users type: `ms.` in any case (`Mosseri, Jacques, ms. VIII.402`). Stored variants keep
# the case-sensitive HEAD_RE, so the institution table is derived exactly as measured.
QUERY_HEAD_RE = re.compile(HEAD_RE.pattern, re.IGNORECASE)
MIN_HEAD_USES = 3
_LEADING_ZEROS = re.compile(r'(?<!\d)0+(?=\d)')
_HAS_LEADING_ZERO = re.compile(r'(?<!\d)0\d')
_NUMBER = re.compile(r'\d+')
_FIRST_WORD = re.compile(r'[,\s]+')
_PREFIX_TRIM = ' ,:|-\t'


def zs(text):
    """Strip leading zeros from every digit run of the RAW text, keeping a lone 0."""
    return _LEADING_ZEROS.sub('', text)


def zkey(text):
    """Zero-insensitive key: ``normalize_shelfmark(zs(text))`` -- zeros first, then normalize."""
    return normalize_shelfmark(zs(text)) if text else ''


def numbers(text):
    return tuple(int(n) for n in _NUMBER.findall(text or ''))


_CATALOGUE_SHAPE = re.compile(r'(?:^|[\s,])ms\.?\s+\S', re.IGNORECASE)


def looks_like_catalogue_form(text):
    """Cheap gate for callers that must not build the index for every empty search: an
    ``Ms.``/``MS`` token after a comma or space (or at the start), or a zero-padded number."""
    return bool(text) and bool(_CATALOGUE_SHAPE.search(text) or _HAS_LEADING_ZERO.search(text))


def head_of(variant):
    """The institution head of a stored/typed ``<head>[,] Ms. <rest>`` form, lowercased, or None."""
    m = HEAD_RE.match(variant)
    if not m:
        return None
    head = m.group('head').strip().rstrip(',').strip().lower()
    return head or None


def _drop_the(text):
    text = text.strip().lower()
    return text[4:] if text.startswith('the ') else text


def _code_classes():
    """Codes that differ only in case are one class (``BnF``/``BNF``)."""
    classes = defaultdict(set)
    for code in LIBRARY_CODES:
        if code != 'LOCAL':
            classes[code.lower()].add(code)
    return classes


def alias_codes():
    """Lowercased ``LIBRARY_CODES`` code or name -> every code it names (case classes expanded)."""
    classes = _code_classes()
    out = defaultdict(set)
    for code, name in LIBRARY_CODES.items():
        if code == 'LOCAL':
            continue
        out[code.lower()] |= classes[code.lower()]
        out[str(name).lower()] |= classes[code.lower()]
    return {k: frozenset(v) for k, v in out.items()}


def head_codes(head):
    """Authoritative codes for a head: it equals a code's name (a leading ``the`` ignored on
    both sides) or starts with it followed by ``,``/space, or its first word is a code."""
    classes = _code_classes()
    h = _drop_the(head)
    first = _FIRST_WORD.split(h, maxsplit=1)[0]
    out = set()
    for code, name in LIBRARY_CODES.items():
        if code == 'LOCAL':
            continue
        n = _drop_the(str(name))
        if h == n or h.startswith(n + ',') or h.startswith(n + ' ') or first == code.lower():
            out |= classes[code.lower()]
    return frozenset(out)


def prefix_codes(query, stripped, codes_by_alias=None):
    """Codes named by the prefix ``_strip_library_prefix`` removed from ``query``, or empty.

    ``stripped`` is ``_strip_library_prefix(query)``. The removed text must be exactly a
    ``LIBRARY_CODES`` code or name (``alias_codes``); any other alias (``cambridge``,
    ``bodleian``, ...) names no library. Empty too when nothing is left to look up (a bare
    library name: T1 answers it with nothing, so T2 must as well). T1's prefix rule and T2's
    alias institution both use this, so they never disagree about what was named.
    """
    if not query or stripped == query or not normalize_shelfmark(stripped):
        return frozenset()
    prefix = query[:len(query) - len(stripped)].rstrip(_PREFIX_TRIM).strip().lower()
    table = alias_codes() if codes_by_alias is None else codes_by_alias
    return table.get(prefix, frozenset())


def named_prefix(query, stripped, codes_by_alias=None):
    """``(codes, whole key, whole input)`` when ``query``'s prefix names a library, else
    ``(frozenset(), None, None)``. ``query`` is the trimmed input, ``stripped`` its stripped form."""
    codes = prefix_codes(query, stripped, codes_by_alias)
    if not codes:
        return frozenset(), None, None
    return codes, normalize_shelfmark(query) or None, query


@functools.lru_cache(maxsize=1 << 17)
def _provenance(variants):
    """``(codes, zkey(rest), numbers(rest))`` for each stored ``<head> Ms. <rest>`` whose head
    names library codes. ``variants`` is a record's call numbers as a tuple (cache key); the
    ``Ms.`` token is read in any case, as in queries."""
    out = []
    for variant in variants:
        m = QUERY_HEAD_RE.match(variant)
        if m:
            head = m.group('head').strip().rstrip(',').strip().lower()
            codes = head_codes(head) if head else frozenset()
            if codes:
                rest = m.group('rest')
                out.append((codes, zkey(rest), numbers(rest)))
    return tuple(out)


def in_libraries(sys_id, codes, csv_bank, key=None, want=None, cached_variants=None):
    """Whether a record lies in one of ``codes`` for the shelfmark being looked up.

    Yes when its library code is one of them; when nothing says where it is (a record known
    only from the NLI cache: unknown is not outside); or -- collection provenance -- when it
    stores ``<head> Ms. <rest>`` with a head naming one of the codes, ``zkey(rest) == key`` and
    ``numbers(rest) == want`` (``key`` = ``zkey`` of what is looked up, so a zero-padded stored
    form still counts). Provenance is per shelfmark: the JTS record that stores ``Adler, Elkan
    Nathan Ms. 222`` is Adler 222, while the JTS record stored ``Adler ... Ms. 1873`` and
    ``Ms. 1512`` is not Adler 1512, nor is the Sassoon 222 record Adler 222. The stored forms are
    the record's call numbers plus ``cached_variants[sys_id]``: the NLI-cache ``shelfmark``/
    ``shelfmark_alt`` strings the shelfmark index was built from (``MetadataManager`` snapshots
    them with the index, so a later cache eviction or bad value cannot change the answer).
    """
    data = csv_bank.get(sys_id)
    if not data:
        return True
    if data.get('library_code') in codes:
        return True
    if key is None:
        return False
    variants = tuple(data.get('call_numbers_raw') or ())
    if cached_variants:
        variants += cached_variants.get(sys_id, ())
    for head_set, rest_key, rest_numbers in _provenance(variants):
        if rest_key == key and rest_numbers == want and head_set & codes:
            return True
    return False


def _record_variants(sys_id, csv_bank, cached_variants):
    """Every stored form of a record the index was built from: its call numbers (or shelfmark)
    and its cached NLI strings."""
    data = csv_bank.get(sys_id) or {}
    out = tuple(data.get('call_numbers_raw') or ((data['shelfmark'],) if data.get('shelfmark') else ()))
    if cached_variants:
        out += cached_variants.get(sys_id, ())
    return out


def _carrying_variant(hit, key, want, csv_bank, cached_variants, dotless=False, zeros=False):
    """The record's stored form with normalized key ``key`` (or, for a dotless key, that key
    once dots are dropped; with ``zeros``, zero-insensitive keys) and the integers ``want``:
    the hit's own shelfmark when it has them, else the first such variant; None if there is
    none. The index keeps one variant per (record, key), so checking only that one could miss
    `GASTER 123` behind `GASTER 12 3` -- and the option must then show `GASTER 123`."""
    if numbers(hit['shelfmark']) == want:
        return hit['shelfmark']
    for variant in _record_variants(hit['sys_id'], csv_bank, cached_variants):
        norm = zkey(variant) if zeros else normalize_shelfmark(variant)
        if (norm == key or (dotless and norm.replace('.', '') == key)) and numbers(variant) == want:
            return variant
    return None


def _carries(hit, key, want, csv_bank, cached_variants, dotless=False, zeros=False):
    """Whether the record stores such a form (``_carrying_variant``)."""
    return _carrying_variant(hit, key, want, csv_bank, cached_variants, dotless, zeros) is not None


def named_library_exact(stripped, key_hits, bridged, whole_hits, whole_query, codes, csv_bank, cached_variants=None):
    """T1's exact candidates for an input whose prefix named ``codes`` and that T1 answers
    exactly; ``[]`` = keep today's answer.

    ``key_hits`` are today's exact entries for ``stripped`` (key or dotless key), ``bridged``
    today's bridge entry or None, ``whole_hits`` the entries whose key equals the whole
    input's. The answer, in this order: the bridge entry if its record is in a named library
    and no kept key hit has it (as today, so the shelfmark shown does not change); the key
    hits in a named library; the whole hits with ``whole_query``'s integers, in any library
    (they store what was typed). So scoping removes only other libraries' records.

    Membership is ``in_libraries``: code, provenance per shelfmark, or unknown library.
    Provenance matters most where a code holds no record: ``Gaster`` has none (the Gaster
    texts are coded BL/Manchester), and the full BL record of ``GASTER 1201.17`` is kept
    because it stores ``Gaster, Moses Collection Ms. 1201.17`` -- without that the stored
    ``GASTER 1201.17`` stub would replace it. The library of a whole hit is NOT named as a
    whole: Katz holds Halper 275 and also Penn ``Ms. 275``, another manuscript.

    ``[]`` too when nothing kept genuinely carries the numbers -- no whole hit, no in-library
    bridge hit, no key hit with ``stripped``'s integers -- so a list never shrinks to a
    dotless-rule match alone (`Westminster College Ms. L-G Glass 12` must not become the
    Westminster record stored `L-G Glass.1.2`).
    """
    want_whole = numbers(whole_query)
    whole_key = normalize_shelfmark(whole_query)
    whole = []
    for h in whole_hits:
        shown = _carrying_variant(h, whole_key, want_whole, csv_bank, cached_variants)
        if shown is not None:
            whole.append(h if shown == h['shelfmark'] else {**h, 'shelfmark': shown})
    key, want = zkey(stripped), numbers(stripped)
    t1_key = normalize_shelfmark(stripped)
    whole_ids = {h['sys_id'] for h in whole}
    out, carried = [], False
    for h in key_hits:
        if not in_libraries(h['sys_id'], codes, csv_bank, key, want, cached_variants):
            continue
        # A kept key hit shows a variant with the typed integers when the one the index kept
        # lacks them (`123` behind `12 3`); otherwise as today, so the shortest form still wins.
        # With no such variant, a whole-input match of the same record stands for it instead.
        shown = _carrying_variant(h, t1_key, want, csv_bank, cached_variants, '.' not in t1_key)
        if shown is None and h['sys_id'] in whole_ids:
            continue
        carried = carried or shown is not None
        out.append(h if shown in (None, h['shelfmark']) else {**h, 'shelfmark': shown})
    bridge_in = bool(bridged) and in_libraries(bridged['sys_id'], codes, csv_bank, key, want, cached_variants)
    if not (whole or bridge_in or carried):
        return []
    if bridge_in and not any(h['sys_id'] == bridged['sys_id'] for h in out):
        out.insert(0, bridged)
    out.extend(whole)
    return out


class CatalogTables:
    """Lookup tables for ONE shelfmark index list.

    Keeps a reference to that list: ``MetadataManager`` rebuilds the tables when
    ``tables.index is not index`` (never by ``id()``, which Python reuses).
    """

    __slots__ = ('index', 'heads', 'mosseri_heads', 'codes_by_head', 'codes_by_alias', 'zero_side')

    def __init__(self, index, csv_bank):
        self.index = index
        counts = Counter()
        for data in list(csv_bank.values()):
            for variant in data.get('call_numbers_raw') or ():
                if ' Ms' in variant or ' MS' in variant:
                    head = head_of(variant)
                    if head:
                        counts[head] += 1
        self.heads = tuple(sorted((h for h, n in counts.items() if n >= MIN_HEAD_USES), key=len, reverse=True))
        self.mosseri_heads = frozenset(h for h in self.heads if h.startswith('mosseri'))
        self.codes_by_head = {h: head_codes(h) for h in self.heads}
        self.codes_by_alias = alias_codes()
        side = defaultdict(list)
        for entry in index:
            if _HAS_LEADING_ZERO.search(entry[1]):
                key = zkey(entry[1])
                if key and key != entry[3]:
                    side[key].append(entry)
        self.zero_side = dict(side)

    def split(self, query):
        """``(head, rest)`` when the query starts with a known institution head, else None."""
        m = QUERY_HEAD_RE.match(query)
        if not m:
            return None
        head = m.group('head').strip().rstrip(',').strip().lower()
        for name in self.heads:
            if head == name or head.startswith(name + ' ') or head.startswith(name + ','):
                return name, m.group('rest').strip()
        return None


def _shelf_sort_key(entry):
    return (natural_sort_key(entry.get('shelfmark', '')), natural_sort_key(entry.get('title', '')),
            natural_sort_key(entry.get('sys_id', '')))


def _bridge(text):
    try:
        from shared.shelfmark_bridge import lookup_cudl
        return lookup_cudl(text)
    except Exception:
        return None


_NOT_NAMED = (frozenset(), None, None)


class _Plan:
    """The keys one query looks up. ``exact_*`` = T1's own exact conditions on the input."""

    __slots__ = ('query', 'keys', 'nodot', 'bridge_text', 'exact_key', 'exact_nodot', 'exact_bridge',
                 'exact_named', 'institution', 'parent_key', 'parent_form', 'no_t2')

    def __init__(self, query):
        self.query = query
        self.keys = []            # (norm key, query form for the numeric check, tier, needs institution)
        self.nodot = None         # D0 dotless key
        self.bridge_text = None   # D0 bridge text
        self.exact_key = self.exact_nodot = self.exact_bridge = None
        self.exact_named = _NOT_NAMED   # named_prefix() of the input, for T1's prefix rule
        self.institution = None   # (kind, head or None, codes) when one is named
        self.parent_key = self.parent_form = None
        self.no_t2 = False


def _t1_exact_keys(text, stripped=None):
    """The keys of T1's three exact conditions for `text`, exactly as T1 computes them:
    strip a library prefix, normalize; (normalized key, dotless key if the key has no dot,
    bridge text). Used for the input itself and, as D0, for the input without zeros.
    `stripped` is `_strip_library_prefix(text.strip())` when the caller already has it."""
    if stripped is None:
        stripped = _strip_library_prefix(text.strip())
    key = normalize_shelfmark(stripped)
    if not key:
        return None, None, None
    return key, (key if '.' not in key else None), stripped


def t1_exact_entries(queries, tables, csv_bank, scoped=True, cached_variants=None):
    """T1's exact candidates for each query, before deduplication and before T2's numeric
    check or institution filter: index entries with an equal normalized key, or an equal
    dotless key for a dotless query, plus the bridge hit -- narrowed by T1's prefix rule
    (``named_library_exact``) when the input's prefix names a library. ``scoped=False`` gives
    the candidates without the prefix rule (what T1 answered before it). Tests pin this to
    T1's own exact answers."""
    by_key, by_nodot, by_whole, out, whole = (defaultdict(list), defaultdict(list), defaultdict(list),
                                              {}, defaultdict(list))
    keys, named = {}, {}
    for query in queries:
        text = (query or '').strip()
        stripped = _strip_library_prefix(text)
        keys[query] = _t1_exact_keys(text, stripped)
        key, nodot, _ = keys[query]
        named[query] = named_prefix(text, stripped, tables.codes_by_alias) if scoped and key else _NOT_NAMED
        out[query] = []
        if key:
            by_key[key].append(query)
        if nodot:
            by_nodot[nodot].append(query)
        if named[query][1]:
            by_whole[named[query][1]].append(query)
    for e in tables.index:
        for query in by_key.get(e[3], ()):
            out[query].append(_entry(e))
        if by_nodot:
            for query in by_nodot.get(e[4], ()):
                out[query].append(_entry(e))
        if by_whole:
            for query in by_whole.get(e[3], ()):
                whole[query].append(_entry(e))
    for query, (key, _nodot, bridge_text) in keys.items():
        if key:
            key_hits = out[query]
            bridged = _bridge_entry(bridge_text, csv_bank)
            hits = list(key_hits)
            if bridged and not any(h['sys_id'] == bridged['sys_id'] for h in hits):
                hits.insert(0, bridged)
            codes, _whole_key, whole_query = named[query]
            if codes and hits:
                hits = named_library_exact(bridge_text, key_hits, bridged, whole[query], whole_query,
                                           codes, csv_bank, cached_variants) or hits
            out[query] = hits
    return out


def _plan(query, tables, with_exact, prefix_rule=True):
    q = (query or '').strip()
    plan = _Plan(q)
    if not q:
        plan.no_t2 = True
        return plan
    sq = _strip_library_prefix(q)
    if with_exact:
        plan.exact_key, plan.exact_nodot, plan.exact_bridge = _t1_exact_keys(q, sq)
        if plan.exact_key and prefix_rule:
            plan.exact_named = named_prefix(q, sq, tables.codes_by_alias)
    split = tables.split(q)
    if split:
        name, rest = split
        plan.institution = ('head', name, tables.codes_by_head.get(name, frozenset()))
    elif sq != q:
        codes = prefix_codes(q, sq, tables.codes_by_alias)
        if not codes:
            plan.no_t2 = True     # an alias that names no single library: keep today's answer
            return plan
        plan.institution = ('alias', None, codes)
        rest = sq
    named = plan.institution is not None
    dq = zs(q)
    if dq != q:
        dnq, plan.nodot, plan.bridge_text = _t1_exact_keys(dq)
        if dnq:
            plan.keys.append((dnq, q, 'd0', named))
    if split:
        long_form = f"{name} Ms. {rest}"
        plan.keys.append((zkey(long_form), long_form, 'a', False))
        if name in tables.mosseri_heads:
            short = f"Moss. {rest}"
            plan.keys.append((zkey(short), short, 'b', False))
            plan.parent_key, plan.parent_form = zkey(short), short
    if named:
        plan.keys.append((zkey(rest), rest, 'r', True))
        if prefix_rule and plan.institution[0] == 'alias' and rest:
            # The whole input, any library: records that store it up to leading zeros, with
            # its integers -- zero-insensitive on both sides like every T2 key (`Halper 077`
            # -> the record that stores `Halper 77`; `... Acc. 04362` -> the record that
            # stores exactly that). Never for a bare library name, which T1 answers with
            # nothing.
            plan.keys.append((zkey(q), q, 'w', False))
    elif sq:
        plan.keys.append((zkey(sq), sq, 'c', False))
    plan.keys = [k for k in plan.keys if k[0]]
    return plan


def _belongs(sys_id, institution, csv_bank, rest=None, prefix_rule=True, cached_variants=None):
    kind, head, codes = institution
    if kind == 'alias' and prefix_rule:
        # The same membership as T1's prefix rule; provenance compared on zero-insensitive keys.
        if rest is None:
            return in_libraries(sys_id, codes, csv_bank)
        return in_libraries(sys_id, codes, csv_bank, zkey(rest), numbers(rest), cached_variants)
    data = csv_bank.get(sys_id) or {}
    if data.get('library_code') in codes:
        return True
    if kind == 'head':
        for variant in data.get('call_numbers_raw') or ():
            if head_of(variant) == head:
                return True
    return False


def _entry(e):
    return {'sys_id': e[0], 'shelfmark': e[1], 'title': e[2]}


def _dedup(entries):
    by_sid = {}
    for e in entries:
        sid = e['sys_id']
        if sid not in by_sid or len(e['shelfmark']) < len(by_sid[sid]['shelfmark']):
            by_sid[sid] = e
    return list(by_sid.values())


def _response(hits, limit, tier):
    if not hits:
        return {'sys_id': None, 'options': [], 'selected_shelfmark': None, 'total': 0, 'tier': None}
    if len(hits) == 1:
        return {'sys_id': hits[0]['sys_id'], 'options': [], 'selected_shelfmark': hits[0]['shelfmark'],
                'total': 1, 'tier': tier}
    hits.sort(key=_shelf_sort_key)
    return {'sys_id': None, 'options': hits[:limit], 'selected_shelfmark': None, 'total': len(hits),
            'tier': tier}


def _bridge_entry(text, csv_bank):
    hit = _bridge(text)
    if hit and hit.get('sys_id'):
        data = csv_bank.get(hit['sys_id'], {})
        return {'sys_id': hit['sys_id'], 'shelfmark': data.get('shelfmark') or hit.get('shelfmark', '') or '',
                'title': data.get('title', '')}
    return None


def resolve_many(queries, tables, csv_bank, limit=100, with_exact=True, prefix_rule=True, cached_variants=None):
    """Resolve many inputs with one pass over ``tables.index`` (two if a Mosseri parent is needed).

    ``with_exact=True`` first applies T1's exact conditions to the input itself and returns
    that answer when there is one (what ``resolve_system_by_shelfmark`` would answer
    exactly); otherwise, and always when ``with_exact=False``, it returns T2's answer.
    Returns ``{query: {'sys_id', 'options', 'selected_shelfmark', 'total', 'tier'}}``;
    ``tier`` is ``'exact'``, ``'direct'``, ``'parent'`` or None. ``prefix_rule=False`` answers as
    before the library-prefix rule (no scoping, no ``w`` key, code-only alias membership) -- the
    real-data gate compares the two in one process.
    """
    plans = {}
    for query in queries:
        if query not in plans:
            plans[query] = _plan(query, tables, with_exact, prefix_rule)
    by_key = defaultdict(list)
    by_nodot = defaultdict(list)
    exact_key = defaultdict(list)
    exact_nodot = defaultdict(list)
    exact_whole = defaultdict(list)
    for query, plan in plans.items():
        if plan.exact_key:
            exact_key[plan.exact_key].append(query)
            if plan.exact_nodot:
                exact_nodot[plan.exact_nodot].append(query)
            if plan.exact_named[1]:
                exact_whole[plan.exact_named[1]].append(query)
        if plan.no_t2:
            continue
        for key, form, tier, needs in plan.keys:
            by_key[key].append((query, form, needs, tier))
        if plan.nodot:
            by_nodot[plan.nodot].append(query)
    exact_found = defaultdict(list)
    whole_found = defaultdict(list)
    found = defaultdict(list)       # query -> [(entry tuple, query form or None, needs institution)]
    nodot_found = defaultdict(list)
    # Independent checks (not elif): an entry can match one query's key and another
    # query's dotless key. A query matching the same entry twice is deduplicated later.
    for e in tables.index:
        norm = e[3]
        hit = exact_key.get(norm)
        if hit:
            for query in hit:
                exact_found[query].append(e)
        if exact_nodot:
            hit = exact_nodot.get(e[4])
            if hit:
                for query in hit:
                    exact_found[query].append(e)
        if exact_whole:
            hit = exact_whole.get(norm)
            if hit:
                for query in hit:
                    whole_found[query].append(e)
        hit = by_key.get(norm)
        if hit:
            for query, form, needs, tier in hit:
                found[query].append((e, form, needs, tier))
        if by_nodot:
            hit = by_nodot.get(e[4])
            if hit:
                for query in hit:
                    nodot_found[query].append(e)

    results = {}
    need_parent = {}
    for query, plan in plans.items():
        if plan.exact_key:
            key_hits = [_entry(e) for e in exact_found.get(query, ())]
            bridged = _bridge_entry(plan.exact_bridge, csv_bank)
            hits = list(key_hits)
            if bridged and not any(h['sys_id'] == bridged['sys_id'] for h in hits):
                hits.insert(0, bridged)
            codes, _whole_key, whole_query = plan.exact_named
            if codes and hits:
                hits = named_library_exact(plan.exact_bridge, key_hits, bridged,
                                           [_entry(e) for e in whole_found.get(query, ())],
                                           whole_query, codes, csv_bank, cached_variants) or hits
            if hits:
                results[query] = _response(_dedup(hits), limit, 'exact')
                continue
        if plan.no_t2:
            results[query] = _response([], limit, None)
            continue
        cands = []
        want = numbers(plan.query)
        institution = plan.institution

        alias_rest = plan.query[len(plan.query) - len(_strip_library_prefix(plan.query)):] if (
            institution and institution[0] == 'alias') else None

        def keep(e_sys_id, needs):
            return not needs or _belongs(e_sys_id, institution, csv_bank, alias_rest, prefix_rule, cached_variants)

        def shown(e, form, tier):
            """The stored form to offer for index entry ``e`` matched on a key of ``form``."""
            if numbers(e[1]) == numbers(form):
                return e[1]
            if tier != 'w':
                return None
            # The whole-input key looks at every stored variant, as T1's prefix rule does: the
            # index may have kept `GASTER 12 3` for a record that also stores `GASTER 123`,
            # which is then the form offered.
            return _carrying_variant(_entry(e), zkey(form), numbers(form), csv_bank, cached_variants, zeros=True)

        def offer(e, form, tier, needs):
            s = shown(e, form, tier)
            if s is not None and keep(e[0], needs):
                cands.append({'sys_id': e[0], 'shelfmark': s, 'title': e[2]})

        for e, form, needs, tier in found.get(query, ()):
            offer(e, form, tier, needs)
        for e in nodot_found.get(query, ()):
            if numbers(e[1]) == want and keep(e[0], institution is not None):
                cands.append(_entry(e))
        for key, form, tier, needs in plan.keys:
            for e in tables.zero_side.get(key, ()):
                offer(e, form, tier, needs)
        if plan.bridge_text:
            bridged = _bridge_entry(plan.bridge_text, csv_bank)
            if bridged and numbers(bridged['shelfmark']) == want and keep(bridged['sys_id'], institution is not None):
                cands.append(bridged)
        if cands:
            results[query] = _response(_dedup(cands), limit, 'direct')
        elif plan.parent_key:
            need_parent[query] = plan
        else:
            results[query] = _response([], limit, None)

    if need_parent:
        parents = defaultdict(list)
        for query, plan in need_parent.items():
            parents[plan.parent_key].append(query)
        parent_found = defaultdict(list)
        for e in tables.index:
            norm = e[3]
            start = norm.find('.')
            while start != -1:
                hit = parents.get(norm[:start])
                if hit:
                    for query in hit:
                        parent_found[query].append(e)
                start = norm.find('.', start + 1)
        for query, plan in need_parent.items():
            want = numbers(plan.parent_form)
            cands = []
            for e in parent_found.get(query, ()):
                got = numbers(e[1])
                if len(got) > len(want) and got[:len(want)] == want:
                    cands.append(_entry(e))
            results[query] = _response(_dedup(cands), limit, 'parent' if cands else None)
    return results
