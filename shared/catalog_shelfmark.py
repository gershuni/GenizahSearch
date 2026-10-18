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

Pure module: no Qt, no NiceGUI, no I/O. ``MetadataManager`` owns the index and the
tables' lifetime.
"""
import re
from collections import Counter, defaultdict

from shared.browse_map_utils import (
    LIBRARY_CODES,
    _strip_library_prefix,
    natural_sort_key,
    normalize_shelfmark,
)

HEAD_RE = re.compile(r'^(?P<head>.+?)[,\s]+(?:Ms\.?|MS\.?)\s+(?P<rest>\S.*)$')
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
        m = HEAD_RE.match(query)
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


class _Plan:
    """The keys one query looks up. ``exact_*`` = T1's own exact conditions on the input."""

    __slots__ = ('query', 'keys', 'nodot', 'bridge_text', 'exact_key', 'exact_nodot', 'exact_bridge',
                 'institution', 'parent_key', 'parent_form', 'no_t2')

    def __init__(self, query):
        self.query = query
        self.keys = []            # (norm key, query form for the numeric check, tier, needs institution)
        self.nodot = None         # D0 dotless key
        self.bridge_text = None   # D0 bridge text
        self.exact_key = self.exact_nodot = self.exact_bridge = None
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


def t1_exact_entries(queries, tables, csv_bank):
    """T1's exact candidates for each query, before deduplication, the numeric check or the
    institution filter: index entries with an equal normalized key, or an equal dotless key
    for a dotless query, plus the bridge hit. Tests pin this to T1's own exact answers."""
    by_key, by_nodot, out = defaultdict(list), defaultdict(list), {}
    keys = {}
    for query in queries:
        keys[query] = _t1_exact_keys(query or '')
        key, nodot, _ = keys[query]
        out[query] = []
        if key:
            by_key[key].append(query)
        if nodot:
            by_nodot[nodot].append(query)
    for e in tables.index:
        for query in by_key.get(e[3], ()):
            out[query].append(_entry(e))
        if by_nodot:
            for query in by_nodot.get(e[4], ()):
                out[query].append(_entry(e))
    for query, (key, _nodot, bridge_text) in keys.items():
        if key:
            bridged = _bridge_entry(bridge_text, csv_bank)
            if bridged and not any(h['sys_id'] == bridged['sys_id'] for h in out[query]):
                out[query].insert(0, bridged)
    return out


def _plan(query, tables, with_exact):
    q = (query or '').strip()
    plan = _Plan(q)
    if not q:
        plan.no_t2 = True
        return plan
    sq = _strip_library_prefix(q)
    if with_exact:
        plan.exact_key, plan.exact_nodot, plan.exact_bridge = _t1_exact_keys(q, sq)
    split = tables.split(q)
    if split:
        name, rest = split
        plan.institution = ('head', name, tables.codes_by_head.get(name, frozenset()))
    elif sq != q:
        prefix = q[:len(q) - len(sq)].rstrip(_PREFIX_TRIM).strip().lower()
        codes = tables.codes_by_alias.get(prefix)
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
    elif sq:
        plan.keys.append((zkey(sq), sq, 'c', False))
    plan.keys = [k for k in plan.keys if k[0]]
    return plan


def _belongs(sys_id, institution, csv_bank):
    kind, head, codes = institution
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


def resolve_many(queries, tables, csv_bank, limit=100, with_exact=True):
    """Resolve many inputs with one pass over ``tables.index`` (two if a Mosseri parent is needed).

    ``with_exact=True`` first applies T1's exact conditions to the input itself and returns
    that answer when there is one (what ``resolve_system_by_shelfmark`` would answer
    exactly); otherwise, and always when ``with_exact=False``, it returns T2's answer.
    Returns ``{query: {'sys_id', 'options', 'selected_shelfmark', 'total', 'tier'}}``;
    ``tier`` is ``'exact'``, ``'direct'``, ``'parent'`` or None.
    """
    plans = {}
    for query in queries:
        if query not in plans:
            plans[query] = _plan(query, tables, with_exact)
    by_key = defaultdict(list)
    by_nodot = defaultdict(list)
    exact_key = defaultdict(list)
    exact_nodot = defaultdict(list)
    for query, plan in plans.items():
        if plan.exact_key:
            exact_key[plan.exact_key].append(query)
            if plan.exact_nodot:
                exact_nodot[plan.exact_nodot].append(query)
        if plan.no_t2:
            continue
        for key, form, tier, needs in plan.keys:
            by_key[key].append((query, form, needs))
        if plan.nodot:
            by_nodot[plan.nodot].append(query)
    exact_found = defaultdict(list)
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
        hit = by_key.get(norm)
        if hit:
            for query, form, needs in hit:
                found[query].append((e, form, needs))
        if by_nodot:
            hit = by_nodot.get(e[4])
            if hit:
                for query in hit:
                    nodot_found[query].append(e)

    results = {}
    need_parent = {}
    for query, plan in plans.items():
        if plan.exact_key:
            hits = [_entry(e) for e in exact_found.get(query, ())]
            bridged = _bridge_entry(plan.exact_bridge, csv_bank)
            if bridged and not any(h['sys_id'] == bridged['sys_id'] for h in hits):
                hits.insert(0, bridged)
            if hits:
                results[query] = _response(_dedup(hits), limit, 'exact')
                continue
        if plan.no_t2:
            results[query] = _response([], limit, None)
            continue
        cands = []
        want = numbers(plan.query)

        def keep(e_sys_id, needs):
            return not needs or _belongs(e_sys_id, plan.institution, csv_bank)

        for e, form, needs in found.get(query, ()):
            if numbers(e[1]) == numbers(form) and keep(e[0], needs):
                cands.append(_entry(e))
        for e in nodot_found.get(query, ()):
            if numbers(e[1]) == want and keep(e[0], plan.institution is not None):
                cands.append(_entry(e))
        for key, form, tier, needs in plan.keys:
            for e in tables.zero_side.get(key, ()):
                if numbers(e[1]) == numbers(form) and keep(e[0], needs):
                    cands.append(_entry(e))
        if plan.bridge_text:
            bridged = _bridge_entry(plan.bridge_text, csv_bank)
            if bridged and numbers(bridged['shelfmark']) == want and keep(bridged['sys_id'], plan.institution is not None):
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
