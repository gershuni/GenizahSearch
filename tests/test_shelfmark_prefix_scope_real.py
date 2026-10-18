"""A library prefix keeps its library, on the real corpus (docs/plans/SHELFMARK_PREFIX_SCOPE_PLAN.md).

libraries.csv is tracked, so these run in CI; they skip only when the corpus is absent
(< 100,000 rows). The CUDL collision report goes to a temporary directory and the per-user
NLI cache is not used (tests/shelfmark_corpus.py).

The population gate is `slow`: CI's main job checks every 20th input, the must-be-zero
counts and that slice's class counts; `pytest -m slow` or GENIZAH_SHELFMARK_FULL=1 checks all
~1.96M inputs and the class counts measured for the plan.
"""
import collections
import os
import random
import re

import pytest

import shared.catalog_shelfmark as cs
from shared.browse_map_utils import LIBRARY_CODES, _strip_library_prefix, normalize_shelfmark
from tests.shelfmark_corpus import MIN_ROWS, load_real_manager

HALPER_77 = '990001664240205171'
MAN_P45 = '990053746560205171'
# query -> (ids in order, tier), from resolve_catalog_form and resolve_system_by_shelfmark alike.
GOLDEN = {
    # The named library's record; today it was not offered (other libraries' records were).
    'Halper 77': ([HALPER_77], 'exact'),
    'The University of Manchester Library, Ms. P 45': ([MAN_P45], 'exact'),
    'Manchester Ms. P 45': ([MAN_P45], 'exact'),
    'GASTER 1775.5': (['990053556510205171'], 'exact'),                # not JTS `ENA 1775.5`
    'Corwin 1': (['990044306100205171'], 'exact'),
    # Two records carry the typed shelfmark: both, never one swapped for the other.
    'The Bodleian Libraries, University of Oxford, arch. O.d.8/1': (['990053385670205171', '990053402960205171'], 'exact'),
    # Only a dotless-rule match (Glass.1.2) is in Westminster: today's two records stay -- an
    # INHERITED answer (Glass.1.2 is another item), pinned because the rule must not change it.
    'Westminster College Ms. L-G Glass 12': (['990053937450205171', '990053937850205171'], 'exact'),
    # Provenance per shelfmark: the record that stores `Adler, Elkan Nathan Ms. 222`, not the
    # Sassoon 222 record that also stores `Ms. 222`.
    'adler Ms. 222': (['990001118220205171'], 'exact'),
    # ... and not the JTS record whose Adler number is 1873 though it stores `Ms. 1512`: no
    # Adler 1512 qualifies, so today's answer -- INHERITED (it lists Mosseri 151/2 and Adler 1873).
    'adler MS 1512': (['990053870900205171', '990002013740205171', '990002013750205171', '990053004400205171',
                       '990001035710205171'], 'exact'),
    # Provenance: the Schocken record stores `Sofer, David Ms. 111.12`; both stay (today: both).
    'sofer Ms. 111.12': (['997008707146905171', '990053965890205171'], 'exact'),
    # Nothing in the named library and nothing stores the input: today's answer -- INHERITED
    # (the HAS record stored `441` is not Halper 441; owner question 2 of the plan).
    'Halper 441': (['990001035960205171'], 'exact'),
    # Unchanged and INCOMPLETE: the BL stub that stores `GASTER 1201.17` is not offered (the
    # whole-input key keeps the `Ms.`); logged in docs/OPEN_ISSUES.md.
    'Gaster Ms. 1201.17': (['990053540570205171'], 'exact'),
    # No prefix, or an alias that names no library: unchanged.
    'Cambridge T-S 12.123': (['990051334060205171'], 'exact'),
    'CUL T-S 12123': (['990051334060205171'], 'exact'),
    # Not exact today: T2 as before (Ms. 2162, not also Ms. 216.2).
    'Adler, Elkan Nathan Ms. 2162': (['990001021020205171'], 'direct'),
    # T2 names the library of the record that stores the whole input: the padded form reaches
    # both BL records, as `GASTER 1201.17` does.
    'GASTER 01201.017': (['990053540570205171', '990053541630205171'], 'direct'),
}


@pytest.fixture(scope="module")
def mm(tmp_path_factory):
    m = load_real_manager(tmp_path_factory.mktemp("cudl_report"))
    if len(m.csv_bank) < MIN_ROWS:
        pytest.skip(f"libraries.csv has only {len(m.csv_bank)} rows (corpus not present)")
    return m


@pytest.fixture(scope="module")
def tables(mm):
    return mm._get_catalog_tables(mm._get_shelfmark_index())


@pytest.fixture(scope="module")
def populations(mm, tables):
    return _populations(mm, tables)


def _ids(r):
    return [r['sys_id']] if r.get('sys_id') else [o['sys_id'] for o in r.get('options') or []]


def _full(request):
    return os.environ.get('GENIZAH_SHELFMARK_FULL') == '1' or (request.config.option.markexpr or '').strip() == 'slow'


def test_golden_prefixed_inputs(mm):
    batch = mm.resolve_catalog_forms(list(GOLDEN))
    for query, (want, tier) in GOLDEN.items():
        assert (_ids(batch[query]), batch[query]['tier']) == (want, tier), query
        assert _ids(mm.resolve_system_by_shelfmark(query)) == want, query


def test_halper_numbers_reach_their_katz_records(mm, tables):
    """Halper 1-499: today most take the records numbered N in other libraries (T1), or
    substring options. Every number that a record stores as `Halper N` / `Genizah N` now
    answers with exactly those records (T1's prefix rule when T1 answers exactly, else T2's
    whole-input key); the others keep today's answer."""
    queries = [f'Halper {n}' for n in range(1, 500)]
    stores = collections.defaultdict(set)
    for e in tables.index:
        if e[3].startswith('genizah'):
            stores[e[3]].add((e[0], cs.numbers(e[1])))
    answers = mm.resolve_catalog_forms(queries, limit=10 ** 6)
    before = cs.t1_exact_entries(queries, tables, mm.csv_bank, scoped=False)
    reached = 0
    for q in queries:
        want = {sid for sid, nums in stores.get(cs.normalize_shelfmark(q), ()) if nums == cs.numbers(q)}
        got = set(_ids(answers[q]))
        if want:
            reached += 1
            assert got == want, q
        elif before[q]:
            assert got == {h['sys_id'] for h in before[q]}, q      # R3: today's answer
    assert reached >= 480


# ---------------------------------------------------------------- the population gate

def _strip_ms(v):
    return re.sub(r'^\s*ms\.?\s+', '', v, flags=re.IGNORECASE)


def _populations(mm, tables):
    """The plan's populations (section 4): (name, query, source sys_id)."""
    index = mm._get_shelfmark_index()
    bank = mm.csv_bank
    out, stored, seen = [], [], set()
    for e in index:
        v = e[1].strip()
        if _strip_library_prefix(v) != v and (v, e[0]) not in seen:
            seen.add((v, e[0]))
            stored.append((v, e[0]))
            out.append(('P1', v, e[0]))
    short = collections.defaultdict(list)      # variants with no prefix and no head
    for sid, d in bank.items():
        for v in d.get('call_numbers_raw') or [d.get('shelfmark', '')]:
            v = (v or '').strip()
            if v and _strip_library_prefix(v) == v and not cs.head_of(v):
                short[sid].append(v)
    for sid, d in bank.items():
        code = d.get('library_code')
        if code not in LIBRARY_CODES or code == 'LOCAL':
            continue
        name = LIBRARY_CODES[code]
        for v in short[sid]:
            out.append(('P2code', f'{code} {v}', sid))
            out.append(('P2name', f'{name}, {v}', sid))
            out.append(('P3', f'{name}, Ms. {_strip_ms(v)}', sid))
    head_text = {}
    for sid, d in bank.items():
        for v in d.get('call_numbers_raw') or ():
            m = cs.HEAD_RE.match(v)
            if m and (' Ms' in v or ' MS' in v):
                head = m.group('head').rstrip(',')
                out.append(('P4', f"{head}, Ms. {m.group('rest')}", sid))
                if head.strip().lower() in tables.codes_by_head:
                    head_text.setdefault(head.strip().lower(), collections.Counter())[head] += 1
    # P5: a collection alias that stored texts use for records of another code, typed before
    # the short variants of that code's records.
    alias = cs.alias_codes()
    pairs = collections.Counter()
    for v, sid in stored:
        sq = _strip_library_prefix(v)
        codes = cs.prefix_codes(v, sq, alias)
        if codes and bank[sid].get('library_code') not in codes:
            pairs[(v[:len(v) - len(sq)].rstrip(' ,:|-\t').strip().lower(), bank[sid].get('library_code'))] += 1
    by_code = collections.defaultdict(list)
    for sid, d in bank.items():
        by_code[d.get('library_code')].append(sid)
    for (prefix, code), n in pairs.items():
        if n >= 3:
            out.extend(('P5', f'{prefix} {v}', sid) for sid in by_code[code] for v in short[sid])
    # P6: each real NLI head before the short variants of its library's records, including
    # records that store no long form (`Mosseri, Jacques, Ms. Ch 2`).
    heads_by_code = collections.defaultdict(list)
    for low, counter in head_text.items():
        for code in tables.codes_by_head.get(low, ()):
            heads_by_code[code].append(counter.most_common(1)[0][0])
    for sid, d in bank.items():
        for head in heads_by_code.get(d.get('library_code'), ()):
            out.extend(('P6', f'{head}, Ms. {_strip_ms(v)}', sid) for v in short[sid])
    return out


def _classify(old, new, source, lib):
    if new == old:
        return 'same'
    if source in old and source not in new:
        return 'LOST_SOURCE'
    if source not in old and source in new:
        if new & old:
            return 'added_kept'
        old_libs = {lib.get(s) for s in old}
        return 'REPLACED_SAME_LIBRARY' if any(lib.get(s) in old_libs for s in new) else 'replaced_other_library'
    if source not in new:
        return 'changed_source_absent'
    if len(new) == 1:
        return 'narrowed_to_source'
    if len(old) == 1:
        return 'widened'
    return 'narrowed' if new < old else 'changed_source_kept'


# Measured for the plan (section 4), same classes as this gate: full runs must reach each
# count minus 0.5%. The every-20th run takes a fixed slice whose classes are not spread evenly
# (it holds 65% of a twentieth of P4's narrowed_to_source), so SLICE_FLOORS are that slice's own
# counts, measured on the same code, under the same rule.
FLOORS = {
    ('P1', 'narrowed_to_source'): 757, ('P1', 'narrowed'): 251, ('P1', 'added_kept'): 2_177,
    ('P1', 'replaced_other_library'): 41, ('P1', 't2 grew'): 8_255,
    ('P2code', 'narrowed_to_source'): 1_942, ('P2code', 'narrowed'): 1_665,
    ('P2name', 'narrowed_to_source'): 1_942, ('P2name', 'narrowed'): 1_665, ('P2name', 'widened'): 99,
    ('P3', 'narrowed_to_source'): 1_942, ('P3', 'narrowed'): 1_665,
    ('P4', 'narrowed_to_source'): 710, ('P4', 'narrowed'): 251,
    ('P5', 'narrowed_to_source'): 335, ('P5', 'narrowed'): 262,
    ('P6', 'narrowed_to_source'): 1_111, ('P6', 'narrowed'): 326,
}
SLICE_FLOORS = {
    ('P1', 'narrowed_to_source'): 43, ('P1', 'narrowed'): 11, ('P1', 'added_kept'): 111,
    ('P1', 'replaced_other_library'): 2, ('P1', 't2 grew'): 376,
    ('P2code', 'narrowed_to_source'): 86, ('P2code', 'narrowed'): 83,
    ('P2name', 'narrowed_to_source'): 101, ('P2name', 'narrowed'): 84, ('P2name', 'widened'): 5,
    ('P3', 'narrowed_to_source'): 81, ('P3', 'narrowed'): 74,
    ('P4', 'narrowed_to_source'): 23, ('P4', 'narrowed'): 11,
    ('P5', 'narrowed_to_source'): 13, ('P5', 'narrowed'): 13,
    ('P6', 'narrowed_to_source'): 55, ('P6', 'narrowed'): 11,
}
# P5 pairs an alias with ANOTHER record's number, so its source is not always the intended
# record. These eight lose it to the records that store `Adler, Elkan Nathan Ms. N` (the source
# is Adler 3273 / Sassoon 222 / ... that also stores JTS `Ms. N`).
P5_ACCEPTED = {
    ('adler Ms. 222', '990000905200205171'), ('adler Ms. 699', '990001021120205171'),
    ('adler Ms. 415', '990001012950205171'), ('adler Ms. 674', '990001016460205171'),
    ('adler Ms. 410', '990001012880205171'), ('adler Ms. 411', '990001012890205171'),
    ('adler Ms. 697', '990001021090205171'), ('adler Ms. 689', '990001021000205171'),
}
# The gate judges membership with its OWN restatement of the rule's inputs -- which codes a
# prefix names, which codes a stored head names, zeros stripped -- so a defect in the production
# helpers cannot hide itself (Codex rounds 2 and 3).
_HEAD = re.compile(r'^(?P<head>.+?)[,\s]+ms\.?\s+(?P<rest>\S.*)$', re.IGNORECASE)
_ZEROS = re.compile(r'(?<!\d)0+(?=\d)')


def _case_class(code):
    return {c for c in LIBRARY_CODES if c != 'LOCAL' and c.lower() == code.lower()}


def _codes_named_by(prefix):
    """A typed prefix names a library when it is exactly a LIBRARY_CODES code or name."""
    out = set()
    for code, name in LIBRARY_CODES.items():
        if code != 'LOCAL' and prefix in (code.lower(), str(name).lower()):
            out |= _case_class(code)
    return out


def _drop_the(text):
    text = text.strip().lower()
    return text[4:] if text.startswith('the ') else text


def _codes_of_head(head):
    """A stored head names a library when it is (or starts with) its name, or starts with its code."""
    h = _drop_the(head.rstrip(','))
    first = re.split(r'[,\s]+', h, maxsplit=1)[0]
    out = set()
    for code, name in LIBRARY_CODES.items():
        n = _drop_the(str(name))
        if code != 'LOCAL' and (h == n or h.startswith(n + ',') or h.startswith(n + ' ') or first == code.lower()):
            out |= _case_class(code)
    return out


def _same_shelfmark(a, b):
    return (normalize_shelfmark(_ZEROS.sub('', a)) == normalize_shelfmark(_ZEROS.sub('', b))
            and cs.numbers(a) == cs.numbers(b))


def _stores_collection_form(data, codes, text):
    """The record stores `<head> Ms. <rest>` with a head naming one of `codes` and `<rest>` this
    shelfmark (zeros ignored, same integers)."""
    for v in (data or {}).get('call_numbers_raw') or ():
        m = _HEAD.match(v)
        if m and _codes_of_head(m.group('head')) & codes and _same_shelfmark(m.group('rest'), text):
            return True
    return False


def _shown(answer):
    if answer['sys_id']:
        return {answer['sys_id']: answer['selected_shelfmark']}
    return {o['sys_id']: o['shelfmark'] for o in answer['options']}


@pytest.mark.slow
def test_prefix_rule_population_gate(mm, tables, populations, request):
    """Against the same code with prefix_rule=False (before the rule; byte-identical to the
    pristine base over every input, plan section 6), full responses: nothing changes for an input
    whose prefix names no library or for a head form's T2 answer; exactness never changes; an
    exact answer loses no record that lies in the named library (its code, no csv row, or a stored
    `<head> Ms. <this shelfmark>` whose head names it) and gains only records that store the whole
    input; a T2 answer only grows, by such records or records that store the input; totals are
    right; surviving records keep their order and shown shelfmark; no source is lost outside
    P5's listed cases; class counts reach the measured floors."""
    full = _full(request)
    cases = populations if full else populations[::20]
    bank = mm.csv_bank
    lib = {sid: d.get('library_code') for sid, d in bank.items()}
    stored = collections.defaultdict(list)      # normalized key -> [(sys_id, stored variant)]
    for e in tables.index:
        stored[e[3]].append((e[0], e[1]))
    counts = collections.Counter()
    problems = []
    chunk = 50_000
    for start in range(0, len(cases), chunk):
        part = cases[start:start + chunk]
        queries = list(dict.fromkeys(q for _, q, _ in part))
        now_all = cs.resolve_many(queries, tables, bank, limit=10 ** 9)
        before_all = cs.resolve_many(queries, tables, bank, limit=10 ** 9, prefix_rule=False)
        for pop, q, sid in part:
            a, b = now_all[q], before_all[q]
            ids_a, ids_b = _ids(a), _ids(b)
            if a['total'] != len(ids_a) or b['total'] != len(ids_b):
                problems.append((pop, 'WRONG TOTAL', q, a['total'], len(ids_a)))
            text = q.strip()
            stripped = _strip_library_prefix(text)
            prefix = text[:len(text) - len(stripped)].rstrip(' ,:|-\t').strip().lower()
            codes = _codes_named_by(prefix) if stripped != text and normalize_shelfmark(stripped) else set()
            if not codes:
                if a != b:
                    problems.append((pop, 'CHANGED WITHOUT A NAMED LIBRARY', q))
                counts[pop, 'no named library'] += 1
                continue
            if (a['tier'] == 'exact') != (b['tier'] == 'exact'):
                problems.append((pop, 'EXACTNESS CHANGED', q, b['tier'], a['tier']))
                continue
            new, old = set(ids_a), set(ids_b)
            both = new & old
            if [s for s in ids_a if s in both] != [s for s in ids_b if s in both]:
                problems.append((pop, 'ORDER CHANGED', q))
            shown_now, shown_before = _shown(a), _shown(b)
            if any(shown_now[s] != shown_before[s] for s in both):
                problems.append((pop, 'SHOWN SHELFMARK CHANGED', q))

            def in_library(s):
                return lib.get(s) in codes or s not in bank or _stores_collection_form(bank.get(s), codes, stripped)

            if a['tier'] != 'exact':
                if tables.split(text) is not None:
                    if a != b:                       # T2 for a head form is not touched
                        problems.append((pop, 'T2 CHANGED FOR A HEAD FORM', q))
                    counts[pop, 't2 head form'] += 1
                    continue
                if not old <= new:
                    problems.append((pop, 'T2 ANSWER LOST A RECORD', q, sorted(old - new)[:4]))
                zstores = {s for s, v in stored.get(normalize_shelfmark(_ZEROS.sub('', text)), ())
                           if cs.numbers(v) == cs.numbers(text)}
                zstores |= {s for s in new - old for v in (bank.get(s) or {}).get('call_numbers_raw') or ()
                            if _same_shelfmark(v, text)}
                stray = {s for s in new - old if s not in zstores and not in_library(s)}
                if stray:
                    problems.append((pop, 'T2 ADDED A RECORD OUTSIDE THE LIBRARY', q, sorted(stray)[:4]))
                counts[pop, 't2 grew' if new != old else 't2 same'] += 1
                continue
            status = _classify(old, new, sid, lib)
            counts[pop, status] += 1
            if status.isupper() and not (pop == 'P5' and status == 'LOST_SOURCE' and (q, sid) in P5_ACCEPTED):
                problems.append((pop, status, q, sid, sorted(old)[:4], sorted(new)[:4]))
            whole = {s for s, v in stored.get(normalize_shelfmark(text), ()) if cs.numbers(v) == cs.numbers(text)}
            if new - old - whole:
                problems.append((pop, 'ADDED A RECORD THAT DOES NOT STORE THE INPUT', q, sorted(new - old - whole)[:4]))
            dropped_in_library = [s for s in old - new if in_library(s)]
            if dropped_in_library:
                problems.append((pop, 'DROPPED AN IN-LIBRARY RECORD', q, dropped_in_library[:4]))
    print(sorted(counts.items()))
    assert not problems, problems[:20]
    assert sum(n for (p, _), n in counts.items() if p == 'P1') >= (261_000 if full else 13_000)
    for key, want in (FLOORS if full else SLICE_FLOORS).items():
        assert counts[key] >= want * 0.995, (key, counts[key], want)


def test_resolver_and_batch_agree_on_prefixed_inputs(mm, populations, request):
    """The resolver and the restated T1 apply one prefix rule: every field -- the record, the
    shown shelfmark, each option's id, shelfmark and title, in order -- at limits 0, 1 and
    unlimited, for prefixed inputs either answers from exact or catalogue-form matches."""
    rng = random.Random(11)
    pool = sorted({q for _, q, _ in populations})
    sample = rng.sample(pool, 1000 if _full(request) else 100)
    checked = 0
    for limit in (0, 1, 10 ** 6):
        batch = mm.resolve_catalog_forms(sample, limit=limit)
        for q in sample:
            b = batch[q]
            if not b['total']:
                continue                     # the resolver then adds substring options
            r = mm.resolve_system_by_shelfmark(q, limit=limit)
            assert r == {'sys_id': b['sys_id'], 'options': b['options'], 'selected_shelfmark': b['selected_shelfmark']}, (q, limit)
            checked += 1
    assert checked >= len(sample)
