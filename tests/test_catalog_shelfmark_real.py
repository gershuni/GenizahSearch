"""Catalogue-form shelfmarks on the real corpus (plan gates 3, 4 and the gate-5 drift check).

docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md. libraries.csv is tracked, so these run
in CI. They skip only when the corpus is absent (< 100,000 rows); any other failure
fails. The CUDL collision report goes to a temporary directory and the per-user NLI
cache is not used (tests/shelfmark_corpus.py).

The adversarial and simulation gates are `slow`. CI's main job runs `slow` tests too,
so there they check a fixed subsample (every 20th input) and only the must-be-zero
counts; the dedicated `pytest -m slow` job, or GENIZAH_SHELFMARK_FULL=1, runs every
input and also checks the recall counts measured for the plan.
"""
import collections
import json
import os
import random
import re
from pathlib import Path

import pytest

import shared.catalog_shelfmark as cs
from tests.shelfmark_corpus import MIN_ROWS, load_real_manager

BASELINE = Path(__file__).parent / "fixtures" / "shelfmark_resolve_baseline.json"
CUL_K826 = '990051208260205171'
MOSS_402 = ['990053809560205171', '990053809570205171']
BNF_1489 = '990035991030205171'
NONE = ([], 0, None)

# query -> (ids in order, total, tier) from resolve_catalog_form; resolve_system_by_shelfmark
# must give the same ids (it adds substring options only where both are empty).
GOLDEN = {
    # The two forms from the user's screenshot, and their variants.
    'Cambridge University Library, Ms. T-S K 08.026': ([CUL_K826], 1, 'direct'),
    'Cambridge University Library Ms. T-S K 08.026': ([CUL_K826], 1, 'direct'),
    'T-S K 08.026': ([CUL_K826], 1, 'direct'),
    'CUL T-S K 08.026': ([CUL_K826], 1, 'direct'),
    'Mosseri, Jacques, Ms. VIII.402': (MOSS_402, 2, 'parent'),
    'Mosseri, Jacques Ms. VIII.402': (MOSS_402, 2, 'parent'),
    'Mosseri Jacques, Ms. VIII.402': (MOSS_402, 2, 'parent'),
    'Mosseri, Jacques Paris France Ms. VIII.402': (MOSS_402, 2, 'parent'),
    'Mosseri, Jacques, ms. VIII.402': (MOSS_402, 2, 'parent'),             # any case of "Ms."
    'cambridge university library, MS. T-S K 08.026': ([CUL_K826], 1, 'direct'),
    'Mosseri, Jacques, Ms. IV.366': (['990053835860205171'], 1, 'direct'),     # the unit, not its parts
    'Mosseri, Jacques, Ms. III 27O': (['990053835020205171'], 1, 'direct'),
    'Mosseri, Jacques, Ms. III 270': NONE,                                     # zero is not letter O
    # A named library is never dropped (Codex round 1, HIGH 2).
    'Cambridge University Library, Ms. F 002': NONE,
    'The British Library, Ms. F 002': NONE,
    'British Library, Ms. F 002': NONE,
    'CUL F 002': NONE,
    'Ms. F 002': (['990053942440205171'], 1, 'direct'),
    'F 001': (['990053864050205171', '990053942430205171'], 2, 'direct'),      # never one silently
    # Zeros stripped before normalizing (Codex round 1, HIGH 1).
    'The National Library of Israel, Ms. Heb. 577.10.06=04': (['990025438040205171'], 1, 'direct'),
    'the national library of israel, Ms. Heb. 0577.010.01=04': (['990025437890205171'], 1, 'direct'),
    # Words between the library and "Ms." defeat today's exact step, so only the rebuilt
    # long form can answer -- and only if its zeros were stripped before normalizing.
    'The National Library of Israel Jerusalem Ms. Heb. 577.10.06=04': (['990025438040205171'], 1, 'direct'),
    # A record reached only through its stored long form (no short variant to match).
    'Cambridge University Library, Ms. T-S 12.661': (['990051339130205171'], 1, 'direct'),
    # Same-library records are kept (Codex round 2, HIGH 1).
    'Library of the Hungarian Academy of Sciences, Ms. 06': (['990001004110205171', '990001261670205171'], 2, 'direct'),
    'Library of the Hungarian Academy of Sciences, Ms. 037':
        (['990001962920205171', '990000951170205171', '990001261890205171'], 3, 'direct'),
    # The numeric check (Codex round 2, LOW 5): not "EVR II A 72, 1".
    'The National Library of Russia, Ms. EVR II A 0721': (['990001442050205171'], 1, 'direct'),
    # Authoritative library codes only (Codex rounds 3-4).
    'Sassoon, David Solomon, Ms. T-S K 08.026': NONE,
    'Vernadsky National Library of Ukraine, Ms. T-S K 08.026': NONE,
    'State Library of Berlin, Ms. or. qu. 0943': (['990001756390205171', '990001756460205171'], 2, 'direct'),
    'BnF Heb 01489.08': ([BNF_1489], 1, 'direct'),
    'BNF Heb 01489.08': ([BNF_1489], 1, 'direct'),
    'National Library of France Heb 01489.08': ([BNF_1489], 1, 'direct'),
    'The National Library of France, Ms. Heb 01489.08': ([BNF_1489], 1, 'direct'),
    'Bibliothèque nationale de France Heb 01489.08': ([BNF_1489], 1, 'direct'),
    # Today's answer for the unpadded input (a stub stores the short form).
    'The Bodleian Libraries, University of Oxford Arch. O.d. 08.028': (['990053385720205171'], 1, 'direct'),
    'GASTER 0967.03': NONE,
    # Exact today, unchanged.
    'F 01': (['990053942430205171'], 1, 'exact'),
    'GASTER 1201.17': (['990053540570205171'], 1, 'exact'),
    'T-S 12.123': (['990051334060205171'], 1, 'exact'),
    'Halper 381': (['990001961980205171'], 1, 'exact'),
    'T-S AS 31.1': (['990051828880205171'], 1, 'exact'),
    'T-S AS 3.11': (['990051762290205171'], 1, 'exact'),
    'Or. 10578R.98': (['990053532080205171'], 1, 'exact'),
    'T-S F 8.2': (['990026242400205171'], 1, 'exact'),
    'T-S F 8/002': (['990026242400205171'], 1, 'exact'),
    'T-S K 8.026': ([CUL_K826], 1, 'exact'),
    'Cambridge University Library, Ms. T-S K 8.26': ([CUL_K826], 1, 'exact'),
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


def _ids(r):
    return [r['sys_id']] if r.get('sys_id') else [o['sys_id'] for o in r.get('options') or []]


def _full(request):
    return os.environ.get('GENIZAH_SHELFMARK_FULL') == '1' or (request.config.option.markexpr or '').strip() == 'slow'


# ---------------------------------------------------------------- gate 3: golden

def test_golden_catalogue_forms(mm):
    batch = mm.resolve_catalog_forms(list(GOLDEN))
    for query, (want, total, tier) in GOLDEN.items():
        got = mm.resolve_catalog_form(query)
        assert (_ids(got), got['total'], got['tier']) == (want, total, tier), query
        assert got == batch[query], query
        if want:
            assert _ids(mm.resolve_system_by_shelfmark(query)) == want, query


def test_a_cap_family_reports_its_full_count(mm):
    r = mm.resolve_catalog_form('Gaster, Moses Collection, Ms. 1358')
    assert r['sys_id'] is None and len(r['options']) == 100 and r['total'] == 110


# ---------------------------------------------------------------- gate 5 drift: T1's exact conditions

def test_restated_exact_conditions_equal_todays_resolver(mm, tables):
    """The helper behind D0 and the exact stage, against answers recorded on master-main
    before this work (tests/fixtures/shelfmark_resolve_baseline.json)."""
    data = json.loads(BASELINE.read_text(encoding='utf-8'))
    entries = data['entries']
    queries = [e['query'] for e in entries]
    raw = cs.t1_exact_entries(queries, tables, mm.csv_bank)
    answers = mm.resolve_catalog_forms(queries, limit=10 ** 9)
    for e in entries:
        q, base = e['query'], e['response']
        exact_ids = {h['sys_id'] for h in raw[q]}
        assert bool(exact_ids) == e['exact'], q
        assert (answers[q]['tier'] == 'exact') == e['exact'], q
        if e['exact'] and base['sys_id']:
            assert exact_ids == {base['sys_id']}, q
        elif e['exact'] and len(base['options']) < e['limit']:
            assert exact_ids == set(base['options']), q


def test_d0_contains_todays_exact_answer_for_the_unpadded_input(mm, tables):
    rng = random.Random(7)
    sample = rng.sample([e[1] for e in tables.index if re.search(r'\d', e[1])], 3000)
    padded = [re.sub(r'\d+', lambda m: '0' + m.group(), v) for v in sample]
    plans = {q: cs._plan(q, tables, False) for q in padded}
    padded = [q for q in padded if plans[q].institution is None and not plans[q].no_t2]
    answers = cs.resolve_many(padded, tables, mm.csv_bank, limit=10 ** 9, with_exact=False)
    unpadded = cs.t1_exact_entries([cs.zs(q) for q in padded], tables, mm.csv_bank)
    checked = 0
    for q in padded:
        base = {h['sys_id'] for h in unpadded[cs.zs(q)] if cs.numbers(h['shelfmark']) == cs.numbers(q)}
        if base:
            checked += 1
            assert base <= set(_ids(answers[q])), q
    assert checked > 1000


# ---------------------------------------------------------------- gate 4: adversarial (slow)

def _adversarial_inputs(mm, tables, every):
    pad = lambda s: re.sub(r'\d+', lambda m: '0' + m.group(), s)  # noqa: E731
    seen, out, n = set(), [], 0
    for sid, d in mm.csv_bank.items():
        for v in d.get('call_numbers_raw') or [d.get('shelfmark', '')]:
            if not v:
                continue
            split = tables.split(v)
            cases = []
            if split:
                head, rest = split
                cases.append(('comma', f'{head}, Ms. {rest}'))
                if head.startswith('mosseri'):
                    cases.append(('no_comma', f"{head.replace(',', '')}, Ms. {rest}"))
                if re.search(r'\d', rest):
                    cases.append(('padded', f'{head}, Ms. {pad(rest)}'))
            elif re.search(r'\d', v):
                cases.append(('bare_padded', pad(v)))
            for kind, q in cases:
                if (kind, q, sid) in seen:
                    continue
                seen.add((kind, q, sid))
                n += 1
                if n % every == 0:
                    out.append((kind, q, sid))
    return out


def _unpadded_base(q, plan, exact_entries, csv_bank):
    base = {h['sys_id'] for h in exact_entries if cs.numbers(h['shelfmark']) == cs.numbers(q)}
    if plan.institution is not None:
        base = {s for s in base if cs._belongs(s, plan.institution, csv_bank)}
    return base


@pytest.mark.slow
def test_adversarial_catalogue_forms(mm, tables, request):
    full = _full(request)
    cases = _adversarial_inputs(mm, tables, 1 if full else 20)
    counts = collections.Counter()
    problems, cap_cases = [], []
    chunk = 50_000
    for start in range(0, len(cases), chunk):
        part = cases[start:start + chunk]
        queries = list(dict.fromkeys(q for _, q, _ in part))
        everything = mm.resolve_catalog_forms(queries, limit=10 ** 9)
        padded = [q for q in queries if cs.zs(q) != q and everything[q]['tier'] != 'exact']
        unpadded = cs.t1_exact_entries([cs.zs(q) for q in padded], tables, mm.csv_bank)
        for kind, q, sid in part:
            a = everything[q]
            if a['tier'] == 'exact':
                continue                     # T1 answers it today; T2 never runs
            got = set(_ids(a))
            status = ('correct' if got == {sid} else 'contains' if sid in got else
                      'wrong_unique' if len(got) == 1 else 'without_target' if got else 'empty')
            base = set()
            if q in padded:
                base = _unpadded_base(q, cs._plan(q, tables, False), unpadded[cs.zs(q)], mm.csv_bank)
            if status == 'wrong_unique' and base == got:
                status = 'unique_as_today_unpadded'   # the Oxford Arch. O.d. stubs
            counts[kind, status] += 1
            if status in ('wrong_unique', 'without_target'):
                problems.append((kind, status, q, sid, sorted(got)[:5]))
            if base and not base <= got:
                counts[kind, 'narrower'] += 1
                problems.append((kind, 'narrower', q, sorted(base)[:5], sorted(got)[:5]))
            if a['total'] > 100 and sid in got:
                cap_cases.append((kind, q, sid, [o['sys_id'] for o in a['options'][:100]]))
    # Returned lists are capped at 100: a target may be missing only when there are more
    # than 100 candidates, and then the list is the first 100 in today's order.
    capped = mm.resolve_catalog_forms(list(dict.fromkeys(q for _, q, _, _ in cap_cases)), limit=100)
    for kind, q, sid, first100 in cap_cases:
        c = capped[q]
        assert c['total'] > 100 and _ids(c) == first100, q
        if sid not in first100:
            counts[kind, 'beyond_cap'] += 1
    assert not problems, problems[:20]
    if full:
        floor = {'comma': 37_497, 'no_comma': 3_644, 'padded': 242_762, 'bare_padded': 449_580}
        for kind, want in floor.items():
            assert counts[kind, 'correct'] >= want * 0.995, (kind, counts[kind, 'correct'])
        assert sum(v for (k, s), v in counts.items() if s == 'beyond_cap') <= 46


@pytest.mark.slow
def test_simulated_nli_forms_recall(mm, tables, request):
    """The plan's seed-42 simulation: real records rewritten as the NLI catalogue writes them."""
    rng = random.Random(42)
    by_lib = collections.defaultdict(list)
    for sid, d in mm.csv_bank.items():
        by_lib[d.get('library_code', '')].append(sid)
    picks = []
    for lib, sids in by_lib.items():
        pool = []
        for sid in sids:
            for v in mm.csv_bank[sid].get('call_numbers_raw') or []:
                split = tables.split(v)
                if split:
                    pool.append((sid, split[0], split[1], lib, v))
                    break
        if pool:
            picks.extend(rng.sample(pool, min(max(5, round(3000 * len(pool) / 230000)), len(pool))))

    def pad2(x):
        nums = list(re.finditer(r'\d+', x))
        out, last = [], 0
        for i, m in enumerate(nums[:2]):
            out.extend([x[last:m.start()], m.group().zfill([2, 3][i])])
            last = m.end()
        out.append(x[last:])
        y = ''.join(out)
        return y if y != x else None

    cases = []
    for sid, head, rest, lib, v in picks:
        original_head = v[:len(head)]
        cases.append(('comma', f'{original_head}, Ms. {rest}', sid))
        padded = pad2(rest)
        if lib == 'CUL' and padded:
            cases.append(('cul_zero_padded', f'{original_head}, Ms. {padded}', sid))
        if lib == 'Mosseri':
            cases.append(('mosseri_paris', f'{original_head} Paris France Ms. {rest}', sid))
    answers = mm.resolve_catalog_forms(list(dict.fromkeys(q for _, q, _ in cases)))
    counts = collections.Counter()
    for kind, q, sid in cases:
        a = answers[q]
        counts[kind, 'n'] += 1
        if a['sys_id'] == sid:
            counts[kind, 'correct'] += 1
        elif a['sys_id']:
            counts[kind, 'wrong'] += 1
    assert counts['comma', 'n'] > 2500
    assert not any(v for (k, s), v in counts.items() if s == 'wrong'), counts
    assert counts['comma', 'correct'] / counts['comma', 'n'] >= 0.970, counts
    assert counts['cul_zero_padded', 'correct'] / counts['cul_zero_padded', 'n'] >= 0.975, counts
