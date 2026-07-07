# -*- coding: utf-8 -*-
"""Resolve Birkat Hamazon witness-index shelfmarks -> sys_ids via libraries.csv.

Input:  data/bh_index_raw.json  (from dump_bh_index.py)
Output: data/bh_witnesses.json  (siglum -> shelfmarks -> sys_ids) + resolution report
"""
import csv
import json
import re
import sys
from collections import defaultdict

ROOT = r"C:\Genizahsearch"
RAW = ROOT + r"\same_work_spike\probe\data\bh_index_raw.json"
OUT = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
REPORT = ROOT + r"\same_work_spike\probe\data\bh_resolution_report.txt"


def normalize_shelfmark(shelfmark: str) -> str:
    """Copy of the canonical impl in shared/browse_map_utils.py (kept dependency-free)."""
    if not shelfmark:
        return ""
    temp = shelfmark.replace('/', '.')
    temp = re.sub(r'(\d)\.(\d)', r'\1DOTMARKER\2', temp)
    cleaned = re.sub(r'\W+', '', temp).casefold()
    cleaned = cleaned.replace('dotmarker', '.')
    if cleaned.startswith("ms"):
        cleaned = cleaned[2:]
    return cleaned


# Hebrew library heading -> allowed library codes (None = known-absent from corpus)
def lib_codes(library_he: str):
    s = library_he or ""
    if 'מוצרי' in s:
        return {'Mosseri'}
    if 'לואיס' in s or 'גיבסון' in s:
        return {'CUL'}
    if 'קימברידג' in s:
        return {'CUL'}
    if 'JTS' in s or 'ניו יורק' in s:
        return {'JTS'}
    if 'אוקספורד' in s:
        return {'Oxford'}
    if 'לונדון' in s:
        return {'BL'}
    if 'מנצ' in s:
        return {'Manchester'}
    if 'פטרבורג' in s:
        return {'RNL'}
    if 'ירושלים' in s:
        return {'NLI'}
    if 'פילדלפיה' in s:
        return {'Katz', 'Halper'}
    if 'כי"ח' in s or 'פריז' in s:
        return {'AIU'}
    if 'בודפשט' in s:
        return {'HAS'}      # Kaufmann collection, Hungarian Academy of Sciences
    if 'וינה' in s:
        return {'Vienna'}
    if 'סינסינטי' in s:
        return {'HUC'}
    return None  # Letchworth / Frankfurt etc. -> known-absent, no matching


RANGE_RE = re.compile(r'^(.*?)(\d+)\s*[-–]\s*(\d+)\s*$')
PAREN_RE = re.compile(r'\(([^)]*)\)')


def shelfmark_candidates(raw: str, codes):
    """Generate candidate strings to normalize+look up for one raw shelfmark."""
    raw = raw.strip()
    if not raw:
        return []
    cands = []
    # parenthetical alternatives: "OR. 10578R.106 (Gaster 1358.106)"
    parens = PAREN_RE.findall(raw)
    main = PAREN_RE.sub('', raw).strip()
    bases = [main] + [p.strip() for p in parens if re.search(r'\d', p or '')]
    for base in bases:
        # range expansion on trailing numeric range
        m = RANGE_RE.match(base)
        if m:
            prefix, a, b = m.group(1), int(m.group(2)), int(m.group(3))
            if b > a and (b - a) < 40:
                expanded = [f"{prefix}{n}" for n in range(a, b + 1)]
            else:
                expanded = [base]
        else:
            expanded = [base]
        for e in expanded:
            cands.append(e)
            if codes and 'NLI' in codes:
                cands.append("Heb. " + e)
            if codes and 'Mosseri' in codes:
                cands.append(e.replace(',', ' '))
                cands.append("Moss. " + e)
            # CUL "Or. 1080" series: CSV writes "Or.1080 15.4" (space after 1080)
            if re.match(r'(?i)or\.?\s*1080\.', e):
                cands.append(re.sub(r'(?i)(or\.?\s*1080)\.', r'\1 ', e, count=1))
            # trailing sub-leaf letter: "T-S K 6.40a" -> "T-S K 6.40"
            stripped = re.sub(r'(\d)[a-zA-Z]$', r'\1', e)
            if stripped != e:
                cands.append(stripped)
    # dedupe preserving order
    seen = set()
    out = []
    for c in cands:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def main():
    # --- build lookup from libraries.csv ---
    index = defaultdict(list)  # norm -> [(sys_id, code)]
    n_rows = 0
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) < 4 or not row[0]:
                continue
            n_rows += 1
            sys_id, call_numbers, code = row[0], row[2], row[3]
            for variant in call_numbers.split('|'):
                v = variant.strip()
                if not v:
                    continue
                v = re.sub(r'=\d+\s*$', '', v)  # NLI size marker "...=4"
                norm = normalize_shelfmark(v)
                if norm:
                    index[norm].append((sys_id, code))

    data = json.load(open(RAW, encoding='utf-8'))

    # --- walk table rows; empty siglum = continuation (join) of previous witness ---
    witnesses = {}  # siglum -> {'shelfmarks': [...]}
    order = []
    current = None
    for row in data['table_rows']:
        sig = row['siglum'].strip()
        if sig:
            current = sig
            if sig not in witnesses:
                witnesses[sig] = {'shelfmarks': []}
                order.append(sig)
        if current is None:
            continue
        witnesses[current]['shelfmarks'].append({
            'raw': row['shelfmark'],
            'library_he': row['library_he'],
            'source': 'table',
        })

    # --- tzerufim lines: "SHELFMARK <heb siglum>" ---
    tz_re = re.compile(r'^(.*?)\s+([֐-׿][֐-׿0-9\-–]*)\s*$')
    tz_added, tz_unparsed = 0, []
    for line in data['tzerufim_lines']:
        m = tz_re.match(line['line'])
        if not m:
            tz_unparsed.append(line['line'])
            continue
        shelf, sig = m.group(1).strip(), m.group(2).strip()
        target = witnesses.get(sig)
        if target is None:
            # ambiguous like "תג1-תג2" or siglum not in table -> its own group
            witnesses[sig] = {'shelfmarks': []}
            order.append(sig)
            target = witnesses[sig]
        existing = {normalize_shelfmark(s['raw']) for s in target['shelfmarks']}
        if normalize_shelfmark(shelf) not in existing:
            target['shelfmarks'].append({
                'raw': shelf, 'library_he': line['library_he'], 'source': 'tzerufim',
            })
            tz_added += 1

    # --- resolve ---
    stats = defaultdict(lambda: [0, 0])  # library_he -> [total, resolved]
    unresolved = []
    known_absent = []
    n_shelf_total = n_shelf_resolved = 0
    all_sys_ids = set()
    for sig in order:
        w = witnesses[sig]
        for s in w['shelfmarks']:
            codes = lib_codes(s['library_he'])
            if codes is not None and '1080' in s['raw']:
                codes = codes | {'CUL'}  # Or. 1080 is a CUL series even when listed under BL
            hits = []
            if codes is not None:
                cand_list = shelfmark_candidates(s['raw'], codes)
                for cand in cand_list:
                    norm = normalize_shelfmark(cand)
                    for sys_id, code in index.get(norm, []):
                        if code in codes and sys_id not in [h[0] for h in hits]:
                            hits.append((sys_id, code, cand))
                if not hits:
                    # prefix-scan fallback: codex-level entry -> its leaves
                    # (next char after the prefix must be non-digit: '.', letter)
                    for cand in cand_list:
                        norm = normalize_shelfmark(cand)
                        if len(norm) < 4:
                            continue
                        for k, entries in index.items():
                            if k.startswith(norm) and len(k) > len(norm) \
                                    and not k[len(norm)].isdigit():
                                for sys_id, code in entries:
                                    if code in codes and sys_id not in [h[0] for h in hits]:
                                        hits.append((sys_id, code, k))
                    if len(hits) > 40:
                        hits = hits[:40]  # cap codex explosion
            s['sys_ids'] = sorted({h[0] for h in hits})
            s['matched_codes'] = sorted({h[1] for h in hits})
            n_shelf_total += 1
            key = s['library_he'][:30]
            stats[key][0] += 1
            if s['sys_ids']:
                n_shelf_resolved += 1
                stats[key][1] += 1
                all_sys_ids.update(s['sys_ids'])
            elif codes is None:
                known_absent.append(f"{sig}: {s['raw']} [{s['library_he'][:25]}]")
            else:
                unresolved.append(f"{sig}: {s['raw']} [{s['library_he'][:25]}]")

    n_wit_resolved = sum(
        1 for sig in order
        if any(s['sys_ids'] for s in witnesses[sig]['shelfmarks'])
    )

    json.dump(
        {'witnesses': {sig: witnesses[sig] for sig in order}, 'order': order},
        open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=1,
    )

    lines = []
    lines.append(f"libraries.csv rows indexed: {n_rows}")
    lines.append(f"witnesses (sigla): {len(order)}")
    lines.append(f"witnesses with >=1 resolved sys_id: {n_wit_resolved}")
    lines.append(f"shelfmarks total: {n_shelf_total}, resolved: {n_shelf_resolved} "
                 f"({100 * n_shelf_resolved / max(1, n_shelf_total):.1f}%)")
    lines.append(f"distinct sys_ids: {len(all_sys_ids)}")
    lines.append(f"tzerufim lines added: {tz_added}, unparsed: {len(tz_unparsed)}")
    lines.append("\nper-library (total/resolved):")
    for k, (t, res) in sorted(stats.items(), key=lambda x: -x[1][0]):
        lines.append(f"  {k}: {t}/{res}")
    lines.append(f"\nKNOWN-ABSENT libraries ({len(known_absent)}):")
    lines.extend("  " + u for u in known_absent)
    lines.append(f"\nUNRESOLVED (expected-present libraries) ({len(unresolved)}):")
    lines.extend("  " + u for u in unresolved)
    if tz_unparsed:
        lines.append("\nUNPARSED tzerufim lines:")
        lines.extend("  " + u for u in tz_unparsed)
    open(REPORT, 'w', encoding='utf-8').write("\n".join(lines))
    print(f"witnesses={len(order)} resolved={n_wit_resolved} "
          f"shelfmarks={n_shelf_resolved}/{n_shelf_total} sys_ids={len(all_sys_ids)}")
    print(f"report: {REPORT}")


if __name__ == '__main__':
    main()
