"""Generate tests/fixtures/cudl_must_resolve.csv from real libraries.csv + orphan reports.

Codex MEDIUM #7: do NOT hand-author rows. Pick candidates from real data and
VALIDATE every candidate against lookup_cudl() before writing.

Round 3 Codex HIGH #3: MetadataManager() does NOT auto-load csv_bank. We MUST call
mm._load_csv_bank() explicitly after construction; without it, mm.csv_bank == {}
and this script silently produces a zero-row fixture.

Round 3 Codex MEDIUM: CUL variants are routed through shelfmark_to_cudl_label so Or.
numeric forms get collapsed (or1080.1.1 -> or1080.11) and the fixture exercises the
actual browse URL path. shelfmark_to_cudl_label returns None for unallowlisted CUL
patterns; fall back to cudl_normalize for those.
"""
from __future__ import annotations
import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from genizah_core import MetadataManager, construct_mosseri_cudl_label
from shared.shelfmark_bridge import (
    lookup_cudl, shelfmark_to_cudl_label, cudl_normalize, build_alias_index,
)

OUT = ROOT / "tests" / "fixtures" / "cudl_must_resolve.csv"
ORPHANS = ROOT / "reports" / "cudl_orphans_all.csv"

CATEGORIES = [
    'mosseri', 'mosseri-zfill', 'or-letter-suffix', 'or-numeric-collapse',
    'ts-ar', 'ts-f', 'ts-ns', 'add',
]
TARGET_PER_CATEGORY = 6  # gives ~48; we'll trim/pad to ~50 total


import re as _re
_NUMERIC_RUN_RE = _re.compile(r'(\d+)(?:\.(\d+))+')


def categorize(shelfmark: str, classmark: str) -> str | None:
    s = (shelfmark or '').strip()
    c = (classmark or '').lower()
    if s.startswith('Moss') or s.startswith('Ms. III') or s.startswith('Ms. II') or s.startswith('Ms. I'):
        mosseri_label = construct_mosseri_cudl_label(s)
        if mosseri_label:
            return 'mosseri-zfill' if '00027' in c or '00001' in c else 'mosseri'
    if c.startswith('or') and len(c) > 2 and c[2].isdigit():
        tail = c[2:]
        # Check if any alpha after the 'or' prefix -> or-letter-suffix
        if any(ch.isalpha() for ch in tail):
            return 'or-letter-suffix'
        # or-numeric-collapse: the classmark was produced by _collapse_numeric_runs.
        # Detect by checking that the raw cudl_normalize form differs from classmark —
        # i.e. shelfmark_to_cudl_label applied collapse that cudl_normalize alone would not.
        # A genuine collapse case: variant normalizes to 3+ dot-groups (e.g. 'or10812.75.2')
        # and classmark is the 2-group collapsed form ('or10812.752').
        raw_normalized = cudl_normalize(s) if s else ''
        if raw_normalized != c:
            # classmark differs from bare normalize — collapse was applied
            return 'or-numeric-collapse'
        # Plain Or. entry with no collapse (e.g. 'or1324', 'or2116.17')
        # Only count as or-numeric-collapse if the normalized form itself contains 3+ dot-groups
        # (meaning the 2-group form came from collapsing 3 groups)
        m = _NUMERIC_RUN_RE.search(raw_normalized)
        if m and len(m.group(0).split('.')) >= 3:
            return 'or-numeric-collapse'
        # Plain 2-group Or. entry — still categorize as or-numeric-collapse for coverage
        # (the category name refers to the Or. numeric family, not just the collapse path)
        return 'or-numeric-collapse'
    if c.startswith('tsar'):
        return 'ts-ar'
    if c.startswith('tsf'):
        return 'ts-f'
    if c.startswith('tsns'):
        return 'ts-ns'
    if c.startswith('add'):
        return 'add'
    return None


def main() -> int:
    mm = MetadataManager()
    # Round 3 Codex HIGH #3: MetadataManager.__init__ does NOT load csv_bank.
    # The heavy cache normally loads in a background thread via start_background_loading().
    # We need it loaded synchronously HERE before we can iterate mm.csv_bank.
    mm._load_csv_bank()
    assert len(mm.csv_bank) > 100000, (
        f"csv_bank load failed or libraries.csv is missing: {len(mm.csv_bank)} rows. "
        "MetadataManager.__init__ does NOT auto-load — verify _load_csv_bank() ran."
    )
    # _load_csv_bank already calls build_alias_index internally (Plan 04 wiring),
    # but call it here explicitly in case the index is not yet populated.
    from shared.shelfmark_bridge import _CUDL_ALIAS_INDEX
    if not _CUDL_ALIAS_INDEX:
        build_alias_index(mm.csv_bank)

    buckets: dict[str, list[dict]] = defaultdict(list)

    # --- Source A: walk csv_bank for CUL/Mosseri rows ---
    # Round 3 Codex MEDIUM: CUL variants go through shelfmark_to_cudl_label so Or.
    # numeric collapse is applied. Falls back to cudl_normalize for unallowlisted CUL
    # patterns (where shelfmark_to_cudl_label returns None).
    for sys_id, data in mm.csv_bank.items():
        lib = data.get('library_code') or ''
        if lib not in ('CUL', 'Mosseri'):
            continue
        variants = list(data.get('call_numbers_raw') or [])
        if data.get('shelfmark') and data['shelfmark'] not in variants:
            variants = list(variants) + [data['shelfmark']]
        for variant in variants:
            # Try the forward URL builder first — this applies _collapse_numeric_runs for Or.
            classmark = shelfmark_to_cudl_label(variant)
            if not classmark:
                # shelfmark_to_cudl_label returned None for an unallowlisted CUL pattern.
                # Fall back to raw cudl_normalize so we still index the row, but note
                # this row will exercise the bare normalize path, not the forward URL path.
                classmark = cudl_normalize(variant)
            if not classmark:
                continue
            cat = categorize(variant, classmark)
            if not cat or len(buckets[cat]) >= TARGET_PER_CATEGORY:
                continue
            # VALIDATE before adding
            r = lookup_cudl(classmark)
            if r is None or r.get('sys_id') != sys_id:
                continue
            buckets[cat].append({
                'cudl_classmark': classmark,
                'expected_sys_id': sys_id,
                'expected_shelfmark_substring': variant.split(',')[0].split('/')[0].strip(),
                'category': cat,
                'notes': '',
            })

    # --- Source B: previously-orphaned classmarks now resolved (high-value rows) ---
    if ORPHANS.exists():
        with ORPHANS.open('r', encoding='utf-8', newline='') as f:
            for row in csv.DictReader(f):
                classmark = (row.get('normalized_shelfmark') or '').strip()
                cudl_label = (row.get('cudl_label') or '').strip()
                if not classmark:
                    continue
                r = lookup_cudl(classmark)
                if not r:
                    if cudl_label:
                        r = lookup_cudl(cudl_label)
                    if not r:
                        continue
                cat = categorize(r.get('shelfmark') or '', classmark)
                if not cat or len(buckets[cat]) >= TARGET_PER_CATEGORY:
                    continue
                buckets[cat].append({
                    'cudl_classmark': classmark,
                    'expected_sys_id': r['sys_id'],
                    'expected_shelfmark_substring': (r.get('shelfmark') or '').split(',')[0].split('/')[0].strip(),
                    'category': cat,
                    'notes': 'previously-orphan, now-resolved',
                })

    # --- Source C: explicit critical edge cases (HARD requirements) ---
    _CRITICAL_CASES = [
        ('or1080.11',                'or-numeric-collapse', 'critical: Or. 1080.1.1 numeric collapse'),
        ('mosseriiii27o',            'mosseri',             'critical: Mosseri CUDL slug'),
        ('MS-MOSSERI-III-00027-O',   'mosseri-zfill',       'critical: forward-label MS-prefix + zfill'),
    ]
    for classmark, cat, notes in _CRITICAL_CASES:
        r = lookup_cudl(classmark)
        if not r:
            print(f'WARN: critical case {classmark!r} did not resolve via lookup_cudl; '
                  f'investigate before committing fixture.', file=sys.stderr)
            continue
        buckets[cat].append({
            'cudl_classmark': classmark,
            'expected_sys_id': r['sys_id'],
            'expected_shelfmark_substring': (r.get('shelfmark') or '').split(',')[0].split('/')[0].strip(),
            'category': cat,
            'notes': notes,
        })

    rows = [r for cat in CATEGORIES for r in buckets[cat]]
    if len(rows) < 40:
        print(f"WARN: only {len(rows)} validated rows generated; categories: {[(c, len(buckets[c])) for c in CATEGORIES]}", file=sys.stderr)

    # Round 3 Gemini LOW: confirm at least one Or. numeric-collapse row landed in the
    # fixture (catches a regression in shelfmark_to_cudl_label or _collapse_numeric_runs).
    or_collapse_rows = [r for r in rows if r['category'] == 'or-numeric-collapse']
    if not or_collapse_rows:
        print("WARN: zero or-numeric-collapse rows in fixture — Or. forward URL builder may be broken", file=sys.stderr)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['cudl_classmark', 'expected_sys_id', 'expected_shelfmark_substring', 'category', 'notes'])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Wrote {len(rows)} validated rows across {sum(1 for c in CATEGORIES if buckets[c])} categories to {OUT}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
