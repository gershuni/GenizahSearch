#!/usr/bin/env python3
"""
מייצר רשימה מאוחדת של כל חילופי האותיות (1↔1 ו-2↔1)
ממוינת לפי שכיחות - לשימוש עם סליידר וריאנטים

שימוש:
    python generate_unified_variants.py char_merges_report.xlsx --output unified_variants.py
"""

import argparse
import sys
from pathlib import Path


def load_from_excel(filepath: str) -> list:
    """טוען נתונים מקובץ אקסל"""
    try:
        import openpyxl
    except ImportError:
        print("נדרש openpyxl: pip install openpyxl")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath)
    ws = wb.active

    data = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] and row[1] and row[3]:
            source = str(row[0]).strip()
            target = str(row[1]).strip()
            count = int(row[3])
            sub_type = str(row[2]).strip() if row[2] else ''
            data.append((source, target, count, sub_type))

    return data


def merge_bidirectional(data: list) -> list:
    """
    מאחד חילופים דו-כיווניים
    """
    merged = {}

    for source, target, count, sub_type in data:
        # מפתח קנוני - ממוין לפי אורך (הארוך קודם) ואז אלפביתית
        if len(source) > len(target):
            key = (source, target)
        elif len(target) > len(source):
            key = (target, source)
        else:
            key = tuple(sorted([source, target]))

        if key in merged:
            merged[key]['count'] += count
        else:
            merged[key] = {
                'pair': key,
                'count': count,
                'type': sub_type
            }

    # ממיר לרשימה וממיין לפי שכיחות
    result = []
    for key, val in merged.items():
        result.append((key[0], key[1], val['count']))

    result.sort(key=lambda x: x[2], reverse=True)
    return result


def generate_python_code(merged_data: list, max_pairs: int = None) -> str:
    """
    מייצר קובץ Python עם רשימה מאוחדת
    """
    if max_pairs:
        merged_data = merged_data[:max_pairs]

    code = []
    code.append('"""')
    code.append('Unified Hebrew variant pairs - sorted by frequency')
    code.append('Generated from V0.7 vs V0.8 HTR comparison')
    code.append('')
    code.append('Format: (source, target, frequency)')
    code.append('  - 1↔1 pairs: single character substitutions')
    code.append('  - 2↔1 pairs: two characters merge to one (or vice versa)')
    code.append('')
    code.append('Usage: Use top N pairs based on user slider setting')
    code.append('"""')
    code.append('')
    code.append(f'# Total pairs: {len(merged_data)}')
    code.append('')
    code.append('UNIFIED_VARIANT_PAIRS = [')

    for i, (source, target, count) in enumerate(merged_data):
        pair_type = '1↔1' if len(source) == 1 and len(target) == 1 else f'{len(source)}↔{len(target)}'
        code.append(f"    ('{source}', '{target}', {count}),  # {i+1}. {pair_type}")

    code.append(']')
    code.append('')
    code.append('# Quick lookup: pairs without frequency')
    code.append('VARIANT_PAIRS_ONLY = [(s, t) for s, t, _ in UNIFIED_VARIANT_PAIRS]')
    code.append('')
    code.append('def get_top_pairs(n: int) -> list:')
    code.append('    """Get top N variant pairs by frequency."""')
    code.append('    return VARIANT_PAIRS_ONLY[:n]')
    code.append('')
    code.append('def get_pairs_above_frequency(min_freq: int) -> list:')
    code.append('    """Get all pairs with frequency >= min_freq."""')
    code.append('    return [(s, t) for s, t, f in UNIFIED_VARIANT_PAIRS if f >= min_freq]')

    return '\n'.join(code)


def print_summary(merged_data: list):
    """מדפיס סיכום"""
    single = [x for x in merged_data if len(x[0]) == 1 and len(x[1]) == 1]
    multi = [x for x in merged_data if len(x[0]) != 1 or len(x[1]) != 1]

    print("\n" + "=" * 60)
    print("סיכום חילופים מאוחדים (לפי שכיחות)")
    print("=" * 60)
    print(f"  חילופי 1↔1: {len(single)} זוגות")
    print(f"  חילופי רב-תוויים: {len(multi)} זוגות")
    print(f"  סה\"כ: {len(merged_data)} זוגות")

    print("\n--- Top 30 (כל הסוגים מעורבים) ---")
    print(f"{'#':<4} {'מקור':<6} {'יעד':<6} {'שכיחות':<10} {'סוג'}")
    print("-" * 40)
    for i, (src, tgt, cnt) in enumerate(merged_data[:30], 1):
        pair_type = '1↔1' if len(src) == 1 and len(tgt) == 1 else f'{len(src)}↔{len(tgt)}'
        print(f"{i:<4} {src:<6} {tgt:<6} {cnt:<10} {pair_type}")


def main():
    parser = argparse.ArgumentParser(description='יצירת רשימת וריאנטים מאוחדת')
    parser.add_argument('input', help='קובץ קלט (xlsx מתוצאות analyze_char_merges)')
    parser.add_argument('--output', '-o', default='unified_variants.py',
                        help='קובץ פלט')
    parser.add_argument('--max', type=int, default=None,
                        help='מספר מקסימלי של זוגות לכלול')
    parser.add_argument('--min-freq', type=int, default=10,
                        help='שכיחות מינימלית לכלול (ברירת מחדל: 10)')

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"שגיאה: הקובץ {args.input} לא נמצא")
        sys.exit(1)

    print(f"טוען נתונים מ: {args.input}")
    data = load_from_excel(args.input)
    print(f"  נטענו {len(data)} רשומות")

    # סינון לפי שכיחות מינימלית
    data = [x for x in data if x[2] >= args.min_freq]
    print(f"  אחרי סינון (≥{args.min_freq}): {len(data)} רשומות")

    print("מאחד חילופים דו-כיווניים...")
    merged = merge_bidirectional(data)
    print(f"  אחרי איחוד: {len(merged)} זוגות ייחודיים")

    print_summary(merged)

    # יצירת קוד
    code = generate_python_code(merged, args.max)

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(code)

    print(f"\nקובץ Python נשמר ב: {args.output}")


if __name__ == '__main__':
    main()
