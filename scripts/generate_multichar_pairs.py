#!/usr/bin/env python3
"""
סקריפט לעיבוד תוצאות חילופי אותיות ויצירת רשימות מוכנות לקוד
קורא את קובץ האקסל ומייצר רשימות Python מאוחדות וממוינות
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

def load_from_excel(filepath: str) -> list:
    """טוען נתונים מקובץ אקסל"""
    try:
        import openpyxl
    except ImportError:
        print("נדרש openpyxl: pip install openpyxl")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath)
    ws = wb.active  # גיליון הסיכום

    data = []
    for row in ws.iter_rows(min_row=2, values_only=True):  # דילוג על כותרות
        if row[0] and row[1] and row[3]:  # מקור, יעד, שכיחות
            source = str(row[0]).strip()
            target = str(row[1]).strip()
            count = int(row[3])
            sub_type = str(row[2]).strip() if row[2] else ''
            data.append((source, target, count, sub_type))

    return data

def load_from_csv(filepath: str) -> list:
    """טוען נתונים מקובץ CSV"""
    import csv

    data = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader)  # דילוג על כותרות
        for row in reader:
            if len(row) >= 4:
                source = row[0].strip()
                target = row[1].strip()
                count = int(row[3])
                sub_type = row[2].strip() if len(row) > 2 else ''
                data.append((source, target, count, sub_type))

    return data

def merge_bidirectional(data: list) -> list:
    """
    מאחד חילופים דו-כיווניים
    אם יש גם A→B וגם B→A, מאחד לזוג אחד עם סכום השכיחויות
    """
    merged = {}

    for source, target, count, sub_type in data:
        # יוצר מפתח קנוני (ממוין אלפביתית)
        key = tuple(sorted([source, target]))

        if key in merged:
            merged[key]['count'] += count
        else:
            merged[key] = {
                'pair': (source, target),
                'count': count,
                'type': sub_type
            }

    # ממיר חזרה לרשימה
    result = []
    for key, val in merged.items():
        # שומר את הזוג בסדר: הקצר קודם (2 אותיות לפני 1)
        a, b = key
        if len(a) > len(b):
            pair = (a, b)
        else:
            pair = (b, a)
        result.append((pair[0], pair[1], val['count']))

    # מיון לפי שכיחות יורדת
    result.sort(key=lambda x: x[2], reverse=True)
    return result

def generate_python_code(merged_data: list, thresholds: dict) -> str:
    """
    מייצר קוד Python מוכן להכנסה ל-genizah_core.py
    """
    basic = []
    extended = []
    maximum = []

    for source, target, count in merged_data:
        pair_str = f"('{source}', '{target}')"

        if count >= thresholds['basic']:
            basic.append((pair_str, count))
        elif count >= thresholds['extended']:
            extended.append((pair_str, count))
        elif count >= thresholds['maximum']:
            maximum.append((pair_str, count))

    code = []
    code.append("# === MULTICHAR PAIR DEFINITIONS ===")
    code.append("# Generated from V0.7 vs V0.8 comparison analysis")
    code.append("")

    code.append(f"# Basic: High-frequency merges (≥{thresholds['basic']} occurrences)")
    code.append("_BASIC_MULTICHAR_PAIRS = [")
    for pair, count in basic:
        code.append(f"    {pair},  # {count}")
    code.append("]")
    code.append("")

    code.append(f"# Extended: Medium-frequency merges ({thresholds['extended']}-{thresholds['basic']-1} occurrences)")
    code.append("_EXTENDED_MULTICHAR_PAIRS = [")
    for pair, count in extended:
        code.append(f"    {pair},  # {count}")
    code.append("]")
    code.append("")

    code.append(f"# Maximum: Lower-frequency merges ({thresholds['maximum']}-{thresholds['extended']-1} occurrences)")
    code.append("_MAXIMUM_MULTICHAR_PAIRS = [")
    for pair, count in maximum:
        code.append(f"    {pair},  # {count}")
    code.append("]")

    return '\n'.join(code)

def print_summary(merged_data: list, thresholds: dict):
    """מדפיס סיכום"""
    basic = [x for x in merged_data if x[2] >= thresholds['basic']]
    extended = [x for x in merged_data if thresholds['extended'] <= x[2] < thresholds['basic']]
    maximum = [x for x in merged_data if thresholds['maximum'] <= x[2] < thresholds['extended']]
    below = [x for x in merged_data if x[2] < thresholds['maximum']]

    print("\n" + "="*60)
    print("סיכום חילופים רב-תוויים (אחרי איחוד)")
    print("="*60)
    print(f"  BASIC (≥{thresholds['basic']}):     {len(basic)} זוגות")
    print(f"  EXTENDED ({thresholds['extended']}-{thresholds['basic']-1}): {len(extended)} זוגות")
    print(f"  MAXIMUM ({thresholds['maximum']}-{thresholds['extended']-1}):  {len(maximum)} זוגות")
    print(f"  מתחת לסף:        {len(below)} זוגות (לא נכללים)")
    print(f"  סה\"כ:            {len(merged_data)} זוגות ייחודיים")

    print("\n--- Top 20 BASIC ---")
    for src, tgt, cnt in basic[:20]:
        print(f"  {src} ↔ {tgt}: {cnt}")

def main():
    parser = argparse.ArgumentParser(description='עיבוד תוצאות חילופי אותיות')
    parser.add_argument('input', help='קובץ קלט (xlsx או csv)')
    parser.add_argument('--output', '-o', default='multichar_pairs.py',
                        help='קובץ פלט לקוד Python')
    parser.add_argument('--basic', type=int, default=1000,
                        help='סף לרמת BASIC (ברירת מחדל: 1000)')
    parser.add_argument('--extended', type=int, default=500,
                        help='סף לרמת EXTENDED (ברירת מחדל: 500)')
    parser.add_argument('--maximum', type=int, default=100,
                        help='סף לרמת MAXIMUM (ברירת מחדל: 100)')

    args = parser.parse_args()

    # טעינת נתונים
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"שגיאה: הקובץ {args.input} לא נמצא")
        sys.exit(1)

    print(f"טוען נתונים מ: {args.input}")
    if input_path.suffix == '.xlsx':
        data = load_from_excel(args.input)
    else:
        data = load_from_csv(args.input)

    print(f"  נטענו {len(data)} רשומות")

    # איחוד
    print("מאחד חילופים דו-כיווניים...")
    merged = merge_bidirectional(data)
    print(f"  אחרי איחוד: {len(merged)} זוגות ייחודיים")

    # סיכום
    thresholds = {
        'basic': args.basic,
        'extended': args.extended,
        'maximum': args.maximum
    }
    print_summary(merged, thresholds)

    # יצירת קוד
    code = generate_python_code(merged, thresholds)

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(code)

    print(f"\nקוד Python נשמר ב: {args.output}")
    print("אפשר להעתיק את התוכן ל-genizah_core.py")

if __name__ == '__main__':
    main()
