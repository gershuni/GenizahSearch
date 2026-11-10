#!/usr/bin/env python3
"""
סקריפט להכנת קורפוס - מפשיט קבצי טקסט לרשימת מילים עבריות בלבד

שימוש:
    python prepare_corpus.py /path/to/corpus_folder --output corpus_words.txt

הפלט: קובץ עם מילה אחת בכל שורה (ללא כפילויות)
"""

import argparse
import re
import sys
import unicodedata
from pathlib import Path
from typing import Set


def normalize_hebrew(text: str) -> str:
    """נרמול טקסט עברי - הסרת ניקוד וסימנים"""
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text


def extract_hebrew_words(text: str, min_len: int = 2) -> Set[str]:
    """חילוץ מילים עבריות מטקסט"""
    text = normalize_hebrew(text)
    words = re.findall(r'[\u0590-\u05FF]+', text)
    return set(w for w in words if len(w) >= min_len)


def process_corpus(corpus_path: str, min_word_len: int = 2) -> Set[str]:
    """
    עובר על כל קבצי הטקסט בתיקייה ומחלץ מילים עבריות
    """
    corpus_dir = Path(corpus_path)
    all_words = set()

    if not corpus_dir.exists():
        print(f"שגיאה: התיקייה {corpus_path} לא נמצאה")
        return all_words

    # מוצא את כל קבצי הטקסט
    text_files = list(corpus_dir.glob('*.txt')) + list(corpus_dir.glob('**/*.txt'))
    print(f"נמצאו {len(text_files)} קבצי טקסט")

    for i, txt_file in enumerate(text_files):
        if i % 100 == 0:
            print(f"  מעבד: {i}/{len(text_files)} ({len(all_words)} מילים עד כה)", end='\r')

        try:
            with open(txt_file, 'r', encoding='utf-8-sig') as f:
                text = f.read()
            words = extract_hebrew_words(text, min_word_len)
            all_words.update(words)
        except Exception as e:
            print(f"\n  שגיאה בקריאת {txt_file}: {e}")

    print(f"\n  סה\"כ: {len(all_words)} מילים ייחודיות")
    return all_words


def main():
    parser = argparse.ArgumentParser(description='הכנת קורפוס מילים עבריות')
    parser.add_argument('corpus', help='תיקיית הקורפוס')
    parser.add_argument('--output', '-o', default='corpus_words.txt',
                        help='קובץ פלט (ברירת מחדל: corpus_words.txt)')
    parser.add_argument('--min-len', type=int, default=2,
                        help='אורך מילה מינימלי (ברירת מחדל: 2)')
    parser.add_argument('--stats', action='store_true',
                        help='הצג סטטיסטיקות על המילים')

    args = parser.parse_args()

    print(f"מעבד קורפוס: {args.corpus}")
    words = process_corpus(args.corpus, args.min_len)

    if not words:
        print("לא נמצאו מילים!")
        sys.exit(1)

    # שמירה לקובץ
    sorted_words = sorted(words)
    with open(args.output, 'w', encoding='utf-8') as f:
        for word in sorted_words:
            f.write(word + '\n')

    print(f"\nנשמר לקובץ: {args.output}")
    print(f"  {len(words)} מילים ייחודיות")

    if args.stats:
        print("\n--- סטטיסטיקות ---")
        by_len = {}
        for w in words:
            l = len(w)
            by_len[l] = by_len.get(l, 0) + 1

        for length in sorted(by_len.keys()):
            print(f"  אורך {length}: {by_len[length]} מילים")


if __name__ == '__main__':
    main()
