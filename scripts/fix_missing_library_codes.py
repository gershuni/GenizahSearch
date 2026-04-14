#!/usr/bin/env python3
"""
Fix missing library codes in libraries.csv

Analyzes records without library codes and assigns them based on patterns
in the call_numbers field.
"""

import csv
from collections import Counter

# Patterns to detect libraries - order matters (more specific first)
LIBRARY_PATTERNS = [
    # Major libraries
    ('Tel Aviv University', 'TAU', 'Tel Aviv University Library'),
    ('University of Haifa', 'Haifa', 'University of Haifa Library'),
    ('Senckenberg', 'Senckenberg', 'University Library Johann Christian Senckenberg (Frankfurt)'),
    ('Birmingham', 'Birmingham', 'University of Birmingham Library'),
    ('Heidelberg', 'Heidelberg', 'Heidelberg University Library'),
    ('Columbia', 'Columbia', 'Columbia University Library'),
    ('Ben Zvi', 'BenZvi', 'Ben Zvi Institute'),
    ('Ben-Zvi', 'BenZvi', 'Ben Zvi Institute'),
    ('Sassoon', 'Sassoon', 'Sassoon Collection'),
    ('Wallach', 'Wallach', 'Wallach Collection'),
    ('Lutzki', 'Lutzki', 'Lutzki Collection'),
    ('Adler', 'Adler', 'Adler Collection'),

    # European libraries
    ('Library of Geneva', 'Geneva', 'Library of Geneva'),
    ('Bavarian State Library', 'Munich', 'Bavarian State Library (Munich)'),
    ('National Library of France', 'BNF', 'National Library of France'),
    ('Russian State Library', 'RSL', 'Russian State Library'),

    # US libraries
    ('University of Chicago', 'UChicago', 'University of Chicago Library'),
    ('University of Toronto', 'Toronto', 'University of Toronto Library'),
    ('University of Michigan', 'UMich', 'University of Michigan Library'),
    ('McGill University', 'McGill', 'McGill University Library'),
    ('Duke University', 'Duke', 'Duke University Libraries'),
    ('Yale University', 'Yale', 'Yale University Library'),
    ('Yeshiva University', 'YU', 'Yeshiva University Library'),
    ('Princeton', 'Princeton', 'Princeton University Library'),

    # Israeli institutions
    ('Schocken Institute', 'Schocken', 'Schocken Institute for Jewish Research'),
    ('Bar-Ilan University', 'BarIlan', 'Bar-Ilan University Library'),

    # Collections and foundations
    ('Lehmann Foundation', 'Lehmann', 'Manfred and Anne Lehmann Foundation'),
    ('Jewish Community of Berlin', 'JCBerlin', 'Jewish Community of Berlin'),
    ('Jewish Community of Erfurt', 'JCErfurt', 'Jewish Community of Erfurt'),
    ('Allony-Loewinger', 'AllonyLoew', 'Allony-Loewinger Catalogue'),
    ('Allony-Kupfer', 'AllonyKupf', 'Allony-Kupfer Catalogue'),
    ('Benayahu', 'Benayahu', 'Benayahu Collection'),
    ('Nahum, Yehuda', 'Nahum', 'Yehuda Nahum Collection'),
    ('Salmon, Chava', 'Salmon', 'Chava Salmon Collection'),
    ('Sofer, David', 'Sofer', 'David Sofer Collection'),
    ('Sofer David', 'Sofer', 'David Sofer Collection'),
    ('Shapira, Bernard', 'Shapira', 'Bernard Shapira Collection'),
    ('Weiss, Steve', 'Weiss', 'Steve Weiss Collection'),
    ('Karp, Abraham', 'Karp', 'Abraham Karp Collection'),

    # UK libraries
    ('Trinity College Dubl', 'TCD', 'Trinity College Dublin'),
    ('Leeds University', 'Leeds', 'Leeds University Library'),
    ("Chetham's Library", 'Chetham', "Chetham's Library (Manchester)"),
    ('Wellcome Library', 'Wellcome', 'Wellcome Library'),

    # Other institutions
    ('Oriental Studies Library of the Depar', 'Turin', 'Oriental Studies Library, Turin'),
    ('Museum of the Bible', 'MotB', 'Museum of the Bible'),
    ('Basel University', 'Basel', 'Basel University Library'),
    ('Oriental Manuscripts, the Russian Aca', 'IOM', 'Institute of Oriental Manuscripts (St. Petersburg)'),
    ('Goldsmith Museum', 'Goldsmith', 'Goldsmith Museum'),
    ('Separated Orthodox Society', 'SOS', 'Separated Orthodox Society'),
    ('National Library of Israel', 'NLI', 'National Library of Israel'),
    ('State Library of Be', 'SBB', 'State Library of Berlin'),
    ('Steinschneider, Berlin', 'SBB', 'State Library of Berlin'),

    # Russian collections (Guenzburg)
    ('Guenzburg', 'RSL', 'Russian State Library'),
]


def detect_library(call_numbers: str) -> tuple:
    """Detect library from call_numbers field.
    Returns (code, full_name) or (None, None) if not detected."""
    cn_lower = call_numbers.lower()

    for pattern, code, full_name in LIBRARY_PATTERNS:
        if pattern.lower() in cn_lower:
            return code, full_name

    return None, None


def main():
    input_file = 'libraries.csv'
    output_file = 'libraries_fixed.csv'

    total = 0
    fixed = 0
    still_missing = []
    library_counts = Counter()

    rows = []

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        rows.append(header)

        for row in reader:
            total += 1
            lib_code = row[3].strip() if len(row) > 3 else ''

            if not lib_code:
                call_nums = row[2] if len(row) > 2 else ''
                detected_code, detected_name = detect_library(call_nums)

                if detected_code:
                    # Pad row if needed
                    while len(row) < 4:
                        row.append('')
                    row[3] = detected_code
                    fixed += 1
                    library_counts[detected_code] += 1
                else:
                    still_missing.append((row[0], call_nums[:80]))

            rows.append(row)

    # Write output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    # Report
    print(f"Total records: {total}")
    print(f"Fixed: {fixed}")
    print(f"Still missing: {len(still_missing)}")
    print()

    print("Library codes assigned:")
    for code, count in library_counts.most_common():
        print(f"  {count:4d}: {code}")

    if still_missing:
        print()
        print("Still missing library codes:")
        for sys_id, call_nums in still_missing[:20]:
            print(f"  {sys_id}: {call_nums}")
        if len(still_missing) > 20:
            print(f"  ... and {len(still_missing) - 20} more")

    print()
    print(f"Output written to: {output_file}")
    print("To apply: copy libraries_fixed.csv to libraries.csv")


if __name__ == '__main__':
    main()
