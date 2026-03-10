"""
Fix shelfmark-to-sys_id mismatches in libraries.csv.

Problem: ~11,000 records have a single sys_id mapped to multiple genuinely
different shelfmarks (e.g., sys_id X mapped to both "EVR ARAB I 100" and
"EVR ARAB I 1681"). The correct mapping is determined from the NLI crossref
database which has authoritative sys_id -> shelfmark data.

Strategy:
1. For each record with multiple truly-different shelfmarks:
   a. Look up the sys_id in NLI crossref to get the correct shelfmark
   b. Keep only the matching variant(s), remove the rest
   c. For each removed (orphaned) shelfmark, check if it has its own
      correct sys_id in NLI -- if so, ensure a record exists for it
2. Produce a detailed report of all changes
3. Write a corrected libraries.csv

Usage:
    python scripts/fix_shelfmark_sysid_mismatch.py [--dry-run] [--apply]

By default runs in dry-run mode and produces a report.
Use --apply to write the corrected libraries.csv.
"""
import csv
import re
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

PROJECT_DIR = Path(__file__).resolve().parent.parent
LIBRARIES_CSV = PROJECT_DIR / "libraries.csv"
NLI_CROSSREF_DB = PROJECT_DIR / "nli_data" / "nli_crossref.db"
REPORT_FILE = PROJECT_DIR / "shelfmark_mismatch_report.txt"
OUTPUT_CSV = PROJECT_DIR / "libraries_fixed.csv"

# Library name prefixes to strip when comparing shelfmarks
LIBRARY_PREFIXES = [
    "The National Library of Russia ",
    "Cambridge University Library ",
    "Jewish Theological Seminary ",
    "British Library ",
    "Bodleian Libraries ",
    "University of Manchester ",
    "Alliance Israélite Universelle ",
    "Alliance Israelite Universelle ",
    "Hungarian Academy of Sciences ",
    "Hebrew Union College ",
    "Freer Gallery of Art ",
    "University of Toronto ",
    "University of Haifa ",
    "Schocken Institute ",
    "Ben-Zvi Institute ",
    "University of Heidelberg ",
    "University of Geneva ",
    "University of Birmingham ",
    "University of Chicago ",
    "University of Pennsylvania ",
    "Senckenberg University Library ",
    "Austrian National Library ",
    "National Library of France ",
    "Bavarian State Library ",
    "Berlin State Library ",
    "Russian State Library ",
    "The Library ",  # Manchester prefix
    "The University of Manchester Library ",
    "Library of the Hungarian Academy of Sciences ",
    "Hebrew Union College Library ",
    "Mosseri, Jacques ",
    "Freer Gallery of Art, ",
    "Rainer, Ferdinand ",
    "Rainer Collection ",
]


# Sort longest-first so "Hebrew Union College Library " matches before "Hebrew Union College "
LIBRARY_PREFIXES.sort(key=len, reverse=True)


def strip_lib_prefix(variant):
    """Strip library name prefix from a shelfmark variant."""
    v = variant.strip()
    for prefix in LIBRARY_PREFIXES:
        if v.startswith(prefix):
            v = v[len(prefix):]
            break
    return v.strip()


def norm(s):
    """Normalize a shelfmark for comparison. Single canonical function."""
    s = s.lower().strip()
    # Strip "ms." and "ms " prefix
    s = re.sub(r"^ms\.?\s*", "", s)
    # Normalize Yevr/EVR to a common form
    s = s.replace("yevr.", "evr").replace("yevr", "evr")
    # Normalize punctuation: dots, dashes, commas, slashes -> space
    s = re.sub(r"[.\-,]+", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def extract_series_and_number(shelfmark):
    """
    Extract the series prefix and trailing number from a shelfmark.
    Returns (series, number) tuple.

    Examples:
        "EVR ARAB I 100"     -> ("evr arab i", "100")
        "Yevr.-Arab. I 1681" -> ("evr arab i", "1681")
        "Or. 10599"          -> ("or", "10599")
        "Gaster Ms. 1448"    -> ("gaster", "1448")
        "Ms. B 2141"         -> ("b", "2141")
    """
    s = norm(shelfmark)
    # Try to split into series + number
    m = re.match(r"^(.+?)\s+(\d+(?:/\d+)?)$", s)
    if m:
        return m.group(1), m.group(2)
    return s, ""


def same_series(core1, core2):
    """Check if two shelfmarks are from the same series (same prefix, possibly different number)."""
    s1, n1 = extract_series_and_number(core1)
    s2, n2 = extract_series_and_number(core2)
    return s1 == s2 and n1 and n2 and n1 != n2


def group_by_series(cores):
    """
    Group shelfmark cores by their series prefix.
    Returns dict of series -> list of (core, number) tuples.
    """
    groups = defaultdict(list)
    for core in cores:
        series, number = extract_series_and_number(core)
        groups[series].append((core, number))
    return groups


def has_same_series_conflicts(cores):
    """
    Check if any cores share the same series but have different numbers.
    This indicates a true mismatch (e.g., "EVR ARAB I 100" vs "EVR ARAB I 1681").
    Different series (e.g., "Or. 10599" vs "Gaster Ms. 1448") are valid aliases.
    """
    groups = group_by_series(cores)
    for series, items in groups.items():
        numbers = set(n for _, n in items if n)
        if len(numbers) > 1:
            return True
    return False


def load_nli_crossref():
    """Load NLI crossref data as ground truth."""
    if not NLI_CROSSREF_DB.exists():
        print(f"ERROR: NLI crossref DB not found at {NLI_CROSSREF_DB}")
        sys.exit(1)

    conn = sqlite3.connect(str(NLI_CROSSREF_DB))
    cur = conn.cursor()

    cur.execute(
        "SELECT DISTINCT NLI_AlmaId, Shelfmark FROM nli_images WHERE NLI_AlmaId != ''"
    )
    by_sysid = defaultdict(set)
    by_norm = {}  # normalized shelfmark -> sys_id
    for alma_id, shelf in cur.fetchall():
        by_sysid[alma_id].add(shelf)
        by_norm[norm(shelf)] = alma_id

    conn.close()

    authoritative = {k: list(v)[0] for k, v in by_sysid.items() if len(v) == 1}

    print(f"NLI crossref loaded:")
    print(f"  Total sys_ids: {len(by_sysid)}")
    print(f"  Authoritative (1 shelfmark): {len(authoritative)}")
    print(f"  Ambiguous (2+ shelfmarks): {len(by_sysid) - len(authoritative)}")
    print(f"  Normalized index entries: {len(by_norm)}")

    return authoritative, by_sysid, by_norm


def load_libraries_csv():
    """Load libraries.csv and return header + rows."""
    with open(LIBRARIES_CSV, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    return header, rows


def match_variants_to_nli(cores_to_variants, nli_shelfmarks):
    """
    Given CSV cores and NLI shelfmark(s), find which CSV variant(s) match.
    Returns (matching_variants, orphaned_variants).
    """
    nli_norms = {norm(s) for s in nli_shelfmarks}
    matching = []
    orphaned = []

    for core, full_variants in cores_to_variants.items():
        core_norm = norm(core)
        if core_norm in nli_norms:
            matching.extend(full_variants)
        else:
            # Also check if core matches NLI by series+number
            core_series, core_num = extract_series_and_number(core)
            matched = False
            for nli_s in nli_shelfmarks:
                nli_series, nli_num = extract_series_and_number(nli_s)
                if core_series == nli_series and core_num == nli_num:
                    matched = True
                    break
            if matched:
                matching.extend(full_variants)
            else:
                orphaned.extend(full_variants)

    return matching, orphaned


def main():
    apply_mode = "--apply" in sys.argv
    dry_run = not apply_mode

    if dry_run:
        print("=== DRY RUN MODE (use --apply to write changes) ===\n")
    else:
        print("=== APPLY MODE - will write corrected CSV ===\n")

    authoritative, nli_by_sysid, nli_by_norm = load_nli_crossref()
    header, rows = load_libraries_csv()
    print(f"\nLibraries.csv: {len(rows)} records")

    # Build index of existing sys_ids
    existing_sysids = set()
    for row in rows:
        if row:
            existing_sysids.add(row[0])

    stats = defaultdict(int)
    by_library = defaultdict(int)
    report_lines = []
    new_rows = []
    modified_rows = {}  # index -> new call_numbers value

    for idx, row in enumerate(rows):
        if len(row) < 4:
            continue

        sys_id = row[0]
        calls = row[2]
        lib = row[3]
        title = row[7] if len(row) > 7 else ""

        variants = [v.strip() for v in calls.split("|")]

        # Group by core shelfmark (without library prefix)
        cores_to_variants = defaultdict(list)
        for v in variants:
            core = strip_lib_prefix(v)
            cores_to_variants[core].append(v)

        cores = set(cores_to_variants.keys())

        # Only fix records where cores from the SAME series have different numbers
        if not has_same_series_conflicts(cores):
            continue

        stats["conflicts_found"] += 1

        # Look up NLI
        nli_shelves = nli_by_sysid.get(sys_id, set())
        if not nli_shelves:
            stats["no_nli_data"] += 1
            report_lines.append(
                f"NO NLI DATA [{lib}] sys_id={sys_id}:\n"
                f"  CSV cores: {cores}\n"
            )
            continue

        # Check if NLI is ambiguous for this sys_id
        if len(nli_shelves) > 1:
            stats["nli_ambiguous"] += 1
            report_lines.append(
                f"AMBIGUOUS [{lib}] sys_id={sys_id}:\n"
                f"  CSV cores: {cores}\n"
                f"  NLI shelves: {nli_shelves}\n"
            )
            continue

        # NLI authoritative - match variants
        nli_shelf = list(nli_shelves)[0]
        matching, orphaned = match_variants_to_nli(cores_to_variants, nli_shelves)

        if not matching:
            stats["no_variant_matched"] += 1
            report_lines.append(
                f"NO MATCH [{lib}] sys_id={sys_id}:\n"
                f"  CSV cores: {sorted(cores)}\n"
                f"  NLI shelf: {nli_shelf}\n"
                f"  CSV norms: {sorted(norm(c) for c in cores)}\n"
                f"  NLI norm:  {norm(nli_shelf)}\n"
            )
            continue

        if not orphaned:
            # All variants matched NLI — no conflict after all
            stats["false_positive"] += 1
            continue

        # We have a fix: keep matching, remove orphaned
        stats["fixed"] += 1
        by_library[lib] += 1

        new_calls = " | ".join(matching)
        modified_rows[idx] = new_calls

        report_lines.append(
            f"FIXED [{lib}] sys_id={sys_id}:\n"
            f"  BEFORE: {calls}\n"
            f"  AFTER:  {new_calls}\n"
            f"  NLI:    {nli_shelf}\n"
            f"  Title:  {title}"
        )

        # Handle orphaned shelfmarks
        for orphan_variant in orphaned:
            orphan_core = strip_lib_prefix(orphan_variant)
            orphan_norm = norm(orphan_core)
            orphan_sysid = nli_by_norm.get(orphan_norm)

            if orphan_sysid:
                stats["orphans_with_sysid"] += 1
                if orphan_sysid in existing_sysids:
                    stats["orphans_already_exist"] += 1
                    report_lines.append(
                        f"  ORPHAN: '{orphan_core}' -> already exists as sys_id={orphan_sysid}"
                    )
                else:
                    stats["orphans_new_record"] += 1
                    new_row = [""] * max(len(header), 8)
                    new_row[0] = orphan_sysid
                    new_row[2] = orphan_variant
                    new_row[3] = lib
                    new_rows.append(new_row)
                    existing_sysids.add(orphan_sysid)
                    report_lines.append(
                        f"  ORPHAN: '{orphan_core}' -> NEW RECORD sys_id={orphan_sysid}"
                    )
            else:
                stats["orphans_no_sysid"] += 1
                report_lines.append(
                    f"  ORPHAN: '{orphan_core}' -> no NLI sys_id found"
                )

        report_lines.append("")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Same-series conflicts found:    {stats['conflicts_found']}")
    print(f"Fixed via NLI:                  {stats['fixed']}")
    print(f"  No NLI data:                  {stats['no_nli_data']}")
    print(f"  NLI ambiguous:                {stats['nli_ambiguous']}")
    print(f"  No variant matched NLI:       {stats['no_variant_matched']}")
    print(f"  False positive (all matched): {stats['false_positive']}")
    print(f"\nOrphaned shelfmarks:")
    print(f"  Have own NLI sys_id:          {stats['orphans_with_sysid']}")
    print(f"    Already in CSV:             {stats['orphans_already_exist']}")
    print(f"    New records needed:          {stats['orphans_new_record']}")
    print(f"  No NLI sys_id:                {stats['orphans_no_sysid']}")
    print(f"\nNew records to add:             {len(new_rows)}")

    print(f"\nFixes by library:")
    for lib, count in sorted(by_library.items(), key=lambda x: -x[1]):
        print(f"  {lib}: {count}")

    # Write report
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("Shelfmark-SysID Mismatch Report\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total fixes: {stats['fixed']}\n")
        f.write(f"New records: {len(new_rows)}\n")
        f.write(f"Manual review: {stats['no_nli_data'] + stats['nli_ambiguous'] + stats['no_variant_matched']}\n\n")
        f.write("\n".join(report_lines))

    print(f"\nReport written to: {REPORT_FILE}")

    if apply_mode:
        with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for idx, row in enumerate(rows):
                if idx in modified_rows:
                    row = list(row)
                    row[2] = modified_rows[idx]
                writer.writerow(row)
            for new_row in new_rows:
                writer.writerow(new_row)

        print(f"Corrected CSV written to: {OUTPUT_CSV}")
        print(f"\nTo apply: review the report, then rename libraries_fixed.csv -> libraries.csv")
    else:
        print(f"\nRun with --apply to write corrected CSV")


if __name__ == "__main__":
    main()
