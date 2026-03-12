"""
Fix 43K catalog records with NULL SourceName in fjms_enrichment.db.

These records come from FJMS personal handlists (SourceId=300), team entries (100),
site users (850), and a few other source types that had no mapping in dbo_CodeSource.

Maps each record via UnitCatalogRecId → FIST.db dbo_Signature → SourceId/SubId,
then assigns the correct handlist author name based on FJMS website verification.

SubId mapping (verified against FJMS website 2026-03-12):
  SourceId=300, SubId=120 → Chaim Milikowsky
  SourceId=300, SubId=51  → Ben Sasson Menahem
  SourceId=300, SubId=186 → Ben-Shammai Haggai
  SourceId=300, SubId=271 → Gregor Schwarb
  SourceId=300, SubId=1   → Emanuel Friedberg
  All others              → preliminary description
"""

import sqlite3
import shutil
import sys
import os
from datetime import datetime

ENRICHMENT_DB = "fist_data/fjms_enrichment.db"
FIST_DB = "fist_data/FIST.db"

# Verified mapping: (SourceId, SubId) → (SourceName_EN, SourceNameHeb)
HANDLIST_MAP = {
    (300, 120): ("Chaim Milikowsky, Personal Handlist", "חיים מיליקובסקי, רשימות אישיות"),
    (300, 51):  ("Ben Sasson Menahem, Personal Handlist", "בן ששון מנחם, רשימות אישיות"),
    (300, 186): ("Ben-Shammai Haggai, Personal Handlist", "בן-שמאי חגי, רשימות אישיות"),
    (300, 271): ("Gregor Schwarb, Personal Handlist", "גריגור שוורב, רשימות אישיות"),
    (300, 1):   ("Emanuel Friedberg, Personal Handlist", "עמנואל פרידברג, רשימות אישיות"),
}

PRELIMINARY_EN = "Personal handlist - preliminary"
PRELIMINARY_HE = "זיהוי ראשוני - טעון בדיקה חוזרת"


def main():
    dry_run = "--dry-run" in sys.argv

    if not os.path.exists(FIST_DB):
        print(f"ERROR: {FIST_DB} not found")
        sys.exit(1)

    # Backup
    if not dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = f"fist_data/fjms_enrichment_pre_handlist_fix_{ts}.db"
        print(f"Creating backup: {backup}")
        shutil.copy2(ENRICHMENT_DB, backup)

    conn_fist = sqlite3.connect(FIST_DB)
    cf = conn_fist.cursor()

    conn_enr = sqlite3.connect(ENRICHMENT_DB)
    ce = conn_enr.cursor()

    # Get all NULL SourceName records
    ce.execute("SELECT AlmaId, UnitCatalogRecId FROM catalog WHERE SourceName IS NULL")
    null_rows = ce.fetchall()
    print(f"NULL SourceName records: {len(null_rows):,}")

    # Cache UCR → (SourceId, SubId)
    ucr_cache = {}
    for _, ucr_id in null_rows:
        if ucr_id not in ucr_cache:
            cf.execute("""SELECT s.SourceId, s.SubId FROM dbo_Signature s
                         JOIN dbo_UnitCatalogRec ucr ON ucr.SignatureId = s.SignatureId
                         WHERE ucr.UnitCatalogRecId = ?""", (ucr_id,))
            ucr_cache[ucr_id] = cf.fetchone()

    # Build updates
    named_counts = {}
    prelim_count = 0
    unmapped = 0
    updates = []

    for alma, ucr_id in null_rows:
        result = ucr_cache.get(ucr_id)
        if not result:
            unmapped += 1
            continue

        source_id, sub_id = result
        key = (source_id, sub_id)

        if key in HANDLIST_MAP:
            name_en, name_he = HANDLIST_MAP[key]
            label = name_en.split(",")[0]
            named_counts[label] = named_counts.get(label, 0) + 1
        else:
            name_en = PRELIMINARY_EN
            name_he = PRELIMINARY_HE
            prelim_count += 1

        updates.append((name_en, name_he, alma, ucr_id))

    # Report
    print(f"\nMapping results:")
    for name, cnt in sorted(named_counts.items(), key=lambda x: -x[1]):
        print(f"  {name}: {cnt:,}")
    print(f"  preliminary: {prelim_count:,}")
    print(f"  unmapped: {unmapped:,}")
    print(f"  TOTAL: {len(updates):,}")

    if dry_run:
        print("\n--dry-run: no changes made")
        conn_fist.close()
        conn_enr.close()
        return

    # Apply updates
    print(f"\nApplying {len(updates):,} updates...")
    batch_size = 5000
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        ce.executemany(
            "UPDATE catalog SET SourceName = ?, SourceNameHeb = ? WHERE AlmaId = ? AND UnitCatalogRecId = ?",
            batch
        )
        conn_enr.commit()
        print(f"  {min(i + batch_size, len(updates)):,}/{len(updates):,}")

    # Verify
    ce.execute("SELECT COUNT(*) FROM catalog WHERE SourceName IS NULL")
    remaining = ce.fetchone()[0]
    print(f"\nRemaining NULL SourceName: {remaining:,}")

    # Show final distribution
    ce.execute("""SELECT SourceName, COUNT(*) FROM catalog
                  WHERE SourceName LIKE '%Handlist%' OR SourceName LIKE '%preliminary%'
                  GROUP BY SourceName ORDER BY COUNT(*) DESC""")
    print("\nNew handlist SourceName distribution:")
    for name, cnt in ce.fetchall():
        print(f"  {name}: {cnt:,}")

    conn_fist.close()
    conn_enr.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
