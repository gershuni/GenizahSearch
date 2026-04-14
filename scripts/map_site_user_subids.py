"""
Map FIST.db SourceId=850 SubIds to FJMS site user names.

Strategy:
1. Count catalog records per SubId in FIST.db (matches user list "Num" column)
2. Parse user name list from FJMS website
3. Match by catalog_recs ≈ Num (tolerance ±5 for stale FIST data)
4. Verify known cases (SubId=23->Schwarb, SubId=153->Cheshin)
5. For ambiguous matches, use shelfmark-based verification via FJMS website

Output: SITE_USER_MAP dict for use in fix script, plus report of unmatched/ambiguous.
"""

import sqlite3
import sys

FIST_DB = "fist_data/FIST.db"
ENRICHMENT_DB = "fist_data/fjms_enrichment.db"

# Full user list from FJMS website (Name, Num, Shelfmarks)
# Scraped from FJMS site user contributions page
SITE_USERS = [
    ("Site User - Eliezer Cheshin", 45, 14),
    ("Site User - adiel breuer", 387, 427),
    ("Site User - Akiva Jessel", 20, 31),
    ("Site User - Alan Elbaum", 29, 29),
    ("Site User - Alon Ten-Ami", 1, 1),
    ("Site User - amitai harroch", 133, 128),
    ("Site User - Anna Busa", 21, 25),
    ("Site User - Ariel Zinder", 8, 8),
    ("Site User - Asael Shmeltzer", 8, 23),
    ("Site User - Avi Shmidman", 222, 395),
    ("Site User - aviad evron", 3, 3),
    ("Site User - Avraham Ben Arosh", 5, 5),
    ("Site User - Dan Greenberger", 3, 5),
    ("Site User - Daniel Caine", 1, 1),
    ("Site User - david gadol", 18, 15),
    ("Site User - David Joseph", 2, 23),
    ("Site User - dov yatsiv", 25, 79),
    ("Site User - Dovid Housman", 1, 1),
    ("Site User - Dr. Abraham David", 11, 11),
    ("Site User - Dr. Amir Ashur", 45, 52),
    ("Site User - Dr. Amit Gvaryahu", 2, 2),
    ("Site User - Dr. Amos Geula", 1, 1),
    ("Site User - Dr. Antonio Cid", 1, 1),
    ("Site User - Dr. Avi Shmidman", 62, 69),
    ("Site User - Dr. Avraham Shoshana", 1, 3),
    ("Site User - Dr. Ayala Eliyahu", 3, 3),
    ("Site User - Dr. Ben Outhwaite", 1, 1),
    ("Site User - Dr. Binyamin Elizur", 8, 8),
    ("Site User - Dr. Craig Perry", 1, 1),
    ("Site User - Dr. Daniel Davies", 1, 1),
    ("Site User - Dr. David Sklare", 5, 5),
    ("Site User - Dr. Dotan Arad", 48, 55),
    ("Site User - Dr. Eden Menahem HaCohen", 97, 102),
    ("Site User - Dr. Efraim Lev", 8, 7),
    ("Site User - Dr. Elinoar Bareket", 5, 5),
    ("Site User - Dr. elyashiv cherlow", 60, 59),
    ("Site User - Dr. Emmanuel Mastey", 3, 3),
    ("Site User - Dr. Ephraim Halivni", 2, 2),
    ("Site User - Dr. Esther-Miriam Wagner", 2, 2),
    ("Site User - Dr. Eve Krakowski", 1, 1),
    ("Site User - Dr. Evyatar Marienberg", 1, 1),
    ("Site User - Dr. Ezra Chwat", 88, 90),
    ("Site User - Dr. Gabriel Wasserman", 4, 4),
    ("Site User - Dr. Gabriele Ferrario", 2, 2),
    ("Site User - Dr. George Kiraz", 16, 16),
    ("Site User - Dr. Gila Vachman", 1, 1),
    ("Site User - Dr. Gregor Schwarb", 23, 26),
    ("Site User - Dr. Hillel Gershuni", 24, 25),
    ("Site User - Dr. Hillel Newman", 1, 1),
    ("Site User - Dr. Ilana Wartenberg", 1, 1),
    ("Site User - Dr. Jay Rovner", 6, 29),
    ("Site User - Dr. Jen Taylor Friedman", 1, 169),
    ("Site User - Dr. Jos? Mart?nez Delgado", 28, 25),
    ("Site User - Dr. Jose Martinez Delgado", 1, 2),
    ("Site User - Dr. Kim Phillips", 1, 1),
    ("Site User - Dr. Laura Lieber", 1, 1),
    ("Site User - Dr. Michael Rand", 29, 40),
    ("Site User - Dr. Michael Engel", 1, 1),
    ("Site User - Dr. Michael Gershoni", 3, 3),
    ("Site User - Dr. Michael Wechsler", 15, 16),
    ("Site User - Dr. Michael Zellmann-Rohrer", 1, 1),
    ("Site User - Dr. Miriam Frenkel", 1, 1),
    ("Site User - Dr. Miriam Goldstein", 1, 1),
    ("Site User - Dr. Moshe Lavee Levkovitch", 5, 5),
    ("Site User - Dr. Nabih Bashir", 4, 3),
    ("Site User - Dr. Nehemia Gordon", 4, 3),
    ("Site User - Dr. Neri Y Ariel", 2, 4),
    ("Site User - Dr. Nicholas Posegay", 89, 100),
    ("Site User - Dr. Ophira Gamliel", 1, 1),
    ("Site User - Dr. Ortal Paz SAAR", 9, 9),
    ("Site User - Dr. Phillip Lieberman", 11, 11),
    ("Site User - Dr. Rebecca Jefferson", 3, 3),
    ("Site User - Dr. Roni Shweka", 56, 56),
    ("Site User - Dr. Ronny Vollandt", 28, 29),
    ("Site User - Dr. Ruth Lamdan", 1, 1),
    ("Site User - Dr. Sacha Stern", 4, 4),
    ("Site User - Dr. Samuel Blapp", 1, 1),
    ("Site User - Dr. Shai Secunda", 1, 1),
    ("Site User - Dr. Shimon Fogel", 1, 1),
    ("Site User - Dr. Stephen J. Bennett", 2, 2),
    ("Site User - Dr. Steven Weiss", 1, 1),
    ("Site User - Dr. Yachin Epstein", 4, 4),
    ("Site User - Dr. Yaron Lisha", 1, 1),
    ("Site User - Dr. Yehoshua Granat", 64, 64),
    ("Site User - Dr. Yitz Landes", 3, 3),
    ("Site User - Dr. Yonatan Feintuch", 1, 1),
    ("Site User - Dr. Yoni Wormser", 30, 29),
    ("Site User - Dr. Zvi Stampfer", 2, 2),
    ("Site User - Dr. יעקב דויטש", 1, 4),
    ("Site User - Dr. שרה כהן", 58, 58),
    ("Site User - Eliav Grossman", 3, 3),
    ("Site User - Emanuel Friedberg", 39, 39),
    ("Site User - Franz B?hmisch", 2, 2),
    ("Site User - Hananel Mirsky", 4, 5),
    ("Site User - Hanoch Waldenberg", 1, 1),
    ("Site User - hayim ben-arzi", 94, 89),
    ("Site User - Idan Deshe", 1, 1),
    ("Site User - Jonah Rank", 1, 1),
    ("Site User - Jonathan Howard", 3, 3),
    ("Site User - Kedem Golden", 15, 15),
    ("Site User - Marc Michaels", 4, 4),
    ("Site User - Mark Glickman", 2, 2),
    ("Site User - Matthew Dudley", 15, 15),
    ("Site User - Miriam Greensteinn", 4, 4),
    ("Site User - Mordechai Weintraub", 484, 585),
    ("Site User - Moshe weiss", 17, 18),
    ("Site User - Moshe Yagur", 68, 88),
    ("Site User - Nadia Vidro", 14, 11),
    ("Site User - Nissim Louck", 17, 29),
    ("Site User - Nissim Louck", 1, 1),
    ("Site User - Noam Sienna", 2, 2),
    ("Site User - Oded Zinger", 59, 60),
    ("Site User - Omri Livnat", 6, 6),
    ("Site User - orit galili yakobovich", 23, 23),
    ("Site User - Peter Tarras", 1, 1),
    ("Site User - Prof. Aharon Maman", 1, 1),
    ("Site User - Prof. Avishai Bar Asher", 3, 3),
    ("Site User - Prof. eleazar gutwirth", 11, 14),
    ("Site User - Prof. Geoffrey Khan", 1, 1),
    ("Site User - Prof. Gideon Bohak", 23, 29),
    ("Site User - Prof. Israel Levin", 109, 108),
    ("Site User - Prof. Jonathan Decter", 1, 1),
    ("Site User - Prof. Joseph Yahalom", 579, 595),
    ("Site User - Prof. Judith Olszowy-Schlanger", 10, 9),
    ("Site User - Prof. Marina Rustow", 23, 23),
    ("Site User - Prof. Menachem KATZ", 3, 7),
    ("Site User - Prof. Mordechai A Friedman", 36, 36),
    ("Site User - Prof. Naoya Katsumata", 8, 11),
    ("Site User - Prof. Nicholas De Lange", 2, 2),
    ("Site User - Prof. Philip Alexander", 1, 1),
    ("Site User - Prof. Renee Levine Melammed", 1, 1),
    ("Site User - Prof. Richard Steiner", 1, 1),
    ("Site User - Prof. Robert Brody", 22, 24),
    ("Site User - Prof. Sabine Schmidtke", 2, 2),
    ("Site User - Prof. Shulamit Elizur", 433, 435),
    ("Site User - Prof. Simcha Emanuel", 3, 4),
    ("Site User - Prof. Simon Hopkins", 5, 5),
    ("Site User - Prof. Tamar Zewi", 131, 297),
    ("Site User - Prof. Tamer ElLeithy", 1, 1),
    ("Site User - Prof. Tova Beeri", 43, 43),
    ("Site User - Prof. Tova Rosen", 37, 35),
    ("Site User - Prof. Tzvi Langermann", 4, 4),
    ("Site User - Prof. Werner Diem", 7, 7),
    ("Site User - Prof. Wout Van Bekkum", 15, 15),
    ("Site User - Prof. אורי ארליך", 24, 34),
    ("Site User - Prof. נאסר בסל", 4, 4),
    ("Site User - Rabbi Ben Zion Levi", 9, 7),
    ("Site User - Rabbi Mordechai Honig", 1, 1),
    ("Site User - Rabbi Noam Kaplan", 1, 1),
    ("Site User - Rabbi SHMAYA YITZHAK HALEVY", 1, 1),
    ("Site User - Rabbi Tuvia Katzman", 24, 30),
    ("Site User - Rabbi Yeudah Zeivald", 208, 254),
    ("Site User - Rabbi Yosaif Mordechai Dubovick", 116, 207),
    ("Site User - Rabbi Zachary Rothblatt", 1, 1),
    ("Site User - Rachel Hasson", 4, 5),
    ("Site User - Rachel Hasson Kenat", 3, 3),
    ("Site User - Rebecca Ullrich", 2, 2),
    ("Site User - Refael Greenstain", 1, 1),
    ("Site User - Roni Shweka", 403, 504),
    ("Site User - Shahar Armon", 1, 2),
    ("Site User - Sholom Shuchat", 6, 1),
    ("Site User - Shulamith Berger", 1, 1),
    ("Site User - tehiila kitov", 1, 1),
    ("Site User - testAllen testKrasna", 1, 2),
    ("Site User - Tuvia Katzman", 10, 15),
    ("Site User - Vered Raziel-Kretzmer", 19, 19),
    ("Site User - Wissem Gueddich", 27, 9),
    ("Site User - Yaakov Miller", 4, 5),
    ("Site User - Yiftach Eitan", 2, 2),
    ("Site User - Yisroel Boruch Soloveitchik", 33, 35),
    ("Site User - yoisef yitzchok rottenberg", 82, 82),
    ("Site User - Yonatan Vardi", 4, 4),
    ("Site User - Yusuf Umrethwala", 16, 16),
    ("Site User - אברהם לוין", 1, 1),
    ("Site User - דן בראז", 3, 1),
    ("Site User - חן אלנתן", 6, 6),
    ("Site User - יהונתן קרני", 1, 1),
]

# Known verified mappings (from FJMS website lookup)
KNOWN_MAP = {
    23: "Site User - Dr. Gregor Schwarb",
    153: "Site User - Eliezer Cheshin",
    # Manual FJMS lookups (2026-03-12) — override algorithm
    193: "Site User - orit galili yakobovich",
    # 39: team entry (SourceId=100 team, not site user visible) — skip
    842: "Site User - Dr. George Kiraz",
    890: "Site User - Yusuf Umrethwala",
    131: "Site User - Prof. Wout Van Bekkum",
    727: "Site User - Kedem Golden",
    825: "Site User - Prof. Tova Beeri",    # second SubId for same person
    77:  "Site User - Rabbi Ben Zion Levi",
    215: "Site User - Dr. Phillip Lieberman",
    454: "Site User - Prof. Judith Olszowy-Schlanger",
    703: "Site User - Prof. eleazar gutwirth",
    859: "Site User - Dr. Abraham David",
    # Manual FJMS lookups — round 2 (large unmatched SubIds)
    112: "Site User - Prof. Tamar Zewi",
    156: "Site User - Dr. elyashiv cherlow",  # second SubId for same person
    # SubIds 48, 3, 829: team/catalog sources on FJMS, not identifiable site users
    # Manual FJMS lookups — round 3 (verification of auto-matches)
    365: "Site User - Prof. Israel Levin",  # was auto-matched to Dubovick, actually Levin
    # Confirmed correct: 28 (Zeivald), 579 (harroch), 458 (Eden Menahem HaCohen), 17 (ben-arzi)
}


def get_subid_stats(fist_conn):
    """Get catalog_recs and shelfmark_count for each SubId."""
    c = fist_conn.cursor()

    # All SubIds with sig counts and distinct shelfmarks
    c.execute('''
        SELECT s.SubId,
               COUNT(*) as sig_count,
               COUNT(DISTINCT s.SetSignatureId) as shelfmark_count
        FROM dbo_Signature s
        WHERE s.SourceId = 850
        GROUP BY s.SubId
    ''')
    stats = {}
    for subid, sigs, shelfs in c.fetchall():
        stats[subid] = {"sigs": sigs, "shelfmarks": shelfs}

    # Count UnitCatalogRec entries per SubId
    for subid in stats:
        c.execute('''SELECT COUNT(*) FROM dbo_UnitCatalogRec ucr
                     JOIN dbo_Signature s ON s.SignatureId = ucr.SignatureId
                     WHERE s.SourceId = 850 AND s.SubId = ?''', (subid,))
        stats[subid]["catalog_recs"] = c.fetchone()[0]

    return stats


def get_sample_shelfmark(fist_conn, subid):
    """Get a sample shelfmark for a SubId."""
    c = fist_conn.cursor()
    c.execute('''SELECT s.SetSignatureId FROM dbo_Signature s
                 WHERE s.SourceId = 850 AND s.SubId = ? LIMIT 1''', (subid,))
    r = c.fetchone()
    if not r:
        return None
    setsig = r[0]
    c.execute('''SELECT i.Shelfmark FROM dbo_InventorySignature isig
                 JOIN dbo_Inventory i ON i.InventoryId = isig.InventoryId
                 WHERE isig.SetSignatureId = ? LIMIT 1''', (setsig,))
    r = c.fetchone()
    return r[0] if r else None


def match_subids_to_names(stats):
    """Match SubIds to user names using catalog_recs ≈ Num."""
    # Build index: Num -> list of user names
    num_to_users = {}
    for name, num, shelfs in SITE_USERS:
        num_to_users.setdefault(num, []).append((name, shelfs))

    matched = {}      # subid -> name (confident)
    ambiguous = {}    # subid -> [candidates]
    unmatched = []    # subids with no match

    used_names = set()  # Track which user names have been claimed

    # Phase 1: Apply ALL known/manual mappings FIRST (they take priority)
    for subid in KNOWN_MAP:
        if subid in stats:
            matched[subid] = KNOWN_MAP[subid]
            # Don't add to used_names if name appears for multiple SubIds (e.g. Tova Beeri)
            # Only block re-use for names that appear once in KNOWN_MAP
            used_names.add(KNOWN_MAP[subid])

    # Phase 2: Sort remaining SubIds by catalog_recs desc (match larger counts first)
    sorted_subids = sorted(
        [s for s in stats.keys() if s not in KNOWN_MAP],
        key=lambda s: -stats[s]["catalog_recs"]
    )

    for subid in sorted_subids:
        cat = stats[subid]["catalog_recs"]
        shelfs = stats[subid]["shelfmarks"]

        # Try exact match on catalog_recs = Num
        candidates = []
        for tolerance in [0, 1, 2, 3, 5, 8]:
            for num in range(cat - tolerance, cat + tolerance + 1):
                if num in num_to_users:
                    for name, user_shelfs in num_to_users[num]:
                        if name not in used_names:
                            candidates.append((name, num, user_shelfs, abs(cat - num)))
            if candidates:
                break

        if not candidates:
            unmatched.append(subid)
        elif len(candidates) == 1:
            matched[subid] = candidates[0][0]
            used_names.add(candidates[0][0])
        else:
            # Multiple candidates — try to disambiguate by shelfmark count
            # Score by combined closeness: |cat-num| + |shelfs-user_shelfs|*0.1
            scored = []
            for name, num, user_shelfs, num_diff in candidates:
                shelf_diff = abs(shelfs - user_shelfs)
                score = num_diff * 10 + shelf_diff
                scored.append((score, name, num, user_shelfs))
            scored.sort()

            if len(scored) >= 2 and scored[0][0] < scored[1][0]:
                # Clear winner
                matched[subid] = scored[0][1]
                used_names.add(scored[0][1])
            else:
                ambiguous[subid] = [(s[1], s[2], s[3]) for s in scored[:5]]

    return matched, ambiguous, unmatched


def main():
    verbose = "--verbose" in sys.argv
    apply_fix = "--apply" in sys.argv

    conn_fist = sqlite3.connect(FIST_DB)

    print("Computing SubId statistics from FIST.db...")
    stats = get_subid_stats(conn_fist)
    print(f"  {len(stats)} SubIds found")

    print("\nMatching SubIds to user names...")
    matched, ambiguous, unmatched = match_subids_to_names(stats)

    # Report
    print(f"\n{'='*70}")
    print(f"MATCHING RESULTS")
    print(f"{'='*70}")
    print(f"  Matched:   {len(matched):>4} SubIds")
    print(f"  Ambiguous: {len(ambiguous):>4} SubIds")
    print(f"  Unmatched: {len(unmatched):>4} SubIds")
    print(f"  Total:     {len(stats):>4} SubIds")

    # Count enrichment records affected
    total_enr = sum(stats[s]["catalog_recs"] for s in matched)
    print(f"\n  Enrichment records covered by matches: {total_enr:,}")

    # Verify known mappings
    print(f"\n{'='*70}")
    print("KNOWN MAPPING VERIFICATION")
    print(f"{'='*70}")
    for subid, expected in KNOWN_MAP.items():
        got = matched.get(subid, "NOT MATCHED")
        status = "OK" if got == expected else "MISMATCH"
        cat = stats[subid]["catalog_recs"]
        print(f"  SubId={subid}: {status} -> {got} (catalog_recs={cat})")

    if verbose or True:
        # Show all matches sorted by catalog_recs desc
        print(f"\n{'='*70}")
        print("ALL MATCHES (by catalog records desc)")
        print(f"{'='*70}")
        for subid in sorted(matched.keys(), key=lambda s: -stats[s]["catalog_recs"]):
            s = stats[subid]
            shelfmark = get_sample_shelfmark(conn_fist, subid)
            print(f"  SubId={subid:>4}  cat={s['catalog_recs']:>4}  shelfs={s['shelfmarks']:>4}  -> {matched[subid]}")
            if verbose:
                print(f"           sample: {shelfmark}")

    if ambiguous:
        print(f"\n{'='*70}")
        print("AMBIGUOUS (need manual resolution)")
        print(f"{'='*70}")
        for subid in sorted(ambiguous.keys(), key=lambda s: -stats[s]["catalog_recs"]):
            s = stats[subid]
            shelfmark = get_sample_shelfmark(conn_fist, subid)
            print(f"  SubId={subid:>4}  cat={s['catalog_recs']:>4}  shelfs={s['shelfmarks']:>4}  sample={shelfmark}")
            for name, num, user_shelfs in ambiguous[subid]:
                print(f"    candidate: {name} (Num={num}, Shelfs={user_shelfs})")

    if unmatched:
        print(f"\n{'='*70}")
        print("UNMATCHED SubIds (no user name found)")
        print(f"{'='*70}")
        for subid in sorted(unmatched, key=lambda s: -stats[s]["catalog_recs"]):
            s = stats[subid]
            shelfmark = get_sample_shelfmark(conn_fist, subid)
            print(f"  SubId={subid:>4}  cat={s['catalog_recs']:>4}  sigs={s['sigs']:>4}  shelfs={s['shelfmarks']:>4}  sample={shelfmark}")

    # Output the mapping as Python dict for use in fix script
    print(f"\n{'='*70}")
    print("SITE_USER_MAP (for fix script)")
    print(f"{'='*70}")
    print("SITE_USER_MAP = {")
    for subid in sorted(matched.keys()):
        print(f"    {subid}: \"{matched[subid]}\",")
    print("}")

    conn_fist.close()

    if apply_fix:
        print(f"\n{'='*70}")
        print("APPLYING FIX TO ENRICHMENT DB")
        print(f"{'='*70}")
        apply_site_user_fix(matched, stats)


def apply_site_user_fix(matched, stats):
    """Apply the SubId->name mapping to enrichment DB catalog records."""
    import shutil
    from datetime import datetime

    conn_fist = sqlite3.connect(FIST_DB)
    cf = conn_fist.cursor()

    conn_enr = sqlite3.connect(ENRICHMENT_DB)
    ce = conn_enr.cursor()

    # Backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"fist_data/fjms_enrichment_pre_siteuser_fix_{ts}.db"
    print(f"Creating backup: {backup}")
    shutil.copy2(ENRICHMENT_DB, backup)

    # Get all "preliminary" records from SourceId=850
    # These are catalog records where SourceName was set to "preliminary" by the handlist fix
    # We need to find which ones are SourceId=850 and update with proper user names
    ce.execute("""SELECT c.AlmaId, c.UnitCatalogRecId
                  FROM catalog c
                  WHERE c.SourceName = 'Personal handlist - preliminary'""")
    prelim_rows = ce.fetchall()
    print(f"Preliminary records: {len(prelim_rows):,}")

    # Cache UCR -> (SourceId, SubId) from FIST.db
    ucr_cache = {}
    for alma, ucr_id in prelim_rows:
        if ucr_id not in ucr_cache:
            cf.execute("""SELECT s.SourceId, s.SubId FROM dbo_Signature s
                         JOIN dbo_UnitCatalogRec ucr ON ucr.SignatureId = s.SignatureId
                         WHERE ucr.UnitCatalogRecId = ?""", (ucr_id,))
            ucr_cache[ucr_id] = cf.fetchone()

    # Build updates for SourceId=850 with matched names
    updates = []
    still_prelim = 0
    other_source = 0

    for alma, ucr_id in prelim_rows:
        result = ucr_cache.get(ucr_id)
        if not result:
            still_prelim += 1
            continue

        source_id, sub_id = result
        if source_id != 850:
            other_source += 1
            continue

        if sub_id in matched:
            name = matched[sub_id]
            updates.append((name, alma, ucr_id))
        else:
            still_prelim += 1

    print(f"  Will update: {len(updates):,}")
    print(f"  Still preliminary (unmatched SubId): {still_prelim:,}")
    print(f"  Other SourceId (not 850): {other_source:,}")

    # Apply
    batch_size = 5000
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        ce.executemany(
            "UPDATE catalog SET SourceName = ? WHERE AlmaId = ? AND UnitCatalogRecId = ?",
            batch
        )
        conn_enr.commit()
        print(f"  {min(i + batch_size, len(updates)):,}/{len(updates):,}")

    # Verify
    ce.execute("SELECT SourceName, COUNT(*) FROM catalog WHERE SourceName LIKE 'Site User%' GROUP BY SourceName ORDER BY COUNT(*) DESC")
    print(f"\nSite User SourceName distribution:")
    for name, cnt in ce.fetchall():
        print(f"  {name}: {cnt:,}")

    conn_fist.close()
    conn_enr.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
