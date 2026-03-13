"""
Complete source attribution for fjms_enrichment.db catalog records.
Uses FJMS BooleanSearch WCF API to bridge website user IDs → FIST SubIds.

Strategy:
1. Login to FJMS via SSO
2. Extract all 8 BooleanSearch panels (SearchParam=15)
3. For each panel entry, call GetShelfmarks WCF with strSource=web_id
   → returns shelfmarks with InventoryIds
   → map InventoryId to FIST SubId via dbo_InventorySignature
4. Build complete SourceId/SubId → display name mapping
5. Update enrichment DB

Usage:
    python scripts/attribute_sources_via_api.py              # dry run (report only)
    python scripts/attribute_sources_via_api.py --apply      # apply to enrichment DB
    python scripts/attribute_sources_via_api.py --resume     # resume from checkpoint
"""

import requests
import re
import sqlite3
import json
import time
import os
import sys
import signal
import shutil
from datetime import datetime

WCF_BASE = 'https://fgp.genizah.org/WCFServices/AjaxWcfHelper.svc'
BOOLEAN_URL = ('https://fgp.genizah.org/SelectionPages/SearchPages/'
               'BooleanSearch/BooleanSearch.aspx')
FIST_DB = 'fist_data/FIST.db'
ENRICHMENT_DB = 'fist_data/fjms_enrichment.db'
CHECKPOINT_FILE = 'scripts/source_attribution_checkpoint.json'

PANEL_NAMES = {
    1: 'Teams', 2: 'Computerized Catalogs', 3: 'Printed Catalogs',
    4: 'Books', 5: 'Site Users', 6: 'Handlist',
    7: 'Institutions', 8: 'Articles'
}

# Build empty CatRecParams template
CAT_REC_TEMPLATE = {
    "hasJoinType": None, "JoinTypeNum": None, "HasJoinListItem": None,
    "filterCollection": None, "filterSubCol": None, "filterVol": None,
    "intLibraryId": "", "intCollection": "", "strVol": "",
    "notShelfmark": "", "strDomain": "", "DomainList": "", "notDomain": "",
    "intCreation": "", "notCreation": "", "intFrame": "", "strFrameItems": "",
    "notFrame": "", "intCreationParent": "", "intFrameParent": "",
    "strFrameItemsParent": "", "intAuthor": "", "notAuthor": "",
    "intFromDate": "", "intToDate": "", "notCopyDate": "",
    "strLang": "", "notLang": "", "strLetter": "", "notLetter": "",
    "strArea": "", "notArea": "", "strWriting": "", "notWriting": "",
    "vocal": None, "strVocal": "", "notVocal": "",
    "filterColumns": "", "intColumnsFrom": "", "intColumnsTo": "",
    "notColumn": "", "filterLines": "", "intRowsFrom": "", "intRowsTo": "",
    "notRow": "", "fromCalcRow": "", "toCalcRow": "",
    "strPhysical": "", "notPhysical": "", "strMaterial": "", "notMaterial": "",
    "isPalimpsest": None, "hasIllustration": None, "hasColophon": None,
    "hasCantillation": None,
    "strSource": "", "notSource": "",
    "hasTranscInSite": None, "hasEngTransl": None, "hasHebTransl": None,
    "hasCatalogScans": None, "hasImageInSite": None,
    "minBibNum": None, "maxBibNum": None, "identificationsNum": None,
    "genizahCode": None, "YearsBibRef": "",
    "selFromYearBibRef": None, "selToYearBibRef": None,
    "selModeBibRef": None, "selTitleBibRef": None, "selAuthorBibRef": None,
    "ModeBibRefTxt": "", "TitleBibRefTxt": "", "AuthorBibRefTxt": "",
    "seldiscussedBibRef": None, "selTranscriptionBibRef": None,
    "selTranslationBibRef": None, "selImageBibRef": None,
    "fromComputedDpi": None, "fromComponentHeight": None,
    "fromComponentWidth": None, "fromNumberOfLines": None,
    "fromRightMargin": None, "fromLeftMargin": None,
    "fromTopMargin": None, "fromBottomMargin": None,
    "fromAverageLinesHeight": None, "fromAvgLineSpacing": None,
    "fromMedianTextDensity": None,
    "toComputedDpi": None, "toComponentHeight": None,
    "toComponentWidth": None, "toNumberOfLines": None,
    "toRightMargin": None, "toLeftMargin": None,
    "toTopMargin": None, "toBottomMargin": None,
    "toAverageLinesHeight": None, "toAvgLineSpacing": None,
    "toMedianTextDensity": None,
    "filterAreaAndWriting": "", "areaAndWritingTxt": "",
    "colTxt": "", "domainTxt": "", "titleTxt": "", "authorTxt": "",
    "filterLangAndScript": "", "filterLangAndScriptTxt": "",
    "filterCopyDate": "", "filterCopyDateTxt": "",
    "filterVocalTxt": "", "filterMaterialTxt": "", "filterPhysicalTxt": "",
    "filterColumnsTxt": "", "filterLinesTxt": "",
    "filterSourceTxt": "", "GenizahCodeTxt": "",
    "filterAttributes": "", "filterAdditional": "",
    "filterAuthor": "", "filterAuthorTxt": "",
    "filterCreation": "", "filtertitleTxt": "",
    "BibNumRef": "", "BibOperator": "",
    "DateDetailSheetBibRefTxt": "",
    "strCriteria": "",
    "selectedSaveArchiveFolders": "", "strArchiveInventoryIds": ""
}

shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    print("\n⚠ Shutdown requested, saving checkpoint...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)


def login_fjms():
    """Login to FJMS and return authenticated session."""
    s = requests.Session()
    s.headers.update({
        'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36')
    })

    print("Logging in to FJMS...")
    resp = s.get('https://SSO.genizah.org/login/GetLoginUIT', params={
        'username': 'Miriamg', 'password': 'Fgp123',
        'screenWidth': '1920', 'callback': 'cb',
    })
    m = re.search(r'"UIT":"([^"]+)"', resp.text)
    if not m:
        raise RuntimeError(f"Login failed: {resp.text[:200]}")

    s.get('https://fgp.genizah.org/FgpFrames.aspx', params={
        'lang': 'eng', 'UIT': m.group(1), 'mainSiteType': 'GenizahSite',
    })
    s.cookies.set('frame', '1', domain='fgp.genizah.org')
    s.cookies.set('selLang', 'eng', domain='fgp.genizah.org')
    assert '.ASPXAUTH' in str(s.cookies), "Authentication failed"
    print("  OK: Authenticated")
    return s


def extract_panels(html):
    """Extract all 8 source panels from BooleanSearch page."""
    panels = {}
    for panel_num in range(1, 9):
        entries = re.findall(
            rf'<span\s+alt="([^"]+)">\s*'
            rf'<input\s+id="([^"]*SourceListCB{panel_num}[^"]*)"[^/]*/>\s*'
            rf'<label[^>]*>([^<]+)</label>',
            html, re.DOTALL
        )
        panels[panel_num] = [
            {'web_ids': alt, 'checkbox_id': cb_id, 'name': name.strip()}
            for alt, cb_id, name in entries
        ]
    return panels


def wcf_get_shelfmarks(session, web_id, source_name="", max_results=5):
    """
    Call GetShelfmarks WCF with strSource=web_id.
    Returns list of {Text, Value} dicts or None.
    Value format: "InventoryId;NegSignatureId"
    """
    params = dict(CAT_REC_TEMPLATE)
    params['strSource'] = str(web_id)
    params['filterSourceTxt'] = source_name

    try:
        resp = session.post(
            f'{WCF_BASE}/GetShelfmarks',
            json={
                'searchParams': params,
                'firstRec': 1,
                'lastRec': max_results,
                'currFrame': 1,
                'SrcPg': 'BooleanSearch'
            },
            headers={'Content-Type': 'application/json'},
            timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            d = data.get('d', {})
            if d and isinstance(d, dict):
                total = d.get('_sumTotalRows', 0)
                arr = d.get('_arr', [])
                return arr, total
    except Exception as e:
        print(f"    WCF error: {e}")
    return None, 0


def extract_inventory_ids(shelfmark_arr):
    """Extract InventoryIds from GetShelfmarks _arr response."""
    inv_ids = []
    for item in (shelfmark_arr or []):
        value = item.get('Value', '')
        if isinstance(value, str) and ';' in value:
            inv_str = value.split(';')[0]
            try:
                inv_ids.append(int(inv_str))
            except ValueError:
                pass
    return inv_ids


def build_inventory_to_subid_map(fist_conn, source_id):
    """Build InventoryId → set of SubIds reverse map from FIST.db."""
    c = fist_conn.cursor()
    c.execute('''
        SELECT DISTINCT isig.InventoryId, s.SubId
        FROM dbo_Signature s
        JOIN dbo_InventorySignature isig ON isig.SetSignatureId = s.SetSignatureId
        WHERE s.SourceId = ?
    ''', (source_id,))
    inv_to_subid = {}
    for inv_id, subid in c.fetchall():
        inv_to_subid.setdefault(inv_id, set()).add(subid)
    return inv_to_subid


def get_subid_stats(fist_conn, source_id):
    """Get catalog record counts per SubId."""
    c = fist_conn.cursor()
    c.execute('''
        SELECT s.SubId, COUNT(DISTINCT ucr.UnitCatalogRecId) as cat_recs
        FROM dbo_Signature s
        JOIN dbo_UnitCatalogRec ucr ON ucr.SignatureId = s.SignatureId
        WHERE s.SourceId = ?
        GROUP BY s.SubId
    ''', (source_id,))
    return {subid: cat_recs for subid, cat_recs in c.fetchall()}


def get_sample_shelfmarks(fist_conn, source_id, subid, limit=3):
    """Get sample shelfmarks for a SubId."""
    c = fist_conn.cursor()
    c.execute('''
        SELECT DISTINCT i.Shelfmark
        FROM dbo_Signature s
        JOIN dbo_InventorySignature isig ON isig.SetSignatureId = s.SetSignatureId
        JOIN dbo_Inventory i ON i.InventoryId = isig.InventoryId
        WHERE s.SourceId = ? AND s.SubId = ?
        LIMIT ?
    ''', (source_id, subid, limit))
    return [r[0] for r in c.fetchall()]


def bridge_panel_entries(session, fist_conn, entries, source_id, checkpoint_key,
                         checkpoint):
    """
    Bridge a panel's entries to FIST SubIds via GetShelfmarks WCF.

    For each entry:
    1. Call GetShelfmarks with strSource=web_id
    2. Extract InventoryIds from response
    3. Map to FIST SubIds via dbo_InventorySignature
    """
    inv_to_subid = build_inventory_to_subid_map(fist_conn, source_id)

    # Load previous progress
    bridged = checkpoint.get(checkpoint_key, {})
    bridged = {int(k): v for k, v in bridged.items()}
    processed = set(checkpoint.get(f'{checkpoint_key}_processed', []))

    new_matches = 0
    failed = 0
    multi = 0

    for i, entry in enumerate(entries):
        if shutdown_requested:
            break

        web_id = entry['web_ids']
        name = entry['name']

        if web_id in processed:
            continue

        # Call WCF
        arr, total = wcf_get_shelfmarks(session, web_id, name, max_results=5)

        if arr:
            inv_ids = extract_inventory_ids(arr)
            matched_subids = set()
            for inv_id in inv_ids:
                if inv_id in inv_to_subid:
                    matched_subids.update(inv_to_subid[inv_id])

            if len(matched_subids) == 1:
                subid = matched_subids.pop()
                bridged[subid] = name
                new_matches += 1
                print(f"  [{i+1}/{len(entries)}] ✓ {name} → SubId={subid} "
                      f"({total} shelfmarks)")
            elif len(matched_subids) > 1:
                # Multiple SubIds — likely same person with multiple SubIds
                # or overlapping shelfmarks. Take the most common one.
                print(f"  [{i+1}/{len(entries)}] ⚠ {name} → multiple SubIds: "
                      f"{matched_subids} ({total} shelfmarks)")
                for sid in matched_subids:
                    bridged[sid] = name
                multi += 1
                new_matches += len(matched_subids)
            elif total == 0:
                print(f"  [{i+1}/{len(entries)}] · {name} → 0 shelfmarks (no catalog)")
            else:
                # Has shelfmarks but no match in FIST for this SourceId
                texts = [item.get('Text', '?') for item in arr[:3]]
                print(f"  [{i+1}/{len(entries)}] ✗ {name} → {total} shelfmarks, "
                      f"no SubId match. Shelfs: {texts}")
                failed += 1
        else:
            print(f"  [{i+1}/{len(entries)}] ✗ {name} → WCF failed")
            failed += 1

        processed.add(web_id)

        # Checkpoint every 20
        if (i + 1) % 20 == 0:
            checkpoint[checkpoint_key] = {str(k): v for k, v in bridged.items()}
            checkpoint[f'{checkpoint_key}_processed'] = list(processed)
            save_checkpoint(checkpoint)

        time.sleep(0.8)  # Rate limit

    # Final checkpoint
    checkpoint[checkpoint_key] = {str(k): v for k, v in bridged.items()}
    checkpoint[f'{checkpoint_key}_processed'] = list(processed)
    save_checkpoint(checkpoint)

    print(f"\n  Panel results: {new_matches} matched, {multi} multi-SubId, "
          f"{failed} failed")
    return bridged


def bridge_teams_via_api(session, fist_conn, team_entries, checkpoint):
    """Bridge Panel 1 (Teams) entries to SourceId=100 SubIds."""
    print("\n--- Teams (Panel 1) ---")
    # Teams have different SourceId patterns
    # Panel 1 alt values don't directly match any SourceId
    # Try all source IDs that might contain teams
    for source_id in [100, 141, 160, 161]:
        inv_to_subid = build_inventory_to_subid_map(fist_conn, source_id)
        if not inv_to_subid:
            continue

        stats = get_subid_stats(fist_conn, source_id)
        if not stats:
            continue

        print(f"\n  Trying SourceId={source_id} ({len(stats)} SubIds, "
              f"{sum(stats.values())} cat_recs):")

        for entry in team_entries:
            web_id = entry['web_ids']
            name = entry['name']

            arr, total = wcf_get_shelfmarks(session, web_id, name, max_results=3)
            if arr and total > 0:
                inv_ids = extract_inventory_ids(arr)
                matched = set()
                for inv_id in inv_ids:
                    if inv_id in inv_to_subid:
                        matched.update(inv_to_subid[inv_id])
                if matched:
                    print(f"    {name} (alt={web_id}, {total} shelfs) → "
                          f"SourceId={source_id} SubIds={matched}")
            time.sleep(0.5)


def save_checkpoint(checkpoint):
    """Save checkpoint atomically."""
    checkpoint['last_updated'] = datetime.now().isoformat()
    tmp = CHECKPOINT_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)
    os.replace(tmp, CHECKPOINT_FILE)


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}


def apply_attributions(fist_conn, enrichment_conn, site_user_map, team_map,
                       other_maps):
    """Apply all attributions to enrichment DB."""
    cf = fist_conn.cursor()
    ce = enrichment_conn.cursor()

    # Backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"fist_data/fjms_enrichment_pre_attribution_{ts}.db"
    print(f"\nCreating backup: {backup}")
    shutil.copy2(ENRICHMENT_DB, backup)

    # Get all "preliminary" records
    ce.execute("""SELECT c.AlmaId, c.UnitCatalogRecId
                  FROM catalog c
                  WHERE c.SourceName = 'Personal handlist - preliminary'""")
    prelim_rows = ce.fetchall()
    print(f"Preliminary records: {len(prelim_rows):,}")

    # Build UCR → (SourceId, SubId) cache from FIST.db
    ucr_ids = list(set(r[1] for r in prelim_rows))
    ucr_cache = {}
    for i in range(0, len(ucr_ids), 500):
        batch = ucr_ids[i:i + 500]
        placeholders = ','.join('?' * len(batch))
        cf.execute(f"""SELECT ucr.UnitCatalogRecId, s.SourceId, s.SubId
                       FROM dbo_Signature s
                       JOIN dbo_UnitCatalogRec ucr ON ucr.SignatureId = s.SignatureId
                       WHERE ucr.UnitCatalogRecId IN ({placeholders})""", batch)
        for ucr_id, src_id, sub_id in cf.fetchall():
            ucr_cache[ucr_id] = (src_id, sub_id)
    print(f"  UCR cache: {len(ucr_cache):,} entries")

    # Build updates
    updates = []
    still_prelim = 0
    no_cache = 0

    for alma, ucr_id in prelim_rows:
        result = ucr_cache.get(ucr_id)
        if not result:
            no_cache += 1
            continue

        source_id, sub_id = result
        name = None

        if source_id == 850 and sub_id in site_user_map:
            name = site_user_map[sub_id]
        elif source_id == 100 and sub_id in team_map:
            name = team_map[sub_id]
        elif source_id in other_maps and sub_id in other_maps[source_id]:
            name = other_maps[source_id][sub_id]
        elif source_id in (141, 160, 161):
            name = "FGP Team (internal)"
        elif source_id == 601:
            name = "Book reference"

        if name:
            updates.append((name, alma, ucr_id))
        else:
            still_prelim += 1

    # Breakdown
    by_source = {}
    for alma, ucr_id in prelim_rows:
        r = ucr_cache.get(ucr_id)
        if r:
            by_source[r[0]] = by_source.get(r[0], 0) + 1
    print(f"\n  Preliminary by SourceId: {by_source}")
    print(f"  Updates to apply: {len(updates):,}")
    print(f"  Still preliminary: {still_prelim:,}")
    print(f"  No FIST cache: {no_cache:,}")

    # Apply
    for i in range(0, len(updates), 5000):
        batch = updates[i:i + 5000]
        ce.executemany(
            "UPDATE catalog SET SourceName = ? "
            "WHERE AlmaId = ? AND UnitCatalogRecId = ?",
            batch
        )
        enrichment_conn.commit()
        print(f"  Applied {min(i + 5000, len(updates)):,}/{len(updates):,}")

    # Final stats
    print("\n  SourceName distribution:")
    ce.execute("""SELECT
        CASE
            WHEN SourceName LIKE 'Site User%' THEN 'Site User (named)'
            WHEN SourceName LIKE 'FGP%' THEN 'FGP Team'
            WHEN SourceName = 'Personal handlist - preliminary' THEN 'Still preliminary'
            WHEN SourceName LIKE '%Personal Handlist%' THEN 'Named handlist'
            WHEN SourceName = 'Book reference' THEN 'Book reference'
            ELSE 'Other'
        END as category,
        COUNT(*)
        FROM catalog
        GROUP BY category
        ORDER BY COUNT(*) DESC""")
    for cat, cnt in ce.fetchall():
        print(f"    {cat}: {cnt:,}")

    return len(updates)


def main():
    apply_fix = "--apply" in sys.argv
    resume = "--resume" in sys.argv

    print("=" * 70)
    print("FJMS Source Attribution via API")
    print("=" * 70)

    checkpoint = load_checkpoint() if resume else {}
    conn_fist = sqlite3.connect(FIST_DB)
    conn_enr = sqlite3.connect(ENRICHMENT_DB)

    # Step 1: Run existing count-based matching
    print("\n" + "=" * 70)
    print("STEP 1: Count-based matching (existing algorithm)")
    print("=" * 70)

    sys.path.insert(0, 'scripts')
    from map_site_user_subids import (get_subid_stats as get_850_stats,
                                       match_subids_to_names, KNOWN_MAP)

    stats_850 = get_850_stats(conn_fist)
    algo_matched, ambiguous, unmatched_list = match_subids_to_names(stats_850)

    # Merge known + algorithm matches
    site_user_map = dict(KNOWN_MAP)
    site_user_map.update(algo_matched)

    covered = sum(stats_850[s]["catalog_recs"]
                  for s in site_user_map if s in stats_850)
    total = sum(s["catalog_recs"] for s in stats_850.values())
    print(f"\n  Count-based: {len(site_user_map)} SubIds matched, "
          f"{covered:,}/{total:,} records")
    print(f"  Ambiguous: {len(ambiguous)}, Unmatched: {len(unmatched_list)}")

    # Step 2: API-based bridging for ALL panels
    print("\n" + "=" * 70)
    print("STEP 2: API-based bridging via GetShelfmarks WCF")
    print("=" * 70)

    session = login_fjms()

    print("\nFetching BooleanSearch page...")
    resp = session.get(BOOLEAN_URL, params={'SearchParam': '15'})
    panels = extract_panels(resp.text)
    for num, entries in panels.items():
        print(f"  Panel {num} ({PANEL_NAMES[num]}): {len(entries)} entries")

    # Bridge Site Users (Panel 5) → SourceId=850
    print("\n" + "-" * 50)
    print("Panel 5: Site Users → SourceId=850")
    print("-" * 50)
    api_bridged = bridge_panel_entries(
        session, conn_fist, panels[5], 850, 'site_users', checkpoint
    )

    # Merge API results into site_user_map
    for subid, name in api_bridged.items():
        if subid not in site_user_map:
            site_user_map[subid] = name

    # Bridge Teams (Panel 1) → try SourceId=100
    print("\n" + "-" * 50)
    print("Panel 1: Teams → SourceId=100 / others")
    print("-" * 50)
    team_bridged = bridge_panel_entries(
        session, conn_fist, panels[1], 100, 'teams', checkpoint
    )

    # Also try bridging teams against SourceId 141, 160, 161
    other_maps = {}
    for sid in [141, 160, 161]:
        stats = get_subid_stats(conn_fist, sid)
        if stats:
            print(f"\n  Trying Panel 1 entries against SourceId={sid}...")
            bridged = bridge_panel_entries(
                session, conn_fist, panels[1], sid,
                f'teams_sid{sid}', checkpoint
            )
            if bridged:
                other_maps[sid] = bridged

    # Bridge Institutions (Panel 7) → SourceId=400
    print("\n" + "-" * 50)
    print("Panel 7: Institutions")
    print("-" * 50)
    stats_400 = get_subid_stats(conn_fist, 400)
    if stats_400:
        inst_bridged = bridge_panel_entries(
            session, conn_fist, panels[7], 400, 'institutions', checkpoint
        )
        other_maps[400] = inst_bridged

    # Step 3: Report
    print("\n" + "=" * 70)
    print("FINAL REPORT")
    print("=" * 70)

    # Site Users (850)
    total_subids = len(stats_850)
    matched_subids = len(site_user_map)
    covered = sum(stats_850[s]["catalog_recs"]
                  for s in site_user_map if s in stats_850)
    total_recs = sum(s["catalog_recs"] for s in stats_850.values())
    print(f"\n  Site Users (850):")
    print(f"    SubIds matched: {matched_subids}/{total_subids}")
    print(f"    Records covered: {covered:,}/{total_recs:,} "
          f"({covered/total_recs*100:.1f}%)")

    remaining = set(stats_850.keys()) - set(site_user_map.keys())
    if remaining:
        print(f"    Unmatched ({len(remaining)}):")
        for subid in sorted(remaining,
                            key=lambda s: -stats_850[s]["catalog_recs"])[:15]:
            shelfs = get_sample_shelfmarks(conn_fist, 850, subid, 2)
            print(f"      SubId={subid:>4}: "
                  f"{stats_850[subid]['catalog_recs']:>3} recs, "
                  f"shelfs={shelfs}")

    # Teams (100)
    stats_100 = get_subid_stats(conn_fist, 100)
    if stats_100:
        print(f"\n  Teams (100): {len(team_bridged)} SubIds matched / "
              f"{len(stats_100)} total")

    # Other sources
    for sid, bridged in other_maps.items():
        stats = get_subid_stats(conn_fist, sid)
        if stats:
            print(f"  SourceId={sid}: {len(bridged)} matched / {len(stats)} total")

    # Step 4: Apply
    if apply_fix:
        print("\n" + "=" * 70)
        print("APPLYING TO ENRICHMENT DB")
        print("=" * 70)
        updated = apply_attributions(
            conn_fist, conn_enr, site_user_map, team_bridged, other_maps
        )
        print(f"\n  Total records updated: {updated:,}")
    else:
        print("\n  (Dry run — use --apply to update enrichment DB)")

    conn_fist.close()
    conn_enr.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
