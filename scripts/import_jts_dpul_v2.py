#!/usr/bin/env python3
"""
Import ALL JTS/Princeton DPUL catalog items into the NLI crossref sidecar database.

V2: Instead of searching per-shelfmark (v1 got 453/44K), this iterates the entire
DPUL cairo_geniza catalog (36,283 items) via paginated listing API, then fetches
each item's detail page for the Figgy IIIF manifest URL.

Two phases:
  Phase 1: Iterate catalog listing pages (100 items/page, ~363 pages)
           → extracts shelfmark + ark_suffix from listing
  Phase 2: Fetch item detail JSON for each ark_suffix (parallel workers)
           → extracts manifest_url + thumbnail_url

Usage:
    python scripts/import_jts_dpul_v2.py --dry-run --limit 50     # Test mode
    python scripts/import_jts_dpul_v2.py --workers 5               # Full import
    python scripts/import_jts_dpul_v2.py --resume                  # Resume interrupted
    python scripts/import_jts_dpul_v2.py --phase1-only             # Just get listing
"""

import argparse
import json
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ── Constants ────────────────────────────────────────────────────────

DPUL_CATALOG_URL = "https://dpul.princeton.edu/cairo_geniza/catalog.json"
DPUL_ITEM_URL = "https://dpul.princeton.edu/cairo_geniza/catalog/{ark_suffix}.json"
DPUL_PAGE_URL = "https://dpul.princeton.edu/cairo_geniza/catalog/{ark_suffix}"
PER_PAGE = 100
DEFAULT_DELAY = 0.15
MAX_WORKERS = 5
CHECKPOINT_INTERVAL = 200

DB_PATH = "nli_data/nli_crossref.db"
CHECKPOINT_PATH = Path("scripts/import_jts_dpul_v2_checkpoint.json")

VERSION = "2.0.0"


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_title(html_title: str) -> str:
    """Strip HTML tags from DPUL title field: <ul><li dir="ltr">ENA 3714.1</li></ul> → ENA 3714.1"""
    return re.sub(r"<[^>]+>", "", html_title).strip()


# ── Phase 1: Iterate catalog listing ─────────────────────────────────

def fetch_catalog_listing(session: requests.Session, delay: float, limit: int = 0) -> list[dict]:
    """
    Iterate all pages of the DPUL cairo_geniza catalog listing.

    Returns list of dicts: {shelfmark, ark_suffix, dpul_url}
    """
    items = []
    page = 1

    # First request to get total count
    resp = session.get(DPUL_CATALOG_URL, params={
        "per_page": PER_PAGE, "page": 1, "sort": "sort_title",
        "search_field": "all_fields", "q": "",
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    total = data["meta"]["pages"]["total_count"]
    total_pages = data["meta"]["pages"]["total_pages"]

    if limit:
        total = min(total, limit)
        total_pages = min(total_pages, (limit + PER_PAGE - 1) // PER_PAGE)

    print(f"  DPUL catalog: {total:,} items across {total_pages} pages")
    pbar = tqdm(total=total, desc="  Phase 1: listing", unit=" items")

    while True:
        if page > 1:
            time.sleep(delay)
            resp = session.get(DPUL_CATALOG_URL, params={
                "per_page": PER_PAGE, "page": page, "sort": "sort_title",
                "search_field": "all_fields", "q": "",
            }, timeout=30)
            if resp.status_code != 200:
                print(f"\n  Warning: page {page} returned {resp.status_code}, stopping listing")
                break
            data = resp.json()

        page_items = data.get("data", [])
        if not page_items:
            break

        for item in page_items:
            title = extract_title(item.get("attributes", {}).get("title", ""))
            self_url = item.get("links", {}).get("self", "")
            if not title or not self_url:
                continue
            ark_suffix = self_url.rsplit("/", 1)[-1]
            items.append({
                "shelfmark": title,
                "ark_suffix": ark_suffix,
                "dpul_url": DPUL_PAGE_URL.format(ark_suffix=ark_suffix),
            })
            pbar.update(1)

            if limit and len(items) >= limit:
                break

        if limit and len(items) >= limit:
            break

        if page >= total_pages:
            break
        page += 1

    pbar.close()
    return items


# ── Phase 2: Fetch item details for manifest URLs ───────────────────

def fetch_item_detail(session: requests.Session, ark_suffix: str, delay: float) -> dict | None:
    """Fetch a single DPUL item detail JSON and extract manifest_url + thumbnail."""
    time.sleep(delay)
    try:
        url = DPUL_ITEM_URL.format(ark_suffix=ark_suffix)
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        doc = resp.json().get("response", {}).get("document", {})
        manifest_url = doc.get("content_metadata_iiif_manifest_field_ssi")
        thumbs = doc.get("thumbnail_ssim", [])
        thumbnail_url = thumbs[0] if thumbs else None
        return {"manifest_url": manifest_url, "thumbnail_url": thumbnail_url}
    except Exception:
        return None


def fetch_all_details(
    items: list[dict], checkpoint: dict, delay: float, workers: int
) -> tuple[dict, set]:
    """
    Fetch manifest details for all items using parallel workers.

    Returns (results dict keyed by ark_suffix, completed set of ark_suffixes).
    """
    completed = checkpoint.get("completed_arks", set())
    results = checkpoint.get("detail_results", {})
    remaining = [it for it in items if it["ark_suffix"] not in completed]

    if not remaining:
        print("  All item details already fetched!")
        return results, completed

    print(f"  Phase 2: {len(remaining):,} detail fetches ({len(completed):,} already done)")
    pbar = tqdm(total=len(items), initial=len(completed), desc="  Phase 2: details", unit=" items")

    lock = Lock()
    sessions = [create_session() for _ in range(max(workers, 1))]

    def process(idx, item):
        sess = sessions[idx % len(sessions)]
        detail = fetch_item_detail(sess, item["ark_suffix"], delay)
        return item["ark_suffix"], detail

    if workers <= 1:
        for i, item in enumerate(remaining):
            ark, detail = process(0, item)
            completed.add(ark)
            if detail:
                results[ark] = detail
            pbar.update(1)
            if len(completed) % CHECKPOINT_INTERVAL == 0:
                yield results, completed  # signal checkpoint
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for i, item in enumerate(remaining):
                fut = executor.submit(process, i, item)
                futures[fut] = item["ark_suffix"]

            batch_count = 0
            for fut in as_completed(futures):
                ark = futures[fut]
                try:
                    _, detail = fut.result()
                except Exception:
                    detail = None

                with lock:
                    completed.add(ark)
                    if detail:
                        results[ark] = detail
                    pbar.update(1)
                    batch_count += 1

                    if batch_count % CHECKPOINT_INTERVAL == 0:
                        yield results, completed  # signal checkpoint

    pbar.close()
    yield results, completed  # final


# ── Checkpoint management ────────────────────────────────────────────

def load_checkpoint() -> dict:
    if not CHECKPOINT_PATH.exists():
        return {}
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["completed_arks"] = set(data.get("completed_arks", []))
        return data
    except (json.JSONDecodeError, IOError):
        return {}


def save_checkpoint(items: list[dict], results: dict, completed: set):
    data = {
        "items": items,
        "detail_results": results,
        "completed_arks": sorted(completed),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    tmp.replace(CHECKPOINT_PATH)


# ── Database operations ──────────────────────────────────────────────

def rebuild_jts_dpul_table(db_path: str, items: list[dict], detail_results: dict):
    """Rebuild the jts_dpul table with all items."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS jts_dpul")
    cursor.execute("""
        CREATE TABLE jts_dpul (
            shelfmark TEXT PRIMARY KEY,
            ark_suffix TEXT NOT NULL,
            manifest_url TEXT,
            dpul_url TEXT NOT NULL,
            thumbnail_url TEXT
        )
    """)
    cursor.execute("CREATE INDEX idx_jd_ark ON jts_dpul(ark_suffix)")

    rows = []
    for item in items:
        ark = item["ark_suffix"]
        detail = detail_results.get(ark, {})
        rows.append((
            item["shelfmark"],
            ark,
            detail.get("manifest_url"),
            item["dpul_url"],
            detail.get("thumbnail_url"),
        ))

    cursor.executemany(
        "INSERT OR REPLACE INTO jts_dpul "
        "(shelfmark, ark_suffix, manifest_url, dpul_url, thumbnail_url) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )

    # Update meta
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ("version", VERSION))
    cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ("source_jts", "DPUL catalog iteration v2"))
    cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ("jts_imported", now))
    cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ("jts_total", str(len(rows))))

    conn.commit()

    with_manifest = sum(1 for r in detail_results.values() if r.get("manifest_url"))
    print(f"\n  Database updated: {len(rows):,} rows in jts_dpul ({with_manifest:,} with manifests)")
    conn.close()


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import all DPUL Cairo Geniza items into jts_dpul table")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of items to process")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help=f"Delay between API calls (default: {DEFAULT_DELAY}s)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"Parallel workers (default: {MAX_WORKERS})")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--phase1-only", action="store_true", help="Only fetch catalog listing, skip detail fetches")
    parser.add_argument("--db", default=DB_PATH, help=f"Database path (default: {DB_PATH})")
    args = parser.parse_args()

    print(f"== DPUL Cairo Geniza Full Import v{VERSION} ==")
    print()

    if not Path(args.db).exists():
        print(f"  Error: Database not found: {args.db}")
        sys.exit(1)

    # Phase 1: Get catalog listing
    checkpoint = load_checkpoint() if args.resume else {}

    if args.resume and checkpoint.get("items"):
        items = checkpoint["items"]
        print(f"  Resuming with {len(items):,} items from checkpoint")
    else:
        print("  Phase 1: Iterating DPUL catalog listing...")
        session = create_session()
        items = fetch_catalog_listing(session, args.delay, args.limit)
        print(f"  Got {len(items):,} items from catalog listing")

        if not items:
            print("  Error: No items found!")
            sys.exit(1)

        # Save items to checkpoint
        save_checkpoint(items, {}, set())

    if args.phase1_only:
        print(f"\n  Phase 1 complete. {len(items):,} items saved to checkpoint.")
        print(f"  Run again without --phase1-only to fetch details.")
        return

    # Phase 2: Fetch item details for manifest URLs
    detail_results = checkpoint.get("detail_results", {})
    completed_arks = checkpoint.get("completed_arks", set())

    for results, completed in fetch_all_details(
        items, {"completed_arks": completed_arks, "detail_results": detail_results},
        args.delay, args.workers
    ):
        detail_results = results
        completed_arks = completed
        save_checkpoint(items, detail_results, completed_arks)

    with_manifest = sum(1 for r in detail_results.values() if r.get("manifest_url"))
    print(f"\n  Phase 2 complete: {len(detail_results):,} details fetched, {with_manifest:,} with manifests")

    # Write to database
    if not args.dry_run:
        print(f"\n  Writing to database: {args.db}")
        rebuild_jts_dpul_table(args.db, items, detail_results)
    else:
        print("\n  Dry run — skipping database write")

    # Cleanup checkpoint on success
    if not args.dry_run and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        tmp = CHECKPOINT_PATH.with_suffix(".tmp")
        if tmp.exists():
            tmp.unlink()
        print("  Checkpoint cleaned up")

    print("\n  Done!")


if __name__ == "__main__":
    main()
