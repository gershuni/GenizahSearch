#!/usr/bin/env python3
"""
Batch translate Oxford Bodleian metadata (title, contents, provenance) EN->HE.

Reads from oxford_full_db.json, translates unique English texts via Dicta API,
stores results in libraries_translations.db `oxford_translations` table.

~6K unique texts, ~1M characters total.
"""

import json
import sqlite3
import signal
import sys
import os
import time
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OXFORD_DB = PROJECT_ROOT / "oxford_full_db.json"
OUTPUT_DB = PROJECT_ROOT / "libraries_translations.db"

# Add project root to path for shared imports
sys.path.insert(0, str(PROJECT_ROOT))


def create_table(conn: sqlite3.Connection):
    """Create oxford_translations table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oxford_translations (
            english_text TEXT PRIMARY KEY,
            hebrew_text TEXT,
            field_type TEXT,
            translated_at TEXT
        )
    """)
    conn.commit()


def extract_unique_texts(oxford_path: Path) -> dict:
    """Extract unique (text -> field_type) from oxford_full_db.json."""
    with open(oxford_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    texts = {}  # english_text -> field_type
    for sm, vol in data.items():
        for part_key, info in vol.items():
            meta = info.get("metadata", {})
            for field in ("title", "contents", "provenance"):
                txt = meta.get(field, "").strip()
                if txt and txt not in texts:
                    texts[txt] = field
    return texts


def main():
    if not OXFORD_DB.exists():
        print(f"ERROR: {OXFORD_DB} not found")
        sys.exit(1)

    print("Extracting unique Oxford metadata texts...")
    texts = extract_unique_texts(OXFORD_DB)
    print(f"  {len(texts)} unique texts ({sum(len(t) for t in texts):,} chars)")

    # Open DB and check what's already translated
    conn = sqlite3.connect(str(OUTPUT_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    create_table(conn)

    existing = set()
    for row in conn.execute("SELECT english_text FROM oxford_translations"):
        existing.add(row[0])
    pending = {t: ft for t, ft in texts.items() if t not in existing}
    print(f"  Already translated: {len(existing)}")
    print(f"  Pending: {len(pending)}")

    if not pending:
        print("Nothing to translate. Done!")
        conn.close()
        return

    # Import Dicta client
    from shared.dicta_client import translate_text

    # SIGINT handler for graceful stop
    _stop = {"flag": False}
    def handle_sigint(sig, frame):
        print("\nSIGINT received — finishing current item and saving...")
        _stop["flag"] = True
    signal.signal(signal.SIGINT, handle_sigint)

    # Translate with sequential API calls (Dicta rate limits)
    items = list(pending.items())
    done = 0
    failed = 0
    batch = []
    now = datetime.now(timezone.utc).isoformat()

    print(f"\nTranslating {len(items)} texts...")
    for i, (eng_text, field_type) in enumerate(items):
        if _stop["flag"]:
            print(f"Stopped at {done}/{len(items)}")
            break

        try:
            heb = translate_text(eng_text, source="en", target="he")
            if heb:
                batch.append((eng_text, heb, field_type, now))
                done += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  Error on item {i}: {e}")
            failed += 1
            time.sleep(2)  # Back off on error

        # Commit every 100 items
        if len(batch) >= 100:
            conn.executemany(
                "INSERT OR REPLACE INTO oxford_translations "
                "(english_text, hebrew_text, field_type, translated_at) "
                "VALUES (?, ?, ?, ?)",
                batch,
            )
            conn.commit()
            batch = []
            elapsed = time.time()
            print(f"  {done + failed}/{len(items)} ({done} ok, {failed} failed)")

        # Throttle: 3s between requests to avoid 429
        time.sleep(3)

    # Final commit
    if batch:
        conn.executemany(
            "INSERT OR REPLACE INTO oxford_translations "
            "(english_text, hebrew_text, field_type, translated_at) "
            "VALUES (?, ?, ?, ?)",
            batch,
        )
        conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM oxford_translations").fetchone()[0]
    print(f"\nDone! {done} translated, {failed} failed")
    print(f"Total in oxford_translations: {total}")
    conn.close()


if __name__ == "__main__":
    main()
