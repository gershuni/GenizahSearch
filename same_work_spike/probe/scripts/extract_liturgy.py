# -*- coding: utf-8 -*-
"""Liturgy-domain subcorpus for the per-domain second pass.

The DF<=100 cap starves short high-witness texts (df_damage_full.md) —
overwhelmingly liturgy/piyyut. Over a ~liturgy-only subcorpus the same
absolute cap is relatively ~4x looser, recovering their anchor budget
without a global volume blowup.

Selects MSS whose FJMS domains match Piyyut / Liturgy and Brakhot
(+children), copies their fullcorpus pages + track1_matches rows (for
maskcanon spans) into liturgy.db — rehearsal_run.py runs on it unchanged.
"""
import sqlite3
import time

ROOT = r"C:\Genizahsearch"
FULL = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT = ROOT + r"\same_work_spike\probe\data\liturgy.db"
FJMS = ROOT + r"\fist_data\fjms_enrichment.db"

t0 = time.time()
fj = sqlite3.connect(FJMS)
lit_sys = {str(r[0]) for r in fj.execute("""
    SELECT DISTINCT AlmaId FROM domains
    WHERE Domain LIKE '%Piyyut%' OR Domain LIKE '%Piyut%'
       OR Domain LIKE '%Liturg%' OR Domain LIKE '%Prayer%'
       OR ParentDomain LIKE '%Piyut%' OR ParentDomain LIKE '%Liturg%'
""")}
fj.close()
print(f"liturgy-domain MSS: {len(lit_sys):,}")

src = sqlite3.connect(FULL)
out = sqlite3.connect(OUT)
out.execute("DROP TABLE IF EXISTS pages")
out.execute("""CREATE TABLE pages (
    page_id TEXT PRIMARY KEY, sys_id TEXT NOT NULL, buckets TEXT NOT NULL,
    n_chars INTEGER, text TEXT)""")
out.execute("DROP TABLE IF EXISTS track1_matches")
out.execute("""CREATE TABLE track1_matches (
    page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT,
    author TEXT, title TEXT, mesirah TEXT, matched_letters INT,
    best_density REAL, n_spans INT, spans_json TEXT)""")

n_pages = 0
batch = []
for row in src.execute(
        "SELECT page_id, sys_id, buckets, n_chars, text FROM pages"):
    if row[1] in lit_sys:
        batch.append(row)
        n_pages += 1
        if len(batch) >= 5000:
            out.executemany("INSERT INTO pages VALUES (?,?,?,?,?)", batch)
            batch = []
if batch:
    out.executemany("INSERT INTO pages VALUES (?,?,?,?,?)", batch)
n_t1 = 0
batch = []
for row in src.execute("SELECT * FROM track1_matches"):
    if row[1] in lit_sys:
        batch.append(row)
        n_t1 += 1
        if len(batch) >= 5000:
            out.executemany(
                "INSERT INTO track1_matches VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            batch = []
if batch:
    out.executemany("INSERT INTO track1_matches VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?)", batch)
out.execute("CREATE INDEX idx_sys ON pages(sys_id)")
out.execute("CREATE INDEX idx_t1_page ON track1_matches(page_id)")
out.commit()
n_bh = out.execute(
    "SELECT COUNT(DISTINCT sys_id) FROM pages WHERE buckets LIKE '%bh%'"
).fetchone()[0]
print(f"liturgy.db: {n_pages:,} pages, {n_t1:,} track1 rows, "
      f"{n_bh} BH-witness MSS ({time.time() - t0:.0f}s)")
out.close()
src.close()
