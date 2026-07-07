# -*- coding: utf-8 -*-
"""Define pilot buckets: join-groups, title-groups, FGP-overlap pages.

Output: data/buckets.json + data/buckets_report.txt
"""
import json
import random
import re
import sqlite3

ROOT = r"C:\Genizahsearch"
FJMS = ROOT + r"\fist_data\fjms_enrichment.db"
FGP = ROOT + r"\fgp_data\fgp_transcriptions.db"
OUT = ROOT + r"\same_work_spike\probe\data\buckets.json"
REPORT = ROOT + r"\same_work_spike\probe\data\buckets_report.txt"

random.seed(42)
lines = []

# ---------------- fjms joins ----------------
con = sqlite3.connect(FJMS)
cur = con.cursor()

rows = cur.execute("""
    SELECT JoinGroupId, AlmaId FROM joins
    WHERE JoinGroupId IS NOT NULL AND AlmaId IS NOT NULL
""").fetchall()
groups = {}
for gid, alma in rows:
    groups.setdefault(gid, set()).add(str(alma))
multi = {g: sorted(m) for g, m in groups.items() if len(m) >= 2}
lines.append(f"joins: {len(groups)} groups, {len(multi)} with >=2 members")

# sample 150 groups, prefer small-to-mid groups (2-6 members)
small = [g for g, m in multi.items() if len(m) <= 6]
sample_g = random.sample(small, min(150, len(small)))
joins_bucket = {str(g): multi[g] for g in sample_g}
n_join_sys = len({s for m in joins_bucket.values() for s in m})
lines.append(f"joins bucket: {len(joins_bucket)} groups, {n_join_sys} sys_ids")

# ---------------- fjms title groups ----------------
# schema peek
cols_titles = [r[1] for r in cur.execute("PRAGMA table_info(genizah_titles)")]
lines.append(f"genizah_titles columns: {cols_titles}")

tg = cur.execute("""
    SELECT c.GenizahTitleId, COUNT(DISTINCT c.AlmaId) AS n
    FROM catalog c
    WHERE c.GenizahTitleId IS NOT NULL AND c.GenizahTitleId != ''
    GROUP BY c.GenizahTitleId
    HAVING n BETWEEN 5 AND 40
""").fetchall()
lines.append(f"title groups with 5-40 MSS: {len(tg)}")

# fetch names
name_col = None
for cand in ('OrgTitle', 'TitleHeb', 'Title', 'HebTitle', 'Name', 'TitleName'):
    if cand in cols_titles:
        name_col = cand
        break
id_col = None
for cand in ('GenizahTitleId', 'Id', 'TitleId'):
    if cand in cols_titles:
        id_col = cand
        break
names = {}
if name_col and id_col:
    for tid, nm in cur.execute(f"SELECT {id_col}, {name_col} FROM genizah_titles"):
        names[str(tid)] = nm or ''

CANON_RE = re.compile(r'מקרא|תנ"ך|תורה|תלמוד|משנה|נביאים|כתובים|תהלים|בבלי|ירושלמי')
candidates = []
for tid, n in tg:
    nm = names.get(str(tid), '')
    if not nm:
        continue  # unnamed groups are uninterpretable - skip
    if CANON_RE.search(nm):
        continue
    candidates.append((str(tid), nm, n))
random.shuffle(candidates)
picked = candidates[:14]

titles_bucket = {}
for tid, nm, n in picked:
    sys_ids = [str(r[0]) for r in cur.execute(
        "SELECT DISTINCT AlmaId FROM catalog WHERE GenizahTitleId = ?", (tid,))]
    titles_bucket[tid] = {'name': nm, 'sys_ids': sorted(sys_ids)}
n_title_sys = len({s for t in titles_bucket.values() for s in t['sys_ids']})
lines.append(f"title bucket: {len(titles_bucket)} groups, {n_title_sys} sys_ids")
for tid, t in titles_bucket.items():
    lines.append(f"  title {tid} ({len(t['sys_ids'])} MSS): {t['name']}")
con.close()

# ---------------- FGP human transcriptions ----------------
con = sqlite3.connect(FGP)
cur = con.cursor()
tables = [r[0] for r in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table'")]
lines.append(f"fgp tables: {tables}")
cols_fgp = [r[1] for r in cur.execute("PRAGMA table_info(fgp_transcriptions)")]
lines.append(f"fgp_transcriptions columns: {cols_fgp}")

# find the text-bearing column
text_col = None
for cand in ('text', 'transcription_text', 'content', 'plain_text', 'xml_text'):
    if cand in cols_fgp:
        text_col = cand
        break
lines.append(f"fgp text column: {text_col}")

fgp_bucket = []
if text_col:
    rows = cur.execute(f"""
        SELECT sys_id, image_side, LENGTH({text_col})
        FROM fgp_transcriptions
        WHERE sys_id IS NOT NULL AND {text_col} IS NOT NULL
              AND LENGTH({text_col}) > 400
    """).fetchall()
    lines.append(f"fgp rows with text>400 chars: {len(rows)}")
    by_sys = {}
    for sys_id, side, ln in rows:
        by_sys.setdefault(str(sys_id), []).append((side, ln))
    sys_sample = random.sample(sorted(by_sys), min(200, len(by_sys)))
    fgp_bucket = sys_sample
    lines.append(f"fgp bucket: {len(fgp_bucket)} sys_ids")
else:
    # sample a row to see what's there
    row = cur.execute("SELECT * FROM fgp_transcriptions LIMIT 1").fetchone()
    lines.append("NO text column found; sample row (truncated):")
    for c, v in zip(cols_fgp, row):
        lines.append(f"  {c} = {str(v)[:80]!r}")
con.close()

json.dump({
    'joins': joins_bucket,
    'titles': titles_bucket,
    'fgp': fgp_bucket,
}, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)

open(REPORT, 'w', encoding='utf-8').write("\n".join(lines))
print(f"joins groups={len(joins_bucket)} ({n_join_sys} sys) | "
      f"titles={len(titles_bucket)} ({n_title_sys} sys) | fgp={len(fgp_bucket)}")
print(f"report: {REPORT}")
