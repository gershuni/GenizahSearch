# -*- coding: utf-8 -*-
"""RSG postmortem — step 2: pull cluster members + raw text samples.
Read-only. Prints member counts and a few raw page slices per cluster."""
import sqlite3
from collections import Counter
from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
MEM = "passage_unit_members_accepted_pairs_canonmask"

CLUSTERS = {
    1430332: "RSG-dominated (תפסיר אלאלפאט אלצעבה + תפסיר רס\"ג)",
    303006: "Deut commentary (פירוש/תפסיר רס\"ג/יפת)",
    1157648: "Psalms commentary",
    1038702: "Leviticus (מקרא+אונקלוס+תפסיר רס\"ג)",
}


def main():
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")
    for u, desc in CLUSTERS.items():
        rows = con.execute(
            f"SELECT page_id, sys_id, start, end, cov, role FROM {MEM} "
            f"WHERE unit=?", (u,)).fetchall()
        print(f"\n######## unit {u} — {desc}")
        print(f"members: {len(rows)} rows, "
              f"{len({r[1] for r in rows})} distinct sys_ids")
        roles = Counter(r[5] for r in rows)
        print("roles:", dict(roles))
        lens = sorted(r[3] - r[2] for r in rows)
        print(f"passage-len (end-start): min={lens[0]} "
              f"med={lens[len(lens)//2]} max={lens[-1]}")
        # show 3 raw slices (longest passages, distinct sys)
        ranked = sorted(rows, key=lambda r: -(r[3] - r[2]))
        seen = set()
        shown = 0
        for pid, sid, s, e, cov, role in ranked:
            if sid in seen:
                continue
            seen.add(sid)
            tx = con.execute("SELECT text FROM pages WHERE page_id=?",
                             (pid,)).fetchone()[0]
            stream, offs = norm_stream(tx)
            # raw slice of the shared passage
            if len(offs) and s < len(offs):
                ee = min(e, len(offs))
                raw = tx[offs[s]:offs[ee - 1] + 1] if ee > s else ''
            else:
                raw = ''
            print(f"\n  -- {pid} (sys {sid}) passage[{s}:{e}] cov={cov:.2f} "
                  f"role={role} pagelen={len(stream)}")
            print("     RAW:", raw[:420].replace('\n', ' '))
            print("     NORM:", stream[s:e][:300])
            shown += 1
            if shown >= 3:
                break
    con.close()


if __name__ == '__main__':
    main()
