# -*- coding: utf-8 -*-
"""Stage-0 of the FRAG2 v2 map: build data/fullcorpus_v2.db whose pages.text
IS the "search text" — a human transcription (FGP or PGP) where a page-level
sanity gate passes, else the original MiDRASH HTR text (v1 unchanged).

Rationale (FRAG2-PLAN, Codex HIGH): every downstream coverage/length/span
computation must read search text. We achieve this STRUCTURALLY by making
pages.text the search text in a NEW db; v1 (fullcorpus.db) is opened strictly
read-only and never touched.

Transcription sources (Hillel 2026-07-10):
  * FGP  (fgp_data/fgp_transcriptions.db, doc_relation='Digital Edition'
    only — translations are NOT transcriptions and are excluded);
  * PGP  (pgp_data/pgp.db documents.transcription, has_transcription=1,
    sys_id via document_fragments) — documentary Genizah; catches copies /
    parallels (e.g. similar מעשה בית דין) the HTR noise hides.

Substitution gate — a page is substituted only if ALL hold (Hillel's warning:
both FGP and PGP are sometimes PARTIAL — a draft, only the Tefillah part, or
a description — and a partial transcription must NEVER replace fuller HTR):
  * best rapidfuzz partial_ratio_alignment score between
    norm_stream(HTR page) and norm_stream(transcription) >= MIN_SCORE
    (a prose description "it's a text about X" cannot align at 70);
  * both norm_streams >= MIN_LEN letters;
  * COVERAGE: when the transcription stream is SHORTER than the HTR stream,
    the aligned window must cover >= COVER_MIN of the HTR stream — a
    transcription of only part of the page fails and the page keeps HTR
    (gate key 'partial_coverage'). Candidates shorter than PREFILTER_MIN of
    the HTR stream are skipped without aligning (cannot reach COVER_MIN).
  * WINDOW CROP: when the transcription stream is LONGER than
    WINDOW_RATIO x the HTR stream (e.g. a multi-page PGP document
    transcription), only the ALIGNED raw window (+ WINDOW_PAD stream letters
    each side, mapped back through norm_stream offsets) is stored — so a
    page never carries text of OTHER pages. Absurd length ratios
    (> RATIO_MAX) are rejected outright.
  * 1:1 greedy assignment by score DESC across BOTH sources within a sys_id
    (each transcription substitutes at most one page, each page receives at
    most one transcription; a multi-page PGP doc thus upgrades only its
    best-matching page — conservative; per-window multi-page assignment is
    a possible follow-on).

Output db (data/fullcorpus_v2.db) contains ONLY:
  pages(page_id, sys_id, buckets, n_chars, text, provenance, fgp_id,
        fgp_score, htr_n_chars) + INDEX idx_sys ON pages(sys_id)
    provenance in ('htr','fgp','pgp'); fgp_id = FGP row id or PGP pgpid
    (per provenance); fgp_score = alignment score.
  stage0_sys_flags(sys_id PK, n_pages, n_fgp_rows, fgp_disagree)
    fgp_disagree = FGP edition rows > our page count (two-page-merge
    signal; FGP-based only — PGP documents carry no folio-count evidence).

Resume: builds into data/fullcorpus_v2.db.build; checkpoint json (atomic
tmp+os.replace) every CKPT_EVERY sys_ids. The bulk pages copy restarts from
scratch if interrupted, substitutions resume from the checkpoint; a
crash-window guard skips any sys whose committed pages already carry a
transcription. os.replace() to the final name only at the very end.
KNOWN LIMITATION (Codex r3 MEDIUM 1, accepted): the db commit and the json
checkpoint are not mutually atomic — a crash between them makes resume skip
the committed sys_ids (guard above), so the FINAL DB IS CORRECT but report
counters (score decades, letters, windows_cropped, samples) undercount that
one checkpoint window. Counters are report-only; no downstream consumer
reads them.

Usage:
  python -X utf8 -u mapv2_stage0.py                # full build
  python -X utf8 -u mapv2_stage0.py --sample 30    # validation sample ->
        data/fullcorpus_v2_SAMPLE.db (only the sampled sys_ids' pages),
        prints matched HTR/transcription pair previews for eyeballing.
"""
import argparse
import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402
from rapidfuzz.fuzz import partial_ratio_alignment  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
V1_DB = PROBE + r"\data\fullcorpus.db"          # READ-ONLY, never written
FGP_DB = ROOT + r"\fgp_data\fgp_transcriptions.db"  # READ-ONLY
PGP_DB = ROOT + r"\pgp_data\pgp.db"                 # READ-ONLY
OUT_DB = PROBE + r"\data\fullcorpus_v2.db"
BUILD_DB = OUT_DB + ".build"
CKPT_PATH = PROBE + r"\data\stage0_ckpt.json"
REPORT_PATH = PROBE + r"\results\stage0_report.md"

SAMPLE_DB = PROBE + r"\data\fullcorpus_v2_SAMPLE.db"

MIN_SCORE = 70.0    # partial-ratio floor to accept an HTR<->transcription match
MIN_LEN = 200       # min norm_stream length on both sides
COVER_MIN = 0.80    # shorter-transcription case: aligned window must cover
                    # >= this fraction of the HTR stream (partial-draft guard)
PREFILTER_MIN = 0.70  # skip alignment when len(tr)/len(htr) below this —
                      # coverage can never reach COVER_MIN
WINDOW_RATIO = 1.3  # transcription longer than this x HTR -> store only the
                    # aligned raw window (multi-page transcription guard)
WINDOW_PAD = 10     # stream letters of context kept each side of the window
                    # (kept SMALL: a bigger pad was seen pulling the work's
                    # TITLE header into the stored text — a title-fingerprint
                    # injection risk for Track-1)
RATIO_MAX = 12.0    # absurd transcription/HTR length ratio -> reject
CKPT_EVERY = 200    # checkpoint every N sys_ids
PROGRESS_EVERY = 500
COPY_BATCH = 2000

GATE_KEYS = ("no_candidate", "low_score", "short", "partial_coverage",
             "ratio", "lost_greedy")

GRAM_PRE_K = 5
GRAM_PRE_MIN = 0.08  # shared distinct 5-gram containment (vs the smaller
                     # set) below which a >=MIN_SCORE alignment is
                     # unreachable — skip the expensive alignment (the
                     # FRAG2-PLAN gate's "shared-gram overlap" leg; measured
                     # true HTR<->transcription pairs sit ~0.2-0.35 at 16-20%
                     # CER, unrelated Hebrew ~0.02-0.08; big codices waste
                     # thousands of alignments on non-matching pages — this
                     # cut the 40h ETA to ~2h). A PARTIAL draft still passes
                     # (coverage_frac x 0.3 >> 0.08 for any draft worth
                     # gating) and is then attributed 'partial_coverage' by
                     # the real gate.


def gram_set(s):
    return {s[i:i + GRAM_PRE_K] for i in range(len(s) - GRAM_PRE_K + 1)}


def open_ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def fresh_counters():
    return {
        "pages_substituted": 0,
        "by_source": {"fgp": 0, "pgp": 0},
        "windows_cropped": 0,
        "gate": {k: 0 for k in GATE_KEYS},
        "score_decades": {},           # "70"/"80"/"90"/"100" -> count
        "letters_htr_sub": 0,          # norm-stream letters of replaced HTR pages
        "letters_tr_sub": 0,           # norm-stream letters of substituting texts
        "sys_in_corpus": 0,
        "sys_not_in_corpus": 0,
        "fgp_disagree_in_corpus": 0,
        "samples": [],                 # up to 10 [page_id, src, id, score]
    }


def create_schema(con):
    con.execute("""CREATE TABLE pages (
        page_id TEXT PRIMARY KEY,
        sys_id TEXT NOT NULL,
        buckets TEXT NOT NULL,
        n_chars INTEGER,
        text TEXT,
        provenance TEXT,
        fgp_id INTEGER,
        fgp_score REAL,
        htr_n_chars INTEGER
    )""")
    con.execute("""CREATE TABLE stage0_sys_flags (
        sys_id TEXT PRIMARY KEY,
        n_pages INTEGER,
        n_fgp_rows INTEGER,
        fgp_disagree INTEGER
    )""")


def copy_pages(v1, con, restrict_sys=None):
    """Bulk copy v1 pages -> build db, provenance='htr'."""
    t0 = time.time()
    cur = v1.execute(
        "SELECT page_id, sys_id, buckets, n_chars, text FROM pages")
    n = 0
    batch = []
    for pid, sid, buckets, n_chars, text in cur:
        if restrict_sys is not None and sid not in restrict_sys:
            continue
        batch.append((pid, sid, buckets, n_chars, text, "htr",
                      None, None, n_chars))
        if len(batch) >= COPY_BATCH:
            con.executemany(
                "INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?)", batch)
            n += len(batch)
            batch = []
            if n % 100000 == 0:
                print(f"  copy: {n} pages ({time.time()-t0:.0f}s)", flush=True)
    if batch:
        con.executemany("INSERT INTO pages VALUES (?,?,?,?,?,?,?,?,?)", batch)
        n += len(batch)
    con.execute("CREATE INDEX idx_sys ON pages(sys_id)")
    con.commit()
    print(f"  copy done: {n} pages in {time.time()-t0:.0f}s", flush=True)
    return n


def load_fgp_sys_rows(fgp):
    """sys_id -> FGP edition row count (translations excluded)."""
    return dict(fgp.execute(
        "SELECT sys_id, COUNT(*) FROM fgp_transcriptions "
        "WHERE doc_relation='Digital Edition' AND sys_id IS NOT NULL "
        "AND content IS NOT NULL GROUP BY sys_id"))


def load_pgp_sys_docs(pgp):
    """sys_id -> sorted [pgpid, ...] for transcribed PGP documents (one
    document may map to several sys_ids — joins; each sys gets the doc as a
    candidate). DEDUPED + SORTED (Codex r3 MEDIUM 2: duplicate
    (sys_id, pgpid) fragment rows would let one transcription enter the
    greedy pool twice and substitute two pages; sorting keeps the candidate
    order — and thus equal-score tie-breaks — deterministic across SQLite
    query plans). Current data has 0 duplicate pairs; this is defensive."""
    out = {}
    for sid, pgpid in pgp.execute("""
            SELECT DISTINCT f.sys_id, d.pgpid
            FROM documents d
            JOIN document_fragments f ON f.document_id = d.pgpid
            WHERE d.has_transcription = 1 AND d.transcription IS NOT NULL
              AND LENGTH(d.transcription) > 100
              AND f.sys_id IS NOT NULL AND f.sys_id != ''"""):
        out.setdefault(sid, set()).add(pgpid)
    return {sid: sorted(v) for sid, v in out.items()}


def load_candidates(fgp, pgp, sid, pgp_docs_of):
    """[(source, id, stream, offs, raw, gramset)] candidates for sid."""
    cands = []
    for fid, content in fgp.execute(
            "SELECT id, content FROM fgp_transcriptions "
            "WHERE sys_id=? AND doc_relation='Digital Edition' "
            "AND content IS NOT NULL ORDER BY id", (sid,)):
        s, offs = norm_stream(content)
        if len(s) >= MIN_LEN:
            cands.append(("fgp", fid, s, offs, content, gram_set(s)))
    for pgpid in pgp_docs_of.get(sid, []):
        row = pgp.execute(
            "SELECT transcription FROM documents WHERE pgpid=?",
            (pgpid,)).fetchone()
        if not row or not row[0]:
            continue
        s, offs = norm_stream(row[0])
        if len(s) >= MIN_LEN:
            cands.append(("pgp", pgpid, s, offs, row[0], gram_set(s)))
    return cands


def gate_candidate(page_stream, tr_stream, tr_offs, tr_raw):
    """Apply score + coverage + window rules for one (page, transcription).

    Returns (score, stored_raw, cropped) on pass, or a gate-fail key str."""
    ratio = len(tr_stream) / len(page_stream)
    if ratio > RATIO_MAX:
        return "ratio"
    if ratio < PREFILTER_MIN:
        return "partial_coverage"       # cannot reach COVER_MIN
    if len(page_stream) <= len(tr_stream):
        # HTR aligned INSIDE the transcription -> full page covered
        res = partial_ratio_alignment(page_stream, tr_stream,
                                      score_cutoff=MIN_SCORE)
        if res is None or res.score < MIN_SCORE:
            return "low_score"
        if ratio > WINDOW_RATIO:
            # multi-page transcription: store only the aligned raw window
            # (+ pad), mapped back through the norm_stream offsets, so this
            # page never carries other pages' text
            w0 = max(0, res.dest_start - WINDOW_PAD)
            w1 = min(len(tr_stream), res.dest_end + WINDOW_PAD)
            if w1 <= w0:
                return "low_score"
            raw = tr_raw[tr_offs[w0]:tr_offs[w1 - 1] + 1]
            return (res.score, raw, True)
        return (res.score, tr_raw, False)
    # transcription SHORTER than the page: partial-draft danger zone —
    # require the aligned window to cover most of the HTR stream
    res = partial_ratio_alignment(tr_stream, page_stream,
                                  score_cutoff=MIN_SCORE)
    if res is None or res.score < MIN_SCORE:
        return "low_score"
    coverage = (res.dest_end - res.dest_start) / len(page_stream)
    if coverage < COVER_MIN:
        return "partial_coverage"
    return (res.score, tr_raw, False)


def substitute_sys(con, fgp, pgp, sid, pgp_docs_of, counters,
                   verbose_pairs=None):
    """Run the substitution gate for one transcription-bearing sys_id."""
    # Crash-window guard (Codex code-gate HIGH 4): if a db commit landed
    # but the json checkpoint write didn't, resume re-offers the committed
    # sys_ids. Their pages already carry transcription text — re-aligning
    # would treat it as the "HTR" stream (self-match, wrong greedy pool).
    # Commits cover whole sys_ids (never mid-sys), so ANY substituted page
    # proves this sys completed: skip it. (A committed sys with zero
    # substitutions reprocesses deterministically to the same zero outcome;
    # only its gate counters double-count — cosmetic, bounded by one
    # checkpoint window.)
    if con.execute("SELECT 1 FROM pages WHERE sys_id=? AND provenance!='htr' "
                   "LIMIT 1", (sid,)).fetchone():
        return
    pages = con.execute(
        "SELECT page_id, text FROM pages WHERE sys_id=?", (sid,)).fetchall()
    cands = load_candidates(fgp, pgp, sid, pgp_docs_of)

    page_state = {}                 # page_id -> gate state
    pstreams = {}                   # page_id -> stream
    passing = []                    # (score, page_id, cand_idx, raw, cropped)
    for pid, text in pages:
        s, _ = norm_stream(text or "")
        pstreams[pid] = s
        if len(s) < MIN_LEN:
            page_state[pid] = "short"
            continue
        if not cands:
            page_state[pid] = "no_candidate"
            continue
        worst = "no_candidate"
        # partial_coverage OUTRANKS low_score (Codex r3 LOW 3): a page with
        # one high-score-but-partial candidate and one unrelated low-score
        # candidate belongs to the partial-draft class — that's the signal
        # Hillel asked to see, don't let low_score mask it in the histogram
        RANK = {"ratio": 1, "low_score": 2, "partial_coverage": 3}
        got_pass = False
        gs_p = gram_set(s)
        for ci, (src, cid, ts, toffs, traw, tgs) in enumerate(cands):
            cont = (len(gs_p & tgs) / max(1, min(len(gs_p), len(tgs))))
            if cont < GRAM_PRE_MIN:
                # unreachable score — same class as low_score, skip alignment
                if RANK["low_score"] > RANK.get(worst, 0):
                    worst = "low_score"
                continue
            r = gate_candidate(s, ts, toffs, traw)
            if isinstance(r, str):
                if RANK.get(r, 0) > RANK.get(worst, 0):
                    worst = r
                continue
            score, raw, cropped = r
            passing.append((score, pid, ci, raw, cropped))
            got_pass = True
        page_state[pid] = "candidate" if got_pass else worst

    # greedy 1:1 assignment across BOTH sources, score DESC (deterministic)
    passing.sort(key=lambda t: (-t[0], t[1], t[2]))
    used_pages, used_cands = set(), set()
    for score, pid, ci, raw, cropped in passing:
        if pid in used_pages or ci in used_cands:
            continue
        used_pages.add(pid)
        used_cands.add(ci)
        src, cid, ts, _toffs, _traw, _tgs = cands[ci]
        con.execute(
            "UPDATE pages SET text=?, n_chars=?, provenance=?, "
            "fgp_id=?, fgp_score=? WHERE page_id=?",
            (raw, len(raw), src, cid, round(score, 2), pid))
        counters["pages_substituted"] += 1
        counters["by_source"][src] += 1
        if cropped:
            counters["windows_cropped"] += 1
        dec = str(min(100, int(score // 10) * 10))
        counters["score_decades"][dec] = \
            counters["score_decades"].get(dec, 0) + 1
        counters["letters_htr_sub"] += len(pstreams[pid])
        counters["letters_tr_sub"] += len(norm_stream(raw)[0])
        if len(counters["samples"]) < 10:
            counters["samples"].append([pid, src, cid, round(score, 2)])
        if verbose_pairs is not None:
            verbose_pairs.append((pid, src, cid, score, raw))

    for pid, state in page_state.items():
        if state == "candidate" and pid not in used_pages:
            counters["gate"]["lost_greedy"] += 1
        elif state != "candidate":
            counters["gate"][state] += 1


def write_report(con, counters, n_fgp_sys, n_pgp_sys, n_sys_union,
                 n_in_corpus, elapsed):
    total_pages = con.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    n_sub = con.execute(
        "SELECT COUNT(*) FROM pages WHERE provenance!='htr'").fetchone()[0]
    by_src = dict(con.execute(
        "SELECT provenance, COUNT(*) FROM pages WHERE provenance!='htr' "
        "GROUP BY provenance"))
    chars_before = con.execute(
        "SELECT SUM(htr_n_chars) FROM pages").fetchone()[0] or 0
    chars_after = con.execute(
        "SELECT SUM(n_chars) FROM pages").fetchone()[0] or 0
    n_disagree = con.execute(
        "SELECT COUNT(*) FROM stage0_sys_flags WHERE fgp_disagree=1"
    ).fetchone()[0]
    n_disagree_corpus = con.execute(
        "SELECT COUNT(*) FROM stage0_sys_flags "
        "WHERE fgp_disagree=1 AND n_pages>0").fetchone()[0]

    g = counters["gate"]
    lines = [
        "# Stage-0 report — fullcorpus_v2.db (transcription-preferred "
        "search text)",
        "",
        f"- generated: {time.strftime('%Y-%m-%d %H:%M:%S')}"
        f" (elapsed {elapsed/60:.1f} min)",
        f"- gate: score >= {MIN_SCORE}, both streams >= {MIN_LEN} letters, "
        f"COVERAGE >= {COVER_MIN} of the HTR stream when the transcription "
        f"is shorter (partial-draft guard), window-crop when longer than "
        f"{WINDOW_RATIO}x (multi-page guard), ratio cap {RATIO_MAX}, "
        f"greedy 1:1 per sys_id across both sources",
        "- sources: FGP doc_relation='Digital Edition' only (translations "
        "excluded); PGP documents.transcription (has_transcription=1, "
        "sys_id via document_fragments)",
        "",
        "## Corpus",
        f"- total pages: **{total_pages}**",
        f"- sys_ids with FGP edition rows: **{n_fgp_sys}**; with PGP "
        f"transcriptions: **{n_pgp_sys}**; union: **{n_sys_union}** "
        f"(of which in fullcorpus: **{n_in_corpus}**)",
        f"- pages substituted: **{n_sub}** — by source: {by_src}",
        f"- windows cropped (multi-page transcriptions): "
        f"{counters['windows_cropped']}",
        "",
        "## Gate-failure histogram (pages in transcription-bearing, "
        "in-corpus sys_ids that were NOT substituted)",
        f"- short (page stream < {MIN_LEN}): {g['short']}",
        f"- no_candidate (no transcription with stream >= {MIN_LEN}): "
        f"{g['no_candidate']}",
        f"- partial_coverage (transcription covers < {COVER_MIN} of the "
        f"page — Hillel's partial-draft class): {g['partial_coverage']}",
        f"- ratio (transcription > {RATIO_MAX}x page): {g['ratio']}",
        f"- low_score (best score < {MIN_SCORE}): {g['low_score']}",
        f"- lost_greedy (had a passing pair, lost 1:1 assignment): "
        f"{g['lost_greedy']}",
        "",
        "## Score distribution of substitutions (by decade)",
    ]
    for dec in sorted(counters["score_decades"], key=int):
        lines.append(f"- {dec}-{int(dec)+9 if dec != '100' else 100}: "
                     f"{counters['score_decades'][dec]}")
    lines += [
        "",
        "## Letters / chars",
        f"- norm-stream letters over substituted pages — HTR: "
        f"{counters['letters_htr_sub']}, transcription: "
        f"{counters['letters_tr_sub']} "
        f"(delta {counters['letters_tr_sub']-counters['letters_htr_sub']:+d})",
        f"- corpus raw chars (SUM n_chars) — before: {chars_before}, "
        f"after: {chars_after} (delta {chars_after-chars_before:+d})",
        "",
        "## fgp_disagree (FGP rows > n_pages — two-page-merge signal)",
        f"- total flagged sys_ids: {n_disagree}",
        f"- flagged AND in fullcorpus (n_pages > 0): {n_disagree_corpus}",
        "",
        "## 10 sample substituted pages",
        "",
        "| page_id | source | id | score |",
        "|---|---|---|---|",
    ]
    for pid, src, cid, score in counters["samples"]:
        lines.append(f"| {pid} | {src} | {cid} | {score} |")
    lines.append("")
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"report: {REPORT_PATH}", flush=True)


def save_ckpt(done, counters):
    tmp = CKPT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"pages_copied": True, "done": sorted(done),
                   "counters": counters}, f)
    os.replace(tmp, CKPT_PATH)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0,
                    help="limit to N transcription-bearing in-corpus sys_ids;"
                         " write to fullcorpus_v2_SAMPLE.db and print pair "
                         "previews")
    args = ap.parse_args()
    sample_n = args.sample
    t_start = time.time()

    v1 = open_ro(V1_DB)
    fgp = open_ro(FGP_DB)
    pgp = open_ro(PGP_DB)

    fgp_sys_rows = load_fgp_sys_rows(fgp)        # sys -> n edition rows
    pgp_docs_of = load_pgp_sys_docs(pgp)         # sys -> [pgpid]
    pages_per_sys = dict(v1.execute(
        "SELECT sys_id, COUNT(*) FROM pages GROUP BY sys_id"))
    all_tr_sys = sorted(set(fgp_sys_rows) | set(pgp_docs_of))
    in_corpus = [s for s in all_tr_sys if s in pages_per_sys]
    print(f"transcription sys_ids: FGP {len(fgp_sys_rows)}, "
          f"PGP {len(pgp_docs_of)}, union {len(all_tr_sys)} "
          f"(in corpus: {len(in_corpus)})", flush=True)

    if sample_n:
        sample_sys = in_corpus[:sample_n]
        sample_set = set(sample_sys)
        if os.path.exists(SAMPLE_DB):
            os.remove(SAMPLE_DB)
        con = sqlite3.connect(SAMPLE_DB)
        con.execute("PRAGMA journal_mode=MEMORY")
        con.execute("PRAGMA synchronous=OFF")
        create_schema(con)
        print(f"SAMPLE mode: {len(sample_sys)} sys_ids -> {SAMPLE_DB}",
              flush=True)
        copy_pages(v1, con, restrict_sys=sample_set)
        counters = fresh_counters()
        pairs = []
        for sid in sample_sys:
            counters["sys_in_corpus"] += 1
            n_pages = pages_per_sys.get(sid, 0)
            n_rows = fgp_sys_rows.get(sid, 0)
            disagree = 1 if n_rows > n_pages else 0
            con.execute("INSERT OR REPLACE INTO stage0_sys_flags "
                        "VALUES (?,?,?,?)", (sid, n_pages, n_rows, disagree))
            substitute_sys(con, fgp, pgp, sid, pgp_docs_of, counters,
                           verbose_pairs=pairs)
        con.commit()
        print(f"\nsubstituted {counters['pages_substituted']} pages "
              f"(by source {counters['by_source']}, windows cropped "
              f"{counters['windows_cropped']}); gate: {counters['gate']}",
              flush=True)
        print("\n--- matched pair previews (HTR page vs stored text) ---",
              flush=True)
        for pid, src, cid, score, raw in pairs[:12]:
            htr_text = v1.execute(
                "SELECT text FROM pages WHERE page_id=?", (pid,)
            ).fetchone()[0] or ""
            print(f"\n[{pid}]  {src}:{cid}  score={score:.1f}")
            print("  HTR: " + " ".join(htr_text[:120].split()))
            print(f"  {src.upper()}: " + " ".join(raw[:120].split()))
        con.close()
        print("\nSAMPLE done — eyeball the pairs above, then delete "
              f"{SAMPLE_DB}", flush=True)
        return

    # ------------------------- FULL BUILD -------------------------
    ckpt = None
    if os.path.exists(CKPT_PATH) and os.path.exists(BUILD_DB):
        with open(CKPT_PATH, encoding="utf-8") as f:
            ckpt = json.load(f)
        if not ckpt.get("pages_copied"):
            ckpt = None
    if ckpt is None and os.path.exists(BUILD_DB):
        os.remove(BUILD_DB)

    con = sqlite3.connect(BUILD_DB)
    con.execute("PRAGMA cache_size=-200000")
    if ckpt is None:
        con.execute("PRAGMA journal_mode=OFF")
        con.execute("PRAGMA synchronous=OFF")
        create_schema(con)
        print("phase A: copying v1 pages ...", flush=True)
        copy_pages(v1, con)
        counters = fresh_counters()
        done = set()
        save_ckpt(done, counters)
    else:
        counters = ckpt["counters"]
        for k in GATE_KEYS:
            counters["gate"].setdefault(k, 0)
        counters.setdefault("by_source", {"fgp": 0, "pgp": 0})
        counters.setdefault("windows_cropped", 0)
        done = set(ckpt["done"])
        print(f"RESUME: {len(done)} sys_ids already done "
              f"({counters['pages_substituted']} substituted)", flush=True)
    # durable-ish journaling for the incremental substitution phase
    con.execute("PRAGMA journal_mode=DELETE")
    con.execute("PRAGMA synchronous=NORMAL")

    print("phase B: transcription substitution ...", flush=True)
    t0 = time.time()
    n_done_this_run = 0
    todo = [s for s in all_tr_sys if s not in done]
    for i, sid in enumerate(todo, 1):
        n_pages = pages_per_sys.get(sid, 0)
        n_rows = fgp_sys_rows.get(sid, 0)
        disagree = 1 if n_rows > n_pages else 0
        con.execute("INSERT OR REPLACE INTO stage0_sys_flags VALUES (?,?,?,?)",
                    (sid, n_pages, n_rows, disagree))
        if n_pages == 0:
            counters["sys_not_in_corpus"] += 1
        else:
            counters["sys_in_corpus"] += 1
            if disagree:
                counters["fgp_disagree_in_corpus"] += 1
            substitute_sys(con, fgp, pgp, sid, pgp_docs_of, counters)
        done.add(sid)
        n_done_this_run += 1
        if n_done_this_run % CKPT_EVERY == 0:
            con.commit()
            save_ckpt(done, counters)
        if n_done_this_run % PROGRESS_EVERY == 0:
            rate = n_done_this_run / max(1e-9, time.time() - t0)
            eta = (len(todo) - i) / max(1e-9, rate)
            print(f"  sys {len(done)}/{len(all_tr_sys)} | substituted "
                  f"{counters['pages_substituted']} "
                  f"(fgp {counters['by_source']['fgp']} / pgp "
                  f"{counters['by_source']['pgp']}) | "
                  f"{rate:.1f} sys/s | ETA {eta/60:.0f} min", flush=True)
    con.commit()

    elapsed = time.time() - t_start
    write_report(con, counters, len(fgp_sys_rows), len(pgp_docs_of),
                 len(all_tr_sys), len(in_corpus), elapsed)
    con.close()
    os.replace(BUILD_DB, OUT_DB)
    if os.path.exists(CKPT_PATH):
        os.remove(CKPT_PATH)
    print(f"DONE: {OUT_DB} ({elapsed/60:.1f} min total)", flush=True)


if __name__ == "__main__":
    main()
