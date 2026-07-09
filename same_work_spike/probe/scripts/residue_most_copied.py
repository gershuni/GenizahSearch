# -*- coding: utf-8 -*-
"""B2 — residue mining: most-copied unidentified texts.

Ranks the passage-units residue (unlabeled / low-confidence units — 'the
discovery/residue census' from passage_units.py) by distinct-witness-MS
count, then attempts an AUTO-LABEL via catalog cross-reference for each of
the top ~200: do the members' OWN catalog metadata (libraries.csv col-7
title + FJMS `catalog`/`bibliography`, AlmaId==sys_id) converge on the same
work, even though Track-1's reference corpus doesn't have the text (so it
never got a Track-1 label)?

This directly reuses B3's (`frag_tail_catalog_check.py`) agreement/scoring
machinery — `is_informative_title` (EXTRA_GENERIC-filtered informativeness
gate), `content_weight`, `labels_equiv` (pairwise title-equivalence via
`track1_bib.title_bucket2`'s acronym/translation machinery) — UNMODIFIED,
imported directly, never edited. B3's setup was "does a NEW member's
catalog title agree with an identity derived from OLD members' Track-1
labels"; B2's residue units have NO Track-1 label at all (that's why
they're in the residue), so here the "identity" itself must be derived
purely from clustering the members' OWN catalog titles (falling back to a
low-confidence Track-1 label if the unit happens to carry one,
`t1_label`/`conf='low'` — 2 of the top-30 units do, per `units_full.md`).

B3's clustering (`UF`/union-find over 2-4 OLD members) and its
Bible/Talmud-commentary-tuned `EXTRA_GENERIC` list do not survive scaling
up to this liturgy/poetry-heavy residue's 10-150+ candidates per unit
unchanged — two additive, NON-invasive fixes were needed (development
trail + concrete false-positive/false-label cases are in the code
comments and in the report below; `frag_tail_catalog_check.py` /
`track1_bib.py` themselves are untouched throughout):
  (a) clip the string fed to `labels_equiv` to each candidate's leading
      content tokens, and additionally strip tokens with high empirical
      document-frequency across THIS residue's own candidate pool (a
      data-driven generalization of `EXTRA_GENERIC` for the
      liturgy/poetry/archival-classification vocabulary that dominates
      RNL/Bodleian catalog fields, which B3's Bible/Talmud-tuned list
      doesn't cover);
  (b) cluster by a non-transitive ANCHOR (one-hop star, not `UF`'s
      transitive closure) — at n=50-150+ candidates, a small number of
      false pairwise edges chain-collapse unrelated works into one
      spurious component under transitive closure.

Convergence rule (unit auto-labels as "catalog knows this work, Track-1's
reference corpus doesn't"):
  1. Build one candidate label per DISTINCT sys_id in the unit (computed
     ONCE globally, not per unit): its best informative Track-1 label if
     the unit's member row for that sys_id carries one
     (passage_unit_members.t1_label, already computed by
     passage_units.py's label-propagation pass), else its best informative
     catalog title (libraries.csv col-7 / FJMS `catalog`, picked by
     `content_weight`) — both clipped + DF-stripped per (a) above. Sys_ids
     with NO informative signal left contribute no candidate.
  2. Score all direct (one-hop) pairwise `labels_equiv` edges among a
     unit's candidates; the ANCHOR = the candidate with the most direct
     edges (ties -> Track-1-sourced, then weight). Cluster = anchor + its
     direct neighbors (no transitive closure).
  3. AUTO-LABELED iff >=2 candidates exist AND the cluster is a STRICT
     MAJORITY (> 50%) of all candidates AND has >= 2 members ("a
     meaningful fraction of members' catalogs converge on the same
     work" — majority-of-non-generic-titles, per the brief). The
     representative label = the ANCHOR itself (the best-SUPPORTED
     candidate, not a re-pick favoring Track-1 — see the unit-9140
     case in the code comments for why that re-pick is wrong).
  4. SUGGESTIVE (reported but NOT counted as auto-labeled): exactly one
     candidate, or a cluster that is a plurality but not a majority —
     some catalog signal exists but it's not "a meaningful fraction
     converging".
  5. NO-CATALOG: zero informative candidates anywhere in the unit — the
     true discovery case (unattested in Track-1's ref corpus AND
     uncataloged/generically-cataloged everywhere else). None of the
     top-200 landed here (see report) — at this witness-count scale,
     >=1 of many distinct MSS almost always carries SOME catalog title.

Usage: python -X utf8 -u residue_most_copied.py
Out: ../results/b2_residue_most_copied.md
     ../results/b2_residue_most_copied.json
     ../review/b2_residue_review.html
"""
import csv
import html
import json
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict

from normalize import norm_stream
from track1_bib import FjmsInfo, load_acronym_equiv, norm_title
from frag_tail_catalog_check import (
    content_weight, is_informative_title, labels_equiv,
    load_lib_meta as _load_lib_meta_full,
)

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
UNITS_TABLE = "passage_units_accepted_pairs_canonmask"
MEMBERS_TABLE = "passage_unit_members_accepted_pairs_canonmask"
CONTINUUM_UNIT = 367274      # quarantined per brief — the 18,676-MS chain
TOP_N = 200

MD_OUT = ROOT + r"\same_work_spike\probe\results\b2_residue_most_copied.md"
JSON_OUT = ROOT + r"\same_work_spike\probe\results\b2_residue_most_copied.json"
HTML_OUT = ROOT + r"\same_work_spike\probe\review\b2_residue_review.html"

P_RE = re.compile(r'_P(\d+)_')


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)

    cols = [r[1] for r in con.execute(f"PRAGMA table_info({UNITS_TABLE})")]
    assert 'labeled' in cols, "schema check: passage_units table missing " \
        "'labeled' column -- schema drift, see passage_units.py"
    mcols = [r[1] for r in con.execute(f"PRAGMA table_info({MEMBERS_TABLE})")]
    HAS_SUSPECT = 'suspect' in mcols or 'suspect' in cols
    print(f"schema check: no 'suspect' flag column in {UNITS_TABLE}/"
          f"{MEMBERS_TABLE} (checked) -- only the continuum unit "
          f"{CONTINUUM_UNIT} is quarantined per the brief.",
          flush=True)

    # ---- rank residue units by distinct-witness-MS count ----
    unit_rows = con.execute(f"""
        SELECT unit, n_pages, n_ms, med_len, roles, libs, t1_label, t1_n, conf
        FROM {UNITS_TABLE}
        WHERE labeled = 0 AND unit != {CONTINUUM_UNIT}
        ORDER BY n_ms DESC""").fetchall()
    total_residue = len(unit_rows)
    top = unit_rows[:TOP_N]
    print(f"residue census: {total_residue:,} unlabeled/low-conf units "
          f"(continuum unit {CONTINUUM_UNIT} excluded); "
          f"top {len(top):,} taken by witness-MS count "
          f"({time.time() - t0:.0f}s)", flush=True)

    top_units = [r[0] for r in top]
    unit_meta = {r[0]: {
        'n_pages': r[1], 'n_ms': r[2], 'med_len': r[3],
        'roles': json.loads(r[4]), 'libs_top4': json.loads(r[5]),
        't1_label': r[6], 't1_n': r[7], 'conf': r[8],
    } for r in top}

    # ---- members of the top-N units ----
    ph = ','.join('?' * len(top_units))
    mem_rows = con.execute(
        f"SELECT unit, page_id, sys_id, start, end, cov, role, t1_label "
        f"FROM {MEMBERS_TABLE} WHERE unit IN ({ph})", top_units).fetchall()
    members_by_unit = defaultdict(list)
    for u, pid, sid, s, e, cov, role, t1lab in mem_rows:
        members_by_unit[u].append({
            'page_id': pid, 'sys_id': sid, 'start': s, 'end': e,
            'cov': cov, 'role': role, 't1_label': t1lab or '',
        })
    print(f"members loaded for top-{len(top_units)}: {len(mem_rows):,} rows "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- catalog metadata (libraries.csv + FJMS, AlmaId==sys_id) ----
    lib_meta = _load_lib_meta_full()
    print(f"libraries.csv: {len(lib_meta):,} sys_ids "
          f"({time.time() - t0:.0f}s)", flush=True)
    all_sys = {m['sys_id'] for ms in members_by_unit.values() for m in ms}
    fjms = FjmsInfo(all_sys)
    equiv = load_acronym_equiv()
    print(f"fjms: titles for {len(fjms.titles):,} / bib for "
          f"{len(fjms.bib):,} of {len(all_sys):,} relevant sys_ids "
          f"({time.time() - t0:.0f}s)", flush=True)

    def cat_titles_of(sid):
        lib_title = lib_meta.get(sid, (sid, '?', ''))[2]
        out = [t for t in [lib_title] + fjms.titles.get(sid, [])
               if t and t.strip()]
        seen, uniq = set(), []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq

    # ---- per-sid candidate labels (computed ONCE globally, not per unit) --
    # B2-local guard (does NOT touch frag_tail_catalog_check.py /
    # track1_bib.py): RNL/Bodleian catalog fields for composite prayer-book
    # /miscellany manuscripts list MANY distinct bound items in one string
    # (e.g. "פיוט. ; Piyyut: \"יעלה ויבוא\" for Rosh ha-Shanah)\"קול שופר
    # הדרור\"...; Common Prayers: Musaf Rosh ha-Shanah ; ..." -- a single
    # sys_id's OWN catalog title enumerating ~7 different liturgical
    # pieces). B3's EXTRA_GENERIC/has_specific_overlap guard was tuned on
    # FJMS's per-work catalog titles for a Bible/Talmud-commentary corpus;
    # against this liturgy/poetry-heavy residue it still spuriously
    # 'match'es -- both because these blobs are so token-rich that SOME
    # overlap is near-guaranteed, AND because EXTRA_GENERIC doesn't cover
    # liturgy/poetry-genre vocabulary (פיוט/שירת/קראים/סדור/תפילה...) or
    # this corpus's archival CONTENT-TYPE classification tags
    # ("עזרי הוראה, נסיונות קולמוס..." = "teaching aids, pen trials..." --
    # boilerplate attached to thousands of unrelated fragments). Two
    # additive, non-invasive fixes (spot-checked against unit 59088, whose
    # candidates visibly include BOTH "דיואן ר' יהודה הלוי" (Yehuda
    # ha-Levi's Diwan) AND "סדור מנהג קראים" (a Karaite prayer-rite
    # siddur) -- unrelated works that a naive matcher conflates):
    #   (1) clip the MATCHING input to the leading MATCH_CLIP_TOKENS
    #       content tokens (author/work name is front-loaded; the tail is
    #       catalog padding/kitchen-sink enumeration);
    #   (2) additionally strip tokens with high DOCUMENT FREQUENCY across
    #       this residue's OWN candidate pool (empirically-detected
    #       promiscuous connector words -- the DF-based generalization of
    #       B3's manually-curated EXTRA_GENERIC for a different domain).
    # Both apply ONLY to the matching string passed to `labels_equiv`; the
    # FULL raw title is kept separately for display, so no evidence is
    # lost in the report/review page.
    MATCH_CLIP_TOKENS = 10
    DF_GENERIC_FRAC = 0.02     # empirical: >=2% of candidate sys_ids

    def clip_for_match(title):
        toks = norm_title(title).split()
        return ' '.join(toks[:MATCH_CLIP_TOKENS])

    # pass 1: best raw (clipped) candidate per sid, ignoring unit grouping
    sid_raw = {}      # sid -> (clip_title, source, weight, display_title)
    members_by_sid = defaultdict(list)
    for ms in members_by_unit.values():
        for m in ms:
            members_by_sid[m['sys_id']].append(m)
    for sid in sorted(all_sys):
        best_t1 = None
        for m in members_by_sid[sid]:
            lab = m['t1_label']
            if not lab:
                continue
            clipped = clip_for_match(lab)
            if is_informative_title(clipped):
                w = content_weight(clipped)
                if best_t1 is None or w > best_t1[1]:
                    best_t1 = (clipped, w, lab)
        if best_t1 is not None:
            sid_raw[sid] = (best_t1[0], 'track1', 1000 + best_t1[1],
                             best_t1[2])
            continue
        best_cat = None
        for title in cat_titles_of(sid):
            clipped = clip_for_match(title)
            if is_informative_title(clipped):
                w = content_weight(clipped)
                if best_cat is None or w > best_cat[1]:
                    best_cat = (clipped, w, title)
        if best_cat is not None:
            sid_raw[sid] = (best_cat[0], 'catalog', best_cat[1], best_cat[2])

    # pass 2: empirical DF of matching-tokens across the candidate pool
    df = Counter()
    for clip_title, _src, _w, _disp in sid_raw.values():
        for t in set(clip_title.split()):
            df[t] += 1
    n_cand_sids = max(1, len(sid_raw))
    LOCAL_GENERIC = {t for t, c in df.items() if c / n_cand_sids
                      >= DF_GENERIC_FRAC}
    print(f"empirical DF-generic strip list: {len(LOCAL_GENERIC):,} tokens "
          f"(>= {DF_GENERIC_FRAC:.0%} of {n_cand_sids:,} candidate sys_ids) "
          f"-- top: {[t for t, _ in df.most_common(15)]} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # pass 3: final per-sid candidate = raw clip with LOCAL_GENERIC tokens
    # stripped; re-check informativeness on what's left (a candidate whose
    # ENTIRE signal was promiscuous connector words is demoted to no
    # candidate at all, same as never having had a catalog title).
    sid_cand = {}    # sid -> (match_title, source, weight, display_title)
    for sid, (clip_title, src, w, disp) in sid_raw.items():
        stripped = ' '.join(t for t in clip_title.split()
                            if t not in LOCAL_GENERIC)
        if is_informative_title(stripped):
            sid_cand[sid] = (stripped, src, w, disp)
    print(f"candidates surviving DF-strip: {len(sid_cand):,} / "
          f"{len(sid_raw):,} sids ({time.time() - t0:.0f}s)", flush=True)

    # ---- per-unit auto-label attempt ----
    def group_key(candidates, idxs):
        has_t1 = any(candidates[i][2] == 'track1' for i in idxs)
        return (len(idxs), has_t1, sum(candidates[i][4] for i in idxs))

    verdicts = {}     # unit -> dict
    n_auto = n_suggestive = n_nocatalog = 0
    for u in top_units:
        ms = members_by_unit[u]
        sids_in_unit = sorted({m['sys_id'] for m in ms})
        candidates = []   # (author, match_title, source, sid, weight, disp)
        for sid in sids_in_unit:
            c = sid_cand.get(sid)
            if c is not None:
                match_title, src, w, disp = c
                candidates.append(('', match_title, src, sid, w, disp))
        n = len(candidates)
        if n == 0:
            verdicts[u] = {
                'verdict': 'NO-CATALOG', 'label': '', 'label_source': '',
                'n_candidates': 0, 'cluster_size': 0, 'n_groups': 0,
                'candidates': [],
            }
            n_nocatalog += 1
            continue
        # B2-local clustering choice (also does NOT touch
        # frag_tail_catalog_check.py -- same non-invasive-guard pattern as
        # the clipping above): B3's transitive union-find (`UF`) is safe
        # over its ORIGINAL use case (a handful of OLD members per motif,
        # n typically 2-4). Here n can be 50-150+ candidates per unit, and
        # transitive closure over a noisy pairwise-equivalence graph at
        # that scale chain-collapses unrelated works into one giant
        # spurious 'cluster' (spot-checked: unit 59088's candidates
        # visibly include BOTH "דיואן ר' יהודה הלוי" (Yehuda ha-Levi's
        # Diwan) AND "סדור מנהג קראים" (a Karaite prayer-rite siddur) --
        # unrelated works transitively chained via intermediate
        # near-matches -- yet UF merged ~90% of all 84 candidates into
        # one component). Fix: score each candidate as an ANCHOR by its
        # DIRECT (one-hop, non-transitive) equivalence-edge count and take
        # the best anchor's direct-match set as the cluster -- "how many
        # OTHER members' catalogs directly agree with THIS specific
        # label", never merged through a third party.
        edges = [[] for _ in range(n)]
        for i in range(n):
            for j in range(i + 1, n):
                a = (candidates[i][0], candidates[i][1])
                b = (candidates[j][0], candidates[j][1])
                if labels_equiv(a, b, equiv) or labels_equiv(b, a, equiv):
                    edges[i].append(j)
                    edges[j].append(i)

        def anchor_key(i):
            return (len(edges[i]), candidates[i][2] == 'track1',
                    candidates[i][4])
        anchor = max(range(n), key=anchor_key)
        best_idxs = [anchor] + edges[anchor]
        n_groups = 1 if len(best_idxs) == n else 2   # informational only
        # representative = the ANCHOR itself, not a re-pick from within
        # best_idxs. Spot-check finding (unit 9140): re-picking "the best
        # track1-sourced member of best_idxs" as representative surfaced a
        # single SPURIOUS edge (a shared-given-name collision: "יצחק" is
        # both Rashi's patronymic, שלמה בן יצחק, and Rif's given name,
        # יצחק אלפסי -- `load_acronym_equiv`'s author-name expansion
        # matches on that bare common name, not a real identity link) and
        # displayed "רש\"י -- פירוש לתורה" as the unit's label, even though
        # the anchor with by far the strongest support (15/26 direct
        # edges) was "הלכות הרי\"ף (ברכות, שבת)" -- the actually-correct
        # convergence (this unit's real content is Alfasi's Halakhot on
        # Shabbat/the underlying Gemara passage, catalogued inconsistently
        # as "הלכות הרי\"ף" or "תלמוד בבלי שבת" by different institutions,
        # which do genuinely overlap). The anchor is BY CONSTRUCTION the
        # best-supported node (max direct-edge count, ties broken toward
        # track1); using it directly as the representative avoids letting
        # one low-confidence, possibly-spurious edge hijack the display
        # label. The underlying false-edge risk (common personal names
        # colliding through equiv's author-acronym expansion) is a real,
        # NOT fully fixed, limitation of reusing `labels_equiv` at this
        # scale -- documented, not patched (see report).
        rep = candidates[anchor]
        majority = len(best_idxs) >= 2 and len(best_idxs) > n / 2
        verdict = 'AUTO-LABELED' if majority else 'SUGGESTIVE'
        if majority:
            n_auto += 1
        else:
            n_suggestive += 1
        verdicts[u] = {
            'verdict': verdict, 'label': rep[5], 'label_source': rep[2],
            'n_candidates': n, 'cluster_size': len(best_idxs),
            'n_groups': n_groups,
            'candidates': [(c[0], c[5], c[2], c[3]) for c in candidates],
        }
    print(f"auto-label attempt: AUTO-LABELED {n_auto:,} · SUGGESTIVE "
          f"{n_suggestive:,} · NO-CATALOG {n_nocatalog:,} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- library spread (by distinct witness MS) + text snippets ----
    _page_cache = {}

    def page_view(pid):
        if pid not in _page_cache:
            row = con.execute("SELECT text FROM pages WHERE page_id=?",
                              (pid,)).fetchone()
            tx = row[0] if row else ''
            _page_cache[pid] = (tx, *norm_stream(tx))
            if len(_page_cache) > 500:
                _page_cache.pop(next(iter(_page_cache)))
        return _page_cache[pid]

    def orig_slice(pid, s, e, cap=400):
        tx, stream, offs = page_view(pid)
        if not len(offs) or s >= len(offs):
            return ''
        e = min(e, len(offs))
        if e <= s:
            return ''
        frag = tx[offs[s]:offs[e - 1] + 1]
        return frag[:cap] + ('…' if len(frag) > cap else '')

    unit_reports = []
    for u in top_units:
        ms = members_by_unit[u]
        by_sid = defaultdict(list)
        for m in ms:
            by_sid[m['sys_id']].append(m)
        lib_counts = Counter()
        for sid in by_sid:
            lib_counts[lib_meta.get(sid, (sid, '?', ''))[1]] += 1
        # 1-2 evidence snippets: longest occurrences, distinct sys_ids
        ranked = sorted(ms, key=lambda m: -(m['end'] - m['start']))
        snips = []
        seen_sid = set()
        for m in ranked:
            if m['sys_id'] in seen_sid:
                continue
            seen_sid.add(m['sys_id'])
            sm, lib, cat_title = lib_meta.get(
                m['sys_id'], (m['sys_id'], '?', ''))
            snips.append({
                'sys_id': m['sys_id'], 'shelfmark': sm, 'library': lib,
                'page_id': m['page_id'], 'page_num': pnum(m['page_id']),
                'cat_title': cat_title,
                'fjms_titles': fjms.titles.get(m['sys_id'], []),
                'text': orig_slice(m['page_id'], m['start'], m['end']),
                'browse_url': (f"https://genizahsearch.com/browse?"
                                f"sys_id={m['sys_id']}"
                                f"&page={pnum(m['page_id'])}"),
            })
            if len(snips) >= 2:
                break

        v = verdicts[u]
        um = unit_meta[u]
        # full member list for the review card (capped)
        mem_list = []
        for sid in sorted(by_sid,
                           key=lambda s: -max(m['end'] - m['start']
                                              for m in by_sid[s])):
            best = max(by_sid[sid], key=lambda m: m['end'] - m['start'])
            sm, lib, cat_title = lib_meta.get(sid, (sid, '?', ''))
            mem_list.append({
                'sys_id': sid, 'shelfmark': sm, 'library': lib,
                'cat_title': cat_title,
                'page_id': best['page_id'], 'page_num': pnum(best['page_id']),
                'role': best['role'], 'cov': best['cov'],
                't1_label': best['t1_label'],
                'browse_url': (f"https://genizahsearch.com/browse?"
                                f"sys_id={sid}&page={pnum(best['page_id'])}"),
            })

        unit_reports.append({
            'unit': u, 'n_ms': um['n_ms'], 'n_pages': um['n_pages'],
            'med_len': um['med_len'], 'roles': um['roles'],
            'lib_counts_by_ms': dict(lib_counts.most_common()),
            'weak_t1_label': um['t1_label'], 'weak_t1_conf': um['conf'],
            'verdict': v['verdict'], 'auto_label': v['label'],
            'label_source': v['label_source'],
            'n_candidates': v['n_candidates'],
            'cluster_size': v['cluster_size'], 'n_groups': v['n_groups'],
            'candidates': v['candidates'],
            'snippets': snips,
            'members': mem_list,
        })

    print(f"unit reports built ({time.time() - t0:.0f}s)", flush=True)

    # ---- full-scale library distribution (the 70%-RNL check) ----
    all_residue_units = [r[0] for r in unit_rows]   # all 73,101, no cap
    ph2 = ','.join('?' * len(all_residue_units))
    full_lib_by_ms = Counter()
    full_lib_by_pagerow = Counter()
    sid_seen = set()
    BATCH = 3000
    for i in range(0, len(all_residue_units), BATCH):
        batch = all_residue_units[i:i + BATCH]
        ph3 = ','.join('?' * len(batch))
        for sid, in con.execute(
                f"SELECT sys_id FROM {MEMBERS_TABLE} WHERE unit IN ({ph3})",
                batch):
            lib = lib_meta.get(sid, (sid, '?', ''))[1]
            full_lib_by_pagerow[lib] += 1
            if sid not in sid_seen:
                sid_seen.add(sid)
                full_lib_by_ms[lib] += 1
    tot_ms = sum(full_lib_by_ms.values())
    tot_pr = sum(full_lib_by_pagerow.values())
    rnl_frac_ms = full_lib_by_ms.get('RNL', 0) / max(1, tot_ms)
    rnl_frac_pr = full_lib_by_pagerow.get('RNL', 0) / max(1, tot_pr)
    print(f"full-scale residue library check: {tot_ms:,} distinct MSS, "
          f"{tot_pr:,} member-page-rows; RNL {rnl_frac_ms:.1%} of MSS, "
          f"{rnl_frac_pr:.1%} of page-rows ({time.time() - t0:.0f}s)",
          flush=True)

    fjms.close()
    con.close()

    write_report(unit_reports, total_residue, n_auto, n_suggestive,
                 n_nocatalog, full_lib_by_ms, full_lib_by_pagerow,
                 rnl_frac_ms, rnl_frac_pr, tot_ms, tot_pr, t0)
    write_html(unit_reports)
    print(f"total time: {time.time() - t0:.0f}s")


def esc(x):
    return html.escape(str(x))


def mesc(x):
    """Markdown-table-safe (NOT HTML-escaped): neutralize pipes/newlines
    only. `esc`/html.escape is for the HTML review page; using it in the
    .md report corrupts Hebrew quote marks into &quot;/&#x27; entities."""
    return str(x).replace('|', '\\|').replace('\n', ' ').replace('\r', ' ')


def fmt_candidates(cands):
    if not cands:
        return '(none)'
    by_src = Counter(c[2] for c in cands)
    parts = [f"{c[1][:60]} [{c[2]}:{c[3]}]" for c in cands[:6]]
    more = f" … +{len(cands) - 6} more" if len(cands) > 6 else ''
    return (f"{len(cands)} candidates ({dict(by_src)}): "
            + '; '.join(parts) + more)


def write_report(unit_reports, total_residue, n_auto, n_suggestive,
                 n_nocatalog, full_lib_by_ms, full_lib_by_pagerow,
                 rnl_frac_ms, rnl_frac_pr, tot_ms, tot_pr, t0):
    auto = [r for r in unit_reports if r['verdict'] == 'AUTO-LABELED']
    other = [r for r in unit_reports
             if r['verdict'] in ('SUGGESTIVE', 'NO-CATALOG')]

    lines = [
        "# B2 — residue mining: most-copied unidentified texts",
        "",
        f"Generated {time.strftime('%Y-%m-%d %H:%M')}. Substrate: "
        f"`fullcorpus.db` tables `passage_units_accepted_pairs_canonmask` "
        f"(81,365 units) / `passage_unit_members_accepted_pairs_canonmask` "
        f"(412,471 rows) — the fresh rebuild with live Track-1 (competitive "
        f"span assignment) labels. Read-only, light SQLite/CSV reads, no "
        f"engine runs.",
        "",
        "## Method",
        "",
        f"1. **Residue census** = units with `labeled=0` (no confident "
        f"Track-1 label), continuum unit 367274 (18,676 MSS) excluded per "
        f"the brief. **{total_residue:,} units.** No other `suspect`-style "
        f"flag exists in the `passage_units`/`passage_unit_members` schema "
        f"(checked `PRAGMA table_info`) — only the continuum unit needed "
        f"quarantine.",
        f"2. Ranked by **distinct witness-MS count** (`n_ms`), top "
        f"**{len(unit_reports):,}** taken.",
        "3. **Auto-label attempt** per unit, reusing "
        "`frag_tail_catalog_check.py` (B3)'s scoring machinery UNMODIFIED "
        "(`is_informative_title`, `content_weight`, `labels_equiv` — "
        "itself built on `track1_bib.title_bucket2`'s acronym/translation "
        "equivalence tables; `frag_tail_catalog_check.py`/`track1_bib.py` "
        "are NEVER edited): one candidate label per distinct sys_id (its "
        "own low-confidence Track-1 label if the unit carries one for "
        "that member, else its best informative catalog title from "
        "libraries.csv/FJMS).",
        "4. **Two additive, non-invasive fixes were required before the "
        "reused scorer produced trustworthy results at THIS scale** "
        "(B3 was tuned for 2-4 OLD members per motif and a "
        "Bible/Talmud-commentary corpus; this residue has 10-150+ "
        "candidates per unit and is liturgy/poetry/RNL-miscellany-heavy "
        "— full development trail with concrete before/after cases is in "
        "the script's comments):",
        "   - **(a) clip + empirical DF-strip.** RNL/Bodleian catalog "
        "fields for composite prayer-book manuscripts enumerate MANY "
        "bound items in one string (unit 59088's sys_id 990053433000... "
        "catalog title alone lists 7 different Rosh-ha-Shanah piyyutim). "
        "The matching input is clipped to each candidate's leading 10 "
        "content tokens, then further stripped of tokens whose document "
        "frequency across this residue's OWN candidate pool exceeds 2% "
        "(an empirically-derived, data-driven generalization of B3's "
        "hand-curated `EXTRA_GENERIC` for THIS corpus's liturgy/poetry/"
        "archival-classification vocabulary — top stripped tokens: פיוט, "
        "קטע, בן, מקרא, קטעים, פרוש, תרגום, קראים, טקסט, כתאב, תפילה, על, "
        "ערבי, וברכות, מנהג; 46 tokens total, see run log). Without this, "
        "175/200 units falsely 'AUTO-LABELED' by generic liturgical-genre "
        "overlap alone (verified by hand on unit 59088: candidates "
        "visibly include BOTH \"דיואן ר' יהודה הלוי\" (Yehuda ha-Levi's "
        "Diwan) and \"סדור מנהג קראים\" (a Karaite siddur) — unrelated "
        "works).",
        "   - **(b) non-transitive ANCHOR clustering, not `UF`.** At "
        "n=50-150+ candidates, B3's transitive union-find chain-collapses "
        "unrelated works through a handful of false pairwise edges "
        "(spot-checked: pre-fix, unit 59088's 84 candidates merged ~90% "
        "into one component under `UF`). Switched to a one-hop STAR: the "
        "ANCHOR = the candidate with the most direct (non-transitive) "
        "`labels_equiv` edges; cluster = anchor + its direct neighbors "
        "only. The representative label is the ANCHOR ITSELF, not a "
        "re-pick favoring Track-1-sourced members of the cluster — an "
        "earlier version of this script did that re-pick and it "
        "surfaced a genuine bug (unit 9140: a single spurious edge from "
        "a shared-GIVEN-NAME collision — `יצחק` is both Rashi's "
        "patronymic, שלמה בן יצחק, and Rif's given name, יצחק אלפסי, so "
        "`track1_bib`'s author-acronym equivalence table linked them — "
        "displayed \"Rashi's Torah commentary\" when the actual, "
        "best-supported (15/26 direct edges) identity was \"הלכות הרי"
        "\"ף\"/Alfasi's Halakhot on Shabbat, which the underlying text "
        "verified as correct). **This exact false-edge mechanism (rare "
        "common-name collisions in the acronym-equivalence table) is a "
        "real, NOT fully fixed residual limitation of reusing "
        "`labels_equiv` unmodified at this scale — documented, not "
        "patched, consistent with the brief's reuse-don't-reinvent "
        "instruction.**",
        "5. **Convergence rule: AUTO-LABELED iff >=2 candidates exist and "
        "the anchor cluster is a STRICT MAJORITY (>50%) with >=2 "
        "members** — 'a meaningful fraction of members' catalogs "
        "converge on the same work'. SUGGESTIVE = some catalog signal "
        "but no majority convergence. NO-CATALOG = zero informative "
        "candidates anywhere in the unit.",
        "",
        "## Headline split",
        "",
        f"- **(a) Auto-labeled by catalog** (Track-1 reference-gap: the "
        f"work exists in catalogs/bibliography but not in Track-1's "
        f"reference corpus): **{n_auto:,}** / {len(unit_reports):,} of "
        f"the top units.",
        f"- **(b) Truly unidentified** (discovery queue: no catalog "
        f"convergence, incl. SUGGESTIVE + NO-CATALOG): "
        f"**{n_suggestive + n_nocatalog:,}** ({n_suggestive:,} SUGGESTIVE "
        f"+ {n_nocatalog:,} NO-CATALOG).",
        "",
        "## The 70%-RNL Karaite-liturgy hypothesis — full-scale check",
        "",
        f"Computed over the FULL {total_residue:,}-unit residue (not just "
        f"the top {len(unit_reports)}): {tot_ms:,} distinct witness MSS, "
        f"{tot_pr:,} member-page-rows (one row per unit×page a MS "
        f"participates in — a MS that recurs across many different "
        f"residue units contributes one row per unit).",
        "",
        f"- **By distinct witness MS: RNL = {rnl_frac_ms:.1%}** "
        f"({full_lib_by_ms.get('RNL', 0):,} / {tot_ms:,}). "
        f"CUL is actually the plurality library by this measure "
        f"({full_lib_by_ms.get('CUL', 0):,}, "
        f"{full_lib_by_ms.get('CUL', 0) / max(1, tot_ms):.1%}).",
        f"- **By member-page-row (occurrence count): RNL = "
        f"{rnl_frac_pr:.1%}** ({full_lib_by_pagerow.get('RNL', 0):,} / "
        f"{tot_pr:,}) — THIS is where the ~70% figure lives.",
        "- **Verdict: the 70% signal is real but is a concentration "
        "effect, not a distinct-manuscript effect.** Only "
        f"{full_lib_by_ms.get('RNL', 0):,} distinct RNL MSS are involved "
        "(24.5% of witnesses), but a handful of them recur across "
        "hundreds of DIFFERENT residue units each — e.g. sys_id "
        "990001538710205171 (`Ms. EVR ARAB I 2064`, catalogued "
        "\"תרגום ופרוש ערבי לתורה לישועה בן יהודה (דברים)\" — Yeshua ben "
        "Yehuda's Judeo-Arabic Torah commentary) alone touches 535 "
        "distinct residue units. The top RNL contributors checked by "
        "hand are large Judeo-Arabic Karaite Bible-commentary/"
        "philological codices (al-Qirqisani's כתאב אלאנואר ואלמראקב, ibn "
        "Janah's ספר השרשים/כתאב אלאצול, Yosef ben Noah's Torah "
        "commentary, מדרש דוד) — running discursive commentary prose "
        "whose surrounding Arabic argument never matches the reference "
        "corpus (only its embedded Bible citations do), so it fragments "
        "into hundreds of small residue units per codex. Genuinely "
        "liturgical Karaite items also appear in the top-30 by witness "
        "count (units 666840 \"פיוטי מאורה ואהבה\", 695000 \"קדושת היום "
        "במוסף לרגלים\") but the RNL page-row mass is dominated by "
        "**commentary/philology, not liturgy specifically** — the "
        "hypothesis should be re-stated as 'RNL's uncatalogued Karaite "
        "philological corpus', not 'Karaite liturgy'.",
        "",
        "## Library distribution, full residue (by distinct MS)",
        "",
    ]
    for lib, n in full_lib_by_ms.most_common(12):
        lines.append(f"- {lib}: {n:,} ({n / max(1, tot_ms):.1%})")
    lines += ["", "## Library distribution, full residue (by member-page-row)",
             ""]
    for lib, n in full_lib_by_pagerow.most_common(12):
        lines.append(f"- {lib}: {n:,} ({n / max(1, tot_pr):.1%})")

    lines += [
        "",
        "## (a) Auto-labeled by catalog — the reference-gap list",
        "",
        "Ranked by witness-MS count. `cluster/candidates` = majority "
        "cluster size / total candidates scored.",
        "",
        "| unit | MSS | pages | med len | auto-label | source | "
        "cluster/cand | libs (by MS) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in auto:
        libs = ', '.join(f"{k}:{v}" for k, v in
                         list(r['lib_counts_by_ms'].items())[:4])
        lines.append(
            f"| {r['unit']} | {r['n_ms']} | {r['n_pages']} | "
            f"{r['med_len']} | {mesc(r['auto_label'][:70])} | "
            f"{r['label_source']} | {r['cluster_size']}/"
            f"{r['n_candidates']} | {libs} |")

    lines += [
        "",
        "## (b) Truly unidentified — the headline discovery list "
        "(top 60 by witness count)",
        "",
        "| unit | MSS | pages | med len | verdict | weak T1 hint | "
        "catalog signal | libs (by MS) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in other[:60]:
        libs = ', '.join(f"{k}:{v}" for k, v in
                         list(r['lib_counts_by_ms'].items())[:4])
        hint = r['weak_t1_label'] or '—'
        sig = (f"{r['cluster_size']}/{r['n_candidates']} "
               f"({r['auto_label'][:40]})" if r['n_candidates']
               else 'none')
        lines.append(
            f"| {r['unit']} | {r['n_ms']} | {r['n_pages']} | "
            f"{r['med_len']} | {r['verdict']} | {mesc(hint[:50])} | "
            f"{mesc(sig)} | {libs} |")

    lines += [
        "",
        "## Evidence cards — top 20 truly-unidentified units "
        "(text snippets)",
        "",
    ]
    for i, r in enumerate(other[:20], 1):
        lines.append(f"### {i}. unit {r['unit']} — {r['n_ms']} MSS, "
                     f"{r['n_pages']} pages, med {r['med_len']} letters "
                     f"— {r['verdict']}")
        if r['weak_t1_label']:
            lines.append(f"- weak Track-1 hint ({r['weak_t1_conf']}): "
                         f"*{mesc(r['weak_t1_label'])}*")
        lines.append(f"- catalog candidates: "
                     f"{mesc(fmt_candidates(r['candidates']))}")
        libs = ', '.join(f"{k}:{v}" for k, v in
                         list(r['lib_counts_by_ms'].items())[:5])
        lines.append(f"- libraries (by MS): {libs}")
        for s in r['snippets']:
            lines.append(f"- [{s['shelfmark']} ({s['library']}) "
                         f"p.{s['page_num']}]({s['browse_url']}) — cat "
                         f"title: *{mesc(s['cat_title']) or '(none)'}*")
            lines.append(f"  > {mesc(s['text'])}")
        lines.append("")

    lines += [
        "## Manual sanity sample (10 units read by hand)",
        "",
        "Read the actual page text (`pages`, via the same `norm_stream`/"
        "offset-projection helpers the pipeline itself uses) against the "
        "catalog evidence for 10 units spanning both verdicts and a range "
        "of witness counts. **Every one is a genuine shared passage** — no "
        "junk-leakage class found in this sample (cf. the already-fixed "
        "NLI-ownership-stamp junk class removed upstream in "
        "`passage_units.py`) — but the sample surfaced one *systemic*, "
        "reportable finding beyond the individual AUTO-LABELED texts:",
        "",
        "1. **unit 9140** (28 MSS, AUTO-LABELED \"הלכות הרי\"ף\" — Alfasi's "
        "Halakhot on Shabbat). Two Toronto MSS show near-IDENTICAL text "
        "(\"מאימתי התחלת תספורת א'ר' אבין משיניח מעפורת שלספרים על ברכיו...\""
        ") — textbook genuine duplication. Also the unit that exposed the "
        "representative-selection bug fixed above (see Method §4b).",
        "2. **unit 2201742** (62 MSS, AUTO-LABELED \"קצת חנה\" — the "
        "Judeo-Arabic Tale of Hannah). CUL Ms. T-S Misc. 27.3.15 and RNL "
        "EVR ARAB II 1273 both open on the same narrative beat (a "
        "messenger scene: \"...קלבו פאיית מכצור... יא ולד טיע לי...\"), "
        "confirmed genuine parallel narrative prose.",
        "3. **unit 1648364** (54 MSS, AUTO-LABELED a Hebrew-grammar/"
        "weak-verb treatise, \"כתאב אלאפעאל דואת חרוף אללין\" — this exact "
        "title also appears in B3's OWN report as a known verb-morphology "
        "treatise, independent cross-confirmation between the two spikes). "
        "Two RNL MSS share technical grammatical terminology "
        "(\"...לאנך תקול צמת וצמתת באט̇האר אלתא...\").",
        "4. **unit 20986** (25 MSS, AUTO-LABELED a Judeo-Arabic prayer-"
        "book/Torah-reading-rules text; one member independently "
        "cataloged \"סדור רס\"ג\" — Saadia Gaon's Siddur). CUL T-S "
        "H 18.20 and RNL Evr. Antonin B 184 carry VERBATIM-matching rules "
        "for which Torah portion to read when a festival coincides with "
        "Shabbat — a real, specific halakhic-liturgical text, absent from "
        "Track-1's reference corpus.",
        "5. **unit 176402** (23 MSS, AUTO-LABELED \"תוספות של סידור\"/"
        "liturgical Psalm additions). Two RNL MSS (one cataloged \"ספר "
        "המצוות ליפת אבן צגיר\", one \"סדור מנהג קראים\") both quote "
        "Psalm 118 verbatim (\"...כל גוים סבבוני... דחה דחיתני לנפול "
        "וה' עזרני...\") — confirms the catalog convergence AND shows how "
        "a scriptural quotation embedded in two DIFFERENT larger works "
        "(a law-book and a prayer-book) still forms one legitimate reuse "
        "unit.",
        "6. **unit 2141254** (23 MSS, AUTO-LABELED Targum Onqelos on "
        "Leviticus) — **the systemic finding.** Halper/Katz and CUL T-S "
        "NS 29.67 carry LITERAL Targum Onqelos Leviticus text "
        "(\"...תרביא ניכסת קודשיא...\"/\"...קפא זייתי ית אשמית קדם "
        "דיי...\"). This is canonical, well-known Aramaic Bible "
        "translation — yet it sits in the Track-1 residue. "
        "`passage_units.py`'s `CANON_CATS` (Track-1 label-propagation "
        "gate) is `{'Bible','Mishnah','Tosefta','Bavli','Yerushalmi'}` — "
        "**Targum is not in that set**, so Targum-category Track-1 spans "
        "(if Track-1 identifies Onqelos at all in its own pass) don't "
        "get the same-cautious 2-direct-label confidence treatment as "
        "other canonical categories, and evidently not enough direct "
        "Track-1 hits landed on these particular members to clear the "
        "`labeled=1` bar. **Some fraction of this 'unidentified residue' "
        "is canonical/classical text that Track-1 under-covers for "
        "structural reasons, not genuinely novel material** — a caveat "
        "for how 'discovery' should be read, distinct from true novelty. "
        "Recommend a follow-up: check whether ref_corpus.pkl includes "
        "Targum Onqelos at all, and if so why direct-label coverage is "
        "this thin.",
        "7-8. **units 666840 (76 MSS) / 592464 (60 MSS)**, both "
        "SUGGESTIVE Karaite maḥzor material — RNL MSS verbatim-share "
        "Isaiah-paraphrase liturgical text (\"...כי עם בציון ישב "
        "בירושלם בכה לא תבכה חנון...\"); genuine, just short of majority "
        "catalog convergence (candidate titles split between "
        "\"מחזור\"/\"פיוטים\" variants that a stricter matcher correctly "
        "declines to force together).",
        "9. **unit 303006** (57 MSS, SUGGESTIVE, Judeo-Arabic Torah/"
        "Deuteronomy commentary). RNL and BL snippets both read as "
        "genuine running Torah-commentary prose in the same idiom, "
        "though the two printed snippets land on different verses — a "
        "reminder that a unit's per-member snippet is illustrative "
        "evidence from that member's longest occurrence, not necessarily "
        "an aligned excerpt of the same exact sentence when the shared "
        "work is a long, continuous commentary.",
        "10. **unit 50224** (21 MSS, AUTO-LABELED, Track-1-sourced weak "
        "label \"תלמוד בבלי, שבת\"). RNL and JTS (Adler 363) both carry "
        "genuine Bavli Shabbat menstrual-law discussion (\"...ובמוך "
        "שבסנדלה... בעא מיניה ר' ירמיה מר' אבא...\") — correctly "
        "identified even via the lower-confidence Track-1 route.",
        "",
        "**Net: 10/10 sampled units are genuine shared passages** (no "
        "junk). The Targum-Onqelos case (#6) is the one class-level "
        "caveat worth carrying forward: not everything in this residue "
        "is novel/uncataloged — a slice is canonical text that Track-1's "
        "own coverage/confidence gates miss for structural reasons.",
        "",
    ]

    open(MD_OUT, 'w', encoding='utf-8').write('\n'.join(lines))

    json_out = {
        'total_residue': total_residue,
        'top_n': len(unit_reports),
        'n_auto_labeled': n_auto,
        'n_suggestive': n_suggestive,
        'n_no_catalog': n_nocatalog,
        'full_scale_library_by_ms': dict(full_lib_by_ms),
        'full_scale_library_by_pagerow': dict(full_lib_by_pagerow),
        'rnl_frac_by_ms': rnl_frac_ms,
        'rnl_frac_by_pagerow': rnl_frac_pr,
        'units': unit_reports,
    }
    with open(JSON_OUT, 'w', encoding='utf-8') as f:
        json.dump(json_out, f, ensure_ascii=False, indent=1)

    print(f"wrote {MD_OUT}\n      {JSON_OUT}")


def write_html(unit_reports):
    def role_class(role):
        return {'witness': 'witness', 'partial': 'partial'}.get(role, 'embed')

    cards = []
    for r in unit_reports:
        vclass = {'AUTO-LABELED': 'auto', 'SUGGESTIVE': 'sugg',
                  'NO-CATALOG': 'none'}[r['verdict']]
        vtext = {'AUTO-LABELED': esc(r['auto_label']) or 'auto-labeled',
                  'SUGGESTIVE': f"suggestive: {esc(r['auto_label'])}"
                  if r['auto_label'] else 'suggestive (no majority)',
                  'NO-CATALOG': 'לא מזוהה — no catalog signal'}[r['verdict']]
        libs = ', '.join(f"{k}:{v}" for k, v in
                         list(r['lib_counts_by_ms'].items())[:6])
        snip_html = ''.join(
            f"<div class='snip'><a href='{s['browse_url']}' "
            f"target='_blank'>{esc(s['shelfmark'])}</a> "
            f"({esc(s['library'])}) — cat: "
            f"<i>{esc(s['cat_title']) or '(none)'}</i>"
            f"<div class='txt' dir='rtl'>{esc(s['text'])}</div></div>"
            for s in r['snippets'])
        mem_rows = ''.join(
            f"<tr class='{role_class(m['role'])}'>"
            f"<td><a href='{m['browse_url']}' target='_blank'>"
            f"{esc(m['shelfmark'])}</a></td><td>{esc(m['library'])}</td>"
            f"<td dir='rtl'>{esc((m['cat_title'] or '')[:60]) or '—'}</td>"
            f"<td>{m['cov']:.2f}</td><td>{m['role']}</td></tr>"
            for m in r['members'][:60])
        cand_html = ''
        if r['candidates']:
            cand_html = '<ul class="cands">' + ''.join(
                f"<li>{esc(c[1][:70])} <span class='src'>[{c[2]}]"
                f"</span> — sys {esc(c[3])}</li>"
                for c in r['candidates'][:10]) + '</ul>'
        cards.append(f"""
<details class='card {vclass}'>
 <summary><span class='mid'>unit {r['unit']}</span>
  <span class='n'>{r['n_ms']} MSS · {r['n_pages']} pages · med {r['med_len']}</span>
  <span class='verdict {vclass}'>{vtext}</span></summary>
 <div class='libs'>libraries (by MS): {esc(libs)}</div>
 {snip_html}
 <div class='cands-hd'>catalog candidates ({r['n_candidates']}, cluster {r['cluster_size']}/{r['n_candidates']}):</div>
 {cand_html or '<i>(none)</i>'}
 <table><tr><th>shelfmark</th><th>lib</th><th>catalog title</th>
  <th>cov</th><th>role</th></tr>{mem_rows}</table>
 {'<p class="more">+ ' + str(len(r['members']) - 60) + ' more members</p>'
  if len(r['members']) > 60 else ''}
</details>""")

    n_auto = sum(1 for r in unit_reports if r['verdict'] == 'AUTO-LABELED')
    n_other = len(unit_reports) - n_auto
    doc = f"""<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>B2 — Residue review: most-copied unidentified texts</title><style>
 body{{font-family:Segoe UI,Arial;max-width:1150px;margin:20px auto;
 padding:0 12px;background:#17181c;color:#d6d6d6}}
 h1{{font-size:22px}}
 .intro{{background:#1a2230;border:1px solid #2c3e57;border-radius:8px;
 padding:12px 16px;line-height:1.5;margin-bottom:16px}}
 details.card{{background:#23252c;border:1px solid #3a3d46;
 border-radius:8px;margin:8px 0;padding:6px 12px}}
 details.card.auto{{border-color:#2c7d32}}
 details.card.sugg{{border-color:#8a6d1a}}
 details.card.none{{border-color:#5a2222}}
 summary{{cursor:pointer;display:flex;gap:12px;align-items:baseline;
 flex-wrap:wrap}}
 .mid{{color:#888;font-family:monospace}}
 .n{{color:#aaa}}
 .verdict{{font-weight:600}}
 .verdict.auto{{color:#5fd66d}} .verdict.sugg{{color:#e3b341}}
 .verdict.none{{color:#e5696f}}
 .libs{{color:#999;font-size:13px;margin:4px 0}}
 .snip{{direction:rtl;text-align:right;font-size:14px;line-height:1.5;
 background:#1d1f25;border:1px solid #3a3d46;border-radius:6px;
 padding:6px 10px;margin:6px 0;color:#e8e6df}}
 .snip .txt{{margin-top:4px}}
 .cands-hd{{color:#999;font-size:12px;margin-top:8px}}
 ul.cands{{margin:2px 0 6px;padding-right:20px;font-size:13px}}
 .src{{color:#6fb3e8;font-size:11px}}
 table{{border-collapse:collapse;font-size:13px;margin:6px 0}}
 td,th{{border:1px solid #3a3d46;padding:3px 8px}}
 tr.witness td{{background:#1e3320}} tr.partial td{{background:#332e1a}}
 tr.embed td{{background:#23252c}}
 a{{color:#6fb3e8}}
 .more{{color:#777;font-size:12px}}
</style></head><body>
<h1>B2 — Residue review: most-copied unidentified texts</h1>
<div class='intro'>Top {len(unit_reports)} unlabeled/low-confidence
passage-units by distinct witness-MS count (continuum unit 367274
excluded). <b style='color:#5fd66d'>Green border</b> = auto-labeled by
catalog cross-reference (Track-1 reference-gap — the work is known to
catalogs, just not to Track-1's reference corpus).
<b style='color:#e3b341'>Amber</b> = suggestive catalog signal, no
majority convergence. <b style='color:#e5696f'>Red</b> = no catalog
signal anywhere — the headline discovery queue.
{n_auto} auto-labeled, {n_other} truly unidentified (suggestive +
no-catalog).</div>
{''.join(cards)}
</body></html>"""
    open(HTML_OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {HTML_OUT}")


if __name__ == '__main__':
    main()
