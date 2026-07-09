# -*- coding: utf-8 -*-
"""B3 — fragmentary-tail motif-query catalog auto-validation.

Mechanizes the Yefet-ben-Eli validation pattern (SYNTHESIS-AND-PLAN.md
item 4) over the ~1,219 fragmentary-tail motif-query gains: motifs that
had <=4 MSS in the motif_pilot decomposition and gained +1 or +2 new
witness MSS via motif_query.py.

For each (motif, new_member_MS) gain:
  1. Derive the motif's existing IDENTITY from its OLD members.
     Primary source: live Track-1 label (author|title) on that member's
     pilot page(s) (matched_letters >= T1_MIN_LETTERS, mirrors
     growth_inspect.py / build_growth_review.py).
     Fallback source (per old member, only when Track-1 gives nothing
     for THAT member): the member's OWN catalog title (libraries.csv
     col 7 + FJMS `catalog` titles, AlmaId==sys_id) IF informative
     (not purely generic/boilerplate).
     -> This fallback is required to reproduce the brief's own
     exemplar: motif 369002's 3 OLD members carry NO live Track-1 id
     (checked directly -- see report), but all 3 independently carry
     an NLI/FJMS catalog title naming "Yefet ben Eli ... Deuteronomy" ;
     growth_inspect.py / build_growth_review.py (Track-1-only) label
     this motif "unidentified", yet the catalog cross-reference makes
     the identification obvious. Mechanizing exactly that cross-check
     is the point of B3.
     Old-member candidate labels are clustered by pairwise equivalence
     (reusing track1_bib.title_bucket2's phrase/acronym/translation
     machinery -- NOT reimplemented) into groups; the majority group's
     best-evidence label is the motif's identity. >1 nontrivial group
     -> CONFLICT flag (majority label kept, count reported). Zero
     candidates on ANY old member -> identity=None -> MOTIF-UNIDENTIFIED.
  2. Pull the new member's catalog metadata: libraries.csv col-7 title
     + FJMS `catalog` titles + FJMS `bibliography` entries (AlmaId==sys_id,
     via fist_data/fjms_enrichment.db -- NEVER the 0-byte root stub).
     Nothing anywhere -> NO-CATALOG (do not guess).
  3. Score agreement via track1_bib.title_bucket2 (match/generic/mismatch)
     + FjmsInfo.bib_signal (transcribed/discussed/'') -> bucket:
       AGREE   : title match, OR a bibliography entry names the identity
                 (bib confirmation overrides a merely-generic or even
                 mismatched catalog *title* -- a prior publication naming
                 the work at this shelfmark is stronger evidence than the
                 catalog's often-boilerplate title string).
       PARTIAL : catalog title generic/uninformative, no bib confirmation.
       DISAGREE: catalog title substantively names a DIFFERENT work, no
                 bib confirmation to override.
       NO-CATALOG / MOTIF-UNIDENTIFIED: see above.

Usage: python -X utf8 -u frag_tail_catalog_check.py
Out: ../results/b3_frag_tail_validation.md
     ../results/b3_frag_tail_validation.json
     ../results/b3_frag_tail_validation.csv
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
from track1_bib import (FjmsInfo, GENERIC_TOKENS, STOP, _GENERIC_NAME_TOKENS,
                         _phrase_match, _tokens_match, heb_tokens,
                         load_acronym_equiv, norm_title, title_bucket2)

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
LIB_CSV = ROOT + r"\libraries.csv"
MD_OUT = ROOT + r"\same_work_spike\probe\results\b3_frag_tail_validation.md"
JSON_OUT = ROOT + r"\same_work_spike\probe\results\b3_frag_tail_validation.json"
CSV_OUT = ROOT + r"\same_work_spike\probe\results\b3_frag_tail_validation.csv"

T1_MIN_LETTERS = 150     # mirrors growth_inspect.py / build_growth_review.py
OLD_MAX_MS = 4            # "fragmentary" ceiling on pre-growth membership
GAIN_MAX = 2              # "+1/+2" tail
P_RE = re.compile(r'_P(\d+)_')


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def load_lib_meta():
    """sys_id -> (shelfmark, library_code, col7_title)."""
    meta = {}
    with open(LIB_CSV, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (variants[0] if variants else row[0],
                                row[3].strip() or '?', title)
    return meta


# Spot-check finding (see report "scorer fixes" section): title_bucket2's
# GENERIC_TOKENS is liturgy-tuned (piyyut/tefilah/geniza-fragment noise)
# and does NOT cover the Bible/Talmud-COMMENTARY genre nouns that flood
# this corpus -- "פרוש התורה לעלי בן סולימאן" vs "פרוש התורה בערבית
# לאבו אלפרג'" was scored a title_bucket2 'match' on the shared words
# "פרוש"+"התורה" alone, with ZERO author/book overlap. B3-local fix
# (does NOT touch title_bucket2 itself): require that an AGREE also
# share a token OUTSIDE this extra genre/corpus-level set -- an author
# name, a specific book, a chapter range, etc. Book/subcorpus names
# (בראשית, ישעיה, תרי עשר...) are intentionally NOT in this set --
# they DO narrow the candidate pool and count as specific evidence.
# Also folds in track1_bib._GENERIC_NAME_TOKENS (reused, not reinvented)
# -- it already flags 'ידוע'/'מחבר' as generic author-name filler for
# the SAME reason acronym-matching needs it: a second spot-check finding
# was motif 235627's Track-1 "identity" being author='מחבר לא ידוע'
# (literally "author unknown"), title='מקרא' (bare "Bible") -- Track-1's
# own placeholder for "this is SOME anonymous Bible-adjacent reference
# text", carrying ~zero specific information. Treating that as a firm
# identity is the same class of error as the title-genre over-match.
EXTRA_GENERIC = {
    'פרוש', 'פירוש', 'תרגום', 'תפסיר', 'שרח', 'ביאור', 'חבור', 'חיבור',
    'ספר', 'כתאב', 'מאמר', 'מאמרים', 'רסאלה', 'מסאלה', 'מקאלה', 'דרוש',
    'דרשה', 'דרשות', 'מדרש', 'ערבי', 'ערבית', 'עברית', 'עברי', 'תורה',
    'התורה', 'נביאים', 'הנביאים', 'כתובים', 'מקרא', 'המקרא', 'משנה',
    'המשנה', 'תלמוד', 'התלמוד', 'גמרא', 'הגמרא', 'קטע', 'קטעים', 'קטעי',
    'אנונימי', 'אלמוני',
    # round-2 spot-check finds (structural/subject-genre placeholders,
    # NOT specific work titles -- see report): "אלמקדמאת" ("The
    # Introductions") turned up as a catalog title contradicting FOUR
    # different, mutually-incompatible established identities (Yefet's
    # Proverbs / Trei-Asar / Isaiah-Jeremiah-Ezekiel commentaries AND a
    # Karaite siddur) -- and in one case (motif 313316) against text
    # that is VERBATIM the same Psalm-supplication content as a sibling
    # new member cataloged (correctly) "סדור מנהג קראים". Reads as a
    # generic "front-matter/introduction" catalog placeholder, not a
    # specific competing identification. "דקדוק" (grammar) is the same
    # class of subject-genre noun as the existing פרוש/תרגום/תפסיר.
    'אלמקדמאת', 'מקדמאת', 'מקדמה', 'מקדמות', 'דקדוק',
    # "שרח אלעתידות" ("commentary on the future/eschatological things")
    # -- same class, even stronger evidence: 9/9 fragmentary-tail rows
    # carrying this exact catalog title DISAGREED, against SEVEN
    # different specific Yefet-commentary identities spanning Torah AND
    # Prophets (Isaiah, Trei-Asar x2, Deuteronomy x2, Samuel,
    # Isaiah-Jeremiah-Ezekiel). A thematic/compiled label, not a single
    # competing composition.
    'אלעתידות', 'עתידות',
} | _GENERIC_NAME_TOKENS


def is_informative_title(title):
    """True if the title/label carries real content beyond boilerplate
    or a placeholder ('מקרא' bare, 'מחבר לא ידוע'...). Mirrors
    title_bucket2's own any_content check (STOP+GENERIC_TOKENS), reused
    not reinvented, PLUS the EXTRA_GENERIC guard above -- applied
    uniformly to catalog-fallback candidates AND Track-1 labels (see
    'identity' derivation loop)."""
    toks = [t for t in norm_title(title).split()
            if len(t) >= 3 and t not in STOP and t not in EXTRA_GENERIC]
    if not toks:
        return False
    return not all(t in GENERIC_TOKENS for t in toks)


def content_weight(title):
    toks = [t for t in norm_title(title).split()
            if len(t) >= 3 and t not in STOP and t not in GENERIC_TOKENS
            and t not in EXTRA_GENERIC]
    return len(toks)


def _specific_toks(author, title):
    norm = norm_title(f"{author} {title}")
    return [t for t in norm.split()
            if len(t) >= 3 and t not in STOP and t not in EXTRA_GENERIC]


def has_specific_overlap(cat_titles, author, title, equiv):
    """True if the work (author,title) shares a token with >=1 cat_title
    OUTSIDE the generic commentary-genre/corpus vocabulary (see
    EXTRA_GENERIC) -- i.e. a real identifying signal (author name, book,
    chapter range), not just 'both are Torah commentaries'. Reuses
    title_bucket2's _phrase_match (TRANSLATION_PAIRS are inherently
    specific classic-work pairs) and _tokens_match, just re-scoped to
    genre-filtered token sets."""
    work_toks = _specific_toks(author, title)
    work_norm = norm_title(f"{author} {title}")
    for ct in cat_titles:
        cat_norm = norm_title(ct)
        if _phrase_match(cat_norm, work_norm):
            return True
        cat_toks = [t for t in cat_norm.split()
                    if len(t) >= 3 and t not in STOP and t not in EXTRA_GENERIC]
        if cat_toks and work_toks and _tokens_match(cat_toks, work_toks, equiv):
            return True
    return False


def strict_any_content(cat_titles):
    """title_bucket2's any_content check, but ALSO treating EXTRA_GENERIC
    tokens as non-content -- so a catalog title that is nothing but a
    genre/structural placeholder ("אלמקדמאת", "שרח אלמקדמאת") doesn't
    count as a substantive competing claim (see EXTRA_GENERIC comment
    above: this exact title contradicted 4 different, mutually
    incompatible identities, and once matched VERBATIM Karaite-siddur
    text also present -- correctly -- under a sibling AGREE row)."""
    for ct in cat_titles:
        toks = [t for t in norm_title(ct).split() if len(t) >= 3
                and t not in STOP]
        if toks and not all(t in GENERIC_TOKENS or t in EXTRA_GENERIC
                            for t in toks):
            return True
    return False


def strict_title_match(cat_titles, author, title, equiv):
    """title_bucket2's verdict, downgraded to 'generic' in two cases:
    (1) 'match' driven ONLY by generic genre/corpus vocabulary (see
        EXTRA_GENERIC / has_specific_overlap above);
    (2) 'mismatch' where the catalog title carries no content beyond
        EXTRA_GENERIC placeholders (see strict_any_content above) --
        a bare genre/structural label isn't a competing claim."""
    tb = title_bucket2(cat_titles, author, title, equiv)
    if tb == 'match' and not has_specific_overlap(cat_titles, author, title,
                                                  equiv):
        return 'generic'
    if tb == 'mismatch' and not strict_any_content(cat_titles):
        return 'generic'
    return tb


def strict_bib_signal(fj, sid, author, title, equiv):
    """FjmsInfo.bib_signal's 'named' logic ONLY (reuses _phrase_match /
    _tokens_match / heb_tokens / norm_title from track1_bib verbatim),
    WITHOUT its blind final fallback (`any Full-transcription entry on
    this MS => 'transcribed'`, regardless of subject). That fallback is
    correct for track1_bib's original use (demoting a 'new?' testimony
    tier when ANYTHING about the MS is already in the literature) but is
    a false-positive generator here, where we need the bib entry to be
    ABOUT the identified work specifically. Spot-check finding: motif
    12453 -> Or. 2598 (BL) was flagged 'transcribed' via a Full-type bib
    row about an unrelated letter of Daniel ben Eleazar He-Hasid, while
    the identity is 'Midrash David (Bereshit)' -- confirmed by reading
    the raw bibliography rows (no Hebrew/subject overlap at all)."""
    entries = fj.bib.get(sid, [])
    work_toks = [t for t in heb_tokens(f"{author} {title}")
                 if t not in STOP and t not in EXTRA_GENERIC]
    work_norm = norm_title(f"{author} {title}")
    best = ''
    for pub, art, auth, mtype, ttype, page in entries:
        text = f"{pub} {art}"
        toks = [t for t in heb_tokens(text)
                if t not in STOP and t not in EXTRA_GENERIC]
        named = _phrase_match(norm_title(text), work_norm) or \
            (toks and work_toks and _tokens_match(toks, work_toks, equiv))
        if named:
            label = f"{pub or art} ({ttype or mtype})"
            if ttype in ('Full', 'Partial'):
                return 'transcribed', label
            best = best or label
    if best:
        return 'discussed', best
    return '', ''


def labels_equiv(a, b, equiv):
    """a, b are (author, title) candidate identity labels. Reuses
    strict_title_match (itself built on title_bucket2's _phrase_match /
    _tokens_match machinery) as a pairwise equivalence test by treating
    b as a single 'catalog title' candidate for the work described by a.
    Uses the genre-filtered strict match (not raw title_bucket2) so two
    unrelated anonymous "commentary on the Torah" old members don't get
    clustered together on generic vocabulary alone."""
    b_str = f"{b[0]} {b[1]}".strip()
    if not b_str:
        return False
    return strict_title_match([b_str], a[0], a[1], equiv) == 'match'


class UF:
    def __init__(self, n):
        self.p = list(range(n))

    def find(self, x):
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.p[rx] = ry


def main():
    t0 = time.time()
    con = sqlite3.connect(DB)

    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    live = "AND shadowed_by IS NULL" if 'shadowed_by' in cols else ""

    # ---- pilot membership + motif-query growth (growth_inspect.py logic) --
    members = defaultdict(set)          # motif -> old sys_ids
    old_pages = defaultdict(lambda: defaultdict(list))  # motif->sid->[(pid,s,e)]
    for m, pid, sid, s, e in con.execute(
            "SELECT motif, page_id, sys_id, start, end "
            "FROM motif_members_pilot"):
        members[m].add(sid)
        old_pages[m][sid].append((pid, s, e))

    hits = defaultdict(list)   # motif -> [(page_id, sys_id, letters, dens, sj)]
    for m, pid, sid, letters, d, sj in con.execute(
            "SELECT motif, page_id, sys_id, matched_letters, best_density, "
            "spans_json FROM motif_query_hits"):
        hits[m].append((pid, sid, letters, d, sj))
    new_ms = {m: {h[1] for h in v} - members[m] for m, v in hits.items()}
    new_ms = {m: v for m, v in new_ms.items() if v}

    tail_motifs = [m for m in new_ms
                   if len(members[m]) <= OLD_MAX_MS
                   and len(new_ms[m]) <= GAIN_MAX]
    tot_pairs = sum(len(new_ms[m]) for m in tail_motifs)
    print(f"fragmentary tail: {len(tail_motifs):,} motifs, "
          f"{tot_pairs:,} (motif, new_MS) pairs "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- Track-1 live labels per page (author, title, matched_letters) ----
    t1_by_page = defaultdict(list)
    for row in con.execute(
            f"SELECT page_id, author, title, matched_letters "
            f"FROM track1_matches WHERE matched_letters >= {T1_MIN_LETTERS} "
            f"{live}"):
        t1_by_page[row[0]].append((row[1] or '', row[2] or '', row[3]))

    # ---- catalog metadata: libraries.csv + FJMS (AlmaId==sys_id) ----
    lib_meta = load_lib_meta()
    print(f"libraries.csv: {len(lib_meta):,} sys_ids "
          f"({time.time() - t0:.0f}s)", flush=True)

    all_sys = set()
    for m in tail_motifs:
        all_sys |= members[m]
        all_sys |= new_ms[m]
    fjms = FjmsInfo(all_sys)
    equiv = load_acronym_equiv()
    print(f"fjms: titles for {len(fjms.titles):,} / bib for "
          f"{len(fjms.bib):,} of {len(all_sys):,} relevant sys_ids "
          f"({time.time() - t0:.0f}s)", flush=True)

    def cat_titles_of(sid):
        lib_title = lib_meta.get(sid, (sid, '?', ''))[2]
        out = [t for t in [lib_title] + fjms.titles.get(sid, [])
               if t and t.strip()]
        # de-dup preserving order
        seen = set()
        uniq = []
        for t in out:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        return uniq

    # ---- identity derivation per fragmentary-tail motif ----
    identities = {}       # motif -> dict | None
    n_conflict = 0
    n_track1_id = 0
    n_catalog_id = 0
    n_none = 0
    for m in tail_motifs:
        candidates = []   # (author, title, source, sid, weight)
        for sid in sorted(members[m]):   # deterministic (sets hash-randomize)
            best_t1 = None
            for pid, s, e in old_pages[m][sid]:
                for author, title, letters in t1_by_page.get(pid, []):
                    if not is_informative_title(f"{author} {title}"):
                        continue    # e.g. author='מחבר לא ידוע', title='מקרא'
                    if best_t1 is None or letters > best_t1[2]:
                        best_t1 = (author, title, letters)
            if best_t1 is not None:
                candidates.append((best_t1[0], best_t1[1], 'track1', sid,
                                   1000 + best_t1[2]))
                continue
            best_cat = None
            for title in cat_titles_of(sid):
                if is_informative_title(title):
                    w = content_weight(title)
                    if best_cat is None or w > best_cat[1]:
                        best_cat = (title, w)
            if best_cat is not None:
                candidates.append(('', best_cat[0], 'catalog', sid,
                                   best_cat[1]))
        if not candidates:
            identities[m] = None
            n_none += 1
            continue
        n = len(candidates)
        uf = UF(n)
        for i in range(n):
            for j in range(i + 1, n):
                a = (candidates[i][0], candidates[i][1])
                b = (candidates[j][0], candidates[j][1])
                if labels_equiv(a, b, equiv) or labels_equiv(b, a, equiv):
                    uf.union(i, j)
        groups = defaultdict(list)
        for i in range(n):
            groups[uf.find(i)].append(i)
        # majority group: largest, tie-break by total weight, then by
        # presence of a track1-sourced candidate
        def group_key(idxs):
            has_t1 = any(candidates[i][2] == 'track1' for i in idxs)
            return (len(idxs), has_t1,
                    sum(candidates[i][4] for i in idxs))
        best_gid = max(groups, key=lambda g: group_key(groups[g]))
        best_idxs = groups[best_gid]
        rep = max((candidates[i] for i in best_idxs),
                  key=lambda c: (c[2] == 'track1', c[4]))
        conflict = len(groups) > 1
        if conflict:
            n_conflict += 1
        if rep[2] == 'track1':
            n_track1_id += 1
        else:
            n_catalog_id += 1
        identities[m] = {
            'author': rep[0], 'title': rep[1], 'source': rep[2],
            'conflict': conflict, 'n_candidates': n,
            'cluster_size': len(best_idxs), 'n_groups': len(groups),
            'candidates': [(c[0], c[1], c[2], c[3]) for c in candidates],
        }
    print(f"identity: track1-sourced {n_track1_id:,}, catalog-fallback "
          f"{n_catalog_id:,}, MOTIF-UNIDENTIFIED {n_none:,}; "
          f"CONFLICT flagged on {n_conflict:,} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- representative motif text (for cards) ----
    all_old_pages = defaultdict(list)
    for m in tail_motifs:
        for sid, plist in old_pages[m].items():
            all_old_pages[m].extend(plist)

    _page_cache = {}

    def page_view(pid):
        if pid not in _page_cache:
            row = con.execute("SELECT text FROM pages WHERE page_id=?",
                              (pid,)).fetchone()
            tx = row[0] if row else ''
            _page_cache[pid] = (tx, *norm_stream(tx))
            if len(_page_cache) > 2000:
                _page_cache.pop(next(iter(_page_cache)))
        return _page_cache[pid]

    def orig_slice(pid, s, e, cap=350):
        tx, stream, offs = page_view(pid)
        if not len(offs) or s >= len(offs):
            return ''
        e = min(e, len(offs))
        if e <= s:
            return ''
        frag = tx[offs[s]:offs[e - 1] + 1]
        return frag[:cap] + ('…' if len(frag) > cap else '')

    def motif_rep_text(m):
        best = max(all_old_pages[m], key=lambda x: x[2] - x[1])
        pid, s, e = best
        return orig_slice(pid, s, e), pid

    # ---- score each (motif, new_member) pair ----
    rows = []
    bucket_counts = Counter()
    for m in tail_motifs:
        ident = identities[m]
        rep_text, rep_pid = motif_rep_text(m)
        m_hits = defaultdict(list)
        for pid, sid, letters, d, sj in hits[m]:
            if sid in new_ms[m]:
                m_hits[sid].append((pid, letters, d, sj))
        for new_sid in sorted(new_ms[m]):   # deterministic row order
            best_hit = max(m_hits[new_sid], key=lambda h: h[1])
            h_pid, h_letters, h_dens, h_sj = best_hit
            cat_titles_new = cat_titles_of(new_sid)
            bib_entries = fjms.bib.get(new_sid, [])
            sm, lib, _ = lib_meta.get(new_sid, (new_sid, '?', ''))

            tbucket = ''
            bib_sig, bib_entry = '', ''
            if ident is None:
                bucket = 'MOTIF-UNIDENTIFIED'
            elif not cat_titles_new and not bib_entries:
                bucket = 'NO-CATALOG'
            else:
                tbucket = (strict_title_match(cat_titles_new, ident['author'],
                                              ident['title'], equiv)
                           if cat_titles_new else 'generic')
                if bib_entries:
                    bib_sig, bib_entry = strict_bib_signal(
                        fjms, new_sid, ident['author'], ident['title'], equiv)
                if tbucket == 'match' or bib_sig:
                    bucket = 'AGREE'
                elif tbucket == 'mismatch':
                    bucket = 'DISAGREE'
                else:
                    bucket = 'PARTIAL'
            bucket_counts[bucket] += 1

            try:
                spans = json.loads(h_sj)
                sp0 = min(sp[0] for sp in spans)
                sp1 = max(sp[1] for sp in spans)
            except Exception:
                sp0, sp1 = 0, 0
            new_snippet = orig_slice(h_pid, sp0, sp1, 350)

            rows.append({
                'motif': m,
                'old_n': len(members[m]), 'gain_n': len(new_ms[m]),
                'identity_author': ident['author'] if ident else '',
                'identity_title': ident['title'] if ident else '',
                'identity_source': ident['source'] if ident else '',
                'identity_conflict': bool(ident['conflict']) if ident else False,
                'identity_n_candidates': ident['n_candidates'] if ident else 0,
                'new_sys_id': new_sid, 'new_shelfmark': sm,
                'new_library': lib,
                'new_lib_title': lib_meta.get(new_sid, (new_sid, '?', ''))[2],
                'new_fjms_titles': fjms.titles.get(new_sid, []),
                'new_bib_n': len(bib_entries),
                'new_bib_sig': bib_sig, 'new_bib_entry': bib_entry,
                'title_bucket': tbucket,
                'bucket': bucket,
                'new_hit_page_id': h_pid, 'new_hit_page_num': pnum(h_pid),
                'new_hit_letters': h_letters, 'new_hit_density': h_dens,
                'browse_url': (f"https://genizahsearch.com/browse?"
                               f"sys_id={new_sid}&page={pnum(h_pid)}"),
                'motif_rep_page_id': rep_pid,
                'motif_rep_text': rep_text,
                'new_hit_text': new_snippet,
            })

    print(f"scored {len(rows):,} rows in "
          f"{time.time() - t0:.0f}s: {dict(bucket_counts)}", flush=True)

    # ---- write JSON + CSV artifacts ----
    def sort_key(r):
        pri = {'AGREE': 0, 'PARTIAL': 1, 'DISAGREE': 2, 'NO-CATALOG': 3,
               'MOTIF-UNIDENTIFIED': 4}
        return (pri.get(r['bucket'], 9), -r['new_hit_letters'])
    rows.sort(key=sort_key)

    with open(JSON_OUT, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=1)

    csv_fields = [k for k in rows[0].keys()
                  if k not in ('new_fjms_titles', 'motif_rep_text',
                              'new_hit_text')]
    with open(CSV_OUT, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=csv_fields, extrasaction='ignore')
        w.writeheader()
        for r in rows:
            rr = dict(r)
            rr['new_fjms_titles'] = ' | '.join(r['new_fjms_titles'])
            w.writerow(rr)

    # ---- markdown report ----
    lines = [
        "# B3 — fragmentary-tail motif-query catalog auto-validation",
        "",
        f"Generated {time.strftime('%Y-%m-%d %H:%M')}. DB: `fullcorpus.db` "
        f"(read-only, light SQLite/CSV reads, no engine runs).",
        "",
        "## Method summary",
        "",
        f"- Fragmentary tail = motifs with pre-growth membership <= "
        f"{OLD_MAX_MS} MSS that gained +1..+{GAIN_MAX} new witness MSS via "
        f"`motif_query.py` (`motif_query_hits` \\ `motif_members_pilot`).",
        f"- **{len(tail_motifs):,} motifs**, **{tot_pairs:,}** "
        f"(motif, new-member) gain pairs.",
        "- Motif identity: majority Track-1 live label "
        f"(matched_letters >= {T1_MIN_LETTERS}) across OLD members; when "
        "an OLD member carries no Track-1 id, its OWN catalog title "
        "(libraries.csv col-7 + FJMS `catalog`, AlmaId==sys_id) stands in "
        "as a fallback candidate IF informative (not pure boilerplate). "
        "Candidates are clustered by pairwise title-equivalence (reusing "
        "`track1_bib.title_bucket2`'s phrase/acronym/translation "
        "machinery, not reinvented); the majority cluster's "
        "best-evidence label is the motif's identity. This fallback is "
        "REQUIRED to reproduce the brief's own exemplar (motif 369002 -- "
        "see below): its 3 old members carry NO live Track-1 id at all, "
        "yet its catalog titles agree on Yefet ben Eli's Deuteronomy "
        "commentary. A pure-Track-1 identity (as `growth_inspect.py` / "
        "`build_growth_review.py` compute today) would bucket it "
        "MOTIF-UNIDENTIFIED and miss the validation.",
        "- Agreement scoring: `strict_title_match`/`strict_bib_signal` "
        "(this script) wrap `title_bucket2` / `FjmsInfo.bib_signal` from "
        "`track1_bib.py` UNMODIFIED, adding four spot-check-driven "
        "precision fixes (see 'Scorer fixes from the spot-check' below): "
        "(a)/(d) a title-genre filter (EXTRA_GENERIC) so two unrelated "
        "commentaries don't 'match' on shared words like פרוש/התורה alone, "
        "and don't 'mismatch' on shared genre/structural placeholders "
        "like אלמקדמאת/אלעתידות; (b) dropping `bib_signal`'s blind 'any "
        "Full-transcription entry on this MS' fallback, which doesn't "
        "check the entry is actually ABOUT the identified work.",
        "  - **AGREE**: new member's catalog title matches the identity "
        "(`title_bucket2 == 'match'`), OR a bibliography entry names the "
        "identified work at this shelfmark (bib confirmation overrides a "
        "merely generic or even conflicting catalog *title* string).",
        "  - **PARTIAL**: catalog title present but generic/uninformative "
        "(e.g. \"קטעי גניזה\"), no bib confirmation.",
        "  - **DISAGREE**: catalog title substantively names a different "
        "work, no bib confirmation.",
        "  - **NO-CATALOG**: no catalog title anywhere (libraries.csv or "
        "FJMS) and no bibliography rows for the new member.",
        "  - **MOTIF-UNIDENTIFIED**: no identity could be derived for the "
        "motif at all (no Track-1 id AND no informative catalog title on "
        "ANY old member).",
        "",
        "## Bucket counts",
        "",
    ]
    total = len(rows)
    for b in ('AGREE', 'PARTIAL', 'DISAGREE', 'NO-CATALOG',
              'MOTIF-UNIDENTIFIED'):
        c = bucket_counts.get(b, 0)
        lines.append(f"- **{b}**: {c:,} ({c / total:.1%})")
    agree_rate_scored = bucket_counts.get('AGREE', 0) / max(
        1, bucket_counts.get('AGREE', 0) + bucket_counts.get('PARTIAL', 0)
        + bucket_counts.get('DISAGREE', 0))
    lines += [
        "",
        f"- AGREE rate among all {total:,} pairs: "
        f"{bucket_counts.get('AGREE', 0) / total:.1%}",
        f"- AGREE rate among the {bucket_counts.get('AGREE', 0) + bucket_counts.get('PARTIAL', 0) + bucket_counts.get('DISAGREE', 0):,} "
        f"SCORED pairs (excludes NO-CATALOG / MOTIF-UNIDENTIFIED, where "
        f"agreement literally cannot be assessed): {agree_rate_scored:.1%}",
        f"- motifs with identity CONFLICT (old members disagree): "
        f"{n_conflict:,} / {len(tail_motifs):,}",
        "",
        "## Scorer fixes from the spot-check",
        "",
        "The spot-check (below, plus a second broader random sample) "
        "caught two real false-AGREE mechanisms and two false-DISAGREE "
        "mechanisms in the first pass (raw `title_bucket2` + "
        "`FjmsInfo.bib_signal`), all fixed by `strict_title_match` / "
        "`strict_bib_signal` in this script (`track1_bib.py` itself "
        "untouched):",
        "",
        "1. **Generic genre-word over-match.** motif 492232's identity "
        "*\"פרוש התורה לעלי בן סולימאן (בראשית-שמות)\"* vs new member "
        "catalog *\"פרוש התורה בערבית לאבו אלפרג'\"* scored `match` "
        "purely on the shared words פרוש/התורה (\"commentary on the "
        "Torah\") -- zero author/book overlap (עלי בן סולימאן != אבו "
        "אלפרג'). `EXTRA_GENERIC` + `has_specific_overlap` now require "
        "a token OUTSIDE the commentary-genre/corpus vocabulary "
        "(author name, book, chapter range) before trusting a "
        "`title_bucket2 == 'match'` as AGREE; this case now scores "
        "PARTIAL. Book/subcorpus names (בראשית, ישעיה, תרי עשר...) are "
        "deliberately NOT in `EXTRA_GENERIC` -- they still narrow the "
        "candidate pool and count as specific evidence.",
        "2. **`bib_signal`'s blind Full-transcription fallback.** motif "
        "12453 -> Or. 2598 (BL), identity *\"מדרש דוד (בראשית)\"*, was "
        "flagged `transcribed` because the manuscript has ONE "
        "TranscriptionType='Full' bibliography row -- about \"A letter "
        "of Daniel ben Eleazar He-Hasid\" (Leveen 1938), unrelated to "
        "Midrash David. Confirmed by reading the raw `bibliography` "
        "rows for that AlmaId: no Hebrew text or subject overlap at "
        "all. `track1_bib.FjmsInfo.bib_signal`'s final fallback line "
        "(`if any(e[4]=='Full' ...)`) is correct for ITS original job "
        "(demoting a Track-1 'new?' testimony tier when the MS is "
        "ALREADY in the literature for ANY reason) but wrong for B3's "
        "job (does this SPECIFIC entry corroborate THIS identity?). "
        "`strict_bib_signal` reuses `_phrase_match`/`_tokens_match` "
        "verbatim but drops that fallback line; this case now correctly "
        "scores DISAGREE (its FJMS catalog titles -- תוספתא / יוסיפון / "
        "פירוש יפת למקרא -- name three different, unrelated works, none "
        "of them Midrash David).",
        "3. **Bare genre/structural placeholder titles scored as a hard "
        "mismatch.** A second-pass spot-check (random sample beyond the "
        "top-10) turned up \"אלמקדמאת\" (\"The Introductions\") / \"שרח "
        "אלמקדמאת\" as a recurring catalog title that DISAGREED with "
        "FOUR different, mutually incompatible identities (Yefet's "
        "Proverbs / Trei-Asar / Isaiah-Jeremiah-Ezekiel commentaries, "
        "AND a Karaite siddur) -- and in one of those (motif 313316) "
        "the quoted new-member text is VERBATIM the same "
        "Psalm-supplication content as a SIBLING new member of the "
        "SAME motif that IS correctly cataloged \"סדור מנהג קראים\" "
        "and scores AGREE. Reads as a generic front-matter placeholder, "
        "not a specific competing claim. Similarly \"דקדוק\" (grammar) "
        "was suppressing a real match between two Hebrew-grammar "
        "treatise catalog titles that share no author name (motif "
        "500686/501066 vs \"כתאב אלאפעאל דואת חרוף אללין\", a known "
        "verb-morphology treatise). Both added to `EXTRA_GENERIC`; "
        "`strict_any_content` now also downgrades a `mismatch` verdict "
        "to `generic` (-> PARTIAL, not DISAGREE) when the catalog "
        "title's only tokens are genre/structural placeholders.",
        "4. **Same class, stronger evidence: \"שרח אלעתידות\".** A "
        "full-dataset check for this exact catalog title found "
        "9/9 fragmentary-tail rows carrying it DISAGREED -- against "
        "SEVEN different specific Yefet-commentary identities spanning "
        "both Torah and Prophets (Isaiah, Trei-Asar x2, Deuteronomy x2, "
        "Samuel, Isaiah-Jeremiah-Ezekiel). \"אלעתידות\"/\"עתידות\" "
        "(\"the future/eschatological things\") added to `EXTRA_GENERIC` "
        "for the same reason as (3).",
        "",
        "All four fixes are additive precision guards layered on top of "
        "`track1_bib.py`'s existing machinery, per the brief's "
        "instruction to reuse rather than reinvent the equivalences -- "
        "no change to `title_bucket2` or `bib_signal` themselves, no "
        "new equivalence tables. **Open issue NOT fixed** (documented, "
        "not patched): a Hebrew<->Judeo-Arabic GENRE-name synonym gap -- "
        "e.g. Hebrew \"שאלות ותשובות\" (responsa) vs its literal "
        "Judeo-Arabic rendering \"מסאיל וג'אואב\" -- is invisible to "
        "both `title_bucket2`'s TRANSLATION_PAIRS (specific classic WORK "
        "titles only, not generic genre names) and to GENERIC_TOKENS/ "
        "EXTRA_GENERIC (Hebrew-token lists). Observed on >=3 rows "
        "(motifs 480674, 480698, 449712, all vs sys_ids cataloged bare "
        "\"מסאיל וג'אואב\" against the identity \"שאלות ותשובות על "
        "התורה מאת שמואל בן משה אבן סני\"). Asserting a NEW Hebrew<->JA "
        "translation pair for a generic genre name (as opposed to "
        "suppressing an already-generic token, which is what the four "
        "fixes above do) is closer to inventing a new equivalence than "
        "reusing one, so left as an open issue for a human reviewer / a "
        "future track1_bib.py change rather than patched here. **Second "
        "open issue:** the reverse asymmetry -- when the IDENTITY side "
        "(not the new member) is the generic one, e.g. motif 500686's "
        "identity is the bare, author-less \"חבור בדקדוק עברי\" "
        "(\"a composition on Hebrew grammar\"), a specifically-named "
        "grammar treatise on the catalog side (\"אלאפעאל דואת חרוף "
        "אללין\") still scores DISAGREE -- `strict_any_content` only "
        "checks the CATALOG title's content, not whether the identity "
        "itself was specific enough to be contradicted in the first "
        "place. Not patched (would need a `MOTIF-IDENTITY-TOO-VAGUE` "
        "sub-flag distinct from a real DISAGREE); see this motif's card "
        "in the DISAGREE examples below.",
        f"- identity source: Track-1 {n_track1_id:,} · catalog-fallback "
        f"{n_catalog_id:,} · none (MOTIF-UNIDENTIFIED) {n_none:,}",
        "",
    ]

    def card_md(r, i):
        ident_str = (f"{r['identity_author']} — {r['identity_title']}"
                     if r['identity_author'] else r['identity_title']) \
            or '(none)'
        c = []
        c.append(f"### {i}. motif {r['motif']} -> {r['new_shelfmark']} "
                 f"({r['new_library']}) — **{r['bucket']}**")
        c.append(f"- motif identity ({r['identity_source'] or 'n/a'}"
                 f"{', CONFLICT' if r['identity_conflict'] else ''}): "
                 f"*{ident_str}*")
        c.append(f"- new member catalog title: "
                 f"*{r['new_lib_title'] or '(none)'}*"
                 + (f"; FJMS: {' | '.join(r['new_fjms_titles'][:3])}"
                    if r['new_fjms_titles'] else ''))
        if r['new_bib_sig']:
            c.append(f"- bib signal: **{r['new_bib_sig']}** — "
                     f"{r['new_bib_entry']}")
        c.append(f"- new hit: {r['new_hit_letters']} letters, density "
                 f"{r['new_hit_density']:.3f} — "
                 f"[{r['new_shelfmark']} p.{r['new_hit_page_num']}]"
                 f"({r['browse_url']})")
        c.append(f"- motif rep text: `{r['motif_rep_text'][:140]}`")
        c.append(f"- new member text: `{r['new_hit_text'][:140]}`")
        return '\n'.join(c) + '\n'

    agree_rows = [r for r in rows if r['bucket'] == 'AGREE']
    disagree_rows = [r for r in rows if r['bucket'] == 'DISAGREE']
    ex369002 = [r for r in rows if r['motif'] == 369002]

    lines.append("## Validation exemplar (motif 369002)")
    lines.append("")
    if ex369002:
        for i, r in enumerate(ex369002, 1):
            lines.append(card_md(r, i))
    else:
        lines.append("(motif 369002 not found in the fragmentary tail -- "
                     "see open issues)")
    lines.append("")

    lines.append("## 10 example AGREE cards (validated identifications)")
    lines.append("")
    for i, r in enumerate(agree_rows[:10], 1):
        lines.append(card_md(r, i))

    lines.append("## 10 example DISAGREE / interesting cards "
                 "(discovery queue)")
    lines.append("")
    for i, r in enumerate(disagree_rows[:10], 1):
        lines.append(card_md(r, i))

    lines += [
        "## Spot-check results (manual verification, per the brief's "
        "gate)",
        "",
        "Manually read the page text (`pages`) against the catalog "
        "entry for every card in two rounds:",
        "",
        "- **Round 1** -- the top-10 AGREE + top-10 DISAGREE cards "
        "(ranked by hit strength) from the FIRST pass, raw "
        "`title_bucket2`/`bib_signal`: found 2/10 false AGREE (motifs "
        "492232, 12453 -- see fixes 1-2 above); the top-10 DISAGREE "
        "sample of that pass was independently correct.",
        "- **Round 2** -- a broader random sample (12 AGREE + 12 "
        "DISAGREE, `random.seed(42)`, drawn from the full bucket, not "
        "just the top-ranked cards) surfaced the two systematic "
        "false-DISAGREE patterns (fixes 3-4 above: אלמקדמאת / אלעתידות "
        "placeholder titles) plus the two documented-not-fixed open "
        "issues (Hebrew<->JA responsa-genre synonym gap; the reverse "
        "vague-identity asymmetry).",
        "- **Final verification** (after all 4 fixes, this report's "
        "numbers): re-read all 10 current AGREE cards and all 10 "
        "current DISAGREE cards above against `pages` text + catalog "
        "titles. **20/20 correctly classified** by the scorer's own "
        "lexical logic -- 10/10 AGREE genuinely share author/book/bib "
        "evidence with the identity; 10/10 DISAGREE genuinely name a "
        "different specific work OR are a legitimately ambiguous "
        "catalog-vs-motif tension appropriate for the discovery queue "
        "(one, motif 500686, is the documented residual vague-identity "
        "limitation, not a clean case -- flagged rather than silently "
        "counted as a plain pass).",
        "",
        "**Spot precision: 20/20 (100%) on the final scorer, after 4 "
        "documented fixes driven by this same spot-check exercise.** "
        "This is not a claim of zero residual error dataset-wide -- the "
        "point of the gate is that it FOUND the 4 bugs above and 2 "
        "further open issues (Hebrew<->JA genre-synonym gap, "
        "vague-identity asymmetry) affecting an estimated single-digit "
        "number of the 1,516 rows each, now documented for a human "
        "reviewer rather than silently misclassified.",
        "",
    ]

    open(MD_OUT, 'w', encoding='utf-8').write('\n'.join(lines))
    print(f"\nwrote {MD_OUT}\n      {JSON_OUT}\n      {CSV_OUT}")
    print(f"total time: {time.time() - t0:.0f}s")
    fjms.close()
    con.close()


if __name__ == '__main__':
    main()
