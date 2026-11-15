# -*- coding: utf-8 -*-
"""Gate for a generation of the editions index (editions search plan, §3.8, PR 1 checks).

    python scripts/verify_editions_index.py --gen <generation dir> [inputs...]
    python scripts/verify_editions_index.py --self-test [inputs...]

PR 1's checks read the data and the index directly (Tantivy queries, no search engine):

* **membership** -- the index's (uid, source, sys_id) set equals the set recomputed HERE
  from the sidecars by the plan's written rule (its own SQL, not the builder's code), with
  no uid twice; the meta's inputs equal the files.
* **cleaner** -- per rule, the texts it touched against the census of plan §2.4: none
  where the census found some fails, and so does more than ten times the census.
* **placement** -- documents placed on a page, per source, against the 2026-11-15 count:
  under half fails, whatever the input fingerprints say (a wrong NLI crossref or browse map).
* **content** -- deterministic records (the first ones by id of each source and text
  source): a 3-word span from a kept line of the expected search text, phrase-queried on
  ``content`` together with that record's uid, finds exactly that document.
* **readiness** -- index, browse map, format marker and meta share one build id; the format
  marker sits inside ``tantivy_db`` with the reversed-words field and no seams; the schema
  has every required field; the document count equals the meta's.

Every check reports how many cases it ran and fails on zero. ``--self-test`` builds small
generations with a planted fault each and requires the check named for that fault to fail
(and a clean one to pass): proof that the gate can fail.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import tempfile
from collections import Counter, defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from shared.edition_search_text import SOURCE_FGP, SOURCE_PGP, search_text  # noqa: E402

#: Plan §2.4/§2.3 census: texts carrying each structure (2026-11-15). A rule that touches
#: none of them, or more than ten times as many, is wrong.
CENSUS = {
    SOURCE_FGP: {"header_block": 1679, "folio_lines": 1685, "numbered_texts": 7443, "heading_lines": 880},
    SOURCE_PGP: {"folio_lines": 2731},
}
#: Documents placed on a page, wholly or in part (2026-11-15). A generation that places fewer
#: than half as many has lost an input (an empty or wrong NLI crossref or browse map), whatever
#: the files' fingerprints say.
PLACED_CENSUS = {SOURCE_FGP: 20111, SOURCE_PGP: 4317}
CONTENT_PER_SOURCE = 40
_HEB_WORD = re.compile(r"^[א-ת]{2,}$")


class Result:
    def __init__(self, name: str):
        self.name = name
        self.cases = 0
        self.failures: List[str] = []

    @property
    def ok(self) -> bool:
        return self.cases > 0 and not self.failures

    def fail(self, msg: str) -> None:
        self.failures.append(msg)

    def line(self) -> str:
        state = "PASS" if self.ok else "FAIL"
        extra = "" if self.cases else " (no cases ran)"
        head = f"[{state}] {self.name}: {self.cases:,} cases{extra}"
        return head + "".join(f"\n       - {f}" for f in self.failures[:12])


# --- the expected set, recomputed from the sidecars -------------------------------------

def _decode(text: str) -> str:
    from shared.fgp_service import decode_stored_entities
    return decode_stored_entities(text) or ""


def _genuzot(row: sqlite3.Row) -> bool:
    credits = [row["source_credit_en"], row["source_credit_he"]]
    if not any(credits):
        credits = [row["source_credit"]]
    return any(m in (c or "") for c in credits for m in ("Genuzos", "גנוזות"))


def expected_fgp(fgp_db: str, sys_ids: Optional[set] = None) -> Dict[str, Dict[str, Any]]:
    """uid -> record, by the plan's rule, from the FGP sidecar's own rows."""
    con = sqlite3.connect(f"file:{fgp_db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM fgp_transcriptions WHERE sys_id IS NOT NULL AND sys_id <> '' "
                       "ORDER BY sys_id, doc_relation, folio_num, id").fetchall()
    con.close()
    by_ms: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    for r in rows:
        if sys_ids is None or str(r["sys_id"]) in sys_ids:
            by_ms[str(r["sys_id"])].append(r)
    out: Dict[str, Dict[str, Any]] = {}
    for sys_id, ms_rows in by_ms.items():
        with_cnum = {(r["doc_relation"] or "Digital Edition", r["language"]) for r in ms_rows if r["c_number"]}
        kept: List[sqlite3.Row] = []
        by_key: Dict[tuple, int] = {}
        for r in ms_rows:
            rel = r["doc_relation"] or "Digital Edition"
            if not r["c_number"]:
                if (rel, r["language"]) not in with_cnum:
                    kept.append(r)
                continue
            key = (r["c_number"], rel, r["language"])
            if key in by_key:
                if len(_decode(r["content"] or "")) > len(_decode(kept[by_key[key]]["content"] or "")):
                    kept[by_key[key]] = r
                continue
            by_key[key] = len(kept)
            kept.append(r)
        texts: Dict[str, str] = {}
        for r in sorted(kept, key=lambda r: int(r["id"])):
            if "Edition" not in (r["doc_relation"] or "Digital Edition"):
                continue
            if not _decode(r["content"] or "").strip():
                continue
            st = search_text(_decode(r["content"]), SOURCE_FGP, genuzot_marks=_genuzot(r))
            if not st.text.strip() or st.text in texts:
                continue
            uid = f"ED:fgp:{r['id']}"
            texts[st.text] = uid
            out[uid] = {"uid": uid, "source": "FGP", "sys_id": sys_id, "text": st.text,
                        "group": f"fgp {r['text_source']}", "id": int(r["id"])}
    return out


def expected_pgp(pgp_db: str, sys_ids: Optional[set] = None) -> Dict[str, Dict[str, Any]]:
    con = sqlite3.connect(f"file:{pgp_db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT s.id, s.doc_relation, s.content, f.sys_id FROM document_sources s "
        "JOIN document_fragments f ON f.document_id = s.pgpid "
        "WHERE f.sys_id IS NOT NULL AND f.sys_id <> '' AND s.content IS NOT NULL AND s.content <> ''").fetchall()
    con.close()
    out: Dict[str, Dict[str, Any]] = {}
    texts: Dict[int, str] = {}
    for r in rows:
        if "Edition" not in (r["doc_relation"] or ""):
            continue
        sys_id = str(r["sys_id"])
        if sys_ids is not None and sys_id not in sys_ids:
            continue
        if r["id"] not in texts:
            texts[r["id"]] = search_text(r["content"], SOURCE_PGP).text
        if not texts[r["id"]].strip():
            continue
        uid = f"ED:pgp:{r['id']}:{sys_id}"
        out[uid] = {"uid": uid, "source": "PGP", "sys_id": sys_id, "text": texts[r["id"]],
                    "group": "pgp", "id": int(r["id"])}
    return out


# --- reading the generation --------------------------------------------------------------

def _open(gen_dir: str):
    import tantivy

    from shared.editions_indexer import DB_DIRNAME
    from shared.search_tokenizer import register_search_tokenizers
    index = tantivy.Index.open(os.path.join(gen_dir, DB_DIRNAME))
    register_search_tokenizers(index)
    index.reload()
    return index


def indexed_docs(index) -> List[Tuple[str, str, str]]:
    import tantivy
    searcher = index.searcher()
    n = searcher.num_docs
    hits = searcher.search(tantivy.Query.all_query(), limit=max(n, 1)).hits
    out = []
    for _score, addr in hits:
        d = searcher.doc(addr)
        header = (d["full_header"] or [""])[0]
        out.append(((d["unique_id"] or [""])[0], (d["source"] or [""])[0], header.split("_ED_")[0]))
    return out


# --- the checks ------------------------------------------------------------------------

def check_membership(index, meta, expected: Dict[str, Dict[str, Any]], inputs: Dict[str, Any]) -> Result:
    res = Result("membership")
    docs = indexed_docs(index)
    res.cases = len(expected)
    seen = Counter(uid for uid, _s, _m in docs)
    for uid, n in seen.items():
        if n > 1:
            res.fail(f"{uid} indexed {n} times")
    got = {(uid, s, m) for uid, s, m in docs}
    want = {(r["uid"], r["source"], r["sys_id"]) for r in expected.values()}
    for item in sorted(want - got)[:6]:
        res.fail(f"missing {item}")
    for item in sorted(got - want)[:6]:
        res.fail(f"not in the canonical set {item}")
    if len(want - got) > 6 or len(got - want) > 6:
        res.fail(f"{len(want - got):,} missing, {len(got - want):,} extra in all")
    if inputs is not None:
        if not inputs.get("nli_crossref"):
            res.fail("no NLI crossref: FGP per-image texts cannot be placed on their pages")
        recorded = (meta or {}).get("inputs") or {}
        for key, value in inputs.items():
            if recorded.get(key) != value:
                res.fail(f"meta input {key} differs from the file: {recorded.get(key)!r} vs {value!r}"[:300])
    return res


def check_cleaner(meta) -> Result:
    res = Result("cleaner (real data)")
    rules = (meta or {}).get("rules_texts") or {}
    for source, census in CENSUS.items():
        for rule, expected in census.items():
            res.cases += 1
            touched = (rules.get(source) or {}).get(rule, 0)
            if touched == 0:
                res.fail(f"{source} {rule}: touched no text (census {expected:,})")
            elif touched > 10 * expected:
                res.fail(f"{source} {rule}: touched {touched:,} texts, over ten times the census {expected:,}")
    return res


def check_placement(meta) -> Result:
    res = Result("placement")
    located = ((meta or {}).get("counts") or {}).get("located") or {}
    for source, census in PLACED_CENSUS.items():
        res.cases += 1
        got = (located.get(source) or {})
        placed = got.get("all", 0) + got.get("part", 0)
        if placed < census / 2:
            res.fail(f"{source}: {placed:,} documents placed on a page, under half of {census:,}")
    return res


def _span(text: str, analyzer) -> Optional[List[str]]:
    """The first three consecutive plain Hebrew words of a kept line."""
    for line in text.split("\n"):
        words = [w for w in analyzer.analyze(line)]
        for i in range(len(words) - 2):
            trio = words[i:i + 3]
            if all(_HEB_WORD.match(w) for w in trio):
                return trio
    return None


def content_records(expected: Dict[str, Dict[str, Any]], per_group: int = CONTENT_PER_SOURCE) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in sorted(expected.values(), key=lambda r: (r["group"], r["id"], r["uid"])):
        groups[rec["group"]].append(rec)
    return [rec for recs in groups.values() for rec in recs[:per_group]]


def check_content(index, records: List[Dict[str, Any]]) -> Result:
    from shared.index_seams import hebword_analyzer
    res = Result("content")
    analyzer = hebword_analyzer()
    searcher = index.searcher()
    for rec in records:
        trio = _span(rec["text"], analyzer)
        if not trio:
            continue
        res.cases += 1
        uid_terms = " ".join(re.findall(r"[0-9A-Za-z]+", rec["uid"]))
        q = index.parse_query(f'content:"{" ".join(trio)}" AND unique_id:"{uid_terms}"', ["content"])
        hits = searcher.search(q, limit=3).hits
        found = [searcher.doc(a)["unique_id"][0] for _s, a in hits]
        if found != [rec["uid"]]:
            res.fail(f"{rec['uid']}: {' '.join(trio)!r} found {found}")
    return res


def check_readiness(gen_dir: str, index, meta) -> Result:
    from shared.editions_indexer import DB_DIRNAME, REQUIRED_FIELDS, read_generation
    from shared.index_format import read_index_format
    res = Result("readiness")
    res.cases = 1
    marker = read_index_format(os.path.join(gen_dir, DB_DIRNAME))
    _meta, payload = read_generation(gen_dir)
    if not marker:
        res.fail("no index_format.json inside tantivy_db")
    else:
        if not marker.get("reversed_field"):
            res.fail("format marker: no reversed-words field")
        if marker.get("seams"):
            res.fail("format marker: seams recorded for an editions index")
    ids = {"marker": (marker or {}).get("build_id"), "meta": (meta or {}).get("build_id"),
           "browse map": (payload or {}).get("build_id")}
    if len(set(ids.values())) != 1 or None in ids.values():
        res.fail(f"build ids differ: {ids}")
    for name in REQUIRED_FIELDS:
        try:
            index.parse_query(f'{name}:"x"', [name])
        except Exception:  # the parser refuses a field the schema does not have
            res.fail(f"schema: no field {name}")
    n = index.searcher().num_docs
    if n != ((meta or {}).get("counts") or {}).get("documents"):
        res.fail(f"{n:,} documents in the index, meta says {((meta or {}).get('counts') or {}).get('documents')}")
    return res


def run_checks(gen_dir: str, expected: Dict[str, Dict[str, Any]], inputs: Optional[Dict[str, Any]],
               checks=("membership", "cleaner", "placement", "content", "readiness")) -> List[Result]:
    from shared.editions_indexer import read_generation
    meta, _payload = read_generation(gen_dir)
    index = _open(gen_dir)
    out = []
    if "membership" in checks:
        out.append(check_membership(index, meta, expected, inputs))
    if "cleaner" in checks:
        out.append(check_cleaner(meta))
    if "placement" in checks:
        out.append(check_placement(meta))
    if "content" in checks:
        out.append(check_content(index, content_records(expected)))
    if "readiness" in checks:
        out.append(check_readiness(gen_dir, index, meta))
    return out


# --- self-test: every check can fail -----------------------------------------------------

def _docs_from_expected(expected, fgp_db, pgp_db, mutate: Optional[Callable[[str], str]] = None,
                        drop: Callable[[Dict[str, Any]], bool] = lambda r: False):
    """EditionDoc objects carrying the expected texts (optionally broken)."""
    from shared.edition_search_text import SearchText
    from shared.editions_indexer import EditionDoc, _cover
    docs = []
    for rec in sorted(expected.values(), key=lambda r: r["uid"]):
        if drop(rec):
            continue
        text = mutate(rec["text"]) if mutate else rec["text"]
        kind = SOURCE_FGP if rec["source"] == "FGP" else SOURCE_PGP
        st = SearchText(text=text, original=text)
        doc = EditionDoc(kind=kind, edition_id=rec["id"], sys_id=rec["sys_id"], search=st)
        doc.boundaries = _cover([], len(text), kind, rec["sys_id"])
        docs.append(doc)
    return docs


def self_test(args) -> int:
    from shared.editions_indexer import META_FILENAME, build_generation
    con = sqlite3.connect(f"file:{args.fgp_db}?mode=ro", uri=True)
    fgp_ids = [str(r[0]) for r in con.execute(
        "SELECT DISTINCT sys_id FROM fgp_transcriptions WHERE sys_id <> '' ORDER BY sys_id LIMIT 120")]
    con.close()
    con = sqlite3.connect(f"file:{args.pgp_db}?mode=ro", uri=True)
    pgp_ids = [str(r[0]) for r in con.execute(
        "SELECT DISTINCT f.sys_id FROM document_fragments f JOIN document_sources s ON s.pgpid = f.document_id "
        "WHERE f.sys_id <> '' AND s.doc_relation LIKE '%Edition%' ORDER BY f.sys_id LIMIT 60")]
    con.close()
    subset = set(fgp_ids) | set(pgp_ids)
    expected = dict(expected_fgp(args.fgp_db, subset))
    expected.update(expected_pgp(args.pgp_db, subset))
    print(f"self-test on {len(subset)} manuscripts, {len(expected)} expected documents")
    real_rules = {SOURCE_FGP: dict(CENSUS[SOURCE_FGP]), SOURCE_PGP: dict(CENSUS[SOURCE_PGP])}
    # The subset's documents carry no page; the census stands in for the full build's counts.
    real_placed = {s: {"all": n, "part": 0, "none": 0} for s, n in PLACED_CENSUS.items()}

    def build(tmp, name, docs, rules=real_rules, after=None, placed=real_placed):
        out = os.path.join(tmp, name)
        build_generation(out, fgp_db=args.fgp_db, pgp_db=args.pgp_db, browse_map_path=args.browse_map,
                         nli_db=None, documents=docs, folio_images_for=lambda s: [], progress=lambda m: None)
        meta_path = os.path.join(out, META_FILENAME)
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        meta["rules_texts"] = rules
        meta["counts"]["located"] = placed
        if after:
            after(meta)
        with open(meta_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(meta, f, ensure_ascii=False)
        return out

    first_dup, last_uid = sorted(expected)[0], sorted(expected)[-1]
    word_class = re.compile(r"(?<!\S)ו\S*")
    faults = [
        ("clean", None, lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db), {}),
        ("one dropped, one duplicated", "membership",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db, drop=lambda r: r["uid"] == last_uid)
         + _docs_from_expected({first_dup: expected[first_dup]}, args.fgp_db, args.pgp_db), {}),
        ("PGP left out", "membership",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db, drop=lambda r: r["source"] == "PGP"), {}),
        ("cleaner removes nothing", "cleaner",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db), {"rules": {}}),
        ("cleaner run wild", "cleaner",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db),
         {"rules": {s: {k: v * 20 for k, v in c.items()} for s, c in CENSUS.items()}}),
        ("nothing placed on a page (a wrong NLI crossref)", "placement",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db),
         {"placed": {s: {"all": 0, "part": 0, "none": n} for s, n in PLACED_CENSUS.items()}}),
        ("a word class deleted", "content",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db, mutate=lambda t: word_class.sub("", t)), {}),
        ("meta from another build", "readiness",
         lambda: _docs_from_expected(expected, args.fgp_db, args.pgp_db), {"after": lambda m: m.update(build_id="other")}),
    ]
    ok = True
    with tempfile.TemporaryDirectory(prefix="editions-selftest-") as tmp:
        for i, (name, target, make, opts) in enumerate(faults):
            gen = build(tmp, f"g{i}", make(), rules=opts.get("rules", real_rules), after=opts.get("after"),
                        placed=opts.get("placed", real_placed))
            results = {r.name.split(" ")[0]: r for r in run_checks(gen, expected, inputs=None)}
            if target is None:
                bad = [r for r in results.values() if not r.ok]
                state = "PASS" if not bad else "FAIL"
                ok &= not bad
                print(f"[{state}] {name}: every check passes" + "".join(f"\n       {r.line()}" for r in bad))
            else:
                caught = not results[target].ok
                ok &= caught
                print(f"[{'PASS' if caught else 'FAIL'}] {name}: the {target} check "
                      f"{'fails, as it must' if caught else 'did NOT fail'}")
    return 0 if ok else 1


def main(argv=None) -> int:
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    from build_editions_index import default_inputs
    from shared.editions_indexer import input_stamp
    d = default_inputs()
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--gen", help="the generation directory to check")
    p.add_argument("--fgp-db", default=d["fgp_db"])
    p.add_argument("--pgp-db", default=d["pgp_db"])
    p.add_argument("--browse-map", default=d["browse_map"])
    p.add_argument("--nli-db", default=d["nli_db"])
    p.add_argument("--self-test", action="store_true", help="prove each check can fail")
    args = p.parse_args(argv)
    if args.self_test:
        return self_test(args)
    if not args.gen:
        p.error("--gen is required (or --self-test)")
    expected = dict(expected_fgp(args.fgp_db))
    expected.update(expected_pgp(args.pgp_db))
    inputs = input_stamp(args.fgp_db, args.pgp_db, args.browse_map, args.nli_db)
    results = run_checks(args.gen, expected, inputs)
    for r in results:
        print(r.line())
    ok = all(r.ok for r in results)
    print("GATE PASS" if ok else "GATE FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
