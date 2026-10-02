"""Snapshot execute_search output on a small real Tantivy fixture index.

Used to prove a search-engine performance change leaves results byte-identical:

    PYTHONHASHSEED=0 python scripts/search_output_snapshot.py before.json
    (apply the change)
    PYTHONHASHSEED=0 python scripts/search_output_snapshot.py after.json
    fc /b before.json after.json        (cmp on Linux)

PYTHONHASHSEED must be fixed: variant lists are built from sets, so the
highlight_pattern ORDER differs between runs without it (results do not).
The fixture covers brackets, V0.7/V0.8 same-uid duplicates, a system-scope doc
with page boundaries, newlines, asterisks and a diacritic form.
Add fixture rows when a change touches a case not listed here.
"""
import json, os, sys, tempfile
from unittest.mock import patch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tantivy
from shared.config import Config
from shared.indexer import Indexer
from shared.search_tokenizer import register_search_tokenizers
from shared.text_normalize import strip_search_diacritics
from shared.variants import VariantManager
import shared.search_engine as se

def schema():
    b = tantivy.SchemaBuilder()
    b.add_text_field("unique_id", stored=True)
    b.add_text_field("content", stored=True, tokenizer_name="hebword")
    for f in ("content_head", "content_tail", "line_starts", "line_ends"):
        b.add_text_field(f, stored=False, tokenizer_name="whitespace")
    b.add_text_field("content_search", stored=False, tokenizer_name="hebword")
    for f in ("source", "full_header", "shelfmark", "scope", "boundaries"):
        b.add_text_field(f, stored=True)
    return b.build()

PAGES = [
    ("u1", "V0.8", "IE1_P001_FL1 990001", "שלום עליכם\nוברכה שלום רב"),
    ("u2", "V0.8", "IE1_P002_FL2 990001", "שלו[ם] לכל ישראל * כוכב"),
    ("u3", "V0.7", "IE1_P002_FL2 990001", "שלום לכל ישראל"),          # dup uid? different uid
    ("u2", "V0.7", "IE1_P002_FL2 990001", "שלום ישראל dup"),          # same uid as V0.8
    ("u4", "V0.7", "IE2_P001_FL3 990002", "ש]לום בתחילת ]שלום השורה"),
    ("u5", "V0.8", "IE3_P001_FL4 990003", "x" * 200 + " שלום " + "y\n" * 50),
    ("u6", "V0.8", "IE4_P001_FL5 990004", "שלים וברכה שולם"),
    ("u7", "V0.8", "IE5_P001_FL6 990005", "צ̇מאן שלום ברכה והצלחה ברכה"),
    # The system doc's own pages, as the real index has them (every page is also a page doc).
    ("p1", "V0.8", "IE9_P001_FL9 990009", "עמוד ראשון שלום"),
    ("p2", "V0.8", "IE9_P002_FL10 990009", "עמוד שני שלום ברכה"),
]
SYSTEM = [
    ("s1", "V0.8", "IE9_P001_FL9 990009", "עמוד ראשון שלום\nעמוד שני שלום ברכה",
     [{"start": 0, "end": 16, "uid": "p1", "p_num": 1, "full_header": "IE9_P001_FL9 990009", "source": "V0.8", "sys_id": "990009"},
      {"start": 16, "end": 40, "uid": "p2", "p_num": 2, "full_header": "IE9_P002_FL10 990009", "source": "V0.8", "sys_id": "990009"}]),
]

class Meta:
    def get_display_data(self, h, s):
        return {"shelfmark": h, "title": "", "img": "1", "source": s, "id": h.split()[-1], "library_code": ""}
    def parse_full_id_components(self, h):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}

def build(d):
    db = os.path.join(d, "tantivy_db"); os.makedirs(db)
    idx = tantivy.Index(schema(), path=db); register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)  # one segment: stable tie order
    rows = [(u, s, h, c, "page", "") for u, s, h, c in PAGES] + \
           [(u, s, h, c, "system", json.dumps(b)) for u, s, h, c, b in SYSTEM]
    for u, s, h, c, scope, bnd in rows:
        pos = Indexer._extract_position_fields(c)
        w.add_document(tantivy.Document(unique_id=u, content=c, source=s, content_search=strip_search_diacritics(c),
            full_header=h, shelfmark=h, scope=scope, boundaries=bnd, **pos))
    w.commit(); w.wait_merging_threads()

QUERIES = [("שלום", "literal"), ("שלום", "variants"), ("שלום", "variants_extended"),
           ("שלום ברכה", "literal"), ("שלום ברכה", "variants"), ("שלו[ם]", "literal"),
           ("ש.ום", "Regex"), ("שלום ישראל", "literal"), ("צמאן", "literal"), ("שלום", "fuzzy")]

with tempfile.TemporaryDirectory() as d:
    build(d)
    with patch.object(Config, "INDEX_DIR", d):
        eng = se.SearchEngine(Meta(), VariantManager(), worker_mode=True, open_local=False)
        out = {}
        for q, m in QUERIES:
            ticks = []
            for gap in (0, 3):
                for pos in (None, "start"):
                    try:
                        r = eng.execute_search(q, m, gap, progress_callback=lambda i, t: ticks.append(i),
                                               text_position=pos, corpus_scope="genizah")
                    except Exception as e:
                        r = f"ERR {type(e).__name__}: {e}"
                    out[f"{q}|{m}|{gap}|{pos}"] = r
        json.dump(out, open(sys.argv[1], "w", encoding="utf-8"), ensure_ascii=False, indent=1, sort_keys=True, default=str)
        print({k: (len(v) if isinstance(v, list) else v) for k, v in out.items()})
