"""A small corpus built by the real Indexer, as format 2 (the seam index and the
reversed-words field, Responsa plan §6.1) and as format 1 (neither: every index before
Phase 2). Not a test module; the Phase 3 tests import it.

Each corpus is (V0.8 pages, V0.7 pages), every page ``(sys_id, IE, page number, text)``,
written as the corpus files are and indexed by ``Indexer.create_index``, so the page
documents, the whole-manuscript documents, the seams and the browse map are the ones a
real build makes.
"""
import os
from contextlib import contextmanager
from unittest.mock import patch

from shared.config import Config
from shared.indexer import Indexer
from shared.metadata_manager import MetadataManager
from shared.search_engine import SearchEngine
from shared.variants import VariantManager


class FakeCodico:
    part_to_folios = {}

    def load(self, csv_bank=None):
        return True

    def _get_folio_number(self, shelfmark):
        return 0


class FakeMeta:
    """The metadata the indexer and the engine ask for, with the real header parsers.
    A row's shelfmark is its header, so an order by shelfmark is an order by header."""
    meta_map = {}
    csv_bank = {}
    extract_unique_id = MetadataManager.extract_unique_id
    parse_full_id_components = MetadataManager.parse_full_id_components
    parse_header_smart = MetadataManager.parse_header_smart

    def __init__(self):
        self.codico_mgr = FakeCodico()

    def _load_csv_bank(self):
        pass

    def get_shelfmark_from_header(self, header):
        return ""

    def get_display_data(self, header, source):
        sys_id = self.parse_header_smart(header)[0]
        return {"shelfmark": header, "title": "", "img": "1", "source": source, "id": sys_id,
                "library_code": ""}


def uid(ie, p):
    """The page uid the indexer gives page *p* of volume *ie*."""
    return f"IE{ie}_P{p:06d}_FL{ie}{p}"


def write_corpus(root, v08, v07=()):
    v8, v7 = os.path.join(root, "v8.txt"), os.path.join(root, "v7.txt")
    with open(v8, "w", encoding="utf-8") as f:
        for sid, ie, p, text in v08:
            f.write(f"==> {sid}_{uid(ie, p)} <==\n{text}\n")
    with open(v7, "w", encoding="utf-8") as f:
        for sid, ie, p, text in v07:
            f.write(f"### Q:\\ERC\\x\\{sid}\\IE{ie}\\{uid(ie, p)}-reco.xml - TEST\n{text}\n")
    return v8, v7


def build(root, v08, v07=(), **kwargs):
    """The index (and browse map) under *root*, built by Indexer.create_index. Format 2
    by default; ``seams=False, reversed_field=False`` builds format 1."""
    root = str(root)
    os.makedirs(root, exist_ok=True)
    v8, v7 = write_corpus(root, v08, v07)
    with patch.object(Config, "INDEX_DIR", root), \
            patch.object(Config, "BROWSE_MAP", os.path.join(root, "browse_map.pkl")), \
            patch.object(Config, "FILE_V8", v8), patch.object(Config, "FILE_V7", v7):
        Indexer(FakeMeta()).create_index(**kwargs)
    return root


def engine(root, var_mgr=None):
    with patch.object(Config, "INDEX_DIR", str(root)), \
            patch.object(SearchEngine, "start_fl_id_index_build"):
        return SearchEngine(FakeMeta(), var_mgr or VariantManager(), worker_mode=True, open_local=False)


@contextmanager
def formats(tmp_path_factory, name, v08, v07=()):
    """{'2': engine on format 2, '1': engine on format 1} over one corpus, the browse map
    pointed at the fixture's for as long as they are open (never the real one)."""
    base = tmp_path_factory.mktemp(name)
    new = build(base / "format2", v08, v07)
    old = build(base / "format1", v08, v07, seams=False, reversed_field=False)
    browse = patch.object(Config, "BROWSE_MAP", os.path.join(new, "browse_map.pkl"))
    browse.start()
    SearchEngine._shared_browse_map = None
    engines = {"2": engine(new), "1": engine(old)}
    try:
        yield engines
    finally:
        for e in engines.values():
            e.close_index()
        SearchEngine._shared_browse_map = None
        browse.stop()
