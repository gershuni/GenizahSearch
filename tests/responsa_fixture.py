"""A small real Tantivy corpus for the Responsa tests: page docs, whole-manuscript docs
over them (built as Indexer._add_continuous_document builds them), and the same pages
as a LOCAL (My Library) index. Not a test module; the tests import it.

docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md §8 gate 3: one fixture corpus indexed as main
and as LOCAL, so a query's witnesses can be compared over their common domain.
"""
import gc
import json
import os
from unittest.mock import MagicMock, patch

import tantivy

from shared.config import Config
from shared.indexer import Indexer, build_main_schema
from shared.search_tokenizer import register_search_tokenizers
from shared.text_normalize import strip_search_diacritics


class Meta:
    """The metadata manager the engine asks for a row's display fields."""

    def get_display_data(self, header, source):
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header.split()[-1], "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}

    def parse_header_smart(self, header):
        return header.split()[-1], None

    def get_shelfmark_from_header(self, header):
        return header


def _add(writer, uid, text, scope, header, boundaries=""):
    writer.add_document(tantivy.Document(
        unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
        full_header=header, shelfmark=header, scope=scope, boundaries=boundaries,
        **Indexer._extract_position_fields(text)))


def build_main(root, manuscripts):
    """*manuscripts*: {sys_id: [(page uid, text), ...]} -> a main index under *root*:
    one page doc per page and one whole-manuscript (scope system) doc per manuscript
    with two or more pages, its pages joined by a line break."""
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(build_main_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    for sys_id, pages in manuscripts.items():
        headers = []
        for n, (uid, text) in enumerate(pages, 1):
            header = f"IE{sys_id}_P{n:03d}_FL{uid} {sys_id}"
            headers.append(header)
            _add(w, uid, text, "page", header)
        if len(pages) > 1:
            parts, bounds, cursor = [], [], 0
            for n, ((uid, text), header) in enumerate(zip(pages, headers), 1):
                start = cursor
                parts.append(text)
                cursor += len(text)
                if n != len(pages):
                    parts.append("\n")
                    cursor += 1
                bounds.append({"uid": uid, "p_num": n, "full_header": header, "source": "V0.8",
                               "sys_id": sys_id, "start": start, "end": cursor})
            _add(w, f"sys:{sys_id}", "".join(parts), "system", headers[0],
                 json.dumps(bounds, ensure_ascii=False))
    w.commit()
    w.wait_merging_threads()
    return str(root)


def build_local(index_dir, pages):
    """*pages*: [(uid, text)] -> a LOCAL index (pages only, as My Library stores them)."""
    from shared.local_indexer import build_local_schema, _compute_schema_marker, _write_schema_marker
    os.makedirs(index_dir)
    idx = tantivy.Index(build_local_schema(), path=index_dir)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=15_000_000)
    for n, (uid, text) in enumerate(pages, 1):
        pos = Indexer._extract_position_fields(text)
        w.add_document(tantivy.Document(
            unique_id=[uid], content=[text], content_search=[strip_search_diacritics(text)],
            content_head=[pos["content_head"]], content_tail=[pos["content_tail"]],
            line_starts=[pos["line_starts"]], line_ends=[pos["line_ends"]],
            source=["LOCAL"], full_header=[f"97000000010000{n:04d}_LOCAL_P1_F0001"],
            shelfmark=[uid], scope=["page"], boundaries=[""]))
    w.commit()
    del w, idx
    _write_schema_marker(index_dir, _compute_schema_marker(build_local_schema))


def make_engine(root, local_dir=None, var_mgr=None):
    """A SearchEngine on the fixture under *root* (and the LOCAL index at *local_dir*)."""
    from shared.search_engine import SearchEngine
    from shared.variants import VariantManager
    patches = [patch.object(Config, "INDEX_DIR", str(root)),
               patch.object(SearchEngine, "start_fl_id_index_build")]
    if local_dir is not None:
        patches.append(patch.object(Config, "LOCAL_INDEX_DIR", str(local_dir)))
        patches.append(patch.object(Config, "LOCAL_LAB_INDEX_DIR", os.path.join(str(root), "nolab")))
    for p in patches:
        p.start()
    try:
        eng = SearchEngine(Meta(), var_mgr or VariantManager(), worker_mode=True,
                           open_local=local_dir is not None)
    finally:
        for p in reversed(patches):
            p.stop()
    if local_dir is not None:
        eng._my_library_tab_ref = None
    return eng


def release(*engines):
    for e in engines:
        try:
            e.close_index()
            e.close_local_searcher()
        except Exception:
            pass
    gc.collect()


def witnesses(rows):
    """{(uid, highlighted text)} of result rows: the page and the marked span."""
    out = set()
    for r in rows:
        snippet = r.get("raw_file_hl") or r.get("snippet") or ""
        marked = snippet.split("*")[1] if snippet.count("*") >= 2 else None
        out.add((r["uid"], marked))
    return out


__all__ = ["Meta", "build_main", "build_local", "make_engine", "release", "witnesses", "MagicMock"]
