"""Composition grouping keeps one page per page IDENTITY, not per page number.

`SearchEngine.group_pages_by_manuscript` de-duplicates the pages of one
manuscript (or Codicological Part) because the same page can arrive from both
transcription files, V0.7 and V0.8. It used to key that on the page number
alone, so two DIFFERENT images sharing a number collapsed into one: the same P
in two image sets (IEs) of one manuscript -- 2,480 manuscripts in the installed
letter-level index have IEs that share P numbers -- or in two catalogue records
grouped into one Part. The key is now (sys_id, IE, P).
"""

from shared.metadata_manager import MetadataManager
from shared.search_engine import SearchEngine

SYS_A = '990000412990205171'
SYS_B = '990000413480205171'


class _Meta(MetadataManager):
    """The real header parsers; Part lookups from a fixed map."""

    def __init__(self, parts=None):  # skip MetadataManager.__init__
        self._parts = parts or {}

    def get_part_for_folio(self, sid):
        return self._parts.get(sid)

    def get_part_metadata(self, part_id):
        return {}

    def get_folios_for_part(self, part_id):
        return sorted(s for s, p in self._parts.items() if p == part_id)

    @property
    def codico_mgr(self):
        class _Codico:
            @staticmethod
            def get_part_display_name(part_id):
                return part_id
        return _Codico()


def _engine(parts=None):
    engine = SearchEngine.__new__(SearchEngine)  # bypass __init__
    engine.meta_mgr = _Meta(parts)
    return engine


def _page(header, score, uid):
    return {'raw_header': header, 'score': score, 'uid': uid, 'text': uid}


def _uids(items):
    return sorted(p['uid'] for item in items for p in item['pages'])


def test_same_page_number_in_two_image_sets_keeps_both():
    pages = [
        _page(f'{SYS_A}_IE104549337_P000002_FL104549340', 50.0, 'ie1-p2'),
        _page(f'{SYS_A}_IE19213988_P000002_FL19213993', 40.0, 'ie2-p2'),
    ]
    items = _engine().group_pages_by_manuscript(pages)
    assert len(items) == 1
    assert _uids(items) == ['ie1-p2', 'ie2-p2']


def test_same_page_number_in_two_records_of_one_part_keeps_both():
    pages = [
        _page(f'{SYS_A}_IE500_P000003_FL1', 50.0, 'a-p3'),
        _page(f'{SYS_B}_IE500_P000003_FL2', 40.0, 'b-p3'),
    ]
    items = _engine({SYS_A: 'part-1', SYS_B: 'part-1'}).group_pages_by_manuscript(pages)
    assert len(items) == 1 and items[0]['type'] == 'part'
    assert _uids(items) == ['a-p3', 'b-p3']


def test_same_page_from_both_transcription_files_still_collapses():
    v8 = f'{SYS_A}_IE104549337_P000001_FL104549339'
    v7 = (f'### Q:\\ERC\\all_version_0.9\\{SYS_A}\\IE104549337\\'
          'IE104549337_P000001_FL104549339\u2014reco.xml - JER NLI 6972=8')
    pages = [dict(_page(v8, 30.0, 'v8'), src_lbl='V0.8'),
             dict(_page(v7, 45.0, 'v7'), src_lbl='V0.7')]
    items = _engine().group_pages_by_manuscript(pages)
    assert _uids(items) == ['v7']  # one page, the higher-scoring copy


def test_distinct_pages_of_one_image_set_are_all_kept():
    pages = [_page(f'{SYS_A}_IE104549337_P00000{n}_FL1', 10.0 + n, f'p{n}')
             for n in (1, 2, 3)]
    items = _engine().group_pages_by_manuscript(pages)
    assert _uids(items) == ['p1', 'p2', 'p3']
    assert items[0]['score'] == sum(10.0 + n for n in (1, 2, 3))
