# -*- coding: utf-8 -*-
"""Which edition becomes ``documents.transcription`` -- and whose name goes on it.

On 2026-09-22 a two-pass rewrite of this lookup dropped the ``pgpid not in lookup`` guard
from its first pass, so the LAST exact ``Digital Edition`` row won where the first used to.
Measured on the real corpus: **657 documents** received a different scholar's text and a
different ``transcription_source`` -- Gil where it should have been Goitein, Friedman where
it should have been Olszowy-Schlanger. The import had already run, so live data needed
correcting.

Attribution is not cosmetic in this project: ``shared/transcription_credits.py`` drives the
printed sheet, the web "How to cite" chip, Word exports and the desktop citation bar from
this field, so a wrong scholar here becomes a wrong citation everywhere.

The lookup was inline in a 1,200-line ``main()``, which is why nothing caught it. It is a
function now, and these tests pin both passes and their ordering.
"""
from __future__ import annotations

import importlib.util
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "import_pgp_full.py"


@pytest.fixture(scope="module")
def importer():
    spec = importlib.util.spec_from_file_location("_import_pgp_full", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rec(pgpid, relation, scholar, content="text"):
    return {
        "pgpid": pgpid,
        "doc_relation": relation,
        "source_scholar": scholar,
        "content": content,
        "languages": "",
        "content_length": str(len(content)),
    }


def test_the_first_exact_digital_edition_wins(importer):
    """The regression, in one test. Several exact editions for one pgpid: the FIRST is
    the one whose text and attribution are stored."""
    records = [
        _rec(451, "Digital Edition", "Moshe Gil", "gil text"),
        _rec(451, "Digital Edition", "S. D. Goitein", "goitein text"),
        _rec(451, "Digital Edition", "Jacob Mann", "mann text"),
    ]
    lookup = importer.build_transcription_lookup(records)
    assert lookup[451]["source_scholar"] == "Moshe Gil"
    assert lookup[451]["content"] == "gil text"


def test_a_bare_edition_relation_is_rescued(importer):
    """pgpid 38267's only edition carries the relation 'Edition', not 'Digital Edition'.
    Before the second pass existed it got no transcription at all."""
    lookup = importer.build_transcription_lookup(
        [_rec(38267, "Edition", "S. D. Goitein", "goitein text")]
    )
    assert lookup[38267]["source_scholar"] == "S. D. Goitein"


def test_an_exact_digital_edition_beats_an_earlier_loose_one(importer):
    """pgpid 20107: a bare 'Edition' row appears BEFORE the explicit 'Digital Edition'.
    The exact match must still win, which is what the two passes are for."""
    records = [
        _rec(20107, "Edition", "Yusuf Umrethwala", "loose"),
        _rec(20107, "Digital Edition", "Alan Elbaum, Marina Rustow and Yusuf Umrethwala",
             "exact"),
    ]
    lookup = importer.build_transcription_lookup(records)
    assert lookup[20107]["content"] == "exact"
    assert lookup[20107]["source_scholar"].startswith("Alan Elbaum")


def test_the_first_loose_edition_wins_among_loose_ones(importer):
    """The fallback pass keeps first-wins too."""
    records = [
        _rec(9, "Edition", "First Scholar", "first"),
        _rec(9, "Edition ; Translation", "Second Scholar", "second"),
    ]
    assert importer.build_transcription_lookup(records)[9]["source_scholar"] == "First Scholar"


def test_translations_never_supply_the_transcription(importer):
    """A document whose only rows are translations gets NO transcription, so the upsert
    leaves any existing text alone rather than overwriting it with a translation."""
    records = [
        _rec(7, "Digital Translation", "A Translator", "translated"),
        _rec(7, "Translation", "Another", "also translated"),
    ]
    assert 7 not in importer.build_transcription_lookup(records)


def test_is_edition_relation_is_the_one_predicate(importer):
    """Classification (update_doc_relation.py) and selection must agree; they disagreed
    once, and a compound relation was classified as an edition while no row was selected."""
    assert importer.is_edition_relation("Digital Edition")
    assert importer.is_edition_relation("Edition")
    assert importer.is_edition_relation("Edition ; Translation ; Discussion")
    assert not importer.is_edition_relation("Digital Translation")
    assert not importer.is_edition_relation("")
    assert not importer.is_edition_relation(None)


class _RecordingTable:
    """Captures what would actually be sent to PostgREST."""

    def __init__(self, sink):
        self._sink = sink

    def upsert(self, batch, on_conflict=None):
        self._sink.append(batch)
        return self

    def execute(self):
        return self


class _RecordingClient:
    def __init__(self):
        self.batches = []

    def table(self, _name):
        return _RecordingTable(self.batches)


def test_records_with_different_columns_are_never_batched_together(importer):
    """The 2026-09-22 data loss, in one test.

    prepare_document_records() omits `transcription` / `transcription_source` for
    documents with no edition content, so the upsert leaves the stored value alone.
    PostgREST sends one request per batch whose column list is the UNION of the payload's
    keys -- so the moment a record that omitted the key shares a batch with one that
    carries it, the omission becomes an explicit NULL. Eight documents lost both fields
    that way, and re-running the import could not repair them, because omitting the key
    then faithfully preserved the NULL.
    """
    records = [
        {"pgpid": 1, "description": "has edition", "transcription": "text",
         "transcription_source": "Goitein"},
        {"pgpid": 2, "description": "no edition"},          # deliberately omits both
        {"pgpid": 3, "description": "has edition", "transcription": "more",
         "transcription_source": "Gil"},
    ]
    client = _RecordingClient()
    processed = importer.upsert_in_batches(
        client, "documents", records, on_conflict="pgpid", dry_run=False
    )

    assert processed == 3
    for batch in client.batches:
        column_sets = {frozenset(record) for record in batch}
        assert len(column_sets) == 1, (
            "a batch mixing column sets makes PostgREST NULL the omitted columns: %r"
            % column_sets
        )

    # ...and the record that omitted the keys must never appear beside them.
    for batch in client.batches:
        if any(r["pgpid"] == 2 for r in batch):
            assert all("transcription" not in r for r in batch)


def test_grouping_does_not_drop_or_duplicate_records(importer):
    records = [{"pgpid": i, **({"transcription": "t"} if i % 3 else {})} for i in range(50)]
    client = _RecordingClient()
    processed = importer.upsert_in_batches(
        client, "documents", records, on_conflict="pgpid", dry_run=False
    )
    sent = [r for batch in client.batches for r in batch]
    assert processed == 50
    assert sorted(r["pgpid"] for r in sent) == list(range(50))


def test_a_dry_run_sends_nothing(importer):
    client = _RecordingClient()
    assert importer.upsert_in_batches(
        client, "documents", [{"pgpid": 1}], on_conflict="pgpid", dry_run=True
    ) == 1
    assert client.batches == []
