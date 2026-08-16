# -*- coding: utf-8 -*-
"""Producer-side ``identity_mode: public_first`` support (discovery-v4.2 plan,
C5/C6/C8/C12, PRODUCER half).

The reconcile CONSUMER (scripts/discovery_v4_reconcile.py) and the artifact
loader (scripts/discovery_public_first_identity.py) already understand
public_first identities; this file covers the producer side that closes the
gap the plan's Status paragraph calls out:

 - scripts/discovery_v4_common.py::load_source_config's new identity_mode /
   identity_key validation, and source_target_ids's public_first skip.
 - scripts/discovery_v4_fetch_sources.py's public_first-aware mappings
   equality (both sides None), the C8 fail-closed completeness gate for the
   generic Wikisource chapter-link path, and identity_mode/identity_key
   carry-through into the acquisition manifest.
 - scripts/discovery_v4_build_reference.py's ``--public-first-artifact``
   wiring, artifact-only reference metadata, ref_id minting, locus labeling,
   and additive-only manifest/report shape.

EVERY fixture here is FABRICATED, masking-clean synthetic data: a made-up
Wikisource prefix, made-up Hebrew filler text, and synthetic ``pf-####``
identity keys. Nothing from ``same_work_spike/`` or ``discovery_builds/`` is
read, and the real repo artifact (scripts/discovery_v4_2_public_first_identities.json)
is never modified. No test performs network I/O.
"""

from __future__ import annotations

import argparse
import json
import pickle
import sqlite3
from pathlib import Path

import pytest

from scripts import discovery_v4_build_reference as build_reference
from scripts.build_work_divisions import WorkUnits
from scripts.discovery_public_first_identity import (
    SCHEMA_VERSION as PF_SCHEMA_VERSION,
    content_hash_for_entries,
)
from scripts.discovery_v4_common import (
    IDENTITY_MODE_PUBLIC_FIRST,
    load_source_config,
    sha256_file,
    source_target_ids,
)
from scripts.discovery_v4_fetch_sources import (
    Fetcher,
    _acquire_wikisource,
    _acquire_wikisource_page_clusters,
    select_chapter_links,
    run as fetch_run,
)
from shared.discovery_locus import daf_label_he, heb_numeral

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
V4_MAP = SCRIPTS / "discovery_v4_sources.json"
V4_1_MAP = SCRIPTS / "discovery_v4_1_sources.json"
V4_2_MAP = SCRIPTS / "discovery_v4_2_sources.json"

DAF_PREFIX = "בדיקה זהר הפקה דמו"


# ===========================================================================
# Shared fixture helpers
# ===========================================================================


def _write_map(tmp_path: Path, sources: list, *, namespace: str = "REF6", **extra) -> Path:
    doc = {
        "schema_version": "discovery-v4-sources-v1",
        "reference_namespace": namespace,
        "sources": sources,
        **extra,
    }
    path = tmp_path / "sources.json"
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return path


def _pf_source(*, key="pf_plain", identity_key="pf-1001", provider="sefaria", **overrides) -> dict:
    source = {
        "key": key,
        "provider": provider,
        "source_ref": "Fabricated Source Ref",
        "identity_mode": "public_first",
        "identity_key": identity_key,
    }
    source.update(overrides)
    return source


def _sibling_source(*, key="sib_one", target="w000001", **overrides) -> dict:
    source = {
        "key": key,
        "provider": "sefaria",
        "source_ref": "Fabricated Sibling Ref",
        "mappings": [{"target_work_id": target}],
    }
    source.update(overrides)
    return source


def _pf_artifact_entry(identity_key: str, verdict: str = "approve", **overrides) -> dict:
    base = {
        "identity_key": identity_key,
        "title_he": "חיבור בדוי דוגמה" if verdict == "approve" else "",
        "author": "מחבר בדוי" if verdict == "approve" else "",
        "genre": "הלכה" if verdict == "approve" else "",
        "domain_parent": "הלכה" if verdict == "approve" else "",
        "domain_leaf": "כללי" if verdict == "approve" else "",
        "provider": "sefaria" if verdict == "approve" else "",
        "source_ref": "Fabricated Source Ref" if verdict == "approve" else "",
        "license": "Public Domain" if verdict == "approve" else "",
        "verdict": verdict,
        "note": "",
    }
    base.update(overrides)
    return base


def _write_pf_artifact(tmp_path: Path, entries: list, *, name: str = "pf_artifact.json") -> Path:
    doc = {
        "schema_version": PF_SCHEMA_VERSION,
        "ruled_on": "2026-08-16",
        "entries": entries,
        "content_hash": content_hash_for_entries(entries),
    }
    path = tmp_path / name
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return path


def _write_normalized(path: Path, *, provider: str, units: list, mappings=None, **extra) -> None:
    doc = {
        "schema_version": "discovery-v4-acquired-source-v1",
        "key": path.stem,
        "provider": provider,
        "source_url": "https://example.invalid/fabricated",
        "license": "Public Domain",
        "mappings": mappings,
        "units": units,
        **extra,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


_LOCUS_SCHEMA = """
CREATE TABLE locus_work (
  locus_ref_id  TEXT PRIMARY KEY,
  family        TEXT NOT NULL,
  grain         TEXT NOT NULL,
  stream_len    INTEGER NOT NULL,
  unit_count    INTEGER NOT NULL);

CREATE TABLE locus_unit (
  locus_ref_id  TEXT NOT NULL REFERENCES locus_work(locus_ref_id),
  unit_ord      INTEGER NOT NULL,
  start_offset  INTEGER NOT NULL,
  part_key      TEXT NOT NULL,
  label_he      TEXT NOT NULL,
  citation_pos  INTEGER,
  PRIMARY KEY (locus_ref_id, unit_ord));
"""


def _base_corpus_row() -> dict:
    # Shaped exactly like a real reference_work row (id, cat, author, title,
    # date, genre, <legacy-key>, stream, ...) so _legacy_reference_metadata_key
    # derives correctly from it, same as production.
    return {
        "id": "BASE:seed",
        "cat": "Sefaria",
        "author": "",
        "title": "Seed",
        "date": "",
        "genre": "Genre",
        "legacy_private_field": "",
        "stream": "אבגד",
        "title_en": "",
        "provenance": "sefaria",
        "source_url": "",
        "license": "Public Domain",
        "ref_kind": "public_reference",
        "vgroup": None,
        "split_parent": None,
        "split_division": None,
    }


class _RunFixture:
    def __init__(self, tmp_path: Path, namespace: str) -> None:
        self.tmp_path = tmp_path
        self.namespace = namespace
        self.normalized_dir = tmp_path / "normalized"
        self.normalized_dir.mkdir(parents=True, exist_ok=True)


def _build_run_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    sources: list,
    acquired_entries: list,
    normalized: dict,
    private_works: tuple = (),
    namespace: str = "REF6",
    public_first_artifact_path: str | None = None,
    public_first_artifact_sha256: str | None = None,
) -> argparse.Namespace:
    """Build every input discovery_v4_build_reference.run() needs, entirely
    from synthetic fixtures, and monkeypatch the (unrelated) real Guide for
    the Perplexed chapter-matching so a fabricated refs_staging body never
    has to reproduce its 187-chapter structure."""
    fixture = _RunFixture(tmp_path, namespace)

    source_map_path = _write_map(tmp_path, sources, namespace=namespace)

    for key, doc_kwargs in normalized.items():
        _write_normalized(fixture.normalized_dir / f"{key}.json", **doc_kwargs)

    entries_out = []
    for entry in acquired_entries:
        key = entry["key"]
        normalized_path = fixture.normalized_dir / f"{key}.json"
        entries_out.append(
            {
                **entry,
                "normalized_file": normalized_path.name,
                "normalized_sha256": sha256_file(normalized_path),
            }
        )
    acquisition_manifest = {
        "schema_version": "discovery-v4-acquisition-manifest-v1",
        "source_map": str(source_map_path.resolve()),
        "source_map_sha256": sha256_file(source_map_path),
        "license_allowlist": ["public domain"],
        "minimum_hebrew_letters": 1,
        "entries": entries_out,
        "summary": {},
    }
    acquisition_path = tmp_path / "acquisition-manifest.json"
    acquisition_path.write_text(
        json.dumps(acquisition_manifest, ensure_ascii=False), encoding="utf-8"
    )

    private_db_path = tmp_path / "private.db"
    conn = sqlite3.connect(private_db_path)
    try:
        conn.execute(
            "CREATE TABLE works (work_id TEXT, neutral_title TEXT, author TEXT, "
            "genre TEXT, identity_visibility TEXT)"
        )
        conn.executemany(
            "INSERT INTO works VALUES (?, ?, ?, ?, ?)",
            [
                (w["work_id"], w["title"], w.get("author", ""), w.get("genre", ""), "private")
                for w in private_works
            ],
        )
        conn.commit()
    finally:
        conn.close()

    base_ref_path = tmp_path / "base-reference.pkl"
    with base_ref_path.open("wb") as stream:
        pickle.dump([_base_corpus_row()], stream, protocol=4)
    base_ref_sha256 = sha256_file(base_ref_path)

    base_locus_db_path = tmp_path / "base-locus.db"
    conn = sqlite3.connect(base_locus_db_path)
    try:
        conn.executescript(_LOCUS_SCHEMA)
        conn.commit()
    finally:
        conn.close()

    base_locus_coverage_path = tmp_path / "base-locus-coverage.json"
    base_locus_coverage_path.write_text(
        json.dumps(
            {
                "reference_corpus_sha256": base_ref_sha256,
                "works_with_units": 0,
                "units_total": 0,
                "by_family": {},
                "by_grain": {},
                "invariant_problems": [],
            }
        ),
        encoding="utf-8",
    )

    refs_staging = tmp_path / "refs_staging"
    refs_staging.mkdir(parents=True, exist_ok=True)
    (refs_staging / "guide.txt").write_text("placeholder", encoding="utf-8")
    (refs_staging / "manifest.json").write_text(
        json.dumps(
            {"entries": [{"key": "ja2_rambam_moreh", "body_file": "guide.txt"}]}
        ),
        encoding="utf-8",
    )

    def _fake_build_staged_ja_chapters(key, staging_dir, body_file, ref_id, shipped):
        # The real Guide chapter-matcher needs 187 markers in an exact,
        # pinned real text; that structure is orthogonal to what this file
        # tests (identity_mode/public_first routing), so it is stubbed with
        # a trivial, valid WorkUnits rather than faked with real content.
        return WorkUnits(ref_id, "ja", "chapter", [], 0)

    monkeypatch.setattr(
        build_reference, "build_staged_ja_chapters", _fake_build_staged_ja_chapters
    )

    ns = argparse.Namespace(
        base_reference=str(base_ref_path),
        base_reference_sha256=base_ref_sha256,
        acquisition_manifest=str(acquisition_path),
        acquisition_manifest_sha256=sha256_file(acquisition_path),
        normalized_dir=str(fixture.normalized_dir),
        private_db=str(private_db_path),
        source_map=str(source_map_path),
        reference_namespace=namespace,
        base_locus_db=str(base_locus_db_path),
        base_locus_sha256=sha256_file(base_locus_db_path),
        base_locus_coverage=str(base_locus_coverage_path),
        base_locus_coverage_sha256=sha256_file(base_locus_coverage_path),
        refs_staging=str(refs_staging),
        output_reference=str(tmp_path / "out-reference.pkl"),
        output_manifest=str(tmp_path / "out-manifest.json"),
        output_locus_db=str(tmp_path / "out-locus.db"),
        output_locus_coverage=str(tmp_path / "out-locus-coverage.json"),
    )
    if public_first_artifact_path is not None:
        ns.public_first_artifact = public_first_artifact_path
        ns.public_first_artifact_sha256 = public_first_artifact_sha256
    return ns


# ===========================================================================
# 1. scripts/discovery_v4_common.py -- load_source_config validation battery
# ===========================================================================


def test_unknown_identity_mode_is_rejected(tmp_path):
    path = _write_map(tmp_path, [_pf_source(identity_mode="bogus_mode")])
    with pytest.raises(ValueError, match="invalid identity_mode"):
        load_source_config(path)


def test_public_first_without_identity_key_is_rejected(tmp_path):
    source = _pf_source()
    del source["identity_key"]
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="invalid identity_key"):
        load_source_config(path)


def test_public_first_with_mappings_is_rejected(tmp_path):
    source = _pf_source(mappings=[{"target_work_id": "w000001"}])
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="must not carry mappings"):
        load_source_config(path)


def test_container_and_public_first_is_rejected(tmp_path):
    source = _pf_source(
        provider="sefaria",
        container=True,
        children=[{"child_key": "a", "source_ref": "Ref A"}],
        locus_grain="section",
    )
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="cannot be a container"):
        load_source_config(path)


def test_duplicate_identity_key_across_sources_is_rejected(tmp_path):
    path = _write_map(
        tmp_path,
        [
            _pf_source(key="pf_a", identity_key="pf-1001"),
            _pf_source(key="pf_b", identity_key="pf-1001"),
        ],
    )
    with pytest.raises(ValueError, match="duplicate identity_key"):
        load_source_config(path)


def test_private_sibling_carrying_identity_key_is_rejected(tmp_path):
    source = _sibling_source(identity_key="pf-1001")
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="carries an identity_key"):
        load_source_config(path)


def test_identity_mode_absent_carrying_identity_key_is_also_rejected(tmp_path):
    # Same gate as above, exercised via the ABSENT (not explicit
    # "private_sibling") default path.
    source = _sibling_source()
    source["identity_key"] = "pf-1001"
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="carries an identity_key"):
        load_source_config(path)


@pytest.mark.parametrize(
    "bad_key", ["pf-1", "pf-10011", "PF-1001", "pf_1001", "1001", ""]
)
def test_identity_key_syntax_is_enforced(tmp_path, bad_key):
    path = _write_map(tmp_path, [_pf_source(identity_key=bad_key)])
    with pytest.raises(ValueError, match="invalid identity_key"):
        load_source_config(path)


def test_public_first_source_with_valid_shape_is_accepted(tmp_path):
    path = _write_map(tmp_path, [_pf_source()])
    config = load_source_config(path)
    source = config["sources"][0]
    assert source["identity_mode"] == IDENTITY_MODE_PUBLIC_FIRST
    assert "mappings" not in source


def test_public_first_source_may_carry_daf_pages_mode(tmp_path):
    source = _pf_source(
        key="pf_daf",
        identity_key="pf-1002",
        provider="hewikisource",
        mode="daf_pages",
        link_prefix=DAF_PREFIX,
        daf_range=[1, 3],
    )
    del source["source_ref"]
    path = _write_map(tmp_path, [source])
    config = load_source_config(path)
    assert config["sources"][0]["mode"] == "daf_pages"


def test_source_target_ids_skips_public_first_sources(tmp_path):
    path = _write_map(
        tmp_path,
        [
            _sibling_source(key="sib", target="w000001"),
            _pf_source(key="pf_one", identity_key="pf-1001"),
        ],
    )
    config = load_source_config(path)
    assert source_target_ids(config) == {"w000001"}


def test_existing_v4_v4_1_v4_2_maps_still_load_unaffected():
    # V4/V4.1 predate C5 entirely -- no source in either map may carry
    # identity_mode/identity_key. V4.2 now DOES (31 owner-approved
    # public_first additions, this session; exact composition pinned in
    # tests/test_discovery_v4_2_containers.py) -- its 19 pre-existing
    # sources (15 containers + 4 private_sibling additions) are still
    # identity_mode-absent, proving the C5 extension over this map is
    # strictly additive.
    v4 = load_source_config(V4_MAP)
    v4_1 = load_source_config(V4_1_MAP)
    for config in (v4, v4_1):
        for source in config["sources"]:
            assert source.get("identity_mode") is None
            assert "identity_key" not in source

    v4_2 = load_source_config(V4_2_MAP)
    pre_existing = [
        source
        for source in v4_2["sources"]
        if source.get("identity_mode") != "public_first"
    ]
    assert len(pre_existing) == 19
    for source in pre_existing:
        assert source.get("identity_mode") is None
        assert "identity_key" not in source
    public_first = [
        source for source in v4_2["sources"] if source.get("identity_mode") == "public_first"
    ]
    assert len(public_first) == 31


# ===========================================================================
# 2. scripts/discovery_v4_fetch_sources.py
# ===========================================================================


class _FakeFetcher:
    """Stand-in for Fetcher.wikisource_parse: canned per-title replies."""

    def __init__(self, raw_root: Path, pages: dict) -> None:
        self.raw_dir = raw_root
        self._pages = pages

    def wikisource_parse(self, page: str, raw_path: Path) -> dict:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        doc = self._pages.get(page, {"error": {"code": "missingtitle"}})
        raw_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        return doc


def _wiki_page_doc(title: str, revid: int, text: str) -> dict:
    return {
        "parse": {
            "title": title,
            "revid": revid,
            "text": f'<div class="mw-parser-output"><p>{text}</p></div>',
        }
    }


def test_fetch_partial_coverage_is_a_hard_error_for_public_first(tmp_path):
    prefix = "בדיקה פרק"
    main_title = "בדיקה שער"
    pages = {
        main_title: {
            "parse": {
                "title": main_title,
                "links": [
                    {"ns": 0, "title": f"{prefix} א"},
                    {"ns": 0, "title": f"{prefix} ב"},
                ],
            }
        },
        f"{prefix} א": _wiki_page_doc(f"{prefix} א", 1, "טקסט בדיקה אחד"),
        # "{prefix} ב" deliberately absent -> missing_pages non-empty.
    }
    fetcher = _FakeFetcher(tmp_path, pages)
    source = {
        "key": "pf_partial",
        "provider": "hewikisource",
        "source_ref": main_title,
        "link_prefix": prefix,
        "identity_mode": "public_first",
        "identity_key": "pf-1003",
    }
    with pytest.raises(ValueError, match="public_first completeness gate failed"):
        _acquire_wikisource(fetcher, source)


def test_fetch_partial_coverage_still_succeeds_for_private_sibling(tmp_path):
    """The same missing-page shape must NOT raise for a private_sibling (or
    identity_mode-absent) source -- the existing coverage_status="partial"
    escape is unaffected."""
    prefix = "בדיקה פרק"
    main_title = "בדיקה שער"
    pages = {
        main_title: {
            "parse": {
                "title": main_title,
                "links": [
                    {"ns": 0, "title": f"{prefix} א"},
                    {"ns": 0, "title": f"{prefix} ב"},
                ],
            }
        },
        f"{prefix} א": _wiki_page_doc(f"{prefix} א", 1, "טקסט בדיקה אחד"),
    }
    fetcher = _FakeFetcher(tmp_path, pages)
    source = {
        "key": "sib_partial",
        "provider": "hewikisource",
        "source_ref": main_title,
        "link_prefix": prefix,
        "mappings": [{"target_work_id": "w000001"}],
    }
    acquired, _raw_paths = _acquire_wikisource(fetcher, source)
    assert acquired["coverage_status"] == "partial"
    assert acquired["missing_pages"]


def test_fetch_run_carries_identity_mode_into_manifest_entry(tmp_path, monkeypatch):
    prefix = "בדיקה פרק שלם"
    main_title = "בדיקה שער שלם"
    pages = {
        main_title: {
            "parse": {
                "title": main_title,
                "links": [{"ns": 0, "title": f"{prefix} א"}],
            }
        },
        f"{prefix} א": _wiki_page_doc(f"{prefix} א", 1, "טקסט בדיקה שלם"),
    }

    def fake_wikisource_parse(self, page, raw_path):
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        doc = pages.get(page, {"error": {"code": "missingtitle"}})
        raw_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        return doc

    monkeypatch.setattr(Fetcher, "wikisource_parse", fake_wikisource_parse)

    source_map = _write_map(
        tmp_path,
        [
            {
                "key": "pf_complete",
                "provider": "hewikisource",
                "source_ref": main_title,
                "link_prefix": prefix,
                "identity_mode": "public_first",
                "identity_key": "pf-1004",
            }
        ],
        namespace="REF6",
        license_allowlist=["cc-by-sa"],
        minimum_hebrew_letters=1,
    )
    args = argparse.Namespace(
        source_map=str(source_map),
        output_dir=str(tmp_path / "out"),
        timeout=5,
        reuse_existing=False,
    )
    manifest = fetch_run(args)
    assert manifest["summary"]["acquired_sources"] == 1
    entry = manifest["entries"][0]
    assert entry["status"] == "acquired"
    assert entry["identity_mode"] == "public_first"
    assert entry["identity_key"] == "pf-1004"
    assert entry["target_work_ids"] == []

    normalized_path = tmp_path / "out" / "normalized" / "pf_complete.json"
    normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
    assert normalized["mappings"] is None


def test_fetch_run_reuse_existing_agrees_on_absent_mappings_for_public_first(
    tmp_path, monkeypatch
):
    """Both the freshly re-read normalized file and the source map must agree
    that ``mappings`` is absent/None for a public_first source -- never a
    KeyError, and never a spurious 'mapping drift' error."""
    prefix = "בדיקה פרק שני"
    main_title = "בדיקה שער שני"
    pages = {
        main_title: {
            "parse": {"title": main_title, "links": [{"ns": 0, "title": f"{prefix} א"}]}
        },
        f"{prefix} א": _wiki_page_doc(f"{prefix} א", 1, "טקסט בדיקה שני"),
    }

    def fake_wikisource_parse(self, page, raw_path):
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        doc = pages.get(page, {"error": {"code": "missingtitle"}})
        raw_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        return doc

    monkeypatch.setattr(Fetcher, "wikisource_parse", fake_wikisource_parse)

    source_map = _write_map(
        tmp_path,
        [
            {
                "key": "pf_reuse",
                "provider": "hewikisource",
                "source_ref": main_title,
                "link_prefix": prefix,
                "identity_mode": "public_first",
                "identity_key": "pf-1005",
            }
        ],
        license_allowlist=["cc-by-sa"],
        minimum_hebrew_letters=1,
    )
    out_dir = tmp_path / "out"
    args = argparse.Namespace(
        source_map=str(source_map), output_dir=str(out_dir), timeout=5, reuse_existing=False
    )
    fetch_run(args)

    # Second run with --reuse-existing must not raise "mapping drift".
    args_reuse = argparse.Namespace(
        source_map=str(source_map), output_dir=str(out_dir), timeout=5, reuse_existing=True
    )
    manifest = fetch_run(args_reuse)
    assert manifest["entries"][0]["status"] == "acquired"


# ===========================================================================
# 3. scripts/discovery_v4_build_reference.py
# ===========================================================================


def _daf_units():
    return [
        {
            "ordinal": 1,
            "label": daf_label_he(1, 1),
            "provider_ref": f"{DAF_PREFIX} {heb_numeral(1)} א",
            "text": "טקסט בדיקה עבור עמוד ראשון",
            "hebrew_letters": 10,
        },
        {
            "ordinal": 2,
            "label": daf_label_he(1, 2),
            "provider_ref": f"{DAF_PREFIX} {heb_numeral(1)} ב",
            "text": "טקסט בדיקה עבור עמוד שני",
            "hebrew_letters": 10,
        },
    ]


def _plain_units():
    return [
        {"ordinal": 1, "label": "פרק א", "provider_ref": "ref-1", "text": "טקסט ראשון", "hebrew_letters": 6},
        {"ordinal": 2, "label": "פרק ב", "provider_ref": "ref-2", "text": "טקסט שני", "hebrew_letters": 6},
    ]


def test_build_hard_errors_when_public_first_live_and_no_artifact_supplied(
    tmp_path, monkeypatch
):
    source = _pf_source(key="pf_x", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_x",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_x": {"provider": "sefaria", "units": _plain_units()}},
    )
    with pytest.raises(ValueError, match="no --public-first-artifact was supplied"):
        build_reference.run(ns)


def test_build_both_or_neither_cli_validation(tmp_path, monkeypatch):
    source = _pf_source(key="pf_x", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_x",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_x": {"provider": "sefaria", "units": _plain_units()}},
    )
    ns.public_first_artifact = str(tmp_path / "does-not-need-to-exist.json")
    # Deliberately omit public_first_artifact_sha256.
    with pytest.raises(ValueError, match="supplied together"):
        build_reference.run(ns)


@pytest.mark.parametrize("verdict", ["reject", "defer"])
def test_build_hard_errors_on_rejected_or_deferred_artifact_entry(
    tmp_path, monkeypatch, verdict
):
    artifact_path = _write_pf_artifact(
        tmp_path, [_pf_artifact_entry("pf-1001", verdict=verdict, note="x")]
    )
    source = _pf_source(key="pf_x", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_x",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_x": {"provider": "sefaria", "units": _plain_units()}},
        public_first_artifact_path=str(artifact_path),
        public_first_artifact_sha256=sha256_file(artifact_path),
    )
    with pytest.raises(ValueError, match="absent, rejected, or deferred"):
        build_reference.run(ns)


def test_build_hard_errors_on_identity_key_absent_from_artifact(tmp_path, monkeypatch):
    artifact_path = _write_pf_artifact(tmp_path, [_pf_artifact_entry("pf-9999")])
    source = _pf_source(key="pf_x", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_x",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_x": {"provider": "sefaria", "units": _plain_units()}},
        public_first_artifact_path=str(artifact_path),
        public_first_artifact_sha256=sha256_file(artifact_path),
    )
    with pytest.raises(ValueError, match="absent, rejected, or deferred"):
        build_reference.run(ns)


def test_build_takes_title_author_genre_from_artifact_not_provider(tmp_path, monkeypatch):
    artifact_path = _write_pf_artifact(
        tmp_path,
        [
            _pf_artifact_entry(
                "pf-1001",
                title_he="כותרת מהאמנה",
                author="מחבר מהאמנה",
                genre="סוגה מהאמנה",
            )
        ],
    )
    source = _pf_source(key="pf_x", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_x",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={
            # A provider-fetched title/author/genre that must NEVER surface
            # anywhere in the built reference (C5: the provider title is
            # evidence, never an identifier).
            "pf_x": {
                "provider": "sefaria",
                "units": _plain_units(),
                "provider_title": "כותרת מהספק, לא בשימוש",
            }
        },
        public_first_artifact_path=str(artifact_path),
        public_first_artifact_sha256=sha256_file(artifact_path),
    )
    build_reference.run(ns)

    with Path(ns.output_reference).open("rb") as stream:
        corpus = pickle.load(stream)
    minted = next(work for work in corpus if work["id"] == "REF6:pf_x")
    assert minted["title"] == "כותרת מהאמנה"
    assert minted["author"] == "מחבר מהאמנה"
    assert minted["genre"] == "סוגה מהאמנה"
    assert "מהספק" not in minted["title"]


def test_build_manifest_entry_shape_for_public_first(tmp_path, monkeypatch):
    artifact_path = _write_pf_artifact(tmp_path, [_pf_artifact_entry("pf-1001")])
    source = _pf_source(key="pf_x", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_x",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_x": {"provider": "sefaria", "units": _plain_units()}},
        public_first_artifact_path=str(artifact_path),
        public_first_artifact_sha256=sha256_file(artifact_path),
    )
    report = build_reference.run(ns)
    entry = next(e for e in report["entries"] if e["source_key"] == "pf_x")
    assert entry["identity_mode"] == "public_first"
    assert entry["identity_key"] == "pf-1001"
    assert entry["raw_reference_id"] == "REF6:pf_x"
    assert "target_private_work_id" not in entry
    assert report["public_first_artifact_sha256"] == sha256_file(artifact_path)
    assert report["public_first_artifact_content_hash"] == json.loads(
        artifact_path.read_text(encoding="utf-8")
    )["content_hash"]


def test_build_ref_id_is_namespace_plus_key_with_no_chapter_splitting(tmp_path, monkeypatch):
    artifact_path = _write_pf_artifact(tmp_path, [_pf_artifact_entry("pf-1001")])
    source = _pf_source(key="pf_solo", identity_key="pf-1001")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_solo",
                "provider": "sefaria",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_solo": {"provider": "sefaria", "units": _plain_units()}},
        public_first_artifact_path=str(artifact_path),
        public_first_artifact_sha256=sha256_file(artifact_path),
    )
    report = build_reference.run(ns)
    assert report["new_reference_count"] == 1
    assert report["entries"][0]["raw_reference_id"] == "REF6:pf_solo"
    assert report["entries"][0]["unit_offsets"][0]["source_ordinal"] == 1
    assert report["entries"][0]["unit_offsets"][1]["source_ordinal"] == 2


def test_build_daf_grain_locus_labels_for_public_first(tmp_path, monkeypatch):
    """Exercises _locus_label's "daf" branch with mapping={} and a synthetic
    work carrying only a title -- the grain that never touches ``work`` at
    all, proving the substitution is safe even when work-lookup would have
    been irrelevant."""
    artifact_path = _write_pf_artifact(tmp_path, [_pf_artifact_entry("pf-1001")])
    source = _pf_source(
        key="pf_daf",
        identity_key="pf-1001",
        provider="hewikisource",
        mode="daf_pages",
        link_prefix=DAF_PREFIX,
        daf_range=[1, 1],
    )
    del source["source_ref"]
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "pf_daf",
                "provider": "hewikisource",
                "status": "acquired",
                "identity_mode": "public_first",
                "identity_key": "pf-1001",
            }
        ],
        normalized={"pf_daf": {"provider": "hewikisource", "units": _daf_units()}},
        public_first_artifact_path=str(artifact_path),
        public_first_artifact_sha256=sha256_file(artifact_path),
    )
    report = build_reference.run(ns)
    entry = report["entries"][0]
    assert entry["locus_grain"] == "daf"
    rows = entry["unit_offsets"]
    assert rows[0]["label_he"] == daf_label_he(1, 1)
    assert rows[1]["label_he"] == daf_label_he(1, 2)
    assert rows[0]["citation_pos"] == 1 * 2 + 1 - 1
    assert rows[1]["citation_pos"] == 1 * 2 + 2 - 1


def test_build_zero_public_first_sources_is_additive_only(tmp_path, monkeypatch):
    """A map with ONLY private_sibling sources (no public_first at all, no
    artifact supplied) must produce a manifest/report with NO identity_mode
    keys anywhere and NO public_first report keys -- proving the producer
    change is strictly additive over the pre-C5 shape."""
    source = _sibling_source(key="sib_only", target="w000010")
    ns = _build_run_namespace(
        tmp_path,
        monkeypatch,
        sources=[source],
        acquired_entries=[
            {
                "key": "sib_only",
                "provider": "sefaria",
                "status": "acquired",
            }
        ],
        normalized={
            "sib_only": {
                "provider": "sefaria",
                "units": _plain_units(),
                "mappings": [{"target_work_id": "w000010"}],
            }
        },
        private_works=[{"work_id": "w000010", "title": "Private Ten", "author": "A", "genre": "G"}],
    )
    report = build_reference.run(ns)

    assert "public_first_artifact_sha256" not in report
    assert "public_first_artifact_content_hash" not in report
    assert report["new_reference_count"] == 1
    entry = report["entries"][0]
    assert set(entry) == {
        "raw_reference_id",
        "source_key",
        "target_private_work_id",
        "title",
        "provider",
        "license",
        "source_url",
        "source_coverage_status",
        "source_missing_pages",
        "stream_len",
        "stream_sha256",
        "locus_grain",
        "unit_offsets",
    }
    assert entry["target_private_work_id"] == "w000010"
    assert entry["title"] == "Private Ten"

    with Path(ns.output_reference).open("rb") as stream:
        corpus = pickle.load(stream)
    minted = next(work for work in corpus if work["id"] == "REF6:sib_only")
    assert minted["title"] == "Private Ten"
    assert minted["author"] == "A"
    assert minted["genre"] == "G"


# ===========================================================================
# 4. discovery-v4.2 A1/A2/A4: exclude_pages, duplicate-ordinal naming, and
#    the dead "missing" filter over select_chapter_links -- all FABRICATED,
#    masking-clean synthetic titles (no network I/O; mirrors the real Tur/
#    redirect-twin shape without copying any restricted fact).
# ===========================================================================


def _fake_toc_links(*titles: str) -> list[dict]:
    return [{"ns": 0, "title": title} for title in titles]


def test_exclude_pages_removes_exactly_the_named_pages():
    # The Tur trap (A1): an auxiliary page's title ("הקדמה") gematria-parses
    # to the same value (154) as a real, numbered page ("קנד") -- excluding
    # the auxiliary page resolves the collision and leaves every other page
    # untouched.
    links = _fake_toc_links(
        "בדיקה טור הקדמה", "בדיקה טור קנד", "בדיקה טור א", "בדיקה טור ב"
    )
    selected = select_chapter_links(
        links, "בדיקה טור ", exclude_pages=["בדיקה טור הקדמה"]
    )
    assert selected == [
        (1, "בדיקה טור א"),
        (2, "בדיקה טור ב"),
        (154, "בדיקה טור קנד"),
    ]


def test_exclude_pages_stale_entry_hard_errors():
    links = _fake_toc_links("בדיקה טור א", "בדיקה טור ב")
    with pytest.raises(ValueError, match="stale exclusion"):
        select_chapter_links(links, "בדיקה טור ", exclude_pages=["בדיקה טור לא קיים"])


def test_duplicate_ordinal_selection_names_ordinal_and_titles():
    # The live-redirect-twin shape (A2): two titles sharing one gematria
    # value (304) with no exclusion supplied to resolve it.
    links = _fake_toc_links("בדיקה טור שד", "בדיקה טור דש")
    with pytest.raises(ValueError) as excinfo:
        select_chapter_links(links, "בדיקה טור ")
    message = str(excinfo.value)
    assert "304" in message
    assert "בדיקה טור שד" in message
    assert "בדיקה טור דש" in message


def test_duplicate_ordinal_survives_after_exclusion_of_an_unrelated_page():
    # An exclusion resolves ONLY the collision it names -- a second,
    # unrelated collision must still hard-error.
    links = _fake_toc_links(
        "בדיקה טור הקדמה", "בדיקה טור קנד", "בדיקה טור שד", "בדיקה טור דש"
    )
    with pytest.raises(ValueError, match="duplicate ordinal"):
        select_chapter_links(links, "בדיקה טור ", exclude_pages=["בדיקה טור הקדמה"])


def test_missing_link_flag_is_not_filtered_a_dead_filter_removed():
    # A4: MediaWiki's formatversion=2 "parse" response marks non-existence
    # via "exists": false, never "missing" -- select_chapter_links must not
    # special-case either key at selection time. A link explicitly carrying
    # "missing": True is still SELECTED here; whether it is genuinely
    # unwritten is resolved at FETCH time (the per-page request), never by
    # a selection-time filter that would let a public_first source
    # under-report its own coverage.
    links = [
        {"ns": 0, "title": "בדיקה טור א"},
        {"ns": 0, "title": "בדיקה טור ב", "missing": True},
    ]
    assert select_chapter_links(links, "בדיקה טור ") == [
        (1, "בדיקה טור א"),
        (2, "בדיקה טור ב"),
    ]


# ===========================================================================
# 5. discovery-v4.2 A3: page_clusters -- load_source_config validation
# ===========================================================================


def _page_clusters_source(*, key: str = "pc_src", clusters: list | None = None, **overrides) -> dict:
    # identity_mode-absent (private_sibling) by default: the C5 "public_first
    # cannot be a container" gate fires BEFORE page_clusters validation ever
    # runs, so a mutex test that sets ``container`` would hit that gate
    # first under public_first -- keeping this shape identity_mode-absent
    # lets the page_clusters-specific gates be exercised in isolation.
    # Callers that need public_first pass identity_mode/identity_key explicitly.
    source = {
        "key": key,
        "provider": "hewikisource",
        "page_clusters": clusters
        if clusters is not None
        else [
            {"toc_page": "בדיקה עמוד א", "link_prefix": "בדיקה עמוד א/פרק "},
            {"toc_page": "בדיקה עמוד ב", "link_prefix": "בדיקה עמוד ב/פרק "},
        ],
        "mappings": [{"target_work_id": "w000001"}],
    }
    source.update(overrides)
    return source


def test_page_clusters_source_with_valid_shape_is_accepted(tmp_path):
    path = _write_map(tmp_path, [_page_clusters_source()])
    config = load_source_config(path)
    source = config["sources"][0]
    assert len(source["page_clusters"]) == 2


@pytest.mark.parametrize(
    "mutex_field,mutex_value",
    [
        ("source_ref", "Something"),
        ("link_prefix", "בדיקה "),
        ("mode", "daf_pages"),
        ("container", True),
        ("exclude_pages", ["x"]),
    ],
)
def test_page_clusters_is_mutually_exclusive_with_single_toc_fields(
    tmp_path, mutex_field, mutex_value
):
    source = _page_clusters_source(**{mutex_field: mutex_value})
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="mutually exclusive"):
        load_source_config(path)


def test_page_clusters_requires_hewikisource_provider(tmp_path):
    source = _page_clusters_source(provider="sefaria")
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="requires provider 'hewikisource'"):
        load_source_config(path)


def test_page_clusters_must_be_a_non_empty_list(tmp_path):
    source = _page_clusters_source(clusters=[])
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="non-empty list"):
        load_source_config(path)


def test_page_clusters_cluster_requires_toc_page(tmp_path):
    source = _page_clusters_source(clusters=[{"link_prefix": "x "}])
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="invalid toc_page"):
        load_source_config(path)


def test_page_clusters_cluster_requires_link_prefix(tmp_path):
    source = _page_clusters_source(clusters=[{"toc_page": "x"}])
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="invalid link_prefix"):
        load_source_config(path)


def test_page_clusters_cluster_rejects_unknown_keys(tmp_path):
    source = _page_clusters_source(
        clusters=[{"toc_page": "x", "link_prefix": "y ", "bogus": 1}]
    )
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="unknown keys"):
        load_source_config(path)


def test_page_clusters_cluster_exclude_pages_is_shape_validated(tmp_path):
    source = _page_clusters_source(
        clusters=[{"toc_page": "x", "link_prefix": "y ", "exclude_pages": []}]
    )
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="exclude_pages must be a non-empty list"):
        load_source_config(path)


def test_top_level_exclude_pages_requires_hewikisource_non_container(tmp_path):
    source = _sibling_source(provider="sefaria")
    source["exclude_pages"] = ["x"]
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="only allowed on a hewikisource ToC source"):
        load_source_config(path)


def test_top_level_exclude_pages_rejected_on_container(tmp_path):
    source = {
        "key": "cont",
        "provider": "sefaria",
        "container": True,
        "children": [{"child_key": "a", "source_ref": "Ref A"}],
        "exclude_pages": ["x"],
        "mappings": [{"target_work_id": "w000001"}],
    }
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="only allowed on a hewikisource ToC source"):
        load_source_config(path)


def test_top_level_exclude_pages_shape_validated(tmp_path):
    source = _sibling_source(provider="hewikisource")
    source["link_prefix"] = "בדיקה "
    source["exclude_pages"] = ["dup", "dup"]
    path = _write_map(tmp_path, [source])
    with pytest.raises(ValueError, match="duplicate title"):
        load_source_config(path)


# ===========================================================================
# 6. discovery-v4.2 A3: page_clusters -- acquisition (_FakeFetcher, no I/O)
# ===========================================================================


def test_page_clusters_global_reordinalization_and_labels_are_page_titles(tmp_path):
    prefix_a = "בדיקה ספר א"
    prefix_b = "בדיקה ספר ב"
    toc_a = "בדיקה ראשי א"
    toc_b = "בדיקה ראשי ב"
    pages = {
        toc_a: {
            "parse": {
                "title": toc_a,
                "links": [
                    {"ns": 0, "title": f"{prefix_a}/א"},
                    {"ns": 0, "title": f"{prefix_a}/ב"},
                ],
            }
        },
        toc_b: {
            "parse": {
                "title": toc_b,
                "links": [
                    {"ns": 0, "title": f"{prefix_b}/א"},
                    {"ns": 0, "title": f"{prefix_b}/ב"},
                ],
            }
        },
        f"{prefix_a}/א": _wiki_page_doc(f"{prefix_a}/א", 1, "טקסט אחד"),
        f"{prefix_a}/ב": _wiki_page_doc(f"{prefix_a}/ב", 2, "טקסט שתיים"),
        f"{prefix_b}/א": _wiki_page_doc(f"{prefix_b}/א", 3, "טקסט שלוש"),
        f"{prefix_b}/ב": _wiki_page_doc(f"{prefix_b}/ב", 4, "טקסט ארבע"),
    }
    fetcher = _FakeFetcher(tmp_path, pages)
    source = {
        "key": "pc_test",
        "provider": "hewikisource",
        "page_clusters": [
            {"toc_page": toc_a, "link_prefix": f"{prefix_a}/"},
            {"toc_page": toc_b, "link_prefix": f"{prefix_b}/"},
        ],
        "identity_mode": "public_first",
        "identity_key": "pf-1007",
    }
    acquired, _raw_paths = _acquire_wikisource_page_clusters(fetcher, source)
    # Both clusters restart their own local ordinal at 1 ("א") -- the
    # combined stream must NOT repeat 1/2 for the second cluster (that
    # would silently reorder/collide the composite stream's offsets).
    ordinals = [unit["ordinal"] for unit in acquired["units"]]
    labels = [unit["label"] for unit in acquired["units"]]
    assert ordinals == [1, 2, 3, 4]
    assert labels == [
        f"{prefix_a}/א",
        f"{prefix_a}/ב",
        f"{prefix_b}/א",
        f"{prefix_b}/ב",
    ]
    assert acquired["cluster_count"] == 2
    assert acquired["coverage_status"] == "complete"
    assert acquired["provider"] == "hewikisource"
    assert acquired["source_url"].startswith("https://he.wikisource.org/wiki/")
    assert "2 clusters" in acquired["source_ref"]


def test_page_clusters_cluster_level_exclude_pages_applied_during_acquisition(tmp_path):
    prefix_a = "בדיקה נכלל א"
    toc_a = "בדיקה ראשי נכלל א"
    pages = {
        toc_a: {
            "parse": {
                "title": toc_a,
                "links": [
                    {"ns": 0, "title": f"{prefix_a}/הקדמה"},
                    {"ns": 0, "title": f"{prefix_a}/א"},
                ],
            }
        },
        f"{prefix_a}/הקדמה": _wiki_page_doc(f"{prefix_a}/הקדמה", 1, "הקדמה"),
        f"{prefix_a}/א": _wiki_page_doc(f"{prefix_a}/א", 2, "טקסט"),
    }
    fetcher = _FakeFetcher(tmp_path, pages)
    source = {
        "key": "pc_excl",
        "provider": "hewikisource",
        "page_clusters": [
            {
                "toc_page": toc_a,
                "link_prefix": f"{prefix_a}/",
                "exclude_pages": [f"{prefix_a}/הקדמה"],
            }
        ],
        "identity_mode": "public_first",
        "identity_key": "pf-1009",
    }
    acquired, _raw_paths = _acquire_wikisource_page_clusters(fetcher, source)
    assert [unit["label"] for unit in acquired["units"]] == [f"{prefix_a}/א"]


def test_page_clusters_public_first_missing_page_is_a_hard_error_across_clusters(
    tmp_path,
):
    prefix_a = "בדיקה שלם א"
    prefix_b = "בדיקה שלם ב"
    toc_a = "בדיקה ראשי שלם א"
    toc_b = "בדיקה ראשי שלם ב"
    pages = {
        toc_a: {
            "parse": {"title": toc_a, "links": [{"ns": 0, "title": f"{prefix_a}/א"}]}
        },
        toc_b: {
            "parse": {
                "title": toc_b,
                "links": [
                    {"ns": 0, "title": f"{prefix_b}/א"},
                    {"ns": 0, "title": f"{prefix_b}/ב"},
                ],
            }
        },
        f"{prefix_a}/א": _wiki_page_doc(f"{prefix_a}/א", 1, "טקסט קיים"),
        f"{prefix_b}/א": _wiki_page_doc(f"{prefix_b}/א", 2, "טקסט קיים שני"),
        # f"{prefix_b}/ב" deliberately absent from `pages` -> missing.
    }
    fetcher = _FakeFetcher(tmp_path, pages)
    source = {
        "key": "pc_missing",
        "provider": "hewikisource",
        "page_clusters": [
            {"toc_page": toc_a, "link_prefix": f"{prefix_a}/"},
            {"toc_page": toc_b, "link_prefix": f"{prefix_b}/"},
        ],
        "identity_mode": "public_first",
        "identity_key": "pf-1008",
    }
    with pytest.raises(ValueError, match="public_first completeness gate failed"):
        _acquire_wikisource_page_clusters(fetcher, source)


def test_page_clusters_missing_page_still_partial_for_private_sibling(tmp_path):
    """The same cross-cluster missing-page shape must NOT raise for a
    private_sibling (identity_mode-absent) source -- the existing
    coverage_status="partial" escape applies to page_clusters exactly as it
    does to the single-ToC path."""
    prefix_a = "בדיקה חלקי א"
    toc_a = "בדיקה ראשי חלקי א"
    pages = {
        toc_a: {
            "parse": {
                "title": toc_a,
                "links": [
                    {"ns": 0, "title": f"{prefix_a}/א"},
                    {"ns": 0, "title": f"{prefix_a}/ב"},
                ],
            }
        },
        f"{prefix_a}/א": _wiki_page_doc(f"{prefix_a}/א", 1, "טקסט קיים"),
        # f"{prefix_a}/ב" deliberately absent -> missing_pages non-empty.
    }
    fetcher = _FakeFetcher(tmp_path, pages)
    source = {
        "key": "pc_partial",
        "provider": "hewikisource",
        "page_clusters": [{"toc_page": toc_a, "link_prefix": f"{prefix_a}/"}],
        "mappings": [{"target_work_id": "w000001"}],
    }
    acquired, _raw_paths = _acquire_wikisource_page_clusters(fetcher, source)
    assert acquired["coverage_status"] == "partial"
    assert acquired["missing_pages"]
