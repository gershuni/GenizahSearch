"""`emit_novelty_input_fingerprints.py` must emit what the BUILDER can read.

The gate this feeds (gate 13, `tests/test_v3_novelty_fingerprint.py`) is only as
good as the file handed to it, and the two halves are written in different
places: the emitter chooses a shape, and `build_discovery_sidecar.
_load_novelty_fingerprints` accepts exactly one. The first draft of the emitter
wrapped the map in a provenance envelope -- readable, self-describing, and
rejected by its only consumer, which requires a FLAT string-to-string object.
Nothing would have caught that until a full corpus build failed at the end.

So this suite checks the handshake itself: the emitter's real output is fed to
the real loader. The heavy data sources are mocked -- the shape contract is what
is under test, not the corpus.

MASKING (D-25): the recorded-witness source is exercised through an opaque stub;
no restricted name or shelfmark appears here.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts import emit_novelty_input_fingerprints as emit  # noqa: E402
from scripts.build_discovery_sidecar import (  # noqa: E402
    NoveltyVerdictCacheError,
    _load_novelty_fingerprints,
)
from scripts.discovery_novelty_funnel import NoveltyCandidate  # noqa: E402


def _candidate(n: int, **over) -> NoveltyCandidate:
    base = dict(
        sys_id=f"99000000000000000{n}",
        ref_work_id=f"M:Ytext100{n}_01",
        claimed_title="a title",
        claimed_author="an author",
        catalogue_text="catalogue prose",
        bibliography_rows=(),
        pgp_description="desc",
        pgp_transcription="trans",
        fgp_texts=(),
        m_source_shelfmark_text="opaque-attribution",
    )
    base.update(over)
    return NoveltyCandidate(**base)


@pytest.fixture
def stubbed(monkeypatch):
    """Replace the corpus reads; keep the real fingerprinting and serialization."""
    cands = [
        _candidate(1, known_witness_confidence="high"),
        _candidate(2, known_witness_confidence=None),
        _candidate(3, known_witness_confidence="low"),
    ]
    monkeypatch.setattr(emit, "build_all_candidates",
                        lambda *a, **k: (cands, {}, {}))
    monkeypatch.setattr(emit, "load_work_witnesses",
                        lambda *a, **k: {"w000001": {}})
    return cands


def _run(tmp_path, stubbed, *extra) -> Path:
    out = tmp_path / "fp.json"
    rc = emit.main([
        "--asset", str(tmp_path / "asset.db"),
        "--work-witnesses", str(tmp_path / "witnesses.json"),
        "--out", str(out), *extra,
    ])
    assert rc == 0
    return out


def test_the_emitted_file_is_what_the_builder_accepts(tmp_path, stubbed):
    """THE handshake. Real emitter output, real loader, no adapter between."""
    out = _run(tmp_path, stubbed)
    loaded = _load_novelty_fingerprints(str(out))
    assert len(loaded) == 3
    assert all(isinstance(k, str) and isinstance(v, str) and v
               for k, v in loaded.items())


def test_a_wrapped_envelope_would_be_rejected(tmp_path):
    """The demonstration that the test above can fail.

    This is the shape the first draft emitted. Pinning the rejection means the
    handshake test is checking something real rather than restating a shape both
    sides happen to share today.
    """
    wrapped = tmp_path / "wrapped.json"
    wrapped.write_text(json.dumps({
        "schema": "novelty-input-fingerprints-v1",
        "pairs": 1,
        "fingerprints": {"99000000000000001::M:Ytext1001_01": "deadbeef"},
    }), encoding="utf-8")
    with pytest.raises(NoveltyVerdictCacheError):
        _load_novelty_fingerprints(str(wrapped))


def test_keys_match_the_verdict_cache_keying(tmp_path, stubbed):
    """`{sys_id}::{ref_work_id}` -- the same key the model arm writes.

    A correct fingerprint under a different key is a miss, not a match, and the
    build would fail closed for every pair with no way to tell why.
    """
    loaded = _load_novelty_fingerprints(str(_run(tmp_path, stubbed)))
    for c in stubbed:
        assert f"{c.sys_id}::{c.ref_work_id}" in loaded


def test_provenance_travels_beside_the_map_not_inside_it(tmp_path, stubbed):
    """The envelope was not deleted, it was moved -- an opaque hex map that
    cannot say which question it describes is barely better than none."""
    out = _run(tmp_path, stubbed)
    meta = json.loads(Path(str(out) + ".meta.json").read_text(encoding="utf-8"))
    assert meta["describes"] == out.name
    assert meta["pairs"] == 3
    assert meta["witness_source_supplied"] is True
    assert "known_witness_confidence" in meta["input_fingerprint_fields"]


def test_a_witness_blind_emit_must_be_asked_for_out_loud(tmp_path, stubbed):
    """Omitting the witness map silently would describe a different question
    than a witness-aware cache answered -- every pair would mismatch, and the
    build would fail closed with no hint that the EMIT was the mistake."""
    rc = emit.main([
        "--asset", str(tmp_path / "asset.db"),
        "--out", str(tmp_path / "nope.json"),
    ])
    assert rc == 2
    assert not (tmp_path / "nope.json").exists()

    rc = emit.main([
        "--asset", str(tmp_path / "asset.db"),
        "--allow-no-witnesses",
        "--out", str(tmp_path / "blind.json"),
    ])
    assert rc == 0
    meta = json.loads((tmp_path / "blind.json.meta.json").read_text(encoding="utf-8"))
    assert meta["witness_source_supplied"] is False, (
        "a witness-blind emit must SAY it is witness-blind, or a later reader "
        "cannot tell which question the fingerprints describe"
    )
