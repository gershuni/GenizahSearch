"""The canonical-mask category set -- which works are MASKING AUTHORITIES.

A masking authority's text, quoted inside an edited work, is blanked there, so a
fragment of the quoted text is never credited to the quoting work. Getting the
set wrong is silent in both directions: too small and a reference becomes a
proxy for a shared known text (measured: a prayer-book compendium earned 6,869
live matches across 6,227 manuscripts and shadowed 6,948 rows, more than it
earned, including rows belonging to the purpose-built Amidah reference); too
large -- specifically, an authority masked against its own category -- and a
reference is blanked end to end and drops out of matching entirely.

The mask engine itself lives in the operator's restricted research tree, so
these tests stub it: a temporary probe root with the two modules ``run()``
imports, which record what they were handed. That is exactly what is under
test here -- WHICH works are handed over as authorities, and which are refused.
"""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import pytest

from scripts.discovery_v4_extend_masks import (
    CANONICAL_CATEGORIES,
    MASK_POLICY_VERSION,
    run,
)

STUB_MASK_REF_CANON = '''
RECORD = {}


def mask_one_work(stream, seg_streams, codes_f, seg_f, pos_f, stats):
    stats["accepted"] += 1
    # Blank one 10-letter window per authority the index was given, so the
    # output is a function of the authority set the caller assembled.
    return [[0, 10]] if seg_streams else []


def mask_edited_works(edited, *index_and_stats):
    return {work["id"]: [[0, 10]] for work in edited}
'''

STUB_TRACK1_MATCH = '''
import json
from pathlib import Path


def build_ref_index(works, masks=None):
    Path("authorities.json").write_text(
        json.dumps([w["id"] for w in works]), encoding="utf-8"
    )
    # (seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f, df_dropped)
    return (["seg"], {}, {}, {}, {}, {}, 0)
'''


def _probe_root(tmp_path: Path) -> Path:
    scripts_dir = tmp_path / "probe" / "scripts"
    scripts_dir.mkdir(parents=True)
    (scripts_dir / "mask_ref_canon.py").write_text(STUB_MASK_REF_CANON, encoding="utf-8")
    (scripts_dir / "track1_match.py").write_text(STUB_TRACK1_MATCH, encoding="utf-8")
    return tmp_path / "probe"


def _corpus(tmp_path: Path, base: list, appended: list) -> tuple[Path, Path, Path]:
    base_path = tmp_path / "base.pkl"
    with base_path.open("wb") as handle:
        pickle.dump(base, handle)
    v4_path = tmp_path / "v4.pkl"
    with v4_path.open("wb") as handle:
        pickle.dump([*base, *appended], handle)
    masks_path = tmp_path / "base_masks.json"
    masks_path.write_text(json.dumps({}), encoding="utf-8")
    return base_path, v4_path, masks_path


def _args(tmp_path: Path, base_path, v4_path, masks_path, **overrides):
    from scripts.discovery_v4_common import sha256_file

    args = argparse.Namespace(
        probe_root=str(_probe_root(tmp_path)),
        base_reference=str(base_path),
        base_reference_sha256=sha256_file(base_path),
        v4_reference=str(v4_path),
        v4_reference_sha256=sha256_file(v4_path),
        base_masks=str(masks_path),
        base_masks_sha256=sha256_file(masks_path),
        reference_namespace="REF9",
        output=str(tmp_path / "out_masks.json"),
        workers=2,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _work(raw_id: str, cat: str, stream: str = "אבגדהוזחטי" * 3) -> dict:
    return {"id": raw_id, "cat": cat, "stream": stream}


# --------------------------------------------------------------------------
# the category set itself
# --------------------------------------------------------------------------


def test_liturgy_is_a_masking_authority():
    """Owner ruling liturgy-is-a-masking-authority (2026-08-17)."""
    assert "Liturgy" in CANONICAL_CATEGORIES


def test_the_authority_set_is_exactly_the_ruled_six():
    # Pinned so a category cannot be added or dropped without a ruling. Targum
    # is deliberately ABSENT: same class of shared known text, not yet ruled.
    assert CANONICAL_CATEGORIES == {
        "Bible",
        "Mishnah",
        "Tosefta",
        "Bavli",
        "Yerushalmi",
        "Liturgy",
    }
    assert "Targum" not in CANONICAL_CATEGORIES


# --------------------------------------------------------------------------
# which works are handed over as authorities
# --------------------------------------------------------------------------


def test_a_liturgy_reference_is_handed_to_the_index_as_an_authority(tmp_path, monkeypatch):
    base = [
        _work("REF2:liturgy_amidah", "Liturgy"),
        _work("M:private", "EditedWorkCategory"),
        _work("REF2:bavli_berakhot", "Bavli"),
    ]
    appended = [_work("REF9:compendium", "Sefaria")]
    base_path, v4_path, masks_path = _corpus(tmp_path, base, appended)
    monkeypatch.chdir(tmp_path)  # the stub writes authorities.json into cwd

    report = run(_args(tmp_path, base_path, v4_path, masks_path))

    authorities = json.loads((tmp_path / "authorities.json").read_text(encoding="utf-8"))
    assert "REF2:liturgy_amidah" in authorities
    assert "REF2:bavli_berakhot" in authorities
    # A private/edited work is never an authority.
    assert "M:private" not in authorities
    assert report["canonical_authority_count"] == 2
    assert report["mask_policy_version"] == MASK_POLICY_VERSION
    assert "Liturgy" in report["mask_policy_categories"]


def test_an_appended_masking_authority_is_a_hard_error(tmp_path, monkeypatch):
    """Self-masking would blank the reference ~entirely, silently."""
    base = [_work("REF2:liturgy_amidah", "Liturgy")]
    appended = [_work("REF9:another_amidah", "Liturgy")]
    base_path, v4_path, masks_path = _corpus(tmp_path, base, appended)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="itself a masking authority"):
        run(_args(tmp_path, base_path, v4_path, masks_path))


def test_the_guard_names_the_offending_reference(tmp_path, monkeypatch):
    base = [_work("REF2:liturgy_amidah", "Liturgy")]
    appended = [
        _work("REF9:ok", "Sefaria"),
        _work("REF9:bad_haggadah", "Liturgy"),
    ]
    base_path, v4_path, masks_path = _corpus(tmp_path, base, appended)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="REF9:bad_haggadah"):
        run(_args(tmp_path, base_path, v4_path, masks_path))
