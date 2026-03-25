# -*- coding: utf-8 -*-
"""Tests for PuzzleDocument and PuzzleFragment data model."""

import pytest
from shared.puzzle_model import PuzzleDocument, PuzzleFragment


class TestPuzzleFragment:
    """Tests for PuzzleFragment dataclass."""

    def test_fragment_has_all_fields(self):
        """PuzzleFragment has all attributes with correct defaults."""
        frag = PuzzleFragment(sys_id="S1", folio_label="1r", fl_id="FL100")
        assert frag.sys_id == "S1"
        assert frag.folio_label == "1r"
        assert frag.fl_id == "FL100"
        assert frag.x == 0.0
        assert frag.y == 0.0
        assert frag.rotation == 0.0
        assert frag.scale == 1.0
        assert frag.flip_h is False
        assert frag.flip_v is False
        assert frag.bg_removal_threshold == 30.0

    def test_fragment_non_default_values(self):
        """Fragment with non-default values roundtrips correctly."""
        frag = PuzzleFragment(
            sys_id="S2", folio_label="2v", fl_id="FL200",
            x=150.5, y=-30.2, rotation=45.0, scale=0.8,
            flip_h=True, flip_v=False, bg_removal_threshold=50.0
        )
        doc = PuzzleDocument(fragments=[frag])
        restored = PuzzleDocument.from_json(doc.to_json())
        rf = restored.fragments[0]
        assert rf.x == 150.5
        assert rf.y == -30.2
        assert rf.rotation == 45.0
        assert rf.scale == 0.8
        assert rf.flip_h is True
        assert rf.flip_v is False
        assert rf.bg_removal_threshold == 50.0


class TestPuzzleDocument:
    """Tests for PuzzleDocument dataclass."""

    def test_document_has_all_fields(self):
        """PuzzleDocument has all attributes with correct defaults."""
        doc = PuzzleDocument()
        assert isinstance(doc.id, str) and len(doc.id) > 0
        assert doc.title == ''
        assert doc.notes == ''
        assert doc.join_type == 'physical'
        assert doc.fragments == []
        assert isinstance(doc.created_at, str) and len(doc.created_at) > 0
        assert isinstance(doc.updated_at, str) and len(doc.updated_at) > 0

    def test_roundtrip_serialization(self):
        """PuzzleDocument with 2 fragments roundtrips via JSON."""
        frag1 = PuzzleFragment(sys_id="S1", folio_label="1r", fl_id="FL100",
                               x=10.0, y=20.0, rotation=90.0)
        frag2 = PuzzleFragment(sys_id="S2", folio_label="2v", fl_id="FL200",
                               scale=0.5, flip_h=True, bg_removal_threshold=45.0)
        doc = PuzzleDocument(
            title="Test Join",
            notes="Some notes",
            join_type="physical",
            fragments=[frag1, frag2]
        )
        json_str = doc.to_json()
        restored = PuzzleDocument.from_json(json_str)

        assert restored.id == doc.id
        assert restored.title == "Test Join"
        assert restored.notes == "Some notes"
        assert restored.join_type == "physical"
        assert restored.created_at == doc.created_at
        assert restored.updated_at == doc.updated_at
        assert len(restored.fragments) == 2

        rf1 = restored.fragments[0]
        assert rf1.sys_id == "S1"
        assert rf1.folio_label == "1r"
        assert rf1.fl_id == "FL100"
        assert rf1.x == 10.0
        assert rf1.y == 20.0
        assert rf1.rotation == 90.0
        assert rf1.scale == 1.0

        rf2 = restored.fragments[1]
        assert rf2.sys_id == "S2"
        assert rf2.scale == 0.5
        assert rf2.flip_h is True
        assert rf2.bg_removal_threshold == 45.0

    def test_empty_document_roundtrip(self):
        """PuzzleDocument with no fragments roundtrips correctly."""
        doc = PuzzleDocument()
        restored = PuzzleDocument.from_json(doc.to_json())
        assert restored.id == doc.id
        assert restored.fragments == []
        assert restored.title == ''
        assert restored.join_type == 'physical'

    def test_join_type_values(self):
        """join_type accepts physical, content, uncertain."""
        for jt in ('physical', 'content', 'uncertain'):
            doc = PuzzleDocument(join_type=jt)
            restored = PuzzleDocument.from_json(doc.to_json())
            assert restored.join_type == jt


class TestExternalFragmentFields:
    """Tests for external library image fields (image_url, external_provider, page_index)."""

    def test_fragment_external_fields_defaults(self):
        """New external fields have correct defaults."""
        frag = PuzzleFragment(sys_id="S1", folio_label="1r", fl_id="FL100")
        assert frag.image_url == ''
        assert frag.external_provider == ''
        assert frag.page_index == -1

    def test_fragment_external_fields_roundtrip(self):
        """External fragment fields survive JSON roundtrip."""
        frag = PuzzleFragment(
            sys_id="S1", folio_label="1r", fl_id="",
            image_url="https://luna.manchester.ac.uk/luna/servlet/iiif/abc",
            external_provider="manchester",
            page_index=3
        )
        doc = PuzzleDocument(fragments=[frag])
        restored = PuzzleDocument.from_json(doc.to_json())
        rf = restored.fragments[0]
        assert rf.fl_id == ""
        assert rf.image_url == "https://luna.manchester.ac.uk/luna/servlet/iiif/abc"
        assert rf.external_provider == "manchester"
        assert rf.page_index == 3

    def test_backward_compat_old_json_without_new_fields(self):
        """Old JSON without image_url/external_provider/page_index loads with defaults."""
        import json
        old_data = {
            "id": "test-old", "title": "Old Doc", "notes": "", "join_type": "physical",
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:00:00",
            "fragments": [
                {"sys_id": "S1", "folio_label": "1r", "fl_id": "FL100",
                 "shelfmark": "T-S 12.1", "x": 0, "y": 0, "rotation": 0, "scale": 1,
                 "flip_h": False, "flip_v": False, "bg_removal_threshold": 30.0,
                 "crop_top": 0, "crop_bottom": 0, "crop_left": 0, "crop_right": 0,
                 "processed": True}
            ]
        }
        doc = PuzzleDocument.from_json(json.dumps(old_data))
        frag = doc.fragments[0]
        assert frag.fl_id == "FL100"
        assert frag.image_url == ''
        assert frag.external_provider == ''
        assert frag.page_index == -1

    def test_nli_fragment_external_fields_are_empty_by_default(self):
        """NLI fragments (with fl_id) have empty external fields by default."""
        frag = PuzzleFragment(sys_id="S1", folio_label="2r", fl_id="FL7734473",
                              shelfmark="T-S 12.1")
        assert frag.image_url == ''
        assert frag.external_provider == ''
        assert frag.page_index == -1
