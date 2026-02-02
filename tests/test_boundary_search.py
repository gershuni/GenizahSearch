# -*- coding: utf-8 -*-
"""
Unit tests for boundary-crossing parallel search functions.

Tests the core algorithm functions for:
- Parsing boundaries from text
- Checking if chunks cross boundaries
- Calculating boundary match quality
- Applying score boosts
"""

import pytest
from genizah_core import (
    parse_boundaries,
    chunk_crosses_boundary,
    calculate_boundary_quality,
    calculate_final_score_with_boost,
    get_boundary_stats
)


class TestParseBoundaries:
    """Test parse_boundaries function."""

    def test_paragraph_boundaries(self):
        """Test parsing boundaries with paragraph breaks."""
        text = "First paragraph.\n\nSecond paragraph.\n\nThird."
        boundaries = parse_boundaries(text, '\n\n')
        assert len(boundaries) == 2

    def test_no_boundaries(self):
        """Test text without any boundaries."""
        text = "Continuous text without any paragraph breaks at all"
        boundaries = parse_boundaries(text, '\n\n')
        assert len(boundaries) == 0

    def test_single_boundary(self):
        """Test text with a single boundary."""
        text = "Part one with words.\n\nPart two with more words."
        boundaries = parse_boundaries(text, '\n\n')
        assert len(boundaries) == 1

    def test_line_break_boundaries(self):
        """Test parsing boundaries with line breaks."""
        text = "Line one\nLine two\nLine three"
        boundaries = parse_boundaries(text, '\n')
        assert len(boundaries) == 2

    def test_period_boundaries(self):
        """Test parsing boundaries with periods."""
        text = "Sentence one. Sentence two. Sentence three."
        boundaries = parse_boundaries(text, '.', min_distance=1)
        assert len(boundaries) >= 2

    def test_min_distance_filter(self):
        """Test that min_distance filters out close boundaries."""
        text = "A. B. C. D. E. F."  # Periods every word
        boundaries = parse_boundaries(text, '.', min_distance=3)
        # Should skip some boundaries that are too close
        assert len(boundaries) < 6

    def test_empty_text(self):
        """Test with empty text."""
        boundaries = parse_boundaries("", '\n\n')
        assert boundaries == []

    def test_empty_delimiter(self):
        """Test with empty delimiter."""
        boundaries = parse_boundaries("Some text", '')
        assert boundaries == []

    def test_hebrew_text(self):
        """Test with Hebrew text."""
        text = "פסקה ראשונה עם מילים\n\nפסקה שנייה עם מילים נוספות"
        boundaries = parse_boundaries(text, '\n\n')
        assert len(boundaries) == 1


class TestChunkCrossesBoundary:
    """Test chunk_crosses_boundary function."""

    def test_chunk_crosses_single_boundary(self):
        """Test chunk that crosses a single boundary."""
        boundaries = [4, 10]  # Boundaries after words 4 and 10
        assert chunk_crosses_boundary(2, 6, boundaries) == True  # Crosses 4

    def test_chunk_before_boundary(self):
        """Test chunk that's entirely before boundaries."""
        boundaries = [4, 10]
        assert chunk_crosses_boundary(0, 3, boundaries) == False

    def test_chunk_between_boundaries(self):
        """Test chunk that's between two boundaries."""
        boundaries = [4, 10]
        assert chunk_crosses_boundary(5, 9, boundaries) == False

    def test_chunk_after_boundaries(self):
        """Test chunk that's after all boundaries."""
        boundaries = [4, 10]
        assert chunk_crosses_boundary(11, 15, boundaries) == False

    def test_chunk_crosses_multiple_boundaries(self):
        """Test chunk that crosses multiple boundaries."""
        boundaries = [3, 5, 7]
        # Chunk from 2 to 8 crosses all three boundaries
        assert chunk_crosses_boundary(2, 8, boundaries) == True

    def test_empty_boundaries(self):
        """Test with no boundaries."""
        assert chunk_crosses_boundary(0, 10, []) == False

    def test_boundary_at_chunk_start(self):
        """Test boundary at exact chunk start."""
        boundaries = [5]
        # Boundary at 5, chunk from 5 to 10 - boundary is at start, so it's included
        assert chunk_crosses_boundary(5, 10, boundaries) == True

    def test_boundary_at_chunk_end(self):
        """Test boundary at exact chunk end (exclusive)."""
        boundaries = [10]
        # Chunk from 5 to 10 (exclusive), boundary at 10 - not included
        assert chunk_crosses_boundary(5, 10, boundaries) == False


class TestCalculateBoundaryQuality:
    """Test calculate_boundary_quality function."""

    def test_average_calculation(self):
        """Test basic average calculation."""
        scores = [800, 900, 850]
        quality = calculate_boundary_quality(scores)
        assert quality == 850.0

    def test_single_score(self):
        """Test with a single score."""
        scores = [500]
        quality = calculate_boundary_quality(scores)
        assert quality == 500.0

    def test_empty_scores(self):
        """Test with no scores."""
        quality = calculate_boundary_quality([])
        assert quality == 0.0

    def test_varied_scores(self):
        """Test with varied scores."""
        scores = [100, 200, 300, 400]
        quality = calculate_boundary_quality(scores)
        assert quality == 250.0


class TestCalculateFinalScoreWithBoost:
    """Test calculate_final_score_with_boost function."""

    def test_no_boundary_matches(self):
        """Test that score is unchanged without boundary matches."""
        final = calculate_final_score_with_boost(1000, 0, False, 1.5)
        assert final == 1000

    def test_weak_boundary_quality(self):
        """Test boost with weak boundary quality."""
        # Base score 1000, boundary quality 500 -> normalized 0.5
        # Multiplier = 1 + (1.5 - 1) * 0.5 = 1.25
        final = calculate_final_score_with_boost(1000, 500, True, 1.5)
        assert final == 1250

    def test_good_boundary_quality(self):
        """Test boost with good boundary quality."""
        # Base score 1000, boundary quality 800 -> normalized 0.8
        # Multiplier = 1 + (1.5 - 1) * 0.8 = 1.40
        final = calculate_final_score_with_boost(1000, 800, True, 1.5)
        assert final == 1400

    def test_strong_boundary_quality(self):
        """Test boost with strong (maximum) boundary quality."""
        # Base score 1000, boundary quality >= 1000 -> normalized 1.0
        # Multiplier = 1 + (1.5 - 1) * 1.0 = 1.50
        final = calculate_final_score_with_boost(1000, 1000, True, 1.5)
        assert final == 1500

    def test_very_high_quality_capped(self):
        """Test that quality above base score is capped at 1.0."""
        # Boundary quality 1500 with base 1000 -> normalized capped at 1.0
        final = calculate_final_score_with_boost(1000, 1500, True, 1.5)
        assert final == 1500  # Same as 1.0 normalized

    def test_zero_base_score(self):
        """Test with zero base score."""
        final = calculate_final_score_with_boost(0, 100, True, 1.5)
        assert final == 0

    def test_custom_boost_factor(self):
        """Test with custom boost factor."""
        # Base 1000, quality 1000, boost 2.0
        # Multiplier = 1 + (2.0 - 1) * 1.0 = 2.0
        final = calculate_final_score_with_boost(1000, 1000, True, 2.0)
        assert final == 2000

    def test_no_boost_factor(self):
        """Test with boost factor of 1.0 (no boost)."""
        final = calculate_final_score_with_boost(1000, 1000, True, 1.0)
        assert final == 1000


class TestGetBoundaryStats:
    """Test get_boundary_stats function."""

    def test_basic_stats(self):
        """Test basic statistics calculation."""
        text = "Part one with words.\n\nPart two with more words.\n\nPart three."
        stats = get_boundary_stats(text, '\n\n', chunk_size=5)
        assert 'boundary_count' in stats
        assert 'crossing_chunk_count' in stats
        assert 'total_chunks' in stats
        assert stats['boundary_count'] == 2

    def test_no_boundaries_stats(self):
        """Test stats with no boundaries."""
        text = "Continuous text without any breaks"
        stats = get_boundary_stats(text, '\n\n', chunk_size=5)
        assert stats['boundary_count'] == 0
        assert stats['crossing_chunk_count'] == 0

    def test_short_text(self):
        """Test stats with text shorter than chunk size."""
        text = "Short"
        stats = get_boundary_stats(text, '\n\n', chunk_size=5)
        assert stats['total_chunks'] == 1

    def test_crossing_chunks_counted(self):
        """Test that crossing chunks are counted correctly."""
        # With a boundary in the middle, some chunks should cross it
        text = "One two three four.\n\nFive six seven eight."
        stats = get_boundary_stats(text, '\n\n', chunk_size=3)
        assert stats['boundary_count'] == 1
        assert stats['crossing_chunk_count'] > 0

    def test_hebrew_stats(self):
        """Test stats with Hebrew text."""
        text = "מילה ראשונה שנייה שלישית רביעית\n\nחמישית שישית שביעית"
        stats = get_boundary_stats(text, '\n\n', chunk_size=3)
        assert stats['boundary_count'] == 1
