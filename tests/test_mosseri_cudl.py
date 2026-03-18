"""Tests for construct_mosseri_cudl_label() — Mosseri shelfmark to CUDL label conversion."""

import pytest
from genizah_core import construct_mosseri_cudl_label


class TestConstructMosseriCudlLabel:
    """Unit tests for CUDL label construction from Mosseri shelfmark variants."""

    # ── Basic patterns ─────────────────────────────────────────────

    def test_basic_roman_numeral_series(self):
        """Ms. VI 108 -> MS-MOSSERI-VI-00108"""
        assert construct_mosseri_cudl_label("Ms. VI 108") == "MS-MOSSERI-VI-00108"

    def test_sub_fragment_with_dot(self):
        """Moss. VI,129.3 -> MS-MOSSERI-VI-00129-00003"""
        assert construct_mosseri_cudl_label("Moss. VI,129.3") == "MS-MOSSERI-VI-00129-00003"

    def test_letter_suffix(self):
        """Ms. III 27O -> MS-MOSSERI-III-00027-O"""
        assert construct_mosseri_cudl_label("Ms. III 27O") == "MS-MOSSERI-III-00027-O"

    def test_sub_fragment_and_letter_suffix(self):
        """Ms. III 145.3C -> MS-MOSSERI-III-00145-00003-C"""
        assert construct_mosseri_cudl_label("Ms. III 145.3C") == "MS-MOSSERI-III-00145-00003-C"

    def test_series_with_lowercase_suffix(self):
        """Ms. IIIa 15 -> MS-MOSSERI-IIIA-00015"""
        assert construct_mosseri_cudl_label("Ms. IIIa 15") == "MS-MOSSERI-IIIA-00015"

    def test_full_complex_pattern(self):
        """Ms. VIII 179.2B -> MS-MOSSERI-VIII-00179-00002-B"""
        assert construct_mosseri_cudl_label("Ms. VIII 179.2B") == "MS-MOSSERI-VIII-00179-00002-B"

    # ── Long-form "Mosseri, Jacques" prefix ────────────────────────

    def test_long_form_mosseri_jacques(self):
        """Mosseri, Jacques Ms. VII 173.3 -> MS-MOSSERI-VII-00173-00003"""
        assert construct_mosseri_cudl_label("Mosseri, Jacques Ms. VII 173.3") == "MS-MOSSERI-VII-00173-00003"

    # ── Rejection cases (should return None) ───────────────────────

    def test_non_mosseri_shelfmark(self):
        """T-S 12.123 is not a Mosseri shelfmark — should return None."""
        assert construct_mosseri_cudl_label("T-S 12.123") is None

    def test_second_series_single_letter_L(self):
        """Ms. L 241 is a 2nd-series designator, not a Roman numeral — should return None."""
        assert construct_mosseri_cudl_label("Ms. L 241") is None

    def test_empty_string(self):
        """Empty string should return None."""
        assert construct_mosseri_cudl_label("") is None

    def test_none_input(self):
        """None input should return None."""
        assert construct_mosseri_cudl_label(None) is None

    # ── Additional edge cases ──────────────────────────────────────

    def test_series_I(self):
        """Ms. I 5 -> MS-MOSSERI-I-00005"""
        assert construct_mosseri_cudl_label("Ms. I 5") == "MS-MOSSERI-I-00005"

    def test_series_IA(self):
        """Ms. Ia 10 -> MS-MOSSERI-IA-00010"""
        assert construct_mosseri_cudl_label("Ms. Ia 10") == "MS-MOSSERI-IA-00010"

    def test_series_X(self):
        """Ms. X 200 -> MS-MOSSERI-X-00200"""
        assert construct_mosseri_cudl_label("Ms. X 200") == "MS-MOSSERI-X-00200"

    def test_series_IV(self):
        """Ms. IV 42 -> MS-MOSSERI-IV-00042"""
        assert construct_mosseri_cudl_label("Ms. IV 42") == "MS-MOSSERI-IV-00042"

    def test_second_series_A(self):
        """Ms. A 100 — 2nd-series, not valid Roman numeral series — should return None."""
        assert construct_mosseri_cudl_label("Ms. A 100") is None

    def test_second_series_P(self):
        """Ms. P 59 — 2nd-series — should return None."""
        assert construct_mosseri_cudl_label("Ms. P 59") is None
