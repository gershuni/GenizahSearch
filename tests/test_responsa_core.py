"""
Tests for Responsa core functions: data model, parser, expansion, and explosion guard.

Tests cover:
- ResponsaComponent dataclass
- parse_responsa_query() parser
- expand_grammatical_prefixes() Hebrew prefix expansion
- expand_judeo_arabic() Judeo-Arabic definite article expansion
- _apply_explosion_guard() combinatorial explosion guard
"""

import pytest
from unittest.mock import MagicMock

from genizah_core import (
    ResponsaComponent,
    parse_responsa_query,
    expand_grammatical_prefixes,
    expand_judeo_arabic,
    _apply_explosion_guard,
    GRAMMATICAL_PREFIXES,
    Config,
)


# ============================================================================
# ResponsaComponent dataclass
# ============================================================================

class TestResponsaComponent:
    """Tests for the ResponsaComponent dataclass."""

    def test_default_values(self):
        """A plain component has sensible defaults."""
        comp = ResponsaComponent(words=["שלום"])
        assert comp.words == ["שלום"]
        assert comp.grammatical_prefixes is False
        assert comp.wildcard is None
        assert comp.wildcard_pattern is None
        assert comp.inline_pattern is None

    def test_with_grammatical_prefixes(self):
        """Component can be marked for grammatical prefix expansion."""
        comp = ResponsaComponent(words=["שלום"], grammatical_prefixes=True)
        assert comp.grammatical_prefixes is True

    def test_with_wildcard(self):
        """Component can have a wildcard type."""
        comp = ResponsaComponent(words=["שלום"], wildcard="suffix")
        assert comp.wildcard == "suffix"

    def test_with_or_group(self):
        """Component can hold multiple words (OR group)."""
        comp = ResponsaComponent(words=["עץ", "אילן"])
        assert len(comp.words) == 2

    def test_with_inline_pattern(self):
        """Component can have an inline alternation pattern."""
        comp = ResponsaComponent(
            words=["אירו(ס/ש)ין"],
            inline_pattern="אירו(ס/ש)ין"
        )
        assert comp.inline_pattern == "אירו(ס/ש)ין"


# ============================================================================
# parse_responsa_query
# ============================================================================

class TestParseResponsaQuery:
    """Tests for parse_responsa_query()."""

    def test_empty_string(self):
        """Empty string returns empty list."""
        result = parse_responsa_query("")
        assert result == []

    def test_single_plain_word(self):
        """Single word without any prefix or modifier."""
        result = parse_responsa_query("שלום")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].grammatical_prefixes is False
        assert result[0].wildcard is None

    def test_suffix_wildcard(self):
        """Word with trailing asterisk = suffix wildcard."""
        result = parse_responsa_query("שלום*")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].wildcard == "suffix"

    def test_prefix_wildcard(self):
        """Asterisk before a word = prefix wildcard."""
        result = parse_responsa_query("*נדר")
        assert len(result) == 1
        assert result[0].words == ["נדר"]
        assert result[0].wildcard == "prefix"

    def test_character_pattern_wildcard(self):
        """Pattern like *פ*ט*ר*פ* = character pattern wildcard."""
        result = parse_responsa_query("*פ*ט*ר*פ*")
        assert len(result) == 1
        assert result[0].wildcard == "pattern"
        assert result[0].wildcard_pattern == "*פ*ט*ר*פ*"

    def test_grammatical_prefix_hash(self):
        """Hash prefix = grammatical prefixes flag."""
        result = parse_responsa_query("#שלום")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].grammatical_prefixes is True

    def test_or_group(self):
        """Parenthesized slash-separated group = OR alternatives."""
        result = parse_responsa_query("(עץ/אילן)")
        assert len(result) == 1
        assert result[0].words == ["עץ", "אילן"]

    def test_or_group_with_hash(self):
        """Hash before OR group = grammatical prefixes on all alternatives."""
        result = parse_responsa_query("#(שלום/שלומות)")
        assert len(result) == 1
        assert result[0].words == ["שלום", "שלומות"]
        assert result[0].grammatical_prefixes is True

    def test_multiple_components(self):
        """Multiple space-separated tokens become separate components."""
        result = parse_responsa_query("#(שלום/שלומות) עולם*")
        assert len(result) == 2
        # First: OR group with hash
        assert result[0].words == ["שלום", "שלומות"]
        assert result[0].grammatical_prefixes is True
        # Second: suffix wildcard
        assert result[1].words == ["עולם"]
        assert result[1].wildcard == "suffix"

    def test_inline_alternation(self):
        """Inline alternation like אירו(ס/ש)ין."""
        result = parse_responsa_query("אירו(ס/ש)ין")
        assert len(result) == 1
        assert result[0].inline_pattern == "אירו(ס/ש)ין"

    def test_two_plain_words(self):
        """Two plain words become two plain components."""
        result = parse_responsa_query("שלום עולם")
        assert len(result) == 2
        assert result[0].words == ["שלום"]
        assert result[1].words == ["עולם"]

    def test_whitespace_only(self):
        """Whitespace-only input returns empty list."""
        result = parse_responsa_query("   ")
        assert result == []

    def test_or_group_three_alternatives(self):
        """OR group with three alternatives."""
        result = parse_responsa_query("(א/ב/ג)")
        assert len(result) == 1
        assert result[0].words == ["א", "ב", "ג"]


# ============================================================================
# expand_grammatical_prefixes
# ============================================================================

class TestExpandGrammaticalPrefixes:
    """Tests for expand_grammatical_prefixes()."""

    def test_basic_expansion(self):
        """Expanding a word returns ~25 forms with Hebrew prefixes."""
        forms = expand_grammatical_prefixes("שלום")
        assert isinstance(forms, list)
        # Should have approximately 25 forms (one per prefix)
        assert len(forms) >= 20
        assert len(forms) <= 30

    def test_contains_bare_word(self):
        """Result includes the original word (no prefix)."""
        forms = expand_grammatical_prefixes("שלום")
        assert "שלום" in forms

    def test_contains_single_prefixes(self):
        """Result includes common single-letter prefixes."""
        forms = expand_grammatical_prefixes("שלום")
        expected_forms = ["ושלום", "השלום", "בשלום", "כשלום", "לשלום", "משלום", "ששלום"]
        for form in expected_forms:
            assert form in forms, f"Missing form: {form}"

    def test_contains_double_prefixes(self):
        """Result includes common two-letter prefix combinations."""
        forms = expand_grammatical_prefixes("שלום")
        expected_double = ["והשלום", "ובשלום", "וכשלום", "ולשלום", "ומשלום", "וששלום"]
        for form in expected_double:
            assert form in forms, f"Missing double prefix form: {form}"

    def test_no_duplicates(self):
        """No duplicate forms in the result."""
        forms = expand_grammatical_prefixes("שלום")
        assert len(forms) == len(set(forms)), "Found duplicates in expanded forms"

    def test_grammatical_prefixes_constant(self):
        """GRAMMATICAL_PREFIXES constant has expected entries."""
        assert '' in GRAMMATICAL_PREFIXES
        assert 'ו' in GRAMMATICAL_PREFIXES
        assert 'ה' in GRAMMATICAL_PREFIXES
        assert 'ב' in GRAMMATICAL_PREFIXES
        assert 'וה' in GRAMMATICAL_PREFIXES
        assert 'כש' in GRAMMATICAL_PREFIXES
        assert len(GRAMMATICAL_PREFIXES) >= 20


# ============================================================================
# expand_judeo_arabic
# ============================================================================

class TestExpandJudeoArabic:
    """Tests for expand_judeo_arabic().

    Judeo-Arabic expansion uses a SIMPLIFIED model: the definite article
    is ALWAYS 'אל' regardless of the first letter (no sun letter assimilation).
    Every word gets exactly 8 forms.
    """

    def test_basic_word(self):
        """Any word gets 8 forms: base + al- + 5 prep+al- + ll."""
        forms = expand_judeo_arabic("כלמה")
        assert len(forms) == 8
        assert "כלמה" in forms
        assert "אלכלמה" in forms
        assert "ואלכלמה" in forms
        assert "באלכלמה" in forms
        assert "פאלכלמה" in forms
        assert "כאלכלמה" in forms
        assert "לאלכלמה" in forms
        assert "ללכלמה" in forms

    def test_shin_word_no_assimilation(self):
        """Shin-initial word gets same 8 forms (no sun letter assimilation)."""
        forms = expand_judeo_arabic("שוא")
        assert len(forms) == 8
        assert "שוא" in forms
        assert "אלשוא" in forms
        assert "ואלשוא" in forms
        assert "באלשוא" in forms
        assert "פאלשוא" in forms
        assert "כאלשוא" in forms
        assert "לאלשוא" in forms
        assert "ללשוא" in forms

    def test_lamed_word(self):
        """Lamed-initial word: al- + lamed = אללסאן (8 forms)."""
        forms = expand_judeo_arabic("לסאן")
        assert len(forms) == 8
        assert "לסאן" in forms
        assert "אללסאן" in forms
        assert "ואללסאן" in forms
        assert "באללסאן" in forms

    def test_no_duplicates(self):
        """No duplicate forms for any word."""
        for word in ["כלמה", "שוא", "לסאן", "תורה", "דאר"]:
            forms = expand_judeo_arabic(word)
            assert len(forms) == len(set(forms)), f"Duplicates found for '{word}'"

    def test_all_words_get_eight_forms(self):
        """Every word gets exactly 8 forms regardless of first letter."""
        for word in ["כלמה", "שוא", "תורה", "דאר", "נהר", "רוח"]:
            forms = expand_judeo_arabic(word)
            assert len(forms) == 8, f"Expected 8 forms for '{word}', got {len(forms)}"

    def test_tav_word_no_assimilation(self):
        """Tav-initial word gets 8 forms (no assimilation)."""
        forms = expand_judeo_arabic("תורה")
        assert "אלתורה" in forms  # NOT אתתורה
        assert len(forms) == 8

    def test_dalet_word_no_assimilation(self):
        """Dalet-initial word gets 8 forms (no assimilation)."""
        forms = expand_judeo_arabic("דאר")
        assert "אלדאר" in forms  # NOT אדדאר
        assert len(forms) == 8


# ============================================================================
# _apply_explosion_guard
# ============================================================================

class TestApplyExplosionGuard:
    """Tests for _apply_explosion_guard()."""

    def _make_mock_var_mgr(self, variants_per_term=30):
        """Create a mock VariantManager that returns a fixed number of variants."""
        mock = MagicMock()
        mock.get_variants = MagicMock(
            side_effect=lambda term, mode, limit=200: [f"{term}_v{i}" for i in range(variants_per_term)]
        )
        return mock

    def test_under_limit_preserves_all(self):
        """Under MAX_EXPANDED_TERMS: all options preserved, no warning."""
        # 2 plain components, each expanding to ~5 variants = 10 total
        components = [
            ResponsaComponent(words=["שלום"]),
            ResponsaComponent(words=["עולם"]),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=5)
        expanded, warning, actual_options = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=False,
            var_mgr=var_mgr,
            variant_mode='variants'
        )
        assert warning is None
        assert actual_options['variants_on'] is True
        assert actual_options['ja_on'] is False

    def test_over_limit_downgrades_variants(self):
        """Over limit: first cascade step = downgrade variant mode to 'variants' (basic)."""
        # Many components with hash prefixes + large variant counts
        components = [
            ResponsaComponent(words=["שלום"], grammatical_prefixes=True),
            ResponsaComponent(words=["עולם"], grammatical_prefixes=True),
            ResponsaComponent(words=["תורה"], grammatical_prefixes=True),
        ]
        # 3 components * 25 prefixes * 100 variants each = 7500 >> 500
        var_mgr = self._make_mock_var_mgr(variants_per_term=100)
        expanded, warning, actual_options = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants_maximum'  # 150 pairs
        )
        assert warning is not None
        # Should have downgraded something

    def test_over_limit_disables_variants(self):
        """If downgrading variants is not enough, disable variants entirely."""
        components = [
            ResponsaComponent(words=["שלום"], grammatical_prefixes=True),
            ResponsaComponent(words=["עולם"], grammatical_prefixes=True),
            ResponsaComponent(words=["תורה"], grammatical_prefixes=True),
            ResponsaComponent(words=["שמש"], grammatical_prefixes=True),
            ResponsaComponent(words=["ירח"], grammatical_prefixes=True),
        ]
        # 5 components * 25 prefixes * 50 variants = 6250
        var_mgr = self._make_mock_var_mgr(variants_per_term=50)
        expanded, warning, actual_options = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants_extended'
        )
        assert warning is not None

    def test_over_limit_disables_ja(self):
        """If disabling variants is not enough, disable JA."""
        # 20 words * 24 prefixes * 8 JA forms = 3840 >> 500
        # After disabling JA: 20 * 24 = 480 <= 500
        words = [f"word{i}" for i in range(20)]
        components = [
            ResponsaComponent(words=words, grammatical_prefixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=5)
        expanded, warning, actual_options = _apply_explosion_guard(
            components,
            variants_on=False,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants'
        )
        assert warning is not None

    def test_still_over_raises_error(self):
        """If all downgrades exhausted and still over limit, raise ValueError."""
        # Absurdly large OR group that can't fit even without any expansions
        words = [f"word{i}" for i in range(600)]
        components = [
            ResponsaComponent(words=words, grammatical_prefixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=0)
        with pytest.raises(ValueError, match="500|too many|limit|explosion"):
            _apply_explosion_guard(
                components,
                variants_on=False,
                ja_on=False,
                var_mgr=var_mgr,
                variant_mode='variants'
            )

    def test_max_expanded_terms_in_config(self):
        """MAX_EXPANDED_TERMS = 500 is in Config class."""
        assert hasattr(Config, 'MAX_EXPANDED_TERMS')
        assert Config.MAX_EXPANDED_TERMS == 500
