"""
Tests for Responsa core functions: data model, parser, expansion, and explosion guard.

Tests cover:
- ResponsaComponent dataclass
- parse_responsa_query() parser (including #suffix, %plene/defective)
- expand_grammatical_prefixes() Hebrew prefix expansion
- expand_grammatical_suffixes() Hebrew suffix expansion
- expand_plene_defective() plene/defective spelling variants
- expand_judeo_arabic() Judeo-Arabic definite article expansion
- _apply_explosion_guard() combinatorial explosion guard
"""

import pytest
from unittest.mock import MagicMock

from genizah_core import (
    ResponsaComponent,
    parse_responsa_query,
    extract_per_pair_gaps,
    generate_tabular_syntax,
    expand_grammatical_prefixes,
    expand_grammatical_suffixes,
    expand_judeo_arabic,
    expand_plene_defective,
    _apply_explosion_guard,
    GRAMMATICAL_PREFIXES,
    GRAMMATICAL_SUFFIXES,
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
        assert comp.grammatical_suffixes is False
        assert comp.plene_defective is False
        assert comp.wildcard is None
        assert comp.wildcard_pattern is None
        assert comp.inline_pattern is None

    def test_with_grammatical_prefixes(self):
        """Component can be marked for grammatical prefix expansion."""
        comp = ResponsaComponent(words=["שלום"], grammatical_prefixes=True)
        assert comp.grammatical_prefixes is True

    def test_with_grammatical_suffixes(self):
        """Component can be marked for grammatical suffix expansion."""
        comp = ResponsaComponent(words=["שלום"], grammatical_suffixes=True)
        assert comp.grammatical_suffixes is True

    def test_with_plene_defective(self):
        """Component can be marked for plene/defective spelling."""
        comp = ResponsaComponent(words=["שלום"], plene_defective=True)
        assert comp.plene_defective is True

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

    def test_all_flags_combined(self):
        """Component can have prefixes + suffixes + plene simultaneously."""
        comp = ResponsaComponent(
            words=["שלום"],
            grammatical_prefixes=True,
            grammatical_suffixes=True,
            plene_defective=True,
        )
        assert comp.grammatical_prefixes is True
        assert comp.grammatical_suffixes is True
        assert comp.plene_defective is True


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
        assert result[0].grammatical_suffixes is False
        assert result[0].plene_defective is False
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
        assert result[0].grammatical_suffixes is False

    def test_grammatical_suffix_hash(self):
        """Trailing hash = grammatical suffixes flag."""
        result = parse_responsa_query("שלום#")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].grammatical_prefixes is False
        assert result[0].grammatical_suffixes is True

    def test_both_prefix_and_suffix_hash(self):
        """#word# = both prefix and suffix expansion."""
        result = parse_responsa_query("#שלום#")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].grammatical_prefixes is True
        assert result[0].grammatical_suffixes is True

    def test_plene_defective_percent(self):
        """%word = plene/defective spelling variants."""
        result = parse_responsa_query("%שלום")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].plene_defective is True
        assert result[0].grammatical_prefixes is False

    def test_percent_with_prefix_hash(self):
        """%#word = plene/defective + prefix expansion."""
        result = parse_responsa_query("%#שלום")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].plene_defective is True
        assert result[0].grammatical_prefixes is True

    def test_percent_with_both_hashes(self):
        """%#word# = plene/defective + prefix + suffix expansion."""
        result = parse_responsa_query("%#שלום#")
        assert len(result) == 1
        assert result[0].words == ["שלום"]
        assert result[0].plene_defective is True
        assert result[0].grammatical_prefixes is True
        assert result[0].grammatical_suffixes is True

    def test_prefix_hash_with_suffix_wildcard(self):
        """#word* = prefix expansion + suffix wildcard."""
        result = parse_responsa_query("#שלו*")
        assert len(result) == 1
        assert result[0].words == ["שלו"]
        assert result[0].grammatical_prefixes is True
        assert result[0].wildcard == "suffix"

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
        assert result[0].words == ["שלום", "שלומות"]
        assert result[0].grammatical_prefixes is True
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

    def test_mixed_operators_in_query(self):
        """Complex query with different operator types on each component."""
        result = parse_responsa_query("#שלום# %עולם תורה*")
        assert len(result) == 3
        # First: prefix + suffix
        assert result[0].grammatical_prefixes is True
        assert result[0].grammatical_suffixes is True
        # Second: plene/defective
        assert result[1].plene_defective is True
        # Third: wildcard suffix
        assert result[2].wildcard == "suffix"


# ============================================================================
# expand_grammatical_prefixes
# ============================================================================

class TestExpandGrammaticalPrefixes:
    """Tests for expand_grammatical_prefixes()."""

    def test_basic_expansion(self):
        """Expanding a word returns ~25 forms with Hebrew prefixes."""
        forms = expand_grammatical_prefixes("שלום")
        assert isinstance(forms, list)
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
# expand_grammatical_suffixes
# ============================================================================

class TestExpandGrammaticalSuffixes:
    """Tests for expand_grammatical_suffixes().

    Note: The suffix function does direct concatenation — the user provides
    the stem, not the complete word. E.g., user types "שלומ#" (stem) to get
    שלומים, שלומות, etc. This is how Bar-Ilan Responsa works.
    """

    def test_basic_expansion(self):
        """Expanding a stem returns ~25 forms with Hebrew suffixes."""
        forms = expand_grammatical_suffixes("שלומ")
        assert isinstance(forms, list)
        assert len(forms) >= 20
        assert len(forms) <= 30

    def test_contains_bare_word(self):
        """Result includes the original stem (no suffix)."""
        forms = expand_grammatical_suffixes("שלומ")
        assert "שלומ" in forms

    def test_contains_plural_suffixes(self):
        """Result includes plural forms."""
        forms = expand_grammatical_suffixes("שלומ")
        assert "שלומים" in forms
        assert "שלומות" in forms

    def test_contains_feminine_suffix(self):
        """Result includes feminine marker."""
        forms = expand_grammatical_suffixes("שלומ")
        assert "שלומה" in forms

    def test_contains_possessive_suffixes(self):
        """Result includes possessive suffixes."""
        forms = expand_grammatical_suffixes("שלומ")
        assert "שלומי" in forms   # my
        assert "שלומך" in forms   # your
        assert "שלומו" in forms   # his
        assert "שלומנו" in forms  # our
        assert "שלומכם" in forms  # your (m.pl.)

    def test_contains_plural_possessive_suffixes(self):
        """Result includes possessive suffixes used on plural forms."""
        forms = expand_grammatical_suffixes("שלומ")
        assert "שלומיו" in forms   # his (on plurals)
        assert "שלומיה" in forms   # her (on plurals)
        assert "שלומיהם" in forms  # their (m.pl., on plurals)
        assert "שלומיהן" in forms  # their (f.pl., on plurals)

    def test_no_duplicates(self):
        """No duplicate forms in the result."""
        forms = expand_grammatical_suffixes("שלומ")
        assert len(forms) == len(set(forms)), "Found duplicates in expanded forms"

    def test_grammatical_suffixes_constant(self):
        """GRAMMATICAL_SUFFIXES constant has expected entries."""
        assert '' in GRAMMATICAL_SUFFIXES
        assert 'ה' in GRAMMATICAL_SUFFIXES
        assert 'ים' in GRAMMATICAL_SUFFIXES
        assert 'ות' in GRAMMATICAL_SUFFIXES
        assert 'יהם' in GRAMMATICAL_SUFFIXES
        assert len(GRAMMATICAL_SUFFIXES) >= 20


# ============================================================================
# expand_plene_defective
# ============================================================================

class TestExpandPleneDefective:
    """Tests for expand_plene_defective()."""

    def test_includes_original(self):
        """Result always includes the original word."""
        forms = expand_plene_defective("שלום")
        assert "שלום" in forms

    def test_removal_vav(self):
        """Removes interior vav: שלום -> שלם."""
        forms = expand_plene_defective("שלום")
        assert "שלם" in forms

    def test_removal_yod(self):
        """Removes interior yod: בית -> בת."""
        forms = expand_plene_defective("בית")
        assert "בת" in forms

    def test_addition_vav(self):
        """Adds vav in plausible positions: שלם -> includes שלום."""
        forms = expand_plene_defective("שלם")
        assert "שלום" in forms  # vav after lamed

    def test_addition_yod(self):
        """Adds yod in plausible positions."""
        forms = expand_plene_defective("בת")
        assert "בית" in forms  # yod after bet

    def test_no_duplicates(self):
        """No duplicate forms in the result."""
        forms = expand_plene_defective("שלום")
        assert len(forms) == len(set(forms)), "Found duplicates"

    def test_short_word(self):
        """Single-character word returns just itself."""
        forms = expand_plene_defective("א")
        assert forms == ["א"]

    def test_two_char_word(self):
        """Two-character word with no interior matres returns itself + additions."""
        forms = expand_plene_defective("בת")
        assert "בת" in forms
        # Should have addition variants (insert between ב and ת)
        assert "בית" in forms or "בות" in forms

    def test_word_without_matres(self):
        """Word without any ו/י still gets addition variants."""
        forms = expand_plene_defective("שלם")
        assert len(forms) > 1  # Should have additions

    def test_does_not_remove_first_or_last(self):
        """Does not remove ו/י at first or last position."""
        # ויקרא - vav is first, should not be removed
        forms = expand_plene_defective("ויקרא")
        # The original should be there
        assert "ויקרא" in forms
        # 'יקרא' should NOT be there (would remove first char)
        assert "יקרא" not in forms

    def test_no_insert_after_mater(self):
        """Does not insert ו/י after an existing mater lectionis."""
        forms = expand_plene_defective("שלום")
        # Should not insert after the existing ו (position 3)
        # since word[2] = 'ו' which is a mater
        assert "שלוום" not in forms
        assert "שלוים" not in forms


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
        components = [
            ResponsaComponent(words=["שלום"], grammatical_prefixes=True),
            ResponsaComponent(words=["עולם"], grammatical_prefixes=True),
            ResponsaComponent(words=["תורה"], grammatical_prefixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=100)
        expanded, warning, actual_options = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants_maximum'
        )
        assert warning is not None

    def test_over_limit_disables_variants(self):
        """If downgrading variants is not enough, disable variants entirely."""
        components = [
            ResponsaComponent(words=["שלום"], grammatical_prefixes=True),
            ResponsaComponent(words=["עולם"], grammatical_prefixes=True),
            ResponsaComponent(words=["תורה"], grammatical_prefixes=True),
            ResponsaComponent(words=["שמש"], grammatical_prefixes=True),
            ResponsaComponent(words=["ירח"], grammatical_prefixes=True),
        ]
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

    def test_suffixes_counted_in_explosion_guard(self):
        """Suffix expansion is counted when estimating explosion.

        1 word * 24 prefixes * 25 suffixes * 5 variants = 3000 >> 500.
        After disabling variants: 1 * 24 * 25 = 600 > 500 still.
        With expanded cascade, the guard disables suffixes (bringing to 24*1=24)
        instead of raising ValueError.
        """
        components = [
            ResponsaComponent(words=["שלום"], grammatical_prefixes=True, grammatical_suffixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=5)
        expanded, warning, actual_opts = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=False,
            var_mgr=var_mgr,
            variant_mode='variants'
        )
        # Cascade should have disabled suffixes instead of erroring
        assert warning is not None, "Should have triggered cascade warning"
        assert "suffix" in warning.lower() or "סיומות" in warning, f"Warning should mention suffix disabling, got: {warning}"
        assert expanded[0].grammatical_suffixes is False

    def test_suffixes_with_manageable_count(self):
        """Suffix expansion only (no prefixes) stays under limit without cascading."""
        # 1 word * 25 suffixes = 25, well under 500
        components = [
            ResponsaComponent(words=["שלום"], grammatical_suffixes=True),
        ]
        var_mgr = self._make_mock_var_mgr(variants_per_term=5)
        expanded, warning, actual_options = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=False,
            var_mgr=var_mgr,
            variant_mode='variants'
        )
        # 25 suffixes * 5 variants = 125, under 500
        assert warning is None
        assert actual_options['variants_on'] is True


# ============================================================================
# Gap notation: extract_per_pair_gaps + parse_responsa_query skips [N]
# ============================================================================

class TestGapNotation:
    """Tests for [N] gap notation parsing via extract_per_pair_gaps()."""

    def test_gap_token_parsed(self):
        """parse_responsa_query('#\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd*') returns 2 components. extract_per_pair_gaps returns [3]."""
        components = parse_responsa_query("#\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd*")
        assert len(components) == 2
        gaps = extract_per_pair_gaps("#\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd*")
        assert gaps == [3]

    def test_multiple_gap_tokens(self):
        """'#word1 [3] word2 [5] word3' -> gaps = [3, 5]."""
        gaps = extract_per_pair_gaps("#word1 [3] word2 [5] word3")
        assert gaps == [3, 5]

    def test_no_gap_tokens_returns_none_list(self):
        """'#word1 word2' -> gaps = [None] (no gap token, use global gap)."""
        gaps = extract_per_pair_gaps("#word1 word2")
        assert gaps == [None]

    def test_mixed_gap_and_no_gap(self):
        """'word1 [2] word2 word3' -> gaps = [2, None]."""
        gaps = extract_per_pair_gaps("word1 [2] word2 word3")
        assert gaps == [2, None]

    def test_gap_zero(self):
        """'word1 [0] word2' -> gaps = [0]."""
        gaps = extract_per_pair_gaps("word1 [0] word2")
        assert gaps == [0]

    def test_gap_does_not_become_component(self):
        """'#\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd' -> exactly 2 components (not 3)."""
        components = parse_responsa_query("#\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd")
        assert len(components) == 2


# ============================================================================
# generate_tabular_syntax
# ============================================================================

class TestGenerateTabularSyntax:
    """Tests for generate_tabular_syntax() — converting builder state to syntax."""

    def test_basic_two_components(self):
        """Two components each with one word, distance 3 -> 'word1 [3] word2'."""
        components = [
            {'words': [{'text': 'word1', 'mods': {}}]},
            {'words': [{'text': 'word2', 'mods': {}}]},
        ]
        syntax, negated = generate_tabular_syntax(components, [3])
        assert syntax == "word1 [3] word2"
        assert negated == []

    def test_prefix_modifier(self):
        """Word with prefix=True -> '#word'."""
        components = [
            {'words': [{'text': 'word', 'mods': {'prefix': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '#word' in syntax

    def test_suffix_modifier(self):
        """Word with suffix=True -> 'word#'."""
        components = [
            {'words': [{'text': 'word', 'mods': {'suffix': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert 'word#' in syntax

    def test_both_prefix_suffix(self):
        """Word with both prefix and suffix -> '#word#'."""
        components = [
            {'words': [{'text': 'word', 'mods': {'prefix': True, 'suffix': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '#word#' in syntax

    def test_wildcard_suffix(self):
        """Word with wildcard_suffix=True -> 'word*'."""
        components = [
            {'words': [{'text': 'word', 'mods': {'wildcard_suffix': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert 'word*' in syntax

    def test_wildcard_prefix(self):
        """Word with wildcard_prefix=True -> '*word'."""
        components = [
            {'words': [{'text': 'word', 'mods': {'wildcard_prefix': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '*word' in syntax

    def test_plene(self):
        """Word with plene=True -> '%word'."""
        components = [
            {'words': [{'text': 'word', 'mods': {'plene': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '%word' in syntax

    def test_combined_plene_prefix(self):
        """Plene + prefix -> '#%word' (plene applied first, then prefix wraps)."""
        components = [
            {'words': [{'text': 'word', 'mods': {'plene': True, 'prefix': True}}]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '#%word' in syntax

    def test_or_alternatives(self):
        """Component with 2 words -> '(word1/word2)'."""
        components = [
            {'words': [
                {'text': 'word1', 'mods': {}},
                {'text': 'word2', 'mods': {}},
            ]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '(word1/word2)' in syntax

    def test_or_with_modifiers(self):
        """Component with #word1 and word2* -> '(#word1/word2*)'."""
        components = [
            {'words': [
                {'text': 'word1', 'mods': {'prefix': True}},
                {'text': 'word2', 'mods': {'wildcard_suffix': True}},
            ]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        assert '(#word1/word2*)' in syntax

    def test_empty_words_skipped(self):
        """Component with one word and one empty string -> just the single word, no parens."""
        components = [
            {'words': [
                {'text': 'word1', 'mods': {}},
                {'text': '', 'mods': {}},
            ]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [1])
        # Should NOT have parentheses since only one valid word
        assert '(' not in syntax
        assert 'word1' in syntax

    def test_all_empty_component_skipped(self):
        """Component with all empty words -> skipped entirely."""
        components = [
            {'words': [{'text': '', 'mods': {}}, {'text': '', 'mods': {}}]},
            {'words': [{'text': 'word', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [0])
        assert syntax.strip() == 'word'

    def test_within_document_no_gaps(self):
        """Scope 'within_document' -> no [N] tokens in output."""
        components = [
            {'words': [{'text': 'word1', 'mods': {}}]},
            {'words': [{'text': 'word2', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [5], scope='within_document')
        assert '[' not in syntax
        assert ']' not in syntax
        assert 'word1' in syntax
        assert 'word2' in syntax

    def test_negated_words_extracted_and_inline(self):
        """Word with negation=True -> -word in syntax AND in returned negated_words."""
        components = [
            {'words': [
                {'text': 'good', 'mods': {}},
                {'text': 'bad', 'mods': {'negation': True}},
            ]},
            {'words': [{'text': 'other', 'mods': {}}]},
        ]
        syntax, negated = generate_tabular_syntax(components, [1])
        assert '-bad' in syntax  # negated word embedded as -word
        assert 'bad' in negated  # also extracted for backward compat
        assert 'good' in syntax

    def test_distance_zero_no_bracket(self):
        """Distance 0 between components -> no [0] token (just space)."""
        components = [
            {'words': [{'text': 'word1', 'mods': {}}]},
            {'words': [{'text': 'word2', 'mods': {}}]},
        ]
        syntax, _ = generate_tabular_syntax(components, [0])
        assert '[0]' not in syntax
        assert 'word1' in syntax
        assert 'word2' in syntax


# ============================================================================
# Negation with -word syntax (parse_responsa_query)
# ============================================================================

class TestNegationParsing:
    """Tests for -word negation syntax in Responsa parser."""

    def test_simple_negation(self):
        """-word should produce a negated component."""
        components = parse_responsa_query("שלום -רע")
        assert len(components) == 2
        assert components[0].negated is False
        assert components[0].words == ["שלום"]
        assert components[1].negated is True
        assert components[1].words == ["רע"]

    def test_negation_with_prefix(self):
        """-#word should be negated with grammatical prefixes."""
        components = parse_responsa_query("-#שלום")
        assert len(components) == 1
        assert components[0].negated is True
        assert components[0].grammatical_prefixes is True
        assert components[0].words == ["שלום"]

    def test_negation_with_suffix(self):
        """-word# should be negated with grammatical suffixes."""
        components = parse_responsa_query("-שלום#")
        assert len(components) == 1
        assert components[0].negated is True
        assert components[0].grammatical_suffixes is True
        assert components[0].words == ["שלום"]

    def test_negation_with_plene(self):
        """-%word should be negated with plene/defective expansion."""
        components = parse_responsa_query("-%שלום")
        assert len(components) == 1
        assert components[0].negated is True
        assert components[0].plene_defective is True
        assert components[0].words == ["שלום"]

    def test_negation_with_wildcard_suffix(self):
        """-word* should be negated with wildcard suffix."""
        components = parse_responsa_query("-שלום*")
        assert len(components) == 1
        assert components[0].negated is True
        assert components[0].wildcard == 'suffix'

    def test_negation_with_or_group(self):
        """-(word1/word2) should be negated OR group."""
        components = parse_responsa_query("-(טוב/רע)")
        assert len(components) == 1
        assert components[0].negated is True
        assert set(components[0].words) == {"טוב", "רע"}

    def test_negation_mixed_query(self):
        """Multiple components with some negated."""
        components = parse_responsa_query("שלום עולם -רע -חטא")
        positive = [c for c in components if not c.negated]
        negated = [c for c in components if c.negated]
        assert len(positive) == 2
        assert len(negated) == 2
        assert positive[0].words == ["שלום"]
        assert positive[1].words == ["עולם"]
        negated_words = [c.words[0] for c in negated]
        assert "רע" in negated_words
        assert "חטא" in negated_words

    def test_lone_minus_ignored(self):
        """A lone '-' should not crash the parser."""
        components = parse_responsa_query("שלום - עולם")
        # The lone '-' is either skipped or treated as empty; 2 real components
        real_comps = [c for c in components if c.words and c.words[0].strip()]
        assert len(real_comps) >= 2

    def test_negation_in_component_dataclass(self):
        """ResponsaComponent dataclass supports negated field."""
        comp = ResponsaComponent(words=["test"], negated=True)
        assert comp.negated is True
        comp2 = ResponsaComponent(words=["test"])
        assert comp2.negated is False

    def test_generate_tabular_syntax_negation_inline(self):
        """Tabular builder emits -word inline for negated words."""
        components = [
            {'words': [
                {'text': 'שלום', 'mods': {}},
                {'text': 'רע', 'mods': {'negation': True}},
            ]},
            {'words': [{'text': 'עולם', 'mods': {}}]},
        ]
        syntax, negated = generate_tabular_syntax(components, [3])
        assert '-רע' in syntax  # inline negation
        assert 'רע' in negated  # also in extracted list
        assert 'שלום' in syntax
        assert 'עולם' in syntax

    def test_negation_roundtrip(self):
        """Syntax with -word should roundtrip through parser."""
        # Generate syntax with negation
        components = [
            {'words': [{'text': 'שלום', 'mods': {}}]},
            {'words': [{'text': 'רע', 'mods': {'negation': True}}]},
        ]
        syntax, neg = generate_tabular_syntax(components, [])
        assert '-רע' in syntax

        # Parse it back
        parsed = parse_responsa_query(syntax)
        positive = [c for c in parsed if not c.negated]
        negated = [c for c in parsed if c.negated]
        assert len(positive) >= 1
        assert any('שלום' in c.words for c in positive)
        assert len(negated) >= 1
        assert any('רע' in c.words for c in negated)
