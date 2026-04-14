# -*- coding: utf-8 -*-
"""Tests for translation QC heuristics."""

from shared.translation_qc import (
    run_qc,
    run_qc_batch,
    summarize_qc_results,
    check_copied_source,
    check_script_mismatch,
    check_length_ratio,
    check_number_drift,
    check_bracket_mismatch,
    check_added_questions,
    check_truncation,
    check_empty_translation,
    check_too_short,
)


class TestIndividualChecks:
    def test_copied_source_exact(self):
        assert check_copied_source('Hello world test', 'Hello world test') == 'copied_source'

    def test_copied_source_different(self):
        assert check_copied_source('Hello world', 'שלום עולם') is None

    def test_copied_source_near_copy(self):
        result = check_copied_source(
            'This is a long enough text to test',
            'This is a long enough text to tset'
        )
        assert result is not None and 'near_copy' in result

    def test_script_mismatch_en2he_wrong(self):
        result = check_script_mismatch(
            'A letter from Cairo', 'Still all English text here', 'en2he'
        )
        assert result is not None and 'target_mostly_latin' in result

    def test_script_mismatch_en2he_correct(self):
        assert check_script_mismatch(
            'A letter from Cairo', 'מכתב מקהיר', 'en2he'
        ) is None

    def test_script_mismatch_he2en_correct(self):
        assert check_script_mismatch(
            'מכתב מקהיר', 'A letter from Cairo', 'he2en'
        ) is None

    def test_script_mismatch_he2en_wrong(self):
        result = check_script_mismatch(
            'מכתב מקהיר', 'עדיין טקסט עברי כאן', 'he2en'
        )
        assert result is not None and 'target_mostly_hebrew' in result

    def test_length_ratio_extreme_high(self):
        source = 'מכתב קצר מסוחר בקהיר'
        target = 'A ' * 200  # Way too long
        result = check_length_ratio(source, target, 'he2en')
        assert result is not None and 'length_ratio_high' in result

    def test_length_ratio_normal(self):
        assert check_length_ratio(
            'A letter from a merchant', 'מכתב מסוחר', 'en2he'
        ) is None

    def test_number_drift_added(self):
        result = check_number_drift(
            'A fragment of a legal document',
            'שבר של מסמך משפטי מ-1247 בקהיר'
        )
        assert result is not None and 'numbers_added' in result

    def test_number_drift_preserved(self):
        assert check_number_drift(
            'Fragment 1247 from Cairo', 'קטע 1247 מקהיר'
        ) is None

    def test_bracket_mismatch_dropped(self):
        result = check_bracket_mismatch(
            'Torah (text) [fragment]', 'תורה טקסט קטע'
        )
        assert result is not None

    def test_bracket_mismatch_preserved(self):
        assert check_bracket_mismatch(
            'Torah (text)', 'תורה (טקסט)'
        ) is None

    def test_added_questions(self):
        result = check_added_questions('A letter', 'מכתב? אולי? לא ברור?')
        assert result is not None and 'questions_added' in result

    def test_truncation(self):
        assert check_truncation('מכתב מקהיר...') is not None

    def test_no_truncation(self):
        assert check_truncation('מכתב מקהיר') is None

    def test_empty(self):
        assert check_empty_translation('') is not None
        assert check_empty_translation('   ') is not None

    def test_not_empty(self):
        assert check_empty_translation('שלום') is None

    def test_too_short(self):
        result = check_too_short(
            'A long description of a legal document found in the Cairo Genizah',
            'מכתב'
        )
        assert result is not None and 'too_short' in result


class TestRunQC:
    def test_good_translation(self):
        result = run_qc('A letter from Cairo', 'מכתב מקהיר', 'en2he')
        assert result['qc_score'] == 1.0
        assert result['flag_count'] == 0

    def test_copied_source(self):
        result = run_qc('A letter from Cairo', 'A letter from Cairo', 'en2he')
        assert result['qc_score'] < 0.5
        assert 'copied_source' in result['qc_flags']

    def test_missing_text(self):
        result = run_qc('Hello', '', 'en2he')
        assert result['qc_score'] == 0.0
        assert 'missing_text' in result['qc_flags']

    def test_none_inputs(self):
        result = run_qc(None, None, 'en2he')
        assert result['qc_score'] == 0.0


class TestBatch:
    def test_batch(self):
        pairs = [
            ('Hello', 'שלום', 'en2he'),
            ('Hello', 'Hello', 'en2he'),
        ]
        results = run_qc_batch(pairs)
        assert len(results) == 2
        assert results[0]['qc_score'] > results[1]['qc_score']

    def test_summary(self):
        results = [
            run_qc('Hello', 'שלום', 'en2he'),
            run_qc('Hello', 'Hello', 'en2he'),
            run_qc('Test', '', 'en2he'),
        ]
        summary = summarize_qc_results(results)
        assert summary['total'] == 3
        assert summary['flagged'] == 2
        assert summary['clean'] == 1
        assert 'flag_distribution' in summary
