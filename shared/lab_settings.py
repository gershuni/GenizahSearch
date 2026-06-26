# -*- coding: utf-8 -*-
"""Lab Mode scoring-weights configuration with JSON persistence.

Phase 125: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import LabSettings`` callers continue working.
"""

import json
import logging
import os

from shared.config import Config

LOGGER = logging.getLogger("genizah." + __name__)


class LabSettings:
    """Manages configuration for the Lab Mode, including scoring weights."""
    def __init__(self):
        self.custom_variants = {}
        self.candidate_limit = 5000
        self.min_should_match = 75
        self.gap_penalty = 2

        # Scoring Weights
        self.length_bonus_factor = 1.5
        self.common_penalty_factor = 0.1
        self.unique_bonus_base = 100
        self.density_penalty = 0.2
        self.coverage_power = 2.0
        self.order_bonus = 10.0

        # --- New Settings: Noise Suppression (Stop Words) ---
        self.stop_word_score = 1.0       # Score for short words (<3 chars)
        self.common_3char_score = 2.0    # Score for common 3-letter words

        # Composition Settings
        self.comp_chunk_limit = 500
        self.comp_min_score = 70
        self.comp_max_final_results = 200

        # Deep Scan Settings
        self.lab_scan_limit = 50000

        # Display Limit
        self.lab_display_limit = 500

        self.use_dynamic_weights = False

        # Variant Search Settings (affects standard search when using variants mode)
        self.variant_min_word_len = 2      # Words <= this length get only 1 change
        self.variant_max_changes = 2       # Max character changes per word
        self.variant_aggressive = False    # If True, ignore length limits (like old behavior)
        self.variant_pairs_count = 50      # Number of top variant pairs to use (slider value)
        self.variant_use_slider = False    # If True, show slider instead of preset buttons

        # Boundary Search Settings
        self.boundary_mode = 'full'           # 'full', 'boundary', 'combined'
        self.boundary_delimiter = '\n'        # What marks a paragraph boundary
        self.boundary_boost = 1.5             # Score multiplier for boundary matches (1.0-3.0)
        self.min_boundary_matches = 0         # Filter results with fewer matches (0-10)
        self.min_delimiter_distance = 3       # Min words between delimiters

        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                with open(Config.LAB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_variants = data.get('custom_variants', {})
                    self.use_dynamic_weights = data.get('use_dynamic_weights', False)
                    self.candidate_limit = data.get('candidate_limit', 2000)
                    self.min_should_match = data.get('min_should_match', 60)
                    self.gap_penalty = data.get('gap_penalty', 2)

                    self.length_bonus_factor = data.get('length_bonus_factor', 1.5)
                    self.common_penalty_factor = data.get('common_penalty_factor', 0.1)
                    self.unique_bonus_base = data.get('unique_bonus_base', 100)
                    self.density_penalty = data.get('density_penalty', 0.2)
                    self.coverage_power = data.get('coverage_power', 2.0)
                    self.order_bonus = data.get('order_bonus', 10.0)

                    # Load noise settings
                    self.stop_word_score = data.get('stop_word_score', 1.0)
                    self.common_3char_score = data.get('common_3char_score', 2.0)

                    self.comp_chunk_limit = data.get('comp_chunk_limit', 200)
                    self.comp_min_score = data.get('comp_min_score', 70)
                    self.comp_max_final_results = data.get('comp_max_final_results', 100)

                    self.lab_scan_limit = data.get('lab_scan_limit', 50000)
                    self.lab_display_limit = data.get('lab_display_limit', 500)

                    # Load variant settings
                    self.variant_min_word_len = data.get('variant_min_word_len', 2)
                    self.variant_max_changes = data.get('variant_max_changes', 2)
                    self.variant_aggressive = data.get('variant_aggressive', False)
                    self.variant_pairs_count = data.get('variant_pairs_count', 50)
                    self.variant_use_slider = data.get('variant_use_slider', False)

                    # Load boundary search settings
                    self.boundary_mode = data.get('boundary_mode', 'full')
                    self.boundary_delimiter = data.get('boundary_delimiter', '\n')
                    self.boundary_boost = data.get('boundary_boost', 1.5)
                    self.min_boundary_matches = data.get('min_boundary_matches', 0)
                    self.min_delimiter_distance = data.get('min_delimiter_distance', 3)
            except Exception as e:
                LOGGER.warning('Failed to load lab config from %s: %s', Config.LAB_CONFIG_FILE, e)

    def save(self):
        try:
            os.makedirs(Config.LAB_DIR, exist_ok=True)
            with open(Config.LAB_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'custom_variants': self.custom_variants,
                    'use_dynamic_weights': self.use_dynamic_weights,
                    'candidate_limit': self.candidate_limit,
                    'min_should_match': self.min_should_match,
                    'gap_penalty': self.gap_penalty,

                    'length_bonus_factor': self.length_bonus_factor,
                    'common_penalty_factor': self.common_penalty_factor,
                    'unique_bonus_base': self.unique_bonus_base,
                    'density_penalty': self.density_penalty,
                    'coverage_power': self.coverage_power,
                    'order_bonus': self.order_bonus,

                    # Save noise settings
                    'stop_word_score': self.stop_word_score,
                    'common_3char_score': self.common_3char_score,

                    'comp_chunk_limit': self.comp_chunk_limit,
                    'comp_min_score': self.comp_min_score,
                    'comp_max_final_results': self.comp_max_final_results,

                    'lab_scan_limit': self.lab_scan_limit,
                    'lab_display_limit': self.lab_display_limit,

                    # Variant settings
                    'variant_min_word_len': self.variant_min_word_len,
                    'variant_max_changes': self.variant_max_changes,
                    'variant_aggressive': self.variant_aggressive,
                    'variant_pairs_count': self.variant_pairs_count,
                    'variant_use_slider': self.variant_use_slider,

                    # Boundary search settings
                    'boundary_mode': self.boundary_mode,
                    'boundary_delimiter': self.boundary_delimiter,
                    'boundary_boost': self.boundary_boost,
                    'min_boundary_matches': self.min_boundary_matches,
                    'min_delimiter_distance': self.min_delimiter_distance
                }, f, indent=4)
        except Exception as e:
            LOGGER.warning('Failed to save lab config to %s: %s', Config.LAB_CONFIG_FILE, e)
