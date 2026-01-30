# -*- coding: utf-8 -*-
"""
Configuration for Corpus Mapper.

Paths are configured for Windows by default.
Adjust BASE_DIR if running on different system.
"""

import os
import platform

# Detect OS and set base directory
if platform.system() == 'Windows':
    BASE_DIR = r"C:\GenizahSearch"
else:
    # For development/testing on Linux/Mac
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Corpus paths
CORPORA = {
    'ja': {
        'name': 'Judeo-Arabic (Friedberg)',
        'name_he': 'ערבית יהודית (פרידברג)',
        'path': os.path.join(BASE_DIR, 'big_data_files', 'JA'),
        'pattern': '*.JSON',
        'format': 'json',
        'description': 'Judeo-Arabic texts from Friedberg Genizah Project'
    },
    'maagarim': {
        'name': 'Maagarim (Academy)',
        'name_he': 'מאגרים (האקדמיה)',
        'path': os.path.join(BASE_DIR, 'big_data_files', 'Maagarim'),
        'pattern': '*.txt',
        'format': 'txt',
        'description': 'Historical Dictionary of the Hebrew Language corpus'
    }
}

# Output paths
OUTPUT_DIR = os.path.join(BASE_DIR, 'corpus_mapper_output')
CHECKPOINTS_DIR = os.path.join(OUTPUT_DIR, 'checkpoints')
LOGS_DIR = os.path.join(OUTPUT_DIR, 'logs')
RESULTS_DB = os.path.join(OUTPUT_DIR, 'corpus_connections.sqlite')

# Configuration files
CLEANING_RULES_FILE = os.path.join(BASE_DIR, 'corpus_mapper', 'cleaning_rules.json')
SYMBOL_REPORT_FILE = os.path.join(OUTPUT_DIR, 'symbol_report.json')

# Search settings
DEFAULT_MIN_SCORE = 300
DEFAULT_CHUNK_SIZE = 15
DEFAULT_BATCH_SIZE = 100  # Files per checkpoint

# Ensure directories exist
def ensure_dirs():
    """Create output directories if they don't exist."""
    for d in [OUTPUT_DIR, CHECKPOINTS_DIR, LOGS_DIR]:
        os.makedirs(d, exist_ok=True)
