"""Test configuration for ensuring project modules are importable."""

import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ROOT_PATH = str(ROOT_DIR)

if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)
