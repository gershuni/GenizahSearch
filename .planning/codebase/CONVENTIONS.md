# Coding Conventions

**Analysis Date:** 2026-02-05

## Naming Patterns

**Files:**
- Python modules use `snake_case` (e.g., `genizah_core.py`, `genizah_app.py`, `corrections_client.py`)
- Web components use `snake_case` (e.g., `search_box.py`, `add_to_list_dialog.py`)
- Test files follow pattern `test_*.py` or `*_test.py` (e.g., `test_shelfmark_normalization.py`)
- UI dialogs use explicit names: `*_dialog.py` (e.g., `filter_text_dialog.py`, `column_filter_dialog.py`)

**Functions:**
- Standard functions use `snake_case`: `normalize_shelfmark()`, `strip_nikud()`, `encode_word_shmidman()`
- Private/internal functions use leading underscore: `_normalize_shelfmark()`, `_reload_lab_index()`, `_ensure_lab_tokenizers()`
- Callback/handler functions use `on_*` or `handle_*` pattern (e.g., `handle_search()`)
- Threading functions use `*_thread` or thread class names (e.g., `SearchThread`, `IndexerThread`)

**Variables:**
- Standard variables: `snake_case` (e.g., `base_score`, `boundary_quality`, `dynamic_rank_map`)
- Constants: `UPPER_CASE` (e.g., `HEBREW_FREQ`, `NIKUD_PATTERN`, `LAB_INDEX_DIR`)
- Private/class-internal variables: leading underscore `_var_name`
- Hebrew text variables may use descriptive names with Hebrew terms preserved in comments

**Types:**
- Classes use `PascalCase`: `MetadataManager`, `SearchEngine`, `LabEngine`, `SafeRotatingFileHandler`
- Type hints encouraged (e.g., `def normalize_shelfmark(shelfmark: str) -> str:`)
- Return type annotations preferred for all public functions

## Code Style

**Formatting:**
- No formal linter detected; follow PEP 8 conventions
- Indentation: 4 spaces (Python standard)
- Line length: No strict limit observed, but keep under 100 characters where practical
- File encoding: UTF-8 (all files have `# -*- coding: utf-8 -*-`)

**Linting:**
- No automated linting tools detected (no `.flake8`, `.pylintrc`, etc.)
- Follows PEP 8 style informally
- Code uses type hints but not strict type checking (no mypy detected)

## Import Organization

**Order:**
1. Built-in modules (`os`, `sys`, `re`, `json`, `logging`, `threading`, etc.)
2. Standard library (`csv`, `shutil`, `pickle`, etc.)
3. Third-party packages (`PyQt6`, `requests`, `tantivy`, `google-genai`, etc.)
4. Local imports (from `genizah_core`, `genizah_translations`, etc.)

**Pattern from `genizah_core.py`:**
```python
import logging
import os
import sys
import re
import shutil
import pickle
import requests
import threading
import time
# ... more stdlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
import platform
# ... blank line
from typing import Mapping
from functools import lru_cache
# ... more stdlib
# ... blank line
from genizah_translations import TRANSLATIONS  # Local imports last
try:
    from unified_variants import UNIFIED_VARIANT_PAIRS, get_top_pairs
except ImportError:
    UNIFIED_VARIANT_PAIRS = []
    def get_top_pairs(n): return []
```

**Path Aliases:**
- No explicit path aliases detected; uses direct imports
- For desktop app: `from genizah_core import Config, MetadataManager, ...`
- For web app: `from web.state import state` and `sys.path.insert(0, os.path.dirname(...))`

## Error Handling

**Patterns:**
- Try-except blocks catch specific exceptions: `except (PermissionError, OSError) as e:`
- Graceful degradation with fallbacks:
  ```python
  try:
      from unified_variants import UNIFIED_VARIANT_PAIRS
  except ImportError:
      UNIFIED_VARIANT_PAIRS = []
  ```
- Platform-specific handling:
  ```python
  if platform.system() == 'Windows':
      pass  # Silently continue without rotation
  else:
      raise
  ```
- Functions return tuple `(result, error)` pattern in some services:
  ```python
  user, error = AuthService.authenticate_user(db, email, password)
  if error:
      print(f"  ❌ Login failed: {error}")
  ```
- Custom exception classes used for domain logic (e.g., `CorrectionStatus`, `UserRole`)
- File operations wrapped in try-except with logging

## Logging

**Framework:** Python's built-in `logging` module

**Patterns:**
- Get logger with `get_logger(__name__)` function in `genizah_core.py`
- Module-level logger: `LOGGER = get_logger(__name__)`
- Specialized loggers: `LAB_LOGGER = configure_lab_logger()` for Lab Mode operations
- Log files rotated using `SafeRotatingFileHandler` (custom class for Windows safety)

**Usage from `genizah_core.py`:**
```python
def get_logger(name=None):
    base_logger = configure_logger()
    return base_logger.getChild(name) if name else base_logger

LOGGER = get_logger(__name__)

# Usage:
LAB_LOGGER.info("Reloading Lab Index...")
LAB_LOGGER.error(f"Failed to load Lab Index: {e}")
```

**File Locations:**
- Desktop app logs: `crash_log.txt` (in app directory)
- Lab mode logs: `Config.LAB_LOG_FILE` (rotating, max 1MB per file, 3 backups)
- Console output for debugging (optional)

**Log Levels:**
- `DEBUG`: Detailed diagnostic information
- `INFO`: Confirmation of expected behavior ("✓ successful operations")
- `ERROR`: Error conditions that need attention ("❌ failures")
- `WARNING`: Potential issues (not consistently used)

## Comments

**When to Comment:**
- Complex algorithms (e.g., boundary matching logic, fingerprinting)
- Non-obvious design decisions ("Treat "/" as "." for consistency")
- System-specific notes (Windows file locking in `SafeRotatingFileHandler`)
- TODO/FIXME comments for known issues

**Example from `genizah_core.py`:**
```python
def normalize_shelfmark(shelfmark: str) -> str:
    """
    Normalize shelfmarks for consistent matching across the codebase.

    This is the CANONICAL implementation - all other normalizations should use this.

    Rules:
    - Convert to lowercase
    - Treat "/" as "." for consistency (192/23 -> 192.23)
    - Preserve dots between digits (e.g., "12.123" stays as "12.123")
    - Remove all other non-alphanumeric characters
    - Remove "MS" or "Ms." prefix (common in Oxford shelfmarks)
    """
```

**JSDoc/Docstring:**
- All public functions include docstrings with description
- Arguments documented with type and purpose:
  ```python
  def calculate_boundary_quality(boundary_chunk_scores: list) -> float:
      """
      Calculate boundary match quality as average of match strengths.

      Args:
          boundary_chunk_scores: List of scores from chunks that crossed boundaries

      Returns:
          Average score (0 if no boundary matches)
      """
  ```
- Class docstrings explain purpose and key methods

## Function Design

**Size:**
- Functions generally 20-100 lines
- Complex operations (indexing, search) broken into helper functions
- Example: `_reload_lab_index()`, `_ensure_lab_tokenizers()` support main `rebuild_lab_index()`

**Parameters:**
- Explicit parameters preferred over globals (except Config constants)
- Optional parameters with defaults: `boundary_boost: float = 1.5`
- Type hints for all parameters (strongly encouraged)

**Return Values:**
- Single return value for most functions: `def normalize_shelfmark(...) -> str:`
- Tuple returns for complex results: `(result, error)` pattern in services
- None returns acceptable for side-effect functions (e.g., logging)
- Boolean returns for predicates: `chunk_crosses_boundary(...) -> True/False`

## Module Design

**Exports:**
- Main modules export core classes and functions at module level
- Example from `genizah_core.py`:
  ```python
  from genizah_core import (
      Config, MetadataManager, SearchEngine, LabEngine, Indexer,
      AIManager, ListsManager, tr, get_logger
  )
  ```
- Utility functions typically public (no leading underscore)
- Constants (HEBREW_FREQ, NIKUD_PATTERN) exported

**Barrel Files:**
- Web components use `__init__.py` for grouping (e.g., `web/components/__init__.py`)
- No observed barrel file pattern for re-exporting all utilities

**Module Organization:**
- `genizah_core.py` - Core search, indexing, metadata (~3000 lines)
- `genizah_app.py` - Desktop PyQt6 UI (~720 lines)
- `web/` directory - Web app (NiceGUI)
  - `main.py` - Entry point
  - `pages/` - Page components
  - `components/` - Reusable UI components
  - `supabase_client.py` - Database integration
- `tests/` - Unit tests

---

*Convention analysis: 2026-02-05*
