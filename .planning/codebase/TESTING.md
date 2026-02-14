# Testing Patterns

**Analysis Date:** 2026-02-05

## Test Framework

**Runner:**
- pytest (pytest framework)
- Config: `tests/conftest.py` (minimal setup)

**Assertion Library:**
- Python built-in `assert` statements
- unittest assertions (`self.assertEqual()`, `self.assertTrue()`, etc.) in some legacy tests

**Run Commands:**
```bash
pytest tests/                    # Run all tests
pytest tests/test_shelfmark_normalization.py  # Run specific test file
pytest -v                        # Verbose output
pytest --tb=short               # Short traceback format
```

## Test File Organization

**Location:**
- Co-located in `tests/` directory at project root
- Separate from source code (not next to implementation)

**Naming:**
- Pattern: `test_*.py`
- Examples:
  - `tests/test_api_flow.py`
  - `tests/test_boundary_search.py`
  - `tests/test_corrections_integration.py`
  - `tests/test_excel_logic.py`
  - `tests/test_export_service.py`
  - `tests/test_shelfmark_normalization.py`

**Structure:**
```
tests/
├── conftest.py                     # Pytest configuration
├── test_api_flow.py               # API integration tests
├── test_boundary_search.py        # Search algorithm tests
├── test_corrections_api.py        # Correction endpoint tests
├── test_corrections_integration.py # Full corrections system tests
├── test_excel_logic.py            # Excel export logic
├── test_export_service.py         # Export service tests
├── test_missing_tantivy.py        # Search index tests
├── test_shelfmark_normalization.py      # Shelfmark matching
└── test_shelfmark_normalization_unified.py
```

## Test Structure

**Suite Organization:**

Pytest class-based organization from `test_boundary_search.py`:
```python
class TestParseBoundaries:
    """Test parse_boundaries function."""

    def test_paragraph_boundaries(self):
        """Test parsing boundaries with paragraph breaks."""
        text = "First paragraph.\n\nSecond paragraph.\n\nThird."
        boundaries = parse_boundaries(text, '\n\n')
        assert len(boundaries) == 2

class TestChunkCrossesBoundary:
    """Test chunk_crosses_boundary function."""
    # More test methods...
```

**Unittest class organization from `test_excel_logic.py`:**
```python
class TestExcelExport(unittest.TestCase):
    def test_sanitize(self):
        """Test basic cleaning"""
        self.assertEqual(sanitize_for_excel("Hello"), "Hello")
        self.assertEqual(sanitize_for_excel("Hello\x0bWorld"), "HelloWorld")

    def test_clean_and_marker(self):
        """Test HTML cleaning"""
        html = "Start <span style='color:red'>match</span> end"
        cleaned = _clean_and_marker(html)
        self.assertEqual(cleaned, "Start *match* end")
```

**Patterns:**
- Class-based organization by feature/function being tested
- One test method per assertion/behavior
- Descriptive test method names: `test_normalized_match_different_spacing()`
- Docstrings explain what is being tested and expected result

## Mocking

**Framework:** Python `unittest.mock` (built-in) or manual mock classes

**Patterns:**

Manual mock implementation from `test_shelfmark_normalization.py`:
```python
class MockMetadataManager:
    """Mock class with just the normalization method for testing."""

    def _normalize_shelfmark(self, shelfmark: str) -> str:
        """Normalize shelfmarks: remove non-alphanumeric chars but preserve dots between digits."""
        if not shelfmark:
            return ""
        # ... implementation for testing
        return cleaned

def get_meta_mgr_class(self):
    """Import MetadataManager dynamically to avoid initialization issues."""
    return MockMetadataManager
```

**What to Mock:**
- Database operations (SessionLocal, database queries)
- External APIs (Supabase calls, NLI endpoints)
- File system operations (reading/writing logs)
- Expensive computations (index loading)

**What NOT to Mock:**
- Core business logic (shelfmark normalization, search algorithms)
- String processing (HTML cleaning, Excel sanitization)
- Local algorithmic operations (boundary crossing, score calculation)
- Data transformations

## Fixtures and Factories

**Test Data:**

From `test_shelfmark_normalization.py`:
```python
def create_mock_meta_manager(self):
    """Create a minimal mock MetadataManager for testing search_by_meta."""
    class MockMetadataManager:
        def __init__(self):
            self.csv_bank = {
                "sys001": {"shelfmark": "T-S 12.123", "title": "Fragment 1"},
                "sys002": {"shelfmark": "T-S NS 120.2", "title": "Fragment 2"},
                "sys003": {"shelfmark": "T-S NS 121.4", "title": "Fragment 3"},
                "sys004": {"shelfmark": "MS Heb. a.1", "title": "Oxford Fragment"},
            }
    return MockMetadataManager()
```

**Location:**
- Test fixtures defined in test methods or setup methods
- Factory methods as class methods or static methods
- No central fixture file observed (not using conftest.py for fixtures extensively)

## Coverage

**Requirements:** Not enforced

**View Coverage:**
```bash
pytest --cov=genizah_core tests/
pytest --cov=web tests/
```

No coverage threshold detected in codebase. Coverage is optional.

## Test Types

**Unit Tests:**
- Scope: Individual functions and algorithms
- Approach: Test mathematical operations, string processing, matching logic
- Example from `test_boundary_search.py`:
  ```python
  def test_paragraph_boundaries(self):
      """Test parsing boundaries with paragraph breaks."""
      text = "First paragraph.\n\nSecond paragraph.\n\nThird."
      boundaries = parse_boundaries(text, '\n\n')
      assert len(boundaries) == 2
  ```
- Files: `test_boundary_search.py`, `test_excel_logic.py`, `test_shelfmark_normalization.py`

**Integration Tests:**
- Scope: Multiple components working together
- Approach: Test user registration → login → create correction → view correction
- Example from `test_corrections_integration.py`:
  ```python
  def test_user_registration():
      """Test user registration"""
      db = SessionLocal()
      user_data = UserCreate(...)
      user, error = UserService.create_user(db, user_data)
      assert error is None
      assert user.username == "testuser"
  ```
- Files: `test_corrections_integration.py`, `test_corrections_api.py`

**E2E Tests:**
- Framework: Not observed (no Selenium, Cypress, or Playwright detected)
- Not currently used in this codebase

## Common Patterns

**Async Testing:**

No async test patterns detected. Tests use synchronous database and API calls.

**Error Testing:**

From `test_corrections_integration.py`:
```python
def test_user_login(email="test@example.com", password="TestPassword123!"):
    """Test user login"""
    db = SessionLocal()
    try:
        user, error = AuthService.authenticate_user(db, email, password)

        if error:
            print(f"  ❌ Login failed: {error}")
            return None, None

        return user, tokens.access_token
    finally:
        db.close()
```

Testing error cases:
- Check error return value is not None
- Verify error messages are appropriate
- Test database cleanup in finally blocks

**Setup and Teardown:**

From `test_corrections_integration.py`:
```python
def test_user_registration():
    """Test user registration"""
    db = SessionLocal()
    try:
        # Test code
        pass
    finally:
        db.close()
```

Manual setup/teardown with try-finally blocks.

**Boundary/Edge Cases:**

From `test_shelfmark_normalization.py`:
```python
def test_normalize_empty_string(self):
    """Test empty string handling."""
    mgr = self.get_meta_mgr_class()()
    assert mgr._normalize_shelfmark("") == ""
    assert mgr._normalize_shelfmark(None) == ""

def test_no_false_positive_120_vs_121(self):
    """Test that '120.2' does not match '121.4'."""
    mgr = self.create_mock_meta_manager()
    results = mgr.search_by_meta("T-S NS 120", "shelfmark")
    assert "sys002" in results
    assert "sys003" not in results  # Must NOT match
```

Testing edge cases:
- Empty strings and None values
- Boundary conditions (first char, last char)
- False positive prevention
- Unicode/Hebrew text handling

**Hebrew Text Testing:**

From `test_boundary_search.py`:
```python
def test_hebrew_text(self):
    """Test with Hebrew text."""
    text = "פסקה ראשונה עם מילים\n\nפסקה שנייה עם מילים נוספות"
    boundaries = parse_boundaries(text, '\n\n')
    assert len(boundaries) == 1
```

From `test_excel_logic.py`:
```python
def test_hebrew_corruption(self):
    """Excel XML doesn't like certain unicode control characters even if valid in Python strings."""
    # Specifically RTL markers if not handled right?
    pass
```

## Best Practices Observed

**Good:**
1. Descriptive test names that explain what is being tested
2. One assertion per test method (when practical)
3. Docstrings explaining the purpose and expected behavior
4. Proper resource cleanup with try-finally or context managers
5. Testing both happy path and error cases
6. Edge case coverage (empty strings, None, boundaries)
7. Hebrew text support in tests
8. Mock objects created locally to avoid complex setup

**Gaps:**
1. No observed use of pytest fixtures for common setup
2. No parametrized tests observed (`@pytest.mark.parametrize`)
3. Limited E2E testing (no browser automation)
4. No observed use of conftest.py for shared fixtures
5. Coverage not enforced or measured
6. Some tests still use unittest style instead of pure pytest style

## Test Execution

**Current State:**
- Tests can be run with `pytest tests/`
- Path setup in conftest.py ensures imports work:
  ```python
  ROOT_DIR = Path(__file__).resolve().parent.parent
  ROOT_PATH = str(ROOT_DIR)
  if ROOT_PATH not in sys.path:
      sys.path.insert(0, ROOT_PATH)
  ```
- Database tests use `SessionLocal()` for test database sessions
- Manual database teardown required

---

*Testing analysis: 2026-02-05*
