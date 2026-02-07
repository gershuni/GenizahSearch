# Domain Pitfalls: Shared Service Extraction & Desktop PGP Integration

**Domain:** Dual-app service extraction (NiceGUI web + PyQt6 desktop)
**Researched:** 2026-02-07
**Confidence:** HIGH (based on direct codebase analysis of all files involved)

This document supersedes the v1 External Data Integration pitfalls. Those pitfalls (Tantivy schema rebuild, granularity mismatch, shelfmark normalization, etc.) were addressed in the v1 milestone. This document covers pitfalls specific to the NEXT milestone: shared service extraction, desktop PGP integration, and transcription indexing.

---

## Critical Pitfalls

Mistakes that cause regressions, data corruption, or fundamental architecture problems.

---

### Pitfall 1: Breaking Web App Import Chains During Service Extraction

**What goes wrong:** Moving `web/document_service.py` out of the `web/` package breaks every `from web.document_service import ...` statement across at least 4 files (`web/pages/browse.py`, `web/pages/search.py`, `web/components/joins_panel.py`, and several lazy imports within those files). The web app stops working entirely.

**Why it happens:** The service module currently lives inside `web/` and imports `from web.supabase_client import get_client` (line 18 of document_service.py). Moving it to a shared location like `services/document_service.py` or `shared/document_service.py` breaks two directions:
1. The service's own import of `get_client` (it currently depends on `web.supabase_client`)
2. All 15+ import sites in web pages that do `from web.document_service import get_document_for_fragment, get_section_for_page, ...`

**Consequences:**
- Web app fails to start (ImportError at module load)
- If fixed hastily with `sys.path` hacks, creates fragile import chains
- Production deployment breaks if any import path is missed

**Prevention:**
1. Use dependency injection: The shared document_service should accept a `get_client` callable as a parameter rather than importing it directly. Both apps provide their own client getter.
2. Create a compatibility shim: After moving, leave a thin `web/document_service.py` that re-exports from the new location:
   ```python
   # web/document_service.py (shim for backward compatibility)
   from shared.document_service import *
   ```
3. Run all web imports as a smoke test before committing: `python -c "from web.pages.browse import *; from web.pages.search import *"`
4. Audit every import site using grep before moving (there are at least 15 direct imports spread across lazy `from web.document_service import ...` inside functions)

**Detection:** Any `ImportError` at web app startup. Test with `python -m web.main` after every extraction step.

**Which phase should address:** First phase (service extraction). This must be verified before any other work proceeds.

**Severity:** BLOCKING -- web app will not start if this is wrong.

---

### Pitfall 2: Supabase Client Singleton Conflict Between Two Client Modules

**What goes wrong:** The project currently has THREE separate Supabase client initialization paths, each with its own singleton:
1. `web/supabase_client.py` -- module-level `_client` singleton via `get_client()` (line 30-43)
2. `supabase_corrections_client.py` -- class-level `self._client` within `SupabaseCorrectionsClient` (line 292-307)
3. `lists_sync.py` -- its own `_get_client()` that can use an "external client" (line 78)

When extracting a shared service, connecting it to the wrong client instance means:
- Auth tokens are not shared (user is "logged in" on one client but not the other)
- Rate limiting hits because two clients make independent connections
- Session refresh on one client does not propagate to the other

**Why it happens:** Each module was developed independently. The web client assumes server-side auth (NiceGUI session), while the desktop client manages auth tokens on disk with keyring storage. These are fundamentally different auth models.

**Consequences:**
- Desktop PGP queries fail with 401 because the shared service uses the unauthenticated web client instead of the desktop's authenticated client
- Doubled connection overhead
- Subtle bugs where data appears for logged-in web users but not logged-in desktop users

**Prevention:**
1. Create a single `shared/supabase_provider.py` that defines the interface: `get_client() -> Client`
2. Each app registers its client factory at startup:
   ```python
   # Desktop startup
   from shared.supabase_provider import set_client_factory
   set_client_factory(lambda: corrections_client._get_client())

   # Web startup
   from shared.supabase_provider import set_client_factory
   set_client_factory(web.supabase_client.get_client)
   ```
3. The shared document_service calls `get_client()` from the provider, not from any specific module
4. **Key insight:** PGP document data (documents, document_sources, document_fragments tables) is public/read-only -- it does not require user auth. The shared service only needs an anon-key client, not an authenticated one. This simplifies the problem significantly: a simple anon client can be created anywhere without worrying about session state.

**Detection:** After extraction, verify that `get_client()` returns a working client in both apps by running a simple query like `client.table('documents').select('pgpid').limit(1).execute()`.

**Which phase should address:** Service extraction phase, before desktop integration begins.

**Severity:** BLOCKING -- desktop PGP features will silently return empty results if the client is misconfigured.

---

### Pitfall 3: Blocking PyQt6 UI Thread with Synchronous Supabase Calls

**What goes wrong:** All functions in `document_service.py` are synchronous (no async/await -- confirmed by grep). Calling `get_document_for_fragment()` or `get_all_sources_for_fragment()` from the desktop's main thread will freeze the UI for 100-500ms per call (network round-trip to Supabase). For batch operations like `get_sys_ids_with_transcriptions()` checking dozens of sys_ids, the freeze could be several seconds.

**Why it happens:** In the web app, NiceGUI handles this because page rendering is server-side and individual requests do not block other users. But PyQt6's event loop runs on the main thread. Any synchronous network call in the main thread blocks painting, input handling, and all user interaction.

**Consequences:**
- Desktop app appears "frozen" or "not responding" when loading PGP data
- Windows may show "Not Responding" title bar after ~5 seconds of blocking
- Users cannot cancel operations or interact with other parts of the UI
- Worst case: PyQt6 timers and animations stutter or skip frames

**Prevention:**
1. Every Supabase call from the desktop must go through a QThread worker, following the existing pattern in `gui_threads.py`
2. Create a `PGPDataThread(QThread)` worker class with signals:
   ```python
   class PGPDataThread(QThread):
       document_loaded = pyqtSignal(dict)  # document dict or empty
       sources_loaded = pyqtSignal(list)
       error_occurred = pyqtSignal(str)

       def __init__(self, operation, *args):
           super().__init__()
           self.operation = operation
           self.args = args

       def run(self):
           try:
               result = self.operation(*self.args)
               # emit appropriate signal based on return type
           except Exception as e:
               self.error_occurred.emit(str(e))
   ```
3. Do NOT try to make document_service async -- that would break the web app's current synchronous usage pattern and add unnecessary complexity
4. Use the existing pattern: the desktop app already has `ImageLoaderThread` (line 1797), `ShelfmarkLoaderThread` (line 235 of gui_threads.py), `ExternalResourceThread`, and `AIWorkerThread` -- follow the same signal/slot pattern
5. Cache aggressively: Once a PGP document is loaded for a sys_id, store it in a local dict. The data changes rarely (PGP imports are infrequent).

**Detection:** Any call to `document_service.*` from `genizah_app.py` that is NOT inside a QThread `run()` method is a bug. Audit for this pattern.

**Which phase should address:** Desktop integration phase (when adding PGP features to desktop).

**Severity:** BLOCKING -- the desktop app will feel broken without this.

---

### Pitfall 4: Tantivy Index Rebuild Destroys Existing Index Without Rollback

**What goes wrong:** The current `Indexer.create_index()` method (genizah_core.py lines 3897-3899) does `shutil.rmtree(db_path)` followed by `os.makedirs(db_path)` before building the new index. If the rebuild fails midway (out of memory, corrupted source file, power loss), the user has NO index at all -- neither old nor new.

Adding transcription fields to the schema requires a full rebuild (Tantivy does not support adding fields to an existing schema -- confirmed by [tantivy issue #301](https://github.com/quickwit-oss/tantivy/issues/301)). The rebuild processes ~217K records from HTR transcription files plus potentially ~9,364 PGP transcriptions. This takes minutes. A failure midway is a real risk.

**Why it happens:** Tantivy requires a complete schema definition at index creation time. There is no `ALTER INDEX ADD FIELD` equivalent. The current code assumes rebuild always succeeds.

**Consequences:**
- User's search is completely broken until they manually rebuild
- Desktop app becomes unusable (search is the primary feature)
- No way to recover without re-running the full rebuild process
- If the new schema code has a bug, every user who rebuilds is stuck

**Prevention:**
1. Build the new index in a temporary directory (`tantivy_db_new/`) alongside the existing one
2. Only after successful commit and verification, swap directories:
   ```python
   # Build in temp location
   new_db_path = os.path.join(Config.INDEX_DIR, "tantivy_db_new")
   # ... build index ...
   writer.commit()

   # Verify the new index opens and has expected doc count
   test_index = tantivy.Index.open(new_db_path)
   test_searcher = test_index.searcher()
   if test_searcher.num_docs < expected_minimum:
       raise RuntimeError("New index has too few documents")

   # Atomic swap
   old_db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
   backup_path = os.path.join(Config.INDEX_DIR, "tantivy_db_old")
   os.rename(old_db_path, backup_path)
   os.rename(new_db_path, old_db_path)
   shutil.rmtree(backup_path)
   ```
3. Keep the old index as a backup until the new one is verified
4. Add a minimum document count assertion after rebuild

**Detection:** After any index rebuild, verify `searcher.num_docs` matches expected count before declaring success.

**Which phase should address:** Tantivy index rebuild phase. Must be implemented before shipping the new index schema.

**Severity:** BLOCKING -- search is the primary feature; losing the index makes the app useless.

---

## Moderate Pitfalls

Mistakes that cause delays, confusion, or technical debt.

---

### Pitfall 5: Two Incompatible Data Class Hierarchies for the Same Domain Objects

**What goes wrong:** The desktop's `supabase_corrections_client.py` defines its own `User`, `Correction`, `Comment`, `Discovery`, `FragmentJoin` dataclasses (lines 86-254) that are structurally different from the web's dict-based returns. The web's `document_service.py` returns raw dicts from Supabase. If a shared service returns dicts, the desktop code that expects dataclass objects will break. If it returns dataclasses, the web code that expects dicts will break.

**Why it happens:** The desktop client was a "drop-in replacement" for an older REST API client (corrections_client.py) and preserved its dataclass interface. The web client was built later with a simpler dict-based approach. Neither was designed for sharing.

**Prevention:**
1. For PGP document data specifically (the scope of this milestone), define the shared return types clearly:
   - `get_document_for_fragment()` returns `dict | None` -- keep this, it is simple and both apps can consume it
   - Do NOT try to unify the corrections/comments/discoveries dataclasses yet -- that is a separate milestone
2. The shared document_service should return plain dicts (matching current web behavior). The desktop adapter wraps these into whatever the desktop UI expects if needed.
3. Add type annotations to the shared service so both consumers know the exact dict shape:
   ```python
   from typing import TypedDict

   class DocumentDict(TypedDict, total=False):
       pgpid: int
       shelfmark_combined: str
       document_type: str
       tags: list
       transcription: str
       # ... etc
   ```

**Detection:** Type errors or `AttributeError` when desktop code tries to access `.attribute` on a dict, or web code tries to access `['key']` on a dataclass.

**Which phase should address:** Service extraction phase. Define the contract clearly upfront.

**Severity:** ANNOYING -- causes runtime errors but each one is easy to fix individually.

---

### Pitfall 6: Duplicating UI Logic Instead of Sharing Display Logic

**What goes wrong:** When adding PGP features to the desktop, the temptation is to copy-paste the web's transcription display logic (recto/verso section parsing, source selector, translation display) into PyQt6 widgets. This creates two independent implementations that drift apart over time.

**Why it happens:** NiceGUI widgets (`ui.html`, `ui.label`, `ui.select`) have completely different APIs from PyQt6 widgets (`QTextBrowser`, `QLabel`, `QComboBox`). It feels easier to rewrite than to share.

**Consequences:**
- Bug fixes applied to one UI but not the other
- Feature improvements only added to one app
- The transcription section parser (`parse_transcription_sections`, `get_section_for_page`) gets duplicated or subtly modified
- Recto/verso display bugs (already a known tech debt item from v1) get fixed in one place but not the other

**Prevention:**
1. The business logic is already properly separated in `document_service.py`: functions like `parse_transcription_sections()` and `get_section_for_page()` are pure functions that take strings and return strings/dicts. These MUST remain in the shared service.
2. Only the UI rendering (how to display the result) should differ between apps.
3. Pattern: `shared service (data) -> app-specific presenter (formatting) -> framework widget (display)`
4. Specifically for transcription display:
   - Shared: `get_section_for_page(transcription, page_num)` -- returns plain text
   - Web: wraps in `ui.html()` with CSS styling
   - Desktop: wraps in `QTextBrowser.setHtml()` with inline styling

**Detection:** Any function in the desktop app that re-implements string parsing or data transformation already present in document_service.py.

**Which phase should address:** Desktop PGP integration phase.

**Severity:** MODERATE -- creates maintenance burden that grows over time.

---

### Pitfall 7: Not Handling Offline/Timeout for Desktop PGP Features

**What goes wrong:** The desktop app may run without internet connectivity (it is a local application). The web app always has internet (it runs on a server). When PGP features are added to the desktop, they will silently fail or hang if Supabase is unreachable.

**Why it happens:** `document_service.py` catches exceptions and returns `None` or `[]` (graceful degradation), but the desktop UI may not distinguish between "no PGP data exists for this fragment" and "network error prevented loading." The user sees nothing and assumes there is no transcription.

**Consequences:**
- Users on poor connections see blank PGP sections with no indication of why
- Users offline see no error message, just missing features
- The `is_server_available()` check in `supabase_corrections_client.py` (line 516-542) has a 0.5s socket timeout, but this is not used by `document_service.py`

**Prevention:**
1. Add a connectivity check before PGP data loads in the desktop app (reuse `is_server_available()` from the corrections client)
2. Return a distinct sentinel from the shared service for errors vs. "not found":
   ```python
   # Option A: Exception-based (cleaner)
   class PGPNetworkError(Exception): pass

   def get_document_for_fragment(sys_id, page_num=None):
       try:
           # ... existing logic ...
       except ConnectionError as e:
           raise PGPNetworkError(f"Cannot reach Supabase: {e}")
       except Exception as e:
           print(f"Error: {e}")
           return None  # Data error, not network

   # Option B: Flag-based (simpler)
   # Return (data, error_message) tuple
   ```
3. Show a small indicator in the desktop UI: "PGP data unavailable (offline)" rather than silently hiding the feature
4. Cache successfully loaded PGP data to a local dict or disk cache so it works offline after first load

**Detection:** Run the desktop app with network disabled and verify PGP features show appropriate messages rather than empty sections.

**Which phase should address:** Desktop PGP integration phase, as part of the UI implementation.

**Severity:** MODERATE -- degrades UX silently but does not cause crashes.

---

### Pitfall 8: Tantivy Schema Field Ordering and Query Compatibility

**What goes wrong:** The current main index schema has these fields: `unique_id`, `content`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries` (genizah_core.py lines 3902-3909). Adding a `transcription` field (or `pgp_transcription`) changes the schema. The `SearchEngine.build_tantivy_query()` method (line 4187) and all query construction assumes the current field set. If queries reference the old schema against the new index (or vice versa), tantivy will raise errors.

**Why it happens:** Tantivy queries reference fields by name. If the index has a `transcription` field but the query code has not been updated to use it, the field exists but is never searched. Conversely, if query code references a field that does not exist in an older index (user has not rebuilt yet), the query fails.

**Consequences:**
- Users who have not rebuilt their index get errors when new query code tries to access new fields
- Transcription search does not work even though the data is indexed, because query code was not updated
- Lab mode index (separate schema at lines 574-587) and main index (lines 3902-3909) schemas diverge further

**Prevention:**
1. Add a schema version check: Store a version number in the index metadata or a file in the index directory. On startup, compare against expected version. If mismatch, prompt for rebuild.
   ```python
   EXPECTED_SCHEMA_VERSION = 2  # Bump when schema changes
   # Write during rebuild:
   with open(os.path.join(db_path, "schema_version.txt"), "w") as f:
       f.write(str(EXPECTED_SCHEMA_VERSION))
   # Check on load:
   if loaded_version < EXPECTED_SCHEMA_VERSION:
       prompt_user_to_rebuild()
   ```
2. Make new field queries conditional / graceful:
   ```python
   # Graceful degradation for users with old index
   available_fields = {f.name for f in schema.fields()}
   if "transcription" in available_fields:
       # Include transcription field in query
   else:
       # Old index, skip transcription search
   ```
3. Document the schema change in release notes so users know to rebuild
4. The rebuild already happens via the desktop's "Rebuild Index" button (genizah_app.py line 948, `RebuildThread` class) -- make sure this builds the new schema

**Detection:** Start the app with an old index and verify it does not crash. Start with a new index and verify transcription search works.

**Which phase should address:** Tantivy index rebuild phase.

**Severity:** MODERATE -- causes errors for users who have not rebuilt, but is fixable by rebuilding.

---

### Pitfall 9: The `sys.path.insert(0, ...)` Hack in Desktop Supabase Client

**What goes wrong:** `supabase_corrections_client.py` line 21 does `sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'web'))` to enable imports from the web package. This is a fragile hack that:
- Pollutes the module namespace
- Can cause unexpected imports if `web/` has modules that shadow stdlib or third-party packages
- Breaks if the file is moved to a different location
- Does not actually import anything from web (it creates its own client -- the path insert was likely a leftover from development)

**Why it happens:** During the Phase 5 desktop Supabase migration, the developer needed web-compatible imports but did not want to create a proper shared module.

**Prevention:**
1. During service extraction, remove this `sys.path` hack entirely
2. The shared service module should be importable without path manipulation
3. Place shared code in a location that is naturally on Python's import path (e.g., project root level `shared/` package or simply root-level module)
4. Audit for any other `sys.path` manipulations in the codebase

**Detection:** Grep for `sys.path.insert` and `sys.path.append` across the project.

**Which phase should address:** Service extraction phase (cleanup).

**Severity:** MODERATE -- works today but creates import fragility and confusion.

---

## Minor Pitfalls

Mistakes that cause annoyance but are fixable.

---

### Pitfall 10: Recto/Verso Section Header Tech Debt Carries Forward

**What goes wrong:** The MEMORY.md notes "Recto/verso section headers stripped during parsing" as existing tech debt from v1. The `parse_transcription_sections()` function (document_service.py lines 181-233) strips section headers like "Recto" and "Verso - right margin" when extracting section text. If the desktop app displays transcriptions, users will see text without these contextual headers, which may confuse scholars who expect to see them.

**Why it happens:** The regex pattern (line 199) matches header lines and `match.end()` skips past them. The header text is used for classification but discarded from the output.

**Prevention:**
1. Fix during the service extraction phase: preserve section headers in the output
2. Add a `include_headers=True` parameter to `get_section_for_page()`:
   ```python
   def get_section_for_page(transcription, page_num, include_headers=False):
       sections = parse_transcription_sections(transcription)
       # ... existing logic ...
       if include_headers and section_header:
           return f"{section_header}\n{section_text}"
       return section_text
   ```
3. This is a good opportunity to fix this debt since the function is being extracted anyway

**Detection:** Compare displayed transcription text with the raw `transcription` field in the database. If headers are missing, the bug persists.

**Which phase should address:** Service extraction phase (fix while extracting).

**Severity:** MINOR -- scholars may notice but it does not break functionality.

---

### Pitfall 11: Missing Integration Tests for Cross-App Data Flow

**What goes wrong:** The MEMORY.md notes "No integration tests for E2E flows" as existing tech debt. When extracting a shared service consumed by two different apps, the lack of tests means regressions can only be caught by manual testing in both apps.

**Prevention:**
1. Before extracting, write a minimal test file that imports and calls every public function in `document_service.py` with mock data
2. After extracting, run these tests from both the web and desktop import paths
3. Minimum viable test:
   ```python
   def test_document_service_imports():
       """Verify all public functions are importable from shared location."""
       from shared.document_service import (
           get_document_for_fragment,
           get_fragments_for_document,
           get_transcription_for_document,
           get_document_metadata,
           parse_transcription_sections,
           get_section_for_page,
           get_sources_for_document,
           get_all_sources_for_fragment,
           get_editions_for_document,
           get_translations_for_document,
           get_sys_ids_with_transcriptions,
           get_fragments_by_tag,
       )

   def test_web_shim_reexports():
       """Verify the web compatibility shim works."""
       from web.document_service import get_document_for_fragment

   def test_parse_transcription_sections():
       """Pure function test, no network needed."""
       from shared.document_service import parse_transcription_sections
       result = parse_transcription_sections("Recto\nline1\nVerso\nline2")
       assert 'recto' in result
       assert 'verso' in result
   ```

**Detection:** Run `pytest` after every extraction step.

**Which phase should address:** Service extraction phase (write tests before extracting).

**Severity:** MINOR -- absence of tests increases risk of regressions but does not directly cause them.

---

### Pitfall 12: PGP Data Enrichment in Search Results Could Create N+1 Query Problem

**What goes wrong:** The web's `get_sys_ids_with_transcriptions()` function (document_service.py lines 426-450) does a batch `.in_()` query to check which sys_ids have PGP transcriptions. This works well. But if the desktop implements PGP enrichment naively (calling `get_document_for_fragment()` for each search result individually), it creates an N+1 query pattern: one query per result, potentially 50+ network calls for a single search page.

**Why it happens:** The web already handles this correctly with batch lookup. But when porting to the desktop, developers may not notice the batch function exists and instead call the single-document function in a loop.

**Prevention:**
1. Make `get_sys_ids_with_transcriptions()` the primary entry point for search result enrichment in both apps
2. Consider adding a batch function for loading multiple documents at once:
   ```python
   def get_documents_for_fragments(sys_ids: List[str]) -> Dict[str, dict]:
       """Batch load PGP documents for multiple fragments."""
       # Single query to document_fragments
       # Single query to documents
       # Return {sys_id: document_dict}
   ```
3. Document in the shared service's docstrings: "For search results, use `get_sys_ids_with_transcriptions()` for batch checking. Do NOT call `get_document_for_fragment()` in a loop."

**Detection:** Monitor Supabase query count during desktop search. If you see >5 sequential queries to `document_fragments` in rapid succession, you have an N+1 problem.

**Which phase should address:** Desktop PGP integration phase.

**Severity:** MINOR -- causes slowness but not breakage. Desktop users with slow connections will notice.

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|---|---|---|
| Service extraction | Pitfall 1 (broken imports) | Shim + grep audit of all import sites |
| Service extraction | Pitfall 2 (client singletons) | Client factory / dependency injection; note PGP is read-only |
| Service extraction | Pitfall 9 (sys.path hack) | Remove hack, use proper package structure |
| Service extraction | Pitfall 5 (dataclass mismatch) | Return dicts from shared service, let each app adapt |
| Service extraction | Pitfall 10 (section headers) | Fix while extracting, add include_headers param |
| Service extraction | Pitfall 11 (no tests) | Write minimal tests before moving any code |
| Desktop PGP integration | Pitfall 3 (blocking UI) | QThread workers for ALL Supabase calls |
| Desktop PGP integration | Pitfall 7 (offline handling) | Connectivity check + caching + user messaging |
| Desktop PGP integration | Pitfall 6 (duplicated UI logic) | Share business logic, only differ on rendering |
| Desktop PGP integration | Pitfall 12 (N+1 queries) | Use batch functions for search enrichment |
| Tantivy index rebuild | Pitfall 4 (destructive rebuild) | Build-in-temp-then-swap pattern |
| Tantivy index rebuild | Pitfall 8 (schema version) | Version check + graceful degradation for old indexes |

---

## Risk Matrix

| Risk Area | Severity | Likelihood | Mitigation Effort |
|---|---|---|---|
| Web app import breakage (P1) | Critical | Certain if not careful | Low (shim pattern) |
| Client singleton mismatch (P2) | Critical | High | Medium (provider pattern) |
| UI thread blocking (P3) | Critical | Certain if naive | Medium (QThread pattern exists) |
| Destructive index rebuild (P4) | Critical | Medium (on failure) | Low (temp directory swap) |
| Dataclass mismatch (P5) | Moderate | High | Low (keep dicts) |
| Duplicated UI logic (P6) | Moderate | High | Low (discipline) |
| Offline handling (P7) | Moderate | Medium | Medium (caching + UI) |
| Schema version (P8) | Moderate | Certain for existing users | Low (version file) |
| sys.path hack (P9) | Moderate | Already exists | Low (delete line) |
| Section headers (P10) | Minor | Already exists | Low (small code change) |
| Missing tests (P11) | Minor | Already exists | Low (write before moving) |
| N+1 queries (P12) | Minor | Medium | Low (use existing batch fn) |

---

## Sources

**Codebase analysis (HIGH confidence):**
- `web/document_service.py` -- 507 lines, all 12 public functions analyzed
- `web/supabase_client.py` -- singleton pattern at line 30-43
- `supabase_corrections_client.py` -- class-based client at line 261-307, sys.path hack at line 21
- `lists_sync.py` -- third client pattern at line 78
- `genizah_core.py` -- Indexer class at line 3882, schema at 3902-3909, SearchEngine at 4158+
- `gui_threads.py` -- existing QThread patterns (IndexerThread, SearchThread, etc.)
- `genizah_app.py` -- corrections_client usage, QThread patterns, RebuildThread at line 948
- `corrections_client.py` -- factory function at line 1579, fallback chain

**External references:**
- [Tantivy Issue #301: No in-place schema changes](https://github.com/quickwit-oss/tantivy/issues/301)
- [Real Python: PyQt QThread patterns](https://realpython.com/python-pyqt-qthread/)
- [PythonGUIs: Multithreading PyQt6](https://www.pythonguis.com/tutorials/multithreading-pyqt6-applications-qthreadpool/)
- [Python circular imports](https://www.datacamp.com/tutorial/python-circular-import)

**Project memory (HIGH confidence -- actual experience):**
- MEMORY.md: recto/verso headers stripped, no integration tests, TODO at document_service.py:253
- MEMORY.md: "shared service layer (Option C)" -- user's chosen approach

---

*Pitfalls audit: 2026-02-07*
*Confidence: HIGH -- every pitfall verified against actual codebase line numbers*
