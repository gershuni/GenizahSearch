# Phase 133: Visual Atlas Preview (early quick win) - Pattern Map

**Mapped:** 2026-07-20
**Files analyzed:** 9 new/modified committed files + 4 test files
**Analogs found:** 13 / 13 (all have at least a role-match analog; the offline
graph-math/typed-array-binary/Canvas-2D-renderer *content* itself has no
committed analog and is called out explicitly in "No Analog Found")

> **Masking note:** `same_work_spike/probe/scripts/build_atlas_draft.py` is
> gitignored research (confirmed via `.gitignore` line 205 `same_work_spike/`
> and `git ls-files | grep same_work_spike` = 0 tracked files). It is
> referenced below **only as "the prototype"**, described structurally
> (function names, control flow, CLI-arg shape) — no line is quoted verbatim,
> and it must never be committed. All concrete code excerpts in this document
> are taken from **already-committed** repo files only.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `scripts/build_atlas_asset.py` (NEW) | build/offline-batch script | batch (SQLite + CSV read → aggregate/transform → static asset write) | `scripts/fgp_fill_credits_bilingual.py` (committed CLI/argparse+sqlite3 shape) — *pipeline logic* forked from the prototype (gitignored, described only) | role-match (script shape) |
| `scripts/check_atlas_masking.py` (NEW) | build/offline-batch guard script | batch (walk files → pattern match → exit code) | `scripts/check_docs.py` | exact |
| `web/feature_flags.py` (+`ATLAS_PREVIEW_ENABLED`) | config | none (env read, re-checked per call) | `web/feature_flags.py::WEB_PUZZLE_ENABLED` (same file, existing line) | exact |
| `web/main.py` (+`@ui.page('/atlas')`) | route / frontend-server page | request-response | `web/main.py::puzzle_page_route` (`@ui.page('/puzzle')`) | exact |
| `web/main.py` (+ Brotli asset-serving route) | API route / static-asset server | request-response, file-I/O | `web/api.py` (`robots_txt`, `nli_image_by_sysid` — `@target_app.get(...)` returning `Response(content=..., media_type=..., headers={...})`) | role-match |
| `web/main.py` (+ nav item in `create_layout()`) | UI component (nav) | request-response (render) | `web/main.py` `nav_items` + `WEB_PUZZLE_ENABLED` gating (same file, existing lines) | exact |
| `web/pages/atlas.py` (NEW) | UI component / page chrome | request-response | `web/pages/puzzle.py::create_puzzle_page` (flag-gated page body) + `web/main.py::_resolve_ui_language`/`apply_theme_immediately` (bilingual/RTL primitives) | role-match |
| `web/pages/home.py` (+ teaser card) | UI component | request-response (render) | `web/pages/home.py` "Main Action Cards Grid" card block (Community Card, same file) | exact |
| `.gitignore` (+ atlas static-asset dir entry) | config | n/a | `.gitignore` sidecar entries (`/fjms_enrichment.db`, `/pgp.db`, `same_work_spike/`) | exact |
| `tests/test_atlas_bake.py` (NEW) | test (unit) | batch (assert on bake output) | any `tests/test_*` asserting counts/assertions on a generated artifact — closest shape: `tests/test_no_back_edges_core.py`'s smoke-assertion style | role-match |
| `tests/test_atlas_flag_gating.py` (NEW) | test (static/headless) | request-response (headless) | `tests/test_web_library_options_no_local.py` (AST scan for a required guard pattern) | role-match |
| `tests/test_atlas_masking_scan.py` (NEW) | test (self-test) | batch (sanity-injection) | `tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation` (inject a known-bad pattern, assert the scanner catches it) | exact |
| `tests/render_smoke/test_atlas_render_smoke.py` (NEW) | test (live render) | request-response (async, in-process) | `tests/render_smoke/test_joins_lab_render_smoke.py` | exact |

## Pattern Assignments

### `scripts/build_atlas_asset.py` (build/offline-batch script, batch data flow)

**Analog:** `scripts/fgp_fill_credits_bilingual.py` (committed script shape) —
**pipeline logic** to fork from the prototype (`same_work_spike/probe/scripts/build_atlas_draft.py`,
gitignored, described structurally only, never quoted/committed).

**Script-shape pattern** (path bootstrap + argparse + module-level constants —
source: `scripts/fgp_fill_credits_bilingual.py` lines 42-66, 288-304, 400):
```python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FIST_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fist_data", "FIST.db"
)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("db_path")
    ap.add_argument("--report", action="store_true", help="don't write; print coverage + samples")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db_path)
    fi = sqlite3.connect(f"file:{FIST_DB}?mode=ro", uri=True)
    ...

if __name__ == "__main__":
    sys.exit(main())
```
Replicate this shape for `build_atlas_asset.py`: `argparse` flags for the
research DB path / output dir / byte-budget assertion toggle (the prototype
instead used bare `sys.argv[N]` positional defaults — RESEARCH.md recommends
the committed fork upgrade to `argparse` for auditability, matching this
established committed-script convention), a module-level `OUT_DIR` computed
from `os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`, and a
`def main() -> int: ...` / `if __name__ == "__main__": sys.exit(main())` tail.

**Offline pipeline structure to fork (described, not quoted)** — the
prototype's `main()` calls, in order: `load_lib_meta()` (reads
`libraries.csv` via `csv.reader`, `utf-8-sig`), `load_domains()` (reads
`fjms_enrichment.db`'s `domains` table via `sqlite3.connect` + a plain
`SELECT`), a Louvain community pass over a `scipy.sparse.coo_matrix` +
`connected_components`, a recursive split helper for oversized components, a
force-layout + phyllotaxis-scatter step, then a final HTML/JSON write to an
`OUT` path. Fork this control flow verbatim (per RESEARCH.md "Don't
Hand-Roll" — the graph math is already proven); the *new* work is (a)
stripping the discovery-overlay load entirely (D-04 — no `discovery_scored_flank`-
style read at all), (b) extending node inclusion to the full manuscript-pair
universe (Pitfall #1 in RESEARCH.md), (c) re-encoding the payload as
struct-of-typed-arrays instead of `json.dumps` + `str.replace('__DATA__', ...)`,
and (d) a final `Brotli`-compress step writing both `.bin` and `.bin.br`.

**DB access pattern** (source: `scripts/fgp_fill_credits_bilingual.py` lines
288-304 — read-only attach + `PRAGMA table_info` capability probe before
assuming a column exists):
```python
conn = sqlite3.connect(args.db_path)
fi = sqlite3.connect(f"file:{FIST_DB}?mode=ro", uri=True)   # read-only attach
cols = {r[1] for r in conn.execute("PRAGMA table_info(fgp_transcriptions)")}
```
Use the same `?mode=ro` URI style when opening the gitignored research DB
(`fullcorpus_v2.db`) and `fjms_enrichment.db` from the bake script — the bake
never writes to either input DB.

**Output-location convention:** write the baked asset under a **gitignored**
static directory (e.g. `web/static/atlas/`), never a committed path — see
the `.gitignore` Shared Pattern below. Bake-time-only dependencies
(`networkx`, `python-louvain`, `Brotli`) must **not** be added to
`requirements.txt`/`requirements-lock.txt` (RESEARCH.md: "this tooling never
runs inside the web process").

---

### `scripts/check_atlas_masking.py` (build/offline-batch guard script, batch data flow)

**Analog:** `scripts/check_docs.py` (full file read; exact structural match).

**Pattern-list + scan-function + issue-collection shape** (source:
`scripts/check_docs.py` lines 24-30, 81-110, 202-277):
```python
# Terms that may indicate outdated content
# Format: (term, reason, exclude_files)
OUTDATED_TERMS = [
    ('genizah-backend', 'Service removed - only genizah-web exists', []),
    ('backend/requirements.txt', 'File no longer exists', []),
    ('DATABASE_URL', 'No longer used - replaced by SUPABASE_URL', []),
]

def check_outdated_terms() -> list:
    """Search for terms that may indicate outdated content."""
    issues = []
    for md_file in DOCS_DIR.rglob('*.md'):
        if 'archive' in str(md_file):
            continue
        try:
            content = md_file.read_text(encoding='utf-8')
        except Exception:
            continue
        relative_path = md_file.relative_to(ROOT_DIR)
        for term, reason, exclude_files in OUTDATED_TERMS:
            if md_file.name in exclude_files:
                continue
            if re.search(re.escape(term), content, re.IGNORECASE):
                issues.append(f"{relative_path}: Contains '{term}' - {reason}")
    return issues

def main():
    ...
    total_issues = 0
    outdated = check_outdated_terms()
    ...
    return 0 if total_issues == 0 else 1

if __name__ == '__main__':
    exit(main())
```
Replicate this exact shape for `check_atlas_masking.py`: a `scan_repo()`
function that walks `git ls-files` (or `Path.rglob`) over the committed tree,
and a `scan_asset(path)` function that reads every embedded string out of the
built HTML/JSON/binary asset — both checking against a pattern list, both
returning `issues: list`, with `main()` aggregating and `return 0 if
total_issues == 0 else 1`. Per D-07/RESEARCH.md Security Domain, the pattern
list itself must **never be hardcoded** in this committed `.py` file — source
it from a gitignored/env-var location, following the exact existing
sensitive-value idiom in this repo:

**Sensitive-value-from-env idiom to reuse** (source: `web/puzzle_tokens.py`
lines 16-18):
```python
# Secret key for HMAC signing. In production, set PUZZLE_UPLOAD_SECRET env var.
# Falls back to a random key per process (tokens won't survive restarts).
PUZZLE_SECRET = os.environ.get('PUZZLE_UPLOAD_SECRET', os.urandom(32).hex())
```
Model `MASKING_SCAN_PATTERNS_FILE = os.environ.get('MASKING_SCAN_PATTERNS_FILE')`
on this same shape (env var pointing at a gitignored local file — RESEARCH.md
Open Question #3's recommendation), never a literal pattern string in the
script itself. `web/main.py`/`web/auth_state.py`/`web/user_lists.py` all
`from dotenv import load_dotenv; load_dotenv()` at module top — call the same
in this script if `.env`-based loading is preferred over a standalone file
path.

**Self-test / sanity-injection pattern to mirror** (source:
`tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation`,
lines 235-247 — proves the scanner actually catches a known-bad input, not
just that it returns clean on real content):
```python
def test_lint_rejects_synthetic_violation():
    """FOUND-04 SC4: verify the lint visitor detects a synthetic raw access."""
    synthetic = textwrap.dedent("""\
        from nicegui import app
        def bad():
            return app.storage.user.get('foo')
    """)
    tree = ast.parse(synthetic)
    aliases = _find_app_aliases(tree)
    assert aliases == {'app'}, f"Expected alias 'app', got {aliases}"
    visitor = _StorageAccessVisitor(aliases, synthetic)
    visitor.visit(tree)
    assert visitor.violations, "Lint visitor failed to detect synthetic raw access"
```
`tests/test_atlas_masking_scan.py` should do the same: inject a fabricated
"known-bad" test-only pattern (NOT the real restricted string) into a temp
file/string and assert `check_atlas_masking` flags it — proving the scan is
load-bearing, not a no-op.

---

### `web/feature_flags.py` (+ `ATLAS_PREVIEW_ENABLED`) (config, no data flow)

**Analog:** same file, existing `WEB_PUZZLE_ENABLED` (source:
`web/feature_flags.py` lines 8-15, verbatim, already committed):
```python
def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


WEB_PUZZLE_ENABLED = _env_enabled("WEB_PUZZLE_ENABLED", True)
```
Add directly below: `ATLAS_PREVIEW_ENABLED = _env_enabled("ATLAS_PREVIEW_ENABLED", False)`
(default **OFF** — D-13 says the flag is the safety mechanism controlling
exposure; matches `web_fgp_enabled()`'s pattern of a plain module-level
constant for a simple on/off gate rather than needing the more complex
web-override-of-shared-default shape `web_fgp_enabled()` uses, since there is
no "shared vs web-only" split here).

---

### `web/main.py` (+ `@ui.page('/atlas')` route) (route/frontend-server page, request-response)

**Analog:** `web/main.py::puzzle_page_route` (source: lines 1920-1960,
verbatim, already committed) — the closest existing precedent for a
feature-flag-gated page with a clean-hide fallback:
```python
@ui.page('/puzzle', title='Fragment Puzzle | Dicta Genizah Search')
def puzzle_page_route(add: str = None, doc: str = None):
    safe_user_set('current_page', '/puzzle')
    ui.add_head_html(page_meta(
        '/puzzle',
        title='Fragment Puzzle | Dicta Genizah Search',
        description='...',
        needs_iiif=True,
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        if not WEB_PUZZLE_ENABLED:
            is_hebrew = get_language() == 'he'
            with ui.column().classes('w-full max-w-3xl mx-auto p-6'):
                with ui.card().classes('w-full p-8'):
                    ui.icon('construction').classes('text-4xl text-amber-600 mb-3')
                    ui.label('...' if is_hebrew else '...').classes('text-2xl font-bold mb-2')
                    ...
            return
        from web.pages.puzzle import create_puzzle_page
        create_puzzle_page(initial_add=add, initial_doc=doc)
```
Replicate exactly for `/atlas`: `@ui.page('/atlas', title='Connections Atlas | ... — Dicta Genizah Search')`,
`ui.add_head_html(page_meta('/atlas', ..., noindex=True))` (per D-16/Pitfall's
`noindex` requirement — see the `page_meta` excerpt below), gate on
`ATLAS_PREVIEW_ENABLED` **inside the route handler itself** (not just the nav
link — RESEARCH.md Security Domain flags nav-only gating as a real
access-control gap), fall through to a clean "temporarily unavailable" card
identical in shape to the puzzle one when the flag is OFF or the baked asset
file is missing, and delegate the real chrome to `web/pages/atlas.py`.

**Bilingual language resolution to reuse verbatim** (source: `web/main.py`
lines 851-859, already used by every other page):
```python
def _resolve_ui_language() -> str:
    """Return the persisted UI language so layout and bootstrap agree on first render."""
    saved_lang = safe_user_get('ui_language')
    if saved_lang in ('he', 'en'):
        return saved_lang
    current_lang = get_language()
    return current_lang if current_lang in ('he', 'en') else 'he'
```

**`noindex` SEO primitive to reuse, zero new code** (source: `web/main.py`
lines 752-765, 1909):
```python
def page_meta(
    path: str = '/', title: str = _DEFAULT_TITLE, description: str = _DEFAULT_DESCRIPTION,
    og_type: str = 'website', noindex: bool = False, needs_iiif: bool = False,
) -> str:
    ...
    robots = '<meta name="robots" content="noindex, noarchive, follow">\n' if noindex else ''
    ...

# usage elsewhere in the same file, e.g. /lists:
ui.add_head_html(page_meta('/lists', noindex=True))
```
Use `noindex=True` on **both** `/atlas` and the homepage teaser's target link
per D-16 ("set `noindex` until the REL-01 gate").

---

### `web/main.py` (+ Brotli asset-serving route) (API route/static-asset server, request-response + file-I/O)

**Analog:** `web/api.py` — `@target_app.get(...)` handlers returning a raw
`Response` with custom headers (source: `web/api.py` lines 407-449, 1068-1091,
already committed):
```python
@target_app.get('/robots.txt')
def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        ...
    )
    return Response(content=content, media_type="text/plain")

@target_app.get('/api/nli_image_by_sysid/{sys_id}')
def nli_image_by_sysid(sys_id: str, page: int = 0, width: int = 2000, suffix: int = 1):
    ...
    content, content_type, _fl_id = got
    return Response(
        content=content,
        media_type=content_type,
        headers={"Cache-Control": "public, max-age=600"},
    )
```
Replicate this exact `Response(content=..., media_type=..., headers={...})`
shape for the new atlas-data route (per RESEARCH.md Pattern 3): a hardcoded
server-side file path (no user-supplied path segment — closes the
path-traversal surface noted in RESEARCH.md Security Domain), an
`Accept-Encoding: br` check on the incoming request, `Content-Encoding: br`
+ a far-future immutable `Cache-Control` on the compressed branch, and a
plain-bytes fallback branch with no `Content-Encoding` header for the ~4% of
clients without Brotli support. Register it either directly on the NiceGUI
`app` singleton in `web/main.py` (alongside the existing
`app.add_static_files('/static', STATIC_DIR)` call, source: line 740) or via
`init_api_routes()` in `web/api.py` (source: `def init_api_routes(app_override=None):`
line 391) — either location follows this same established
`@target_app.get(...) -> Response(...)` idiom; **do not** attempt to serve
the `.br` file through `add_static_files` (confirmed in RESEARCH.md Pitfall 5
that `CacheControlledStaticFiles` does not set `Content-Encoding`).

---

### `web/main.py` (+ nav item in `create_layout()`) (UI component/nav, request-response render)

**Analog:** same file, existing `nav_items` list + `WEB_PUZZLE_ENABLED` gate
(source: `web/main.py` lines 1147-1159, verbatim, already committed):
```python
nav_items = [
    ('/', 'home', tr('Home'), None),
    ('/about', 'info', tr('About the Genizah'), None),
    ('/search', 'search', tr('Search'), None),
    ('/parallels', 'compare_arrows', tr('Find Parallels'), None),
    ('/browse', 'menu_book', tr('Browse by Shelfmark'), None),
    ('/catalog-browse', 'category', tr('Browse by Identification'), None),
    ('/discoveries', 'lightbulb', tr('Community'), None),
    ('/lists', 'star', tr('My Lists'), None),
    ('/joins-lab', 'join_inner', tr('Joins Lab'), None),
]
if WEB_PUZZLE_ENABLED:
    nav_items.append(('/puzzle', 'extension', tr('Fragment Puzzle'), None))

for path, icon, label, badge in nav_items:
    is_active = current_page == path
    with ui.row().classes(f'nav-item {"active" if is_active else ""}').on('click', lambda p=path: nav_to(p)):
        ui.icon(icon).classes('nav-item-icon')
        ui.label(label)
        if badge:
            ui.label(badge).classes('nav-item-badge')
```
The 4-tuple already supports a badge string (rendered via `nav-item-badge`
class) — this is the exact mechanism for D-14/D-15's **"beta"-tagged nav
link**:
```python
if ATLAS_PREVIEW_ENABLED:
    nav_items.append(('/atlas', 'hub', tr('Connections Atlas'), tr('Beta')))
```
(Import `ATLAS_PREVIEW_ENABLED` from `web.feature_flags` alongside the
existing `from web.feature_flags import WEB_PUZZLE_ENABLED`, source: line 688.)

---

### `web/pages/atlas.py` (NEW) (UI component/page chrome, request-response)

**Analog:** `web/pages/puzzle.py::create_puzzle_page` (flag-gated page-body
delegate, imported lazily from the route function — same
`from web.pages.X import create_X_page` shape used at `web/main.py` line
1959) + the bilingual/RTL primitives already read above
(`_resolve_ui_language`, `apply_theme_immediately`, `tr`/`is_rtl` from
`web/translations.py`).

**Module header shape to mirror** (source: `web/pages/home.py` lines 1-18,
verbatim, already committed):
```python
# -*- coding: utf-8 -*-
"""
Research Dashboard - Dicta Genizah Search
...
"""
import asyncio
from nicegui import ui
from web.state import state
from web.translations import tr, is_rtl
from web.components.typography import h1, h2, h3


def create_page():
    """Create the research dashboard home page."""
    page_client = ui.context.client
    with ui.column().classes('w-full max-w-7xl mx-auto gap-3 fade-in'):
        ...
```
`web/pages/atlas.py` should follow this exact `create_atlas_page()` function
shape: a top-level `ui.column`/`ui.element` container, `tr()`/`is_rtl()` for
every string and layout direction, and a dismissible-banner pattern (see
Shared Patterns below) for the standing D-15 honesty banner + "Beta /
preview" badge. The Canvas-2D renderer + typed-array decode JS itself has
**no committed analog** (see "No Analog Found") — port it from the
prototype's existing JS (described structurally, not quoted), fetching the
binary payload from the new Brotli route instead of an inlined `<script>`
(Pitfall 3 — avoids the `</script>`-breakout injection class).

---

### `web/pages/home.py` (+ teaser card) (UI component, request-response render)

**Analog:** same file, existing "Main Action Cards Grid" — the Community
Card is the closest single-card shape to copy (source: `web/pages/home.py`
lines 397-416, verbatim, already committed):
```python
# Community Card
with ui.card().classes('p-0 overflow-hidden cursor-pointer hover:shadow-xl transition-all').props(
    'role=button tabindex=0'
).on('click', lambda: ui.navigate.to('/discoveries')).on('keydown.enter', lambda: ui.navigate.to('/discoveries')).on('keydown.space', lambda: ui.navigate.to('/discoveries')):
    with ui.column().classes('w-full'):
        with ui.row().classes('w-full p-4 items-center gap-3').style(
            'background: linear-gradient(135deg, #ec4899, #be185d);'
        ):
            ui.icon('lightbulb').classes('text-3xl text-white')
            with ui.column().classes('gap-0'):
                h3(tr('Community'), classes='text-base font-bold text-white')
                ui.label(tr('Community discoveries, questions, and contributions')).classes('text-xs text-white/80')

        with ui.column().classes('p-4 gap-3'):
            ui.label(tr('View community discoveries, questions, and share your own findings')).classes('text-sm').style(
                'color: var(--text-secondary);'
            )
            with ui.row().classes('gap-2 flex-wrap'):
                ui.badge(tr('Discoveries')).props('outline color=pink-9').classes('text-xs')
                ui.badge(tr('Corrections')).props('outline color=pink-9').classes('text-xs')
```
This card block is naturally **CLS-safe** — fixed structure, no async
content fetch, no layout shift — exactly what D-16's teaser requires.
Fork this card verbatim into the grid, gated:
```python
from web.feature_flags import ATLAS_PREVIEW_ENABLED
...
if ATLAS_PREVIEW_ENABLED:
    with ui.card()...on('click', lambda: ui.navigate.to('/atlas')):
        ...
        ui.badge(tr('Beta')).props('outline color=...').classes('text-xs')
```
Keep the card **claim-free**: no counts, no "discoveries found," no dynamic
data — a static label + description + click-through only, per D-16 (mirrors
this Community Card's own static-only content, unlike the async `recent_container`
pattern elsewhere in the same file at line 487, which must NOT be the model
here).

---

### `.gitignore` (+ atlas static-asset dir entry) (config)

**Analog:** existing sidecar/gitignored-generated-artifact entries (source:
`.gitignore` lines 155-157, 205, verbatim, already committed):
```
/fjms_enrichment.db
/nli_crossref.db
/pgp.db
...
same_work_spike/
```
Add an entry for the baked atlas output directory (e.g.
`web/static/atlas/` or wherever the plan locates it), following this exact
convention — the generated, masking-sensitive, potentially multi-MB asset is
deployed via scp alongside code (like the other sidecar DBs), never
committed to git.

---

## Tests (grouped — all four follow an existing committed shape 1:1)

### `tests/test_atlas_bake.py`
Assert on the bake script's own output (node/edge/cluster counts match the
62,414-target post-Pitfall-#1-fix; no discovery-overlay fields present;
`assert brotli_size <= 6_000_000` byte-budget gate per RESEARCH.md Code
Examples). No single existing file is an exact analog (no prior "assert
counts on a generated binary artifact" test exists in this repo) — write it
as a plain pytest module with straightforward `assert` statements reading the
bake script's output files, mirroring the general "read fixture, assert
counts" style already used throughout `tests/test_*.py`.

### `tests/test_atlas_flag_gating.py`
**Analog:** `tests/test_web_library_options_no_local.py` (AST-scan-for-a-
required-guard-pattern shape, source: whole file read above) — write a
similar static check (or a lightweight `TestClient`/`httpx.ASGITransport`
smoke, matching `tests/test_no_raw_storage_access.py`'s file-scan style) that
asserts the `/atlas` route handler itself references
`ATLAS_PREVIEW_ENABLED` (not just the nav-list gate), closing the exact
access-control gap RESEARCH.md's Security Domain calls out.

### `tests/test_atlas_masking_scan.py`
**Analog:** `tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation`
(exact — see excerpt already given above under `check_atlas_masking.py`).

### `tests/render_smoke/test_atlas_render_smoke.py`
**Analog:** `tests/render_smoke/test_joins_lab_render_smoke.py` (exact —
source: lines 1-90 read above). Reuses the same `nicegui.testing.User` +
`httpx.ASGITransport(core.app)` harness (auto-tagged `render_smoke` marker via
`tests/conftest.py`'s path-based injection for anything under
`tests/render_smoke/`), with heavy seams (the baked asset load, any
DB-backed enrichment) mocked. Cover: flag ON+asset-present renders; flag
OFF/asset-absent clean-hides (no 500); EN/HE + RTL chrome; CLS (canvas
dimensions reserved before data loads); homepage teaser renders/click-throughs
to `/atlas`.

## Shared Patterns

### Feature-flag env idiom
**Source:** `web/feature_flags.py::_env_enabled` (lines 8-12)
**Apply to:** `ATLAS_PREVIEW_ENABLED` definition, and every gate site
(`/atlas` route body, nav item, homepage teaser card).
```python
def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
```

### Bilingual/RTL page chrome
**Source:** `web/main.py::_resolve_ui_language` (lines 851-859) +
`web/translations.py::tr`/`is_rtl` (imported the same way in every
`web/pages/*.py` module, e.g. `web/pages/home.py` line 16)
**Apply to:** `web/pages/atlas.py` chrome (badge, banner, intro text), the
`/atlas` route's `title=` string, and the homepage teaser card.

### Custom `Response` with headers (no `add_static_files`)
**Source:** `web/api.py` (lines 407-449, 1068-1091) — `Response(content=...,
media_type=..., headers={...})`
**Apply to:** the Brotli-serving atlas-data route exclusively (never route
the compressed payload through `app.add_static_files`, per RESEARCH.md
Pitfall 5).

### `noindex` SEO gate
**Source:** `web/main.py::page_meta(noindex=True)` (lines 752-765), used
already at `/lists`, `/settings`, `/corrections`, `/admin`, `/profile` (line
1909, 2007, 2039, 2071, 2085)
**Apply to:** `/atlas` page meta and the homepage teaser's link target, per
D-16.

### Dismissible-banner + auto-dismiss-without-premature-persist shape
**Source:** `web/pages/home.py` OCR disclaimer banner (lines 26-61) and
`web/main.py`'s What's New banner (lines ~1100-1126) — both use
`asyncio.get_event_loop().call_later(...)` (never `ui.timer`, which raises
`RuntimeError: parent_slot has been deleted` if the user navigates away —
see project memory `reference_nicegui_flex_height_css.md`/CLAUDE.md
"NiceGUI `ui.timer` in ephemeral containers") and only persist the
"dismissed" flag on an actual successful `.delete()`, not on a bare timer
fire.
**Apply to:** the standing D-15 honesty banner and "Beta / preview" badge if
made dismissible.

### Sensitive-value-from-env sourcing (never hardcode in a committed file)
**Source:** `web/puzzle_tokens.py` lines 16-18 (`PUZZLE_SECRET =
os.environ.get('PUZZLE_UPLOAD_SECRET', os.urandom(32).hex())`)
**Apply to:** `scripts/check_atlas_masking.py`'s restricted-pattern-list
sourcing (D-07 hard constraint — the pattern list must never be committed in
cleartext, including inside this script itself).

### Sidecar/gitignored-generated-artifact convention
**Source:** `.gitignore` lines 155-157 (`/fjms_enrichment.db`, `/nli_crossref.db`,
`/pgp.db`) and line 205 (`same_work_spike/`)
**Apply to:** the new baked atlas static-asset output directory.

## No Analog Found

| File / Concern | Role | Data Flow | Reason |
|---|---|---|---|
| Typed/delta-encoded binary payload format (nodes/edges struct-of-arrays + header) | data-encoding utility | transform | No existing committed code in this repo builds a custom binary asset format — the closest *conceptual* precedent is the prototype's JSON-in-`<script>` embedding (gitignored, being explicitly replaced per D-10/Pitfall 3), not a committed pattern. Follow RESEARCH.md's "Don't Hand-Roll" guidance (fixed header + `numpy.tobytes()`/`np.frombuffer`) rather than inventing further. |
| Client-side Canvas 2D renderer (zoom/pan/search/filter/focus/click-through/intro JS) | browser/client script | event-driven | No existing page in `web/` ships a hand-written Canvas 2D interactive renderer of this complexity (other pages use NiceGUI components or Fabric.js in `web/pages/puzzle.py`, a different rendering model). Port from the prototype's JS (described structurally in CONTEXT.md/RESEARCH.md, never quoted here) rather than searching for a closer committed analog. |
| Offline Louvain/force-layout/phyllotaxis graph math | build/offline-batch (algorithm) | batch | No existing `scripts/*.py` performs graph-community-detection or force-directed layout; this is genuinely new algorithmic territory for the committed codebase, sourced entirely from the (gitignored, described-only) prototype per RESEARCH.md "Don't Hand-Roll." |

## Metadata

**Analog search scope:** `web/main.py`, `web/api.py`, `web/feature_flags.py`,
`web/pages/home.py`, `web/pages/puzzle.py`, `web/puzzle_tokens.py`,
`scripts/*.py` (`check_docs.py`, `fgp_fill_credits_bilingual.py`,
`build_residue_patterns_artifact.py`), `tests/test_no_raw_storage_access.py`,
`tests/test_web_library_options_no_local.py`, `tests/test_no_back_edges_core.py`,
`tests/render_smoke/test_joins_lab_render_smoke.py`, `.gitignore`. The
gitignored prototype (`same_work_spike/probe/scripts/build_atlas_draft.py`)
was read for structural understanding only, per the phase's masking
constraint.
**Files scanned:** ~20 (committed) + 1 (gitignored, read-only, structure-only)
**Pattern extraction date:** 2026-07-20
