# Phase 133: Visual Atlas Preview (early quick win) - Pattern Map

**Mapped:** 2026-07-20
**Updated:** 2026-07-20 (Codex confirmation pass round 2 — three-pass scanner, shared `atlas_preview_available()` predicate on all four surfaces, exact-node-set test, current test paths)
**Files analyzed:** 9 new/modified committed files + test files
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
| `tests/atlas_bake/test_atlas_bake.py` (NEW) | test (unit) | batch (assert on bake output) | any `tests/test_*` asserting counts/assertions on a generated artifact — closest shape: `tests/test_no_back_edges_core.py`'s smoke-assertion style | role-match |
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
universe (Pitfall #1 in RESEARCH.md) and proving EXACT eligible==placed set
equality (missing==0, extra==0 — Codex HIGH-5), (c) re-encoding the payload as
struct-of-typed-arrays instead of `json.dumps` + `str.replace('__DATA__', ...)`
(sys_id is BigUint64-only, no fallback — the bake FAILS on any non-pure-digit/
>=2^64 sys_id, NEW LOW), and (d) a final `Brotli`-compress step writing both
`.bin` and `.bin.br` with a content-hashed filename.

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

**Output-location convention:** write the baked asset into a **gitignored**
served-data directory OUTSIDE the `/static` mount — repo-root `atlas_data/`
(per plans 133-01/133-03; NOT `web/static/atlas/`, which the `/static` mount
serves publicly and would let the asset bypass the ATLAS_PREVIEW_ENABLED flag,
Codex HIGH-1) — never a committed path — see the `.gitignore` Shared Pattern below. Bake-time-only dependencies
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
Replicate the issue-collection + exit-code shape, but `check_atlas_masking.py`
is deliberately STRONGER than `check_docs.py`'s single `rglob` pass:

- `scan_repo()` runs **THREE SEPARATE PASSES** (Codex HIGH-4), NOT a single
  `git ls-files`: (1) HEAD/index blobs — `git ls-tree -r HEAD` + staged
  `git diff --cached` blobs read via `git show` (catches a staged blob that
  differs from the worktree); (2) tracked worktree files — `git ls-files`
  (read bytes from disk); (3) non-ignored untracked candidates —
  `git ls-files --others --exclude-standard` (files about to be added). A hit
  on **any** surface is a failure.
- `scan_asset(path)` accepts a FILE **or a DIRECTORY** and RECURSIVELY walks
  the whole tree (every `.bin` / `.bin.br` / `manifest.json` / rendered-HTML
  file), matching each pattern as text, as its UTF-8 byte encoding, and in
  normalized (Unicode NFC/NFD + casefold) and common encoded/escaped (URL `%`,
  HTML-entity, JS `\uXXXX`) forms.
- Both return `issues: list`; `main()` aggregates and `return 0 if
  total_issues == 0 else 1`, with argparse flags `--scan-repo`,
  `--scan-asset <path>`, `--self-test`.
- **FAIL-SAFE:** exit 1 (never a false green) when `MASKING_SCAN_PATTERNS_FILE`
  is unset / missing / empty. **NEVER-ECHO:** report only relative file path,
  byte offset, and a pattern-INDEX — never the matched pattern text.

Per D-07/RESEARCH.md Security Domain, the pattern list itself must **never be
hardcoded** in this committed `.py` file — source it from a gitignored/env-var
location, following the exact existing sensitive-value idiom in this repo:

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
script itself. LOCALLY the file is the repo-root `<repo-root>/.masking_patterns`
(gitignored via the `/.masking_patterns` rule — NOT the filesystem root `/`);
in CI a `run:` step writes `${{ secrets.MASKING_SCAN_PATTERNS }}` to
`${GITHUB_WORKSPACE}/.masking_patterns` with `chmod 600`, runs the scan, and
deletes it in an `if: always()` step (Codex NEW MEDIUM — CI secret path).

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
file/string and assert `check_atlas_masking` flags it across each surface
(worktree file, UTF-8 bytes in a binary, encoded/escaped form) — proving the
scan is load-bearing, not a no-op — plus the fail-safe (no patterns → exit 1)
and never-echo (caught-fixture output does NOT contain the pattern) cases.

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
exposure). NOTE: the flag is only the DEFINITION. Every runtime GATE
(`/atlas` route body, nav item, homepage teaser card, and both `/atlas-data/*`
routes) uses the shared `web.atlas_assets.atlas_preview_available()` predicate
(= `ATLAS_PREVIEW_ENABLED AND asset loaded`), NOT the bare flag, so no surface
advertises the atlas when the asset is not actually loaded (Codex MEDIUM-6).

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
Replicate for `/atlas`: `@ui.page('/atlas', title='Connections Atlas | ... — Dicta Genizah Search')`,
`ui.add_head_html(page_meta('/atlas', ..., noindex=True))` (per D-16/Pitfall's
`noindex` requirement — see the `page_meta` excerpt below), gate the route
handler itself on `atlas_preview_available()` (the SHARED predicate — flag AND
loaded asset — RESEARCH.md Security Domain flags nav-only gating as a real
access-control gap, and MEDIUM-6 requires the same predicate as nav/data-route/
teaser), fall through to a clean "temporarily unavailable" card identical in
shape to the puzzle one when the predicate is False (flag OFF **or** asset not
loaded), and delegate the real chrome to `web/pages/atlas.py` only when True.

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
Use `noindex=True` on `/atlas` per D-16 ("set `noindex` until the REL-01 gate").
The homepage teaser lives on the indexed `/` but is claim-free (see the teaser
section); only `/atlas` itself is noindex.

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
Replicate this `Response(content=..., media_type=..., headers={...})` shape for
TWO routes (per RESEARCH.md Pattern 3), both gated on `atlas_preview_available()`
(404 when unavailable): (1) `/atlas-data/manifest.json` — the MUTABLE pointer,
served `Cache-Control: no-cache, must-revalidate` + an `ETag` + `Vary:
Accept-Encoding`, with `If-None-Match` → 304 (Codex NEW MEDIUM stale-manifest —
NOT immutable, so a rebake never strands a client on an old hash); (2)
`/atlas-data/{asset_name}` — the content-hashed asset, `asset_name`
whitelist-compared to the loaded bin name (no filesystem use of user input —
path-traversal mitigation), with `_negotiate_encoding(header, have_br,
have_plain)` parsing q-values for `br`, `identity`, AND the wildcard `*`
(honoring `br;q=0` / `identity;q=0` / `*;q=0`): br when acceptable+present
(`Content-Encoding: br`), plain when identity acceptable+present, else a
REACHABLE 406 — never an invalid 200; the asset uses far-future immutable
`Cache-Control` (safe because content-hashed). **Do not** serve the payload
through `add_static_files` (confirmed in RESEARCH.md Pitfall 5 that
`CacheControlledStaticFiles` does not set `Content-Encoding`), and the asset
must NOT live under `web/static/` (Codex HIGH-1).

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
link**, but gate it on the SHARED PREDICATE, not the bare flag (Codex MEDIUM-6):
```python
if atlas_preview_available():
    nav_items.append(('/atlas', 'hub', tr('Connections Atlas'), tr('Beta')))
```
Import `atlas_preview_available` from `web.atlas_assets` (= flag AND loaded
asset, so a missing asset never advertises a broken nav link);
`ATLAS_PREVIEW_ENABLED` itself is imported from `web.feature_flags` alongside
`WEB_PUZZLE_ENABLED` (source: line 688) only for the flag definition — the nav
GATE uses the predicate.

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
every string and layout direction, a "Beta / preview" badge + the standing
D-15 honesty banner, and a CLS-reserved fixed-dimension canvas container. The
Canvas-2D renderer + typed-array decode JS itself has **no committed analog**
(see "No Analog Found") — port it from the prototype's existing JS (described
structurally, not quoted), fetching the binary payload from the new data route
instead of an inlined `<script>` (Pitfall 3 — avoids the `</script>`-breakout
injection class), decoding sys_id as BigUint64-only (no fallback).

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
Fork this card verbatim into the grid, gated on the SHARED PREDICATE (the
FOURTH availability surface — Codex MEDIUM-6, NOT the bare flag):
```python
from web.atlas_assets import atlas_preview_available
...
if atlas_preview_available():
    with ui.card()...on('click', lambda: ui.navigate.to('/atlas')):
        ...
        ui.badge(tr('Beta')).props('outline color=...').classes('text-xs')
```
Keep the card **claim-free**: no counts, no "discoveries found," no dynamic
data — a static label + description + click-through only, per D-16 (mirrors
this Community Card's own static-only content, unlike the async `recent_container`
pattern elsewhere in the same file at line 487, which must NOT be the model
here). The teaser's HE strings are the keys pre-registered by plan 133-03.

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
Add TWO entries following this exact convention: (a) `/.masking_patterns` (the
repo-root-anchored gitignored pattern file — NOT the filesystem root), and (b)
`/atlas_data/` (the baked atlas output directory, OUTSIDE `web/static/` per
Codex HIGH-1) — the generated, masking-sensitive, multi-MB asset is deployed
via scp alongside code (like the other sidecar DBs), never committed to git.

---

## Tests (grouped — all follow an existing committed shape 1:1)

Current atlas test paths (executors: use these EXACT paths):
`tests/test_atlas_masking_scan.py`, `tests/atlas_bake/test_atlas_bake.py`,
`tests/atlas_bake/test_atlas_golden_js.py`, `tests/test_atlas_flag_gating.py`,
`tests/render_smoke/test_atlas_render_smoke.py`,
`tests/render_smoke/test_home_teaser_render_smoke.py`,
`tests/render_smoke/test_atlas_four_surface.py`.

### `tests/atlas_bake/test_atlas_bake.py`
Assert on the bake script's own `--smoke`/`--golden` output (no research DB):
**EXACT node-set equality** — the placed node-id set EQUALS the eligible
connected-endpoint set (missing==0, extra==0; the live DB currently yields
~62,645 and 62,414 is only a historical regression FLOOR — Codex HIGH-5, NOT a
`>=` count target); no discovery-overlay fields (D-04); `assert brotli_size <=
6_000_000` byte-budget gate; sys_id BigUint64 round-trip AND a non-pure-digit/
>=2^64 sys_id FAILS the bake (no fallback — NEW LOW); determinism; content-hash-
changes-on-byte-change; and a golden per-field Python decode against
`tests/fixtures/atlas/golden-v1-expected.json` (sys_id as a decimal STRING,
compared via `int(str)`). Lives under `tests/atlas_bake/` (auto-tagged
`atlas_bake` via conftest, run in the dedicated pinned `atlas-bake-tests` CI
job with Node available for the cross-language `test_atlas_golden_js.py`).

### `tests/test_atlas_flag_gating.py`
**Analog:** `tests/test_web_library_options_no_local.py` (AST-scan-for-a-
required-guard-pattern shape) + a lightweight `httpx.ASGITransport` behavioral
smoke (matching `tests/test_no_raw_storage_access.py`'s file-scan style). Assert
the `/atlas` route handler, the nav-append, and BOTH data routes reference the
SHARED `atlas_preview_available()` predicate (flag AND loaded asset — not the
bare flag; three of the four MEDIUM-6 surfaces, the teaser being 133-05 and the
behavioral four-surface test being `tests/render_smoke/test_atlas_four_surface.py`
in 133-06); the three states (flag-OFF clean-hide, flag-ON/asset-not-loaded
clean-hide + data-route 404, ready); the response-level br/identity/* q-value
negotiation with a REACHABLE 406; the manifest no-cache + ETag + 304 + a
stale-manifest transition; HE translated values; and a flag-independent
`/static/atlas/*` → 404 regression guard (Codex HIGH-1 structural fix).

### `tests/test_atlas_masking_scan.py`
**Analog:** `tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation`
(exact — see excerpt above under `check_atlas_masking.py`). Inject a fabricated
known-bad pattern across each surface (worktree/bytes/encoded), assert exit 1;
clean content → exit 0; no patterns → exit 1 (fail-safe); caught-fixture output
never echoes the pattern (never-echo).

### `tests/render_smoke/test_atlas_render_smoke.py` + `test_home_teaser_render_smoke.py` + `test_atlas_four_surface.py`
**Analog:** `tests/render_smoke/test_joins_lab_render_smoke.py` (exact). Reuse the
`nicegui.testing.User` + `httpx.ASGITransport(core.app)` harness (auto-tagged
`render_smoke` via `tests/conftest.py`'s path-based injection for anything under
`tests/render_smoke/`), with heavy seams (the baked asset load, DB-backed
enrichment) mocked. `test_atlas_render_smoke.py` covers ONLY server render
(chrome/CLS/EN-HE-RTL/decoder injection — NOT fetch/decode/interactions, which
are Node-golden + manual UAT, MEDIUM-2). `test_home_teaser_render_smoke.py`
covers the teaser present/absent across ready/OFF/asset-missing.
`test_atlas_four_surface.py` (133-06) is the PARAMETRIZED four-surface
(page/data/nav/teaser) × three-state (OFF/asset-missing/ready) behavioral test
that covers nav + teaser behaviorally (Codex MEDIUM-6).

## Shared Patterns

### Feature-flag env idiom
**Source:** `web/feature_flags.py::_env_enabled` (lines 8-12)
**Apply to:** the `ATLAS_PREVIEW_ENABLED` DEFINITION only. Every runtime GATE
site (`/atlas` route body, nav item, homepage teaser card, AND both
`/atlas-data/*` routes) uses the shared `web.atlas_assets.atlas_preview_available()`
predicate (= flag AND loaded asset), NOT the bare flag — so no surface
advertises the atlas when the asset is not loaded (Codex MEDIUM-6).
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
**Apply to:** the atlas-data routes exclusively — the immutable content-hashed
asset (Brotli via `_negotiate_encoding`) and the no-cache + ETag manifest
(never route either through `app.add_static_files`, per RESEARCH.md Pitfall 5).

### `noindex` SEO gate
**Source:** `web/main.py::page_meta(noindex=True)` (lines 752-765), used
already at `/lists`, `/settings`, `/corrections`, `/admin`, `/profile` (line
1909, 2007, 2039, 2071, 2085)
**Apply to:** the `/atlas` page meta per D-16 (the homepage teaser stays on the
indexed `/` but is claim-free).

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
cleartext, including inside this script itself). Local file = repo-root
`/.masking_patterns`; CI = `${GITHUB_WORKSPACE}/.masking_patterns` (chmod 600,
delete-after).

### Sidecar/gitignored-generated-artifact convention
**Source:** `.gitignore` lines 155-157 (`/fjms_enrichment.db`, `/nli_crossref.db`,
`/pgp.db`) and line 205 (`same_work_spike/`)
**Apply to:** the new baked atlas static-asset output directory (`/atlas_data/`)
and the gitignored pattern file (`/.masking_patterns`).

## No Analog Found

| File / Concern | Role | Data Flow | Reason |
|---|---|---|---|
| Typed/delta-encoded binary payload format (nodes/edges struct-of-arrays + header) | data-encoding utility | transform | No existing committed code in this repo builds a custom binary asset format — the closest *conceptual* precedent is the prototype's JSON-in-`<script>` embedding (gitignored, being explicitly replaced per D-10/Pitfall 3), not a committed pattern. Follow RESEARCH.md's "Don't Hand-Roll" guidance (fixed header + `numpy.tobytes()`/`np.frombuffer`) rather than inventing further. sys_id is BigUint64-only (no fallback). |
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
**Pattern extraction date:** 2026-07-20 (updated 2026-07-20 — Codex confirmation pass round 2)
