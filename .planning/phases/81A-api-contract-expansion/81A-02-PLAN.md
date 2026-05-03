---
phase: 81A-api-contract-expansion
plan: 02
type: execute
wave: 2
depends_on:
  - 81A-01
files_modified:
  - shared/search_serializer.py
  - web/search_api.py
  - genizah_core.py
autonomous: true
requirements:
  - API-EXPAND-06
  - API-EXPAND-07
  - API-EXPAND-08
requirements_addressed:
  - API-EXPAND-06
  - API-EXPAND-07
  - API-EXPAND-08
tags:
  - api
  - serializer
  - envelope
must_haves:
  truths:
    - "Every /api/search 200 response envelope contains a top-level `request` block."
    - "The /api/search `request` block contains keys: `search_mode`, `responsa_options`, `responsa_options_effective`, `gap`, `limit`, `limit_effective`, `filters`."
    - "When client sends `search_mode='responsa'` with `responsa_options.ja=true` and the cascade disables JA, `responsa_options.ja` stays true in the echo, `responsa_options_effective.ja` is false, and `warnings[]` contains the existing tr() string for JA disabled."
    - "When client sends a non-responsa mode, both `responsa_options` and `responsa_options_effective` in the echo are null."
    - "Every /api/parallels 200 response envelope contains a top-level `request` block with keys: `mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective`, `filters` (NO `search_mode`, NO `responsa_options`, NO `gap` — ParallelsRequest at `web/search_api.py:163-188` does not accept `gap`, per D-07). [API-EXPAND-07]"
    - "The echo's `search_mode` is ALWAYS identical to what the client sent (D-04 — never silently downgraded). Cascade is exposed via `responsa_options_effective` + `warnings[]` only."
    - "`SearchEngine.execute_search` entry drains BOTH thread-local channels (string + meta) symmetrically, matching the legacy drain pattern at `genizah_core.py:7254`."
  artifacts:
    - path: "shared/search_serializer.py"
      provides: "_apply_request_echo helper OR inline echo construction in serialize_search_payload + serialize_parallels_payload"
      contains: "request"
    - path: "genizah_core.py"
      provides: "Thread-local channel extension carrying structured cascade meta dict alongside the message + symmetric entry-drain at execute_search start"
      contains: "_LAST_RESPONSA_DOWNGRADE_META"
  key_links:
    - from: "web/search_api.py search_endpoint"
      to: "shared/search_serializer.py serialize_search_payload"
      via: "new keyword args carrying request echo data (search_mode, responsa_options dict, responsa_options_effective dict, limit, limit_effective)"
      pattern: "serialize_search_payload"
    - from: "genizah_core.py Responsa cascade decision site (line 7657-7658)"
      to: "web/search_api.py search_endpoint"
      via: "_consume_last_responsa_downgrade_meta() returns the structured {variants, ja, flex_spacing, bidirectional} dict"
      pattern: "_consume_last_responsa_downgrade_meta"
---

<objective>
Add a `request` echo block to BOTH `/api/search` AND `/api/parallels` response envelopes. The echo block lets the v7.10 skill (81B) detect what the server actually applied — particularly when the Responsa cascade silently disabled options. Per D-04, `search_mode` in the echo is always identical to the client's input; cascade is exposed via `responsa_options_effective` + the existing `tr()` strings in `warnings[]`.

This plan implements the substantive `/api/parallels` request echo block work (API-EXPAND-07). Plan 04 verifies presence; Plan 02 builds it. **Note: ParallelsRequest at `web/search_api.py:163-188` does NOT have a `gap` field — the parallels echo's 6 keys are `mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective`, `filters` (no `gap`).**

Purpose: This is the contract that 81B's skill keys its "did the server downgrade me?" detection on. AC2 of 81A specifies all 5 search_mode values appear in echoes; AC6 specifies the responsa_options vs responsa_options_effective divergence on cascade.

Output: `shared/search_serializer.py` accepts new keyword args and embeds the `request` block. `web/search_api.py` passes the new args. `genizah_core.py` extends the thread-local downgrade signal with a structured dict so `responsa_options_effective` reflects exactly which cascade tier fired (per-flag booleans), not a parsed string. **Additionally**: `SearchEngine.execute_search` entry (`genizah_core.py:7254`) drains BOTH the legacy string channel and the new meta channel symmetrically, preventing stale meta from a direct core caller leaking into a later web request (Codex MEDIUM-2 from 81A-REVIEWS.md).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@CLAUDE.md
@.planning/STATE.md
@.planning/phases/81A-api-contract-expansion/81A-CONTEXT.md
@.planning/phases/81A-api-contract-expansion/81A-01-SUMMARY.md
@.planning/phases/81B-claude-skill-consumer/81-RESCOPE.md

<interfaces>
<!-- Existing relevant exports the executor will use. Variable names PINNED at revision time. -->

From shared/search_serializer.py:
```python
def serialize_search_payload(
    results: list[dict],
    *,
    meta_mgr: Any,
    query: str = '',
    mode: str = 'text',           # ← legacy; KEEP this top-level field for back-compat
    gap: Optional[int] = None,
    filters: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
    total: Optional[int] = None,
) -> dict
# Returns dict with keys: schema_version, source, query, mode, gap, filters,
# count, total, warnings, generated_at, results

def serialize_parallels_payload(
    main_results: list[dict],
    filtered_results: Optional[list[dict]] = None,
    *,
    meta_mgr: Any,
    source_text: str = '',
    chunk_size: Optional[int] = 5,
    mode: str = 'exact',
    max_freq: Optional[float] = None,
    boundary_options: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
) -> dict
# Returns dict with keys: schema_version, source, source_text, chunk_size,
# mode, max_freq, boundary_options, count, total, warnings, generated_at,
# results, filtered
```

From genizah_core.py (existing thread-local signal at lines 65-91; verified at revision time):
```python
_LAST_RESPONSA_DOWNGRADE = threading.local()  # carries str message only

def _set_last_responsa_downgrade(message: str) -> None: ...   # line 68
def _consume_last_responsa_downgrade() -> Optional[str]: ...  # line 77
```

**Legacy entry-drain at `genizah_core.py:7254` (PINNED — verified at revision time):**
```python
# genizah_core.py:7249 — execute_search signature
def execute_search(self, query_str, mode, gap, progress_callback=None,
                   exclude_words=None, responsa_options=None,
                   restrict_sys_ids: set = None, text_position: str = None):
    ...
    # Line 7254 — legacy entry-drain (drains stale string state from prior caller):
    _consume_last_responsa_downgrade()
```
Plan 02 Task 1 Step E adds a parallel meta-drain on the SAME line (or immediately after) so both channels are cleared symmetrically at every entry to `execute_search`. This prevents direct-core callers from leaving stale meta that the next web request would read (Codex MEDIUM-2).

**PINNED variable names from genizah_core.py — verified at revision time (no executor exploration required):**

The Responsa pipeline begins at `execute_search` (line 7249) and reads input flags into local variables at lines 7295-7299:
```python
# genizah_core.py:7295-7299 (inside execute_search, the Responsa branch):
variants_on   = responsa_options.get('variants', False)
ja_on         = responsa_options.get('ja', False)
flex_spacing  = responsa_options.get('flex_spacing', False)
bidirectional = responsa_options.get('bidirectional', False)
variant_mode  = responsa_options.get('variant_mode', 'exact')
```

The cascade is invoked at lines 7323-7334 (already in `execute_search`'s scope — local names confirmed):
```python
# genizah_core.py:7323-7334:
_components, guard_warning, actual_opts = _apply_explosion_guard(
    components,
    variants_on=variants_on,
    ja_on=ja_on,
    var_mgr=self.var_mgr,
    variant_mode=variant_mode,
)
if guard_warning:
    responsa_warning = f"{responsa_warning}; {guard_warning}" if responsa_warning else guard_warning
    variants_on  = actual_opts['variants_on']     # ← cascade-mutated value
    ja_on        = actual_opts['ja_on']            # ← cascade-mutated value
    variant_mode = actual_opts['variant_mode']
```

After the Responsa pipeline runs and dedup completes, the thread-local downgrade SET site is at lines 7657-7660:
```python
# genizah_core.py:7657-7660 (PINNED — the canonical set site):
if responsa_warning:
    _set_last_responsa_downgrade(responsa_warning)
if responsa_warning and deduped:
    deduped[0]['responsa_warning'] = responsa_warning
```

**At line 7657-7658 the in-scope local variables are:** `responsa_warning` (str|None), `responsa_options` (dict|None — the input arg), `variants_on` (bool — cascade-mutated), `ja_on` (bool — cascade-mutated), `flex_spacing` (bool — pass-through from input), `bidirectional` (bool — pass-through from input). Plan 02 Task 1's structured-meta call site uses these EXACT names — no executor exploration required.

The cascade tr() strings (genizah_core.py:6024, 6036, 6048, 6062, 6076, 6090) — only the first three are exposed in 81A:
- "Variant mode downgraded to basic (30 pairs)"  → variants tier change (variant_mode flips to 'variants', variants_on stays True)
- "Spelling variants disabled"                    → variants_on becomes False
- "Judeo-Arabic expansion disabled"               → ja_on becomes False
- "Plene/defective expansion disabled"            → not exposed in API (deferred)
- "Grammatical suffix expansion disabled"         → not exposed
- "Grammatical prefix expansion disabled"         → not exposed

**Note on flex_spacing / bidirectional pass-through (Codex MEDIUM-1 follow-up):** In 81A scope, the cascade does NOT touch `flex_spacing` or `bidirectional` — they are pass-through-only. Plan 05 Section 2 tests assert that toggling these flags reaches the engine via the `responsa_options` dict (handler-level pass-through), which is sufficient for AC3. The variants and ja flags ARE cascade-mutated (the cascade can disable them), so Plan 05 Section 2 tests for those two assert the meta channel reflects the post-cascade state.

From web/search_api.py search_endpoint (post-Plan-01):
```python
# After execute_search returns (line 522):
downgrade_msg = _consume_last_responsa_downgrade()  # str | None
warnings_list: list = []
if downgrade_msg:
    warnings_list.append(f'query_downgraded: {downgrade_msg}')
```

Existing finally-block defensive drain at lines 598-602:
```python
finally:
    ...
    try:
        _consume_last_responsa_downgrade()
    except Exception:
        logger.warning('thread-local downgrade drain failed in finally')
```

From web/search_api.py:163-188 (verified at revision time — ParallelsRequest schema):
```python
class ParallelsRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    composition: str
    chunk_size: int = 5
    max_freq: Optional[float] = None
    boundary_options: Optional[BoundaryOptionsModel] = None
    mode: Literal['exact', 'variants', 'fuzzy'] = 'exact'
    filters: Optional[FiltersModel] = None
    # NOTE: ParallelsRequest does NOT have a `gap` field. The parallels echo
    # therefore has 6 keys (no gap).
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Extend genizah_core thread-local channel with structured cascade-meta dict + symmetric entry-drain</name>
  <files>genizah_core.py</files>
  <read_first>
    - genizah_core.py lines 60-95 (existing _LAST_RESPONSA_DOWNGRADE thread-local — pinned: `_set_last_responsa_downgrade` line 68, `_consume_last_responsa_downgrade` line 77)
    - genizah_core.py lines 7249-7340 (Responsa pipeline entry + cascade invocation; pinned local names: `variants_on`, `ja_on`, `flex_spacing`, `bidirectional`, `variant_mode`, `responsa_options`, `actual_opts`, `responsa_warning`)
    - genizah_core.py line 7254 (PINNED — legacy entry-drain `_consume_last_responsa_downgrade()` at top of execute_search)
    - genizah_core.py lines 7655-7665 (the canonical set site — line 7657-7658 — where `_set_last_responsa_downgrade(responsa_warning)` fires)
    - genizah_core.py lines 5965-6100 (`_apply_explosion_guard` — confirms `actual_opts` shape `{'variants_on', 'ja_on', 'variant_mode'}`)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (response envelope shape — see "Validation matrix" + "Response envelope (final)" sections)
    - .planning/phases/81A-api-contract-expansion/81A-REVIEWS.md (MEDIUM-2 — entry-drain symmetry)
  </read_first>
  <behavior>
    - On a Responsa search where cascade disables JA only: after execute_search returns, `_consume_last_responsa_downgrade_meta()` returns a dict like `{'variants': True, 'ja': False, 'flex_spacing': True, 'bidirectional': True}` reflecting which of the user's requested flags survived. Subsequent calls return None.
    - On a Responsa search with no cascade (under-limit): `_consume_last_responsa_downgrade_meta()` returns None (no downgrade occurred).
    - On a non-Responsa search: same — returns None.
    - The existing `_consume_last_responsa_downgrade()` (string variant) still works unchanged for back-compat.
    - Both consume calls drain the thread-local independently (read-and-clear semantics preserved).
    - **Entry-drain symmetry (Codex MEDIUM-2):** A test that pre-seeds `_LAST_RESPONSA_DOWNGRADE_META.value = {...}` BEFORE invoking `execute_search` (simulating a stale direct-core caller) sees the value cleared on entry — `_consume_last_responsa_downgrade_meta()` AFTER an unrelated non-Responsa `execute_search` call returns None.
  </behavior>
  <action>
    Add a parallel thread-local + setter + consumer alongside the existing string channel, plus a symmetric entry-drain.

    **Step A.** Around line 65 (right after `_LAST_RESPONSA_DOWNGRADE = threading.local()`), add:

    ```python
    # Phase 81A — structured per-flag cascade outcome carried alongside the
    # legacy string message. Skill consumer (81B) reads this to populate
    # responsa_options_effective in the /api/search envelope echo.
    _LAST_RESPONSA_DOWNGRADE_META = threading.local()
    ```

    **Step B.** After the existing `_set_last_responsa_downgrade` and `_consume_last_responsa_downgrade` functions (after line 91), add:

    ```python
    def _set_last_responsa_downgrade_meta(meta: dict) -> None:
        """Phase 81A — record a structured per-flag cascade outcome.

        `meta` is a dict with the four ResponsaOptions field names as keys and
        booleans indicating whether each was applied (True) or cascade-disabled
        (False). The skill consumer compares this to the request's
        responsa_options to detect server-side downgrades.
        """
        _LAST_RESPONSA_DOWNGRADE_META.value = meta


    def _consume_last_responsa_downgrade_meta() -> Optional[dict]:
        """Phase 81A — read-and-clear the structured cascade outcome.

        Returns None when no downgrade occurred OR when already consumed
        on the current thread.
        """
        meta = getattr(_LAST_RESPONSA_DOWNGRADE_META, 'value', None)
        if meta is not None:
            try:
                del _LAST_RESPONSA_DOWNGRADE_META.value
            except AttributeError:
                pass
        return meta
    ```

    **Step C — set the structured meta at the canonical cascade-decision site.** The set site is `genizah_core.py:7657-7658` (PINNED at revision time):

    Find:
    ```python
    if responsa_warning:
        _set_last_responsa_downgrade(responsa_warning)
    ```

    Replace with:

    ```python
    if responsa_warning:
        _set_last_responsa_downgrade(responsa_warning)
        # Phase 81A — structured per-flag effective state alongside the
        # legacy string channel. The local variables `variants_on`, `ja_on`,
        # `flex_spacing`, `bidirectional` are bound at lines 7295-7298 from
        # the input responsa_options dict and are mutated by the cascade at
        # lines 7332-7334 (variants_on, ja_on; variant_mode is internal-only).
        # flex_spacing and bidirectional are pass-through (the cascade does
        # not touch them in 81A scope; deferred tiers 4-6 affect plene/
        # suffix/prefix instead).
        _set_last_responsa_downgrade_meta({
            'variants':      bool(variants_on),
            'ja':            bool(ja_on),
            'flex_spacing':  bool(flex_spacing),
            'bidirectional': bool(bidirectional),
        })
    ```

    **Verify the binding names are in scope at line 7657:** Re-read lines 7250-7660 sequentially. Confirm that `variants_on`, `ja_on`, `flex_spacing`, `bidirectional` are all in the same function (`execute_search`) scope at the point where `_set_last_responsa_downgrade(responsa_warning)` fires. They are bound at 7295-7298 inside the `if responsa_options and responsa_options.get('responsa_mode'):` block, and the line-7657 set site is reached via the same code path. If any of the four variables is shadowed or out-of-scope at line 7657 (e.g., because the line-break-syntax early return at 7284-7292 takes a different path), the executor MUST replicate the same set call inside that branch too — read the file to confirm. The expected execution path: lines 7270 → 7295-7298 (bind) → 7322-7334 (cascade) → 7657 (set site), all within `execute_search`.

    **Step D.** Verify import of `Optional` and `threading` is already at the top of `genizah_core.py` — both are. No import change needed.

    **Step E — Symmetric entry-drain (Codex MEDIUM-2).** At `genizah_core.py:7254` (verified at revision time — the legacy `_consume_last_responsa_downgrade()` drain at the top of `execute_search`), add a parallel meta-drain so both channels are cleared on every entry to `execute_search`. This prevents a direct-core caller (e.g., desktop or a unit test) from leaving a stale meta value that a subsequent web request would read.

    Find the existing line at 7254:
    ```python
    _consume_last_responsa_downgrade()
    ```

    Replace with:
    ```python
    # Phase 78 — drain the legacy string channel from any prior caller.
    _consume_last_responsa_downgrade()
    # Phase 81A (Codex MEDIUM-2) — drain the structured-meta channel
    # symmetrically so a direct-core caller cannot leave a stale meta dict
    # that a later web request would read as "the cascade fired."
    _consume_last_responsa_downgrade_meta()
    ```

    **Step F — Confirm Task 1 acceptance:** After the edit, the first ~20 lines of `execute_search` (starting at line 7249) must contain BOTH consume calls. Verify with a grep that returns ≥2 occurrences of `_consume_last_responsa_downgrade` in the 7249–7270 line range (one with the `_meta` suffix).
  </action>
  <verify>
    <automated>python -m py_compile genizah_core.py</automated>
    <automated>python -c "from genizah_core import _set_last_responsa_downgrade_meta, _consume_last_responsa_downgrade_meta; _set_last_responsa_downgrade_meta({'variants':True,'ja':False,'flex_spacing':False,'bidirectional':False}); m = _consume_last_responsa_downgrade_meta(); assert m == {'variants':True,'ja':False,'flex_spacing':False,'bidirectional':False}; assert _consume_last_responsa_downgrade_meta() is None; print('OK')"</automated>
    <automated>grep -q "_LAST_RESPONSA_DOWNGRADE_META" genizah_core.py && echo found-meta-tlocal || (echo missing-meta-tlocal; exit 1)</automated>
    <automated>grep -q "_set_last_responsa_downgrade_meta" genizah_core.py && echo found-setter || (echo missing-setter; exit 1)</automated>
    <automated>grep -q "_consume_last_responsa_downgrade_meta" genizah_core.py && echo found-consumer || (echo missing-consumer; exit 1)</automated>
    <automated>python -c "import re; src = open('genizah_core.py').read().splitlines(); window = '\n'.join(src[7248:7270]); n_string = window.count('_consume_last_responsa_downgrade()'); n_meta = window.count('_consume_last_responsa_downgrade_meta()'); assert n_string >= 1 and n_meta >= 1, f'expected entry-drain for both channels in execute_search prelude; got string={n_string} meta={n_meta}'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `genizah_core.py` exports both `_set_last_responsa_downgrade_meta` and `_consume_last_responsa_downgrade_meta`.
    - The thread-local `_LAST_RESPONSA_DOWNGRADE_META` is defined separately from `_LAST_RESPONSA_DOWNGRADE`.
    - The setter is called at line ~7658 (the same code path as `_set_last_responsa_downgrade(responsa_warning)`); both calls fire together.
    - The setter receives the in-scope locals `variants_on`, `ja_on`, `flex_spacing`, `bidirectional` (PINNED variable names — no executor exploration required).
    - **Entry-drain symmetry (Codex MEDIUM-2):** `SearchEngine.execute_search` at line 7254 (or immediately following) contains BOTH `_consume_last_responsa_downgrade()` AND `_consume_last_responsa_downgrade_meta()` calls. Verified by the python check that scans lines 7249–7270 for both call shapes.
    - REPL round-trip test (set → consume → consume) passes.
    - The legacy string consumer continues to work; existing Phase 78 tests for downgrade warnings are not broken (Plan 04 verifies this end-to-end).
  </acceptance_criteria>
  <done>
    Structured per-flag cascade outcome is now available to `web/search_api.py` via a thread-local channel that mirrors the existing string-message channel. Both channels are drained symmetrically at `execute_search` entry, eliminating the cross-caller stale-meta leak (Codex MEDIUM-2).
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Add `request` echo block to /api/search and /api/parallels envelopes</name>
  <files>shared/search_serializer.py, web/search_api.py</files>
  <read_first>
    - shared/search_serializer.py (read fully — focus on serialize_search_payload at line 357 and serialize_parallels_payload at line 791)
    - web/search_api.py (post-Plan-01) — focus on search_endpoint serialize call at line 545 and parallels_endpoint serialize call at line ~839
    - web/search_api.py:163-188 (ParallelsRequest schema — confirms NO `gap` field; the parallels echo is 6 keys not 7)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (Response envelope (final) — copy the JSON shape verbatim)
    - .planning/phases/81B-claude-skill-consumer/81-RESCOPE.md §3.5
  </read_first>
  <behavior>
    - serialize_search_payload returns a dict whose top-level `request` key is a dict with EXACTLY: `search_mode`, `responsa_options`, `responsa_options_effective`, `gap`, `limit`, `limit_effective`, `filters`.
    - For `search_mode='exact'` with no responsa_options: `request.responsa_options is None` AND `request.responsa_options_effective is None`.
    - For `search_mode='responsa'` with all-True responsa_options and no cascade: `request.responsa_options == {variants:T, ja:T, flex_spacing:T, bidirectional:T}` AND `request.responsa_options_effective` equals it (no divergence).
    - For `search_mode='responsa'` with all-True responsa_options and cascade disabling JA: `request.responsa_options.ja == True`, `request.responsa_options_effective.ja == False`, `warnings[]` contains the JA tr() string.
    - serialize_parallels_payload returns a dict whose top-level `request` key has EXACTLY 6 keys: `mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective` (= total before truncation, or 200 when truncated_to_200), `filters` (when filters were applied; else null). NO `search_mode`, NO `responsa_options`, NO `gap` (ParallelsRequest does not accept gap).
    - Existing top-level fields (`query`, `mode`, `gap`, `filters`, `count`, `total`, `warnings`, `generated_at`, `results`, `filtered`, `source`, `schema_version`, `source_text`) are preserved unchanged.
    - For /api/parallels echo, `mode` field name is preserved (D-07) — NOT renamed to `search_mode`.
  </behavior>
  <action>
    **Step A — Extend serialize_search_payload signature.** In `shared/search_serializer.py`, modify `serialize_search_payload` (around line 357) to accept new keyword-only args:

    ```python
    def serialize_search_payload(
        results: list[dict],
        *,
        meta_mgr: Any,
        query: str = '',
        mode: str = 'text',
        gap: Optional[int] = None,
        filters: Optional[dict] = None,
        warnings: Optional[list[str]] = None,
        total: Optional[int] = None,
        # Phase 81A additions:
        request_echo: Optional[dict] = None,
    ) -> dict:
    ```

    Inside the function, just before the final `return {...}` statement, change to a build-then-augment-then-return pattern:

    ```python
    envelope = {
        'schema_version': SCHEMA_VERSION,
        'source': 'search',
        'query': query or '',
        'mode': mode or 'text',
        'gap': gap,
        'filters': filters,
        'count': len(items),
        'total': total if total is not None else len(items),
        'warnings': list(warnings) if warnings else [],
        'generated_at': _utc_iso_now(),
        'results': items,
    }
    if request_echo is not None:
        envelope['request'] = request_echo
    return envelope
    ```

    **Step B — Extend serialize_parallels_payload signature.** Same pattern at line 791:

    ```python
    def serialize_parallels_payload(
        main_results: list[dict],
        filtered_results: Optional[list[dict]] = None,
        *,
        meta_mgr: Any,
        source_text: str = '',
        chunk_size: Optional[int] = 5,
        mode: str = 'exact',
        max_freq: Optional[float] = None,
        boundary_options: Optional[dict] = None,
        warnings: Optional[list[str]] = None,
        # Phase 81A addition:
        request_echo: Optional[dict] = None,
    ) -> dict:
    ```

    And inside the function, change the final `return {...}` to:

    ```python
    envelope = {
        'schema_version': SCHEMA_VERSION,
        'source': 'parallels',
        'source_text': source_text or '',
        'chunk_size': chunk_size,
        'mode': mode or 'exact',
        'max_freq': max_freq,
        'boundary_options': boundary_options,
        'count': len(main_envelope),
        'total': len(main_envelope),
        'warnings': list(warnings) if warnings else [],
        'generated_at': _utc_iso_now(),
        'results': main_envelope,
        'filtered': filt_envelope,
    }
    if request_echo is not None:
        envelope['request'] = request_echo
    return envelope
    ```

    **Step C — Build `request_echo` in /api/search handler.** In `web/search_api.py`, in `search_endpoint` (around line 544 where `serialize_search_payload` is called), build the echo dict from the validated request and the consumed cascade meta. Just before the call:

    ```python
    # 81A D-04/D-05/D-06 — build request echo for the response envelope.
    # responsa_options is null for non-Responsa modes; responsa_options_effective
    # reflects the cascade outcome (all-True input + cascade disable JA → ja=False).
    from genizah_core import _consume_last_responsa_downgrade_meta
    cascade_meta = _consume_last_responsa_downgrade_meta()  # dict|None

    if req.search_mode == 'responsa':
        opts_dict = (req.responsa_options or ResponsaOptions()).model_dump()
        if cascade_meta is not None:
            effective_dict = cascade_meta  # already shaped {variants, ja, flex_spacing, bidirectional}
        else:
            # No cascade fired — effective == requested.
            effective_dict = dict(opts_dict)
    else:
        opts_dict = None
        effective_dict = None

    request_echo = {
        'search_mode': req.search_mode,
        'responsa_options': opts_dict,
        'responsa_options_effective': effective_dict,
        'gap': req.gap,
        'limit': req.limit,
        'limit_effective': min(req.limit, MAX_LIMIT),
        'filters': filters_dict,  # already a dict-or-None from earlier
    }
    ```

    Then update the `serialize_search_payload(...)` call to pass `request_echo=request_echo`.

    **IMPORTANT — interaction with `_consume_last_responsa_downgrade()`.** Plan 01 already calls this earlier in the handler (around line 522) to populate `warnings_list`. The new `_consume_last_responsa_downgrade_meta()` is a SEPARATE thread-local; calling it here does not affect the existing string channel. Place the new consume IMMEDIATELY ADJACENT to the existing string consume (move them together if convenient) so they always drain on the same code paths.

    **Defensive drain in finally block — coordinate with Plan 03.** The existing `finally` block at lines 593-603 drains `_consume_last_responsa_downgrade()` (string channel). Plan 02 adds a parallel drain for the meta channel. Plan 03 (now Wave 3 per revision 1, after this plan) extends the SAME finally block with PostHog properties. To avoid file-write conflicts:

    - **Plan 02's finally edit is LIMITED to the meta-drain insertion.** Add the meta-drain immediately after the existing string-drain (not inside it). Do not touch the `capture_api_event` call.
    - Insert AFTER the existing string-drain block (after line 603):

    ```python
    try:
        from genizah_core import _consume_last_responsa_downgrade_meta as _drain_meta
        _drain_meta()
    except Exception:
        logger.warning('thread-local downgrade-meta drain failed in finally')
    ```

    Plan 03 (Wave 3, runs after this plan) extends `capture_api_event` properties — a different region of the same finally block. The two edits do not overlap (Plan 02 edits the drain region only; Plan 03 edits the capture_api_event call only).

    **Step D — Build `request_echo` in /api/parallels handler.** In `parallels_endpoint` (around line 839 where `serialize_parallels_payload` is called):

    ```python
    # 81A D-07 — request echo for /api/parallels. Field name `mode` preserved
    # (NOT renamed to search_mode); no responsa_options (parallels never used Responsa);
    # no gap (ParallelsRequest at web/search_api.py:163-188 has no gap field).
    parallels_echo = {
        'mode': req.mode,
        'chunk_size': req.chunk_size,
        'max_freq': req.max_freq,
        'boundary_options': bundle.boundary_options,  # mirrors existing top-level field
        'limit_effective': len(bundle.main_results),  # post-truncation count
        'filters': filters_dict,
    }
    ```

    Pass `request_echo=parallels_echo` to `serialize_parallels_payload(...)`. **This is the substantive API-EXPAND-07 implementation. Note: 6 keys, no `gap` (ParallelsRequest does not accept it).**

    **Step E — Inline-vs-helper choice.** The planner gave executor discretion to inline the echo construction at the endpoint sites OR introduce a `_apply_request_echo()` helper. Choice: **inline construction** (Steps C and D above) because (a) the echo dicts differ in shape between /api/search and /api/parallels, (b) the construction depends on handler-local state (cascade_meta from a thread-local), and (c) a helper would either need both shapes or two helpers — the inline form keeps it clearer. Document this choice in the SUMMARY.
  </action>
  <verify>
    <automated>python -m py_compile shared/search_serializer.py web/search_api.py</automated>
    <automated>python -c "from shared.search_serializer import serialize_search_payload; e = serialize_search_payload([], meta_mgr=None, query='x', mode='text', request_echo={'search_mode':'exact','responsa_options':None,'responsa_options_effective':None,'gap':0,'limit':50,'limit_effective':50,'filters':None}); assert 'request' in e and e['request']['search_mode']=='exact' and e['request']['responsa_options'] is None; print('OK')"</automated>
    <automated>python -c "from shared.search_serializer import serialize_parallels_payload; e = serialize_parallels_payload([], [], meta_mgr=None, request_echo={'mode':'exact','chunk_size':5,'max_freq':None,'boundary_options':None,'limit_effective':0,'filters':None}); assert 'request' in e and e['request']['mode']=='exact' and 'search_mode' not in e['request'] and 'gap' not in e['request']; print('OK')"</automated>
    <automated>grep -q "request_echo" shared/search_serializer.py && echo found || (echo missing; exit 1)</automated>
    <automated>grep -q "request_echo" web/search_api.py && echo found || (echo missing; exit 1)</automated>
    <automated>grep -q "_consume_last_responsa_downgrade_meta" web/search_api.py && echo found || (echo missing; exit 1)</automated>
  </verify>
  <acceptance_criteria>
    - `serialize_search_payload` signature includes a keyword-only `request_echo: Optional[dict] = None` parameter.
    - `serialize_parallels_payload` signature includes a keyword-only `request_echo: Optional[dict] = None` parameter.
    - When `request_echo` is None, both functions return envelopes without a `request` key (back-compat for Phase 77 download path).
    - When `request_echo` is a dict, both functions embed it under the top-level `request` key verbatim (no rewriting).
    - search_endpoint passes a `request_echo` dict containing exactly the 7 keys: `search_mode`, `responsa_options`, `responsa_options_effective`, `gap`, `limit`, `limit_effective`, `filters`.
    - parallels_endpoint passes a `request_echo` dict containing exactly the 6 keys: `mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective`, `filters` (no `search_mode`, no `responsa_options`, no `gap`). [API-EXPAND-07]
    - search_endpoint's `finally` block defensively drains BOTH `_consume_last_responsa_downgrade()` AND `_consume_last_responsa_downgrade_meta()`.
    - The Plan 02 finally-block edit is LIMITED to inserting the meta-drain block after the existing string-drain block (not modifying the capture_api_event call — that is Plan 03's region in Wave 3).
    - For non-responsa search_mode, both `responsa_options` and `responsa_options_effective` in the echo are `None`.
    - Phase 77 download tests (existing `tests/test_search_serializer.py`) still pass — the function signature change is additive (default None preserves old behavior).
  </acceptance_criteria>
  <done>
    Both endpoints emit a `request` echo block. The Responsa cascade case shows divergence between `responsa_options` (input) and `responsa_options_effective` (post-cascade). Plans 04 and 05 verify this end-to-end via tests.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| genizah_core.py thread-local → /api/search response | Internal channel; risk of cross-request leak if not drained on every code path. |
| /api/search response → external skill | Echo block exposes server-applied state; must not leak fields the threat model rejected (e.g., hashed IP per D-10). |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81A02-01 | Information Disclosure | request echo block | mitigate | Echo contains ONLY what the client sent + server-applied limit/cascade. NO IP, NO bucket key, NO filter vocabulary that the client did not supply (D-10). |
| T-81A02-02 | Tampering / Repudiation | thread-local cascade signal | mitigate | Defensive drain in `finally` for BOTH the legacy string channel AND the new meta channel. ADDITIONALLY a symmetric entry-drain at `execute_search:7254` so direct-core callers cannot leave stale meta (Codex MEDIUM-2). Prevents cross-request leak on the same worker thread (matches Phase 78 R2-#1 precedent). |
| T-81A02-03 | Denial of Service | echo block size | accept | Echo dict has fixed-size keys (max ~12 fields total). No client-controllable list growth. Negligible payload bloat. |
| T-81A02-04 | Spoofing | search_mode echo fidelity | mitigate | D-04: `search_mode` echo is ALWAYS identical to the validated client input. The handler reads `req.search_mode` directly (Pydantic-validated); cannot be tampered post-validation. |
</threat_model>

<verification>
- `python -m py_compile shared/search_serializer.py web/search_api.py genizah_core.py` exits 0.
- All three REPL round-trip checks in Task 1 and Task 2 verifies pass.
- `pytest tests/test_search_serializer.py -x` exits 0 (the additive signature change preserves Phase 77 download semantics).
</verification>

<success_criteria>
The `request` echo block is wired into both endpoint envelopes. `responsa_options_effective` accurately reflects the cascade outcome (per-flag booleans). The cascade-divergence case (request.ja=true, effective.ja=false) is observable end-to-end. Both thread-local channels are drained symmetrically at execute_search entry (Codex MEDIUM-2). Plans 04 and 05 verify the matrix.
</success_criteria>

<output>
Create `.planning/phases/81A-api-contract-expansion/81A-02-SUMMARY.md` documenting: the new thread-local channel and its set/consume call sites; the symmetric entry-drain added at line 7254; the new `request_echo` keyword arg on both serializers; the choice of inline echo construction (no helper); the exact key sets in each endpoint's echo dict (7 for search, 6 for parallels — note: parallels has NO `gap`); the defensive-drain additions to the `finally` block. Note that API-EXPAND-07 is owned by this plan (the substantive parallels echo block work) and Codex MEDIUM-2 is closed by the entry-drain symmetry.
</output>
