---
phase: 81A-api-contract-expansion
plan: 02
subsystem: api
tags: [api, serializer, envelope, request-echo, responsa, cascade]
requires:
  - 81A-01 (SearchRequest/ResponsaOptions models, _SEARCH_MODE_TO_INTERNAL)
provides:
  - shared.search_serializer.serialize_search_payload(..., request_echo=)
  - shared.search_serializer.serialize_parallels_payload(..., request_echo=)
  - genizah_core._set_last_responsa_downgrade_meta
  - genizah_core._consume_last_responsa_downgrade_meta
  - /api/search response envelope.request (7 keys)
  - /api/parallels response envelope.request (6 keys -- no gap, no search_mode, no responsa_options)
affects:
  - /api/search response shape (additive: new top-level 'request' block)
  - /api/parallels response shape (additive: new top-level 'request' block) [API-EXPAND-07]
  - SearchEngine.execute_search entry semantics (now drains BOTH thread-local channels)
tech-stack:
  added: []
  patterns:
    - "Parallel thread-local channels (string + structured-meta) with read-and-clear consumers"
    - "Symmetric entry-drain at execute_search prelude (clears both channels per call)"
    - "Inline echo construction at endpoint sites (no helper) -- the two shapes diverge"
    - "Additive keyword-only kwargs on serializers (default None preserves Phase 77 shape)"
key-files:
  created: []
  modified:
    - genizah_core.py
    - shared/search_serializer.py
    - web/search_api.py
decisions:
  - "Inline construction over helper: search-echo and parallels-echo dicts have different keysets, and search-echo depends on handler-local cascade_meta from a thread-local consume; a helper would either be two helpers or paramerize both shapes -- inline is clearer."
  - "responsa_options_effective falls back to a copy of opts_dict when no cascade fired (consistent shape: skill consumer always sees both fields populated for Responsa, both None for non-Responsa)."
  - "Parallels echo uses field name 'mode' (preserved per D-07), no 'gap' (ParallelsRequest does not accept it), no responsa_options (parallels never used Responsa). Result: 6 keys, not 7."
  - "Symmetric entry-drain at genizah_core.py execute_search prelude clears BOTH the legacy string channel AND the new meta channel, closing Codex MEDIUM-2 (direct-core caller cannot leak stale meta into a subsequent web request)."
  - "limit_effective in the search echo is min(req.limit, MAX_LIMIT) -- belt-and-braces, since Pydantic Field(le=100) already constrains req.limit. In the parallels echo limit_effective mirrors len(bundle.main_results) (post-200-group cap)."
metrics:
  tasks: 2
  files_changed: 3
  commits: 2
  completed: 2026-05-04
---

# Phase 81A Plan 02: Request Echo Block + Structured Cascade Meta Channel

Adds a `request` echo block to BOTH `/api/search` and `/api/parallels` response envelopes. The skill consumer (Phase 81B) keys its "did the server downgrade me?" detection on the divergence between `request.responsa_options` (input) and `request.responsa_options_effective` (post-cascade). Implements API-EXPAND-06, API-EXPAND-07, API-EXPAND-08. Closes Codex MEDIUM-2 (entry-drain symmetry).

## What Changed

### Task 1 -- `genizah_core.py` (commit `7f4ce44e`)

| Region | Change |
|---|---|
| Lines ~65-67 | Added `_LAST_RESPONSA_DOWNGRADE_META = threading.local()` parallel to the existing `_LAST_RESPONSA_DOWNGRADE` channel |
| After `_consume_last_responsa_downgrade` | Added `_set_last_responsa_downgrade_meta(meta: dict)` and `_consume_last_responsa_downgrade_meta() -> Optional[dict]` with the same read-and-clear semantics as the legacy string consumer |
| Line ~7254 (execute_search prelude) | Added `_consume_last_responsa_downgrade_meta()` adjacent to the existing `_consume_last_responsa_downgrade()` so BOTH channels drain on every entry to execute_search (Codex MEDIUM-2) |
| Lines ~7657-7672 (canonical cascade-decision set site) | After the existing `_set_last_responsa_downgrade(responsa_warning)` call, added `_set_last_responsa_downgrade_meta({'variants', 'ja', 'flex_spacing', 'bidirectional'})` with values pulled from the in-scope `variants_on` / `ja_on` (cascade-mutated at lines 7332-7334) and `flex_spacing` / `bidirectional` (pass-through; the cascade does not touch them in 81A scope) |

**Verified:** `variants_on`, `ja_on`, `flex_spacing`, `bidirectional` are all bound at lines 7295-7298 inside `execute_search`'s Responsa branch and remain in scope at the line-7657 set site. The line-break-syntax early return at lines 7284-7292 happens BEFORE these locals are bound, so the set site at 7657 is unreachable from that branch -- the structured-meta call is correctly placed.

### Task 2 -- serializers + endpoints (commit `50195c8c`)

`shared/search_serializer.py`:

| Function | Change |
|---|---|
| `serialize_search_payload` | Added keyword-only `request_echo: Optional[dict] = None`. The function now builds an `envelope` dict; when `request_echo is not None` it adds `envelope['request'] = request_echo` before returning. None keeps the prior shape (Phase 77 download path back-compat). |
| `serialize_parallels_payload` | Same additive change. |

`web/search_api.py` `search_endpoint`:

| Region | Change |
|---|---|
| Step 8a (after the success-path consume) | Added late import + call to `_consume_last_responsa_downgrade_meta()` adjacent to the existing string-channel consume. Result stored in `cascade_meta`. |
| New step before serialize | Build `request_echo` dict with EXACTLY 7 keys: `search_mode`, `responsa_options`, `responsa_options_effective`, `gap`, `limit`, `limit_effective`, `filters`. For non-Responsa search modes both `responsa_options` and `responsa_options_effective` are None. For Responsa with no cascade, `responsa_options_effective` mirrors `responsa_options` (echo of input). For Responsa with cascade, `responsa_options_effective` reflects the cascade outcome. |
| Step 10 (serialize call) | Pass `request_echo=request_echo` into `serialize_search_payload`. |
| `finally` block | Added a defensive `_consume_last_responsa_downgrade_meta()` drain adjacent to the existing string-channel drain (limited to the drain region per the plan's coordination with Plan 03's `capture_api_event` work). |

`web/search_api.py` `parallels_endpoint` (API-EXPAND-07):

| Region | Change |
|---|---|
| Before serialize | Build `parallels_echo` with EXACTLY 6 keys: `mode`, `chunk_size`, `max_freq`, `boundary_options` (mirrors `bundle.boundary_options`), `limit_effective` (post-truncation `len(bundle.main_results)`), `filters`. NO `search_mode` (D-07 keeps `mode` here), NO `gap` (ParallelsRequest has no gap field), NO `responsa_options` (parallels never used Responsa). |
| Serialize call | Pass `request_echo=parallels_echo` into `serialize_parallels_payload`. |

## Echo Block Contract (Final)

`/api/search` -- 7 keys:
```json
{
  "search_mode": "responsa",
  "responsa_options": {"variants": true, "ja": true, "flex_spacing": false, "bidirectional": false},
  "responsa_options_effective": {"variants": true, "ja": false, "flex_spacing": false, "bidirectional": false},
  "gap": 0,
  "limit": 50,
  "limit_effective": 50,
  "filters": null
}
```

The `responsa_options.ja=true` -> `responsa_options_effective.ja=false` divergence is the canonical "server cascade-disabled JA" signal that the skill (81B) reads. The same outcome is also surfaced as a `tr()` string in the top-level `warnings[]` array (existing behavior preserved).

`/api/parallels` -- 6 keys (NO `search_mode`, NO `responsa_options`, NO `gap`):
```json
{
  "mode": "exact",
  "chunk_size": 5,
  "max_freq": null,
  "boundary_options": {"boundary_mode": "full", "...": "..."},
  "limit_effective": 47,
  "filters": null
}
```

## Verification

Compile: `python -m py_compile shared/search_serializer.py web/search_api.py genizah_core.py` -> exit 0.

Round-trip:
- `_set_last_responsa_downgrade_meta({...}) -> _consume_..._meta() == {...}; second consume -> None` -> OK.
- `serialize_search_payload(request_echo={7 keys})` -> envelope has top-level `request` -> OK.
- `serialize_parallels_payload(request_echo={6 keys})` -> envelope has top-level `request` with no `search_mode` and no `gap` -> OK.
- Back-compat: omit `request_echo` -> envelope has no `request` key -> OK.

Entry-drain symmetry: window scan of `genizah_core.py:execute_search` prelude (first 25 lines) confirms BOTH `_consume_last_responsa_downgrade()` and `_consume_last_responsa_downgrade_meta()` calls are present.

`pytest tests/test_search_serializer.py -x -q` -> 26 passed (signature change is additive; default None preserves prior envelope shape).

## Commits

| Hash | Subject |
|---|---|
| `7f4ce44e` | `feat(81A-02): structured-meta thread-local + symmetric entry-drain` |
| `50195c8c` | `feat(81A-02): request echo block on /api/search and /api/parallels` |

## Deviations from Plan

None of substance. One small naming reconciliation:

- **ParallelsRequest field name.** The plan-text references `composition` and `boundary_options` on the request side, but the live `ParallelsRequest` (post-Phase-80) has `text` and `boundary_mode` (the latter is a Literal enum, not a dict). The echo block's `boundary_options` correctly mirrors `bundle.boundary_options` (the dict the service layer builds), so the echo contract is satisfied. Documented here for downstream reviewers.

The plan's referenced line numbers (e.g. `web/search_api.py:163-188` for ParallelsRequest) are also slightly off in the live tree (it lives at ~228-254 today), which made no functional difference -- the relevant fact ("no `gap` field") still holds.

## Threat Flags

None. Echo block is fixed-shape and contains only what the client sent + server-applied limits/cascade-meta. No IP, no bucket key, no out-of-band identifiers (D-10 honored). Symmetric entry-drain closes the cross-caller stale-meta leak (T-81A02-02).

## Self-Check: PASSED

- `genizah_core.py` modified -- contains `_LAST_RESPONSA_DOWNGRADE_META`, `_set_last_responsa_downgrade_meta`, `_consume_last_responsa_downgrade_meta`, both consume calls in execute_search prelude, and the structured set site adjacent to the legacy set site.
- `shared/search_serializer.py` modified -- both serializers accept `request_echo` and embed it under top-level `request` when supplied.
- `web/search_api.py` modified -- search_endpoint builds 7-key echo + drains meta channel; parallels_endpoint builds 6-key echo. finally block drains both channels.
- Commits `7f4ce44e` and `50195c8c` exist on the worktree branch (`git log --oneline -3` confirmed).
- Compile + round-trip + serializer pytest checks all green.
