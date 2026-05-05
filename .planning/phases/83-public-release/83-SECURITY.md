# Phase 83 Security Audit — Public Release of Search API

**Plan:** 83-01
**Date:** 2026-05-05
**Auditor:** GSD executor (re-grepping live code at HEAD `a3282dc0`)
**Scope:** Verify Phase 78–81B mitigations are still load-bearing for public exposure of `/api/search`, `/api/browse`, `/api/parallels`. Per Codex review concern #4, this audit covers EXISTING mitigations only; Phase 83 OpenAPI sub-mount threats (T-83-OAS-LEAK / T-83-OAS-DOS) are Plan 03's responsibility and are recheck-verified by the Wave 0 OpenAPI scope tests (`tests/test_openapi_scope.py`).
**Verdict:** **APPROVED — proceed to Plan 02 (docs reframe).**

---

## Section A — Mitigation-Coverage Audit (baseline)

Each row below cites a concrete `file:line` confirmed by direct read of the worktree at HEAD. STRIDE category and the D-05 audit item (a)–(f) from `83-CONTEXT.md` are listed for each.

| Threat ID | STRIDE | Component | D-05 | Mitigation (file:line) | Status |
|-----------|--------|-----------|------|------------------------|--------|
| T-78-01 | Tampering | XFF spoofing for rate-limit key | (a) | `_resolve_rate_limit_key` at `web/api_hardening.py:84` walks XFF right-to-left skipping `_TRUSTED_PROXIES` (defined `web/api_hardening.py:69-77`); ignores XFF entirely when direct peer is not trusted (line 109-110). Concern #1 mitigation. | VERIFIED |
| T-78-04 | Spoofing | XFF spoofing for `localhost-only` mode bypass | (a)/(c) | `_is_loopback_request` at `web/api_hardening.py:123` requires direct peer in `LOOPBACK_IPS` AND every XFF entry in `LOOPBACK_IPS` (line 144-148). RFC1918 ranges deliberately excluded (line 57). Concern #4 mitigation. | VERIFIED |
| T-78-05 | DoS | Memory growth via per-IP buckets | (a) | `RateLimiter._evict_stale` at `web/api_hardening.py:182` prunes empty buckets older than `RATE_LIMIT_BUCKET_TTL` (default 3600s, `web/api_hardening.py:64`). Called every `check()` (line 225). Concern #5 mitigation. | VERIFIED |
| T-78-09 | Information Disclosure | Raw client IP in PostHog events | (b) | `POSTHOG_IP_SALT` HMAC-hashes IPs before capture (`web/api_hardening.py:437` env read; `web/api_hardening.py:471` `_POSTHOG_IP_SALT` constant; `web/api_hardening.py:479` HMAC-sha256 site). Auto-generated and persisted to `web/_secrets/` if unset (`web/api_hardening.py:459`). HARDEN-05 mitigation. | VERIFIED |
| T-78-03 | Information Disclosure | Tracebacks / internal paths in error responses | (d) | `_build_envelope_response` at `web/api_hardening.py:274` returns `{error:{code,message}}` only — message comes from `APIError.message` or Pydantic's `errors()[0].msg`, never `traceback`/`exc_info`. Generic 500 path at line 327-330 returns literal `'Internal error'`. Per-endpoint via `wrap_endpoint` (line 333), NOT global handler. Concern #2/#3 mitigation. | VERIFIED |
| T-78-NEUTRAL | Architectural | Cross-layer dependency inversion (web/ → shared/) | (d) | `APIError` and `ERROR_CODES` (frozenset of 18 codes) in `shared/api_errors.py:24-45` and `shared/api_errors.py:55-89`. Module-level docstring forbids any `web.*` import (lines 8-12). Re-exported by `web/api_hardening.py:46` for legacy callers. Concern #3 mitigation. | VERIFIED |
| T-78-MODE | Tampering / DoS | Production kill switch / read-only mode | (c) | `enforce_mode_gate` at `web/api_hardening.py:251` re-reads `SEARCH_API_MODE` env per request (line 258); supports `open`/`localhost-only`/`disabled`. `disabled` → 503 (line 260); `localhost-only` → 403 unless loopback (line 262-267). Flippable without restart. D-02/D-04 mitigation. | VERIFIED |
| T-78-FILTER | Injection / Ambiguity | Unknown filter values silently allow-all | (e) | `validate_filter_values` at `shared/fjms_service.py:1375` (and module shorthand at line 3438). Raises `APIError('unresolvable_filter_value', 400)` on unknown token (lines 1415, 1451, 1487, 1514); raises `APIError('filter_vocabulary_unavailable', 503)` when vocab loader fails (lines 1399, 1409, 1427, 1433, 1463, 1469, 1499, 1507). Imports `APIError` from `shared.api_errors` (NOT `web.api_hardening`), preserving Concern #3. R2-#3 mitigation. | VERIFIED |
| T-78-LEN | DoS | Unbounded query length | (f) | `QUERY_LENGTH_CAP = 1000` at `web/search_api.py:175`; enforced at `web/search_api.py:550-553` raising `APIError('query_too_long', 400)`. | VERIFIED |
| T-78-EXP | DoS | Responsa expansion blow-up | (f) | `MAX_EXPANDED_TERMS = 500` at `genizah_core.py:1996`; enforced in expansion path at `genizah_core.py:6034` (`Config.MAX_EXPANDED_TERMS`). Thread-local downgrade marker `_LAST_RESPONSA_DOWNGRADE` at `genizah_core.py:65` so concurrent requests don't cross-contaminate. D-05(f) mitigation. | VERIFIED |
| T-78-EXTRA | Tampering | Unknown request fields silently accepted | (e) | All Pydantic request models declare `model_config = ConfigDict(extra='forbid')` — `web/search_api.py:102, 120, 142, 219, 246` (FiltersModel, ResponsaOptions, SearchRequest, BrowseRequest, ParallelsRequest). | VERIFIED |
| T-81-CODES | Contract | Error code taxonomy drift | (d) | `ERROR_CODES` frozenset in `shared/api_errors.py:24-45` documents 18 codes as the public contract (`invalid_request`, `rate_limited`, `disabled`, `localhost_only`, `unresolvable_filter_value`, `filter_vocabulary_unavailable`, `internal_error`, etc., plus Phase 79/80/81A additions). `WARNING_CODES` at line 48-52 separates non-fatal warnings. | VERIFIED |

**Row count:** 12 mitigations covering D-05 items (a) rate limiter + IP resolution, (b) IP exposure surface, (c) mode gate, (d) error envelope, (e) filter injection, (f) query/expansion caps, plus the structural neutral-module and contract-codes invariants. Phase 78 Concerns #1, #3, #4, #5, #9 are each represented.

---

## Section B — Post-Deploy Verification Checklist

Run from a developer laptop after `bash deploy.sh master-main` completes (Plan 05). Each item is a single curl/browser action that takes <30s. Failure on any item BLOCKS deploy completion and triggers rollback per the Plan 05 runbook.

1. **Rate limiter still active (D-05a):**
   Hit `/api/search` 35 times in <60s with the same payload. Expect at least one `429` response with `error.code == "rate_limited"` and a `Retry-After` header.
   ```bash
   for i in $(seq 1 35); do
     curl -s -o /dev/null -w "%{http_code}\n" \
       -X POST https://genizahsearch.com/api/search \
       -H "Content-Type: application/json" \
       -d '{"query":"חיים","search_mode":"exact","limit":1}'
   done | sort | uniq -c
   # Expect: at least one 429 line in output.
   ```

2. **IP exposure surface (D-05b):**
   ```bash
   curl -s https://genizahsearch.com/api/search \
     -X POST \
     -H "X-Forwarded-For: 1.2.3.4, 5.6.7.8" \
     -H "Content-Type: application/json" \
     -d '{"query":"test","search_mode":"exact","limit":1}'
   ```
   Confirm response is 200 (or 429). Within 5 minutes, check PostHog dashboard for the resulting `api_search` event — the `client_ip_hash` property must be 16 hex chars (HMAC-sha256 truncated), NOT a raw IP like `1.2.3.4`.

3. **Mode gate flip (D-05c) — OPTIONAL smoke, only if validating kill switch:**
   On the production server: `export SEARCH_API_MODE=disabled && systemctl restart genizah-web`.
   From laptop: `curl -s https://genizahsearch.com/api/search -X POST -H "Content-Type: application/json" -d '{"query":"x","search_mode":"exact","limit":1}'` returns 503 with `error.code == "disabled"`.
   Then revert: `unset SEARCH_API_MODE && systemctl restart genizah-web`. (Skip if simply confirming public-default `open` mode.)

4. **Error envelope sanity (D-05d):**
   ```bash
   curl -s -X POST https://genizahsearch.com/api/search \
     -H "Content-Type: application/json" \
     -d '{"junk":"x"}'
   ```
   Response status MUST be 400. Body MUST be `{"error": {"code": "invalid_request", "message": "...", "fields": [...]}}`. The message MUST NOT contain a Python traceback, file path (`/home/`, `web/`), or stack frame.

5. **Filter fail-closed (D-05e):**
   ```bash
   curl -s -X POST https://genizahsearch.com/api/search \
     -H "Content-Type: application/json" \
     -d '{"query":"test","search_mode":"exact","limit":1,"filters":{"domains":["NOT_A_REAL_DOMAIN_XYZ"]}}'
   ```
   Response MUST be 400 with `error.code == "unresolvable_filter_value"`. Critically, NOT 200 with empty results (which would mean fail-open allow-all is back).

6. **OpenAPI schema populated (Codex concern #2):**
   ```bash
   curl -s https://genizahsearch.com/api/openapi.json | \
     python -c "import json,sys; s=json.load(sys.stdin); paths=s['paths']; assert any('requestBody' in p.get('post',{}) for p in paths.values()), 'no requestBody schemas'; print('OK')"
   ```
   Exit code 0 means the production spec has populated request schemas — Swagger UI will render parameter forms instead of buttonless cards.

7. **Swagger UI loads:**
   Browser → `https://genizahsearch.com/api/docs`. Three endpoint cards (`/search`, `/browse`, `/parallels`) visible. Each card expandable; "Try it out" button reveals editable parameter/body fields (NOT a blank panel — the Codex concern #2 failure mode).

---

## Out-of-Scope (Deferred to Plan 03)

The following Phase 83 NEW threats are NOT verified by this audit. They are introduced by Plan 03's OpenAPI sub-mount and are validated by the Wave 0 RED tests in `tests/test_openapi_scope.py`:

- **T-83-OAS-LEAK** — Legacy `/api/cambridge_image`, `/_internal/memstat`, `/robots.txt` routes leaking into the public OpenAPI spec. Mitigated by mounting a separate `FastAPI` instance scoped to the three search-helper endpoints; verified by `test_openapi_excludes_legacy_routes`.
- **T-83-OAS-DOS** — Unauthenticated `/api/openapi.json` and `/api/docs` requests counting against the same rate-limit bucket as `/api/search`, allowing trivial denial of service via repeated docs pulls. Plan 03 must either exempt the docs paths from `RateLimiter` OR document the chosen rate sharing.

These are Plan 03's responsibility and recheck-verified by the Wave 0 test scaffold; no remediation is required at this stage of Plan 01.

---

## Overall Verdict

**APPROVED.** All 12 baseline mitigations are present at their claimed file:line. No gap requires a remediation plan to be inserted before Plan 05. Phase 83 may proceed to Plan 02 (docs reframe), Plan 03 (OpenAPI sub-mount), Plan 04 (README + skill), and Plan 05 (release).

Operator running Plan 05's manual deploy MUST execute Section B's seven checks before declaring the deploy successful.
