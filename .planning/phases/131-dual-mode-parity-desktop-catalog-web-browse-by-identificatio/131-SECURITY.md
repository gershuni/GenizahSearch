---
phase: 131
slug: dual-mode-parity-desktop-catalog-web-browse-by-identificatio
status: verified
threats_open: 0
asvs_level: 1
block_on: high
created: 2026-07-01
auditor: gsd-security-auditor (claude-sonnet-4-6)
register_authored_at_plan_time: true
---

# Phase 131 — Security

> Per-phase security contract: threat register, accepted risks, and audit trail.
> Dual-Mode Library Filter parity — desktop catalog / web Browse-by-Identification / web `/parallels` / web `/search` label param.

**Threats Closed:** 24 / 24 · **Threats Open:** 0 · **Unregistered Flags:** 0

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| browser → NiceGUI server (catalog / parallels / search) | Checked-library-code list returns from client JS; persisted `catalog_library_filter` / `parallels_library_filter` / `search_library_filter` values and the `incoming_filters` handoff dict can be crafted or stale. | `{'mode','codes'}` library-filter dict (untrusted) |
| per-user state ↔ `web/safe_storage.py` | All web per-surface library-filter state written/read through the Phase-87 chokepoint; multitenant leakage is the guarded boundary. | Per-user filter selection |
| caller → `get_browse_results` / `get_browse_library_facets` | `library_mode` + `library_codes` are caller-supplied; `sys_id_to_library` is a bound full-corpus callable, not raw input. | Sanitized library codes + mode string |
| desktop dialog → `get_browse_results` | Single-user local app; `_catalog_library_mode` is an in-process attribute; codes chosen from the constructed `library_codes_with_manuscripts()` list. | In-memory codes + mode (no network) |

---

## Threat Register

| Threat ID | Category | Component | Disposition | Mitigation / Evidence | Status |
|-----------|----------|-----------|-------------|------------------------|--------|
| T-131-01-01 | Tampering | test scaffolds (Wave 0) | accept | Additive test scaffolds asserting V5/V3 controls; no production attack surface (no network/auth/persistence). | closed |
| T-131-01-SC | Tampering | package installs | mitigate | Zero `requirements*.txt` / `package.json` / `pyproject.toml` changes across all Phase 131 commits. | closed |
| T-131-02-01 | Tampering | `library_mode` value | mitigate | `shared/fjms_service.py:2343` — `_exists_kw = "NOT EXISTS" if library_mode == "hide" else "EXISTS"`; unrecognised values fail-safe to `"EXISTS"`. Fixed SQL keyword, never interpolated as free text. | closed |
| T-131-02-02 | Info disclosure | Hide-mode / facet result set | accept | Same public catalog corpus the user already browses; same WHERE conditions; no privileged data path. | closed |
| T-131-02-03 | Tampering | facet condition reuse + callable mapper | mitigate | `shared/fjms_service.py:2464` — `not callable(sys_id_to_library) → {}`. Facets reuse the shared parameterized `_build_browse_conditions` helper (2057+); user terms stay bound params. | closed |
| T-131-02-SC | Tampering | package installs | mitigate | No installs. | closed |
| T-131-03-01 | Tampering | codes → `get_browse_results` (desktop) | mitigate | `desktop/dialogs_filter.py:1711` — `[c for c in library_codes_with_manuscripts() if c != 'LOCAL']`. `genizah_app.py:566` — `library_mode=self._library_mode` routes through the fixed SQL-keyword dispatch. | closed |
| T-131-03-02 | Info disclosure | Hide catalog view | accept | Same public desktop catalog corpus. | closed |
| T-131-03-03 | Tampering | Hide-mode search/composition handoff | mitigate | `genizah_app.py:10835` — `filters['library']` set ONLY when `_catalog_library_mode == 'show_only'`; recompute paths (10874, 10917) suppress the restriction + emit a statusBar notice in Hide mode (no invert-to-allowlist). | closed |
| T-131-03-SC | Tampering | package installs | mitigate | No installs. | closed |
| T-131-04-01 | Tampering | crafted/stale storage, JS checked list, incoming dict | mitigate | Restore `catalog_browse.py:118-137` (list + dict branches call `sanitize_library_codes`, bogus mode → `'hide'`); apply `catalog_browse.py:1286`; consume handoff `filter_panel.py:353-365` (mode validated against `('show_only','hide')`, codes sanitized). | closed |
| T-131-04-02 | Spoofing/Info | cross-user state leak | mitigate | No `app.storage.user` writes in `catalog_browse.py`; `filter_panel.py:373` `persist_value(...)` → safe_storage. `tests/test_no_raw_storage_access.py` green (allowlist `[]`). | closed |
| T-131-04-03 | Tampering | `'LOCAL'` injected as web option | mitigate | `catalog_browse.py:1071,1142,1150,1299` inline `c != 'LOCAL'` guards + `sanitize_library_codes` drops `'LOCAL'`. `tests/test_web_library_options_no_local.py` AST scan green. | closed |
| T-131-04-04 | Info disclosure | facet counts | accept | Same public catalog metadata over the user's own active filters. | closed |
| T-131-04-SC | Tampering | package installs | mitigate | No installs. | closed |
| T-131-05-01 | Tampering | crafted/stale `parallels_library_filter` / JS checked list | mitigate | Restore `parallels.py:347-360` (list + dict branches sanitize; bogus mode → `'hide'`); apply `parallels.py:1696` sanitizes. | closed |
| T-131-05-02 | Spoofing/Info | cross-user state leak | mitigate | `parallels.py:1722` `safe_user_set('parallels_library_filter', ...)`; no direct `app.storage.user`. `tests/test_no_raw_storage_access.py` green (allowlist `[]`). | closed |
| T-131-05-03 | Tampering | `'LOCAL'` injected as web option | mitigate | `parallels.py:1513,1576,1582-1585` inline `c != 'LOCAL'` guards + sanitize. `tests/test_web_library_options_no_local.py` AST scan green. | closed |
| T-131-05-04 | Info disclosure | unscoped export / stored payload | mitigate | `parallels.py:2792-2795` — Hide filter applied to `main_results`/`filtered_results` BEFORE `set_parallels_export(...)` (2800) and `safe_user_set('parallels_results', ...)` (2810). Show-only scoped pre-query at 2660-2666 (outside the `_has_active_filters()` gate). | closed |
| T-131-05-05 | Denial of service | resolution cost | accept | Show-only resolves once in `run.io_bound` (2662, off event loop) reusing `resolve_library_sys_ids`; Hide iterates the bounded result list; no new heavy scan. | closed |
| T-131-05-SC | Tampering | package installs | mitigate | No installs. | closed |
| T-131-08-01 | Tampering | `get_library_display` default regression | mitigate | `shared/browse_map_utils.py:259-260` — `with_code: bool = False`; path reached only when `with_code=True and short=False` (287). Existing callers byte-identical. | closed |
| T-131-08-02 | Info disclosure | library code in label | accept | Codes (CUL/JTS) are public identifiers already shown throughout the app. | closed |
| T-131-08-03 | Tampering | web data-label/label HTML injection | accept | Codes from the fixed `LIBRARY_CODES` dict; `_make_cat_cb_row` already applies `html.escape()`. | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

| Risk ID | Threat Ref | Rationale | Accepted By | Date |
|---------|------------|-----------|-------------|------|
| AR-131-1 | T-131-01-01 | Test scaffolds introduce no production attack surface. | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |
| AR-131-2 | T-131-02-02 | Hide/facet result set is over the same public corpus. | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |
| AR-131-3 | T-131-03-02 | Hide catalog view is over the same public desktop corpus. | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |
| AR-131-4 | T-131-04-04 | Facet counts are over the user's own active filter state. | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |
| AR-131-5 | T-131-05-05 | Resolution cost uses existing bounded helpers, off the event loop. | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |
| AR-131-6 | T-131-08-02 | Public library codes rendered in the label (both languages). | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |
| AR-131-7 | T-131-08-03 | `html.escape()` already applied at the row-builder site. | gsd-security-auditor + Hillel Gershuni | 2026-07-01 |

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-07-01 | 24 | 24 | 0 | gsd-security-auditor (claude-sonnet-4-6), verify-only mode |

### CI guards corroborating the audit (green in the current tree)

- `tests/test_web_library_options_no_local.py` — AST scan enforcing `c != 'LOCAL'` in every web function touching `LIBRARY_CODES` (T-131-04-03, T-131-05-03).
- `tests/test_no_raw_storage_access.py` — `app.storage.user` allowlist `[]`; all writes via `web/safe_storage.py` (T-131-04-02, T-131-05-02).
- `tests/test_phase_97_invariants.py` — web library empty-allowlist + cloud-write-gate invariants.

Run 2026-07-01: **13 passed**.

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-07-01
