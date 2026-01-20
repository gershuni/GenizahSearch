# Site Readiness Report
**Date:** 2026-01-18
**Status:** 🔴 NOT READY FOR LAUNCH

## Executive Summary
The application is **not yet ready for launch**. While previous critical logic bugs have been addressed, significant issues remain regarding external connectivity (NLI) and backend test stability. Additionally, the dependency management (requirements files) is incomplete.

---

## 1. Bugs & Integrity 🐛

### ✅ Fixed Issues
The following issues from previous audits (`CODE_REVIEW_REPORT.md`) have been verified as **FIXED**:
- **Correction Logic:** The `page_number` filtering logic in `CorrectionService` now correctly handles `None` values.
- **Frontend Submission:** The `rejection_reason` is now correctly sent instead of `notes` during rejection in `web/pages/corrections.py`.
- **Auto-Save:** The `text_editor.py` auto-save mechanism uses synchronous callbacks compatible with NiceGUI's timer, contrary to the previous report's concern.

### ❌ Active Issues
- **Functional Test Failures:**
  - `tests/test_corrections_api.py` is failing with `400 Bad Request` for:
    - `test_create_correction`
    - `test_submit_correction`
  - This indicates a regression or a mismatch between the test data and current validation rules.
- **Incomplete Dependencies:**
  - The root `requirements.txt` is missing critical backend packages: `sqlalchemy`, `pydantic-settings`, `python-jose`, `pytest-asyncio`.
  - The application relies on `backend/requirements.txt` which is not automatically installed by standard setup procedures.

---

## 2. Connectivity & External Services 🌐

### ✅ Operational
- **GitHub Updates:** The update checker endpoint (`api.github.com`) is reachable and responding correctly (Status 200).
- **Google AI:** The API endpoint (`generativelanguage.googleapis.com`) is reachable (DNS/Network path is open).

### 🔴 Critical Failure
- **National Library of Israel (NLI):**
  - **Status:** TIMEOUT
  - **Details:** Requests to `iiif.nli.org.il` are timing out.
  - **Impact:** Users will likely be unable to view manuscript images or fetch metadata. This is a critical blocker for a Genizah research tool.
  - **Potential Cause:** Geo-blocking or firewall restrictions in the current environment.

---

## 3. Safety & Security 🔒

### ✅ Passed Checks
- **Hardcoded Secrets:** A scan of the codebase revealed **no hardcoded API keys** (Google, OpenAI, GitHub tokens).
- **Permissions:** The `CorrectionService` implements explicit permission checks (`can_submit_corrections`, `can_review_corrections`).

---

## 4. Recommendations for Launch

1.  **Fix NLI Connectivity:** Investigate the connection to `iiif.nli.org.il`. Implement a proxy or alternative data source if direct access is blocked.
2.  **Repair Test Suite:** Debug the `400 Bad Request` errors in `test_corrections_api.py` to ensure the core correction workflow is stable.
3.  **Consolidate Dependencies:** Merge `backend/requirements.txt` into the root `requirements.txt` or create a unified `setup.py` to ensure all necessary packages (like `sqlalchemy`) are installed.
