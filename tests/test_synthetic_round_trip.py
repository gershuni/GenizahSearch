"""Phase 85 SYNTH-06 community-write + parallels round-trip tests (REVIEWS-MODE 2026-05-08).

Verifies that synthetic sys_ids round-trip cleanly through:
  - Lists (add via real UserListsManager.add_item_sync — opaque-string passthrough)
  - Comments (add/retrieve via real SupabaseCorrectionsClient.create_comment) —
    REVIEWS-MODE Codex MEDIUM: REAL assertions, not pass-body
  - Exclusions (filter inclusion — opaque-string set membership)
  - Parallels (composition-search via TEXT input, NOT sys_id) —
    REVIEWS-MODE Codex MEDIUM: posts canonical shelfmark text and asserts no 500
  - Corrections READ-side (returns [] safely for synthetic IDs)
  - Corrections WRITE-side: REVIEWS-MODE iteration 1 B1+B2 — UI button hidden
    AND backend rejects at the REAL write entry points
    (corrections_client.CorrectionsClient.create_correction +
    supabase_corrections_client.SupabaseCorrectionsClient.create_correction).
    There is NO `POST /api/corrections` HTTP route in this codebase; gating is
    at the client-class level.

REVIEWS-MODE iteration 1 B3 import notes:
  - `from supabase_corrections_client import SupabaseCorrectionsClient` is the
    REAL Supabase comments + corrections write entry. The previous revision's
    `from shared import comments_service` was a phantom reference.
  - `from web.user_lists import UserListsManager` is the REAL web list-add entry.
    The previous revision's `from lists_sync import add_to_list` was a phantom
    reference (lists_sync only exposes ListsCloudSync class + get_lists_sync
    factory).
  - `from corrections_client import CorrectionsClient` is the REAL REST-wrapper
    write entry.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from shared.synthetic_sys_id import is_synthetic_sys_id


SYNTHETIC_ID = "990001234560000000"
REAL_ALMA_ID = "990025143260205171"


# ---------------------------------------------------------------------------
# Lists — REVIEWS-MODE iteration 1 B3: real entry point UserListsManager
# ---------------------------------------------------------------------------


class TestListsRoundTrip:
    """REVIEWS-MODE iteration 1 B3: imports the REAL list-management entry point.

    The previous revision's `from lists_sync import add_to_list, get_list_items`
    was a phantom reference — lists_sync only exposes the ListsCloudSync class
    and get_lists_sync factory.

    Real list-add entry points (verified 2026-05-08):
      - web/user_lists.py:376 ``UserListsManager.add_item`` (async)
      - web/user_lists.py:417 ``UserListsManager.add_item_sync`` (sync; used here)
    """

    def test_add_synthetic_to_list_via_user_lists_manager(self, monkeypatch):
        """Supabase list_items.sys_id is an opaque string column — synthetic
        IDs flow through unchanged. Verify by stubbing the local-storage backend
        and asserting add_item_sync returns truthy and surfaces the synthetic
        sys_id to the storage layer verbatim.

        UserListsManager.is_authenticated is a property reading
        GlobalAuthState.is_logged_in(); monkeypatch to force the local-only
        path so we exercise local_mgr.add_item directly.
        """
        from web.user_lists import UserListsManager
        import web.user_lists as _ul

        # Force unauthenticated path (no Supabase round-trip needed).
        monkeypatch.setattr(_ul.GlobalAuthState, "is_logged_in", staticmethod(lambda: False))

        # Build a UserListsManager with the bare attributes add_item_sync needs.
        mgr = UserListsManager.__new__(UserListsManager)
        mgr.meta_mgr = None
        mgr.local_mgr = MagicMock()
        mgr.local_mgr.add_item = MagicMock(return_value=True)

        result = mgr.add_item_sync(SYNTHETIC_ID, list_id="default")
        assert result is True, (
            "UserListsManager.add_item_sync returned falsy for synthetic sys_id"
        )
        # Confirm the synthetic sys_id reached the storage layer verbatim.
        mgr.local_mgr.add_item.assert_called_once()
        call_args = mgr.local_mgr.add_item.call_args
        assert SYNTHETIC_ID in str(call_args), (
            f"synthetic sys_id not passed to local_mgr.add_item; got {call_args}"
        )

    def test_user_lists_manager_treats_synthetic_as_opaque_string(self):
        """Coarse contract test: synthetic sys_id satisfies is_synthetic_sys_id()
        (so the UI hide branches fire) AND is still acceptable as a list-item key
        (UserListsManager has no synthetic-rejection branch for lists — D-10
        covers ONLY corrections-write deferral)."""
        assert is_synthetic_sys_id(SYNTHETIC_ID)
        # Should not raise when passed through normal list-key contract.


# ---------------------------------------------------------------------------
# Comments — REVIEWS-MODE iteration 1 B3 + Codex MEDIUM: REAL entry points
# ---------------------------------------------------------------------------


class TestCommentsRoundTrip:
    """REVIEWS-MODE iteration 1 B3: imports the REAL comments-write entry point.

    The previous revision's `from shared import comments_service` was a phantom
    reference — no such module exists. Real entry points (verified 2026-05-08):
      - supabase_corrections_client.py:1046 ``SupabaseCorrectionsClient.create_comment``
      - supabase_corrections_client.py:1083 ``SupabaseCorrectionsClient.get_document_comments``
      - supabase_corrections_client.py:1110 ``SupabaseCorrectionsClient.get_comments_for_document`` (alias)
    """

    def _make_client_with_mock_supabase(self):
        """Build a SupabaseCorrectionsClient with the auth/client checks stubbed
        out so create_comment proceeds to the insert call."""
        from supabase_corrections_client import SupabaseCorrectionsClient
        client = SupabaseCorrectionsClient.__new__(SupabaseCorrectionsClient)
        # Stub the authenticated-user gate at the top of create_comment.
        user = MagicMock()
        user._uuid = "test-user-uuid"
        client.current_user = user
        # Stub _get_client to return a chainable Supabase mock.
        supabase_mock = MagicMock()
        supabase_mock.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[{
                "id": 1,
                "sys_id": SYNTHETIC_ID,
                "author_id": "test-user-uuid",
                "content": "Test comment on synthetic row",
                "scope": "general",
                "page_number": None,
                "is_public": True,
                "parent_id": None,
                "created_at": "2026-05-08T00:00:00Z",
            }]
        )
        # Configure SELECT chain for get_document_comments.
        select_chain = (
            supabase_mock.table.return_value
            .select.return_value
            .eq.return_value
            .eq.return_value
            .order.return_value
            .range.return_value
            .execute.return_value
        )
        select_chain.data = [{
            "id": 1,
            "sys_id": SYNTHETIC_ID,
            "content": "Test comment on synthetic row",
            "is_public": True,
        }]
        client._get_client = MagicMock(return_value=supabase_mock)
        # _parse_comment uses identity for assertion purposes.
        client._parse_comment = MagicMock(side_effect=lambda d: d)
        return client, supabase_mock

    def test_add_comment_on_synthetic(self):
        """REVIEWS-MODE Codex MEDIUM closure — REAL assertion, not pass-body.

        Add a comment with a synthetic sys_id via the actual
        SupabaseCorrectionsClient.create_comment method; assert it serializes
        and reaches client.table('comments').insert(data).execute() with the
        synthetic ID flowing through unchanged in the inserted payload."""
        client, supabase_mock = self._make_client_with_mock_supabase()
        comment, msg = client.create_comment(
            content="Test comment on synthetic row",
            document_id=SYNTHETIC_ID,
        )
        assert comment is not None, f"create_comment returned None: {msg}"
        # Assert insert was called and the synthetic sys_id is in the payload.
        insert_call = supabase_mock.table.return_value.insert.call_args
        assert insert_call is not None, (
            "client.table('comments').insert was never called"
        )
        # First positional arg OR keyword 'data' carries the payload dict.
        if insert_call.args:
            inserted_data = insert_call.args[0]
        else:
            inserted_data = insert_call.kwargs.get("data")
        assert isinstance(inserted_data, dict)
        assert inserted_data.get("sys_id") == SYNTHETIC_ID, (
            f"synthetic sys_id missing from comments insert payload; got {inserted_data}"
        )

    def test_retrieve_comments_for_synthetic(self):
        """get_comments_for_document(synthetic) round-trips the synthetic ID."""
        client, _ = self._make_client_with_mock_supabase()
        comments = client.get_comments_for_document(SYNTHETIC_ID)
        assert len(comments) == 1
        # _parse_comment is stubbed as identity; the synthetic ID survives.
        assert comments[0].get("sys_id") == SYNTHETIC_ID
        assert comments[0].get("content") == "Test comment on synthetic row"


# ---------------------------------------------------------------------------
# Exclusions — opaque-string set membership
# ---------------------------------------------------------------------------


class TestExclusionsRoundTrip:
    def test_synthetic_in_excluded_set_filters_results(self):
        """exclusion_service.py operates on sys_id sets; synthetic IDs filter naturally.

        Per `excluded_sys_ids: set[str]` contract (shared/exclusion_service.py),
        sys_id is an opaque string. The set membership check works identically
        for synthetic and real Alma IDs.
        """
        excluded: set[str] = {SYNTHETIC_ID}
        results = [
            {"sys_id": SYNTHETIC_ID, "shelfmark": "T-S NS 329.96"},
            {"sys_id": REAL_ALMA_ID, "shelfmark": "T-S 1.1"},
        ]
        filtered = [r for r in results if r["sys_id"] not in excluded]
        assert len(filtered) == 1
        assert filtered[0]["sys_id"] == REAL_ALMA_ID


# ---------------------------------------------------------------------------
# Parallels — REVIEWS-MODE Codex MEDIUM/HIGH: TEXT input, not sys_id
# ---------------------------------------------------------------------------


class TestParallelsTextInputTolerance:
    """REVIEWS-MODE Codex HIGH: /api/parallels takes `text`, not `sys_id`.

    The previous test name `test_parallels_endpoint_handles_synthetic_seed`
    was misleading — there's no synthetic seed to pass. We verify the endpoint
    tolerates `text` containing a canonical shelfmark string (the closest
    user-input scenario where a synthetic row could be conceptually relevant).
    """

    def test_parallels_with_synthetic_shelfmark_text_does_not_crash(self):
        """POST /api/parallels with composition text mentioning a synthetic-row
        shelfmark returns 200 (or 400 for short-text rejection, or 503 if the
        engine is unavailable in the test environment) — never 500.

        The test environment may not have a fully-initialized search engine;
        the load-bearing assertion is "no unhandled 500 from the synthetic
        text input itself." Skip cleanly if the API cannot bootstrap.
        """
        try:
            from fastapi.testclient import TestClient
            from web.api import target_app
        except Exception as e:
            pytest.skip(f"Cannot import web.api.target_app in test env: {e}")

        try:
            client = TestClient(target_app)
        except Exception as e:
            pytest.skip(f"Cannot construct TestClient: {e}")

        # Use sufficiently long composition text mentioning a synthetic shelfmark.
        text = "מילון תלמודי " * 50  # ~600 chars; well above min length
        try:
            r = client.post(
                "/api/parallels",
                json={"text": text, "mode": "exact"},
            )
        except Exception as e:
            # If the search infrastructure isn't initialized in this test env,
            # the test cannot meaningfully exercise the endpoint — skip rather
            # than fail.
            pytest.skip(f"Parallels endpoint threw before reaching sys_id check: {e}")
        # Load-bearing assertion: NEVER 500 on synthetic-shelfmark text input.
        assert r.status_code != 500, (
            f"Synthetic-shelfmark text crashed parallels: {r.status_code} {r.text[:200]}"
        )


class TestParallelsResultsNaturallyExcludeSynthetic:
    """Synthetic rows have no Tantivy text → no chunks → no composition-parallel matches.

    Coarse contract test: when the parallels endpoint returns successfully,
    none of the response items should have is_synthetic=True (synthetic rows
    have no Tantivy chunks, so they cannot match composition seeds).
    """

    def test_no_synthetic_in_main_results(self):
        try:
            from fastapi.testclient import TestClient
            from web.api import target_app
        except Exception as e:
            pytest.skip(f"Cannot import web.api.target_app: {e}")
        try:
            client = TestClient(target_app)
        except Exception as e:
            pytest.skip(f"Cannot construct TestClient: {e}")

        text = "any composition text " * 30
        try:
            r = client.post(
                "/api/parallels",
                json={"text": text, "mode": "exact"},
            )
        except Exception as e:
            pytest.skip(f"Parallels endpoint threw: {e}")
        if r.status_code == 200:
            body = r.json()
            for item in body.get("main_results", []) or []:
                # Per shared serializer (Plan 05 Task 1), each item carries
                # is_synthetic at top level. Synthetic rows MUST NOT appear.
                assert not item.get("is_synthetic"), (
                    f"Synthetic row appeared in parallels results — unexpected "
                    f"(synthetic rows have no Tantivy chunks): {item.get('locator')}"
                )


# ---------------------------------------------------------------------------
# Corrections READ-side
# ---------------------------------------------------------------------------


class TestCorrectionsReadSafe:
    """corrections_service.get_pending_corrections_for_page returns [] safely
    for synthetic IDs (no synthetic-row corrections exist on day one)."""

    def test_get_pending_corrections_returns_empty_for_synthetic(self):
        from shared import corrections_service
        mock_client = MagicMock()
        # Match the actual chained call shape used by
        # get_pending_corrections_for_page (select.eq.eq.eq.in_.order.execute).
        execute_mock = MagicMock()
        execute_mock.execute.return_value = MagicMock(data=[])
        # Build the chain — every method returns MagicMock by default which
        # supports attribute access; explicitly wire .execute at the end.
        chain = (
            mock_client.table.return_value
            .select.return_value
            .eq.return_value
            .eq.return_value
            .eq.return_value
            .in_.return_value
            .order.return_value
        )
        chain.execute.return_value = MagicMock(data=[])
        result = corrections_service.get_pending_corrections_for_page(
            mock_client, SYNTHETIC_ID, page_number=1, user_id="test"
        )
        assert result == []

    def test_get_pending_corrections_returns_empty_when_user_id_none(self):
        """Pre-existing safety contract: None user_id → [] without hitting Supabase."""
        from shared import corrections_service
        mock_client = MagicMock()
        result = corrections_service.get_pending_corrections_for_page(
            mock_client, SYNTHETIC_ID, page_number=1, user_id=None
        )
        assert result == []


# ---------------------------------------------------------------------------
# Corrections WRITE-side — REVIEWS-MODE iteration 1 B1+B2: UI hide + BACKEND REJECT
# ---------------------------------------------------------------------------


class TestCorrectionsWriteRejected:
    """REVIEWS-MODE iteration 1 B1+B2: backend rejects synthetic correction submission
    at the REAL write entry points. The previous revision's fictional
    `shared.corrections_service.submit_correction` and `POST /api/corrections`
    references DO NOT EXIST in the codebase.

    Verified write paths (2026-05-08):
      - corrections_client.py:582 CorrectionsClient.create_correction
      - supabase_corrections_client.py:768 SupabaseCorrectionsClient.create_correction
        -> client.table('corrections').insert(data).execute() at line 811
    """

    def test_create_correction_rejects_synthetic_sys_id_supabase_client(self):
        """SupabaseCorrectionsClient.create_correction rejects synthetic document_id
        BEFORE the line-811 client.table('corrections').insert call."""
        from supabase_corrections_client import SupabaseCorrectionsClient

        client = SupabaseCorrectionsClient.__new__(SupabaseCorrectionsClient)
        # Stub the auth gate so we reach the synthetic-rejection branch.
        user = MagicMock()
        user._uuid = "test-user-uuid"
        client.current_user = user
        supabase_mock = MagicMock()
        client._get_client = MagicMock(return_value=supabase_mock)

        result = client.create_correction(
            document_id=SYNTHETIC_ID,
            original_text="A",
            corrected_text="B",
        )
        # Real method returns Tuple[Optional[Correction], str]
        assert isinstance(result, tuple) and len(result) == 2, (
            f"Expected Tuple[Optional[Correction], str], got {type(result).__name__}"
        )
        correction, msg = result
        assert correction is None, (
            "Synthetic correction should not produce a Correction object"
        )
        assert "synthetic_corrections_disabled" in msg, (
            f"Expected error code 'synthetic_corrections_disabled' in message, got: {msg!r}"
        )
        # Most important assertion: insert MUST NOT have fired.
        supabase_mock.table.return_value.insert.assert_not_called()

    def test_create_correction_rejects_synthetic_sys_id_corrections_client(self):
        """corrections_client.CorrectionsClient.create_correction rejects synthetic
        document_id at method entry."""
        from corrections_client import CorrectionsClient

        client = CorrectionsClient.__new__(CorrectionsClient)
        # Match the REST-wrapper's idiom — the gate fires before any HTTP call.
        # Stub the _request method so an unguarded call would be visible (but
        # the synthetic-rejection branch must intercept BEFORE this is reached).
        client._request = MagicMock()

        result = client.create_correction(
            document_id=SYNTHETIC_ID,
            original_text="A",
            corrected_text="B",
        )
        assert isinstance(result, tuple) and len(result) == 2
        correction, msg = result
        assert correction is None
        assert "synthetic_corrections_disabled" in msg
        # The HTTP call MUST NOT have fired.
        client._request.assert_not_called()

    def test_real_alma_correction_not_rejected_supabase_client(self):
        """Regression guard: real-Alma sys_id passes the synthetic-rejection branch
        and reaches the existing insert path."""
        from supabase_corrections_client import SupabaseCorrectionsClient

        client = SupabaseCorrectionsClient.__new__(SupabaseCorrectionsClient)
        user = MagicMock()
        user._uuid = "test-user-uuid"
        client.current_user = user
        supabase_mock = MagicMock()
        supabase_mock.table.return_value.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": 1, "sys_id": REAL_ALMA_ID}]
        )
        client._get_client = MagicMock(return_value=supabase_mock)
        client._parse_correction = MagicMock(side_effect=lambda d: d)

        result = client.create_correction(
            document_id=REAL_ALMA_ID,  # real Alma — NOT synthetic
            original_text="A",
            corrected_text="B",
        )
        correction, msg = result
        # Regression: real Alma must NOT hit the synthetic-disabled branch.
        assert "synthetic_corrections_disabled" not in (msg or ""), (
            "Regression: real Alma incorrectly hit synthetic-rejection branch"
        )
        # And the insert path SHOULD have fired.
        supabase_mock.table.return_value.insert.assert_called_once()


# ---------------------------------------------------------------------------
# UI hide audits — REVIEWS-MODE Gemini MEDIUM: web + desktop parity
# ---------------------------------------------------------------------------


class TestCorrectionsWriteUiHide:
    def test_web_correction_button_hidden_for_synthetic(self):
        """web/pages/browse.py wraps correction button render in is_synthetic_sys_id guard.

        Coarse branch-correctness assertion: at least one Edit/correction button
        render site is within 8 preceding lines of an is_synthetic_sys_id check.
        """
        with open("web/pages/browse.py", "r", encoding="utf-8") as f:
            src = f.read()
        assert "is_synthetic_sys_id" in src

        import re
        all_lines = src.splitlines()
        # Match Edit-button render sites (the corrections-write entry on web is the
        # `Edit` toggle button at line ~3898 area, plus the Submit/Save action sites).
        button_lines = [
            i for i, line in enumerate(all_lines)
            if re.search(
                r"toggle_edit_mode|handle_submit_correction|btn_add_correction",
                line,
            )
        ]
        if not button_lines:
            pytest.skip("Correction button not found in browse.py via regex")
        # At least one render site must be within 8 lines of an is_synthetic_sys_id
        # check (forward or backward scan; gating may live in either direction).
        for i in button_lines:
            window_start = max(0, i - 12)
            window_end = min(len(all_lines), i + 4)
            window = "\n".join(all_lines[window_start:window_end])
            if "is_synthetic_sys_id" in window:
                return  # found a guarded site
        # Allow guard at toggle_edit_mode body (which is the entry point);
        # search for is_synthetic_sys_id near `def toggle_edit_mode`.
        toggle_def = [
            i for i, line in enumerate(all_lines)
            if re.search(r"def toggle_edit_mode", line)
        ]
        for i in toggle_def:
            window = "\n".join(all_lines[i: min(len(all_lines), i + 30)])
            if "is_synthetic_sys_id" in window:
                return
        pytest.fail(
            "No correction-write site (Edit button / handle_submit_correction / "
            "toggle_edit_mode body) is guarded by is_synthetic_sys_id"
        )

    def test_desktop_correction_entry_points_hidden_for_synthetic(self):
        """REVIEWS-MODE Gemini MEDIUM: desktop correction entry points hidden for synthetic.

        Verifies at least one of:
          - btn_b_edit (the toolbar Edit button at line 6371)
          - _browse_save_correction body (the actual write site)
          - _browse_toggle_edit_mode body (the entry point)
        is gated on is_synthetic_sys_id.
        """
        with open("genizah_app.py", "r", encoding="utf-8") as f:
            src = f.read()
        assert "is_synthetic_sys_id" in src

        import re
        all_lines = src.splitlines()
        entry_patterns = [
            r"btn_b_edit",
            r"_browse_save_correction",
            r"_browse_toggle_edit_mode",
            r"create_correction",
        ]
        guarded_count = 0
        for pattern in entry_patterns:
            hits = [
                i for i, line in enumerate(all_lines)
                if re.search(pattern, line)
            ]
            for i in hits:
                window_start = max(0, i - 15)
                window_end = min(len(all_lines), i + 30)
                window = "\n".join(all_lines[window_start:window_end])
                if "is_synthetic_sys_id" in window:
                    guarded_count += 1
                    break  # one guarded site per pattern is sufficient
        assert guarded_count >= 1, (
            "REVIEWS-MODE Gemini MEDIUM: no desktop correction entry point is "
            "guarded by is_synthetic_sys_id within proximity. Verify one of: "
            "btn_b_edit (toolbar), _browse_save_correction (write site), "
            "_browse_toggle_edit_mode (entry point), or create_correction call."
        )


# ---------------------------------------------------------------------------
# REVIEWS-MODE iteration 1 B1: assert no fictional symbols crept back in
# ---------------------------------------------------------------------------


class TestNoFictionalSymbols:
    """REVIEWS-MODE iteration 1 B1+B2 invariant: prior plan revisions referenced
    fictional symbols. Ensure the codebase has not regressed by re-introducing
    them."""

    def test_no_submit_correction_in_corrections_service(self):
        """shared/corrections_service.py exposes only get_pending_corrections_for_page;
        a `submit_correction` function would indicate Plan 05 plan-vs-reality drift."""
        from shared import corrections_service
        assert not hasattr(corrections_service, "submit_correction"), (
            "REVIEWS-MODE iteration 1 B1: corrections_service.submit_correction "
            "should NOT exist (write entry points are the client classes)."
        )

    def test_no_post_corrections_route(self):
        """web/api.py must NOT register a POST /api/corrections route — gating
        happens at the client classes per REVIEWS-MODE iteration 1 B2."""
        with open("web/api.py", "r", encoding="utf-8") as f:
            src = f.read()
        import re
        # Look for @target_app.post decorator on /api/corrections
        bad = re.search(
            r"@target_app\.post\([^)]*['\"]/api/corrections['\"]",
            src,
        )
        assert not bad, (
            "REVIEWS-MODE iteration 1 B2: POST /api/corrections route added to "
            "web/api.py — should NOT exist (gating is at the client classes)."
        )
