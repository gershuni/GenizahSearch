# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-2: LOCAL sys_id namespace guarantees.

Real implementation: shared/local_sys_id.py (Wave 1, Plan 95-02).
All tests raise NotImplementedError until Plan 95-02 ships.
"""
import pytest

try:
    from shared.local_sys_id import is_local_sys_id  # noqa: F401
except ImportError:
    pytest.skip(
        "Wave 0 stub — shared.local_sys_id not yet implemented (Plan 95-02 Wave 1)",
        allow_module_level=True,
    )



class TestIsLocalSysId:
    def test_is_local_sys_id_golden(self):
        raise NotImplementedError(
            "Wave 0 stub for REQ-2 — implemented in Wave 1 plan 95-02"
        )

    def test_is_local_sys_id_negative(self):
        raise NotImplementedError(
            "Wave 0 stub for REQ-2 — implemented in Wave 1 plan 95-02"
        )

    def test_machine_id_always_8_digits(self):
        raise NotImplementedError(
            "Wave 0 stub for D-19 machine_id digit width — implemented in Wave 1 plan 95-02"
        )

    def test_content_hash_always_8_digits(self):
        raise NotImplementedError(
            "Wave 0 stub for D-19 content_hash digit width — implemented in Wave 1 plan 95-02"
        )


class TestNoIntCoercion:
    """AST lint: shared/local_sys_id.py must not pass int to is_local_sys_id
    (mirrors test_synthetic_sys_id.py::TestNoIntCoercion pattern).
    ALLOWLIST = {'shared/local_sys_id.py', 'tests/test_local_sys_id_namespace.py'}
    """

    def test_no_int_coercion(self):
        raise NotImplementedError(
            "Wave 0 stub for int-coercion AST lint — implemented in Wave 1 plan 95-02"
        )
