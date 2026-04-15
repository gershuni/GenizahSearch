# -*- coding: utf-8 -*-
"""
HMAC-based upload token system for puzzle image cache.

Prevents cache poisoning by ensuring only images fetched through
legitimate cache-miss flows can be written to the server cache.
Tokens are bound to a specific fl_id and expire after 5 minutes.
"""

import hmac
import hashlib
import time
import json
import os

# Secret key for HMAC signing. In production, set PUZZLE_UPLOAD_SECRET env var.
# Falls back to a random key per process (tokens won't survive restarts).
PUZZLE_SECRET = os.environ.get('PUZZLE_UPLOAD_SECRET', os.urandom(32).hex())


def generate_upload_token(fl_id: str, threshold: float, is_cul: bool) -> str:
    """Generate a signed upload token for a specific image cache write.

    Args:
        fl_id: NLI FL ID the token authorizes writing for.
        threshold: Background removal threshold used.
        is_cul: Whether CUL blue mat removal was applied.

    Returns:
        Token string in format "{json_payload}|{hmac_signature}".
    """
    payload = json.dumps({
        'fl_id': fl_id, 'threshold': threshold, 'is_cul': is_cul,
        'exp': int(time.time()) + 300  # 5 min expiry
    }, separators=(',', ':'))
    sig = hmac.new(PUZZLE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}|{sig}"


def verify_upload_token(token: str, fl_id: str) -> bool:
    """Verify an upload token is valid and matches the given fl_id.

    Checks HMAC signature, fl_id binding, and expiry.

    Args:
        token: The token string to verify.
        fl_id: The fl_id the upload claims to be for.

    Returns:
        True if token is valid and authorized for this fl_id.
    """
    try:
        payload_str, sig = token.rsplit('|', 1)
        expected = hmac.new(PUZZLE_SECRET.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        payload = json.loads(payload_str)
        if payload['fl_id'] != fl_id:
            return False
        if payload['exp'] < time.time():
            return False
        return True
    except Exception:
        return False  # Puzzle operation failed; continue with defaults
