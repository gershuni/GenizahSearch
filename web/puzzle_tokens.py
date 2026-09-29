# -*- coding: utf-8 -*-
"""
HMAC-signed upload tokens for the Fragment Puzzle image cache.

A token is handed out with a cache miss from ``GET /api/puzzle_image`` and is
the permission for exactly one follow-up upload to ``POST /api/puzzle_process``
for exactly the same image: the same fl_id, size, threshold, processed flag
and CUL flag. Each token carries a random nonce, can be used once, and expires
after five minutes.

A token says only "this browser just asked for this image and the cache did not
have it". It says nothing about whether the bytes that are later uploaded are
the right picture, so where uploaded bytes may be stored is decided separately
(see ``shared.puzzle_image_service.PuzzleImageService.store_upload``).
"""

import hashlib
import hmac
import json
import os
import secrets
import threading
import time

# Secret key for HMAC signing. In production, set PUZZLE_UPLOAD_SECRET env var.
# Falls back to a random key per process (tokens won't survive restarts).
PUZZLE_SECRET = os.environ.get('PUZZLE_UPLOAD_SECRET', os.urandom(32).hex())

TOKEN_TTL_SECONDS = 300

# Nonces of tokens that have already been used, with the time each one stops
# mattering (its token's expiry). Pruned on every verify, so the set holds at
# most five minutes of uploads.
_used_nonces: dict = {}
_used_nonces_lock = threading.Lock()


def _cache_key_fields(fl_id, size, threshold, processed, is_cul) -> dict:
    """The fields that decide which cache file an upload is written to.

    ``threshold`` is compared as the same one-decimal string the cache file
    name uses, so 30 and 30.0 are the same image and 30.0 and 30.4 are not.
    """
    return {
        'fl_id': str(fl_id),
        'size': int(size),
        'threshold': f"{float(threshold):.1f}",
        'processed': bool(processed),
        'is_cul': bool(is_cul),
    }


def generate_upload_token(fl_id: str, threshold: float, is_cul: bool, *,
                          size: int, processed: bool) -> str:
    """Generate a signed, single-use upload token for one cache entry.

    Args:
        fl_id: NLI FL ID the token authorizes writing for.
        threshold: Background removal threshold of the requested image.
        is_cul: Whether CUL blue mat removal was requested.
        size: Requested image width in pixels.
        processed: Whether the background-removed image was requested.

    Returns:
        Token string in format "{json_payload}|{hmac_signature}".
    """
    payload = dict(_cache_key_fields(fl_id, size, threshold, processed, is_cul))
    payload['nonce'] = secrets.token_hex(16)
    payload['exp'] = int(time.time()) + TOKEN_TTL_SECONDS
    payload_str = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    sig = hmac.new(PUZZLE_SECRET.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
    return f"{payload_str}|{sig}"


def _consume_nonce(nonce: str, exp: float, now: float) -> bool:
    """Mark ``nonce`` used. Return False if it had been used already."""
    with _used_nonces_lock:
        for old in [n for n, until in _used_nonces.items() if until < now]:
            _used_nonces.pop(old, None)
        if nonce in _used_nonces:
            return False
        _used_nonces[nonce] = exp
        return True


def verify_upload_token(token: str, fl_id: str, *, size: int, threshold: float,
                        processed: bool, is_cul: bool) -> bool:
    """Verify an upload token for one specific cache entry, and use it up.

    Checks the HMAC signature, that every cache-key field matches the upload,
    the expiry, and that the token has not been used before. A token that
    passes is consumed: a second upload with it is refused.

    Args:
        token: The token string to verify.
        fl_id, size, threshold, processed, is_cul: The cache entry the upload
            would be written to, after the route has normalized them.

    Returns:
        True if the token is valid, unused and issued for exactly this entry.
    """
    try:
        payload_str, sig = token.rsplit('|', 1)
        expected = hmac.new(PUZZLE_SECRET.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        payload = json.loads(payload_str)
        wanted = _cache_key_fields(fl_id, size, threshold, processed, is_cul)
        for field, value in wanted.items():
            if payload.get(field) != value:
                return False
        now = time.time()
        if payload['exp'] < now:
            return False
        nonce = payload.get('nonce')
        if not isinstance(nonce, str) or not nonce:
            return False
        return _consume_nonce(nonce, float(payload['exp']), now)
    except Exception:
        # Malformed token or bad payload: deny the upload.
        return False
