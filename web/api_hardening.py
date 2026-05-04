"""Web-layer hardening primitives for Phase 78 search-helper API.

This module is the cross-cutting hardening shell consumed by:
- POST /api/search (Phase 78, Plan 03)
- GET /api/browse (Phase 79)
- POST /api/parallels (Phase 80)

Concern #2 fix (78-REVIEWS.md): this module no longer installs exception
handlers globally on the FastAPI app. Instead it exports `wrap_endpoint`
and `_build_envelope_response` helpers that Plan 03's endpoint uses inside
its own try/except. Legacy /api/* routes keep FastAPI's default behavior.

Concern #3 fix: APIError lives in shared/api_errors.py. This module
re-exports it so legacy callers can keep `from web.api_hardening import APIError`.

R2-#4 — locked algorithm spec for _resolve_rate_limit_key (verbatim):

    Walk the X-Forwarded-For entries from right to left. Skip entries that
    are in _TRUSTED_PROXIES. Return the first non-trusted entry encountered.
    If no non-trusted entry exists, return request.client.host. If the
    direct peer is itself NOT in _TRUSTED_PROXIES, ignore the XFF header
    entirely and return the direct peer (untrusted peers cannot inject XFF
    claims).
"""

import os
import time
import math
import hmac
import hashlib
import logging
import threading
import queue
import secrets
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.requests import Request
from pydantic import ValidationError as PydanticValidationError
import requests

# Concern #3: APIError lives in shared/api_errors.py; re-export for legacy import paths.
from shared.api_errors import APIError, ERROR_CODES, WARNING_CODES

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants.
# ---------------------------------------------------------------------------

# D-03: loopback set used by both _is_loopback_request and the trusted-proxy
# defaults. RFC1918 ranges (10.*, 192.168.*, 172.16-31.*) are deliberately
# NOT included — "localhost-only" means the box itself, not the LAN.
LOOPBACK_IPS = frozenset({'127.0.0.1', '::1'})

POSTHOG_HOST = 'https://eu.i.posthog.com'
POSTHOG_CAPTURE_URL = f'{POSTHOG_HOST}/capture'

# Concern #5: bucket eviction TTL — empty deques older than this are dropped
# on next check(). Default 1 hour; override via env for tests / tuning.
RATE_LIMIT_BUCKET_TTL = float(os.environ.get('RATE_LIMIT_BUCKET_TTL', '3600'))


# Concern #1: trusted-proxy set, env-overridable via API_TRUSTED_PROXIES.
# Single-nginx production: only loopback (request.client.host == 127.0.0.1).
def _load_trusted_proxies() -> frozenset:
    raw = os.environ.get('API_TRUSTED_PROXIES', '').strip()
    if not raw:
        return LOOPBACK_IPS
    items = {p.strip() for p in raw.split(',') if p.strip()}
    return frozenset(items | LOOPBACK_IPS)


_TRUSTED_PROXIES = _load_trusted_proxies()


# ---------------------------------------------------------------------------
# Concern #1 — TWO distinct IP-resolution helpers.
# ---------------------------------------------------------------------------

def _resolve_rate_limit_key(request: Request) -> str:
    """Return the IP used as the rate-limit bucket key.

    Concern #1 (78-REVIEWS.md): behind nginx, request.client.host collapses
    to 127.0.0.1 for every external request. This helper consults
    X-Forwarded-For ONLY when the direct peer is in _TRUSTED_PROXIES. For
    untrusted peers, XFF is ignored entirely (untrusted peers cannot inject
    XFF claims).

    R2-#4 — locked algorithm (verbatim from module docstring):

        Walk the X-Forwarded-For entries from right to left. Skip entries
        that are in _TRUSTED_PROXIES. Return the first non-trusted entry
        encountered. If no non-trusted entry exists, return
        request.client.host. If the direct peer is itself NOT in
        _TRUSTED_PROXIES, ignore the XFF header entirely and return the
        direct peer.

    Concrete consequence: with the default _TRUSTED_PROXIES = LOOPBACK_IPS,
    every entry in XFF='client.ip, hop1.ip, hop2.ip' is non-trusted, so the
    right-most entry ('hop2.ip') wins.
    """
    if not request.client or not request.client.host:
        return 'unknown'
    direct = request.client.host.strip()
    if direct not in _TRUSTED_PROXIES:
        return direct
    xff_raw = request.headers.get('x-forwarded-for', '') or ''
    if not xff_raw.strip():
        return direct
    entries = [e.strip() for e in xff_raw.split(',') if e.strip()]
    if not entries:
        return direct
    for entry in reversed(entries):
        if entry not in _TRUSTED_PROXIES:
            return entry
    return direct


def _is_loopback_request(request: Request) -> bool:
    """Return True iff request originates from the local box.

    Concern #4 fix (78-REVIEWS.md): drop the .split(',')[0] "first hop only"
    rule entirely. Every XFF entry (if present) MUST be loopback.

    Algorithm:
      - direct peer NOT in LOOPBACK_IPS → False (regardless of XFF).
      - direct peer in LOOPBACK_IPS AND no XFF → True.
      - direct peer in LOOPBACK_IPS AND XFF present → True iff EVERY entry
        in LOOPBACK_IPS.

    Spoof case (concern #4): peer=127.0.0.1, XFF='127.0.0.1, 203.0.113.5'
    MUST return False. RFC1918 ranges (10.*, 192.168.*, 172.16-31.*) are
    NOT loopback per D-03.
    """
    if not request.client or not request.client.host:
        return False
    direct = request.client.host.strip()
    if direct not in LOOPBACK_IPS:
        return False
    xff_raw = request.headers.get('x-forwarded-for', '') or ''
    if not xff_raw.strip():
        return True
    entries = [e.strip() for e in xff_raw.split(',') if e.strip()]
    return all(e in LOOPBACK_IPS for e in entries)


# ---------------------------------------------------------------------------
# RateLimiter — sliding 60s window with TTL eviction (D-01, Concern #5).
# ---------------------------------------------------------------------------

class RateLimiter:
    """Per-IP sliding 60s window with bucket eviction.

    Concern #5: _buckets dict had no eviction policy. This version tracks
    last_seen per bucket; check() prunes empty buckets older than
    RATE_LIMIT_BUCKET_TTL.

    check(client_ip) raises APIError('rate_limited', http_status=429,
    headers={'Retry-After': N}) when the per-IP request count in the
    trailing 60s window has reached the limit. The Retry-After value is
    computed from the oldest in-window entry — honest backpressure, NOT
    the fixed-window puzzle limiter pattern.
    """

    def __init__(self, default_limit: int = 30):
        # Bucket value: (deque[float], last_seen: float).
        self._buckets: dict[str, tuple[deque, float]] = {}
        self._lock = threading.Lock()
        self._default_limit = default_limit

    def _current_limit(self) -> int:
        try:
            v = int(os.environ.get('SEARCH_API_RATE_LIMIT', str(self._default_limit)))
            return max(1, v)
        except ValueError:
            return self._default_limit

    def _evict_stale(self, now: float) -> None:
        """Concern #5: drop empty buckets older than RATE_LIMIT_BUCKET_TTL.

        Sweep semantics:
          1. For every bucket, prune the deque against the active 60s window
             cutoff. Buckets that haven't been touched since the last sweep
             may still hold expired timestamps — pruning them here is what
             makes the "empty AND stale" check below catch them.
          2. Evict any bucket whose deque is now empty AND whose last_seen
             is older than RATE_LIMIT_BUCKET_TTL. Active buckets (non-empty
             deque) are NEVER evicted, even if they crossed TTL.
        """
        ttl = RATE_LIMIT_BUCKET_TTL
        cutoff = now - 60.0
        stale: list[str] = []
        for ip, (dq, last_seen) in self._buckets.items():
            while dq and dq[0] < cutoff:
                dq.popleft()
            if not dq and (now - last_seen) > ttl:
                stale.append(ip)
        for ip in stale:
            del self._buckets[ip]

    def check(self, client_ip: str) -> None:
        """Record a request and raise APIError if over the limit.

        On success: appends `now` to the per-IP deque, returns None.
        On limit hit: raises APIError('rate_limited', http_status=429,
            headers={'Retry-After': str(N)}) where N >= 1.
        """
        limit = self._current_limit()
        now = time.time()
        cutoff = now - 60.0
        with self._lock:
            entry = self._buckets.get(client_ip)
            if entry is None:
                dq: deque = deque()
                self._buckets[client_ip] = (dq, now)
            else:
                dq, _ = entry
                self._buckets[client_ip] = (dq, now)
            while dq and dq[0] < cutoff:
                dq.popleft()
            self._evict_stale(now)
            if len(dq) >= limit:
                oldest = dq[0]
                retry_after = max(1, math.ceil(60.0 - (now - oldest)))
                raise APIError(
                    'rate_limited',
                    'Too many requests',
                    http_status=429,
                    headers={'Retry-After': str(retry_after)},
                )
            dq.append(now)

    def reset_for_tests(self) -> None:
        """R2-#2: clear all bucket state. Called from autouse pytest fixtures
        so tests don't observe state from prior tests' rate-limit consumption.

        NEVER call this from production code paths.
        """
        with self._lock:
            self._buckets.clear()


# ---------------------------------------------------------------------------
# Mode gate — re-reads SEARCH_API_MODE per call (D-02).
# ---------------------------------------------------------------------------

def enforce_mode_gate(request: Request) -> None:
    """Raise APIError if SEARCH_API_MODE forbids this request.

    D-02: env re-read per call so production can flip the flag without
    restart. D-03: localhost-only uses _is_loopback_request (every-XFF-entry
    must be loopback per Concern #4). D-04: disabled is HTTP 503.
    """
    mode = (os.environ.get('SEARCH_API_MODE', 'open') or 'open').strip().lower()
    if mode == 'disabled':
        raise APIError('disabled', 'Search API disabled', http_status=503)
    if mode == 'localhost-only':
        if not _is_loopback_request(request):
            raise APIError(
                'localhost_only',
                'Endpoint restricted to localhost',
                http_status=403,
            )


# ---------------------------------------------------------------------------
# Concern #2 — per-endpoint envelope rendering (replaces global handlers).
# ---------------------------------------------------------------------------

def _build_envelope_response(*args) -> JSONResponse:
    """Build a Phase 78 error envelope from APIError or validation errors.

    Concern #2: invoked from INSIDE endpoint bodies via `wrap_endpoint`,
    NOT installed as a global FastAPI exception handler. Legacy /api/*
    routes keep their default behavior (e.g., 422 dump on validation
    failure).

    Signatures (both supported):
        _build_envelope_response(exc)
        _build_envelope_response(request, exc)

    The single-arg form is what the helper-level tests call. The two-arg
    form is what wrap_endpoint uses internally (request reserved for future
    enrichment, e.g. correlation IDs).
    """
    if len(args) == 1:
        exc = args[0]
    elif len(args) == 2:
        _request, exc = args
    else:
        raise TypeError(
            f"_build_envelope_response takes 1 or 2 positional args, got {len(args)}"
        )

    if isinstance(exc, APIError):
        body = {'error': {'code': exc.code, 'message': exc.message}}
        return JSONResponse(
            status_code=exc.http_status,
            content=body,
            headers=exc.headers or None,
        )
    if isinstance(exc, (RequestValidationError, PydanticValidationError)):
        try:
            errs = list(exc.errors() if hasattr(exc, 'errors') else [])
        except Exception:
            errs = []
        if not errs:
            errs = [{'msg': 'invalid request', 'loc': ()}]
        first = errs[0]
        message = str(first.get('msg', 'invalid request'))
        fields: list[str] = []
        for e in errs:
            loc = e.get('loc') or []
            parts = [str(p) for p in loc if p != 'body']
            if parts:
                fields.append('.'.join(parts))
        body = {'error': {
            'code': 'invalid_request',
            'message': message,
            'fields': fields,
        }}
        return JSONResponse(status_code=400, content=body)
    return JSONResponse(
        status_code=500,
        content={'error': {'code': 'internal_error', 'message': 'Internal error'}},
    )


def wrap_endpoint(*, endpoint_name: str):
    """Decorator that owns the try/except/finally + envelope + PostHog
    capture pattern shared by all Phase 78+ search-helper endpoints.

    R2-#6 fix (78-REVIEWS.md): previously this was a no-op marker; the real
    reusable piece was _build_envelope_response. Phases 79/80 had to
    hand-roll the same try/except/finally structure in each endpoint,
    creating drift risk. This version OWNS the boilerplate — future
    endpoints get the full structure for free.

    Usage (in Plan 03's web/search_api.py):

        @wrap_endpoint(endpoint_name='search')
        async def _search_body(request: Request, *, captured_state: dict):
            # Pure handler body. Raise APIError or PydanticValidationError
            # on errors. captured_state is a dict the wrapper passes in for
            # the finally-block PostHog capture (mode, result_count, etc.).
            captured_state['mode'] = req.mode
            captured_state['result_count'] = len(results)
            return envelope

    The wrapper:
      - Resolves client_ip via _resolve_rate_limit_key.
      - Catches APIError → routes through _build_envelope_response.
      - Catches RequestValidationError / PydanticValidationError → routes
        through _build_envelope_response with code='invalid_request' AND
        fires PostHog 'invalid_request' capture (R2-#6 + Concern #12).
      - Catches any other Exception → logger.exception + 500 envelope.
      - finally: capture_api_event with status_code/error_code pinned by
        the branch above.
    """
    def _decorator(handler: Callable[..., Awaitable]) -> Callable[..., Awaitable]:
        async def _wrapped(request: Request):
            t0 = time.monotonic()
            client_ip = _resolve_rate_limit_key(request)
            captured_state: dict = {
                'mode': None,
                'result_count': None,
                # 81A D-08 — uniform PostHog event shape across endpoints.
                # browse + parallels handlers may overwrite if they ever start
                # accepting a search_mode field; today both leave them at
                # None/0 (set explicitly in the handler bodies for clarity).
                'search_mode_value': None,
                'responsa_options_count': 0,
            }
            status_code = 200
            error_code: Optional[str] = None
            try:
                result = await handler(
                    request,
                    captured_state=captured_state,
                )
                return result
            except APIError as exc:
                status_code = exc.http_status
                error_code = exc.code
                return _build_envelope_response(request, exc)
            except (RequestValidationError, PydanticValidationError) as exc:
                status_code = 400
                error_code = 'invalid_request'
                return _build_envelope_response(request, exc)
            except Exception:
                logger.exception('%s endpoint unhandled exception', endpoint_name)
                status_code = 500
                error_code = 'internal_error'
                fallback = APIError('internal_error', 'Internal error', http_status=500)
                return _build_envelope_response(request, fallback)
            finally:
                try:
                    elapsed = time.monotonic() - t0
                    capture_api_event(
                        endpoint=endpoint_name,
                        mode=captured_state.get('mode'),
                        latency_seconds=elapsed,
                        result_count=captured_state.get('result_count'),
                        status_code=status_code,
                        error_code=error_code,
                        client_ip=client_ip,
                        # 81A D-08 — plumb the two new properties from the
                        # captured_state contract so wrap_endpoint-decorated
                        # endpoints (browse, parallels) emit a uniform shape.
                        search_mode_value=captured_state.get('search_mode_value'),
                        responsa_options_count=captured_state.get('responsa_options_count', 0),
                    )
                except Exception:
                    logger.warning(
                        'capture_api_event failed in %s finally', endpoint_name,
                    )
        return _wrapped
    return _decorator


# ---------------------------------------------------------------------------
# IP-hash with persistent salt (D-11, Concern #11).
# ---------------------------------------------------------------------------

def _resolve_posthog_ip_salt() -> str:
    """Read POSTHOG_IP_SALT from env. If absent, auto-generate AND persist.

    Concern #11 (Gemini LOW): on some Windows environments os.replace can
    behave inconsistently if target file is being read; os.chmod is a no-op
    on Windows. Both are documented as best-effort here. Production should
    set POSTHOG_IP_SALT explicitly so hashes survive restarts.
    """
    env_salt = os.environ.get('POSTHOG_IP_SALT', '').strip()
    if env_salt:
        return env_salt
    salt_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_secrets')
    salt_path = os.path.join(salt_dir, 'posthog_ip_salt')
    try:
        if os.path.exists(salt_path):
            with open(salt_path, 'r', encoding='utf-8') as f:
                v = f.read().strip()
                if v:
                    return v
        os.makedirs(salt_dir, exist_ok=True)
        new_salt = secrets.token_hex(32)
        tmp_path = salt_path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(new_salt)
        os.replace(tmp_path, salt_path)  # Concern #11: best-effort on Windows.
        try:
            os.chmod(salt_path, 0o600)  # Concern #11: no-op on Windows.
        except OSError:
            pass
        logger.warning(
            "POSTHOG_IP_SALT not set; auto-generated and persisted to %s. "
            "Production should set POSTHOG_IP_SALT explicitly.", salt_path,
        )
        return new_salt
    except OSError as exc:
        logger.warning(
            "POSTHOG_IP_SALT auto-persist failed (%s); using process-local salt.",
            exc,
        )
        return secrets.token_hex(32)


_POSTHOG_IP_SALT = _resolve_posthog_ip_salt()


def hash_ip(ip: str) -> str:
    """16-hex-char HMAC-SHA256 of an IP using the persistent salt (D-11)."""
    if not ip:
        ip = 'unknown'
    return hmac.new(
        _POSTHOG_IP_SALT.encode('utf-8'),
        ip.encode('utf-8'),
        hashlib.sha256,
    ).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Bucket helpers (D-12) — verbatim string labels sent to PostHog.
# ---------------------------------------------------------------------------

def latency_bucket(elapsed_seconds: float) -> str:
    if elapsed_seconds < 0.1:
        return 'lt_100ms'
    if elapsed_seconds < 0.5:
        return 'lt_500ms'
    if elapsed_seconds < 2.0:
        return 'lt_2s'
    if elapsed_seconds < 10.0:
        return 'lt_10s'
    return 'gte_10s'


def result_count_bucket(n: int) -> str:
    if n <= 0:
        return 'zero'
    if n <= 10:
        return 'count_1_10'
    if n <= 50:
        return 'count_11_50'
    return 'count_51_200'


# ---------------------------------------------------------------------------
# PostHog server-side capture with drop counter (D-10..D-14, Concern #9).
# ---------------------------------------------------------------------------

_event_queue: queue.Queue = queue.Queue(maxsize=10000)
_drain_thread_started = threading.Event()
_request_counter = 0
_request_counter_lock = threading.Lock()

# Concern #9: count silent drops on queue.Full so operators can diagnose
# observability gaps under load. No public endpoint — exposed via
# get_dropped_event_count() for diagnostics / future health endpoint.
_dropped_events: int = 0
_dropped_events_lock = threading.Lock()


def get_dropped_event_count() -> int:
    """Return the number of PostHog events dropped because the queue was full.

    Concern #9 (78-REVIEWS.md): without this counter, queue.Full drops are
    silent — hard to diagnose under load. This counter is monotonic; it
    does not reset across the process lifetime.
    """
    with _dropped_events_lock:
        return _dropped_events


def _drain_posthog_queue() -> None:
    """Daemon thread: drain _event_queue, POST each event to PostHog."""
    api_key = os.environ.get('POSTHOG_API_KEY', '').strip()
    while True:
        try:
            event = _event_queue.get(timeout=60)
        except queue.Empty:
            continue
        if not api_key:
            continue
        try:
            payload = {
                'api_key': api_key,
                'event': event['event'],
                'distinct_id': event['distinct_id'],
                'properties': event['properties'],
                'timestamp': event['timestamp'],
            }
            requests.post(POSTHOG_CAPTURE_URL, json=payload, timeout=2.0)
        except Exception:
            pass


def _start_drain_thread_once() -> None:
    if _drain_thread_started.is_set():
        return
    _drain_thread_started.set()
    t = threading.Thread(
        target=_drain_posthog_queue,
        name='posthog-api-drain',
        daemon=True,
    )
    t.start()


def _should_sample() -> bool:
    """Atomic-ish counter mod N. Default N=1 → every request. D-13."""
    global _request_counter
    try:
        n = max(1, int(os.environ.get('SEARCH_API_POSTHOG_SAMPLE_N', '1')))
    except ValueError:
        n = 1
    with _request_counter_lock:
        _request_counter += 1
        idx = _request_counter
    return (idx % n) == 0


def capture_api_event(
    *,
    endpoint: str,
    mode: Optional[str],
    latency_seconds: float,
    result_count: Optional[int],
    status_code: int,
    error_code: Optional[str],
    client_ip: str,
    # Phase 81A D-08 additions: literal `search_mode` enum value (one of
    # exact|variants|responsa|title|shelfmark, or None when the field is
    # structurally absent/invalid) and count of True flags in the validated
    # ResponsaOptions (0 when search_mode != 'responsa' or options omitted).
    search_mode_value: Optional[str] = None,
    responsa_options_count: int = 0,
) -> None:
    """Enqueue a search_api_request PostHog event. Never blocks; never raises.

    Concern #9 fix: drops are counted via _dropped_events, exposed by
    get_dropped_event_count(). Best-effort observability.

    NEVER logged: query, filters, gap, response items, snippets, full text.
    (HARDEN-05.)
    """
    global _dropped_events
    _start_drain_thread_once()
    if not _should_sample():
        return
    try:
        props: dict = {
            'endpoint': endpoint,
            'mode': mode,
            'latency_bucket': latency_bucket(latency_seconds),
            'status_code': status_code,
            'error_code': error_code,
        }
        if result_count is not None:
            props['result_count_bucket'] = result_count_bucket(result_count)
        # Phase 81A D-08 additions — always present (None/0 when not applicable)
        # so PostHog dashboards see a uniform event shape across endpoints.
        props['search_mode_value'] = search_mode_value
        props['responsa_options_count'] = int(responsa_options_count or 0)
        event = {
            'event': 'search_api_request',
            'distinct_id': hash_ip(client_ip),
            'properties': props,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        # NOTE: we look up _event_queue via the module attribute (not as a
        # closure) so test monkeypatch.setattr('web.api_hardening._event_queue', ...)
        # is respected. The local name binds to whatever the module currently
        # holds when this function is called.
        try:
            _event_queue.put_nowait(event)
        except queue.Full:
            with _dropped_events_lock:
                _dropped_events += 1  # Concern #9
    except Exception:
        pass


__all__ = [
    'RateLimiter', 'APIError', 'ERROR_CODES', 'WARNING_CODES',
    'enforce_mode_gate',
    '_resolve_rate_limit_key', '_is_loopback_request',
    'hash_ip', 'capture_api_event', 'get_dropped_event_count',
    'latency_bucket', 'result_count_bucket',
    'wrap_endpoint', '_build_envelope_response',
    'POSTHOG_CAPTURE_URL', 'LOOPBACK_IPS', 'RATE_LIMIT_BUCKET_TTL',
    '_TRUSTED_PROXIES',
]
