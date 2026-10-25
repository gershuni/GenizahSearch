"""Pull a usage, error and user report out of PostHog into one Markdown file.

Reads the SHARED PostHog project (id 134161, EU region) that both apps send to:
web events carry ``platform=web``, desktop events ``platform=desktop`` and the
``desktop_`` prefix (docs/guides/TELEMETRY_RUNBOOK.md). Every query is a
read-only HogQL ``SELECT`` sent to ``/api/projects/<id>/query/``.

Needs a PostHog PERSONAL API key (``phx_...``) with the "Query: Read" scope in
``POSTHOG_PERSONAL_API_KEY``. The ``phc_`` key in the apps is write-only and
cannot read anything; it is refused here. Never commit the personal key.

The report names users by email and quotes what they searched for, so it is
written under ``_tmp/`` (git-ignored) by default. Do not commit or publish it.

    python scripts/posthog_report.py                  # last 30 days
    python scripts/posthog_report.py --days 7
    python scripts/posthog_report.py --only errors,users
    python scripts/posthog_report.py --list           # section ids, no network

Exit codes: 0 report written; 1 every query failed; 2 no usable key, or
PostHog refused the key.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import requests

DEFAULT_HOST = 'https://eu.posthog.com'
DEFAULT_PROJECT = '134161'
KEY_ENV = 'POSTHOG_PERSONAL_API_KEY'
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / '_tmp'

REQUEST_TIMEOUT = 180
MAX_RATE_LIMIT_RETRIES = 3
MAX_CELL = 100

GROUPS: dict[str, str] = {
    'overview': 'Overview',
    'traffic': 'Web traffic',
    'search': 'Searches',
    'errors': 'Errors and crashes',
    'users': 'Users',
    'desktop': 'Desktop app',
    'perf': 'Performance',
}


@dataclass(frozen=True)
class Query:
    """One report table. ``sql`` uses the tokens ``{W}`` (the time window as a
    ``WHERE`` condition on ``timestamp``) and ``{DAYS}`` (the window length)."""

    id: str
    group: str
    title: str
    sql: str
    note: str = ''


# The headline numbers, run twice: this window and the one before it.
HEADLINE_SQL = """
SELECT
    uniqIf(person_id, properties.platform = 'web') AS web_visitors,
    uniqIf(person_id, properties.platform = 'desktop') AS desktop_users,
    countIf(event = '$pageview') AS web_pageviews,
    uniq(properties.$session_id) AS web_sessions,
    countIf(event = 'search_executed') AS web_searches,
    countIf(event = 'parallels_search') AS web_parallels_searches,
    countIf(event = 'browse_manuscript') AS web_manuscripts_browsed,
    countIf(event = 'desktop_session_start') AS desktop_sessions,
    countIf(event = 'desktop_search_executed') AS desktop_searches,
    countIf(event = 'search_api_request') AS api_requests_sampled,
    countIf(event = 'mcp_tool_call') AS mcp_tool_calls,
    countIf(event = 'login_success') AS web_logins,
    countIf(event = 'login_failed') AS web_login_failures,
    countIf(event = '$exception') AS web_exceptions,
    countIf(event = 'desktop_crash') AS desktop_crashes,
    countIf(event = 'desktop_prior_crash') AS desktop_native_crashes
FROM events
WHERE {W}
"""

QUERIES: tuple[Query, ...] = (
    # ------------------------------------------------------------------ overview
    Query('platforms', 'overview', 'Events and users by platform', """
SELECT coalesce(properties.platform, '(server / none)') AS platform,
       count() AS events, uniq(person_id) AS users, uniq(distinct_id) AS distinct_ids
FROM events WHERE {W}
GROUP BY platform ORDER BY events DESC LIMIT 20
""", 'Server-side events (API, MCP, circuit breaker) carry no platform; their '
         'distinct_id is a hashed client IP, not a person.'),
    Query('daily', 'overview', 'Daily activity', """
SELECT toDate(timestamp) AS day,
       uniqIf(person_id, properties.platform = 'web') AS web_visitors,
       uniqIf(person_id, properties.platform = 'desktop') AS desktop_users,
       countIf(event = '$pageview') AS pageviews,
       countIf(event = 'search_executed') AS web_searches,
       countIf(event = 'desktop_search_executed') AS desktop_searches,
       countIf(event = 'search_api_request') AS api_sampled,
       countIf(event = 'mcp_tool_call') AS mcp_calls,
       countIf(event IN ('desktop_crash', 'desktop_prior_crash', '$exception')) AS errors
FROM events WHERE {W}
GROUP BY day ORDER BY day LIMIT 400
"""),
    Query('weekly', 'overview', 'Weekly trend, last 12 weeks', """
SELECT toStartOfWeek(timestamp) AS week,
       uniqIf(person_id, properties.platform = 'web') AS web_visitors,
       uniqIf(person_id, properties.platform = 'desktop') AS desktop_users,
       countIf(event = '$pageview') AS pageviews,
       countIf(event IN ('search_executed', 'desktop_search_executed')) AS searches,
       countIf(event = 'mcp_tool_call') AS mcp_calls
FROM events WHERE timestamp >= now() - INTERVAL 12 WEEK
GROUP BY week ORDER BY week LIMIT 20
""", 'Always the last 12 weeks, whatever --days says.'),
    Query('top_events', 'overview', 'Most frequent events', """
SELECT event, coalesce(properties.platform, '(none)') AS platform,
       count() AS n, uniq(person_id) AS users
FROM events WHERE {W}
GROUP BY event, platform ORDER BY n DESC LIMIT 60
"""),
    # ------------------------------------------------------------------ traffic
    Query('web_sessions', 'traffic', 'Web sessions: length, depth and what they did', """
SELECT count() AS sessions,
       round(avg(pages), 2) AS avg_pages,
       quantile(0.5)(dur) AS median_seconds,
       quantile(0.9)(dur) AS p90_seconds,
       round(countIf(pages <= 1) / count() * 100, 1) AS single_page_pct,
       round(countIf(searches > 0) / count() * 100, 1) AS with_search_pct,
       round(countIf(opened > 0) / count() * 100, 1) AS opened_result_pct,
       round(countIf(browsed > 0) / count() * 100, 1) AS browsed_ms_pct,
       round(countIf(parallels > 0) / count() * 100, 1) AS parallels_pct
FROM (
    SELECT properties.$session_id AS sid,
           dateDiff('second', min(timestamp), max(timestamp)) AS dur,
           countIf(event = '$pageview') AS pages,
           countIf(event = 'search_executed') AS searches,
           countIf(event = 'result_opened') AS opened,
           countIf(event = 'browse_manuscript') AS browsed,
           countIf(event = 'parallels_search') AS parallels
    FROM events
    WHERE {W} AND properties.platform = 'web' AND properties.$session_id IS NOT NULL
    GROUP BY sid
)
LIMIT 1
"""),
    Query('top_pages', 'traffic', 'Most viewed pages', """
SELECT properties.$pathname AS path, count() AS views, uniq(person_id) AS visitors
FROM events WHERE {W} AND event = '$pageview'
GROUP BY path ORDER BY views DESC LIMIT 40
"""),
    Query('referrers', 'traffic', 'Where visitors came from', """
SELECT coalesce(properties.$referring_domain, '(none)') AS referrer,
       count() AS pageviews, uniq(person_id) AS visitors
FROM events
WHERE {W} AND event = '$pageview'
  AND coalesce(properties.$referring_domain, '') NOT LIKE '%genizahsearch.com%'
GROUP BY referrer ORDER BY visitors DESC LIMIT 30
"""),
    Query('countries', 'traffic', 'Visitors by country', """
SELECT coalesce(properties.$geoip_country_name, '(unknown)') AS country,
       uniq(person_id) AS visitors, count() AS pageviews
FROM events WHERE {W} AND event = '$pageview'
GROUP BY country ORDER BY visitors DESC LIMIT 30
"""),
    Query('devices', 'traffic', 'Visitors by device, OS and browser', """
SELECT properties.$device_type AS device, properties.$os AS os,
       properties.$browser AS browser, uniq(person_id) AS visitors, count() AS pageviews
FROM events WHERE {W} AND event = '$pageview'
GROUP BY device, os, browser ORDER BY visitors DESC LIMIT 25
"""),
    Query('clicks', 'traffic', 'Most clicked elements (autocapture)', """
SELECT properties.$pathname AS path, substring(toString(properties.$el_text), 1, 60) AS element_text,
       count() AS clicks, uniq(person_id) AS visitors
FROM events
WHERE {W} AND event = '$autocapture' AND properties.$event_type = 'click'
GROUP BY path, element_text ORDER BY clicks DESC LIMIT 40
"""),
    # ------------------------------------------------------------------ search
    Query('web_search_modes', 'search', 'Web searches by mode', """
SELECT properties.mode AS mode, count() AS searches, uniq(person_id) AS users,
       round(countIf(toFloat(properties.result_count) = 0) / count() * 100, 1) AS zero_result_pct,
       quantile(0.5)(toFloat(properties.result_count)) AS median_results,
       quantile(0.5)(toFloat(properties.duration_seconds)) AS median_seconds,
       quantile(0.9)(toFloat(properties.duration_seconds)) AS p90_seconds,
       max(toFloat(properties.duration_seconds)) AS max_seconds
FROM events WHERE {W} AND event = 'search_executed'
GROUP BY mode ORDER BY searches DESC LIMIT 20
"""),
    Query('web_top_queries', 'search', 'Most repeated web searches', """
SELECT properties.query AS query, properties.mode AS mode, count() AS times,
       uniq(person_id) AS users, round(avg(toFloat(properties.result_count)), 0) AS avg_results
FROM events WHERE {W} AND event = 'search_executed'
GROUP BY query, mode ORDER BY times DESC LIMIT 40
""", 'Query text is cut at 100 characters when it is sent.'),
    Query('web_zero_queries', 'search', 'Web searches that found nothing', """
SELECT properties.query AS query, properties.mode AS mode, count() AS times,
       uniq(person_id) AS users, max(timestamp) AS last_seen
FROM events
WHERE {W} AND event = 'search_executed' AND toFloat(properties.result_count) = 0
GROUP BY query, mode ORDER BY times DESC LIMIT 30
"""),
    Query('web_slow_searches', 'search', 'Slowest web searches', """
SELECT timestamp, properties.query AS query, properties.mode AS mode,
       toFloat(properties.duration_seconds) AS seconds, properties.result_count AS results
FROM events WHERE {W} AND event = 'search_executed'
ORDER BY seconds DESC LIMIT 20
"""),
    Query('parallels', 'search', 'Web parallels (composition) searches', """
SELECT properties.mode AS mode, count() AS searches, uniq(person_id) AS users,
       quantile(0.5)(toFloat(properties.word_count)) AS median_words,
       round(countIf(toFloat(properties.result_count) = 0) / count() * 100, 1) AS zero_result_pct,
       quantile(0.5)(toFloat(properties.duration_seconds)) AS median_seconds,
       quantile(0.9)(toFloat(properties.duration_seconds)) AS p90_seconds,
       countIf(toString(properties.is_partial) IN ('true', '1')) AS partial_runs
FROM events WHERE {W} AND event = 'parallels_search'
GROUP BY mode ORDER BY searches DESC LIMIT 20
"""),
    Query('desktop_searches', 'search', 'Desktop searches by mode and scope', """
SELECT properties.search_mode AS mode, properties.corpus_scope AS scope,
       properties.method AS method, count() AS searches, uniq(person_id) AS users
FROM events WHERE {W} AND event = 'desktop_search_executed'
GROUP BY mode, scope, method ORDER BY searches DESC LIMIT 30
""", 'Desktop never sends query text; only opted-in installs report.'),
    Query('desktop_search_buckets', 'search', 'Desktop searches by result count and duration', """
SELECT properties.result_count_bucket AS results, properties.duration_bucket_ms AS duration_ms,
       count() AS searches
FROM events WHERE {W} AND event = 'desktop_search_executed'
GROUP BY results, duration_ms ORDER BY searches DESC LIMIT 40
"""),
    Query('browsed_manuscripts', 'search', 'Most browsed manuscripts (web)', """
SELECT properties.shelfmark AS shelfmark, properties.sys_id AS sys_id,
       count() AS page_views, uniq(person_id) AS visitors
FROM events WHERE {W} AND event = 'browse_manuscript'
GROUP BY shelfmark, sys_id ORDER BY visitors DESC, page_views DESC LIMIT 30
"""),
    Query('api_usage', 'search', 'Public API by endpoint, channel and mode', """
SELECT properties.endpoint AS endpoint, properties.channel AS channel, properties.mode AS mode,
       count() AS requests, uniq(distinct_id) AS clients,
       countIf(toInt(properties.status_code) >= 400) AS errors
FROM events WHERE {W} AND event = 'search_api_request'
GROUP BY endpoint, channel, mode ORDER BY requests DESC LIMIT 30
""", 'API events are SAMPLED before sending, so counts are a fraction of real traffic; '
         'clients are hashed IPs.'),
    Query('mcp_usage', 'search', 'MCP tool calls by tool and client', """
SELECT properties.tool AS tool, properties.client_label AS client, count() AS calls,
       uniq(distinct_id) AS clients, countIf(toInt(properties.status_code) >= 400) AS errors
FROM events WHERE {W} AND event = 'mcp_tool_call'
GROUP BY tool, client ORDER BY calls DESC LIMIT 30
""", 'Sampled like the API events; client is the caller-supplied MCP clientInfo name.'),
    # ------------------------------------------------------------------ errors
    Query('desktop_crashes', 'errors', 'Desktop crashes (Python exceptions)', """
SELECT properties.error_fingerprint AS fingerprint, properties.context AS context,
       properties.app_version AS version, count() AS crashes, uniq(person_id) AS users,
       countIf(toString(properties.is_background_thread) IN ('true', '1')) AS on_bg_thread,
       max(timestamp) AS last_seen
FROM events WHERE {W} AND event = 'desktop_crash'
GROUP BY fingerprint, context, version ORDER BY crashes DESC LIMIT 40
""", 'Fingerprint is exc_type:module:line. Exception messages are never sent.'),
    Query('desktop_native_crashes', 'errors', 'Desktop native crashes (detected on next launch)', """
SELECT properties.fatal_error AS fatal_error, properties.app_version AS version,
       count() AS crashes, uniq(person_id) AS users, max(timestamp) AS last_seen
FROM events WHERE {W} AND event = 'desktop_prior_crash'
GROUP BY fatal_error, version ORDER BY crashes DESC LIMIT 20
"""),
    Query('crash_rate', 'errors', 'Desktop crash rate by version', """
SELECT properties.app_version AS version,
       countIf(event = 'desktop_session_start') AS sessions,
       uniqIf(person_id, event = 'desktop_session_start') AS users,
       countIf(event = 'desktop_crash') AS crashes,
       countIf(event = 'desktop_prior_crash') AS native_crashes,
       round((countIf(event = 'desktop_crash') + countIf(event = 'desktop_prior_crash'))
             / greatest(countIf(event = 'desktop_session_start'), 1) * 100, 1) AS crashes_per_100_sessions
FROM events WHERE {W} AND properties.platform = 'desktop'
GROUP BY version ORDER BY sessions DESC LIMIT 20
"""),
    Query('web_exceptions', 'errors', 'Web browser exceptions', """
SELECT coalesce(toString(properties.$exception_type), toString(properties.$exception_types)) AS type,
       substring(coalesce(toString(properties.$exception_message),
                          toString(properties.$exception_values), ''), 1, 120) AS message,
       properties.$pathname AS path, count() AS n, uniq(person_id) AS visitors,
       max(timestamp) AS last_seen
FROM events WHERE {W} AND event = '$exception'
GROUP BY type, message, path ORDER BY n DESC LIMIT 30
""", 'Empty if browser exception capture is off for the project.'),
    Query('auth', 'errors', 'Sign-in and password recovery', """
SELECT event, coalesce(toString(properties.reason), toString(properties.error_code), '') AS reason,
       coalesce(toString(properties.method), '') AS method,
       count() AS n, uniq(person_id) AS people, max(timestamp) AS last_seen
FROM events
WHERE {W} AND event IN ('login_success', 'login_failed', 'password_recovery_started',
                        'password_recovery_completed', 'password_recovery_failed')
GROUP BY event, reason, method ORDER BY event, n DESC LIMIT 40
"""),
    Query('api_errors', 'errors', 'API and MCP error responses', """
SELECT event, coalesce(toString(properties.endpoint), toString(properties.tool)) AS target,
       properties.status_code AS status, properties.error_code AS error_code,
       count() AS n, max(timestamp) AS last_seen
FROM events
WHERE {W} AND event IN ('search_api_request', 'mcp_tool_call')
  AND toInt(properties.status_code) >= 400
GROUP BY event, target, status, error_code ORDER BY n DESC LIMIT 30
"""),
    Query('reliability', 'errors', 'NLI image breaker and forced reloads by day', """
SELECT toDate(timestamp) AS day,
       countIf(event = 'nli_breaker_opened') AS nli_breaker_opened,
       countIf(event = 'nli_breaker_closed') AS nli_breaker_closed,
       countIf(event = 'reload_scheduled') AS reloads_scheduled,
       uniqIf(person_id, event = 'reload_scheduled') AS visitors_reloaded
FROM events
WHERE {W} AND event IN ('nli_breaker_opened', 'nli_breaker_closed', 'reload_scheduled')
GROUP BY day ORDER BY day LIMIT 400
""", 'reload_scheduled fires when the browser lost the server, usually a restart or deploy.'),
    Query('frustration', 'errors', 'Rage clicks and dead clicks', """
SELECT event, properties.$pathname AS path, count() AS n, uniq(person_id) AS visitors
FROM events WHERE {W} AND event IN ('$rageclick', '$dead_click')
GROUP BY event, path ORDER BY n DESC LIMIT 30
"""),
    # ------------------------------------------------------------------ users
    Query('accounts', 'users', 'Signed-in accounts', """
SELECT count() AS accounts_with_email,
       countIf(created_at >= now() - INTERVAL {DAYS} DAY) AS profiles_created_in_window
FROM persons
WHERE properties.email IS NOT NULL AND toString(properties.email) != ''
LIMIT 1
""", 'A PostHog person gets an email the first time that account signs in on the web.'),
    Query('platform_overlap', 'users', 'Signed-in users: web, desktop or both', """
SELECT countIf(w > 0 AND d = 0) AS web_only, countIf(d > 0 AND w = 0) AS desktop_only,
       countIf(w > 0 AND d > 0) AS both, count() AS total
FROM (
    SELECT person_id, countIf(properties.platform = 'web') AS w,
           countIf(properties.platform = 'desktop') AS d
    FROM events
    WHERE {W} AND person.properties.email IS NOT NULL AND toString(person.properties.email) != ''
    GROUP BY person_id
)
LIMIT 1
""", 'Counts only accounts PostHog knows an email for, so desktop users who never '
         'signed in on the web are missing.'),
    Query('top_users', 'users', 'Most active signed-in users', """
SELECT person.properties.email AS email, person.properties.name AS name,
       count() AS events, uniq(toDate(timestamp)) AS active_days,
       countIf(event IN ('search_executed', 'desktop_search_executed')) AS searches,
       countIf(event = 'parallels_search') AS parallels,
       countIf(event IN ('browse_manuscript', 'result_opened')) AS manuscripts,
       if(countIf(properties.platform = 'web') > 0, 'yes', '') AS web,
       if(countIf(properties.platform = 'desktop') > 0, 'yes', '') AS desktop,
       max(timestamp) AS last_seen
FROM events
WHERE {W} AND person.properties.email IS NOT NULL AND toString(person.properties.email) != ''
GROUP BY person_id, email, name ORDER BY active_days DESC, events DESC LIMIT 50
"""),
    Query('new_accounts', 'users', 'Accounts first seen in the window', """
SELECT properties.email AS email, properties.name AS name, created_at
FROM persons
WHERE created_at >= now() - INTERVAL {DAYS} DAY
  AND properties.email IS NOT NULL AND toString(properties.email) != ''
ORDER BY created_at DESC LIMIT 60
"""),
    Query('retention', 'users', 'Returning web visitors', """
SELECT countIf(days = 1) AS one_day, countIf(days BETWEEN 2 AND 3) AS two_to_three_days,
       countIf(days BETWEEN 4 AND 9) AS four_to_nine_days, countIf(days >= 10) AS ten_plus_days,
       count() AS visitors
FROM (
    SELECT person_id, uniq(toDate(timestamp)) AS days
    FROM events WHERE {W} AND event = '$pageview'
    GROUP BY person_id
)
LIMIT 1
""", 'Days active per visitor in the window. Anonymous visitors are counted per browser.'),
    # ------------------------------------------------------------------ desktop
    Query('desktop_versions', 'desktop', 'Desktop versions in use', """
SELECT properties.app_version AS version, count() AS sessions, uniq(distinct_id) AS installs,
       min(timestamp) AS first_seen, max(timestamp) AS last_seen
FROM events WHERE {W} AND event = 'desktop_session_start'
GROUP BY version ORDER BY sessions DESC LIMIT 25
"""),
    Query('desktop_env', 'desktop', 'Desktop OS and interface language', """
SELECT properties.os_family AS os, properties.os_version AS os_version,
       properties.ui_language AS ui_language, count() AS sessions, uniq(distinct_id) AS installs
FROM events WHERE {W} AND event = 'desktop_session_start'
GROUP BY os, os_version, ui_language ORDER BY installs DESC LIMIT 25
"""),
    Query('desktop_session_length', 'desktop', 'Desktop session length', """
SELECT count() AS sessions, round(quantile(0.5)(dur) / 60, 1) AS median_minutes,
       round(quantile(0.9)(dur) / 60, 1) AS p90_minutes, round(avg(dur) / 60, 1) AS avg_minutes
FROM (
    SELECT properties.session_id AS sid, dateDiff('second', min(timestamp), max(timestamp)) AS dur
    FROM events
    WHERE {W} AND properties.platform = 'desktop' AND properties.session_id IS NOT NULL
    GROUP BY sid HAVING count() > 1
)
LIMIT 1
""", 'From first to last event of each session (start, pings, end).'),
    Query('desktop_tabs', 'desktop', 'Desktop tabs opened', """
SELECT properties.tab_name AS tab, count() AS opens, uniq(distinct_id) AS installs
FROM events WHERE {W} AND event = 'desktop_tab_activated'
GROUP BY tab ORDER BY opens DESC LIMIT 30
"""),
    Query('desktop_features', 'desktop', 'Desktop features and dialogs used', """
SELECT properties.feature_name AS feature, properties.dialog_name AS dialog,
       properties.action AS action, count() AS uses, uniq(distinct_id) AS installs
FROM events WHERE {W} AND event = 'desktop_feature_opened'
GROUP BY feature, dialog, action ORDER BY uses DESC LIMIT 50
"""),
    Query('desktop_indexing', 'desktop', 'My Library indexing runs', """
SELECT properties.operation_kind AS operation, properties.doc_count_bucket AS documents,
       count() AS runs, uniq(distinct_id) AS installs,
       round(quantile(0.5)(toFloat(properties.duration_ms)) / 1000, 1) AS median_seconds,
       round(max(toFloat(properties.duration_ms)) / 1000, 1) AS max_seconds
FROM events WHERE {W} AND event = 'desktop_indexing_complete'
GROUP BY operation, documents ORDER BY runs DESC LIMIT 20
"""),
    # ------------------------------------------------------------------ perf
    Query('ux_timing', 'perf', 'What web visitors wait for (ux_timing)', """
SELECT properties.workflow AS workflow, properties.outcome AS outcome, count() AS samples,
       quantile(0.5)(toFloat(properties.total_ms)) AS median_total_ms,
       quantile(0.9)(toFloat(properties.total_ms)) AS p90_total_ms,
       quantile(0.5)(toFloat(properties.server_ms)) AS median_server_ms,
       quantile(0.5)(toFloat(properties.img_ms)) AS median_img_ms
FROM events WHERE {W} AND event = 'ux_timing'
GROUP BY workflow, outcome ORDER BY samples DESC LIMIT 40
""", 'Sampled in the browser; outcomes other than ok are the slow or failed ones.'),
    Query('ux_timing_paths', 'perf', 'Slowest page turns by path (ux_timing, p90)', """
SELECT properties.workflow AS workflow, properties.$pathname AS path, count() AS samples,
       quantile(0.5)(toFloat(properties.total_ms)) AS median_total_ms,
       quantile(0.9)(toFloat(properties.total_ms)) AS p90_total_ms
FROM events WHERE {W} AND event = 'ux_timing'
GROUP BY workflow, path HAVING samples >= 5
ORDER BY p90_total_ms DESC LIMIT 25
"""),
)

_TOKEN_RE = re.compile(r'\{[A-Z]+\}')
_ISO_DT_RE = re.compile(r'^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$')


def window_condition(days: int, *, previous: bool = False) -> str:
    """The ``WHERE`` condition for the last ``days`` days, or the ``days`` before them."""
    if previous:
        return (f'timestamp >= now() - INTERVAL {2 * days} DAY '
                f'AND timestamp < now() - INTERVAL {days} DAY')
    return f'timestamp >= now() - INTERVAL {days} DAY'


def render_sql(sql: str, days: int, *, previous: bool = False) -> str:
    out = sql.replace('{W}', window_condition(days, previous=previous)).replace('{DAYS}', str(days))
    leftover = _TOKEN_RE.search(out)
    if leftover:
        raise ValueError(f'unknown token {leftover.group(0)} in query')
    return out.strip()


class AuthError(Exception):
    """PostHog refused the key (401/403)."""


class QueryError(Exception):
    """One query failed; the report notes it and carries on."""


class PostHogClient:
    def __init__(self, host: str, project: str, key: str, session=None):
        self.url = f"{host.rstrip('/')}/api/projects/{project}/query/"
        self.session = session or requests.Session()
        self.headers = {'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}

    def query(self, sql: str, name: str) -> tuple[list[str], list[list]]:
        body = {'query': {'kind': 'HogQLQuery', 'query': sql}, 'name': f'genizah_report_{name}'}
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            try:
                resp = self.session.post(self.url, headers=self.headers, json=body,
                                         timeout=REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                raise QueryError(f'network error: {exc}') from exc
            if resp.status_code == 429 and attempt < MAX_RATE_LIMIT_RETRIES:
                wait = _retry_after(resp)
                print(f'  rate limited; waiting {wait}s', file=sys.stderr)
                time.sleep(wait)
                continue
            if resp.status_code in (401, 403):
                raise AuthError(f'HTTP {resp.status_code}: {_error_text(resp)}')
            if resp.status_code >= 400:
                raise QueryError(f'HTTP {resp.status_code}: {_error_text(resp)}')
            data = resp.json()
            return list(data.get('columns') or []), list(data.get('results') or [])
        raise QueryError('still rate limited after retries')


def _retry_after(resp) -> int:
    try:
        return max(1, min(120, int(float(resp.headers.get('Retry-After', '30')))))
    except (TypeError, ValueError):
        return 30


def _error_text(resp) -> str:
    try:
        data = resp.json()
        text = data.get('detail') or data.get('error') or json.dumps(data)
    except ValueError:
        text = resp.text
    return ' '.join(str(text).split())[:400]


def format_cell(value) -> str:
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    if isinstance(value, float):
        if value != value:  # NaN
            return ''
        return str(int(value)) if value.is_integer() else f'{value:.2f}'
    if isinstance(value, (list, dict)):
        value = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    text = str(value)
    m = _ISO_DT_RE.match(text)
    if m:
        text = f'{m.group(1)} {m.group(2)}'
    text = ' '.join(text.split()).replace('|', '\\|')
    if len(text) > MAX_CELL:
        text = text[:MAX_CELL - 1] + '…'
    return text


def md_table(columns: list[str], rows: list[list]) -> str:
    if not rows:
        return '_No data in this window._'
    head = '| ' + ' | '.join(columns) + ' |'
    sep = '| ' + ' | '.join('---' for _ in columns) + ' |'
    body = ['| ' + ' | '.join(format_cell(v) for v in row) + ' |' for row in rows]
    return '\n'.join([head, sep, *body])


def headline_table(columns: list[str], now_row: list, prev_row: list | None, days: int) -> str:
    lines = [f'| metric | last {days} days | previous {days} days | change |',
             '| --- | --- | --- | --- |']
    for i, col in enumerate(columns):
        cur = now_row[i] if i < len(now_row) else None
        prev = prev_row[i] if prev_row is not None and i < len(prev_row) else None
        lines.append(f'| {col} | {format_cell(cur)} | {format_cell(prev)} | {_change(cur, prev)} |')
    return '\n'.join(lines)


def _change(cur, prev) -> str:
    if not isinstance(cur, (int, float)) or not isinstance(prev, (int, float)):
        return ''
    if prev == 0:
        return 'new' if cur else '0'
    return f'{(cur - prev) / prev * 100:+.0f}%'


def select_queries(only: str | None) -> list[Query]:
    if not only:
        return list(QUERIES)
    wanted = {part.strip() for part in only.split(',') if part.strip()}
    unknown = wanted - set(GROUPS) - {q.id for q in QUERIES}
    if unknown:
        raise SystemExit(f'unknown section(s): {", ".join(sorted(unknown))} (see --list)')
    return [q for q in QUERIES if q.group in wanted or q.id in wanted]


def build_report(client: PostHogClient, queries: list[Query], days: int, *,
                 host: str, project: str, include_headline: bool = True) -> tuple[str, dict, int, int]:
    """Run every query. Returns (markdown, raw results, ok count, failed count).

    Raises AuthError if the key is refused on the first query: nothing else
    would succeed either.
    """
    now = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    parts = [
        f'# GenizahSearch PostHog report, last {days} days',
        '',
        f'Generated {now} from project {project} at {host}.',
        '',
        '> Private: this report contains account emails and the text people searched for. '
        'Keep it out of git and do not share it outside the team.',
        '',
        'Coverage: web events exist only for visitors whose browser loads PostHog; desktop '
        'events only for installs that opted in to telemetry. API and MCP events are sampled.',
        '',
    ]
    raw: dict = {}
    ok = failed = 0
    first = True

    def run(name: str, sql: str):
        nonlocal first, ok, failed
        print(f'  {name}', file=sys.stderr)
        try:
            cols, rows = client.query(sql, name)
        except AuthError:
            if first:
                raise
            failed += 1
            return None, 'PostHog refused the key for this query.'
        except QueryError as exc:
            first = False
            failed += 1
            return None, str(exc)
        first = False
        ok += 1
        return (cols, rows), None

    if include_headline:
        parts += ['## Headline numbers', '']
        cur, err = run('headline', render_sql(HEADLINE_SQL, days))
        prev, prev_err = (None, None)
        if cur:
            prev, prev_err = run('headline_previous', render_sql(HEADLINE_SQL, days, previous=True))
        if cur and cur[1]:
            prev_row = prev[1][0] if prev and prev[1] else None
            parts.append(headline_table(cur[0], cur[1][0], prev_row, days))
            raw['headline'] = {'columns': cur[0], 'rows': cur[1],
                               'previous_rows': prev[1] if prev else None}
            if prev_err:
                parts += ['', f'_Previous period failed: {prev_err}_']
        else:
            parts.append(f'**Query failed:** {err}' if err else '_No data in this window._')
            raw['headline'] = {'error': err}
        parts.append('')

    current_group = None
    for q in queries:
        if q.group != current_group:
            current_group = q.group
            parts += [f'## {GROUPS[q.group]}', '']
        parts += [f'### {q.title}', '']
        if q.note:
            parts += [f'_{q.note}_', '']
        result, err = run(q.id, render_sql(q.sql, days))
        if result is None:
            parts.append(f'**Query failed:** {err}')
            raw[q.id] = {'error': err}
        else:
            parts.append(md_table(*result))
            raw[q.id] = {'columns': result[0], 'rows': result[1]}
        parts.append('')

    return '\n'.join(parts).rstrip() + '\n', raw, ok, failed


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    p.add_argument('--days', type=int, default=30, help='window length in days (default 30)')
    p.add_argument('--only', help='comma-separated group or query ids (see --list)')
    p.add_argument('--out', type=Path, help='Markdown output path (default _tmp/posthog_report_<date>.md)')
    p.add_argument('--host', default=os.environ.get('POSTHOG_HOST', DEFAULT_HOST))
    p.add_argument('--project', default=os.environ.get('POSTHOG_PROJECT_ID', DEFAULT_PROJECT))
    p.add_argument('--list', action='store_true', help='print section ids and exit')
    args = p.parse_args(argv)
    if args.days < 1 or args.days > 365:
        p.error('--days must be between 1 and 365')
    return args


def main(argv=None, *, session=None) -> int:
    args = parse_args(argv)
    if args.list:
        for gid, gname in GROUPS.items():
            print(f'{gid}: {gname}')
            for q in QUERIES:
                if q.group == gid:
                    print(f'    {q.id}: {q.title}')
        return 0

    key = os.environ.get(KEY_ENV, '').strip()
    if not key:
        print(f'{KEY_ENV} is not set. Create a personal API key in PostHog (Settings > '
              f'Personal API keys) with the "Query: Read" scope for project {args.project}, '
              f'and put it in that environment variable.', file=sys.stderr)
        return 2
    if not key.startswith('phx_'):
        print(f'{KEY_ENV} must be a personal API key (phx_...). A phc_ project key can only '
              f'send events and cannot read them.', file=sys.stderr)
        return 2

    queries = select_queries(args.only)
    client = PostHogClient(args.host, args.project, key, session=session)
    print(f'Querying PostHog project {args.project} ({args.host}), last {args.days} days:',
          file=sys.stderr)
    try:
        report, raw, ok, failed = build_report(
            client, queries, args.days, host=args.host, project=args.project,
            include_headline=not args.only)
    except AuthError as exc:
        print(f'PostHog refused the key: {exc}\nCheck that it has the "Query: Read" scope '
              f'and access to project {args.project}.', file=sys.stderr)
        return 2

    out = args.out or DEFAULT_OUT_DIR / f'posthog_report_{dt.date.today().isoformat()}.md'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding='utf-8')
    raw_path = out.with_suffix('.json')
    raw_path.write_text(json.dumps({'days': args.days, 'project': args.project, 'results': raw},
                                   ensure_ascii=False, indent=1, default=str), encoding='utf-8')
    print(f'Wrote {out} and {raw_path.name}: {ok} queries ok, {failed} failed.', file=sys.stderr)
    return 1 if ok == 0 else 0


if __name__ == '__main__':
    sys.exit(main())
