"""Crawler visibility policy for non-document web app URLs."""

ARCHIVE_USER_AGENT_TOKENS = (
    'archive.org_bot',
    'special_archiver',
    'ia_archiver',
    'ia_archiver-web.archive.org',
)

# SEO-tool crawlers: no search-engine visibility value, but they walk the
# ~255K-manuscript sitemap 24/7 (2026-07-08: SemrushBot at ~7K /browse
# requests/day drove the web process to its cgroup MemoryHigh cap within
# ~12h of every restart). Deliberately does NOT include Googlebot/bingbot
# (real SEO) or ClaudeBot (negligible volume). nginx carries the same list
# as the PRIMARY enforcement layer; this app-side list is defense-in-depth
# and the source of truth for robots.txt generation.
SEO_TOOL_USER_AGENT_TOKENS = (
    'semrushbot',
    'siteauditbot',
    'ahrefsbot',
    'ahrefssiteaudit',
    'mj12bot',
    'dotbot',
    'blexbot',
    'dataforseobot',
    'serpstatbot',
)

# Canonical product tokens for robots.txt (case matters for readability;
# matching above is lowercase-substring so these need no parity guarantee).
SEO_TOOL_ROBOTS_AGENTS = (
    'SemrushBot',
    'SiteAuditBot',
    'SemrushBot-BA',
    'AhrefsBot',
    'AhrefsSiteAudit',
    'MJ12bot',
    'DotBot',
    'BLEXBot',
    'DataForSeoBot',
    'serpstatbot',
)

# Internet Archive should preserve public pages, not implementation assets or
# transient app states that are not useful as standalone archived documents.
ARCHIVE_DISALLOWED_PATHS = (
    '/_nicegui/',
    '/static/',
    '/favicon.ico',
    '/search',
    '/parallels',
    '/admin',
    '/auth/',
    '/profile',
    '/settings',
    '/corrections',
    '/lists',
    '/reset-hints',
    '/api/',
)

ARCHIVE_RUNTIME_BLOCKED_PATHS = (
    '/_nicegui/',
    '/static/',
    '/favicon.ico',
    '/search',
    '/parallels',
)

NON_INDEXABLE_PATH_PREFIXES = (
    '/_nicegui/',
    '/static/',
)

NON_INDEXABLE_EXACT_PATHS = (
    '/favicon.ico',
)


def is_archive_crawler(user_agent: str | None) -> bool:
    """Return True for known Internet Archive crawler user agents."""
    ua = (user_agent or '').lower()
    return any(token in ua for token in ARCHIVE_USER_AGENT_TOKENS)


def is_seo_tool_crawler(user_agent: str | None) -> bool:
    """Return True for known SEO-tool crawler user agents (Semrush etc.)."""
    ua = (user_agent or '').lower()
    return any(token in ua for token in SEO_TOOL_USER_AGENT_TOKENS)


def should_block_seo_tool_request(path: str, user_agent: str | None) -> bool:
    """Return True when an SEO-tool crawler requests anything but robots.txt.

    robots.txt stays reachable so these crawlers can read their Disallow
    group and deregister the site (RFC 9309 treats a 4xx robots.txt as
    "allow all", which would keep them probing forever).
    """
    return (path or '/') != '/robots.txt' and is_seo_tool_crawler(user_agent)


def _path_matches_rule(path: str, rule: str) -> bool:
    """Match request paths using conservative robots-style path prefixes."""
    normalized = path or '/'
    if rule.endswith('/'):
        return normalized == rule[:-1] or normalized.startswith(rule)
    return normalized == rule or normalized.startswith(f'{rule}/')


def should_block_archive_request(path: str, user_agent: str | None) -> bool:
    """Return True when an archive crawler requests a non-user-facing path."""
    return (
        is_archive_crawler(user_agent)
        and any(_path_matches_rule(path, rule) for rule in ARCHIVE_RUNTIME_BLOCKED_PATHS)
    )


def should_mark_noindex(path: str) -> bool:
    """Return True for assets and API endpoints that should not appear in search."""
    normalized = path or '/'
    return (
        normalized in NON_INDEXABLE_EXACT_PATHS
        or any(_path_matches_rule(normalized, rule) for rule in NON_INDEXABLE_PATH_PREFIXES)
    )
