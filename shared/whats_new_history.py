# -*- coding: utf-8 -*-
"""What's New in every version since 9.0.0, in English and Hebrew (owner, 2026-11-05).

One list feeds four surfaces: the website's ``/whats-new`` page, its
``GET /api/whats-new`` (the desktop update notice reads it, so an installed copy
can show what changed between its own version and the new one), and the desktop
app's "What's New in each version" dialog. The entries themselves live in
``shared/whats_new_data.py``; this module checks and reads them.

An entry is one release::

    {"version": "9.14.0", "date": "2026-11-05", "apps": ["web", "desktop"],
     "title": {"en": ..., "he": ...},
     "items": [{"category": "feature", "apps": ["web"], "en": ..., "he": ...}, ...]}

``apps`` on the release says which apps it shipped to; on an item, where a
reader sees it. ``category`` is one of ``CATEGORIES``. Every release adds its
entry (``tests/test_whats_new_history.py`` fails while ``version.APP_VERSION``
has none).
"""
from __future__ import annotations

import copy
import re
from typing import Iterable, Optional

from shared.whats_new_data import HISTORY

#: The categories, in the order a release shows them.
CATEGORIES = ('feature', 'improvement', 'fix')
#: Their headings (English keys; the surfaces translate them with ``tr``).
CATEGORY_LABELS = {
    'feature': 'New features',
    'improvement': 'Improvements',
    'fix': 'Fixes',
}
APPS = ('web', 'desktop')
#: The labels a surface shows beside an item that is not in both apps.
APP_LABELS = {'web': 'Website', 'desktop': 'Desktop app'}
#: The history starts here (owner, 2026-11-05).
FIRST_VERSION = '9.0.0'

_VERSION_RE = re.compile(r'^\s*[vV]?(\d+)\.(\d+)\.(\d+)\s*$')


def version_key(version: str) -> tuple[int, int, int]:
    """``'9.13.1'`` (or ``'v9.13.1'``) -> ``(9, 13, 1)``; anything else raises ValueError."""
    match = _VERSION_RE.match(str(version or ''))
    if not match:
        raise ValueError(f'not a version: {version!r}')
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def _for_app(release: dict, app: Optional[str]) -> Optional[dict]:
    """A copy of ``release`` with only the items ``app`` shows (all of them for None);
    None when no item is left."""
    out = copy.deepcopy(release)
    if app is not None:
        out['items'] = [item for item in out['items'] if app in item['apps']]
    return out if out['items'] else None


def releases(app: Optional[str] = None) -> list[dict]:
    """Every release, newest first; with ``app``, only the items that app shows, and only
    the releases that still have one."""
    if app is not None and app not in APPS:
        raise ValueError(f'not an app: {app!r}')
    out = []
    for release in sorted(HISTORY, key=lambda r: version_key(r['version']), reverse=True):
        kept = _for_app(release, app)
        if kept is not None:
            out.append(kept)
    return out


def releases_between(since: str, upto: Optional[str] = None,
                     app: Optional[str] = None) -> list[dict]:
    """The releases after ``since`` up to and including ``upto`` (no upper bound when
    None), newest first: what a copy at ``since`` gains by updating to ``upto``."""
    low = version_key(since)
    high = version_key(upto) if upto else None
    return [r for r in releases(app)
            if version_key(r['version']) > low and (high is None or version_key(r['version']) <= high)]


def localize(release: dict, lang: str) -> dict:
    """One release in one language: ``{'version', 'date', 'apps', 'title', 'sections'}``,
    where ``sections`` is ``[(category, [{'text', 'apps'}, ...]), ...]`` in
    ``CATEGORIES`` order, empty categories left out."""
    key = 'he' if lang == 'he' else 'en'
    sections = []
    for category in CATEGORIES:
        items = [{'text': item[key], 'apps': list(item['apps'])}
                 for item in release['items'] if item['category'] == category]
        if items:
            sections.append((category, items))
    return {
        'version': release['version'],
        'date': release['date'],
        'apps': list(release['apps']),
        'title': release['title'][key],
        'sections': sections,
    }


def app_note(item_apps: Iterable[str]) -> Optional[str]:
    """The label to show beside an item that is in one app only (English key), else None."""
    apps = set(item_apps)
    if apps == {'web'}:
        return APP_LABELS['web']
    if apps == {'desktop'}:
        return APP_LABELS['desktop']
    return None


def problems(history: Optional[list] = None) -> list[str]:
    """Everything wrong with the entries, one sentence each (empty when they are sound).
    The test runs it; a surface never shows an entry it would reject."""
    history = HISTORY if history is None else history
    out = []
    seen = set()
    previous = None
    for index, release in enumerate(history):
        where = f'entry {index}'
        try:
            key = version_key(release.get('version'))
        except (ValueError, AttributeError):
            out.append(f'{where}: version {release.get("version")!r} is not X.Y.Z')
            continue
        where = release['version']
        if key in seen:
            out.append(f'{where}: listed twice')
        seen.add(key)
        if previous is not None and key >= previous:
            out.append(f'{where}: not newest first')
        previous = key
        if key < version_key(FIRST_VERSION):
            out.append(f'{where}: older than {FIRST_VERSION}')
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', str(release.get('date', ''))):
            out.append(f'{where}: date {release.get("date")!r} is not YYYY-MM-DD')
        apps = release.get('apps') or []
        if not apps or any(a not in APPS for a in apps):
            out.append(f'{where}: apps {apps!r}')
        title = release.get('title') or {}
        if not (str(title.get('en', '')).strip() and str(title.get('he', '')).strip()):
            out.append(f'{where}: the title needs English and Hebrew')
        items = release.get('items') or []
        if not items:
            out.append(f'{where}: no items')
        for n, item in enumerate(items, 1):
            if item.get('category') not in CATEGORIES:
                out.append(f'{where} item {n}: category {item.get("category")!r}')
            item_apps = item.get('apps') or []
            if not item_apps or any(a not in APPS for a in item_apps):
                out.append(f'{where} item {n}: apps {item_apps!r}')
            elif any(a not in apps for a in item_apps):
                out.append(f'{where} item {n}: in {item_apps!r}, but the release shipped to {apps!r}')
            if not (str(item.get('en', '')).strip() and str(item.get('he', '')).strip()):
                out.append(f'{where} item {n}: needs English and Hebrew')
            extra = set(item) - {'category', 'apps', 'en', 'he'}
            if extra:
                out.append(f'{where} item {n}: unknown fields {sorted(extra)}')
    return out


def to_public(release: dict) -> dict:
    """A release as ``GET /api/whats-new`` serves it (both languages)."""
    return {
        'version': release['version'],
        'date': release['date'],
        'apps': list(release['apps']),
        'title': dict(release['title']),
        'items': [{'category': i['category'], 'apps': list(i['apps']), 'en': i['en'], 'he': i['he']}
                  for i in release['items']],
    }


def from_public(payload: object) -> list[dict]:
    """The releases in a ``GET /api/whats-new`` answer, checked; raises ValueError on
    anything malformed (the desktop then falls back to the release page)."""
    if not isinstance(payload, dict) or not isinstance(payload.get('releases'), list):
        raise ValueError('no releases list')
    out = []
    for raw in payload['releases']:
        if not isinstance(raw, dict):
            raise ValueError('a release is not an object')
        release = {
            'version': raw.get('version'),
            'date': raw.get('date'),
            'apps': raw.get('apps'),
            'title': raw.get('title'),
            'items': [
                {k: item.get(k) for k in ('category', 'apps', 'en', 'he')}
                for item in (raw.get('items') or []) if isinstance(item, dict)
            ],
        }
        out.append(release)
    found = problems(out)
    if found:
        raise ValueError('; '.join(found[:3]))
    return out
