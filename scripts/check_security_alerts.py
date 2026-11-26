# -*- coding: utf-8 -*-
"""Show GitHub's open security alerts, and fail while one needs a decision.

The alerts (Dependabot, code scanning, secret scanning) are on the public
repository, where nobody works, so nothing was read there until it held 251 of
them. `scripts/init.ps1` runs this at session start and the release pre-flight
runs it before a release. It only reads; it changes nothing on GitHub.

An alert BLOCKS when it is open and
- Dependabot: high or critical, and the version pinned in THIS checkout is still
  vulnerable (an upgrade that is merged but not yet published is counted apart:
  publishing closes the alert);
- code scanning: high or critical, outside tests/;
- secret scanning: any.

Two ways to clear one: fix it, or dismiss it on GitHub with a reason. There is
no ignore list here, so the decision and its reason stay with the alert.

Exit code: 0 nothing blocks, 1 something does, 2 it could not be checked (no
`gh`, not signed in, GitHub did not answer). 2 is never a pass.

    python scripts/check_security_alerts.py            # the summary and what blocks
    python scripts/check_security_alerts.py --all      # every open alert
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

try:
    from packaging.requirements import InvalidRequirement, Requirement
    from packaging.specifiers import InvalidSpecifier, SpecifierSet
    from packaging.utils import canonicalize_name
    from packaging.version import InvalidVersion, Version
except ImportError:  # exit 1 would read as "an alert blocks"
    print('  NOT CHECKED: the packaging library is not installed')
    sys.exit(2)

REPO = 'gershuni/GenizahSearch'
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCK = 'requirements-lock.txt'
BLOCKING = ('critical', 'high')
TIMEOUT_SECONDS = 60

# Only the fields used are taken, so a leaked secret's text never leaves GitHub.
QUERIES = {
    'dependabot': '.[] | {number, severity: .security_advisory.severity,'
                  ' package: .dependency.package.name, manifest: .dependency.manifest_path,'
                  ' range: .security_vulnerability.vulnerable_version_range,'
                  ' fixed: .security_vulnerability.first_patched_version.identifier,'
                  ' summary: .security_advisory.summary} | @json',
    'code-scanning': '.[] | {number, severity: .rule.security_severity_level, rule: .rule.id,'
                     ' path: .most_recent_instance.location.path,'
                     ' line: .most_recent_instance.location.start_line} | @json',
    'secret-scanning': '.[] | {number, type: .secret_type_display_name} | @json',
}


class CannotCheck(Exception):
    """GitHub could not be asked, or answered something unreadable."""


def fetch(kind, repo=REPO):
    """The open alerts of one kind, as dicts."""
    cmd = ['gh', 'api', '--paginate', '-H', 'Accept: application/vnd.github+json',
           f'repos/{repo}/{kind}/alerts?state=open&per_page=100', '-q', QUERIES[kind]]
    try:
        done = subprocess.run(cmd, capture_output=True, encoding='utf-8', errors='replace',
                              timeout=TIMEOUT_SECONDS)
    except FileNotFoundError:
        raise CannotCheck('the GitHub CLI (gh) is not installed')
    except subprocess.TimeoutExpired:
        raise CannotCheck(f'{kind}: GitHub did not answer in {TIMEOUT_SECONDS} s')
    if done.returncode != 0:
        lines = (done.stderr or done.stdout).strip().splitlines()
        raise CannotCheck(f'{kind}: {lines[0] if lines else "gh failed"}')
    try:
        return [json.loads(line) for line in done.stdout.splitlines() if line.strip()]
    except ValueError:
        raise CannotCheck(f'{kind}: unreadable answer from GitHub')


def read_pins(manifest, root=ROOT):
    """{canonical name: Requirement} of a requirements file, or None if it is gone."""
    try:
        with open(os.path.join(root, manifest), encoding='utf-8') as fh:
            lines = fh.read().splitlines()
    except FileNotFoundError:
        return None
    pins = {}
    for line in lines:
        line = line.split('#', 1)[0].strip()
        if not line or line.startswith('-'):
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        pins[canonicalize_name(req.name)] = req
    return pins


def _exact(req):
    versions = [s.version for s in req.specifier if s.operator == '==']
    return versions[0] if len(versions) == 1 else None


def fixed_here(alert, pins_of):
    """True when this checkout no longer asks for a vulnerable version.

    Anything that cannot be worked out counts as NOT fixed. A range (lxml>=6.1)
    is resolved by the lock's pin, which is what CI and deploy.sh install.
    """
    pins = pins_of(alert['manifest'])
    name = canonicalize_name(alert['package'])
    req = pins.get(name) if pins is not None else None
    if req is None:
        # Unpinned here, removed, or its manifest deleted (the public copy keeps
        # it until the publish): only its absence from the lock too is a fix.
        lock = pins_of(LOCK)
        return lock is not None and name not in lock
    version = _exact(req)
    if version is None:
        locked = (pins_of(LOCK) or {}).get(name)
        version = _exact(locked) if locked is not None else None
    if version is None:
        return False
    try:
        return not SpecifierSet(alert['range']).contains(version, prereleases=True)
    except (InvalidSpecifier, TypeError):
        return False


def sort_out(alerts, root=ROOT):
    """(blocking, waiting for a publish, the rest), each a list of (kind, alert)."""
    cache = {}

    def pins_of(manifest):
        if manifest not in cache:
            cache[manifest] = read_pins(manifest, root)
        return cache[manifest]

    blocking, publish, rest = [], [], []
    for alert in alerts.get('dependabot', []):
        if fixed_here(alert, pins_of):
            publish.append(('dependabot', alert))
        else:
            (blocking if alert['severity'] in BLOCKING else rest).append(('dependabot', alert))
    for alert in alerts.get('code-scanning', []):
        in_tests = (alert.get('path') or '').startswith('tests/')
        blocks = alert.get('severity') in BLOCKING and not in_tests
        (blocking if blocks else rest).append(('code-scanning', alert))
    for alert in alerts.get('secret-scanning', []):
        blocking.append(('secret-scanning', alert))
    return blocking, publish, rest


def describe(kind, alert):
    if kind == 'dependabot':
        return (f"{alert['package']} {alert['range']} in {alert['manifest']}, "
                f"fixed in {alert['fixed'] or '(no fix yet)'}: {alert['summary']}")
    if kind == 'code-scanning':
        return f"{alert['rule']} at {alert['path']}:{alert['line']}"
    return f"{alert['type']} (open it on GitHub)"


def url(kind, alert, repo=REPO):
    return f"https://github.com/{repo}/security/{kind}/{alert['number']}"


def _grouped(entries):
    """One line per package / rule, worst first; the number of alerts on it."""
    groups = defaultdict(list)
    for kind, alert in entries:
        name = alert.get('package') or alert.get('rule') or alert.get('type')
        groups[kind, canonicalize_name(name) if kind == 'dependabot' else name].append(alert)
    order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}

    def worst(alerts):
        return min(order.get(a.get('severity'), 4) for a in alerts)

    for (kind, name), alerts in sorted(groups.items(), key=lambda g: (worst(g[1]), g[0])):
        counts = Counter(a.get('severity') or 'unrated' for a in alerts)
        parts = ', '.join(f'{counts[s]} {s}' for s in sorted(counts, key=lambda s: order.get(s, 4)))
        fixes = {a['fixed'] for a in alerts if a.get('fixed')} if kind == 'dependabot' else set()
        # The highest fix is the one that clears the group.
        target = f', fixed in {max(fixes, key=_version_key)}' if fixes else ''
        yield f'{kind}: {name} ({parts}{target})'


def _version_key(text):
    try:
        return Version(text)
    except InvalidVersion:
        return Version('0')


def report(alerts, show_all=False, root=ROOT, out=print):
    """Print what is open; 1 when something blocks, else 0."""
    blocking, publish, rest = sort_out(alerts, root)
    total = len(blocking) + len(publish) + len(rest)
    out(f'  {total} open on {REPO}: {len(blocking)} need a decision, '
        f'{len(publish)} fixed here and waiting for a publish, {len(rest)} lower or in tests/')
    for line in _grouped(blocking):
        out('    ' + line)
    if show_all:
        for title, entries in (('needs a decision', blocking), ('waits for a publish', publish),
                               ('lower or in tests/', rest)):
            for kind, alert in entries:
                out(f"  [{title}] {alert.get('severity') or 'unrated'}  {describe(kind, alert)}")
                out(f'      {url(kind, alert)}')
    if blocking:
        out('  Fix each one, or dismiss it on GitHub with a reason (ask first). --all lists them.')
    return 1 if blocking else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--all', action='store_true', help='list every open alert')
    args = parser.parse_args(argv)
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            stream.reconfigure(encoding='utf-8', errors='replace')
    try:
        alerts = {kind: fetch(kind) for kind in QUERIES}
    except CannotCheck as exc:
        print(f'  NOT CHECKED: {exc}')
        return 2
    return report(alerts, show_all=args.all)


if __name__ == '__main__':
    sys.exit(main())
