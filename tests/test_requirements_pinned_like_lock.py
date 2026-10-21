# -*- coding: utf-8 -*-
"""requirements.txt must ask for what CI tested.

deploy.sh installs requirements.txt; every CI job installs
requirements-lock.txt. A requirement the lock's pin does not satisfy means
the server resolves a version CI never ran. A package requirements.txt leaves
out is worse: it floats on every deploy to the newest release its dependents
allow. NiceGUI 3.18 puts only floors on Starlette and python-socketio, so they
moved with each deploy while web/framework_patches.py's session patch was
audited against Starlette 1.9.0 alone (finding 2c-06).

Containment, not string equality: several lines in requirements.txt are
ranges (pymupdf, zstandard, defusedxml, lxml) and the lock pins one version
inside each.
"""
from __future__ import annotations

import os

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Reached only through nicegui's floors, so unpinned they float on deploy.
MUST_PIN = ('starlette', 'python-socketio', 'python-engineio')


def _read(name):
    with open(os.path.join(ROOT, name), encoding='utf-8') as f:
        return f.read().splitlines()


def _parse(lines):
    """{canonical name: Requirement}, skipping comments, blank lines and pip
    options (-r, -c, --index-url, ...)."""
    out = {}
    for line in lines:
        line = line.split('#', 1)[0].strip()
        if not line or line.startswith('-'):
            continue
        req = Requirement(line)
        out[canonicalize_name(req.name)] = req
    return out


def _mismatches(required, locked):
    """Every requirement the lock does not pin to a version satisfying it."""
    problems = []
    for name, req in sorted(required.items()):
        pin = locked.get(name)
        if pin is None:
            problems.append('%s: not in the lock' % req)
            continue
        versions = [s.version for s in pin.specifier if s.operator == '==']
        if len(versions) != 1:
            problems.append('%s: the lock does not pin it (%s)' % (req, pin))
            continue
        if not req.specifier.contains(versions[0], prereleases=True):
            problems.append('%s: the lock pins %s' % (req, versions[0]))
        # A lock pin that applies on fewer platforms than the requirement
        # leaves it floating everywhere else.
        if pin.marker is not None and str(pin.marker) != str(req.marker):
            problems.append('%s: the lock pins it only where %s' % (req, pin.marker))
    return problems


def test_the_check_flags_a_requirement_its_lock_pin_does_not_satisfy():
    required = _parse([
        '# a comment line',
        'Brotli==1.2.0   # case and spelling differ from the lock',
        'lxml>=5.0,<7.0',
        'starlette>=1.10',
        'pywin32==312; sys_platform == "win32"',
        'colorama==0.4.6',
        'missing-pkg==1.0',
    ])
    locked = _parse([
        'brotli==1.2.0',
        'lxml==6.0.2',
        'starlette==1.9.0',
        'pywin32==312; sys_platform == "win32"',
        'colorama==0.4.6; sys_platform == "win32"',
    ])

    assert _mismatches(required, locked) == [
        'colorama==0.4.6: the lock pins it only where sys_platform == "win32"',
        'missing-pkg==1.0: not in the lock',
        'starlette>=1.10: the lock pins 1.9.0',
    ]


def test_every_requirement_is_satisfied_by_its_lock_pin():
    problems = _mismatches(_parse(_read('requirements.txt')),
                           _parse(_read('requirements-lock.txt')))

    assert not problems, (
        'requirements.txt asks for versions CI (requirements-lock.txt) never '
        'tested:\n  ' + '\n  '.join(problems))


def test_deploy_installs_under_the_lock_as_constraints():
    """requirements.txt pins only what the app names; the lock as constraints
    holds every transitive package (httpx, anyio, ...) to the version CI ran."""
    installs = [ln.strip() for ln in _read('deploy.sh')
                if ln.strip().startswith('pip install')]

    assert installs, 'deploy.sh no longer installs requirements'
    for ln in installs:
        assert '-r requirements.txt' in ln and '-c requirements-lock.txt' in ln, ln


def test_packages_nicegui_bounds_only_from_below_are_pinned():
    required = _parse(_read('requirements.txt'))

    unpinned = [name for name in MUST_PIN
                if name not in required
                or [s.operator for s in required[name].specifier] != ['==']]

    assert not unpinned, (
        'requirements.txt must pin these to the lock, or each deploy installs '
        'whatever is newest: %s' % ', '.join(unpinned))
