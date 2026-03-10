#!/usr/bin/env python3
"""
Bump version across all GenizahSearch files that contain version info.

Usage:
    python scripts/bump_version.py 6.3.0
    python scripts/bump_version.py 6.3.0 --dry-run

Updates:
    - version.py              (APP_VERSION)
    - version_info.txt        (filevers, prodvers, FileVersion, ProductVersion)
    - CompileScriptGenizah.iss (#define MyAppVersion + OutputBaseFilename)
    - README.md               (header line)
    - CLAUDE.md               (no auto-update -- just reminds you)
    - CHANGELOG.md            (no auto-update -- just reminds you)
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Files and their replacement patterns
# Each entry: (relative_path, [(pattern, replacement_template), ...])
TARGETS = [
    (
        "version.py",
        [
            (r'APP_VERSION\s*=\s*"[\d.]+"', 'APP_VERSION = "{version}"'),
        ],
    ),
    (
        "version_info.txt",
        [
            (r"filevers=\(\d+,\s*\d+,\s*\d+,\s*\d+\)", "filevers=({major}, {minor}, {patch}, 0)"),
            (r"prodvers=\(\d+,\s*\d+,\s*\d+,\s*\d+\)", "prodvers=({major}, {minor}, {patch}, 0)"),
            (r"u'FileVersion',\s*u'[\d.]+'", "u'FileVersion', u'{version}.0'"),
            (r"u'ProductVersion',\s*u'[\d.]+'", "u'ProductVersion', u'{version}.0'"),
        ],
    ),
    (
        "CompileScriptGenizah.iss",
        [
            (r'#define MyAppVersion "[\d.]+"', '#define MyAppVersion "{version}"'),
            (r"OutputBaseFilename=GenizahSearchPro_V[\d.]+_Setup", "OutputBaseFilename=GenizahSearchPro_V{version}_Setup"),
        ],
    ),
    (
        "README.md",
        [
            (r"^# Genizah Search Pro [\d.]+", "# Genizah Search Pro {version}"),
        ],
    ),
]


def parse_version(v: str) -> tuple[int, int, int]:
    parts = v.split(".")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise ValueError(f"Invalid version format: {v!r} (expected X.Y.Z)")
    return int(parts[0]), int(parts[1]), int(parts[2])


def get_current_version() -> str:
    text = (ROOT / "version.py").read_text(encoding="utf-8")
    m = re.search(r'APP_VERSION\s*=\s*"([\d.]+)"', text)
    if not m:
        raise RuntimeError("Could not read current version from version.py")
    return m.group(1)


def bump(new_version: str, dry_run: bool = False) -> list[str]:
    major, minor, patch = parse_version(new_version)
    fmt = {
        "version": new_version,
        "major": major,
        "minor": minor,
        "patch": patch,
    }

    changes: list[str] = []

    for rel_path, patterns in TARGETS:
        fpath = ROOT / rel_path
        if not fpath.exists():
            print(f"  SKIP {rel_path} (not found)")
            continue

        text = fpath.read_text(encoding="utf-8")
        original = text

        for pattern, template in patterns:
            replacement = template.format(**fmt)
            text, count = re.subn(pattern, replacement, text, count=0, flags=re.MULTILINE)
            if count == 0:
                print(f"  WARN {rel_path}: pattern not matched: {pattern}")

        if text != original:
            changes.append(rel_path)
            if dry_run:
                print(f"  WOULD UPDATE {rel_path}")
            else:
                fpath.write_text(text, encoding="utf-8")
                print(f"  UPDATED {rel_path}")
        else:
            print(f"  NO CHANGE {rel_path}")

    return changes


def main():
    parser = argparse.ArgumentParser(description="Bump GenizahSearch version across all files")
    parser.add_argument("version", help="New version in X.Y.Z format")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = parser.parse_args()

    current = get_current_version()
    parse_version(args.version)  # validate

    if args.version == current:
        print(f"Version is already {current}, nothing to do.")
        sys.exit(0)

    print(f"Bumping version: {current} -> {args.version}")
    if args.dry_run:
        print("(dry run -- no files will be modified)\n")
    print()

    changes = bump(args.version, dry_run=args.dry_run)

    print()
    if changes:
        print(f"Updated {len(changes)} file(s): {', '.join(changes)}")
    else:
        print("No files changed.")

    print()
    print("Manual steps remaining:")
    print(f"  1. CHANGELOG.md -- add ## [{args.version}] section with release notes")
    print(f"  2. CLAUDE.md 'Recently Changed' -- add entry for v{args.version}")
    print(f"  3. README.md 'What's New' section -- update feature description")
    print(f"  4. git commit -m 'chore: bump version to {args.version}'")


if __name__ == "__main__":
    main()
