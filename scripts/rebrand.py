#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Rebranding Tool for Dicta Genizah Search

Usage:
    python rebrand.py                           # Interactive mode
    python rebrand.py "New App Name"            # Replace current name with new name
    python rebrand.py --restore                 # Restore to "Genizah Search Pro"
    python rebrand.py --show                    # Show current app name
"""

import os
import sys

# Files to update (relative to script directory)
# Currently only web files - desktop/docs can be added later after approval
FILES_TO_UPDATE = [
    "web/main.py",
    "web/api.py",
    "web/components/__init__.py",
    "web/pages/accessibility.py",
    "web/pages/admin.py",
    "web/pages/corrections.py",
    "web/pages/download.py",
    "web/pages/help.py",
    "web/pages/home.py",
    "web/pages/parallels.py",
    "web/pages/profile.py",
    "web/pages/search.py",
    "web/pages/settings.py",
]

# Known app names (for detection)
KNOWN_NAMES = [
    "Dicta Genizah Search",
    "Genizah Search Pro",
    "Dicta Genizah",
]

ORIGINAL_NAME = "Genizah Search Pro"


def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))


def detect_current_name():
    """Detect current app name from web/main.py."""
    main_file = os.path.join(get_script_dir(), "web/main.py")
    try:
        with open(main_file, 'r', encoding='utf-8') as f:
            content = f.read()
        for name in KNOWN_NAMES:
            if name in content:
                return name
    except:
        pass
    return None


def replace_in_file(filepath, old_name, new_name):
    """Replace all occurrences of old_name with new_name in a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        if old_name not in content:
            return 0

        count = content.count(old_name)
        new_content = content.replace(old_name, new_name)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

        return count
    except Exception as e:
        print(f"  Error in {filepath}: {e}")
        return -1


def rebrand(old_name, new_name):
    """Perform the rebranding."""
    if old_name == new_name:
        print("Names are identical. Nothing to do.")
        return

    print(f"\n🔄 Rebranding: '{old_name}' → '{new_name}'\n")

    base_dir = get_script_dir()
    total_replacements = 0
    files_changed = 0

    for rel_path in FILES_TO_UPDATE:
        filepath = os.path.join(base_dir, rel_path)
        if not os.path.exists(filepath):
            print(f"  ⚠ Missing: {rel_path}")
            continue

        count = replace_in_file(filepath, old_name, new_name)
        if count > 0:
            print(f"  ✓ {rel_path} ({count} replacements)")
            total_replacements += count
            files_changed += 1
        elif count == 0:
            pass  # No matches, skip silently
        # count == -1 means error, already printed

    print(f"\n✅ Done! Changed {total_replacements} occurrences in {files_changed} files.")
    print(f"\nDon't forget to commit:\n  git add -A && git commit -m \"Rebrand to {new_name}\"")


def show_current():
    """Show the current app name."""
    name = detect_current_name()
    if name:
        print(f"Current app name: {name}")
    else:
        print("Could not detect current app name.")


def interactive_mode():
    """Interactive rebranding."""
    current = detect_current_name()

    print("\n" + "="*50)
    print("  Dicta Genizah Search - Rebranding Tool")
    print("="*50)

    if current:
        print(f"\nCurrent name: {current}")

    print("\nOptions:")
    print("  1. Dicta Genizah Search")
    print("  2. Genizah Search Pro (original)")
    print("  3. Dicta Genizah")
    print("  4. Custom name")
    print("  0. Cancel")

    choice = input("\nSelect option (0-4): ").strip()

    if choice == "0":
        print("Cancelled.")
        return
    elif choice == "1":
        new_name = "Dicta Genizah Search"
    elif choice == "2":
        new_name = "Genizah Search Pro"
    elif choice == "3":
        new_name = "Dicta Genizah"
    elif choice == "4":
        new_name = input("Enter new name: ").strip()
        if not new_name:
            print("No name entered. Cancelled.")
            return
    else:
        print("Invalid option.")
        return

    if not current:
        current = input(f"Enter current name to replace: ").strip()
        if not current:
            print("No name entered. Cancelled.")
            return

    confirm = input(f"\nReplace '{current}' with '{new_name}'? (y/n): ").strip().lower()
    if confirm == 'y':
        rebrand(current, new_name)
    else:
        print("Cancelled.")


def main():
    if len(sys.argv) == 1:
        interactive_mode()
    elif sys.argv[1] == "--show":
        show_current()
    elif sys.argv[1] == "--restore":
        current = detect_current_name()
        if current:
            rebrand(current, ORIGINAL_NAME)
        else:
            print("Could not detect current name. Use: python rebrand.py 'Current Name' --restore")
    elif sys.argv[1] == "--help" or sys.argv[1] == "-h":
        print(__doc__)
    else:
        new_name = sys.argv[1]
        current = detect_current_name()
        if current:
            rebrand(current, new_name)
        else:
            print("Could not detect current name.")
            print(f"Usage: python rebrand.py 'Old Name' 'New Name'")


if __name__ == "__main__":
    main()
