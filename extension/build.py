#!/usr/bin/env python3
"""Build Chrome and Firefox extension packages from shared source files."""

import zipfile
from pathlib import Path

EXT_DIR = Path(__file__).parent
DIST_DIR = EXT_DIR / "dist"

SHARED_FILES = [
    "background.js",
    "content_script.js",
    "icons/icon16.png",
    "icons/icon48.png",
    "icons/icon128.png",
]

TARGETS = {
    "chrome": {
        "manifest": "manifest.json",
        "output": "genizah-extension-chrome-v{version}.zip",
    },
    "firefox": {
        "manifest": "manifest.firefox.json",
        "output": "genizah-extension-firefox-v{version}.zip",
    },
}


def get_version():
    import json
    with open(EXT_DIR / "manifest.json", encoding="utf-8") as f:
        return json.load(f)["version"]


def build(target: str):
    cfg = TARGETS[target]
    version = get_version()
    manifest_src = EXT_DIR / cfg["manifest"]
    if not manifest_src.exists():
        print(f"  SKIP {target}: {cfg['manifest']} not found")
        return None

    DIST_DIR.mkdir(exist_ok=True)
    out_path = DIST_DIR / cfg["output"].format(version=version)

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Always store as manifest.json inside the zip
        zf.write(manifest_src, "manifest.json")
        for rel in SHARED_FILES:
            src = EXT_DIR / rel
            if src.exists():
                zf.write(src, rel)
            else:
                print(f"  WARN: {rel} not found, skipping")

    size_kb = out_path.stat().st_size / 1024
    print(f"  {target}: {out_path.name} ({size_kb:.0f} KB)")
    return out_path


def main():
    print(f"Building extension packages...")
    for target in TARGETS:
        build(target)
    print("Done.")


if __name__ == "__main__":
    main()
