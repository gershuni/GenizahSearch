"""Test configuration for ensuring project modules are importable."""

import sys
import types
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ROOT_PATH = str(ROOT_DIR)

if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)


# ---------------------------------------------------------------------------
# Phase 85 (Plan 85-03 + 85-02): pin `scripts` to the root namespace package.
#
# Both `C:/Genizahsearch/scripts/` (root, namespace package) and
# `C:/Genizahsearch/skills/cairo-genizah-research/scripts/__init__.py`
# (skill, regular package) coexist. When pytest collects tests across both
# trees in one process, sys.modules['scripts'] can end up bound to whichever
# was imported first — leading to ImportError ("cannot import name 'X' from
# 'scripts' ...skills/.../scripts/__init__.py") for callers expecting the
# root one. Bind the root namespace package up-front so subsequent
# `from scripts import X` resolves to root scripts/.
# ---------------------------------------------------------------------------
def _pin_root_scripts_namespace() -> None:
    root_scripts = ROOT_DIR / "scripts"
    if not root_scripts.is_dir():
        return
    existing = sys.modules.get("scripts")
    existing_path = getattr(existing, "__path__", None)
    # If already bound to a path that does not include root scripts/, replace.
    if existing is None or str(root_scripts) not in (list(existing_path) if existing_path else []):
        pkg = types.ModuleType("scripts")
        pkg.__path__ = [str(root_scripts)]  # type: ignore[attr-defined]
        pkg.__package__ = "scripts"
        sys.modules["scripts"] = pkg


_pin_root_scripts_namespace()


# ---------------------------------------------------------------------------
# Skill import bridge: skills/cairo-genizah-research/ (hyphens in dir name)
# is not directly importable as `skills.cairo_genizah_research`. Register it
# as a package under the underscore name so test imports work without renaming
# the directory (Anthropic Skill naming mandates hyphens).
# ---------------------------------------------------------------------------
def _register_skill_package() -> None:
    skill_dir = ROOT_DIR / "skills" / "cairo-genizah-research"
    if not skill_dir.is_dir():
        return  # skill not yet scaffolded

    # Ensure top-level `skills` package exists in sys.modules
    if "skills" not in sys.modules:
        skills_pkg = types.ModuleType("skills")
        skills_pkg.__path__ = [str(ROOT_DIR / "skills")]  # type: ignore[attr-defined]
        skills_pkg.__package__ = "skills"
        sys.modules["skills"] = skills_pkg

    # Register `skills.cairo_genizah_research` pointing at the hyphenated dir
    pkg_name = "skills.cairo_genizah_research"
    if pkg_name not in sys.modules:
        pkg = types.ModuleType(pkg_name)
        pkg.__path__ = [str(skill_dir)]  # type: ignore[attr-defined]
        pkg.__package__ = pkg_name
        sys.modules[pkg_name] = pkg

    # Register `skills.cairo_genizah_research.scripts`
    scripts_dir = skill_dir / "scripts"
    scripts_name = "skills.cairo_genizah_research.scripts"
    if scripts_name not in sys.modules and scripts_dir.is_dir():
        scripts_pkg = types.ModuleType(scripts_name)
        scripts_pkg.__path__ = [str(scripts_dir)]  # type: ignore[attr-defined]
        scripts_pkg.__package__ = scripts_name
        sys.modules[scripts_name] = scripts_pkg


_register_skill_package()
