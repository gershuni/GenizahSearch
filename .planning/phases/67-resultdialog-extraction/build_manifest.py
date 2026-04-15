"""AST-derived dependency manifest for ResultDialog.

Walks the ResultDialog class body in genizah_app.py and catalogs every
external Name/Attribute reference (loads, not stores). Output is a
classified manifest that Phase 67 Plan 67-02 consumes verbatim.
"""
import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]  # C:\GenizahSearch
SRC = ROOT / "genizah_app.py"
OUT = Path(__file__).resolve().parent / "67-MANIFEST-raw.json"

STDLIB = {
    "abc", "argparse", "ast", "base64", "collections", "contextlib",
    "copy", "csv", "datetime", "enum", "functools", "glob", "hashlib",
    "html", "io", "itertools", "json", "logging", "math", "mimetypes",
    "os", "pathlib", "pickle", "random", "re", "shutil", "socket",
    "sqlite3", "string", "struct", "subprocess", "sys", "tempfile",
    "threading", "time", "traceback", "typing", "unicodedata", "urllib",
    "uuid", "warnings", "webbrowser", "zipfile",
}

THIRD_PARTY = {
    "requests", "urllib3", "PyQt6", "sip", "pytest",
}

# Categorize a top-level name (bare import/attribute root).
def classify(name, source):
    """Return category tag for a referenced name."""
    if name in STDLIB:
        return "a_stdlib"
    if name in THIRD_PARTY:
        return "a_third_party"
    # Qt classes typically start with Q
    if name.startswith("Q") and len(name) > 1 and name[1].isupper():
        return "a_qt"
    if name == "pyqtSignal":
        return "a_qt"
    # Module-imported top-level names are detected from source imports
    if name in source["genizah_core_names"]:
        return "b_genizah_core"
    if name in source["gui_threads_names"]:
        return "b_gui_threads"
    if name in source["corrections_ui_names"]:
        return "c_corrections_ui"
    if name in source["shared_names"]:
        return "b_shared"
    if name in source["desktop_widgets_names"]:
        return "b_desktop_widgets"
    if name in source["same_module_names"]:
        return "e_co_resident"
    # Heuristics for likely classes vs functions
    return "unclassified"


def collect_import_aliases(tree):
    """Map of module → list of names imported from it at module top level."""
    aliases = {"from_imports": {}, "plain_imports": set()}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for alias in node.names:
                aliases["from_imports"].setdefault(mod, []).append(
                    alias.asname or alias.name
                )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                aliases["plain_imports"].add(alias.asname or alias.name)
    return aliases


def collect_same_module_defs(tree):
    """Top-level defs/classes/assignments in genizah_app.py."""
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    names.add(t.id)
    return names


def find_class(tree, class_name):
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    return None


def extract_class_names(class_node):
    """All Name/Attribute loads inside a class body (excluding self.* and cls.*)."""
    refs = set()
    attr_refs = set()  # record self.X attrs for parent-access analysis

    class Visitor(ast.NodeVisitor):
        def visit_Name(self, node):
            if isinstance(node.ctx, ast.Load):
                refs.add(node.id)
            self.generic_visit(node)

        def visit_Attribute(self, node):
            # Trace back to root
            root = node
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name) and root.id == "self":
                # Record top-level self.X attribute name
                # Walk back one level to get the immediate attr
                cur = node
                chain = []
                while isinstance(cur, ast.Attribute):
                    chain.insert(0, cur.attr)
                    cur = cur.value
                if chain:
                    attr_refs.add(chain[0])
            self.generic_visit(node)

    Visitor().visit(class_node)
    return refs, attr_refs


def main():
    print(f"Reading {SRC}...", file=sys.stderr)
    source_text = SRC.read_text(encoding="utf-8")
    tree = ast.parse(source_text)

    aliases = collect_import_aliases(tree)
    same_module = collect_same_module_defs(tree)

    source = {
        "genizah_core_names": set(aliases["from_imports"].get("genizah_core", [])),
        "gui_threads_names": set(aliases["from_imports"].get("gui_threads", [])),
        "corrections_ui_names": set(aliases["from_imports"].get("corrections_ui", [])),
        "desktop_widgets_names": set(aliases["from_imports"].get("desktop.widgets", [])),
        "shared_names": set(),
        "same_module_names": same_module,
    }
    for mod, names in aliases["from_imports"].items():
        if mod.startswith("shared."):
            source["shared_names"].update(names)

    rd = find_class(tree, "ResultDialog")
    if rd is None:
        print("ResultDialog not found!", file=sys.stderr)
        sys.exit(1)

    refs, attr_refs = extract_class_names(rd)

    # Classify
    classified = {}
    for name in sorted(refs):
        cat = classify(name, source)
        classified.setdefault(cat, []).append(name)

    # Filter out builtins and locals-looking stuff
    builtins_like = {
        "True", "False", "None", "len", "str", "int", "float", "bool",
        "list", "dict", "set", "tuple", "range", "print", "open",
        "isinstance", "hasattr", "getattr", "setattr", "type",
        "enumerate", "zip", "sorted", "reversed", "min", "max", "sum",
        "any", "all", "filter", "map", "iter", "next", "super",
        "Exception", "ValueError", "KeyError", "TypeError", "RuntimeError",
        "IOError", "OSError", "StopIteration", "AttributeError",
        "NotImplementedError", "FileNotFoundError", "ImportError",
        "IndexError", "ZeroDivisionError", "UnicodeDecodeError",
        "UnicodeEncodeError", "NameError", "LookupError",
        "self", "cls",
    }
    for cat, names in classified.items():
        classified[cat] = [n for n in names if n not in builtins_like]

    # Output
    manifest = {
        "total_external_names": sum(len(v) for v in classified.values()),
        "categories": {cat: names for cat, names in classified.items() if names},
        "attr_refs_sample_top50": sorted(attr_refs)[:50],
        "attr_refs_total": len(attr_refs),
    }

    OUT.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {OUT}", file=sys.stderr)
    print(f"Total external names: {manifest['total_external_names']}", file=sys.stderr)
    for cat in sorted(manifest["categories"].keys()):
        print(f"  {cat}: {len(manifest['categories'][cat])}", file=sys.stderr)


if __name__ == "__main__":
    main()
