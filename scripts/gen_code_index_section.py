"""Generate a CODE_INDEX.md-compatible section for a Python module.

Walks top-level functions and classes via the ast module and emits bullets
matching the style of docs/CODE_INDEX.md:

    ## path/to/file.py

    - **Function** `name` (Line N) — docstring first line
    - **Class** `Name` (Line N) — docstring first line
        - Method `m` (Line N) — docstring first line
        - Property `p` (Line N)

Usage:
    python scripts/gen_code_index_section.py path/to/file.py [path/to/file2.py ...]

Prints all sections to stdout. Redirect or pipe to append to CODE_INDEX.md.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def _doc_first_line(node: ast.AST) -> str:
    doc = ast.get_docstring(node)
    if not doc:
        return ""
    first = doc.strip().splitlines()[0].strip()
    return f" \u2014 {first}" if first else ""


def _is_property(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return any(
        isinstance(d, ast.Name) and d.id == "property"
        for d in node.decorator_list
    )


def emit_section(path: Path) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    lines: list[str] = [f"## {path.as_posix()}", ""]

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            lines.append(
                f"- **Function** `{node.name}` (Line {node.lineno}){_doc_first_line(node)}"
            )
        elif isinstance(node, ast.ClassDef):
            lines.append(
                f"- **Class** `{node.name}` (Line {node.lineno}){_doc_first_line(node)}"
            )
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    kind = "Property" if _is_property(child) else "Method"
                    lines.append(
                        f"    - {kind} `{child.name}` (Line {child.lineno}){_doc_first_line(child)}"
                    )

    lines.append("")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if not argv:
        sys.stderr.write("usage: gen_code_index_section.py <file.py> [<file.py> ...]\n")
        return 2
    out: list[str] = []
    for arg in argv:
        path = Path(arg)
        if not path.exists():
            sys.stderr.write(f"skip (missing): {arg}\n")
            continue
        out.append(emit_section(path))
    sys.stdout.write("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
