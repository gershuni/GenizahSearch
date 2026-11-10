"""Create a portable skill ZIP without caches or local runtime state."""
import argparse
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED


def package(output: Path) -> None:
    root = Path(__file__).resolve().parent.parent
    files = [root / 'SKILL.md', root / 'README.md']
    for folder in ('scripts', 'references'):
        files.extend(p for p in (root / folder).rglob('*') if p.is_file()
                     and '__pycache__' not in p.parts and p.suffix in {'.py', '.md', '.json'})
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
        for path in sorted(files):
            archive.write(path, root.name + '/' + path.relative_to(root).as_posix())
    print(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    package(parser.parse_args().output)
