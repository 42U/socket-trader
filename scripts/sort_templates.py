"""Sort the NinjaTrader templates in strategy/ by kind for the installer.

NinjaTrader keeps ATM strategy templates in templates\\AtmStrategy and stop
strategy templates in templates\\StopStrategy, and shows

    Unable to load ATM strategy template from file '...\\algoNQmed.xml'

for any file in the ATM folder it cannot read as an ATM template. strategy/
in this repo holds both kinds side by side (the app's Setup installs them
by name, see STRATEGY_FILES in SocketTrader.py), so the installer must not
copy the folder wholesale — that is exactly what 1.0.0's Setup.exe did.

build.ps1 runs this before Inno Setup:

    python scripts/sort_templates.py strategy dist/templates

Each file lands in <out>/AtmStrategy or <out>/StopStrategy according to its
root element, and anything that is neither fails the build instead of
shipping to a folder NinjaTrader will complain about.
"""
from __future__ import annotations

import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

KINDS = ("AtmStrategy", "StopStrategy")


def template_kind(path: Path) -> str:
    """Return 'AtmStrategy' or 'StopStrategy' for a NinjaTrader template file.

    Raises ValueError for anything NinjaTrader would not load from either
    templates folder: not XML, not a <NinjaTrader> document, or a document
    whose single top-level element is something else.
    """
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise ValueError(f"{path.name}: not well-formed XML ({exc})") from None
    if root.tag != "NinjaTrader":
        raise ValueError(f"{path.name}: root element is <{root.tag}>, not <NinjaTrader>")
    children = [c for c in root]
    if len(children) != 1:
        raise ValueError(f"{path.name}: expected one element under <NinjaTrader>, found {len(children)}")
    kind = children[0].tag
    if kind not in KINDS:
        raise ValueError(f"{path.name}: <{kind}> is not an ATM or stop strategy template")
    return kind


def sort_templates(src: Path, out: Path) -> dict[str, list[str]]:
    """Copy every *.xml in src into out/<kind>/, replacing out entirely.

    Returns {kind: [file names]} for the build log. Raises ValueError on
    the first file that is not a template of a known kind; nothing is
    written in that case, so a bad file can never half-ship.
    """
    files = sorted(p for p in src.glob("*.xml") if p.is_file())
    kinds = {p: template_kind(p) for p in files}  # validate all before writing
    if out.exists():
        shutil.rmtree(out)
    sorted_names: dict[str, list[str]] = {k: [] for k in KINDS}
    for p, kind in kinds.items():
        dest = out / kind
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dest / p.name)
        sorted_names[kind].append(p.name)
    return sorted_names


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[0])
        print(f"usage: {Path(argv[0]).name} <strategy dir> <out dir>")
        return 2
    src, out = Path(argv[1]), Path(argv[2])
    if not src.is_dir():
        print(f"sort_templates: {src} is not a directory")
        return 2
    try:
        result = sort_templates(src, out)
    except ValueError as exc:
        print(f"sort_templates: {exc}")
        return 1
    for kind in KINDS:
        print(f"  {kind}: {len(result[kind])} template(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
