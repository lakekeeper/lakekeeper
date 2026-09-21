#!/usr/bin/env python3
"""Build the notebooks from the plain-Python sources in `notebooks/src/`.

Edit `notebooks/src/*.py`, then run:

    python build_notebooks.py

Why the sources are separate files rather than strings in here: an earlier version kept
each cell as a Python string inside this script, so every backslash passed through two
layers of escaping. A `\n` that loses one layer becomes a *real* newline inside a quoted
string, producing a notebook that writes out fine and raises SyntaxError on the reader's
first run. Keeping cell bodies in their own files removes the nesting, so the bug cannot
occur — and the sources stay readable, diffable and editable with normal tooling.

Format is the standard "percent" one, which editors and jupytext understand:

    # %% [markdown]
    # Prose, one `# ` per line.

    # %%
    code = "goes here"

Notebooks are generated: edit the sources, never the `.ipynb`.
"""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "notebooks" / "src"
OUT = HERE / "notebooks"

MARK_CODE = "# %%"
MARK_MD = "# %% [markdown]"


def parse(text: str) -> list[dict]:
    """Split percent-format source into notebook cells."""
    cells: list[dict] = []
    state = {"kind": None, "buffer": []}

    def flush() -> None:
        kind, buffer = state["kind"], state["buffer"]
        if kind is None:
            return
        body = "\n".join(buffer).strip("\n")
        if not body:
            return
        if kind == "markdown":
            # Strip the leading "# " that keeps prose valid Python.
            body = "\n".join(
                line[2:] if line.startswith("# ") else ("" if line == "#" else line)
                for line in body.split("\n")
            )
        # nbformat 4.5+ wants an id per cell and warns without one; a future version
        # makes it an error. Derive it from position and content so a rebuild that
        # changes nothing produces a byte-identical file — a random id would make every
        # regeneration a diff.
        digest = hashlib.sha256(f"{len(cells)}:{body}".encode()).hexdigest()[:8]
        cell = {
            "cell_type": kind,
            "id": f"{kind[:2]}-{len(cells):02d}-{digest}",
            "metadata": {},
            "source": body.splitlines(keepends=True),
        }
        if kind == "code":
            cell["execution_count"] = None
            cell["outputs"] = []
        cells.append(cell)

    for line in text.split("\n"):
        if line.startswith(MARK_MD):
            flush()
            state["kind"], state["buffer"] = "markdown", []
        elif line.startswith(MARK_CODE):
            flush()
            state["kind"], state["buffer"] = "code", []
        else:
            state["buffer"].append(line)
    flush()
    return cells


def validate(name: str, cells: list[dict]) -> list[str]:
    """Parse every code cell, so a notebook that cannot run is never written."""
    problems = []
    for i, cell in enumerate(cells):
        if cell["cell_type"] != "code":
            continue
        try:
            ast.parse("".join(cell["source"]))
        except SyntaxError as exc:
            problems.append(f"{name} cell {i}: {exc.msg} (line {exc.lineno})")
    return problems


def notebook(cells: list[dict]) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.12"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    sources = sorted(SRC.glob("*.py"))
    if not sources:
        raise SystemExit(f"no sources in {SRC}")

    built, problems = [], []
    for source in sources:
        cells = parse(source.read_text())
        problems += validate(source.name, cells)
        built.append((OUT / f"{source.stem}.ipynb", cells))

    if problems:
        raise SystemExit(
            "refusing to write notebooks that will not run:\n  " + "\n  ".join(problems)
        )

    for path, cells in built:
        path.write_text(json.dumps(notebook(cells), indent=1) + "\n")
        print(f"wrote {path.relative_to(HERE)} ({len(cells)} cells)")


if __name__ == "__main__":
    main()
