# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Adapt Internal's typing package-data helper to current OSS code generation."""

import argparse
import ast
from pathlib import Path
import sys
from typing import Optional, Sequence

LEGACY_PACKAGE_DATA = (
    'package_data={"synapseml": ["../LICENSE.txt", "../README.txt"], '
    '"": ["*.pyi", "py.typed"]},'
)
CURRENT_PACKAGE_DATA = (
    "package_data={\n"
    '        "": ["*.pyi", "py.typed"],\n'
    '        "synapseml": ["../LICENSE.txt", "../README.txt"],\n'
    "    },"
)
CURRENT_DECLARATION = r"""TYPING_PACKAGE_DATA = (
    "package_data={\n"
    "        \"\": [\"*.pyi\", \"py.typed\"],\n"
    "        \"synapseml\": [\"../LICENSE.txt\", \"../README.txt\"],\n"
    "    },"
)"""


def patch_internal_typing_support(path: Path) -> bool:
    """Update the known legacy constant and fail closed on unexpected drift."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    assignments = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "TYPING_PACKAGE_DATA"
            for target in node.targets
        )
    ]
    if len(assignments) != 1:
        raise ValueError(
            "Expected exactly one TYPING_PACKAGE_DATA assignment in "
            f"{path}; found {len(assignments)}"
        )

    assignment = assignments[0]
    try:
        package_data = ast.literal_eval(assignment.value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"TYPING_PACKAGE_DATA must be a string literal in {path}"
        ) from error
    if not isinstance(package_data, str):
        raise ValueError(f"TYPING_PACKAGE_DATA must be a string in {path}")
    if package_data == CURRENT_PACKAGE_DATA:
        return False
    if package_data != LEGACY_PACKAGE_DATA:
        raise ValueError(
            f"Unsupported TYPING_PACKAGE_DATA value in {path}: {package_data!r}"
        )

    lines = source.splitlines(keepends=True)
    replacement = [f"{line}\n" for line in CURRENT_DECLARATION.splitlines()]
    lines[assignment.lineno - 1 : assignment.end_lineno] = replacement
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write("".join(lines))
    return True


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args(argv)

    try:
        changed = patch_internal_typing_support(args.path)
    except (OSError, SyntaxError, ValueError) as error:
        parser.exit(2, f"error: {error}\n")

    state = "updated" if changed else "already compatible"
    print(f"Internal typing package-data helper is {state}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
