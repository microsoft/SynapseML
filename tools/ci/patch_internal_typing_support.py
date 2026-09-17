# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Adapt Internal's typing package-data helper to current OSS code generation."""

import argparse
import ast
import os
from pathlib import Path
import re
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


def _active_scala(source: str) -> str:
    """Remove comments and multiline strings before recognizing a legacy task."""
    output = []
    position = 0
    depth = 0
    while position < len(source):
        if source.startswith("/*", position):
            depth += 1
            output.append(" ")
            position += 2
        elif depth:
            if source.startswith("*/", position):
                depth -= 1
                position += 2
            else:
                output.append("\n" if source[position] == "\n" else " ")
                position += 1
        elif source.startswith("//", position):
            end = source.find("\n", position)
            position = len(source) if end < 0 else end
        elif source.startswith('"""', position):
            end = source.find('"""', position + 3)
            if end < 0:
                raise ValueError("Unterminated multiline string in legacy codegen")
            while end + 3 < len(source) and source[end + 3] == '"':
                end += 1
            output.append('""')
            position = end + 3
        elif source[position] == "'" and (
            character := re.match(r"'(?:\\.|[^'\\])'", source[position:])
        ):
            output.append(character.group())
            position += len(character.group())
        elif source[position] == '"':
            quoted = re.match(r'"(?:\\.|[^"\\])*"', source[position:])
            if quoted is None:
                line = source.count("\n", 0, position) + 1
                raise ValueError(
                    f"Unterminated string in legacy codegen at line {line}"
                )
            output.append(quoted.group())
            position += len(quoted.group())
        else:
            output.append(source[position])
            position += 1
    if depth:
        raise ValueError("Unterminated comment in legacy codegen")
    return "".join(output)


def validate_legacy_codegen(path: Path) -> None:
    """Recognize older builds that use OSS codegen without a typing adapter."""
    root = path.parent.parent
    if path.name != "typing_build_support.py" or path.parent.name != "utils":
        raise ValueError(f"Unexpected missing typing-helper path: {path}")
    build = root / "build.sbt"
    plugin = root / "project" / "CodegenPlugin.scala"
    if not build.is_file():
        raise ValueError(f"Missing Internal build definition: {build}")
    if not plugin.is_file():
        raise ValueError(f"Missing Internal codegen plugin: {plugin}")
    source = plugin.read_text(encoding="utf-8")
    build_sources = []
    for directory, directories, files in os.walk(root):
        directories[:] = [
            name
            for name in directories
            if name not in {"target", ".git", ".venv", "node_modules"}
        ]
        directory = Path(directory)
        in_project = directory.relative_to(root).parts[:1] == ("project",)
        build_sources.extend(
            directory / name
            for name in files
            if name.endswith(".sbt") or (in_project and name.endswith(".scala"))
        )
    if any(path.stem in file.read_text(encoding="utf-8") for file in build_sources):
        raise ValueError(f"Internal build references the missing typing helper: {path}")
    active = _active_scala(source)
    direct_codegen = re.search(
        r"(?m)^\s*codegen\s*:=\s*\(Def\.taskDyn\s*\{\s*"
        r"\(Compile\s*/\s*compile\)\.value\s*"
        r"\(Test\s*/\s*compile\)\.value\s*"
        r"val\s+arg\s*=\s*codegenArgs\.value\s*"
        r"Def\.task\s*\{\s*"
        r'\(Compile\s*/\s*runMain\)\.toTask\(s"\s*'
        r'com\.microsoft\.azure\.synapse\.ml\.codegen\.CodeGen\s+\$arg"\)\.value'
        r"\s*\}\s*\}\s*\.value\s*\)",
        active,
    )
    packaged_codegen = re.search(
        r"(?m)^\s*packagePython\s*:=\s*\{\s*codegen\.value\b", active
    )
    if direct_codegen is None or packaged_codegen is None:
        raise ValueError(
            f"Unrecognized Internal codegen without a typing helper: {plugin}"
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args(argv)

    try:
        if not args.path.exists():
            validate_legacy_codegen(args.path)
            print(
                "Legacy Internal build uses OSS code generation directly; "
                "no typing-helper patch is required. Packaging and tests remain enabled."
            )
            return 0
        changed = patch_internal_typing_support(args.path)
    except (OSError, SyntaxError, ValueError) as error:
        parser.exit(2, f"error: {error}\n")

    state = "updated" if changed else "already compatible"
    print(f"Internal typing package-data helper is {state}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
