#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Stage one explicit Maven release for ESRP without modifying the Ivy cache."""

import argparse
import json
import re
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "release"))
from verify_release import PUBLIC_MAVEN_MODULES  # noqa: E402


def collect_artifacts(root, version, scala):
    if not re.fullmatch(r"[0-9][0-9A-Za-z.+_-]*", version):
        raise ValueError("version must be an explicit safe Maven version")
    if scala not in {"2.12", "2.13"}:
        raise ValueError("unsupported Scala binary version")
    root = Path(root).resolve()
    artifacts = []
    destinations = set()
    for name in PUBLIC_MAVEN_MODULES:
        module = f"{name}_{scala}"
        source = root / module / version
        if (
            not source.is_dir()
            or source.is_symlink()
            or not source.resolve().is_relative_to(root)
        ):
            raise ValueError(
                f"missing or linked release artifact directory: {module}/{version}"
            )
        selected = {}
        for path in sorted(source.rglob("*")):
            if not path.is_file() or not path.name.startswith(module):
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(source.resolve()):
                raise ValueError(
                    "release artifacts must stay inside the selected source directory"
                )
            suffix = path.name[len(module) :]
            if not suffix.startswith((".", "-")):
                continue
            filename = (
                path.name
                if suffix.startswith(f"-{version}.")
                or suffix.startswith(f"-{version}-")
                else f"{module}-{version}{suffix}"
            )
            destination = Path(module) / filename
            if destination in destinations:
                raise ValueError(
                    f"duplicate release artifact destination: {destination}"
                )
            if path.stat().st_size == 0:
                raise ValueError(f"empty release artifact: {destination}")
            destinations.add(destination)
            selected[filename] = path
            artifacts.append((path, destination))
        expected = [f"{module}-{version}.pom", f"{module}-{version}.jar"]
        if name == "synapseml-core":
            expected.append(f"{module}-{version}-tests.jar")
        if any(filename not in selected for filename in expected):
            raise ValueError(f"incomplete Maven release output for {module}/{version}")
        pom = ET.parse(selected[expected[0]]).getroot()

        def value(element):
            child = next(
                (child for child in pom if child.tag.rsplit("}", 1)[-1] == element),
                None,
            )
            return None if child is None or child.text is None else child.text.strip()

        if (value("groupId"), value("artifactId"), value("version")) != (
            "com.microsoft.azure",
            module,
            version,
        ):
            raise ValueError(f"unexpected Maven coordinates in {module} POM")
        for filename in expected[1:]:
            if not zipfile.is_zipfile(selected[filename]):
                raise ValueError(f"invalid release JAR: {filename}")
    return artifacts


def stage_release(root, output, version, scala):
    artifacts = collect_artifacts(root, version, scala)
    output = Path(output).resolve()
    if output.exists() or output.is_relative_to(Path(root).resolve()):
        raise ValueError("output must be a new directory outside the Ivy cache")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".esrp-stage-", dir=output.parent
    ) as temporary:
        staging = Path(temporary) / "artifacts"
        staging.mkdir()
        for source, relative in artifacts:
            destination = staging / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        staging.rename(output)
    return [relative.as_posix() for _, relative in artifacts]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--scala", required=True)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.home() / ".ivy2" / "local" / "com.microsoft.azure",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        files = stage_release(args.root, args.output, args.version, args.scala)
    except (ValueError, OSError, ET.ParseError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"version": args.version, "scala": args.scala, "files": files}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
