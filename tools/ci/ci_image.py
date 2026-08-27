#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Manage the immutable content tag and runtime inputs for the CI image."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
PIPELINE_PATH = Path("pipeline.yaml")
IMAGE_INPUTS = (
    Path(".dockerignore"),
    Path("environment.yml"),
    Path("build.sbt"),
    Path("templates/java_setup.yml"),
    Path("tools/ci/ci_image.py"),
    Path("tools/docker/ci/Dockerfile"),
)

TAG_PATTERN = re.compile(r"ci-[0-9a-f]{12}")
SPARK_VERSION_PATTERN = re.compile(
    r'^\s*val\s+sparkVersion\s*=\s*"(?P<version>[^"]+)"\s*$',
    re.MULTILINE,
)
JAVA_VERSION_PATTERN = re.compile(
    r"^\s*versionSpec:\s*['\"](?P<version>[^'\"]+)['\"]\s*$",
    re.MULTILINE,
)
PIP_DEPENDENCY_PATTERNS = (
    r"^\s*-\s*['\"]?{name}==(?P<version>[0-9][0-9A-Za-z.]*)"
    r"(?:\+cpu)?['\"]?(?:\s+#.*)?$",
    r"^\s*-\s*['\"]?https?://[^'\"\s]*/{name}-"
    r"(?P<version>[0-9][0-9A-Za-z.]*)"
    r"(?:%2B|\+)cpu-[^'\"\s]+['\"]?\s*$",
)

SPARK_SHA512 = {
    "3.5.0": (
        "8883c67e0a138069e597f3e7d4edbbd5c3a565d50b28644aad02856a1ec1da7c"
        "b92b8f80454ca427118f69459ea326eaa073cf7b1a860c3b796f4b07c2101319"
    ),
    "4.0.1": (
        "9198602c6b931b46686f32a25793b3bb58b522cd98a5b6a94d2484bae32e3e7b"
        "520d60f4bffe72ba29ff5c9ecd862443841ee47dde0f2f9e1bf52539f7baef41"
    ),
    "4.1.1": (
        "9f39e588e7d4c70ec0126109679f386eb9bfa26979dc42669fe4f3e3446a082dc"
        "a8ffbf5e8dbe8ad411cf2ce5bf803ce670341620bf52d968067acf86626106e"
    ),
}


class CIImageConfigError(ValueError):
    """Raised when CI image metadata is missing, stale, or ambiguous."""


def _read(root: Path, relative_path: Path) -> str:
    return (root / relative_path).read_text(encoding="utf-8")


def _matched_version(
    root: Path, relative_path: Path, pattern: re.Pattern[str], description: str
) -> str:
    match = pattern.search(_read(root, relative_path))
    if not match:
        raise CIImageConfigError(f"Could not find {description} in {relative_path}")
    return match.group("version")


def calculate_tag(root: Path = ROOT) -> str:
    digest = hashlib.sha256()
    for relative_path in IMAGE_INPUTS:
        path = root / relative_path
        if not path.is_file():
            raise CIImageConfigError(f"CI image input does not exist: {relative_path}")
        digest.update(relative_path.as_posix().encode("utf-8"))
        digest.update(b"\0")
        contents = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        digest.update(contents)
        digest.update(b"\0")
    return f"ci-{digest.hexdigest()[:12]}"


content_tag = calculate_tag


def spark_version(root: Path = ROOT) -> str:
    return _matched_version(
        root, Path("build.sbt"), SPARK_VERSION_PATTERN, "Spark version"
    )


def java_version(root: Path = ROOT) -> str:
    return _matched_version(
        root,
        Path("templates/java_setup.yml"),
        JAVA_VERSION_PATTERN,
        "Java version",
    )


def spark_sha512(root: Path = ROOT) -> str:
    version = spark_version(root)
    try:
        return SPARK_SHA512[version]
    except KeyError as exc:
        supported = ", ".join(sorted(SPARK_SHA512))
        raise CIImageConfigError(
            f"Unsupported Spark version {version!r}; add its official SHA-512 "
            f"to tools/ci/ci_image.py. Supported versions: {supported}"
        ) from exc


def pip_dependency_version(name: str, root: Path = ROOT) -> str:
    environment = _read(root, Path("environment.yml"))
    for raw_pattern in PIP_DEPENDENCY_PATTERNS:
        pattern = re.compile(
            raw_pattern.format(name=re.escape(name)),
            re.MULTILINE | re.IGNORECASE,
        )
        match = pattern.search(environment)
        if match:
            return match.group("version")
    raise CIImageConfigError(f"Could not find an exact {name} pin in environment.yml")


def current_pipeline_tags(root: Path = ROOT) -> list[str]:
    return TAG_PATTERN.findall(_read(root, PIPELINE_PATH))


def _validate_tag_locations(tags: Sequence[str]) -> None:
    if len(tags) != 2:
        raise CIImageConfigError(
            "pipeline.yaml must contain exactly one resource tag and exactly "
            f"one publisher tag (two total); found {len(tags)}"
        )


def check_pipeline(root: Path = ROOT) -> str:
    expected = calculate_tag(root)
    tags = current_pipeline_tags(root)
    _validate_tag_locations(tags)
    if any(tag != expected for tag in tags):
        raise CIImageConfigError(
            "CI image tag is stale or inconsistent: "
            f"expected both locations to use {expected}, found {tags}. "
            "Run `python tools/ci/ci_image.py update`."
        )
    return expected


def update_pipeline(root: Path = ROOT) -> str:
    pipeline_path = root / PIPELINE_PATH
    pipeline = pipeline_path.read_text(encoding="utf-8")
    tags = TAG_PATTERN.findall(pipeline)
    _validate_tag_locations(tags)
    expected = calculate_tag(root)
    with pipeline_path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(TAG_PATTERN.sub(expected, pipeline))
    return expected


check_pipeline_tags = check_pipeline
update_pipeline_tags = update_pipeline


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("tag", "check", "update", "java-version", "spark-version"):
        subparsers.add_parser(command)
    subparsers.add_parser("spark-sha512")
    dependency = subparsers.add_parser("dependency-version")
    dependency.add_argument("name")
    subparsers.add_parser("inputs")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "tag":
            print(calculate_tag())
        elif args.command == "check":
            check_pipeline()
        elif args.command == "update":
            print(update_pipeline())
        elif args.command == "java-version":
            print(java_version())
        elif args.command == "spark-version":
            print(spark_version())
        elif args.command == "spark-sha512":
            print(spark_sha512())
        elif args.command == "dependency-version":
            print(pip_dependency_version(args.name))
        elif args.command == "inputs":
            print("\n".join(path.as_posix() for path in IMAGE_INPUTS))
    except (OSError, CIImageConfigError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
