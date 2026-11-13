# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Append published JAR and wheel download links to Azure's Extensions summary."""

import argparse
import re
from pathlib import Path
from urllib.parse import quote


def artifact_links(version: str, maven_root: Path, repository_root: Path) -> list[str]:
    if not re.fullmatch(r"[A-Za-z0-9._+-]+", version) or version in (".", ".."):
        raise ValueError("Invalid package version.")
    jars = sorted(
        jar
        for jar in maven_root.glob(f"synapseml*/{version}/*.jar")
        if not jar.name.endswith(("-sources.jar", "-javadoc.jar", "-tests.jar"))
    )
    wheels = sorted(
        repository_root.glob("*/target/scala-*/generated/package/python/*.whl")
    )
    if not jars or not wheels:
        raise ValueError(
            "Both published SynapseML JARs and Python wheels are required."
        )
    links = []
    for jar in jars:
        relative = jar.relative_to(maven_root).as_posix()
        links.append(
            f"- [{jar.name}](https://mmlspark.blob.core.windows.net/maven/"
            f"com/microsoft/azure/{quote(relative)})"
        )
    for wheel in wheels:
        links.append(
            f"- [{wheel.name}](https://mmlspark.blob.core.windows.net/pip/"
            f"{quote(version, safe='')}/{quote(wheel.name)})"
        )
    return links


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--maven-root", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    links = artifact_links(args.version, args.maven_root, args.repository_root)
    with args.summary.open("a", encoding="utf-8") as summary:
        summary.write("\n### Download JARs and wheels\n\n" + "\n".join(links) + "\n")


if __name__ == "__main__":
    main()
