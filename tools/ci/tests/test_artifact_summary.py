# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import subprocess
import sys
from pathlib import Path

import pytest

from tools.ci.write_artifact_summary import artifact_links


def create_file(root, relative):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("test artifact")
    return path


def test_summary_appends_actual_versioned_downloads_without_losing_existing_content(
    tmp_path,
):
    maven, repo = tmp_path / "maven", tmp_path / "repo"
    version = "1.2.0-test-SNAPSHOT"
    jar = create_file(
        maven, f"synapseml-core_2.12/{version}/synapseml-core_2.12-{version}.jar"
    )
    create_file(
        maven,
        f"synapseml-core_2.12/{version}/synapseml-core_2.12-{version}-sources.jar",
    )
    create_file(maven, "synapseml-core_2.12/old/synapseml-core-old.jar")
    wheel = create_file(
        repo,
        "core/target/scala-2.12/generated/package/python/synapseml_core-1.2.0.dev1-py3-none-any.whl",
    )
    summary = tmp_path / "Build.md"
    summary.write_text("Existing Maven coordinates and documentation\n")
    script = Path(__file__).resolve().parents[1] / "write_artifact_summary.py"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--version",
            version,
            "--maven-root",
            str(maven),
            "--repository-root",
            str(repo),
            "--summary",
            str(summary),
        ],
        check=True,
    )
    text = summary.read_text()
    assert text.startswith("Existing Maven coordinates and documentation\n")
    assert (
        f"/maven/com/microsoft/azure/synapseml-core_2.12/{version}/{jar.name})" in text
    )
    assert f"/pip/{version}/{wheel.name})" in text
    assert "-sources.jar" not in text
    assert "/old/" not in text


@pytest.mark.parametrize("version", ["../other", "*", "..", ""])
def test_invalid_version_is_not_used_as_glob(tmp_path, version):
    with pytest.raises(ValueError, match="version"):
        artifact_links(version, tmp_path, tmp_path)


@pytest.mark.parametrize("kind", ["jar", "wheel", "neither"])
def test_missing_artifact_family_cannot_produce_a_success_summary(tmp_path, kind):
    if kind == "jar":
        create_file(tmp_path, "synapseml_2.12/1.2.0/synapseml_2.12-1.2.0.jar")
    if kind == "wheel":
        create_file(
            tmp_path,
            "core/target/scala-2.12/generated/package/python/synapseml_core-1.2.0.whl",
        )
    with pytest.raises(ValueError, match="Both"):
        artifact_links("1.2.0", tmp_path, tmp_path)
