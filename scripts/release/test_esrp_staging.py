# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import importlib.util
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_release import PUBLIC_MAVEN_MODULES  # noqa: E402

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "esrp" / "prepare_jar.py"
SPEC = importlib.util.spec_from_file_location("release_esrp_staging", SCRIPT)
staging = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(staging)


def ivy_fixture(root, version, scala):
    for name in PUBLIC_MAVEN_MODULES:
        module = f"{name}_{scala}"
        output = root / module / version / "artifacts"
        output.mkdir(parents=True)
        (output / f"{module}.pom").write_text(
            f"<project><groupId>com.microsoft.azure</groupId><artifactId>{module}</artifactId>"
            f"<version>{version}</version></project>",
            encoding="utf-8",
        )
        classifiers = ["", "-tests"] if name == "synapseml-core" else [""]
        for classifier in classifiers:
            with zipfile.ZipFile(output / f"{module}{classifier}.jar", "w") as jar:
                jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")


@pytest.mark.parametrize(
    "scala,version", [("2.12", "1.1.4"), ("2.13", "1.1.4-spark4.0")]
)
def test_stages_exact_version_for_both_scala_lines_without_mutating_cache(
    tmp_path, scala, version
):
    root, output = tmp_path / "ivy", tmp_path / "stage"
    ivy_fixture(root, "1.0.0", scala)
    ivy_fixture(root, version, scala)
    before = {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    files = staging.stage_release(root, output, version, scala)
    assert len(files) == len(PUBLIC_MAVEN_MODULES) * 2 + 1
    assert all(f"-{version}" in filename for filename in files)
    assert {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    } == before
    assert f"synapseml-core_{scala}/synapseml-core_{scala}-{version}-tests.jar" in files


@pytest.mark.parametrize("corruption", ["missing", "empty", "wrong-pom", "duplicate"])
def test_bad_release_output_fails_before_staging(tmp_path, corruption):
    root, output = tmp_path / "ivy", tmp_path / "stage"
    ivy_fixture(root, "1.1.4", "2.12")
    artifact = root / "synapseml_2.12" / "1.1.4" / "artifacts"
    if corruption == "missing":
        (artifact / "synapseml_2.12.pom").unlink()
    elif corruption == "empty":
        (artifact / "synapseml_2.12.jar").write_bytes(b"")
    elif corruption == "wrong-pom":
        (artifact / "synapseml_2.12.pom").write_text(
            "<project><version>9.9.9</version></project>"
        )
    else:
        duplicate = artifact.parent / "other"
        duplicate.mkdir()
        (duplicate / "synapseml_2.12.pom").write_bytes(
            (artifact / "synapseml_2.12.pom").read_bytes()
        )
    with pytest.raises(ValueError):
        staging.stage_release(root, output, "1.1.4", "2.12")
    assert not output.exists()
