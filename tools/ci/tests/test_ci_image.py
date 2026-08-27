# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pathlib import Path

import pytest

from tools.ci import ci_image


def _write_image_inputs(root: Path) -> None:
    contents = {
        ".dockerignore": "*\n!environment.yml\n",
        "environment.yml": (
            "dependencies:\n"
            "  - python=3.11.8\n"
            "  - pip:\n"
            "    - torch==2.1.2\n"
            "    - torchvision==0.16.2\n"
        ),
        "build.sbt": 'val sparkVersion = "3.5.0"\n',
        "templates/java_setup.yml": "inputs:\n  versionSpec: '11'\n",
        "tools/ci/ci_image.py": "# tag implementation\n",
        "tools/docker/ci/Dockerfile": "FROM ubuntu:22.04\n",
    }
    for relative, content in contents.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _pipeline(tag: str) -> str:
    return f"""resources:
  containers:
  - container: ci
    image: mmlsparkmcr.azurecr.io/synapseml/ci:{tag}
variables:
  CI_IMAGE_TAG: {tag}
"""


def test_tag_changes_when_any_image_input_changes(tmp_path):
    _write_image_inputs(tmp_path)
    original = ci_image.calculate_tag(tmp_path)

    for relative in ci_image.IMAGE_INPUTS:
        path = tmp_path / relative
        before = path.read_bytes()
        path.write_bytes(before + b"# changed\n")
        assert ci_image.calculate_tag(tmp_path) != original
        path.write_bytes(before)


def test_tag_is_stable_across_platform_line_endings(tmp_path):
    _write_image_inputs(tmp_path)
    for relative in ci_image.IMAGE_INPUTS:
        path = tmp_path / relative
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n"))
    original = ci_image.calculate_tag(tmp_path)

    for relative in ci_image.IMAGE_INPUTS:
        path = tmp_path / relative
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))

    assert ci_image.calculate_tag(tmp_path) == original


def test_runtime_values_are_derived_from_branch_files(tmp_path):
    _write_image_inputs(tmp_path)

    assert ci_image.spark_version(tmp_path) == "3.5.0"
    assert ci_image.java_version(tmp_path) == "11"
    assert ci_image.pip_dependency_version("torch", tmp_path) == "2.1.2"
    assert ci_image.pip_dependency_version("torchvision", tmp_path) == "0.16.2"
    assert ci_image.spark_sha512(tmp_path) == (
        "8883c67e0a138069e597f3e7d4edbbd5c3a565d50b28644aad02856a1ec1da7"
        "cb92b8f80454ca427118f69459ea326eaa073cf7b1a860c3b796f4b07c2101319"
    )


def test_cpu_wheel_urls_are_supported_for_spark_release_branches(tmp_path):
    _write_image_inputs(tmp_path)
    (tmp_path / "environment.yml").write_text(
        """dependencies:
  - python=3.12.11
  - pip:
    - "https://download.pytorch.org/whl/cpu/torch-2.9.1%2Bcpu-cp312-cp312-manylinux_2_28_x86_64.whl#sha256=abc"
    - "https://download.pytorch.org/whl/cpu/torchvision-0.24.1%2Bcpu-cp312-cp312-manylinux_2_28_x86_64.whl#sha256=def"
""",
        encoding="utf-8",
    )

    assert ci_image.pip_dependency_version("torch", tmp_path) == "2.9.1"
    assert ci_image.pip_dependency_version("torchvision", tmp_path) == "0.24.1"


@pytest.mark.parametrize(
    ("version", "checksum"),
    [
        (
            "4.0.1",
            "9198602c6b931b46686f32a25793b3bb58b522cd98a5b6a94d2484bae32e3e7b"
            "520d60f4bffe72ba29ff5c9ecd862443841ee47dde0f2f9e1bf52539f7baef41",
        ),
        (
            "4.1.1",
            "9f39e588e7d4c70ec0126109679f386eb9bfa26979dc42669fe4f3e3446a082dc"
            "a8ffbf5e8dbe8ad411cf2ce5bf803ce670341620bf52d968067acf86626106e",
        ),
    ],
)
def test_spark_release_branch_checksums(tmp_path, version, checksum):
    _write_image_inputs(tmp_path)
    (tmp_path / "build.sbt").write_text(
        f'val sparkVersion = "{version}"\n', encoding="utf-8"
    )

    assert ci_image.spark_sha512(tmp_path) == checksum


def test_unknown_spark_version_fails_with_an_actionable_error(tmp_path):
    _write_image_inputs(tmp_path)
    (tmp_path / "build.sbt").write_text(
        'val sparkVersion = "9.9.9"\n', encoding="utf-8"
    )

    with pytest.raises(ci_image.CIImageConfigError, match="9.9.9"):
        ci_image.spark_sha512(tmp_path)


def test_update_and_check_keep_both_pipeline_tags_in_sync(tmp_path):
    _write_image_inputs(tmp_path)
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(_pipeline("ci-000000000000"), encoding="utf-8")

    expected = ci_image.update_pipeline(tmp_path)

    assert expected == ci_image.calculate_tag(tmp_path)
    assert pipeline.read_text(encoding="utf-8").count(expected) == 2
    assert ci_image.check_pipeline(tmp_path) == expected


def test_check_rejects_a_stale_or_ambiguous_pipeline_tag(tmp_path):
    _write_image_inputs(tmp_path)
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(_pipeline("ci-000000000000"), encoding="utf-8")

    with pytest.raises(ci_image.CIImageConfigError, match="Run .* update"):
        ci_image.check_pipeline(tmp_path)

    pipeline.write_text(
        _pipeline("ci-000000000000") + "  CI_IMAGE_TAG: ci-111111111111\n",
        encoding="utf-8",
    )
    with pytest.raises(ci_image.CIImageConfigError, match="exactly one"):
        ci_image.update_pipeline(tmp_path)
