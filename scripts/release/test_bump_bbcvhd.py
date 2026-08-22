# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from pathlib import Path

import pytest

import bump_bbcvhd as bump


def make_component(
    tmp_path: Path,
    *,
    newline="\n",
    oss="1.1.1-spark4-0-1",
    internal="1.1.1-0-spark4.0",
    revision="1.4.26",
):
    component = tmp_path / "Components" / "MMLSpark" / "spark40"
    component.mkdir(parents=True)
    setup = (
        "#!/bin/bash"
        + newline
        + f"{bump.OSS_VAR}={oss}"
        + newline
        + f"{bump.INTERNAL_VAR}={internal}"
        + newline
        + "echo ready"
        + newline
    )
    bump.write_text_exact(component / "setup.sh", setup)
    bump.write_text_exact(component / "version.txt", revision + newline)
    return component


@pytest.mark.parametrize("newline", ["", "\n", "\r\n"])
def test_bump_component_revision_preserves_trailing_newline(newline):
    updated, old, new = bump.bump_component_revision("1.4.26" + newline)
    assert (old, new) == ("1.4.26", "1.4.27")
    assert updated == "1.4.27" + newline


@pytest.mark.parametrize("invalid", [" 1.4.26\n", "1.4.26 \n", "1.4\n", "\n1.4.26\n"])
def test_bump_component_revision_rejects_non_bare_content(invalid):
    with pytest.raises(ValueError):
        bump.bump_component_revision(invalid)


def test_set_shell_var_preserves_crlf():
    text = "A=1\r\nSYNAPSEML_VERSION=old\r\nB=2\r\n"
    updated, old = bump.set_shell_var(text, bump.OSS_VAR, "new")
    assert old == "old"
    assert updated == "A=1\r\nSYNAPSEML_VERSION=new\r\nB=2\r\n"


def test_set_shell_var_rejects_duplicate_assignments():
    text = "SYNAPSEML_VERSION=one\nSYNAPSEML_VERSION=two\n"
    with pytest.raises(ValueError):
        bump.set_shell_var(text, bump.OSS_VAR, "new")


def test_main_updates_component_and_preserves_crlf(tmp_path):
    component = make_component(tmp_path, newline="\r\n")

    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
        ]
    )

    assert result == 0
    setup = bump.read_text_exact(component / "setup.sh")
    assert f"{bump.OSS_VAR}=1.1.3-spark4-0\r\n" in setup
    assert f"{bump.INTERNAL_VAR}=1.1.3-0-spark4.0\r\n" in setup
    assert "\n" not in setup.replace("\r\n", "")
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\r\n"


def test_dry_run_does_not_write(tmp_path):
    component = make_component(tmp_path)
    before_setup = bump.read_text_exact(component / "setup.sh")
    before_revision = bump.read_text_exact(component / "version.txt")

    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
            "--dry-run",
        ]
    )

    assert result == 0
    assert bump.read_text_exact(component / "setup.sh") == before_setup
    assert bump.read_text_exact(component / "version.txt") == before_revision


def test_repeated_release_does_not_bump_component_revision(tmp_path, capsys):
    component = make_component(
        tmp_path,
        oss="1.1.3-spark4-0",
        internal="1.1.3-0-spark4.0",
    )

    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
        ]
    )

    assert result == 2
    assert bump.read_text_exact(component / "version.txt") == "1.4.26\n"
    assert "already references" in capsys.readouterr().err


def test_force_revision_allows_intentional_image_rebuild(tmp_path):
    component = make_component(
        tmp_path,
        oss="1.1.3-spark4-0",
        internal="1.1.3-0-spark4.0",
    )

    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
            "--force-revision",
        ]
    )

    assert result == 0
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\n"


def test_write_failure_rolls_back_both_files(tmp_path, monkeypatch, capsys):
    component = make_component(tmp_path)
    setup_path = component / "setup.sh"
    version_path = component / "version.txt"
    original_setup = bump.read_text_exact(setup_path)
    original_version = bump.read_text_exact(version_path)
    real_write = bump.write_text_exact
    calls = []

    def fail_second_write(path, text):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("simulated version write failure")
        real_write(path, text)

    monkeypatch.setattr(bump, "write_text_exact", fail_second_write)
    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
        ]
    )

    assert result == 1
    assert bump.read_text_exact(setup_path) == original_setup
    assert bump.read_text_exact(version_path) == original_version
    assert "was rolled back" in capsys.readouterr().err
