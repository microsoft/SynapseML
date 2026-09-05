# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.skipif(
    os.environ.get("SYNAPSEML_TEST_RELEASE_SBT") != "1",
    reason="Set SYNAPSEML_TEST_RELEASE_SBT=1 with the branch-selected JDK and sbt",
)
def test_real_sbt_version_resolver_and_immutable_artifact_policy(tmp_path):
    source = tmp_path / "source"
    source.mkdir()

    def git(*arguments):
        return subprocess.run(
            ["git", "-C", str(source), *arguments],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "-q")
    git("config", "user.email", "release-test@example.invalid")
    git("config", "user.name", "Release regression")
    git("config", "core.autocrlf", "false")
    (source / "code.txt").write_text("reviewed\n", encoding="utf-8")
    git("add", "code.txt")
    git("commit", "-qm", "test: create reviewed release fixture")
    commit = git("rev-parse", "HEAD")
    for tag in ("v1.1.4", "v1.1.4-spark3.5", "v1.1.4-python3.11"):
        git("tag", tag)
    git("commit", "--allow-empty", "-qm", "test: create conflicting source")
    git("tag", "v1.1.4-spark4.0")
    git("checkout", "--detach", commit)

    project = tmp_path / "project"
    project.mkdir()
    shutil.copyfile(
        ROOT / "project" / "ReleaseVersion.scala", project / "ReleaseVersion.scala"
    )
    shutil.copyfile(ROOT / "project" / "build.properties", project / "build.properties")
    (tmp_path / "build.sbt").write_text(
        """
name := "release-version-regression"
TaskKey[Unit]("releaseVersionChecks") := {
  val directory = baseDirectory.value / "source"
  val valid = Map(
    "SYNAPSEML_RELEASE_TAG" -> "v1.1.4",
    "SYNAPSEML_RELEASE_COMMIT" -> "COMMIT",
    "SYNAPSEML_RELEASE_PLAN_ID" -> ("c" * 64))
  def resolve(environment: Map[String, String]): String =
    ReleaseVersion.resolve("1.1.4", environment, directory)
  def rejected(environment: Map[String, String]): Unit =
    assert(scala.util.Try(resolve(environment)).isFailure)
  assert(resolve(Map.empty) == "1.1.4-SNAPSHOT")
  assert(resolve(Map("BUILD_SOURCEBRANCH" -> "refs/tags/v1.1.4")) == "1.1.4-SNAPSHOT")
  assert(resolve(valid - "SYNAPSEML_RELEASE_TAG") == "1.1.4-SNAPSHOT")
  assert(resolve(valid) == "1.1.4")
  assert(resolve(valid.updated("SYNAPSEML_RELEASE_TAG",
    "refs/tags/v1.1.4-python3.11")) == "1.1.4-python3.11")
  rejected(valid - "SYNAPSEML_RELEASE_COMMIT")
  rejected(valid - "SYNAPSEML_RELEASE_PLAN_ID")
  rejected(valid.updated("SYNAPSEML_RELEASE_COMMIT", "b" * 40))
  rejected(valid.updated("SYNAPSEML_RELEASE_TAG", "v1.1.04"))
  rejected(valid.updated("SYNAPSEML_RELEASE_TAG", "v1.1.4-spark4.0"))
  IO.write(directory / "code.txt", "dirty")
  rejected(valid)
  IO.write(directory / "code.txt", "reviewed\\n")
  IO.write(directory / "unreviewed.scala", "untracked")
  rejected(valid)
  assert(ReleaseVersion.mayOverwrite("maven", "group/artifact/1.1.4-SNAPSHOT"))
  assert(!ReleaseVersion.mayOverwrite("maven", "group/snapshot-artifact/1.1.4"))
  assert(ReleaseVersion.mayOverwrite("pip", "1.1.4-SNAPSHOT/package.whl"))
  assert(!ReleaseVersion.mayOverwrite("pip", "1.1.4/snapshot-package.whl"))
  assert(ReleaseVersion.mayOverwrite("rrr", "synapseml-1.1.4-SNAPSHOT.zip"))
  assert(!ReleaseVersion.mayOverwrite("rrr", "synapseml-1.1.4.zip"))
  assert(ReleaseVersion.mayOverwrite("docs", "latest/index.html"))
  println("RELEASE_VERSION_CHECKS_PASSED")
}
""".replace(
            '"COMMIT"', f'"{commit}"'
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        ["sbt", "-batch", "releaseVersionChecks"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "RELEASE_VERSION_CHECKS_PASSED" in result.stdout
    assert "ReleaseVersion.resolve(dynverVersion, sys.env)" in (
        ROOT / "build.sbt"
    ).read_text(encoding="utf-8")
    actual_build = (ROOT / "build.sbt").read_text(encoding="utf-8")
    publication_task = (
        "val publishPypi ="
        + actual_build.split("val publishPypi =", 1)[1].split("val publishDocs =", 1)[0]
    )
    (project / "PublicationFixture.scala").write_text(
        """
import sbt._
object Secrets { val pypiApiToken: String = "inert-fixture-value" }
object PublicationFixture {
  val packageSynapseML = taskKey[Unit]("No-op local package fixture")
  val rootGenDir = settingKey[File]("Fixture package directory")
  def pythonizedVersion(value: String): String = value
  def activateCondaEnv: Seq[String] = Seq.empty
  def join(root: File, parts: String*): File =
    parts.foldLeft(root)((dir, part) => new File(dir, part))
  def runCmd(command: Seq[String], envVars: Map[String, String] = Map.empty): Unit = {
    assert(command.take(2) == Seq("twine", "upload"))
    assert(!command.contains("--skip-existing"))
    assert(!command.contains("--password"))
    assert(envVars("TWINE_USERNAME") == "__token__")
    assert(envVars("TWINE_PASSWORD") == Secrets.pypiApiToken)
    throw new IllegalStateException("SIMULATED_PYPI_COLLISION")
  }
}
""",
        encoding="utf-8",
    )
    stub = """
import PublicationFixture._
name := "synapseml"
version := "1.1.4"
packageSynapseML := {}
rootGenDir := baseDirectory.value
"""
    fixture = tmp_path / "build.sbt"
    fixture.write_text(
        fixture.read_text(encoding="utf-8") + stub + publication_task,
        encoding="utf-8",
    )
    collision = subprocess.run(
        ["sbt", "-batch", "publishPypi"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert collision.returncode != 0
    assert "SIMULATED_PYPI_COLLISION" in collision.stdout + collision.stderr
