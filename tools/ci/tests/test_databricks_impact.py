# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import subprocess
import sys

from tools.ci.databricks_impact import should_run_databricks


def test_skips_clearly_non_impacting_changes():
    assert not should_run_databricks(
        [
            ".github/workflows/pr-validation.yml",
            "docs/Reference/Developer Setup.md",
            "website/src/pages/index.js",
            "tools/ci/tests/test_pipeline_yaml.py",
            "lightgbm/src/test/scala/example/TrainUtilsSuite.scala",
            "core/src/test/python/synapsemltest/test_core.py",
        ]
    )


def test_runs_for_runtime_notebook_build_and_pipeline_changes():
    impacting_paths = [
        "core/src/main/scala/example/Transformer.scala",
        "docs/Explore Algorithms/Quickstart.ipynb",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala",
        "build.sbt",
        "project/Build.scala",
        "pipeline.yaml",
        "templates/publish.yml",
    ]
    for path in impacting_paths:
        assert should_run_databricks([path]), path


def test_mixed_changes_run():
    assert should_run_databricks(
        [
            "README.md",
            "cognitive/src/main/scala/example/Service.scala",
        ]
    )


def test_empty_or_unsafe_paths_fail_open():
    assert should_run_databricks([])
    assert should_run_databricks(["../outside-repository"])
    assert should_run_databricks(["/absolute/path"])


def test_cli_accepts_null_delimited_git_paths():
    process = subprocess.run(
        [sys.executable, "-m", "tools.ci.databricks_impact", "--null"],
        input=b"README.md\0tools/ci/README.md\0",
        capture_output=True,
        check=False,
    )
    assert process.returncode == 0
    assert process.stdout == b"false\n"
