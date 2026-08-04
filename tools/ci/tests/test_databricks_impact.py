# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import subprocess
import sys

from tools.ci.databricks_impact import CPU_SUITE, GPU_SUITE, should_run_databricks


def test_skips_clearly_non_impacting_changes():
    paths = [
        ".github/workflows/pr-validation.yml",
        ".pipelines/clean-acr.yml",
        "docs/Reference/Developer Setup.md",
        "website/src/pages/index.js",
        "tools/acr/clean-acr.py",
        "tools/ci/tests/test_pipeline_yaml.py",
        "tools/docker/minimal/Dockerfile",
        "lightgbm/src/test/scala/example/TrainUtilsSuite.scala",
        "core/src/test/python/synapsemltest/test_core.py",
    ]
    assert not should_run_databricks(paths, CPU_SUITE)
    assert not should_run_databricks(paths, GPU_SUITE)


def test_cpu_runs_for_cpu_runtime_and_notebooks_only():
    cpu_paths = [
        "cognitive/src/main/scala/example/Service.scala",
        "lightgbm/src/main/scala/example/TrainUtils.scala",
        "docs/Explore Algorithms/LightGBM/Quickstart.ipynb",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksCPUTests.scala",
    ]
    for path in cpu_paths:
        assert should_run_databricks([path], CPU_SUITE), path
        assert not should_run_databricks([path], GPU_SUITE), path


def test_gpu_runs_for_gpu_runtime_and_notebooks_only():
    gpu_paths = [
        "docs/Explore Algorithms/Deep Learning/Quickstart - Fine-tune a Text Classifier.ipynb",
        "docs/Explore Algorithms/Deep Learning/Quickstart - Apply Phi Model.ipynb",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksGPUTests.scala",
    ]
    for path in gpu_paths:
        assert should_run_databricks([path], GPU_SUITE), path
        assert not should_run_databricks([path], CPU_SUITE), path


def test_shared_runtime_build_and_test_infrastructure_runs_both_suites():
    shared_paths = [
        "core/src/main/scala/example/Transformer.scala",
        "deep-learning/src/main/scala/example/DeepLearning.scala",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/Secrets.scala",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala",
        "core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala",
        "build.sbt",
        "project/Build.scala",
        "pipeline.yaml",
        "templates/publish.yml",
    ]
    for path in shared_paths:
        assert should_run_databricks([path], CPU_SUITE), path
        assert should_run_databricks([path], GPU_SUITE), path


def test_unknown_docs_assets_fail_open_but_known_metadata_skips():
    assert should_run_databricks(["docs/data/model.json"], CPU_SUITE)
    assert should_run_databricks(["docs/data/model.json"], GPU_SUITE)
    assert not should_run_databricks(["docs/.DS_Store"], CPU_SUITE)
    assert not should_run_databricks(["docs/.DS_Store"], GPU_SUITE)


def test_mixed_changes_run():
    paths = [
        "README.md",
        "cognitive/src/main/scala/example/Service.scala",
    ]
    assert should_run_databricks(paths, CPU_SUITE)
    assert not should_run_databricks(paths, GPU_SUITE)


def test_empty_or_unsafe_paths_fail_open():
    for suite in (CPU_SUITE, GPU_SUITE):
        assert should_run_databricks([], suite)
        assert should_run_databricks(["../outside-repository"], suite)
        assert should_run_databricks(["/absolute/path"], suite)


def test_cli_accepts_null_delimited_git_paths():
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.ci.databricks_impact",
            "--null",
            "--suite",
            GPU_SUITE,
        ],
        input=b"README.md\0tools/ci/README.md\0",
        capture_output=True,
        check=False,
    )
    assert process.returncode == 0
    assert process.stdout == b"false\n"


def test_cli_selects_only_the_requested_suite():
    path = b"lightgbm/src/main/scala/example/TrainUtils.scala\0"
    cpu = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.ci.databricks_impact",
            "--null",
            "--suite",
            CPU_SUITE,
        ],
        input=path,
        capture_output=True,
        check=False,
    )
    gpu = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.ci.databricks_impact",
            "--null",
            "--suite",
            GPU_SUITE,
        ],
        input=path,
        capture_output=True,
        check=False,
    )
    assert cpu.stdout == b"true\n"
    assert gpu.stdout == b"false\n"
