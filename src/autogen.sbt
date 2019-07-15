// Automatically generated, DO NOT EDIT

val topDir = file(".")

val `core-contracts` = (project in topDir / "core" / "contracts")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)

val `core-env` = (project in topDir / "core" / "env")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)

val `core-hadoop` = (project in topDir / "core" / "hadoop")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)

val `core-spark` = (project in topDir / "core" / "spark")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)

val `core-test-base` = (project in topDir / "core" / "test" / "base")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)

val `core-test-benchmarks` = (project in topDir / "core" / "test" / "benchmarks")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-test-base` % "compile->compile;test->test",
    `core-env` % "compile->compile;test->test")

val `core-test-datagen` = (project in topDir / "core" / "test" / "datagen")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-test-base` % "compile->compile;test->test")

val `core-utils` = (project in topDir / "core" / "utils")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-env` % "compile->compile;test->test")

val `core-test-fuzzing` = (project in topDir / "core" / "test" / "fuzzing")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-utils` % "compile->compile;test->test",
    `core-test-base` % "compile->compile;test->test",
    `core-contracts` % "compile->compile;test->test")

val `core-test` = (project in topDir / "core" / "test")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .aggregate(
    `core-test-base`,
    `core-test-benchmarks`,
    `core-test-datagen`,
    `core-test-fuzzing`)
  .dependsOn(
    `core-test-base` % "compile->compile;test->test",
    `core-test-benchmarks` % "compile->compile;test->test",
    `core-test-datagen` % "compile->compile;test->test",
    `core-test-fuzzing` % "compile->compile;test->test")

val `core-schema` = (project in topDir / "core" / "schema")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-test` % "compile->compile;test->test",
    `core-spark` % "compile->compile;test->test",
    `core-env` % "compile->compile;test->test")

val `core-metrics` = (project in topDir / "core" / "metrics")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-test` % "compile->compile;test->test",
    `core-spark` % "compile->compile;test->test",
    `core-env` % "compile->compile;test->test",
    `core-schema` % "compile->compile;test->test")

val `core-ml` = (project in topDir / "core" / "ml")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-test` % "compile->compile;test->test",
    `core-spark` % "compile->compile;test->test",
    `core-schema` % "compile->compile;test->test")

val `core-serialize` = (project in topDir / "core" / "serialize")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core-env` % "compile->compile;test->test",
    `core-utils` % "compile->compile;test->test",
    `core-test` % "compile->compile;test->test")

val `core` = (project in topDir / "core")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .aggregate(
    `core-contracts`,
    `core-env`,
    `core-hadoop`,
    `core-metrics`,
    `core-ml`,
    `core-schema`,
    `core-serialize`,
    `core-spark`,
    `core-test`,
    `core-utils`)
  .dependsOn(
    `core-contracts` % "compile->compile;test->test",
    `core-env` % "compile->compile;test->test",
    `core-hadoop` % "compile->compile;test->test",
    `core-metrics` % "compile->compile;test->test",
    `core-ml` % "compile->compile;test->test",
    `core-schema` % "compile->compile;test->test",
    `core-serialize` % "compile->compile;test->test",
    `core-spark` % "compile->compile;test->test",
    `core-test` % "compile->compile;test->test",
    `core-utils` % "compile->compile;test->test")

val `checkpoint-data` = (project in topDir / "checkpoint-data")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `clean-missing-data` = (project in topDir / "clean-missing-data")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `codegen` = (project in topDir / "codegen")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `downloader` = (project in topDir / "downloader")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `ensemble` = (project in topDir / "ensemble")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `io-binary` = (project in topDir / "io" / "binary")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `io-image` = (project in topDir / "io" / "image")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `io-binary` % "compile->compile;test->test")

val `multi-column-adapter` = (project in topDir / "multi-column-adapter")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `partition-sample` = (project in topDir / "partition-sample")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `pipeline-stages` = (project in topDir / "pipeline-stages")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `io-http` = (project in topDir / "io" / "http")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `io-binary` % "compile->compile;test->test",
    `pipeline-stages` % "compile->compile;test->test")

val `io-powerbi` = (project in topDir / "io" / "powerbi")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `io-http` % "compile->compile;test->test")

val `io` = (project in topDir / "io")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .aggregate(
    `io-binary`,
    `io-http`,
    `io-image`,
    `io-powerbi`)
  .dependsOn(
    `io-binary` % "compile->compile;test->test",
    `io-http` % "compile->compile;test->test",
    `io-image` % "compile->compile;test->test",
    `io-powerbi` % "compile->compile;test->test",
    `core` % "compile->compile;test->test")

val `image-transformer` = (project in topDir / "image-transformer")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `io` % "compile->compile;test->test")

val `cntk-model` = (project in topDir / "cntk-model")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `io` % "compile->compile;test->test",
    `image-transformer` % "compile->compile;test->test")

val `image-featurizer` = (project in topDir / "image-featurizer")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `io` % "compile->compile;test->test",
    `downloader` % "compile->compile;test->test",
    `cntk-model` % "compile->compile;test->test",
    `image-transformer` % "compile->compile;test->test",
    `pipeline-stages` % "compile->compile;test->test")

val `plot` = (project in topDir / "plot")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)

val `recommendation` = (project in topDir / "recommendation")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `summarize-data` = (project in topDir / "summarize-data")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `text-featurizer` = (project in topDir / "text-featurizer")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `pipeline-stages` % "compile->compile;test->test")

val `udf` = (project in topDir / "udf")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `value-indexer` = (project in topDir / "value-indexer")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test")

val `data-conversion` = (project in topDir / "data-conversion")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `value-indexer` % "compile->compile;test->test")

val `featurize` = (project in topDir / "featurize")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `multi-column-adapter` % "compile->compile;test->test",
    `value-indexer` % "compile->compile;test->test")

val `cntk-train` = (project in topDir / "cntk-train")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `featurize` % "compile->compile;test->test",
    `cntk-model` % "compile->compile;test->test")

val `lightgbm` = (project in topDir / "lightgbm")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `featurize` % "compile->compile;test->test")

val `train` = (project in topDir / "train")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `featurize` % "compile->compile;test->test")

val `compute-model-statistics` = (project in topDir / "compute-model-statistics")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `train` % "compile->compile;test->test")

val `compute-per-instance-statistics` = (project in topDir / "compute-per-instance-statistics")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `train` % "compile->compile;test->test")

val `find-best-model` = (project in topDir / "find-best-model")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `compute-model-statistics` % "compile->compile;test->test",
    `train` % "compile->compile;test->test")

val `tune-hyperparameters` = (project in topDir / "tune-hyperparameters")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `compute-model-statistics` % "compile->compile;test->test",
    `featurize` % "compile->compile;test->test",
    `train` % "compile->compile;test->test",
    `find-best-model` % "compile->compile;test->test")

val `vw` = (project in topDir / "vw")
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .dependsOn(
    `core` % "compile->compile;test->test",
    `featurize` % "compile->compile;test->test")

val `MMLSpark` = (project in topDir)
  .configs(IntegrationTest)
  .settings(Extras.defaultSettings: _*)
  .aggregate(
    `checkpoint-data`,
    `clean-missing-data`,
    `cntk-model`,
    `cntk-train`,
    `codegen`,
    `compute-model-statistics`,
    `compute-per-instance-statistics`,
    `core`,
    `data-conversion`,
    `downloader`,
    `ensemble`,
    `featurize`,
    `find-best-model`,
    `image-featurizer`,
    `image-transformer`,
    `io`,
    `lightgbm`,
    `multi-column-adapter`,
    `partition-sample`,
    `pipeline-stages`,
    `plot`,
    `recommendation`,
    `summarize-data`,
    `text-featurizer`,
    `train`,
    `tune-hyperparameters`,
    `udf`,
    `value-indexer`,
    `vw`)
  .dependsOn(
    `checkpoint-data` % "compile->compile;optional",
    `clean-missing-data` % "compile->compile;optional",
    `cntk-model` % "compile->compile;optional",
    `cntk-train` % "compile->compile;optional",
    `codegen` % "compile->compile;optional",
    `compute-model-statistics` % "compile->compile;optional",
    `compute-per-instance-statistics` % "compile->compile;optional",
    `core` % "compile->compile;optional",
    `data-conversion` % "compile->compile;optional",
    `downloader` % "compile->compile;optional",
    `ensemble` % "compile->compile;optional",
    `featurize` % "compile->compile;optional",
    `find-best-model` % "compile->compile;optional",
    `image-featurizer` % "compile->compile;optional",
    `image-transformer` % "compile->compile;optional",
    `io` % "compile->compile;optional",
    `lightgbm` % "compile->compile;optional",
    `multi-column-adapter` % "compile->compile;optional",
    `partition-sample` % "compile->compile;optional",
    `pipeline-stages` % "compile->compile;optional",
    `plot` % "compile->compile;optional",
    `recommendation` % "compile->compile;optional",
    `summarize-data` % "compile->compile;optional",
    `text-featurizer` % "compile->compile;optional",
    `train` % "compile->compile;optional",
    `tune-hyperparameters` % "compile->compile;optional",
    `udf` % "compile->compile;optional",
    `value-indexer` % "compile->compile;optional",
    `vw` % "compile->compile;optional")
