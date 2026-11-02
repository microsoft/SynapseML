# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import re
import tempfile
import unittest
from contextlib import ExitStack

from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from synapse.ml.core.init_spark import init_spark
from synapse.ml.lightgbm import (
    LightGBMClassifier,
    LightGBMClassificationModel,
    LightGBMRegressor,
    LightGBMRegressionModel,
    LightGBMRanker,
    LightGBMRankerModel,
)

spark = init_spark()


class FeatureFractionByNodeTests(unittest.TestCase):
    learners = (
        (LightGBMClassifier, LightGBMClassificationModel),
        (LightGBMRegressor, LightGBMRegressionModel),
        (LightGBMRanker, LightGBMRankerModel),
    )

    @classmethod
    def setUpClass(cls):
        cls.data = spark.createDataFrame(
            [
                Row(
                    label=float(index % 2),
                    group=index // 6,
                    features=Vectors.dense(
                        float(index % 2),
                        float(index % 3),
                        float(index % 5),
                        float(index),
                    ),
                )
                for index in range(48)
            ]
        ).coalesce(1)

    def test_default_copy_clear_and_estimator_persistence(self):
        for learner, _ in self.learners:
            with self.subTest(learner=learner.__name__):
                stage = learner()
                self.assertEqual(stage.getFeatureFractionByNode(), 1.0)
                self.assertFalse(stage.isSet(stage.featureFractionByNode))
                self.assertEqual(stage.copy().getFeatureFractionByNode(), 1.0)
                with tempfile.TemporaryDirectory() as directory:
                    path = os.path.join(directory, "unset")
                    stage.save(path)
                    restored = learner.load(path)
                    self.assertEqual(restored.getFeatureFractionByNode(), 1.0)
                    self.assertTrue(restored.isSet(restored.featureFractionByNode))
                    self.assertEqual(
                        restored.getFeatureFractionByNode(),
                        restored.getFeatureFraction(),
                    )
                    self.assertTrue(restored.isSet(restored.featureFraction))
                    restored.setFeatureFractionByNode(0.4)
                    restored.clear(restored.featureFractionByNode)
                    self.assertFalse(restored.isSet(restored.featureFractionByNode))
                    self.assertEqual(restored.getFeatureFractionByNode(), 1.0)
                    stage.setFeatureFractionByNode(0.4)
                    self.assertEqual(stage.copy().getFeatureFractionByNode(), 0.4)
                    path = os.path.join(directory, "set")
                    stage.save(path)
                    self.assertEqual(learner.load(path).getFeatureFractionByNode(), 0.4)
                stage.clear(stage.featureFractionByNode)
                self.assertEqual(stage.getFeatureFractionByNode(), 1.0)
                self.assertFalse(stage.isSet(stage.featureFractionByNode))

    def test_generated_wrappers_train_with_native_parameter_and_persist(self):
        for learner, model_type in self.learners:
            for mode in ("bulk", "streaming"):
                with self.subTest(learner=learner.__name__, mode=mode):
                    stage = learner(
                        numIterations=3,
                        numLeaves=3,
                        minDataInLeaf=1,
                        minDataPerBin=1,
                        numTasks=1,
                        numThreads=1,
                        seed=42,
                        dataTransferMode=mode,
                        featureFractionByNode=0.4,
                    )
                    if learner is LightGBMRanker:
                        stage.setGroupCol("group")
                    with ExitStack() as cleanup:
                        model = stage.fit(self.data)
                        cleanup.callback(
                            model._java_obj.getLightGBMBooster().freeNativeMemory
                        )
                        self.assertAlmostEqual(self.effective_fraction(model), 0.4)
                        expected = model.transform(self.data).collect()
                        with tempfile.TemporaryDirectory() as directory:
                            path = os.path.join(directory, "model")
                            model.save(path)
                            restored = model_type.load(path)
                            cleanup.callback(
                                restored._java_obj.getLightGBMBooster().freeNativeMemory
                            )
                            self.assertAlmostEqual(
                                self.effective_fraction(restored), 0.4
                            )
                            self.assertEqual(
                                restored.transform(self.data).collect(), expected
                            )

    def test_alias_sampling_survives_estimator_persistence(self):
        for mode in ("bulk", "streaming"):
            for alias in ("colsample_bynode", "sub_feature_bynode"):
                with self.subTest(mode=mode, alias=alias):
                    stage = LightGBMRegressor(
                        numIterations=3,
                        numLeaves=3,
                        minDataInLeaf=1,
                        minDataPerBin=1,
                        numTasks=1,
                        numThreads=1,
                        seed=42,
                        featureFractionSeed=42,
                        dataTransferMode=mode,
                        passThroughArgs=f"{alias}=0.7",
                    )
                    with ExitStack() as cleanup:
                        model = stage.fit(self.data)
                        cleanup.callback(
                            model._java_obj.getLightGBMBooster().freeNativeMemory
                        )
                        self.assertAlmostEqual(self.effective_fraction(model), 0.7)
                        expected = model.transform(self.data).collect()
                        with tempfile.TemporaryDirectory() as directory:
                            path = os.path.join(directory, "estimator")
                            stage.save(path)
                            restored = LightGBMRegressor.load(path)
                            restored_model = restored.fit(self.data)
                            cleanup.callback(
                                restored_model._java_obj.getLightGBMBooster().freeNativeMemory
                            )
                            self.assertAlmostEqual(
                                self.effective_fraction(restored_model), 0.7
                            )
                            self.assertEqual(
                                restored_model.transform(self.data).collect(), expected
                            )

    def effective_fraction(self, model):
        match = re.search(
            r"^\[feature_fraction_bynode: ([^\]]+)\]$",
            model.getNativeModel(),
            re.MULTILINE,
        )
        self.assertIsNotNone(match, "Native model must expose effective node sampling")
        return float(match.group(1))
